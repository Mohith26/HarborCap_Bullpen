"""
Sales Tax Revenue Agent
-----------------------
Downloads sales tax allocation data from the TX Comptroller SIFT API,
filters for target Texas cities, computes year-over-year changes,
stores results, and creates signals for notable growth or decline.
"""

from __future__ import annotations

import csv
import io
import uuid
from typing import Any

import httpx
from prefect import flow, task

from shared.config import TX_COMPTROLLER_SIFT_KEY
from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "sales_tax_revenue"

SIFT_LIST_URL = "https://api.comptroller.texas.gov/sift/v1/sift/public/list-files"
SIFT_GET_URL = "https://api.comptroller.texas.gov/sift/v1/sift/public/get-link"

TARGET_CITIES: set[str] = {
    "HOUSTON",
    "DALLAS",
    "FORT WORTH",
    "AUSTIN",
    "SAN ANTONIO",
    "EL PASO",
    "LAREDO",
    "ARLINGTON",
    "GRAND PRAIRIE",
    "MCKINNEY",
    "FRISCO",
    "ROUND ROCK",
    "GEORGETOWN",
    "KYLE",
    "NEW BRAUNFELS",
    "CONROE",
    "SUGAR LAND",
    "KATY",
    "PFLUGERVILLE",
    "MIDLAND",
    "ODESSA",
}

SOURCE_URL = "https://comptroller.texas.gov/transparency/local/allocations/sales-tax/"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="list_sift_files", retries=2, retry_delay_seconds=10)
def list_sift_files() -> list[str]:
    """GET the SIFT list-files endpoint and return paths related to sales tax allocations."""
    headers = {"x-api-key": TX_COMPTROLLER_SIFT_KEY}
    resp = httpx.get(SIFT_LIST_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    # The response may be a list of file objects or a dict with a files key.
    files: list[Any] = []
    if isinstance(payload, list):
        files = payload
    elif isinstance(payload, dict):
        files = payload.get("files", payload.get("data", payload.get("items", [])))

    matching_paths: list[str] = []
    for entry in files:
        # Each entry may be a string path or a dict with a path/name/key field.
        if isinstance(entry, str):
            path = entry
        elif isinstance(entry, dict):
            path = entry.get("path") or entry.get("name") or entry.get("key") or ""
        else:
            continue

        path_lower = path.lower()
        if "alloc" in path_lower or "sales-tax" in path_lower or "sales_tax" in path_lower:
            matching_paths.append(path)

    # Sort so the most recent file (typically last alphabetically by date) comes last.
    matching_paths.sort()
    return matching_paths


@task(name="download_and_parse_csv", retries=2, retry_delay_seconds=30)
def download_and_parse_csv(path: str) -> list[dict[str, Any]]:
    """Download a CSV from SIFT via the get-link endpoint and parse it."""
    headers = {"x-api-key": TX_COMPTROLLER_SIFT_KEY}
    params = {"file-path": path}

    # The API returns a 307 redirect to a signed download URL.
    resp = httpx.get(
        SIFT_GET_URL,
        headers=headers,
        params=params,
        timeout=60,
        follow_redirects=True,
    )
    resp.raise_for_status()

    # If the response is JSON with a URL, follow it; otherwise treat body as CSV.
    content_type = resp.headers.get("content-type", "")
    if "json" in content_type:
        body = resp.json()
        download_url = (
            body.get("url")
            or body.get("link")
            or body.get("download_url")
            or body.get("signedUrl")
            or ""
        )
        if download_url:
            csv_resp = httpx.get(download_url, timeout=120, follow_redirects=True)
            csv_resp.raise_for_status()
            text = csv_resp.text
        else:
            text = resp.text
    else:
        text = resp.text

    reader = csv.DictReader(io.StringIO(text))
    return [row for row in reader]


@task(name="process_and_signal")
def process_and_signal(rows: list[dict[str, Any]]) -> int:
    """Filter for target cities, compute YoY change, upsert, and create signals."""
    client = get_client()
    upsert_rows: list[dict[str, Any]] = []
    signal_count = 0

    for row in rows:
        # Identify city field (may be named differently across CSV versions).
        city_raw = (
            row.get("city")
            or row.get("City")
            or row.get("CITY")
            or row.get("city_name")
            or row.get("City Name")
            or row.get("jurisdiction")
            or row.get("Jurisdiction")
            or ""
        )
        city_upper = city_raw.strip().upper()

        if city_upper not in TARGET_CITIES:
            continue

        # Extract payment fields flexibly.
        net_payment = _parse_float(
            row.get("net_payment")
            or row.get("Net Payment")
            or row.get("net_payment_this_period")
            or row.get("Net Payment This Period")
            or row.get("current_period")
            or row.get("Current Period")
            or "0"
        )
        prior_year_payment = _parse_float(
            row.get("prior_year_payment")
            or row.get("Prior Year Payment")
            or row.get("comparable_payment_prior_year")
            or row.get("Comparable Payment Prior Year")
            or row.get("prior_period")
            or row.get("Prior Period")
            or "0"
        )

        # Extract period field.
        period = (
            row.get("period")
            or row.get("Period")
            or row.get("payment_date")
            or row.get("Payment Date")
            or row.get("reporting_period")
            or row.get("Reporting Period")
            or "unknown"
        )

        # Compute year-over-year change.
        if prior_year_payment and prior_year_payment != 0:
            yoy_change = (net_payment - prior_year_payment) / prior_year_payment * 100
        else:
            yoy_change = 0.0

        db_row: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "city": city_upper,
            "period": str(period).strip(),
            "net_payment": net_payment,
            "prior_year_payment": prior_year_payment,
            "yoy_change_pct": round(yoy_change, 2),
            "source": "tx_comptroller_sift",
            "raw_data": row,
        }
        upsert_rows.append(db_row)

        # Determine signal severity based on YoY change.
        severity = None
        if yoy_change > 15 or yoy_change < -10:
            severity = "alert"
        elif yoy_change > 10 or yoy_change < -5:
            severity = "watch"
        elif yoy_change > 5:
            severity = "info"

        if severity:
            direction = "up" if yoy_change > 0 else "down"
            create_signal(
                agent_name=AGENT_NAME,
                signal_type="sales_tax_growth",
                severity=severity,
                title=f"{city_upper} sales tax {direction} {abs(yoy_change):.1f}% YoY",
                summary=(
                    f"{city_upper} net sales tax payment: ${net_payment:,.0f} vs "
                    f"prior year ${prior_year_payment:,.0f} ({yoy_change:+.1f}% YoY). "
                    f"Period: {period}."
                ),
                data={
                    "city": city_upper,
                    "period": str(period).strip(),
                    "net_payment": net_payment,
                    "prior_year_payment": prior_year_payment,
                    "yoy_change_pct": round(yoy_change, 2),
                },
                source_url=SOURCE_URL,
            )
            signal_count += 1

    # Upsert all rows.
    written = 0
    if upsert_rows:
        result = (
            client.table("sales_tax_data")
            .upsert(upsert_rows, on_conflict="city,period")
            .execute()
        )
        written = len(result.data) if result.data else 0

    print(f"Created {signal_count} signals for {len(upsert_rows)} city records")
    return written


def _parse_float(value: Any) -> float:
    """Safely parse a numeric value, stripping currency symbols and commas."""
    if value is None:
        return 0.0
    s = str(value).strip().replace("$", "").replace(",", "").replace(" ", "")
    if not s or s == "-":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="sales_tax_revenue_agent", log_prints=True)
def sales_tax_revenue_agent() -> dict[str, Any]:
    """Orchestrate: list files, download most recent, process, signal, log."""
    total_pulled = 0
    total_new = 0

    if not TX_COMPTROLLER_SIFT_KEY:
        print("WARNING: TX_COMPTROLLER_SIFT_KEY is not set. Skipping sales tax agent.")
        return {"status": "skipped", "reason": "no_api_key"}

    try:
        file_paths = list_sift_files()
        print(f"Found {len(file_paths)} sales-tax allocation files in SIFT")

        if not file_paths:
            print("No matching sales tax allocation files found. Nothing to process.")
            log_agent_run(
                agent_name=AGENT_NAME,
                status="success",
                records_pulled=0,
                records_new=0,
            )
            return {"status": "success", "pulled": 0, "new": 0, "note": "no_files_found"}

        # Use the most recent file (last after sort).
        most_recent = file_paths[-1]
        print(f"Downloading most recent file: {most_recent}")

        rows = download_and_parse_csv(most_recent)
        total_pulled = len(rows)
        print(f"Parsed {total_pulled} rows from CSV")

        total_new = process_and_signal(rows)
        print(f"Stored {total_new} city records")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_pulled,
            records_new=total_new,
        )
        return {"status": "success", "pulled": total_pulled, "new": total_new}

    except Exception as exc:
        log_agent_run(
            agent_name=AGENT_NAME,
            status="failed",
            records_pulled=total_pulled,
            records_new=total_new,
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    sales_tax_revenue_agent()
