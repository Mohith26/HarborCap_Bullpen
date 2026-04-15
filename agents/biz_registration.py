"""
Business Registration Agent
----------------------------
Scans the TX Comptroller Sales Tax Payer Location API for new business
registrations in target ZIP codes (Houston, DFW, Austin, San Antonio
industrial corridors). Flags businesses with NAICS codes indicating
warehouse/logistics/manufacturing demand.
"""

from __future__ import annotations

import uuid
from typing import Any

import httpx
from prefect import flow, task

from shared.config import TX_COMPTROLLER_TAX_KEY
from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "biz_registration"

COMPTROLLER_API = "https://api.comptroller.texas.gov/public-data/v1/public"

# Target ZIP codes in Texas industrial corridors
TARGET_ZIPS: dict[str, list[str]] = {
    "Houston NW": ["77040", "77041", "77064", "77065", "77070", "77429"],
    "Houston NE": ["77032", "77037", "77038", "77039", "77044", "77396"],
    "Houston SE": ["77034", "77047", "77048", "77053", "77061", "77087"],
    "DFW South": ["75050", "75051", "75052", "76010", "76011", "76015"],
    "DFW North": ["75234", "75235", "75229", "75247", "75261", "75212"],
    "Austin East": ["78724", "78725", "78741", "78744", "78719", "78753"],
    "San Antonio NE": ["78217", "78218", "78219", "78233", "78244", "78109"],
}

# NAICS prefixes that signal warehouse/industrial/logistics tenant demand
HIGH_DEMAND_NAICS = {
    "484": "Truck Transportation",
    "493": "Warehousing and Storage",
    "423": "Durable Goods Wholesale",
    "424": "Nondurable Goods Wholesale",
    "425": "Electronic Markets & Agents",
    "33": "Manufacturing",
    "32": "Manufacturing",
    "31": "Manufacturing",
    "488": "Support Activities for Transportation",
    "492": "Couriers and Messengers",
    "541": "Professional/Scientific/Technical",
}


@task(name="fetch_locations_by_zip", retries=2, retry_delay_seconds=30)
def fetch_locations_by_zip(zip_code: str) -> list[dict[str, Any]]:
    """Pull sales tax payer locations for a ZIP, sorted by newest permits first."""
    all_records: list[dict[str, Any]] = []
    page = 1
    page_size = 100

    while True:
        params = {
            "ZIPCODE": zip_code,
            "page": str(page),
            "pageSize": str(page_size),
            "sortBy": "PERMIT_START_DT",
            "sortOrder": "DESC",
        }
        headers = {"x-api-key": TX_COMPTROLLER_TAX_KEY}

        resp = httpx.get(
            f"{COMPTROLLER_API}/sales-tax-payer-location",
            params=params,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()

        records = body.get("data", [])
        if not records:
            break

        all_records.extend(records)

        # Only fetch first 3 pages (300 records) per ZIP to stay within rate limits
        total = body.get("count", 0)
        if page * page_size >= total or page >= 3:
            break
        page += 1

    return all_records


def _classify_demand(record: dict[str, Any]) -> tuple[str, str | None]:
    """Classify a business record by NAICS prefix.

    Returns (demand_level, industry_label).
    """
    naics = record.get("NAICS_CODE") or record.get("NAICS") or ""
    for prefix, label in HIGH_DEMAND_NAICS.items():
        if naics.startswith(prefix):
            return "high", label
    return "standard", None


@task(name="store_and_signal_registrations")
def store_and_signal_registrations(
    records: list[dict[str, Any]], corridor_name: str
) -> tuple[int, int]:
    """Upsert records into business_registrations and create signals for high-demand.

    Returns (rows_written, signals_created).
    """
    client = get_client()
    rows: list[dict[str, Any]] = []
    signal_count = 0

    for r in records:
        demand, industry = _classify_demand(r)

        taxpayer_id = r.get("TAXPAYER_ID", "")
        location_number = r.get("LOCATION_NUMBER", "")
        unique_id = f"{taxpayer_id}-{location_number}" if location_number else taxpayer_id

        row = {
            "id": str(uuid.uuid4()),
            "taxpayer_id": unique_id,
            "business_name": r.get("LOCATION_NAME") or r.get("BUSINESS_NAME", ""),
            "street": r.get("STREET", ""),
            "city": (r.get("CITY") or "").title(),
            "zip": r.get("ZIPCODE", ""),
            "status": r.get("STATUS", ""),
            "source": "tx_comptroller",
            "raw_data": r,
        }
        rows.append(row)

        if demand == "high":
            biz_name = row["business_name"]
            city = row["city"]
            zip_code = row["zip"]

            create_signal(
                agent_name=AGENT_NAME,
                signal_type="new_business",
                severity="watch",
                title=f"New industrial business: {biz_name[:60]}",
                summary=(
                    f"{biz_name} registered in {city} {zip_code} ({corridor_name}). "
                    f"Industry: {industry}. Likely warehouse/logistics tenant demand."
                ),
                submarket=corridor_name,
                data={
                    "business_name": biz_name,
                    "naics": r.get("NAICS_CODE") or r.get("NAICS", ""),
                    "industry": industry,
                    "zip": zip_code,
                    "corridor": corridor_name,
                    "taxpayer_id": taxpayer_id,
                },
            )
            signal_count += 1

    if not rows:
        return 0, 0

    # Dedup by taxpayer_id — same taxpayer may appear multiple times in a
    # single corridor (multiple locations per taxpayer) and the upsert
    # will 500 on "ON CONFLICT DO UPDATE command cannot affect row a
    # second time". Keep the first occurrence for each unique key.
    by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row.get("taxpayer_id") or str(uuid.uuid4())
        if key not in by_key:
            by_key[key] = row
    deduped = list(by_key.values())

    # Upsert in batches of 50 to avoid payload limits
    written = 0
    for i in range(0, len(deduped), 50):
        batch = deduped[i : i + 50]
        result = (
            client.table("business_registrations")
            .upsert(batch, on_conflict="taxpayer_id")
            .execute()
        )
        written += len(result.data) if result.data else 0

    return written, signal_count


@flow(name="biz_registration_agent", log_prints=True)
def biz_registration_agent() -> dict[str, Any]:
    """Scan TX Comptroller for new business registrations in target ZIP codes."""
    total_pulled = 0
    total_new = 0
    total_signals = 0

    try:
        for corridor_name, zips in TARGET_ZIPS.items():
            print(f"\n--- {corridor_name} ---")
            corridor_records: list[dict[str, Any]] = []

            for zip_code in zips:
                records = fetch_locations_by_zip(zip_code)
                print(f"  ZIP {zip_code}: {len(records)} records")
                corridor_records.extend(records)

            total_pulled += len(corridor_records)

            if corridor_records:
                written, signals = store_and_signal_registrations(
                    corridor_records, corridor_name
                )
                total_new += written
                total_signals += signals
                print(f"  -> {written} stored, {signals} high-demand signals")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_pulled,
            records_new=total_new,
        )
        print(f"\nAgent complete: {total_pulled} pulled, {total_new} stored, {total_signals} signals")
        return {"status": "success", "pulled": total_pulled, "new": total_new, "signals": total_signals}

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
    biz_registration_agent()
