"""
HUD Vacancy Agent
-----------------
Fetches USPS vacancy data from the HUD User API for target ZIP codes
in key Texas industrial submarkets, calculates commercial vacancy rates
and quarter-over-quarter changes, upserts to the vacancy_data table,
and creates signals when significant vacancy trends are detected.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import httpx
from prefect import flow, task

from shared.config import HUD_API_TOKEN
from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "hud_vacancy"

HUD_USPS_URL = "https://www.huduser.gov/hudapi/public/usps"

TARGET_ZIPS: list[str] = [
    "77040", "77041", "77032", "77037",  # Houston area
    "75050", "75051", "75234",            # DFW area
    "78724", "78744", "78217", "78218",   # Austin / San Antonio area
]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch_vacancy", retries=2, retry_delay_seconds=30)
def fetch_vacancy(zip_code: str) -> dict[str, Any]:
    """Fetch USPS vacancy data for a single ZIP from the HUD API.

    type=2 returns All Residential, Business, and Other vacancy data
    at the ZIP code level on a quarterly basis.
    """
    headers = {
        "Authorization": f"Bearer {HUD_API_TOKEN}",
    }
    params = {
        "type": "2",
        "query": zip_code,
    }
    resp = httpx.get(HUD_USPS_URL, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


@task(name="process_and_signal_vacancy")
def process_and_signal(zip_data: Any, zip_code: str) -> int:
    """Extract vacancy metrics, compute QoQ change, upsert, and signal.

    The HUD USPS API returns a list of quarterly observations. Each record
    typically includes fields like:
      - year, quarter
      - bus_vac (business vacancies)
      - total_addr / bus_addr (total and business addresses)
      - ams_res / ams_bus (AMS residential / business)

    Returns the number of rows written.
    """
    client = get_client()

    # The API may return data in a list or under a "data" key
    records = zip_data if isinstance(zip_data, list) else zip_data.get("data", [])

    if not records:
        return 0

    rows_to_upsert: list[dict[str, Any]] = []
    vacancy_rates: list[dict[str, Any]] = []  # for QoQ comparison

    for rec in records:
        year = rec.get("year")
        quarter = rec.get("quarter")
        if not year or not quarter:
            continue

        quarter_label = f"{year}-Q{quarter}"

        # Commercial vacancy rate calculation
        bus_vac = _to_float(rec.get("bus_vac", 0))
        bus_addr = _to_float(rec.get("bus_addr", 0))
        ams_bus = _to_float(rec.get("ams_bus", 0))

        # Use the larger denominator available
        denom = bus_addr if bus_addr > 0 else ams_bus
        vacancy_rate = round((bus_vac / denom) * 100, 2) if denom > 0 else None

        row = {
            "id": str(uuid.uuid4()),
            "zip": zip_code,
            "quarter": quarter_label,
            "year": int(year),
            "quarter_num": int(quarter),
            "business_vacancies": bus_vac,
            "business_addresses": denom,
            "vacancy_rate_pct": vacancy_rate,
            "source": "hud_usps",
            "raw_data": rec,
        }
        rows_to_upsert.append(row)

        if vacancy_rate is not None:
            vacancy_rates.append(
                {
                    "quarter": quarter_label,
                    "year": int(year),
                    "quarter_num": int(quarter),
                    "rate": vacancy_rate,
                }
            )

    if not rows_to_upsert:
        return 0

    result = (
        client.table("vacancy_data")
        .upsert(rows_to_upsert, on_conflict="zip,quarter")
        .execute()
    )
    written = len(result.data) if result.data else 0

    # ------------------------------------------------------------------
    # QoQ signal: compare the two most recent quarters
    # ------------------------------------------------------------------
    if len(vacancy_rates) >= 2:
        sorted_rates = sorted(
            vacancy_rates,
            key=lambda r: (r["year"], r["quarter_num"]),
        )
        latest = sorted_rates[-1]
        previous = sorted_rates[-2]
        qoq_change = round(latest["rate"] - previous["rate"], 2)  # pp change

        _create_vacancy_signal(zip_code, qoq_change, latest, previous)

    return written


def _to_float(val: Any) -> float:
    """Safely convert a value to float, defaulting to 0."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _create_vacancy_signal(
    zip_code: str,
    qoq_pp: float,
    latest: dict[str, Any],
    previous: dict[str, Any],
) -> None:
    """Create a vacancy_trend signal based on QoQ thresholds."""
    abs_change = abs(qoq_pp)

    if qoq_pp > 3:
        severity = "alert"
        direction_note = "rising sharply"
        implication = "Rising commercial vacancy may indicate weakening demand."
    elif qoq_pp > 1.5:
        severity = "watch"
        direction_note = "rising"
        implication = "Moderately rising vacancy warrants monitoring."
    elif qoq_pp < -2:
        severity = "watch"
        direction_note = "tightening"
        implication = "Rapidly tightening vacancy may signal opportunity."
    else:
        return  # Below threshold — no signal

    direction = "up" if qoq_pp > 0 else "down"

    create_signal(
        agent_name=AGENT_NAME,
        signal_type="vacancy_trend",
        severity=severity,
        title=f"ZIP {zip_code} vacancy {direction_note} ({qoq_pp:+.1f}pp QoQ)",
        summary=(
            f"Commercial vacancy in ZIP {zip_code} moved {direction} {abs_change:.1f} "
            f"percentage points QoQ: {previous['rate']:.1f}% ({previous['quarter']}) -> "
            f"{latest['rate']:.1f}% ({latest['quarter']}). {implication}"
        ),
        data={
            "zip": zip_code,
            "current_quarter": latest["quarter"],
            "current_rate_pct": latest["rate"],
            "previous_quarter": previous["quarter"],
            "previous_rate_pct": previous["rate"],
            "qoq_change_pp": qoq_pp,
        },
        source_url="https://www.huduser.gov/portal/usps/home.html",
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="hud_vacancy_agent", log_prints=True)
def hud_vacancy_agent() -> dict[str, Any]:
    """Orchestrate: fetch vacancy data for each target ZIP, process, signal, log."""
    # ---- Credential gate ----
    if not HUD_API_TOKEN:
        print(
            "HUD API token not configured. "
            "Register at https://www.huduser.gov/hudapi/public/register"
        )
        return {"status": "skipped", "reason": "credentials_not_configured"}

    total_pulled = 0
    total_new = 0
    errors: list[str] = []

    for zip_code in TARGET_ZIPS:
        try:
            print(f"Fetching vacancy data for ZIP {zip_code}...")
            data = fetch_vacancy(zip_code)
            records = data if isinstance(data, list) else data.get("data", [])
            total_pulled += len(records)

            new_count = process_and_signal(data, zip_code)
            total_new += new_count
            print(f"  -> {new_count} rows upserted for {zip_code}")

            # Polite delay between API calls
            time.sleep(1)
        except Exception as exc:
            print(f"  -> FAILED for {zip_code}: {exc}")
            errors.append(f"{zip_code}: {exc}")

    status = "success" if not errors else ("success" if total_new > 0 else "failed")
    log_agent_run(
        agent_name=AGENT_NAME,
        status=status,
        records_pulled=total_pulled,
        records_new=total_new,
        error_message="; ".join(errors) if errors else None,
    )
    print(
        f"Agent complete: {total_pulled} pulled, {total_new} new/updated, "
        f"{len(errors)} errors"
    )
    return {
        "status": status,
        "pulled": total_pulled,
        "new": total_new,
        "errors": errors,
    }


if __name__ == "__main__":
    hud_vacancy_agent()
