"""
Census Demographics Agent
--------------------------
Pulls population and median household income data from the Census ACS5 API
for target ZIP codes in Texas industrial corridors. Compares year-over-year
changes and creates signals for significant demographic shifts.
"""

from __future__ import annotations

import uuid
from typing import Any, Optional

import httpx
from prefect import flow, task

from shared.config import CENSUS_API_KEY
from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "census_demographics"

ACS5_BASE = "https://api.census.gov/data/{year}/acs/acs5"

# Flat list of target ZIP codes across Texas industrial corridors
TARGET_ZIPS: list[str] = [
    "77040", "77041", "77064",  # Houston NW
    "77032", "77037", "77044",  # Houston NE
    "77034", "77047",           # Houston SE
    "75050", "75051",           # DFW South
    "75234", "75247",           # DFW North
    "78724", "78741", "78744",  # Austin East
    "78217", "78218", "78219",  # San Antonio NE
]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch_census_data", retries=2, retry_delay_seconds=30)
def fetch_census_data(year: int, zip_code: str) -> Optional[dict[str, Any]]:
    """Fetch ACS5 population and income data for a single ZIP code and year.

    Returns a dict with keys B01003_001E (population), B19013_001E (median
    household income), and NAME, or None on error.
    """
    url = ACS5_BASE.format(year=year)
    params = {
        "get": "B01003_001E,B19013_001E,NAME",
        "for": f"zip code tabulation area:{zip_code}",
        "key": CENSUS_API_KEY,
    }

    try:
        resp = httpx.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if not data or len(data) < 2:
            print(f"  No data returned for ZIP {zip_code} ({year})")
            return None

        headers = data[0]
        values = data[1]
        return dict(zip(headers, values))

    except Exception as exc:
        print(f"  Error fetching ZIP {zip_code} ({year}): {exc}")
        return None


@task(name="compute_and_store")
def compute_and_store(
    zip_code: str,
    current: dict[str, Any],
    previous: dict[str, Any],
) -> int:
    """Compute year-over-year changes and upsert to census_data.

    Returns the number of signals created.
    """
    client = get_client()
    signal_count = 0

    # Parse values (Census returns strings; -666666666 means not available)
    try:
        pop_current = int(current.get("B01003_001E", 0))
        pop_previous = int(previous.get("B01003_001E", 0))
        income_current = int(current.get("B19013_001E", 0))
        income_previous = int(previous.get("B19013_001E", 0))
    except (ValueError, TypeError):
        print(f"  Could not parse numeric values for ZIP {zip_code}")
        return 0

    # Skip sentinel values indicating data unavailable
    if pop_current < 0 or pop_previous < 0:
        print(f"  Population data unavailable for ZIP {zip_code}")
        return 0

    # Compute change percentages
    pop_change_pct = (
        ((pop_current - pop_previous) / pop_previous * 100)
        if pop_previous > 0
        else 0.0
    )
    income_change_pct = (
        ((income_current - income_previous) / income_previous * 100)
        if income_previous > 0 and income_current > 0 and income_previous > 0
        else 0.0
    )

    source_url = f"https://data.census.gov/table?q={zip_code}"

    # Upsert population row
    pop_row = {
        "id": str(uuid.uuid4()),
        "geography_type": "zip",
        "geography_id": zip_code,
        "metric": "population",
        "year": 2023,
        "value": pop_current,
        "previous_value": pop_previous,
        "change_pct": round(pop_change_pct, 2),
        "source": "census_acs5",
        "raw_data": {"source_url": source_url},
    }

    income_row = {
        "id": str(uuid.uuid4()),
        "geography_type": "zip",
        "geography_id": zip_code,
        "metric": "median_household_income",
        "year": 2023,
        "value": income_current,
        "previous_value": income_previous,
        "change_pct": round(income_change_pct, 2),
        "source": "census_acs5",
        "raw_data": {"source_url": source_url},
    }

    client.table("census_data").upsert(
        [pop_row, income_row],
        on_conflict="geography_type,geography_id,metric,year",
    ).execute()

    # --- Signal logic ---
    severity: str | None = None

    # Population change signals
    if abs(pop_change_pct) > 5:
        severity = "alert"
    elif abs(pop_change_pct) > 3:
        severity = "watch"
    elif abs(income_change_pct) > 5:
        severity = "info"

    if severity is not None:
        direction = "grew" if pop_change_pct > 0 else "declined"
        create_signal(
            agent_name=AGENT_NAME,
            signal_type="demographic_shift",
            severity=severity,
            title=f"ZIP {zip_code} population {direction} {abs(pop_change_pct):.1f}%",
            summary=(
                f"Census ACS5 data for ZIP {zip_code}: population {direction} from "
                f"{pop_previous:,} to {pop_current:,} ({pop_change_pct:+.1f}%). "
                f"Median household income changed {income_change_pct:+.1f}% "
                f"(${income_previous:,} -> ${income_current:,})."
            ),
            data={
                "zip_code": zip_code,
                "population_current": pop_current,
                "population_previous": pop_previous,
                "pop_change_pct": round(pop_change_pct, 2),
                "income_current": income_current,
                "income_previous": income_previous,
                "income_change_pct": round(income_change_pct, 2),
            },
            source_url=source_url,
        )
        signal_count += 1

    return signal_count


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="census_demographics_agent", log_prints=True)
def census_demographics_agent() -> dict[str, Any]:
    """Fetch ACS5 demographic data for target ZIPs and detect shifts."""
    if not CENSUS_API_KEY:
        print("WARNING: CENSUS_API_KEY is not set. Skipping census demographics agent.")
        return {"status": "skipped", "reason": "no_api_key"}

    total_pulled = 0
    total_new = 0
    total_signals = 0

    try:
        for zip_code in TARGET_ZIPS:
            print(f"\nFetching census data for ZIP {zip_code} …")

            current = fetch_census_data(2023, zip_code)
            previous = fetch_census_data(2022, zip_code)

            if current is not None:
                total_pulled += 1
            if previous is not None:
                total_pulled += 1

            if current is None or previous is None:
                print(f"  Skipping ZIP {zip_code}: missing data for one or both years")
                continue

            total_new += 1
            signals = compute_and_store(zip_code, current, previous)
            total_signals += signals
            print(f"  ZIP {zip_code}: stored, {signals} signal(s)")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_pulled,
            records_new=total_new,
        )
        print(
            f"\nAgent complete: {total_pulled} fetches, "
            f"{total_new} ZIPs stored, {total_signals} signals"
        )
        return {
            "status": "success",
            "pulled": total_pulled,
            "new": total_new,
            "signals": total_signals,
        }

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
    census_demographics_agent()
