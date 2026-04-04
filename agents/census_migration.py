"""
Census Migration Agent
-----------------------
Pulls county-level migration flow data from the Census ACS Flows API
for target Texas counties. Identifies counties with significant net
in-migration and creates signals for migration surges that may drive
industrial real estate demand.
"""

from __future__ import annotations

import uuid
from typing import Any, Optional

import httpx
from prefect import flow, task

from shared.config import CENSUS_API_KEY
from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "census_migration"

FLOWS_URL = "https://api.census.gov/data/2021/acs/flows"

TX_STATE_FIPS = "48"

SOURCE_URL = "https://www.census.gov/topics/population/migration.html"

# Target Texas counties: FIPS -> (name, submarket or None)
TARGET_COUNTIES: dict[str, tuple[str, Optional[str]]] = {
    "201": ("Harris County", "NW Houston Industrial"),
    "113": ("Dallas County", "DFW South Industrial"),
    "439": ("Tarrant County", "DFW South Industrial"),
    "453": ("Travis County", "Austin East"),
    "029": ("Bexar County", "San Antonio NE"),
    "085": ("Collin County", None),
    "121": ("Denton County", None),
    "491": ("Williamson County", None),
    "157": ("Fort Bend County", None),
    "339": ("Montgomery County", None),
}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch_migration_flows", retries=2, retry_delay_seconds=30)
def fetch_migration_flows() -> list[dict[str, Any]]:
    """Fetch county-level migration flows for all Texas counties from the
    Census ACS Flows API.

    Returns a list of dicts (one per row) with keys from the API header row.
    """
    params = {
        "get": "MOVEDIN,MOVEDOUT,MOVEDNET,FULL1_NAME,FULL2_NAME",
        "for": "county:*",
        "in": "state:48",
        "key": CENSUS_API_KEY,
    }

    resp = httpx.get(FLOWS_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if not data or len(data) < 2:
        print("No migration flow data returned from Census API")
        return []

    headers = data[0]
    rows: list[dict[str, Any]] = []
    for row_values in data[1:]:
        rows.append(dict(zip(headers, row_values)))

    return rows


@task(name="process_and_signal")
def process_and_signal(flows: list[dict[str, Any]]) -> tuple[int, int]:
    """Filter flows for target counties, aggregate net migration, upsert to
    census_data, and create signals.

    Returns (rows_stored, signals_created).
    """
    client = get_client()

    # Aggregate net migration per target county
    county_agg: dict[str, dict[str, Any]] = {}

    for row in flows:
        county_fips = (row.get("county") or "").strip()
        if county_fips not in TARGET_COUNTIES:
            continue

        try:
            moved_in = int(row.get("MOVEDIN") or 0)
            moved_out = int(row.get("MOVEDOUT") or 0)
            moved_net = int(row.get("MOVEDNET") or 0)
        except (ValueError, TypeError):
            continue

        if county_fips not in county_agg:
            county_name, submarket = TARGET_COUNTIES[county_fips]
            county_agg[county_fips] = {
                "county_name": county_name,
                "submarket": submarket,
                "moved_in": 0,
                "moved_out": 0,
                "moved_net": 0,
                "full_name": row.get("FULL2_NAME", county_name),
            }

        agg = county_agg[county_fips]
        agg["moved_in"] += moved_in
        agg["moved_out"] += moved_out
        agg["moved_net"] += moved_net

    if not county_agg:
        print("No matching county data found in flows")
        return 0, 0

    # Upsert and signal
    upsert_rows: list[dict[str, Any]] = []
    signal_count = 0

    for fips, agg in county_agg.items():
        county_name = agg["county_name"]
        submarket = agg["submarket"]
        net = agg["moved_net"]

        upsert_rows.append(
            {
                "id": str(uuid.uuid4()),
                "geography_type": "county",
                "geography_id": f"{TX_STATE_FIPS}{fips}",
                "metric": "net_migration",
                "year": 2021,
                "value": net,
                "previous_value": None,
                "change_pct": None,
                "source": "census_acs_flows",
                "raw_data": {"source_url": SOURCE_URL},
            }
        )

        # Determine signal severity based on net migration magnitude
        severity: str | None = None
        if net > 5000:
            severity = "alert"
        elif net > 2000:
            severity = "watch"
        elif net > 500:
            severity = "info"

        if severity is not None:
            create_signal(
                agent_name=AGENT_NAME,
                signal_type="migration_surge",
                severity=severity,
                title=f"{county_name}: net migration +{net:,}",
                summary=(
                    f"Census ACS Flows (2021) show {county_name} (FIPS {TX_STATE_FIPS}{fips}) "
                    f"had net migration of {net:,} people. "
                    f"Moved in: {agg['moved_in']:,}, moved out: {agg['moved_out']:,}. "
                    f"Strong population inflow may increase industrial space demand."
                ),
                submarket=submarket,
                data={
                    "county_fips": f"{TX_STATE_FIPS}{fips}",
                    "county_name": county_name,
                    "moved_in": agg["moved_in"],
                    "moved_out": agg["moved_out"],
                    "moved_net": net,
                    "year": 2021,
                },
                source_url=SOURCE_URL,
            )
            signal_count += 1

    # Upsert to census_data
    if upsert_rows:
        client.table("census_data").upsert(
            upsert_rows,
            on_conflict="geography_type,geography_id,metric,year",
        ).execute()

    return len(upsert_rows), signal_count


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="census_migration_agent", log_prints=True)
def census_migration_agent() -> dict[str, Any]:
    """Fetch Census ACS migration flows, process target counties, and signal."""
    if not CENSUS_API_KEY:
        print("WARNING: CENSUS_API_KEY is not set. Skipping census migration agent.")
        return {"status": "skipped", "reason": "no_api_key"}

    total_pulled = 0
    total_new = 0
    total_signals = 0

    try:
        print("Fetching Census ACS migration flows for Texas counties …")
        flows = fetch_migration_flows()
        total_pulled = len(flows)
        print(f"Retrieved {total_pulled} flow records")

        if flows:
            rows_stored, signals = process_and_signal(flows)
            total_new = rows_stored
            total_signals = signals
            print(f"Stored {rows_stored} county records, created {signals} signals")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_pulled,
            records_new=total_new,
        )
        print(
            f"\nAgent complete: {total_pulled} flows pulled, "
            f"{total_new} stored, {total_signals} signals"
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
    census_migration_agent()
