"""
IRS Migration Agent
-------------------
Downloads IRS SOI county-to-county migration data (inflow and outflow CSVs),
identifies Texas counties gaining high-income households, stores net migration
summaries, and creates signals for significant shifts.
"""

from __future__ import annotations

import csv
import io
import uuid
from typing import Any

import httpx
from prefect import flow, task

from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "irs_migration"

IRS_INFLOW_URL = "https://www.irs.gov/pub/irs-soi/countyinflow2122.csv"
IRS_OUTFLOW_URL = "https://www.irs.gov/pub/irs-soi/countyoutflow2122.csv"

TX_FIPS = "48"

TARGET_TX_COUNTIES: dict[str, str] = {
    "201": "Harris",
    "113": "Dallas",
    "439": "Tarrant",
    "453": "Travis",
    "029": "Bexar",
    "085": "Collin",
    "121": "Denton",
    "491": "Williamson",
    "157": "Fort Bend",
    "339": "Montgomery",
    "479": "Webb",
    "141": "El Paso",
}

SOURCE_URL = "https://www.irs.gov/statistics/soi-tax-stats-migration-data"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="download_irs_csv", retries=2, retry_delay_seconds=30)
def download_irs_csv(url: str) -> str:
    """Download a large IRS SOI CSV file and return its text content."""
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
    return resp.text


@task(name="parse_and_filter_tx")
def parse_and_filter_tx(csv_text: str, direction: str) -> list[dict[str, Any]]:
    """Parse the IRS CSV and filter rows for target Texas counties.

    Parameters
    ----------
    csv_text : str
        Raw CSV content from the IRS download.
    direction : str
        ``"inflow"`` — people moving INTO Texas (y2_statefips == TX_FIPS).
        ``"outflow"`` — people leaving Texas (y1_statefips == TX_FIPS).

    Returns
    -------
    list[dict]
        Parsed rows with numeric values for n1, n2, and AGI.
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    results: list[dict[str, Any]] = []

    for row in reader:
        # Determine whether this row involves Texas in the correct direction
        if direction == "inflow":
            state_fips = (row.get("y2_statefips") or "").strip()
            county_fips = (row.get("y2_countyfips") or "").strip()
        else:  # outflow
            state_fips = (row.get("y1_statefips") or "").strip()
            county_fips = (row.get("y1_countyfips") or "").strip()

        if state_fips != TX_FIPS:
            continue

        if county_fips not in TARGET_TX_COUNTIES:
            continue

        # Skip suppressed / non-numeric rows
        n1_raw = (row.get("n1") or "").strip()
        n2_raw = (row.get("n2") or "").strip()
        agi_raw = (row.get("AGI") or row.get("agi") or "").strip()

        if n1_raw == "d" or n2_raw == "d" or agi_raw == "d":
            continue

        try:
            n1 = int(n1_raw)
            n2 = int(n2_raw)
            agi = int(agi_raw)
        except (ValueError, TypeError):
            continue

        results.append(
            {
                "y1_statefips": (row.get("y1_statefips") or "").strip(),
                "y2_statefips": (row.get("y2_statefips") or "").strip(),
                "y1_countyfips": (row.get("y1_countyfips") or "").strip(),
                "y2_countyfips": (row.get("y2_countyfips") or "").strip(),
                "y1_state": (row.get("y1_state") or "").strip(),
                "y2_state": (row.get("y2_state") or "").strip(),
                "y1_countyname": (row.get("y1_countyname") or "").strip(),
                "y2_countyname": (row.get("y2_countyname") or "").strip(),
                "n1": n1,
                "n2": n2,
                "agi": agi,
                "direction": direction,
                "tx_county_fips": county_fips,
            }
        )

    return results


@task(name="compute_net_and_signal")
def compute_net_and_signal(
    inflow: list[dict[str, Any]],
    outflow: list[dict[str, Any]],
) -> int:
    """Aggregate inflow/outflow per county, upsert summaries, and create signals.

    Returns the number of signals created.
    """
    client = get_client()

    # Build per-county aggregates
    county_in: dict[str, dict[str, int]] = {}
    county_out: dict[str, dict[str, int]] = {}

    for row in inflow:
        fips = row["tx_county_fips"]
        agg = county_in.setdefault(fips, {"n1": 0, "n2": 0, "agi": 0})
        agg["n1"] += row["n1"]
        agg["n2"] += row["n2"]
        agg["agi"] += row["agi"]

    for row in outflow:
        fips = row["tx_county_fips"]
        agg = county_out.setdefault(fips, {"n1": 0, "n2": 0, "agi": 0})
        agg["n1"] += row["n1"]
        agg["n2"] += row["n2"]
        agg["agi"] += row["agi"]

    signal_count = 0
    upsert_rows: list[dict[str, Any]] = []

    for fips, county_name in TARGET_TX_COUNTIES.items():
        inf = county_in.get(fips, {"n1": 0, "n2": 0, "agi": 0})
        out = county_out.get(fips, {"n1": 0, "n2": 0, "agi": 0})

        net_returns = inf["n1"] - out["n1"]
        net_exemptions = inf["n2"] - out["n2"]
        net_agi = inf["agi"] - out["agi"]

        upsert_rows.append(
            {
                "id": str(uuid.uuid4()),
                "tax_year": 2022,
                "origin_state": "96",
                "origin_county": "000",
                "dest_state": TX_FIPS,
                "dest_county": fips,
                "returns": net_returns,
                "exemptions": net_exemptions,
                "agi_thousands": net_agi,
                "flow_direction": "net_inflow",
                "source": "irs_soi",
                "raw_data": {
                    "county_name": county_name,
                    "inflow_returns": inf["n1"],
                    "outflow_returns": out["n1"],
                    "inflow_agi": inf["agi"],
                    "outflow_agi": out["agi"],
                },
            }
        )

        # Determine signal severity
        severity: str | None = None
        if net_agi > 500_000:
            severity = "alert"
        elif net_agi > 200_000:
            severity = "watch"
        elif net_returns > 2_000:
            severity = "info"

        if severity is not None:
            net_agi_millions = net_agi / 1_000
            create_signal(
                agent_name=AGENT_NAME,
                signal_type="irs_migration_shift",
                severity=severity,
                title=f"High-income migration into {county_name}: +${net_agi_millions:,.0f}M AGI",
                summary=(
                    f"IRS SOI 2021-2022 migration data shows {county_name} County (FIPS {fips}) "
                    f"gained a net {net_returns:,} tax returns and ${net_agi_millions:,.0f}M in "
                    f"adjusted gross income. Inflow: {inf['n1']:,} returns / "
                    f"${inf['agi']/1_000:,.0f}M AGI. Outflow: {out['n1']:,} returns / "
                    f"${out['agi']/1_000:,.0f}M AGI."
                ),
                submarket=f"{county_name} County, TX",
                data={
                    "county_fips": fips,
                    "county_name": county_name,
                    "net_returns": net_returns,
                    "net_exemptions": net_exemptions,
                    "net_agi_thousands": net_agi,
                    "inflow_returns": inf["n1"],
                    "outflow_returns": out["n1"],
                    "inflow_agi_thousands": inf["agi"],
                    "outflow_agi_thousands": out["agi"],
                },
                source_url=SOURCE_URL,
            )
            signal_count += 1

    # Upsert aggregate rows
    if upsert_rows:
        client.table("irs_migration").upsert(
            upsert_rows,
            on_conflict="tax_year,origin_state,origin_county,dest_state,dest_county",
        ).execute()

    return signal_count


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="irs_migration_agent", log_prints=True)
def irs_migration_agent() -> dict[str, Any]:
    """Orchestrate: download IRS CSVs, parse, compute net flows, signal, log."""
    total_pulled = 0
    total_new = 0

    try:
        # Download both CSVs
        print("Downloading IRS inflow CSV …")
        inflow_csv = download_irs_csv(IRS_INFLOW_URL)
        print("Downloading IRS outflow CSV …")
        outflow_csv = download_irs_csv(IRS_OUTFLOW_URL)

        # Parse and filter for Texas target counties
        inflow_rows = parse_and_filter_tx(inflow_csv, "inflow")
        outflow_rows = parse_and_filter_tx(outflow_csv, "outflow")
        total_pulled = len(inflow_rows) + len(outflow_rows)
        print(
            f"Parsed {len(inflow_rows)} inflow rows and "
            f"{len(outflow_rows)} outflow rows for target TX counties"
        )

        # Compute net migration and create signals
        signal_count = compute_net_and_signal(inflow_rows, outflow_rows)
        total_new = signal_count
        print(f"Created {signal_count} signals for significant migration shifts")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_pulled,
            records_new=total_new,
        )
        return {"status": "success", "pulled": total_pulled, "signals": total_new}

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
    irs_migration_agent()
