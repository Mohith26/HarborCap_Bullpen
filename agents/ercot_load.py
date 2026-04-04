"""
ERCOT Load Agent
----------------
Fetches actual system load by weather zone from the ERCOT public API,
computes daily averages and year-over-year changes, upserts to the
ercot_load table, and creates signals when significant grid load shifts
are detected — useful for gauging industrial CRE demand across Texas.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Any

import httpx
from prefect import flow, task

from shared.config import ERCOT_SUBSCRIPTION_KEY, ERCOT_USERNAME, ERCOT_PASSWORD
from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "ercot_load"

TOKEN_URL = (
    "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/"
    "B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"
)
LOAD_URL = "https://api.ercot.com/api/public-reports/np6-345-cd/act_sys_load_by_wzn"

ERCOT_PUBLIC_CLIENT_ID = "fec253ea-0d06-4272-a5e6-b478baeecd70"

# Column indices in the ERCOT data array
# [operatingDay, hourEnding, coast, east, farWest, north, northCentral, southCentral, southern, west, systemTotal, dstFlag]
ZONE_COLUMNS: dict[str, int] = {
    "COAST": 2,       # Houston
    "EAST": 3,        # East TX
    "FARWEST": 4,     # Far West TX
    "NORTH": 5,       # DFW
    "NCENT": 6,       # North Central
    "SCENT": 7,       # Austin/San Antonio
    "SOUTH": 8,       # South TX/Laredo
    "WEST": 9,        # West TX/El Paso
}

ZONE_LABELS: dict[str, str] = {
    "COAST": "Houston",
    "NORTH": "DFW",
    "NCENT": "North Central TX",
    "SCENT": "Austin/San Antonio",
    "SOUTH": "South TX/Laredo",
    "WEST": "West TX/El Paso",
    "EAST": "East TX",
    "FARWEST": "Far West TX",
}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="get_ercot_token", retries=2, retry_delay_seconds=10)
def get_ercot_token() -> str:
    """Authenticate with ERCOT B2C via ROPC flow and return an access token."""
    payload = {
        "grant_type": "password",
        "client_id": ERCOT_PUBLIC_CLIENT_ID,
        "username": ERCOT_USERNAME,
        "password": ERCOT_PASSWORD,
        "scope": f"{ERCOT_PUBLIC_CLIENT_ID} openid",
        "response_type": "token",
    }
    resp = httpx.post(TOKEN_URL, data=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


@task(name="fetch_load_data", retries=2, retry_delay_seconds=10)
def fetch_load_data(
    token: str,
    date_from: str,
    date_to: str,
) -> list[list]:
    """Fetch actual system load for all weather zones from ERCOT API."""
    headers = {
        "Ocp-Apim-Subscription-Key": ERCOT_SUBSCRIPTION_KEY,
        "Authorization": f"Bearer {token}",
    }

    all_data: list[list] = []
    page = 1

    while True:
        params = {
            "operatingDayFrom": date_from,
            "operatingDayTo": date_to,
            "size": "1000",
            "page": str(page),
        }
        resp = httpx.get(LOAD_URL, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        body = resp.json()

        records = body.get("data", [])
        if not records:
            break

        all_data.extend(records)

        meta = body.get("_meta", {})
        if page >= meta.get("totalPages", 1):
            break
        page += 1

    return all_data


@task(name="compute_and_store_ercot")
def compute_and_store(data: list[list]) -> tuple[int, int]:
    """Aggregate hourly data into daily averages per zone, upsert, and return (rows, signals)."""
    client = get_client()

    # Aggregate: {(date, zone_name) -> list of MW values}
    daily: dict[tuple[str, str], list[float]] = {}
    for row in data:
        if not row or len(row) < 11:
            continue
        op_day = str(row[0])[:10]
        for zone_name, col_idx in ZONE_COLUMNS.items():
            val = row[col_idx]
            if val is not None:
                daily.setdefault((op_day, zone_name), []).append(float(val))

    rows_to_upsert: list[dict[str, Any]] = []
    for (op_day, zone_name), values in sorted(daily.items()):
        avg_mw = round(sum(values) / len(values), 2)
        rows_to_upsert.append({
            "id": str(uuid.uuid4()),
            "weather_zone": zone_name,
            "interval_start": op_day,
            "load_mw": avg_mw,
            "load_type": "actual",
            "source": "ercot_api",
            "raw_data": {"hours_sampled": len(values)},
        })

    if not rows_to_upsert:
        return 0, 0

    # Batch upsert in chunks of 100
    written = 0
    for i in range(0, len(rows_to_upsert), 100):
        batch = rows_to_upsert[i:i + 100]
        result = (
            client.table("ercot_load")
            .upsert(batch, on_conflict="weather_zone,interval_start,load_type")
            .execute()
        )
        written += len(result.data) if result.data else 0

    # --- YoY comparison per zone ---
    signal_count = 0
    zone_avgs: dict[str, float] = {}
    for (_, zone_name), values in daily.items():
        zone_avgs.setdefault(zone_name, [])
        zone_avgs[zone_name].append(sum(values) / len(values))

    # Average across all days for each zone
    zone_current: dict[str, float] = {
        z: sum(vals) / len(vals) for z, vals in zone_avgs.items() if vals
    }

    # Query last year's data for comparison
    dates = sorted(set(r["interval_start"] for r in rows_to_upsert))
    if dates:
        ly_start = _shift_year(dates[0], -1)
        ly_end = _shift_year(dates[-1], -1)

        for zone_name, current_avg in zone_current.items():
            try:
                ly_result = (
                    client.table("ercot_load")
                    .select("load_mw")
                    .eq("weather_zone", zone_name)
                    .gte("interval_start", ly_start)
                    .lte("interval_start", ly_end)
                    .execute()
                )
                ly_rows = ly_result.data or []
            except Exception:
                ly_rows = []

            if ly_rows:
                ly_avg = sum(float(r["load_mw"]) for r in ly_rows) / len(ly_rows)
                if ly_avg > 0:
                    yoy_pct = round((current_avg - ly_avg) / ly_avg * 100, 2)
                    created = _create_load_signal(zone_name, yoy_pct, current_avg, ly_avg, dates[-1])
                    if created:
                        signal_count += 1

    return written, signal_count


def _shift_year(date_str: str, years: int) -> str:
    d = date.fromisoformat(date_str)
    try:
        return d.replace(year=d.year + years).isoformat()
    except ValueError:
        return d.replace(year=d.year + years, day=28).isoformat()


def _create_load_signal(
    zone_name: str, yoy_pct: float, current_avg: float, ly_avg: float, as_of: str,
) -> bool:
    abs_yoy = abs(yoy_pct)
    direction = "up" if yoy_pct > 0 else "down"

    if abs_yoy > 8:
        severity = "alert"
    elif abs_yoy > 4:
        severity = "watch"
    elif abs_yoy > 2:
        severity = "info"
    else:
        return False

    label = ZONE_LABELS.get(zone_name, zone_name)
    create_signal(
        agent_name=AGENT_NAME,
        signal_type="grid_load_shift",
        severity=severity,
        title=f"ERCOT {label} load {direction} {abs_yoy}% YoY",
        summary=(
            f"Average grid load in {label} ({zone_name}) "
            f"moved {direction} {abs_yoy}% year-over-year as of {as_of}. "
            f"Current avg: {current_avg:,.0f} MW vs last year: {ly_avg:,.0f} MW. "
            f"{'Rising load signals growing industrial/commercial activity.' if yoy_pct > 0 else 'Declining load may indicate softening demand.'}"
        ),
        data={
            "weather_zone": zone_name,
            "yoy_change_pct": yoy_pct,
            "current_avg_mw": current_avg,
            "prior_year_avg_mw": ly_avg,
        },
        source_url="https://www.ercot.com/gridmktinfo/load/load_hist",
    )
    return True


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="ercot_load_agent", log_prints=True)
def ercot_load_agent() -> dict[str, Any]:
    """Orchestrate: authenticate, fetch load data, compute per-zone trends, signal."""
    if not ERCOT_SUBSCRIPTION_KEY or not ERCOT_USERNAME or not ERCOT_PASSWORD:
        print(
            "ERCOT credentials not configured. "
            "Set ERCOT_SUBSCRIPTION_KEY, ERCOT_USERNAME, ERCOT_PASSWORD in .env"
        )
        return {"status": "skipped", "reason": "credentials_not_configured"}

    total_pulled = 0
    total_new = 0
    total_signals = 0

    try:
        token = get_ercot_token()
        print("Authenticated with ERCOT successfully")

        today = date.today()
        date_from = (today - timedelta(days=30)).isoformat()
        date_to = today.isoformat()

        print(f"Fetching load data from {date_from} to {date_to}...")
        data = fetch_load_data(token, date_from, date_to)
        total_pulled = len(data)
        print(f"Retrieved {total_pulled} hourly records across all zones")

        written, signals = compute_and_store(data)
        total_new = written
        total_signals = signals
        print(f"Stored {written} daily zone averages, created {signals} signals")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_pulled,
            records_new=total_new,
        )
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
    ercot_load_agent()
