"""
FRED Freight Volume Agent
--------------------------
Pulls freight transportation indices from the Federal Reserve (FRED),
computes month-over-month and year-over-year changes, upserts to the
freight_data table, and creates alert signals when significant shifts
are detected — useful for gauging industrial CRE demand.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Any

import httpx
from prefect import flow, task

from shared.config import FRED_API_KEY, SEED_MODE
from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "freight_volume"

FREIGHT_SERIES: dict[str, str] = {
    "TSIFRGHT": "Freight Transportation Services Index",
    "RAILFRTCARLOADSD11": "Rail Freight Carloads",
    "PCU484484": "Trucking PPI - General Freight",
    "CUUR0000SETG01": "CPI Delivery Services",
}

# ---------------------------------------------------------------------------
# Table bootstrap
# ---------------------------------------------------------------------------

def _ensure_freight_table() -> None:
    """Create the freight_data table if it does not already exist."""
    client = get_client()
    # Use Supabase rpc to run raw SQL via a Postgres function.
    # If the project exposes the `exec_sql` rpc, use it; otherwise the table
    # is assumed to exist (created via migration).
    try:
        client.rpc(
            "exec_sql",
            {
                "query": """
                    CREATE TABLE IF NOT EXISTS freight_data (
                        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        metric      TEXT        NOT NULL,
                        period      DATE        NOT NULL,
                        value       NUMERIC,
                        yoy_change  NUMERIC,
                        source      TEXT        DEFAULT 'fred',
                        raw_data    JSONB,
                        created_at  TIMESTAMPTZ DEFAULT now(),
                        UNIQUE (metric, period)
                    );
                """
            },
        ).execute()
    except Exception:
        # Table likely already exists or exec_sql RPC is not available.
        pass


# ---------------------------------------------------------------------------
# Seed-data generators
# ---------------------------------------------------------------------------

def _generate_seed_observations(series_id: str) -> list[dict[str, Any]]:
    """Return 24 months of realistic seed observations."""
    today = date.today().replace(day=1)
    months: list[date] = [
        (today - timedelta(days=30 * i)).replace(day=1) for i in range(24)
    ]
    months.reverse()

    base_values: dict[str, float] = {
        "TSIFRGHT": 135.0,
        "RAILFRTCARLOADSD11": 108.0,
        "LOADRATIO": 6.5,
        "TRUCKING": 120.0,
    }

    # Monthly growth rates chosen so the 12-month total lands near a target
    # YoY %. TRUCKING is set high enough to trigger the alert pathway.
    monthly_rates: dict[str, float] = {
        "TSIFRGHT": 0.0020,            # ~2.4 % YoY
        "RAILFRTCARLOADSD11": -0.0050,  # ~-5.8 % YoY — triggers watch
        "LOADRATIO": 0.0010,            # ~1.2 % YoY — stable
        "TRUCKING": 0.0090,             # ~11.4 % YoY — triggers alert
    }

    base = base_values.get(series_id, 100.0)
    rate = monthly_rates.get(series_id, 0.003)

    observations: list[dict[str, Any]] = []
    value = base
    for m in months:
        value = round(value * (1 + rate), 2)
        observations.append(
            {
                "date": m.isoformat(),
                "value": str(value),
            }
        )

    return observations


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch_fred_series", retries=2, retry_delay_seconds=10)
def fetch_fred_series(series_id: str) -> list[dict[str, Any]]:
    """Fetch the last 24 monthly observations for *series_id* from FRED.

    In SEED_MODE the data is generated locally so no API key is needed.
    """
    if SEED_MODE:
        return _generate_seed_observations(series_id)

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 24,
        "frequency": "m",
    }
    response = httpx.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("observations", [])


@task(name="compute_and_store")
def compute_and_store(
    series_id: str,
    series_name: str,
    observations: list[dict[str, Any]],
) -> int:
    """Calculate MoM / YoY changes, upsert rows, and fire signals.

    Returns the number of rows written.
    """
    client = get_client()

    # Sort ascending by date.
    obs_sorted = sorted(observations, key=lambda o: o["date"])

    rows_to_upsert: list[dict[str, Any]] = []
    for idx, obs in enumerate(obs_sorted):
        raw_val = obs.get("value", "")
        if raw_val in ("", "."):
            continue
        val = float(raw_val)
        obs_date = obs["date"]

        mom_change: float | None = None
        yoy_change: float | None = None

        # MoM -- compare with previous month
        if idx >= 1:
            prev_val_raw = obs_sorted[idx - 1].get("value", "")
            if prev_val_raw not in ("", "."):
                prev_val = float(prev_val_raw)
                if prev_val != 0:
                    mom_change = round((val - prev_val) / prev_val * 100, 2)

        # YoY -- compare with 12 months ago
        if idx >= 12:
            yoy_val_raw = obs_sorted[idx - 12].get("value", "")
            if yoy_val_raw not in ("", "."):
                yoy_val = float(yoy_val_raw)
                if yoy_val != 0:
                    yoy_change = round((val - yoy_val) / yoy_val * 100, 2)

        row = {
            "id": str(uuid.uuid4()),
            "metric": series_id,
            "period": obs_date,
            "value": val,
            "yoy_change": yoy_change,
            "source": "fred",
            "raw_data": {
                "series_name": series_name,
                "mom_change": mom_change,
                "observation": obs,
            },
        }
        rows_to_upsert.append(row)

    if not rows_to_upsert:
        return 0

    # Upsert -- on conflict(metric, period) update values.
    result = (
        client.table("freight_data")
        .upsert(rows_to_upsert, on_conflict="metric,period")
        .execute()
    )
    new_count = len(result.data) if result.data else 0

    # ----- Signal generation for the latest observation -----
    latest = rows_to_upsert[-1]
    yoy = latest.get("yoy_change")

    if yoy is not None and abs(yoy) > 5:
        direction = "up" if yoy > 0 else "down"
        severity = "alert" if abs(yoy) > 10 else "watch"

        create_signal(
            agent_name=AGENT_NAME,
            signal_type="freight_shift",
            severity=severity,
            title=f"{series_name} {direction} {abs(yoy)}% YoY",
            summary=(
                f"{series_name} ({series_id}) moved {direction} {abs(yoy)}% "
                f"year-over-year as of {latest['period']}. "
                f"{'Accelerating freight volumes generally signal stronger ' if yoy > 0 else 'Declining freight activity may foreshadow weaker '}"
                f"industrial and logistics CRE demand in the near term."
            ),
            data={
                "series_id": series_id,
                "period": latest["period"],
                "value": float(latest["value"]),
                "yoy_change": float(yoy),
                "mom_change": latest["raw_data"].get("mom_change"),
            },
            source_url=f"https://fred.stlouisfed.org/series/{series_id}",
        )

    return new_count


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="freight_volume_agent", log_prints=True)
def freight_volume_agent() -> dict[str, Any]:
    """Orchestrate: ensure table, fetch every FRED series, compute, store, log."""
    _ensure_freight_table()

    total_pulled = 0
    total_new = 0

    errors: list[str] = []

    for series_id, series_name in FREIGHT_SERIES.items():
        try:
            print(f"Processing {series_name} ({series_id})...")
            observations = fetch_fred_series(series_id)
            total_pulled += len(observations)

            new_count = compute_and_store(series_id, series_name, observations)
            total_new += new_count
            print(f"  -> {new_count} rows upserted")
        except Exception as exc:
            print(f"  -> FAILED: {exc}")
            errors.append(f"{series_id}: {exc}")

    status = "success" if not errors else ("success" if total_new > 0 else "failed")
    log_agent_run(
        agent_name=AGENT_NAME,
        status=status,
        records_pulled=total_pulled,
        records_new=total_new,
        error_message="; ".join(errors) if errors else None,
    )
    print(f"Agent complete: {total_pulled} pulled, {total_new} new/updated, {len(errors)} errors")
    return {"status": status, "pulled": total_pulled, "new": total_new, "errors": errors}


if __name__ == "__main__":
    freight_volume_agent()
