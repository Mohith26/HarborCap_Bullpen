"""
FRED Materials Price Agent
--------------------------
Pulls construction-material price indices from the Federal Reserve (FRED),
computes month-over-month and year-over-year changes, and creates alert
signals when thresholds are breached.
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

AGENT_NAME = "materials_price"

FRED_SERIES: dict[str, str] = {
    "WPU1017": "Steel Mill Products",
    "PCU327320327320": "Ready-Mix Concrete",
    "PCU327320327320C": "Ready-Mix Concrete South",
    "PCU33231233231212": "Structural Steel Commercial",
    "WPUSI012011": "Construction Materials Composite",
    "WPU10740510": "Structural Iron/Steel Industrial",
}

# ---------------------------------------------------------------------------
# Seed-data generators
# ---------------------------------------------------------------------------

def _generate_seed_observations(series_id: str) -> list[dict[str, Any]]:
    """Return 12 months of realistic seed observations.

    Steel and structural-iron series trend upward ~8-18 % YoY.
    Concrete stays roughly flat. One series (WPU10740510) contains an
    18 % YoY spike to exercise the alert pathway.
    """
    today = date.today().replace(day=1)
    months: list[date] = [
        (today - timedelta(days=30 * i)).replace(day=1) for i in range(12)
    ]
    months.reverse()

    base_values: dict[str, float] = {
        "WPU1017": 280.0,
        "PCU327320327320": 155.0,
        "PCU327320327320C": 158.0,
        "PCU33231233231212": 195.0,
        "WPUSI012011": 240.0,
        "WPU10740510": 310.0,
    }

    # Monthly growth rates (compounding) chosen so that the 12-month total
    # lands near the target YoY percentage.
    monthly_rates: dict[str, float] = {
        "WPU1017": 0.0065,             # ~8 % YoY
        "PCU327320327320": 0.0015,     # ~1.8 % YoY — stable
        "PCU327320327320C": 0.0018,    # ~2.2 % YoY
        "PCU33231233231212": 0.0070,   # ~8.7 % YoY
        "WPUSI012011": 0.0050,         # ~6.2 % YoY
        "WPU10740510": 0.0140,         # ~18 % YoY — triggers alert
    }

    base = base_values.get(series_id, 200.0)
    rate = monthly_rates.get(series_id, 0.005)

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
    """Fetch the last 12 monthly observations for *series_id*.

    In SEED_MODE the data is generated locally; otherwise the FRED API is
    called.
    """
    if SEED_MODE:
        return _generate_seed_observations(series_id)

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 13,  # 13 so we can compute YoY for the latest 12
        "frequency": "m",
    }
    response = httpx.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("observations", [])


@task(name="compute_changes_and_store")
def compute_changes_and_store(series_id: str, series_name: str, observations: list[dict[str, Any]]) -> int:
    """Calculate MoM / YoY changes, upsert rows, and fire signals.

    Returns the number of new rows written.
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

        # MoM — compare with previous month
        if idx >= 1:
            prev_val_raw = obs_sorted[idx - 1].get("value", "")
            if prev_val_raw not in ("", "."):
                prev_val = float(prev_val_raw)
                if prev_val != 0:
                    mom_change = round((val - prev_val) / prev_val * 100, 2)

        # YoY — compare with 12 months ago
        if idx >= 12:
            yoy_val_raw = obs_sorted[idx - 12].get("value", "")
            if yoy_val_raw not in ("", "."):
                yoy_val = float(yoy_val_raw)
                if yoy_val != 0:
                    yoy_change = round((val - yoy_val) / yoy_val * 100, 2)

        row = {
            "id": str(uuid.uuid4()),
            "series_id": series_id,
            "series_name": series_name,
            "observation_date": obs_date,
            "value": val,
            "unit": "index",
            "yoy_change": yoy_change,
            "mom_change": mom_change,
        }
        rows_to_upsert.append(row)

    if not rows_to_upsert:
        return 0

    # Upsert — on conflict(series_id, observation_date) update values.
    result = (
        client.table("material_prices")
        .upsert(rows_to_upsert, on_conflict="series_id,observation_date")
        .execute()
    )
    new_count = len(result.data) if result.data else 0

    # Fire alert signals for the latest observation if thresholds exceeded.
    latest = rows_to_upsert[-1]
    yoy = latest.get("yoy_change")
    mom = latest.get("mom_change")

    if yoy is not None and yoy > 15:
        create_signal(
            agent_name=AGENT_NAME,
            signal_type="material_price_spike",
            severity="alert",
            title=f"{series_name} YoY up {yoy}%",
            summary=(
                f"{series_name} ({series_id}) rose {yoy}% year-over-year "
                f"as of {latest['observation_date']}. Current index value: {latest['value']}."
            ),
            data={
                "series_id": series_id,
                "observation_date": latest["observation_date"],
                "value": float(latest["value"]),
                "yoy_change": float(yoy),
                "mom_change": float(mom) if mom is not None else None,
            },
            source_url=f"https://fred.stlouisfed.org/series/{series_id}",
        )

    if mom is not None and mom > 5:
        create_signal(
            agent_name=AGENT_NAME,
            signal_type="material_price_spike",
            severity="alert",
            title=f"{series_name} MoM up {mom}%",
            summary=(
                f"{series_name} ({series_id}) rose {mom}% month-over-month "
                f"as of {latest['observation_date']}. Current index value: {latest['value']}."
            ),
            data={
                "series_id": series_id,
                "observation_date": latest["observation_date"],
                "value": float(latest["value"]),
                "yoy_change": float(yoy) if yoy is not None else None,
                "mom_change": float(mom),
            },
            source_url=f"https://fred.stlouisfed.org/series/{series_id}",
        )

    return new_count


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="materials_price_agent", log_prints=True)
def materials_price_agent() -> dict[str, Any]:
    """Orchestrate: fetch every FRED series, compute changes, store, log."""
    total_pulled = 0
    total_new = 0

    try:
        for series_id, series_name in FRED_SERIES.items():
            print(f"Processing {series_name} ({series_id})...")
            observations = fetch_fred_series(series_id)
            total_pulled += len(observations)

            new_count = compute_changes_and_store(series_id, series_name, observations)
            total_new += new_count
            print(f"  -> {new_count} rows upserted")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_pulled,
            records_new=total_new,
        )
        print(f"Agent complete: {total_pulled} pulled, {total_new} new/updated")
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
    materials_price_agent()
