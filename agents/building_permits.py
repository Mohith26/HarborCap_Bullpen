"""
Building Permits Agent
----------------------
Fetches building-permit data from the Austin Open Data Portal (Socrata),
filters for industrial / high-value permits, stores them, and creates
watch or alert signals.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Any

import httpx
from prefect import flow, task

from shared.config import SEED_MODE
from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "building_permits"

SOCRATA_URL = "https://data.austintexas.gov/resource/3syk-w9eu.json"

INDUSTRIAL_KEYWORDS: list[str] = [
    "warehouse",
    "industrial",
    "distribution",
    "logistics",
    "manufacturing",
    "cold storage",
    "flex space",
    "data center",
    "fulfillment",
    "storage facility",
    "tilt-up",
    "precast",
    "metal building",
    "loading dock",
]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch_city_permits", retries=2, retry_delay_seconds=10)
def fetch_city_permits() -> list[dict[str, Any]]:
    """Fetch recent commercial building permits from Austin Socrata API."""
    # Pull permits from last 90 days, commercial only
    since = (date.today() - timedelta(days=90)).isoformat()
    params = {
        "$where": f"issue_date > '{since}T00:00:00.000' AND permit_class_mapped = 'Commercial'",
        "$order": "issue_date DESC",
        "$limit": 1000,
    }
    resp = httpx.get(SOCRATA_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _normalize_permit(p: dict[str, Any]) -> dict[str, Any]:
    """Map Socrata API fields to our internal schema."""
    val = 0.0
    for vf in ("total_job_valuation", "building_valuation", "total_valuation_remodel"):
        if p.get(vf):
            val = max(val, float(p[vf]))

    sf = 0
    for sf_field in ("total_new_add_sqft", "total_existing_bldg_sqft", "remodel_repair_sqft"):
        if p.get(sf_field):
            sf = max(sf, int(float(p[sf_field])))

    link = p.get("link", {})
    source_url = link.get("url") if isinstance(link, dict) else None

    return {
        "permit_number": p.get("permit_number", ""),
        "permit_type": p.get("permit_type_desc", ""),
        "status": p.get("status_current", ""),
        "issue_date": p.get("issue_date", "")[:10] if p.get("issue_date") else None,
        "expiration_date": p.get("expiresdate", "")[:10] if p.get("expiresdate") else None,
        "address": p.get("original_address1") or p.get("permit_location", ""),
        "city": (p.get("original_city") or "Austin").title(),
        "latitude": p.get("latitude"),
        "longitude": p.get("longitude"),
        "description": p.get("description", ""),
        "sf": sf,
        "valuation": val,
        "contractor": p.get("contractor_company_name", ""),
        "owner_name": p.get("applicant_org") or p.get("applicant_full_name", ""),
        "source_url": source_url,
        "raw": p,
    }


@task(name="filter_industrial_permits")
def filter_industrial_permits(permits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep permits whose description matches industrial keywords OR valuation > $1M."""
    filtered: list[dict[str, Any]] = []
    for raw_p in permits:
        p = _normalize_permit(raw_p)
        desc = (p["description"]).lower()
        work_class = (raw_p.get("work_class") or "").lower()
        full_text = f"{desc} {work_class}"

        is_industrial = any(kw in full_text for kw in INDUSTRIAL_KEYWORDS)
        high_value = p["valuation"] > 1_000_000

        if is_industrial or high_value:
            filtered.append(p)

    return filtered


@task(name="store_and_signal_permits")
def store_and_signal_permits(permits: list[dict[str, Any]]) -> int:
    """Upsert permits into building_permits and create signals."""
    client = get_client()
    rows: list[dict[str, Any]] = []

    for p in permits:
        lat = p.get("latitude")
        lng = p.get("longitude")
        location = f"POINT({lng} {lat})" if lat and lng else None

        row: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "permit_number": p["permit_number"],
            "permit_type": p.get("permit_type"),
            "status": p.get("status"),
            "issue_date": p.get("issue_date"),
            "expiration_date": p.get("expiration_date"),
            "address": p.get("address"),
            "city": p.get("city", "Austin"),
            "description": p.get("description"),
            "sf": p.get("sf") or None,
            "valuation": p.get("valuation") or None,
            "contractor": p.get("contractor"),
            "owner_name": p.get("owner_name"),
            "source": "austin_socrata",
            "raw_data": p.get("raw", {}),
        }
        if location:
            row["location"] = location

        rows.append(row)

    if not rows:
        return 0

    result = (
        client.table("building_permits")
        .upsert(rows, on_conflict="permit_number,source")
        .execute()
    )
    written = len(result.data) if result.data else 0

    # Create signals for each permit
    for p in permits:
        val = p.get("valuation", 0)
        lat = p.get("latitude")
        lng = p.get("longitude")
        location_wkt = f"POINT({lng} {lat})" if lat and lng else None

        if val >= 5_000_000:
            severity = "alert"
            signal_title = f"Major permit: ${val/1_000_000:.1f}M — {p.get('description', 'N/A')[:60]}"
        elif val >= 1_000_000:
            severity = "watch"
            signal_title = f"Permit: ${val/1_000_000:.1f}M — {p.get('description', 'N/A')[:60]}"
        else:
            severity = "info"
            signal_title = f"Industrial permit: {p.get('description', 'N/A')[:70]}"

        create_signal(
            agent_name=AGENT_NAME,
            signal_type="building_permit",
            severity=severity,
            title=signal_title,
            summary=(
                f"Permit {p['permit_number']} at {p.get('address', 'N/A')}, "
                f"{p.get('city', 'Austin')}: {p.get('description', 'N/A')}. "
                f"Valuation ${val:,.0f}. SF: {p.get('sf', 'N/A')}. "
                f"Owner: {p.get('owner_name', 'N/A')}."
            ),
            location=location_wkt,
            submarket="Austin East",
            data={
                "permit_number": p["permit_number"],
                "valuation": val,
                "sf": p.get("sf"),
                "contractor": p.get("contractor"),
                "owner_name": p.get("owner_name"),
            },
            source_url=p.get("source_url"),
        )

    return written


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="building_permits_agent", log_prints=True)
def building_permits_agent() -> dict[str, Any]:
    """Orchestrate: fetch permits, filter, store, signal, log."""
    total_pulled = 0
    total_new = 0

    try:
        raw_permits = fetch_city_permits()
        total_pulled = len(raw_permits)
        print(f"Fetched {total_pulled} commercial permits from Austin")

        industrial = filter_industrial_permits(raw_permits)
        print(f"Filtered to {len(industrial)} industrial / high-value permits")

        total_new = store_and_signal_permits(industrial)
        print(f"Stored {total_new} permits, created signals")

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
    building_permits_agent()
