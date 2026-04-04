"""
Flood Risk Agent
-----------------
Checks properties / watch locations against FEMA National Flood Hazard Layer
(NFHL) to identify assets in 100-year and 500-year flood zones.  Creates
alert or watch signals accordingly.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import httpx
from prefect import flow, task

from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "flood_risk"

FEMA_URL = (
    "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query"
)

# 100-year Special Flood Hazard Areas (SFHA)
HIGH_RISK_ZONES: set[str] = {"A", "AE", "AH", "AO", "AR", "A99", "V", "VE"}

# 500-year (0.2% annual chance) — only when ZONE_SUBTY confirms it
MODERATE_RISK_ZONES: set[str] = {"X"}

WATCH_LOCATIONS: list[dict[str, Any]] = [
    {"name": "NW Houston Industrial", "lat": 29.85, "lng": -95.55, "submarket": "NW Houston Industrial"},
    {"name": "NE Houston Industrial", "lat": 29.85, "lng": -95.20, "submarket": "NE Houston Industrial"},
    {"name": "DFW South Industrial", "lat": 32.65, "lng": -97.10, "submarket": "DFW South Industrial"},
    {"name": "Austin East Industrial", "lat": 30.25, "lng": -97.60, "submarket": "Austin East"},
    {"name": "San Antonio NE Industrial", "lat": 29.50, "lng": -98.35, "submarket": "San Antonio NE"},
    {"name": "Laredo Port Area", "lat": 27.50, "lng": -99.50, "submarket": None},
    {"name": "El Paso Industrial", "lat": 31.76, "lng": -106.44, "submarket": None},
]


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="fetch_properties_with_location")
def fetch_properties_with_location() -> list[dict[str, Any]]:
    """Return the standard watch locations with lat/lng for flood-zone checks.

    MVP approach: use the same seven WATCH_LOCATIONS used by the TxDOT and
    other infrastructure agents rather than querying the properties table
    (which stores geometry as WKB hex and requires PostGIS conversion).
    """
    return [
        {
            "name": loc["name"],
            "lat": loc["lat"],
            "lng": loc["lng"],
            "submarket": loc["submarket"],
        }
        for loc in WATCH_LOCATIONS
    ]


@task(name="check_flood_zone", retries=2, retry_delay_seconds=10)
def check_flood_zone(lat: float, lng: float, location_name: str) -> dict[str, Any] | None:
    """Query the FEMA NFHL ArcGIS endpoint for the flood zone at a point.

    Returns the first feature's attributes, or None on error / no data.
    """
    params = {
        "geometry": f"{lng},{lat}",
        "geometryType": "esriGeometryPoint",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "FLD_ZONE,ZONE_SUBTY,SFHA_TF,STATIC_BFE,DEPTH,SOURCE_CIT",
        "f": "json",
        "returnGeometry": "false",
        "inSR": "4326",
    }

    try:
        resp = httpx.get(FEMA_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as exc:
        print(f"HTTP error querying FEMA near {location_name}: {exc}")
        return None
    except httpx.RequestError as exc:
        print(f"Request error querying FEMA near {location_name}: {exc}")
        return None
    except Exception as exc:
        print(f"Unexpected error querying FEMA near {location_name}: {exc}")
        return None

    if "error" in data:
        print(f"FEMA ArcGIS error near {location_name}: {data['error']}")
        return None

    features = data.get("features", [])
    if not features:
        print(f"  {location_name}: no flood zone data returned")
        return None

    attrs = features[0].get("attributes", {})
    print(f"  {location_name}: zone={attrs.get('FLD_ZONE')} subtype={attrs.get('ZONE_SUBTY')}")
    return attrs


@task(name="create_flood_signals")
def create_flood_signals(results: list[dict[str, Any]]) -> int:
    """Evaluate flood-zone results and create signals for at-risk locations.

    Returns the number of signals created.
    """
    signals_created = 0

    for r in results:
        attrs = r.get("attrs")
        if attrs is None:
            continue

        zone = attrs.get("FLD_ZONE", "")
        zone_subtype = attrs.get("ZONE_SUBTY") or ""
        sfha = attrs.get("SFHA_TF")
        bfe = attrs.get("STATIC_BFE")
        depth = attrs.get("DEPTH")
        source = attrs.get("SOURCE_CIT")

        location_name = r["name"]
        lat = r["lat"]
        lng = r["lng"]
        submarket = r.get("submarket")

        if zone in HIGH_RISK_ZONES:
            severity = "alert"
            title = f"100-year flood zone: {location_name}"
            summary = (
                f"{location_name} falls within FEMA flood zone {zone} "
                f"(100-year SFHA). Zone subtype: {zone_subtype or 'N/A'}. "
                f"SFHA: {sfha}. Base flood elevation: {bfe or 'N/A'}. "
                f"Depth: {depth or 'N/A'}."
            )
        elif zone in MODERATE_RISK_ZONES and "0.2 PCT" in zone_subtype.upper():
            severity = "watch"
            title = f"500-year flood zone: {location_name}"
            summary = (
                f"{location_name} falls within FEMA flood zone {zone} "
                f"(500-year / 0.2% annual chance). Zone subtype: {zone_subtype}. "
                f"SFHA: {sfha}."
            )
        else:
            # Minimal risk — no signal needed
            continue

        location_wkt = f"POINT({lng} {lat})"

        create_signal(
            agent_name=AGENT_NAME,
            signal_type="flood_risk",
            severity=severity,
            title=title,
            summary=summary,
            location=location_wkt,
            submarket=submarket,
            source_url="https://msc.fema.gov/portal/home",
            data={
                "zone": zone,
                "zone_subtype": zone_subtype,
                "sfha": sfha,
                "bfe": bfe,
                "depth": depth,
                "source": source,
                "location_name": location_name,
            },
        )
        signals_created += 1

    return signals_created


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="flood_risk_agent", log_prints=True)
def flood_risk_agent() -> dict[str, Any]:
    """Orchestrate: fetch locations, check flood zones, create signals, log."""
    total_checked = 0
    total_signals = 0

    try:
        locations = fetch_properties_with_location()
        print(f"Checking {len(locations)} locations against FEMA flood zones")

        results: list[dict[str, Any]] = []

        for loc in locations:
            attrs = check_flood_zone(
                lat=loc["lat"],
                lng=loc["lng"],
                location_name=loc["name"],
            )
            results.append({
                "name": loc["name"],
                "lat": loc["lat"],
                "lng": loc["lng"],
                "submarket": loc.get("submarket"),
                "attrs": attrs,
            })
            total_checked += 1

            # Rate-limit: 1 second between FEMA requests
            time.sleep(1)

        total_signals = create_flood_signals(results)
        print(
            f"Flood risk agent complete: {total_checked} locations checked, "
            f"{total_signals} signals created"
        )

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_checked,
            records_new=total_signals,
        )
        return {
            "status": "success",
            "locations_checked": total_checked,
            "signals_created": total_signals,
        }

    except Exception as exc:
        log_agent_run(
            agent_name=AGENT_NAME,
            status="failed",
            records_pulled=total_checked,
            records_new=total_signals,
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    flood_risk_agent()
