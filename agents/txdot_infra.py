"""
TxDOT Infrastructure Agent
---------------------------
Queries the TxDOT ArcGIS REST API for highway / infrastructure projects
near key Texas industrial areas.  Creates watch signals for projects
that could positively or negatively affect nearby industrial real estate.
"""

from __future__ import annotations

import re
from typing import Any

import httpx
from prefect import flow, task

from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "txdot_infra"

ARCGIS_URL = (
    "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/ArcGIS/rest/services/"
    "TxDOT_Projects/FeatureServer/0/query"
)

SEARCH_RADIUS_METERS = 8046.7  # 5 miles

WATCH_LOCATIONS: list[dict[str, Any]] = [
    {"name": "NW Houston Industrial", "lat": 29.85, "lng": -95.55, "submarket": "NW Houston Industrial"},
    {"name": "NE Houston Industrial", "lat": 29.85, "lng": -95.20, "submarket": "NE Houston Industrial"},
    {"name": "DFW South Industrial", "lat": 32.65, "lng": -97.10, "submarket": "DFW South Industrial"},
    {"name": "Austin East Industrial", "lat": 30.25, "lng": -97.60, "submarket": "Austin East"},
    {"name": "San Antonio NE Industrial", "lat": 29.50, "lng": -98.35, "submarket": "San Antonio NE"},
    {"name": "Laredo Port Area", "lat": 27.50, "lng": -99.50, "submarket": None},
    {"name": "El Paso Industrial", "lat": 31.76, "lng": -106.44, "submarket": None},
]

POSITIVE_PATTERNS = re.compile(
    r"widen|interchange|overpass|new\s+road|expansion|connector|ramp|add\s+lanes|new\s+location|reconstruct|upgrade|improve",
    re.IGNORECASE,
)
NEGATIVE_PATTERNS = re.compile(
    r"closure|demolition|construction\s+zone|detour|removal|remove",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_attr(attrs: dict, *keys: str, default: Any = None) -> Any:
    """Return the first non-None value found among *keys*, or *default*."""
    for k in keys:
        val = attrs.get(k)
        if val is not None:
            return val
    return default


def _classify_impact(description: str) -> str:
    """Return POSITIVE, NEGATIVE, or NEUTRAL based on project description."""
    if POSITIVE_PATTERNS.search(description):
        return "POSITIVE"
    if NEGATIVE_PATTERNS.search(description):
        return "NEGATIVE"
    return "NEUTRAL"


def _fmt_cost(cost: Any) -> str:
    """Format estimated cost for display, or return 'N/A'."""
    if cost is None:
        return "N/A"
    try:
        return f"${float(cost):,.0f}"
    except (ValueError, TypeError):
        return str(cost)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="query_projects_near_point", retries=2, retry_delay_seconds=10)
def query_projects_near_point(
    lat: float,
    lng: float,
    location_name: str,
) -> list[dict[str, Any]]:
    """Query TxDOT ArcGIS endpoint for projects within 5 miles of a point."""
    params = {
        "geometry": f"{lng},{lat}",
        "geometryType": "esriGeometryPoint",
        "spatialRel": "esriSpatialRelIntersects",
        "distance": SEARCH_RADIUS_METERS,
        "units": "esriSRUnit_Meter",
        "outFields": "*",
        "f": "json",
        "returnGeometry": "true",
        "inSR": "4326",
        "outSR": "4326",
    }

    try:
        resp = httpx.get(ARCGIS_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as exc:
        print(f"HTTP error querying TxDOT near {location_name}: {exc}")
        return []
    except httpx.RequestError as exc:
        print(f"Request error querying TxDOT near {location_name}: {exc}")
        return []
    except Exception as exc:
        print(f"Unexpected error querying TxDOT near {location_name}: {exc}")
        return []

    if "error" in data:
        print(f"ArcGIS error near {location_name}: {data['error']}")
        return []

    features = data.get("features", [])
    print(f"  {location_name}: {len(features)} projects found")
    return features


@task(name="classify_and_signal")
def classify_and_signal(
    features: list[dict[str, Any]],
    location_name: str,
    lat: float,
    lng: float,
    submarket: str | None,
) -> int:
    """Classify each project's impact and create signals for non-neutral ones.

    Returns the number of signals created.
    """
    signals_created = 0

    for feature in features:
        attrs = feature.get("attributes", {})

        project_id = _get_attr(attrs, "PROJECT_ID", "OBJECTID", "FID", default="unknown")
        # Build a rich description from multiple TxDOT fields
        proj_class = _get_attr(attrs, "PROJ_CLASS", default="")
        type_of_work = _get_attr(attrs, "TYPE_OF_WORK", default="")
        limits_from = _get_attr(attrs, "LIMITS_FROM", default="")
        limits_to = _get_attr(attrs, "LIMITS_TO", default="")
        limits = f"from {limits_from} to {limits_to}" if limits_from and limits_to else ""
        description = " - ".join(part for part in [proj_class, type_of_work, limits] if part) or "No description"

        status = _get_attr(
            attrs, "PT_PHASE", "PROJ_STAT", "PROJECT_STATUS", "STATUS",
            default="Unknown",
        )
        highway = _get_attr(
            attrs, "HIGHWAY_NUMBER", "STATE_HSE_NBR", "HWY", "HIGHWAY",
            default="N/A",
        )
        county = _get_attr(attrs, "COUNTY_NAME", "COUNTY", "CNTY_NM", default="N/A")
        let_date = _get_attr(attrs, "PRJ_ESMTD_LET_D", "ACTUAL_LET_DATE", "DIST_LET_DATE", default=None)
        est_cost = _get_attr(
            attrs, "EST_CONSTRUCTION_COST", "ESTIMATED_COST", "EST_COST",
            default=None,
        )

        impact = _classify_impact(str(description))
        if impact == "NEUTRAL":
            continue

        title = f"TxDOT: {str(description)[:60]} near {location_name}"
        summary_parts = [
            f"Project {project_id} on {highway} ({county} County).",
            f"Description: {description}.",
            f"Status/Phase: {status}.",
            f"Impact classification: {impact}.",
            f"Estimated cost: {_fmt_cost(est_cost)}.",
        ]
        if let_date:
            summary_parts.append(f"Let date: {let_date}.")

        location_wkt = f"POINT({lng} {lat})"

        # Link to TxDOT project tracker — CSJ (Control Section Job) is the lookup key
        csj = _get_attr(attrs, "CONTROL_SECT_JOB", "CSJ_TEXT", "CSJ", default=None)
        if csj:
            source_url = f"https://www.txdot.gov/projects/project-tracker.html?csj={csj}"
        else:
            source_url = f"https://www.txdot.gov/projects/project-tracker.html"

        create_signal(
            agent_name=AGENT_NAME,
            signal_type="txdot_project",
            severity="watch",
            title=title,
            summary=" ".join(summary_parts),
            location=location_wkt,
            submarket=submarket,
            source_url=source_url,
            data={
                "impact": impact,
                "project_id": project_id,
                "csj": csj,
                "description": description,
                "status": status,
                "highway": highway,
                "county": county,
                "let_date": let_date,
                "estimated_cost": est_cost,
                "location_name": location_name,
            },
        )
        signals_created += 1

    return signals_created


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="txdot_infra_agent", log_prints=True)
def txdot_infra_agent() -> dict[str, Any]:
    """Orchestrate: query each watch location, classify, signal, log."""
    total_projects = 0
    total_signals = 0

    try:
        for loc in WATCH_LOCATIONS:
            features = query_projects_near_point(
                lat=loc["lat"],
                lng=loc["lng"],
                location_name=loc["name"],
            )
            total_projects += len(features)

            if features:
                n_signals = classify_and_signal(
                    features=features,
                    location_name=loc["name"],
                    lat=loc["lat"],
                    lng=loc["lng"],
                    submarket=loc["submarket"],
                )
                total_signals += n_signals

        print(f"TxDOT agent complete: {total_projects} projects, {total_signals} signals")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_projects,
            records_new=total_signals,
        )
        return {
            "status": "success",
            "projects_found": total_projects,
            "signals_created": total_signals,
        }

    except Exception as exc:
        log_agent_run(
            agent_name=AGENT_NAME,
            status="failed",
            records_pulled=total_projects,
            records_new=total_signals,
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    txdot_infra_agent()
