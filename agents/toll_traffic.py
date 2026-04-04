"""
Toll / Traffic Count Agent
---------------------------
Queries TxDOT traffic count data from the ArcGIS REST API near key Texas
industrial areas.  Creates signals for significant traffic trends that may
indicate growing demand around industrial real-estate submarkets.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

import httpx
from prefect import flow, task

from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "toll_traffic"

TRAFFIC_URL = (
    "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/ArcGIS/rest/services"
)

# Candidate service endpoints — tried in order until one succeeds.
_SERVICE_PATHS: list[str] = [
    "/TxDOT_AADT/FeatureServer/0/query",
    "/TxDOT_Traffic_Count_Stations/FeatureServer/0/query",
    # Fallback: the general projects layer filtered later for traffic attrs
    "/TxDOT_Projects/FeatureServer/0/query",
]

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

SOURCE_URL = "https://www.txdot.gov/data-maps/traffic-counts.html"


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


def _safe_float(val: Any) -> float | None:
    """Attempt to cast *val* to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="query_traffic_near_point", retries=2, retry_delay_seconds=10)
def query_traffic_near_point(
    lat: float,
    lng: float,
    location_name: str,
) -> list[dict[str, Any]]:
    """Query TxDOT ArcGIS endpoint for traffic counts within 5 miles of a point.

    Tries multiple service URLs in order and returns the first successful
    result.  Returns an empty list when all attempts fail.
    """
    params: dict[str, Any] = {
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

    for path in _SERVICE_PATHS:
        url = TRAFFIC_URL + path
        try:
            resp = httpx.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as exc:
            print(f"  HTTP {exc.response.status_code} from {path} near {location_name} — trying next URL")
            continue
        except httpx.RequestError as exc:
            print(f"  Request error from {path} near {location_name}: {exc} — trying next URL")
            continue
        except Exception as exc:
            print(f"  Unexpected error from {path} near {location_name}: {exc} — trying next URL")
            continue

        if "error" in data:
            print(f"  ArcGIS error from {path} near {location_name}: {data['error']} — trying next URL")
            continue

        features = data.get("features", [])
        print(f"  {location_name}: {len(features)} traffic records from {path}")
        return features

    print(f"  {location_name}: all service URLs failed — returning empty list")
    return []


@task(name="process_traffic_data")
def process_traffic_data(
    features: list[dict[str, Any]],
    location_name: str,
    lat: float,
    lng: float,
    submarket: str | None,
) -> int:
    """Process traffic features: upsert to DB and create signals for notable trends.

    Returns the number of signals created.
    """
    sb = get_client()
    signals_created = 0
    today_str = date.today().isoformat()

    for feature in features:
        attrs = feature.get("attributes", {})

        # --- Extract fields defensively (field names vary by service) ------
        station_id = str(
            _get_attr(attrs, "OBJECTID", "STATION_ID", "GIS_ID", "FID", default="unknown")
        )
        road_name = _get_attr(
            attrs, "ROAD_NAME", "RTE_NM", "HIGHWAY", "HWY_NAME",
            "HIGHWAY_NUMBER", "STATE_HSE_NBR", "HWY",
            default="N/A",
        )
        daily_total = _safe_float(
            _get_attr(attrs, "AADT", "AADT_CUR", "T_AADT", "DAILY_COUNT")
        )
        prev_year = _safe_float(
            _get_attr(attrs, "AADT_PREV", "PREV_AADT")
        )
        truck_pct = _safe_float(
            _get_attr(attrs, "TRUCK_PCT", "PCT_TRUCK", "T_PCT")
        )
        county = _get_attr(attrs, "COUNTY_NAME", "COUNTY", "CNTY_NM", default="N/A")

        # --- Compute year-over-year change ---------------------------------
        yoy_change: float | None = None
        if daily_total is not None and prev_year is not None and prev_year > 0:
            yoy_change = ((daily_total - prev_year) / prev_year) * 100.0

        # --- Upsert to traffic_counts table --------------------------------
        row: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "station_id": station_id,
            "station_name": county or location_name,
            "road_name": road_name,
            "authority": "TxDOT",
            "count_date": today_str,
            "daily_total": int(daily_total) if daily_total is not None else None,
            "commercial_pct": round(truck_pct, 2) if truck_pct is not None else None,
            "yoy_change_pct": round(yoy_change, 2) if yoy_change is not None else None,
            "source": "txdot_arcgis",
            "raw_data": {"aadt_prev": prev_year, "county": county, "submarket": submarket},
        }
        if lat is not None and lng is not None:
            row["location"] = f"POINT({lng} {lat})"

        try:
            sb.table("traffic_counts").upsert(
                row,
                on_conflict="station_id,count_date,authority",
            ).execute()
        except Exception as exc:
            print(f"  DB upsert error for station {station_id}: {exc}")

        # --- Create signals for significant patterns -----------------------
        location_wkt = f"POINT({lng} {lat})"

        # High-volume road with strong growth
        if daily_total is not None and yoy_change is not None:
            if daily_total > 50_000 and yoy_change > 10:
                title = f"Traffic Alert: {road_name} AADT {int(daily_total):,} (+{yoy_change:.1f}% YoY) near {location_name}"
                summary = (
                    f"Station {station_id} on {road_name} ({county} County) reports "
                    f"AADT of {int(daily_total):,} with a {yoy_change:.1f}% year-over-year increase. "
                    f"High traffic volume with strong growth may signal rising industrial demand."
                )
                create_signal(
                    agent_name=AGENT_NAME,
                    signal_type="traffic_trend",
                    severity="alert",
                    title=title,
                    summary=summary,
                    location=location_wkt,
                    submarket=submarket,
                    source_url=SOURCE_URL,
                    data={
                        "station_id": station_id,
                        "road_name": road_name,
                        "aadt": daily_total,
                        "yoy_change_pct": round(yoy_change, 2),
                        "truck_pct": truck_pct,
                        "county": county,
                        "location_name": location_name,
                    },
                )
                signals_created += 1

            elif daily_total > 20_000 and yoy_change > 5:
                title = f"Traffic Watch: {road_name} AADT {int(daily_total):,} (+{yoy_change:.1f}% YoY) near {location_name}"
                summary = (
                    f"Station {station_id} on {road_name} ({county} County) reports "
                    f"AADT of {int(daily_total):,} with a {yoy_change:.1f}% year-over-year increase. "
                    f"Moderate traffic growth worth monitoring for industrial corridor impact."
                )
                create_signal(
                    agent_name=AGENT_NAME,
                    signal_type="traffic_trend",
                    severity="watch",
                    title=title,
                    summary=summary,
                    location=location_wkt,
                    submarket=submarket,
                    source_url=SOURCE_URL,
                    data={
                        "station_id": station_id,
                        "road_name": road_name,
                        "aadt": daily_total,
                        "yoy_change_pct": round(yoy_change, 2),
                        "truck_pct": truck_pct,
                        "county": county,
                        "location_name": location_name,
                    },
                )
                signals_created += 1

        # High truck percentage near industrial areas
        if truck_pct is not None and truck_pct > 15:
            title = f"High Truck Traffic: {road_name} ({truck_pct:.1f}% trucks) near {location_name}"
            summary = (
                f"Station {station_id} on {road_name} ({county} County) shows "
                f"{truck_pct:.1f}% truck traffic"
            )
            if daily_total is not None:
                summary += f" with AADT of {int(daily_total):,}"
            summary += ". High truck percentage indicates heavy freight activity near industrial areas."

            create_signal(
                agent_name=AGENT_NAME,
                signal_type="traffic_trend",
                severity="watch",
                title=title,
                summary=summary,
                location=location_wkt,
                submarket=submarket,
                source_url=SOURCE_URL,
                data={
                    "station_id": station_id,
                    "road_name": road_name,
                    "aadt": daily_total,
                    "yoy_change_pct": yoy_change,
                    "truck_pct": truck_pct,
                    "county": county,
                    "location_name": location_name,
                },
            )
            signals_created += 1

    return signals_created


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="toll_traffic_agent", log_prints=True)
def toll_traffic_agent() -> dict[str, Any]:
    """Orchestrate: query each watch location, process traffic data, signal, log."""
    total_records = 0
    total_signals = 0

    try:
        for loc in WATCH_LOCATIONS:
            features = query_traffic_near_point(
                lat=loc["lat"],
                lng=loc["lng"],
                location_name=loc["name"],
            )
            total_records += len(features)

            if features:
                n_signals = process_traffic_data(
                    features=features,
                    location_name=loc["name"],
                    lat=loc["lat"],
                    lng=loc["lng"],
                    submarket=loc["submarket"],
                )
                total_signals += n_signals

        print(f"Toll traffic agent complete: {total_records} records, {total_signals} signals")

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_records,
            records_new=total_signals,
        )
        return {
            "status": "success",
            "records_found": total_records,
            "signals_created": total_signals,
        }

    except Exception as exc:
        log_agent_run(
            agent_name=AGENT_NAME,
            status="failed",
            records_pulled=total_records,
            records_new=total_signals,
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    toll_traffic_agent()
