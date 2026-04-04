"""
Environmental Risk Agent
-------------------------
Queries TCEQ ArcGIS layers (brownfield, superfund, hazardous waste) for
environmental contamination sites near key Texas industrial areas.
Creates watch/alert signals for sites that could affect nearby industrial
real estate.
"""

from __future__ import annotations

import re, uuid
from typing import Any

import httpx
from prefect import flow, task

from shared.db import get_client, log_agent_run
from shared.signals import create_signal

AGENT_NAME = "environmental_risk"

TCEQ_LAYERS: dict[str, str] = {
    "brownfield": (
        "https://www.gis.hctx.net/arcgishcpid/rest/services/"
        "Environmental/TCEQ_Pollution/MapServer/0/query"
    ),
    "superfund": (
        "https://www.gis.hctx.net/arcgishcpid/rest/services/"
        "Environmental/TCEQ_Pollution/MapServer/2/query"
    ),
    "hazardous_waste": (
        "https://www.gis.hctx.net/arcgishcpid/rest/services/"
        "Environmental/TCEQ_Pollution/MapServer/3/query"
    ),
}

WATCH_LOCATIONS: list[dict[str, Any]] = [
    {"name": "NW Houston Industrial", "lat": 29.85, "lng": -95.55, "submarket": "NW Houston Industrial"},
    {"name": "NE Houston Industrial", "lat": 29.85, "lng": -95.20, "submarket": "NE Houston Industrial"},
    {"name": "DFW South Industrial", "lat": 32.65, "lng": -97.10, "submarket": "DFW South Industrial"},
    {"name": "Austin East Industrial", "lat": 30.25, "lng": -97.60, "submarket": "Austin East"},
    {"name": "San Antonio NE Industrial", "lat": 29.50, "lng": -98.35, "submarket": "San Antonio NE"},
    {"name": "Laredo Port Area", "lat": 27.50, "lng": -99.50, "submarket": None},
    {"name": "El Paso Industrial", "lat": 31.76, "lng": -106.44, "submarket": None},
]

SOURCE_URLS: dict[str, str] = {
    "superfund": "https://www.tceq.texas.gov/remediation/superfund",
    "brownfield": "https://www.tceq.texas.gov/remediation/brownfields",
    "hazardous_waste": "https://www.tceq.texas.gov/permitting/waste_permits/ihw_permits",
}


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


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="query_env_layer", retries=2, retry_delay_seconds=10)
def query_env_layer(
    layer_url: str,
    lat: float,
    lng: float,
    location_name: str,
    radius_meters: float = 8046.7,
) -> list[dict]:
    """Query a TCEQ ArcGIS layer for features within *radius_meters* of a point."""
    params = {
        "geometry": f"{lng},{lat}",
        "geometryType": "esriGeometryPoint",
        "spatialRel": "esriSpatialRelIntersects",
        "distance": radius_meters,
        "units": "esriSRUnit_Meter",
        "outFields": "*",
        "f": "json",
        "returnGeometry": "true",
        "inSR": "4326",
        "outSR": "4326",
    }

    try:
        resp = httpx.get(layer_url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as exc:
        print(f"HTTP error querying TCEQ near {location_name}: {exc}")
        return []
    except httpx.RequestError as exc:
        print(f"Request error querying TCEQ near {location_name}: {exc}")
        return []
    except Exception as exc:
        print(f"Unexpected error querying TCEQ near {location_name}: {exc}")
        return []

    if "error" in data:
        print(f"ArcGIS error near {location_name}: {data['error']}")
        return []

    features = data.get("features", [])
    print(f"  {location_name}: {len(features)} features found")
    return features


@task(name="store_and_signal_sites")
def store_and_signal_sites(
    features: list[dict],
    layer_type: str,
    location_name: str,
    lat: float,
    lng: float,
    submarket: str | None,
) -> int:
    """Upsert environmental sites and create signals.

    Returns the number of signals created.
    """
    sb = get_client()
    signals_created = 0

    for feature in features:
        attrs = feature.get("attributes", {})

        site_id = _get_attr(attrs, "OBJECTID", "FID", default="unknown")
        site_name = _get_attr(attrs, "SITE_NAME", "NAME", "FACILITY_NAME", default="Unknown Site")
        status = _get_attr(attrs, "STATUS", "SITE_STATUS", default="Unknown")
        address = _get_attr(attrs, "ADDRESS", "STREET", default=None)
        city = _get_attr(attrs, "CITY", default=None)
        county = _get_attr(attrs, "COUNTY", default=None)

        # Extract geometry for the site's actual coordinates
        geom = feature.get("geometry")
        site_lng: float | None = None
        site_lat: float | None = None
        if geom:
            site_lng = geom.get("x")
            site_lat = geom.get("y")

        # Build the upsert row
        row: dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "site_id": str(site_id),
            "source": layer_type,
            "site_name": site_name,
            "status": status,
            "address": address,
            "city": city,
            "county": county,
            "site_type": layer_type,
            "nearby_submarket": submarket or location_name,
        }
        if site_lat is not None and site_lng is not None:
            row["location"] = f"POINT({site_lng} {site_lat})"

        sb.table("environmental_sites").upsert(
            row, on_conflict="site_id,source"
        ).execute()

        # Determine severity and title based on layer type
        if layer_type == "superfund":
            severity = "alert"
            title = f"Superfund site near {location_name}: {site_name}"
        elif layer_type == "brownfield":
            severity = "watch"
            title = f"Brownfield near {location_name}: {site_name}"
        else:  # hazardous_waste
            severity = "watch"
            title = f"Hazardous waste facility near {location_name}: {site_name}"

        # Build signal location from site geometry or search point
        if site_lat is not None and site_lng is not None:
            location_wkt = f"POINT({site_lng} {site_lat})"
        else:
            location_wkt = f"POINT({lng} {lat})"

        summary_parts = [
            f"{layer_type.replace('_', ' ').title()} site: {site_name}.",
            f"Status: {status}.",
        ]
        if address:
            summary_parts.append(f"Address: {address}.")
        if city:
            summary_parts.append(f"City: {city}.")
        if county:
            summary_parts.append(f"County: {county}.")

        create_signal(
            agent_name=AGENT_NAME,
            signal_type="environmental_risk",
            severity=severity,
            title=title,
            summary=" ".join(summary_parts),
            location=location_wkt,
            submarket=submarket,
            source_url=SOURCE_URLS.get(layer_type, "https://www.tceq.texas.gov"),
            data={
                "layer_type": layer_type,
                "site_id": str(site_id),
                "site_name": site_name,
                "status": status,
                "address": address,
                "city": city,
                "county": county,
                "location_name": location_name,
            },
        )
        signals_created += 1

    return signals_created


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="environmental_risk_agent", log_prints=True)
def environmental_risk_agent() -> dict[str, Any]:
    """Orchestrate: query each watch location × each TCEQ layer, store, signal, log."""
    total_features = 0
    total_signals = 0

    try:
        for loc in WATCH_LOCATIONS:
            for layer_name, layer_url in TCEQ_LAYERS.items():
                print(f"Querying {layer_name} near {loc['name']}...")

                features = query_env_layer(
                    layer_url=layer_url,
                    lat=loc["lat"],
                    lng=loc["lng"],
                    location_name=loc["name"],
                )
                total_features += len(features)

                if features:
                    n_signals = store_and_signal_sites(
                        features=features,
                        layer_type=layer_name,
                        location_name=loc["name"],
                        lat=loc["lat"],
                        lng=loc["lng"],
                        submarket=loc["submarket"],
                    )
                    total_signals += n_signals

        print(
            f"Environmental risk agent complete: "
            f"{total_features} features, {total_signals} signals"
        )

        log_agent_run(
            agent_name=AGENT_NAME,
            status="success",
            records_pulled=total_features,
            records_new=total_signals,
        )
        return {
            "status": "success",
            "features_found": total_features,
            "signals_created": total_signals,
        }

    except Exception as exc:
        log_agent_run(
            agent_name=AGENT_NAME,
            status="failed",
            records_pulled=total_features,
            records_new=total_signals,
            error_message=str(exc),
        )
        raise


if __name__ == "__main__":
    environmental_risk_agent()
