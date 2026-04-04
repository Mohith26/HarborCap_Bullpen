"""
Geocoding utility using Google Maps Geocoding API.
Converts street addresses to lat/lng coordinates for PostGIS storage.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Optional

import httpx

from shared.config import GOOGLE_MAPS_API_KEY

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


@lru_cache(maxsize=2048)
def geocode(address: str, city: str = "", state: str = "TX", zip_code: str = "") -> Optional[dict[str, float]]:
    """Geocode an address to lat/lng using Google Maps API.

    Returns {"lat": float, "lng": float} or None if geocoding fails.
    Results are cached in-memory to avoid redundant API calls.
    """
    if not GOOGLE_MAPS_API_KEY:
        return None

    full_address = ", ".join(part for part in [address, city, state, zip_code] if part)
    if not full_address.strip():
        return None

    try:
        resp = httpx.get(
            GEOCODE_URL,
            params={"address": full_address, "key": GOOGLE_MAPS_API_KEY},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("status") == "OK" and data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            return {"lat": loc["lat"], "lng": loc["lng"]}
    except Exception:
        pass

    return None


def to_point_wkt(lat: float, lng: float) -> str:
    """Convert lat/lng to WKT POINT string for PostGIS."""
    return f"POINT({lng} {lat})"
