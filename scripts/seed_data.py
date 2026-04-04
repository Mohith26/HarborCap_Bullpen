"""
Seed Data Script
----------------
Populates the Supabase database with demo submarkets, properties, and
signals, then runs both agents to fill material_prices and building_permits.

Usage:
    python -m scripts.seed_data
"""

from __future__ import annotations

import random
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from shared.db import get_client, log_agent_run
from shared.signals import create_signal

# Ensure reproducible but varied data.
random.seed(42)

# ---------------------------------------------------------------------------
# Submarkets
# ---------------------------------------------------------------------------

SUBMARKETS: list[dict[str, Any]] = [
    {
        "name": "NW Houston Industrial",
        "metro": "Houston",
        "avg_vacancy": 5.2,
        "avg_rent_psf": 8.75,
        "notes": "Major distribution corridor along US-290 and Beltway 8.",
        "sw_lng": -95.64, "sw_lat": 29.85, "ne_lng": -95.50, "ne_lat": 29.95,
    },
    {
        "name": "NE Houston Industrial",
        "metro": "Houston",
        "avg_vacancy": 6.8,
        "avg_rent_psf": 7.90,
        "notes": "Petrochemical-adjacent industrial along Ship Channel.",
        "sw_lng": -95.25, "sw_lat": 29.78, "ne_lng": -95.10, "ne_lat": 29.88,
    },
    {
        "name": "DFW South Industrial",
        "metro": "Dallas-Fort Worth",
        "avg_vacancy": 4.5,
        "avg_rent_psf": 9.10,
        "notes": "I-20/I-35E interchange area, strong e-commerce demand.",
        "sw_lng": -97.00, "sw_lat": 32.55, "ne_lng": -96.85, "ne_lat": 32.68,
    },
    {
        "name": "Austin East",
        "metro": "Austin",
        "avg_vacancy": 7.1,
        "avg_rent_psf": 10.25,
        "notes": "Emerging industrial submarket along SH-130 corridor.",
        "sw_lng": -97.62, "sw_lat": 30.20, "ne_lng": -97.55, "ne_lat": 30.35,
    },
    {
        "name": "San Antonio NE",
        "metro": "San Antonio",
        "avg_vacancy": 5.9,
        "avg_rent_psf": 7.40,
        "notes": "Growth along I-35 toward New Braunfels, manufacturing focus.",
        "sw_lng": -98.38, "sw_lat": 29.52, "ne_lng": -98.28, "ne_lat": 29.60,
    },
]


def _polygon_wkt(sw_lng: float, sw_lat: float, ne_lng: float, ne_lat: float) -> str:
    """Build a WKT POLYGON rectangle from SW and NE corners."""
    return (
        f"POLYGON(({sw_lng} {sw_lat}, {ne_lng} {sw_lat}, "
        f"{ne_lng} {ne_lat}, {sw_lng} {ne_lat}, {sw_lng} {sw_lat}))"
    )


def seed_submarkets() -> list[dict[str, Any]]:
    """Insert submarkets and return the inserted rows."""
    client = get_client()
    rows: list[dict[str, Any]] = []
    for sm in SUBMARKETS:
        row = {
            "id": str(uuid.uuid4()),
            "name": sm["name"],
            "metro": sm["metro"],
            "boundary": _polygon_wkt(sm["sw_lng"], sm["sw_lat"], sm["ne_lng"], sm["ne_lat"]),
            "properties_count": 0,
            "avg_vacancy": sm["avg_vacancy"],
            "avg_rent_psf": sm["avg_rent_psf"],
            "notes": sm["notes"],
        }
        rows.append(row)

    result = client.table("submarkets").upsert(rows, on_conflict="name").execute()
    print(f"  Submarkets upserted: {len(result.data) if result.data else 0}")
    return result.data or rows


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

_PROPERTY_TEMPLATES: list[dict[str, Any]] = [
    # NW Houston Industrial (4 properties)
    {"address": "12400 N Gessner Rd", "city": "Houston", "county": "Harris", "zip": "77064", "lat": 29.90, "lng": -95.56, "property_type": "warehouse", "sf": 185000, "acreage": 8.5, "year_built": 2018, "owner_name": "John Martinez", "owner_entity": "Gulf Logistics LLC", "cad_account": "HAR-001-NW", "appraised_value": 12500000, "market_value": 13200000, "submarket": "NW Houston Industrial"},
    {"address": "8900 W Little York Rd", "city": "Houston", "county": "Harris", "zip": "77040", "lat": 29.87, "lng": -95.52, "property_type": "industrial", "sf": 320000, "acreage": 15.2, "year_built": 2015, "owner_name": "Sarah Chen", "owner_entity": "Bayou Industrial Partners", "cad_account": "HAR-002-NW", "appraised_value": 22000000, "market_value": 24500000, "submarket": "NW Houston Industrial"},
    {"address": "5500 Bingle Rd", "city": "Houston", "county": "Harris", "zip": "77092", "lat": 29.86, "lng": -95.53, "property_type": "warehouse", "sf": 75000, "acreage": 3.8, "year_built": 2020, "owner_name": "David Park", "owner_entity": "Lone Star Storage Inc", "cad_account": "HAR-003-NW", "appraised_value": 5800000, "market_value": 6100000, "submarket": "NW Houston Industrial"},
    {"address": "15200 Hempstead Rd", "city": "Houston", "county": "Harris", "zip": "77040", "lat": 29.89, "lng": -95.58, "property_type": "industrial", "sf": 500000, "acreage": 25.0, "year_built": 2012, "owner_name": "Robert Williams", "owner_entity": "Prologis Houston NW", "cad_account": "HAR-004-NW", "appraised_value": 38000000, "market_value": 41000000, "submarket": "NW Houston Industrial"},
    # NE Houston Industrial (4 properties)
    {"address": "3200 E Mount Houston Rd", "city": "Houston", "county": "Harris", "zip": "77050", "lat": 29.84, "lng": -95.22, "property_type": "warehouse", "sf": 110000, "acreage": 5.5, "year_built": 2019, "owner_name": "Maria Garcia", "owner_entity": "Channel Warehouse Co", "cad_account": "HAR-005-NE", "appraised_value": 7200000, "market_value": 7800000, "submarket": "NE Houston Industrial"},
    {"address": "1400 Jacinto Port Blvd", "city": "Houston", "county": "Harris", "zip": "77015", "lat": 29.80, "lng": -95.15, "property_type": "industrial", "sf": 210000, "acreage": 12.0, "year_built": 2010, "owner_name": "James Nguyen", "owner_entity": "Port Industrial LLC", "cad_account": "HAR-006-NE", "appraised_value": 14500000, "market_value": 15000000, "submarket": "NE Houston Industrial"},
    {"address": "8700 Wallisville Rd", "city": "Houston", "county": "Harris", "zip": "77029", "lat": 29.79, "lng": -95.18, "property_type": "warehouse", "sf": 48000, "acreage": 2.5, "year_built": 2021, "owner_name": "Lisa Thompson", "owner_entity": "East Loop Ventures", "cad_account": "HAR-007-NE", "appraised_value": 3500000, "market_value": 3800000, "submarket": "NE Houston Industrial"},
    {"address": "11500 Beaumont Hwy", "city": "Houston", "county": "Harris", "zip": "77049", "lat": 29.82, "lng": -95.12, "property_type": "industrial", "sf": 155000, "acreage": 7.8, "year_built": 2016, "owner_name": "Mark Anderson", "owner_entity": "Ship Channel Partners", "cad_account": "HAR-008-NE", "appraised_value": 10200000, "market_value": 10800000, "submarket": "NE Houston Industrial"},
    # DFW South Industrial (4 properties)
    {"address": "2500 W Pioneer Pkwy", "city": "Arlington", "county": "Tarrant", "zip": "76013", "lat": 32.66, "lng": -96.98, "property_type": "warehouse", "sf": 280000, "acreage": 13.5, "year_built": 2022, "owner_name": "Kevin Brown", "owner_entity": "DFW Industrial Trust", "cad_account": "TAR-001-SW", "appraised_value": 19500000, "market_value": 21000000, "submarket": "DFW South Industrial"},
    {"address": "4100 S Cockrell Hill Rd", "city": "Dallas", "county": "Dallas", "zip": "75236", "lat": 32.63, "lng": -96.92, "property_type": "industrial", "sf": 420000, "acreage": 20.0, "year_built": 2017, "owner_name": "Angela Davis", "owner_entity": "Metroplex Logistics LP", "cad_account": "DAL-002-SW", "appraised_value": 28000000, "market_value": 31000000, "submarket": "DFW South Industrial"},
    {"address": "600 E Wintergreen Rd", "city": "DeSoto", "county": "Dallas", "zip": "75115", "lat": 32.57, "lng": -96.87, "property_type": "warehouse", "sf": 95000, "acreage": 4.8, "year_built": 2023, "owner_name": "Paul Robinson", "owner_entity": "Link Logistics DFW", "cad_account": "DAL-003-SW", "appraised_value": 7800000, "market_value": 8500000, "submarket": "DFW South Industrial"},
    {"address": "1800 Mountain Creek Pkwy", "city": "Dallas", "county": "Dallas", "zip": "75211", "lat": 32.60, "lng": -96.95, "property_type": "industrial", "sf": 175000, "acreage": 9.0, "year_built": 2014, "owner_name": "Nancy Lee", "owner_entity": "Creek Industrial LLC", "cad_account": "DAL-004-SW", "appraised_value": 11500000, "market_value": 12800000, "submarket": "DFW South Industrial"},
    # Austin East (4 properties)
    {"address": "9800 SH 130 S", "city": "Austin", "county": "Travis", "zip": "78747", "lat": 30.25, "lng": -97.58, "property_type": "warehouse", "sf": 140000, "acreage": 7.0, "year_built": 2024, "owner_name": "Chris Johnson", "owner_entity": "Austin Industrial Dev", "cad_account": "TRA-001-AE", "appraised_value": 10500000, "market_value": 11200000, "submarket": "Austin East"},
    {"address": "2200 Burleson Rd", "city": "Austin", "county": "Travis", "zip": "78744", "lat": 30.22, "lng": -97.60, "property_type": "industrial", "sf": 65000, "acreage": 3.2, "year_built": 2020, "owner_name": "Jennifer White", "owner_entity": "Lone Star Flex LLC", "cad_account": "TRA-002-AE", "appraised_value": 5200000, "market_value": 5600000, "submarket": "Austin East"},
    {"address": "7300 E Ben White Blvd", "city": "Austin", "county": "Travis", "zip": "78741", "lat": 30.23, "lng": -97.57, "property_type": "warehouse", "sf": 10000, "acreage": 0.8, "year_built": 2019, "owner_name": "Michael Adams", "owner_entity": "East Austin Storage", "cad_account": "TRA-003-AE", "appraised_value": 890000, "market_value": 920000, "submarket": "Austin East"},
    {"address": "14500 SH 130 N", "city": "Austin", "county": "Travis", "zip": "78653", "lat": 30.33, "lng": -97.56, "property_type": "industrial", "sf": 230000, "acreage": 11.5, "year_built": 2023, "owner_name": "Steven Clark", "owner_entity": "Samsung Austin Semi", "cad_account": "TRA-004-AE", "appraised_value": 18000000, "market_value": 19500000, "submarket": "Austin East"},
    # San Antonio NE (4 properties)
    {"address": "4600 Dietrich Rd", "city": "San Antonio", "county": "Bexar", "zip": "78219", "lat": 29.55, "lng": -98.35, "property_type": "warehouse", "sf": 92000, "acreage": 4.6, "year_built": 2021, "owner_name": "Daniel Perez", "owner_entity": "Alamo Industrial LP", "cad_account": "BEX-001-NE", "appraised_value": 6200000, "market_value": 6700000, "submarket": "San Antonio NE"},
    {"address": "8200 IH-35 N", "city": "San Antonio", "county": "Bexar", "zip": "78233", "lat": 29.56, "lng": -98.32, "property_type": "industrial", "sf": 160000, "acreage": 8.0, "year_built": 2018, "owner_name": "Karen Miller", "owner_entity": "River City Logistics", "cad_account": "BEX-002-NE", "appraised_value": 10800000, "market_value": 11500000, "submarket": "San Antonio NE"},
    {"address": "1100 Ackerman Rd", "city": "San Antonio", "county": "Bexar", "zip": "78219", "lat": 29.53, "lng": -98.36, "property_type": "warehouse", "sf": 55000, "acreage": 2.8, "year_built": 2022, "owner_name": "Tom Harris", "owner_entity": "SA Cold Chain Inc", "cad_account": "BEX-003-NE", "appraised_value": 4100000, "market_value": 4400000, "submarket": "San Antonio NE"},
    {"address": "12000 Toepperwein Rd", "city": "San Antonio", "county": "Bexar", "zip": "78233", "lat": 29.58, "lng": -98.30, "property_type": "industrial", "sf": 310000, "acreage": 16.0, "year_built": 2016, "owner_name": "Rachel Kim", "owner_entity": "Alamo Distribution Center", "cad_account": "BEX-004-NE", "appraised_value": 21000000, "market_value": 23000000, "submarket": "San Antonio NE"},
]


def seed_properties() -> list[dict[str, Any]]:
    """Insert 20 properties and return inserted rows."""
    client = get_client()
    rows: list[dict[str, Any]] = []
    for t in _PROPERTY_TEMPLATES:
        row = {
            "id": str(uuid.uuid4()),
            "address": t["address"],
            "city": t["city"],
            "county": t["county"],
            "zip": t["zip"],
            "state": "TX",
            "location": f"POINT({t['lng']} {t['lat']})",
            "property_type": t["property_type"],
            "sf": t["sf"],
            "acreage": t["acreage"],
            "year_built": t["year_built"],
            "owner_name": t["owner_name"],
            "owner_entity": t["owner_entity"],
            "cad_account": t["cad_account"],
            "appraised_value": t["appraised_value"],
            "market_value": t["market_value"],
            "source": "seed",
            "raw_data": {},
        }
        rows.append(row)

    result = client.table("properties").upsert(rows, on_conflict="cad_account,source").execute()
    print(f"  Properties upserted: {len(result.data) if result.data else 0}")
    return result.data or rows


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------

_SIGNAL_TEMPLATES: list[dict[str, Any]] = [
    {"agent_name": "building_permits", "signal_type": "building_permit", "severity": "alert", "title": "Major permit: $14.5M warehouse in NW Houston"},
    {"agent_name": "building_permits", "signal_type": "building_permit", "severity": "alert", "title": "$12M distribution center DFW South"},
    {"agent_name": "building_permits", "signal_type": "building_permit", "severity": "watch", "title": "New 45K SF flex space permit in Austin East"},
    {"agent_name": "building_permits", "signal_type": "building_permit", "severity": "watch", "title": "Cold storage expansion permit NE Houston"},
    {"agent_name": "building_permits", "signal_type": "building_permit", "severity": "info", "title": "Minor remodel permit — 8K SF office buildout"},
    {"agent_name": "materials_price", "signal_type": "material_price_spike", "severity": "alert", "title": "Structural Iron/Steel YoY up 18.2%"},
    {"agent_name": "materials_price", "signal_type": "material_price_spike", "severity": "alert", "title": "Steel Mill Products MoM up 5.3%"},
    {"agent_name": "materials_price", "signal_type": "material_price_spike", "severity": "watch", "title": "Construction Materials Composite up 6.2% YoY"},
    {"agent_name": "materials_price", "signal_type": "material_price_spike", "severity": "info", "title": "Ready-Mix Concrete stable at +1.8% YoY"},
    {"agent_name": "cad_monitor", "signal_type": "ownership_change", "severity": "critical", "title": "500K SF portfolio sold — Prologis Houston NW"},
    {"agent_name": "cad_monitor", "signal_type": "ownership_change", "severity": "alert", "title": "Ownership transfer: Ship Channel Partners property"},
    {"agent_name": "cad_monitor", "signal_type": "ownership_change", "severity": "watch", "title": "Entity name change: Gulf Logistics -> GulfStar Logistics"},
    {"agent_name": "cad_monitor", "signal_type": "value_change", "severity": "alert", "title": "Appraised value jumped 35% — 8200 IH-35 N San Antonio"},
    {"agent_name": "cad_monitor", "signal_type": "value_change", "severity": "watch", "title": "Market value decline 12% — 8700 Wallisville Rd Houston"},
    {"agent_name": "cad_monitor", "signal_type": "value_change", "severity": "info", "title": "Routine 3% appraisal increase — multiple properties"},
    {"agent_name": "lease_monitor", "signal_type": "lease_activity", "severity": "critical", "title": "Amazon signs 250K SF lease in DFW South"},
    {"agent_name": "lease_monitor", "signal_type": "lease_activity", "severity": "alert", "title": "Samsung expanding 80K SF at Austin East campus"},
    {"agent_name": "lease_monitor", "signal_type": "lease_activity", "severity": "watch", "title": "New 30K SF logistics tenant in NE Houston"},
    {"agent_name": "lease_monitor", "signal_type": "lease_activity", "severity": "info", "title": "Lease renewal: CubeSmart 55K SF San Antonio NE"},
    {"agent_name": "vacancy_monitor", "signal_type": "vacancy_change", "severity": "alert", "title": "NW Houston vacancy drops to 4.1% — tightest in 3 years"},
    {"agent_name": "vacancy_monitor", "signal_type": "vacancy_change", "severity": "alert", "title": "Austin East vacancy spikes to 9.2% on new deliveries"},
    {"agent_name": "vacancy_monitor", "signal_type": "vacancy_change", "severity": "watch", "title": "DFW South vacancy stable at 4.5%"},
    {"agent_name": "vacancy_monitor", "signal_type": "vacancy_change", "severity": "info", "title": "San Antonio NE vacancy at 5.9% — normal range"},
    {"agent_name": "news_scanner", "signal_type": "news_mention", "severity": "critical", "title": "Tesla eyeing 1M+ SF site in Austin for Megapack factory"},
    {"agent_name": "news_scanner", "signal_type": "news_mention", "severity": "alert", "title": "Port of Houston expansion could boost NE corridor demand"},
    {"agent_name": "news_scanner", "signal_type": "news_mention", "severity": "watch", "title": "Texas Legislature proposes industrial property tax reform"},
    {"agent_name": "news_scanner", "signal_type": "news_mention", "severity": "info", "title": "Houston Chronicle: Industrial rents flatten in Q1 2026"},
    {"agent_name": "zoning_monitor", "signal_type": "zoning_change", "severity": "alert", "title": "Rezoning approved: 45 acres to industrial — NW Houston"},
    {"agent_name": "zoning_monitor", "signal_type": "zoning_change", "severity": "watch", "title": "Zoning hearing scheduled for SH-130 corridor parcel"},
    {"agent_name": "zoning_monitor", "signal_type": "zoning_change", "severity": "info", "title": "Minor variance granted for loading-dock setback"},
]

SUBMARKET_NAMES = [sm["name"] for sm in SUBMARKETS]


def seed_signals() -> None:
    """Insert 30 signals spread over the last 30 days."""
    now = datetime.now(timezone.utc)

    for i, tmpl in enumerate(_SIGNAL_TEMPLATES):
        days_ago = i  # spread 0..29 days back
        created = now - timedelta(days=days_ago, hours=random.randint(0, 12))
        submarket = random.choice(SUBMARKET_NAMES)

        # Pick a random location within Texas.
        lat = round(random.uniform(29.5, 32.8), 4)
        lng = round(random.uniform(-98.5, -95.1), 4)

        create_signal(
            agent_name=tmpl["agent_name"],
            signal_type=tmpl["signal_type"],
            severity=tmpl["severity"],
            title=tmpl["title"],
            summary=f"Auto-generated seed signal: {tmpl['title']}",
            location=f"POINT({lng} {lat})",
            submarket=submarket,
            data={"seed": True, "index": i},
        )

    print(f"  Signals created: {len(_SIGNAL_TEMPLATES)}")


# ---------------------------------------------------------------------------
# Run agents
# ---------------------------------------------------------------------------

def run_agents() -> None:
    """Execute both agents to populate material_prices and building_permits."""
    from agents.building_permits import building_permits_agent
    from agents.materials_price import materials_price_agent

    print("  Running materials_price_agent...")
    materials_price_agent()

    print("  Running building_permits_agent...")
    building_permits_agent()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=== The Bullpen — Seed Data ===\n")

    print("[1/4] Seeding submarkets...")
    seed_submarkets()

    print("[2/4] Seeding properties...")
    seed_properties()

    print("[3/4] Seeding signals...")
    seed_signals()

    print("[4/4] Running agents (materials prices + building permits)...")
    run_agents()

    print("\nSeed complete.")


if __name__ == "__main__":
    main()
