# The Bullpen: AI Agent Analyst Platform for Texas CRE

## What This Is

A system of autonomous data agents that continuously pull public data, run it through analysis pipelines, and surface actionable intelligence for a Texas industrial/warehouse PE firm. Each agent operates on its own schedule, writes to a shared database, and feeds a unified dashboard where the team sees live data streams — like a Bloomberg terminal purpose-built for offmarket CRE deal origination.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    THE BULLPEN DASHBOARD                 │
│              (Next.js + Mapbox + Recharts)               │
│                                                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │ Offmarket│ │  Tenant  │ │  Supply  │ │  Broker  │   │
│  │  Deals   │ │  Demand  │ │  Risk    │ │  Intel   │   │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘   │
│       │             │            │             │         │
│  ┌────┴─────────────┴────────────┴─────────────┴─────┐  │
│  │              SUPABASE (PostgreSQL + PostGIS)       │  │
│  │         + Realtime subscriptions for live feed     │  │
│  └────┬─────────────┬────────────┬─────────────┬─────┘  │
│       │             │            │             │         │
└───────┼─────────────┼────────────┼─────────────┼─────────┘
        │             │            │             │
┌───────┼─────────────┼────────────┼─────────────┼─────────┐
│       │      PREFECT ORCHESTRATION LAYER       │         │
│  ┌────┴────┐  ┌─────┴────┐ ┌────┴─────┐ ┌────┴──────┐  │
│  │ Agent 1 │  │ Agent 2  │ │ Agent 3  │ │ Agent N   │  │
│  │Loan Mat.│  │ Biz Reg  │ │ Permits  │ │ ...       │  │
│  └────┬────┘  └─────┬────┘ └────┬─────┘ └────┬──────┘  │
│       │             │           │             │          │
│  ┌────┴─────────────┴───────────┴─────────────┴───────┐  │
│  │               EXTERNAL DATA SOURCES                │  │
│  │  FRED API · TX Comptroller · TxDOT ArcGIS         │  │
│  │  County CADs · SEC EDGAR · City Open Data          │  │
│  │  JobSpy · BTS Freight · County Clerks              │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Why |
|-------|------|-----|
| **Orchestration** | Prefect (self-hosted or Cloud free tier) | Pure Python decorators, no DAG boilerplate. Built-in retries, caching, scheduling. Dashboard for monitoring agent runs. |
| **Database** | Supabase (PostgreSQL + PostGIS) | Free tier gets you 500MB + PostGIS for spatial queries + Realtime subscriptions for live dashboard updates + Row Level Security. |
| **Dashboard** | Next.js (or Streamlit for v1) | Streamlit for fast v1 prototype. Next.js + Mapbox + Recharts for production version with map views and real-time updates. |
| **AI Layer** | Claude API (via Anthropic SDK) | NLP extraction from broker emails, summarization of market signals, anomaly detection narrative generation. |
| **Alerts** | Slack webhook + email (Resend) | Push notifications when agents detect high-priority signals. |
| **Hosting** | Railway or Fly.io | Run Prefect server + workers. $5-20/month for the entire pipeline. |
| **Geocoding** | Google Maps Geocoding API or Nominatim (free) | Convert addresses to lat/lng for PostGIS spatial queries. |

---

## Database Schema

All agents write to a shared Supabase PostgreSQL database. PostGIS is enabled for spatial queries (buffer analysis around target properties, nearest-neighbor lookups).

### Core Tables

```sql
-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis SCHEMA extensions;

-- ============================================
-- AGENT RUNS (operational metadata)
-- ============================================
CREATE TABLE agent_runs (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    agent_name      TEXT NOT NULL,           -- 'loan_maturity', 'biz_registration', etc.
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running',  -- running | success | failed
    records_pulled  INTEGER DEFAULT 0,
    records_new     INTEGER DEFAULT 0,
    error_message   TEXT,
    metadata        JSONB DEFAULT '{}'::jsonb
);

-- ============================================
-- PROPERTIES (canonical property records)
-- ============================================
CREATE TABLE properties (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    address         TEXT,
    city            TEXT,
    county          TEXT,
    zip             TEXT,
    state           TEXT DEFAULT 'TX',
    location        GEOGRAPHY(POINT, 4326),  -- PostGIS point
    property_type   TEXT,                     -- industrial, warehouse, flex, land
    sf              INTEGER,                  -- square footage
    acreage         NUMERIC(10,4),
    year_built      INTEGER,
    owner_name      TEXT,
    owner_entity    TEXT,
    cad_account     TEXT,                     -- county appraisal district ID
    appraised_value NUMERIC(14,2),
    market_value    NUMERIC(14,2),
    source          TEXT,                     -- 'hcad', 'tcad', 'dcad', etc.
    raw_data        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(cad_account, source)
);

CREATE INDEX idx_properties_location ON properties USING GIST (location);
CREATE INDEX idx_properties_owner ON properties (owner_entity);
CREATE INDEX idx_properties_type ON properties (property_type);

-- ============================================
-- SIGNALS (the core intelligence feed)
-- Every agent writes signals here. This is what
-- the dashboard live-streams to the team.
-- ============================================
CREATE TABLE signals (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    agent_name      TEXT NOT NULL,
    signal_type     TEXT NOT NULL,
    -- Signal types:
    --   loan_maturity, distress_flag, new_business,
    --   job_cluster, permit_filed, freight_shift,
    --   material_price_move, txdot_project,
    --   broker_deal, supply_risk
    severity        TEXT NOT NULL DEFAULT 'info',  -- info | watch | alert | critical
    title           TEXT NOT NULL,
    summary         TEXT,                          -- 1-2 sentence human summary
    property_id     UUID REFERENCES properties(id),
    location        GEOGRAPHY(POINT, 4326),
    submarket       TEXT,
    data            JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_url      TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    expires_at      TIMESTAMPTZ,                   -- auto-archive old signals
    acknowledged    BOOLEAN DEFAULT false,
    acknowledged_by TEXT,
    notes           TEXT
);

CREATE INDEX idx_signals_agent ON signals (agent_name);
CREATE INDEX idx_signals_type ON signals (signal_type);
CREATE INDEX idx_signals_severity ON signals (severity);
CREATE INDEX idx_signals_created ON signals (created_at DESC);
CREATE INDEX idx_signals_location ON signals USING GIST (location);

-- ============================================
-- LOAN MATURITIES
-- ============================================
CREATE TABLE loan_maturities (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    property_id     UUID REFERENCES properties(id),
    loan_type       TEXT,            -- CMBS, conventional, SBA, bridge
    origination     DATE,
    maturity_date   DATE,
    original_amount NUMERIC(14,2),
    current_balance NUMERIC(14,2),
    interest_rate   NUMERIC(5,4),
    lender          TEXT,
    dscr            NUMERIC(6,4),    -- debt service coverage ratio
    ltv             NUMERIC(5,4),    -- loan to value
    status          TEXT,            -- current, delinquent, special_servicing
    source          TEXT,
    raw_data        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_loans_maturity ON loan_maturities (maturity_date);
CREATE INDEX idx_loans_property ON loan_maturities (property_id);

-- ============================================
-- BUSINESS REGISTRATIONS
-- ============================================
CREATE TABLE business_registrations (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    taxpayer_id     TEXT,
    business_name   TEXT NOT NULL,
    naics_code      TEXT,
    industry        TEXT,
    street          TEXT,
    city            TEXT,
    zip             TEXT,
    location        GEOGRAPHY(POINT, 4326),
    registration_date DATE,
    status          TEXT,            -- active, inactive, forfeited
    entity_type     TEXT,            -- LLC, Corp, LP, etc.
    source          TEXT DEFAULT 'tx_comptroller',
    raw_data        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_biz_location ON business_registrations USING GIST (location);
CREATE INDEX idx_biz_zip ON business_registrations (zip);
CREATE INDEX idx_biz_date ON business_registrations (registration_date DESC);

-- ============================================
-- BUILDING PERMITS
-- ============================================
CREATE TABLE building_permits (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    permit_number   TEXT,
    permit_type     TEXT,            -- commercial, industrial, demolition
    status          TEXT,            -- issued, pending, expired
    issue_date      DATE,
    expiration_date DATE,
    address         TEXT,
    city            TEXT,
    location        GEOGRAPHY(POINT, 4326),
    description     TEXT,
    sf              INTEGER,
    valuation       NUMERIC(14,2),
    contractor      TEXT,
    owner_name      TEXT,
    source          TEXT,            -- 'austin_opendata', 'dallas_opendata', etc.
    raw_data        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(permit_number, source)
);

CREATE INDEX idx_permits_location ON building_permits USING GIST (location);
CREATE INDEX idx_permits_date ON building_permits (issue_date DESC);
CREATE INDEX idx_permits_type ON building_permits (permit_type);

-- ============================================
-- MATERIAL PRICES (time series)
-- ============================================
CREATE TABLE material_prices (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    series_id       TEXT NOT NULL,    -- FRED series ID
    series_name     TEXT,
    observation_date DATE NOT NULL,
    value           NUMERIC(12,4),
    unit            TEXT,
    yoy_change      NUMERIC(8,4),    -- year over year % change
    mom_change      NUMERIC(8,4),    -- month over month % change
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(series_id, observation_date)
);

-- ============================================
-- FREIGHT DATA
-- ============================================
CREATE TABLE freight_data (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    metric          TEXT NOT NULL,     -- 'tsi_freight', 'laredo_truck_crossings', etc.
    period          DATE NOT NULL,
    value           NUMERIC(14,4),
    yoy_change      NUMERIC(8,4),
    source          TEXT,
    raw_data        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(metric, period)
);

-- ============================================
-- JOB POSTINGS
-- ============================================
CREATE TABLE job_postings (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    title           TEXT,
    company         TEXT,
    company_industry TEXT,
    city            TEXT,
    zip             TEXT,
    location        GEOGRAPHY(POINT, 4326),
    job_type        TEXT,             -- fulltime, parttime, contract
    salary_min      NUMERIC(10,2),
    salary_max      NUMERIC(10,2),
    date_posted     DATE,
    source          TEXT,             -- indeed, linkedin, glassdoor
    url             TEXT,
    raw_data        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_jobs_location ON job_postings USING GIST (location);
CREATE INDEX idx_jobs_posted ON job_postings (date_posted DESC);
CREATE INDEX idx_jobs_industry ON job_postings (company_industry);

-- ============================================
-- TXDOT PROJECTS
-- ============================================
CREATE TABLE txdot_projects (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    project_id      TEXT UNIQUE,
    project_name    TEXT,
    district        TEXT,
    county          TEXT,
    highway         TEXT,
    phase           TEXT,            -- under_construction, developing, planned, long_range
    let_date        DATE,            -- contract let date
    estimated_cost  NUMERIC(14,2),
    description     TEXT,
    geometry        GEOGRAPHY,       -- line or polygon geometry
    raw_data        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_txdot_geometry ON txdot_projects USING GIST (geometry);

-- ============================================
-- BROKER DEALS (internal tracking)
-- ============================================
CREATE TABLE broker_deals (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    broker_name     TEXT,
    broker_firm     TEXT,
    property_address TEXT,
    property_type   TEXT,
    sf              INTEGER,
    asking_price    NUMERIC(14,2),
    guided_price    NUMERIC(14,2),
    price_per_sf    NUMERIC(10,2),
    cap_rate        NUMERIC(5,4),
    received_date   TIMESTAMPTZ,
    first_public_date TIMESTAMPTZ,   -- when it hit LoopNet/Crexi
    days_before_public INTEGER,      -- computed: first_public - received
    is_first_look   BOOLEAN,
    deal_status     TEXT,            -- new, reviewing, passed, bidding, closed
    location        GEOGRAPHY(POINT, 4326),
    notes           TEXT,
    attachments     JSONB DEFAULT '[]'::jsonb,
    raw_email_data  JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- ============================================
-- SUBMARKETS (reference geography)
-- ============================================
CREATE TABLE submarkets (
    id              UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    metro           TEXT,            -- Houston, DFW, Austin, San Antonio
    boundary        GEOGRAPHY(POLYGON, 4326),
    properties_count INTEGER DEFAULT 0,
    avg_vacancy     NUMERIC(5,4),
    avg_rent_psf    NUMERIC(8,2),
    notes           TEXT
);

CREATE INDEX idx_submarkets_boundary ON submarkets USING GIST (boundary);

-- ============================================
-- VIEWS (pre-computed queries for dashboard)
-- ============================================

-- Live signal feed (last 30 days, unacknowledged first)
CREATE VIEW v_signal_feed AS
SELECT
    s.*,
    p.address as property_address,
    p.sf as property_sf,
    p.owner_name,
    sm.name as submarket_name
FROM signals s
LEFT JOIN properties p ON s.property_id = p.id
LEFT JOIN submarkets sm ON ST_Within(
    s.location::geometry,
    sm.boundary::geometry
)
WHERE s.created_at > now() - interval '30 days'
ORDER BY
    CASE s.severity
        WHEN 'critical' THEN 0
        WHEN 'alert' THEN 1
        WHEN 'watch' THEN 2
        ELSE 3
    END,
    s.acknowledged ASC,
    s.created_at DESC;

-- Agent health dashboard
CREATE VIEW v_agent_health AS
SELECT
    agent_name,
    COUNT(*) as total_runs,
    COUNT(*) FILTER (WHERE status = 'success') as successful,
    COUNT(*) FILTER (WHERE status = 'failed') as failed,
    MAX(finished_at) as last_run,
    AVG(EXTRACT(EPOCH FROM (finished_at - started_at))) as avg_duration_seconds,
    SUM(records_new) as total_new_records
FROM agent_runs
WHERE started_at > now() - interval '7 days'
GROUP BY agent_name;

-- Submarket heatmap data
CREATE VIEW v_submarket_activity AS
SELECT
    sm.name,
    sm.metro,
    COUNT(DISTINCT bp.id) FILTER (WHERE bp.issue_date > now() - interval '90 days')
        as permits_90d,
    COUNT(DISTINCT br.id) FILTER (WHERE br.registration_date > now() - interval '90 days')
        as new_biz_90d,
    COUNT(DISTINCT jp.id) FILTER (WHERE jp.date_posted > now() - interval '30 days')
        as jobs_30d,
    COUNT(DISTINCT s.id) FILTER (WHERE s.severity IN ('alert','critical')
        AND s.created_at > now() - interval '30 days')
        as alerts_30d
FROM submarkets sm
LEFT JOIN building_permits bp ON ST_Within(bp.location::geometry, sm.boundary::geometry)
LEFT JOIN business_registrations br ON ST_Within(br.location::geometry, sm.boundary::geometry)
LEFT JOIN job_postings jp ON ST_Within(jp.location::geometry, sm.boundary::geometry)
LEFT JOIN signals s ON ST_Within(s.location::geometry, sm.boundary::geometry)
GROUP BY sm.name, sm.metro;
```

---

## Agent Specifications

Each agent is a Prefect flow — a Python function with `@flow` and `@task` decorators. They run on independent schedules, handle their own retries, and write results to the shared database.

### Agent 1: New Business Registration

**Schedule:** Daily at 6 AM CT
**Source:** TX Comptroller Public Data API
**Endpoint:** `https://api.comptroller.texas.gov/public-data/v1/public/sales-tax-payer`

```python
# agents/biz_registration.py
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import httpx
from supabase import create_client

COMPTROLLER_API = "https://api.comptroller.texas.gov/public-data/v1/public"
TARGET_ZIPS = ["77001", "77002", ...]  # Houston industrial corridors

@task(retries=3, retry_delay_seconds=30)
def fetch_new_permits_by_zip(zip_code: str, api_key: str) -> list[dict]:
    """Pull new sales tax payer registrations for a ZIP code."""
    logger = get_run_logger()
    url = f"{COMPTROLLER_API}/sales-tax-payer-location"
    params = {"ZIPCODE": zip_code, "page": 1, "pageSize": 100}
    headers = {"x-api-key": api_key}

    response = httpx.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    logger.info(f"ZIP {zip_code}: {data.get('count', 0)} records")
    return data.get("data", [])

@task
def geocode_and_classify(records: list[dict]) -> list[dict]:
    """Geocode addresses and classify by likely property need."""
    # NAICS codes that signal warehouse/industrial demand:
    # 484xxx = Truck transportation
    # 493xxx = Warehousing and storage
    # 423xxx = Durable goods wholesale
    # 424xxx = Nondurable goods wholesale
    # 33xxxx = Manufacturing
    HIGH_DEMAND_NAICS = ["484", "493", "423", "424", "33"]
    enriched = []
    for r in records:
        r["demand_score"] = "high" if any(
            r.get("NAICS", "").startswith(n) for n in HIGH_DEMAND_NAICS
        ) else "standard"
        # geocode address -> lat/lng (Google Maps or Nominatim)
        r["location"] = geocode(r["STREET"], r["CITY"], r["ZIPCODE"])
        enriched.append(r)
    return enriched

@task
def write_to_db(records: list[dict], supabase_url: str, supabase_key: str):
    """Upsert new registrations to Supabase + create signals for high-demand."""
    sb = create_client(supabase_url, supabase_key)
    new_count = 0
    for r in records:
        # upsert to business_registrations table
        result = sb.table("business_registrations").upsert({
            "taxpayer_id": r["TAXPAYER_ID"],
            "business_name": r["BUSINESS_NAME"],
            "street": r["STREET"],
            "city": r["CITY"],
            "zip": r["ZIPCODE"],
            "status": r["STATUS"],
            "location": f"POINT({r['location']['lng']} {r['location']['lat']})",
            "raw_data": r
        }, on_conflict="taxpayer_id").execute()

        if result.data and r["demand_score"] == "high":
            # Create a signal for the dashboard feed
            sb.table("signals").insert({
                "agent_name": "biz_registration",
                "signal_type": "new_business",
                "severity": "watch",
                "title": f"New industrial business: {r['BUSINESS_NAME']}",
                "summary": f"{r['BUSINESS_NAME']} registered in {r['CITY']} {r['ZIPCODE']}. "
                           f"NAICS indicates warehouse/logistics demand.",
                "location": f"POINT({r['location']['lng']} {r['location']['lat']})",
                "data": r
            }).execute()
            new_count += 1
    return new_count

@flow(name="Biz Registration Agent", log_prints=True)
def biz_registration_agent():
    """Daily scan of TX Comptroller for new business registrations."""
    logger = get_run_logger()
    api_key = get_secret("TX_COMPTROLLER_API_KEY")

    all_records = []
    for zip_code in TARGET_ZIPS:
        records = fetch_new_permits_by_zip(zip_code, api_key)
        all_records.extend(records)

    enriched = geocode_and_classify(all_records)
    new_signals = write_to_db(enriched, SUPABASE_URL, SUPABASE_KEY)
    logger.info(f"Agent complete. {len(all_records)} records, {new_signals} new signals.")

if __name__ == "__main__":
    biz_registration_agent.serve(
        name="biz-registration-daily",
        cron="0 6 * * *",  # 6 AM daily
        tags=["tenant-demand"]
    )
```

### Agent 2: FRED Materials Price Monitor

**Schedule:** Monthly on the 20th (after BLS data release)
**Source:** FRED API v2

```python
# agents/materials_price.py
FRED_SERIES = {
    "WPU1017":           "Steel Mill Products",
    "PCU327320327320":   "Ready-Mix Concrete",
    "PCU327320327320C":  "Ready-Mix Concrete (South Region)",
    "PCU33231233231212": "Structural Steel (Commercial Buildings)",
    "WPUSI012011":       "Construction Materials Composite",
    "WPU10740510":       "Structural Iron/Steel (Industrial Buildings)",
}

@task(retries=3, retry_delay_seconds=60,
      cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=24))
def fetch_fred_series(series_id: str, api_key: str) -> list[dict]:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 24  # last 2 years of monthly data
    }
    response = httpx.get(url, params=params, timeout=30)
    return response.json()["observations"]

@task
def compute_changes_and_store(series_id: str, series_name: str,
                               observations: list[dict]):
    """Calculate MoM and YoY changes, write to DB, create signals on big moves."""
    # ... compute changes
    # If YoY > 15% or MoM > 5%, create an alert signal
    if abs(yoy_change) > 15:
        create_signal(
            agent_name="materials_price",
            signal_type="material_price_move",
            severity="alert",
            title=f"{series_name} moved {yoy_change:+.1f}% YoY",
            summary=f"Current index: {current_value}. "
                    f"This affects replacement cost assumptions for new builds.",
            data={"series_id": series_id, "yoy": yoy_change, "mom": mom_change}
        )

@flow(name="Materials Price Agent")
def materials_price_agent():
    for series_id, name in FRED_SERIES.items():
        obs = fetch_fred_series(series_id, FRED_API_KEY)
        compute_changes_and_store(series_id, name, obs)
```

### Agent 3: Building Permits Monitor

**Schedule:** Daily at 7 AM CT
**Sources:** City open data APIs (Socrata)

```python
# agents/building_permits.py
CITY_APIS = {
    "austin": {
        "url": "https://data.austintexas.gov/resource/3syk-w9eu.json",
        "date_field": "issued_date",
        "type_field": "permit_type_desc",
        "value_field": "original_val1",
        "sf_field": "total_existing_bldg_sqft",
    },
    "dallas": {
        "url": "https://www.dallasopendata.com/resource/e7gq-4sah.json",
        "date_field": "issue_date",
        "type_field": "type",
        "value_field": "valuation",
    },
    # Houston, San Antonio, Fort Worth configs...
}

INDUSTRIAL_KEYWORDS = [
    "warehouse", "industrial", "distribution", "logistics",
    "manufacturing", "storage", "cold storage", "flex",
    "tilt-up", "precast", "metal building"
]

@task(retries=2, retry_delay_seconds=30)
def fetch_city_permits(city: str, config: dict, since_date: str) -> list[dict]:
    """Query Socrata API for permits issued since last run."""
    params = {
        "$where": f"{config['date_field']} > '{since_date}'",
        "$limit": 5000,
        "$order": f"{config['date_field']} DESC"
    }
    response = httpx.get(config["url"], params=params, timeout=60)
    return response.json()

@task
def filter_industrial_permits(permits: list[dict], config: dict) -> list[dict]:
    """Filter for industrial/commercial permits above $1M valuation."""
    filtered = []
    for p in permits:
        desc = (p.get("description", "") + " " + p.get(config.get("type_field",""), "")).lower()
        val = float(p.get(config.get("value_field",""), 0) or 0)

        is_industrial = any(kw in desc for kw in INDUSTRIAL_KEYWORDS)
        is_large = val > 1_000_000

        if is_industrial or is_large:
            filtered.append(p)
    return filtered

@flow(name="Building Permits Agent")
def building_permits_agent():
    last_run = get_last_successful_run("building_permits")
    since = last_run or "2025-01-01"

    for city, config in CITY_APIS.items():
        permits = fetch_city_permits(city, config, since)
        industrial = filter_industrial_permits(permits, config)

        for p in industrial:
            # Write to building_permits table
            # Create signal: "New 150,000 SF warehouse permit in NE Houston"
            create_signal(
                agent_name="building_permits",
                signal_type="permit_filed",
                severity="watch" if val < 5_000_000 else "alert",
                title=f"New permit: {p.get('description','')[:80]}",
                summary=f"{city.title()}: ${val:,.0f} valuation, "
                        f"{p.get(config.get('sf_field',''),'?')} SF. "
                        f"Potential new supply in submarket.",
            )
```

### Agent 4: TxDOT Infrastructure Monitor

**Schedule:** Weekly on Monday at 5 AM CT
**Source:** TxDOT ArcGIS REST API

```python
# agents/txdot_infra.py
TXDOT_API = "https://maps.txdot.gov/arcgis/rest/services/TxDOT_Projects/MapServer/0/query"

# Target property locations to monitor (5-mile buffer)
WATCH_LOCATIONS = [
    {"name": "NW Houston Industrial", "lat": 29.85, "lng": -95.55},
    {"name": "DFW South Industrial", "lat": 32.65, "lng": -97.10},
    # ... more target coordinates
]

@task(retries=3, retry_delay_seconds=30)
def query_projects_near_point(lat: float, lng: float, radius_miles: float = 5.0):
    """ArcGIS spatial query: all TxDOT projects within radius of a point."""
    # Convert radius to meters for ArcGIS
    radius_m = radius_miles * 1609.34
    params = {
        "geometry": f"{lng},{lat}",
        "geometryType": "esriGeometryPoint",
        "spatialRel": "esriSpatialRelIntersects",
        "distance": radius_m,
        "units": "esriSRUnit_Meter",
        "outFields": "*",
        "f": "json",
        "returnGeometry": True,
    }
    response = httpx.get(TXDOT_API, params=params, timeout=60)
    return response.json().get("features", [])

@task
def classify_impact(projects: list[dict], watch_name: str):
    """Classify each project as positive or negative for CRE value."""
    POSITIVE = ["widening", "interchange", "overpass", "new road", "expansion"]
    NEGATIVE = ["closure", "demolition", "construction zone"]

    for proj in projects:
        desc = proj["attributes"].get("PROJECT_DESCRIPTION", "").lower()
        impact = "positive" if any(k in desc for k in POSITIVE) else \
                 "negative" if any(k in desc for k in NEGATIVE) else "neutral"

        if impact != "neutral":
            create_signal(
                agent_name="txdot_infra",
                signal_type="txdot_project",
                severity="watch",
                title=f"TxDOT project near {watch_name}",
                summary=f"{proj['attributes'].get('PROJECT_DESCRIPTION','')}. "
                        f"Phase: {proj['attributes'].get('PHASE','')}. "
                        f"Impact: {impact}.",
                data={"impact": impact, "project": proj["attributes"]}
            )
```

### Agent 5: Freight Volume Tracker

**Schedule:** Monthly on 1st at 4 AM CT
**Source:** FRED API + BTS

```python
# agents/freight_volume.py
FREIGHT_SERIES = {
    "TSIFRGHT":  "Freight Transportation Services Index",
    "TSIFRGHTC": "Freight TSI (Rate component)",
}

@flow(name="Freight Volume Agent")
def freight_volume_agent():
    for series_id, name in FREIGHT_SERIES.items():
        obs = fetch_fred_series(series_id, FRED_API_KEY)
        store_freight_data(series_id, name, obs)

    # Also pull BTS transborder data for TX-specific ports
    laredo_data = fetch_bts_transborder("Laredo")
    houston_data = fetch_bts_transborder("Houston")
    # ... store and signal on significant changes
```

### Agent 6: Job Posting Demand Scanner

**Schedule:** Daily at 8 AM CT
**Source:** JobSpy (open-source scraper)

```python
# agents/job_demand.py
from jobspy import scrape_jobs

TARGET_SEARCHES = [
    {"keywords": "warehouse operations", "location": "Houston, TX"},
    {"keywords": "logistics manager", "location": "Dallas, TX"},
    {"keywords": "distribution center", "location": "Austin, TX"},
    {"keywords": "manufacturing technician", "location": "San Antonio, TX"},
    {"keywords": "forklift operator", "location": "Houston, TX"},
    {"keywords": "supply chain", "location": "Fort Worth, TX"},
]

@task(retries=2, retry_delay_seconds=120)  # Indeed rate limits
def scrape_job_listings(search: dict) -> list[dict]:
    jobs = scrape_jobs(
        site_name=["indeed"],
        search_term=search["keywords"],
        location=search["location"],
        results_wanted=50,
        hours_old=24,
        country_indeed="USA",
    )
    return jobs.to_dict("records")

@flow(name="Job Demand Agent")
def job_demand_agent():
    all_jobs = []
    for search in TARGET_SEARCHES:
        jobs = scrape_job_listings(search)
        all_jobs.extend(jobs)

    # Cluster analysis: if >10 new warehouse jobs in one ZIP in 24h
    clusters = cluster_by_zip(all_jobs)
    for zip_code, count in clusters.items():
        if count >= 10:
            create_signal(
                agent_name="job_demand",
                signal_type="job_cluster",
                severity="alert",
                title=f"Hiring surge: {count} warehouse/logistics jobs in {zip_code}",
                summary=f"{count} new warehouse/logistics job postings in ZIP {zip_code} "
                        f"in the last 24 hours. Companies: {top_companies}. "
                        f"This signals expanding tenant demand.",
            )
```

### Agent 7: Property Ownership & Distress

**Schedule:** Weekly on Sunday at 2 AM CT
**Source:** County CAD bulk data downloads (HCAD, DCAD, TCAD)

```python
# agents/property_ownership.py
HCAD_BULK_URL = "https://hcad.org/hcad-online-services/pdata/"

@task
def download_hcad_bulk():
    """Download HCAD real property flat files (updated periodically)."""
    # HCAD publishes tab-delimited text files:
    # - real_acct.txt (account info, owner, values)
    # - real_building.txt (building details, SF, year built)
    # Download, parse, filter for industrial/warehouse properties
    pass

@task
def identify_distressed_owners(properties: list[dict]):
    """Cross-reference owners with UCC filings, tax delinquencies."""
    # Flag: owner has multiple properties + declining values
    # Flag: owner has recent UCC filings (construction debt)
    # Flag: property tax delinquent
    pass
```

### Agent 8: Broker Deal Flow Tracker

**Schedule:** Every 15 minutes during business hours
**Source:** Gmail API (internal email parsing)

```python
# agents/broker_tracker.py

@task
def scan_inbox_for_deals():
    """Parse recent emails for broker deal flow (OMs, teasers)."""
    # Gmail API: search for emails with attachments from known broker domains
    # Known broker domains: @cbre.com, @jll.com, @cushwake.com, @colliers.com,
    #   @nmrk.com, @lfrg.com, @marcusmillichap.com, etc.
    # Extract: property address, SF, asking price, broker name
    pass

@task
def ai_extract_deal_details(email_body: str, attachments: list) -> dict:
    """Use Claude to extract structured deal data from broker emails."""
    prompt = f"""Extract the following from this commercial real estate deal email:
    - Property address
    - Square footage
    - Asking price or price guidance
    - Cap rate (if mentioned)
    - Broker name and firm
    - Property type (warehouse, industrial, flex, etc.)

    Email: {email_body}

    Return as JSON."""

    response = anthropic_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )
    return json.loads(response.content[0].text)
```

---

## Dashboard Design

The dashboard is the team's daily operating surface. It has four main views.

### View 1: Signal Feed (Home)

A reverse-chronological stream of all signals from all agents, filterable by type, severity, submarket. Think Twitter/X feed but for CRE intelligence.

Each signal card shows:
- Agent badge (colored by agent type)
- Severity indicator (green/yellow/orange/red dot)
- Title (1 line)
- Summary (2-3 lines)
- Timestamp + source link
- Property link (if applicable)
- Acknowledge button + notes field

**Implementation:** Supabase Realtime subscription on the `signals` table. New signals push to the dashboard instantly without polling.

```javascript
// Dashboard: real-time signal subscription
const channel = supabase
  .channel('signal-feed')
  .on('postgres_changes',
    { event: 'INSERT', schema: 'public', table: 'signals' },
    (payload) => {
      // Prepend new signal to feed
      setSignals(prev => [payload.new, ...prev])
      // Toast notification for alert/critical
      if (['alert','critical'].includes(payload.new.severity)) {
        toast.warning(payload.new.title)
      }
    }
  )
  .subscribe()
```

### View 2: Map View

Full-screen Mapbox map with layers that can be toggled:
- Property markers (colored by owner/type)
- Building permit markers (sized by valuation)
- New business registration clusters (heat map)
- TxDOT project lines (colored by phase)
- Job posting density (heat map)
- Submarket boundaries (polygon overlays)

Click any marker to see the full signal detail + property card.

**Implementation:** Mapbox GL JS with GeoJSON sources pulled from Supabase PostGIS queries.

```javascript
// Fetch all signals with location for map layer
const { data } = await supabase
  .rpc('signals_as_geojson', { days_back: 90 })

map.addSource('signals', { type: 'geojson', data })
map.addLayer({
  id: 'signal-points',
  type: 'circle',
  source: 'signals',
  paint: {
    'circle-radius': ['match', ['get', 'severity'],
      'critical', 12, 'alert', 9, 'watch', 6, 4],
    'circle-color': ['match', ['get', 'severity'],
      'critical', '#ef4444', 'alert', '#f59e0b',
      'watch', '#3b82f6', '#6b7280']
  }
})
```

### View 3: Agent Health

Operational dashboard showing each agent's status:
- Last run time + next scheduled run
- Success/failure rate (7-day rolling)
- Records pulled / new signals generated
- Average run duration
- Error log (last 5 failures)

Pulled from `v_agent_health` view.

### View 4: Submarket Scorecards

Per-submarket dashboards with:
- Permits filed (rolling 90-day) — supply pipeline
- New business registrations (rolling 90-day) — demand signal
- Job posting volume (rolling 30-day) — demand signal
- Freight volume index — macro demand
- Materials price trends — replacement cost
- Active TxDOT projects — infrastructure changes
- Broker deals received vs. market average
- Composite "opportunity score" (weighted signal count)

Pulled from `v_submarket_activity` view.

---

## Alert Rules

Signals above certain thresholds trigger immediate Slack/email notifications.

```python
# alerts/rules.py
ALERT_RULES = [
    {
        "name": "Large industrial permit",
        "signal_type": "permit_filed",
        "condition": lambda s: s["data"].get("valuation", 0) > 5_000_000,
        "channel": "#deals",
        "severity": "alert",
    },
    {
        "name": "Loan maturity < 6 months",
        "signal_type": "loan_maturity",
        "condition": lambda s: s["data"].get("months_to_maturity", 99) < 6,
        "channel": "#acquisitions",
        "severity": "critical",
    },
    {
        "name": "Job cluster detected",
        "signal_type": "job_cluster",
        "condition": lambda s: s["data"].get("count", 0) >= 10,
        "channel": "#market-intel",
        "severity": "alert",
    },
    {
        "name": "Steel price spike",
        "signal_type": "material_price_move",
        "condition": lambda s: abs(s["data"].get("yoy", 0)) > 20,
        "channel": "#development",
        "severity": "alert",
    },
    {
        "name": "First-look deal received",
        "signal_type": "broker_deal",
        "condition": lambda s: s["data"].get("is_first_look", False),
        "channel": "#deals",
        "severity": "watch",
    },
]
```

---

## Directory Structure

```
bullpen/
├── README.md
├── docker-compose.yml          # Prefect server + worker + Postgres
├── pyproject.toml
├── .env.example                # API keys template
│
├── agents/                     # Each agent is a standalone Prefect flow
│   ├── __init__.py
│   ├── biz_registration.py     # TX Comptroller API
│   ├── building_permits.py     # City open data APIs
│   ├── broker_tracker.py       # Gmail API + Claude NLP
│   ├── freight_volume.py       # FRED + BTS
│   ├── job_demand.py           # JobSpy scraper
│   ├── materials_price.py      # FRED API
│   ├── property_ownership.py   # County CAD bulk data
│   ├── txdot_infra.py          # TxDOT ArcGIS
│   └── loan_maturity.py        # SEC EDGAR + CMBS
│
├── shared/                     # Shared utilities
│   ├── __init__.py
│   ├── db.py                   # Supabase client + helpers
│   ├── geocode.py              # Address -> lat/lng
│   ├── signals.py              # create_signal() helper
│   ├── alerts.py               # Slack/email notification logic
│   └── config.py               # Environment variables + secrets
│
├── sql/                        # Database migrations
│   ├── 001_create_tables.sql
│   ├── 002_create_views.sql
│   ├── 003_create_functions.sql
│   └── seed_submarkets.sql     # TX submarket boundary polygons
│
├── dashboard/                  # Next.js dashboard app
│   ├── package.json
│   ├── app/
│   │   ├── page.tsx            # Signal feed (home)
│   │   ├── map/page.tsx        # Map view
│   │   ├── agents/page.tsx     # Agent health
│   │   └── submarkets/page.tsx # Submarket scorecards
│   ├── components/
│   │   ├── SignalCard.tsx
│   │   ├── MapView.tsx
│   │   ├── AgentStatusCard.tsx
│   │   └── SubmarketScore.tsx
│   └── lib/
│       └── supabase.ts         # Supabase client + realtime
│
├── scripts/                    # One-off data loading
│   ├── load_hcad_bulk.py       # Parse HCAD flat files
│   ├── load_submarkets.py      # Load submarket GeoJSON boundaries
│   └── backfill_fred.py        # Historical FRED data load
│
└── tests/
    ├── test_biz_registration.py
    ├── test_signals.py
    └── test_geocode.py
```

---

## Implementation Phases

### Phase 1: Foundation (Week 1-2)

1. Set up Supabase project, enable PostGIS, run migration SQL
2. Install Prefect, set up local server (`prefect server start`)
3. Build `shared/` utilities: Supabase client, geocoder, signal writer
4. Implement Agent 2 (FRED Materials Price) — simplest agent, proves the pattern
5. Implement Agent 3 (Building Permits) — Austin first, then other cities
6. Build Streamlit v1 dashboard: signal feed + basic charts

**Deliverable:** Working pipeline that pulls FRED data + Austin permits daily, writes signals to Supabase, displays in Streamlit.

### Phase 2: Core Agents (Week 3-4)

1. Implement Agent 1 (New Business Registration) — TX Comptroller API
2. Implement Agent 4 (TxDOT Infrastructure) — ArcGIS spatial queries
3. Implement Agent 5 (Freight Volume) — FRED + BTS data
4. Load submarket boundary polygons for spatial joins
5. Set up Slack webhook alerts for high-severity signals
6. Add map view to dashboard (Streamlit + pydeck or folium)

**Deliverable:** 5 agents running on schedule. Map view showing all signals. Slack alerts working.

### Phase 3: Advanced Agents (Week 5-6)

1. Implement Agent 6 (Job Demand) — JobSpy scraper with rate limiting
2. Implement Agent 7 (Property Ownership) — HCAD bulk data pipeline
3. Implement Agent 8 (Broker Tracker) — Gmail API + Claude extraction
4. Build submarket scorecard view
5. Build agent health monitoring view

**Deliverable:** Full agent suite operational. All dashboard views functional.

### Phase 4: Production Hardening (Week 7-8)

1. Dockerize everything (docker-compose with Prefect + workers)
2. Deploy to Railway or Fly.io
3. Migrate dashboard to Next.js if Streamlit performance is inadequate
4. Add authentication (Supabase Auth or Clerk)
5. Add deal scoring model (composite score from all signals for a property)
6. Add "similar properties" spatial query (PostGIS nearest-neighbor)
7. Set up pg_cron for database maintenance (expire old signals, vacuum)

**Deliverable:** Production system accessible to the team. Deployed and monitored.

---

## API Keys Required

| Service | Key Type | Cost | Sign Up |
|---------|----------|------|---------|
| TX Comptroller | API Key | Free | api-doc.comptroller.texas.gov |
| FRED | API Key | Free | fred.stlouisfed.org/docs/api/api_key.html |
| Supabase | Project URL + anon key | Free tier (500MB) | supabase.com |
| Anthropic (Claude) | API Key | Pay per use (~$3/1M tokens) | console.anthropic.com |
| Google Maps Geocoding | API Key | $5/1000 requests | console.cloud.google.com |
| Mapbox | Access token | Free tier (50K loads/mo) | mapbox.com |
| Gmail | OAuth2 credentials | Free | console.cloud.google.com |
| Slack | Webhook URL | Free | api.slack.com/messaging/webhooks |

---

## Key Design Decisions

**Why Prefect over Airflow:** Prefect lets you write normal Python functions with decorators. No DAG definition files, no Airflow-specific operators. You can run and test each agent locally as a regular Python script before deploying. The free cloud tier gives you monitoring for up to 3 users.

**Why Supabase over raw Postgres:** PostGIS comes pre-installed and toggleable from the dashboard. Realtime subscriptions give you push-based dashboard updates without building a websocket layer. Row Level Security means you can expose the API directly to the dashboard without a backend. The JS client works natively with Next.js.

**Why signals as the core abstraction:** Every agent writes to the same `signals` table regardless of what data it pulls. This means the dashboard only needs to subscribe to one table to get a unified feed. Each signal is self-contained with a title, summary, severity, location, and raw data blob — the dashboard can render any signal without knowing which agent produced it.

**Why PostGIS spatial queries:** The core question "what's happening near my target property" is fundamentally spatial. Buffer queries (`ST_DWithin`), containment checks (`ST_Within`), and nearest-neighbor sorts (`<->` operator) let you answer questions like "show me all permits filed within 3 miles of our deal" or "which submarket has the most new business registrations" without writing custom distance math.

**Why Claude for broker email extraction:** Broker emails and OMs are unstructured, vary wildly in format, and contain embedded PDFs. Rule-based parsing breaks constantly. Claude can extract property address, SF, price, broker name, and property type from messy email HTML with high accuracy and zero template maintenance.
