-- ============================================================
-- The Bullpen: Core Schema
-- Texas CRE Intelligence Platform
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------
-- Agent Runs  (telemetry for every agent execution)
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_runs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    agent_name      TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL CHECK (status IN ('running','success','failed')),
    records_pulled  INTEGER DEFAULT 0,
    records_new     INTEGER DEFAULT 0,
    error_message   TEXT,
    metadata        JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX idx_agent_runs_agent_name ON agent_runs (agent_name);
CREATE INDEX idx_agent_runs_status     ON agent_runs (status);
CREATE INDEX idx_agent_runs_started    ON agent_runs (started_at DESC);

-- -----------------------------------------------------------
-- Properties
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS properties (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    address         TEXT,
    city            TEXT,
    county          TEXT,
    zip             TEXT,
    state           TEXT NOT NULL DEFAULT 'TX',
    location        GEOGRAPHY(POINT, 4326),
    property_type   TEXT,
    sf              INTEGER,
    acreage         NUMERIC,
    year_built      INTEGER,
    owner_name      TEXT,
    owner_entity    TEXT,
    cad_account     TEXT,
    appraised_value NUMERIC,
    market_value    NUMERIC,
    source          TEXT,
    raw_data        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (cad_account, source)
);

CREATE INDEX idx_properties_city          ON properties (city);
CREATE INDEX idx_properties_county        ON properties (county);
CREATE INDEX idx_properties_property_type ON properties (property_type);
CREATE INDEX idx_properties_location      ON properties USING GIST (location);

-- -----------------------------------------------------------
-- Signals
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS signals (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    agent_name      TEXT NOT NULL,
    signal_type     TEXT NOT NULL,
    severity        TEXT NOT NULL DEFAULT 'info' CHECK (severity IN ('info','watch','alert','critical')),
    title           TEXT NOT NULL,
    summary         TEXT,
    property_id     UUID REFERENCES properties(id) ON DELETE SET NULL,
    location        GEOGRAPHY(POINT, 4326),
    submarket       TEXT,
    data            JSONB DEFAULT '{}'::jsonb,
    source_url      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ,
    acknowledged    BOOLEAN NOT NULL DEFAULT false,
    acknowledged_by TEXT,
    notes           TEXT
);

CREATE INDEX idx_signals_agent_name  ON signals (agent_name);
CREATE INDEX idx_signals_signal_type ON signals (signal_type);
CREATE INDEX idx_signals_severity    ON signals (severity);
CREATE INDEX idx_signals_created_at  ON signals (created_at DESC);
CREATE INDEX idx_signals_location    ON signals USING GIST (location);

-- -----------------------------------------------------------
-- Material Prices  (FRED economic series)
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS material_prices (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    series_id        TEXT NOT NULL,
    series_name      TEXT,
    observation_date DATE NOT NULL,
    value            NUMERIC,
    unit             TEXT,
    yoy_change       NUMERIC,
    mom_change       NUMERIC,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (series_id, observation_date)
);

CREATE INDEX idx_material_prices_series ON material_prices (series_id);
CREATE INDEX idx_material_prices_date   ON material_prices (observation_date DESC);

-- -----------------------------------------------------------
-- Building Permits
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS building_permits (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    permit_number   TEXT NOT NULL,
    permit_type     TEXT,
    status          TEXT,
    issue_date      DATE,
    expiration_date DATE,
    address         TEXT,
    city            TEXT,
    location        GEOGRAPHY(POINT, 4326),
    description     TEXT,
    sf              INTEGER,
    valuation       NUMERIC,
    contractor      TEXT,
    owner_name      TEXT,
    source          TEXT,
    raw_data        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (permit_number, source)
);

CREATE INDEX idx_building_permits_city     ON building_permits (city);
CREATE INDEX idx_building_permits_type     ON building_permits (permit_type);
CREATE INDEX idx_building_permits_date     ON building_permits (issue_date DESC);
CREATE INDEX idx_building_permits_location ON building_permits USING GIST (location);

-- -----------------------------------------------------------
-- Submarkets
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS submarkets (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name              TEXT NOT NULL UNIQUE,
    metro             TEXT,
    boundary          GEOGRAPHY(POLYGON, 4326),
    properties_count  INTEGER DEFAULT 0,
    avg_vacancy       NUMERIC,
    avg_rent_psf      NUMERIC,
    notes             TEXT
);

CREATE INDEX idx_submarkets_metro    ON submarkets (metro);
CREATE INDEX idx_submarkets_boundary ON submarkets USING GIST (boundary);
