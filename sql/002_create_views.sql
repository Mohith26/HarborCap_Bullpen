-- ============================================================
-- The Bullpen: Views
-- ============================================================

-- -----------------------------------------------------------
-- v_signal_feed
-- Latest 30 days of signals joined to properties,
-- sorted by severity weight, then unacknowledged first,
-- then newest first.
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_signal_feed AS
SELECT
    s.id              AS signal_id,
    s.agent_name,
    s.signal_type,
    s.severity,
    s.title,
    s.summary,
    s.submarket,
    s.data,
    s.source_url,
    s.created_at,
    s.expires_at,
    s.acknowledged,
    s.acknowledged_by,
    s.notes,
    p.id              AS property_id,
    p.address         AS property_address,
    p.city            AS property_city,
    p.county          AS property_county,
    p.property_type,
    p.sf              AS property_sf,
    p.owner_name      AS property_owner,
    p.owner_entity    AS property_owner_entity,
    p.market_value    AS property_market_value,
    COALESCE(s.location, p.location) AS location
FROM signals s
LEFT JOIN properties p ON s.property_id = p.id
WHERE s.created_at >= now() - INTERVAL '30 days'
ORDER BY
    CASE s.severity
        WHEN 'critical' THEN 0
        WHEN 'alert'    THEN 1
        WHEN 'watch'    THEN 2
        WHEN 'info'     THEN 3
    END ASC,
    s.acknowledged ASC,
    s.created_at DESC;

-- -----------------------------------------------------------
-- v_agent_health
-- Per-agent stats for the last 7 days.
-- -----------------------------------------------------------
CREATE OR REPLACE VIEW v_agent_health AS
SELECT
    agent_name,
    COUNT(*)                                                   AS total_runs,
    COUNT(*) FILTER (WHERE status = 'success')                 AS successful,
    COUNT(*) FILTER (WHERE status = 'failed')                  AS failed,
    MAX(started_at)                                            AS last_run,
    ROUND(
        AVG(
            EXTRACT(EPOCH FROM (finished_at - started_at))
        ) FILTER (WHERE finished_at IS NOT NULL)
    , 1)                                                       AS avg_duration_seconds,
    COALESCE(SUM(records_new) FILTER (WHERE status = 'success'), 0) AS total_new_records
FROM agent_runs
WHERE started_at >= now() - INTERVAL '7 days'
GROUP BY agent_name
ORDER BY last_run DESC;
