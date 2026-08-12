-- =====================================================================
-- 03_meta_control_plane.sql
-- Pipeline control plane.
--
-- The control plane lives in the same database as the landed data so
-- that "did this batch land?" and "what did it land?" can be answered
-- in one transaction. Watermarks are committed in the SAME transaction
-- as the rows they describe, which is what makes the ingestion layer
-- exactly-once with respect to Postgres (see ingestion/README).
-- =====================================================================

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS meta;

-- ---------------------------------------------------------------------
-- Per-entity incremental watermark.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.ingestion_watermark (
    entity              TEXT        PRIMARY KEY,
    last_success_at     TIMESTAMPTZ,
    last_cursor         TEXT,               -- opaque: max id, ISO date, offset
    last_row_count      BIGINT      DEFAULT 0,
    total_rows_loaded   BIGINT      DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE meta.ingestion_watermark IS
  'Incremental cursor per source entity; updated transactionally with the load.';

-- ---------------------------------------------------------------------
-- Run history - one row per (entity, attempt). Feeds Prometheus.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.ingestion_run (
    run_id              BIGSERIAL   PRIMARY KEY,
    batch_id            UUID        NOT NULL,
    entity              TEXT        NOT NULL,
    dag_run_id          TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    duration_seconds    NUMERIC(12,3),
    status              TEXT        NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running','success','failed','skipped')),
    rows_read           BIGINT      DEFAULT 0,
    rows_inserted       BIGINT      DEFAULT 0,
    rows_updated        BIGINT      DEFAULT 0,
    rows_unchanged      BIGINT      DEFAULT 0,
    rows_rejected       BIGINT      DEFAULT 0,
    api_requests        BIGINT      DEFAULT 0,
    api_retries         BIGINT      DEFAULT 0,
    error_message       TEXT
);
COMMENT ON TABLE meta.ingestion_run IS
  'Observable run history for the ingestion layer; scraped by the pipeline exporter.';

CREATE INDEX IF NOT EXISTS ix_ingestion_run_entity_started
    ON meta.ingestion_run (entity, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_ingestion_run_batch
    ON meta.ingestion_run (batch_id);

-- ---------------------------------------------------------------------
-- Quarantine / dead-letter for records that fail validation.
--
-- A bad record must never fail a whole batch, and must never silently
-- disappear. It lands here with the raw payload and the reason, and the
-- reject count is what alerts fire on.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.ingestion_reject (
    reject_id           BIGSERIAL   PRIMARY KEY,
    batch_id            UUID,
    entity              TEXT        NOT NULL,
    source_key          TEXT,
    rule                TEXT        NOT NULL,
    error_message       TEXT,
    payload             JSONB,
    rejected_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE meta.ingestion_reject IS
  'Quarantined source records that failed schema/business validation.';

CREATE INDEX IF NOT EXISTS ix_ingestion_reject_entity
    ON meta.ingestion_reject (entity, rejected_at DESC);

-- ---------------------------------------------------------------------
-- Data-quality assertion results (ingestion-side expectations).
-- dbt test results are captured separately in ClickHouse.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS meta.data_quality_result (
    result_id           BIGSERIAL   PRIMARY KEY,
    batch_id            UUID,
    layer               TEXT        NOT NULL,   -- 'ingestion' | 'oltp'
    entity              TEXT        NOT NULL,
    check_name          TEXT        NOT NULL,
    severity            TEXT        NOT NULL DEFAULT 'error'
                        CHECK (severity IN ('warn','error')),
    passed              BOOLEAN     NOT NULL,
    observed_value      NUMERIC,
    threshold_value     NUMERIC,
    details             TEXT,
    checked_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE meta.data_quality_result IS
  'Outcome of every data-quality assertion run against the landing layer.';

CREATE INDEX IF NOT EXISTS ix_dq_result_entity
    ON meta.data_quality_result (entity, checked_at DESC);

-- ---------------------------------------------------------------------
-- Convenience view: latest run per entity (used by the exporter and by
-- the "did the pipeline work?" runbook query).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW meta.v_latest_ingestion_run AS
SELECT DISTINCT ON (entity)
       entity,
       run_id,
       batch_id,
       status,
       started_at,
       finished_at,
       duration_seconds,
       rows_read,
       rows_inserted,
       rows_updated,
       rows_rejected,
       EXTRACT(EPOCH FROM (now() - coalesce(finished_at, started_at))) AS seconds_since_run
FROM   meta.ingestion_run
ORDER  BY entity, started_at DESC;

-- ---------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------
GRANT USAGE ON SCHEMA oltp, meta TO app_ingest;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA oltp TO app_ingest;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA meta TO app_ingest;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA meta TO app_ingest;
ALTER DEFAULT PRIVILEGES IN SCHEMA oltp
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_ingest;
ALTER DEFAULT PRIVILEGES IN SCHEMA meta
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_ingest;

GRANT USAGE ON SCHEMA oltp, meta TO analyst_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA oltp TO analyst_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA meta TO analyst_ro;
