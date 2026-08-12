-- =====================================================================
-- 04_cdc_publication.sql
-- Logical-decoding setup consumed by Debezium.
--
-- WHY A NAMED PUBLICATION (rather than letting Debezium create one)
-- -----------------------------------------------------------------
-- Debezium can create `FOR ALL TABLES` itself, but that requires the
-- connector role to be superuser and silently starts capturing every
-- future table - including the control plane, which would feed pipeline
-- metadata back into the analytics stream. Declaring the publication
-- here keeps the capture set explicit, reviewable in git, and lets the
-- Debezium role stay a plain REPLICATION role.
--
-- WHY pgoutput
-- ------------
-- `pgoutput` is the decoding plugin built into Postgres 10+, so there is
-- no extension to install into the image and no version-skew risk with
-- wal2json/decoderbufs.
--
-- REPLICA IDENTITY
-- ----------------
-- FULL on the dimension-ish tables => Debezium emits complete `before`
-- images, so downstream can diff an update without re-reading the source.
-- loan_transactions keeps DEFAULT (PK only) to bound WAL amplification on
-- the highest-volume, append-mostly table.
-- =====================================================================

\set ON_ERROR_STOP on

-- Debezium must be able to read the captured tables for its initial snapshot.
GRANT USAGE ON SCHEMA oltp TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA oltp TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA oltp GRANT SELECT ON TABLES TO debezium;

-- Debezium's heartbeat/signal table (see connector config `signal.data.collection`).
CREATE SCHEMA IF NOT EXISTS cdc;
GRANT USAGE, CREATE ON SCHEMA cdc TO debezium;

CREATE TABLE IF NOT EXISTS cdc.debezium_signal (
    id    VARCHAR(64) PRIMARY KEY,
    type  VARCHAR(32) NOT NULL,
    data  VARCHAR(2048)
);
COMMENT ON TABLE cdc.debezium_signal IS
  'Debezium signalling table: incremental snapshots, log pauses, ad-hoc re-reads.';
GRANT SELECT, INSERT, UPDATE, DELETE ON cdc.debezium_signal TO debezium;

-- A heartbeat table gives Debezium something to write to on low-traffic
-- periods. Without it, an idle capture set means the replication slot's
-- confirmed LSN never advances and WAL accumulates on disk.
CREATE TABLE IF NOT EXISTS cdc.debezium_heartbeat (
    id           INTEGER PRIMARY KEY,
    beat_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO cdc.debezium_heartbeat (id, beat_at)
VALUES (1, now())
ON CONFLICT (id) DO NOTHING;
GRANT SELECT, INSERT, UPDATE ON cdc.debezium_heartbeat TO debezium;

-- ---------------------------------------------------------------------
-- Replica identity
-- ---------------------------------------------------------------------
ALTER TABLE oltp.offices           REPLICA IDENTITY FULL;
ALTER TABLE oltp.staff             REPLICA IDENTITY FULL;
ALTER TABLE oltp.loan_products     REPLICA IDENTITY FULL;
ALTER TABLE oltp.savings_products  REPLICA IDENTITY FULL;
ALTER TABLE oltp.clients           REPLICA IDENTITY FULL;
ALTER TABLE oltp.loans             REPLICA IDENTITY FULL;
ALTER TABLE oltp.savings_accounts  REPLICA IDENTITY FULL;
ALTER TABLE oltp.loan_transactions REPLICA IDENTITY DEFAULT;  -- PK only, by design

-- ---------------------------------------------------------------------
-- Publication: the explicit capture set.
-- ---------------------------------------------------------------------
DROP PUBLICATION IF EXISTS fineract_cdc_pub;
CREATE PUBLICATION fineract_cdc_pub FOR TABLE
    oltp.offices,
    oltp.staff,
    oltp.loan_products,
    oltp.savings_products,
    oltp.clients,
    oltp.loans,
    oltp.loan_transactions,
    oltp.savings_accounts,
    cdc.debezium_heartbeat;

COMMENT ON PUBLICATION fineract_cdc_pub IS
  'Explicit CDC capture set streamed to ClickHouse via Debezium.';
