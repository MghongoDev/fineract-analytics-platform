-- =====================================================================
-- 01_roles_and_databases.sql
-- Bootstrap roles required by the platform.
--
-- Executed automatically by the postgres entrypoint on first boot
-- (files in /docker-entrypoint-initdb.d run in lexical order).
--
-- Roles:
--   app_ingest  -> used by the Fineract ingestion service (DML only)
--   debezium    -> used by the Debezium connector (REPLICATION + SELECT)
--   analyst_ro  -> read-only human/BI access to the OLTP layer
-- =====================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------
-- Ingestion role: writes the landed Fineract data.
-- ---------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ingest') THEN
    EXECUTE format(
      'CREATE ROLE app_ingest LOGIN PASSWORD %L',
      coalesce(current_setting('custom.app_ingest_password', true), 'app_ingest')
    );
  END IF;
END
$$;

-- ---------------------------------------------------------------------
-- Debezium role.
--
-- REPLICATION is required to open a logical replication slot.
-- LOGIN + SELECT on the captured tables is required for the initial
-- consistent snapshot Debezium takes before streaming.
-- ---------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
    EXECUTE format(
      'CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD %L',
      coalesce(current_setting('custom.debezium_password', true), 'debezium')
    );
  END IF;
END
$$;

-- ---------------------------------------------------------------------
-- Read-only analyst role (BI tools, ad-hoc verification).
-- ---------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst_ro') THEN
    EXECUTE format(
      'CREATE ROLE analyst_ro LOGIN PASSWORD %L',
      coalesce(current_setting('custom.analyst_password', true), 'analyst_ro')
    );
  END IF;
END
$$;
