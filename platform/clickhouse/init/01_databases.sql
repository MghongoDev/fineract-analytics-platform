-- =====================================================================
-- 01_databases.sql - ClickHouse database layout.
--
-- One database per layer rather than one database with name prefixes:
-- grants, retention policies and `DROP DATABASE` blast radius all follow
-- the layer boundary, and dbt's custom-schema macro maps cleanly onto it.
--
--   fineract_raw          landing zone fed by CDC. Append-only,
--                         ReplacingMergeTree, no business logic.
--   fineract_staging      dbt: deduplicated, typed, renamed. 1:1 with raw.
--   fineract_intermediate dbt: reusable business logic, not exposed to BI.
--   fineract_marts        dbt: analytics-ready facts and dimensions.
--   fineract_ml           dbt: point-in-time-correct feature tables.
--   fineract_ops          pipeline observability (freshness, CDC lag,
--                         dbt test results). Deliberately separate from
--                         the data itself.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS fineract_raw;
CREATE DATABASE IF NOT EXISTS fineract_staging;
CREATE DATABASE IF NOT EXISTS fineract_intermediate;
CREATE DATABASE IF NOT EXISTS fineract_marts;
CREATE DATABASE IF NOT EXISTS fineract_ml;
CREATE DATABASE IF NOT EXISTS fineract_ops;
