-- =====================================================================
-- 05_ops_views.sql - the observability contract.
--
-- Everything Prometheus scrapes about *data* (as opposed to processes)
-- is defined here, so there is exactly one definition of "fresh" and one
-- definition of "lag" shared by the exporter, the Grafana dashboards, the
-- alert rules and the Airflow quality gate. When those four disagree, you
-- get pages nobody trusts.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Per-table CDC freshness and lag.
--
-- freshness_seconds = how old the newest row in ClickHouse is, measured
--                     against the SOURCE commit time. This is the number
--                     a business user means by "how current is the data".
-- lag_seconds       = commit-in-Postgres -> visible-in-ClickHouse. This is
--                     the pipeline's own latency, independent of whether
--                     the source is busy.
--
-- Splitting the two matters: a quiet Sunday makes freshness look bad while
-- lag is perfect. Alerting on freshness alone pages you for an idle source.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW fineract_ops.v_cdc_freshness AS
SELECT
    source_table,
    max(source_commit_at)                                       AS last_source_commit_at,
    max(ch_inserted_at)                                         AS last_clickhouse_insert_at,
    dateDiff('second', max(source_commit_at), now())            AS freshness_seconds,
    quantile(0.5)(lag_ms) / 1000                                AS lag_p50_seconds,
    quantile(0.95)(lag_ms) / 1000                               AS lag_p95_seconds,
    max(lag_ms) / 1000                                          AS lag_max_seconds,
    countIf(source_commit_at > now() - INTERVAL 5 MINUTE)       AS events_last_5m,
    countIf(source_commit_at > now() - INTERVAL 1 HOUR)         AS events_last_1h,
    countIf(op = 'c')                                           AS creates,
    countIf(op = 'u')                                           AS updates,
    countIf(op = 'd')                                           AS deletes,
    countIf(op = 'r')                                           AS snapshot_reads
FROM fineract_raw.cdc_audit
WHERE source_commit_at > now() - INTERVAL 24 HOUR
GROUP BY source_table;

-- ---------------------------------------------------------------------
-- Row counts and staleness of every raw table, including the ones with
-- no audit stream. Deliberately reads the raw tables rather than
-- system.parts so that the number matches what a query would return.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW fineract_ops.v_raw_table_health AS
SELECT 'offices' AS table_name, count() AS row_count,
       countIf(_is_deleted = 1) AS deleted_rows,
       max(_ch_inserted_at) AS last_insert_at,
       dateDiff('second', max(_ch_inserted_at), now()) AS seconds_since_insert
FROM fineract_raw.offices
UNION ALL
SELECT 'staff', count(), countIf(_is_deleted = 1), max(_ch_inserted_at),
       dateDiff('second', max(_ch_inserted_at), now()) FROM fineract_raw.staff
UNION ALL
SELECT 'loan_products', count(), countIf(_is_deleted = 1), max(_ch_inserted_at),
       dateDiff('second', max(_ch_inserted_at), now()) FROM fineract_raw.loan_products
UNION ALL
SELECT 'savings_products', count(), countIf(_is_deleted = 1), max(_ch_inserted_at),
       dateDiff('second', max(_ch_inserted_at), now()) FROM fineract_raw.savings_products
UNION ALL
SELECT 'clients', count(), countIf(_is_deleted = 1), max(_ch_inserted_at),
       dateDiff('second', max(_ch_inserted_at), now()) FROM fineract_raw.clients
UNION ALL
SELECT 'loans', count(), countIf(_is_deleted = 1), max(_ch_inserted_at),
       dateDiff('second', max(_ch_inserted_at), now()) FROM fineract_raw.loans
UNION ALL
SELECT 'loan_transactions', count(), countIf(_is_deleted = 1), max(_ch_inserted_at),
       dateDiff('second', max(_ch_inserted_at), now()) FROM fineract_raw.loan_transactions
UNION ALL
SELECT 'savings_accounts', count(), countIf(_is_deleted = 1), max(_ch_inserted_at),
       dateDiff('second', max(_ch_inserted_at), now()) FROM fineract_raw.savings_accounts;

-- ---------------------------------------------------------------------
-- Kafka consumer health, straight from ClickHouse's own system table.
-- `num_rebalance_revocations` climbing is the earliest visible sign of a
-- consumer that is failing to keep up and being kicked from the group.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW fineract_ops.v_kafka_consumers AS
SELECT
    database,
    table,
    consumer_id,
    assignments.topic                AS topics,
    assignments.current_offset       AS current_offsets,
    num_messages_read,
    last_poll_time,
    last_commit_time,
    num_commits,
    last_rebalance_time,
    num_rebalance_revocations,
    num_rebalance_assignments,
    is_currently_used,
    length(exceptions.time)                                   AS exception_count,
    arrayLast(x -> true, exceptions.text)                     AS last_exception,
    dateDiff('second', last_poll_time, now())                 AS seconds_since_poll
FROM system.kafka_consumers;

-- ---------------------------------------------------------------------
-- Merge pressure. ReplacingMergeTree only collapses on merge, so an
-- unbounded part count means queries increasingly read duplicate
-- versions - slow, and then wrong-looking, before anything actually
-- breaks.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW fineract_ops.v_merge_health AS
SELECT
    database,
    table,
    count()                                     AS active_parts,
    sum(rows)                                   AS total_rows,
    round(sum(bytes_on_disk) / 1024 / 1024, 2)  AS size_mb,
    round(sum(data_uncompressed_bytes) /
          nullIf(sum(data_compressed_bytes), 0), 2) AS compression_ratio,
    max(modification_time)                      AS last_modified
FROM system.parts
WHERE active AND database LIKE 'fineract%'
GROUP BY database, table
ORDER BY active_parts DESC;

-- ---------------------------------------------------------------------
-- dbt test results.
--
-- Written by the Airflow `publish_dbt_results` task from run_results.json.
-- Keeping test history in the warehouse (rather than only in Airflow logs)
-- is what makes "has this test ever failed before?" a query instead of an
-- archaeology exercise.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_ops.dbt_test_results
(
    invocation_id   String,
    dag_run_id      String,
    executed_at     DateTime64(3, 'UTC'),
    node_id         String,
    test_name       String,
    model_name      String,
    layer           LowCardinality(String),
    status          LowCardinality(String),   -- pass | fail | warn | error | skipped
    severity        LowCardinality(String),
    failures        UInt64,
    execution_time  Float64,
    message         String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(executed_at)
ORDER BY (executed_at, node_id)
TTL toDateTime(executed_at) + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------
-- dbt model run history - build times per model, so a regression is
-- visible as a trend rather than as a suddenly-missed SLA.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fineract_ops.dbt_model_runs
(
    invocation_id   String,
    dag_run_id      String,
    executed_at     DateTime64(3, 'UTC'),
    node_id         String,
    model_name      String,
    layer           LowCardinality(String),
    materialization LowCardinality(String),
    status          LowCardinality(String),
    rows_affected   Int64,
    execution_time  Float64,
    message         String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(executed_at)
ORDER BY (executed_at, node_id)
TTL toDateTime(executed_at) + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

CREATE OR REPLACE VIEW fineract_ops.v_dbt_latest_run AS
SELECT
    layer,
    countIf(status IN ('pass', 'success')) AS passed,
    countIf(status = 'warn')               AS warned,
    countIf(status IN ('fail', 'error'))   AS failed,
    round(sum(execution_time), 2)          AS total_seconds,
    max(executed_at)                       AS executed_at
FROM fineract_ops.dbt_test_results
WHERE invocation_id = (
    SELECT invocation_id FROM fineract_ops.dbt_test_results
    ORDER BY executed_at DESC LIMIT 1
)
GROUP BY layer;

-- ---------------------------------------------------------------------
-- Reconciliation: does ClickHouse agree with the source of truth?
--
-- The exporter compares these counts with the Postgres row counts and
-- emits `fineract_reconciliation_row_delta`. A non-zero, non-shrinking
-- delta is the signal that CDC has silently dropped something - the one
-- failure mode that no per-component health check can see.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW fineract_ops.v_reconciliation_counts AS
SELECT 'clients'  AS entity, countDistinct(client_id) AS distinct_keys,
       countDistinctIf(client_id, _is_deleted = 0)    AS live_keys
FROM fineract_raw.clients
UNION ALL
SELECT 'loans', countDistinct(loan_id), countDistinctIf(loan_id, _is_deleted = 0)
FROM fineract_raw.loans
UNION ALL
SELECT 'loan_transactions', countDistinct(transaction_id),
       countDistinctIf(transaction_id, _is_deleted = 0)
FROM fineract_raw.loan_transactions
UNION ALL
SELECT 'savings_accounts', countDistinct(savings_id),
       countDistinctIf(savings_id, _is_deleted = 0)
FROM fineract_raw.savings_accounts
UNION ALL
SELECT 'offices', countDistinct(office_id), countDistinctIf(office_id, _is_deleted = 0)
FROM fineract_raw.offices
UNION ALL
SELECT 'staff', countDistinct(staff_id), countDistinctIf(staff_id, _is_deleted = 0)
FROM fineract_raw.staff
UNION ALL
SELECT 'loan_products', countDistinct(product_id), countDistinctIf(product_id, _is_deleted = 0)
FROM fineract_raw.loan_products
UNION ALL
SELECT 'savings_products', countDistinct(product_id), countDistinctIf(product_id, _is_deleted = 0)
FROM fineract_raw.savings_products;
