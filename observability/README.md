# Observability

Monitoring for the Fineract analytics pipeline: Postgres (OLTP + control
plane) -> Debezium CDC -> Kafka -> ClickHouse (raw -> staging ->
intermediate -> marts) -> dbt tests -> Airflow orchestration.

## What is monitored, and why

Every metric in this stack traces back to one of four operational
questions. If a metric doesn't answer one of these, it probably doesn't
belong here.

| Question | Answered by |
|---|---|
| **1. Did it run?** | `fineract_ingestion_last_success_timestamp_seconds`, `fineract_cdc_replication_slot_active`, Airflow scheduler heartbeat |
| **2. Did it work?** | `fineract_ingestion_run_status`, `fineract_dbt_test_failures`, Kafka Connect / task states |
| **3. Was the data good?** | `fineract_ingestion_rejected_rows_total`, `fineract_reconciliation_row_delta`, `fineract_cdc_parse_errors_total` |
| **4. Was it fast enough?** | `fineract_cdc_freshness_seconds`, `fineract_cdc_lag_p50/p95_seconds`, `fineract_dbt_model_runtime_seconds` |

Two things do not overlap, deliberately:

- **Freshness vs lag.** Freshness is "how old is the newest row" measured
  against the source; lag is "how long does our own pipeline take". A
  quiet source makes freshness look bad while lag is perfect - alerting
  on freshness alone pages you for an idle system. See
  `platform/clickhouse/init/05_ops_views.sql` for the exact definitions
  both share with the exporter and the alert rules.
- **Reconciliation vs everything else.** Every other check can be green
  while a handful of CDC events were silently dropped (a poison message,
  a materialized-view bug, a mid-flight schema change).
  `fineract_reconciliation_row_delta` - Postgres row count minus
  ClickHouse distinct live keys - is the one check built specifically to
  catch that failure mode.

## Components

| Component | Role | Port |
|---|---|---|
| `observability/exporter` (pipeline-exporter) | Scrapes Postgres `meta.*` and ClickHouse `fineract_ops.*` and republishes as Prometheus metrics under the `fineract_` namespace | 9105 |
| Prometheus | Scrapes every component below, evaluates alert/recording rules | 9090 |
| Pushgateway | Receives metrics from the batch ingestion job (`fineract_ingest_*`), which cannot itself be scraped | 9091 |
| postgres-exporter | Generic Postgres server metrics (connections, locks, replication) | 9187 |
| kafka-exporter | Kafka broker and consumer-group metrics (`kafka_consumergroup_lag`) | 9308 |
| ClickHouse | Native Prometheus endpoint (`asynchronous_metrics`, `events`, `metrics`) | 9363 |
| Kafka Connect (Debezium) | JMX exporter javaagent | 9404 |
| statsd-exporter | Translates Airflow's StatsD metrics (see `observability/statsd/statsd-mapping.yml`) | 9102 |
| Fineract | Spring Boot actuator / Micrometer metrics | 8443 (`/actuator/prometheus`) |
| Grafana | Dashboards, provisioned from `observability/grafana` | 3000 (default) |

## Alerts

Full detail (expressions, `for:` windows, exact runbook text) lives in
`observability/prometheus/rules/pipeline_alerts.yml` and
`platform_alerts.yml`. Summary:

| Alert | Threshold | Meaning | First action |
|---|---|---|---|
| IngestionStalled | no success > 2h (10m `for`) | entity's data is going stale, or the job stopped running | Check the Airflow DAG run; trigger manually if stuck |
| IngestionFailed | latest run status = failed | most recent load errored | Read `meta.ingestion_run.error_message` for the run |
| IngestionRejectRateHigh | >50 rejects/hour for 15m | validators are quarantining a lot of records | Query `meta.ingestion_reject` grouped by `rule` to find the dominant failure |
| CDCSlotLagHigh | slot WAL retention >512MB for 10m | Debezium consumer has fallen behind or stopped | `make cdc-status`; `make cdc-restart` if tasks are failed |
| CDCSlotInactive | slot has no active consumer for 5m | connector is down; WAL is accumulating | `make cdc-status` then `make cdc-register` |
| CDCFreshnessBreached | loans/loan_transactions >15m stale for 5m | the two most business-critical tables are behind | Check `job:cdc_lag_p95:max`; distinguish pipeline latency from a quiet source |
| CDCParseErrors | any parse error in 15m | poison messages quarantined instead of applied | Inspect `fineract_raw.cdc_errors` for the topic |
| ReconciliationMismatch | non-zero delta for 15m | Postgres and ClickHouse disagree - CDC likely dropped events | Confirm lag/freshness are healthy, then trigger an incremental re-snapshot |
| ClickHousePartsExplosion | >300 active parts for 15m | ReplacingMergeTree merges aren't keeping pace | Check `system.merges`; look for a drop in upstream insert batch size |
| KafkaConsumerLag | >100k messages behind for 10m | a consumer group is falling behind | Check `fineract_ops.v_kafka_consumers` for rebalance churn |
| DbtTestFailures | >0 failing tests for 5m | a transform-layer assertion is failing | Query `fineract_ops.dbt_test_results` for the failing test/model |
| PostgresDown / ClickHouseDown / KafkaConnectDown | target unreachable for 2m | a core component is unscrapable - likely down | `docker compose ps`/`logs` for the affected service |
| DiskPressure | <10% free on a non-tmpfs filesystem for 10m | disk is close to full; every core service fails ungracefully on ENOSPC | Identify the largest consumer before deleting anything |

## Recording rules

`observability/prometheus/rules/recording_rules.yml` pre-computes the
expressions both the alert rules and the dashboards evaluate repeatedly
(`job:cdc_lag_p95:max`, `job:ingestion_freshness_seconds:max`,
`job:clickhouse_active_parts:max`, `job:reconciliation_row_delta:max_abs`),
so the alert engine and every dashboard panel read the same pre-aggregated
number rather than recomputing a `max()` over a live label set on every
tick and every page load.

## Dashboards

Provisioned automatically from `observability/grafana/dashboards`
(`dashboards.yml` file provider - dashboards are code, reviewed like
everything else):

- **pipeline-health.json** - ingestion run status, freshness, reject
  rate, dbt results, exporter self-health.
- **cdc-and-data-quality.json** - replication slot health, CDC
  freshness/lag, parse errors, reconciliation deltas.
- **portfolio-overview.json** - raw table volumes, ClickHouse merge
  pressure, a single-screen operational summary.

## Reaching each UI

Assuming the platform's docker compose network (service names as scraped
above), from the host:

- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090` (targets: `/targets`, rules: `/rules`, alerts: `/alerts`)
- Pipeline exporter metrics directly: `http://localhost:9105/metrics`
- Pushgateway UI: `http://localhost:9091`
- Kafka Connect REST API: `http://localhost:8083/connectors` (see `cdc/README.md` for `make cdc-status`)
