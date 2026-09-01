# Operator runbook

Operational reference for the Fineract analytics platform: starting and
stopping the stack, verifying that data actually moved through each
stage, and a numbered incident playbook for every alert defined in
`observability/prometheus/rules/*.yml`.

All commands assume the repo root as the working directory and the
default `docker-compose.yml` port mappings. If you have overridden any
`*_PORT` variable in `.env`, substitute your value.

---

## 1. Starting and stopping the stack

```bash
cp .env.example .env               # once, before first start

make up                            # everything: postgres, kafka, connect,
                                    # clickhouse, airflow, exporters, prometheus,
                                    # grafana (source system NOT included)

# Pick a source (see .env.example "Pick ONE"):
docker compose --profile fineract up -d     # self-hosted Fineract
docker compose --profile mock up -d         # offline mock (no internet)
# or set FINERACT_BASE_URL=https://demo.mifos.io/... and use neither profile

make bootstrap                     # up-core + wait for health + apply
                                    # ClickHouse init SQL + register CDC
                                    # connector + seed via mock ingestion

make up-core                       # data plane only: fineract, postgres,
                                    # kafka, kafka-connect, clickhouse

make down                          # stop, keep volumes (data survives)
make clean                         # stop AND remove volumes (irreversible)

make ps                            # service status
make logs                          # tail all logs
make logs SERVICE=kafka-connect    # tail one service
```

`tools` profile services (`kafka-ui`, the interactive `dbt` shell) are
not started by `make up` — bring them up explicitly:

```bash
docker compose --profile tools up -d kafka-ui
docker compose run --rm dbt run --select tag:marts
```

`make urls` prints every UI URL and credential below, resolved against
your current `.env`.

---

## 2. UIs, ports and credentials

| Service | URL | Credentials | Notes |
|---|---|---|---|
| Fineract API (self-hosted) | `https://localhost:8443/fineract-provider/api/v1` | `mifos` / `password` | self-signed cert; `curl -k` |
| Fineract mock | `http://localhost:8090/fineract-provider/api/v1` | none | profile `mock` only |
| Postgres (OLTP) | `postgresql://localhost:5433/fineract_oltp` | superuser `postgres` / `postgres`; app role `app_ingest` / `app_ingest` | see `.env` |
| Kafka (external listener) | `localhost:9092` | none | broker only, no UI |
| Kafka UI | `http://localhost:8091` | none | profile `tools` |
| Kafka Connect REST API | `http://localhost:8083/connectors` | none | Debezium connector lives here |
| ClickHouse HTTP | `http://localhost:8123` | `analytics` / `analytics` | `?query=` or POST body |
| ClickHouse native | `localhost:9000` | `analytics` / `analytics` | `clickhouse-client` |
| Airflow webserver | `http://localhost:8085` | `admin` / `admin` | DAG: `fineract_analytics_pipeline` |
| Prometheus | `http://localhost:9090` | none | |
| Grafana | `http://localhost:3000` | `admin` / `admin` | ClickHouse datasource pre-provisioned |
| Pushgateway | `http://localhost:9091` | none | batch job metrics |
| Pipeline exporter | `http://localhost:9105/metrics` | none | freshness/lag/reconciliation |

Defaults come from `Makefile` variables and `.env` — override either to
change them (e.g. `POSTGRES_PASSWORD=... make urls`).

---

## 3. Verifying data at each stage

Work top to bottom; each stage's check assumes the previous one passed.

### 3.1 Ingestion landed in Postgres (`oltp.*`)

```bash
make psql
# or directly:
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d fineract_oltp

-- row counts and last watermark per entity
SELECT * FROM meta.v_latest_ingestion_run ORDER BY entity;

-- did the last run for an entity actually succeed?
SELECT entity, status, rows_inserted, rows_updated, rows_rejected, error_message
  FROM meta.ingestion_run
 WHERE entity = 'loans'
 ORDER BY started_at DESC LIMIT 5;

-- spot-check the landed rows themselves
SELECT loan_id, status_value, principal, disbursed_on_date, _updated_at
  FROM oltp.loans ORDER BY _updated_at DESC LIMIT 10;
```

### 3.2 WAL / replication slot is active and not falling behind

```bash
make cdc-lag
# equivalent:
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d fineract_oltp -c \
  "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag_bytes
     FROM pg_replication_slots;"
```

Expect one row: `slot_name = fineract_cdc_slot`, `active = t`, `lag_bytes`
close to `0 bytes` on an idle system.

### 3.3 Debezium connector is running

```bash
make cdc-status
# equivalent:
curl -fsS http://localhost:8083/connectors?expand=status | python3 -m json.tool
```

Expect `connector.state = RUNNING` and every entry in `tasks[]` also
`RUNNING`. The connector name is `fineract-oltp-source`.

### 3.4 Messages are reaching Kafka

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic fineract.oltp.loan_transactions \
  --from-beginning --max-messages 5
```

Or watch consumer lag directly:

```bash
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --describe --group clickhouse_fineract_loan_transactions
```

### 3.5 ClickHouse Kafka engine + materialized views are consuming

```bash
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics -q \
  "SELECT * FROM fineract_ops.v_kafka_consumers FORMAT Vertical"
```

`seconds_since_poll` should be small (single-digit to low tens of
seconds); `num_rebalance_revocations` climbing over time means the
consumer is being kicked from the group, not just slow.

### 3.6 Rows landed in the raw layer with correct version/delete semantics

```bash
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics -q \
  "SELECT source_table, freshness_seconds, lag_p50_seconds, lag_p95_seconds,
          events_last_5m, creates, updates, deletes
     FROM fineract_ops.v_cdc_freshness
    ORDER BY source_table"

# or via HTTP:
curl -s "http://localhost:8123/?user=analytics&password=analytics" \
  --data-binary "SELECT * FROM fineract_ops.v_raw_table_health FORMAT PrettyCompact"
```

`freshness_seconds` should be under a few minutes on an active source.
`lag_p95_seconds` (Postgres commit → ClickHouse insert) is normally
1–3 seconds.

### 3.7 Postgres and ClickHouse agree on row counts (reconciliation)

```bash
# Postgres side
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d fineract_oltp -c \
  "SELECT 'loans', count(*) FROM oltp.loans
   UNION ALL SELECT 'clients', count(*) FROM oltp.clients
   UNION ALL SELECT 'loan_transactions', count(*) FROM oltp.loan_transactions;"

# ClickHouse side (only live rows, matching Postgres semantics)
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics -q \
  "SELECT entity, distinct_keys, live_keys FROM fineract_ops.v_reconciliation_counts"
```

`live_keys` in ClickHouse should equal the Postgres `count(*)` for the
same entity. The pipeline exporter turns any persistent mismatch into
`fineract_reconciliation_row_delta`.

### 3.8 dbt built the warehouse and tests passed

```bash
make dbt-run          # or: docker compose run --rm dbt run
make dbt-test

# from the warehouse itself:
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics -q \
  "SELECT * FROM fineract_ops.v_dbt_latest_run ORDER BY layer"

# spot-check a mart
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics -q \
  "SELECT count(), sum(total_outstanding) FROM fineract_marts.fct_loan"
```

### 3.9 Airflow ran the pipeline end to end

```bash
curl -fsS -u admin:admin \
  "http://localhost:8085/api/v1/dags/fineract_analytics_pipeline/dagRuns?limit=5&order_by=-start_date" \
  | python3 -m json.tool
```

Or open `http://localhost:8085` → `fineract_analytics_pipeline` → Graph
view. The chain is `preflight → ingest → cdc_gate → transform → test →
publish → quality_gate`; a stall at `cdc_gate` means Airflow is waiting
for ClickHouse row counts to match Postgres (see §4.5/§4.6 below).

---

## 4. Incident playbook

Each entry: symptom → likely cause → diagnostics → remedy → confirm
recovery. Alert names match `observability/prometheus/rules/*.yml`.

### 4.1 `IngestionStalled` / `IngestionFailed`

**Symptom.** No successful ingestion run for an entity in >2h, or the
latest run has `status=failed`.

**Likely cause.** Airflow scheduler stuck or task queued behind a retry
backoff; Fineract API unreachable/slow; a validation rule rejecting most
of a batch and tripping `INGEST_MAX_REJECT_RATIO`.

**Diagnostics.**
```bash
# Airflow: is the task running at all?
curl -fsS -u admin:admin \
  "http://localhost:8085/api/v1/dags/fineract_analytics_pipeline/dagRuns/~/taskInstances?limit=20&order_by=-start_date"

# Postgres: last run's error
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d fineract_oltp -c \
  "SELECT run_id, status, error_message FROM meta.ingestion_run
     WHERE entity = '<entity>' ORDER BY started_at DESC LIMIT 3;"

# ingestion container logs for the batch_id in the row above
docker compose logs ingestion --tail 200
```

**Remedy.**
```bash
make airflow-trigger DAG=fineract_analytics_pipeline    # if nothing is running
make ingest                                              # run ingestion directly, bypassing Airflow
```

**Confirm recovery.** `meta.v_latest_ingestion_run` shows `status =
success` and `seconds_since_run` resets close to 0; `IngestionStalled`
clears in Prometheus within one scrape interval once the metric updates.

### 4.2 `IngestionRejectRateHigh`

**Symptom.** >50 rows for an entity quarantined into
`meta.ingestion_reject` in the last hour.

**Likely cause.** An upstream Fineract schema or business-rule change
the validators do not know about yet (new enum value, a field that
became nullable, a date format change).

**Diagnostics.**
```sql
SELECT rule, count(*) FROM meta.ingestion_reject
 WHERE entity = '<entity>' AND rejected_at > now() - interval '1 hour'
 GROUP BY rule ORDER BY 2 DESC;

SELECT payload FROM meta.ingestion_reject
 WHERE entity = '<entity>' AND rule = '<dominant rule>'
 ORDER BY rejected_at DESC LIMIT 5;
```

**Remedy.** Fix or relax the validator in
`ingestion/fineract_ingest/validation.py` for the specific rule, or if
the source data is genuinely bad, escalate to whoever owns the Fineract
tenant rather than silently loosening the check.

**Confirm recovery.** Re-run `make ingest --entities <entity>` (or wait
for the next scheduled run) and confirm the reject count for the new
batch is back near zero.

### 4.3 `CDCSlotLagHigh` — replication slot lag growing

**Symptom.** `fineract_cdc_replication_slot_lag_bytes > 512MB` for 10
minutes. Left unchecked, WAL fills the Postgres data disk.

**Likely cause.** The Debezium connector task has failed or stopped
consuming (most common); ClickHouse or Kafka is down so nothing is
draining the topic and the connector itself is backed up; a very large
burst of writes outpacing the connector.

**Diagnostics.**
```bash
make cdc-status                    # connector + task state
make cdc-lag                       # current lag in bytes
docker compose logs kafka-connect --tail 200
```

**Remedy.**
```bash
# tasks FAILED:
make cdc-restart

# Kafka Connect container itself is unhealthy:
docker compose restart kafka-connect
make cdc-register                  # idempotent: re-applies connector config

# Kafka is down (not Connect): this is expected behaviour, not a bug.
# The slot holds WAL (capped by max_slot_wal_keep_size=1GB) until Kafka
# recovers. Do NOT drop the slot - that forces a full re-snapshot.
docker compose up -d kafka
```

**Confirm recovery.** `make cdc-lag` shows `lag_bytes` shrinking back
toward zero; `fineract_cdc_replication_slot_lag_bytes` in Prometheus
drops below the 512MB threshold and the alert resolves after its `for:
10m` window.

### 4.4 `CDCSlotInactive` / connector task `FAILED`

**Symptom.** No process is reading from `fineract_cdc_slot`, or
`make cdc-status` shows a task in state `FAILED`.

**Likely cause.** Kafka Connect crashed and lost the connector; a task
threw an unhandled exception (schema mismatch, serialization error with
`errors.tolerance=none`); the connector was never registered after a
fresh volume.

**Diagnostics.**
```bash
make cdc-status
docker compose logs kafka-connect --tail 300 | grep -i -A5 error
```

**Remedy.**
```bash
make cdc-register     # idempotent PUT /connectors/<name>/config; safe to
                       # run repeatedly, reconciles to what is in git

# if that doesn't clear a FAILED task:
curl -X POST "http://localhost:8083/connectors/fineract-oltp-source/restart?includeTasks=true&onlyFailed=false"
```

**If the slot was invalidated** (WAL exceeded `max_slot_wal_keep_size`
before recovery — Postgres drops the slot and the connector fails
loudly on restart): the connector's `snapshot.mode=initial` means
re-registering triggers a fresh consistent snapshot automatically. This
is slower (a full table scan per captured table) but requires no manual
data surgery.

**Confirm recovery.** `make cdc-status` shows `connector.state =
RUNNING` and all tasks `RUNNING`; `fineract_cdc_replication_slot_active`
returns to `1`.

### 4.5 `CDCFreshnessBreached`

**Symptom.** `loans` or `loan_transactions` in ClickHouse is >15 minutes
behind its source commit time.

**Likely cause.** Either genuine pipeline latency (Kafka Connect
backlog, ClickHouse consumer stalled) or a quiet source — check
`job:cdc_lag_p95:max` first to distinguish the two, since freshness
alone cannot.

**Diagnostics.**
```promql
job:cdc_lag_p95:max
```
```bash
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics -q \
  "SELECT * FROM fineract_ops.v_cdc_freshness WHERE source_table IN ('loans','loan_transactions')"
```

**Remedy.** If `lag_p95_seconds` is also elevated, this is really
§4.3/§4.4/§4.6 — work those first. If lag is normal (1-3s) and
freshness is still stale, the source itself has gone quiet (check
Fineract/ingestion, §4.1) rather than the pipeline being broken.

**Confirm recovery.** `freshness_seconds` for both tables drops under
900s and stays there through the alert's `for: 5m` window.

### 4.6 `ClickHouse consumer stopped` (surfaces as `KafkaConsumerLag` or a stalled `v_kafka_consumers`)

**Symptom.** `kafka_consumergroup_lag > 100000` for a
`clickhouse_fineract_*` consumer group, or
`fineract_ops.v_kafka_consumers.seconds_since_poll` is large and not
moving.

**Likely cause.** The Kafka engine table's background consumer thread
inside ClickHouse has died (commonly after a schema mismatch it could
not tolerate, or a ClickHouse restart that didn't reattach the
consumer); a rebalance storm from ClickHouse and another consumer
fighting over the same group id (should not happen — group ids are
per-table-per-service by design, but check if someone reused one).

**Diagnostics.**
```sql
SELECT database, table, consumer_id, num_messages_read, last_poll_time,
       num_rebalance_revocations, exception_count, last_exception
  FROM fineract_ops.v_kafka_consumers;
```

**Remedy.** Detaching and reattaching the Kafka engine table's MV
restarts its consumer without touching data:
```sql
DETACH TABLE fineract_raw.kafka_loan_transactions;
ATTACH TABLE fineract_raw.kafka_loan_transactions;
```
If that does not resume consumption, check for a poison message
stalling the partition (§4.8) — `kafka_handle_error_mode='stream'`
should prevent this, but a message the MV's `WHERE length(_error) = 0`
filter cannot classify (e.g. a truncated JSON payload) can still wedge
a partition.

**Confirm recovery.** `num_messages_read` increases on the next check;
`seconds_since_poll` returns to single digits; consumer lag in Kafka
trends back to zero.

### 4.7 `ReconciliationMismatch`

**Symptom.** `fineract_reconciliation_row_delta != 0` for 15+ minutes —
longer than normal CDC lag explains. This means events were dropped,
not merely delayed.

**Likely cause.** A message landed in `fineract_raw.cdc_errors` instead
of the raw table (parse failure); a Kafka Engine consumer restarted
mid-batch and lost an uncommitted offset window (rare, but possible
under `kafka_flush_interval_ms` boundaries); a manual `DELETE` or
`TRUNCATE` against a `fineract_raw.*` table.

**Diagnostics.**
```bash
# confirm this is not just slow (rules out "just CDC lag")
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics -q \
  "SELECT * FROM fineract_ops.v_cdc_freshness"

# check for quarantined messages for the affected entity's topic
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics -q \
  "SELECT count(), min(observed_at), max(observed_at) FROM fineract_raw.cdc_errors
    WHERE topic = 'fineract.oltp.<entity>'"

# compare exact counts
docker compose exec clickhouse clickhouse-client \
  --user analytics --password analytics -q \
  "SELECT * FROM fineract_ops.v_reconciliation_counts WHERE entity = '<entity>'"
```

**Remedy.** If freshness/lag are healthy and the mismatch persists,
trigger an incremental re-snapshot of just the affected table (§4.9) —
this is the standard recovery and does not require stopping the
connector or losing in-flight events for other tables.

**Confirm recovery.** `v_reconciliation_counts.live_keys` for the
entity equals the Postgres `count(*)`; `fineract_reconciliation_row_delta`
returns to 0 and the alert resolves.

### 4.8 Poison message in `fineract_raw.cdc_errors`

**Symptom.** `CDCParseErrors` fires: `increase(fineract_cdc_parse_errors_total[15m]) > 0`.

**Likely cause.** A malformed or unexpected-shape JSON message on a
topic — usually an upstream schema change (new column with a type the
materialized view's cast expression cannot parse) or a genuinely
corrupt message.

**Diagnostics.**
```sql
SELECT topic, partition, offset, error, raw_message, observed_at
  FROM fineract_raw.cdc_errors
 WHERE topic = 'fineract.oltp.<table>'
 ORDER BY observed_at DESC LIMIT 20;
```

**Remedy.**
1. Read `raw_message` to understand the actual shape versus what
   `platform/clickhouse/init/02_kafka_sources.sql` /
   `04_materialized_views.sql` expect.
2. If it is a genuine schema change (new/renamed column, changed type),
   update the `kafka_<table>` Kafka engine table's column list and the
   corresponding `mv_<table>` cast in `04_materialized_views.sql`, then
   re-apply:
   ```bash
   curl -fsS "http://localhost:8123/?user=analytics&password=analytics" \
     --data-binary @platform/clickhouse/init/04_materialized_views.sql
   ```
3. To reprocess the specific message once the MV is fixed, replay it
   from Kafka by resetting the consumer group's offset for that
   partition (only if the topic still retains it — 7-day retention), or
   trigger a re-snapshot of the source table (§4.9) which regenerates
   the message from Postgres directly.

**Confirm recovery.** No new rows appear in `fineract_raw.cdc_errors`
for the topic after the fix; `fineract_cdc_parse_errors_total` stops
incrementing.

### 4.9 dbt test failure

**Symptom.** `DbtTestFailures` fires: `fineract_dbt_test_failures > 0`
for a layer.

**Likely cause.** Bad or unexpected source data (a reconciliation
delta, a NULL where a test expects `not_null`, a new status value
outside an `accepted_values` test); a model change that broke an
existing assumption.

**Diagnostics.**
```sql
SELECT test_name, model_name, message FROM fineract_ops.dbt_test_results
 WHERE layer = '<layer>' AND status IN ('fail','error')
   AND invocation_id = (
     SELECT invocation_id FROM fineract_ops.dbt_test_results
      ORDER BY executed_at DESC LIMIT 1);
```
Or locally:
```bash
cd transform/fineract_analytics
dbt test --select <model_name>
```

**Remedy.** If the failure traces to bad source data, fix at the source
(ingestion validation, or an upstream CDC issue per §4.7/§4.8) rather
than loosening the test — the test existing is what caught it. If the
failure is a genuine model bug, fix the model and re-run:
```bash
dbt run --select <model_name>+
dbt test --select <model_name>+
```

**Confirm recovery.** `fineract_ops.v_dbt_latest_run` for the layer
shows `failed = 0` on the next invocation; the Airflow `test` task group
goes green.

### 4.10 Full CDC re-snapshot via `cdc.debezium_signal`

Use this when reconciliation cannot otherwise close (§4.7), after
recovering from a slot invalidation, or when a materialized view was
fixed after quarantining messages (§4.8) and replay-from-Kafka is not
viable (retention expired, or the fix changes how historical rows should
have been shaped).

**Single-table incremental re-snapshot (no connector downtime):**
```bash
docker compose exec postgres psql -U postgres -d fineract_oltp -c \
  "INSERT INTO cdc.debezium_signal (id, type, data) VALUES
   ('$(uuidgen)', 'execute-snapshot',
    '{\"data-collections\": [\"oltp.loans\"], \"type\": \"incremental\"}')"
```
Multiple tables in one signal: `"data-collections": ["oltp.loans", "oltp.savings_accounts"]`.

**Full capture-set re-snapshot (all 8 tables):** the connector's
`snapshot.mode=initial` only auto-snapshots on first registration
against a fresh slot, so forcing a full re-snapshot means dropping and
recreating the slot deliberately:
```bash
# 1. stop the connector so it releases the slot
curl -X DELETE http://localhost:8083/connectors/fineract-oltp-source

# 2. drop the slot (irreversible - only do this if you intend a full re-snapshot)
docker compose exec postgres psql -U postgres -d fineract_oltp -c \
  "SELECT pg_drop_replication_slot('fineract_cdc_slot');"

# 3. re-register: a fresh slot triggers snapshot.mode=initial again
make cdc-register
```

**During either re-snapshot:** ClickHouse's `_op = 'r'` handling treats
snapshot reads the same as any other upsert — `_version` is still
`__source_ts_ms`, so a re-snapshot naturally overwrites older versions
and cannot regress data that is already newer in `fineract_raw.*`.
Expect a burst of `snapshot_reads` in `fineract_ops.v_cdc_freshness` for
the affected table(s) and elevated `active_parts` until the next merge
cycle.

**Confirm recovery.**
```sql
-- freshness/lag return to normal once the snapshot drains
SELECT * FROM fineract_ops.v_cdc_freshness WHERE source_table = 'loans';

-- reconciliation closes
SELECT * FROM fineract_ops.v_reconciliation_counts WHERE entity = 'loans';
```
And confirm `make cdc-status` shows the connector `RUNNING` with all
tasks `RUNNING` before considering the incident closed.

---

## 5. Platform-level alerts (component down / resource pressure)

These are the alerts in `platform_alerts.yml` — they gate every pipeline
alert above, since a dead component produces no other signal.

| Alert | First command |
|---|---|
| `PostgresDown` | `docker compose ps postgres postgres-exporter` |
| `ClickHouseDown` | `docker compose exec clickhouse clickhouse-client -q "SELECT 1"` |
| `KafkaConnectDown` | `make cdc-status`; if unreachable, `docker compose restart kafka-connect && make cdc-register` |
| `ClickHousePartsExplosion` | `SELECT * FROM system.merges WHERE table = '<table>'` — check whether merges are running at all, and whether upstream insert batch size dropped |
| `KafkaConsumerLag` | see §4.6 |
| `DiskPressure` | `du -sh /var/lib/{clickhouse,postgresql,kafka}/*` on the host — identify the largest consumer before deleting anything; a full replication slot (§4.3) is the most common root cause of Postgres WAL growth |
