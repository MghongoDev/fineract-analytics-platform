# Fineract Analytics Platform

An end-to-end, CDC-driven analytics engineering pipeline for a
microfinance institution: **Apache Fineract REST API → PostgreSQL →
Debezium/Kafka → ClickHouse → dbt → analytics-ready marts and ML
features**, orchestrated by Airflow, monitored with Prometheus and
Grafana, and packaged so the whole thing starts with one command.

```
Fineract API ──► ingestion ──► PostgreSQL ──WAL──► Debezium ──► Kafka ──► ClickHouse
                                   │                                          │
                                   └── meta control plane          Kafka engine + MVs
                                                                              │
   Airflow ──────────────── orchestrates ─────────────►  dbt: staging → intermediate → marts / ml
                                                                              │
   Prometheus + Grafana ◄── metrics from every stage ──►  BI · ML training · dashboards
```

- **Design rationale:** [`docs/DESIGN_REPORT.md`](docs/DESIGN_REPORT.md)
- **Operating it:** [`docs/RUNBOOK.md`](docs/RUNBOOK.md)
- **Diagrams:** [`docs/diagrams/`](docs/diagrams/) — architecture, ERD, CDC sequence
- **Decision records:** [`docs/ADR/`](docs/ADR/)

---

## Quick start

**Prerequisites:** Docker 24+ with Compose v2, GNU Make, and about 8 GB of
RAM free. Nothing else — every runtime is containerised.

```bash
git clone <this-repo> && cd fineract-analytics-platform
cp .env.example .env

make up          # build, start everything, register the CDC connector, wait for health
make urls        # every UI, with credentials
```

`make up` is the single command the brief asks for. It:

1. builds the ingestion, Airflow and exporter images
2. starts PostgreSQL with logical replication enabled and applies the
   OLTP schema, control plane and CDC publication
3. starts Kafka (KRaft), Kafka Connect, and ClickHouse (which applies its
   own DDL: Kafka engine tables, raw tables, materialized views, ops views)
4. registers the Debezium connector idempotently
5. starts Airflow (initialised, unpaused), Prometheus, Grafana, the
   Pushgateway and the exporters
6. waits for every healthcheck before returning

Then trigger a run and watch the data move:

```bash
make airflow-trigger      # run the pipeline now instead of waiting for the schedule
make cdc-lag              # replication slot lag and ClickHouse freshness
make metrics              # the pipeline exporter's current view
```

### Choosing a data source

The source is selected by Compose profile, not by editing files.

| Mode | Command | Notes |
|---|---|---|
| **Self-hosted Fineract** | `docker compose --profile fineract up -d` | Full fidelity. First boot takes ~3 minutes while Fineract migrates its schema. |
| **Offline mock** (recommended for review) | `docker compose --profile mock up -d` then set `FINERACT_BASE_URL=http://fineract-mock:8090/fineract-provider/api/v1` | Deterministic, no internet, same resource shapes. |
| **Public Apache demo** | set `FINERACT_BASE_URL=https://demo.mifos.io/fineract-provider/api/v1` | Credentials `mifos` / `password`, tenant `default`. Shared and mutable — fine for a look, not for tests. |

```bash
make up-core                        # platform only, no source, no observability
docker compose --profile tools up -d   # adds Kafka UI and an interactive dbt shell
make down                           # stop
make clean                          # stop and delete all volumes
```

---

## What you get

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8085 | `admin` / `admin` |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | — |
| ClickHouse HTTP | http://localhost:8123 | `analytics` / `analytics` |
| Kafka Connect | http://localhost:8083 | — |
| Pipeline exporter | http://localhost:9105/metrics | — |
| Pushgateway | http://localhost:9091 | — |
| Kafka UI (`tools` profile) | http://localhost:8091 | — |
| Fineract (`fineract` profile) | https://localhost:8443/fineract-provider | `mifos` / `password`, tenant `default` |
| PostgreSQL | `localhost:5432` | `postgres` / `postgres`, db `fineract_oltp` |

`make urls` prints this list from the running stack.

---

## Verifying that data moved through each stage

Each command below answers exactly one question. Run them in order after
a pipeline run.

**1. Did the API reach PostgreSQL?**

```bash
make psql
```
```sql
SELECT entity, status, rows_inserted, rows_updated, rows_unchanged,
       rows_rejected, round(duration_seconds::numeric, 1) AS seconds
FROM   meta.v_latest_ingestion_run ORDER BY entity;

SELECT count(*) FROM oltp.clients;
SELECT count(*) FROM oltp.loans;
SELECT count(*) FROM oltp.loan_transactions;
```

A healthy **second** run shows `rows_inserted = 0`, `rows_updated = 0`
and a large `rows_unchanged`: unchanged records are detected by payload
hash and produce no write, therefore no WAL, therefore no CDC event.

**2. Is CDC streaming?**

```bash
make cdc-status     # connector and task states — both must be RUNNING
make cdc-lag        # slot lag in bytes; should be small and not growing
```

**3. Did the changes reach ClickHouse?**

```bash
make clickhouse-client
```
```sql
SELECT * FROM fineract_ops.v_raw_table_health ORDER BY table_name;
SELECT * FROM fineract_ops.v_cdc_freshness;
SELECT * FROM fineract_ops.v_reconciliation_counts;   -- must match Postgres
SELECT count() FROM fineract_raw.cdc_errors;          -- must be 0
```

**4. Did dbt build the warehouse?**

```sql
SELECT database, name, total_rows
FROM   system.tables
WHERE  database LIKE 'fineract_%' AND engine != 'MaterializedView'
ORDER  BY database, name;

SELECT * FROM fineract_ops.v_dbt_latest_run;
```

**5. Is the output usable?**

```sql
-- Portfolio at Risk by branch
SELECT office_name, active_loans, round(par_ratio * 100, 2) AS par_pct
FROM   fineract_marts.dim_office ORDER BY par_pct DESC;

-- The ML training set, mature labels only
SELECT split_hint, count() AS rows, avg(label_defaulted) AS default_rate
FROM   fineract_ml.ml_loan_default_features
WHERE  is_label_mature = 1 GROUP BY split_hint;
```

**6. Is it observable?** Open Grafana → *Pipeline Health*, *CDC and Data
Quality*, *Portfolio Overview*.

---

## Running the pipeline

```bash
make airflow-trigger                # trigger fineract_analytics_pipeline
make ingest                         # ingestion only, outside Airflow
make dbt-run                        # dbt run
make dbt-test                       # dbt test
make dbt-docs                       # generate and serve dbt docs
```

The three DAGs:

| DAG | Schedule | Purpose |
|---|---|---|
| `fineract_analytics_pipeline` | every 4 hours | preflight → ingest → **CDC gate** → dbt by layer → test → publish → quality gate |
| `platform_maintenance` | hourly | CDC heartbeat, replication-slot lag, ClickHouse merges, retention |
| `fineract_backfill` | manual | re-ingestion and full rebuild, behind a typed confirmation |

The CDC gate is the important one: it blocks transformation until
ClickHouse row counts match PostgreSQL **and** CDC freshness is under
five minutes. Without it, dbt would happily transform a warehouse that
has not received the rows yet, succeed, and publish stale numbers.

---

## Tests

```bash
make test               # 105 unit + integration tests
make validate           # execute all ClickHouse DDL and build the whole dbt DAG
make lint               # ruff, yamllint, shellcheck, sqlfluff
```

`make validate` is worth knowing about: it runs
`scripts/validate_clickhouse_sql.py` and `scripts/validate_dbt_sql.py`,
which execute the real ClickHouse DDL and build **all 22 dbt models** on
an *embedded* ClickHouse engine, then run every data test — in about two
seconds, with no containers. `dbt parse` only proves the Jinja renders;
this proves the SQL is valid ClickHouse and the models actually produce
rows.

---

## CI/CD

GitHub Actions, triggered on push and pull request:

`lint` → `unit-tests` → `sql-validation` → `dbt-checks` →
`integration-tests` · `connector-config-validation` · `compose-validation`
→ `build-images` → `ci-summary`

What each gate protects is described in
[`docs/DESIGN_REPORT.md` §7](docs/DESIGN_REPORT.md#7-cicd). Notably,
`connector-config-validation` asserts the Debezium settings that fail
*silently at runtime* if they regress, and `dbt-checks` fails the build
if a model is undocumented or a staging model has no test.

Deployment (`cd.yml`) is tag-triggered, gated by a GitHub Environment
approval, uses the dbt slim-CI pattern, and runs a post-deploy smoke
check asserting data freshness before declaring success.

---

## Layout

```
├── docker-compose.yml            17 services, 4 profiles
├── Makefile                      28 targets; run `make help`
├── .env.example                  every configuration knob, documented
├── ingestion/                    Fineract REST → PostgreSQL
│   └── fineract_ingest/          client · entities · validation · loader · metrics · mock server
├── platform/
│   ├── postgres/init/            OLTP schema, control plane, CDC publication
│   ├── postgres/conf/            tuned for logical replication
│   ├── clickhouse/init/          Kafka sources, raw tables, MVs, ops views
│   └── kafka/                    JMX → Prometheus for Debezium
├── cdc/                          connector config + idempotent registration
├── transform/fineract_analytics/ dbt project: 22 models, tests, macros, seeds
├── orchestration/                3 DAGs, custom hooks/sensors/operators, integrity tests
├── observability/                Prometheus rules, Grafana dashboards, custom exporter
├── scripts/                      embedded-engine SQL validators
├── tests/                        unit and integration suites
└── docs/                         design report, runbook, ADRs, diagrams
```

---

## Configuration

Everything is environment-driven; see [`.env.example`](.env.example),
which documents each variable inline. The settings most likely to need
changing:

| Variable | Default | Why you would change it |
|---|---|---|
| `FINERACT_BASE_URL` | self-hosted | Point at the demo, the mock, or your own instance |
| `FINERACT_RPS` | `8` | Client-side rate limit. Lower it against a busy production core banking system. |
| `INGEST_MAX_REJECT_RATIO` | `0.05` | How many quarantined records before a load is treated as a failure |
| `DBT_THREADS` | `4` | dbt parallelism |
| `cdc_lookback_hours` (dbt var) | `48` | How far back incremental models reprocess to absorb late CDC arrivals |

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `make up` hangs on Fineract | First boot runs schema migrations | Wait ~3 min, or use `--profile mock` |
| Connector not RUNNING | Connect started before Postgres was ready | `make cdc-restart` |
| ClickHouse tables empty | Connector never registered | `make cdc-register`, then `make cdc-status` |
| Replication slot lag growing | ClickHouse or Connect is down | [`RUNBOOK.md` §Incident 1](docs/RUNBOOK.md) |
| dbt cannot connect | ClickHouse still starting | `docker compose ps clickhouse` |
| Port already in use | Something else on 8085/3000/5432 | Override the port in `.env` |

Full alert-by-alert procedures are in [`docs/RUNBOOK.md`](docs/RUNBOOK.md).

---

## Data source attribution

Data comes from the **Apache Fineract** v1 REST API
(<https://fineract.apache.org>, Apache-2.0). Authentication is HTTP Basic
plus the mandatory `Fineract-Platform-TenantId` header; the public demo
instance is `https://demo.mifos.io/fineract-provider/api/v1` with
`mifos` / `password` and tenant `default`. API reference:
<https://demo.mifos.io/api-docs/apiLive.htm>.
