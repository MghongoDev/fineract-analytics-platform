# Ingestion service — Apache Fineract → PostgreSQL

Pulls the Apache Fineract v1 REST API into the `oltp` schema of the
landing database. Everything downstream (CDC → ClickHouse → dbt) reads
what this service writes.

## Design in one page

| Concern | Decision | Why |
|---|---|---|
| Shape of landed data | Mirrors the API resource, no business logic | Ingestion stays replayable; reshaping is dbt's job and dbt is versioned, tested and cheap to re-run |
| Idempotency | `INSERT … ON CONFLICT (natural key) DO UPDATE` | Re-running any batch converges; no dedupe step needed |
| Change detection | `_payload_hash`, update only when it differs | An unchanged row produces no WAL record, so the CDC stream carries only real change |
| Atomicity | Rows + rejects + expectations + watermark + run record in one transaction | The watermark can never claim progress the data does not have |
| Bad records | Quarantined in `meta.ingestion_reject`, run fails above a reject-ratio threshold | One bad loan must not fail 40k good ones, and must not vanish either |
| Batch quality gates | Declarative expectations per entity; `error` severity aborts before commit | Fail closed: bad financial data never reaches the analytics layer |
| Source protection | Client-side rate limit + jittered exponential backoff | Fineract is a live core-banking system; the crawler must never be the reason a teller waits |
| Metrics | Pushed to Pushgateway at end of run | Batch jobs exit before a scraper could reach them |

## Entity registry

Entities are declared once in [`fineract_ingest/entities.py`](fineract_ingest/entities.py);
the client, loader, validator, CLI, metrics and Airflow DAG are all generated from it.

```
offices · staff · loan_products · savings_products
clients · loans · savings_accounts · loan_transactions
```

`loan_transactions` is a *parent-driven* entity: Fineract exposes
transactions only under `/loans/{id}/transactions`, so the loader reads
loan ids back out of what it already landed and crawls per loan.

## Usage

```bash
python -m fineract_ingest health                       # API + Postgres reachability
python -m fineract_ingest list-entities                # the registry, as JSON
python -m fineract_ingest ingest --all                 # full pass
python -m fineract_ingest ingest --entities clients,loans
python -m fineract_ingest ingest --all --dry-run       # fetch + validate, roll back
python -m fineract_ingest ingest --all --parent-limit 50   # smoke run
python -m fineract_ingest status                       # watermarks and row counts
```

Exit codes: `0` success · `1` an entity failed · `2` config/connectivity error.

## Offline / CI mode

```bash
python -m fineract_ingest.mock_server --port 8090 --clients 400 --loans 700
export FINERACT_BASE_URL=http://localhost:8090/fineract-provider/api/v1
```

The mock serves the same resource shapes as Fineract — including the
`[yyyy, m, d]` date arrays and the `pageItems` envelope — from a seeded
generator, and can inject 503s (`--failure-rate 0.2`) to exercise the
retry path. CI runs the real ingestion code against it, offline and
deterministically.

## Configuration

See [`.env.example`](../.env.example) at the repository root. Everything is
environment driven; nothing is baked into the image.
