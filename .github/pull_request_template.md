<!--
Thanks for contributing to the Fineract analytics platform. This is a
financial-data pipeline: the checklist below exists because each of these
questions maps to a real failure mode we've hit before (a silent schema
drift, an untested mart, a backfill nobody ran). Answer every item -
"N/A" is a fine answer, "skipped" is not.
-->

## Summary

<!-- What does this PR change, and why? -->

## Type of change

- [ ] New feature / new model(s)
- [ ] Bug fix
- [ ] Refactor (no behaviour change)
- [ ] Infrastructure / CI-CD
- [ ] Documentation only

## Data-engineering checklist

### Schema
- [ ] This PR changes a table/column shape (OLTP, ClickHouse raw layer, or a dbt model's output columns)
  - [ ] The schema change is backward compatible, **or** consumers have been notified/updated in the same PR
  - [ ] `platform/postgres/init` and/or `platform/clickhouse/init` DDL updated to match
  - [ ] `scripts/validate_clickhouse_sql.py` and `scripts/validate_dbt_sql.py` pass locally

### Backfill
- [ ] This PR requires a backfill of historical data
  - [ ] Backfill procedure documented below (command, expected duration, blast radius)
  - [ ] Backfill is idempotent (safe to re-run without duplicating or corrupting data)
- [ ] No backfill needed - new code only affects data landing after deploy

### dbt tests & documentation
- [ ] New or changed models have a `description:` in their schema `.yml`
- [ ] New staging models have at least one `data_tests:` entry (this is enforced by the `dbt-checks` CI gate)
- [ ] New marts have `not_null`/`unique` tests on their grain and any FK `relationships` tests needed
- [ ] `dbt build` / `python scripts/validate_dbt_sql.py` passes locally

### Observability
- [ ] New pipeline stage or entity is covered by the pipeline exporter / Prometheus rules
- [ ] Grafana dashboard(s) updated if a new metric or SLA is introduced
- [ ] Alert thresholds reviewed (not just copy-pasted from an existing rule)
- [ ] No observability changes needed

### Contracts & breaking changes
- [ ] This PR is a breaking change to a mart's contract (renamed/removed column, changed grain, changed semantics of an existing metric)
  - [ ] Downstream consumers (dashboards, exports, other teams) identified and notified
  - [ ] Migration/deprecation window documented below
- [ ] Not a breaking change

### CDC / connectors
- [ ] This PR touches `cdc/debezium/*.json` or the Kafka source DDL
  - [ ] `connector-config-validation` CI job passes
  - [ ] Verified against a running Kafka Connect locally (`make cdc-status`)

## Backfill / migration notes

<!-- Command(s) to run, expected duration, who needs to run it, rollback plan. -->

## How was this tested?

<!-- Unit tests, `make validate`, integration run against the mock server, manual verification, etc. -->
