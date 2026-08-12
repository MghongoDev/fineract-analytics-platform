---
name: Data incident
about: Report a data quality, freshness, or correctness incident in the pipeline
title: "[INCIDENT] "
labels: ["data-incident"]
assignees: []
---

<!--
File this for anything a stakeholder would call "the numbers are wrong" or
"the dashboard is stale" - not for routine bugs. Fill in as much as you
know now; it is fine to update sections as the investigation progresses.
-->

## Summary

<!-- One or two sentences: what is wrong, and who noticed. -->

## Affected layer

<!-- Check every layer that is confirmed or suspected affected. -->

- [ ] Fineract source API
- [ ] Postgres OLTP landing (`ingestion/`)
- [ ] CDC / Debezium / Kafka (`cdc/`)
- [ ] ClickHouse raw layer (`platform/clickhouse/init`)
- [ ] dbt staging models
- [ ] dbt intermediate models
- [ ] dbt marts (core / finance)
- [ ] dbt ml features
- [ ] Observability / metrics / dashboards (the numbers are right, but the *reporting on them* is wrong)

## Detection source

<!-- Which alert, dashboard, or person surfaced this? Be specific -
     "Grafana" is not enough, name the panel/alert. -->

- **Alert / check name:**
- **Where it fired (Grafana panel, Prometheus rule, `scheduled-quality` GitHub issue, manual report, etc.):**
- **Link:**
- **First detected at (UTC):**

## Blast radius

<!-- Who/what is affected, and how much. -->

- **Entities/tables affected:**
- **Time range of affected data:**
- **Approximate row/record count affected:**
- **Downstream consumers affected (dashboards, exports, other teams, ML features):**
- **Is this customer-facing or reporting-only?**

## Suspected root cause

<!-- Best guess so far - a schema change, connector drift, a bad deploy, an upstream Fineract change, etc. -->

## Reconciliation status

- [ ] Not started
- [ ] In progress
- [ ] Reconciliation query/script identified: <!-- link or command -->
- [ ] Backfill/replay required
- [ ] Backfill/replay completed
- [ ] Verified: affected marts now match source of truth
- [ ] Stakeholders notified of resolution

## Timeline

| Time (UTC) | Event |
|---|---|
| | Detected |
| | Investigation started |
| | Root cause identified |
| | Fix/backfill deployed |
| | Verified resolved |

## Follow-up actions

<!-- New test, new alert, runbook update, etc. Link the PR(s) once opened. -->
