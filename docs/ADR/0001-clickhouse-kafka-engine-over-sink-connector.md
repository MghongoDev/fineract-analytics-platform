# 0001. ClickHouse Kafka engine over a Kafka Connect sink connector

## Status

Accepted.

## Context

Change events reach Kafka via the Debezium source connector
(`fineract-oltp-source`, running in Kafka Connect). Getting those events
from Kafka into ClickHouse's raw layer requires a consumer somewhere.
Two standard options exist:

1. **A sink connector** (`clickhouse-kafka-connect` or similar) running
   as a second connector inside the same Kafka Connect cluster used for
   the Debezium source.
2. **ClickHouse's native `Kafka` table engine** — a table that is itself
   a Kafka consumer, paired with a materialized view that reads from it
   and writes into the real (`ReplacingMergeTree`) target table.

This repository uses the second option (see
`platform/clickhouse/init/02_kafka_sources.sql` and
`04_materialized_views.sql`).

## Decision

Consume Kafka directly into ClickHouse using `ENGINE = Kafka` tables,
one per topic, with a materialized view per table converting wire types
to analytical types and routing the result into
`fineract_raw.<table>`. No sink connector is deployed.

## Consequences

**Positive.**
- One fewer JVM process to operate, monitor, and capacity-plan for.
  Kafka Connect already runs the Debezium *source* side; not also
  running a sink halves the number of Connect tasks that can go
  `FAILED` independently of each other.
- Inserts are native ClickHouse inserts, batched by ClickHouse's own
  flush settings (`kafka_flush_interval_ms`, `kafka_max_block_size`)
  rather than by a connector's `batch.size`/`linger.ms` tuned for a
  different system.
- Consumer health — offsets, lag, rebalances, exceptions — is visible
  directly in `system.kafka_consumers` (exposed here as
  `fineract_ops.v_kafka_consumers`), not buried in Kafka Connect task
  logs that require correlating a connector name back to a ClickHouse
  table by convention.
- The schema boundary is explicit: a Kafka engine table declares
  exactly the wire-format columns it expects
  (`input_format_skip_unknown_fields = 1` on top), and the cast to
  analytical types happens in one reviewable place — the materialized
  view — rather than in connector-specific transform configuration.

**Negative / accepted trade-offs.**
- The consumer's schema now lives in ClickHouse DDL instead of
  connector config. A new column added upstream by Fineract does not
  break ingestion (`input_format_skip_unknown_fields`), but it also
  does not appear anywhere until someone edits
  `02_kafka_sources.sql` and `04_materialized_views.sql` by hand — a
  sink connector with schema inference would pick it up automatically.
  This is treated as acceptable: an unreviewed schema change silently
  flowing into the warehouse is a worse failure mode than a visible gap
  that a migration closes.
- Recovering a stuck Kafka engine consumer means `DETACH`/`ATTACH` on
  the ClickHouse table (see `docs/RUNBOOK.md` §4.6) rather than a
  connector-level restart API call. This is a different operational
  muscle memory than the Debezium side, which does use the Connect
  REST API.
- Scaling consumption further means increasing `kafka_num_consumers`
  on a table (already 3 for `loan_transactions`) or partitioning the
  topic further, rather than scaling connector tasks — a different
  scaling knob to know about.

## Alternatives considered

- **`clickhouse-kafka-connect` sink connector.** Rejected primarily for
  the operational cost of a second connector class inside the same
  Connect cluster, and because back-pressure/lag on a sink connector is
  visible only through Connect's own metrics — a second dashboard and
  a second alerting surface, duplicating what `system.kafka_consumers`
  already gives for free on the ClickHouse side.
- **A custom consumer service** (e.g. a small Python/Go process reading
  Kafka and issuing ClickHouse inserts). Rejected as unnecessary
  complexity: it would reimplement batching, offset management, and
  error handling that both the Kafka engine and a sink connector
  already provide, for no capability this project needs.
- **Direct ClickHouse writes from Debezium** (bypassing Kafka
  entirely, e.g. a Debezium Engine embedded in a custom process).
  Rejected because it removes Kafka's role as a durable, replayable
  buffer — the 7-day topic retention is what lets ClickHouse be down
  for a long weekend without losing a single change event, and losing
  that safety margin to save one hop is not a good trade for a
  financial data pipeline.
