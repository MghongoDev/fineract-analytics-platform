# 0002. `_payload_hash` change detection in the ingestion upsert

## Status

Accepted.

## Context

The ingestion service (`ingestion/fineract_ingest/loader.py`) polls the
Fineract REST API on a schedule and upserts what it reads into
`oltp.*`. Fineract has no incremental/delta endpoint — every poll of an
entity returns its full current representation, whether or not
anything about that record actually changed since the last poll.

A naive `INSERT ... ON CONFLICT (pk) DO UPDATE` therefore issues an
`UPDATE` for every row on every run, regardless of whether the row's
data changed. In Postgres, every `UPDATE` — even one that sets every
column to the value it already had — writes a new row version and a
WAL record. Because `oltp.*` is the table set captured by
`fineract_cdc_pub` (see `04_cdc_publication.sql`), every one of those
no-op WAL records becomes a CDC event: a message on a Kafka topic, a
row through a ClickHouse materialized view, and a version in a
`ReplacingMergeTree` table that downstream readers must still collapse.

On a steady-state loan book, most polled rows have not changed since
the last poll. Without change detection, "everything, every run"
becomes the CDC stream's baseline volume, dwarfing the events that
represent real business activity.

## Decision

Every landed table carries a `_payload_hash` column. The loader
computes a hash of the row's business payload before upserting, and the
upsert statement is:

```sql
INSERT INTO {schema}.{table} (...)
VALUES (...)
ON CONFLICT ({pk}) DO UPDATE SET ...
WHERE {schema}.{table}._payload_hash IS DISTINCT FROM EXCLUDED._payload_hash
RETURNING (xmax = 0) AS was_inserted
```

The `WHERE` clause on the `DO UPDATE` means Postgres only writes a new
row version — and therefore only emits a WAL record — when the payload
actually differs from what is already stored. `RETURNING (xmax = 0)`
lets the loader distinguish insert / update / unchanged in one
round-trip without a separate `SELECT`, which is what makes
`LoadResult` (`rows_inserted` / `rows_updated` / `rows_unchanged`)
possible without extra queries.

## Consequences

**Positive.**
- The CDC stream carries only real change. "Everything, every run"
  becomes "only what actually moved" — this is the difference between
  Kafka topic volume tracking business activity versus tracking polling
  frequency, and it is what keeps the ClickHouse Kafka engine
  consumers, the materialized views, and `ReplacingMergeTree` merge
  pressure all proportional to real change rather than to how often the
  scheduler happens to run.
- `IS DISTINCT FROM` (not `!=`) correctly treats `NULL` payload-hash
  states as distinct from any concrete value, which matters on the very
  first load of a row (no prior hash to compare against — the `INSERT`
  branch handles that case, not this `WHERE` clause, but the predicate
  still needs to be NULL-safe for any future column that can be NULL).
- The `xmax = 0` trick means the insert/update split is known without
  a pre-read — important because a pre-read-then-write pattern would
  reintroduce a race between concurrent loader runs that upsert-with-
  `RETURNING` avoids by construction.

**Negative / accepted trade-offs.**
- The hash is computed application-side (Python, before the row is
  sent to Postgres), not database-side. This means the hash's stability
  depends on the loader always constructing the hashed payload the same
  way — a change to field ordering or serialization in the ingestion
  code that is not also a genuine data change would still register as
  "changed" and produce a WAL record. This is a correctness
  responsibility that lives in code, not in the schema.
- A hash collision would cause a genuine change to be silently treated
  as unchanged. This is accepted because the hash space is large enough
  that this project treats the probability as negligible relative to
  the other failure modes it defends against, and because the
  consequence of a missed update — one stale field until the next
  differing poll — is far less severe than a lost transaction would be.
- This only suppresses churn at the *ingestion* layer. It does nothing
  for genuine no-op writes originating anywhere else (there are none in
  this architecture, since Fineract itself is the only writer to
  `oltp.*`), and it does not replace the separate `REPLICA IDENTITY`
  decision (dimension tables `FULL`, `loan_transactions` `DEFAULT`) that
  controls how much of a *changed* row Debezium includes in its
  `before` image.

## Alternatives considered

- **Compare the full row client-side before deciding whether to
  upsert at all** (read-then-write). Rejected: it reintroduces a
  read-modify-write race between concurrent or overlapping loader runs
  that a single `INSERT ... ON CONFLICT` statement avoids atomically,
  and it costs a read per row for no benefit over letting Postgres make
  the same comparison inside the `DO UPDATE` predicate.
- **Compare individual columns instead of a single hash** (e.g. an
  `OR`-chain of `IS DISTINCT FROM` across every business column).
  Rejected as unnecessary complexity: it would need to be kept in sync
  with every schema change to every one of the eight landed tables, for
  no behavioral difference from one hash covering the same columns —
  the hash is exactly this comparison, computed once and stored, rather
  than recomputed as an increasingly large `WHERE` clause.
- **Let every poll write through, and de-duplicate downstream instead**
  (e.g. only in the ClickHouse `ReplacingMergeTree` collapse). Rejected:
  `ReplacingMergeTree` already has to absorb genuine CDC noise (retries,
  out-of-order delivery); asking it to also absorb 100% of the
  ingestion polling volume as a matter of course would multiply merge
  pressure and Kafka topic volume for information the pipeline can
  cheaply avoid producing in the first place.
