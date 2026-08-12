# 0003. `argMax` versus `FINAL` for collapsing `ReplacingMergeTree`

## Status

Accepted.

## Context

`fineract_raw.*` tables use `ENGINE = ReplacingMergeTree(_version,
_is_deleted)` (see `platform/clickhouse/init/03_raw_tables.sql`). CDC
delivery is at-least-once and events can arrive out of order across
partitions, so a given natural key can be present in the table multiple
times — once per version received — until a background merge collapses
them. Merges are asynchronous and give no completion guarantee, so any
query against a raw table must assume it is reading a table that is not
yet collapsed, and must collapse it itself.

ClickHouse offers two ways to do this at query time:

1. **`FINAL`** — append `FINAL` to the table reference; ClickHouse
   performs a merge-on-read across all parts touched by the query
   before returning rows.
2. **`argMax` (or equivalent) with `GROUP BY`** — explicitly select
   `argMax(column, _version)` per key, grouped by the key, taking the
   value from the row with the highest version.

`transform/fineract_analytics/macros/cdc_helpers.sql` provides both as
macros — `current_rows()` (`FINAL`-based) and `latest_by_key()`
(`argMax`-based) — and the staging layer uses different ones for
different tables.

## Decision

Use `FINAL` for small, dimension-sized tables (`stg_fineract__clients`,
`stg_fineract__offices`, `stg_fineract__staff`, and the product
staging models), and use `argMax` via `latest_by_key()` for the
transaction fact (`stg_fineract__loan_transactions`, reading
`fineract_raw.loan_transactions`).

`current_rows()`:
```sql
select * from {{ relation }} final
where _is_deleted = 0
```

`latest_by_key()`:
```sql
select
    {{ key_columns }},
    argMax({{ column }}, _version) as {{ column }},   -- per value column
    argMax(_is_deleted, _version) as _is_deleted,
    argMax(_source_commit_at, _version) as _source_commit_at,
    max(_version) as _version
from {{ relation }}
{% if filter %}where {{ filter }}{% endif %}
group by {{ key_columns }}
```

## Consequences

**Positive.**
- `FINAL` on the small tables is correct at any merge state and reads
  as a single line at each call site — for tables in the tens-of-
  thousands-of-rows range at scale, the merge-on-read cost is trivially
  small, and the readability win (no `argMax` boilerplate repeated per
  column) is worth taking.
- `argMax` on `loan_transactions` is materially cheaper for a wide,
  partitioned, high-volume fact table: `FINAL` forces a merge-on-read
  over *all* columns of *all* parts the query touches and disables some
  read optimizations, whereas an explicit `argMax` only has to carry the
  columns actually projected.
- `argMax` composes correctly with an incremental filter; `FINAL` does
  not, safely. `latest_by_key()` accepts a `filter` argument (used by
  `incremental_cdc_filter()` — the standard "reprocess since the
  incremental model's high-water mark, minus a lookback window"
  predicate) applied *before* the `GROUP BY`/`argMax` collapse. Applying
  `FINAL` after an incremental `WHERE` clause, by contrast, can produce
  a partially-collapsed result: `FINAL` collapses within the rows the
  query happens to touch, and an incremental filter can exclude the
  very row that would have "won" the collapse for a given key, silently
  changing which version a key resolves to depending on the filter
  window. This class of bug is easy to introduce and hard to notice
  (it looks like correct output — just the wrong version of one row) —
  which is exactly why the fact table's staging model does not use
  `FINAL` at all, incremental or not.

**Negative / accepted trade-offs.**
- `argMax` requires listing every non-key column explicitly in
  `latest_by_key()`'s `value_columns` argument. A new column added to
  `oltp.loan_transactions` (and therefore to
  `fineract_raw.loan_transactions`) does not automatically appear in
  the collapsed result the way `select * ... final` would pick it up —
  someone has to add it to the staging model's call site. This is the
  same trade-off ADR 0001 accepts at the Kafka engine boundary: an
  explicit, reviewable list over implicit propagation.
- Two collapse strategies in one codebase is one more thing a new
  contributor has to learn and apply correctly — the macros exist
  specifically to make that one decision (`FINAL` vs `argMax`) instead
  of a decision to be relitigated per model. The doc comment in
  `cdc_helpers.sql` and this ADR are the record of *why* the split
  exists, so a future contributor extends the pattern instead of
  "fixing" it into one universal approach.
- `argMax`'s `GROUP BY` still has to scan every version of every key
  within the query's window (it does not skip already-merged
  duplicates any more than `FINAL` does) — it is cheaper than `FINAL`
  on a wide table, not free. Both strategies still ultimately depend on
  background merges eventually running to keep `active_parts` bounded;
  neither is a substitute for `ClickHousePartsExplosion` monitoring
  (see `docs/RUNBOOK.md` §5).

## Alternatives considered

- **`FINAL` everywhere, including the transaction fact.** Rejected on
  cost: `loan_transactions` is the highest-volume, most-frequently-
  queried table in the warehouse (partitioned by month, three Kafka
  consumers feeding it), and the merge-on-read + disabled-optimization
  cost of `FINAL` there would be paid on every dashboard query and every
  incremental dbt run.
- **`argMax` everywhere, including the dimension tables.** Rejected on
  readability, not cost: at dimension-table scale the performance
  difference from `FINAL` is immaterial, and writing out an `argMax`
  per column for `stg_fineract__clients`' two dozen columns would be
  pure boilerplate with no compensating benefit.
- **`OPTIMIZE TABLE ... FINAL` run on a schedule, then plain `SELECT *`
  everywhere.** Rejected as the primary strategy (it remains a
  documented manual escape hatch — see the `ClickHousePartsExplosion`
  runbook entry): forcing a full merge is an expensive, blocking
  operation that does not scale with table size, and it does not
  provide a correctness guarantee between runs — a query executed
  between two scheduled `OPTIMIZE` runs is exactly as uncollapsed as
  one under the `FINAL`/`argMax` regime, so it solves nothing that
  those two do not already solve, while adding an operational job that
  can itself fall behind.
