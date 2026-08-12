# CDC — PostgreSQL → Debezium → Kafka → ClickHouse

Near-real-time replication of the OLTP landing tables into the ClickHouse
raw layer. Typical end-to-end lag on the demo stack is **1–3 seconds**
from Postgres commit to a queryable row in ClickHouse.

```
oltp.* ──WAL──► pgoutput ──► Debezium ──► Kafka ──► ClickHouse Kafka engine
                                topic:                    │
                          fineract.oltp.<table>           ▼
                                                  materialized view
                                                          │
                                                          ▼
                                          fineract_raw.<table>  (ReplacingMergeTree)
```

## The five decisions that matter

**1. `pgoutput`, not `wal2json`.** Built into Postgres 10+, so there is no
extension to bake into the image and no plugin/server version skew.

**2. An explicitly declared publication.** `fineract_cdc_pub` is created in
[`04_cdc_publication.sql`](../platform/postgres/init/04_cdc_publication.sql)
and the connector runs with `publication.autocreate.mode=filtered`. Letting
Debezium create `FOR ALL TABLES` would require a superuser connector role
and would start capturing every future table — including `meta.*`, which
would feed pipeline telemetry back into the analytics stream.

**3. `decimal.handling.mode=string`.** Debezium's default (`precise`)
encodes `NUMERIC` as base64 bytes plus a scale. Every consumer then has to
decode it, and the usual "fix" is `double` — which quietly rounds money.
`string` preserves full precision and casts cleanly with
`toDecimal64OrNull` in ClickHouse.

**4. A heartbeat, with an action query.** If the capture set is idle, the
replication slot's confirmed LSN never advances and Postgres retains WAL
forever. `heartbeat.interval.ms=30000` plus a write to
`cdc.debezium_heartbeat` (which is itself in the publication) keeps the
slot moving on a quiet Sunday. This is the most common way a working CDC
pipeline takes down its source database.

**5. Deletes are rewritten, not dropped.**
`delete.handling.mode=rewrite` emits a `__deleted=true` row instead of a
bare tombstone, which is what lets
`ReplacingMergeTree(_version, _is_deleted)` reconcile a delete rather than
leave a ghost row that no query can see is stale.

## Version and ordering semantics

| Field | Source | Used for |
|---|---|---|
| `__source_ts_ms` | Postgres **commit** time | `_version` in ReplacingMergeTree; end-to-end lag numerator |
| `__ts_ms` | when Debezium **read** the record | connector-side lag |
| `__source_lsn` | WAL position | strict ordering tiebreak, replay position |
| `__op` | `c` / `u` / `d` / `r` (read = snapshot) | delete reconciliation, snapshot vs stream split |

Version is the **source commit time**, never the Debezium read time — if
the connector falls behind and catches up, read-time versioning would let
an older row overwrite a newer one.

Kafka topics are partitioned by primary key, so all changes to one loan
land on one partition and stay ordered. Ordering is guaranteed per key,
which is the only ordering the merge semantics actually need.

## Operating it

```bash
make cdc-register            # idempotent: PUT /connectors/<name>/config
make cdc-status              # connector + task states
make cdc-lag                 # replication slot lag in bytes + CH freshness
make cdc-restart             # restart failed tasks

# ad-hoc re-snapshot of one table without stopping the connector
docker compose exec postgres psql -U postgres -d fineract_oltp -c \
  "INSERT INTO cdc.debezium_signal (id, type, data) VALUES
   ('$(uuidgen)', 'execute-snapshot',
    '{\"data-collections\": [\"oltp.loans\"], \"type\": \"incremental\"}')"
```

## Failure modes and what happens

| Failure | Behaviour | Recovery |
|---|---|---|
| Connect restarts | Offsets are in `connect-offsets`; resumes from last committed LSN | automatic |
| ClickHouse down | Kafka retains 7 days; consumer group offset stops advancing | automatic on restart |
| Kafka down | Slot holds WAL, `max_slot_wal_keep_size=1GB` caps the damage | automatic; alert fires on slot lag |
| Slot invalidated (WAL exceeded) | Connector fails loudly | `snapshot.mode=initial` re-snapshots; alert `CDCSlotInactive` |
| Duplicate delivery after a crash | At-least-once by design | `ReplacingMergeTree` + `FINAL`/`argMax` in staging makes it idempotent |
