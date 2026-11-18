# 0004. Disable Debezium publication autocreate

## Status

Accepted.

## Context

`04_cdc_publication.sql` creates `fineract_cdc_pub` as an explicit list of
the eight `oltp.*` tables plus the heartbeat table. The connector connects
as `debezium`, a role with `REPLICATION` and `SELECT` rights. The role does
not own the publication. Postgres created the publication under `postgres`
because the init scripts run as the superuser.

With `publication.autocreate.mode=filtered`, Debezium reconciles the
publication on startup. Its desired set is the `table.include.list` plus
`cdc.debezium_signal`. The live publication also contains
`cdc.debezium_heartbeat`, so Debezium issues an `ALTER PUBLICATION` to make
the two sets match. Postgres rejects the statement:

```
ERROR: must be owner of publication fineract_cdc_pub
```

The task then enters an unlimited retry loop. It still reports `RUNNING`,
because the retry backoff happens inside a running task. The connector
status check in `cdc/scripts/register-connectors.sh` reads only that state,
so `make cdc-register` reported success while no change event reached Kafka.
The replication slot stayed inactive and ClickHouse received zero rows.

## Decision

Set `publication.autocreate.mode=disabled` in
`cdc/debezium/postgres-source-connector.json`. Keep the publication defined
only in `04_cdc_publication.sql`. The CI connector-config gate asserts this
value so the fault cannot return through a config edit.

## Alternatives considered

- **Make `debezium` the publication owner.** This lets `filtered` reconcile
  tables, but the runtime table list then drifts from the SQL in git. The
  database stops being the reviewable source of truth for the capture set.
- **Match the publication to Debezium's desired set** (add the signal table,
  drop the heartbeat table). This avoids the error only while the two lists
  stay equal. A future edit to `table.include.list` reintroduces the fault.
- **Grant `debezium` superuser.** Rejected. A source reader needs
  `REPLICATION`, not full database control.

## Consequences

- The connector starts streaming on a fresh stack. Verified on the
  running compose stack: the slot became active and ClickHouse live-key
  counts matched Postgres for all eight captured tables.
- Adding a table to the capture set now needs two edits: the publication
  SQL and `table.include.list`. The CI gate catches a config that drifts
  from the required mode, but it does not compare the two lists.
- The heartbeat table stays in the publication, so the periodic write from
  `platform_maintenance` keeps the slot LSN moving when the source is idle.
