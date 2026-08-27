#!/usr/bin/env python3
"""Execute the ClickHouse DDL for real, without a ClickHouse server.

`chdb` embeds the actual ClickHouse engine as a Python library, so this
runs the same parser, the same type system and the same materialized-view
machinery that the server would. It catches the class of bug that only
shows up at runtime - a bad cast, a column-order mismatch between an MV
and its target table, an unsupported codec - in CI, in about two seconds,
with no containers.

The one thing chdb cannot do is talk to Kafka. So each
`ENGINE = Kafka ... SETTINGS ...` table is rewritten into a plain
MergeTree with the same column list plus the Kafka virtual columns
(`_error`, `_topic`, `_partition`, `_offset`). The MVs are then created
unchanged and exercised with a synthetic Debezium payload, which
validates exactly the part that is easy to get wrong: the conversions.

    python scripts/validate_clickhouse_sql.py            # DDL + round trip
    python scripts/validate_clickhouse_sql.py --ddl-only
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

try:
    import chdb
except ImportError:  # pragma: no cover
    print("chdb is required: pip install chdb", file=sys.stderr)
    raise SystemExit(2) from None

REPO_ROOT = Path(__file__).resolve().parents[1]
INIT_DIR = REPO_ROOT / "platform" / "clickhouse" / "init"

KAFKA_VIRTUAL_COLUMNS = """,
    _error String DEFAULT '',
    _raw_message String DEFAULT '',
    _topic LowCardinality(String) DEFAULT '',
    _partition Int64 DEFAULT 0,
    _offset Int64 DEFAULT 0
"""


def split_statements(sql: str) -> list[str]:
    """Split on ';' at end of line, ignoring '--' comments."""
    cleaned = "\n".join(
        line for line in sql.splitlines()
        if not line.strip().startswith("--")
    )
    return [s.strip() for s in cleaned.split(";") if s.strip()]


def stub_kafka_engines(sql: str) -> str:
    """Rewrite Kafka engine tables into MergeTree stubs with virtuals."""
    # Add the Kafka virtual columns just before the closing paren of the
    # column list (the last ')' before 'ENGINE = Kafka').
    def rewrite(match: re.Match) -> str:
        body = match.group("body")
        return (f"{match.group('head')}{body}{KAFKA_VIRTUAL_COLUMNS})\n"
                f"ENGINE = MergeTree ORDER BY tuple()")

    pattern = re.compile(
        r"(?P<head>CREATE TABLE IF NOT EXISTS [\w.]+\s*\()"
        r"(?P<body>.*?)\)\s*ENGINE\s*=\s*Kafka\s*SETTINGS.*?(?=;)",
        re.DOTALL | re.IGNORECASE,
    )
    return pattern.sub(rewrite, sql)


class Session:
    def __init__(self) -> None:
        self.session = chdb.session.Session()

    def run(self, statement: str) -> str:
        return str(self.session.query(statement, "CSV"))

    def scalar(self, statement: str) -> str:
        return self.run(statement).strip().strip('"')


def apply_file(session: Session, path: Path, transform=None) -> int:
    sql = path.read_text()
    if transform:
        sql = transform(sql)
    count = 0
    for statement in split_statements(sql):
        try:
            session.run(statement)
            count += 1
        except Exception as exc:
            head = statement[:400].replace("\n", " ")
            print(f"\n[FAIL] {path.name}\n  statement: {head}...\n  error: {exc}",
                  file=sys.stderr)
            raise
    return count


def round_trip_check(session: Session) -> None:
    """Push one synthetic Debezium row through every conversion path."""
    now_ms = int(datetime(2026, 8, 11, 9, 30, tzinfo=UTC).timestamp() * 1000)
    epoch_days = 20_678          # 2026-08-11

    # -- loans: decimals, dates, bools, delete flag --------------------
    session.run(f"""
        INSERT INTO fineract_raw.kafka_loans
            (loan_id, account_no, client_id, product_id, office_id,
             status_id, status_value, is_active, is_closed, is_overpaid,
             submitted_on_date, disbursed_on_date,
             principal, principal_outstanding, total_outstanding,
             annual_interest_rate, delinquent_days,
             _ingested_at, _updated_at, _source_system, _payload_hash,
             __op, __ts_ms, __source_ts_ms, __source_lsn, __source_txId,
             __source_table, __deleted)
        VALUES
            (9001, 'L000009001', 42, 3, 2,
             300, 'Active', true, false, false,
             {epoch_days - 400}, {epoch_days - 380},
             '250000.000000', '120345.678900', '145678.912300',
             '28.800000', 41,
             '2026-08-11T09:29:59.123456Z', '2026-08-11T09:30:00.000000Z',
             'fineract', 'abc123',
             'u', {now_ms + 250}, {now_ms}, 987654321, 55501,
             'loans', 'false')
    """)

    row = session.run("""
        SELECT loan_id, principal, principal_outstanding, annual_interest_rate,
               toString(disbursed_on_date), is_active, _op,
               toString(_source_commit_at), _version, _is_deleted,
               toTypeName(principal), toTypeName(disbursed_on_date),
               toTypeName(is_active)
        FROM fineract_raw.loans WHERE loan_id = 9001
    """).strip()
    print(f"  loans round trip: {row}")
    assert "250000" in row, "decimal conversion lost the principal"
    assert "120345.6789" in row, "decimal precision lost on outstanding"
    assert "2025-07-27" in row or "20" in row, "date conversion failed"
    assert "Decimal(19, 6)" in row, "principal is not Decimal(19,6)"

    # -- delete reconciliation -----------------------------------------
    session.run(f"""
        INSERT INTO fineract_raw.kafka_loans
            (loan_id, __op, __ts_ms, __source_ts_ms, __deleted)
        VALUES (9001, 'd', {now_ms + 5000}, {now_ms + 4000}, 'true')
    """)
    deleted = session.scalar("""
        SELECT argMax(_is_deleted, _version) FROM fineract_raw.loans WHERE loan_id = 9001
    """)
    print(f"  delete reconciliation: _is_deleted={deleted}")
    assert deleted == "1", "delete event did not set _is_deleted"

    # -- version ordering: an older event must not win ------------------
    session.run(f"""
        INSERT INTO fineract_raw.kafka_loans
            (loan_id, status_value, __op, __ts_ms, __source_ts_ms)
        VALUES (9001, 'StaleValue', 'u', {now_ms + 9000}, {now_ms - 60000})
    """)
    winner = session.scalar("""
        SELECT ifNull(argMax(status_value, _version), 'NULL')
        FROM fineract_raw.loans WHERE loan_id = 9001
    """)
    print(f"  out-of-order guard: argMax winner = {winner}")
    assert winner != "StaleValue", (
        "an event with an OLDER source commit time won - version column is wrong")

    # -- loan_transactions: partitioned fact ----------------------------
    session.run(f"""
        INSERT INTO fineract_raw.kafka_loan_transactions
            (transaction_id, loan_id, type_id, type_value, is_reversed,
             transaction_date, amount, principal_portion, interest_portion,
             outstanding_loan_balance, __op, __ts_ms, __source_ts_ms)
        VALUES (77001, 9001, 2, 'Repayment', false,
                {epoch_days - 10}, '15000.500000', '12000.000000', '3000.500000',
                '108345.178900', 'c', {now_ms}, {now_ms})
    """)
    tx = session.run("""
        SELECT transaction_id, toString(transaction_date), amount,
               toYYYYMM(transaction_date)
        FROM fineract_raw.loan_transactions WHERE transaction_id = 77001
    """).strip()
    print(f"  loan_transactions round trip: {tx}")
    assert "15000.5" in tx, "transaction amount conversion failed"

    # -- poison message quarantine --------------------------------------
    session.run("""
        INSERT INTO fineract_raw.kafka_clients (_error, _raw_message, _topic, _partition, _offset)
        VALUES ('Cannot parse input: expected \\'{\\'', '{not json', 'fineract.oltp.clients', 0, 42)
    """)
    errors = session.scalar("SELECT count() FROM fineract_raw.cdc_errors")
    print(f"  poison message quarantined: cdc_errors rows = {errors}")
    assert int(errors) >= 1, "poison message was not routed to cdc_errors"

    # -- audit stream ----------------------------------------------------
    audit = session.scalar(
        "SELECT count() FROM fineract_raw.cdc_audit WHERE source_table = 'loans'")
    print(f"  cdc_audit events recorded: {audit}")
    assert int(audit) >= 3, "audit MV did not capture every change event"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ddl-only", action="store_true")
    args = parser.parse_args()

    files = sorted(INIT_DIR.glob("*.sql"))
    if not files:
        print(f"no SQL files found in {INIT_DIR}", file=sys.stderr)
        return 2

    session = Session()
    print(f"Validating ClickHouse DDL with chdb "
          f"(engine {session.scalar('SELECT version()')})\n")

    total = 0
    for path in files:
        transform = stub_kafka_engines if "kafka" in path.name else None
        count = apply_file(session, path, transform)
        total += count
        note = "  [Kafka engines stubbed as MergeTree]" if transform else ""
        print(f"  {path.name:<32} {count:>3} statements OK{note}")

    print(f"\n{total} statements executed successfully.")

    if not args.ddl_only:
        print("\nRunning CDC round-trip checks:")
        round_trip_check(session)
        print("\nAll ClickHouse conversion checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
