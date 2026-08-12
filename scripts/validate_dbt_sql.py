#!/usr/bin/env python3
"""Build the entire dbt DAG for real, against an embedded ClickHouse.

`dbt parse` only checks that Jinja renders. `dbt compile` needs a live
warehouse. Neither tells you whether the *SQL* is valid ClickHouse until
something is running - which in practice means a broken model is found by
the nightly run rather than by the pull request.

This script closes that gap:

  1. creates the real raw-layer tables from platform/clickhouse/init
  2. seeds them with a synthetic but realistically-shaped Fineract dataset
  3. renders every model with Jinja - project macros are loaded from
     macros/*.sql, so `money()`, `par_bucket()`, `surrogate_key()` etc.
     are the real implementations, not stubs
  4. topologically sorts on ref() and executes each model as
     CREATE TABLE ... AS <rendered sql> on the embedded engine
  5. runs the project's schema tests (not_null / unique / accepted_values
     / relationships) and the singular tests in tests/

Everything runs in-process in a couple of seconds, so it belongs in CI on
every push - and it catches the class of bug (bad cast, wrong column
name, aggregate in the wrong place) that a parse cannot see.

    python scripts/validate_dbt_sql.py
    python scripts/validate_dbt_sql.py --layer staging --verbose
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

try:
    import chdb
    import yaml
    from jinja2 import Environment, StrictUndefined
except ImportError:  # pragma: no cover
    print("requires: pip install chdb jinja2 pyyaml", file=sys.stderr)
    raise SystemExit(2)

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = REPO_ROOT / "transform" / "fineract_analytics"
MODELS_DIR = DBT_ROOT / "models"
MACROS_DIR = DBT_ROOT / "macros"
TESTS_DIR = DBT_ROOT / "tests"
SEEDS_DIR = DBT_ROOT / "seeds"
CH_INIT = REPO_ROOT / "platform" / "clickhouse" / "init"

LAYER_DATABASE = {
    "staging": "fineract_staging",
    "intermediate": "fineract_intermediate",
    "marts": "fineract_marts",
    "ml": "fineract_ml",
}


# ---------------------------------------------------------------------
# Jinja context that behaves enough like dbt
# ---------------------------------------------------------------------
@dataclass
class Model:
    name: str
    path: Path
    layer: str
    raw_sql: str
    refs: set[str] = field(default_factory=set)
    sources: set[str] = field(default_factory=set)
    config: dict[str, Any] = field(default_factory=dict)

    @property
    def relation(self) -> str:
        return f"{LAYER_DATABASE[self.layer]}.{self.name}"


class DbtJinja:
    def __init__(self, project_vars: dict[str, Any]):
        self.env = Environment(undefined=StrictUndefined, keep_trailing_newline=True)
        self.project_vars = project_vars
        self.current: Model | None = None
        self.models: dict[str, Model] = {}
        self._install_globals()
        self._load_project_macros()

    # -- dbt built-ins -------------------------------------------------
    def _ref(self, *args: str) -> str:
        name = args[-1]
        if self.current is not None:
            self.current.refs.add(name)
        target = self.models.get(name)
        return target.relation if target else f"UNKNOWN_REF_{name}"

    def _source(self, source_name: str, table_name: str) -> str:
        if self.current is not None:
            self.current.sources.add(f"{source_name}.{table_name}")
        return f"{source_name}.{table_name}"

    def _config(self, **kwargs: Any) -> str:
        if self.current is not None:
            self.current.config.update(kwargs)
        return ""

    def _var(self, name: str, default: Any = None) -> Any:
        return self.project_vars.get(name, default)

    def _this(self) -> str:
        return self.current.relation if self.current else "this"

    def _install_globals(self) -> None:
        self.env.globals.update({
            "ref": self._ref,
            "source": self._source,
            "config": self._config,
            "var": self._var,
            "is_incremental": lambda: False,   # validate the full-refresh path
            "log": lambda *a, **k: "",
            "target": {"name": "ci", "schema": "fineract", "type": "clickhouse"},
            "run_started_at": "2026-08-11 00:00:00",
            "invocation_id": "validate-dbt-sql",
        })
        # `this` must behave as a value, not a callable, in templates.
        self.env.globals["this"] = _LazyThis(self)

    def _load_project_macros(self) -> None:
        """Load macros/*.sql so models use the real implementations."""
        source = "\n".join(p.read_text() for p in sorted(MACROS_DIR.glob("*.sql")))
        module = self.env.from_string(source).module
        for attribute in dir(module):
            if attribute.startswith("_"):
                continue
            value = getattr(module, attribute)
            if callable(value):
                self.env.globals[attribute] = value

    def render(self, model: Model) -> str:
        self.current = model
        try:
            return self.env.from_string(model.raw_sql).render()
        finally:
            self.current = None


class _LazyThis:
    def __init__(self, jinja: DbtJinja):
        self._jinja = jinja

    def __str__(self) -> str:
        return self._jinja._this()

    def __getattr__(self, item: str) -> str:
        return self._jinja._this()


# ---------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------
class Engine:
    def __init__(self) -> None:
        self.session = chdb.session.Session()

    def run(self, statement: str) -> str:
        return str(self.session.query(statement, "CSV"))

    def scalar(self, statement: str) -> str:
        return self.run(statement).strip().strip('"')


def split_statements(sql: str) -> list[str]:
    cleaned = "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in cleaned.split(";") if s.strip()]


def load_project_vars() -> dict[str, Any]:
    project = yaml.safe_load((DBT_ROOT / "dbt_project.yml").read_text())
    return project.get("vars", {}) or {}


def discover_models() -> dict[str, Model]:
    models: dict[str, Model] = {}
    for path in sorted(MODELS_DIR.rglob("*.sql")):
        layer = path.relative_to(MODELS_DIR).parts[0]
        if layer not in LAYER_DATABASE:
            continue
        models[path.stem] = Model(name=path.stem, path=path, layer=layer,
                                  raw_sql=path.read_text())
    return models


def topological_order(models: dict[str, Model]) -> list[Model]:
    ordered: list[Model] = []
    seen: set[str] = set()
    visiting: set[str] = set()

    def visit(name: str) -> None:
        if name in seen or name not in models:
            return
        if name in visiting:
            raise ValueError(f"circular ref detected at '{name}'")
        visiting.add(name)
        for dependency in sorted(models[name].refs):
            visit(dependency)
        visiting.discard(name)
        seen.add(name)
        ordered.append(models[name])

    for name in sorted(models):
        visit(name)
    return ordered


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------
def create_raw_layer(engine: Engine) -> None:
    """Create the real raw tables (Kafka engines stubbed out)."""
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from validate_clickhouse_sql import stub_kafka_engines  # noqa: E402

    for path in sorted(CH_INIT.glob("*.sql")):
        sql = path.read_text()
        if "kafka" in path.name:
            sql = stub_kafka_engines(sql)
        for statement in split_statements(sql):
            engine.run(statement)

    # dbt sources resolve to `fineract_raw.<table>`; the init scripts
    # already create that database, so nothing further is needed.


def seed_raw_layer(engine: Engine) -> None:
    """Insert a small, realistically-shaped dataset into the raw tables."""
    engine.run("""
        INSERT INTO fineract_raw.offices
            (office_id, name, parent_id, hierarchy, opening_date, _op,
             _source_commit_at, _cdc_read_at, _lsn, _tx_id, _version, _is_deleted)
        SELECT
            number + 1,
            ['Head Office','Nairobi CBD','Kisumu','Mombasa'][(number % 4) + 1],
            if(number = 0, NULL, 1),
            if(number = 0, '.', concat('.', toString(number + 1), '.')),
            toDate('2018-01-01') + number * 90,
            'r', now64(3), now64(3), number, number, toUInt64(1700000000000 + number), 0
        FROM numbers(4)
    """)
    engine.run("""
        INSERT INTO fineract_raw.staff
            (staff_id, display_name, office_id, is_loan_officer, is_active,
             joining_date, _op, _source_commit_at, _cdc_read_at, _lsn, _tx_id,
             _version, _is_deleted)
        SELECT number + 1, concat('Officer ', toString(number + 1)),
               (number % 4) + 1, 1, 1, toDate('2020-01-01') + number * 30,
               'r', now64(3), now64(3), number, number,
               toUInt64(1700000000000 + number), 0
        FROM numbers(8)
    """)
    engine.run("""
        INSERT INTO fineract_raw.loan_products
            (product_id, name, short_name, currency_code, principal,
             number_of_repayments, interest_rate_per_period, annual_interest_rate,
             status, start_date, _op, _source_commit_at, _cdc_read_at, _lsn,
             _tx_id, _version, _is_deleted)
        SELECT number + 1, concat('Product ', toString(number + 1)),
               concat('P', toString(number + 1)), 'KES',
               toDecimal64(50000 * (number + 1), 6), 12,
               toDecimal64(2.5, 6), toDecimal64(30, 6),
               'loanProduct.active', toDate('2019-06-01'),
               'r', now64(3), now64(3), number, number,
               toUInt64(1700000000000 + number), 0
        FROM numbers(3)
    """)
    engine.run("""
        INSERT INTO fineract_raw.savings_products
            (product_id, name, currency_code, nominal_annual_interest_rate,
             status, _op, _source_commit_at, _cdc_read_at, _lsn, _tx_id,
             _version, _is_deleted)
        SELECT number + 1, concat('Savings ', toString(number + 1)), 'KES',
               toDecimal64(4.5, 6), 'savingsProduct.active',
               'r', now64(3), now64(3), number, number,
               toUInt64(1700000000000 + number), 0
        FROM numbers(2)
    """)
    engine.run("""
        INSERT INTO fineract_raw.clients
            (client_id, account_no, status_id, status_value, is_active,
             activation_date, submitted_on_date, office_id, staff_id,
             legal_form_value, gender_value, client_classification_value,
             firstname, lastname, display_name, date_of_birth,
             _op, _source_commit_at, _cdc_read_at, _lsn, _tx_id, _version, _is_deleted)
        SELECT
            number + 1, leftPad(toString(number + 1), 9, '0'),
            300, 'Active', 1,
            toDate('2023-01-01') + (number % 700),
            toDate('2023-01-01') + (number % 700) - 3,
            (number % 4) + 1, (number % 8) + 1,
            ['PERSON','ENTITY'][(number % 2) + 1],
            ['Female','Male'][(number % 2) + 1],
            ['Refugee entrepreneur','Host community','Youth'][(number % 3) + 1],
            concat('First', toString(number)), concat('Last', toString(number)),
            concat('Client ', toString(number + 1)),
            toDate('1985-01-01') + (number * 37) % 8000,
            'r', now64(3), now64(3), number, number,
            toUInt64(1700000000000 + number), 0
        FROM numbers(120)
    """)
    engine.run("""
        INSERT INTO fineract_raw.loan_transactions
            (transaction_id, loan_id, office_id, type_id, type_value, is_reversed,
             transaction_date, submitted_on_date, currency_code, amount,
             principal_portion, interest_portion, fee_charges_portion,
             penalty_charges_portion, overpayment_portion, outstanding_loan_balance,
             _op, _source_commit_at, _cdc_read_at, _lsn, _tx_id, _version, _is_deleted)
        SELECT
            number + 1, (number % 200) + 1, ((number % 200) % 4) + 1,
            if(number % 13 = 0, 1, 2),
            if(number % 13 = 0, 'Disbursement', 'Repayment'),
            if(number % 97 = 0, 1, 0),
            toDate('2025-01-01') + (number % 500),
            toDate('2025-01-01') + (number % 500),
            'KES',
            toDecimal64(9500, 6), toDecimal64(8000, 6), toDecimal64(1500, 6),
            toDecimal64(0, 6), toDecimal64(0, 6), toDecimal64(0, 6),
            toDecimal64(50000, 6),
            'r', now64(3), now64(3), number, number,
            toUInt64(1700000000000 + number), 0
        FROM numbers(2000)
    """)
    # Loans are inserted AFTER the ledger and derive their summary
    # balances FROM it, so the fixture is internally consistent. That
    # matters: assert_loan_ledger_reconciles compares the loan summary
    # with the sum of its transactions, and a fixture that disagrees with
    # itself would make a correct test look broken.
    engine.run("""
        INSERT INTO fineract_raw.loans
            (loan_id, account_no, client_id, product_id, office_id, loan_officer_id,
             currency_code, status_id, status_value, is_active, is_closed, is_overpaid,
             submitted_on_date, approved_on_date, disbursed_on_date,
             expected_maturity_date, closed_on_date, overdue_since_date,
             number_of_repayments, term_frequency, annual_interest_rate,
             principal, approved_principal, principal_disbursed, principal_paid,
             principal_written_off, principal_outstanding, principal_overdue,
             interest_charged, interest_paid, interest_outstanding, interest_overdue,
             fee_charges_charged, fee_charges_paid, fee_charges_outstanding,
             penalty_charges_charged, penalty_charges_paid, penalty_charges_outstanding,
             total_expected_repayment, total_repayment, total_outstanding, total_overdue,
             delinquent_days, delinquent_amount,
             _op, _source_commit_at, _cdc_read_at, _lsn, _tx_id, _version, _is_deleted)
        SELECT
            n.number + 1                                        AS loan_id,
            concat('L', leftPad(toString(n.number + 1), 9, '0')),
            (n.number % 120) + 1,
            (n.number % 3) + 1,
            (n.number % 4) + 1,
            (n.number % 8) + 1,
            'KES',
            [100, 300, 300, 300, 600, 601, 700][(n.number % 7) + 1],
            ['Submitted','Active','Active','Active','Closed','Written off','Overpaid'][(n.number % 7) + 1],
            if((n.number % 7) + 1 IN (2,3,4), 1, 0),
            if((n.number % 7) + 1 IN (5,6), 1, 0),
            if((n.number % 7) + 1 = 7, 1, 0),
            toDate('2024-01-01') + (n.number % 500),
            toDate('2024-01-08') + (n.number % 500),
            toDate('2024-01-15') + (n.number % 500),
            toDate('2025-01-15') + (n.number % 500),
            if((n.number % 7) + 1 IN (5,6), toDate('2025-06-01'), NULL),
            if(n.number % 5 = 0, toDate('2026-05-01'), NULL),
            12, 12, toDecimal64(30, 6),
            toDecimal64(200000, 6)                              AS principal,
            toDecimal64(200000, 6),
            toDecimal64(200000, 6)                              AS principal_disbursed,
            toDecimal64(ifNull(t.principal_repaid, 0), 6)       AS principal_paid,
            toDecimal64(0, 6),
            toDecimal64(200000 - ifNull(t.principal_repaid, 0), 6) AS principal_outstanding,
            toDecimal64(if(n.number % 5 = 0, 15000, 0), 6),
            toDecimal64(24000, 6),
            toDecimal64(ifNull(t.interest_repaid, 0), 6),
            toDecimal64(24000 - ifNull(t.interest_repaid, 0), 6),
            toDecimal64(if(n.number % 5 = 0, 2000, 0), 6),
            toDecimal64(2000, 6), toDecimal64(1500, 6), toDecimal64(500, 6),
            toDecimal64(if(n.number % 5 = 0, 500, 0), 6), toDecimal64(0, 6),
            toDecimal64(if(n.number % 5 = 0, 500, 0), 6),
            toDecimal64(224000, 6),
            toDecimal64(ifNull(t.total_repaid, 0), 6)           AS total_repayment,
            toDecimal64(224000 - ifNull(t.total_repaid, 0), 6)  AS total_outstanding,
            toDecimal64(if(n.number % 5 = 0, 17500, 0), 6),
            if(n.number % 5 = 0, [5, 45, 75, 120, 200][(n.number % 5) + 1], 0),
            toDecimal64(if(n.number % 5 = 0, 17500, 0), 6),
            'r', now64(3), now64(3), n.number, n.number,
            toUInt64(1700000000000 + n.number), 0
        FROM numbers(200) n
        LEFT JOIN (
            SELECT loan_id,
                   sum(amount)            AS total_repaid,
                   sum(principal_portion) AS principal_repaid,
                   sum(interest_portion)  AS interest_repaid
            FROM fineract_raw.loan_transactions
            WHERE type_id = 2 AND is_reversed = 0
            GROUP BY loan_id
        ) t ON toInt64(n.number + 1) = t.loan_id
    """)
    engine.run("""
        INSERT INTO fineract_raw.savings_accounts
            (savings_id, account_no, client_id, product_id, office_id,
             status_id, status_value, is_active, currency_code,
             nominal_annual_interest_rate, submitted_on_date, activated_on_date,
             account_balance, available_balance, total_deposits, total_withdrawals,
             total_interest_posted,
             _op, _source_commit_at, _cdc_read_at, _lsn, _tx_id, _version, _is_deleted)
        SELECT
            number + 1, concat('S', leftPad(toString(number + 1), 9, '0')),
            (number % 120) + 1, (number % 2) + 1, (number % 4) + 1,
            300, 'Active', 1, 'KES', toDecimal64(4.5, 6),
            toDate('2024-03-01') + (number % 300),
            toDate('2024-03-05') + (number % 300),
            toDecimal64(25000 + number * 100, 6), toDecimal64(25000 + number * 100, 6),
            toDecimal64(60000 + number * 200, 6), toDecimal64(35000 + number * 100, 6),
            toDecimal64(600, 6),
            'r', now64(3), now64(3), number, number,
            toUInt64(1700000000000 + number), 0
        FROM numbers(60)
    """)


def load_seeds(engine: Engine) -> None:
    """Materialise dbt seeds as tables so models can ref() them."""
    import csv

    for path in sorted(SEEDS_DIR.glob("*.csv")):
        with path.open() as handle:
            rows = list(csv.DictReader(handle))
        if not rows:
            continue
        columns = list(rows[0].keys())
        engine.run(
            f"CREATE TABLE IF NOT EXISTS fineract_staging.{path.stem} ("
            + ", ".join(f"{c} String" for c in columns)
            + ") ENGINE = MergeTree ORDER BY tuple()")
        values = ", ".join(
            "(" + ", ".join("'" + str(row[c]).replace("'", "''") + "'" for c in columns) + ")"
            for row in rows)
        engine.run(
            f"INSERT INTO fineract_staging.{path.stem} "
            f"({', '.join(columns)}) VALUES {values}")


# ---------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------
def collect_schema_tests(models: dict[str, Model]) -> list[tuple[str, str, str]]:
    """Turn models/**/*.yml tests into executable ClickHouse assertions."""
    checks: list[tuple[str, str, str]] = []
    for path in sorted(MODELS_DIR.rglob("*.yml")):
        document = yaml.safe_load(path.read_text()) or {}
        for model_entry in document.get("models", []) or []:
            name = model_entry.get("name")
            model = models.get(name)
            if not model:
                continue
            relation = model.relation
            for test in model_entry.get("data_tests", []) or model_entry.get("tests", []) or []:
                if isinstance(test, dict) and "unique_combination_of_columns" in str(test):
                    continue
            for column in model_entry.get("columns", []) or []:
                column_name = column["name"]
                for test in column.get("data_tests", []) or column.get("tests", []) or []:
                    if test == "not_null":
                        checks.append((
                            f"{name}.{column_name}.not_null",
                            f"SELECT count() FROM {relation} WHERE {column_name} IS NULL",
                            "0"))
                    elif test == "unique":
                        checks.append((
                            f"{name}.{column_name}.unique",
                            f"SELECT count() - countDistinct({column_name}) FROM {relation}",
                            "0"))
                    elif isinstance(test, dict) and "accepted_values" in test:
                        values = test["accepted_values"]["values"]
                        rendered = ", ".join(f"'{v}'" for v in values)
                        checks.append((
                            f"{name}.{column_name}.accepted_values",
                            f"SELECT count() FROM {relation} "
                            f"WHERE {column_name} IS NOT NULL "
                            f"AND toString({column_name}) NOT IN ({rendered})",
                            "0"))
                    elif isinstance(test, dict) and "relationships" in test:
                        spec = test["relationships"]
                        target_name = re.sub(r".*ref\(\s*['\"]([\w_]+)['\"]\s*\).*", r"\1",
                                             str(spec.get("to", "")))
                        target = models.get(target_name)
                        if not target:
                            continue
                        checks.append((
                            f"{name}.{column_name}.relationships",
                            f"SELECT count() FROM {relation} a "
                            f"LEFT JOIN {target.relation} b "
                            f"ON a.{column_name} = b.{spec['field']} "
                            f"WHERE a.{column_name} IS NOT NULL AND b.{spec['field']} IS NULL",
                            "0"))
    return checks


def run_singular_tests(engine: Engine, jinja: DbtJinja,
                       models: dict[str, Model], verbose: bool) -> list[str]:
    failures: list[str] = []
    for path in sorted(TESTS_DIR.glob("*.sql")):
        probe = Model(name=path.stem, path=path, layer="marts", raw_sql=path.read_text())
        jinja.models = models
        sql = jinja.render(probe)
        count = engine.scalar(f"SELECT count() FROM ({sql}) AS t")
        status = "PASS" if count == "0" else f"FAIL ({count} rows)"
        if count != "0":
            failures.append(f"{path.stem}: {count} failing rows")
        if verbose or count != "0":
            print(f"    {path.stem:<50} {status}")
    return failures


# ---------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=sorted(LAYER_DATABASE), default=None)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--skip-tests", action="store_true")
    args = parser.parse_args()

    engine = Engine()
    print(f"Building the dbt DAG on embedded ClickHouse "
          f"{engine.scalar('SELECT version()')}\n")

    print("  [1/5] creating raw layer from platform/clickhouse/init")
    create_raw_layer(engine)
    print("  [2/5] seeding synthetic Fineract data")
    seed_raw_layer(engine)

    project_vars = load_project_vars()
    models = discover_models()
    jinja = DbtJinja(project_vars)
    jinja.models = models

    # First pass: render everything once to discover refs.
    for model in models.values():
        try:
            jinja.render(model)
        except Exception as exc:
            print(f"\n[JINJA FAIL] {model.path.relative_to(REPO_ROOT)}: {exc}",
                  file=sys.stderr)
            return 1

    ordered = topological_order(models)
    if args.layer:
        ordered = [m for m in ordered if m.layer == args.layer]

    for database in LAYER_DATABASE.values():
        engine.run(f"CREATE DATABASE IF NOT EXISTS {database}")
    print("  [3/5] loading seeds")
    load_seeds(engine)

    print(f"  [4/5] building {len(ordered)} models\n")
    built = 0
    for model in ordered:
        sql = jinja.render(model)
        try:
            engine.run(f"DROP TABLE IF EXISTS {model.relation}")
            engine.run(
                f"CREATE TABLE {model.relation} ENGINE = MergeTree ORDER BY tuple() "
                f"AS {sql}")
            rows = engine.scalar(f"SELECT count() FROM {model.relation}")

            # Guard against a genuinely nasty ClickHouse behaviour: when a
            # column name is ambiguous across joined relations and the
            # SELECT does not alias it, the resulting column is literally
            # named "alias.column". Everything downstream then fails with
            # a misleading "correlated subqueries are not supported"
            # error. Catch it here, where the message is clear.
            bad_columns = engine.run(
                f"SELECT name FROM system.columns "
                f"WHERE database = '{model.relation.split('.')[0]}' "
                f"AND table = '{model.name}' AND position(name, '.') > 0"
            ).strip()
            if bad_columns:
                names = bad_columns.replace("\n", ", ")
                raise RuntimeError(
                    f"unaliased ambiguous column(s) produced qualified names: "
                    f"{names} - add an explicit AS alias")

            built += 1
            print(f"    {model.layer:<13} {model.name:<44} OK  {rows:>8} rows")
        except Exception as exc:
            print(f"\n[MODEL FAIL] {model.path.relative_to(REPO_ROOT)}\n  {exc}\n",
                  file=sys.stderr)
            if args.verbose:
                print(sql, file=sys.stderr)
            return 1

    print(f"\n  {built} models built successfully.")

    if args.skip_tests:
        return 0

    print("\n  [5/5] running data tests")
    failures: list[str] = []
    for name, query, expected in collect_schema_tests(models):
        try:
            observed = engine.scalar(query)
        except Exception as exc:
            failures.append(f"{name}: query error {exc}")
            print(f"    {name:<50} ERROR")
            continue
        ok = observed == expected
        if not ok:
            failures.append(f"{name}: expected {expected}, got {observed}")
        if args.verbose or not ok:
            print(f"    {name:<50} {'PASS' if ok else 'FAIL (' + observed + ')'}")

    failures += run_singular_tests(engine, jinja, models, args.verbose)

    if failures:
        print(f"\n{len(failures)} test failure(s):", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print("\nAll models built and all data tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
