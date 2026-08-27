"""Operators for the transformation and quality stages."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from .hooks import ClickHouseHook

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")


class DbtOperator(BaseOperator):
    """Run a dbt command and surface what it actually did.

    Why shell out rather than use dbt's Python entry point: dbt's
    programmatic API is explicitly not a stable interface, and pinning to
    it makes a dbt upgrade an Airflow problem. A subprocess with a parsed
    `run_results.json` is boring, stable and gives better logs.

    The operator streams dbt output into the task log line by line rather
    than buffering it - a 20-minute build that only prints at the end is
    unusable when you are trying to work out where it hung.
    """

    ui_color = "#ff7043"
    template_fields: Sequence[str] = ("select", "exclude", "vars", "full_refresh")

    def __init__(self,
                 command: str = "run",
                 select: str | None = None,
                 exclude: str | None = None,
                 vars: dict | None = None,      # noqa: A002 - dbt's own name
                 full_refresh: bool = False,
                 target: str | None = None,
                 fail_fast: bool = False,
                 warn_error: bool = False,
                 project_dir: str = DBT_PROJECT_DIR,
                 profiles_dir: str = DBT_PROFILES_DIR,
                 **kwargs: Any):
        super().__init__(**kwargs)
        self.command = command
        self.select = select
        self.exclude = exclude
        self.vars = vars or {}
        self.full_refresh = full_refresh
        self.target = target or os.environ.get("DBT_TARGET", "dev")
        self.fail_fast = fail_fast
        self.warn_error = warn_error
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir

    def _build_command(self) -> list[str]:
        argv = ["dbt", "--no-use-colors", self.command,
                "--project-dir", self.project_dir,
                "--profiles-dir", self.profiles_dir,
                "--target", self.target]
        if self.select:
            argv += ["--select", self.select]
        if self.exclude:
            argv += ["--exclude", self.exclude]
        if self.vars:
            argv += ["--vars", json.dumps(self.vars)]
        if self.full_refresh:
            argv += ["--full-refresh"]
        if self.fail_fast:
            argv += ["--fail-fast"]
        if self.warn_error:
            argv.insert(1, "--warn-error")
        return argv

    def execute(self, context: Context) -> dict:
        argv = self._build_command()
        self.log.info("running: %s", " ".join(argv))

        process = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env={**os.environ})
        assert process.stdout is not None
        for line in process.stdout:
            self.log.info(line.rstrip())
        return_code = process.wait()

        summary = self._summarise_run_results()
        self.log.info("dbt summary: %s", json.dumps(summary))

        if return_code != 0:
            raise AirflowFailException(
                f"dbt {self.command} exited {return_code}: {summary}")

        # Push to XCom so the quality gate and the metrics publisher can
        # read the outcome without re-parsing the artefacts.
        return summary

    def _summarise_run_results(self) -> dict:
        path = Path(self.project_dir) / "target" / "run_results.json"
        if not path.exists():
            return {"status": "no run_results.json"}
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            return {"status": "unparseable run_results.json"}
        results = payload.get("results", [])
        return {
            "invocation_id": payload.get("metadata", {}).get("invocation_id"),
            "nodes": len(results),
            "success": sum(1 for r in results if r.get("status") in ("success", "pass")),
            "warn": sum(1 for r in results if r.get("status") == "warn"),
            "error": sum(1 for r in results if r.get("status") in ("error", "fail")),
            "skipped": sum(1 for r in results if r.get("status") == "skipped"),
            "elapsed": round(payload.get("elapsed_time", 0), 2),
            "failed_nodes": [r.get("unique_id") for r in results
                             if r.get("status") in ("error", "fail")][:20],
        }


class PublishDbtResultsOperator(BaseOperator):
    """Load dbt's run_results.json into ClickHouse.

    Test history belongs in the warehouse, not only in Airflow logs. Once
    it is a table, "has this test ever failed before?" and "which model
    got 3x slower this month?" are queries rather than an archaeology
    exercise through task logs - and the Grafana panels can show test
    health next to data health.
    """

    ui_color = "#8bc34a"

    def __init__(self, project_dir: str = DBT_PROJECT_DIR, **kwargs: Any):
        super().__init__(**kwargs)
        self.project_dir = project_dir

    @staticmethod
    def _layer_of(unique_id: str) -> str:
        for layer in ("staging", "intermediate", "marts", "ml"):
            if f".{layer}." in unique_id or unique_id.startswith(f"test.{layer}"):
                return layer
        if "stg_" in unique_id:
            return "staging"
        if "int_" in unique_id:
            return "intermediate"
        if unique_id.startswith("test."):
            return "tests"
        return "other"

    @staticmethod
    def _escape(value: Any) -> str:
        return str(value).replace("\\", "\\\\").replace("'", "\\'")[:2000]

    def execute(self, context: Context) -> dict:
        path = Path(self.project_dir) / "target" / "run_results.json"
        if not path.exists():
            raise AirflowSkipException("no run_results.json to publish")

        payload = json.loads(path.read_text())
        invocation_id = payload.get("metadata", {}).get("invocation_id", "unknown")
        executed_at = payload.get("metadata", {}).get("generated_at", "")[:23].replace("T", " ")
        dag_run_id = context["run_id"]

        test_rows, model_rows = [], []
        for result in payload.get("results", []):
            unique_id = result.get("unique_id", "")
            status = result.get("status", "unknown")
            timing = round(result.get("execution_time", 0.0), 4)
            message = self._escape(result.get("message") or "")
            layer = self._layer_of(unique_id)
            name = unique_id.split(".")[-1]

            if unique_id.startswith("test."):
                test_rows.append(
                    f"('{invocation_id}', '{dag_run_id}', '{executed_at}', "
                    f"'{self._escape(unique_id)}', '{self._escape(name)}', "
                    f"'{self._escape(name)}', '{layer}', '{status}', "
                    f"'{result.get('failures') is not None and 'error' or 'error'}', "
                    f"{int(result.get('failures') or 0)}, {timing}, '{message}')")
            else:
                model_rows.append(
                    f"('{invocation_id}', '{dag_run_id}', '{executed_at}', "
                    f"'{self._escape(unique_id)}', '{self._escape(name)}', "
                    f"'{layer}', "
                    f"'{self._escape(result.get('adapter_response', {}).get('materialization', ''))}', "  # noqa: E501
                    f"'{status}', "
                    f"{int(result.get('adapter_response', {}).get('rows_affected') or 0)}, "
                    f"{timing}, '{message}')")

        clickhouse = ClickHouseHook(database="fineract_ops")
        if test_rows:
            clickhouse.execute(
                "INSERT INTO fineract_ops.dbt_test_results "
                "(invocation_id, dag_run_id, executed_at, node_id, test_name, "
                " model_name, layer, status, severity, failures, execution_time, message) "
                "VALUES " + ", ".join(test_rows))
        if model_rows:
            clickhouse.execute(
                "INSERT INTO fineract_ops.dbt_model_runs "
                "(invocation_id, dag_run_id, executed_at, node_id, model_name, "
                " layer, materialization, status, rows_affected, execution_time, message) "
                "VALUES " + ", ".join(model_rows))

        summary = {"tests_published": len(test_rows), "models_published": len(model_rows),
                   "invocation_id": invocation_id}
        self.log.info("published dbt artefacts: %s", json.dumps(summary))
        return summary


class DataQualityGateOperator(BaseOperator):
    """The final gate: is this run fit to be consumed?

    Runs a set of warehouse-level assertions after the marts are built and
    fails the DAG if any blocking one trips. The point is that a failure
    here is loud *and* the marts are already built - so the on-call
    engineer can inspect exactly what was produced rather than guessing.

    Assertions are declared as data, so adding one is a one-line change
    and every one of them is visible in the DAG's source.
    """

    ui_color = "#e53935"

    #: (name, query returning a single number, comparison, threshold, blocking)
    DEFAULT_CHECKS: tuple[tuple[str, str, str, float, bool], ...] = (
        ("marts_not_empty",
         "SELECT count() FROM fineract_marts.fct_loan", ">", 0, True),
        ("no_duplicate_loans",
         "SELECT count() - countDistinct(loan_id) FROM fineract_marts.fct_loan",
         "==", 0, True),
        ("no_duplicate_transactions",
         "SELECT count() - countDistinct(transaction_id) "
         "FROM fineract_marts.fct_loan_transaction", "==", 0, True),
        ("no_negative_outstanding",
         "SELECT countIf(total_outstanding < 0) FROM fineract_marts.fct_loan",
         "==", 0, True),
        ("cdc_parse_errors",
         "SELECT count() FROM fineract_raw.cdc_errors "
         "WHERE observed_at > now() - INTERVAL 1 DAY", "==", 0, False),
        ("ml_label_leakage",
         "SELECT countIf(feat_days_since_prior_loan <= 0) "
         "FROM fineract_ml.ml_loan_default_features", "==", 0, True),
        ("dimension_coverage",
         "SELECT countIf(client_segment = '') FROM fineract_marts.fct_loan",
         "==", 0, False),
    )

    def __init__(self, checks: Sequence[tuple] | None = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.checks = list(checks or self.DEFAULT_CHECKS)

    def execute(self, context: Context) -> dict:
        clickhouse = ClickHouseHook()
        results, blocking_failures = [], []

        for name, query, comparison, threshold, blocking in self.checks:
            try:
                observed = float(clickhouse.scalar(query) or 0)
            except Exception as exc:
                results.append({"check": name, "status": "error", "detail": str(exc)})
                if blocking:
                    blocking_failures.append(f"{name}: query failed ({exc})")
                continue

            passed = {
                ">": observed > threshold,
                ">=": observed >= threshold,
                "==": observed == threshold,
                "<": observed < threshold,
                "<=": observed <= threshold,
            }[comparison]

            results.append({"check": name, "observed": observed,
                            "expected": f"{comparison} {threshold}",
                            "status": "pass" if passed else "fail",
                            "blocking": blocking})
            level = self.log.info if passed else self.log.error
            level("quality check %-28s observed=%s expected %s %s -> %s",
                  name, observed, comparison, threshold, "PASS" if passed else "FAIL")
            if not passed and blocking:
                blocking_failures.append(
                    f"{name}: observed {observed}, expected {comparison} {threshold}")

        if blocking_failures:
            raise AirflowFailException(
                "blocking data-quality failures:\n  - " + "\n  - ".join(blocking_failures))

        return {"checks": results,
                "passed": sum(1 for r in results if r["status"] == "pass"),
                "failed": sum(1 for r in results if r["status"] != "pass")}
