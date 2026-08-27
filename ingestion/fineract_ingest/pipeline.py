"""The ingestion pipeline: API -> validate -> land -> record -> report.

One entity load is a single unit of work with a single transaction:

    BEGIN
      upsert rows
      insert rejects
      insert expectation results
      update watermark
      close out meta.ingestion_run
    COMMIT

If any step raises, nothing lands and the run is marked ``failed`` in a
separate transaction. There is deliberately no partial-commit path: for
financial data, a half-loaded batch is worse than no batch, because it is
indistinguishable from a complete one downstream.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .client import FineractClient, FineractError
from .config import Settings
from .entities import DEFAULT_ORDER, EntitySpec, get_entity
from .loader import LoadResult, PostgresLoader
from .logging_setup import bind, get_logger
from .metrics import IngestionMetrics
from .parsers import payload_hash
from .validation import (
    ExpectationResult,
    RejectedRecord,
    evaluate_expectations,
    summarise,
    validate_record,
)

log = get_logger(__name__)


@dataclass
class EntityOutcome:
    entity: str
    status: str
    result: LoadResult
    duration_seconds: float
    expectations: list[ExpectationResult] = field(default_factory=list)
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == "success"


class IngestionPipeline:
    def __init__(self, settings: Settings | None = None,
                 client: FineractClient | None = None,
                 loader: PostgresLoader | None = None,
                 metrics: IngestionMetrics | None = None):
        self.settings = settings or Settings.load()
        self.client = client or FineractClient(self.settings.fineract)
        self.loader = loader or PostgresLoader(self.settings.postgres)
        self.metrics = metrics or IngestionMetrics(
            pushgateway_url=self.settings.runtime.pushgateway_url,
            enabled=self.settings.runtime.push_metrics,
            environment=self.settings.runtime.environment,
        )
        self.batch_id = uuid.uuid4()
        bind(batch_id=str(self.batch_id), environment=self.settings.runtime.environment)

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------
    def _fetch_records(self, spec: EntitySpec,
                       parent_limit: int | None = None) -> Iterable[Mapping[str, Any]]:
        """Yield raw API records for an entity, flat or parent-driven."""
        if spec.mode == "parent":
            parent_ids = self.loader.fetch_parent_ids(spec.parent_id_query or "", parent_limit)
            log.info("fetching_child_collection", extra={
                "entity": spec.name, "parents": len(parent_ids)})
            for parent_id in parent_ids:
                path = spec.path.format(parent_id=parent_id)
                try:
                    for record in self.client.iter_items(path, dict(spec.params),
                                                         paged=spec.paged):
                        # Some Fineract builds omit loanId on the nested
                        # transaction resource; carry the parent down.
                        enriched = dict(record)
                        enriched.setdefault("loanId", parent_id)
                        enriched["_loan_id"] = parent_id
                        yield enriched
                except FineractError as exc:
                    # A single unreadable loan (permissions, deleted mid-crawl)
                    # must not sink the whole entity.
                    log.warning("child_collection_failed", extra={
                        "entity": spec.name, "parent_id": parent_id,
                        "status": exc.status_code, "error": str(exc)})
        else:
            yield from self.client.iter_items(spec.path, dict(spec.params),
                                              paged=spec.paged)

    # ------------------------------------------------------------------
    # Map + validate
    # ------------------------------------------------------------------
    def _map_and_validate(self, spec: EntitySpec,
                          raw_records: Iterable[Mapping[str, Any]]
                          ) -> tuple[list[dict], list[RejectedRecord]]:
        mapped_rows: list[dict] = []
        rejects: list[RejectedRecord] = []
        seen_keys: set[Any] = set()

        for raw in raw_records:
            try:
                mapped = spec.mapper(raw)
            except Exception as exc:  # mapping bug or wildly unexpected payload
                rejects.append(RejectedRecord(
                    spec.name, None, "mapper_exception", str(exc), raw))
                continue

            rejection = validate_record(spec, mapped, raw)
            if rejection:
                rejects.append(rejection)
                continue

            key = mapped[spec.primary_key]
            if key in seen_keys:
                # The API paged the same record twice (concurrent writes
                # shift offsets). Last write wins; keep the batch unique so
                # ON CONFLICT never sees a duplicate key in one statement.
                mapped_rows = [r for r in mapped_rows if r[spec.primary_key] != key]
            seen_keys.add(key)

            mapped["_source_system"] = "fineract"
            mapped["_payload_hash"] = payload_hash(mapped)
            mapped_rows.append(mapped)

        return mapped_rows, rejects

    # ------------------------------------------------------------------
    # Run one entity
    # ------------------------------------------------------------------
    def run_entity(self, entity_name: str, parent_limit: int | None = None,
                   dry_run: bool = False) -> EntityOutcome:
        spec = get_entity(entity_name)
        started = time.monotonic()
        bind(entity=entity_name)
        log.info("entity_load_started", extra={
            "entity": entity_name, "table": spec.table, "mode": spec.mode})

        connection = self.loader.connect()
        run_id = self.loader.start_run(
            connection, entity_name, self.batch_id, self.settings.runtime.dag_run_id)

        requests_before = self.client.request_count
        retries_before = self.client.retry_count
        latency_before = self.client.total_latency_seconds

        try:
            raw_records = list(self._fetch_records(spec, parent_limit))
            mapped_rows, rejects = self._map_and_validate(spec, raw_records)

            expectations = evaluate_expectations(spec, mapped_rows)
            summary = summarise(expectations)
            blocking = [e for e in expectations if e.is_blocking_failure]

            reject_ratio = (len(rejects) / len(raw_records)) if raw_records else 0.0
            if reject_ratio > self.settings.runtime.max_reject_ratio:
                raise ValueError(
                    f"reject ratio {reject_ratio:.1%} exceeds threshold "
                    f"{self.settings.runtime.max_reject_ratio:.1%} "
                    f"({len(rejects)}/{len(raw_records)} records)")

            if blocking:
                raise ValueError(
                    "blocking data-quality failures: "
                    + ", ".join(f"{e.name} ({e.details})" for e in blocking))

            if dry_run:
                log.info("dry_run_no_write", extra={
                    "entity": entity_name, "would_write": len(mapped_rows),
                    "rejects": len(rejects)})
                connection.rollback()
                result = LoadResult(entity_name)
                result.rows_read = len(mapped_rows)
                result.rows_rejected = len(rejects)
                outcome = EntityOutcome(entity_name, "skipped", result,
                                        time.monotonic() - started, expectations)
                self.loader.finish_run(connection, run_id, "skipped", result)
                connection.commit()
                return outcome

            # ---- single atomic unit of work --------------------------
            result = self.loader.upsert(connection, spec.table, spec.primary_key, mapped_rows)
            result.rows_rejected = self.loader.record_rejects(connection, self.batch_id, rejects)
            self.loader.record_expectations(connection, self.batch_id, entity_name, expectations)
            self.loader.update_watermark(
                connection, entity_name,
                cursor_value=self._cursor_value(spec, mapped_rows),
                row_count=result.rows_read)
            self.loader.finish_run(
                connection, run_id, "success", result,
                api_requests=self.client.request_count - requests_before,
                api_retries=self.client.retry_count - retries_before)
            connection.commit()
            # ----------------------------------------------------------

            duration = time.monotonic() - started
            requests = self.client.request_count - requests_before
            mean_latency = ((self.client.total_latency_seconds - latency_before) / requests
                            if requests else 0.0)

            self.metrics.record_load(
                entity_name, **result.as_dict(), duration_seconds=duration,
                api_requests=requests,
                api_retries=self.client.retry_count - retries_before,
                mean_latency_seconds=mean_latency, status="success",
                table_rows=self.loader.table_count(spec.table))
            self.metrics.record_expectations(
                entity_name, summary["errors"], summary["warnings"])

            log.info("entity_load_succeeded", extra={
                "entity": entity_name, **result.as_dict(),
                "duration_seconds": round(duration, 2),
                "expectations": summary})

            return EntityOutcome(entity_name, "success", result, duration, expectations)

        except Exception as exc:
            connection.rollback()
            duration = time.monotonic() - started
            failed = LoadResult(entity_name)
            try:
                self.loader.finish_run(connection, run_id, "failed", failed,
                                       error_message=str(exc)[:2000])
                connection.commit()
            except Exception:  # pragma: no cover
                connection.rollback()
            self.metrics.record_load(
                entity_name, **failed.as_dict(), duration_seconds=duration,
                api_requests=self.client.request_count - requests_before,
                api_retries=self.client.retry_count - retries_before,
                mean_latency_seconds=0.0, status="failed")
            log.error("entity_load_failed", extra={
                "entity": entity_name, "error": str(exc),
                "duration_seconds": round(duration, 2)}, exc_info=True)
            return EntityOutcome(entity_name, "failed", failed, duration, error=str(exc))

    @staticmethod
    def _cursor_value(spec: EntitySpec, rows: Sequence[Mapping[str, Any]]) -> str | None:
        if not rows:
            return None
        keys = [r.get(spec.primary_key) for r in rows if r.get(spec.primary_key) is not None]
        return str(max(keys)) if keys else None

    # ------------------------------------------------------------------
    # Run many
    # ------------------------------------------------------------------
    def run(self, entities: Sequence[str] | None = None,
            parent_limit: int | None = None,
            dry_run: bool = False,
            fail_fast: bool = False) -> list[EntityOutcome]:
        selected = list(entities) if entities else list(DEFAULT_ORDER)
        log.info("ingestion_started", extra={
            "entities": selected, "dry_run": dry_run,
            "source": self.settings.fineract.masked()})

        self.client.authenticate()
        outcomes: list[EntityOutcome] = []
        for name in selected:
            outcome = self.run_entity(name, parent_limit=parent_limit, dry_run=dry_run)
            outcomes.append(outcome)
            if fail_fast and not outcome.ok and outcome.status != "skipped":
                log.error("fail_fast_abort", extra={"entity": name})
                break

        self.metrics.push()
        failed = [o.entity for o in outcomes if o.status == "failed"]
        log.info("ingestion_finished", extra={
            "entities": len(outcomes), "failed": failed,
            "rows_inserted": sum(o.result.rows_inserted for o in outcomes),
            "rows_updated": sum(o.result.rows_updated for o in outcomes),
            "rows_unchanged": sum(o.result.rows_unchanged for o in outcomes),
            "rows_rejected": sum(o.result.rows_rejected for o in outcomes)})
        return outcomes

    def close(self) -> None:
        self.client.close()
        self.loader.close()
