"""Command line entry point.

    python -m fineract_ingest ingest --entities clients,loans
    python -m fineract_ingest ingest --all --dry-run
    python -m fineract_ingest health
    python -m fineract_ingest status
    python -m fineract_ingest heartbeat

Exit codes are meaningful because Airflow (and CI) branch on them:
    0 success | 1 one or more entities failed | 2 configuration/connectivity error
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from .client import FineractClient
from .config import Settings
from .entities import DEFAULT_ORDER, ENTITIES
from .loader import PostgresLoader
from .logging_setup import configure, get_logger
from .pipeline import IngestionPipeline

log = get_logger("fineract_ingest.cli")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fineract-ingest",
        description="Ingest Apache Fineract REST data into the Postgres OLTP layer.")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Run an ingestion pass.")
    group = ingest.add_mutually_exclusive_group()
    group.add_argument("--entities", help="Comma separated entity names.")
    group.add_argument("--all", action="store_true",
                       help=f"Run every entity in order: {', '.join(DEFAULT_ORDER)}")
    ingest.add_argument("--parent-limit", type=int, default=None,
                        help="Cap parent ids for child collections "
                             "(loan_transactions); useful for smoke runs.")
    ingest.add_argument("--dry-run", action="store_true",
                        help="Fetch, map and validate but roll back all writes.")
    ingest.add_argument("--fail-fast", action="store_true",
                        help="Stop at the first failing entity.")

    sub.add_parser("health", help="Check Fineract API and Postgres reachability.")
    sub.add_parser("status", help="Print watermarks and last run per entity.")
    sub.add_parser("list-entities", help="Print the entity registry.")
    sub.add_parser("heartbeat", help="Advance the CDC heartbeat row.")
    return parser


def cmd_health(settings: Settings) -> int:
    ok = True
    with FineractClient(settings.fineract) as client:
        api_ok = client.health_check()
    print(json.dumps({"component": "fineract_api", "healthy": api_ok,
                      "base_url": settings.fineract.base_url}))
    ok &= api_ok

    loader = PostgresLoader(settings.postgres)
    try:
        loader.connect()
        slots = loader.replication_slot_status()
        print(json.dumps({"component": "postgres", "healthy": True,
                          "replication_slots": slots}, default=str))
    except Exception as exc:
        print(json.dumps({"component": "postgres", "healthy": False, "error": str(exc)}))
        ok = False
    finally:
        loader.close()
    return 0 if ok else 2


def cmd_status(settings: Settings) -> int:
    loader = PostgresLoader(settings.postgres)
    try:
        rows = []
        for name, spec in ENTITIES.items():
            watermark = loader.read_watermark(name) or {}
            rows.append({
                "entity": name,
                "table": spec.table,
                "rows": loader.table_count(spec.table),
                "last_success_at": watermark.get("last_success_at"),
                "last_cursor": watermark.get("last_cursor"),
                "total_rows_loaded": watermark.get("total_rows_loaded"),
            })
        print(json.dumps(rows, indent=2, default=str))
        return 0
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        return 2
    finally:
        loader.close()


def cmd_list_entities() -> int:
    print(json.dumps([
        {"entity": name, "path": spec.path, "table": spec.table,
         "mode": spec.mode, "paged": spec.paged,
         "expectations": [e.name for e in spec.expectations],
         "description": spec.description}
        for name, spec in ENTITIES.items()
    ], indent=2))
    return 0


def cmd_heartbeat(settings: Settings) -> int:
    loader = PostgresLoader(settings.postgres)
    try:
        loader.touch_heartbeat()
        print(json.dumps({"heartbeat": "ok"}))
        return 0
    except Exception as exc:
        print(json.dumps({"heartbeat": "failed", "error": str(exc)}))
        return 2
    finally:
        loader.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.load()
    configure(settings.runtime.log_level, settings.runtime.log_format)

    if args.command == "health":
        return cmd_health(settings)
    if args.command == "status":
        return cmd_status(settings)
    if args.command == "list-entities":
        return cmd_list_entities()
    if args.command == "heartbeat":
        return cmd_heartbeat(settings)

    if args.command == "ingest":
        entities = None
        if args.entities:
            entities = [e.strip() for e in args.entities.split(",") if e.strip()]
        pipeline = IngestionPipeline(settings)
        try:
            outcomes = pipeline.run(entities=entities,
                                    parent_limit=args.parent_limit,
                                    dry_run=args.dry_run,
                                    fail_fast=args.fail_fast)
        finally:
            pipeline.close()

        print(json.dumps([
            {"entity": o.entity, "status": o.status, **o.result.as_dict(),
             "duration_seconds": round(o.duration_seconds, 2), "error": o.error}
            for o in outcomes
        ], indent=2))
        return 1 if any(o.status == "failed" for o in outcomes) else 0

    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
