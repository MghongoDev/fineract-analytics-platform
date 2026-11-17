#!/usr/bin/env python3
"""Apply ClickHouse init SQL over HTTP, one statement per request.

The HTTP interface rejects a POST body with more than one statement:

    Code: 62. DB::Exception: Syntax error (Multi-statements are not allowed)

Posting a whole init file therefore returns HTTP 400. ClickHouse's own
clients split on semicolons before sending, and this script does the same
thing for CI, where only curl/urllib are available.

Comment-only lines are dropped. None of the init files contain a semicolon
inside a string literal; if one ever does, use clickhouse-client instead of
extending the naive splitter here.
"""

from __future__ import annotations

import argparse
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path


def split_statements(sql: str) -> list[str]:
    """Split on semicolons, ignoring comment-only lines."""
    cleaned = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in cleaned.split(";") if s.strip()]


def apply_file(url: str, path: Path) -> tuple[int, str | None]:
    """POST each statement; return (count, error) where error is None on success."""
    statements = split_statements(path.read_text())
    for number, statement in enumerate(statements, 1):
        request = urllib.request.Request(
            url,
            data=statement.encode("utf-8"),
            method="POST",
            headers={"Content-Type": "text/plain; charset=utf-8"},
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                if response.status != 200:
                    return number - 1, f"HTTP {response.status}"
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace").strip()
            return number - 1, f"HTTP {exc.code}: {body}\nstatement: {statement[:400]}"
        except OSError as exc:
            return number - 1, f"connection error: {exc}"

    return len(statements), None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path, help="SQL files to apply")
    parser.add_argument(
        "--url",
        default=os.environ.get("CLICKHOUSE_HTTP_URL", "http://localhost:8123/"),
        help="ClickHouse HTTP endpoint (default: %(default)s)",
    )
    args = parser.parse_args()

    total = 0
    for path in args.files:
        count, error = apply_file(args.url, path)
        if error is not None:
            print(f"::error::{path.name} statement {count + 1} failed: {error}", file=sys.stderr)
            return 1
        print(f"{path.name}: {count} statements OK")
        total += count

    print(f"{total} statements applied successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
