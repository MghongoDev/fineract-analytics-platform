"""Pure functions that turn Fineract JSON into typed Python values.

Kept free of I/O so the tricky bits - and Fineract's date encoding is the
trickiest bit - are unit-testable in isolation.

Fineract date encoding
----------------------
The v1 API serialises dates as a **year/month/day integer array**
(``[2026, 8, 11]``) rather than an ISO string, and some endpoints return
``[2026, 8, 11, 14, 30, 0]`` for timestamps. Deployments that set a global
``dateFormat`` return ISO strings instead, and optional fields are simply
absent. All four shapes are handled here so that no downstream layer has
to know about it.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


# ---------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------
def parse_date(value: Any) -> date | None:
    """Parse any of Fineract's date encodings into a ``date``."""
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (list, tuple)):
        parts = [int(p) for p in value[:3] if p is not None]
        if len(parts) < 3:
            return None
        try:
            return date(parts[0], parts[1], parts[2])
        except ValueError:
            return None
    if isinstance(value, (int, float)):
        # epoch millis (used by a few audit fields)
        try:
            seconds = float(value) / 1000.0 if float(value) > 1e11 else float(value)
            return datetime.fromtimestamp(seconds, tz=timezone.utc).date()
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        for fmt in ("%Y-%m-%d", "%d %B %Y", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(text[:len(fmt) + 8], fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


def parse_timestamp(value: Any) -> datetime | None:
    """Parse a Fineract timestamp into a timezone-aware ``datetime``."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (list, tuple)) and len(value) >= 6:
        try:
            return datetime(*[int(p) for p in value[:6]], tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return None
    if isinstance(value, (list, tuple)):
        d = parse_date(value)
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc) if d else None
    if isinstance(value, (int, float)):
        try:
            seconds = float(value) / 1000.0 if float(value) > 1e11 else float(value)
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            d = parse_date(value)
            return datetime(d.year, d.month, d.day, tzinfo=timezone.utc) if d else None
    return None


# ---------------------------------------------------------------------
# Scalars
# ---------------------------------------------------------------------
def parse_decimal(value: Any) -> Decimal | None:
    """Money-safe numeric parse. Never float - these are ledger amounts."""
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def parse_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None


def parse_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def parse_text(value: Any, max_length: int | None = None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    return text[:max_length] if max_length else text


# ---------------------------------------------------------------------
# Nested access
# ---------------------------------------------------------------------
def dig(payload: Mapping[str, Any], *path: str, default: Any = None) -> Any:
    """Safely walk a nested dict: ``dig(loan, 'summary', 'totalOutstanding')``."""
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping) or key not in current:
            return default
        current = current[key]
    return current if current is not None else default


def enum_value(payload: Mapping[str, Any], key: str, attribute: str = "value") -> str | None:
    """Fineract enums are ``{"id": 300, "code": "...", "value": "Active"}``."""
    node = payload.get(key)
    if isinstance(node, Mapping):
        return parse_text(node.get(attribute))
    return parse_text(node)


def enum_id(payload: Mapping[str, Any], key: str) -> int | None:
    node = payload.get(key)
    if isinstance(node, Mapping):
        return parse_int(node.get("id"))
    return parse_int(node)


# ---------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------
def payload_hash(record: Mapping[str, Any], exclude: Iterable[str] = ()) -> str:
    """Stable SHA-256 over the business columns of a mapped record.

    Used by the loader to turn "the API returned this row again" into a
    no-op UPDATE. That keeps the Postgres WAL - and therefore the CDC
    stream and everything downstream of it - free of empty churn, which
    is the difference between a CDC pipeline that idles at ~0 events/s
    and one that replays the whole book every 15 minutes.
    """
    excluded = set(exclude) | {"_ingested_at", "_updated_at", "_payload_hash"}
    material = {k: v for k, v in sorted(record.items()) if k not in excluded}
    encoded = json.dumps(material, sort_keys=True, default=_json_default,
                         separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def chunked(sequence: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for start in range(0, len(sequence), size):
        yield sequence[start:start + size]
