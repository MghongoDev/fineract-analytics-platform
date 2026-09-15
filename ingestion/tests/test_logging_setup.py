"""Tests for structured logging: the JSON formatter, context filter and
log-level/fmt configuration."""

from __future__ import annotations

import json
import logging

from fineract_ingest.logging_setup import JsonFormatter, configure, get_logger


def _record(message: str = "hello", **extra_attrs) -> logging.LogRecord:
    record = logging.LogRecord(
        name="fineract_ingest.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg=message,
        args=(),
        exc_info=None,
    )
    for key, value in extra_attrs.items():
        setattr(record, key, value)
    return record


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(self.format(record))


class TestJsonFormatter:
    def test_basic_fields_present(self):
        payload = json.loads(JsonFormatter().format(_record("hello")))
        assert payload["message"] == "hello"
        assert payload["level"] == "INFO"
        assert payload["logger"] == "fineract_ingest.test"
        assert "ts" in payload

    def test_extra_fields_are_inlined(self):
        payload = json.loads(JsonFormatter().format(_record("hello", batch_id="abc")))
        assert payload["batch_id"] == "abc"

    def test_underscore_prefixed_fields_are_omitted(self):
        payload = json.loads(JsonFormatter().format(_record("hello", _private=True)))
        assert "_private" not in payload

    def test_exc_info_is_formatted(self):
        try:
            raise ValueError("boom")
        except ValueError:
            record = logging.LogRecord(
                name="x",
                level=logging.ERROR,
                pathname=__file__,
                lineno=1,
                msg="failed",
                args=(),
                exc_info=__import__("sys").exc_info(),
            )
        payload = json.loads(JsonFormatter().format(record))
        assert "ValueError: boom" in payload["exception"]


class TestConfiguration:
    def test_json_format_installs_a_json_formatter(self):
        import sys

        root = logging.getLogger()
        root.handlers.clear()
        configure("JSON", "json")
        handler = root.handlers[0]
        assert isinstance(handler, logging.StreamHandler)
        assert isinstance(handler.formatter, JsonFormatter)
        assert handler.stream is sys.stdout
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def test_configure_accepts_lowercase_level(self):
        root = logging.getLogger()
        root.handlers.clear()
        configure("debug", "json")
        assert root.level == logging.DEBUG
        root.handlers.clear()
        root.setLevel(logging.WARNING)


class TestLogger:
    def test_get_logger_returns_a_named_logger(self):
        logger = get_logger("fineract_ingest.test")
        assert logger.name == "fineract_ingest.test"

    def test_get_logger_defaults_to_the_package_name(self):
        assert get_logger().name == "fineract_ingest"


class TestContext:
    def test_bound_values_are_attached_to_a_record(self):
        from fineract_ingest.logging_setup import ContextFilter, bind

        bind(batch_id="abc-123")
        record = _record("hello")
        assert ContextFilter().filter(record) is True
        assert record.batch_id == "abc-123"

    def test_bound_values_do_not_overwrite_existing_fields(self):
        from fineract_ingest.logging_setup import ContextFilter, bind

        bind(batch_id="bound-value")
        record = _record("hello", batch_id="own-value")
        ContextFilter().filter(record)
        assert record.batch_id == "own-value"

    def test_none_values_are_not_bound(self):
        from fineract_ingest.logging_setup import bind

        bind(entity=None)
        record = _record("hello")
        assert not hasattr(record, "entity")

    def test_configure_wires_a_context_filter_into_json_output(self):

        root = logging.getLogger()
        root.handlers.clear()
        configure("INFO", "json")
        handler = root.handlers[0]
        capture = _Capture()
        capture.setFormatter(handler.formatter)
        capture.addFilter(handler.filters[0])

        from fineract_ingest.logging_setup import bind

        bind(environment="test-env")
        capture.handle(_record("hello"))
        payload = json.loads(capture.records[0])
        assert payload["environment"] == "test-env"
        root.handlers.clear()
        root.setLevel(logging.WARNING)
