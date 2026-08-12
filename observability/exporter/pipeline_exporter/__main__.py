"""Entry point: ``python -m pipeline_exporter``."""

from __future__ import annotations

import logging
import signal
import sys

from .config import ExporterConfig
from .exporter import PipelineExporter


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("pipeline_exporter")

    cfg = ExporterConfig()
    exporter = PipelineExporter(cfg)

    def _shutdown(signum, frame):  # noqa: ANN001 - signal handler signature
        log.info("shutting_down", extra={"signal": signum})
        exporter.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("starting_exporter", extra={
        "port": cfg.port,
        "scrape_interval_seconds": cfg.scrape_interval_seconds,
    })
    exporter.start()

    # start_http_server runs its own daemon thread; block the main thread
    # here so the process stays alive until a signal arrives.
    signal.pause()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
