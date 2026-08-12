"""Pipeline exporter: a Prometheus exporter that scrapes the control plane
(Postgres ``meta.*``) and the warehouse observability views
(ClickHouse ``fineract_ops.*``) and republishes them under the
``fineract_`` namespace.

Everything else in this platform is either a one-shot batch job (the
ingestion job pushes to a Pushgateway) or already speaks Prometheus
natively (ClickHouse, Kafka Connect via the JMX exporter). This process
exists for the metrics that only make sense as a *query result*: a
reconciliation delta, a replication slot's lag, a dbt run's outcome.
"""

from .config import ExporterConfig
from .exporter import PipelineExporter

__all__ = ["ExporterConfig", "PipelineExporter"]
