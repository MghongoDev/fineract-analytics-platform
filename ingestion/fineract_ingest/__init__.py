"""Fineract -> Postgres ingestion service.

Public surface:

    from fineract_ingest import IngestionPipeline, Settings
    IngestionPipeline(Settings.load()).run(["clients", "loans"])
"""

from .client import FineractClient, FineractError
from .config import FineractConfig, PostgresConfig, RuntimeConfig, Settings
from .entities import DEFAULT_ORDER, ENTITIES, EntitySpec, get_entity
from .loader import PostgresLoader
from .pipeline import EntityOutcome, IngestionPipeline

__version__ = "1.0.0"

__all__ = [
    "FineractClient",
    "FineractError",
    "FineractConfig",
    "PostgresConfig",
    "RuntimeConfig",
    "Settings",
    "ENTITIES",
    "DEFAULT_ORDER",
    "EntitySpec",
    "get_entity",
    "PostgresLoader",
    "IngestionPipeline",
    "EntityOutcome",
    "__version__",
]
