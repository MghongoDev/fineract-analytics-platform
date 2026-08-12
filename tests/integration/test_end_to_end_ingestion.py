"""End-to-end ingestion against a real Postgres.

These are the tests that would have caught every bug worth catching in
the loader: idempotency, churn suppression, transactional watermarks and
quarantine behaviour. None of them can be tested against a mock, because
what is being tested *is* the database interaction.

Skipped automatically when POSTGRES_HOST is unset.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def settings(postgres_available, mock_fineract_url):
    if not postgres_available:
        pytest.skip("POSTGRES_HOST not set - integration tests skipped")
    from fineract_ingest.config import Settings

    os.environ["INGEST_PUSH_METRICS"] = "false"
    os.environ["FINERACT_RPS"] = "0"
    return Settings.load()


@pytest.fixture(scope="module")
def loader(settings):
    from fineract_ingest.loader import PostgresLoader

    instance = PostgresLoader(settings.postgres)
    instance.connect()
    yield instance
    instance.close()


@pytest.fixture(scope="module")
def first_run(settings):
    """One full ingestion pass, shared by the tests below."""
    from fineract_ingest.pipeline import IngestionPipeline

    pipeline = IngestionPipeline(settings)
    try:
        outcomes = pipeline.run(
            entities=["offices", "staff", "loan_products", "savings_products",
                      "clients", "loans", "savings_accounts"])
    finally:
        pipeline.close()
    return {outcome.entity: outcome for outcome in outcomes}


class TestFirstLoad:
    def test_every_entity_succeeds(self, first_run):
        failed = [name for name, outcome in first_run.items() if not outcome.ok]
        assert not failed, f"entities failed: {failed}"

    def test_rows_actually_landed(self, first_run, loader):
        assert loader.table_count("oltp.clients") == 120
        assert loader.table_count("oltp.loans") == 200
        assert loader.table_count("oltp.offices") == 6

    def test_nothing_was_rejected(self, first_run):
        assert all(o.result.rows_rejected == 0 for o in first_run.values())

    def test_watermarks_were_written(self, first_run, loader):
        for entity in first_run:
            watermark = loader.read_watermark(entity)
            assert watermark is not None, f"no watermark for {entity}"
            assert watermark["last_success_at"] is not None
            assert watermark["last_row_count"] > 0

    def test_run_history_was_recorded(self, loader):
        rows = loader.connect().execute(
            "SELECT entity, status FROM meta.v_latest_ingestion_run").fetchall()
        assert rows
        assert all(status == "success" for _entity, status in rows)


class TestIdempotency:
    def test_a_second_identical_run_writes_nothing(self, settings, first_run):
        """The core guarantee: re-running an unchanged batch produces zero
        row versions, and therefore zero WAL, and therefore zero CDC
        events. Without this, every scheduled run would replay the whole
        book downstream."""
        from fineract_ingest.pipeline import IngestionPipeline

        pipeline = IngestionPipeline(settings)
        try:
            outcomes = pipeline.run(entities=list(first_run))
        finally:
            pipeline.close()

        for outcome in outcomes:
            assert outcome.ok
            assert outcome.result.rows_inserted == 0, (
                f"{outcome.entity} re-inserted rows on an unchanged run")
            assert outcome.result.rows_updated == 0, (
                f"{outcome.entity} rewrote unchanged rows - the payload hash "
                f"is not suppressing churn")
            assert outcome.result.rows_unchanged > 0

    def test_row_counts_are_stable_after_a_re_run(self, loader):
        assert loader.table_count("oltp.clients") == 120
        assert loader.table_count("oltp.loans") == 200

    def test_a_changed_record_is_updated(self, settings, loader, first_run):
        """Mutate the landed row, re-ingest, and confirm the source value
        wins - which proves the update path is live and not just being
        skipped by the hash."""
        connection = loader.connect()
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE oltp.clients SET display_name = 'STALE', "
                "_payload_hash = 'forced' WHERE client_id = 1")
        connection.commit()

        from fineract_ingest.pipeline import IngestionPipeline

        pipeline = IngestionPipeline(settings)
        try:
            outcomes = pipeline.run(entities=["clients"])
        finally:
            pipeline.close()

        assert outcomes[0].result.rows_updated == 1
        with connection.cursor() as cursor:
            cursor.execute("SELECT display_name FROM oltp.clients WHERE client_id = 1")
            assert cursor.fetchone()[0] != "STALE"
        connection.commit()


class TestCdcReadiness:
    def test_publication_covers_every_landed_table(self, loader):
        connection = loader.connect()
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT tablename FROM pg_publication_tables "
                "WHERE pubname = 'fineract_cdc_pub' AND schemaname = 'oltp'")
            published = {row[0] for row in cursor.fetchall()}
        connection.commit()
        expected = {"offices", "staff", "loan_products", "savings_products",
                    "clients", "loans", "loan_transactions", "savings_accounts"}
        assert expected.issubset(published), (
            f"tables missing from the CDC publication: {expected - published}")

    def test_wal_level_is_logical(self, loader):
        connection = loader.connect()
        with connection.cursor() as cursor:
            cursor.execute("SHOW wal_level")
            assert cursor.fetchone()[0] == "logical"
        connection.commit()

    def test_replica_identity_is_full_on_the_dimension_tables(self, loader):
        """FULL is what gives Debezium a complete before-image. The high
        volume transaction table is deliberately excluded."""
        connection = loader.connect()
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT c.relname, c.relreplident FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'oltp'")
            identities = dict(cursor.fetchall())
        connection.commit()
        assert identities["clients"] == "f"
        assert identities["loans"] == "f"
        assert identities["loan_transactions"] == "d"

    def test_heartbeat_row_exists_and_can_be_advanced(self, loader):
        loader.touch_heartbeat()
        connection = loader.connect()
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM cdc.debezium_heartbeat")
            assert cursor.fetchone()[0] == 1
        connection.commit()


class TestQuarantine:
    def test_a_malformed_record_is_quarantined_and_the_batch_survives(
            self, settings, loader):
        """One bad record must not fail a good batch, and must not
        disappear either."""
        from fineract_ingest.entities import ENTITIES
        from fineract_ingest.loader import new_batch_id
        from fineract_ingest.pipeline import IngestionPipeline

        pipeline = IngestionPipeline(settings)
        spec = ENTITIES["clients"]
        rows, rejects = pipeline._map_and_validate(
            spec, [{"displayName": "no id at all"}])
        assert rows == []
        assert len(rejects) == 1

        connection = loader.connect()
        batch_id = new_batch_id()
        written = loader.record_rejects(connection, batch_id, rejects)
        connection.commit()
        assert written == 1

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT rule, payload FROM meta.ingestion_reject "
                "WHERE batch_id = %s", (str(batch_id),))
            rule, payload = cursor.fetchone()
        connection.commit()
        assert rule == "primary_key_not_null"
        assert payload["displayName"] == "no id at all"
        pipeline.close()


class TestParentDrivenIngestion:
    @pytest.mark.slow
    def test_loan_transactions_crawl_the_landed_loan_ids(self, settings, first_run):
        from fineract_ingest.pipeline import IngestionPipeline

        pipeline = IngestionPipeline(settings)
        try:
            outcome = pipeline.run_entity("loan_transactions", parent_limit=25)
        finally:
            pipeline.close()

        assert outcome.ok
        assert outcome.result.rows_read > 0, (
            "the child crawl read nothing - parent ids were not resolved")
