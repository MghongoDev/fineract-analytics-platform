# =====================================================================
# Fineract analytics platform - operator entry point.
#
# This is the ONE place a human (or CI) drives the whole stack from.
# Every target is a thin wrapper around docker compose / the repo's own
# scripts, so what runs here is exactly what runs in CI and in a
# developer's terminal - no hidden logic lives only in someone's head.
#
# Requires: docker compose v2, python3, psql, curl.
# =====================================================================
SHELL := /bin/bash
.DEFAULT_GOAL := help
.SUPPRESS_UNUSED := TRUE

COMPOSE       ?= docker compose
COMPOSE_FILE  ?= docker-compose.yml
PYTHON        ?= python3

# ---- connection defaults (override via .env or the environment) ------
POSTGRES_HOST     ?= localhost
POSTGRES_PORT     ?= 5432
POSTGRES_DB       ?= fineract_oltp
POSTGRES_USER     ?= postgres
POSTGRES_PASSWORD ?= postgres

CLICKHOUSE_HOST      ?= localhost
CLICKHOUSE_HTTP_PORT ?= 8123
CLICKHOUSE_TCP_PORT  ?= 9000
CLICKHOUSE_USER      ?= analytics
CLICKHOUSE_PASSWORD  ?= analytics

FINERACT_HOST ?= localhost
FINERACT_PORT ?= 8443

KAFKA_CONNECT_URL ?= http://localhost:8083

AIRFLOW_HOST ?= localhost
AIRFLOW_PORT ?= 8085
AIRFLOW_USER ?= admin
AIRFLOW_PASSWORD ?= admin

GRAFANA_HOST ?= localhost
GRAFANA_PORT ?= 3000
GRAFANA_USER ?= admin
GRAFANA_PASSWORD ?= admin

PROMETHEUS_HOST ?= localhost
PROMETHEUS_PORT ?= 9090

EXPORTER_HOST ?= localhost
EXPORTER_PORT ?= 9105

BOOTSTRAP_TIMEOUT ?= 180

.PHONY: help up up-core down clean logs ps bootstrap \
        ingest ingest-mock cdc-register cdc-status cdc-lag cdc-restart \
        dbt-run dbt-test dbt-docs dbt-build \
        test test-integration validate lint format \
        airflow-trigger psql clickhouse-client metrics urls

help: ## Show this help (self-documented from the double-hash comments below)
	@echo "Fineract analytics platform - available targets:"
	@echo
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---------------------------------------------------------------------
# Stack lifecycle
# ---------------------------------------------------------------------

up: ## Start the whole stack (all services in docker-compose.yml)
	$(COMPOSE) -f $(COMPOSE_FILE) up -d
	@$(MAKE) --no-print-directory ps

up-core: ## Start only the core data plane (postgres, kafka, kafka-connect, clickhouse) - no source, no observability
	# `fineract` is deliberately absent: it lives behind the `fineract`
	# profile, so naming it here would fail on a stack started without it.
	$(COMPOSE) -f $(COMPOSE_FILE) up -d postgres kafka kafka-connect clickhouse
	@$(MAKE) --no-print-directory ps

down: ## Stop the stack, keeping volumes (data survives)
	$(COMPOSE) -f $(COMPOSE_FILE) down

clean: ## Stop the stack and remove volumes (irreversible - drops all local data)
	$(COMPOSE) -f $(COMPOSE_FILE) down -v --remove-orphans

logs: ## Tail logs for every service (Ctrl-C to stop; SERVICE=<name> to scope to one)
	$(COMPOSE) -f $(COMPOSE_FILE) logs -f --tail=200 $(SERVICE)

ps: ## Show status of every service in the stack
	$(COMPOSE) -f $(COMPOSE_FILE) ps

bootstrap: up-core ## Wait for health, apply ClickHouse init SQL, register CDC connectors, seed data
	@echo "[bootstrap] waiting for postgres, clickhouse and kafka-connect to be healthy..."
	@deadline=$$(( $$(date +%s) + $(BOOTSTRAP_TIMEOUT) )); \
	for svc in postgres clickhouse kafka-connect; do \
		until [ "$$($(COMPOSE) -f $(COMPOSE_FILE) ps -q $$svc | xargs -r docker inspect -f '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do \
			if [ "$$(date +%s)" -ge "$$deadline" ]; then echo "[bootstrap] $$svc did not become healthy in $(BOOTSTRAP_TIMEOUT)s" >&2; exit 1; fi; \
			echo "[bootstrap] waiting for $$svc..."; sleep 3; \
		done; \
	done
	@echo "[bootstrap] applying ClickHouse init SQL"
	@for f in platform/clickhouse/init/*.sql; do \
		echo "  -> $$f"; \
		curl -fsS "http://$(CLICKHOUSE_HOST):$(CLICKHOUSE_HTTP_PORT)/?user=$(CLICKHOUSE_USER)&password=$(CLICKHOUSE_PASSWORD)" --data-binary @$$f; \
	done
	@$(MAKE) --no-print-directory cdc-register
	@echo "[bootstrap] seeding via ingestion (mock server)"
	@$(MAKE) --no-print-directory ingest-mock
	@echo "[bootstrap] done."

# ---------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------

ingest: ## Run the real ingestion pipeline against the configured FINERACT_BASE_URL
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm ingestion python -m fineract_ingest ingest --all

ingest-mock: ## Start the mock Fineract server and run ingestion against it (offline, deterministic)
	$(COMPOSE) -f $(COMPOSE_FILE) up -d fineract-mock || true
	$(COMPOSE) -f $(COMPOSE_FILE) run --rm \
		-e FINERACT_BASE_URL=http://fineract-mock:8090/fineract-provider/api/v1 \
		ingestion python -m fineract_ingest ingest --all

# ---------------------------------------------------------------------
# CDC / Debezium
# ---------------------------------------------------------------------

cdc-register: ## Register (or reconcile) Debezium connectors against Kafka Connect
	CONNECT_URL=$(KAFKA_CONNECT_URL) bash cdc/scripts/register-connectors.sh

cdc-status: ## Show status of every registered Debezium connector and its tasks
	@curl -fsS "$(KAFKA_CONNECT_URL)/connectors?expand=status" | python3 -m json.tool

cdc-lag: ## Report replication slot lag from Postgres (bytes and approximate seconds)
	@PGPASSWORD=$(POSTGRES_PASSWORD) psql -h $(POSTGRES_HOST) -p $(POSTGRES_PORT) -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
		"SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag_bytes FROM pg_replication_slots;"

cdc-restart: ## Restart every registered Debezium connector (and its tasks)
	@for name in $$(curl -fsS "$(KAFKA_CONNECT_URL)/connectors" | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))"); do \
		echo "restarting $$name"; \
		curl -fsS -X POST "$(KAFKA_CONNECT_URL)/connectors/$$name/restart?includeTasks=true&onlyFailed=false"; \
	done

# ---------------------------------------------------------------------
# dbt
# ---------------------------------------------------------------------

dbt-run: ## Run all dbt models against the dev target
	cd transform/fineract_analytics && dbt run

dbt-test: ## Run dbt data tests against the dev target
	cd transform/fineract_analytics && dbt test

dbt-docs: ## Generate and serve dbt docs locally
	cd transform/fineract_analytics && dbt docs generate && dbt docs serve --port 8081

dbt-build: ## Run dbt build (models + tests + seeds + snapshots, in DAG order)
	cd transform/fineract_analytics && dbt build

# ---------------------------------------------------------------------
# Tests & validation
# ---------------------------------------------------------------------

test: ## Run unit tests (ingestion + repo-level unit tests) with coverage
	$(PYTHON) -m pytest ingestion/tests tests/unit -v --cov=fineract_ingest --cov-report=term

test-integration: ## Run the integration test suite (requires a running stack; see `make bootstrap`)
	$(PYTHON) -m pytest tests/integration -v

validate: ## Run the offline validation scripts (embedded-ClickHouse DDL + full dbt DAG build)
	$(PYTHON) scripts/validate_clickhouse_sql.py
	$(PYTHON) scripts/validate_dbt_sql.py --verbose

lint: ## Run ruff check over ingestion, orchestration, observability, scripts
	ruff check ingestion orchestration observability scripts

format: ## Auto-format ingestion, orchestration, observability, scripts with ruff
	ruff format ingestion orchestration observability scripts

# ---------------------------------------------------------------------
# Airflow
# ---------------------------------------------------------------------

airflow-trigger: ## Trigger a DAG run (default: fineract_analytics_pipeline; override with DAG=<dag_id>)
	@$(COMPOSE) -f $(COMPOSE_FILE) ps -q airflow-webserver >/dev/null 2>&1 || \
		$(COMPOSE) -f $(COMPOSE_FILE) up -d airflow-webserver
	$(COMPOSE) -f $(COMPOSE_FILE) exec airflow-webserver \
		airflow dags trigger $(or $(DAG),fineract_analytics_pipeline)

# ---------------------------------------------------------------------
# Shells / clients
# ---------------------------------------------------------------------

psql: ## Open a psql shell against the OLTP Postgres database
	PGPASSWORD=$(POSTGRES_PASSWORD) psql -h $(POSTGRES_HOST) -p $(POSTGRES_PORT) -U $(POSTGRES_USER) -d $(POSTGRES_DB)

clickhouse-client: ## Open a clickhouse-client shell against ClickHouse
	$(COMPOSE) -f $(COMPOSE_FILE) exec clickhouse clickhouse-client \
		--user $(CLICKHOUSE_USER) --password $(CLICKHOUSE_PASSWORD)

metrics: ## Print current pipeline exporter metrics
	@curl -fsS "http://$(EXPORTER_HOST):$(EXPORTER_PORT)/metrics"

urls: ## Print every UI URL in the stack, with credentials
	@echo "Fineract API      : https://$(FINERACT_HOST):$(FINERACT_PORT)/fineract-provider/api/v1  (user: mifos / password: password)"
	@echo "Postgres          : postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)"
	@echo "Kafka             : $(FINERACT_HOST):9092"
	@echo "Kafka Connect API : $(KAFKA_CONNECT_URL)/connectors"
	@echo "ClickHouse HTTP   : http://$(CLICKHOUSE_HOST):$(CLICKHOUSE_HTTP_PORT)  (user: $(CLICKHOUSE_USER) / password: $(CLICKHOUSE_PASSWORD))"
	@echo "ClickHouse native : $(CLICKHOUSE_HOST):$(CLICKHOUSE_TCP_PORT)"
	@echo "Airflow           : http://$(AIRFLOW_HOST):$(AIRFLOW_PORT)  (user: $(AIRFLOW_USER) / password: $(AIRFLOW_PASSWORD))"
	@echo "Prometheus        : http://$(PROMETHEUS_HOST):$(PROMETHEUS_PORT)"
	@echo "Grafana           : http://$(GRAFANA_HOST):$(GRAFANA_PORT)  (user: $(GRAFANA_USER) / password: $(GRAFANA_PASSWORD))"
	@echo "Pipeline exporter : http://$(EXPORTER_HOST):$(EXPORTER_PORT)/metrics"
	@echo
	@echo "Defaults above come from Makefile variables; override any of them"
	@echo "via the environment or a .env file (e.g. POSTGRES_PASSWORD=... make urls)."
