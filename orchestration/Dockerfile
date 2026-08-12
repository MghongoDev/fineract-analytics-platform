# =====================================================================
# Airflow image: scheduler, webserver and the dbt runtime in one image.
#
# dbt lives in the same image as Airflow rather than in a sidecar because
# the DbtOperator shells out to `dbt` locally. The alternative (a separate
# dbt container invoked over Docker/Kubernetes) is the right answer at
# production scale and the operator's CLI contract makes that swap a
# config change - but for a single-command local stack, one image is one
# fewer moving part.
#
# The ingestion package is installed here too, so the pipeline DAG can
# call it in-process and its logs land in the Airflow task log.
# =====================================================================
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential libpq-dev curl git \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow

COPY orchestration/requirements.txt /tmp/orchestration-requirements.txt
RUN pip install --no-cache-dir -r /tmp/orchestration-requirements.txt

# The ingestion package - installed non-editable so the image is
# self-contained and reproducible.
COPY --chown=airflow:root ingestion /tmp/ingestion
RUN pip install --no-cache-dir /tmp/ingestion

# dbt project. Mounted over in local development (see docker-compose.yml)
# so a model edit does not need an image rebuild; baked in here so the
# image is deployable on its own.
COPY --chown=airflow:root transform/fineract_analytics /opt/airflow/dbt

ENV DBT_PROJECT_DIR=/opt/airflow/dbt \
    DBT_PROFILES_DIR=/opt/airflow/dbt \
    PYTHONPATH=/opt/airflow/plugins:/opt/airflow/dags \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/airflow
