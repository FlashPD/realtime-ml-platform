FROM apache/airflow:3.3.1-python3.12

WORKDIR /opt/tripml

COPY --chown=airflow:root pyproject.toml README.md LICENSE ./
COPY --chown=airflow:root src ./src

# Requiring the base version in the same transaction prevents project dependencies from
# silently changing the Airflow runtime selected for the platform image.
RUN pip install --no-cache-dir "apache-airflow==3.3.1" '.[transformation]'

COPY --chown=airflow:root dags /opt/airflow/dags

WORKDIR /opt/airflow
