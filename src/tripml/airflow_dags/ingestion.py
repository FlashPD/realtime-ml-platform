"""Manually triggered, quality-gated TLC ingestion DAG."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, task
from airflow.sdk.exceptions import AirflowFailException

from tripml.ingestion import PartitionStatus
from tripml.workflows.ingestion import run_ingestion


def execute_ingestion_task(month: str, config_path: str | None) -> dict[str, object]:
    """Execute the shared workflow and map quarantine to a non-retryable Airflow failure."""

    execution = run_ingestion(
        month,
        config_path=Path(config_path) if config_path else None,
        require_lineage=True,
    )
    if execution.report.status is PartitionStatus.QUARANTINED:
        raise AirflowFailException(
            f"partition {month} exceeded its quality threshold; see lineage run "
            f"{execution.lineage_run_id}"
        )
    return execution.as_document()


@dag(
    dag_id="tripml_ingestion",
    description="Download and quality-gate one NYC TLC yellow-taxi partition",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    render_template_as_native_obj=True,
    params={
        "month": Param(
            "2024-01",
            type="string",
            pattern=r"^20[0-9]{2}-(0[1-9]|1[0-2])$",
            description="TLC source month in YYYY-MM format",
        ),
        "config_path": Param(
            None,
            type=["null", "string"],
            description="Optional configuration file visible to the Airflow worker",
        ),
    },
    tags=["tripml", "batch", "ingestion"],
)
def _tripml_ingestion() -> None:
    @task(
        task_id="ingest_partition",
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=20),
    )
    def ingest_partition(month: str, config_path: str | None) -> dict[str, object]:
        return execute_ingestion_task(month, config_path)

    ingest_partition("{{ params.month }}", "{{ params.config_path }}")


tripml_ingestion_dag = _tripml_ingestion()
