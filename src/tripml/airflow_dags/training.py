"""Manually triggered, registry-gated model training DAG."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, task

from tripml.settings import load_settings
from tripml.tracking import run_training_workflow


def execute_training_task(config_path: str | None) -> dict[str, object]:
    """Run deterministic training and publish its guarded MLflow result."""

    settings = load_settings(Path(config_path) if config_path else None)
    result = run_training_workflow(settings)
    return result.model_dump(mode="json")


@dag(
    dag_id="tripml_training",
    description="Train, evaluate, and conditionally register the trip-duration model",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    render_template_as_native_obj=True,
    params={
        "config_path": Param(
            None,
            type=["null", "string"],
            description="Optional configuration file visible to the Airflow worker",
        )
    },
    tags=["tripml", "training", "mlflow"],
)
def _tripml_training() -> None:
    @task(
        task_id="train_and_register",
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=90),
    )
    def train_and_register(config_path: str | None) -> dict[str, object]:
        return execute_training_task(config_path)

    train_and_register("{{ params.config_path }}")


tripml_training_dag = _tripml_training()
