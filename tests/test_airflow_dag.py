from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from airflow.sdk.exceptions import AirflowFailException

from tripml.airflow_dags import ingestion as dag_module
from tripml.features import FeatureBuildReport
from tripml.ingestion import PartitionQualityReport, PartitionStatus
from tripml.workflows.ingestion import IngestionExecution

RUN_ID = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")


def _execution(status: PartitionStatus) -> IngestionExecution:
    invalid_rows = 1 if status is PartitionStatus.ACCEPTED else 5
    return IngestionExecution(
        report=PartitionQualityReport(
            month="2024-01",
            status=status,
            total_rows=10,
            valid_rows=10 - invalid_rows,
            invalid_rows=invalid_rows,
            violation_rate=invalid_rows / 10,
            max_violation_rate=0.2,
            checks=(),
            source_path="data/bronze/source.parquet",
            source_sha256="a" * 64,
            silver_path=(
                "data/silver/trips.parquet" if status is PartitionStatus.ACCEPTED else None
            ),
            quarantine_path="data/quarantine/source.parquet",
            evaluated_at=datetime(2024, 1, 31, tzinfo=UTC),
        ),
        lineage_run_id=RUN_ID,
    )


def test_ingestion_dag_has_bounded_retry_and_concurrency_policy() -> None:
    airflow_dag = dag_module.tripml_ingestion_dag

    assert airflow_dag.dag_id == "tripml_ingestion"
    assert airflow_dag.schedule is None
    assert airflow_dag.catchup is False
    assert airflow_dag.max_active_runs == 1
    assert airflow_dag.params.dump() == {"month": "2024-01", "config_path": None}
    assert airflow_dag.params.get_param("month").schema["pattern"].startswith("^20")
    assert airflow_dag.task_ids == ["ingest_partition", "build_gold_features"]
    task = airflow_dag.get_task("ingest_partition")
    assert task.retries == 2
    assert task.retry_delay.total_seconds() == 60
    assert task.execution_timeout is not None
    assert task.execution_timeout.total_seconds() == 20 * 60
    feature_task = airflow_dag.get_task("build_gold_features")
    assert feature_task.retries == 1
    assert feature_task.execution_timeout is not None
    assert feature_task.execution_timeout.total_seconds() == 15 * 60
    assert task.downstream_task_ids == {"build_gold_features"}


def test_airflow_task_returns_small_json_result(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        dag_module, "run_ingestion", lambda *_args, **_kwargs: _execution(PartitionStatus.ACCEPTED)
    )

    result = dag_module.execute_ingestion_task("2024-01", None)

    assert result["status"] == "accepted"
    assert result["lineage_run_id"] == str(RUN_ID)


def test_quarantine_fails_without_retrying_transient_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        dag_module,
        "run_ingestion",
        lambda *_args, **_kwargs: _execution(PartitionStatus.QUARANTINED),
    )

    with pytest.raises(AirflowFailException, match=str(RUN_ID)):
        dag_module.execute_ingestion_task("2024-01", None)


def test_airflow_feature_task_returns_manifest_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = FeatureBuildReport(
        month="2024-01",
        model_version="gold-features-v1",
        output_path="data/gold/yellow/month=2024-01/training_features.parquet",
        manifest_path="data/gold/yellow/month=2024-01/manifest.json",
        row_count=10,
        output_sha256="b" * 64,
        output_size_bytes=100,
        input_rows=10,
        inputs=(),
        short_window_seconds=900,
        long_window_seconds=3600,
        config_fingerprint="a" * 64,
        built_at=datetime(2024, 2, 1, tzinfo=UTC),
    )
    monkeypatch.setattr(dag_module, "build_gold_features", lambda *_args, **_kwargs: report)

    result = dag_module.execute_feature_task("2024-01", None)

    assert result["row_count"] == 10
    assert result["model_version"] == "gold-features-v1"
