from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tripml.airflow_dags import training as dag_module


def test_training_dag_has_bounded_execution_policy() -> None:
    airflow_dag = dag_module.tripml_training_dag

    assert airflow_dag.dag_id == "tripml_training"
    assert airflow_dag.schedule is None
    assert airflow_dag.catchup is False
    assert airflow_dag.max_active_runs == 1
    assert airflow_dag.params.dump() == {"config_path": None}
    assert airflow_dag.task_ids == ["train_and_register"]
    task = airflow_dag.get_task("train_and_register")
    assert task.retries == 1
    assert task.retry_delay.total_seconds() == 120
    assert task.execution_timeout is not None
    assert task.execution_timeout.total_seconds() == 90 * 60


def test_training_task_returns_small_json_result(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeWorkflow:
        def model_dump(self, *, mode: str) -> dict[str, object]:
            assert mode == "json"
            return {
                "training": {"run_id": "0123456789abcdef"},
                "tracking": {
                    "registered_version": "1",
                    "production_alias_version": "1",
                    "published_at": datetime(2024, 5, 1, tzinfo=UTC).isoformat(),
                },
            }

    monkeypatch.setattr(
        dag_module, "run_training_workflow", lambda *_args, **_kwargs: FakeWorkflow()
    )

    result = dag_module.execute_training_task(None)

    assert result["training"] == {"run_id": "0123456789abcdef"}
    assert result["tracking"]["production_alias_version"] == "1"  # type: ignore[index]
