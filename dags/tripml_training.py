"""Airflow discovery entry point for the TripML training DAG."""

from tripml.airflow_dags.training import tripml_training_dag

__all__ = ["tripml_training_dag"]
