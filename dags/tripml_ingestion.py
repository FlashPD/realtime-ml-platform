"""Airflow discovery entry point for the TripML ingestion DAG."""

from tripml.airflow_dags.ingestion import tripml_ingestion_dag

__all__ = ["tripml_ingestion_dag"]
