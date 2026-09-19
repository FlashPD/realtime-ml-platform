"""Opt-in HTTP-to-Redpanda delivery verification using an isolated temporary topic."""

from __future__ import annotations

import os
from time import monotonic
from unittest.mock import MagicMock
from uuid import uuid4

import lightgbm as lgb
import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi.testclient import TestClient

from tripml.contracts import Prediction
from tripml.publication import KafkaPredictionPublisher
from tripml.serving import ServingModel, create_app
from tripml.settings import PublicationSettings

pytestmark = pytest.mark.integration


def test_http_prediction_is_consumable_with_identical_contract_and_key() -> None:
    brokers = os.getenv("TRIPML_TEST_KAFKA_BOOTSTRAP_SERVERS")
    if brokers is None:
        pytest.skip("TRIPML_TEST_KAFKA_BOOTSTRAP_SERVERS is not configured")
    topic = f"tripml-publication-test-{uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": brokers})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(15)
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"test-{uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    publisher = None
    try:
        publisher = KafkaPredictionPublisher.from_settings(
            PublicationSettings(
                bootstrap_servers=brokers,
                topic=topic,
                delivery_timeout_ms=5000,
                ack_timeout_seconds=6,
            )
        )
        # Model correctness is tested with native artifacts elsewhere; isolate transport here.
        booster = MagicMock(spec=lgb.Booster)
        booster.predict.return_value = [600.0]
        model = ServingModel(booster, "integration-static", None)
        with TestClient(create_app(model_loader=lambda: model, publisher=publisher)) as client:
            response = client.post(
                "/v1/eta",
                json={
                    "trip_id": "broker-test-trip",
                    "pickup_zone_id": 161,
                    "dropoff_zone_id": 236,
                    "pickup_time": "2024-04-15T12:00:00-04:00",
                    "trip_distance_miles": 3.2,
                    "passenger_count": 2,
                },
            )
            assert response.status_code == 200, response.text
            assert response.headers["X-TripML-Publication"] == "acknowledged"
            expected = Prediction.model_validate(response.json())
        consumer.assign([TopicPartition(topic, partition, 0) for partition in range(3)])
        deadline = monotonic() + 10
        while monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            assert message.error() is None
            assert message.key() == b"broker-test-trip"
            assert Prediction.model_validate_json(message.value()) == expected
            headers = dict(message.headers())
            assert headers["schema-version"] == b"1.0"
            assert headers["prediction-id"] == str(expected.prediction_id).encode()
            assert message.timestamp()[1] == int(expected.served_at.timestamp() * 1000)
            break
        else:
            pytest.fail("acknowledged prediction was not consumed before the deadline")
    finally:
        try:
            consumer.close()
            if publisher is not None:
                publisher.close()
        finally:
            admin.delete_topics([topic])[topic].result(15)
