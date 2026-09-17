from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from queue import Empty, Queue
from threading import Event
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from tripml.contracts import Prediction
from tripml.publication import (
    KafkaPredictionPublisher,
    PublicationError,
    PublicationOutcome,
    ensure_prediction_topic,
)
from tripml.settings import PublicationSettings


class FakeProducer:
    def __init__(self) -> None:
        self.messages: Queue[dict[str, Any]] = Queue()
        self.enqueued: list[dict[str, Any]] = []
        self.allow_delivery = Event()
        self.allow_delivery.set()
        self.enqueue_error: Exception | None = None
        self.delivery_error: KafkaError | None = None
        self.poll_error: Exception | None = None
        self.flushed: list[float] = []

    def produce(self, topic: str, **kwargs: object) -> None:
        if self.enqueue_error:
            raise self.enqueue_error
        message = {"topic": topic, **kwargs}
        self.enqueued.append(message)
        self.messages.put(message)

    def poll(self, timeout: float) -> None:
        if self.poll_error:
            raise self.poll_error
        if not self.allow_delivery.wait(timeout):
            return
        try:
            message = self.messages.get(timeout=timeout)
        except Empty:
            return
        message["on_delivery"](self.delivery_error, None)

    def flush(self, timeout: float) -> int:
        self.flushed.append(timeout)
        while not self.messages.empty():
            message = self.messages.get_nowait()
            message["on_delivery"](self.delivery_error, None)
        return 0


@pytest.fixture
def prediction() -> Prediction:
    return Prediction(
        trip_id="trip-123",
        model_version="bundle-streaming",
        features_used={"pu_zone_trips_15m": 5},
        feature_timestamps={"pu_zone_trips_15m": datetime(2024, 4, 15, 12, tzinfo=UTC)},
        feature_fallback=False,
        estimated_duration_seconds=600,
        served_at=datetime(2026, 9, 16, 12, tzinfo=UTC),
    )


@pytest.fixture
def publisher() -> Iterator[tuple[KafkaPredictionPublisher, FakeProducer]]:
    producer = FakeProducer()
    settings = PublicationSettings(delivery_timeout_ms=10, ack_timeout_seconds=0.1)
    active = KafkaPredictionPublisher(producer, settings)
    try:
        yield active, producer
    finally:
        active.close()


def test_acknowledged_message_preserves_contract_key_timestamp_and_headers(
    publisher: tuple[KafkaPredictionPublisher, FakeProducer],
    prediction: Prediction,
) -> None:
    active, producer = publisher
    active.publish(prediction)
    [message] = producer.enqueued
    assert message["topic"] == "predictions"
    assert message["key"] == b"trip-123"
    assert Prediction.model_validate_json(message["value"]) == prediction
    assert message["timestamp"] == int(prediction.served_at.timestamp() * 1000)
    assert message["headers"] == {
        "content-type": "application/json",
        "schema-version": "1.0",
        "prediction-id": str(prediction.prediction_id),
    }
    assert producer.flushed == []


@pytest.mark.parametrize(
    ("error", "outcome"),
    [
        (BufferError("full"), PublicationOutcome.QUEUE_FULL),
        (KafkaException(KafkaError(KafkaError._FAIL)), PublicationOutcome.FAILED),
    ],
)
def test_enqueue_failure_does_not_wait_for_delivery(
    publisher: tuple[KafkaPredictionPublisher, FakeProducer],
    prediction: Prediction,
    error: Exception,
    outcome: PublicationOutcome,
) -> None:
    active, producer = publisher
    producer.enqueue_error = error
    with pytest.raises(PublicationError) as raised:
        active.publish(prediction)
    assert raised.value.outcome is outcome
    assert not producer.enqueued


def test_broker_rejection_is_not_success(
    publisher: tuple[KafkaPredictionPublisher, FakeProducer],
    prediction: Prediction,
) -> None:
    active, producer = publisher
    producer.delivery_error = KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)
    with pytest.raises(PublicationError) as raised:
        active.publish(prediction)
    assert raised.value.outcome is PublicationOutcome.FAILED


def test_no_ack_times_out_even_though_message_may_be_delivered_later(
    prediction: Prediction,
) -> None:
    producer = FakeProducer()
    producer.allow_delivery.clear()
    active = KafkaPredictionPublisher(
        producer, PublicationSettings(delivery_timeout_ms=1, ack_timeout_seconds=0.01)
    )
    try:
        with pytest.raises(PublicationError) as raised:
            active.publish(prediction)
        assert raised.value.outcome is PublicationOutcome.TIMEOUT
        assert len(producer.enqueued) == 1
    finally:
        active.close()  # Dispatching the late callback must also be safe.


def test_shutdown_is_idempotent_and_rejects_new_messages(prediction: Prediction) -> None:
    producer = FakeProducer()
    active = KafkaPredictionPublisher(producer, PublicationSettings())
    active.close()
    active.close()
    assert producer.flushed == [2]
    assert not active._poller.is_alive()
    with pytest.raises(PublicationError) as raised:
        active.publish(prediction)
    assert raised.value.outcome is PublicationOutcome.CLOSED


def test_poll_failure_disables_new_publications(prediction: Prediction) -> None:
    producer = FakeProducer()
    producer.poll_error = RuntimeError("poll failed")
    active = KafkaPredictionPublisher(producer, PublicationSettings())
    try:
        assert active._poll_failed.wait(1)
        with pytest.raises(PublicationError) as raised:
            active.publish(prediction)
        assert raised.value.outcome is PublicationOutcome.FAILED
    finally:
        active.close()


def test_concurrent_requests_wait_for_their_own_callbacks(prediction: Prediction) -> None:
    producer = FakeProducer()
    active = KafkaPredictionPublisher(producer, PublicationSettings())
    try:
        predictions = [prediction.model_copy(update={"trip_id": f"trip-{i}"}) for i in range(20)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(active.publish, predictions))
        assert len(producer.enqueued) == 20
        assert {item["key"] for item in producer.enqueued} == {
            f"trip-{i}".encode() for i in range(20)
        }
        assert producer.messages.empty()
    finally:
        active.close()


def test_factory_enforces_delivery_and_memory_policy() -> None:
    producer = FakeProducer()
    with patch("tripml.publication.Producer", return_value=producer) as factory:
        active = KafkaPredictionPublisher.from_settings(
            PublicationSettings(bootstrap_servers="broker:9092")
        )
        try:
            config = factory.call_args.args[0]
            assert config["enable.idempotence"] is True
            assert config["acks"] == "all"
            assert config["allow.auto.create.topics"] is False
            assert config["delivery.timeout.ms"] == 1000
            assert config["queue.buffering.max.messages"] == 10_000
            assert config["queue.buffering.max.kbytes"] == 10 * 1024
        finally:
            active.close()
    with pytest.raises(ValueError, match="bootstrap_servers is required"):
        KafkaPredictionPublisher.from_settings(PublicationSettings())


def test_shutdown_reports_unconfirmed_messages(caplog: pytest.LogCaptureFixture) -> None:
    producer = FakeProducer()
    with patch.object(producer, "flush", return_value=3):
        active = KafkaPredictionPublisher(producer, PublicationSettings())
        active.close()
    assert "3 predictions without delivery confirmation" in caplog.text


def _topic_metadata(partitions: int = 3) -> SimpleNamespace:
    return SimpleNamespace(
        topics={
            "predictions": SimpleNamespace(
                error=None,
                partitions={index: SimpleNamespace(replicas=[0]) for index in range(partitions)},
            )
        }
    )


def test_topic_provisioning_creates_missing_topic_and_preserves_existing_layout() -> None:
    client = MagicMock()
    client.list_topics.side_effect = [
        SimpleNamespace(topics={}),
        _topic_metadata(),
        _topic_metadata(),
    ]
    settings = PublicationSettings(bootstrap_servers="broker:9092")
    ensure_prediction_topic(settings, client=client)
    ensure_prediction_topic(settings, client=client)
    client.create_topics.assert_called_once()
    topic = client.create_topics.call_args.args[0][0]
    assert (topic.topic, topic.num_partitions, topic.replication_factor) == ("predictions", 3, 1)


def test_concurrent_topic_creation_is_tolerated_but_other_errors_propagate() -> None:
    client = MagicMock()
    settings = PublicationSettings(bootstrap_servers="broker:9092")
    client.list_topics.side_effect = [SimpleNamespace(topics={}), _topic_metadata()]
    future = client.create_topics.return_value.__getitem__.return_value
    future.result.side_effect = KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS))
    ensure_prediction_topic(settings, client=client)
    client.list_topics.side_effect = [SimpleNamespace(topics={})]
    future.result.side_effect = KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED))
    with pytest.raises(KafkaException):
        ensure_prediction_topic(settings, client=client)


@pytest.mark.parametrize("metadata", [SimpleNamespace(topics={}), _topic_metadata(1)])
def test_topic_provisioning_rejects_missing_metadata_or_wrong_layout(
    metadata: SimpleNamespace,
) -> None:
    client = MagicMock()
    client.list_topics.return_value = metadata
    with pytest.raises(RuntimeError):
        ensure_prediction_topic(PublicationSettings(bootstrap_servers="broker:9092"), client=client)


def test_topic_provisioning_requires_explicit_broker_and_positive_sizes() -> None:
    with pytest.raises(ValueError, match="bootstrap_servers"):
        ensure_prediction_topic(PublicationSettings())
    with pytest.raises(ValueError, match="positive"):
        ensure_prediction_topic(PublicationSettings(bootstrap_servers="broker:9092"), partitions=0)
