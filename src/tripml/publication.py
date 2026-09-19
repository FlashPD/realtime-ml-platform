"""Acknowledged Kafka publication; a queued message alone is not a successful delivery."""

from __future__ import annotations

import logging
from enum import StrEnum
from threading import Event, Lock, Thread

from confluent_kafka import KafkaError, KafkaException, Message, Producer

# NewTopic is a documented public API but lacks an explicit typed re-export upstream.
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from tripml.contracts import Prediction
from tripml.settings import PublicationSettings

logger = logging.getLogger(__name__)


def ensure_prediction_topic(
    settings: PublicationSettings,
    *,
    partitions: int = 3,
    replication_factor: int = 1,
    client: AdminClient | None = None,
) -> None:
    """Provision the explicitly configured topic; never alter an existing topic's layout."""
    if settings.bootstrap_servers is None:
        raise ValueError("bootstrap_servers is required for topic provisioning")
    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")
    active = client or AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    metadata = active.list_topics(timeout=10)
    if settings.topic not in metadata.topics:
        try:
            active.create_topics(
                [
                    NewTopic(
                        settings.topic,
                        num_partitions=partitions,
                        replication_factor=replication_factor,
                    )
                ],
                request_timeout=10,
            )[settings.topic].result(timeout=10)
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        metadata = active.list_topics(timeout=10)
    topic = metadata.topics.get(settings.topic)
    if topic is None or topic.error is not None:
        raise RuntimeError("prediction topic metadata is unavailable")
    if len(topic.partitions) != partitions or any(
        len(item.replicas) != replication_factor for item in topic.partitions.values()
    ):
        raise RuntimeError(
            "existing prediction topic does not match the requested partition/replica layout"
        )


class PublicationOutcome(StrEnum):
    ACKNOWLEDGED = "acknowledged"
    DISABLED = "disabled"
    QUEUE_FULL = "queue_full"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CLOSED = "closed"


class PublicationError(RuntimeError):
    def __init__(self, outcome: PublicationOutcome) -> None:
        self.outcome = outcome
        super().__init__(f"prediction publication {outcome.value}")


class KafkaPredictionPublisher:
    """One shared producer and callback pump; each request waits for its own delivery."""

    def __init__(self, producer: Producer, settings: PublicationSettings) -> None:
        self.producer = producer
        self.settings = settings
        self._lock = Lock()
        self._closed = False
        self._stop = Event()
        self._poll_failed = Event()
        self._poller = Thread(target=self._poll, name="tripml-kafka-delivery", daemon=True)
        self._poller.start()

    @classmethod
    def from_settings(cls, settings: PublicationSettings) -> KafkaPredictionPublisher:
        if settings.bootstrap_servers is None:
            raise ValueError("bootstrap_servers is required for prediction publication")
        producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "client.id": "tripml-prediction-api",
                "enable.idempotence": True,
                "acks": "all",
                "allow.auto.create.topics": False,
                "partitioner": "murmur2_random",
                "linger.ms": 0,
                "delivery.timeout.ms": settings.delivery_timeout_ms,
                "queue.buffering.max.messages": settings.queue_max_messages,
                "queue.buffering.max.kbytes": 10 * 1024,
            }
        )
        return cls(producer, settings)

    def _poll(self) -> None:
        try:
            while not self._stop.is_set():
                self.producer.poll(0.05)
        except Exception:
            self._poll_failed.set()
            logger.exception("Kafka delivery polling stopped")

    def publish(self, prediction: Prediction) -> None:
        completed = Event()
        delivery_error: KafkaError | None = None

        def delivered(error: KafkaError | None, message: Message) -> None:
            nonlocal delivery_error
            del message
            delivery_error = error
            completed.set()

        # Serialize before taking the admission lock; no network wait holds this lock.
        payload = prediction.model_dump_json().encode("utf-8")
        with self._lock:
            if self._closed:
                raise PublicationError(PublicationOutcome.CLOSED)
            if self._poll_failed.is_set():
                raise PublicationError(PublicationOutcome.FAILED)
            try:
                self.producer.produce(
                    self.settings.topic,
                    key=prediction.trip_id.encode("utf-8"),
                    value=payload,
                    timestamp=int(prediction.served_at.timestamp() * 1000),
                    headers={
                        "content-type": "application/json",
                        "schema-version": prediction.schema_version,
                        "prediction-id": str(prediction.prediction_id),
                    },
                    on_delivery=delivered,
                )
            except BufferError as error:
                raise PublicationError(PublicationOutcome.QUEUE_FULL) from error
            except KafkaException as error:
                raise PublicationError(PublicationOutcome.FAILED) from error
        if not completed.wait(self.settings.ack_timeout_seconds):
            # This message may still reach the broker. Never report definite non-delivery.
            raise PublicationError(PublicationOutcome.TIMEOUT)
        if delivery_error is not None:
            raise PublicationError(PublicationOutcome.FAILED)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._stop.set()
        self._poller.join(timeout=0.1)
        # flush also dispatches callbacks; never flush the whole queue on a request path.
        remaining = self.producer.flush(self.settings.shutdown_timeout_seconds)
        if remaining:
            logger.warning(
                "Kafka shutdown left %s predictions without delivery confirmation", remaining
            )
