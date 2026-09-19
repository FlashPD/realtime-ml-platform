# ADR-0010: Acknowledged prediction publication

- Status: Accepted
- Date: 2026-09-16

## Context

The API serves predictions with online features or a static fallback, but the future ground-truth
joiner needs the exact predictions that clients receive. Enqueuing a Kafka message in process memory
is not evidence of broker delivery. The service also needs bounded behavior when the broker fails.

## Decision

When broker bootstrap servers are configured, publish every valid inference result and require its
delivery callback to report success before returning HTTP 200. Queue rejection, delivery failure, and
acknowledgment timeout return HTTP 503, with the prediction ID for correlation. Do not return a
successful prediction on a best-effort background publish. Model/validation failures never publish.

Use one shared `confluent-kafka` producer per service process, `acks=all`, idempotence enabled, zero
linger, and the `murmur2_random` partitioner. Key by UTF-8 trip ID so consumers can group predictions
for a trip. The value is plain UTF-8 JSON serialized from the same immutable `Prediction` instance
returned by the API. Headers carry content type, schema version, and prediction ID; the Kafka record
timestamp is `served_at` in milliseconds. This is not Schema Registry wire framing. Registration and
broker schema enforcement remain part of the broader event-contract milestone.

The configured topic must already exist; disable auto-creation so application requests do not
silently provision a topic with unintended settings. The local bootstrap configuration uses plaintext
Kafka to match the private single-node platform profile. Managed/shared broker TLS and SASL require
an explicit later configuration extension.

A dedicated daemon thread polls delivery callbacks. Each request waits on its own completion event,
not on `flush()` for the entire producer queue. Admission takes a short lock so shutdown cannot race
with enqueueing; that lock is released before waiting for acknowledgment. Concurrent HTTP workers
share the producer. If callback polling fails, subsequent publications fail closed; in-flight waits
remain bounded. Topic metadata and connectivity are handled lazily by the producer. Readiness reports
loaded-model capability and configured publication mode, not broker reachability; outages surface as
503 responses and publication metrics.

Default limits are a 1,000 ms producer delivery timeout, a 1.5 second callback wait, and a queue bounded
by both 10,000 messages and 10 MiB. These bounds concern delivery failure, not achievement of the 50 ms
request objective. Shutdown stops admission, stops the poller, and attempts a two-second flush that
also dispatches callbacks. Remaining unconfirmed messages generate a warning. No unacknowledged
message has justified an HTTP 200. This is not a durable application outbox; crashing before broker
delivery can lose an in-flight request that never received a successful response.

For local model-only work, omitting broker configuration explicitly disables publication. Success
responses carry `X-TripML-Publication: disabled` or `acknowledged`, and readiness exposes the mode.
Unconfirmed responses carry a prediction ID, outcome, and `delivery_unknown` flag. Bounded-label
publication counters describe outcomes observed by requests, not a complete broker-side audit of
late delivery; a duration histogram captures time spent waiting. Prediction IDs never become metric
labels.

## Failure semantics and trade-offs

- Queue-full and already-closed rejection occur before enqueueing the attempted record.
- Timeout and delivery failure are conservatively treated as ambiguous: an acknowledgment can be
  lost, or a queued record can arrive after the request has stopped waiting. Do not claim the event
  is absent from the broker.
- A process crash or broken HTTP connection can occur after a successful broker append. An event may
  exist even though the caller did not observe HTTP 200.
- Producer idempotence protects Kafka transport retries within the producer session. Retried HTTP
  requests run inference again and receive new prediction IDs. There is no request idempotency key
  or cross-process deduplication. The future joiner must deduplicate prediction IDs and decide how
  to evaluate multiple predictions for one trip; it must not assume one prediction per trip.
- The local topic's single replica provides no broker-storage redundancy. `acks=all` means all
  required in-sync replicas, which locally is one. Replication and retention need deliberate settings
  in any shared deployment.

This favors traceability of successful responses over API availability during broker outages. A
durable transactional outbox could relax that coupling later, at the cost of a database write and
an additional delivery worker. It is unnecessary to introduce that subsystem for this milestone.

## Validation

Tests cover JSON/key/header fidelity, callback-confirmed success, transport and queue errors,
missing acknowledgment, late callback safety, concurrent publication, polling failure, shutdown,
and API failure responses/metrics. A real local Redpanda test calls the HTTP API with a deterministic
test model, then consumes and validates the exact event; native model correctness remains covered
by the existing serving tests. The test owns a unique topic and removes it afterward. CI provisions
a disposable broker and runs this integration test automatically. Serving load measurements,
ground-truth joining, and PostgreSQL prediction/error records remain pending.

## References

- [Confluent Python producer API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
- [librdkafka delivery and producer configuration](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html)
