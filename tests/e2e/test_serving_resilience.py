"""Real HTTP load and dependency recovery with disposable, privately addressed containers."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic, sleep
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest
from confluent_kafka import Consumer, TopicPartition
from prometheus_client.parser import text_string_to_metric_families
from redis import Redis

from tripml.benchmark import BenchmarkSettings, RequestSample, run_benchmark
from tripml.contracts import OnlineZoneWindowFeatures, Prediction
from tripml.online_features import feature_key
from tripml.publication import ensure_prediction_topic
from tripml.settings import PublicationSettings
from tripml.training import TrainingRunReport

ROOT = Path(__file__).resolve().parents[2]
REDIS_IMAGE = "redis:8.8.0-alpine3.23"
BROKER_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v26.2.2"


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=check, timeout=90
    )


def _free_port() -> int:
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])


def _wait_until(check: Callable[[], bool], description: str) -> None:
    deadline = monotonic() + 60
    while monotonic() < deadline:
        if check():
            return
        sleep(0.2)
    raise TimeoutError(f"timed out waiting for {description}")


@contextmanager
def _container(output: Path, role: str, *args: str) -> Iterator[str]:
    # Capture the created ID before starting. Cleanup can only target resources we created.
    container = _docker(
        "create",
        "--name",
        f"tripml-resilience-{role}-{uuid4().hex[:10]}",
        "--label",
        "tripml.purpose=serving-resilience",
        *args,
    ).stdout.strip()
    try:
        _docker("start", container)
        (output / f"{role}-container.json").write_text(_docker("inspect", container).stdout)
        yield container
    finally:
        try:
            logs = _docker("logs", container, check=False)
            (output / f"{role}.log").write_text(logs.stdout + logs.stderr)
        finally:
            _docker("rm", "--force", "--volumes", container)


@contextmanager
def _server(output: Path, *, redis_port: int, broker_port: int, topic: str) -> Iterator[str]:
    port = _free_port()
    environment = {key: value for key, value in os.environ.items() if not key.startswith("TRIPML_")}
    environment.update(
        {
            "TRIPML_SERVING__REDIS_URL": f"redis://127.0.0.1:{redis_port}/0",
            "TRIPML_PUBLICATION__BOOTSTRAP_SERVERS": f"127.0.0.1:{broker_port}",
            "TRIPML_PUBLICATION__TOPIC": topic,
        }
    )
    with (output / "api.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tripml",
                "serve",
                "--bundle",
                str(output / "model"),
                "--port",
                str(port),
            ],
            cwd=ROOT,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            (output / "api-process.json").write_text(json.dumps({"pid": process.pid}))
            base_url = f"http://127.0.0.1:{port}"
            with httpx.Client(base_url=base_url, trust_env=False, timeout=3) as client:

                def ready() -> bool:
                    if process.poll() is not None:
                        raise RuntimeError("API exited; inspect api.log")
                    try:
                        return client.get("/readyz").status_code == 200
                    except httpx.RequestError:
                        return False

                _wait_until(ready, "the prediction API")
                (output / "server-ready.json").write_text(client.get("/readyz").text)
            yield base_url
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def _seed_redis(port: int, snapshots: tuple[OnlineZoneWindowFeatures, ...]) -> None:
    with (
        Redis(host="127.0.0.1", port=port, socket_timeout=2) as client,
        client.pipeline(transaction=True) as pipeline,
    ):
        for snapshot in snapshots:
            pipeline.set(
                feature_key(snapshot.zone_role, snapshot.zone_id, snapshot.window_kind),
                snapshot.model_dump_json(),
                ex=600,
            )
        pipeline.execute()


def _lookup_counts(metrics: str) -> dict[str, float]:
    return {
        sample.labels["outcome"]: sample.value
        for family in text_string_to_metric_families(metrics)
        for sample in family.samples
        if sample.name == "tripml_feature_lookup_total"
    }


def _scenario(
    output: Path, base_url: str, name: str, *, lookup: str, broker_down: bool = False
) -> dict[str, Any]:
    settings = BenchmarkSettings(
        base_url=base_url,
        requests=10 if broker_down else 1000,
        rate=5 if broker_down else 100,
        warmup=0 if broker_down else 20,
        # Online phases separately enforce the architecture's <1% fallback objective.
        expected_features="static" if lookup == "unavailable" else "any",
        expected_publication="acknowledged",
        label=f"synthetic-resilience-{name}",
    )
    with httpx.Client(base_url=base_url, trust_env=False, timeout=3) as client:
        before = client.get("/metrics").text
        report = asyncio.run(
            run_benchmark(
                settings,
                requests_path=ROOT / "examples/benchmark/requests.jsonl",
                output=output / name,
            )
        )
        after = client.get("/metrics").text
        ready = client.get("/readyz")
        live = client.get("/healthz")
    (output / name / "metrics-before.txt").write_text(before)
    (output / name / "metrics-after.txt").write_text(after)
    (output / name / "ready.json").write_text(ready.text)
    before_counts, after_counts = _lookup_counts(before), _lookup_counts(after)
    lookup_deltas = {
        outcome: count - before_counts.get(outcome, 0)
        for outcome, count in after_counts.items()
        if count != before_counts.get(outcome, 0)
    }
    total = settings.requests + settings.warmup
    checks = {
        "model_remains_ready": ready.status_code == 200 and ready.json()["status"] == "ready",
        "process_remains_live": live.status_code == 200,
        "all_feature_lookups_accounted_for": sum(lookup_deltas.values()) == total,
        "expected_feature_lookup_cause": (
            lookup_deltas == {"unavailable": total}
            if lookup == "unavailable"
            else lookup_deltas.get("fresh", 0) > 0
            and set(lookup_deltas) <= {"fresh", "unavailable"}
        ),
    }
    if broker_down:
        checks["no_unacknowledged_success"] = (
            not report["passed"]
            and report["outcomes"] == {"http_error": settings.requests}
            and report["http_statuses"] == {"503": settings.requests}
        )
    else:
        checks["load_objectives"] = report["passed"]
        checks["no_failed_measured_predictions"] = report["error_rate"] == 0
        if lookup == "fresh":
            fallback_rate = report["fallback_rate_among_successes"]
            checks["fallback_rate_under_one_percent"] = (
                fallback_rate is not None and fallback_rate < 0.01
            )
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "feature_lookup_deltas": lookup_deltas,
        "benchmark": report,
    }


def _samples(output: Path, scenarios: dict[str, Any]) -> dict[str, RequestSample]:
    return {
        sample.trip_id: sample
        for scenario in scenarios
        for filename in ("samples.jsonl", "warmup.jsonl")
        for line in (output / scenario / filename).read_text().splitlines()
        for sample in [RequestSample.model_validate_json(line)]
    }


def _verify_delivery(
    output: Path, brokers: str, topic: str, scenarios: dict[str, Any]
) -> dict[str, Any]:
    samples = _samples(output, scenarios)
    expected = {
        sample.prediction_id: sample
        for sample in samples.values()
        if sample.status_code == 200
        and sample.publication == "acknowledged"
        and sample.prediction_id is not None
    }
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"resilience-{uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    seen: Counter[str] = Counter()
    matched: set[str] = set()
    mismatched: list[str] = []
    ambiguous: list[str] = []
    positions = dict.fromkeys(range(3), 0)
    try:
        partitions = [TopicPartition(topic, index, 0) for index in positions]
        ends = {
            partition.partition: consumer.get_watermark_offsets(partition, timeout=10)[1]
            for partition in partitions
        }
        consumer.assign(partitions)
        deadline = monotonic() + 30
        with (output / "consumed-predictions.jsonl").open("w", encoding="utf-8") as sink:
            while any(positions[index] < end for index, end in ends.items()):
                if monotonic() > deadline:
                    break
                message = consumer.poll(0.2)
                if message is None:
                    continue
                if message.error() is not None:
                    raise RuntimeError(str(message.error()))
                prediction = Prediction.model_validate_json(message.value())
                identifier = str(prediction.prediction_id)
                seen[identifier] += 1
                positions[message.partition()] = message.offset() + 1
                sample = samples.get(prediction.trip_id)
                headers = dict(message.headers() or [])
                valid_envelope = (
                    message.key() == prediction.trip_id.encode()
                    and headers.get("prediction-id") == identifier.encode()
                    and headers.get("schema-version") == b"1.0"
                )
                if not valid_envelope or sample is None:
                    mismatched.append(identifier)
                elif identifier in expected:
                    if (
                        expected[identifier].trip_id == prediction.trip_id
                        and sample.model_version == prediction.model_version
                        and sample.feature_fallback == prediction.feature_fallback
                        and sample.prediction_sha256
                        == hashlib.sha256(prediction.model_dump_json().encode()).hexdigest()
                    ):
                        matched.add(identifier)
                    else:
                        mismatched.append(identifier)
                elif sample.outcome in {"http_error", "timeout", "transport_error"}:
                    # Failed HTTP delivery may still reach Kafka; never claim exactly once.
                    ambiguous.append(identifier)
                else:
                    mismatched.append(identifier)
                sink.write(
                    json.dumps(
                        {
                            "partition": message.partition(),
                            "offset": message.offset(),
                            "prediction": prediction.model_dump(mode="json"),
                            "key": message.key().decode() if message.key() else None,
                        }
                    )
                    + "\n"
                )
    finally:
        consumer.close()
    missing = sorted(set(expected) - matched)
    duplicates = sorted(identifier for identifier, count in seen.items() if count > 1)
    checks = {
        "reached_captured_end_offsets": positions == ends,
        "all_acknowledged_predictions_consumed": bool(expected) and not missing,
        "no_duplicate_prediction_ids": not duplicates,
        "valid_prediction_envelopes": not mismatched,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "acknowledged_predictions": len(expected),
        "matched_predictions": len(matched),
        "consumed_predictions": sum(seen.values()),
        "missing_prediction_ids": missing,
        "duplicate_prediction_ids": duplicates,
        "mismatched_prediction_ids": mismatched,
        "ambiguous_delivery_prediction_ids": ambiguous,
        "end_offsets": ends,
        "consumed_positions": positions,
    }


@pytest.mark.e2e
@pytest.mark.skipif(
    os.getenv("TRIPML_TEST_SERVING_RESILIENCE") != "1",
    reason="set TRIPML_TEST_SERVING_RESILIENCE=1 to create disposable Docker dependencies",
)
def test_serving_load_degradation_recovery_and_broker_delivery(
    tmp_path: Path,
    trained_report: TrainingRunReport,
    online_snapshots: tuple[OnlineZoneWindowFeatures, ...],
) -> None:
    output = Path(os.getenv("TRIPML_RESILIENCE_OUTPUT", str(tmp_path / "evidence"))).resolve()
    output.mkdir(parents=True, exist_ok=False)
    shutil.copytree(trained_report.artifact_directory, output / "model")
    (output / "online-snapshots.json").write_text(
        json.dumps(
            [snapshot.model_dump(mode="json") for snapshot in online_snapshots],
            indent=2,
        )
    )
    topic = f"tripml-resilience-{uuid4().hex}"
    scenarios: dict[str, Any] = {}
    redis_port = _free_port()
    with _container(
        output,
        "redis",
        "--cpus",
        "0.5",
        "--memory",
        "128m",
        "--publish",
        f"127.0.0.1:{redis_port}:6379",
        REDIS_IMAGE,
        "redis-server",
        "--save",
        "",
        "--appendonly",
        "no",
    ) as redis_container:
        broker_port = _free_port()
        with _container(
            output,
            "broker",
            "--cpus",
            "1",
            "--memory",
            "768m",
            "--publish",
            f"127.0.0.1:{broker_port}:9092",
            BROKER_IMAGE,
            "redpanda",
            "start",
            "--mode",
            "dev-container",
            "--smp",
            "1",
            "--memory",
            "512M",
            "--reserve-memory",
            "0M",
            "--overprovisioned",
            "--check=false",
            "--kafka-addr",
            "0.0.0.0:9092",
            "--advertise-kafka-addr",
            f"127.0.0.1:{broker_port}",
        ) as broker:

            def redis_ready() -> bool:
                return (
                    _docker("exec", redis_container, "redis-cli", "ping", check=False).returncode
                    == 0
                )

            def broker_ready() -> bool:
                return (
                    _docker("exec", broker, "rpk", "cluster", "health", check=False).returncode == 0
                )

            _wait_until(redis_ready, "Redis")
            _wait_until(broker_ready, "Redpanda")
            _seed_redis(redis_port, online_snapshots)
            brokers = f"127.0.0.1:{broker_port}"
            ensure_prediction_topic(PublicationSettings(bootstrap_servers=brokers, topic=topic))
            with _server(
                output, redis_port=redis_port, broker_port=broker_port, topic=topic
            ) as url:
                scenarios["healthy"] = _scenario(output, url, "healthy", lookup="fresh")
                _docker("stop", "--time", "2", redis_container)
                scenarios["redis-unavailable"] = _scenario(
                    output, url, "redis-unavailable", lookup="unavailable"
                )
                _docker("start", redis_container)
                _wait_until(redis_ready, "Redis recovery")
                _seed_redis(redis_port, online_snapshots)
                scenarios["redis-recovered"] = _scenario(
                    output, url, "redis-recovered", lookup="fresh"
                )
                _docker("stop", "--time", "2", broker)
                scenarios["broker-unavailable"] = _scenario(
                    output, url, "broker-unavailable", lookup="fresh", broker_down=True
                )
                _docker("start", broker)
                _wait_until(broker_ready, "Redpanda recovery")
                scenarios["broker-recovered"] = _scenario(
                    output, url, "broker-recovered", lookup="fresh"
                )
            delivery = _verify_delivery(output, brokers, topic, scenarios)
    report = {
        "schema_version": "1.0",
        "passed": all(scenario["passed"] for scenario in scenarios.values()) and delivery["passed"],
        "scenarios": scenarios,
        "delivery_verification": delivery,
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "logical_cpus": os.cpu_count(),
            "source_sha256": {
                str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in [
                    *sorted((ROOT / "src/tripml").glob("*.py")),
                    ROOT / "tests/conftest.py",
                    Path(__file__),
                    ROOT / "pyproject.toml",
                ]
            },
            "model_run_id": trained_report.run_id,
            "model_provenance": "Synthetic 200-row training and 100-row holdout fixture",
            "topology": "One host API worker; Docker Redis/Redpanda via loopback published ports",
            "registry": "Local bundle, no platform registry changes",
            "redis_image": REDIS_IMAGE,
            "broker_image": BROKER_IMAGE,
        },
        "artifacts_sha256": {
            str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(output.rglob("*"))
            if path.is_file()
        },
    }
    (output / "summary.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    lines = [
        "# Synthetic serving resilience evidence",
        "",
        f"Suite result: **{'PASS' if report['passed'] else 'FAIL'}**",
        "",
        "| Scenario | Expected behavior | Result | Measured P95 | Fallback rate |",
        "|---|---|---|---:|---:|",
    ]
    for name, scenario in scenarios.items():
        p95 = scenario["benchmark"]["successful_latency_ms"]["p95"]
        latency = f"{p95:.2f} ms" if p95 is not None else "No successful predictions"
        behavior = "HTTP 503" if name == "broker-unavailable" else "Acknowledged predictions"
        fallback_rate = scenario["benchmark"]["fallback_rate_among_successes"]
        fallback = f"{fallback_rate:.2%}" if fallback_rate is not None else "N/A"
        lines.append(
            f"| [{name}]({name}/README.md) | {behavior} | "
            f"{'PASS' if scenario['passed'] else 'FAIL'} | {latency} | {fallback} |"
        )
    lines.extend(
        [
            "",
            f"Broker verification: {delivery['matched_predictions']}/"
            f"{delivery['acknowledged_predictions']} acknowledged predictions consumed "
            "(includes warm-up).",
            "",
            "[Full results and checksums](summary.json), "
            "[consumed events](consumed-predictions.jsonl).",
            "",
            "Synthetic native models and seeded snapshots, four 10-second load phases at "
            "100 requests/s; broker outage uses 10 requests at 5 requests/s. The same API "
            "process stays running across all phases. Redis is reseeded after restart; "
            "online phases require fallback below 1% per the architecture objective. "
            "this does not demonstrate stream processor recovery, feature parity, "
            "real-data accuracy, Kubernetes latency, or HPA behavior.",
            "",
            "Only suite-owned containers and anonymous volumes were removed; "
            "raw evidence is retained.",
            "",
        ]
    )
    (output / "README.md").write_text("\n".join(lines))
    assert report["passed"], f"Serving resilience objectives failed; inspect {output / 'README.md'}"


@pytest.mark.parametrize(
    ("fallbacks", "passed"), [(0, True), (9, True), (10, False), (1000, False)]
)
def test_online_scenario_enforces_the_declared_fallback_boundary(
    tmp_path: Path, fallbacks: int, passed: bool
) -> None:
    (tmp_path / "case").mkdir()
    metrics = (
        f'tripml_feature_lookup_total{{outcome="fresh"}} {1020 - fallbacks}\n'
        f'tripml_feature_lookup_total{{outcome="unavailable"}} {fallbacks}\n'
    )
    client = MagicMock()
    client.get.side_effect = [
        httpx.Response(200, text=""),
        httpx.Response(200, text=metrics),
        httpx.Response(200, json={"status": "ready"}),
        httpx.Response(200),
    ]
    benchmark = AsyncMock(
        return_value={
            "passed": True,
            "error_rate": 0,
            "fallback_rate_among_successes": fallbacks / 1000,
        }
    )
    with (
        patch("httpx.Client") as client_class,
        patch(f"{__name__}.run_benchmark", benchmark),
    ):
        client_class.return_value.__enter__.return_value = client
        result = _scenario(tmp_path, "http://localhost", "case", lookup="fresh")
    assert result["passed"] is passed
    assert result["checks"]["fallback_rate_under_one_percent"] is passed
    assert benchmark.call_args.args[0].expected_features == "any"


@pytest.mark.parametrize("matching_digest", [True, False])
def test_readback_includes_acknowledged_feature_mode_mismatches(
    tmp_path: Path, matching_digest: bool
) -> None:
    prediction = Prediction(
        trip_id="acknowledged-fallback",
        model_version="fixture-static",
        features_used={},
        feature_timestamps={},
        feature_fallback=True,
        estimated_duration_seconds=600,
        served_at=datetime.now(UTC),
    )
    sample = RequestSample(
        sequence=0,
        trip_id=prediction.trip_id,
        scheduled_offset_seconds=0,
        dispatch_lag_ms=0,
        status_code=200,
        outcome="feature_mode_mismatch",
        publication="acknowledged",
        prediction_id=str(prediction.prediction_id),
        feature_fallback=True,
        model_version=prediction.model_version,
        prediction_sha256=hashlib.sha256(prediction.model_dump_json().encode()).hexdigest()
        if matching_digest
        else "incorrect-digest",
    )
    (tmp_path / "case").mkdir()
    (tmp_path / "case/samples.jsonl").write_text(sample.model_dump_json() + "\n")
    (tmp_path / "case/warmup.jsonl").write_text("")
    message = MagicMock()
    message.error.return_value = None
    message.value.return_value = prediction.model_dump_json().encode()
    message.partition.return_value = 0
    message.offset.return_value = 0
    message.key.return_value = prediction.trip_id.encode()
    message.headers.return_value = [
        ("prediction-id", str(prediction.prediction_id).encode()),
        ("schema-version", b"1.0"),
    ]
    consumer = MagicMock()
    consumer.get_watermark_offsets.side_effect = lambda partition, **_: (
        0,
        1 if partition.partition == 0 else 0,
    )
    consumer.poll.return_value = message
    with patch(f"{__name__}.Consumer", return_value=consumer):
        result = _verify_delivery(tmp_path, "unused:9092", "test-topic", {"case": {}})
    assert result["acknowledged_predictions"] == 1
    assert result["passed"] is matching_digest
    assert result["matched_predictions"] == (1 if matching_digest else 0)
    consumer.close.assert_called_once()
