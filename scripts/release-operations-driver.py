"""In-cluster release measurements. Inputs and evidence live under /tmp/release-check.

The host harness supplies the approved image, April workload and predeclared phase settings.
This driver reuses the application's benchmark and does not change the serving application.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition

from tripml.benchmark import BenchmarkSettings, RequestSample, run_benchmark
from tripml.contracts import Prediction

ROOT = Path("/tmp/release-check")
API = "http://tripml-tripml-serving:8000"
PROM = "http://tripml-tripml-prometheus:9090"
GRAFANA = "http://tripml-tripml-grafana:3000"
MODEL = "98ef1dd9ef4447f7-static"


def get(url: str, *, auth: bool = False) -> dict[str, Any]:
    headers = {}
    if auth:
        token = base64.b64encode(("admin:" + os.environ["GRAFANA_PASSWORD"]).encode()).decode()
        headers["Authorization"] = "Basic " + token
    with urlopen(Request(url, headers=headers), timeout=10) as response:
        return json.load(response)


def save(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def phase_checks(report: dict[str, Any], *, outage: bool) -> dict[str, bool]:
    count = report["scheduled_requests"]
    common = {
        "all_arrivals_accounted_for": report["gates"]["all_arrivals_accounted_for"],
        "no_dropped_arrivals": report["gates"]["no_load_generator_overflow"],
    }
    if outage:
        return common | {
            "all_requests_fail_explicitly": report["outcomes"] == {"http_error": count},
            "all_http_503": report["http_statuses"] == {"503": count},
            "all_publication_unconfirmed": report["publication_modes"] == {"unconfirmed": count},
        }
    return common | {
        "benchmark_objectives": report["passed"],
        "approved_model_only": report["model_versions"] == {MODEL: report["successful_requests"]},
        "all_successes_acknowledged": report["publication_modes"].get("acknowledged", 0)
        == report["successful_requests"],
        "all_static": report["fallback_rate_among_successes"] == 1,
        "warmup_successful": report["warmup_outcomes"] == {"success": report["settings"]["warmup"]},
    }


def snapshot(name: str) -> dict[str, Any]:
    ready = get(API + "/readyz")
    if ready["model_version"] != MODEL or ready["publication_mode"] != "acknowledged":
        raise ValueError("wrong serving model or publication configuration")
    if ready["mode"] != "static_primary" or ready["registry_version"] != "1":
        raise ValueError("wrong serving mode or registry version")
    with urlopen(API + "/metrics", timeout=10) as response:
        (ROOT / f"{name}-metrics.txt").write_bytes(response.read())
    save(ROOT / f"{name}-readiness.json", ready)
    return ready


def benchmark(phase: str, requests: int, rate: float, warmup: int) -> dict[str, Any]:
    snapshot(phase + "-before")
    report = asyncio.run(
        run_benchmark(
            BenchmarkSettings(
                base_url=API,
                requests=requests,
                rate=rate,
                concurrency=32,
                warmup=warmup,
                timeout_seconds=2,
                p95_objective_ms=50,
                max_error_rate=0.01,
                expected_features="static",
                expected_publication="acknowledged",
                label="approved-april-" + phase,
            ),
            requests_path=ROOT / "requests.jsonl",
            workload_manifest=ROOT / "manifest.json",
            output=ROOT / phase,
        )
    )
    snapshot(phase + "-after")
    checks = phase_checks(report, outage=phase == "broker-down")
    result = {"passed": all(checks.values()), "checks": checks, "benchmark": report}
    save(ROOT / f"{phase}-result.json", result)
    return result


def monitoring(name: str) -> dict[str, Any]:
    targets = get(PROM + "/api/v1/targets")
    save(ROOT / f"{name}-targets.json", targets)
    active = targets["data"]["activeTargets"]
    if not active or any(target["health"] != "up" for target in active):
        raise ValueError("serving scrape targets are missing or unhealthy")
    try:
        get(GRAFANA + "/api/dashboards/uid/tripml-serving")
    except HTTPError as error:
        if error.code != 401:
            raise
    else:
        raise ValueError("Grafana dashboard unexpectedly permits anonymous access")
    dashboard = get(GRAFANA + "/api/dashboards/uid/tripml-serving", auth=True)
    health = get(GRAFANA + "/api/datasources/uid/tripml-prometheus/health", auth=True)
    if health["status"] != "OK":
        raise ValueError("Grafana datasource is unhealthy")
    save(ROOT / "grafana-dashboard.json", dashboard)
    save(ROOT / f"{name}-grafana-health.json", health)
    reports = [json.loads(path.read_text()) for path in ROOT.glob("*/summary.json")]
    end = datetime.now(UTC).timestamp()
    start = min(datetime.fromisoformat(report["started_at"]).timestamp() for report in reports)
    queries = []
    for panel in dashboard["dashboard"]["panels"]:
        for target in panel.get("targets", []):
            query = target["expr"].replace("$__rate_interval", "1m")
            result = get(
                PROM
                + "/api/v1/query_range?"
                + urlencode({"query": query, "start": start - 60, "end": end, "step": 5})
            )
            if result["status"] != "success":
                raise ValueError("dashboard query failed")
            queries.append({"panel": panel["title"], "query": query, "response": result})
    result = {"start": start - 60, "end": end, "step": 5, "queries": queries}
    save(ROOT / f"{name}-dashboard-queries.json", result)
    return {"passed": True, "healthy_targets": len(active), "queries": len(queries)}


def delivery_matches(prediction: Prediction, sample: RequestSample) -> bool:
    return (
        sample.prediction_id == str(prediction.prediction_id)
        and sample.trip_id == prediction.trip_id
        and sample.model_version == prediction.model_version
        and sample.feature_fallback == prediction.feature_fallback
        and sample.prediction_sha256
        == hashlib.sha256(prediction.model_dump_json().encode()).hexdigest()
    )


def readback(topic: str) -> dict[str, Any]:
    samples = {
        sample.trip_id: sample
        for name in ("samples.jsonl", "warmup.jsonl")
        for path in ROOT.glob("*/" + name)
        for line in path.read_text().splitlines()
        for sample in [RequestSample.model_validate_json(line)]
    }
    expected = {sample.prediction_id for sample in samples.values() if sample.outcome == "success"}
    matched: set[str] = set()
    seen: Counter[str] = Counter()
    mismatched = []
    ambiguous = []
    unrelated = 0
    consumer = Consumer(
        {
            "bootstrap.servers": "tripml-tripml-redpanda:9092",
            "group.id": "release-readback-" + uuid4().hex,
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=10).topics[topic]
        if metadata.error is not None:
            raise ValueError(str(metadata.error))
        partitions = [TopicPartition(topic, index, 0) for index in metadata.partitions]
        ends = {
            part.partition: consumer.get_watermark_offsets(part, timeout=10)[1]
            for part in partitions
        }
        positions = dict.fromkeys(ends, 0)
        consumer.assign(partitions)
        deadline = monotonic() + 90
        with (ROOT / "consumed-predictions.jsonl").open("w") as sink:
            while any(positions[index] < end for index, end in ends.items()):
                if monotonic() > deadline:
                    raise TimeoutError("broker snapshot readback exceeded 90 seconds")
                message = consumer.poll(0.2)
                if message is None:
                    continue
                if message.error() is not None:
                    raise ValueError(str(message.error()))
                positions[message.partition()] = message.offset() + 1
                prediction = Prediction.model_validate_json(message.value())
                sample = samples.get(prediction.trip_id)
                if sample is None:
                    unrelated += 1
                    continue
                identity = str(prediction.prediction_id)
                seen[identity] += 1
                headers = dict(message.headers() or [])
                envelope = (
                    message.key() == prediction.trip_id.encode()
                    and headers.get("prediction-id") == identity.encode()
                    and headers.get("schema-version") == prediction.schema_version.encode()
                )
                if not envelope:
                    mismatched.append(identity)
                elif identity in expected and delivery_matches(prediction, sample):
                    matched.add(identity)
                elif sample.outcome in {"http_error", "timeout", "transport_error"}:
                    ambiguous.append(identity)
                else:
                    mismatched.append(identity)
                sink.write(
                    json.dumps(
                        {
                            "partition": message.partition(),
                            "offset": message.offset(),
                            "prediction": prediction.model_dump(mode="json"),
                        }
                    )
                    + "\n"
                )
    finally:
        consumer.close()
    checks = {
        "all_successes_consumed": matched == expected,
        "no_mismatches": not mismatched,
        "no_duplicate_prediction_ids": all(count == 1 for count in seen.values()),
    }
    result = {
        "passed": all(checks.values()),
        "checks": checks,
        "expected": len(expected),
        "matched": len(matched),
        "mismatched": mismatched,
        "ambiguous": ambiguous,
        "unrelated_records": unrelated,
        "high_watermarks": ends,
    }
    save(ROOT / "broker-readback.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["benchmark", "monitor", "readback"])
    parser.add_argument("--phase", default="healthy")
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--topic", default="predictions-batch-release-v2")
    args = parser.parse_args()
    if args.action == "benchmark":
        result = benchmark(args.phase, args.requests, args.rate, args.warmup)
    elif args.action == "monitor":
        result = monitoring(args.phase)
    else:
        result = readback(args.topic)
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
