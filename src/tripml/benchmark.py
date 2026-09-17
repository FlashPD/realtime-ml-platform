"""Constant-arrival HTTP load with explicit overload accounting and portable evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import platform
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from tripml.contracts import ETARequest, Prediction


class BenchmarkSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    base_url: str = "http://127.0.0.1:8000"
    requests: int = Field(default=1000, ge=1, le=100_000)
    rate: float = Field(default=100, gt=0, le=10_000)
    concurrency: int = Field(default=32, ge=1, le=1000)
    warmup: int = Field(default=20, ge=0, le=1000)
    timeout_seconds: float = Field(default=2, gt=0, le=60)
    p95_objective_ms: float = Field(default=50, gt=0)
    max_error_rate: float = Field(default=0.01, ge=0, lt=1)
    expected_features: Literal["any", "static", "streaming"] = "any"
    expected_publication: Literal["acknowledged", "disabled"] = "acknowledged"
    label: str = Field(default="serving-load", min_length=1, max_length=200)

    @field_validator("base_url")
    @classmethod
    def url_is_an_origin(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("base_url must be an HTTP(S) origin without credentials")
        return value.rstrip("/")


class RequestSample(BaseModel):
    sequence: int
    trip_id: str
    scheduled_offset_seconds: float
    dispatch_lag_ms: float
    latency_ms: float | None = None
    http_latency_ms: float | None = None
    status_code: int | None = None
    outcome: str
    feature_fallback: bool | None = None
    model_version: str | None = None
    prediction_id: str | None = None
    prediction_sha256: str | None = None
    publication: str | None = None


def load_requests(path: Path) -> list[ETARequest]:
    """Read a bounded, nonempty JSONL fixture before sending any traffic."""

    requests = []
    with path.open(encoding="utf-8") as source:
        for line in source:
            if line.strip():
                requests.append(ETARequest.model_validate_json(line))
                if len(requests) > 100_000:
                    raise ValueError("request fixture exceeds 100,000 rows")
    if not requests:
        raise ValueError("request fixture must contain at least one ETARequest")
    return requests


def percentiles(values: list[float]) -> dict[str, float | None]:
    """Exact nearest-rank percentiles; empty populations cannot meet a latency gate."""

    ordered = sorted(values)
    return {
        f"p{percentile}": ordered[math.ceil(len(ordered) * percentile / 100) - 1]
        if ordered
        else None
        for percentile in (50, 95, 99)
    }


async def _request(
    client: httpx.AsyncClient,
    settings: BenchmarkSettings,
    payload: ETARequest,
    *,
    sequence: int,
    scheduled: float,
    origin: float,
) -> RequestSample:
    started = perf_counter()
    sample = RequestSample(
        sequence=sequence,
        trip_id=payload.trip_id,
        scheduled_offset_seconds=scheduled - origin,
        dispatch_lag_ms=max(0, (started - scheduled) * 1000),
        outcome="success",
    )
    try:
        # HTTPX's per-operation timeouts alone are not a total request deadline.
        async with asyncio.timeout(settings.timeout_seconds):
            response = await client.post("/v1/eta", json=payload.model_dump(mode="json"))
        sample.status_code = response.status_code
        sample.publication = response.headers.get("X-TripML-Publication")
        if response.status_code != 200:
            sample.outcome = "http_error"
        else:
            prediction = Prediction.model_validate_json(response.content)
            sample.prediction_id = str(prediction.prediction_id)
            sample.prediction_sha256 = hashlib.sha256(
                prediction.model_dump_json().encode("utf-8")
            ).hexdigest()
            sample.model_version = prediction.model_version
            sample.feature_fallback = prediction.feature_fallback
            if prediction.trip_id != payload.trip_id:
                sample.outcome = "trip_id_mismatch"
            elif sample.publication != settings.expected_publication:
                sample.outcome = "publication_mismatch"
            elif settings.expected_features != "any" and prediction.feature_fallback != (
                settings.expected_features == "static"
            ):
                sample.outcome = "feature_mode_mismatch"
    except (TimeoutError, httpx.TimeoutException):
        sample.outcome = "timeout"
    except httpx.RequestError:
        sample.outcome = "transport_error"
    except ValueError:
        sample.outcome = "invalid_prediction"
    finally:
        finished = perf_counter()
        sample.http_latency_ms = (finished - started) * 1000
        sample.latency_ms = (finished - scheduled) * 1000
    return sample


async def _measure(
    client: httpx.AsyncClient,
    settings: BenchmarkSettings,
    fixtures: list[ETARequest],
    run_id: str,
) -> tuple[list[RequestSample], list[RequestSample], float]:
    warmup = []
    for sequence in range(settings.warmup):
        payload = fixtures[sequence % len(fixtures)].model_copy(
            update={"trip_id": f"benchmark-{run_id}-warmup-{sequence}"}
        )
        now = perf_counter()
        warmup.append(
            await _request(client, settings, payload, sequence=sequence, scheduled=now, origin=now)
        )

    samples: list[RequestSample] = []
    origin = perf_counter()
    # TaskGroup cleans up outstanding requests on interruption or an unexpected failure.
    async with asyncio.TaskGroup() as group:
        pending: set[asyncio.Task[RequestSample]] = set()
        for sequence in range(settings.requests):
            scheduled = origin + sequence / settings.rate
            await asyncio.sleep(max(0, scheduled - perf_counter()))
            completed = {task for task in pending if task.done()}
            samples.extend(task.result() for task in completed)
            pending.difference_update(completed)
            payload = fixtures[sequence % len(fixtures)].model_copy(
                update={"trip_id": f"benchmark-{run_id}-measured-{sequence}"}
            )
            if len(pending) >= settings.concurrency:
                samples.append(
                    RequestSample(
                        sequence=sequence,
                        trip_id=payload.trip_id,
                        scheduled_offset_seconds=sequence / settings.rate,
                        dispatch_lag_ms=max(0, (perf_counter() - scheduled) * 1000),
                        outcome="load_generator_overflow",
                    )
                )
            else:
                pending.add(
                    group.create_task(
                        _request(
                            client,
                            settings,
                            payload,
                            sequence=sequence,
                            scheduled=scheduled,
                            origin=origin,
                        )
                    )
                )
    samples.extend(task.result() for task in pending)
    # Include the final arrival interval in the throughput denominator for short runs.
    elapsed = max(perf_counter() - origin, settings.requests / settings.rate)
    return sorted(samples, key=lambda sample: sample.sequence), warmup, elapsed


def summarize(
    samples: list[RequestSample], settings: BenchmarkSettings, elapsed: float
) -> dict[str, Any]:
    successes = [sample for sample in samples if sample.outcome == "success"]
    outcomes = dict(sorted(Counter(sample.outcome for sample in samples).items()))
    error_rate = (settings.requests - len(successes)) / settings.requests
    latency = percentiles(
        [sample.latency_ms for sample in successes if sample.latency_ms is not None]
    )
    gates = {
        "all_arrivals_accounted_for": len(samples) == settings.requests,
        "successful_predictions": bool(successes),
        "error_rate": error_rate <= settings.max_error_rate,
        "no_load_generator_overflow": outcomes.get("load_generator_overflow", 0) == 0,
        "p95_latency": latency["p95"] is not None and latency["p95"] < settings.p95_objective_ms,
    }
    return {
        "passed": all(gates.values()),
        "gates": gates,
        "scheduled_requests": settings.requests,
        "successful_requests": len(successes),
        "outcomes": outcomes,
        "error_rate": error_rate,
        "elapsed_seconds": elapsed,
        "successful_requests_per_second": len(successes) / elapsed,
        "successful_latency_ms": latency,
        "all_attempted_latency_ms": percentiles(
            [sample.latency_ms for sample in samples if sample.latency_ms is not None]
        ),
        "http_latency_ms": percentiles(
            [sample.http_latency_ms for sample in samples if sample.http_latency_ms is not None]
        ),
        "dispatch_lag_ms": percentiles([sample.dispatch_lag_ms for sample in samples]),
        "fallback_rate_among_successes": (
            sum(sample.feature_fallback is True for sample in successes) / len(successes)
            if successes
            else None
        ),
        "model_versions": dict(Counter(s.model_version for s in samples if s.model_version)),
        "publication_modes": dict(Counter(s.publication for s in samples if s.publication)),
        "http_statuses": dict(Counter(str(s.status_code) for s in samples if s.status_code)),
    }


async def run_benchmark(
    settings: BenchmarkSettings,
    *,
    requests_path: Path,
    output: Path,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, Any]:
    fixtures = load_requests(requests_path)
    # Require a new directory: never overwrite evidence from a previous run.
    output.mkdir(parents=True, exist_ok=False)
    run_id = uuid4().hex
    started_at = datetime.now(UTC).isoformat()
    fixture_bytes = "".join(request.model_dump_json() + "\n" for request in fixtures).encode()
    (output / "requests.jsonl").write_bytes(fixture_bytes)
    configuration = settings.model_dump()
    (output / "config.json").write_text(
        json.dumps(configuration, indent=2) + "\n", encoding="utf-8"
    )
    async with httpx.AsyncClient(
        base_url=settings.base_url,
        timeout=settings.timeout_seconds,
        limits=httpx.Limits(
            max_connections=settings.concurrency, max_keepalive_connections=settings.concurrency
        ),
        follow_redirects=False,
        trust_env=False,
        transport=transport,
    ) as client:
        samples, warmup, elapsed = await _measure(client, settings, fixtures, run_id)
    for name, rows in (("samples.jsonl", samples), ("warmup.jsonl", warmup)):
        (output / name).write_text(
            "".join(row.model_dump_json() + "\n" for row in rows), encoding="utf-8"
        )
    report = {
        "schema_version": "1.0",
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": datetime.now(UTC).isoformat(),
        "settings": configuration,
        "client_environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "httpx": httpx.__version__,
        },
        "artifacts_sha256": {
            name: hashlib.sha256((output / name).read_bytes()).hexdigest()
            for name in ("requests.jsonl", "config.json", "samples.jsonl", "warmup.jsonl")
        },
        "warmup_outcomes": dict(Counter(sample.outcome for sample in warmup)),
        **summarize(samples, settings, elapsed),
    }
    (output / "summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    p95 = report["successful_latency_ms"]["p95"]
    p95_text = f"{p95:.3f} ms" if p95 is not None else "No successful predictions"
    (output / "README.md").write_text(
        "# Serving benchmark\n\n"
        f"Run `{run_id}`: **{'PASS' if report['passed'] else 'FAIL'}**\n\n"
        "| Measurement | Result |\n|---|---:|\n"
        f"| Scheduled requests | {settings.requests} |\n"
        f"| Offered rate | {settings.rate:g} requests/s |\n"
        f"| Successful predictions | {report['successful_requests']} |\n"
        f"| Successful throughput | {report['successful_requests_per_second']:.3f} requests/s |\n"
        f"| Successful P95 including dispatch lag | {p95_text} |\n"
        f"| Error rate including dropped arrivals | {report['error_rate']:.3%} |\n\n"
        "[Summary and gates](summary.json), [raw measured samples](samples.jsonl), "
        "[warm-up samples](warmup.jsonl), [configuration](config.json), "
        "[request fixture](requests.jsonl). SHA-256 digests are recorded in the summary.\n\n"
        "Latency runs from scheduled arrival through response validation. Warm-up is excluded. "
        "Connection pooling is enabled; requests are never retried. Publication is verified "
        "through the API acknowledgment header, not a separate consumer. "
        "This run does not establish model accuracy, feature parity, autoscaling, or production "
        "capacity. Record the target hardware, deployment, model provenance, and dependency "
        "state alongside these files before using the numbers as portfolio evidence.\n",
        encoding="utf-8",
    )
    return report
