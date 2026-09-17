from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from pydantic import ValidationError

from tripml.benchmark import (
    BenchmarkSettings,
    RequestSample,
    load_requests,
    percentiles,
    run_benchmark,
    summarize,
)
from tripml.cli import main
from tripml.contracts import Prediction

FIXTURE = Path(__file__).resolve().parents[1] / "examples/benchmark/requests.jsonl"


def response(request: httpx.Request, *, fallback: bool = True) -> httpx.Response:
    payload = json.loads(request.content)
    prediction = Prediction(
        trip_id=payload["trip_id"],
        model_version="fixture-static" if fallback else "fixture-streaming",
        features_used={},
        feature_timestamps={},
        feature_fallback=fallback,
        estimated_duration_seconds=600,
        served_at=datetime.now(UTC),
    )
    return httpx.Response(
        200,
        json=prediction.model_dump(mode="json"),
        headers={"X-TripML-Publication": "disabled"},
    )


def settings(**overrides: object) -> BenchmarkSettings:
    return BenchmarkSettings(
        **{
            "requests": 5,
            "warmup": 1,
            "rate": 100,
            "expected_features": "static",
            "expected_publication": "disabled",
            "p95_objective_ms": 1000,
            **overrides,
        }
    )


def test_benchmark_records_reproducible_evidence_and_excludes_warmup(tmp_path: Path) -> None:
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content)["trip_id"])
        if "warmup" in seen[-1]:
            return httpx.Response(503)
        return response(request)

    output = tmp_path / "run"
    report = asyncio.run(
        run_benchmark(
            settings(), requests_path=FIXTURE, output=output, transport=httpx.MockTransport(handler)
        )
    )
    assert report["passed"]
    assert report["successful_requests"] == 5
    assert report["fallback_rate_among_successes"] == 1
    assert report["model_versions"] == {"fixture-static": 5}
    assert report["warmup_outcomes"] == {"http_error": 1}
    assert len(set(seen)) == 6
    samples = [json.loads(line) for line in (output / "samples.jsonl").read_text().splitlines()]
    assert [sample["sequence"] for sample in samples] == list(range(5))
    assert all(sample["latency_ms"] >= sample["http_latency_ms"] for sample in samples)
    assert all(sample["prediction_id"] for sample in samples)
    assert json.loads((output / "summary.json").read_text()) == report
    assert "**PASS**" in (output / "README.md").read_text()
    for name, digest in report["artifacts_sha256"].items():
        assert hashlib.sha256((output / name).read_bytes()).hexdigest() == digest
    with pytest.raises(FileExistsError):
        asyncio.run(run_benchmark(settings(), requests_path=FIXTURE, output=output))


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        ("http", "http_error"),
        ("redirect", "http_error"),
        ("invalid", "invalid_prediction"),
        ("trip", "trip_id_mismatch"),
        ("publication", "publication_mismatch"),
        ("features", "feature_mode_mismatch"),
        ("timeout", "timeout"),
        ("deadline", "timeout"),
        ("connection", "transport_error"),
    ],
)
def test_fast_failures_cannot_pass_the_benchmark(
    tmp_path: Path, failure: str, expected: str
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        match failure:
            case "http":
                return httpx.Response(503)
            case "redirect":
                return httpx.Response(307, headers={"Location": "http://other/v1/eta"})
            case "invalid":
                return httpx.Response(200, json={"estimated_duration_seconds": -1})
            case "timeout":
                raise httpx.ReadTimeout("fixture")
            case "deadline":
                await asyncio.sleep(0.1)
            case "connection":
                raise httpx.ConnectError("fixture")
        result = response(request, fallback=failure != "features")
        if failure == "trip":
            result = httpx.Response(200, json=result.json() | {"trip_id": "wrong-trip"})
        if failure == "publication":
            result.headers.pop("X-TripML-Publication")
        return result

    report = asyncio.run(
        run_benchmark(
            settings(requests=1, warmup=0, timeout_seconds=0.02),
            requests_path=FIXTURE,
            output=tmp_path / "run",
            transport=httpx.MockTransport(handler),
        )
    )
    assert not report["passed"]
    assert report["outcomes"] == {expected: 1}
    assert report["error_rate"] == 1
    assert report["successful_latency_ms"]["p95"] is None
    assert report["fallback_rate_among_successes"] is None


def test_slow_service_does_not_reduce_offered_load_or_create_an_unbounded_queue(
    tmp_path: Path,
) -> None:
    in_flight = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.1)
        in_flight -= 1
        return response(request)

    report = asyncio.run(
        run_benchmark(
            settings(requests=20, rate=1000, concurrency=1, warmup=0),
            requests_path=FIXTURE,
            output=tmp_path / "overloaded",
            transport=httpx.MockTransport(handler),
        )
    )
    assert peak == 1
    assert not report["passed"]
    assert not report["gates"]["no_load_generator_overflow"]
    assert report["outcomes"]["load_generator_overflow"] > 0
    assert sum(report["outcomes"].values()) == 20


def test_exact_percentiles_and_objective_boundary() -> None:
    assert percentiles(list(range(1, 101))) == {"p50": 50, "p95": 95, "p99": 99}
    assert percentiles([]) == dict.fromkeys(("p50", "p95", "p99"))
    sample = RequestSample(
        sequence=0,
        trip_id="test",
        scheduled_offset_seconds=0,
        dispatch_lag_ms=49,
        latency_ms=50,
        http_latency_ms=1,
        outcome="success",
    )
    report = summarize([sample], settings(requests=1, p95_objective_ms=50), 1)
    assert not report["gates"]["p95_latency"]
    assert report["http_latency_ms"]["p95"] == 1
    assert summarize([sample], settings(requests=1, p95_objective_ms=51), 1)["passed"]
    samples = [sample] * 99 + [sample.model_copy(update={"outcome": "http_error"})]
    assert summarize(samples, settings(requests=100, max_error_rate=0.01), 1)["passed"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"requests": 0},
        {"requests": 100_001},
        {"concurrency": 0},
        {"rate": float("nan")},
        {"timeout_seconds": float("inf")},
        {"base_url": "http://user:secret@localhost:8000"},
        {"base_url": "http://localhost:8000/?token=secret"},
        {"base_url": "http://localhost:8000/path"},
        {"base_url": "file:///tmp/server"},
    ],
)
def test_invalid_configuration_is_rejected(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        settings(**overrides)


def test_fixtures_are_validated_before_creating_evidence(tmp_path: Path) -> None:
    fixture = tmp_path / "requests.jsonl"
    for content in ("\n", '{"trip_id":"incomplete"}\n'):
        fixture.write_text(content)
        with pytest.raises(ValueError, match=r"at least one|validation errors"):
            asyncio.run(run_benchmark(settings(), requests_path=fixture, output=tmp_path / "run"))
        assert not (tmp_path / "run").exists()
    fixture.write_text(FIXTURE.read_text().splitlines()[0] + "\n\n")
    assert len(load_requests(fixture)) == 1


@pytest.mark.parametrize("passed", [True, False])
def test_cli_exits_nonzero_when_benchmark_objectives_fail(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], passed: bool
) -> None:
    runner = AsyncMock(return_value={"passed": passed})
    with patch("tripml.benchmark.run_benchmark", runner):
        code = main(
            [
                "benchmark",
                "--requests-file",
                str(FIXTURE),
                "--output",
                str(tmp_path),
                "--expected-features",
                "streaming",
                "--rate",
                "200",
            ]
        )
    assert code == (0 if passed else 1)
    assert json.loads(capsys.readouterr().out) == {"passed": passed}
    assert runner.call_args.args[0].expected_features == "streaming"
    assert runner.call_args.args[0].expected_publication == "acknowledged"
    assert runner.call_args.args[0].rate == 200


def test_streaming_mode_accepts_streaming_predictions(tmp_path: Path) -> None:
    report = asyncio.run(
        run_benchmark(
            settings(expected_features="streaming", base_url="http://localhost/"),
            requests_path=FIXTURE,
            output=tmp_path / "streaming",
            transport=httpx.MockTransport(lambda request: response(request, fallback=False)),
        )
    )
    assert report["passed"]
    assert report["fallback_rate_among_successes"] == 0
