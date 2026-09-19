"""Run inside the isolated kind test pod; retain evidence until the namespace is removed."""

from __future__ import annotations

import asyncio
import json
import os
import traceback
from pathlib import Path
from time import sleep

from redis import Redis

from tripml.benchmark import BenchmarkSettings, run_benchmark
from tripml.contracts import OnlineZoneWindowFeatures
from tripml.online_features import feature_key


def main() -> None:
    output = Path("/tmp/evidence")
    output.mkdir()
    result: dict[str, object]
    try:
        snapshots = [
            OnlineZoneWindowFeatures.model_validate(item)
            for item in json.loads(Path("/load/snapshots.json").read_text())
        ]
        with (
            Redis.from_url(
                os.environ["TRIPML_SERVING__REDIS_URL"],
                socket_connect_timeout=2,
                socket_timeout=2,
            ) as client,
            client.pipeline(transaction=True) as pipeline,
        ):
            for snapshot in snapshots:
                pipeline.set(
                    feature_key(snapshot.zone_role, snapshot.zone_id, snapshot.window_kind),
                    snapshot.model_dump_json(),
                    ex=1800,
                )
            pipeline.execute()
        report = asyncio.run(
            run_benchmark(
                BenchmarkSettings(
                    base_url=os.environ["TRIPML_LOAD_URL"],
                    requests=18_000,
                    rate=100,
                    # Cover the 100/s * 2s timeout window, with headroom for scheduling jitter.
                    concurrency=256,
                    no_keepalive=True,
                    expected_features="any",
                    expected_publication="acknowledged",
                    label="synthetic-kind-hpa",
                ),
                requests_path=Path("/load/requests.jsonl"),
                output=output / "benchmark",
            )
        )
        fallback = report["fallback_rate_among_successes"]
        checks = {
            "benchmark_objectives": report["passed"],
            "fallback_under_one_percent": fallback is not None and fallback < 0.01,
        }
        result = {"passed": all(checks.values()), "checks": checks}
    except Exception:
        error = traceback.format_exc()
        (output / "error.txt").write_text(error)
        result = {"passed": False, "error": "load driver failed; inspect error.txt"}
    temporary = output / "result.tmp"
    temporary.write_text(json.dumps(result) + "\n")
    temporary.replace(output / "result.json")
    # Keep the emptyDir available for kubectl cp even when a benchmark gate failed.
    while True:
        sleep(30)


if __name__ == "__main__":
    main()
