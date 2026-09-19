"""Check a deployed API and broker using registry-validation JSON on stdin.

Run inside the cluster with the serving image. This is a three-request deployment
smoke, not a latency or capacity benchmark. Prints a JSON receipt only on success.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import UTC, datetime
from time import monotonic
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition

from tripml.contracts import Prediction


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", required=True)
    args = parser.parse_args()
    evidence = json.load(sys.stdin)
    model_version = f"{evidence['training']['run_id']}-static"
    with urlopen(args.base_url + "/readyz", timeout=10) as response:
        readiness = json.load(response)
    require(readiness["mode"] == "static_primary", "wrong serving mode")
    require(readiness["model_version"] == model_version, "wrong deployed model")
    require(
        readiness["registry_version"] == evidence["publication"]["registered_version"],
        "wrong registry version",
    )
    require(readiness["feature_model_version"] == "gold-features-v2", "wrong feature version")
    cases = evidence["application_smokes"]
    require(set(cases) == {"known_positive", "zero", "unknown"}, "missing passenger cohort")
    smokes = {}
    predictions = {}
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": f"deployment-smoke-{uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        topic = consumer.list_topics(args.topic, timeout=10).topics[args.topic]
        require(topic.error is None, "prediction topic is unavailable")
        consumer.assign(
            [TopicPartition(args.topic, partition, 0) for partition in topic.partitions]
        )
        for group, case in cases.items():
            payload = case["request"] | {"trip_id": f"deployment-{group}-{uuid4().hex}"}
            request = Request(
                args.base_url + "/v1/eta",
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urlopen(request, timeout=10) as response:
                require(response.status == 200, f"{group} prediction failed")
                require(
                    response.headers["X-TripML-Publication"] == "acknowledged",
                    "HTTP success was not acknowledged",
                )
                prediction = Prediction.model_validate_json(response.read())
            require(prediction.model_version == model_version, "prediction used wrong model")
            require(prediction.schema_version == "1.1", "wrong prediction schema")
            require(prediction.trip_id == payload["trip_id"], "trip identity changed")
            require(prediction.feature_fallback and not prediction.feature_timestamps, "not static")
            require(
                prediction.features_used == case["prediction"]["features_used"],
                "feature values changed",
            )
            require(
                math.isclose(
                    prediction.estimated_duration_seconds,
                    case["native_prediction_seconds"],
                    rel_tol=1e-12,
                ),
                "native/API prediction mismatch",
            )
            predictions[str(prediction.prediction_id)] = prediction
            smokes[group] = {
                "request": payload,
                "prediction": prediction.model_dump(mode="json"),
                "native_prediction_seconds": case["native_prediction_seconds"],
                "publication": "acknowledged",
            }
        matched = {}
        deadline = monotonic() + 20
        while len(matched) < len(predictions) and monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            require(message.error() is None, f"broker read failed: {message.error()}")
            prediction = Prediction.model_validate_json(message.value())
            identity = str(prediction.prediction_id)
            if identity not in predictions:
                continue
            require(prediction == predictions[identity], "broker/HTTP payload mismatch")
            require(message.key() == prediction.trip_id.encode(), "wrong broker key")
            headers = dict(message.headers())
            require(headers["schema-version"] == b"1.1", "wrong broker schema")
            require(headers["prediction-id"] == identity.encode(), "wrong broker prediction ID")
            matched[identity] = {"partition": message.partition(), "offset": message.offset()}
        require(len(matched) == 3, "acknowledged predictions were not all consumed")
    finally:
        consumer.close()
    invalid = cases["unknown"]["request"] | {"schema_version": "1.0"}
    try:
        with urlopen(
            Request(
                args.base_url + "/v1/eta",
                data=json.dumps(invalid).encode(),
                headers={"Content-Type": "application/json"},
            ),
            timeout=10,
        ):
            raise ValueError("legacy schema accepted an unknown passenger count")
    except HTTPError as error:
        require(error.code == 422, "wrong rejection status for legacy null request")
    print(
        json.dumps(
            {
                "validated_at": datetime.now(UTC).isoformat(),
                "scope": "In-cluster Service HTTP and broker readback; no load/capacity claim",
                "base_url": args.base_url,
                "topic": args.topic,
                "readiness": readiness,
                "application_smokes": smokes,
                "broker_records": matched,
                "legacy_null_http_status": 422,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
