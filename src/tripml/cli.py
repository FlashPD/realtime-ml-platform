"""Command-line entry point for developer and automation workflows."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Sequence
from pathlib import Path

from tripml.contracts import contract_schemas
from tripml.features import build_gold_features
from tripml.settings import load_settings, settings_as_dict
from tripml.workflows.ingestion import run_ingestion


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tripml", description="Real-time ML platform tooling")
    commands = parser.add_subparsers(dest="command", required=True)

    config = commands.add_parser("config", help="Work with platform configuration")
    config_commands = config.add_subparsers(dest="config_command", required=True)
    validate = config_commands.add_parser("validate", help="Validate and print configuration")
    validate.add_argument("--config", type=Path, help="Optional YAML configuration path")

    contracts = commands.add_parser("contracts", help="Work with versioned data contracts")
    contract_commands = contracts.add_subparsers(dest="contracts_command", required=True)
    export = contract_commands.add_parser("export", help="Export contracts as JSON Schema")
    export.add_argument("--output", type=Path, required=True, help="Destination directory")

    ingest = commands.add_parser("ingest", help="Ingest and validate one TLC partition")
    ingest.add_argument("--month", required=True, help="Source month in YYYY-MM format")
    ingest.add_argument("--config", type=Path, help="Optional YAML configuration path")
    ingest.add_argument(
        "--source",
        type=Path,
        help="Validate a local Parquet file instead of downloading the official source",
    )

    features = commands.add_parser("features", help="Build offline feature tables")
    feature_commands = features.add_subparsers(dest="features_command", required=True)
    build = feature_commands.add_parser(
        "build", help="Build point-in-time-correct gold features for one month"
    )
    build.add_argument("--month", required=True, help="Target month in YYYY-MM format")
    build.add_argument("--config", type=Path, help="Optional YAML configuration path")

    train = commands.add_parser("train", help="Train, evaluate, and gate model candidates")
    train.add_argument("--config", type=Path, help="Optional YAML configuration path")
    train.add_argument(
        "--no-track",
        action="store_true",
        help="Build the local model bundle without publishing it to MLflow",
    )
    serve = commands.add_parser("serve", help="Serve ETA predictions from a verified model")
    serve.add_argument("--config", type=Path, help="Optional YAML configuration path")
    serve.add_argument(
        "--bundle", type=Path, help="Use a local training bundle in development mode"
    )
    serve.add_argument("--host", default="127.0.0.1", help="Bind address (default: loopback)")
    serve.add_argument("--port", type=int, default=8000, help="HTTP port (default: 8000)")
    publication = commands.add_parser("publication", help="Manage prediction publication resources")
    publication_commands = publication.add_subparsers(dest="publication_command", required=True)
    topic = publication_commands.add_parser(
        "ensure-topic", help="Create or verify the prediction topic"
    )
    topic.add_argument("--config", type=Path, help="Optional YAML configuration path")
    topic.add_argument("--partitions", type=int, default=3)
    topic.add_argument("--replication-factor", type=int, default=1)
    benchmark = commands.add_parser("benchmark", help="Measure serving under constant-arrival load")
    benchmark.add_argument("--requests-file", type=Path, required=True, help="ETARequest JSONL")
    benchmark.add_argument("--output", type=Path, required=True, help="New evidence directory")
    benchmark.add_argument("--base-url", default="http://127.0.0.1:8000")
    benchmark.add_argument("--requests", type=int, default=1000)
    benchmark.add_argument("--rate", type=float, default=100, help="Scheduled requests per second")
    benchmark.add_argument("--concurrency", type=int, default=32, help="Maximum in-flight requests")
    benchmark.add_argument(
        "--no-keepalive",
        action="store_true",
        help="Open a new connection per request to exercise Service balancing during scaling",
    )
    benchmark.add_argument("--warmup", type=int, default=20)
    benchmark.add_argument("--timeout-seconds", type=float, default=2)
    benchmark.add_argument("--p95-objective-ms", type=float, default=50)
    benchmark.add_argument("--max-error-rate", type=float, default=0.01)
    benchmark.add_argument(
        "--expected-features", choices=("any", "static", "streaming"), default="any"
    )
    benchmark.add_argument(
        "--expected-publication", choices=("acknowledged", "disabled"), default="acknowledged"
    )
    benchmark.add_argument("--label", default="serving-load")
    return parser


def _validate_config(config_path: Path | None) -> int:
    settings = load_settings(config_path)
    document = {
        "fingerprint": settings.fingerprint,
        "settings": settings_as_dict(settings),
        "valid": True,
    }
    print(json.dumps(document, indent=2, sort_keys=True))
    return 0


def _export_contracts(output: Path) -> int:
    output.mkdir(parents=True, exist_ok=True)
    schemas = contract_schemas()
    for subject, schema in schemas.items():
        destination = output / f"{subject}.json"
        destination.write_text(
            json.dumps(schema, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    print(f"Exported {len(schemas)} contracts to {output}")
    return 0


def _ingest(month_value: str, config_path: Path | None, source: Path | None) -> int:
    execution = run_ingestion(month_value, config_path=config_path, source=source)
    print(json.dumps(execution.as_document(), indent=2, sort_keys=True))
    return execution.exit_code


def _build_features(month_value: str, config_path: Path | None) -> int:
    settings = load_settings(config_path)
    report = build_gold_features(month_value, settings=settings)
    print(json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True))
    return 0


def _train(config_path: Path | None, *, track: bool) -> int:
    try:
        from tripml.training import train_models
    except (ImportError, OSError) as error:
        raise RuntimeError(
            "training dependencies are unavailable; install the 'training' extra and, "
            "on macOS, Homebrew libomp"
        ) from error

    settings = load_settings(config_path)
    if track:
        try:
            from tripml.tracking import run_training_workflow
        except (ImportError, OSError) as error:
            raise RuntimeError(
                "MLflow is unavailable; install the 'tracking' extra or use --no-track"
            ) from error
        document = run_training_workflow(settings).model_dump(mode="json")
    else:
        document = train_models(settings).model_dump(mode="json")
    print(json.dumps(document, indent=2, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "config" and args.config_command == "validate":
        return _validate_config(args.config)
    if args.command == "contracts" and args.contracts_command == "export":
        return _export_contracts(args.output)
    if args.command == "ingest":
        return _ingest(args.month, args.config, args.source)
    if args.command == "features" and args.features_command == "build":
        return _build_features(args.month, args.config)
    if args.command == "train":
        return _train(args.config, track=not args.no_track)
    if args.command == "serve":
        import uvicorn

        from tripml.serving import create_app

        uvicorn.run(
            create_app(load_settings(args.config), bundle=args.bundle),
            host=args.host,
            port=args.port,
        )
        return 0
    if args.command == "publication":
        from tripml.publication import ensure_prediction_topic

        settings = load_settings(args.config)
        ensure_prediction_topic(
            settings.publication,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
        )
        print(f"Prediction topic ready: {settings.publication.topic}")
        return 0
    if args.command == "benchmark":
        from tripml.benchmark import BenchmarkSettings, run_benchmark

        options = vars(args).copy()
        for key in ("command", "requests_file", "output"):
            options.pop(key)
        report = asyncio.run(
            run_benchmark(
                BenchmarkSettings(**options), requests_path=args.requests_file, output=args.output
            )
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["passed"] else 1
    raise AssertionError("unreachable command")


if __name__ == "__main__":
    raise SystemExit(main())
