"""Command-line entry point for developer and automation workflows."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from tripml.contracts import contract_schemas
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


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "config" and args.config_command == "validate":
        return _validate_config(args.config)
    if args.command == "contracts" and args.contracts_command == "export":
        return _export_contracts(args.output)
    if args.command == "ingest":
        return _ingest(args.month, args.config, args.source)
    raise AssertionError("unreachable command")


if __name__ == "__main__":
    raise SystemExit(main())
