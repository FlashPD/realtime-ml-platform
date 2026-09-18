"""Capture a local command's logs, exit status, elapsed time and child-process peak RSS.

Unix only. Peak RSS is the largest child-process high-water mark, not aggregate tree memory.
Use a fresh output directory for every attempt. Arguments are recorded; do not pass secrets.
"""

from __future__ import annotations

import argparse
import json
import platform
import resource
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command is required after --")
    args.output.mkdir(parents=True, exist_ok=False)
    started_at = datetime.now(UTC).isoformat()
    started = perf_counter()
    with (
        (args.output / "stdout.log").open("w") as stdout,
        (args.output / "stderr.log").open("w") as stderr,
    ):
        result = subprocess.run(command, stdout=stdout, stderr=stderr, check=False)
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    summary = {
        "command": command,
        "started_at": started_at,
        "elapsed_seconds": perf_counter() - started,
        "exit_code": result.returncode,
        "max_child_rss_bytes": usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024),
        "child_user_seconds": usage.ru_utime,
        "child_system_seconds": usage.ru_stime,
        "platform": platform.platform(),
        "python_version": platform.python_version(),
    }
    (args.output / "resource.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    return result.returncode if result.returncode >= 0 else 128 - result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
