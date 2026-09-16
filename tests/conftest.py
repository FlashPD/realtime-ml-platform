from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault(
    "AIRFLOW_HOME",
    str(Path(__file__).resolve().parents[1] / ".tripml" / "airflow-tests"),
)
