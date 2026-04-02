import json
import os
import subprocess
import sys
from pathlib import Path


def test_doctor_json():
    repo = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "signals.cli", "--format", "json", "doctor"],
        cwd=repo,
        env={**os.environ, "PYTHONPATH": "src"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert "db_counts" in payload
