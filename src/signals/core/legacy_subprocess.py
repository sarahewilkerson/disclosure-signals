from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def run_legacy_cli(script_path: Path, args: list[str], extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, **(extra_env or {})}
    cwd = str(script_path.parent)
    if script_path.parent.name == "cppi":
        cwd = str(script_path.parent.parent)
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = os.pathsep.join([cwd, existing]) if existing else cwd
    return subprocess.run(
        [sys.executable, str(script_path), *args],
        text=True,
        capture_output=True,
        env=env,
        cwd=cwd,
        check=True,
    )
