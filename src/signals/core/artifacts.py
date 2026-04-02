from __future__ import annotations

import json
from pathlib import Path


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def write_text(path: Path, content: str) -> Path:
    path.write_text(content + ("\n" if not content.endswith("\n") else ""))
    return path

