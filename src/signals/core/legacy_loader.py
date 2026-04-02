from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path


def load_module(module_name: str, file_path: str):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module {module_name} from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def ensure_env_for_legacy_insider() -> None:
    os.environ.setdefault("SKIP_CONFIG_VALIDATION", "1")

