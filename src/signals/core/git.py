from __future__ import annotations

import subprocess
from pathlib import Path


def git_sha(repo_root: Path) -> str:
    try:
        head = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        dirty = subprocess.run(
            ["git", "-C", str(repo_root), "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return "workspace"
    sha = head.stdout.strip() or "workspace"
    if sha == "workspace":
        return sha
    return f"{sha}-dirty" if dirty.stdout.strip() else sha
