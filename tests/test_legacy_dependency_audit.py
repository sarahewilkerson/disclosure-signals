from __future__ import annotations

import subprocess
from pathlib import Path


def _legacy_reference_paths(root: Path) -> set[str]:
    result = subprocess.run(
        [
            "rg",
            "-l",
            "legacy-insider|legacy-congress|run_legacy_cli|legacy_bridge|legacy_loader|legacy_subprocess",
            "src",
            "tests",
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def test_legacy_references_are_quarantined():
    root = Path(__file__).resolve().parents[1]
    expected = {
        "src/signals/cli.py",
        "src/signals/congress/legacy_bridge.py",
        "src/signals/congress/service.py",
        "src/signals/core/legacy_loader.py",
        "src/signals/core/legacy_subprocess.py",
        "src/signals/insider/legacy_bridge.py",
        "src/signals/insider/service.py",
        "tests/test_legacy_dependency_audit.py",
        "tests/test_unified_legacy_workflows.py",
    }

    actual = _legacy_reference_paths(root)
    assert actual == expected


def test_repo_native_parity_fixtures_exist():
    root = Path(__file__).resolve().parents[1]
    expected = {
        "tests/fixtures/insider/form4_simple_buy.xml",
        "tests/fixtures/expected_parity/insider_engine_single.json",
        "tests/fixtures/expected_parity/insider_engine_agg.json",
        "tests/fixtures/expected_parity/insider_flow.json",
        "tests/fixtures/expected_parity/congress_engine.json",
        "tests/fixtures/expected_parity/congress_flow.json",
    }
    missing = {path for path in expected if not (root / path).exists()}
    assert not missing
