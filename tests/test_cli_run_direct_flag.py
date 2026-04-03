from __future__ import annotations

from argparse import Namespace

import pytest

from signals.cli import cmd_run, cmd_validate_live


def test_run_requires_direct_args_unless_legacy():
    args = Namespace(
        legacy=False,
        csv=None,
        sec_user_agent=None,
        insider_cache_dir="/tmp/insider",
        congress_cache_dir="/tmp/congress",
        insider_max_filings=None,
        house_days=90,
        house_max_filings=None,
        senate_days=365,
        senate_max_filings=None,
        date="2026-04-02",
        db="/tmp/derived.db",
        format="json",
        window=90,
        artifacts_dir=None,
        insider_legacy_db="/tmp/insider.db",
        congress_legacy_db="/tmp/congress.db",
    )
    with pytest.raises(SystemExit) as exc:
        cmd_run(args)
    assert "--csv" in str(exc.value)


def test_run_legacy_does_not_require_direct_args(monkeypatch):
    monkeypatch.setattr(
        "signals.cli.run_unified_pipeline",
        lambda **kwargs: type(
            "R",
            (),
            {
                "insider": {"imported_result_count": 1},
                "congress": {"imported_result_count": 2},
                "combined": {"combined_count": 0},
                "artifact_paths": {},
                "to_dict": lambda self: {"ok": True},
            },
        )(),
    )
    args = Namespace(
        legacy=True,
        csv=None,
        sec_user_agent=None,
        insider_cache_dir="/tmp/insider",
        congress_cache_dir="/tmp/congress",
        insider_max_filings=None,
        house_days=90,
        house_max_filings=None,
        senate_days=365,
        senate_max_filings=None,
        date="2026-04-02",
        db="/tmp/derived.db",
        format="json",
        window=90,
        artifacts_dir=None,
        insider_legacy_db="/tmp/insider.db",
        congress_legacy_db="/tmp/congress.db",
    )
    cmd_run(args)


def test_validate_live_prints_analysis_json(monkeypatch, capsys):
    monkeypatch.setattr(
        "signals.cli.run_direct_pipeline",
        lambda **kwargs: type(
            "R",
            (),
            {
                "analysis": {"assessment": {"readiness": "operationally_ready"}},
                "artifact_paths": {"production_confidence_report": "/tmp/report.json"},
                "to_dict": lambda self: {"ok": True},
            },
        )(),
    )
    args = Namespace(
        csv="/tmp/u.csv",
        sec_user_agent="DisclosureSignals/1.0 (test@example.com)",
        insider_cache_dir="/tmp/insider",
        congress_cache_dir="/tmp/congress",
        insider_max_filings=2,
        house_days=90,
        house_max_filings=10,
        senate_days=365,
        senate_max_filings=10,
        date="2026-04-02",
        db="/tmp/derived.db",
        format="json",
        window=90,
        artifacts_dir="/tmp/artifacts",
    )
    cmd_validate_live(args)
    out = capsys.readouterr().out
    assert "production_confidence_report" in out
    assert "operationally_ready" in out
