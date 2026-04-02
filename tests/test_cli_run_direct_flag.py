from __future__ import annotations

from argparse import Namespace

import pytest

from signals.cli import cmd_run


def test_run_direct_requires_direct_args():
    args = Namespace(
        direct=True,
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
