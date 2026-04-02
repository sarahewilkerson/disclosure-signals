from __future__ import annotations

import sys

from signals.core.legacy_loader import ensure_env_for_legacy_insider, load_module, repo_root


_MODULES = {}


def _module(name: str, relative: str):
    if name not in _MODULES:
        ensure_env_for_legacy_insider()
        legacy_root = repo_root() / "legacy-insider"
        if str(legacy_root) not in sys.path:
            sys.path.insert(0, str(legacy_root))
        _MODULES[name] = load_module(name, str(repo_root() / relative))
    return _MODULES[name]


def parse_form4_xml(xml_path: str) -> dict:
    return _module("legacy_insider_parsing", "legacy-insider/parsing.py").parse_form4_xml(xml_path)


def classify_role(*args, **kwargs):
    return _module("legacy_insider_classification", "legacy-insider/classification.py").classify_role(*args, **kwargs)


def classify_transaction_type(*args, **kwargs):
    return _module("legacy_insider_classification", "legacy-insider/classification.py").classify_transaction_type(*args, **kwargs)


def detect_planned_trade(*args, **kwargs):
    return _module("legacy_insider_classification", "legacy-insider/classification.py").detect_planned_trade(*args, **kwargs)


def compute_pct_holdings_changed(*args, **kwargs):
    return _module("legacy_insider_classification", "legacy-insider/classification.py").compute_pct_holdings_changed(*args, **kwargs)


def score_transaction(*args, **kwargs):
    return _module("legacy_insider_scoring", "legacy-insider/scoring.py").score_transaction(*args, **kwargs)


CIK_TO_TICKER = {
    "0000320193": ("entity:aapl", "AAPL", "Apple Inc."),
}


def resolve_issuer(cik_issuer: str) -> tuple[str | None, str | None, str | None]:
    return CIK_TO_TICKER.get(cik_issuer, (None, None, None))


def scoring_service_module():
    return _module("legacy_insider_service_scoring", "legacy-insider/services/scoring_service.py")


def reporting_service_module():
    return _module("legacy_insider_service_reporting", "legacy-insider/services/reporting_service.py")


def status_service_module():
    return _module("legacy_insider_service_status", "legacy-insider/services/status_service.py")


def db_module():
    return _module("legacy_insider_db", "legacy-insider/db.py")
