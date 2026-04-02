from __future__ import annotations

import sys
from pathlib import Path

from signals.core.legacy_loader import load_module, repo_root


_MODULES = {}


def _prepend_repo_root() -> None:
    root = repo_root() / "legacy-congress"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def _module(name: str, relative: str):
    if name not in _MODULES:
        _prepend_repo_root()
        _MODULES[name] = load_module(name, str(repo_root() / relative))
    return _MODULES[name]


def senate_connector_class():
    return _module("legacy_congress_senate", "legacy-congress/cppi/connectors/senate.py").SenateConnector


def entity_resolver_class():
    return _module("legacy_congress_resolution", "legacy-congress/cppi/resolution.py").EntityResolver


def score_transaction(*args, **kwargs):
    return _module("legacy_congress_scoring", "legacy-congress/cppi/scoring.py").score_transaction(*args, **kwargs)


def compute_aggregate(*args, **kwargs):
    return _module("legacy_congress_scoring", "legacy-congress/cppi/scoring.py").compute_aggregate(*args, **kwargs)


def compute_confidence_score(*args, **kwargs):
    return _module("legacy_congress_scoring", "legacy-congress/cppi/scoring.py").compute_confidence_score(*args, **kwargs)


def scoring_service_module():
    return _module("legacy_congress_service_scoring", "legacy-congress/cppi/services/scoring_service.py")


def reporting_service_module():
    return _module("legacy_congress_service_reporting", "legacy-congress/cppi/services/reporting_service.py")


def status_service_module():
    return _module("legacy_congress_service_status", "legacy-congress/cppi/services/status_service.py")


def db_module():
    return _module("legacy_congress_db", "legacy-congress/cppi/db.py")
