from __future__ import annotations

import sys
from pathlib import Path

import pdfplumber

from signals.core.legacy_loader import load_module


_LEGACY_PARSING_MODULE = None


def _legacy_parsing_module(repo_root: Path):
    global _LEGACY_PARSING_MODULE
    if _LEGACY_PARSING_MODULE is None:
        legacy_root = repo_root / "legacy-congress"
        if str(legacy_root) not in sys.path:
            sys.path.insert(0, str(legacy_root))
        _LEGACY_PARSING_MODULE = load_module(
            "signals_legacy_congress_parsing_direct",
            str(legacy_root / "cppi" / "parsing.py"),
        )
    return _LEGACY_PARSING_MODULE


def pdf_has_extractable_text(pdf_path: Path) -> bool:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return any((page.extract_text() or "").strip() for page in pdf.pages)
    except Exception:
        return False


def parse_house_pdf_text_only(repo_root: Path, pdf_path: Path):
    module = _legacy_parsing_module(repo_root)
    if pdf_path.stem.startswith("822"):
        return None, "paper_filing"
    if not pdf_has_extractable_text(pdf_path):
        return None, "no_extractable_text"
    parser = module.HousePDFParser()
    filing = parser.parse(pdf_path)
    if filing.parse_errors:
        return filing, "parse_errors"
    return filing, None
