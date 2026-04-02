from __future__ import annotations

from pathlib import Path

from signals.congress.house_parser import parse_house_pdf_text_only as _parse_house_pdf_text_only


def parse_house_pdf_text_only(repo_root: Path, pdf_path: Path):
    del repo_root
    return _parse_house_pdf_text_only(pdf_path)
