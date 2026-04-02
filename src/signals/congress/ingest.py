from __future__ import annotations

import io
import sys
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

from signals.core.legacy_loader import load_module


HOUSE_FD_ZIP_URL = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.ZIP"


_HOUSE_CONNECTOR_MODULE = None


def _house_connector_class(repo_root: Path):
    global _HOUSE_CONNECTOR_MODULE
    if _HOUSE_CONNECTOR_MODULE is None:
        legacy_root = repo_root / "legacy-congress"
        if str(legacy_root) not in sys.path:
            sys.path.insert(0, str(legacy_root))
        _HOUSE_CONNECTOR_MODULE = load_module(
            "signals_legacy_congress_house_connector_direct",
            str(legacy_root / "cppi" / "connectors" / "house.py"),
        )
    return _HOUSE_CONNECTOR_MODULE.HouseConnector


@dataclass
class DirectHouseIngestResult:
    years: list[int]
    ptr_count: int
    downloaded_count: int
    skipped_cached_count: int
    failed_count: int
    cache_dir: str
    pdf_dir: str

    def to_dict(self) -> dict:
        return asdict(self)


def _download_fd_xml_ptrs(years: list[int], cache_dir: Path) -> list[dict]:
    ptrs: list[dict] = []
    fd_cache = cache_dir / "fd_xml"
    fd_cache.mkdir(parents=True, exist_ok=True)

    for year in years:
        xml_cache = fd_cache / f"{year}FD.xml"
        if xml_cache.exists():
            xml_content = xml_cache.read_text()
        else:
            url = HOUSE_FD_ZIP_URL.format(year=year)
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                xml_filename = f"{year}FD.xml"
                if xml_filename not in zf.namelist():
                    continue
                xml_content = zf.read(xml_filename).decode("utf-8")
                xml_cache.write_text(xml_content)

        root = ET.fromstring(xml_content)
        for member in root.findall(".//Member"):
            if member.findtext("FilingType", "") != "P":
                continue
            doc_id = member.findtext("DocID", "").strip()
            if not doc_id:
                continue
            filing_date = member.findtext("FilingDate", "").strip()
            ptrs.append(
                {
                    "doc_id": doc_id,
                    "filing_date": filing_date,
                    "year": year,
                    "name": " ".join(
                        p
                        for p in [
                            member.findtext("First", "").strip(),
                            member.findtext("Last", "").strip(),
                            member.findtext("Suffix", "").strip(),
                        ]
                        if p
                    ),
                    "state_district": member.findtext("StateDst", "").strip(),
                }
            )
    return ptrs


def _filter_ptrs_by_days(ptrs: list[dict], days: int) -> list[dict]:
    if days >= 3650:
        return ptrs
    cutoff = datetime.now() - timedelta(days=days)
    filtered: list[dict] = []
    for ptr in ptrs:
        fd = ptr.get("filing_date", "")
        if not fd:
            filtered.append(ptr)
            continue
        parsed = None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(fd, fmt)
                break
            except ValueError:
                continue
        if parsed is None or parsed >= cutoff:
            filtered.append(ptr)
    return filtered


def ingest_house_ptrs_direct(
    *,
    repo_root: Path,
    cache_dir: str,
    days: int,
    max_filings: int | None = None,
    force: bool = False,
) -> DirectHouseIngestResult:
    cache_root = Path(cache_dir)
    current_year = datetime.now().year
    years = list(range(2024, current_year + 1))
    ptrs = _filter_ptrs_by_days(_download_fd_xml_ptrs(years, cache_root), days)
    if max_filings is not None:
        ptrs = ptrs[:max_filings]

    HouseConnector = _house_connector_class(repo_root)
    house = HouseConnector(cache_dir=cache_root, request_delay=0.25)

    downloaded = 0
    skipped = 0
    failed = 0
    for ptr in ptrs:
        cache_path = Path(house.cache_dir) / f"{ptr['doc_id']}.pdf"
        if cache_path.exists() and not force:
            skipped += 1
            continue
        result = house.download_pdf(ptr["doc_id"], year=ptr["year"], force=force)
        if result is None:
            failed += 1
        else:
            downloaded += 1

    return DirectHouseIngestResult(
        years=years,
        ptr_count=len(ptrs),
        downloaded_count=downloaded,
        skipped_cached_count=skipped,
        failed_count=failed,
        cache_dir=str(cache_root),
        pdf_dir=str(Path(house.cache_dir)),
    )
