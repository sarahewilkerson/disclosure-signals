from __future__ import annotations

import csv
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

from signals.core.retry import retry_call


SEC_BASE_URL = "https://www.sec.gov"
SEC_DATA_URL = "https://data.sec.gov"
SEC_EFTS_URL = "https://efts.sec.gov/LATEST"
SEC_ARCHIVES_URL = f"{SEC_BASE_URL}/Archives/edgar/data"
SEC_COMPANY_TICKERS_URL = f"{SEC_BASE_URL}/files/company_tickers.json"
SEC_RATE_LIMIT_DELAY = 0.12
SEC_MAX_RETRIES = 3


class DirectEdgarClient:
    def __init__(self, user_agent: str):
        if "example.com" in user_agent.lower() or "@" not in user_agent or "(" not in user_agent:
            raise ValueError("SEC_USER_AGENT must be a real non-placeholder value in the format 'App/1.0 (email@domain.com)'")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})
        self._last_request_time = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < SEC_RATE_LIMIT_DELAY:
            time.sleep(SEC_RATE_LIMIT_DELAY - elapsed)

    def get(self, url: str, timeout: int = 30, retries: int | None = None) -> requests.Response:
        retries = retries if retries is not None else SEC_MAX_RETRIES
        def _request() -> requests.Response:
            self._throttle()
            resp = self.session.get(url, timeout=timeout)
            self._last_request_time = time.time()
            resp.raise_for_status()
            return resp

        return retry_call(
            _request,
            attempts=retries,
            backoff_seconds=2.0,
            retry_on=(requests.RequestException,),
            should_retry=lambda exc: not isinstance(exc, requests.HTTPError)
            or exc.response is None
            or exc.response.status_code >= 500,
        )


def load_company_tickers_map(client: DirectEdgarClient, cache_path: Path) -> dict[str, dict]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if cache_path.exists():
        age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
        if age_hours < 24:
            return _index_by_ticker(json.loads(cache_path.read_text()))
    resp = client.get(SEC_COMPANY_TICKERS_URL)
    raw = resp.json()
    cache_path.write_text(json.dumps(raw))
    return _index_by_ticker(raw)


def _index_by_ticker(raw: dict) -> dict[str, dict]:
    result = {}
    for entry in raw.values():
        ticker = entry.get("ticker", "").upper().strip()
        if ticker:
            result[ticker] = {
                "cik_str": str(entry["cik_str"]).zfill(10),
                "ticker": ticker,
                "title": entry.get("title", ""),
            }
    return result


def resolve_cik(ticker: str, tickers_map: dict[str, dict]) -> str | None:
    ticker = ticker.upper().strip()
    for variant in [ticker, ticker.replace(".", "-"), ticker.replace("-", "."), ticker.replace("-", "").replace(".", "")]:
        entry = tickers_map.get(variant)
        if entry:
            return entry["cik_str"]
    return None


def load_universe_csv(csv_path: str, tickers_map: dict[str, dict]) -> list[dict]:
    companies = []
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        reader.fieldnames = [h.strip().lower().replace(" ", "_") for h in reader.fieldnames]
        for row in reader:
            ticker = row.get("ticker", "").strip().upper()
            cik = (row.get("cik") or "").strip()
            if not cik:
                cik = resolve_cik(ticker, tickers_map)
            if not cik:
                continue
            companies.append(
                {
                    "ticker": ticker,
                    "company_name": row.get("company_name", "").strip(),
                    "sector": row.get("sector", "").strip() or None,
                    "cik": "".join(ch for ch in str(cik) if ch.isdigit()).zfill(10),
                }
            )
    return companies


def search_form4_filings(client: DirectEdgarClient, issuer_cik: str, start_date: str | None = None, end_date: str | None = None, max_results: int = 100) -> list[dict]:
    filings = _search_form4_filings_atom(client, issuer_cik, start_date=start_date, end_date=end_date, max_results=max_results)
    if filings:
        return filings

    cik_padded = str(issuer_cik).lstrip("0").zfill(10)
    cik_clean = str(issuer_cik).lstrip("0") or "0"
    base_url = f"{SEC_EFTS_URL}/search-index"
    params = {"q": f'"{cik_padded}"', "forms": "4,4/A", "from": 0}
    if start_date and end_date:
        params["dateRange"] = "custom"
        params["startdt"] = start_date
        params["enddt"] = end_date
    fetched = 0
    while fetched < max_results:
        params["from"] = fetched
        url = base_url + "?" + "&".join(f"{k}={v}" for k, v in params.items())
        data = client.get(url).json()
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        for hit in hits:
            source = hit.get("_source", {})
            accession = source.get("adsh", "")
            if not accession:
                continue
            filing_ciks = source.get("ciks", [])
            if cik_clean not in [str(c).lstrip("0") for c in filing_ciks]:
                continue
            form_type = source.get("root_form", "4")
            filings.append(
                {
                    "accession_number": accession,
                    "filing_date": source.get("file_date", ""),
                    "form_type": form_type,
                    "is_amendment": form_type == "4/A" or "/A" in str(form_type),
                    "issuer_cik": str(issuer_cik).zfill(10),
                }
            )
        fetched += len(hits)
        total_available = data.get("hits", {}).get("total", {}).get("value", 0)
        if fetched >= total_available:
            break
    return filings


def _search_form4_filings_atom(client: DirectEdgarClient, issuer_cik: str, start_date: str | None = None, end_date: str | None = None, max_results: int = 100) -> list[dict]:
    url = (
        f"{SEC_BASE_URL}/cgi-bin/browse-edgar?action=getcompany&CIK={issuer_cik}"
        f"&type=4&owner=include&count={max_results}&output=atom"
    )
    root = ET.fromstring(client.get(url).content)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    filings = []
    for entry in root.findall("a:entry", ns):
        accession = _entry_text(entry, "accession-number")
        filing_date = _entry_text(entry, "filing-date")
        filing_href = _entry_text(entry, "filing-href")
        form_type = _entry_text(entry, "filing-type") or "4"
        if not accession:
            continue
        if start_date and filing_date and filing_date < start_date:
            continue
        if end_date and filing_date and filing_date > end_date:
            continue
        filings.append(
            {
                "accession_number": accession,
                "filing_date": filing_date,
                "form_type": form_type,
                "is_amendment": form_type == "4/A" or "/A" in str(form_type),
                "issuer_cik": str(issuer_cik).zfill(10),
                "filing_href": filing_href,
            }
        )
    return filings


def _entry_text(entry: ET.Element, tag_name: str) -> str:
    for child in entry.iter():
        tag = child.tag.split("}", 1)[1] if "}" in child.tag else child.tag
        if tag == tag_name and child.text:
            return child.text.strip()
    return ""


def resolve_filing_xml_url(client: DirectEdgarClient, accession_number: str, issuer_cik: str | None = None, filing_href: str | None = None) -> tuple[str | None, str | None]:
    accession_no_dashes = accession_number.replace("-", "")
    parts = accession_number.split("-")
    if len(parts) < 3:
        return None, None
    filer_cik = parts[0].zfill(10)
    if filing_href:
        try:
            html = client.get(filing_href, retries=1).text
            xml_matches = re.findall(r'href="([^"]+\.xml)"', html, re.IGNORECASE)
            for href in xml_matches:
                if "-index" in href.lower():
                    continue
                xml_url = href if href.startswith("http") else f"{SEC_BASE_URL}{href}"
                xml_resp = client.get(xml_url, retries=1)
                if xml_resp.status_code == 200 and b"ownershipDocument" in xml_resp.content:
                    return xml_url, filer_cik
        except Exception:
            pass
    path_cik = (str(issuer_cik).lstrip("0") or "0") if issuer_cik else (filer_cik.lstrip("0") or "0")
    index_url = f"{SEC_ARCHIVES_URL}/{path_cik}/{accession_no_dashes}/{accession_number}-index.json"
    try:
        index_data = client.get(index_url).json()
    except Exception:
        dir_url = f"{SEC_ARCHIVES_URL}/{path_cik}/{accession_no_dashes}/"
        try:
            html = client.get(dir_url, retries=1).text
            xml_matches = re.findall(r'href="[^"]*?([^/"]+\\.xml)"', html, re.IGNORECASE)
            for filename in xml_matches:
                if "-index" in filename.lower():
                    continue
                xml_url = f"{dir_url}{filename}"
                try:
                    xml_resp = client.get(xml_url, retries=1)
                    if xml_resp.status_code == 200 and b"ownershipDocument" in xml_resp.content:
                        return xml_url, filer_cik
                except Exception:
                    continue
        except Exception:
            return None, filer_cik
        return None, filer_cik
    items = index_data.get("directory", {}).get("item", [])
    xml_filename = None
    for item in items:
        name = item.get("name", "")
        if name.endswith(".xml") and name != f"{accession_number}-index.json":
            if any(kw in name.lower() for kw in ["primary", "doc4", "ownership", "form4"]):
                xml_filename = name
                break
            if xml_filename is None:
                xml_filename = name
    xml_filename = xml_filename or "primary_doc.xml"
    return f"{SEC_ARCHIVES_URL}/{path_cik}/{accession_no_dashes}/{xml_filename}", filer_cik


def download_filing_xml(client: DirectEdgarClient, xml_url: str, accession_number: str, filings_cache_dir: Path) -> str | None:
    filings_cache_dir.mkdir(parents=True, exist_ok=True)
    local_path = filings_cache_dir / f"{accession_number.replace('-', '_')}.xml"
    if local_path.exists() and local_path.stat().st_size > 0:
        return str(local_path)
    try:
        resp = client.get(xml_url)
        local_path.write_bytes(resp.content)
        return str(local_path)
    except Exception:
        return None


def ingest_universe_direct(*, csv_path: str, user_agent: str, cache_dir: str, max_filings_per_company: int | None = None, start_date: str | None = None, end_date: str | None = None) -> dict:
    cache_root = Path(cache_dir)
    client = DirectEdgarClient(user_agent)
    tickers_map = load_company_tickers_map(client, cache_root / "company_tickers.json")
    companies = load_universe_csv(csv_path, tickers_map)
    filings_dir = cache_root / "filings"
    total_new_filings = 0
    per_company = []
    for company in companies:
        cik = company["cik"]
        form4s = search_form4_filings(client, cik, start_date=start_date, end_date=end_date, max_results=max_filings_per_company or 200)
        if max_filings_per_company:
            form4s = form4s[:max_filings_per_company]
        downloaded = 0
        for filing_meta in form4s:
            accession = filing_meta["accession_number"]
            xml_url, _ = resolve_filing_xml_url(client, accession, issuer_cik=cik, filing_href=filing_meta.get("filing_href"))
            if not xml_url:
                continue
            local_path = download_filing_xml(client, xml_url, accession, filings_dir)
            if local_path:
                downloaded += 1
                total_new_filings += 1
        per_company.append({"ticker": company["ticker"], "cik": cik, "downloaded": downloaded})
    return {
        "companies_processed": len(companies),
        "total_new_filings": total_new_filings,
        "cache_dir": str(cache_root),
        "filings_dir": str(filings_dir),
        "per_company": per_company,
    }
