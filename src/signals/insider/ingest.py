from __future__ import annotations

import csv
import hashlib
import json
import os
import random
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Callable

import requests

from signals.core.retry import retry_call


SEC_BASE_URL = "https://www.sec.gov"
SEC_DATA_URL = "https://data.sec.gov"
SEC_EFTS_URL = "https://efts.sec.gov/LATEST"
SEC_ARCHIVES_URL = f"{SEC_BASE_URL}/Archives/edgar/data"
SEC_COMPANY_TICKERS_URL = f"{SEC_BASE_URL}/files/company_tickers.json"
SEC_RATE_LIMIT_DELAY = 0.15
SEC_MAX_RETRIES = 5
INGEST_STATE_VERSION = 1


class DirectEdgarClient:
    def __init__(self, user_agent: str):
        if "example.com" in user_agent.lower() or "@" not in user_agent or "(" not in user_agent:
            raise ValueError("SEC_USER_AGENT must be a real non-placeholder value in the format 'App/1.0 (email@domain.com)'")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})
        self._last_request_time = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_request_time
        delay = SEC_RATE_LIMIT_DELAY + random.uniform(0.0, 0.05)  # jitter
        if elapsed < delay:
            time.sleep(delay - elapsed)

    def get(self, url: str, timeout: int = 30, retries: int | None = None) -> requests.Response:
        retries = retries if retries is not None else SEC_MAX_RETRIES
        def _request() -> requests.Response:
            self._throttle()
            resp = self.session.get(url, timeout=timeout)
            self._last_request_time = time.time()
            if resp.status_code == 429:
                # SEC rate limit hit — back off aggressively before retrying
                retry_after = int(resp.headers.get("Retry-After", "10"))
                jitter = random.uniform(0.5, 2.0)
                time.sleep(retry_after + jitter)
            resp.raise_for_status()
            return resp

        return retry_call(
            _request,
            attempts=retries,
            backoff_seconds=3.0,
            retry_on=(requests.RequestException,),
            should_retry=lambda exc: not isinstance(exc, requests.HTTPError)
            or exc.response is None
            or exc.response.status_code == 429
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


def _universe_fingerprint(csv_path: str, *, start_date: str | None, end_date: str | None, max_filings_per_company: int | None) -> str:
    path = Path(csv_path)
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    digest.update(f"|start={start_date or ''}|end={end_date or ''}|max={max_filings_per_company or ''}".encode("utf-8"))
    return digest.hexdigest()


def _ingest_state_path(cache_root: Path) -> Path:
    return cache_root / "insider_ingest_state.json"


def _write_state(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True))
    tmp.replace(path)


def _load_state(path: Path, *, fingerprint: str) -> dict | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return None
    if payload.get("version") != INGEST_STATE_VERSION:
        return None
    if payload.get("fingerprint") != fingerprint:
        return None
    return payload


def _fresh_state(*, fingerprint: str, csv_path: str, start_date: str | None, end_date: str | None, max_filings_per_company: int | None, companies_total: int) -> dict:
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return {
        "version": INGEST_STATE_VERSION,
        "fingerprint": fingerprint,
        "csv_path": str(csv_path),
        "start_date": start_date,
        "end_date": end_date,
        "max_filings_per_company": max_filings_per_company,
        "companies_total": companies_total,
        "companies_completed": 0,
        "total_new_filings": 0,
        "completed_companies": {},
        "started_at": now,
        "updated_at": now,
    }


def _emit_progress(callback: Callable[[dict], None] | None, payload: dict) -> None:
    if callback is not None:
        callback(payload)


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


def ingest_universe_direct(
    *,
    csv_path: str,
    user_agent: str,
    cache_dir: str,
    max_filings_per_company: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    resume: bool = True,
    progress_callback: Callable[[dict], None] | None = None,
) -> dict:
    cache_root = Path(cache_dir)
    client = DirectEdgarClient(user_agent)
    tickers_map = load_company_tickers_map(client, cache_root / "company_tickers.json")
    companies = load_universe_csv(csv_path, tickers_map)
    fingerprint = _universe_fingerprint(
        csv_path,
        start_date=start_date,
        end_date=end_date,
        max_filings_per_company=max_filings_per_company,
    )
    state_path = _ingest_state_path(cache_root)
    state = _load_state(state_path, fingerprint=fingerprint) if resume else None
    if state is None:
        state = _fresh_state(
            fingerprint=fingerprint,
            csv_path=csv_path,
            start_date=start_date,
            end_date=end_date,
            max_filings_per_company=max_filings_per_company,
            companies_total=len(companies),
        )
        _write_state(state_path, state)
    filings_dir = cache_root / "filings"
    total_new_filings = 0
    per_company = []
    completed_companies: dict[str, dict] = state.setdefault("completed_companies", {})
    resumed_count = len(completed_companies)
    _emit_progress(
        progress_callback,
        {
            "event": "start",
            "companies_total": len(companies),
            "companies_completed": resumed_count,
            "remaining_companies": max(0, len(companies) - resumed_count),
            "state_path": str(state_path),
            "resume_enabled": resume,
        },
    )
    for index, company in enumerate(companies, start=1):
        cik = company["cik"]
        completed = completed_companies.get(cik)
        if completed is not None:
            per_company.append({"ticker": company["ticker"], "cik": cik, "downloaded": int(completed.get("downloaded", 0)), "resumed": True})
            _emit_progress(
                progress_callback,
                {
                    "event": "company_skipped",
                    "index": index,
                    "companies_total": len(companies),
                    "ticker": company["ticker"],
                    "cik": cik,
                    "downloaded": int(completed.get("downloaded", 0)),
                    "reason": "already_completed",
                },
            )
            continue
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
        company_payload = {"ticker": company["ticker"], "cik": cik, "downloaded": downloaded, "resumed": False}
        per_company.append(company_payload)
        completed_companies[cik] = {
            "ticker": company["ticker"],
            "downloaded": downloaded,
            "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        state["companies_completed"] = len(completed_companies)
        state["total_new_filings"] = sum(int(item.get("downloaded", 0)) for item in completed_companies.values())
        state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _write_state(state_path, state)
        _emit_progress(
            progress_callback,
            {
                "event": "company_completed",
                "index": index,
                "companies_total": len(companies),
                "ticker": company["ticker"],
                "cik": cik,
                "downloaded": downloaded,
                "total_downloaded": state["total_new_filings"],
            },
        )
    payload = {
        "companies_processed": len(companies),
        "total_new_filings": state["total_new_filings"],
        "cache_dir": str(cache_root),
        "filings_dir": str(filings_dir),
        "per_company": per_company,
        "state_path": str(state_path),
        "companies_completed": state["companies_completed"],
        "remaining_companies": max(0, len(companies) - state["companies_completed"]),
        "resumed_companies": resumed_count,
    }
    _emit_progress(
        progress_callback,
        {
            "event": "finished",
            "companies_total": len(companies),
            "companies_completed": state["companies_completed"],
            "total_new_filings": state["total_new_filings"],
            "state_path": str(state_path),
        },
    )
    return payload
