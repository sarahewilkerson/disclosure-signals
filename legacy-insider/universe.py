"""
Universe management: load Fortune 500 CSV, resolve CIKs via EDGAR.

Expected CSV columns:
    company_name, ticker, rank, revenue, sector, cik (optional)

If CIK is missing or empty, it is resolved via SEC EDGAR's company_tickers.json
and cached locally.
"""

import csv
import json
import logging
import os
import time

import requests

import config
from db import get_connection, upsert_company

logger = logging.getLogger(__name__)


def _ensure_cache_dir():
    os.makedirs(config.CACHE_DIR, exist_ok=True)


def load_company_tickers_map() -> dict:
    """
    Download (or load cached) SEC company_tickers.json.

    Returns a dict mapping uppercase ticker → {cik_str, ticker, title}.
    """
    _ensure_cache_dir()

    if os.path.exists(config.CIK_CACHE_PATH):
        mtime = os.path.getmtime(config.CIK_CACHE_PATH)
        age_hours = (time.time() - mtime) / 3600
        if age_hours < 24:
            with open(config.CIK_CACHE_PATH, "r") as f:
                raw = json.load(f)
            return _index_by_ticker(raw)

    logger.info("Downloading SEC company_tickers.json ...")
    headers = {"User-Agent": config.SEC_USER_AGENT}
    resp = requests.get(
        config.SEC_COMPANY_TICKERS_URL,
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    raw = resp.json()

    with open(config.CIK_CACHE_PATH, "w") as f:
        json.dump(raw, f)

    return _index_by_ticker(raw)


def _index_by_ticker(raw: dict) -> dict:
    """Index the SEC tickers JSON by uppercase ticker symbol."""
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


def _normalize_ticker(ticker: str) -> str:
    """Normalize a ticker for lookup: uppercase, strip, replace dots with hyphens."""
    return ticker.upper().strip().replace(".", "-")


def resolve_cik(ticker: str, tickers_map: dict) -> str | None:
    """
    Resolve a ticker to a zero-padded CIK string.

    Tries multiple ticker format variants:
    - As-is (uppercased)
    - Dots replaced with hyphens (BRK.B → BRK-B)
    - Hyphens replaced with dots (BRK-B → BRK.B)
    - Hyphens/dots stripped (BRK-B → BRKB)

    Returns None if unresolvable.
    """
    ticker = ticker.upper().strip()

    # Try exact match first
    entry = tickers_map.get(ticker)
    if entry:
        return entry["cik_str"]

    # Try dot→hyphen (CSV might have BRK.B, SEC uses BRK-B)
    variant = ticker.replace(".", "-")
    entry = tickers_map.get(variant)
    if entry:
        return entry["cik_str"]

    # Try hyphen→dot
    variant = ticker.replace("-", ".")
    entry = tickers_map.get(variant)
    if entry:
        return entry["cik_str"]

    # Try stripped
    variant = ticker.replace("-", "").replace(".", "")
    entry = tickers_map.get(variant)
    if entry:
        return entry["cik_str"]

    return None


def load_universe_csv(csv_path: str, db_path: str = None) -> list[dict]:
    """
    Load a Fortune 500 CSV into the database.

    Resolves CIKs for any rows where cik is missing.
    Returns list of company dicts with all fields populated.

    Expected CSV columns (case-insensitive headers):
        company_name, ticker, rank, revenue, sector, cik
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Universe CSV not found: {csv_path}")

    tickers_map = load_company_tickers_map()

    companies = []
    unresolved = []

    with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        # Normalize header names
        reader.fieldnames = [h.strip().lower().replace(" ", "_") for h in reader.fieldnames]

        for row in reader:
            ticker = row.get("ticker", "").strip().upper()
            cik = row.get("cik", "").strip()
            company_name = row.get("company_name", "").strip()
            rank = row.get("rank", "")
            revenue = row.get("revenue", "")
            sector = row.get("sector", "").strip()

            # Resolve CIK if missing
            if not cik:
                cik = resolve_cik(ticker, tickers_map)
                if not cik:
                    unresolved.append({"ticker": ticker, "company_name": company_name})
                    logger.warning(f"Could not resolve CIK for {ticker} ({company_name})")
                    continue

            # Validate CIK is numeric
            cik_digits = "".join(c for c in str(cik) if c.isdigit())
            if not cik_digits:
                unresolved.append({"ticker": ticker, "company_name": company_name})
                logger.warning(f"Invalid CIK '{cik}' for {ticker} ({company_name})")
                continue
            cik = cik_digits.zfill(10)

            # Parse rank and revenue safely
            fortune_rank = None
            if rank:
                try:
                    fortune_rank = int(str(rank).replace(",", "").strip())
                except ValueError:
                    logger.warning(f"Non-numeric rank '{rank}' for {ticker}")

            revenue_val = None
            if revenue:
                try:
                    revenue_val = float(str(revenue).replace(",", "").replace("$", "").strip())
                except ValueError:
                    logger.warning(f"Non-numeric revenue '{revenue}' for {ticker}")

            company = {
                "cik": cik,
                "ticker": ticker,
                "company_name": company_name,
                "fortune_rank": fortune_rank,
                "revenue": revenue_val,
                "sector": sector or None,
                "resolved_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            companies.append(company)

    # Persist to database
    with get_connection(db_path) as conn:
        for company in companies:
            upsert_company(conn, company)

    if unresolved:
        logger.warning(
            f"{len(unresolved)} companies could not be resolved: "
            f"{[u['ticker'] for u in unresolved]}"
        )

    logger.info(f"Loaded {len(companies)} companies into universe.")
    return companies
