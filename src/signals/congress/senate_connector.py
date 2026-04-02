from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from signals.core.retry import retry_call


logger = logging.getLogger(__name__)


@dataclass
class SenateFiling:
    filing_id: str
    filer_name: str
    state: str | None
    filing_date: datetime | None
    report_url: str
    filing_type: str = "PTR"
    is_paper: bool = False


@dataclass
class SenateTransaction:
    transaction_date: datetime | None
    owner: str
    ticker: str | None
    asset_name: str
    asset_type: str | None
    transaction_type: str
    amount_range: str
    comment: str | None


class SenateConnector:
    BASE_URL = "https://efdsearch.senate.gov"
    HOME_URL = "https://efdsearch.senate.gov/search/home/"
    SEARCH_URL = "https://efdsearch.senate.gov/search/"
    PTR_URL_PATTERN = "https://efdsearch.senate.gov/search/view/ptr/{uuid}/"

    def __init__(self, cache_dir: Path | None = None, request_delay: float = 0.25, timeout: int = 60):
        self.cache_dir = Path(cache_dir or ".") / "pdfs" / "senate"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.request_delay = request_delay
        self.timeout = timeout
        self._last_request_time = 0.0
        self._session_established = False
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "DisclosureSignals/1.0 (direct congress rewrite)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            }
        )

    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < self.request_delay:
            time.sleep(self.request_delay - elapsed)
        self._last_request_time = time.time()

    def _get(self, url: str, **kwargs) -> requests.Response:
        def _request() -> requests.Response:
            self._rate_limit()
            response = self.session.get(url, timeout=self.timeout, **kwargs)
            response.raise_for_status()
            return response

        return retry_call(
            _request,
            attempts=3,
            backoff_seconds=1.0,
            retry_on=(requests.RequestException,),
            should_retry=lambda exc: not isinstance(exc, requests.HTTPError)
            or exc.response is None
            or exc.response.status_code >= 500,
        )

    def _post(self, url: str, data: dict, **kwargs) -> requests.Response:
        def _request() -> requests.Response:
            self._rate_limit()
            response = self.session.post(url, data=data, timeout=self.timeout, **kwargs)
            response.raise_for_status()
            return response

        return retry_call(
            _request,
            attempts=3,
            backoff_seconds=1.0,
            retry_on=(requests.RequestException,),
            should_retry=lambda exc: not isinstance(exc, requests.HTTPError)
            or exc.response is None
            or exc.response.status_code >= 500,
        )

    def establish_session(self) -> bool:
        if self._session_established:
            return True
        try:
            response = self._get(self.HOME_URL)
            soup = BeautifulSoup(response.text, "lxml")
            csrf_input = soup.find("input", {"name": "csrfmiddlewaretoken"})
            if not csrf_input:
                return False
            csrf_token = csrf_input.get("value", "")
            headers = {"Content-Type": "application/x-www-form-urlencoded", "Referer": self.HOME_URL}
            response = self._post(
                self.HOME_URL,
                data={"csrfmiddlewaretoken": csrf_token, "prohibition_agreement": "1"},
                headers=headers,
            )
            if response.status_code in (200, 302) and ("/search/" in response.url or "search_agreement" in str(self.session.cookies)):
                self._session_established = True
                return True
            test_response = self._get(self.SEARCH_URL)
            if test_response.status_code == 200 and "search" in test_response.url.lower():
                self._session_established = True
                return True
            return False
        except requests.RequestException:
            return False

    def ensure_session(self) -> bool:
        return self._session_established or self.establish_session()

    def get_ptr_url(self, uuid: str) -> str:
        return self.PTR_URL_PATTERN.format(uuid=uuid.lower())

    def download_ptr(self, uuid: str, force: bool = False) -> Path | None:
        cache_path = self.cache_dir / f"ptr_{uuid[:8]}.html"
        if cache_path.exists() and not force:
            return cache_path
        if not self.ensure_session():
            return None
        try:
            response = self._get(self.get_ptr_url(uuid))
            if "home" in response.url:
                self._session_established = False
                if not self.establish_session():
                    return None
                response = self._get(self.get_ptr_url(uuid))
            if len(response.text) < 1000:
                return None
            cache_path.write_text(response.text, encoding="utf-8")
            return cache_path
        except requests.RequestException:
            return None

    def parse_ptr_transactions(self, html_path: Path) -> list[SenateTransaction]:
        transactions: list[SenateTransaction] = []
        try:
            soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "lxml")
            table = soup.find("table", {"class": re.compile(r"table.*striped", re.I)})
            if not table:
                return transactions
            rows = table.find_all("tr")
            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) < 7:
                    continue
                first_cell_text = cells[0].get_text(strip=True)
                offset = 1 if first_cell_text.isdigit() else 0
                if offset == 1 and len(cells) < 8:
                    continue
                date_text = cells[offset].get_text(strip=True)
                transaction_date = None
                if date_text:
                    try:
                        transaction_date = datetime.strptime(date_text, "%m/%d/%Y")
                    except ValueError:
                        transaction_date = None
                owner = cells[offset + 1].get_text(strip=True) or "Self"
                ticker_cell = cells[offset + 2]
                ticker_link = ticker_cell.find("a")
                ticker = ticker_link.get_text(strip=True) if ticker_link else None
                if not ticker:
                    ticker_text = ticker_cell.get_text(strip=True)
                    ticker = ticker_text if ticker_text and ticker_text != "--" else None
                transactions.append(
                    SenateTransaction(
                        transaction_date=transaction_date,
                        owner=owner,
                        ticker=ticker,
                        asset_name=cells[offset + 3].get_text(strip=True),
                        asset_type=cells[offset + 4].get_text(strip=True) if len(cells) > offset + 4 else None,
                        transaction_type=cells[offset + 5].get_text(strip=True) if len(cells) > offset + 5 else "",
                        amount_range=cells[offset + 6].get_text(strip=True) if len(cells) > offset + 6 else "",
                        comment=cells[offset + 7].get_text(strip=True) if len(cells) > offset + 7 else None,
                    )
                )
        except Exception as exc:
            logger.debug("Failed to parse senate html %s: %s", html_path, exc)
        return transactions
