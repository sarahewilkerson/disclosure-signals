from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests

from signals.core.retry import retry_call


@dataclass
class HouseFiling:
    filing_id: str
    filer_name: str
    state: str | None
    district: str | None
    filing_date: datetime | None
    pdf_url: str
    filing_type: str = "PTR"


class HouseConnector:
    BASE_URL = "https://disclosures-clerk.house.gov"
    PTR_PDF_PATTERN = "/public_disc/ptr-pdfs/{year}/{filing_id}.pdf"
    ERROR_PAGE_SIZE = 1245

    def __init__(self, cache_dir: Path | None = None, request_delay: float = 0.25, timeout: int = 60):
        self.cache_dir = Path(cache_dir or ".") / "pdfs" / "house"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.request_delay = request_delay
        self.timeout = timeout
        self._last_request_time = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "DisclosureSignals/1.0 (direct congress rewrite)",
                "Accept": "text/html,application/pdf,*/*",
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

    def get_pdf_url(self, filing_id: str, year: int | None = None) -> str:
        if year is None:
            year = datetime.now().year if filing_id.startswith("822") else int(filing_id[:4])
        return urljoin(self.BASE_URL, self.PTR_PDF_PATTERN.format(year=year, filing_id=filing_id))

    def download_pdf(self, filing_id: str, year: int | None = None, force: bool = False) -> Path | None:
        cache_path = self.cache_dir / f"{filing_id}.pdf"
        if cache_path.exists() and not force:
            return cache_path
        try:
            response = self._get(self.get_pdf_url(filing_id, year))
        except requests.RequestException:
            return None
        if response.status_code == 200 and len(response.content) < self.ERROR_PAGE_SIZE + 100:
            payload = response.content.lower()
            if b"not found" in payload or b"error" in payload:
                return None
        content_type = response.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not response.content.startswith(b"%PDF"):
            return None
        cache_path.write_bytes(response.content)
        return cache_path
