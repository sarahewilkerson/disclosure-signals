"""
Quiver Quantitative API client for validation data.

API Documentation: https://api.quiverquant.com/
Requires API key from Quiver subscription.
"""

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

QUIVER_API_BASE = "https://api.quiverquant.com/beta"
DEFAULT_RATE_LIMIT = 1.0  # seconds between requests


@dataclass
class QuiverTransaction:
    """A transaction from Quiver Quantitative."""

    ticker: str
    representative: str
    transaction_type: str  # 'Purchase', 'Sale', 'Exchange'
    transaction_date: Optional[datetime]
    disclosure_date: Optional[datetime]
    amount_range: str  # e.g., "$1,001 - $15,000"
    amount_min: Optional[int]
    amount_max: Optional[int]
    house_senate: str  # 'House' or 'Senate'

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "ticker": self.ticker,
            "representative": self.representative,
            "transaction_type": self.transaction_type,
            "transaction_date": self.transaction_date.isoformat() if self.transaction_date else None,
            "disclosure_date": self.disclosure_date.isoformat() if self.disclosure_date else None,
            "amount_range": self.amount_range,
            "amount_min": self.amount_min,
            "amount_max": self.amount_max,
            "house_senate": self.house_senate,
        }


def parse_amount_range(amount_str: str) -> tuple[Optional[int], Optional[int]]:
    """
    Parse Quiver amount range string to min/max values.

    Examples:
        "$1,001 - $15,000" -> (1001, 15000)
        "$50,001 - $100,000" -> (50001, 100000)
    """
    if not amount_str:
        return None, None

    try:
        # Remove dollar signs and commas
        cleaned = amount_str.replace("$", "").replace(",", "")

        # Split on dash
        parts = cleaned.split("-")
        if len(parts) == 2:
            min_val = int(parts[0].strip())
            max_val = int(parts[1].strip())
            return min_val, max_val
        elif len(parts) == 1:
            # Single value
            val = int(parts[0].strip())
            return val, val
    except (ValueError, AttributeError):
        pass

    return None, None


class QuiverClient:
    """Client for Quiver Quantitative API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        rate_limit: float = DEFAULT_RATE_LIMIT,
    ):
        """
        Initialize the Quiver client.

        Args:
            api_key: API key. If not provided, reads from QUIVER_API_KEY env var.
            rate_limit: Minimum seconds between API requests.
        """
        self.api_key = api_key or os.getenv("QUIVER_API_KEY")
        if not self.api_key:
            logger.warning(
                "No Quiver API key provided. Set QUIVER_API_KEY environment variable "
                "or pass api_key parameter."
            )

        self.rate_limit = rate_limit
        self._last_request_time: float = 0

        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
            })

    def _rate_limit_wait(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def _get(self, endpoint: str, params: Optional[dict] = None) -> Optional[list]:
        """
        Make a rate-limited GET request to the API.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            JSON response as list, or None if request failed
        """
        if not self.api_key:
            logger.error("Cannot make API request without API key")
            return None

        self._rate_limit_wait()

        url = f"{QUIVER_API_BASE}{endpoint}"
        logger.debug(f"GET {url}")

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("Quiver API authentication failed. Check your API key.")
            elif e.response.status_code == 429:
                logger.warning("Rate limited by Quiver API")
            else:
                logger.error(f"HTTP error: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            return None

    def get_house_trading(
        self,
        ticker: Optional[str] = None,
        limit: int = 500,
    ) -> list[QuiverTransaction]:
        """
        Fetch House trading data.

        Args:
            ticker: Filter by ticker symbol
            limit: Maximum records to fetch

        Returns:
            List of QuiverTransaction objects
        """
        params = {}
        if ticker:
            params["ticker"] = ticker

        endpoint = "/historical/housetrading"
        data = self._get(endpoint, params)

        if not data:
            return []

        return self._parse_transactions(data[:limit], "House")

    def get_senate_trading(
        self,
        ticker: Optional[str] = None,
        limit: int = 500,
    ) -> list[QuiverTransaction]:
        """
        Fetch Senate trading data.

        Args:
            ticker: Filter by ticker symbol
            limit: Maximum records to fetch

        Returns:
            List of QuiverTransaction objects
        """
        params = {}
        if ticker:
            params["ticker"] = ticker

        endpoint = "/historical/senatetrading"
        data = self._get(endpoint, params)

        if not data:
            return []

        return self._parse_transactions(data[:limit], "Senate")

    def _parse_transactions(
        self,
        data: list[dict],
        chamber: str,
    ) -> list[QuiverTransaction]:
        """Parse API response into QuiverTransaction objects."""
        transactions = []

        for item in data:
            try:
                # Parse dates
                trans_date = None
                disc_date = None

                if item.get("TransactionDate"):
                    try:
                        trans_date = datetime.strptime(
                            item["TransactionDate"], "%Y-%m-%d"
                        )
                    except ValueError:
                        pass

                if item.get("DisclosureDate"):
                    try:
                        disc_date = datetime.strptime(
                            item["DisclosureDate"], "%Y-%m-%d"
                        )
                    except ValueError:
                        pass

                # Parse amount range
                amount_str = item.get("Range", "")
                amount_min, amount_max = parse_amount_range(amount_str)

                transactions.append(QuiverTransaction(
                    ticker=item.get("Ticker", ""),
                    representative=item.get("Representative", ""),
                    transaction_type=item.get("Transaction", ""),
                    transaction_date=trans_date,
                    disclosure_date=disc_date,
                    amount_range=amount_str,
                    amount_min=amount_min,
                    amount_max=amount_max,
                    house_senate=chamber,
                ))

            except Exception as e:
                logger.warning(f"Error parsing Quiver transaction: {e}")
                continue

        return transactions

    def get_all_trading(
        self,
        ticker: Optional[str] = None,
        limit: int = 500,
    ) -> list[QuiverTransaction]:
        """
        Fetch trading data from both chambers.

        Args:
            ticker: Filter by ticker symbol
            limit: Maximum records per chamber

        Returns:
            Combined list of QuiverTransaction objects
        """
        house = self.get_house_trading(ticker, limit)
        senate = self.get_senate_trading(ticker, limit)
        return house + senate


def fetch_quiver_transactions(
    api_key: Optional[str] = None,
    ticker: Optional[str] = None,
    chamber: Optional[str] = None,
    limit: int = 500,
) -> list[QuiverTransaction]:
    """
    Convenience function to fetch Quiver transactions.

    Args:
        api_key: Quiver API key
        ticker: Filter by ticker
        chamber: 'house', 'senate', or None for both
        limit: Maximum records

    Returns:
        List of QuiverTransaction objects
    """
    client = QuiverClient(api_key)

    if chamber == "house":
        return client.get_house_trading(ticker, limit)
    elif chamber == "senate":
        return client.get_senate_trading(ticker, limit)
    else:
        return client.get_all_trading(ticker, limit)
