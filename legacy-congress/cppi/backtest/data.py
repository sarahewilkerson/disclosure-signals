"""
Historical price data fetcher for backtesting.

Uses yfinance for price data. This is an optional dependency -
backtesting will fail gracefully if yfinance is not installed.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Try to import yfinance (optional dependency)
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False
    yf = None


@dataclass
class PricePoint:
    """A single price point."""

    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    adj_close: float

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "date": self.date.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "adj_close": self.adj_close,
        }


class PriceCache:
    """
    File-based cache for historical price data.

    Stores prices as JSON files to avoid repeated API calls.
    """

    def __init__(self, cache_dir: Optional[str] = None):
        """
        Initialize the price cache.

        Args:
            cache_dir: Directory for cache files. Defaults to ~/.cppi/price_cache
        """
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path.home() / ".cppi" / "price_cache"

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._staleness_days = 1  # Refresh if cache is older than this

    def _cache_path(self, ticker: str, start: str, end: str) -> Path:
        """Get cache file path for a ticker and date range."""
        # Use a simplified key based on ticker and year-month
        return self.cache_dir / f"{ticker}_{start}_{end}.json"

    def get(
        self, ticker: str, start: str, end: str
    ) -> Optional[list[PricePoint]]:
        """
        Get cached prices for a ticker.

        Args:
            ticker: Stock ticker
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)

        Returns:
            List of PricePoint if cached and fresh, None otherwise
        """
        cache_path = self._cache_path(ticker, start, end)

        if not cache_path.exists():
            return None

        # Check staleness
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        if datetime.now() - mtime > timedelta(days=self._staleness_days):
            logger.debug(f"Cache stale for {ticker}")
            return None

        try:
            with open(cache_path) as f:
                data = json.load(f)

            return [
                PricePoint(
                    date=datetime.fromisoformat(p["date"]),
                    open=p["open"],
                    high=p["high"],
                    low=p["low"],
                    close=p["close"],
                    volume=p["volume"],
                    adj_close=p["adj_close"],
                )
                for p in data
            ]
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"Cache read error for {ticker}: {e}")
            return None

    def set(
        self, ticker: str, start: str, end: str, prices: list[PricePoint]
    ) -> None:
        """
        Cache prices for a ticker.

        Args:
            ticker: Stock ticker
            start: Start date (YYYY-MM-DD)
            end: End date (YYYY-MM-DD)
            prices: List of PricePoint to cache
        """
        cache_path = self._cache_path(ticker, start, end)

        data = [p.to_dict() for p in prices]

        with open(cache_path, "w") as f:
            json.dump(data, f)


def fetch_historical_prices(
    ticker: str,
    start_date: str,
    end_date: str,
    cache: Optional[PriceCache] = None,
) -> list[PricePoint]:
    """
    Fetch historical prices for a ticker.

    Args:
        ticker: Stock ticker symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        cache: Optional PriceCache for caching

    Returns:
        List of PricePoint objects

    Raises:
        ImportError: If yfinance is not installed
        ValueError: If no data could be fetched
    """
    if not HAS_YFINANCE:
        raise ImportError(
            "yfinance is required for backtesting. "
            "Install with: pip install yfinance"
        )

    # Check cache first
    if cache:
        cached = cache.get(ticker, start_date, end_date)
        if cached:
            logger.debug(f"Using cached prices for {ticker}")
            return cached

    # Fetch from yfinance
    logger.info(f"Fetching prices for {ticker} from {start_date} to {end_date}")

    try:
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            progress=False,
            auto_adjust=False,
        )

        if df.empty:
            logger.warning(f"No price data for {ticker}")
            return []

        prices = []

        # Handle both single-ticker and multi-ticker column formats
        # yfinance can return either flat columns ["Open", "High", ...]
        # or multi-index columns [("Open", "SPY"), ("High", "SPY"), ...]
        columns = df.columns.tolist()
        is_multi_index = isinstance(columns[0], tuple) if columns else False

        def get_col_value(row_data, col_name, fallback=0.0):
            """Safely extract column value regardless of column format."""
            try:
                if is_multi_index:
                    # Try multi-index format
                    return row_data[(col_name, ticker)]
                else:
                    # Try flat format
                    return row_data[col_name]
            except KeyError:
                return fallback

        for date, row in df.iterrows():
            try:
                prices.append(PricePoint(
                    date=date.to_pydatetime(),
                    open=float(get_col_value(row, "Open", 0.0)),
                    high=float(get_col_value(row, "High", 0.0)),
                    low=float(get_col_value(row, "Low", 0.0)),
                    close=float(get_col_value(row, "Close", 0.0)),
                    volume=int(get_col_value(row, "Volume", 0)),
                    adj_close=float(get_col_value(row, "Adj Close", get_col_value(row, "Close", 0.0))),
                ))
            except (ValueError, TypeError) as e:
                logger.warning(f"Error parsing price for {date}: {e}")
                continue

        # Cache the results
        if cache and prices:
            cache.set(ticker, start_date, end_date, prices)

        return prices

    except Exception as e:
        logger.error(f"Error fetching prices for {ticker}: {e}")
        return []


def get_price_returns(
    prices: list[PricePoint],
    forward_days: int = 30,
) -> dict[str, float]:
    """
    Compute forward returns from price data.

    Args:
        prices: List of PricePoint objects (sorted by date)
        forward_days: Number of days to compute forward returns

    Returns:
        Dictionary mapping date strings to forward returns
    """
    if len(prices) < 2:
        return {}

    # Sort by date
    sorted_prices = sorted(prices, key=lambda p: p.date)

    returns = {}
    for i, price in enumerate(sorted_prices[:-1]):
        date = price.date.date()

        # Find price approximately forward_days later
        target_date = date + timedelta(days=forward_days)
        future_idx = None

        # Look for closest available date
        for j in range(i + 1, len(sorted_prices)):
            future_date = sorted_prices[j].date.date()
            if future_date >= target_date:
                future_idx = j
                break

        if future_idx is not None:
            future_price = sorted_prices[future_idx].adj_close
            if price.adj_close > 0:
                ret = (future_price - price.adj_close) / price.adj_close
                returns[date.isoformat()] = ret

    return returns


def fetch_index_prices(
    index_symbol: str = "SPY",
    start_date: str = None,
    end_date: str = None,
    cache: Optional[PriceCache] = None,
) -> list[PricePoint]:
    """
    Fetch historical prices for a market index.

    Args:
        index_symbol: Index ticker (default SPY for S&P 500)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        cache: Optional PriceCache

    Returns:
        List of PricePoint objects
    """
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    return fetch_historical_prices(index_symbol, start_date, end_date, cache)
