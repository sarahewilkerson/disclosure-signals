"""
Cross-reference analysis between congressional trades (CPPI) and corporate insider trades.

Compares signals from congressional trading data with SEC Form 4 insider trading data
to identify convergent signals (both bullish/bearish) and divergent signals (conflict).
"""

import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# Default path for insider signal database
DEFAULT_INSIDER_DB = "/tmp/insidertradingsignal/insider_signal.db"

# Ticker aliases for normalization (map variants to canonical ticker)
TICKER_ALIASES = {
    "GOOG": "GOOGL",
    "BRK.A": "BRK-B",
    "BRK/A": "BRK-B",
    "BF.B": "BF-B",
    "BF/B": "BF-B",
}


def normalize_ticker(ticker: str) -> str:
    """Normalize ticker to canonical form."""
    if not ticker:
        return ""
    ticker = ticker.upper().strip()
    return TICKER_ALIASES.get(ticker, ticker)


@dataclass
class TickerSignal:
    """Signal data for a single ticker from one source."""

    ticker: str
    net_value: float  # Net dollar value (positive=buying, negative=selling)
    transaction_count: int
    signal: str  # "BULLISH", "BEARISH", or "NEUTRAL"

    @classmethod
    def from_transactions(
        cls, ticker: str, buys: float, sells: float, count: int
    ) -> "TickerSignal":
        """Create signal from buy/sell values."""
        net = buys - sells
        if net > 0:
            signal = "BULLISH"
        elif net < 0:
            signal = "BEARISH"
        else:
            signal = "NEUTRAL"
        return cls(ticker=ticker, net_value=net, transaction_count=count, signal=signal)


@dataclass
class CrossRefMatch:
    """A ticker that appears in both congressional and insider data."""

    ticker: str
    congress_signal: TickerSignal
    insider_signal: TickerSignal
    is_convergent: bool
    match_type: str  # "BOTH_BULLISH", "BOTH_BEARISH", "DIVERGENT", "MIXED"

    def __post_init__(self):
        # Determine match type
        cs = self.congress_signal.signal
        ins = self.insider_signal.signal
        if cs == "BULLISH" and ins == "BULLISH":
            self.match_type = "BOTH_BULLISH"
            self.is_convergent = True
        elif cs == "BEARISH" and ins == "BEARISH":
            self.match_type = "BOTH_BEARISH"
            self.is_convergent = True
        elif cs == "NEUTRAL" or ins == "NEUTRAL":
            self.match_type = "MIXED"
            self.is_convergent = False
        else:
            self.match_type = "DIVERGENT"
            self.is_convergent = False


@dataclass
class CrossRefReport:
    """Full cross-reference analysis report."""

    window_days: int
    cppi_ticker_count: int
    insider_ticker_count: int
    overlapping_ticker_count: int

    convergent_bullish: list[CrossRefMatch] = field(default_factory=list)
    convergent_bearish: list[CrossRefMatch] = field(default_factory=list)
    divergent: list[CrossRefMatch] = field(default_factory=list)
    congress_only: list[TickerSignal] = field(default_factory=list)

    warnings: list[str] = field(default_factory=list)

    @property
    def agreement_rate(self) -> float:
        """Calculate agreement rate on overlapping tickers."""
        total_overlap = len(self.convergent_bullish) + len(self.convergent_bearish) + len(self.divergent)
        if total_overlap == 0:
            return 0.0
        convergent = len(self.convergent_bullish) + len(self.convergent_bearish)
        return convergent / total_overlap

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "window_days": self.window_days,
            "cppi_ticker_count": self.cppi_ticker_count,
            "insider_ticker_count": self.insider_ticker_count,
            "overlapping_ticker_count": self.overlapping_ticker_count,
            "convergent_bullish_count": len(self.convergent_bullish),
            "convergent_bearish_count": len(self.convergent_bearish),
            "divergent_count": len(self.divergent),
            "congress_only_count": len(self.congress_only),
            "agreement_rate": self.agreement_rate,
            "warnings": self.warnings,
        }


def get_insider_signals(
    db_path: str,
    tickers: list[str],
    window_days: int,
    reference_date: Optional[datetime] = None,
) -> dict[str, TickerSignal]:
    """
    Query insidertradingsignal DB for signals on given tickers.

    Args:
        db_path: Path to insider_signal.db
        tickers: List of tickers to query
        window_days: Number of days to look back
        reference_date: End date for window (defaults to today)

    Returns:
        Dict mapping ticker to TickerSignal
    """
    if not os.path.exists(db_path):
        logger.warning(f"Insider DB not found at {db_path}")
        return {}

    if not tickers:
        return {}

    reference_date = reference_date or datetime.now()
    start_date = reference_date - timedelta(days=window_days)

    signals = {}

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Normalize tickers for query
        normalized = [normalize_ticker(t) for t in tickers]
        placeholders = ",".join("?" * len(normalized))

        # Query transactions joined with companies
        # transaction_code: P=Purchase, S=Sale
        query = f"""
            SELECT
                c.ticker,
                t.transaction_code,
                SUM(COALESCE(t.total_value, t.shares * t.price_per_share, 0)) as total_value,
                COUNT(*) as txn_count
            FROM transactions t
            JOIN companies c ON t.cik_issuer = c.cik
            WHERE c.ticker IN ({placeholders})
              AND t.transaction_date >= ?
              AND t.transaction_date <= ?
              AND t.transaction_code IN ('P', 'S')
            GROUP BY c.ticker, t.transaction_code
        """

        cursor.execute(
            query,
            normalized + [start_date.strftime("%Y-%m-%d"), reference_date.strftime("%Y-%m-%d")],
        )

        # Aggregate by ticker
        ticker_data: dict[str, dict] = {}
        for row in cursor.fetchall():
            ticker, code, value, count = row
            if ticker not in ticker_data:
                ticker_data[ticker] = {"buys": 0.0, "sells": 0.0, "count": 0}
            if code == "P":
                ticker_data[ticker]["buys"] += value or 0
            else:
                ticker_data[ticker]["sells"] += value or 0
            ticker_data[ticker]["count"] += count

        conn.close()

        # Convert to signals
        for ticker, data in ticker_data.items():
            signals[ticker] = TickerSignal.from_transactions(
                ticker=ticker,
                buys=data["buys"],
                sells=data["sells"],
                count=data["count"],
            )

    except sqlite3.Error as e:
        logger.error(f"Error querying insider DB: {e}")
        return {}

    return signals


def get_cppi_signals(
    db_path: str,
    window_days: int,
    reference_date: Optional[datetime] = None,
    limit_top_n: int = 100,
) -> dict[str, TickerSignal]:
    """
    Get congressional trading signals from CPPI database.

    Args:
        db_path: Path to cppi.db
        window_days: Number of days to look back
        reference_date: End date for window (defaults to today)
        limit_top_n: Limit to top N tickers by volume (for performance)

    Returns:
        Dict mapping ticker to TickerSignal
    """
    if not os.path.exists(db_path):
        logger.warning(f"CPPI DB not found at {db_path}")
        return {}

    reference_date = reference_date or datetime.now()
    start_date = reference_date - timedelta(days=window_days)

    signals = {}

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query CPPI transactions
        # transaction_type: 'purchase', 'sale', 'exchange'
        # Note: amount_midpoint is often NULL, so calculate from min/max
        query = """
            SELECT
                resolved_ticker,
                transaction_type,
                SUM(COALESCE(amount_midpoint, (amount_min + amount_max) / 2.0, 0)) as total_value,
                COUNT(*) as txn_count
            FROM transactions
            WHERE resolved_ticker IS NOT NULL
              AND resolved_ticker != ''
              AND execution_date >= ?
              AND execution_date <= ?
              AND include_in_signal = 1
            GROUP BY resolved_ticker, transaction_type
            ORDER BY total_value DESC
        """

        cursor.execute(
            query,
            [start_date.strftime("%Y-%m-%d"), reference_date.strftime("%Y-%m-%d")],
        )

        # Aggregate by ticker
        ticker_data: dict[str, dict] = {}
        for row in cursor.fetchall():
            ticker, txn_type, value, count = row
            ticker = normalize_ticker(ticker)
            if not ticker:
                continue
            if ticker not in ticker_data:
                ticker_data[ticker] = {"buys": 0.0, "sells": 0.0, "count": 0}
            if txn_type and txn_type.lower() in ("purchase", "buy"):
                ticker_data[ticker]["buys"] += value or 0
            elif txn_type and txn_type.lower() in ("sale", "sell"):
                ticker_data[ticker]["sells"] += value or 0
            ticker_data[ticker]["count"] += count

        conn.close()

        # Convert to signals, limiting to top N by total volume
        sorted_tickers = sorted(
            ticker_data.items(),
            key=lambda x: abs(x[1]["buys"]) + abs(x[1]["sells"]),
            reverse=True,
        )[:limit_top_n]

        for ticker, data in sorted_tickers:
            signals[ticker] = TickerSignal.from_transactions(
                ticker=ticker,
                buys=data["buys"],
                sells=data["sells"],
                count=data["count"],
            )

    except sqlite3.Error as e:
        logger.error(f"Error querying CPPI DB: {e}")
        return {}

    return signals


def check_data_freshness(
    db_path: str,
    table: str,
    date_column: str,
    window_days: int,
) -> tuple[bool, Optional[str]]:
    """
    Check if data in database is fresh enough for the analysis window.

    Returns:
        Tuple of (is_fresh, warning_message)
    """
    if not os.path.exists(db_path):
        return False, f"Database not found: {db_path}"

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX({date_column}) FROM {table}")
        row = cursor.fetchone()
        conn.close()

        if not row or not row[0]:
            return False, f"No data in {table}"

        max_date = datetime.strptime(row[0][:10], "%Y-%m-%d")
        days_old = (datetime.now() - max_date).days

        if days_old > window_days:
            return False, f"Data is {days_old} days old (older than {window_days} day window)"

        return True, None

    except (sqlite3.Error, ValueError) as e:
        return False, f"Error checking freshness: {e}"


def compute_crossref_signals(
    cppi_db_path: str,
    insider_db_path: str,
    window_days: int,
    reference_date: Optional[datetime] = None,
) -> CrossRefReport:
    """
    Compare congressional vs insider signals.

    Args:
        cppi_db_path: Path to CPPI database
        insider_db_path: Path to insider trading database
        window_days: Analysis window in days
        reference_date: End date for window (defaults to today)

    Returns:
        CrossRefReport with convergent/divergent signals
    """
    warnings = []

    # Check data freshness
    cppi_fresh, cppi_warn = check_data_freshness(
        cppi_db_path, "transactions", "execution_date", window_days
    )
    if cppi_warn:
        warnings.append(f"CPPI: {cppi_warn}")

    insider_fresh, insider_warn = check_data_freshness(
        insider_db_path, "transactions", "transaction_date", window_days
    )
    if insider_warn:
        warnings.append(f"Insider: {insider_warn}")

    # Get CPPI signals
    cppi_signals = get_cppi_signals(cppi_db_path, window_days, reference_date)
    if not cppi_signals:
        return CrossRefReport(
            window_days=window_days,
            cppi_ticker_count=0,
            insider_ticker_count=0,
            overlapping_ticker_count=0,
            warnings=warnings + ["No CPPI transactions in window"],
        )

    # Get insider signals for the same tickers
    cppi_tickers = list(cppi_signals.keys())
    insider_signals = get_insider_signals(
        insider_db_path, cppi_tickers, window_days, reference_date
    )

    # Find overlapping tickers
    overlapping = set(cppi_signals.keys()) & set(insider_signals.keys())

    # Classify matches
    convergent_bullish = []
    convergent_bearish = []
    divergent = []

    for ticker in overlapping:
        match = CrossRefMatch(
            ticker=ticker,
            congress_signal=cppi_signals[ticker],
            insider_signal=insider_signals[ticker],
            is_convergent=False,  # Will be set in __post_init__
            match_type="",  # Will be set in __post_init__
        )

        if match.match_type == "BOTH_BULLISH":
            convergent_bullish.append(match)
        elif match.match_type == "BOTH_BEARISH":
            convergent_bearish.append(match)
        elif match.match_type == "DIVERGENT":
            divergent.append(match)
        # Skip MIXED (one side is neutral)

    # Congress-only tickers (no insider data)
    congress_only = [
        cppi_signals[t] for t in cppi_signals if t not in insider_signals
    ]

    # Sort by conviction (absolute net value)
    convergent_bullish.sort(key=lambda m: m.congress_signal.net_value, reverse=True)
    convergent_bearish.sort(key=lambda m: m.congress_signal.net_value)
    divergent.sort(key=lambda m: abs(m.congress_signal.net_value), reverse=True)
    congress_only.sort(key=lambda s: abs(s.net_value), reverse=True)

    return CrossRefReport(
        window_days=window_days,
        cppi_ticker_count=len(cppi_signals),
        insider_ticker_count=len(insider_signals),
        overlapping_ticker_count=len(overlapping),
        convergent_bullish=convergent_bullish,
        convergent_bearish=convergent_bearish,
        divergent=divergent,
        congress_only=congress_only,
        warnings=warnings,
    )


def format_crossref_report(report: CrossRefReport, max_items: int = 10) -> str:
    """
    Format cross-reference report for display.

    Args:
        report: CrossRefReport to format
        max_items: Maximum items to show per section

    Returns:
        Formatted text report
    """
    lines = [
        "",
        "=" * 77,
        "CONGRESSIONAL / INSIDER CROSS-REFERENCE",
        "=" * 77,
        "",
        f"Window: {report.window_days} days",
        f"Congressional tickers: {report.cppi_ticker_count}",
        f"Insider tickers: {report.insider_ticker_count}",
        f"Overlapping: {report.overlapping_ticker_count}",
        "",
    ]

    # Warnings
    if report.warnings:
        lines.append("WARNINGS:")
        for warn in report.warnings:
            lines.append(f"  - {warn}")
        lines.append("")

    # No data case
    if report.overlapping_ticker_count == 0:
        lines.extend([
            "No overlapping tickers found.",
            "",
            "=" * 77,
        ])
        return "\n".join(lines)

    # Convergent Bullish
    lines.append("-" * 77)
    lines.append(f"CONVERGENT (BOTH BULLISH): {len(report.convergent_bullish)} tickers")
    lines.append("-" * 77)
    for match in report.convergent_bullish[:max_items]:
        cs = match.congress_signal
        ins = match.insider_signal
        lines.append(
            f"  {match.ticker:<6} Congress: ${cs.net_value:>+12,.0f} | "
            f"Insiders: {ins.transaction_count:>3} txns, ${ins.net_value:>+12,.0f}"
        )
    if len(report.convergent_bullish) > max_items:
        lines.append(f"  ... and {len(report.convergent_bullish) - max_items} more")
    lines.append("")

    # Convergent Bearish
    lines.append("-" * 77)
    lines.append(f"CONVERGENT (BOTH BEARISH): {len(report.convergent_bearish)} tickers")
    lines.append("-" * 77)
    for match in report.convergent_bearish[:max_items]:
        cs = match.congress_signal
        ins = match.insider_signal
        lines.append(
            f"  {match.ticker:<6} Congress: ${cs.net_value:>+12,.0f} | "
            f"Insiders: {ins.transaction_count:>3} txns, ${ins.net_value:>+12,.0f}"
        )
    if len(report.convergent_bearish) > max_items:
        lines.append(f"  ... and {len(report.convergent_bearish) - max_items} more")
    lines.append("")

    # Divergent
    lines.append("-" * 77)
    lines.append(f"DIVERGENT: {len(report.divergent)} tickers")
    lines.append("-" * 77)
    for match in report.divergent[:max_items]:
        cs = match.congress_signal
        ins = match.insider_signal
        c_dir = "BUYING" if cs.signal == "BULLISH" else "SELLING"
        i_dir = "buying" if ins.signal == "BULLISH" else "selling"
        lines.append(
            f"  {match.ticker:<6} Congress {c_dir} (${cs.net_value:>+12,.0f}) | "
            f"Insiders {i_dir} (${ins.net_value:>+12,.0f})"
        )
    if len(report.divergent) > max_items:
        lines.append(f"  ... and {len(report.divergent) - max_items} more")
    lines.append("")

    # Summary
    lines.append("=" * 77)
    lines.append("SUMMARY")
    lines.append("=" * 77)
    lines.append(f"Agreement rate: {report.agreement_rate:.1%} (on overlapping tickers)")
    lines.append(
        f"Congress-only tickers (no insider data): {len(report.congress_only)}"
    )
    lines.append("")

    return "\n".join(lines)


def run_crossref_analysis(
    cppi_db_path: str,
    insider_db_path: Optional[str] = None,
    window_days: int = 90,
    reference_date: Optional[datetime] = None,
) -> CrossRefReport:
    """
    Main entry point for cross-reference analysis.

    Args:
        cppi_db_path: Path to CPPI database
        insider_db_path: Path to insider DB (defaults to DEFAULT_INSIDER_DB)
        window_days: Analysis window in days
        reference_date: End date for window

    Returns:
        CrossRefReport with analysis results
    """
    insider_db = insider_db_path or os.environ.get("INSIDER_SIGNAL_DB", DEFAULT_INSIDER_DB)

    logger.info(f"Running cross-reference analysis (window={window_days} days)")
    logger.info(f"  CPPI DB: {cppi_db_path}")
    logger.info(f"  Insider DB: {insider_db}")

    return compute_crossref_signals(
        cppi_db_path=cppi_db_path,
        insider_db_path=insider_db,
        window_days=window_days,
        reference_date=reference_date,
    )
