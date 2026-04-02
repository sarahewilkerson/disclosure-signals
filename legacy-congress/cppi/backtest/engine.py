"""
Backtesting engine for CPPI signals.

Computes historical positioning scores and compares against
subsequent market returns to assess signal quality.

WARNING: Past correlation does not imply future predictive power.
Results require out-of-sample validation before any conclusions.
"""

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from cppi.backtest.data import (
    PriceCache,
    fetch_index_prices,
    get_price_returns,
)
from cppi.scoring import compute_aggregate, score_transaction

logger = logging.getLogger(__name__)


@dataclass
class BacktestConfig:
    """Configuration for backtesting."""

    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    window_days: int = 90
    forward_return_days: int = 30
    rebalance_frequency_days: int = 7  # How often to recompute signal
    benchmark_ticker: str = "SPY"
    scope: str = "all"  # 'house' | 'senate' | 'all'
    use_cache: bool = True


@dataclass
class SignalPoint:
    """A single signal observation."""

    as_of_date: str
    breadth_pct: float
    net_positioning: float
    confidence_score: float
    transaction_count: int
    forward_return: Optional[float] = None
    benchmark_return: Optional[float] = None


@dataclass
class BacktestResult:
    """Results from a backtest run."""

    config: BacktestConfig
    signal_points: list[SignalPoint]
    correlation_breadth_vs_return: Optional[float]
    correlation_net_vs_return: Optional[float]
    correlation_confidence_vs_return: Optional[float]
    signal_positive_hit_rate: Optional[float]  # % of times positive signal -> positive return
    signal_negative_hit_rate: Optional[float]
    benchmark_total_return: Optional[float]
    average_forward_return: Optional[float]
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "config": {
                "start_date": self.config.start_date,
                "end_date": self.config.end_date,
                "window_days": self.config.window_days,
                "forward_return_days": self.config.forward_return_days,
                "benchmark_ticker": self.config.benchmark_ticker,
                "scope": self.config.scope,
            },
            "metrics": {
                "correlation_breadth_vs_return": self.correlation_breadth_vs_return,
                "correlation_net_vs_return": self.correlation_net_vs_return,
                "correlation_confidence_vs_return": self.correlation_confidence_vs_return,
                "signal_positive_hit_rate": self.signal_positive_hit_rate,
                "signal_negative_hit_rate": self.signal_negative_hit_rate,
                "benchmark_total_return": self.benchmark_total_return,
                "average_forward_return": self.average_forward_return,
            },
            "signal_count": len(self.signal_points),
            "warnings": self.warnings,
        }


def _compute_correlation(xs: list[float], ys: list[float]) -> Optional[float]:
    """Compute Pearson correlation coefficient."""
    if len(xs) < 3 or len(ys) < 3 or len(xs) != len(ys):
        return None

    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    # Covariance
    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / n

    # Standard deviations
    std_x = (sum((x - mean_x) ** 2 for x in xs) / n) ** 0.5
    std_y = (sum((y - mean_y) ** 2 for y in ys) / n) ** 0.5

    if std_x == 0 or std_y == 0:
        return None

    return cov / (std_x * std_y)


def _compute_historical_positioning(
    db_path: str,
    as_of_date: str,
    window_days: int,
    scope: str,
) -> Optional[dict]:
    """
    Compute positioning as of a historical date.

    Args:
        db_path: Path to SQLite database
        as_of_date: Date to compute positioning for
        window_days: Window size in days
        scope: 'house' | 'senate' | 'all'

    Returns:
        Positioning dictionary or None if insufficient data
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Get transactions in window ending at as_of_date
        start_date = (
            datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=window_days)
        ).strftime("%Y-%m-%d")

        if scope == "house":
            chamber_filter = "AND f.chamber = 'house'"
        elif scope == "senate":
            chamber_filter = "AND f.chamber = 'senate'"
        else:
            chamber_filter = ""

        query = f"""
            SELECT
                t.resolved_ticker,
                t.transaction_type,
                t.amount_min,
                t.amount_max,
                t.execution_date,
                f.filer_name,
                f.bioguide_id
            FROM transactions t
            JOIN filings f ON t.filing_id = f.filing_id
            WHERE t.execution_date BETWEEN ? AND ?
                AND t.resolved_ticker IS NOT NULL
                AND t.resolved_ticker != ''
                {chamber_filter}
            ORDER BY t.execution_date
        """

        cursor = conn.execute(query, (start_date, as_of_date))
        transactions = [dict(row) for row in cursor.fetchall()]
        conn.close()

        if len(transactions) < 5:
            return None

        # Score each transaction
        as_of_dt = datetime.strptime(as_of_date, "%Y-%m-%d")
        scored = []
        for t in transactions:
            # Parse execution_date if it's a string
            exec_date = t.get("execution_date")
            if isinstance(exec_date, str):
                try:
                    exec_date = datetime.strptime(exec_date, "%Y-%m-%d")
                except (ValueError, TypeError):
                    exec_date = None

            scored_txn = score_transaction(
                member_id=t.get("bioguide_id") or t.get("filer_name", "unknown"),
                ticker=t.get("resolved_ticker", ""),
                transaction_type=t.get("transaction_type", ""),
                execution_date=exec_date,
                amount_min=t.get("amount_min"),
                amount_max=t.get("amount_max"),
                owner_type="self",  # Default
                resolution_confidence=1.0,  # Default for backtest
                signal_weight=1.0,  # Default for backtest
                reference_date=as_of_dt,
            )
            if scored_txn:
                scored.append(scored_txn)

        if len(scored) < 5:
            return None

        # Compute aggregate
        aggregate = compute_aggregate(scored)

        # Return as dictionary
        return {
            "breadth_pct": aggregate.breadth_pct,
            "net_positioning": aggregate.volume_net / max(
                aggregate.volume_buy + aggregate.volume_sell, 1
            ),
            "confidence_score": 1.0 - aggregate.mean_staleness if aggregate.mean_staleness < 1 else 0.5,
            "transaction_count": aggregate.transactions_included,
        }

    except Exception as e:
        logger.warning(f"Error computing historical positioning: {e}")
        return None


def run_backtest(
    db_path: str,
    config: BacktestConfig,
) -> BacktestResult:
    """
    Run a backtest of CPPI signals against market returns.

    Args:
        db_path: Path to CPPI SQLite database
        config: Backtest configuration

    Returns:
        BacktestResult with correlation metrics

    WARNING: Results are for research purposes only.
    Past performance does not indicate future results.
    """
    warnings = [
        "WARNING: Past correlation does not imply predictive power.",
        "WARNING: Requires out-of-sample validation before any conclusions.",
        "WARNING: Subject to look-ahead bias if not properly isolated.",
    ]

    # Initialize price cache
    cache = PriceCache() if config.use_cache else None

    # Fetch benchmark prices
    benchmark_prices = []
    benchmark_returns = {}
    try:
        benchmark_prices = fetch_index_prices(
            config.benchmark_ticker,
            config.start_date,
            config.end_date,
            cache,
        )
        benchmark_returns = get_price_returns(
            benchmark_prices, config.forward_return_days
        )
    except (ImportError, Exception) as e:
        warnings.append(f"Could not fetch benchmark prices: {e}")

    # Generate signal points at regular intervals
    signal_points = []

    current_date = datetime.strptime(config.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(config.end_date, "%Y-%m-%d")

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")

        # Compute positioning as of this date
        positioning = _compute_historical_positioning(
            db_path,
            date_str,
            config.window_days,
            config.scope,
        )

        if positioning:
            # Get forward return
            forward_return = benchmark_returns.get(date_str)
            benchmark_return = benchmark_returns.get(date_str)

            signal_points.append(SignalPoint(
                as_of_date=date_str,
                breadth_pct=positioning.get("breadth_pct", 0.5),
                net_positioning=positioning.get("net_positioning", 0.0),
                confidence_score=positioning.get("confidence_score", 0.5),
                transaction_count=positioning.get("transaction_count", 0),
                forward_return=forward_return,
                benchmark_return=benchmark_return,
            ))

        current_date += timedelta(days=config.rebalance_frequency_days)

    # Filter points with valid returns
    valid_points = [p for p in signal_points if p.forward_return is not None]

    if len(valid_points) < 5:
        warnings.append(
            f"Only {len(valid_points)} valid signal points. "
            "Results may not be statistically significant."
        )

    # Compute correlations
    breadths = [p.breadth_pct for p in valid_points]
    nets = [p.net_positioning for p in valid_points]
    confidences = [p.confidence_score for p in valid_points]
    returns = [p.forward_return for p in valid_points]

    corr_breadth = _compute_correlation(breadths, returns) if valid_points else None
    corr_net = _compute_correlation(nets, returns) if valid_points else None
    corr_conf = _compute_correlation(confidences, returns) if valid_points else None

    # Compute hit rates
    positive_signals = [p for p in valid_points if p.net_positioning > 0]
    negative_signals = [p for p in valid_points if p.net_positioning < 0]

    positive_hit = (
        sum(1 for p in positive_signals if (p.forward_return or 0) > 0) / len(positive_signals)
        if positive_signals else None
    )
    negative_hit = (
        sum(1 for p in negative_signals if (p.forward_return or 0) < 0) / len(negative_signals)
        if negative_signals else None
    )

    # Benchmark total return
    if benchmark_prices and len(benchmark_prices) >= 2:
        first_price = benchmark_prices[0].adj_close
        last_price = benchmark_prices[-1].adj_close
        benchmark_total = (last_price - first_price) / first_price if first_price > 0 else None
    else:
        benchmark_total = None

    # Average forward return
    avg_return = sum(returns) / len(returns) if returns else None

    return BacktestResult(
        config=config,
        signal_points=signal_points,
        correlation_breadth_vs_return=corr_breadth,
        correlation_net_vs_return=corr_net,
        correlation_confidence_vs_return=corr_conf,
        signal_positive_hit_rate=positive_hit,
        signal_negative_hit_rate=negative_hit,
        benchmark_total_return=benchmark_total,
        average_forward_return=avg_return,
        warnings=warnings,
    )


def store_historical_scores(
    db_path: str,
    signal_points: list[SignalPoint],
    scope: str,
    window_days: int,
) -> int:
    """
    Store historical scores to database for future reference.

    Args:
        db_path: Path to SQLite database
        signal_points: List of SignalPoint to store
        scope: Scope used for computation
        window_days: Window size used

    Returns:
        Number of records inserted
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_scores (
            scope           TEXT,
            window_days     INTEGER,
            as_of_date      TEXT,
            breadth_pct     REAL,
            net_positioning REAL,
            confidence_score REAL,
            PRIMARY KEY (scope, window_days, as_of_date)
        )
    """)

    inserted = 0
    for point in signal_points:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO historical_scores
                (scope, window_days, as_of_date, breadth_pct, net_positioning, confidence_score)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                scope,
                window_days,
                point.as_of_date,
                point.breadth_pct,
                point.net_positioning,
                point.confidence_score,
            ))
            inserted += 1
        except Exception as e:
            logger.warning(f"Error storing score for {point.as_of_date}: {e}")

    conn.commit()
    conn.close()

    return inserted


def format_backtest_report(result: BacktestResult) -> str:
    """Format backtest results as text report."""
    lines = [
        "=" * 77,
        "CPPI BACKTEST REPORT",
        "=" * 77,
        "",
        "CONFIGURATION",
        "-" * 77,
        f"Period:            {result.config.start_date} to {result.config.end_date}",
        f"Signal Window:     {result.config.window_days} days",
        f"Forward Returns:   {result.config.forward_return_days} days",
        f"Benchmark:         {result.config.benchmark_ticker}",
        f"Scope:             {result.config.scope}",
        "",
        "SIGNAL OBSERVATIONS",
        "-" * 77,
        f"Total Observations:    {len(result.signal_points)}",
        f"With Forward Returns:  {len([p for p in result.signal_points if p.forward_return is not None])}",
        "",
    ]

    # Correlation metrics
    lines.extend([
        "CORRELATION ANALYSIS",
        "-" * 77,
    ])

    if result.correlation_breadth_vs_return is not None:
        lines.append(f"Breadth % vs Return:      {result.correlation_breadth_vs_return:+.3f}")
    else:
        lines.append("Breadth % vs Return:      N/A")

    if result.correlation_net_vs_return is not None:
        lines.append(f"Net Positioning vs Return: {result.correlation_net_vs_return:+.3f}")
    else:
        lines.append("Net Positioning vs Return: N/A")

    if result.correlation_confidence_vs_return is not None:
        lines.append(f"Confidence vs Return:     {result.correlation_confidence_vs_return:+.3f}")
    else:
        lines.append("Confidence vs Return:     N/A")

    lines.append("")

    # Hit rates
    lines.extend([
        "HIT RATES",
        "-" * 77,
    ])

    if result.signal_positive_hit_rate is not None:
        lines.append(
            f"Positive Signal Accuracy: {result.signal_positive_hit_rate:.1%} "
            "(% of bullish signals followed by positive returns)"
        )
    else:
        lines.append("Positive Signal Accuracy: N/A")

    if result.signal_negative_hit_rate is not None:
        lines.append(
            f"Negative Signal Accuracy: {result.signal_negative_hit_rate:.1%} "
            "(% of bearish signals followed by negative returns)"
        )
    else:
        lines.append("Negative Signal Accuracy: N/A")

    lines.append("")

    # Return metrics
    lines.extend([
        "RETURN METRICS",
        "-" * 77,
    ])

    if result.benchmark_total_return is not None:
        lines.append(f"Benchmark Total Return:   {result.benchmark_total_return:+.1%}")
    else:
        lines.append("Benchmark Total Return:   N/A")

    if result.average_forward_return is not None:
        lines.append(f"Avg Forward Return:       {result.average_forward_return:+.1%}")
    else:
        lines.append("Avg Forward Return:       N/A")

    lines.append("")

    # Warnings
    if result.warnings:
        lines.extend([
            "IMPORTANT WARNINGS",
            "-" * 77,
        ])
        for warning in result.warnings:
            lines.append(f"  {warning}")
        lines.append("")

    # Interpretation guide
    lines.extend([
        "INTERPRETATION GUIDE",
        "-" * 77,
        "- Correlation near 0: No linear relationship detected",
        "- Correlation > 0.3: Weak positive relationship",
        "- Correlation > 0.5: Moderate positive relationship",
        "- Hit rate = 50%: No better than random chance",
        "- Hit rate > 60%: Potentially meaningful signal",
        "",
        "DISCLAIMER",
        "-" * 77,
        "This analysis is for research purposes only. Past correlations",
        "do not imply future predictive power. Results require rigorous",
        "out-of-sample validation before any investment conclusions.",
        "",
    ])

    return "\n".join(lines)
