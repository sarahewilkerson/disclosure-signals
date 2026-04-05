"""Market regime computation for signal weighting.

Computes SPY trailing return to classify the market regime (bull/bear/neutral)
and derive a regime weight for buy signals. This is an EXPERIMENTAL feature —
validation data (n=8 bear, n=17 bull) is too small for statistical significance.

Conservative weights: bear +10%, bull -5%, neutral no change.
Default weight is 1.0 (no effect) when regime is unknown or yfinance fails.

Usage:
    from signals.core.regime import compute_regime
    regime = compute_regime(reference_date)
    # regime.regime_weight_buy is the multiplier for buy signals
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

try:
    import yfinance as yf

    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

BEAR_THRESHOLD = -0.02
BULL_THRESHOLD = 0.08
BEAR_WEIGHT = 1.1
BULL_WEIGHT = 0.95
NEUTRAL_WEIGHT = 1.0
DEFAULT_LOOKBACK_DAYS = 60


@dataclass
class RegimeContext:
    regime: str  # "bull" | "bear" | "neutral" | "unknown"
    spy_trailing_return: float | None
    regime_weight_buy: float
    lookback_days: int
    computed_at: str

    def to_dict(self) -> dict:
        return asdict(self)


def compute_regime(
    reference_date: datetime,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> RegimeContext:
    """Compute market regime based on SPY trailing return.

    Lookback ends at reference_date - 1 day to avoid forward-looking bias.

    Returns RegimeContext with regime classification and buy weight multiplier.
    Falls back to regime="unknown", weight=1.0 if yfinance unavailable or data insufficient.
    """
    if not HAS_YFINANCE:
        return _unknown_regime(lookback_days)

    lookback_end = reference_date - timedelta(days=1)
    lookback_start = lookback_end - timedelta(days=lookback_days)

    try:
        data = yf.download(
            "SPY",
            start=lookback_start.strftime("%Y-%m-%d"),
            end=(lookback_end + timedelta(days=1)).strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )
        if data.empty or len(data) < 2:
            return _unknown_regime(lookback_days)

        if hasattr(data.columns, "levels"):
            data.columns = data.columns.get_level_values(0)

        close = data["Close"]
        trailing_return = float((close.iloc[-1] - close.iloc[0]) / close.iloc[0])
    except Exception:
        return _unknown_regime(lookback_days)

    if trailing_return <= BEAR_THRESHOLD:
        regime = "bear"
        weight = BEAR_WEIGHT
    elif trailing_return >= BULL_THRESHOLD:
        regime = "bull"
        weight = BULL_WEIGHT
    else:
        regime = "neutral"
        weight = NEUTRAL_WEIGHT

    return RegimeContext(
        regime=regime,
        spy_trailing_return=round(trailing_return, 6),
        regime_weight_buy=weight,
        lookback_days=lookback_days,
        computed_at=datetime.now().isoformat(),
    )


def _unknown_regime(lookback_days: int) -> RegimeContext:
    return RegimeContext(
        regime="unknown",
        spy_trailing_return=None,
        regime_weight_buy=1.0,
        lookback_days=lookback_days,
        computed_at=datetime.now().isoformat(),
    )
