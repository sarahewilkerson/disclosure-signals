"""
Signal scoring engine.

Computes:
1. Per-transaction signal scores
2. Company-level aggregated scores with saturation caps
3. Confidence scores and signal labels
4. Aggregate Fortune 500 Executive Risk Appetite Index
5. Sector-balanced and CEO/CFO-only variants
6. Breadth measures

All scoring logic is deterministic and explainable.
"""

import json
import logging
import math
import time
from collections import defaultdict
from datetime import datetime, timedelta

import config
from db import (
    get_connection,
    get_companies,
    get_signal_transactions,
    get_companies_with_new_filings,
    get_last_score_timestamp,
    upsert_company_score,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-transaction scoring
# ---------------------------------------------------------------------------
def score_transaction(txn: dict, reference_date: datetime) -> dict:
    """
    Score a single transaction.

    Returns a dict with the raw signal components and final transaction_signal.
    """
    txn_code = txn.get("transaction_code", "")
    role = txn.get("role_class", "")
    is_planned = bool(txn.get("is_likely_planned", 0))
    ownership = txn.get("ownership_nature", "D")
    pct_changed = txn.get("pct_holdings_changed")
    txn_date_str = txn.get("transaction_date")

    # Direction weight
    if txn_code == "P":
        direction = config.DIRECTION_WEIGHT_BUY
    elif txn_code == "S":
        direction = config.DIRECTION_WEIGHT_SELL
    else:
        direction = 0.0

    # Role weight
    role_weight = config.ROLE_WEIGHT.get(role, 0.0)

    # Discretionary weight
    discretionary_weight = config.PLANNED_TRADE_DISCOUNT if is_planned else 1.0

    # Size signal
    size_signal = _compute_size_signal(pct_changed)

    # Ownership weight
    ownership_weight = (
        config.DIRECT_OWNERSHIP_WEIGHT
        if ownership == "D"
        else config.INDIRECT_OWNERSHIP_WEIGHT
    )

    # Recency weight
    recency_weight = _compute_recency_weight(txn_date_str, reference_date)

    # Final transaction signal
    transaction_signal = (
        direction
        * role_weight
        * discretionary_weight
        * size_signal
        * ownership_weight
        * recency_weight
    )

    return {
        "direction": direction,
        "role_weight": role_weight,
        "discretionary_weight": discretionary_weight,
        "size_signal": size_signal,
        "ownership_weight": ownership_weight,
        "recency_weight": recency_weight,
        "transaction_signal": transaction_signal,
    }


def _compute_size_signal(pct_changed: float | None) -> float:
    """Compute the size signal from percent of holdings changed."""
    if pct_changed is None:
        return config.SIZE_SIGNAL_UNKNOWN

    for max_pct, weight in config.SIZE_SIGNAL_BRACKETS:
        if pct_changed <= max_pct:
            return weight

    return config.SIZE_SIGNAL_BRACKETS[-1][1]  # cap


def _compute_recency_weight(txn_date_str: str | None, reference_date: datetime) -> float:
    """Compute exponential decay recency weight."""
    if not txn_date_str:
        return 0.5  # penalize missing date

    try:
        txn_date = datetime.strptime(txn_date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return 0.5

    days_ago = (reference_date - txn_date).days
    if days_ago < 0:
        days_ago = 0  # future-dated transaction, treat as today

    half_life = config.RECENCY_HALF_LIFE_DAYS
    return math.exp(-0.693 * days_ago / half_life)


# ---------------------------------------------------------------------------
# Company-level scoring
# ---------------------------------------------------------------------------
def score_company(cik: str, window_days: int, reference_date: datetime,
                  db_path: str = None, roles_filter: set = None) -> dict:
    """
    Compute a company-level signal score for a given time window.

    Args:
        cik: Issuer CIK.
        window_days: Lookback window in days.
        reference_date: The "as of" date for scoring.
        db_path: Optional database path.
        roles_filter: If set, only include these roles (e.g., {'ceo', 'cfo'}).

    Returns dict with score, signal, confidence, explanation, etc.
    """
    since_date = (reference_date - timedelta(days=window_days)).strftime("%Y-%m-%d")

    with get_connection(db_path) as conn:
        transactions = get_signal_transactions(conn, cik, since_date)

    txns = [dict(t) for t in transactions]

    # Apply optional role filter
    if roles_filter:
        txns = [t for t in txns if t.get("role_class") in roles_filter]

    if not txns:
        return _empty_score(cik, window_days)

    # Score each transaction
    scored = []
    for txn in txns:
        score_detail = score_transaction(txn, reference_date)
        scored.append({**txn, **score_detail})

    # Aggregate with per-insider saturation cap
    score, insider_contributions = _aggregate_with_saturation(scored)

    # Compute stats
    buys = [s for s in scored if s["direction"] > 0]
    sells = [s for s in scored if s["direction"] < 0]
    unique_buyers = len(set(s["cik_owner"] for s in buys))
    unique_sellers = len(set(s["cik_owner"] for s in sells))
    unique_insiders = len(set(s["cik_owner"] for s in scored))
    net_buy_value = sum(s.get("total_value", 0) or 0 for s in buys) - sum(
        s.get("total_value", 0) or 0 for s in sells
    )

    # Confidence
    confidence = _compute_confidence(
        total_transactions=len(scored),
        unique_insiders=unique_insiders,
        has_buys=len(buys) > 0,
        has_sells=len(sells) > 0,
    )

    # Signal label
    signal = _label_signal(score, confidence)

    # Confidence tier
    confidence_tier = _confidence_tier(confidence)

    # Explanation
    explanation = _build_explanation(
        signal=signal,
        score=score,
        confidence=confidence,
        confidence_tier=confidence_tier,
        buy_count=len(buys),
        sell_count=len(sells),
        unique_buyers=unique_buyers,
        unique_sellers=unique_sellers,
        insider_contributions=insider_contributions,
        window_days=window_days,
    )

    # Filing accessions
    accessions = list(set(s["accession_number"] for s in scored))

    return {
        "cik": cik,
        "window_days": window_days,
        "computed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "signal": signal,
        "score": round(score, 4),
        "confidence": round(confidence, 4),
        "confidence_tier": confidence_tier,
        "buy_count": len(buys),
        "sell_count": len(sells),
        "unique_buyers": unique_buyers,
        "unique_sellers": unique_sellers,
        "net_buy_value": round(net_buy_value, 2),
        "explanation": explanation,
        "filing_accessions": json.dumps(accessions),
    }


def _empty_score(cik: str, window_days: int) -> dict:
    """Return an empty/insufficient score for a company with no data."""
    return {
        "cik": cik,
        "window_days": window_days,
        "computed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "signal": "insufficient",
        "score": 0.0,
        "confidence": 0.0,
        "confidence_tier": "insufficient",
        "buy_count": 0,
        "sell_count": 0,
        "unique_buyers": 0,
        "unique_sellers": 0,
        "net_buy_value": 0.0,
        "explanation": "No qualifying insider transactions in this window.",
        "filing_accessions": "[]",
    }


def _aggregate_with_saturation(scored: list[dict]) -> tuple[float, dict]:
    """
    Aggregate transaction signals with per-insider saturation cap.

    The cap prevents any single insider from contributing more than
    PER_INSIDER_SATURATION_CAP of the total signal magnitude. The cap
    is computed from the *other* insiders' magnitude to avoid the
    dominant insider inflating the cap threshold.

    Returns (normalized_score, insider_contributions dict).
    """
    # Sum signals by insider
    insider_signals = defaultdict(float)
    for s in scored:
        insider_signals[s["cik_owner"]] += s["transaction_signal"]

    if not insider_signals:
        return 0.0, {}

    total_magnitude = sum(abs(v) for v in insider_signals.values())
    if total_magnitude == 0:
        return 0.0, dict(insider_signals)

    cap_pct = config.PER_INSIDER_SATURATION_CAP

    # For each insider, compute the cap based on OTHER insiders' magnitude.
    # This prevents a dominant insider from inflating the cap threshold.
    # cap_for_insider_i = (sum of |others|) * cap_pct / (1 - cap_pct)
    # This ensures insider_i / (insider_i + others) <= cap_pct
    capped_signals = {}
    for insider, signal in insider_signals.items():
        others_magnitude = total_magnitude - abs(signal)
        if others_magnitude > 0:
            # max_allowed such that max_allowed / (max_allowed + others) = cap_pct
            max_allowed = others_magnitude * cap_pct / (1.0 - cap_pct)
        else:
            # This insider is the only one
            max_allowed = abs(signal)

        if abs(signal) > max_allowed:
            capped_signals[insider] = max_allowed * (1 if signal > 0 else -1)
        else:
            capped_signals[insider] = signal

    # Sum capped signals
    raw_sum = sum(capped_signals.values())

    # Normalize by number of unique insiders
    n_insiders = max(1, len(capped_signals))
    normalized = raw_sum / n_insiders

    # Apply tanh to bound to [-1, 1]
    final_score = math.tanh(normalized)

    return final_score, dict(capped_signals)


def _compute_confidence(total_transactions: int, unique_insiders: int,
                        has_buys: bool, has_sells: bool) -> float:
    """
    Compute a confidence score in [0, CONFIDENCE_MAX].

    Factors:
    - More transactions → more confidence
    - More unique insiders → much more confidence (breadth)
    - Both buys and sells → more confidence (balanced data)
    """
    if total_transactions == 0:
        return 0.0

    # Base: log-scaled transaction count
    txn_factor = min(1.0, math.log(1 + total_transactions) / math.log(11))  # saturates ~10 txns

    # Breadth bonus: unique insiders
    breadth_factor = min(1.0, math.log(1 + unique_insiders) / math.log(6))  # saturates ~5 insiders

    # Balance bonus: having both buy and sell data
    balance_factor = 1.1 if (has_buys and has_sells) else 1.0

    confidence = txn_factor * 0.4 + breadth_factor * 0.6
    confidence *= balance_factor

    return min(confidence, config.CONFIDENCE_MAX)


def _label_signal(score: float, confidence: float) -> str:
    """Assign a signal label based on score and confidence."""
    if confidence < config.CONFIDENCE_INSUFFICIENT:
        return "insufficient"

    if score > config.BULLISH_THRESHOLD:
        return "bullish"
    elif score < config.BEARISH_THRESHOLD:
        return "bearish"
    else:
        return "neutral"


def _confidence_tier(confidence: float) -> str:
    """Map confidence to a human-readable tier."""
    if confidence < config.CONFIDENCE_INSUFFICIENT:
        return "insufficient"
    elif confidence < config.CONFIDENCE_LOW:
        return "low"
    elif confidence < config.CONFIDENCE_MODERATE:
        return "moderate"
    else:
        return "high"


def _build_explanation(signal: str, score: float, confidence: float,
                       confidence_tier: str, buy_count: int, sell_count: int,
                       unique_buyers: int, unique_sellers: int,
                       insider_contributions: dict, window_days: int) -> str:
    """Build a plain-English explanation of the signal."""
    parts = []

    if signal == "insufficient":
        return "Insufficient qualifying insider transactions to determine a signal."

    parts.append(
        f"Signal: {signal.upper()} (score={score:.3f}, "
        f"confidence={confidence:.2f}/{confidence_tier})."
    )
    parts.append(
        f"Based on {buy_count} buy(s) and {sell_count} sell(s) "
        f"from {unique_buyers} buyer(s) and {unique_sellers} seller(s) "
        f"over the past {window_days} days."
    )

    # Note dominant contributors
    if insider_contributions:
        top = sorted(insider_contributions.items(), key=lambda x: abs(x[1]), reverse=True)
        if len(top) > 0:
            top_insider = top[0]
            direction = "bullish" if top_insider[1] > 0 else "bearish"
            parts.append(
                f"Largest contributor: insider CIK {top_insider[0]} ({direction}, "
                f"raw signal={top_insider[1]:.3f})."
            )

    return " ".join(parts)


# ---------------------------------------------------------------------------
# Score all companies
# ---------------------------------------------------------------------------
def score_all_companies(reference_date: datetime = None,
                        db_path: str = None,
                        companies: list = None,
                        incremental: bool = False) -> list[dict]:
    """
    Score companies for all time windows.

    Args:
        reference_date: The "as of" date for scoring. Defaults to now.
        db_path: Optional database path.
        companies: Optional list of companies to score. If None, scores all.
        incremental: If True, only score companies with new filings since
                     the last scoring run. Ignored if companies is provided.

    Stores results in company_scores table using upsert.
    """
    if reference_date is None:
        reference_date = datetime.now()

    # Determine which companies to score
    if companies is not None:
        companies_to_score = companies
    elif incremental:
        with get_connection(db_path) as conn:
            last_computed = get_last_score_timestamp(conn)
            if last_computed:
                companies_to_score = get_companies_with_new_filings(conn, last_computed)
                logger.info(
                    f"Incremental mode: {len(companies_to_score)} companies with new filings "
                    f"since {last_computed}"
                )
            else:
                # No previous scores — score all
                companies_to_score = get_companies(conn)
                logger.info("No previous scores found; scoring all companies.")
    else:
        with get_connection(db_path) as conn:
            companies_to_score = get_companies(conn)

    all_scores = []

    for company in companies_to_score:
        cik = company["cik"]
        ticker = company["ticker"]

        for window in config.ANALYSIS_WINDOWS_DAYS:
            result = score_company(cik, window, reference_date, db_path)
            result["ticker"] = ticker

            all_scores.append(result)
            logger.info(
                f"  {ticker} ({window}d): {result['signal']} "
                f"(score={result['score']:.3f}, conf={result['confidence']:.2f})"
            )

    # Store in DB using upsert (incremental-safe)
    with get_connection(db_path) as conn:
        for s in all_scores:
            upsert_company_score(conn, s)

    logger.info(
        f"Scored {len(companies_to_score)} companies across "
        f"{len(config.ANALYSIS_WINDOWS_DAYS)} windows."
    )
    return all_scores


# ---------------------------------------------------------------------------
# Aggregate index
# ---------------------------------------------------------------------------
def compute_aggregate_index(reference_date: datetime = None,
                            db_path: str = None) -> list[dict]:
    """
    Compute the aggregate Fortune 500 Executive Risk Appetite Index.

    Produces:
    - Sector-balanced index (headline)
    - CEO/CFO-only index
    - Bullish/bearish breadth
    - Cyclical vs defensive breakdown

    Returns list of index records (one per time window).
    """
    if reference_date is None:
        reference_date = datetime.now()

    with get_connection(db_path) as conn:
        companies = get_companies(conn)

    # Build a CIK→sector map
    company_sectors = {}
    for company in companies:
        company_sectors[company["cik"]] = company["sector"]

    indices = []

    for window in config.ANALYSIS_WINDOWS_DAYS:
        # Read standard scores from DB (already computed by score_all_companies)
        company_scores = {}
        with get_connection(db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM company_scores WHERE window_days = ?", (window,)
            ).fetchall()
        for row in rows:
            s = dict(row)
            s["sector"] = company_sectors.get(s["cik"])
            company_scores[s["cik"]] = s

        # If no pre-computed scores, compute them now
        if not company_scores:
            for company in companies:
                result = score_company(
                    company["cik"], window, reference_date, db_path
                )
                result["sector"] = company["sector"]
                company_scores[company["cik"]] = result

        # CEO/CFO-only scores (must be computed fresh — not stored in DB)
        ceo_cfo_scores = {}
        for company in companies:
            result = score_company(
                company["cik"], window, reference_date, db_path,
                roles_filter={"ceo", "cfo"},
            )
            ceo_cfo_scores[company["cik"]] = result

        # Compute sector-balanced index
        sector_scores = defaultdict(list)
        for cik, s in company_scores.items():
            sector = s.get("sector") or "Unknown"
            if s["signal"] != "insufficient":
                sector_scores[sector].append(s["score"])

        sector_means = {}
        for sector, scores in sector_scores.items():
            if scores:
                sector_means[sector] = sum(scores) / len(scores)

        # Equal-weight across sectors
        if sector_means:
            sector_balanced = sum(sector_means.values()) / len(sector_means)
        else:
            sector_balanced = 0.0

        # Raw risk appetite (not sector-balanced)
        all_scores_list = [
            s["score"] for s in company_scores.values()
            if s["signal"] != "insufficient"
        ]
        if all_scores_list:
            risk_appetite = sum(all_scores_list) / len(all_scores_list)
        else:
            risk_appetite = 0.0

        # CEO/CFO-only index
        ceo_cfo_list = [
            s["score"] for s in ceo_cfo_scores.values()
            if s["signal"] != "insufficient"
        ]
        if ceo_cfo_list:
            ceo_cfo_index = sum(ceo_cfo_list) / len(ceo_cfo_list)
        else:
            ceo_cfo_index = 0.0

        # Breadth measures
        with_signal = [s for s in company_scores.values() if s["signal"] != "insufficient"]
        total_with_signal = len(with_signal)
        bullish_count = sum(1 for s in with_signal if s["signal"] == "bullish")
        bearish_count = sum(1 for s in with_signal if s["signal"] == "bearish")
        neutral_count = sum(1 for s in with_signal if s["signal"] == "neutral")
        insufficient_count = sum(
            1 for s in company_scores.values() if s["signal"] == "insufficient"
        )

        total_companies = len(company_scores)
        bullish_breadth = bullish_count / total_with_signal if total_with_signal else 0
        bearish_breadth = bearish_count / total_with_signal if total_with_signal else 0
        neutral_pct = neutral_count / total_companies if total_companies else 0
        insufficient_pct = insufficient_count / total_companies if total_companies else 0

        # Cyclical vs defensive
        cyclical_scores = [
            score for sector, score in sector_means.items()
            if sector in config.CYCLICAL_SECTORS
        ]
        defensive_scores = [
            score for sector, score in sector_means.items()
            if sector in config.DEFENSIVE_SECTORS
        ]
        cyclical_score = sum(cyclical_scores) / len(cyclical_scores) if cyclical_scores else 0.0
        defensive_score = sum(defensive_scores) / len(defensive_scores) if defensive_scores else 0.0

        index_record = {
            "window_days": window,
            "computed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "risk_appetite_index": round(risk_appetite, 4),
            "bullish_breadth": round(bullish_breadth, 4),
            "bearish_breadth": round(bearish_breadth, 4),
            "neutral_pct": round(neutral_pct, 4),
            "insufficient_pct": round(insufficient_pct, 4),
            "ceo_cfo_only_index": round(ceo_cfo_index, 4),
            "sector_balanced_index": round(sector_balanced, 4),
            "cyclical_score": round(cyclical_score, 4),
            "defensive_score": round(defensive_score, 4),
            "sector_breakdown": json.dumps(
                {k: round(v, 4) for k, v in sector_means.items()}
            ),
            "total_companies": total_companies,
            "companies_with_signal": total_with_signal,
        }
        indices.append(index_record)

    # Store in DB
    with get_connection(db_path) as conn:
        conn.execute("DELETE FROM aggregate_index")
        for idx in indices:
            conn.execute("""
                INSERT INTO aggregate_index (
                    window_days, computed_at, risk_appetite_index,
                    bullish_breadth, bearish_breadth, neutral_pct,
                    insufficient_pct, ceo_cfo_only_index, sector_balanced_index,
                    cyclical_score, defensive_score, sector_breakdown,
                    total_companies, companies_with_signal
                ) VALUES (
                    :window_days, :computed_at, :risk_appetite_index,
                    :bullish_breadth, :bearish_breadth, :neutral_pct,
                    :insufficient_pct, :ceo_cfo_only_index, :sector_balanced_index,
                    :cyclical_score, :defensive_score, :sector_breakdown,
                    :total_companies, :companies_with_signal
                )
            """, idx)

    return indices
