from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import datetime


TOP_LEADERSHIP_PATTERNS = [
    (r"\bceo\b", "ceo"),
    (r"\bchief executive officer\b", "ceo"),
    (r"\bcfo\b", "cfo"),
    (r"\bchief financial officer\b", "cfo"),
    (r"\bprincipal financial officer\b", "cfo"),
    (r"\bchairman\b", "chair"),
    (r"\bchairwoman\b", "chair"),
    (r"\bchairperson\b", "chair"),
    (r"\bchair of the board\b", "chair"),
    (r"\bexecutive chair\b", "chair"),
    (r"\bexec\.?\s*chair\b", "chair"),
    (r"(?<!\bvice )(?<!\bvice)\bpresident\b", "president"),
    (r"\bchief operating officer\b", "coo"),
    (r"\bcoo\b", "coo"),
]
ROLE_PRIORITY = ["ceo", "cfo", "chair", "president", "coo"]
ENTITY_EXCLUSION_PATTERNS = [
    r"\bllc\b", r"\bllp\b", r"\blp\b", r"\btrust\b", r"\bfoundation\b", r"\bfund\b",
    r"\bcapital\b", r"\bpartners\b", r"\badvisors\b", r"\bholdings\b", r"\binc\b",
    r"\bcorp\b", r"\bltd\b", r"\bgroup\b", r"\bassociates\b", r"\binvestment\b",
]
FORMER_OFFICER_PATTERNS = [r"\bformer\b", r"\bex-", r"\bfmr\b", r"\bretired\b", r"\bpast\b"]
PLANNED_TRADE_KEYWORDS = ["10b5-1", "10b-5-1", "rule 10b5", "rule 10b-5", "trading plan", "pre-arranged", "pre-established"]

DIRECTION_WEIGHT_BUY = 1.0
DIRECTION_WEIGHT_SELL = -0.5
ROLE_WEIGHT = {"ceo": 1.0, "cfo": 0.95, "chair": 0.9, "president": 0.85, "coo": 0.8, "officer_other": 0.5}
PLANNED_TRADE_DISCOUNT = 0.25
DIRECT_OWNERSHIP_WEIGHT = 1.0
INDIRECT_OWNERSHIP_WEIGHT = 0.6
SIZE_SIGNAL_BRACKETS = [(0.01, 0.5), (0.05, 0.8), (0.20, 1.0), (1.00, 1.2), (float("inf"), 1.2)]
SIZE_SIGNAL_UNKNOWN = 0.6
RECENCY_HALF_LIFE_DAYS = 45
PER_INSIDER_SATURATION_CAP = 0.30
BULLISH_THRESHOLD = 0.15
BEARISH_THRESHOLD = -0.15
CONFIDENCE_INSUFFICIENT = 0.25
CONFIDENCE_LOW = 0.50
CONFIDENCE_MODERATE = 0.75
CONFIDENCE_MAX = 0.90
ANALYSIS_WINDOWS_DAYS = (30, 90, 180)


def classify_role(officer_title: str | None, owner_name: str | None, is_officer: bool, is_director: bool, is_ten_pct_owner: bool, is_other: bool) -> tuple[str, str | None]:
    if owner_name and any(re.search(pattern, owner_name.lower()) for pattern in ENTITY_EXCLUSION_PATTERNS):
        return "excluded", f"entity_name: {owner_name}"
    if is_ten_pct_owner and not is_officer and not is_director:
        return "excluded", "ten_pct_holder_only"
    if is_other and not is_officer:
        return "excluded", "other_relationship_no_officer_role"
    if officer_title and any(re.search(pattern, officer_title.lower()) for pattern in FORMER_OFFICER_PATTERNS):
        return "excluded", f"former_officer: {officer_title}"
    if officer_title:
        matches = {role for pattern, role in TOP_LEADERSHIP_PATTERNS if re.search(pattern, officer_title.lower())}
        for role in ROLE_PRIORITY:
            if role in matches:
                return role, None
    if is_director and not is_officer:
        return "excluded", "director_only"
    if is_officer:
        return "officer_other", None if not officer_title else f"officer_not_top_leadership: {officer_title}"
    return "excluded", "no_officer_role"


def classify_transaction_type(transaction_code: str | None) -> str:
    mapping = {"P": "open_market_buy", "S": "open_market_sell", "M": "option_exercise", "F": "tax_withhold", "A": "award_grant", "G": "gift"}
    if not transaction_code:
        return "unknown"
    return mapping.get(transaction_code.upper(), "other")


def detect_planned_trade(footnotes: str | None) -> bool:
    if not footnotes:
        return False
    lower = footnotes.lower()
    return any(keyword in lower for keyword in PLANNED_TRADE_KEYWORDS)


def compute_pct_holdings_changed(shares: float | None, shares_after: float | None) -> float | None:
    if shares is None or shares_after is None or shares <= 0:
        return None
    total = shares_after + shares
    if total <= 0:
        return None
    return shares / total


def score_transaction(txn: dict, reference_date: datetime) -> dict:
    txn_code = txn.get("transaction_code", "")
    role = txn.get("role_class", "")
    is_planned = bool(txn.get("is_likely_planned", 0))
    ownership = txn.get("ownership_nature", "D")
    pct_changed = txn.get("pct_holdings_changed")
    txn_date_str = txn.get("transaction_date")

    direction = DIRECTION_WEIGHT_BUY if txn_code == "P" else DIRECTION_WEIGHT_SELL if txn_code == "S" else 0.0
    role_weight = ROLE_WEIGHT.get(role, 0.0)
    discretionary_weight = PLANNED_TRADE_DISCOUNT if is_planned else 1.0
    size_signal = _compute_size_signal(pct_changed)
    ownership_weight = DIRECT_OWNERSHIP_WEIGHT if ownership == "D" else INDIRECT_OWNERSHIP_WEIGHT
    recency_weight = _compute_recency_weight(txn_date_str, reference_date)
    transaction_signal = direction * role_weight * discretionary_weight * size_signal * ownership_weight * recency_weight
    return {
        "direction": direction,
        "role_weight": role_weight,
        "discretionary_weight": discretionary_weight,
        "size_signal": size_signal,
        "ownership_weight": ownership_weight,
        "recency_weight": recency_weight,
        "transaction_signal": transaction_signal,
    }


def aggregate_company_signal(scored: list[dict], window_days: int) -> dict:
    if not scored:
        return {
            "signal": "insufficient",
            "score": 0.0,
            "confidence": 0.0,
            "confidence_tier": "insufficient",
            "buy_count": 0,
            "sell_count": 0,
            "unique_buyers": 0,
            "unique_sellers": 0,
            "explanation": "No qualifying insider transactions in this window.",
            "window_days": window_days,
        }
    score, insider_contributions = _aggregate_with_saturation(scored)
    buys = [s for s in scored if s["direction"] > 0]
    sells = [s for s in scored if s["direction"] < 0]
    unique_buyers = len({s["cik_owner"] for s in buys})
    unique_sellers = len({s["cik_owner"] for s in sells})
    unique_insiders = len({s["cik_owner"] for s in scored})
    confidence = _compute_confidence(len(scored), unique_insiders, bool(buys), bool(sells))
    signal = _label_signal(score, confidence)
    confidence_tier = _confidence_tier(confidence)
    return {
        "signal": signal,
        "score": round(score, 4),
        "confidence": round(confidence, 4),
        "confidence_tier": confidence_tier,
        "buy_count": len(buys),
        "sell_count": len(sells),
        "unique_buyers": unique_buyers,
        "unique_sellers": unique_sellers,
        "explanation": _build_explanation(signal, score, confidence, confidence_tier, len(buys), len(sells), unique_buyers, unique_sellers, insider_contributions, window_days),
        "window_days": window_days,
    }


def _compute_size_signal(pct_changed: float | None) -> float:
    if pct_changed is None:
        return SIZE_SIGNAL_UNKNOWN
    for max_pct, weight in SIZE_SIGNAL_BRACKETS:
        if pct_changed <= max_pct:
            return weight
    return SIZE_SIGNAL_BRACKETS[-1][1]


def _compute_recency_weight(txn_date_str: str | None, reference_date: datetime) -> float:
    if not txn_date_str:
        return 0.5
    try:
        txn_date = datetime.strptime(txn_date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return 0.5
    days_ago = max(0, (reference_date - txn_date).days)
    return math.exp(-0.693 * days_ago / RECENCY_HALF_LIFE_DAYS)


def _aggregate_with_saturation(scored: list[dict]) -> tuple[float, dict]:
    insider_signals = defaultdict(float)
    for row in scored:
        insider_signals[row["cik_owner"]] += row["transaction_signal"]
    if not insider_signals:
        return 0.0, {}
    total_magnitude = sum(abs(v) for v in insider_signals.values())
    if total_magnitude == 0:
        return 0.0, dict(insider_signals)
    capped = {}
    for insider, signal in insider_signals.items():
        others_magnitude = total_magnitude - abs(signal)
        max_allowed = abs(signal) if others_magnitude <= 0 else others_magnitude * PER_INSIDER_SATURATION_CAP / (1.0 - PER_INSIDER_SATURATION_CAP)
        capped[insider] = max_allowed * (1 if signal > 0 else -1) if abs(signal) > max_allowed else signal
    final_score = math.tanh(sum(capped.values()) / max(1, len(capped)))
    return final_score, dict(capped)


def _compute_confidence(total_transactions: int, unique_insiders: int, has_buys: bool, has_sells: bool) -> float:
    if total_transactions == 0:
        return 0.0
    txn_factor = min(1.0, math.log(1 + total_transactions) / math.log(11))
    breadth_factor = min(1.0, math.log(1 + unique_insiders) / math.log(6))
    balance_factor = 1.1 if (has_buys and has_sells) else 1.0
    confidence = (txn_factor * 0.4 + breadth_factor * 0.6) * balance_factor
    return min(confidence, CONFIDENCE_MAX)


def _label_signal(score: float, confidence: float) -> str:
    if confidence < CONFIDENCE_INSUFFICIENT:
        return "insufficient"
    if score > BULLISH_THRESHOLD:
        return "bullish"
    if score < BEARISH_THRESHOLD:
        return "bearish"
    return "neutral"


def _confidence_tier(confidence: float) -> str:
    if confidence < CONFIDENCE_INSUFFICIENT:
        return "insufficient"
    if confidence < CONFIDENCE_LOW:
        return "low"
    if confidence < CONFIDENCE_MODERATE:
        return "moderate"
    return "high"


def _build_explanation(signal: str, score: float, confidence: float, confidence_tier: str, buy_count: int, sell_count: int, unique_buyers: int, unique_sellers: int, insider_contributions: dict, window_days: int) -> str:
    if signal == "insufficient":
        return "Insufficient qualifying insider transactions to determine a signal."
    parts = [
        f"Signal: {signal.upper()} (score={score:.3f}, confidence={confidence:.2f}/{confidence_tier}).",
        f"Based on {buy_count} buy(s) and {sell_count} sell(s) from {unique_buyers} buyer(s) and {unique_sellers} seller(s) over the past {window_days} days.",
    ]
    if insider_contributions:
        top_insider, top_signal = sorted(insider_contributions.items(), key=lambda item: abs(item[1]), reverse=True)[0]
        parts.append(f"Largest contributor: insider CIK {top_insider} ({'bullish' if top_signal > 0 else 'bearish'}, raw signal={top_signal:.3f}).")
    return " ".join(parts)
