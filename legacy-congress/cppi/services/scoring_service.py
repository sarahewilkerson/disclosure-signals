"""Scoring application service for CPPI.

Extracts the score orchestration logic out of the CLI while preserving
existing behavior and database semantics.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from cppi.config import MIN_MEMBERS, MIN_TRANSACTIONS
from cppi.db import get_connection, upsert_positioning_score
from cppi.scoring import compute_aggregate, compute_confidence_score, score_transaction

logger = logging.getLogger(__name__)


@dataclass
class ScoreComputationResult:
    window: int
    transaction_count: int
    unique_members: int
    breadth_pct: float
    buyers: int
    sellers: int
    net_volume: float
    confidence_tier: str
    confidence_score: float


def compute_and_store_score(window: int, db_path: str | None = None) -> ScoreComputationResult | None:
    """Compute the aggregate CPPI score and persist it.

    Returns a summary result for presentation, or None if there were too few
    qualifying transactions to compute a score.
    """
    reference_date = datetime.now()
    cutoff_date = reference_date - timedelta(days=window)

    logger.info(f"Computing positioning scores for {window}-day window...")

    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                id, filing_id, owner_type, asset_name_raw, asset_type,
                resolved_ticker, transaction_type, execution_date,
                amount_min, amount_max, include_in_signal,
                resolution_confidence
            FROM transactions
            WHERE include_in_signal = 1
              AND execution_date >= ?
            ORDER BY execution_date DESC
            """,
            (cutoff_date.strftime("%Y-%m-%d"),),
        ).fetchall()

        if len(rows) < MIN_TRANSACTIONS:
            logger.warning(
                f"Insufficient transactions: {len(rows)} < {MIN_TRANSACTIONS} minimum. "
                "Signal cannot be generated."
            )
            return None

        logger.info(f"Scoring {len(rows)} transactions...")

        scored = []
        for row in rows:
            exec_date = None
            if row["execution_date"]:
                try:
                    exec_date = datetime.strptime(row["execution_date"], "%Y-%m-%d")
                except ValueError:
                    pass

            txn = score_transaction(
                member_id=row["filing_id"],
                ticker=row["resolved_ticker"],
                transaction_type=row["transaction_type"] or "purchase",
                execution_date=exec_date,
                amount_min=row["amount_min"],
                amount_max=row["amount_max"],
                owner_type=row["owner_type"] or "self",
                resolution_confidence=row["resolution_confidence"] or 1.0,
                signal_weight=1.0,
                reference_date=reference_date,
            )
            scored.append(txn)

        agg = compute_aggregate(scored)

        if agg.unique_members < MIN_MEMBERS:
            logger.warning(
                f"Insufficient members: {agg.unique_members} < {MIN_MEMBERS} minimum. "
                "Signal may not be reliable."
            )

        total_txns = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE execution_date >= ?",
            (cutoff_date.strftime("%Y-%m-%d"),),
        ).fetchone()[0]
        resolution_rate = len(rows) / total_txns if total_txns > 0 else 0
        conf = compute_confidence_score(agg, resolution_rate)

        upsert_positioning_score(
            conn,
            {
                "scope": "all",
                "window_days": window,
                "computed_at": datetime.now().isoformat(),
                "breadth_pct": agg.breadth_pct,
                "unique_buyers": agg.buyers,
                "unique_sellers": agg.sellers,
                "net_positioning": agg.volume_net,
                "buy_volume": agg.volume_buy,
                "sell_volume": agg.volume_sell,
                "confidence_score": conf["composite_score"],
                "confidence_tier": conf["tier"],
                "confidence_factors": json.dumps(conf["factors"]),
                "top5_share": agg.concentration_top5,
                "transaction_count": agg.transactions_included,
                "excluded_count": agg.transactions_excluded,
                "sector_positioning": None,
            },
        )

    return ScoreComputationResult(
        window=window,
        transaction_count=len(scored),
        unique_members=agg.unique_members,
        breadth_pct=agg.breadth_pct,
        buyers=agg.buyers,
        sellers=agg.sellers,
        net_volume=agg.volume_net,
        confidence_tier=conf["tier"],
        confidence_score=conf["composite_score"],
    )

