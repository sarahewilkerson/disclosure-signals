"""Reporting application service for CPPI."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from cppi.db import get_connection
from cppi.reporting import DataQuality, ReportData, generate_json_report, generate_text_report
from cppi.scoring import AggregateResult

logger = logging.getLogger(__name__)


@dataclass
class ReportBuildResult:
    output_path: Path
    content: str


def build_report(window: int, output: str, report_format: str, db_path: str | None = None) -> ReportBuildResult | None:
    """Build and persist the CPPI report for the requested window."""
    logger.info(f"Generating report to {output}...")

    with get_connection(db_path) as conn:
        score = conn.execute(
            """
            SELECT * FROM positioning_scores
            WHERE scope = 'all' AND window_days = ?
            ORDER BY computed_at DESC
            LIMIT 1
            """,
            (window,),
        ).fetchone()

        if not score:
            logger.error(f"No scores found for {window}-day window. Run 'cppi score' first.")
            return None

        cutoff_date = (datetime.now() - timedelta(days=window)).strftime("%Y-%m-%d")

        included = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE include_in_signal = 1 AND execution_date >= ?",
            (cutoff_date,),
        ).fetchone()[0]

        excluded_asset = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE include_in_signal = 0 AND exclusion_reason LIKE 'asset_excluded%' AND execution_date >= ?",
            (cutoff_date,),
        ).fetchone()[0]

        excluded_other = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE include_in_signal = 0 AND (exclusion_reason IS NULL OR exclusion_reason NOT LIKE 'asset_excluded%') AND execution_date >= ?",
            (cutoff_date,),
        ).fetchone()[0]

        resolved = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE include_in_signal = 1 AND resolved_ticker IS NOT NULL AND execution_date >= ?",
            (cutoff_date,),
        ).fetchone()[0]
        resolution_rate = resolved / included if included > 0 else 0

        try:
            conf_factors = json.loads(score["confidence_factors"]) if score["confidence_factors"] else {}
        except (json.JSONDecodeError, TypeError):
            conf_factors = {}

        agg = AggregateResult(
            breadth_pct=score["breadth_pct"] or 0,
            unique_members=score["unique_buyers"] + score["unique_sellers"],
            buyers=score["unique_buyers"] or 0,
            sellers=score["unique_sellers"] or 0,
            neutral=0,
            volume_net=score["net_positioning"] or 0,
            volume_buy=score["buy_volume"] or 0,
            volume_sell=score["sell_volume"] or 0,
            concentration_top5=conf_factors.get("concentration", 0),
            is_concentrated=conf_factors.get("concentration", 0) < 0.5,
            members_capped=0,
            mean_staleness=conf_factors.get("timeliness", 0.5),
            transactions_included=included,
            transactions_excluded=excluded_asset + excluded_other,
        )

        quality = DataQuality(
            included_transactions=included,
            excluded_asset_class=excluded_asset,
            excluded_unparseable=excluded_other,
            resolution_rate_count=resolution_rate,
            resolution_rate_value=resolution_rate,
            mean_disclosure_lag=45,
            min_disclosure_lag=10,
            max_disclosure_lag=90,
        )

        report_data = ReportData(
            generated_at=datetime.now(),
            window_days=window,
            window_end=datetime.now(),
            aggregate=agg,
            confidence_score=score["confidence_score"] or 0,
            confidence_tier=score["confidence_tier"] or "UNKNOWN",
            confidence_factors=conf_factors,
            quality=quality,
        )

        if report_format == "json":
            report = json.dumps(generate_json_report(report_data), indent=2)
        else:
            report = generate_text_report(report_data)

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report)
    logger.info(f"Report written to {output}")
    return ReportBuildResult(output_path=output_path, content=report)

