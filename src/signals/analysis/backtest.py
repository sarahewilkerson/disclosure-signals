"""Historical time-series backtesting framework.

Runs the scoring pipeline at multiple historical reference dates using
cached XML/PDF/HTML files (no re-downloading). Produces time-series
of signals for stability, turnover, and forward-return analysis.

Usage:
    from signals.analysis.backtest import run_backtest, BacktestConfig
    config = BacktestConfig(
        start_date=datetime(2025, 1, 1), end_date=datetime(2025, 12, 1),
        interval="monthly", insider_xml_dir="...", house_pdf_dir="...",
        senate_html_dir="...", derived_db_path="...",
    )
    result = run_backtest(config)
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable


@dataclass
class BacktestConfig:
    start_date: datetime
    end_date: datetime
    interval: str  # "monthly" | "biweekly"
    insider_xml_dir: str
    house_pdf_dir: str
    senate_html_dir: str
    derived_db_path: str
    lookback_window: int = 90
    regime_weight: float = 1.0


@dataclass
class DateRunResult:
    reference_date: str
    insider_run_id: str | None
    house_run_id: str | None
    senate_run_id: str | None
    combined_run_id: str | None
    insider_signal_count: int
    congress_signal_count: int
    combined_count: int
    duration_seconds: float


@dataclass
class BacktestResult:
    config: dict
    dates: list[str]
    date_results: list[DateRunResult]
    run_ids_by_date: dict[str, list[str]]
    total_duration_seconds: float

    def to_dict(self) -> dict:
        return {
            "config": self.config,
            "dates": self.dates,
            "date_results": [asdict(r) for r in self.date_results],
            "run_ids_by_date": self.run_ids_by_date,
            "total_duration_seconds": self.total_duration_seconds,
        }


def generate_backtest_dates(start: datetime, end: datetime, interval: str) -> list[datetime]:
    """Generate backtest reference dates.

    Monthly: 1st of each month. Biweekly: every 14 days.
    """
    dates = []
    current = start.replace(day=1) if interval == "monthly" else start

    while current <= end:
        dates.append(current)
        if interval == "monthly":
            month = current.month + 1
            year = current.year
            if month > 12:
                month = 1
                year += 1
            current = current.replace(year=year, month=month, day=1)
        else:  # biweekly
            current += timedelta(days=14)

    return dates


def run_backtest(
    config: BacktestConfig,
    progress_callback: Callable[[str, dict], None] | None = None,
) -> BacktestResult:
    """Run scoring pipeline at each historical date.

    Reuses cached files (no re-downloading). Only varies reference_date.
    Sequential execution to avoid DB contention.
    """
    from signals.combined.service import build_from_derived
    from signals.congress.direct_service import run_direct_house_pdfs_into_derived
    from signals.congress.senate_direct import run_direct_senate_html_into_derived
    from signals.core.derived_db import get_connection, init_db
    from signals.insider.direct_service import run_direct_xml_into_derived

    dates = generate_backtest_dates(config.start_date, config.end_date, config.interval)
    repo_root = Path(__file__).resolve().parent.parent.parent.parent
    init_db(config.derived_db_path)

    date_results: list[DateRunResult] = []
    run_ids_by_date: dict[str, list[str]] = {}
    total_start = time.time()

    for i, ref_date in enumerate(dates):
        date_str = ref_date.strftime("%Y-%m-%d")
        if progress_callback:
            progress_callback("backtest_date_start", {"date": date_str, "index": i, "total": len(dates)})

        date_start = time.time()
        insider_run_id = None
        house_run_id = None
        senate_run_id = None
        combined_run_id = None
        insider_count = 0
        congress_count = 0
        combined_count = 0

        try:
            # Score insider
            insider = run_direct_xml_into_derived(
                repo_root=repo_root,
                derived_db_path=config.derived_db_path,
                xml_dir=config.insider_xml_dir,
                reference_date=ref_date,
                regime_weight=config.regime_weight,
            )
            insider_run_id = insider.run_id
            insider_count = insider.imported_result_count

            # Score house
            house = run_direct_house_pdfs_into_derived(
                repo_root=repo_root,
                derived_db_path=config.derived_db_path,
                pdf_dir=config.house_pdf_dir,
                reference_date=ref_date,
                window_days=config.lookback_window,
                regime_weight=config.regime_weight,
            )
            house_run_id = house.run_id

            # Score senate
            senate = run_direct_senate_html_into_derived(
                repo_root=repo_root,
                derived_db_path=config.derived_db_path,
                html_dir=senate_html_dir if (senate_html_dir := config.senate_html_dir) else "",
                reference_date=ref_date,
                window_days=config.lookback_window,
                regime_weight=config.regime_weight,
            )
            senate_run_id = senate.run_id
            congress_count = house.imported_result_count + senate.imported_result_count

            # Build overlay
            combined = build_from_derived(repo_root, config.derived_db_path, lookback_window=config.lookback_window)
            combined_run_id = combined.run_id
            combined_count = combined.combined_count

        except Exception as e:
            if progress_callback:
                progress_callback("backtest_date_error", {"date": date_str, "error": str(e)})

        duration = time.time() - date_start
        run_ids = [rid for rid in [insider_run_id, house_run_id, senate_run_id, combined_run_id] if rid]
        run_ids_by_date[date_str] = run_ids

        date_results.append(DateRunResult(
            reference_date=date_str,
            insider_run_id=insider_run_id,
            house_run_id=house_run_id,
            senate_run_id=senate_run_id,
            combined_run_id=combined_run_id,
            insider_signal_count=insider_count,
            congress_signal_count=congress_count,
            combined_count=combined_count,
            duration_seconds=round(duration, 2),
        ))

        if progress_callback:
            progress_callback("backtest_date_done", {
                "date": date_str, "duration": round(duration, 2),
                "insider": insider_count, "congress": congress_count, "combined": combined_count,
            })

    return BacktestResult(
        config=asdict(config) if hasattr(config, "__dataclass_fields__") else {},
        dates=[d.strftime("%Y-%m-%d") for d in dates],
        date_results=date_results,
        run_ids_by_date=run_ids_by_date,
        total_duration_seconds=round(time.time() - total_start, 2),
    )


def render_backtest_markdown(result: BacktestResult) -> str:
    """Render backtest results as markdown."""
    lines = [
        "# Backtest Results",
        "",
        f"**Dates:** {len(result.dates)} ({result.dates[0]} to {result.dates[-1]})" if result.dates else "No dates",
        f"**Total duration:** {result.total_duration_seconds:.1f}s",
        "",
        "| Date | Insider | Congress | Combined | Duration |",
        "|------|---------|----------|----------|----------|",
    ]
    for dr in result.date_results:
        lines.append(
            f"| {dr.reference_date} | {dr.insider_signal_count} | {dr.congress_signal_count} | {dr.combined_count} | {dr.duration_seconds:.1f}s |"
        )

    return "\n".join(lines) + "\n"
