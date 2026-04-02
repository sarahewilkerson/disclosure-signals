"""Reporting application service for the legacy insider engine."""

from __future__ import annotations

from dataclasses import dataclass

from reporting import save_reports


@dataclass
class ReportRunResult:
    cli_report_path: str
    html_dashboard_path: str
    cli_report: str


def generate_reports(db_path: str | None = None, output_dir: str | None = None) -> ReportRunResult:
    """Generate legacy insider reports and return stable metadata for CLI adapters."""
    reports = save_reports(db_path=db_path, output_dir=output_dir)
    return ReportRunResult(
        cli_report_path=reports["cli_report_path"],
        html_dashboard_path=reports["html_dashboard_path"],
        cli_report=reports["cli_report"],
    )

