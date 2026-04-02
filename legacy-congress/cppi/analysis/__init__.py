"""Analysis modules for CPPI."""

from cppi.analysis.sensitivity import run_sensitivity_analysis
from cppi.analysis.weight_comparison import compare_weighting_methods
from cppi.analysis.crossref import (
    run_crossref_analysis,
    format_crossref_report,
    CrossRefReport,
)

__all__ = [
    "run_sensitivity_analysis",
    "compare_weighting_methods",
    "run_crossref_analysis",
    "format_crossref_report",
    "CrossRefReport",
]
