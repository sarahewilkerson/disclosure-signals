"""Scoring application service for the legacy insider engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import config
from scoring import compute_aggregate_index, score_all_companies


@dataclass
class ScoreRunResult:
    reference_date: datetime
    company_score_count: int
    aggregate_index_count: int


def compute_scores(reference_date: datetime, db_path: str | None = None) -> ScoreRunResult:
    """Compute company scores and aggregate indices while preserving legacy behavior."""
    company_scores = score_all_companies(reference_date=reference_date, db_path=db_path)
    aggregate_indices = compute_aggregate_index(reference_date=reference_date, db_path=db_path)
    return ScoreRunResult(
        reference_date=reference_date,
        company_score_count=len(company_scores),
        aggregate_index_count=len(aggregate_indices),
    )

