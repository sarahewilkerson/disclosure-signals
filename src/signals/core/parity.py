from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ParityReport:
    structural_ok: bool
    analytical_ok: bool
    reporting_ok: bool
    tolerated_deltas: dict
    unexpected_divergences: list[str]

    def to_dict(self) -> dict:
        return {
            "structural_ok": self.structural_ok,
            "analytical_ok": self.analytical_ok,
            "reporting_ok": self.reporting_ok,
            "tolerated_deltas": self.tolerated_deltas,
            "unexpected_divergences": self.unexpected_divergences,
        }


def compare_expected(actual: dict, expected: dict, score_tolerance: float = 1e-6) -> ParityReport:
    divergences: list[str] = []
    tolerated: dict = {}

    structural_ok = actual["normalized_count"] == expected["normalized_count"]
    if not structural_ok:
        divergences.append(
            f"normalized_count expected={expected['normalized_count']} actual={actual['normalized_count']}"
        )

    actual_scores = actual["source_scores"]
    expected_scores = expected["source_scores"]
    analytical_ok = True
    for key, expected_score in expected_scores.items():
        actual_score = actual_scores.get(key)
        if actual_score is None:
            analytical_ok = False
            divergences.append(f"missing score for {key}")
            continue
        delta = abs(actual_score["score"] - expected_score["score"])
        if actual_score["label"] != expected_score["label"] or delta > score_tolerance:
            analytical_ok = False
            divergences.append(
                f"score mismatch for {key}: expected {expected_score}, actual {actual_score}"
            )
        else:
            tolerated[key] = {"score_delta": delta}

    reporting_ok = actual["combined_summary"] == expected["combined_summary"]
    if not reporting_ok:
        divergences.append("combined summary mismatch")

    return ParityReport(
        structural_ok=structural_ok,
        analytical_ok=analytical_ok,
        reporting_ok=reporting_ok,
        tolerated_deltas=tolerated,
        unexpected_divergences=divergences,
    )

