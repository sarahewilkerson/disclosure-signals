from __future__ import annotations

from signals.core.dto import SignalResult


def is_combine_candidate(result: SignalResult, *, lookback_window: int | None = None) -> bool:
    if result.scope != "entity":
        return False
    if lookback_window is not None and int(result.lookback_window) != lookback_window:
        return False
    if result.label == "insufficient":
        return False
    if int(result.input_count or 0) <= 0:
        return False
    if int(result.included_count or 0) <= 0:
        return False
    return True
