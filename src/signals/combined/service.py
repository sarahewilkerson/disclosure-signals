from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from signals.combined.overlay import build_overlay, fingerprint_for_combined
from signals.core.derived_db import (
    fetch_combined_block_events,
    fetch_resolution_events_by_ids,
    fetch_signal_results_by_source,
    get_connection,
    init_db,
    insert_combined_block_event,
    insert_combined_result,
    insert_run,
    update_run_status,
)
from signals.core.dto import SignalResult
from signals.core.git import git_sha
from signals.core.runs import make_run, utcnow_iso
from signals.core.versioning import COMBINE_METHOD_VERSION


@dataclass
class CombinedBuildResult:
    run_id: str
    lookback_window: int
    combined_count: int
    blocked_count: int
    blocked_rows: list[dict]

    def to_dict(self) -> dict:
        return asdict(self)


def _typed_signal_rows(rows: list[dict], lookback_window: int | None = None) -> list[SignalResult]:
    typed = []
    for row in rows:
        if row["scope"] != "entity":
            continue
        if lookback_window is not None and int(row["lookback_window"]) != lookback_window:
            continue
        typed.append(
            SignalResult(
                **{
                    **{k: row[k] for k in SignalResult.__dataclass_fields__.keys()},
                    "provenance_refs": json.loads(row["provenance_refs"]),
                }
            )
        )
    latest: dict[tuple[str, int], SignalResult] = {}
    for row in typed:
        latest[(row.subject_key, row.lookback_window)] = row
    return list(latest.values())


def build_from_derived(repo_root: Path, derived_db_path: str, lookback_window: int = 90) -> CombinedBuildResult:
    init_db(derived_db_path)
    run = make_run(
        "combined_build",
        "combined",
        git_sha(repo_root),
        {"lookback_window": lookback_window},
        {"combine": COMBINE_METHOD_VERSION},
    )
    with get_connection(derived_db_path) as conn:
        insert_run(conn, run)
        insider_rows = fetch_signal_results_by_source(conn, "insider")
        congress_rows = fetch_signal_results_by_source(conn, "congress")
        insider_results = _typed_signal_rows(insider_rows, lookback_window)
        congress_results = _typed_signal_rows(congress_rows, lookback_window)
        resolution_event_ids = []
        for row in insider_results + congress_results:
            resolution_event_ids.extend(row.provenance_refs.get("resolution_event_ids", []))
        resolution_events = fetch_resolution_events_by_ids(conn, sorted(set(event_id for event_id in resolution_event_ids if event_id)))
        combined_rows, blocked = build_overlay(
            insider_results,
            congress_results,
            resolution_events,
            run.run_id,
            lookback_window=lookback_window,
        )
        for row in combined_rows:
            insert_combined_result(conn, row, fingerprint_for_combined(row))
        for row in blocked:
            insert_combined_block_event(conn, row)
        update_run_status(
            conn,
            run.run_id,
            "SUCCEEDED",
            utcnow_iso(),
            {"combined_count": len(combined_rows), "blocked_count": len(blocked)},
        )
        blocked_rows = fetch_combined_block_events(conn, run.run_id)
    return CombinedBuildResult(
        run_id=run.run_id,
        lookback_window=lookback_window,
        combined_count=len(combined_rows),
        blocked_count=len(blocked),
        blocked_rows=blocked_rows,
    )
