from __future__ import annotations

import sqlite3


def build_house_quality_metrics(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    skipped_count: int,
    skip_reasons: dict[str, int],
) -> dict:
    normalized_rows = conn.execute(
        """
        SELECT exclusion_reason_code, include_in_signal, ticker, entity_key
        FROM normalized_transactions
        WHERE source = 'congress' AND run_id = ?
        """,
        (run_id,),
    ).fetchall()
    scored_count = int(
        conn.execute(
            "SELECT COUNT(*) AS c FROM signal_results WHERE source = 'congress' AND run_id = ?",
            (run_id,),
        ).fetchone()["c"]
    )

    exclusion_counts: dict[str, int] = {}
    unresolved_count = 0
    resolved_entity_count = 0
    included_count = 0
    for row in normalized_rows:
        code = row["exclusion_reason_code"]
        if code:
            exclusion_counts[code] = exclusion_counts.get(code, 0) + 1
        if row["include_in_signal"]:
            included_count += 1
        if row["entity_key"] and row["ticker"]:
            resolved_entity_count += 1
        else:
            unresolved_count += 1

    normalized_count = len(normalized_rows)
    scored_signal_rate = (scored_count / normalized_count) if normalized_count else 0.0
    resolved_entity_rate = (resolved_entity_count / normalized_count) if normalized_count else 0.0
    included_rate = (included_count / normalized_count) if normalized_count else 0.0

    return {
        "run_id": run_id,
        "normalized_count": normalized_count,
        "scored_result_count": scored_count,
        "included_count": included_count,
        "unresolved_count": unresolved_count,
        "resolved_entity_count": resolved_entity_count,
        "scored_signal_rate": round(scored_signal_rate, 4),
        "resolved_entity_rate": round(resolved_entity_rate, 4),
        "included_rate": round(included_rate, 4),
        "skipped_count": skipped_count,
        "skip_reasons": dict(sorted(skip_reasons.items())),
        "exclusion_reason_counts": dict(sorted(exclusion_counts.items())),
    }
