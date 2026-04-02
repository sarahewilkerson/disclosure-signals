from __future__ import annotations

import json
from sqlite3 import Connection

from signals.core.dto import CombinedResult, SignalResult
from signals.core.derived_db import fetch_combined_block_events


def load_signal_results(conn: Connection, source: str | None = None, run_id: str | None = None) -> list[SignalResult]:
    if source is None and run_id is None:
        rows = conn.execute("SELECT * FROM signal_results ORDER BY id").fetchall()
    elif source is None:
        rows = conn.execute("SELECT * FROM signal_results WHERE run_id = ? ORDER BY id", (run_id,)).fetchall()
    elif run_id is None:
        rows = conn.execute("SELECT * FROM signal_results WHERE source = ? ORDER BY id", (source,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM signal_results WHERE source = ? AND run_id = ? ORDER BY id",
            (source, run_id),
        ).fetchall()
    return [
        SignalResult(
            **{
                **{k: row[k] for k in SignalResult.__dataclass_fields__.keys()},
                "provenance_refs": json.loads(row["provenance_refs"]),
            }
        )
        for row in rows
    ]


def load_combined_results(conn: Connection, run_id: str | None = None) -> list[CombinedResult]:
    if run_id is None:
        rows = conn.execute("SELECT * FROM combined_results ORDER BY id").fetchall()
    else:
        rows = conn.execute("SELECT * FROM combined_results WHERE run_id = ? ORDER BY id", (run_id,)).fetchall()
    return [
        CombinedResult(
            **{
                **{k: row[k] for k in CombinedResult.__dataclass_fields__.keys()},
                "provenance_refs": json.loads(row["provenance_refs"]),
            }
        )
        for row in rows
    ]


def load_recent_runs(conn: Connection, source: str | None = None, limit: int = 10, run_id: str | None = None) -> list[dict]:
    if run_id is not None:
        rows = conn.execute("SELECT * FROM runs WHERE run_id = ? LIMIT 1", (run_id,)).fetchall()
    elif source is None:
        rows = conn.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM runs WHERE source = ? ORDER BY started_at DESC LIMIT ?",
            (source, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def load_combined_block_events(conn: Connection, run_id: str | None = None) -> list[dict]:
    return fetch_combined_block_events(conn, run_id=run_id)
