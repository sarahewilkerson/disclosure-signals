from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager

from signals.core.dto import CombinedBlockEvent, CombinedResult, EntityResolutionEvent, NormalizedTransaction, SignalResult
from signals.core.runs import RunRecord


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    run_type TEXT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    params_json TEXT NOT NULL,
    code_version TEXT NOT NULL,
    method_versions_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    summary_json TEXT
);

CREATE TABLE IF NOT EXISTS normalized_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    source_filing_id TEXT NOT NULL,
    actor_id TEXT,
    actor_name TEXT,
    actor_type TEXT NOT NULL,
    owner_type TEXT NOT NULL,
    entity_key TEXT,
    instrument_key TEXT,
    ticker TEXT,
    issuer_name TEXT,
    instrument_type TEXT,
    transaction_type TEXT NOT NULL,
    direction TEXT NOT NULL,
    execution_date TEXT,
    disclosure_date TEXT,
    amount_low REAL,
    amount_high REAL,
    amount_estimate REAL,
    currency TEXT,
    units_low REAL,
    units_high REAL,
    price_low REAL,
    price_high REAL,
    quality_score REAL NOT NULL,
    parse_confidence REAL,
    resolution_event_id TEXT,
    resolution_confidence REAL,
    resolution_method_version TEXT NOT NULL,
    include_in_signal INTEGER NOT NULL,
    exclusion_reason_code TEXT,
    exclusion_reason_detail TEXT,
    provenance_payload TEXT NOT NULL,
    normalization_method_version TEXT NOT NULL,
    run_id TEXT NOT NULL,
    UNIQUE(source_record_id, normalization_method_version)
);

CREATE TABLE IF NOT EXISTS signal_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    scope TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    score REAL NOT NULL,
    label TEXT NOT NULL,
    confidence REAL NOT NULL,
    as_of_date TEXT NOT NULL,
    lookback_window INTEGER NOT NULL,
    input_count INTEGER NOT NULL,
    included_count INTEGER NOT NULL,
    excluded_count INTEGER NOT NULL,
    explanation TEXT NOT NULL,
    method_version TEXT NOT NULL,
    code_version TEXT NOT NULL,
    run_id TEXT NOT NULL,
    provenance_refs TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    UNIQUE(source, subject_key, as_of_date, lookback_window, method_version, input_fingerprint)
);

CREATE TABLE IF NOT EXISTS combined_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    scope TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    score REAL NOT NULL,
    label TEXT NOT NULL,
    confidence REAL NOT NULL,
    as_of_date TEXT NOT NULL,
    lookback_window INTEGER NOT NULL,
    input_count INTEGER NOT NULL,
    included_count INTEGER NOT NULL,
    excluded_count INTEGER NOT NULL,
    explanation TEXT NOT NULL,
    method_version TEXT NOT NULL,
    code_version TEXT NOT NULL,
    run_id TEXT NOT NULL,
    provenance_refs TEXT NOT NULL,
    overlay_outcome TEXT,
    agreement_state TEXT,
    conflict_score REAL,
    insider_score REAL,
    congress_score REAL,
    insider_confidence REAL,
    congress_confidence REAL,
    entity_resolution_confidence REAL,
    combine_method_version TEXT NOT NULL,
    do_not_combine_reason_code TEXT,
    do_not_combine_reason_detail TEXT,
    combine_fingerprint TEXT NOT NULL,
    UNIQUE(subject_key, as_of_date, combine_method_version, combine_fingerprint)
);

CREATE TABLE IF NOT EXISTS entity_resolution_events (
    event_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    source_filing_id TEXT NOT NULL,
    entity_key TEXT,
    instrument_key TEXT,
    ticker TEXT,
    issuer_name TEXT,
    instrument_type TEXT,
    resolution_status TEXT NOT NULL,
    resolution_confidence REAL NOT NULL,
    evidence_payload TEXT NOT NULL,
    resolution_method_version TEXT NOT NULL,
    run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS combined_block_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    scope TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    lookback_window INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    overlay_outcome TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    reason_detail TEXT,
    insider_result_ref TEXT,
    congress_result_ref TEXT,
    insider_resolution_event_id TEXT,
    congress_resolution_event_id TEXT,
    combine_method_version TEXT NOT NULL,
    conflict_score REAL,
    provenance_refs TEXT NOT NULL
);
"""


@contextmanager
def get_connection(db_path: str):
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str) -> None:
    with get_connection(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        _ensure_column(conn, "normalized_transactions", "resolution_event_id", "TEXT")
        _ensure_column(conn, "combined_results", "overlay_outcome", "TEXT")
        _ensure_column(conn, "combined_results", "conflict_score", "REAL")


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def insert_run(conn: sqlite3.Connection, run: RunRecord) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO runs (
            run_id, run_type, source, status, params_json, code_version,
            method_versions_json, started_at, ended_at, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run.run_id,
            run.run_type,
            run.source,
            run.status,
            run.params_json,
            run.code_version,
            run.method_versions_json,
            run.started_at,
            run.ended_at,
            run.summary_json,
        ),
    )


def update_run_status(conn: sqlite3.Connection, run_id: str, status: str, ended_at: str, summary: dict | None = None) -> None:
    conn.execute(
        "UPDATE runs SET status = ?, ended_at = ?, summary_json = ? WHERE run_id = ?",
        (status, ended_at, json.dumps(summary or {}, sort_keys=True), run_id),
    )


def insert_normalized(conn: sqlite3.Connection, row: NormalizedTransaction) -> None:
    data = row.to_dict()
    data["include_in_signal"] = 1 if data["include_in_signal"] else 0
    data["provenance_payload"] = json.dumps(data["provenance_payload"], sort_keys=True)
    columns = ", ".join(data.keys())
    placeholders = ", ".join(":" + k for k in data.keys())
    conn.execute(f"INSERT OR REPLACE INTO normalized_transactions ({columns}) VALUES ({placeholders})", data)


def insert_signal_result(conn: sqlite3.Connection, row: SignalResult, input_fingerprint: str) -> None:
    data = row.to_dict()
    data["provenance_refs"] = json.dumps(data["provenance_refs"], sort_keys=True)
    data["input_fingerprint"] = input_fingerprint
    columns = ", ".join(data.keys())
    placeholders = ", ".join(":" + k for k in data.keys())
    conn.execute(f"INSERT OR REPLACE INTO signal_results ({columns}) VALUES ({placeholders})", data)


def insert_combined_result(conn: sqlite3.Connection, row: CombinedResult, combine_fingerprint: str) -> None:
    data = row.to_dict()
    data["provenance_refs"] = json.dumps(data["provenance_refs"], sort_keys=True)
    data["combine_fingerprint"] = combine_fingerprint
    columns = ", ".join(data.keys())
    placeholders = ", ".join(":" + k for k in data.keys())
    conn.execute(f"INSERT OR REPLACE INTO combined_results ({columns}) VALUES ({placeholders})", data)


def insert_resolution_event(conn: sqlite3.Connection, row: EntityResolutionEvent) -> None:
    data = row.to_dict()
    data["evidence_payload"] = json.dumps(data["evidence_payload"], sort_keys=True)
    columns = ", ".join(data.keys())
    placeholders = ", ".join(":" + k for k in data.keys())
    conn.execute(f"INSERT OR REPLACE INTO entity_resolution_events ({columns}) VALUES ({placeholders})", data)


def insert_combined_block_event(conn: sqlite3.Connection, row: CombinedBlockEvent) -> None:
    data = row.to_dict()
    data["insider_result_ref"] = json.dumps(data["insider_result_ref"], sort_keys=True) if data["insider_result_ref"] is not None else None
    data["congress_result_ref"] = json.dumps(data["congress_result_ref"], sort_keys=True) if data["congress_result_ref"] is not None else None
    data["provenance_refs"] = json.dumps(data["provenance_refs"], sort_keys=True)
    columns = ", ".join(data.keys())
    placeholders = ", ".join(":" + k for k in data.keys())
    conn.execute(f"INSERT INTO combined_block_events ({columns}) VALUES ({placeholders})", data)


def fetch_all(conn: sqlite3.Connection, table: str) -> list[dict]:
    return [dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY id" if table != "runs" else "SELECT * FROM runs ORDER BY started_at")]


def count_rows(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"])


def fetch_signal_results_by_source(conn: sqlite3.Connection, source: str) -> list[dict]:
    return [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM signal_results WHERE source = ? ORDER BY id",
            (source,),
        )
    ]


def fetch_failed_runs(conn: sqlite3.Connection) -> list[dict]:
    return [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM runs WHERE status = 'FAILED' ORDER BY started_at DESC"
        )
    ]


def fetch_resolution_events_by_ids(conn: sqlite3.Connection, event_ids: list[str]) -> dict[str, EntityResolutionEvent]:
    if not event_ids:
        return {}
    placeholders = ", ".join("?" for _ in event_ids)
    return {
        row["event_id"]: EntityResolutionEvent(
            **{
                **{k: row[k] for k in EntityResolutionEvent.__dataclass_fields__.keys()},
                "evidence_payload": json.loads(row["evidence_payload"]),
            }
        )
        for row in conn.execute(
            f"SELECT * FROM entity_resolution_events WHERE event_id IN ({placeholders})",
            event_ids,
        ).fetchall()
    }


def fetch_combined_block_events(conn: sqlite3.Connection, run_id: str | None = None) -> list[dict]:
    if run_id is None:
        rows = conn.execute("SELECT * FROM combined_block_events ORDER BY id").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM combined_block_events WHERE run_id = ? ORDER BY id",
            (run_id,),
        ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        if item["insider_result_ref"]:
            item["insider_result_ref"] = json.loads(item["insider_result_ref"])
        if item["congress_result_ref"]:
            item["congress_result_ref"] = json.loads(item["congress_result_ref"])
        item["provenance_refs"] = json.loads(item["provenance_refs"])
        result.append(item)
    return result
