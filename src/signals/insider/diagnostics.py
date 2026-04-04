from __future__ import annotations

import sqlite3

from signals.core.resolution import normalize_entity_name


def build_insider_candidate_discovery(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    limit: int = 10,
) -> dict:
    rows = conn.execute(
        """
        SELECT issuer_name, source_filing_id, exclusion_reason_code, instrument_type
        FROM normalized_transactions
        WHERE source = 'insider'
          AND run_id = ?
          AND ticker IS NULL
          AND entity_key LIKE 'cik:%'
          AND include_in_signal = 1
        ORDER BY source_filing_id, source_record_id
        """,
        (run_id,),
    ).fetchall()

    candidates: dict[str, dict] = {}
    for row in rows:
        issuer_name = row["issuer_name"] or "<unknown>"
        normalized_name = normalize_entity_name(issuer_name)
        if not normalized_name:
            continue
        entry = candidates.setdefault(
            normalized_name,
            {
                "normalized_name": normalized_name,
                "count": 0,
                "raw_examples": [],
                "filing_ids": [],
                "instrument_types": {},
                "reason_codes": {},
            },
        )
        entry["count"] += 1
        instrument_type = row["instrument_type"] or "<unknown>"
        entry["instrument_types"][instrument_type] = entry["instrument_types"].get(instrument_type, 0) + 1
        code = row["exclusion_reason_code"] or "<included>"
        entry["reason_codes"][code] = entry["reason_codes"].get(code, 0) + 1
        if issuer_name not in entry["raw_examples"] and len(entry["raw_examples"]) < 3:
            entry["raw_examples"].append(issuer_name)
        filing_id = row["source_filing_id"]
        if filing_id not in entry["filing_ids"] and len(entry["filing_ids"]) < 3:
            entry["filing_ids"].append(filing_id)

    ordered = sorted(candidates.values(), key=lambda item: (-item["count"], item["normalized_name"]))[:limit]
    return {
        "run_id": run_id,
        "candidate_count": len(ordered),
        "candidates": [
            {
                **item,
                "instrument_types": dict(sorted(item["instrument_types"].items())),
                "reason_codes": dict(sorted(item["reason_codes"].items())),
            }
            for item in ordered
        ],
    }
