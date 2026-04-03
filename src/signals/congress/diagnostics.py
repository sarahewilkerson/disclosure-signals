from __future__ import annotations

import json
import sqlite3

from signals.core.resolution import normalize_entity_name


def build_house_quality_metrics(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    skipped_count: int,
    skip_reasons: dict[str, int],
) -> dict:
    normalized_rows = conn.execute(
        """
        SELECT exclusion_reason_code, include_in_signal, ticker, entity_key, issuer_name, provenance_payload
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
    unresolved_issuer_counts: dict[str, int] = {}
    unresolved_signal_like_issuer_counts: dict[str, int] = {}
    unresolved_non_signal_issuer_counts: dict[str, int] = {}
    recovered_issuer_counts: dict[str, int] = {}
    asset_category_counts: dict[str, int] = {}
    unresolved_asset_category_counts: dict[str, int] = {}
    non_signal_asset_category_counts: dict[str, int] = {}
    for row in normalized_rows:
        code = row["exclusion_reason_code"]
        asset_resolution = row["provenance_payload"]
        category = "<unknown>"
        if isinstance(asset_resolution, str):
            try:
                payload = json.loads(asset_resolution)
                category = payload.get("asset_resolution", {}).get("category") or "<unknown>"
            except Exception:
                category = "<unknown>"
        if code:
            exclusion_counts[code] = exclusion_counts.get(code, 0) + 1
        asset_category_counts[category] = asset_category_counts.get(category, 0) + 1
        if row["include_in_signal"]:
            included_count += 1
            issuer = row["issuer_name"] or "<unknown>"
            recovered_issuer_counts[issuer] = recovered_issuer_counts.get(issuer, 0) + 1
        if row["entity_key"] and row["ticker"]:
            resolved_entity_count += 1
        else:
            unresolved_count += 1
            issuer = row["issuer_name"] or "<unknown>"
            unresolved_issuer_counts[issuer] = unresolved_issuer_counts.get(issuer, 0) + 1
            unresolved_asset_category_counts[category] = unresolved_asset_category_counts.get(category, 0) + 1
            if code in {"MISSING_TICKER", "LOW_RESOLUTION_CONFIDENCE"}:
                unresolved_signal_like_issuer_counts[issuer] = unresolved_signal_like_issuer_counts.get(issuer, 0) + 1
            elif code == "NON_SIGNAL_ASSET":
                unresolved_non_signal_issuer_counts[issuer] = unresolved_non_signal_issuer_counts.get(issuer, 0) + 1
                non_signal_asset_category_counts[category] = non_signal_asset_category_counts.get(category, 0) + 1

    normalized_count = len(normalized_rows)
    scored_signal_rate = (scored_count / normalized_count) if normalized_count else 0.0
    resolved_entity_rate = (resolved_entity_count / normalized_count) if normalized_count else 0.0
    included_rate = (included_count / normalized_count) if normalized_count else 0.0

    top_unresolved_issuers = [
        {"issuer_name": issuer, "count": count}
        for issuer, count in sorted(unresolved_issuer_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]
    top_signal_like_unresolved_issuers = [
        {"issuer_name": issuer, "count": count}
        for issuer, count in sorted(unresolved_signal_like_issuer_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]
    top_non_signal_unresolved_issuers = [
        {"issuer_name": issuer, "count": count}
        for issuer, count in sorted(unresolved_non_signal_issuer_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]
    top_recovered_issuers = [
        {"issuer_name": issuer, "count": count}
        for issuer, count in sorted(recovered_issuer_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]

    scored_subjects = conn.execute(
        """
        SELECT subject_key, label, score, confidence, input_count
        FROM signal_results
        WHERE source = 'congress' AND run_id = ?
        ORDER BY subject_key
        """,
        (run_id,),
    ).fetchall()
    top_scored_subjects = [
        {
            "subject_key": row["subject_key"],
            "label": row["label"],
            "score": row["score"],
            "confidence": row["confidence"],
            "input_count": row["input_count"],
        }
        for row in scored_subjects[:10]
    ]

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
        "asset_category_counts": dict(sorted(asset_category_counts.items())),
        "unresolved_asset_category_counts": dict(sorted(unresolved_asset_category_counts.items())),
        "non_signal_asset_category_counts": dict(sorted(non_signal_asset_category_counts.items())),
        "top_unresolved_issuers": top_unresolved_issuers,
        "top_signal_like_unresolved_issuers": top_signal_like_unresolved_issuers,
        "top_non_signal_unresolved_issuers": top_non_signal_unresolved_issuers,
        "top_recovered_issuers": top_recovered_issuers,
        "top_scored_subjects": top_scored_subjects,
    }


def build_house_candidate_discovery(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    limit: int = 10,
) -> dict:
    rows = conn.execute(
        """
        SELECT issuer_name, exclusion_reason_code, provenance_payload
        FROM normalized_transactions
        WHERE source = 'congress'
          AND run_id = ?
          AND ticker IS NULL
          AND entity_key IS NULL
          AND exclusion_reason_code IN ('MISSING_TICKER', 'LOW_RESOLUTION_CONFIDENCE')
        """,
        (run_id,),
    ).fetchall()

    candidates: dict[str, dict] = {}
    for row in rows:
        issuer_name = row["issuer_name"] or "<unknown>"
        normalized_name = normalize_entity_name(issuer_name)
        if not normalized_name:
            continue
        category = "<unknown>"
        if isinstance(row["provenance_payload"], str):
            try:
                payload = json.loads(row["provenance_payload"])
                category = payload.get("asset_resolution", {}).get("category") or "<unknown>"
            except Exception:
                category = "<unknown>"
        entry = candidates.setdefault(
            normalized_name,
            {
                "normalized_name": normalized_name,
                "count": 0,
                "asset_categories": {},
                "raw_examples": [],
                "reason_codes": {},
            },
        )
        entry["count"] += 1
        entry["asset_categories"][category] = entry["asset_categories"].get(category, 0) + 1
        code = row["exclusion_reason_code"]
        entry["reason_codes"][code] = entry["reason_codes"].get(code, 0) + 1
        if issuer_name not in entry["raw_examples"] and len(entry["raw_examples"]) < 3:
            entry["raw_examples"].append(issuer_name)

    ordered = sorted(candidates.values(), key=lambda item: (-item["count"], item["normalized_name"]))[:limit]
    return {
        "run_id": run_id,
        "candidate_count": len(ordered),
        "candidates": [
            {
                **item,
                "asset_categories": dict(sorted(item["asset_categories"].items())),
                "reason_codes": dict(sorted(item["reason_codes"].items())),
            }
            for item in ordered
        ],
    }
