from __future__ import annotations

from signals.core.derived_db import get_connection, init_db, insert_normalized, insert_run
from signals.core.dto import NormalizedTransaction
from signals.core.runs import make_run
from signals.insider.diagnostics import build_insider_candidate_discovery


def test_insider_candidate_discovery_groups_cik_only_rows(tmp_path):
    db_path = tmp_path / "derived.db"
    init_db(str(db_path))
    run = make_run(
        "direct_xml_score",
        "insider",
        "test-sha",
        {},
        {"normalization": "n", "resolution": "r", "score": "s"},
    )

    with get_connection(str(db_path)) as conn:
        insert_run(conn, run)
        for idx, issuer_name in enumerate(["Acme Corp Common Stock", "Acme Corp CMN", "Mystery Holdings"], start=1):
            insert_normalized(
                conn,
                NormalizedTransaction(
                    source="insider",
                    source_record_id=f"row-{idx}",
                    source_filing_id=f"filing-{idx}",
                    actor_id="owner",
                    actor_name="Owner",
                    actor_type="officer",
                    owner_type="direct",
                    entity_key="cik:0000001" if idx < 3 else "cik:0000002",
                    instrument_key=None,
                    ticker=None,
                    issuer_name=issuer_name,
                    instrument_type="Common Stock",
                    transaction_type="purchase",
                    direction="BUY",
                    execution_date="2026-03-01",
                    disclosure_date="2026-03-02",
                    amount_low=1000.0,
                    amount_high=1000.0,
                    amount_estimate=1000.0,
                    currency="USD",
                    units_low=10.0,
                    units_high=10.0,
                    price_low=100.0,
                    price_high=100.0,
                    quality_score=1.0,
                    parse_confidence=1.0,
                    resolution_event_id=f"evt-{idx}",
                    resolution_confidence=0.0,
                    resolution_method_version="r",
                    include_in_signal=True,
                    exclusion_reason_code=None,
                    exclusion_reason_detail=None,
                    provenance_payload={},
                    normalization_method_version="n",
                    run_id=run.run_id,
                ),
            )

        payload = build_insider_candidate_discovery(conn, run_id=run.run_id, limit=10)

    assert payload["run_id"] == run.run_id
    assert payload["candidate_count"] == 2
    assert payload["candidates"] == [
        {
            "normalized_name": "acme corp",
            "count": 2,
            "raw_examples": ["Acme Corp Common Stock", "Acme Corp CMN"],
            "filing_ids": ["filing-1", "filing-2"],
            "instrument_types": {"Common Stock": 2},
            "reason_codes": {"<included>": 2},
        },
        {
            "normalized_name": "mystery holdings",
            "count": 1,
            "raw_examples": ["Mystery Holdings"],
            "filing_ids": ["filing-3"],
            "instrument_types": {"Common Stock": 1},
            "reason_codes": {"<included>": 1},
        },
    ]


def test_insider_candidate_discovery_excludes_role_filtered_rows(tmp_path):
    db_path = tmp_path / "derived.db"
    init_db(str(db_path))
    run = make_run(
        "direct_xml_score",
        "insider",
        "test-sha",
        {},
        {"normalization": "n", "resolution": "r", "score": "s"},
    )

    with get_connection(str(db_path)) as conn:
        insert_run(conn, run)
        insert_normalized(
            conn,
            NormalizedTransaction(
                source="insider",
                source_record_id="row-1",
                source_filing_id="filing-1",
                actor_id="owner",
                actor_name="Owner",
                actor_type="officer",
                owner_type="direct",
                entity_key="cik:0000001",
                instrument_key=None,
                ticker=None,
                issuer_name="Excluded Corp",
                instrument_type="Common Stock",
                transaction_type="purchase",
                direction="BUY",
                execution_date="2026-03-01",
                disclosure_date="2026-03-02",
                amount_low=1000.0,
                amount_high=1000.0,
                amount_estimate=1000.0,
                currency="USD",
                units_low=10.0,
                units_high=10.0,
                price_low=100.0,
                price_high=100.0,
                quality_score=1.0,
                parse_confidence=1.0,
                resolution_event_id="evt-1",
                resolution_confidence=0.0,
                resolution_method_version="r",
                include_in_signal=False,
                exclusion_reason_code="ENTITY_ROLE_EXCLUDED",
                exclusion_reason_detail="Role excluded",
                provenance_payload={},
                normalization_method_version="n",
                run_id=run.run_id,
            ),
        )

        payload = build_insider_candidate_discovery(conn, run_id=run.run_id, limit=10)

    assert payload == {
        "run_id": run.run_id,
        "candidate_count": 0,
        "candidates": [],
    }
