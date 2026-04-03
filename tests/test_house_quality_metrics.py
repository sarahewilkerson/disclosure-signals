from __future__ import annotations

from signals.congress.diagnostics import build_house_quality_metrics
from signals.core.derived_db import get_connection, init_db, insert_normalized, insert_run, insert_signal_result
from signals.core.dto import NormalizedTransaction, SignalResult
from signals.core.runs import make_run


def test_house_quality_metrics_reports_recovery_and_precision(tmp_path):
    db_path = tmp_path / "derived.db"
    init_db(str(db_path))
    run = make_run(
        "direct_house_score",
        "congress",
        "test-sha",
        {},
        {"normalization": "n", "resolution": "r", "score": "s"},
    )

    with get_connection(str(db_path)) as conn:
        insert_run(conn, run)
        insert_normalized(
            conn,
            NormalizedTransaction(
                source="congress",
                source_record_id="row-1",
                source_filing_id="filing-1",
                actor_id="a1",
                actor_name="Member A",
                actor_type="member",
                owner_type="self",
                entity_key="entity:walmart",
                instrument_key="instrument:wmt:common",
                ticker="WMT",
                issuer_name="Walmart Inc.",
                instrument_type=None,
                transaction_type="purchase",
                direction="BUY",
                execution_date="2026-03-01",
                disclosure_date="2026-03-02",
                amount_low=1001.0,
                amount_high=15000.0,
                amount_estimate=8000.5,
                currency="USD",
                units_low=None,
                units_high=None,
                price_low=None,
                price_high=None,
                quality_score=1.0,
                parse_confidence=1.0,
                resolution_event_id="evt-1",
                resolution_confidence=0.9,
                resolution_method_version="r",
                include_in_signal=True,
                exclusion_reason_code=None,
                exclusion_reason_detail=None,
                provenance_payload={},
                normalization_method_version="n",
                run_id=run.run_id,
            ),
        )
        insert_normalized(
            conn,
            NormalizedTransaction(
                source="congress",
                source_record_id="row-2",
                source_filing_id="filing-2",
                actor_id="a2",
                actor_name="Member B",
                actor_type="member",
                owner_type="self",
                entity_key=None,
                instrument_key=None,
                ticker=None,
                issuer_name="US Treasury Note 4%",
                instrument_type="GS",
                transaction_type="purchase",
                direction="BUY",
                execution_date="2026-03-01",
                disclosure_date="2026-03-02",
                amount_low=50001.0,
                amount_high=100000.0,
                amount_estimate=75000.5,
                currency="USD",
                units_low=None,
                units_high=None,
                price_low=None,
                price_high=None,
                quality_score=1.0,
                parse_confidence=1.0,
                resolution_event_id="evt-2",
                resolution_confidence=0.0,
                resolution_method_version="r",
                include_in_signal=False,
                exclusion_reason_code="NON_SIGNAL_ASSET",
                exclusion_reason_detail=None,
                provenance_payload={},
                normalization_method_version="n",
                run_id=run.run_id,
            ),
        )
        insert_normalized(
            conn,
            NormalizedTransaction(
                source="congress",
                source_record_id="row-3",
                source_filing_id="filing-3",
                actor_id="a3",
                actor_name="Member C",
                actor_type="member",
                owner_type="self",
                entity_key=None,
                instrument_key=None,
                ticker=None,
                issuer_name="pc _ | USD",
                instrument_type=None,
                transaction_type="purchase",
                direction="BUY",
                execution_date="2026-03-01",
                disclosure_date="2026-03-02",
                amount_low=1001.0,
                amount_high=15000.0,
                amount_estimate=8000.5,
                currency="USD",
                units_low=None,
                units_high=None,
                price_low=None,
                price_high=None,
                quality_score=1.0,
                parse_confidence=1.0,
                resolution_event_id="evt-3",
                resolution_confidence=0.0,
                resolution_method_version="r",
                include_in_signal=False,
                exclusion_reason_code="NON_SIGNAL_ASSET",
                exclusion_reason_detail=None,
                provenance_payload={},
                normalization_method_version="n",
                run_id=run.run_id,
            ),
        )
        insert_signal_result(
            conn,
            SignalResult(
                source="congress",
                scope="entity",
                subject_key="entity:wmt",
                score=1.0,
                label="bullish",
                confidence=0.5,
                as_of_date="2026-04-02",
                lookback_window=90,
                input_count=1,
                included_count=1,
                excluded_count=0,
                explanation="1 transaction",
                method_version="s",
                code_version="test-sha",
                run_id=run.run_id,
                provenance_refs={},
            ),
            "fp-1",
        )

        payload = build_house_quality_metrics(
            conn,
            run_id=run.run_id,
            skipped_count=2,
            skip_reasons={"nothing_to_report": 1, "ocr_failed": 1},
        )

    assert payload["normalized_count"] == 3
    assert payload["scored_result_count"] == 1
    assert payload["included_count"] == 1
    assert payload["resolved_entity_count"] == 1
    assert payload["unresolved_count"] == 2
    assert payload["scored_signal_rate"] == 0.3333
    assert payload["resolved_entity_rate"] == 0.3333
    assert payload["included_rate"] == 0.3333
    assert payload["skip_reasons"] == {"nothing_to_report": 1, "ocr_failed": 1}
    assert payload["exclusion_reason_counts"] == {"NON_SIGNAL_ASSET": 2}
    assert payload["top_unresolved_issuers"] == [
        {"issuer_name": "US Treasury Note 4%", "count": 1},
        {"issuer_name": "pc _ | USD", "count": 1},
    ]
    assert payload["top_recovered_issuers"] == [
        {"issuer_name": "Walmart Inc.", "count": 1},
    ]
    assert payload["top_scored_subjects"] == [
        {
            "subject_key": "entity:wmt",
            "label": "bullish",
            "score": 1.0,
            "confidence": 0.5,
            "input_count": 1,
        }
    ]
