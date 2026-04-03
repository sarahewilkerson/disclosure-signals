from __future__ import annotations

from signals.core.resolution import resolve_entity


def test_resolve_entity_uses_csv_backed_indexes():
    event = resolve_entity(
        source="insider",
        source_record_id="row-1",
        source_filing_id="filing-1",
        ticker="AAPL",
        cik=None,
        issuer_name="Apple Inc.",
        instrument_type="ST",
        run_id="run-1",
    )

    assert event.entity_key == "entity:apple"
    assert event.instrument_key == "instrument:aapl:common"
    assert event.ticker == "AAPL"
    assert event.resolution_confidence > 0.9


def test_resolve_entity_falls_back_to_name_aliases():
    event = resolve_entity(
        source="congress",
        source_record_id="row-2",
        source_filing_id="filing-2",
        ticker=None,
        cik=None,
        issuer_name="Microsoft Corp",
        instrument_type="ST",
        run_id="run-2",
    )

    assert event.entity_key == "entity:microsoft"
    assert event.ticker == "MSFT"


def test_resolve_entity_normalizes_dirty_ocr_common_stock_names():
    event = resolve_entity(
        source="congress",
        source_record_id="row-3",
        source_filing_id="filing-3",
        ticker=None,
        cik=None,
        issuer_name="{WALMARTINCCMN — -",
        instrument_type=None,
        run_id="run-3",
    )

    assert event.entity_key == "entity:walmart"
    assert event.ticker == "WMT"
