from __future__ import annotations

from pathlib import Path

from signals.congress.house_parser import PaperHouseFilingParser
from signals.congress.resolution import resolve_transaction
from signals.core.enums import ResolutionStatus
from signals.core.resolution import resolve_entity


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "house_paper_cases"


def _read_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text()


def test_house_paper_amendment_fixture_is_recovered_precisely():
    parser = PaperHouseFilingParser()
    filing = parser.parse_ocr_text(_read_fixture("amendment_letter.txt"), Path("8221334.pdf"), 1)

    assert [txn.asset_name for txn in filing.transactions] == [
        "Dynatrace, Inc.",
        "Humana Inc.",
        "Synopsys, Inc.",
    ]
    assert [txn.transaction_type for txn in filing.transactions] == ["sale", "sale", "purchase"]
    assert [txn.amount_min for txn in filing.transactions] == [1001, 1001, 1001]
    assert [txn.amount_max for txn in filing.transactions] == [15000, 15000, 15000]


def test_house_paper_nothing_to_report_fixture_produces_no_transactions():
    parser = PaperHouseFilingParser()
    filing = parser.parse_ocr_text(_read_fixture("nothing_to_report.txt"), Path("8221310.pdf"), 1)

    assert filing.transactions == []


def test_house_paper_recoverable_common_stock_fixture_resolves_real_entities_only():
    parser = PaperHouseFilingParser()
    filing = parser.parse_ocr_text(_read_fixture("recoverable_common_stock.txt"), Path("8221285.pdf"), 1)

    assert len(filing.transactions) == 4
    resolved = [
        resolve_entity(
            source="congress",
            source_record_id=f"fixture:{idx}",
            source_filing_id="fixture",
            ticker=txn.ticker,
            cik=None,
            issuer_name=txn.asset_name,
            instrument_type=txn.asset_type,
            run_id="run-fixture",
        )
        for idx, txn in enumerate(filing.transactions, start=1)
    ]

    assert [event.ticker for event in resolved] == ["WMT", "NVDA", "CL", "BLDR"]
    assert all(event.resolution_status == ResolutionStatus.RESOLVED.value for event in resolved)


def test_house_paper_non_signal_assets_fixture_stays_excluded():
    parser = PaperHouseFilingParser()
    filing = parser.parse_ocr_text(_read_fixture("non_signal_assets.txt"), Path("8222000.pdf"), 1)

    categories = [
        resolve_transaction(txn.asset_name, ticker=txn.ticker, asset_type_code=txn.asset_type)
        for txn in filing.transactions
    ]

    assert filing.transactions
    assert all(not item.include_in_signal for item in categories)


def test_house_paper_unrecoverable_noise_fixture_stays_unresolved_and_non_signal():
    parser = PaperHouseFilingParser()
    filing = parser.parse_ocr_text(_read_fixture("unrecoverable_noise.txt"), Path("8222999.pdf"), 1)

    assert len(filing.transactions) == 1
    event = resolve_entity(
        source="congress",
        source_record_id="noise-1",
        source_filing_id="noise",
        ticker=None,
        cik=None,
        issuer_name=filing.transactions[0].asset_name,
        instrument_type=filing.transactions[0].asset_type,
        run_id="run-noise",
    )
    assert event.resolution_status != ResolutionStatus.RESOLVED.value
    assert event.ticker is None


def test_house_paper_fixture_precision_does_not_resolve_noise_entities():
    parser = PaperHouseFilingParser()
    filing = parser.parse_ocr_text(_read_fixture("recoverable_common_stock.txt"), Path("8221285.pdf"), 1)

    fake_subjects = {"entity:x", "entity:sp", "entity:cap", "entity:classa"}
    for idx, txn in enumerate(filing.transactions, start=1):
        event = resolve_entity(
            source="congress",
            source_record_id=f"fixture-precision:{idx}",
            source_filing_id="fixture-precision",
            ticker=txn.ticker,
            cik=None,
            issuer_name=txn.asset_name,
            instrument_type=txn.asset_type,
            run_id="run-precision",
        )
        assert event.entity_key not in fake_subjects
