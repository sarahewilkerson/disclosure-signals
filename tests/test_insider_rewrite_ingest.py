import json

from signals.insider.ingest import ingest_universe_direct, load_universe_csv, resolve_cik


def test_resolve_cik_variants():
    tickers_map = {
        "BRK-B": {"cik_str": "0001067983"},
        "AAPL": {"cik_str": "0000320193"},
    }
    assert resolve_cik("AAPL", tickers_map) == "0000320193"
    assert resolve_cik("BRK.B", tickers_map) == "0001067983"


def test_load_universe_csv_resolves_ciks(tmp_path):
    csv_path = tmp_path / "universe.csv"
    csv_path.write_text("ticker,company_name,sector\nAAPL,Apple Inc.,Information Technology\n")
    companies = load_universe_csv(str(csv_path), {"AAPL": {"cik_str": "0000320193"}})
    assert companies == [
        {
            "ticker": "AAPL",
            "company_name": "Apple Inc.",
            "sector": "Information Technology",
            "cik": "0000320193",
        }
    ]


def test_ingest_universe_direct_resumes_completed_companies(tmp_path, monkeypatch):
    csv_path = tmp_path / "universe.csv"
    csv_path.write_text("ticker,company_name,sector\nAAPL,Apple Inc.,Information Technology\nMSFT,Microsoft Corporation,Information Technology\n")

    class _Client:
        def __init__(self, user_agent):
            self.user_agent = user_agent

    monkeypatch.setattr("signals.insider.ingest.DirectEdgarClient", _Client)
    monkeypatch.setattr(
        "signals.insider.ingest.load_company_tickers_map",
        lambda client, cache_path: {
            "AAPL": {"cik_str": "0000320193"},
            "MSFT": {"cik_str": "0000789019"},
        },
    )
    monkeypatch.setattr(
        "signals.insider.ingest.search_form4_filings",
        lambda client, cik, start_date=None, end_date=None, max_results=200: [{"accession_number": f"{cik}-000001"}],
    )
    monkeypatch.setattr(
        "signals.insider.ingest.resolve_filing_xml_url",
        lambda client, accession, issuer_cik=None, filing_href=None: (f"https://example.test/{accession}.xml", issuer_cik),
    )
    monkeypatch.setattr(
        "signals.insider.ingest.download_filing_xml",
        lambda client, xml_url, accession_number, filings_cache_dir: str(filings_cache_dir / f"{accession_number.replace('-', '_')}.xml"),
    )

    progress_events = []
    first = ingest_universe_direct(
        csv_path=str(csv_path),
        user_agent="DisclosureSignals/1.0 (test@example.org)",
        cache_dir=str(tmp_path / "cache"),
        progress_callback=progress_events.append,
    )
    second = ingest_universe_direct(
        csv_path=str(csv_path),
        user_agent="DisclosureSignals/1.0 (test@example.org)",
        cache_dir=str(tmp_path / "cache"),
        progress_callback=progress_events.append,
    )

    assert first["companies_completed"] == 2
    assert first["total_new_filings"] == 2
    assert second["companies_completed"] == 2
    assert second["resumed_companies"] == 2
    assert second["remaining_companies"] == 0

    state = json.loads((tmp_path / "cache" / "insider_ingest_state.json").read_text())
    assert state["companies_completed"] == 2
    assert sorted(state["completed_companies"]) == ["0000320193", "0000789019"]
    assert any(event["event"] == "company_skipped" for event in progress_events)
