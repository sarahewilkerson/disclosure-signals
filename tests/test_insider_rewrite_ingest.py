from signals.insider.ingest import load_universe_csv, resolve_cik


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
