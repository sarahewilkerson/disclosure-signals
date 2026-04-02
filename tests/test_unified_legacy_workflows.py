import json
import os
import sqlite3
import subprocess
from pathlib import Path

from signals.congress.legacy_bridge import db_module as congress_db_module
from signals.insider.legacy_bridge import db_module as insider_db_module


def _run_cli(repo_root: Path, args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(repo_root / ".venv" / "bin" / "python"), "-m", "signals.cli", *args],
        cwd=repo_root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def _setup_insider_db(db_path: Path) -> None:
    db = insider_db_module()
    db.init_db(str(db_path))
    with db.get_connection(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO companies (cik, ticker, company_name, fortune_rank, revenue, sector, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("0000320193", "AAPL", "Apple Inc.", 1, 100.0, "Technology", "2026-04-02"),
        )
        for idx in range(1, 6):
            accession = f"0000320193-24-00000{idx}"
            owner_cik = f"000000000{idx}"
            owner_name = f"Executive {idx}"
            conn.execute(
                """
                INSERT INTO filings (
                    accession_number, cik_issuer, cik_owner, owner_name, officer_title,
                    is_officer, is_director, is_ten_pct_owner, is_other, is_amendment,
                    amendment_type, period_of_report, aff10b5one, additional_owners,
                    filing_date, xml_url, raw_xml_path, parsed_at, parse_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    accession,
                    "0000320193",
                    owner_cik,
                    owner_name,
                    "Chief Executive Officer",
                    1,
                    0,
                    0,
                    0,
                    0,
                    None,
                    f"2026-03-2{idx}",
                    0,
                    None,
                    f"2026-03-2{idx}",
                    "https://example.invalid/form4.xml",
                    f"cache/example-{idx}.xml",
                    "2026-03-21T12:00:00",
                    None,
                ),
            )
            conn.execute(
                """
                INSERT INTO transactions (
                    accession_number, cik_issuer, cik_owner, owner_name, officer_title,
                    security_title, transaction_date, transaction_code, equity_swap,
                    shares, price_per_share, total_value, shares_after,
                    ownership_nature, indirect_entity, is_derivative, underlying_security,
                    footnotes, role_class, transaction_class, is_likely_planned,
                    is_discretionary, pct_holdings_changed, include_in_signal, exclusion_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    accession,
                    "0000320193",
                    owner_cik,
                    owner_name,
                    "Chief Executive Officer",
                    "Common Stock",
                    f"2026-03-2{idx}",
                    "P",
                    0,
                    100.0 + idx,
                    200.0,
                    20000.0 + idx,
                    1000.0 + idx,
                    "D",
                    None,
                    0,
                    None,
                    None,
                    "ceo",
                    "open_market_buy",
                    0,
                    1,
                    0.10,
                    1,
                    None,
                ),
            )


def _setup_congress_db(db_path: Path) -> None:
    db = congress_db_module()
    db.init_db(str(db_path))
    with db.get_connection(str(db_path)) as conn:
        for idx in range(1, 41):
            member_id = f"M{idx:04d}"
            filing_id = f"senate-{idx}"
            conn.execute(
                """
                INSERT INTO members (bioguide_id, name, chamber, state, party, in_office, committees, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    member_id,
                    f"Sen. Example {idx}",
                    "senate",
                    "CA",
                    "I",
                    1,
                    None,
                    "2026-04-02T00:00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO filings (
                    filing_id, bioguide_id, chamber, filer_name, filing_type, disclosure_date,
                    source_url, source_format, source_hash, raw_path, parsed_at, parse_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    filing_id,
                    member_id,
                    "senate",
                    f"Sen. Example {idx}",
                    "PTR",
                    "2026-03-22",
                    "https://example.invalid/ptr",
                    "html",
                    f"hash-{idx}",
                    f"cache/ptr-{idx}.html",
                    "2026-03-22T12:00:00",
                    None,
                ),
            )
            conn.execute(
                """
                INSERT INTO transactions (
                    filing_id, bioguide_id, owner_type, asset_name_raw, asset_type,
                    resolved_ticker, resolved_company, resolution_method, resolution_confidence,
                    transaction_type, execution_date, disclosure_date, ingestion_date,
                    disclosure_lag_days, amount_min, amount_max, amount_code, amount_midpoint,
                    include_in_signal, exclusion_reason, page_number, extraction_confidence
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    filing_id,
                    member_id,
                    "self",
                    "Apple Inc.",
                    "stock",
                    "AAPL",
                    "Apple Inc.",
                    "manual",
                    1.0,
                    "purchase",
                    "2026-03-21",
                    "2026-03-22",
                    "2026-03-22",
                    1,
                    1001,
                    15000,
                    None,
                    8000.5,
                    1,
                    None,
                    1,
                    1.0,
                ),
            )


def test_unified_cli_real_legacy_workflows(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    derived_db = tmp_path / "derived.db"
    insider_db = tmp_path / "insider.db"
    congress_db = tmp_path / "congress.db"
    _setup_insider_db(insider_db)
    _setup_congress_db(congress_db)

    env = {
        **os.environ,
        "SKIP_CONFIG_VALIDATION": "1",
        "CPPI_MIN_TRANSACTIONS": "1",
        "CPPI_MIN_MEMBERS": "1",
    }

    insider_score = _run_cli(
        repo_root,
        [
            "--db", str(derived_db),
            "--insider-legacy-db", str(insider_db),
            "--congress-legacy-db", str(congress_db),
            "--format", "json",
            "insider", "score",
            "--date", "2026-04-02",
        ],
        env,
    )
    congress_score = _run_cli(
        repo_root,
        [
            "--db", str(derived_db),
            "--insider-legacy-db", str(insider_db),
            "--congress-legacy-db", str(congress_db),
            "--format", "json",
            "congress", "score",
            "--window", "90",
        ],
        env,
    )
    combined_build = _run_cli(
        repo_root,
        [
            "--db", str(derived_db),
            "--insider-legacy-db", str(insider_db),
            "--congress-legacy-db", str(congress_db),
            "--format", "json",
            "combined", "build",
            "--window", "90",
        ],
        env,
    )
    insider_report = _run_cli(
        repo_root,
        [
            "--db", str(derived_db),
            "--format", "json",
            "insider", "report",
        ],
        env,
    )
    combined_report = _run_cli(
        repo_root,
        [
            "--db", str(derived_db),
            "--format", "json",
            "combined", "report",
        ],
        env,
    )
    unified_run = _run_cli(
        repo_root,
        [
            "--db", str(tmp_path / "derived-run.db"),
            "--insider-legacy-db", str(insider_db),
            "--congress-legacy-db", str(congress_db),
            "--format", "json",
            "--artifacts-dir", str(tmp_path / "artifacts"),
            "run",
            "--date", "2026-04-02",
            "--window", "90",
        ],
        env,
    )

    insider_payload = json.loads(insider_score.stdout)
    congress_payload = json.loads(congress_score.stdout)
    combined_payload = json.loads(combined_build.stdout)
    insider_report_payload = json.loads(insider_report.stdout)
    combined_report_payload = json.loads(combined_report.stdout)
    unified_payload = json.loads(unified_run.stdout)

    assert insider_payload["imported_result_count"] >= 1
    assert congress_payload["imported_result_count"] >= 1
    assert combined_payload["combined_count"] >= 1
    assert any(row["subject_key"] == "entity:aapl" for row in insider_report_payload["source_results"])
    assert any(row["subject_key"] == "entity:aapl" for row in combined_report_payload["combined_results"])
    assert unified_payload["combined"]["combined_count"] >= 1
    assert Path(unified_payload["artifact_paths"]["run_summary"]).exists()
    assert Path(unified_payload["artifact_paths"]["parity_report"]).exists()
    assert Path(unified_payload["artifact_paths"]["exclusion_histogram"]).exists()
    assert Path(unified_payload["artifact_paths"]["unresolved_entities"]).exists()
    assert Path(unified_payload["artifact_paths"]["combined_block_report"]).exists()

    parity_payload = json.loads(Path(unified_payload["artifact_paths"]["parity_report"]).read_text())
    run_summary_payload = json.loads(Path(unified_payload["artifact_paths"]["run_summary"]).read_text())
    assert parity_payload["structural_ok"] is True
    assert parity_payload["legacy_parity"]["insider"]["result_match"] is True
    assert parity_payload["legacy_parity"]["congress"]["normalized_match"] is True
    assert run_summary_payload["source_counts"]["combined"] >= 1

    with sqlite3.connect(derived_db) as conn:
        run_count = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        normalized_count = conn.execute("SELECT COUNT(*) FROM normalized_transactions").fetchone()[0]
        result_count = conn.execute("SELECT COUNT(*) FROM signal_results").fetchone()[0]
        combined_count = conn.execute("SELECT COUNT(*) FROM combined_results").fetchone()[0]
    assert run_count >= 3
    assert normalized_count >= 3
    assert result_count >= 2
    assert combined_count >= 1


def test_unified_cli_congress_init_passthrough(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    congress_db = tmp_path / "cppi.db"
    env = {
        **os.environ,
        "SKIP_CONFIG_VALIDATION": "1",
    }
    result = _run_cli(
        repo_root,
        [
            "--congress-legacy-db", str(congress_db),
            "--format", "json",
            "congress", "init",
        ],
        env,
    )
    payload = json.loads(result.stdout)
    assert congress_db.exists()
    assert "Database initialized successfully" in payload["stderr"]


def test_status_json_includes_recent_runs_and_source_counts(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    derived_db = tmp_path / "derived.db"
    insider_db = tmp_path / "insider.db"
    congress_db = tmp_path / "congress.db"
    _setup_insider_db(insider_db)
    _setup_congress_db(congress_db)
    env = {
        **os.environ,
        "SKIP_CONFIG_VALIDATION": "1",
        "CPPI_MIN_TRANSACTIONS": "1",
        "CPPI_MIN_MEMBERS": "1",
    }
    _run_cli(
        repo_root,
        [
            "--db", str(derived_db),
            "--insider-legacy-db", str(insider_db),
            "--congress-legacy-db", str(congress_db),
            "--format", "json",
            "run",
            "--date", "2026-04-02",
            "--window", "90",
        ],
        env,
    )
    status = _run_cli(
        repo_root,
        [
            "--db", str(derived_db),
            "--format", "json",
            "status",
        ],
        env,
    )
    payload = json.loads(status.stdout)
    assert payload["source_result_counts"]["insider"] >= 1
    assert payload["source_result_counts"]["congress"] >= 1
    assert payload["recent_runs"]
