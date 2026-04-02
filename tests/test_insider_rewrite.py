from datetime import datetime
from pathlib import Path

from signals.core.derived_db import get_connection, init_db
from signals.insider.direct_service import run_direct_xml_into_derived


def test_direct_insider_xml_into_derived(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    xml_dir = tmp_path / "xml"
    xml_dir.mkdir()
    fixture = repo_root / "legacy-insider" / "tests" / "fixtures" / "form4_simple_buy.xml"
    target = xml_dir / fixture.name
    target.write_text(fixture.read_text())

    db_path = tmp_path / "derived.db"
    result = run_direct_xml_into_derived(
        repo_root=repo_root,
        derived_db_path=str(db_path),
        xml_dir=str(xml_dir),
        reference_date=datetime(2026, 4, 2),
    )

    assert result.xml_count == 1
    assert result.imported_normalized_count >= 1
    assert result.imported_result_count >= 1

    init_db(str(db_path))
    with get_connection(str(db_path)) as conn:
        normalized = conn.execute("SELECT COUNT(*) AS c FROM normalized_transactions").fetchone()["c"]
        results = conn.execute("SELECT COUNT(*) AS c FROM signal_results WHERE source = 'insider'").fetchone()["c"]
        assert normalized == result.imported_normalized_count
        assert results == result.imported_result_count
