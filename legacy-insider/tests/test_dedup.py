"""Tests for amendment deduplication logic."""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import (
    init_db,
    get_connection,
    upsert_company,
    upsert_filing,
    insert_transaction,
    clear_transactions_for_filing,
    get_amendment_candidates,
)


class TestAmendmentDedup(unittest.TestCase):
    def setUp(self):
        self.db_fd, self.db_path = tempfile.mkstemp(suffix=".db")
        init_db(self.db_path)

        # Insert the company first (FK requirement)
        with get_connection(self.db_path) as conn:
            upsert_company(conn, {
                "cik": "0000320193",
                "ticker": "AAPL",
                "company_name": "Apple Inc.",
                "fortune_rank": 1,
                "revenue": 394328,
                "sector": "Information Technology",
                "resolved_at": "2024-06-17",
            })

        # Insert an original filing
        with get_connection(self.db_path) as conn:
            upsert_filing(conn, {
                "accession_number": "0001-24-000001",
                "cik_issuer": "0000320193",
                "cik_owner": "0001234567",
                "owner_name": "DOE JOHN",
                "officer_title": "CEO",
                "is_officer": 1,
                "is_director": 0,
                "is_ten_pct_owner": 0,
                "is_other": 0,
                "is_amendment": 0,
                "amendment_type": None,
                "period_of_report": "2024-06-15",
                "aff10b5one": 0,
                "additional_owners": None,
                "filing_date": "2024-06-17",
                "xml_url": "https://example.com/original.xml",
                "raw_xml_path": "/tmp/original.xml",
                "parsed_at": "2024-06-17 10:00:00",
                "parse_error": None,
            })

            # Insert a transaction for the original filing
            insert_transaction(conn, {
                "accession_number": "0001-24-000001",
                "cik_issuer": "0000320193",
                "cik_owner": "0001234567",
                "owner_name": "DOE JOHN",
                "officer_title": "CEO",
                "security_title": "Common Stock",
                "transaction_date": "2024-06-15",
                "transaction_code": "P",
                "equity_swap": 0,
                "shares": 10000,
                "price_per_share": 185.50,
                "total_value": 1855000.0,
                "shares_after": 50000,
                "ownership_nature": "D",
                "indirect_entity": None,
                "is_derivative": 0,
                "underlying_security": None,
                "footnotes": None,
                "role_class": "ceo",
                "transaction_class": "open_market_buy",
                "is_likely_planned": 0,
                "is_discretionary": 1,
                "pct_holdings_changed": 0.1667,
                "include_in_signal": 1,
                "exclusion_reason": None,
            })

    def tearDown(self):
        os.close(self.db_fd)
        os.unlink(self.db_path)

    def test_find_amendment_candidates(self):
        with get_connection(self.db_path) as conn:
            candidates = get_amendment_candidates(
                conn, "0000320193", "0001234567", "2024-06-15"
            )
            self.assertEqual(len(candidates), 1)
            self.assertEqual(candidates[0]["accession_number"], "0001-24-000001")

    def test_no_candidates_different_owner(self):
        with get_connection(self.db_path) as conn:
            candidates = get_amendment_candidates(
                conn, "0000320193", "0009999999", "2024-06-15"
            )
            self.assertEqual(len(candidates), 0)

    def test_no_candidates_different_period(self):
        with get_connection(self.db_path) as conn:
            candidates = get_amendment_candidates(
                conn, "0000320193", "0001234567", "2024-07-01"
            )
            self.assertEqual(len(candidates), 0)

    def test_exclude_self_from_candidates(self):
        with get_connection(self.db_path) as conn:
            candidates = get_amendment_candidates(
                conn, "0000320193", "0001234567", "2024-06-15",
                exclude_accession="0001-24-000001",
            )
            self.assertEqual(len(candidates), 0)

    def test_clear_original_transactions(self):
        with get_connection(self.db_path) as conn:
            # Verify original transaction exists
            txns = conn.execute(
                "SELECT COUNT(*) as c FROM transactions WHERE accession_number = ?",
                ("0001-24-000001",)
            ).fetchone()
            self.assertEqual(txns["c"], 1)

            # Clear transactions (simulating amendment)
            clear_transactions_for_filing(conn, "0001-24-000001")

            # Verify cleared
            txns = conn.execute(
                "SELECT COUNT(*) as c FROM transactions WHERE accession_number = ?",
                ("0001-24-000001",)
            ).fetchone()
            self.assertEqual(txns["c"], 0)


class TestDatabaseSchema(unittest.TestCase):
    def test_init_db_creates_tables(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        try:
            init_db(path)
            with get_connection(path) as conn:
                tables = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                ).fetchall()
                table_names = {t["name"] for t in tables}
                self.assertIn("companies", table_names)
                self.assertIn("filings", table_names)
                self.assertIn("transactions", table_names)
                self.assertIn("company_scores", table_names)
                self.assertIn("aggregate_index", table_names)
        finally:
            os.close(fd)
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
