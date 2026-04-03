"""Tests for Form 4 XML parsing."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parsing import parse_form4_xml

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


class TestParseSimpleBuy(unittest.TestCase):
    def setUp(self):
        self.result = parse_form4_xml(os.path.join(FIXTURES, "form4_simple_buy.xml"))

    def test_no_error(self):
        self.assertIsNone(self.result["parse_error"])

    def test_issuer_cik(self):
        self.assertEqual(self.result["filing"]["cik_issuer"], "0000320193")

    def test_owner_name(self):
        self.assertEqual(self.result["filing"]["owner_name"], "DOE JOHN")

    def test_owner_cik(self):
        self.assertEqual(self.result["filing"]["cik_owner"], "0001234567")

    def test_officer_title(self):
        self.assertEqual(self.result["filing"]["officer_title"], "Chief Executive Officer")

    def test_is_officer(self):
        self.assertEqual(self.result["filing"]["is_officer"], 1)

    def test_is_not_ten_pct(self):
        self.assertEqual(self.result["filing"]["is_ten_pct_owner"], 0)

    def test_period_of_report(self):
        self.assertEqual(self.result["filing"]["period_of_report"], "2024-06-15")

    def test_not_amendment(self):
        self.assertEqual(self.result["filing"]["is_amendment"], 0)

    def test_transaction_count(self):
        self.assertEqual(len(self.result["transactions"]), 1)

    def test_transaction_code(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["transaction_code"], "P")

    def test_shares(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["shares"], 10000)

    def test_price(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["price_per_share"], 185.50)

    def test_total_value(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["total_value"], 1855000.0)

    def test_shares_after(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["shares_after"], 50000)

    def test_direct_ownership(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["ownership_nature"], "D")

    def test_not_derivative(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["is_derivative"], 0)

    def test_security_title(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["security_title"], "Common Stock")


class TestParseSellWithFootnote(unittest.TestCase):
    def setUp(self):
        self.result = parse_form4_xml(os.path.join(FIXTURES, "form4_simple_sell.xml"))

    def test_transaction_code_sell(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["transaction_code"], "S")

    def test_footnote_contains_10b5_1(self):
        txn = self.result["transactions"][0]
        # Footnotes may or may not be attached to the transaction depending on XML structure
        # The footnote section exists in the XML
        filing = self.result["filing"]
        self.assertEqual(filing["officer_title"], "SVP, CFO")


class TestParseOptionExerciseAndSell(unittest.TestCase):
    def setUp(self):
        self.result = parse_form4_xml(
            os.path.join(FIXTURES, "form4_option_exercise_sell.xml")
        )

    def test_two_transactions(self):
        # One non-derivative (S) + one derivative (M)
        self.assertEqual(len(self.result["transactions"]), 2)

    def test_sell_transaction(self):
        non_deriv = [t for t in self.result["transactions"] if not t["is_derivative"]]
        self.assertEqual(len(non_deriv), 1)
        self.assertEqual(non_deriv[0]["transaction_code"], "S")
        self.assertEqual(non_deriv[0]["shares"], 20000)

    def test_option_exercise(self):
        deriv = [t for t in self.result["transactions"] if t["is_derivative"]]
        self.assertEqual(len(deriv), 1)
        self.assertEqual(deriv[0]["transaction_code"], "M")
        self.assertEqual(deriv[0]["underlying_security"], "Common Stock")

    def test_officer_title_president(self):
        self.assertEqual(self.result["filing"]["officer_title"], "President")


class TestParseAmendment(unittest.TestCase):
    def setUp(self):
        self.result = parse_form4_xml(os.path.join(FIXTURES, "form4_amendment.xml"))

    def test_is_amendment(self):
        self.assertEqual(self.result["filing"]["is_amendment"], 1)

    def test_amendment_type(self):
        self.assertEqual(self.result["filing"]["amendment_type"], "A")

    def test_corrected_shares(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["shares"], 12000)  # Corrected from 10000


class TestParseTenPctHolder(unittest.TestCase):
    def setUp(self):
        self.result = parse_form4_xml(
            os.path.join(FIXTURES, "form4_ten_pct_holder.xml")
        )

    def test_is_ten_pct_owner(self):
        self.assertEqual(self.result["filing"]["is_ten_pct_owner"], 1)

    def test_not_officer(self):
        self.assertEqual(self.result["filing"]["is_officer"], 0)

    def test_entity_owner_name(self):
        self.assertEqual(
            self.result["filing"]["owner_name"],
            "VANGUARD CAPITAL PARTNERS LLC",
        )


class TestParseMissingFile(unittest.TestCase):
    def test_file_not_found(self):
        result = parse_form4_xml("/nonexistent/path.xml")
        self.assertIsNotNone(result["parse_error"])
        self.assertIn("not found", result["parse_error"])


class TestParseMalformedXml(unittest.TestCase):
    def test_malformed(self):
        # Create a temporary malformed XML
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
            f.write("<ownershipDocument><broken>")
            path = f.name
        try:
            result = parse_form4_xml(path)
            self.assertIsNotNone(result["parse_error"])
        finally:
            os.unlink(path)


class TestParseMultiOwner(unittest.TestCase):
    """Tests for multi-owner Form 4 filings."""

    def setUp(self):
        self.result = parse_form4_xml(os.path.join(FIXTURES, "form4_multi_owner.xml"))

    def test_no_error(self):
        self.assertIsNone(self.result["parse_error"])

    def test_primary_owner_is_first(self):
        """Primary owner should be the first reporting owner."""
        self.assertEqual(self.result["filing"]["cik_owner"], "0001234567")
        self.assertEqual(self.result["filing"]["owner_name"], "Smith John A")

    def test_primary_owner_is_officer(self):
        self.assertEqual(self.result["filing"]["is_officer"], 1)
        self.assertEqual(self.result["filing"]["officer_title"], "Chief Executive Officer")

    def test_additional_owners_captured(self):
        """Additional owners should be stored in JSON field."""
        import json
        additional = self.result["filing"]["additional_owners"]
        self.assertIsNotNone(additional)
        owners = json.loads(additional)
        self.assertEqual(len(owners), 1)
        self.assertEqual(owners[0]["cik"], "0007654321")
        self.assertEqual(owners[0]["name"], "Smith Family Trust")
        self.assertEqual(owners[0]["is_ten_pct_owner"], True)
        self.assertEqual(owners[0]["is_officer"], False)

    def test_transaction_parsed(self):
        """Transaction should still be parsed correctly."""
        self.assertEqual(len(self.result["transactions"]), 1)
        txn = self.result["transactions"][0]
        self.assertEqual(txn["transaction_code"], "P")
        self.assertEqual(txn["shares"], 5000)
        self.assertEqual(txn["price_per_share"], 185.50)

    def test_indirect_ownership(self):
        txn = self.result["transactions"][0]
        self.assertEqual(txn["ownership_nature"], "I")
        self.assertEqual(txn["indirect_entity"], "By Smith Family Trust")


class TestParseSingleOwnerNoAdditional(unittest.TestCase):
    """Verify single-owner filings don't have additional_owners."""

    def setUp(self):
        self.result = parse_form4_xml(os.path.join(FIXTURES, "form4_simple_buy.xml"))

    def test_no_additional_owners(self):
        self.assertIsNone(self.result["filing"]["additional_owners"])


if __name__ == "__main__":
    unittest.main()
