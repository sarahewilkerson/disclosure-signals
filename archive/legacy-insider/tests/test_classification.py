"""Tests for role classification, transaction classification, and exclusion logic."""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from classification import (
    classify_role,
    classify_transaction_type,
    detect_planned_trade,
    compute_pct_holdings_changed,
    detect_exercise_and_sell,
    _is_entity_name,
    _is_former_officer,
    _match_leadership_title,
    _determine_inclusion,
)


class TestRoleClassification(unittest.TestCase):
    """Test classify_role with various title patterns."""

    def test_ceo(self):
        role, reason = classify_role("Chief Executive Officer", "DOE JOHN", True, False, False, False)
        self.assertEqual(role, "ceo")
        self.assertIsNone(reason)

    def test_ceo_abbreviation(self):
        role, _ = classify_role("CEO", "DOE JOHN", True, False, False, False)
        self.assertEqual(role, "ceo")

    def test_ceo_compound_title(self):
        role, _ = classify_role("Chairman & CEO", "DOE JOHN", True, False, False, False)
        self.assertEqual(role, "ceo")  # CEO takes priority over Chair

    def test_cfo(self):
        role, _ = classify_role("SVP, CFO", "SMITH JANE", True, False, False, False)
        self.assertEqual(role, "cfo")

    def test_cfo_principal_financial_officer(self):
        role, _ = classify_role("Principal Financial Officer", "SMITH JANE", True, False, False, False)
        self.assertEqual(role, "cfo")

    def test_chair(self):
        role, _ = classify_role("Executive Chairman", "JONES BOB", True, False, False, False)
        self.assertEqual(role, "chair")

    def test_chairwoman(self):
        role, _ = classify_role("Chairwoman of the Board", "JONES BOB", True, False, False, False)
        self.assertEqual(role, "chair")

    def test_president(self):
        role, _ = classify_role("President", "WILLIAMS SAM", True, False, False, False)
        self.assertEqual(role, "president")

    def test_president_and_coo(self):
        role, _ = classify_role("President & COO", "WILLIAMS SAM", True, False, False, False)
        # President has higher priority than COO
        self.assertEqual(role, "president")

    def test_coo(self):
        role, _ = classify_role("Chief Operating Officer", "GARCIA MARIA", True, False, False, False)
        self.assertEqual(role, "coo")

    def test_evp_general_counsel_matches_clo(self):
        """EVP General Counsel matches CLO pattern (general counsel)."""
        role, reason = classify_role("EVP, General Counsel", "LEE ROBERT", True, False, False, False)
        self.assertEqual(role, "clo")
        self.assertIsNone(reason)

    def test_excluded_evp_other(self):
        """EVP without C-suite title is excluded."""
        role, reason = classify_role("EVP, Sales Operations", "LEE ROBERT", True, False, False, False)
        self.assertEqual(role, "excluded")
        self.assertIn("not_top_leadership", reason)

    def test_excluded_senior_vice_president(self):
        """Senior Vice President must NOT match as 'president'."""
        role, reason = classify_role("Senior Vice President", "DOE JOHN", True, False, False, False)
        self.assertEqual(role, "excluded")
        self.assertIn("not_top_leadership", reason)

    def test_excluded_director_only(self):
        role, reason = classify_role(None, "BOARD MEMBER", False, True, False, False)
        self.assertEqual(role, "excluded")
        self.assertIn("director_only", reason)

    def test_excluded_ten_pct_holder(self):
        role, reason = classify_role(None, "BIG FUND LLC", False, False, True, False)
        self.assertEqual(role, "excluded")
        # Should be excluded as entity name or ten_pct_holder_only
        self.assertIsNotNone(reason)

    def test_excluded_entity_name(self):
        role, reason = classify_role("CEO", "ACME HOLDINGS LLC", True, False, False, False)
        self.assertEqual(role, "excluded")
        self.assertIn("entity_name", reason)

    def test_excluded_former_officer(self):
        role, reason = classify_role("Former CEO", "DOE JOHN", True, False, False, False)
        self.assertEqual(role, "excluded")
        self.assertIn("former_officer", reason)

    def test_title_priority_over_is_officer_flag(self):
        """Title patterns should be checked even if is_officer=False.

        The is_officer XML flag is not always reliable, so we prioritize
        the officer_title text for classification.
        """
        role, reason = classify_role("CEO", "DOE JOHN", False, False, False, False)
        self.assertEqual(role, "ceo")  # Title pattern takes priority
        self.assertIsNone(reason)

    def test_cto(self):
        """CTO is included in C-suite patterns."""
        role, _ = classify_role("Chief Technology Officer", "SMITH ALICE", True, False, False, False)
        self.assertEqual(role, "cto")

    def test_cto_abbreviation(self):
        role, _ = classify_role("CTO", "SMITH ALICE", True, False, False, False)
        self.assertEqual(role, "cto")

    def test_clo(self):
        """CLO/General Counsel is included in C-suite patterns."""
        role, _ = classify_role("Chief Legal Officer", "JONES BOB", True, False, False, False)
        self.assertEqual(role, "clo")

    def test_general_counsel(self):
        role, _ = classify_role("General Counsel", "JONES BOB", True, False, False, False)
        self.assertEqual(role, "clo")

    def test_cio(self):
        """CIO is included in C-suite patterns."""
        role, _ = classify_role("Chief Information Officer", "WILLIAMS SAM", True, False, False, False)
        self.assertEqual(role, "cio")

    def test_cmo(self):
        """CMO is included in C-suite patterns."""
        role, _ = classify_role("CMO", "GARCIA MARIA", True, False, False, False)
        self.assertEqual(role, "cmo")

    def test_cao(self):
        """CAO/Principal Accounting Officer is included in C-suite patterns."""
        role, _ = classify_role("Chief Accounting Officer", "LEE CHRIS", True, False, False, False)
        self.assertEqual(role, "cao")

    def test_principal_accounting_officer(self):
        role, _ = classify_role("Principal Accounting Officer", "LEE CHRIS", True, False, False, False)
        self.assertEqual(role, "cao")

    def test_c_suite_with_is_officer_false(self):
        """C-suite titles should be classified even if is_officer=False."""
        role, _ = classify_role("CTO", "DOE JOHN", False, False, False, False)
        self.assertEqual(role, "cto")

    def test_excluded_no_officer_indicators(self):
        """No officer role when no title and is_officer=False."""
        role, reason = classify_role(None, "DOE JOHN", False, False, False, False)
        self.assertEqual(role, "excluded")
        self.assertEqual(reason, "no_officer_role")


class TestEntityNameDetection(unittest.TestCase):
    def test_llc(self):
        self.assertTrue(_is_entity_name("Vanguard Capital Partners LLC"))

    def test_lp(self):
        self.assertTrue(_is_entity_name("Berkshire LP"))

    def test_trust(self):
        self.assertTrue(_is_entity_name("Smith Family Trust"))

    def test_foundation(self):
        self.assertTrue(_is_entity_name("Gates Foundation"))

    def test_fund(self):
        self.assertTrue(_is_entity_name("Tiger Global Fund"))

    def test_normal_name(self):
        self.assertFalse(_is_entity_name("DOE JOHN"))

    def test_normal_name_with_suffix(self):
        self.assertFalse(_is_entity_name("SMITH JR JAMES"))


class TestFormerOfficer(unittest.TestCase):
    def test_former(self):
        self.assertTrue(_is_former_officer("Former CEO"))

    def test_fmr(self):
        self.assertTrue(_is_former_officer("Fmr. President"))

    def test_retired(self):
        self.assertTrue(_is_former_officer("Retired Chairman"))

    def test_current(self):
        self.assertFalse(_is_former_officer("Chief Executive Officer"))


class TestLeadershipTitleMatching(unittest.TestCase):
    def test_ceo_variations(self):
        self.assertEqual(_match_leadership_title("CEO"), "ceo")
        self.assertEqual(_match_leadership_title("Chief Executive Officer"), "ceo")
        self.assertEqual(_match_leadership_title("CEO and President"), "ceo")

    def test_cfo_variations(self):
        self.assertEqual(_match_leadership_title("CFO"), "cfo")
        self.assertEqual(_match_leadership_title("SVP and CFO"), "cfo")
        self.assertEqual(_match_leadership_title("EVP, Chief Financial Officer"), "cfo")
        self.assertEqual(_match_leadership_title("Principal Financial Officer"), "cfo")

    def test_no_match(self):
        """Titles that don't match any leadership pattern."""
        self.assertIsNone(_match_leadership_title("VP of Engineering"))
        self.assertIsNone(_match_leadership_title("Senior Vice President"))
        self.assertIsNone(_match_leadership_title("Director of Sales"))
        self.assertIsNone(_match_leadership_title("Controller"))

    def test_c_suite_patterns(self):
        """C-suite roles should match."""
        self.assertEqual(_match_leadership_title("CTO"), "cto")
        self.assertEqual(_match_leadership_title("Chief Technology Officer"), "cto")
        self.assertEqual(_match_leadership_title("CLO"), "clo")
        self.assertEqual(_match_leadership_title("Chief Legal Officer"), "clo")
        self.assertEqual(_match_leadership_title("General Counsel"), "clo")
        self.assertEqual(_match_leadership_title("CIO"), "cio")
        self.assertEqual(_match_leadership_title("Chief Information Officer"), "cio")
        self.assertEqual(_match_leadership_title("CMO"), "cmo")
        self.assertEqual(_match_leadership_title("Chief Marketing Officer"), "cmo")
        self.assertEqual(_match_leadership_title("CAO"), "cao")
        self.assertEqual(_match_leadership_title("Chief Accounting Officer"), "cao")
        self.assertEqual(_match_leadership_title("Principal Accounting Officer"), "cao")

    def test_vice_president_excluded(self):
        """Vice Presidents must NOT match 'president'."""
        self.assertIsNone(_match_leadership_title("Senior Vice President"))
        self.assertIsNone(_match_leadership_title("Executive Vice President"))
        self.assertIsNone(_match_leadership_title("Vice President of Sales"))
        self.assertIsNone(_match_leadership_title("SVP & Vice President"))
        self.assertIsNone(_match_leadership_title("Vice President"))

    def test_president_still_matches(self):
        """Actual President titles should still match."""
        self.assertEqual(_match_leadership_title("President"), "president")
        self.assertEqual(_match_leadership_title("President & CEO"), "ceo")  # CEO priority
        self.assertEqual(_match_leadership_title("President and COO"), "president")

    def test_empty(self):
        self.assertIsNone(_match_leadership_title(""))
        self.assertIsNone(_match_leadership_title(None))


class TestTransactionCodeMapping(unittest.TestCase):
    def test_purchase(self):
        self.assertEqual(classify_transaction_type("P"), "open_market_buy")

    def test_sale(self):
        self.assertEqual(classify_transaction_type("S"), "open_market_sell")

    def test_option_exercise(self):
        self.assertEqual(classify_transaction_type("M"), "option_exercise")

    def test_tax_withhold(self):
        self.assertEqual(classify_transaction_type("F"), "tax_withhold")

    def test_award(self):
        self.assertEqual(classify_transaction_type("A"), "award_grant")

    def test_gift(self):
        self.assertEqual(classify_transaction_type("G"), "gift")

    def test_conversion(self):
        self.assertEqual(classify_transaction_type("C"), "conversion")

    def test_unknown(self):
        self.assertEqual(classify_transaction_type("X"), "other")

    def test_none(self):
        self.assertEqual(classify_transaction_type(None), "unknown")

    def test_case_insensitive(self):
        self.assertEqual(classify_transaction_type("p"), "open_market_buy")
        self.assertEqual(classify_transaction_type("s"), "open_market_sell")


class TestPlannedTradeDetection(unittest.TestCase):
    def test_10b5_1(self):
        self.assertTrue(detect_planned_trade("Pursuant to a Rule 10b5-1 trading plan"))

    def test_10b_5_1_variant(self):
        self.assertTrue(detect_planned_trade("Under a 10b5-1 plan adopted Jan 2024"))

    def test_trading_plan(self):
        self.assertTrue(detect_planned_trade("Pre-arranged trading plan"))

    def test_no_plan(self):
        self.assertFalse(detect_planned_trade("Open market purchase"))

    def test_none(self):
        self.assertFalse(detect_planned_trade(None))

    def test_empty(self):
        self.assertFalse(detect_planned_trade(""))


class TestPctHoldingsChanged(unittest.TestCase):
    def test_normal(self):
        # Sold 5000 shares, 95000 remaining
        pct = compute_pct_holdings_changed(5000, 95000)
        self.assertAlmostEqual(pct, 0.05, places=2)  # 5000/100000 = 5%

    def test_large_sale(self):
        # Sold 40000 shares, 10000 remaining
        pct = compute_pct_holdings_changed(40000, 10000)
        self.assertAlmostEqual(pct, 0.8, places=2)  # 40000/50000 = 80%

    def test_small_buy(self):
        pct = compute_pct_holdings_changed(100, 50100)
        self.assertAlmostEqual(pct, 100 / 50200, places=4)

    def test_missing_shares(self):
        self.assertIsNone(compute_pct_holdings_changed(None, 50000))

    def test_missing_shares_after(self):
        self.assertIsNone(compute_pct_holdings_changed(5000, None))

    def test_zero_shares(self):
        self.assertIsNone(compute_pct_holdings_changed(0, 50000))


class TestExerciseAndSellDetection(unittest.TestCase):
    def test_same_day_match(self):
        txns = [
            {"id": 1, "cik_owner": "001", "transaction_code": "M",
             "transaction_date": "2024-07-01", "shares": 20000},
            {"id": 2, "cik_owner": "001", "transaction_code": "S",
             "transaction_date": "2024-07-01", "shares": 20000},
        ]
        flagged = detect_exercise_and_sell(txns)
        self.assertIn(2, flagged)

    def test_within_window(self):
        txns = [
            {"id": 1, "cik_owner": "001", "transaction_code": "M",
             "transaction_date": "2024-07-01", "shares": 10000},
            {"id": 2, "cik_owner": "001", "transaction_code": "S",
             "transaction_date": "2024-07-03", "shares": 10000},
        ]
        flagged = detect_exercise_and_sell(txns)
        self.assertIn(2, flagged)

    def test_outside_window(self):
        txns = [
            {"id": 1, "cik_owner": "001", "transaction_code": "M",
             "transaction_date": "2024-07-01", "shares": 10000},
            {"id": 2, "cik_owner": "001", "transaction_code": "S",
             "transaction_date": "2024-07-10", "shares": 10000},
        ]
        flagged = detect_exercise_and_sell(txns)
        self.assertNotIn(2, flagged)

    def test_different_owners_not_flagged(self):
        txns = [
            {"id": 1, "cik_owner": "001", "transaction_code": "M",
             "transaction_date": "2024-07-01", "shares": 10000},
            {"id": 2, "cik_owner": "002", "transaction_code": "S",
             "transaction_date": "2024-07-01", "shares": 10000},
        ]
        flagged = detect_exercise_and_sell(txns)
        self.assertEqual(len(flagged), 0)

    def test_share_count_mismatch(self):
        txns = [
            {"id": 1, "cik_owner": "001", "transaction_code": "M",
             "transaction_date": "2024-07-01", "shares": 10000},
            {"id": 2, "cik_owner": "001", "transaction_code": "S",
             "transaction_date": "2024-07-01", "shares": 5000},
        ]
        flagged = detect_exercise_and_sell(txns)
        self.assertNotIn(2, flagged)  # 50% difference > 10% tolerance


class TestDetermineInclusion(unittest.TestCase):
    def test_included_buy(self):
        include, reason = _determine_inclusion(
            role_class="ceo", role_exclusion=None,
            txn_class="open_market_buy", transaction_code="P",
            is_derivative=False, equity_swap=False,
            ownership_nature="D", indirect_entity=None,
        )
        self.assertTrue(include)
        self.assertIsNone(reason)

    def test_excluded_role(self):
        include, reason = _determine_inclusion(
            role_class="excluded", role_exclusion="director_only",
            txn_class="open_market_buy", transaction_code="P",
            is_derivative=False, equity_swap=False,
            ownership_nature="D", indirect_entity=None,
        )
        self.assertFalse(include)

    def test_excluded_derivative(self):
        include, reason = _determine_inclusion(
            role_class="ceo", role_exclusion=None,
            txn_class="option_exercise", transaction_code="M",
            is_derivative=True, equity_swap=False,
            ownership_nature="D", indirect_entity=None,
        )
        self.assertFalse(include)
        self.assertEqual(reason, "derivative_transaction")

    def test_excluded_non_core_code(self):
        include, reason = _determine_inclusion(
            role_class="cfo", role_exclusion=None,
            txn_class="tax_withhold", transaction_code="F",
            is_derivative=False, equity_swap=False,
            ownership_nature="D", indirect_entity=None,
        )
        self.assertFalse(include)
        self.assertEqual(reason, "transaction_code_F")

    def test_excluded_equity_swap(self):
        include, reason = _determine_inclusion(
            role_class="ceo", role_exclusion=None,
            txn_class="open_market_buy", transaction_code="P",
            is_derivative=False, equity_swap=True,
            ownership_nature="D", indirect_entity=None,
        )
        self.assertFalse(include)

    def test_excluded_indirect_entity(self):
        include, reason = _determine_inclusion(
            role_class="ceo", role_exclusion=None,
            txn_class="open_market_sell", transaction_code="S",
            is_derivative=False, equity_swap=False,
            ownership_nature="I", indirect_entity="Smith Family Trust",
        )
        self.assertFalse(include)
        self.assertIn("indirect_entity", reason)

    def test_included_indirect_personal(self):
        # Indirect but not an entity pattern (e.g., "by spouse")
        include, reason = _determine_inclusion(
            role_class="ceo", role_exclusion=None,
            txn_class="open_market_sell", transaction_code="S",
            is_derivative=False, equity_swap=False,
            ownership_nature="I", indirect_entity="By Spouse",
        )
        self.assertTrue(include)


if __name__ == "__main__":
    unittest.main()
