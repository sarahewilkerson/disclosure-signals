"""Tests for scoring engine."""

import math
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from scoring import (
    score_transaction,
    _compute_size_signal,
    _compute_recency_weight,
    _aggregate_with_saturation,
    _compute_confidence,
    _label_signal,
    _confidence_tier,
)


class TestTransactionScoring(unittest.TestCase):
    def setUp(self):
        self.ref_date = datetime(2024, 7, 15)

    def test_ceo_buy_direct_recent(self):
        txn = {
            "transaction_code": "P",
            "role_class": "ceo",
            "is_likely_planned": 0,
            "ownership_nature": "D",
            "pct_holdings_changed": 0.10,
            "transaction_date": "2024-07-10",
        }
        result = score_transaction(txn, self.ref_date)
        self.assertGreater(result["transaction_signal"], 0)
        self.assertEqual(result["direction"], 1.0)
        self.assertEqual(result["role_weight"], 1.0)
        self.assertEqual(result["discretionary_weight"], 1.0)
        self.assertEqual(result["size_signal"], 1.0)  # 5-20% bracket

    def test_cfo_sell_direct_recent(self):
        txn = {
            "transaction_code": "S",
            "role_class": "cfo",
            "is_likely_planned": 0,
            "ownership_nature": "D",
            "pct_holdings_changed": 0.03,
            "transaction_date": "2024-07-10",
        }
        result = score_transaction(txn, self.ref_date)
        self.assertLess(result["transaction_signal"], 0)
        self.assertEqual(result["direction"], -0.5)
        self.assertEqual(result["role_weight"], 0.95)

    def test_planned_trade_discount(self):
        txn = {
            "transaction_code": "S",
            "role_class": "ceo",
            "is_likely_planned": 1,
            "ownership_nature": "D",
            "pct_holdings_changed": 0.05,
            "transaction_date": "2024-07-10",
        }
        result = score_transaction(txn, self.ref_date)
        self.assertEqual(result["discretionary_weight"], config.PLANNED_TRADE_DISCOUNT)

    def test_indirect_ownership_discount(self):
        txn = {
            "transaction_code": "P",
            "role_class": "ceo",
            "is_likely_planned": 0,
            "ownership_nature": "I",
            "pct_holdings_changed": 0.10,
            "transaction_date": "2024-07-10",
        }
        result = score_transaction(txn, self.ref_date)
        self.assertEqual(result["ownership_weight"], config.INDIRECT_OWNERSHIP_WEIGHT)

    def test_buy_magnitude_greater_than_sell(self):
        """Buy signal magnitude should be 2x sell signal magnitude, all else equal."""
        base = {
            "role_class": "ceo",
            "is_likely_planned": 0,
            "ownership_nature": "D",
            "pct_holdings_changed": 0.10,
            "transaction_date": "2024-07-10",
        }
        buy = score_transaction({**base, "transaction_code": "P"}, self.ref_date)
        sell = score_transaction({**base, "transaction_code": "S"}, self.ref_date)
        self.assertAlmostEqual(
            abs(buy["transaction_signal"]) / abs(sell["transaction_signal"]),
            2.0,
            places=2,
        )


class TestSizeSignal(unittest.TestCase):
    def test_tiny(self):
        self.assertEqual(_compute_size_signal(0.005), 0.5)

    def test_small(self):
        self.assertEqual(_compute_size_signal(0.03), 0.8)

    def test_medium(self):
        self.assertEqual(_compute_size_signal(0.10), 1.0)

    def test_large(self):
        self.assertEqual(_compute_size_signal(0.30), 1.2)

    def test_unknown(self):
        self.assertEqual(_compute_size_signal(None), config.SIZE_SIGNAL_UNKNOWN)


class TestRecencyWeight(unittest.TestCase):
    def test_today(self):
        ref = datetime(2024, 7, 15)
        w = _compute_recency_weight("2024-07-15", ref)
        self.assertAlmostEqual(w, 1.0, places=2)

    def test_half_life(self):
        ref = datetime(2024, 7, 15)
        w = _compute_recency_weight("2024-05-31", ref)  # 45 days ago
        self.assertAlmostEqual(w, 0.5, places=1)

    def test_old(self):
        ref = datetime(2024, 7, 15)
        w = _compute_recency_weight("2024-01-15", ref)  # 182 days ago
        self.assertLess(w, 0.1)

    def test_missing_date(self):
        w = _compute_recency_weight(None, datetime(2024, 7, 15))
        self.assertEqual(w, 0.5)


class TestSaturationCap(unittest.TestCase):
    def test_no_cap_needed(self):
        scored = [
            {"cik_owner": "A", "transaction_signal": 0.5},
            {"cik_owner": "B", "transaction_signal": 0.3},
        ]
        score, contrib = _aggregate_with_saturation(scored)
        self.assertGreater(score, 0)

    def test_single_insider_capped(self):
        """One dominant insider should be capped — much lower than uncapped."""
        scored = [
            {"cik_owner": "A", "transaction_signal": 10.0},
            {"cik_owner": "B", "transaction_signal": 0.1},
            {"cik_owner": "C", "transaction_signal": 0.1},
        ]
        score, contrib = _aggregate_with_saturation(scored)
        # A's uncapped contribution is 10.0. After iterative capping,
        # it should be drastically reduced (well below 10.0)
        self.assertLess(abs(contrib["A"]), 1.0)
        # B and C should remain close to their original values
        self.assertAlmostEqual(contrib["B"], 0.1, places=1)
        self.assertAlmostEqual(contrib["C"], 0.1, places=1)

    def test_empty(self):
        score, contrib = _aggregate_with_saturation([])
        self.assertEqual(score, 0.0)

    def test_score_bounded(self):
        """Output score should be in [-1, 1]."""
        scored = [
            {"cik_owner": "A", "transaction_signal": 100.0},
        ]
        score, _ = _aggregate_with_saturation(scored)
        self.assertLessEqual(abs(score), 1.0)


class TestConfidence(unittest.TestCase):
    def test_zero_transactions(self):
        c = _compute_confidence(0, 0, False, False)
        self.assertEqual(c, 0.0)

    def test_one_transaction_one_insider(self):
        c = _compute_confidence(1, 1, True, False)
        self.assertLess(c, 0.5)

    def test_many_transactions_many_insiders(self):
        c = _compute_confidence(10, 5, True, True)
        self.assertGreater(c, 0.5)

    def test_capped(self):
        c = _compute_confidence(100, 20, True, True)
        self.assertLessEqual(c, config.CONFIDENCE_MAX)

    def test_both_buys_and_sells_bonus(self):
        c_one = _compute_confidence(5, 3, True, False)
        c_both = _compute_confidence(5, 3, True, True)
        self.assertGreater(c_both, c_one)


class TestSignalLabel(unittest.TestCase):
    def test_bullish(self):
        self.assertEqual(_label_signal(0.3, 0.5), "bullish")

    def test_bearish(self):
        self.assertEqual(_label_signal(-0.3, 0.5), "bearish")

    def test_neutral(self):
        self.assertEqual(_label_signal(0.05, 0.5), "neutral")

    def test_insufficient_low_confidence(self):
        self.assertEqual(_label_signal(0.5, 0.1), "insufficient")

    def test_boundary_bullish(self):
        self.assertEqual(_label_signal(0.16, 0.3), "bullish")

    def test_boundary_bearish(self):
        self.assertEqual(_label_signal(-0.16, 0.3), "bearish")


class TestConfidenceTier(unittest.TestCase):
    def test_insufficient(self):
        self.assertEqual(_confidence_tier(0.1), "insufficient")

    def test_low(self):
        self.assertEqual(_confidence_tier(0.3), "low")

    def test_moderate(self):
        self.assertEqual(_confidence_tier(0.6), "moderate")

    def test_high(self):
        self.assertEqual(_confidence_tier(0.8), "high")


if __name__ == "__main__":
    unittest.main()
