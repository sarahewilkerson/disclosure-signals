"""Tests for entity resolution module."""

import pytest

from cppi.resolution import (
    AssetCategory,
    EntityResolver,
    ResolutionResult,
    calculate_resolution_metrics,
    resolve_transaction,
)


class TestAssetClassification:
    """Tests for asset classification."""

    @pytest.fixture
    def resolver(self):
        """Create a resolver instance."""
        return EntityResolver()

    def test_common_stock_with_ticker(self, resolver):
        """Test common stock classification."""
        result = resolver.resolve("Apple Inc. - Common Stock", "AAPL", "ST")
        assert result.category == AssetCategory.COMMON_STOCK
        assert result.resolved_ticker == "AAPL"
        assert result.include_in_signal is True

    def test_treasury_excluded(self, resolver):
        """Test treasury securities are excluded."""
        result = resolver.resolve("U.S. Treasury Bond", None, "GS")
        assert result.category == AssetCategory.TREASURY
        assert result.include_in_signal is False
        assert result.exclusion_reason == "asset_excluded:treasury"

    def test_treasury_pattern_detection(self, resolver):
        """Test treasury detection by name pattern."""
        result = resolver.resolve("US Treasury Bill 912797HP5", None, None)
        assert result.category == AssetCategory.TREASURY
        assert result.include_in_signal is False

    def test_municipal_bond_excluded(self, resolver):
        """Test municipal bonds are excluded."""
        result = resolver.resolve("California St Go Call 12/01/2027", None, None)
        assert result.category == AssetCategory.MUNICIPAL_BOND
        assert result.include_in_signal is False

    def test_private_placement_excluded(self, resolver):
        """Test private placements are excluded."""
        result = resolver.resolve("Forge Investments, LLC", None, "AB")
        assert result.category == AssetCategory.PRIVATE_PLACEMENT
        assert result.include_in_signal is False

    def test_broad_index_etf_excluded(self, resolver):
        """Test broad index ETFs are excluded."""
        result = resolver.resolve("SPDR S&P 500 ETF", "SPY", "EF")
        assert result.category == AssetCategory.BROAD_INDEX_ETF
        assert result.include_in_signal is False

    def test_sector_etf_included(self, resolver):
        """Test sector ETFs are included but flagged."""
        result = resolver.resolve("Technology Select Sector SPDR", "XLK", "EF")
        assert result.category == AssetCategory.SECTOR_ETF
        assert result.include_in_signal is True
        assert result.signal_relevance_weight == 0.8

    def test_option_included(self, resolver):
        """Test options are included."""
        result = resolver.resolve("NVIDIA Call Options", "NVDA", "OP")
        assert result.category == AssetCategory.OPTION
        assert result.include_in_signal is True
        assert result.signal_relevance_weight == 0.7

    def test_preferred_stock_pattern(self, resolver):
        """Test preferred stock detection by pattern."""
        test_cases = [
            "AT&T Inc. Depositary Shares",
            "Cadence Bank 5.50% Series A",
            "JPMorgan Chase Preferred Stock",
        ]
        for asset_name in test_cases:
            result = resolver.resolve(asset_name, None, "ST")
            assert result.category == AssetCategory.PREFERRED_STOCK, f"Failed for: {asset_name}"

    def test_mutual_fund_excluded(self, resolver):
        """Test mutual funds are excluded."""
        result = resolver.resolve("Vanguard 500 Index Fund", None, "MF")
        assert result.category == AssetCategory.MUTUAL_FUND
        assert result.include_in_signal is False

    def test_crypto_excluded(self, resolver):
        """Test crypto is excluded."""
        result = resolver.resolve("Bitcoin Trust", None, "CS")
        assert result.category == AssetCategory.CRYPTO
        assert result.include_in_signal is False


class TestConfidenceScoring:
    """Tests for confidence scoring."""

    @pytest.fixture
    def resolver(self):
        """Create a resolver instance."""
        return EntityResolver()

    def test_no_ticker_zero_confidence(self, resolver):
        """Test that no ticker results in zero confidence."""
        result = resolver.resolve("Unknown Asset", None, None)
        assert result.resolution_confidence == 0.0

    def test_ticker_with_type_higher_confidence(self, resolver):
        """Test that ticker with type code has higher confidence."""
        with_type = resolver.resolve("Apple Inc.", "AAPL", "ST")
        without_type = resolver.resolve("Apple Inc.", "AAPL", None)
        assert with_type.resolution_confidence > without_type.resolution_confidence

    def test_known_etf_high_confidence(self, resolver):
        """Test that known ETFs have high confidence."""
        result = resolver.resolve("S&P 500 ETF", "SPY", "EF")
        assert result.resolution_confidence >= 0.7


class TestSignalWeights:
    """Tests for signal relevance weights."""

    @pytest.fixture
    def resolver(self):
        """Create a resolver instance."""
        return EntityResolver()

    def test_common_stock_full_weight(self, resolver):
        """Test common stock has full weight."""
        result = resolver.resolve("Apple Inc.", "AAPL", "ST")
        assert result.signal_relevance_weight == 1.0

    def test_excluded_zero_weight(self, resolver):
        """Test excluded assets have zero weight."""
        result = resolver.resolve("U.S. Treasury Bond", None, "GS")
        assert result.signal_relevance_weight == 0.0

    def test_option_lower_weight(self, resolver):
        """Test options have lower weight due to uncertainty."""
        result = resolver.resolve("NVIDIA Call", "NVDA", "OP")
        assert result.signal_relevance_weight < 1.0
        assert result.signal_relevance_weight > 0.5


class TestResolutionMetrics:
    """Tests for resolution metrics calculation."""

    def test_calculate_basic_metrics(self):
        """Test basic metrics calculation."""
        results = [
            (ResolutionResult(
                resolved_ticker="AAPL",
                resolved_company="Apple Inc.",
                category=AssetCategory.COMMON_STOCK,
                resolution_method="extracted",
                resolution_confidence=0.9,
                include_in_signal=True,
                exclusion_reason=None,
                signal_relevance_weight=1.0,
            ), 10000),
            (ResolutionResult(
                resolved_ticker=None,
                resolved_company=None,
                category=AssetCategory.TREASURY,
                resolution_method="excluded",
                resolution_confidence=0.0,
                include_in_signal=False,
                exclusion_reason="asset_excluded:treasury",
                signal_relevance_weight=0.0,
            ), 5000),
        ]

        metrics = calculate_resolution_metrics(results)

        assert metrics["total_transactions"] == 2
        assert metrics["included_count"] == 1
        assert metrics["excluded_count"] == 1
        assert metrics["resolved_count"] == 1

    def test_resolution_rate_calculation(self):
        """Test resolution rate calculation."""
        results = [
            (ResolutionResult(
                resolved_ticker="AAPL",
                resolved_company="Apple",
                category=AssetCategory.COMMON_STOCK,
                resolution_method="extracted",
                resolution_confidence=0.9,
                include_in_signal=True,
                exclusion_reason=None,
                signal_relevance_weight=1.0,
            ), 10000),
            (ResolutionResult(
                resolved_ticker=None,
                resolved_company=None,
                category=AssetCategory.COMMON_STOCK,
                resolution_method="unresolved",
                resolution_confidence=0.0,
                include_in_signal=True,
                exclusion_reason=None,
                signal_relevance_weight=0.3,
            ), 5000),
        ]

        metrics = calculate_resolution_metrics(results)

        # 1 resolved out of 2 included = 50%
        assert metrics["resolution_rate_by_count"] == 0.5
        # 10000 resolved out of 15000 included = 66.7%
        assert abs(metrics["resolution_rate_by_value"] - 0.667) < 0.01


class TestConvenienceFunction:
    """Tests for convenience function."""

    def test_resolve_transaction_basic(self):
        """Test resolve_transaction convenience function."""
        result = resolve_transaction("Apple Inc.", "AAPL", "ST")
        assert isinstance(result, ResolutionResult)
        assert result.resolved_ticker == "AAPL"
        assert result.include_in_signal is True

    def test_resolve_transaction_excluded(self):
        """Test resolve_transaction for excluded asset."""
        result = resolve_transaction("US Treasury Bill", None, "GS")
        assert result.include_in_signal is False
        assert result.exclusion_reason is not None


class TestEdgeCases:
    """Tests for edge cases."""

    @pytest.fixture
    def resolver(self):
        """Create a resolver instance."""
        return EntityResolver()

    def test_empty_asset_name(self, resolver):
        """Test handling of empty asset name."""
        result = resolver.resolve("", None, None)
        assert result.category == AssetCategory.UNKNOWN
        assert result.include_in_signal is False

    def test_ticker_normalization(self, resolver):
        """Test ticker is normalized to uppercase."""
        result = resolver.resolve("Apple Inc.", "aapl", "ST")
        assert result.resolved_ticker == "AAPL"

    def test_ticker_whitespace_stripped(self, resolver):
        """Test ticker whitespace is stripped."""
        result = resolver.resolve("Apple Inc.", "  AAPL  ", "ST")
        assert result.resolved_ticker == "AAPL"

    def test_municipal_bond_pattern_variants(self, resolver):
        """Test various municipal bond patterns."""
        test_cases = [
            "Honolulu HI City & 4% Go Utx Due",
            "Illinois ST Sales Tax Rev JR Oblig",
            "New York N Y Go BDS Ser. Fiscal",
        ]
        for asset_name in test_cases:
            result = resolver.resolve(asset_name, None, None)
            assert result.category == AssetCategory.MUNICIPAL_BOND, f"Failed for: {asset_name}"
            assert result.include_in_signal is False
