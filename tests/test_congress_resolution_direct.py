from __future__ import annotations

from signals.congress.resolution import AssetCategory, resolve_transaction


def test_live_house_unknowns_are_reclassified_as_non_signal_assets():
    cases = [
        ("DFA International Core ETF", None, None, AssetCategory.BROAD_INDEX_ETF, False),
        ("VANGUARD TOTAL STOCK MARKET INDEX FD ADMIRAL SHARES", None, None, AssetCategory.MUTUAL_FUND, False),
        ("Harding Loevner International Eq Instl", None, None, AssetCategory.MUTUAL_FUND, False),
        ("Mays-Allocate 2025 LP", None, None, AssetCategory.PRIVATE_PLACEMENT, False),
        ("GS Managed Structured Note Strategy S&P 500 Linked Note", None, None, AssetCategory.CORPORATE_BOND, False),
        ("CITIGROUP INC. HYBRID PERPETUAL", None, None, AssetCategory.CORPORATE_BOND, False),
        ("CINCINNATI OHIO GO 5%", None, None, AssetCategory.MUNICIPAL_BOND, False),
        ("DIA - State Street SPDR Dow Jones", None, "OT", AssetCategory.BROAD_INDEX_ETF, False),
        ("Invesco QQQ", None, "OT", AssetCategory.BROAD_INDEX_ETF, False),
    ]

    for asset_name, ticker, asset_type, expected_category, include in cases:
        result = resolve_transaction(asset_name, ticker=ticker, asset_type_code=asset_type)
        assert result.category == expected_category, asset_name
        assert result.include_in_signal is include, asset_name
