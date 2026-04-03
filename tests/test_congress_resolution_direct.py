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


def test_live_senate_candidate_discovery_examples_are_non_signal_assets():
    cases = [
        ("GS Managed Structured Note Strategy S&P 500 Linked Note", None, None, AssetCategory.CORPORATE_BOND, False),
        ("GS Managed Structured Note Strategy MSCI EAFE Linked Note", None, None, AssetCategory.CORPORATE_BOND, False),
        ("ALLEGHENY CNTY PA ARPT AUTH ARPT REVRate/Coupon:5.5%Matures:2050-01-01", None, None, AssetCategory.MUNICIPAL_BOND, False),
        ("PARKLAND PA SCH DIST GORate/Coupon:5%Matures:2033-02-01", None, None, AssetCategory.MUNICIPAL_BOND, False),
        ("PENNSYLVANIA ST GORate/Coupon:5%Matures:2035-04-01", None, None, AssetCategory.MUNICIPAL_BOND, False),
        ("PENNSYLVANIA ST TPK COMMN TPK REVRate/Coupon:5%Matures:2027-12-01", None, None, AssetCategory.MUNICIPAL_BOND, False),
        ("PENNSYLVANIA ST TPK COMMN TPK REVRate/Coupon:5%Matures:2030-12-01", None, None, AssetCategory.MUNICIPAL_BOND, False),
        ("Pfizer Inc 717081FE8Rate/Coupon:4.500%Matures:2032-11-15", None, None, AssetCategory.CORPORATE_BOND, False),
        ("PHILADELPHIA PA WTR & WASTEWTR REV BDSRate/Coupon:5.25%Matures:2054-09-01", None, None, AssetCategory.MUNICIPAL_BOND, False),
        ("UPPER DUBLIN PA SCH DIST GORate/Coupon:4%Matures:2032-05-15", None, None, AssetCategory.MUNICIPAL_BOND, False),
    ]

    for asset_name, ticker, asset_type, expected_category, include in cases:
        result = resolve_transaction(asset_name, ticker=ticker, asset_type_code=asset_type)
        assert result.category == expected_category, asset_name
        assert result.include_in_signal is include, asset_name
