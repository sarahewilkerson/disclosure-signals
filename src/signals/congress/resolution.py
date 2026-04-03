from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class AssetCategory(Enum):
    COMMON_STOCK = "common_stock"
    PREFERRED_STOCK = "preferred_stock"
    SINGLE_STOCK_ETF = "single_stock_etf"
    SECTOR_ETF = "sector_etf"
    BROAD_INDEX_ETF = "broad_index_etf"
    MUTUAL_FUND = "mutual_fund"
    OPTION = "option"
    CORPORATE_BOND = "corporate_bond"
    TREASURY = "treasury"
    MUNICIPAL_BOND = "municipal_bond"
    PRIVATE_PLACEMENT = "private_placement"
    CRYPTO = "crypto"
    UNKNOWN = "unknown"


@dataclass
class ResolutionResult:
    resolved_ticker: str | None
    resolved_company: str | None
    category: AssetCategory
    resolution_method: str
    resolution_confidence: float
    include_in_signal: bool
    exclusion_reason: str | None
    signal_relevance_weight: float


BROAD_INDEX_ETFS = {
    "SPY", "VOO", "IVV", "QQQ", "QQQM", "VTI", "ITOT", "VT", "ACWI", "DIA",
    "IWM", "IWV", "VEA", "IEFA", "VWO", "IEMG", "BND", "AGG", "TLT", "IEF", "SHY",
}
SECTOR_ETFS = {
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLY", "XLP", "XLB", "XLU", "XLRE",
    "VGT", "VHT", "VFH", "VDE", "VIS", "VCR", "VDC", "VAW", "VPU", "VNQ",
    "SMH", "XBI", "IBB", "ARKK", "ARKG", "XOP", "OIH", "GDX", "GDXJ", "KRE", "KBE", "IAT", "IYR",
}
SINGLE_STOCK_ETFS = {"TSLL", "TSLQ", "NVDL", "NVDS", "AAPD", "AAPU", "MSFO", "MSFD", "AMZD", "AMZU", "GOOGL", "GOOLD", "METD", "METU"}
TREASURY_PATTERNS = [r"treasury", r"t-?bill", r"t-?bond", r"t-?note", r"us\s+gov", r"u\.s\.\s+gov", r"united\s+states.*(?:bond|note|bill)"]
MUNI_PATTERNS = [r"\bgo\b.*(?:call|bond|bds)", r"municipal", r"\bst\b.*(?:sales|tax|rev)", r"city\s+(?:&|and)", r"county\s+(?:&|and)", r"rev(?:enue)?\s+(?:bond|jr)", r"utx\s+due", r"\bn\s*y\b.*\bgo\b", r"\bgo\s+bds\b", r"\bsch\s+dist\b", r"\barpt\b", r"\bwtr\b", r"\bcnty\b", r"\bohio\b.*\bgo\b", r"\bpa\b.*\brev\b"]
PRIVATE_PATTERNS = [r",\s*llc\b", r",\s*lp\b", r"promissory\s+note", r"private\s+equity", r"venture\s+fund", r"capital\s+fund", r"partners?\s+llc", r"allocate\s+\d{4}\s+lp"]
MUTUAL_FUND_PATTERNS = [r"\bfund\b(?!.*etf)", r"portfolio", r"retirement", r"401k", r"ira\s+", r"target\s+date", r"money\s+market", r"admiral\s+shares", r"\binstl\b", r"\binst!\b", r"harding\s+loevner", r"dimensional", r"\bdfa\b", r"core\s+equity"]
ETF_NAME_PATTERNS = [r"\betf\b", r"spdr", r"invesco\s+qqq", r"dow\s+jones", r"trust\s+nysearca", r"total\s+stock\s+market"]
CORPORATE_BOND_PATTERNS = [r"structured\s+note", r"linked\s+note", r"hybrid\s+perpetual", r"rate/coupon", r"matures:\d", r"\b\d{6}[a-z0-9]{2}\b"]
CRYPTO_PATTERNS = [r"bitcoin", r"ethereum", r"crypto", r"blockchain", r"digital\s+asset"]


class EntityResolver:
    def __init__(self) -> None:
        self._treasury_re = re.compile("|".join(TREASURY_PATTERNS), re.IGNORECASE)
        self._muni_re = re.compile("|".join(MUNI_PATTERNS), re.IGNORECASE)
        self._private_re = re.compile("|".join(PRIVATE_PATTERNS), re.IGNORECASE)
        self._mutual_fund_re = re.compile("|".join(MUTUAL_FUND_PATTERNS), re.IGNORECASE)
        self._etf_name_re = re.compile("|".join(ETF_NAME_PATTERNS), re.IGNORECASE)
        self._corporate_bond_re = re.compile("|".join(CORPORATE_BOND_PATTERNS), re.IGNORECASE)
        self._crypto_re = re.compile("|".join(CRYPTO_PATTERNS), re.IGNORECASE)

    def resolve(self, asset_name: str, ticker: str | None = None, asset_type_code: str | None = None) -> ResolutionResult:
        asset_lower = asset_name.lower() if asset_name else ""
        ticker_upper = ticker.upper().strip() if ticker else None
        category = self._classify_asset(asset_lower, ticker_upper, asset_type_code)
        include, exclusion_reason = self._should_include(category, ticker_upper)
        confidence = self._calculate_confidence(ticker_upper, category, asset_type_code)
        signal_weight = self._get_signal_weight(category, include)
        if ticker_upper:
            method = "extracted"
        elif not include:
            method = "excluded"
        else:
            method = "unresolved"
        return ResolutionResult(
            resolved_ticker=ticker_upper if include else None,
            resolved_company=asset_name if ticker_upper else None,
            category=category,
            resolution_method=method,
            resolution_confidence=confidence,
            include_in_signal=include,
            exclusion_reason=exclusion_reason,
            signal_relevance_weight=signal_weight,
        )

    def _classify_asset(self, asset_lower: str, ticker: str | None, asset_type_code: str | None) -> AssetCategory:
        preferred_patterns = ["depositary shares", "depositary share", "series a", "series b", "series c", "preferred", "pfd"]
        if any(p in asset_lower for p in preferred_patterns) or re.search(r"\d+\.\d+%\s+series", asset_lower):
            return AssetCategory.PREFERRED_STOCK
        if asset_type_code:
            code_map = {
                "ST": AssetCategory.COMMON_STOCK,
                "OP": AssetCategory.OPTION,
                "GS": AssetCategory.TREASURY,
                "MF": AssetCategory.MUTUAL_FUND,
                "EF": AssetCategory.SECTOR_ETF,
                "BD": AssetCategory.CORPORATE_BOND,
                "CS": AssetCategory.CRYPTO,
                "OT": AssetCategory.UNKNOWN,
                "AB": AssetCategory.PRIVATE_PLACEMENT,
            }
            category = code_map.get(asset_type_code, AssetCategory.UNKNOWN)
            if category == AssetCategory.SECTOR_ETF and ticker:
                if ticker in BROAD_INDEX_ETFS:
                    return AssetCategory.BROAD_INDEX_ETF
                if ticker in SINGLE_STOCK_ETFS:
                    return AssetCategory.SINGLE_STOCK_ETF
                if ticker in SECTOR_ETFS:
                    return AssetCategory.SECTOR_ETF
            if category != AssetCategory.UNKNOWN:
                return category
        if self._treasury_re.search(asset_lower):
            return AssetCategory.TREASURY
        if self._muni_re.search(asset_lower):
            return AssetCategory.MUNICIPAL_BOND
        if self._private_re.search(asset_lower):
            return AssetCategory.PRIVATE_PLACEMENT
        if self._etf_name_re.search(asset_lower):
            if "admiral shares" in asset_lower:
                return AssetCategory.MUTUAL_FUND
            if ticker == "DIA" or "dow jones" in asset_lower or "qqq" in asset_lower or "total stock market" in asset_lower:
                return AssetCategory.BROAD_INDEX_ETF
            return AssetCategory.SECTOR_ETF if ticker in SECTOR_ETFS else AssetCategory.BROAD_INDEX_ETF
        if self._mutual_fund_re.search(asset_lower):
            return AssetCategory.MUTUAL_FUND
        if self._corporate_bond_re.search(asset_lower):
            return AssetCategory.CORPORATE_BOND
        if self._crypto_re.search(asset_lower):
            return AssetCategory.CRYPTO
        if ticker:
            if ticker in BROAD_INDEX_ETFS:
                return AssetCategory.BROAD_INDEX_ETF
            if ticker in SECTOR_ETFS:
                return AssetCategory.SECTOR_ETF
            if ticker in SINGLE_STOCK_ETFS:
                return AssetCategory.SINGLE_STOCK_ETF
            if "depositary" in asset_lower or "preferred" in asset_lower:
                return AssetCategory.PREFERRED_STOCK
            return AssetCategory.COMMON_STOCK
        return AssetCategory.UNKNOWN

    def _should_include(self, category: AssetCategory, ticker: str | None) -> tuple[bool, str | None]:
        exclusions = {
            AssetCategory.BROAD_INDEX_ETF: "asset_excluded:broad_index_etf",
            AssetCategory.MUTUAL_FUND: "asset_excluded:mutual_fund",
            AssetCategory.TREASURY: "asset_excluded:treasury",
            AssetCategory.MUNICIPAL_BOND: "asset_excluded:municipal_bond",
            AssetCategory.CORPORATE_BOND: "asset_excluded:corporate_bond",
            AssetCategory.PRIVATE_PLACEMENT: "asset_excluded:private_placement",
        }
        if category in exclusions:
            return False, exclusions[category]
        if category == AssetCategory.CRYPTO:
            return False, "asset_excluded:crypto"
        if category == AssetCategory.UNKNOWN and not ticker:
            return False, "asset_excluded:unresolved"
        return True, None

    def _calculate_confidence(self, ticker: str | None, category: AssetCategory, asset_type_code: str | None) -> float:
        if not ticker:
            return 0.0
        confidence = 0.5
        if asset_type_code:
            confidence += 0.2
        if ticker in BROAD_INDEX_ETFS | SECTOR_ETFS | SINGLE_STOCK_ETFS:
            confidence += 0.2
        if category == AssetCategory.COMMON_STOCK and re.match(r"^[A-Z]{1,5}$", ticker):
            confidence += 0.1
        return min(confidence, 1.0)

    def _get_signal_weight(self, category: AssetCategory, include: bool) -> float:
        if not include:
            return 0.0
        weights = {
            AssetCategory.COMMON_STOCK: 1.0,
            AssetCategory.SINGLE_STOCK_ETF: 1.0,
            AssetCategory.SECTOR_ETF: 0.8,
            AssetCategory.OPTION: 0.7,
            AssetCategory.PREFERRED_STOCK: 0.6,
            AssetCategory.UNKNOWN: 0.3,
        }
        return weights.get(category, 0.5)


def resolve_transaction(asset_name: str, ticker: str | None = None, asset_type_code: str | None = None) -> ResolutionResult:
    return EntityResolver().resolve(asset_name, ticker, asset_type_code)
