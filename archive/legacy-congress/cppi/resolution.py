"""Entity resolution module for mapping assets to tickers.

Handles:
- Ticker validation and normalization
- Asset name → ticker resolution
- Exclusion policy (bonds, mutual funds, broad ETFs)
- Confidence scoring
"""

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class AssetCategory(Enum):
    """Categories for asset classification."""

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
    """Result of entity resolution for a transaction."""

    resolved_ticker: Optional[str]
    resolved_company: Optional[str]
    category: AssetCategory
    resolution_method: str  # 'extracted', 'lookup', 'fuzzy', 'manual', 'unresolved'
    resolution_confidence: float  # 0.0 to 1.0
    include_in_signal: bool
    exclusion_reason: Optional[str]
    signal_relevance_weight: float  # Weight for signal calculation


# Broad index ETFs that should be excluded (non-informative)
BROAD_INDEX_ETFS = {
    "SPY", "VOO", "IVV",  # S&P 500
    "QQQ", "QQQM",  # Nasdaq 100
    "VTI", "ITOT",  # Total US market
    "VT", "ACWI",  # Total world
    "DIA",  # Dow Jones
    "IWM", "IWV",  # Russell
    "VEA", "IEFA",  # Developed international
    "VWO", "IEMG",  # Emerging markets
    "BND", "AGG",  # Aggregate bonds
    "TLT", "IEF", "SHY",  # Treasury ETFs
}

# Sector ETFs that are included but flagged
SECTOR_ETFS = {
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLY", "XLP", "XLB", "XLU", "XLRE",  # SPDR sectors
    "VGT", "VHT", "VFH", "VDE", "VIS", "VCR", "VDC", "VAW", "VPU", "VNQ",  # Vanguard sectors
    "SMH", "XBI", "IBB", "ARKK", "ARKG",  # Tech/biotech
    "XOP", "OIH", "GDX", "GDXJ",  # Energy/metals
    "KRE", "KBE", "IAT",  # Financials
    "IYR", "VNQ",  # Real estate
}

# Single-stock ETFs (leveraged exposure to individual stocks)
SINGLE_STOCK_ETFS = {
    "TSLL", "TSLQ",  # Tesla
    "NVDL", "NVDS",  # Nvidia
    "AAPD", "AAPU",  # Apple
    "MSFO", "MSFD",  # Microsoft
    "AMZD", "AMZU",  # Amazon
    "GOOGL", "GOOLD",  # Google
    "METD", "METU",  # Meta
}

# Treasury-related patterns
TREASURY_PATTERNS = [
    r"treasury",
    r"t-?bill",
    r"t-?bond",
    r"t-?note",
    r"us\s+gov",
    r"u\.s\.\s+gov",
    r"united\s+states.*(?:bond|note|bill)",
]

# Municipal bond patterns
MUNI_PATTERNS = [
    r"\bgo\b.*(?:call|bond|bds)",  # General obligation (bonds, bds)
    r"municipal",
    r"\bst\b.*(?:sales|tax|rev)",  # State
    r"city\s+(?:&|and)",
    r"county\s+(?:&|and)",
    r"rev(?:enue)?\s+(?:bond|jr)",
    r"utx\s+due",  # Unrefunded tax
    r"\bn\s*y\b.*\bgo\b",  # New York GO bonds
    r"\bgo\s+bds\b",  # GO BDS (general obligation bonds)
]

# Private placement patterns
PRIVATE_PATTERNS = [
    r",\s*llc\b",
    r",\s*lp\b",
    r",\s*inc\.\s*$",
    r"promissory\s+note",
    r"private\s+equity",
    r"venture\s+fund",
    r"capital\s+fund",
]

# Mutual fund patterns
MUTUAL_FUND_PATTERNS = [
    r"\bfund\b(?!.*etf)",
    r"portfolio",
    r"retirement",
    r"401k",
    r"ira\s+",
    r"target\s+date",
    r"money\s+market",
]

# Crypto patterns
CRYPTO_PATTERNS = [
    r"bitcoin",
    r"ethereum",
    r"crypto",
    r"blockchain",
    r"digital\s+asset",
]


class EntityResolver:
    """Resolves asset names to tickers and applies exclusion rules."""

    def __init__(self):
        """Initialize the resolver."""
        # Compile regex patterns for efficiency
        self._treasury_re = re.compile(
            "|".join(TREASURY_PATTERNS), re.IGNORECASE
        )
        self._muni_re = re.compile("|".join(MUNI_PATTERNS), re.IGNORECASE)
        self._private_re = re.compile("|".join(PRIVATE_PATTERNS), re.IGNORECASE)
        self._mutual_fund_re = re.compile(
            "|".join(MUTUAL_FUND_PATTERNS), re.IGNORECASE
        )
        self._crypto_re = re.compile("|".join(CRYPTO_PATTERNS), re.IGNORECASE)

    def resolve(
        self,
        asset_name: str,
        ticker: Optional[str] = None,
        asset_type_code: Optional[str] = None,
    ) -> ResolutionResult:
        """Resolve an asset to a ticker and determine inclusion status.

        Args:
            asset_name: Raw asset name from the filing
            ticker: Ticker already extracted from PDF (if any)
            asset_type_code: Asset type code from PDF (ST, OP, GS, etc.)

        Returns:
            ResolutionResult with ticker, category, and inclusion status
        """
        # Normalize inputs
        asset_lower = asset_name.lower() if asset_name else ""
        ticker_upper = ticker.upper().strip() if ticker else None

        # Step 1: Classify the asset
        category = self._classify_asset(asset_lower, ticker_upper, asset_type_code)

        # Step 2: Determine exclusion
        include, exclusion_reason = self._should_include(category, ticker_upper)

        # Step 3: Calculate confidence and weights
        resolution_confidence = self._calculate_confidence(
            ticker_upper, category, asset_type_code
        )
        signal_weight = self._get_signal_weight(category, include)

        # Step 4: Determine resolution method
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
            resolution_confidence=resolution_confidence,
            include_in_signal=include,
            exclusion_reason=exclusion_reason,
            signal_relevance_weight=signal_weight,
        )

    def _classify_asset(
        self,
        asset_lower: str,
        ticker: Optional[str],
        asset_type_code: Optional[str],
    ) -> AssetCategory:
        """Classify an asset into a category."""
        # Check for preferred stock patterns first (override ST code)
        preferred_patterns = [
            "depositary shares",
            "depositary share",
            "series a",
            "series b",
            "series c",
            "preferred",
            "pfd",
        ]
        # Also check for "X.XX% Series" pattern (preferred with dividend rate)
        if any(p in asset_lower for p in preferred_patterns):
            return AssetCategory.PREFERRED_STOCK
        if re.search(r"\d+\.\d+%\s+series", asset_lower):
            return AssetCategory.PREFERRED_STOCK

        # Use asset type code if available
        if asset_type_code:
            code_map = {
                "ST": AssetCategory.COMMON_STOCK,
                "OP": AssetCategory.OPTION,
                "GS": AssetCategory.TREASURY,
                "MF": AssetCategory.MUTUAL_FUND,
                "EF": AssetCategory.SECTOR_ETF,  # Classify ETFs further below
                "BD": AssetCategory.CORPORATE_BOND,
                "CS": AssetCategory.CRYPTO,
                "OT": AssetCategory.UNKNOWN,
                "AB": AssetCategory.PRIVATE_PLACEMENT,  # Alternative/private
            }
            category = code_map.get(asset_type_code, AssetCategory.UNKNOWN)

            # Refine ETF classification if we have a ticker
            if category == AssetCategory.SECTOR_ETF and ticker:
                if ticker in BROAD_INDEX_ETFS:
                    return AssetCategory.BROAD_INDEX_ETF
                if ticker in SINGLE_STOCK_ETFS:
                    return AssetCategory.SINGLE_STOCK_ETF
                if ticker in SECTOR_ETFS:
                    return AssetCategory.SECTOR_ETF

            if category != AssetCategory.UNKNOWN:
                return category

        # Pattern-based classification
        if self._treasury_re.search(asset_lower):
            return AssetCategory.TREASURY

        if self._muni_re.search(asset_lower):
            return AssetCategory.MUNICIPAL_BOND

        if self._private_re.search(asset_lower):
            return AssetCategory.PRIVATE_PLACEMENT

        if self._mutual_fund_re.search(asset_lower):
            return AssetCategory.MUTUAL_FUND

        if self._crypto_re.search(asset_lower):
            return AssetCategory.CRYPTO

        # Check ticker-based classification
        if ticker:
            if ticker in BROAD_INDEX_ETFS:
                return AssetCategory.BROAD_INDEX_ETF
            if ticker in SECTOR_ETFS:
                return AssetCategory.SECTOR_ETF
            if ticker in SINGLE_STOCK_ETFS:
                return AssetCategory.SINGLE_STOCK_ETF

            # Check for preferred stock patterns
            if "depositary" in asset_lower or "preferred" in asset_lower:
                return AssetCategory.PREFERRED_STOCK

            # Default to common stock if we have a valid ticker
            return AssetCategory.COMMON_STOCK

        return AssetCategory.UNKNOWN

    def _should_include(
        self, category: AssetCategory, ticker: Optional[str]
    ) -> tuple[bool, Optional[str]]:
        """Determine if asset should be included in signal.

        Returns:
            Tuple of (include, exclusion_reason)
        """
        # Exclusion rules per plan
        exclusions = {
            AssetCategory.BROAD_INDEX_ETF: "broad_index_etf",
            AssetCategory.MUTUAL_FUND: "mutual_fund",
            AssetCategory.TREASURY: "treasury",
            AssetCategory.MUNICIPAL_BOND: "municipal_bond",
            AssetCategory.CORPORATE_BOND: "corporate_bond",
            AssetCategory.PRIVATE_PLACEMENT: "private_placement",
        }

        if category in exclusions:
            return False, f"asset_excluded:{exclusions[category]}"

        # Crypto excluded for now
        if category == AssetCategory.CRYPTO:
            return False, "asset_excluded:crypto"

        # Unknown without ticker should be excluded
        if category == AssetCategory.UNKNOWN and not ticker:
            return False, "asset_excluded:unresolved"

        return True, None

    def _calculate_confidence(
        self,
        ticker: Optional[str],
        category: AssetCategory,
        asset_type_code: Optional[str],
    ) -> float:
        """Calculate resolution confidence score.

        Returns:
            Confidence score from 0.0 to 1.0
        """
        if not ticker:
            return 0.0

        confidence = 0.5  # Base confidence for having a ticker

        # Boost for having asset type code
        if asset_type_code:
            confidence += 0.2

        # Boost for known ETF tickers (validated)
        if ticker in BROAD_INDEX_ETFS | SECTOR_ETFS | SINGLE_STOCK_ETFS:
            confidence += 0.2

        # Boost for common stock with standard ticker format
        if category == AssetCategory.COMMON_STOCK:
            if re.match(r"^[A-Z]{1,5}$", ticker):
                confidence += 0.1

        return min(confidence, 1.0)

    def _get_signal_weight(
        self, category: AssetCategory, include: bool
    ) -> float:
        """Get signal relevance weight for an asset category.

        Returns:
            Weight from 0.0 to 1.0
        """
        if not include:
            return 0.0

        weights = {
            AssetCategory.COMMON_STOCK: 1.0,
            AssetCategory.SINGLE_STOCK_ETF: 1.0,
            AssetCategory.SECTOR_ETF: 0.8,  # Flagged, slightly lower weight
            AssetCategory.OPTION: 0.7,  # Higher uncertainty
            AssetCategory.PREFERRED_STOCK: 0.6,
            AssetCategory.UNKNOWN: 0.3,  # Low confidence
        }

        return weights.get(category, 0.5)


def resolve_transaction(
    asset_name: str,
    ticker: Optional[str] = None,
    asset_type_code: Optional[str] = None,
) -> ResolutionResult:
    """Convenience function to resolve a single transaction.

    Args:
        asset_name: Raw asset name
        ticker: Ticker already extracted (if any)
        asset_type_code: Asset type code (ST, OP, etc.)

    Returns:
        ResolutionResult
    """
    resolver = EntityResolver()
    return resolver.resolve(asset_name, ticker, asset_type_code)


def calculate_resolution_metrics(
    results: list[tuple[ResolutionResult, int]]
) -> dict:
    """Calculate resolution metrics from a list of results.

    Args:
        results: List of (ResolutionResult, estimated_amount) tuples

    Returns:
        Dictionary of metrics
    """
    total_count = len(results)
    total_value = sum(amt for _, amt in results)

    included = [(r, a) for r, a in results if r.include_in_signal]
    excluded = [(r, a) for r, a in results if not r.include_in_signal]
    resolved = [(r, a) for r, a in included if r.resolved_ticker]

    # Category breakdown
    by_category: dict[AssetCategory, list] = {}
    for r, a in results:
        if r.category not in by_category:
            by_category[r.category] = []
        by_category[r.category].append((r, a))

    # Exclusion reasons
    exclusion_reasons: dict[str, int] = {}
    for r, _ in excluded:
        reason = r.exclusion_reason or "unknown"
        exclusion_reasons[reason] = exclusion_reasons.get(reason, 0) + 1

    # Resolution rates by category (for included only)
    resolution_by_category = {}
    for cat, items in by_category.items():
        included_items = [i for i in items if i[0].include_in_signal]
        if included_items:
            resolved_items = [i for i in included_items if i[0].resolved_ticker]
            resolution_by_category[cat.value] = {
                "count": len(included_items),
                "resolved": len(resolved_items),
                "rate": len(resolved_items) / len(included_items),
            }

    return {
        "total_transactions": total_count,
        "total_estimated_value": total_value,
        "included_count": len(included),
        "included_value": sum(a for _, a in included),
        "excluded_count": len(excluded),
        "excluded_value": sum(a for _, a in excluded),
        "resolved_count": len(resolved),
        "resolved_value": sum(a for _, a in resolved),
        "resolution_rate_by_count": len(resolved) / len(included) if included else 0,
        "resolution_rate_by_value": (
            sum(a for _, a in resolved) / sum(a for _, a in included)
            if included else 0
        ),
        "exclusion_reasons": exclusion_reasons,
        "resolution_by_category": resolution_by_category,
    }
