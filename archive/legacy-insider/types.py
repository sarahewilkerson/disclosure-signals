"""
Type definitions for the Insider Trading Signal Engine.

Provides TypedDict definitions for database row structures
to enable better type checking and IDE support.
"""

from typing import TypedDict, Optional


class CompanyRow(TypedDict):
    """Row structure for the companies table."""
    cik: str
    ticker: Optional[str]
    company_name: Optional[str]
    fortune_rank: Optional[int]
    revenue: Optional[float]
    sector: Optional[str]
    resolved_at: Optional[str]


class FilingRow(TypedDict):
    """Row structure for the filings table."""
    accession_number: str
    cik_issuer: Optional[str]
    cik_owner: Optional[str]
    owner_name: Optional[str]
    officer_title: Optional[str]
    is_officer: int  # 0 or 1
    is_director: int  # 0 or 1
    is_ten_pct_owner: int  # 0 or 1
    is_other: int  # 0 or 1
    is_amendment: int  # 0 or 1
    amendment_type: Optional[str]
    period_of_report: Optional[str]
    aff10b5one: int  # 0 or 1, structured Rule 10b5-1 indicator
    additional_owners: Optional[str]  # JSON array of additional owner dicts
    filing_date: Optional[str]
    xml_url: Optional[str]
    raw_xml_path: Optional[str]
    parsed_at: Optional[str]
    parse_error: Optional[str]


class TransactionRow(TypedDict):
    """Row structure for the transactions table."""
    id: int
    accession_number: Optional[str]
    cik_issuer: Optional[str]
    cik_owner: Optional[str]
    owner_name: Optional[str]
    officer_title: Optional[str]
    security_title: Optional[str]
    transaction_date: Optional[str]
    transaction_code: Optional[str]
    equity_swap: int  # 0 or 1
    shares: Optional[float]
    price_per_share: Optional[float]
    total_value: Optional[float]
    shares_after: Optional[float]
    ownership_nature: Optional[str]  # 'D' or 'I'
    indirect_entity: Optional[str]
    is_derivative: int  # 0 or 1
    underlying_security: Optional[str]
    footnotes: Optional[str]
    # Classification fields
    role_class: Optional[str]
    transaction_class: Optional[str]
    is_likely_planned: int  # 0 or 1
    is_discretionary: int  # 0 or 1
    pct_holdings_changed: Optional[float]
    include_in_signal: int  # 0 or 1
    exclusion_reason: Optional[str]


class CompanyScoreRow(TypedDict):
    """Row structure for the company_scores table."""
    cik: str
    ticker: Optional[str]
    window_days: int
    computed_at: Optional[str]
    signal: Optional[str]  # 'bullish', 'bearish', 'neutral', 'insufficient'
    score: Optional[float]
    confidence: Optional[float]
    confidence_tier: Optional[str]  # 'insufficient', 'low', 'moderate', 'high'
    buy_count: int
    sell_count: int
    unique_buyers: int
    unique_sellers: int
    net_buy_value: Optional[float]
    explanation: Optional[str]
    filing_accessions: Optional[str]  # JSON list


class AggregateIndexRow(TypedDict):
    """Row structure for the aggregate_index table."""
    window_days: int
    computed_at: Optional[str]
    risk_appetite_index: Optional[float]
    bullish_breadth: Optional[float]
    bearish_breadth: Optional[float]
    neutral_pct: Optional[float]
    insufficient_pct: Optional[float]
    ceo_cfo_only_index: Optional[float]
    sector_balanced_index: Optional[float]
    cyclical_score: Optional[float]
    defensive_score: Optional[float]
    sector_breakdown: Optional[str]  # JSON {sector: score}
    total_companies: int
    companies_with_signal: int


# Type aliases for common return types
CompanyList = list[CompanyRow]
FilingList = list[FilingRow]
TransactionList = list[TransactionRow]
ScoreList = list[CompanyScoreRow]
