"""
Validation framework for comparing CPPI data against external sources.

Computes match rates and identifies discrepancies between CPPI
extracted data and vendor data sources.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of matching a single transaction."""

    cppi_filing_id: str
    cppi_ticker: Optional[str]
    cppi_transaction_type: str
    cppi_date: Optional[str]
    cppi_amount_min: Optional[int]
    cppi_amount_max: Optional[int]

    external_ticker: Optional[str]
    external_transaction_type: Optional[str]
    external_date: Optional[str]
    external_amount_min: Optional[int]
    external_amount_max: Optional[int]

    is_matched: bool
    match_score: float  # 0-1, how good the match is
    discrepancies: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "cppi_filing_id": self.cppi_filing_id,
            "cppi_ticker": self.cppi_ticker,
            "cppi_transaction_type": self.cppi_transaction_type,
            "cppi_date": self.cppi_date,
            "cppi_amount_min": self.cppi_amount_min,
            "cppi_amount_max": self.cppi_amount_max,
            "external_ticker": self.external_ticker,
            "external_transaction_type": self.external_transaction_type,
            "external_date": self.external_date,
            "external_amount_min": self.external_amount_min,
            "external_amount_max": self.external_amount_max,
            "is_matched": self.is_matched,
            "match_score": self.match_score,
            "discrepancies": self.discrepancies,
        }


@dataclass
class ValidationReport:
    """Complete validation report."""

    source: str  # 'quiver' | 'capitol_trades'
    validated_at: datetime
    cppi_transaction_count: int
    external_transaction_count: int
    matched_count: int
    unmatched_cppi_count: int
    unmatched_external_count: int
    match_rate: float  # 0-1
    ticker_match_rate: float  # When matched, how often tickers agree
    amount_match_rate: float  # When matched, how often amounts agree
    discrepancy_summary: dict  # Categorized discrepancies
    sample_discrepancies: list[MatchResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "source": self.source,
            "validated_at": self.validated_at.isoformat(),
            "cppi_transaction_count": self.cppi_transaction_count,
            "external_transaction_count": self.external_transaction_count,
            "matched_count": self.matched_count,
            "unmatched_cppi_count": self.unmatched_cppi_count,
            "unmatched_external_count": self.unmatched_external_count,
            "match_rate": self.match_rate,
            "ticker_match_rate": self.ticker_match_rate,
            "amount_match_rate": self.amount_match_rate,
            "discrepancy_summary": self.discrepancy_summary,
            "sample_discrepancies": [d.to_dict() for d in self.sample_discrepancies],
        }


def normalize_transaction_type(txn_type: str) -> str:
    """Normalize transaction type for comparison."""
    if not txn_type:
        return "unknown"

    txn_lower = txn_type.lower()

    if "purchase" in txn_lower or "buy" in txn_lower:
        return "purchase"
    elif "sale" in txn_lower or "sell" in txn_lower:
        return "sale"
    elif "exchange" in txn_lower:
        return "exchange"
    else:
        return "unknown"


def normalize_ticker(ticker: str) -> str:
    """Normalize ticker symbol for comparison."""
    if not ticker:
        return ""
    return ticker.upper().strip()


def compare_amounts(
    cppi_min: Optional[int],
    cppi_max: Optional[int],
    ext_min: Optional[int],
    ext_max: Optional[int],
) -> tuple[bool, str]:
    """
    Compare amount ranges, accounting for reporting variations.

    Returns:
        (is_match, discrepancy_description)
    """
    if cppi_min is None or cppi_max is None:
        if ext_min is None or ext_max is None:
            return True, ""  # Both missing is a match
        return False, "CPPI missing amount"

    if ext_min is None or ext_max is None:
        return False, "External missing amount"

    # Check for overlap in ranges
    if cppi_max < ext_min or cppi_min > ext_max:
        return False, f"Amount mismatch: ${cppi_min:,}-${cppi_max:,} vs ${ext_min:,}-${ext_max:,}"

    # Check for significant difference (>50% of midpoint)
    cppi_mid = (cppi_min + cppi_max) / 2
    ext_mid = (ext_min + ext_max) / 2

    if cppi_mid > 0:
        diff_ratio = abs(cppi_mid - ext_mid) / cppi_mid
        if diff_ratio > 0.5:
            return False, f"Large amount difference: {diff_ratio:.0%}"

    return True, ""


def match_transactions(
    cppi_txns: list[dict],
    external_txns: list,
    date_tolerance_days: int = 7,
) -> tuple[list[MatchResult], list[dict], list]:
    """
    Match CPPI transactions against external transactions.

    Args:
        cppi_txns: CPPI transaction dicts
        external_txns: External transaction objects (with to_dict method or dict)
        date_tolerance_days: Days of tolerance for date matching

    Returns:
        (matched_results, unmatched_cppi, unmatched_external)
    """
    matched_results = []
    unmatched_cppi = []
    external_used = set()

    for cppi_txn in cppi_txns:
        cppi_ticker = normalize_ticker(cppi_txn.get("resolved_ticker", "") or "")
        cppi_type = normalize_transaction_type(cppi_txn.get("transaction_type", ""))
        cppi_date = cppi_txn.get("execution_date")

        best_match = None
        best_score = 0.0

        for i, ext_txn in enumerate(external_txns):
            if i in external_used:
                continue

            # Convert to dict if needed
            ext_dict = ext_txn.to_dict() if hasattr(ext_txn, "to_dict") else ext_txn

            ext_ticker = normalize_ticker(ext_dict.get("ticker", ""))
            ext_type = normalize_transaction_type(ext_dict.get("transaction_type", ""))
            ext_date = ext_dict.get("transaction_date")

            # Calculate match score
            score = 0.0
            discrepancies = []

            # Ticker match (most important)
            if cppi_ticker and ext_ticker:
                if cppi_ticker == ext_ticker:
                    score += 0.5
                else:
                    discrepancies.append(f"Ticker: {cppi_ticker} vs {ext_ticker}")

            # Transaction type match
            if cppi_type == ext_type:
                score += 0.2
            elif cppi_type != "unknown" and ext_type != "unknown":
                discrepancies.append(f"Type: {cppi_type} vs {ext_type}")

            # Date match
            if cppi_date and ext_date:
                try:
                    cppi_dt = datetime.strptime(cppi_date, "%Y-%m-%d") if isinstance(cppi_date, str) else cppi_date
                    ext_dt = datetime.strptime(ext_date, "%Y-%m-%d") if isinstance(ext_date, str) else ext_date

                    if hasattr(ext_dt, "date"):
                        ext_dt = ext_dt.date()
                    if hasattr(cppi_dt, "date"):
                        cppi_dt = cppi_dt.date()

                    day_diff = abs((cppi_dt - ext_dt).days) if hasattr(cppi_dt, "__sub__") else 999

                    if day_diff <= date_tolerance_days:
                        score += 0.2
                    else:
                        discrepancies.append(f"Date diff: {day_diff} days")
                except (ValueError, TypeError, AttributeError):
                    pass

            # Amount match
            amount_match, amount_disc = compare_amounts(
                cppi_txn.get("amount_min"),
                cppi_txn.get("amount_max"),
                ext_dict.get("amount_min"),
                ext_dict.get("amount_max"),
            )
            if amount_match:
                score += 0.1
            elif amount_disc:
                discrepancies.append(amount_disc)

            # Track best match
            if score > best_score and score >= 0.5:  # Minimum threshold
                best_match = (i, ext_dict, score, discrepancies)
                best_score = score

        if best_match:
            idx, ext_dict, score, discrepancies = best_match
            external_used.add(idx)

            matched_results.append(MatchResult(
                cppi_filing_id=cppi_txn.get("filing_id", ""),
                cppi_ticker=cppi_ticker,
                cppi_transaction_type=cppi_type,
                cppi_date=cppi_date,
                cppi_amount_min=cppi_txn.get("amount_min"),
                cppi_amount_max=cppi_txn.get("amount_max"),
                external_ticker=ext_dict.get("ticker"),
                external_transaction_type=ext_dict.get("transaction_type"),
                external_date=ext_dict.get("transaction_date"),
                external_amount_min=ext_dict.get("amount_min"),
                external_amount_max=ext_dict.get("amount_max"),
                is_matched=True,
                match_score=score,
                discrepancies=discrepancies,
            ))
        else:
            unmatched_cppi.append(cppi_txn)

    # Collect unmatched external
    unmatched_external = [
        t for i, t in enumerate(external_txns)
        if i not in external_used
    ]

    return matched_results, unmatched_cppi, unmatched_external


def validate_against_source(
    cppi_transactions: list[dict],
    external_transactions: list,
    source_name: str,
) -> ValidationReport:
    """
    Validate CPPI transactions against an external source.

    Args:
        cppi_transactions: CPPI transaction dictionaries
        external_transactions: External transactions (QuiverTransaction, etc.)
        source_name: Name of the external source

    Returns:
        ValidationReport with match rates and discrepancies
    """
    # Match transactions
    matched, unmatched_cppi, unmatched_external = match_transactions(
        cppi_transactions,
        external_transactions,
    )

    # Calculate metrics
    total_cppi = len(cppi_transactions)
    total_external = len(external_transactions)
    matched_count = len(matched)

    match_rate = matched_count / total_cppi if total_cppi > 0 else 0.0

    # Calculate ticker match rate for matched transactions
    ticker_matches = sum(
        1 for m in matched
        if m.cppi_ticker and m.external_ticker and m.cppi_ticker == m.external_ticker
    )
    ticker_match_rate = ticker_matches / matched_count if matched_count > 0 else 0.0

    # Calculate amount match rate
    amount_matches = sum(
        1 for m in matched
        if "Amount" not in " ".join(m.discrepancies)
    )
    amount_match_rate = amount_matches / matched_count if matched_count > 0 else 0.0

    # Summarize discrepancies
    discrepancy_summary: dict[str, int] = {}
    for m in matched:
        for d in m.discrepancies:
            category = d.split(":")[0]
            discrepancy_summary[category] = discrepancy_summary.get(category, 0) + 1

    # Sample discrepancies for review
    sample = [m for m in matched if m.discrepancies][:10]

    return ValidationReport(
        source=source_name,
        validated_at=datetime.now(),
        cppi_transaction_count=total_cppi,
        external_transaction_count=total_external,
        matched_count=matched_count,
        unmatched_cppi_count=len(unmatched_cppi),
        unmatched_external_count=len(unmatched_external),
        match_rate=match_rate,
        ticker_match_rate=ticker_match_rate,
        amount_match_rate=amount_match_rate,
        discrepancy_summary=discrepancy_summary,
        sample_discrepancies=sample,
    )


def format_validation_report(report: ValidationReport) -> str:
    """Format validation report as text."""
    lines = [
        "=" * 77,
        f"VALIDATION REPORT: {report.source.upper()}",
        f"Generated: {report.validated_at.strftime('%Y-%m-%d %H:%M')}",
        "=" * 77,
        "",
        "SUMMARY",
        "-" * 77,
        f"CPPI Transactions:     {report.cppi_transaction_count:,}",
        f"External Transactions: {report.external_transaction_count:,}",
        f"Matched:               {report.matched_count:,}",
        f"Unmatched (CPPI):      {report.unmatched_cppi_count:,}",
        f"Unmatched (External):  {report.unmatched_external_count:,}",
        "",
        "MATCH RATES",
        "-" * 77,
        f"Overall Match Rate:    {report.match_rate:.1%}",
        f"Ticker Agreement:      {report.ticker_match_rate:.1%}",
        f"Amount Agreement:      {report.amount_match_rate:.1%}",
        "",
    ]

    if report.discrepancy_summary:
        lines.extend([
            "DISCREPANCY CATEGORIES",
            "-" * 77,
        ])
        for category, count in sorted(report.discrepancy_summary.items(), key=lambda x: -x[1]):
            lines.append(f"  {category}: {count}")
        lines.append("")

    if report.sample_discrepancies:
        lines.extend([
            "SAMPLE DISCREPANCIES",
            "-" * 77,
        ])
        for m in report.sample_discrepancies[:5]:
            lines.append(f"  Filing: {m.cppi_filing_id}")
            lines.append(f"    CPPI: {m.cppi_ticker} {m.cppi_transaction_type} on {m.cppi_date}")
            lines.append(f"    External: {m.external_ticker} {m.external_transaction_type} on {m.external_date}")
            for d in m.discrepancies:
                lines.append(f"    -> {d}")
            lines.append("")

    lines.extend([
        "INTERPRETATION",
        "-" * 77,
        "- Match rate <50%: Significant data quality issues",
        "- Match rate 50-80%: Expected for different extraction methods",
        "- Match rate >80%: Strong data quality",
        "",
    ])

    return "\n".join(lines)
