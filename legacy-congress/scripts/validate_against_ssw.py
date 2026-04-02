#!/usr/bin/env python3
"""
Validate CPPI Senate transaction data against senate-stock-watcher-data.

Compares our parsed Senate transactions against the senate-stock-watcher-data
repository to identify coverage gaps and discrepancies.

Usage:
    python scripts/validate_against_ssw.py [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD]
"""

import argparse
import json
import logging
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.request import urlopen

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# External data source
# NOTE: senate-stock-watcher-data only covers 2012-2020, so it cannot validate
# our 2024+ data. This script is kept for historical comparison and as a template
# for future validation against updated external sources.
SSW_ALL_TRANSACTIONS_URL = (
    "https://raw.githubusercontent.com/timothycarambat/senate-stock-watcher-data"
    "/master/aggregate/all_transactions.json"
)

# CPPI database path
DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "cppi.db"


@dataclass
class ValidationResult:
    """Result of validation comparison."""

    ssw_senator_count: int
    cppi_senator_count: int
    ssw_transaction_count: int
    cppi_transaction_count: int
    senators_only_in_ssw: list[str]
    senators_only_in_cppi: list[str]
    senator_discrepancies: dict[str, dict[str, int]]  # senator -> {ssw: N, cppi: M}


def fetch_ssw_data() -> list[dict[str, Any]]:
    """Fetch all transactions from senate-stock-watcher-data."""
    logger.info("Fetching senate-stock-watcher-data from GitHub...")
    try:
        with urlopen(SSW_ALL_TRANSACTIONS_URL, timeout=60) as response:
            data = json.loads(response.read().decode("utf-8"))
            logger.info("Fetched %d senator records from SSW", len(data))
            return data
    except Exception as e:
        logger.error("Failed to fetch SSW data: %s", e)
        return []


def normalize_name(name: str) -> str:
    """Normalize senator name for comparison."""
    # Remove prefixes, suffixes, normalize whitespace
    name = name.strip().upper()
    # Remove common prefixes
    for prefix in ["SENATOR ", "SEN. ", "HON. "]:
        if name.startswith(prefix):
            name = name[len(prefix) :]
    # Remove suffixes like Jr., III, etc.
    for suffix in [" JR.", " JR", " III", " II", " IV"]:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    # Normalize whitespace and punctuation
    name = " ".join(name.split())
    return name


def parse_ssw_date(date_str: str) -> datetime | None:
    """Parse date from SSW format."""
    if not date_str:
        return None
    try:
        # SSW uses MM/DD/YYYY format
        return datetime.strptime(date_str, "%m/%d/%Y")
    except ValueError:
        return None


def extract_ssw_transactions(
    ssw_data: list[dict], from_date: datetime | None, to_date: datetime | None
) -> dict[str, list[dict]]:
    """Extract and organize SSW transactions by normalized senator name.

    SSW all_transactions.json is a flat array where each element is a transaction
    with a 'senator' field containing the senator's name.
    """
    result: dict[str, list[dict]] = defaultdict(list)

    for txn in ssw_data:
        # Get senator name from transaction
        senator_name = txn.get("senator", "")
        if not senator_name:
            continue

        normalized = normalize_name(senator_name)

        # Parse transaction date
        txn_date_str = txn.get("transaction_date", "")
        txn_date = parse_ssw_date(txn_date_str)

        # Filter by date range
        if from_date and txn_date and txn_date < from_date:
            continue
        if to_date and txn_date and txn_date > to_date:
            continue

        result[normalized].append(txn)

    return result


def extract_cppi_transactions(
    db_path: Path, from_date: datetime | None, to_date: datetime | None
) -> dict[str, list[dict]]:
    """Extract CPPI Senate transactions by normalized senator name."""
    result: dict[str, list[dict]] = defaultdict(list)

    if not db_path.exists():
        logger.error("Database not found: %s", db_path)
        return result

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Check if tables exist
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'"
    )
    if not cursor.fetchone():
        logger.error("transactions table not found in database")
        conn.close()
        return result

    # Build query with optional date filters
    query = """
        SELECT t.*, f.filer_name
        FROM transactions t
        JOIN filings f ON t.filing_id = f.filing_id
        WHERE f.chamber = 'senate'
    """
    params: list = []

    if from_date:
        query += " AND t.execution_date >= ?"
        params.append(from_date.strftime("%Y-%m-%d"))

    if to_date:
        query += " AND t.execution_date <= ?"
        params.append(to_date.strftime("%Y-%m-%d"))

    cursor.execute(query, params)

    for row in cursor.fetchall():
        filer_name = row["filer_name"] or ""
        normalized = normalize_name(filer_name)
        if not normalized:
            continue

        result[normalized].append(dict(row))

    conn.close()
    return result


def compare_data(
    ssw_by_senator: dict[str, list[dict]],
    cppi_by_senator: dict[str, list[dict]],
) -> ValidationResult:
    """Compare SSW and CPPI transaction data."""
    ssw_senators = set(ssw_by_senator.keys())
    cppi_senators = set(cppi_by_senator.keys())

    # Find differences
    only_in_ssw = sorted(ssw_senators - cppi_senators)
    only_in_cppi = sorted(cppi_senators - ssw_senators)

    # Calculate totals
    ssw_total = sum(len(txns) for txns in ssw_by_senator.values())
    cppi_total = sum(len(txns) for txns in cppi_by_senator.values())

    # Per-senator discrepancies (for senators in both datasets)
    discrepancies: dict[str, dict[str, int]] = {}
    common_senators = ssw_senators & cppi_senators

    for senator in common_senators:
        ssw_count = len(ssw_by_senator[senator])
        cppi_count = len(cppi_by_senator[senator])

        # Report if difference is > 20% or > 5 transactions
        if abs(ssw_count - cppi_count) > 5 or (
            min(ssw_count, cppi_count) > 0
            and abs(ssw_count - cppi_count) / max(ssw_count, cppi_count) > 0.2
        ):
            discrepancies[senator] = {"ssw": ssw_count, "cppi": cppi_count}

    return ValidationResult(
        ssw_senator_count=len(ssw_senators),
        cppi_senator_count=len(cppi_senators),
        ssw_transaction_count=ssw_total,
        cppi_transaction_count=cppi_total,
        senators_only_in_ssw=only_in_ssw,
        senators_only_in_cppi=only_in_cppi,
        senator_discrepancies=discrepancies,
    )


def print_report(result: ValidationResult, from_date: datetime | None, to_date: datetime | None):
    """Print validation report."""
    date_range = ""
    if from_date or to_date:
        start = from_date.strftime("%Y-%m-%d") if from_date else "beginning"
        end = to_date.strftime("%Y-%m-%d") if to_date else "present"
        date_range = f" ({start} to {end})"

    print("\n" + "=" * 70)
    print(f"CPPI vs Senate-Stock-Watcher-Data Validation Report{date_range}")
    print("=" * 70)

    # Summary counts
    print("\n## Summary")
    print(f"  SSW Senators:         {result.ssw_senator_count:,}")
    print(f"  CPPI Senators:        {result.cppi_senator_count:,}")
    print(f"  SSW Transactions:     {result.ssw_transaction_count:,}")
    print(f"  CPPI Transactions:    {result.cppi_transaction_count:,}")

    # Coverage calculation
    if result.ssw_transaction_count > 0:
        coverage = result.cppi_transaction_count / result.ssw_transaction_count * 100
        print(f"  CPPI Coverage:        {coverage:.1f}%")

    # Senators only in one dataset
    if result.senators_only_in_ssw:
        print(f"\n## Senators in SSW but NOT in CPPI ({len(result.senators_only_in_ssw)}):")
        for name in result.senators_only_in_ssw[:20]:  # Limit output
            print(f"    - {name}")
        if len(result.senators_only_in_ssw) > 20:
            print(f"    ... and {len(result.senators_only_in_ssw) - 20} more")

    if result.senators_only_in_cppi:
        print(f"\n## Senators in CPPI but NOT in SSW ({len(result.senators_only_in_cppi)}):")
        for name in result.senators_only_in_cppi[:20]:
            print(f"    - {name}")
        if len(result.senators_only_in_cppi) > 20:
            print(f"    ... and {len(result.senators_only_in_cppi) - 20} more")

    # Per-senator discrepancies
    if result.senator_discrepancies:
        print(f"\n## Significant Per-Senator Discrepancies ({len(result.senator_discrepancies)}):")
        sorted_disc = sorted(
            result.senator_discrepancies.items(),
            key=lambda x: abs(x[1]["ssw"] - x[1]["cppi"]),
            reverse=True,
        )
        for name, counts in sorted_disc[:15]:
            diff = counts["cppi"] - counts["ssw"]
            diff_str = f"+{diff}" if diff > 0 else str(diff)
            print(f"    {name}: SSW={counts['ssw']}, CPPI={counts['cppi']} ({diff_str})")
        if len(sorted_disc) > 15:
            print(f"    ... and {len(sorted_disc) - 15} more")

    # Verdict
    print("\n## Verdict")
    if result.ssw_transaction_count == 0:
        print("  ⚠️  No SSW data available for comparison")
    elif result.cppi_transaction_count == 0:
        print("  ❌ No CPPI data available - run parse command first")
    elif result.cppi_transaction_count >= result.ssw_transaction_count * 0.95:
        print("  ✅ CPPI coverage is >= 95% of SSW - GOOD")
    elif result.cppi_transaction_count >= result.ssw_transaction_count * 0.80:
        print("  ⚠️  CPPI coverage is 80-95% of SSW - some gaps exist")
    else:
        print("  ❌ CPPI coverage is < 80% of SSW - significant data missing")

    # Notes
    print("\n## Notes")
    print("  - SSW data only covers 2012-2020, so 2024+ comparisons are invalid")
    print("  - If CPPI shows only 'UNKNOWN' senators, filer names need extraction fixes")

    print()


def main():
    parser = argparse.ArgumentParser(
        description="Validate CPPI Senate data against senate-stock-watcher-data"
    )
    parser.add_argument(
        "--from-date",
        help="Start date (YYYY-MM-DD) for comparison",
    )
    parser.add_argument(
        "--to-date",
        help="End date (YYYY-MM-DD) for comparison",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to CPPI database (default: {DEFAULT_DB_PATH})",
    )
    args = parser.parse_args()

    # Parse dates
    from_date = None
    to_date = None
    if args.from_date:
        try:
            from_date = datetime.strptime(args.from_date, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid from-date format: %s (use YYYY-MM-DD)", args.from_date)
            sys.exit(1)
    if args.to_date:
        try:
            to_date = datetime.strptime(args.to_date, "%Y-%m-%d")
        except ValueError:
            logger.error("Invalid to-date format: %s (use YYYY-MM-DD)", args.to_date)
            sys.exit(1)

    # Fetch SSW data
    ssw_data = fetch_ssw_data()
    if not ssw_data:
        logger.error("Failed to fetch SSW data")
        sys.exit(1)

    # Extract transactions by senator
    logger.info("Extracting SSW transactions...")
    ssw_by_senator = extract_ssw_transactions(ssw_data, from_date, to_date)
    logger.info("Found %d senators in SSW data", len(ssw_by_senator))

    logger.info("Extracting CPPI transactions from %s...", args.db)
    cppi_by_senator = extract_cppi_transactions(args.db, from_date, to_date)
    logger.info("Found %d senators in CPPI data", len(cppi_by_senator))

    # Compare
    result = compare_data(ssw_by_senator, cppi_by_senator)

    # Print report
    print_report(result, from_date, to_date)


if __name__ == "__main__":
    main()
