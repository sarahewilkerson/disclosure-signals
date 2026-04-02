"""
Transaction and role classification.

Classifies each transaction by:
1. Role of the reporting owner (CEO, CFO, Chair, President, COO, or excluded)
2. Transaction type (open-market buy/sell, option exercise, tax withhold, etc.)
3. Whether the trade appears to be part of a 10b5-1 plan
4. Whether the trade appears discretionary
5. Percent of holdings changed
6. Final include/exclude decision with reason

All classification logic is deterministic and auditable.
"""

import logging
import re
from datetime import datetime, timedelta

import config
from db import get_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Role classification
# ---------------------------------------------------------------------------
def classify_role(officer_title: str | None, owner_name: str | None,
                  is_officer: bool, is_director: bool,
                  is_ten_pct_owner: bool, is_other: bool) -> tuple[str, str | None]:
    """
    Classify a reporting owner into a role category.

    Returns:
        (role_class, exclusion_reason)
        role_class: one of 'ceo', 'cfo', 'chair', 'president', 'coo', 'cto',
                    'clo', 'cio', 'cmo', 'cao', 'officer_other', or 'excluded'
        exclusion_reason: None if included, or a string explaining exclusion

    Classification logic:
    1. Entity names (LLC, Trust, etc.) are excluded
    2. 10% holders with no officer/director role are excluded
    3. "Other" relationship with no officer role is excluded
    4. Former officers are excluded
    5. Title patterns are checked (even if is_officer=False) - this is the
       primary classification mechanism since the XML isOfficer flag is
       not always reliable
    6. Director-only (no officer title) is excluded
    7. is_officer=True with no matching title → officer_other (included)
    8. No officer indicators at all → excluded
    """
    # Check if owner_name looks like an entity (not a natural person)
    if owner_name and _is_entity_name(owner_name):
        return "excluded", f"entity_name: {owner_name}"

    # 10% holder with no officer/director role
    if is_ten_pct_owner and not is_officer and not is_director:
        return "excluded", "ten_pct_holder_only"

    # is_other with no officer role
    if is_other and not is_officer:
        return "excluded", "other_relationship_no_officer_role"

    # Check for former officer (before trying to match titles)
    if officer_title and _is_former_officer(officer_title):
        return "excluded", f"former_officer: {officer_title}"

    # Check title patterns FIRST - this is more reliable than the is_officer
    # boolean flag, which may not be set correctly in all filings
    if officer_title:
        role = _match_leadership_title(officer_title)
        if role:
            return role, None

    # Director-only (no matching officer title)
    if is_director and not is_officer:
        return "excluded", "director_only"

    # Has officer flag but no matching title - include but weight lower
    if is_officer:
        if officer_title:
            return "excluded", f"officer_not_top_leadership: {officer_title}"
        return "officer_other", None

    # No officer indicators at all
    return "excluded", "no_officer_role"


def _is_entity_name(name: str) -> bool:
    """Check if a name matches entity patterns (LLC, Trust, Fund, etc.)."""
    name_lower = name.lower()
    for pattern in config.ENTITY_EXCLUSION_PATTERNS:
        if re.search(pattern, name_lower):
            return True
    return False


def _is_former_officer(title: str) -> bool:
    """Check if an officer title indicates a former/retired officer."""
    title_lower = title.lower()
    for pattern in config.FORMER_OFFICER_PATTERNS:
        if re.search(pattern, title_lower):
            return True
    return False


def _match_leadership_title(title: str) -> str | None:
    """
    Match an officer title against top leadership regex patterns.

    Returns the highest-priority matching role, or None.
    Uses config.ROLE_PRIORITY to break ties.

    Patterns use word boundaries and negative lookbehinds to avoid
    false positives (e.g., "Senior Vice President" does NOT match "president").
    """
    if not title:
        return None

    title_lower = title.lower()
    matched_roles = set()

    for pattern, role in config.TOP_LEADERSHIP_PATTERNS:
        if re.search(pattern, title_lower):
            matched_roles.add(role)

    if not matched_roles:
        return None

    # Return highest-priority role
    for role in config.ROLE_PRIORITY:
        if role in matched_roles:
            return role

    return matched_roles.pop()


# ---------------------------------------------------------------------------
# Transaction classification
# ---------------------------------------------------------------------------
def classify_transaction_type(transaction_code: str | None) -> str:
    """Map a transaction code to a canonical type."""
    if not transaction_code:
        return "unknown"
    return config.TRANSACTION_CODE_MAP.get(transaction_code.upper(), "other")


def detect_planned_trade(footnotes: str | None) -> bool:
    """
    Detect whether a transaction is part of a 10b5-1 trading plan.

    Searches footnote text for plan-related keywords.
    """
    if not footnotes:
        return False
    footnotes_lower = footnotes.lower()
    return any(kw in footnotes_lower for kw in config.PLANNED_TRADE_KEYWORDS)


def compute_pct_holdings_changed(shares: float | None,
                                  shares_after: float | None) -> float | None:
    """
    Compute the percentage of the insider's holdings that changed.

    Uses shares_after (post-transaction holdings) as the reference.
    For buys: shares_before = shares_after - shares; pct = shares / shares_before
    For sells: shares_before = shares_after + shares; pct = shares / shares_before

    Returns a float in [0, 1+], or None if data is insufficient.
    """
    if shares is None or shares_after is None:
        return None
    if shares <= 0:
        return None

    # shares_after is the post-transaction amount.
    # shares_before = shares_after ± shares (depending on buy/sell)
    # But we don't know direction here, so use the simpler:
    # pct = shares / (shares_after + shares) which approximates shares/shares_before for sells
    # and shares / shares_after for buys (approximately right either way)
    total = shares_after + shares
    if total <= 0:
        return None

    return shares / total


# ---------------------------------------------------------------------------
# Exercise-and-sell detection
# ---------------------------------------------------------------------------
def detect_exercise_and_sell(transactions: list[dict]) -> set[int]:
    """
    Detect sell transactions that are likely paired with option exercises.

    Looks for S transactions within EXERCISE_AND_SELL_WINDOW_DAYS of an M
    transaction by the same owner with a similar share count.

    Returns set of transaction IDs that should be flagged as exercise_and_sell.
    """
    flagged_ids = set()
    window = config.EXERCISE_AND_SELL_WINDOW_DAYS
    tolerance = config.EXERCISE_AND_SELL_SHARE_TOLERANCE

    # Group by owner
    by_owner = {}
    for txn in transactions:
        owner = txn.get("cik_owner", "")
        by_owner.setdefault(owner, []).append(txn)

    for owner, owner_txns in by_owner.items():
        exercises = [t for t in owner_txns if t.get("transaction_code") == "M"]
        sells = [t for t in owner_txns if t.get("transaction_code") == "S"]

        for sell in sells:
            sell_date = _parse_date(sell.get("transaction_date"))
            sell_shares = sell.get("shares")
            if sell_date is None or sell_shares is None:
                continue

            for ex in exercises:
                ex_date = _parse_date(ex.get("transaction_date"))
                ex_shares = ex.get("shares")
                if ex_date is None or ex_shares is None:
                    continue

                days_diff = abs((sell_date - ex_date).days)
                if days_diff > window:
                    continue

                # Check share count similarity
                if ex_shares > 0:
                    share_diff = abs(sell_shares - ex_shares) / ex_shares
                    if share_diff <= tolerance:
                        sell_id = sell.get("id")
                        if sell_id is not None:
                            flagged_ids.add(sell_id)
                        break  # Only flag once per sell

    return flagged_ids


def _parse_date(date_str: str | None) -> datetime | None:
    """Parse a YYYY-MM-DD date string."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Main classification pipeline
# ---------------------------------------------------------------------------
def classify_all(db_path: str = None):
    """
    Run classification on all transactions in the database.

    Updates each transaction's classification fields:
    role_class, transaction_class, is_likely_planned, is_discretionary,
    pct_holdings_changed, include_in_signal, exclusion_reason.
    """
    with get_connection(db_path) as conn:
        # Load all transactions
        rows = conn.execute("""
            SELECT t.id, t.accession_number, t.cik_issuer, t.cik_owner,
                   t.owner_name, t.officer_title, t.transaction_code,
                   t.shares, t.shares_after, t.equity_swap,
                   t.ownership_nature, t.indirect_entity,
                   t.is_derivative, t.footnotes, t.transaction_date,
                   f.is_officer, f.is_director, f.is_ten_pct_owner, f.is_other,
                   f.aff10b5one
            FROM transactions t
            JOIN filings f ON t.accession_number = f.accession_number
        """).fetchall()

        logger.info(f"Classifying {len(rows)} transactions...")

        # Convert to dicts for exercise-and-sell detection
        txn_dicts = [dict(row) for row in rows]

        # Detect exercise-and-sell pairs
        exercise_sell_ids = detect_exercise_and_sell(txn_dicts)

        for row in rows:
            txn_id = row["id"]

            # 1. Role classification
            role_class, role_exclusion = classify_role(
                officer_title=row["officer_title"],
                owner_name=row["owner_name"],
                is_officer=bool(row["is_officer"]),
                is_director=bool(row["is_director"]),
                is_ten_pct_owner=bool(row["is_ten_pct_owner"]),
                is_other=bool(row["is_other"]),
            )

            # 2. Transaction type classification
            txn_class = classify_transaction_type(row["transaction_code"])

            # Check for exercise-and-sell override
            if txn_id in exercise_sell_ids:
                txn_class = "exercise_and_sell"

            # 3. 10b5-1 detection (structured flag OR footnote keywords)
            aff_flag = row["aff10b5one"] if "aff10b5one" in row.keys() else 0
            is_planned = bool(aff_flag) or detect_planned_trade(row["footnotes"])

            # 4. Percent of holdings changed
            pct_changed = compute_pct_holdings_changed(
                row["shares"], row["shares_after"]
            )

            # 5. Determine include/exclude and reason
            include, exclusion_reason = _determine_inclusion(
                role_class=role_class,
                role_exclusion=role_exclusion,
                txn_class=txn_class,
                transaction_code=row["transaction_code"],
                is_derivative=bool(row["is_derivative"]),
                equity_swap=bool(row["equity_swap"]),
                ownership_nature=row["ownership_nature"],
                indirect_entity=row["indirect_entity"],
            )

            # 6. Determine discretionary
            is_discretionary = _is_discretionary(txn_class, is_planned)

            # Update the transaction
            conn.execute("""
                UPDATE transactions SET
                    role_class = ?,
                    transaction_class = ?,
                    is_likely_planned = ?,
                    is_discretionary = ?,
                    pct_holdings_changed = ?,
                    include_in_signal = ?,
                    exclusion_reason = ?
                WHERE id = ?
            """, (
                role_class,
                txn_class,
                1 if is_planned else 0,
                1 if is_discretionary else 0,
                pct_changed,
                1 if include else 0,
                exclusion_reason,
                txn_id,
            ))

        logger.info("Classification complete.")


def _determine_inclusion(
    role_class: str,
    role_exclusion: str | None,
    txn_class: str,
    transaction_code: str | None,
    is_derivative: bool,
    equity_swap: bool,
    ownership_nature: str | None,
    indirect_entity: str | None,
) -> tuple[bool, str | None]:
    """
    Determine whether a transaction should be included in the core signal.

    Returns (include: bool, exclusion_reason: str | None).
    """
    # Role exclusion
    if role_class == "excluded":
        return False, role_exclusion

    # Derivative transactions excluded
    if is_derivative:
        return False, "derivative_transaction"

    # Equity swaps excluded
    if equity_swap:
        return False, "equity_swap"

    # Only core signal transaction codes (P, S)
    if transaction_code and transaction_code.upper() not in config.CORE_SIGNAL_CODES:
        return False, f"transaction_code_{transaction_code}"

    # Exercise-and-sell excluded
    if txn_class == "exercise_and_sell":
        return False, "exercise_and_sell"

    # Indirect ownership through entities excluded
    if ownership_nature == "I" and indirect_entity and _is_entity_name(indirect_entity):
        return False, f"indirect_entity: {indirect_entity}"

    return True, None


def _is_discretionary(txn_class: str, is_planned: bool) -> bool:
    """
    Determine if a transaction appears to be discretionary.

    Open-market buys/sells that are NOT part of a 10b5-1 plan are discretionary.
    """
    if txn_class in ("open_market_buy", "open_market_sell"):
        return not is_planned
    return False
