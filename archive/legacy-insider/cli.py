#!/usr/bin/env python3
"""
Insider Trading Signal Engine — CLI entry point.

Usage:
    python cli.py run --csv universe.csv          # Full pipeline
    python cli.py ingest --csv universe.csv       # Ingest only
    python cli.py classify                         # Classify only
    python cli.py score                            # Score only
    python cli.py report                           # Generate reports
    python cli.py status                           # Show DB status
"""

import argparse
import logging
import signal
import sys
import time
from datetime import datetime

import config
from db import init_db, get_connection

# ---------------------------------------------------------------------------
# Graceful shutdown support
# ---------------------------------------------------------------------------
_shutdown_requested = False


def _handle_shutdown_signal(signum, frame):
    """Handle SIGINT/SIGTERM by setting shutdown flag."""
    global _shutdown_requested
    sig_name = signal.Signals(signum).name
    print(f"\n[!] Received {sig_name}. Shutting down gracefully...")
    _shutdown_requested = True


def is_shutdown_requested() -> bool:
    """Check if shutdown has been requested. Use in long-running loops."""
    return _shutdown_requested


def install_signal_handlers():
    """Install signal handlers for graceful shutdown."""
    signal.signal(signal.SIGINT, _handle_shutdown_signal)
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)


def _parse_date_arg(date_str: str | None) -> datetime:
    """Parse a --date argument with a user-friendly error message."""
    if not date_str:
        return datetime.now()
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{date_str}'. Use YYYY-MM-DD.")
        sys.exit(1)


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_run(args):
    """Full pipeline: ingest → parse → classify → score → report."""
    from universe import load_universe_csv
    from ingestion import ingest_universe
    from parsing import parse_all_pending
    from classification import classify_all
    from scoring import score_all_companies, compute_aggregate_index
    from reporting import save_reports, generate_cli_report

    config.validate_runtime_config()
    init_db()

    # Step 1: Load universe
    print(f"\n[1/6] Loading universe from {args.csv}...")
    companies = load_universe_csv(args.csv)
    print(f"  Loaded {len(companies)} companies.")

    # Step 2: Ingest filings from EDGAR
    print("\n[2/6] Ingesting Form 4 filings from SEC EDGAR...")
    result = ingest_universe(max_filings_per_company=args.max_filings)
    print(f"  Downloaded {result['total_new_filings']} new filings.")
    if result["errors"]:
        print(f"  Errors: {len(result['errors'])}")
        for e in result["errors"][:5]:
            print(f"    {e['ticker']}: {e['error']}")

    # Step 3: Parse XML
    print("\n[3/6] Parsing Form 4 XML filings...")
    parse_all_pending()

    # Step 4: Classify
    print("\n[4/6] Classifying transactions...")
    classify_all()

    # Step 5: Score
    ref_date = _parse_date_arg(args.date)
    print(f"\n[5/6] Scoring (reference date: {ref_date.strftime('%Y-%m-%d')})...")
    score_all_companies(reference_date=ref_date)
    compute_aggregate_index(reference_date=ref_date)

    # Step 6: Report
    print("\n[6/6] Generating reports...")
    reports = save_reports()
    print(f"  Report: {reports['cli_report_path']}")
    print(f"  Dashboard: {reports['html_dashboard_path']}")

    # Print summary to console (reuse already-generated report text)
    print("\n" + reports["cli_report"])


def cmd_ingest(args):
    """Ingest filings only."""
    import asyncio
    from universe import load_universe_csv
    from ingestion import ingest_universe, ingest_universe_async

    config.validate_runtime_config()
    init_db()
    load_universe_csv(args.csv)

    start_date = args.start_date if hasattr(args, 'start_date') else None
    end_date = args.end_date if hasattr(args, 'end_date') else None
    use_async = getattr(args, 'use_async', False)

    if start_date or end_date:
        print(f"Historical backfill mode: {start_date or 'earliest'} to {end_date or 'latest'}")

    if use_async:
        print("Using async HTTP mode...")
        result = asyncio.run(ingest_universe_async(
            max_filings_per_company=args.max_filings,
            start_date=start_date,
            end_date=end_date,
            concurrency=getattr(args, 'concurrency', 5),
        ))
    else:
        result = ingest_universe(
            max_filings_per_company=args.max_filings,
            start_date=start_date,
            end_date=end_date,
        )
    print(f"Downloaded {result['total_new_filings']} new filings.")


def cmd_parse(args):
    """Parse pending filings."""
    from parsing import parse_all_pending

    init_db()
    parse_all_pending()


def cmd_classify(args):
    """Run classification on all transactions."""
    from classification import classify_all

    init_db()
    classify_all()


def cmd_score(args):
    """Score all companies and compute aggregate index."""
    from services.scoring_service import compute_scores

    init_db()
    ref_date = _parse_date_arg(args.date)
    result = compute_scores(reference_date=ref_date)
    print(
        f"Scoring complete. company_scores={result.company_score_count} "
        f"aggregate_indices={result.aggregate_index_count}"
    )


def cmd_report(args):
    """Generate reports from existing scores."""
    from services.reporting_service import generate_reports

    init_db()
    reports = generate_reports()
    print(reports.cli_report)


def cmd_status(args):
    """Show database status."""
    from services.status_service import get_status

    init_db()
    status = get_status()
    print("Insider Trading Signal Engine — Database Status")
    print(f"  Companies:            {status.companies}")
    print(f"  Filings:              {status.filings}")
    print(f"  Transactions:         {status.transactions}")
    print(f"  Signal transactions:  {status.signal_transactions}")
    print(f"  Company scores:       {status.company_scores}")

    if status.filings > 0:
        print(f"  Filing date range:    {status.filing_date_oldest} to {status.filing_date_latest}")

    if status.parse_errors:
        print(f"  Parse errors:         {status.parse_errors}")


def main():
    parser = argparse.ArgumentParser(
        description="Insider Trading Signal Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # run
    p_run = subparsers.add_parser("run", help="Full pipeline: ingest → score → report")
    p_run.add_argument("--csv", required=True, help="Path to Fortune 500 CSV")
    p_run.add_argument("--date", help="Reference date (YYYY-MM-DD), default=today")
    p_run.add_argument("--max-filings", type=int, default=None,
                       help="Max filings per company (for testing)")
    p_run.set_defaults(func=cmd_run)

    # ingest
    p_ingest = subparsers.add_parser("ingest", help="Ingest filings from EDGAR")
    p_ingest.add_argument("--csv", required=True, help="Path to Fortune 500 CSV")
    p_ingest.add_argument("--max-filings", type=int, default=None,
                          help="Max filings per company")
    p_ingest.add_argument("--start-date", help="Start date for historical backfill (YYYY-MM-DD)")
    p_ingest.add_argument("--end-date", help="End date for historical backfill (YYYY-MM-DD)")
    p_ingest.add_argument("--async", dest="use_async", action="store_true",
                          help="Use async HTTP for faster ingestion")
    p_ingest.add_argument("--concurrency", type=int, default=5,
                          help="Max concurrent requests in async mode (default: 5)")
    p_ingest.set_defaults(func=cmd_ingest)

    # parse
    p_parse = subparsers.add_parser("parse", help="Parse pending filings")
    p_parse.set_defaults(func=cmd_parse)

    # classify
    p_classify = subparsers.add_parser("classify", help="Classify transactions")
    p_classify.set_defaults(func=cmd_classify)

    # score
    p_score = subparsers.add_parser("score", help="Score companies + aggregate")
    p_score.add_argument("--date", help="Reference date (YYYY-MM-DD), default=today")
    p_score.set_defaults(func=cmd_score)

    # report
    p_report = subparsers.add_parser("report", help="Generate reports")
    p_report.set_defaults(func=cmd_report)

    # status
    p_status = subparsers.add_parser("status", help="Show DB status")
    p_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    setup_logging(args.verbose)
    install_signal_handlers()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\n[!] Interrupted. Exiting.")
        sys.exit(130)  # Standard exit code for SIGINT


if __name__ == "__main__":
    main()
