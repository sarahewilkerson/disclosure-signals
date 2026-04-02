from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from signals.combined.service import build_from_derived
from signals.congress.service import (
    get_legacy_status as get_congress_legacy_status,
    run_legacy_score_into_derived as run_congress_legacy_score_into_derived,
)
from signals.congress.ingest import ingest_house_ptrs_direct
from signals.congress.direct_service import run_direct_house_pdfs_into_derived
from signals.core.derived_db import fetch_failed_runs, get_connection, init_db
from signals.core.legacy_subprocess import run_legacy_cli
from signals.core.pipeline import run_unified_pipeline
from signals.core.vertical_slice import (
    build_combined_fixture,
    derived_status,
    run_congress_fixture,
    run_insider_fixture,
    run_vertical_slice,
)
from signals.insider.service import (
    get_legacy_status as get_insider_legacy_status,
    run_legacy_score_into_derived as run_insider_legacy_score_into_derived,
)
from signals.insider.direct_service import run_direct_xml_into_derived
from signals.insider.ingest import ingest_universe_direct
from signals.reporting.formatters import render_json, render_text
from signals.reporting.service import build_combined_report, build_source_report


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_derived_db() -> Path:
    return repo_root() / "data" / "derived" / "disclosure_signals_derived.db"


def default_insider_legacy_db() -> Path:
    return repo_root() / "legacy-insider" / "insider_signal.db"


def default_insider_xml_cache() -> Path:
    return repo_root() / "legacy-insider" / "cache" / "filings"


def default_insider_rewrite_cache() -> Path:
    return repo_root() / "data" / "rewrite_cache" / "insider"


def default_congress_legacy_db() -> Path:
    return repo_root() / "legacy-congress" / "data" / "cppi.db"


def default_congress_rewrite_cache() -> Path:
    return repo_root() / "data" / "rewrite_cache" / "congress"


def cmd_slice_run(args):
    fixture_dir = repo_root() / "tests" / "fixtures" / "vertical_slice"
    result = run_vertical_slice(
        repo_root=repo_root(),
        db_path=args.db,
        fixture_dir=fixture_dir,
        artifact_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
    )
    source_results = []
    from signals.core.dto import CombinedResult, SignalResult
    for row in result.source_results:
        source_results.append(SignalResult(**{k: row[k] for k in SignalResult.__dataclass_fields__.keys()}))
    combined_results = []
    for row in result.combined_results:
        combined_results.append(CombinedResult(**{k: row[k] for k in CombinedResult.__dataclass_fields__.keys()}))
    if args.format == "json":
        print(json.dumps({
            **render_json(source_results, combined_results, result.blocked_combined, result.parity),
            "artifact_paths": result.artifact_paths,
        }, indent=2))
    else:
        print(render_text(source_results, combined_results, result.blocked_combined, result.parity))
        print("")
        print("Artifacts:")
        for key, value in sorted(result.artifact_paths.items()):
            print(f"- {key}: {value}")


def cmd_insider_fixture(args):
    fixture_dir = repo_root() / "tests" / "fixtures" / "vertical_slice"
    result = run_insider_fixture(
        repo_root=repo_root(),
        db_path=args.db,
        fixture_dir=fixture_dir,
        artifact_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
    )
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(f"insider normalized={len(result.normalized)} source_results={len(result.source_results)}")
        print("Artifacts:")
        for key, value in sorted(result.artifact_paths.items()):
            print(f"- {key}: {value}")


def cmd_congress_fixture(args):
    fixture_dir = repo_root() / "tests" / "fixtures" / "vertical_slice"
    result = run_congress_fixture(
        repo_root=repo_root(),
        db_path=args.db,
        fixture_dir=fixture_dir,
        artifact_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
    )
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(f"congress normalized={len(result.normalized)} source_results={len(result.source_results)}")
        print("Artifacts:")
        for key, value in sorted(result.artifact_paths.items()):
            print(f"- {key}: {value}")


def cmd_combined_fixture(args):
    result = build_combined_fixture(
        repo_root=repo_root(),
        db_path=args.db,
        artifact_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
    )
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(f"combined blocked={len(result.blocked_combined)}")
        print("Artifacts:")
        for key, value in sorted(result.artifact_paths.items()):
            print(f"- {key}: {value}")


def cmd_source_report(args):
    init_db(args.db)
    with get_connection(args.db) as conn:
        text, payload = build_source_report(conn, args.source_name)
    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(text)


def cmd_insider_score(args):
    reference_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
    result = run_insider_legacy_score_into_derived(
        repo_root=repo_root(),
        derived_db_path=args.db,
        legacy_db_path=args.insider_legacy_db,
        reference_date=reference_date,
    )
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(
            f"insider imported_normalized={result.imported_normalized_count} "
            f"imported_results={result.imported_result_count} run_id={result.run_id}"
        )


def cmd_insider_rewrite_score(args):
    reference_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
    result = run_direct_xml_into_derived(
        repo_root=repo_root(),
        derived_db_path=args.db,
        xml_dir=args.xml_dir,
        reference_date=reference_date,
    )
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(
            f"insider-direct xml_count={result.xml_count} "
            f"imported_normalized={result.imported_normalized_count} "
            f"imported_results={result.imported_result_count} run_id={result.run_id}"
        )


def cmd_insider_rewrite_ingest(args):
    result = ingest_universe_direct(
        csv_path=args.csv,
        user_agent=args.sec_user_agent,
        cache_dir=args.cache_dir,
        max_filings_per_company=args.max_filings,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    if args.format == "json":
        print(json.dumps(result, indent=2))
    else:
        print(
            f"insider-direct-ingest companies={result['companies_processed']} "
            f"downloaded={result['total_new_filings']} cache_dir={result['cache_dir']}"
        )


def cmd_insider_rewrite_run(args):
    reference_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
    ingest_result = ingest_universe_direct(
        csv_path=args.csv,
        user_agent=args.sec_user_agent,
        cache_dir=args.cache_dir,
        max_filings_per_company=args.max_filings,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    score_result = run_direct_xml_into_derived(
        repo_root=repo_root(),
        derived_db_path=args.db,
        xml_dir=ingest_result["filings_dir"],
        reference_date=reference_date,
    )
    payload = {
        "ingest": ingest_result,
        "score": score_result.to_dict(),
    }
    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(
            f"insider-direct-run companies={ingest_result['companies_processed']} "
            f"downloaded={ingest_result['total_new_filings']} "
            f"xml_count={score_result.xml_count} normalized={score_result.imported_normalized_count} "
            f"results={score_result.imported_result_count} run_id={score_result.run_id}"
        )


def cmd_insider_status(args):
    payload = {
        "legacy": get_insider_legacy_status(args.insider_legacy_db),
        "derived": derived_status(args.db),
    }
    print(json.dumps(payload, indent=2) if args.format == "json" else "\n".join(
        ["Insider Status", "Legacy:"] +
        [f"  {k}: {v}" for k, v in payload["legacy"].items()] +
        ["Derived:"] +
        [f"  {k}: {v}" for k, v in payload["derived"].items()]
    ))


def cmd_insider_parse(args):
    result = run_legacy_cli(
        repo_root() / "legacy-insider" / "cli.py",
        ["parse"],
        {"DB_PATH": args.insider_legacy_db, "SKIP_CONFIG_VALIDATION": "1"},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_insider_ingest(args):
    legacy_args = ["ingest", "--csv", args.csv]
    if args.max_filings is not None:
        legacy_args.extend(["--max-filings", str(args.max_filings)])
    if args.start_date:
        legacy_args.extend(["--start-date", args.start_date])
    if args.end_date:
        legacy_args.extend(["--end-date", args.end_date])
    if args.use_async:
        legacy_args.append("--async")
        legacy_args.extend(["--concurrency", str(args.concurrency)])
    result = run_legacy_cli(
        repo_root() / "legacy-insider" / "cli.py",
        legacy_args,
        {"DB_PATH": args.insider_legacy_db, "SKIP_CONFIG_VALIDATION": "1"},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_insider_classify(args):
    result = run_legacy_cli(
        repo_root() / "legacy-insider" / "cli.py",
        ["classify"],
        {"DB_PATH": args.insider_legacy_db, "SKIP_CONFIG_VALIDATION": "1"},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_insider_run_legacy(args):
    legacy_args = ["run", "--csv", args.csv]
    if args.date:
        legacy_args.extend(["--date", args.date])
    if args.max_filings is not None:
        legacy_args.extend(["--max-filings", str(args.max_filings)])
    result = run_legacy_cli(
        repo_root() / "legacy-insider" / "cli.py",
        legacy_args,
        {"DB_PATH": args.insider_legacy_db, "SKIP_CONFIG_VALIDATION": "1"},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_congress_score(args):
    result = run_congress_legacy_score_into_derived(
        repo_root=repo_root(),
        derived_db_path=args.db,
        legacy_db_path=args.congress_legacy_db,
        window_days=args.window,
    )
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(
            f"congress imported_normalized={result.imported_normalized_count} "
            f"imported_results={result.imported_result_count} run_id={result.run_id}"
        )


def cmd_congress_rewrite_score_house(args):
    reference_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
    result = run_direct_house_pdfs_into_derived(
        repo_root=repo_root(),
        derived_db_path=args.db,
        pdf_dir=args.pdf_dir,
        reference_date=reference_date,
        window_days=args.window,
        max_files=args.max_files,
    )
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(
            f"congress-direct-house pdf_count={result.pdf_count} "
            f"normalized={result.imported_normalized_count} "
            f"results={result.imported_result_count} skipped={result.skipped_count} "
            f"run_id={result.run_id}"
        )


def cmd_congress_rewrite_ingest_house(args):
    result = ingest_house_ptrs_direct(
        repo_root=repo_root(),
        cache_dir=args.cache_dir,
        days=args.days,
        max_filings=args.max_filings,
        force=args.force,
    )
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(
            f"congress-direct-house-ingest ptrs={result.ptr_count} downloaded={result.downloaded_count} "
            f"cached={result.skipped_cached_count} failed={result.failed_count} pdf_dir={result.pdf_dir}"
        )


def cmd_congress_rewrite_run_house(args):
    ingest = ingest_house_ptrs_direct(
        repo_root=repo_root(),
        cache_dir=args.cache_dir,
        days=args.days,
        max_filings=args.max_filings,
        force=args.force,
    )
    reference_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
    score = run_direct_house_pdfs_into_derived(
        repo_root=repo_root(),
        derived_db_path=args.db,
        pdf_dir=ingest.pdf_dir,
        reference_date=reference_date,
        window_days=args.window,
        max_files=args.score_max_files,
    )
    payload = {
        "ingest": ingest.to_dict(),
        "score": score.to_dict(),
    }
    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(
            f"congress-direct-house-run ptrs={ingest.ptr_count} downloaded={ingest.downloaded_count} "
            f"pdf_count={score.pdf_count} normalized={score.imported_normalized_count} "
            f"results={score.imported_result_count} skipped={score.skipped_count} run_id={score.run_id}"
        )


def cmd_congress_status(args):
    payload = {
        "legacy": get_congress_legacy_status(args.congress_legacy_db),
        "derived": derived_status(args.db),
    }
    print(json.dumps(payload, indent=2) if args.format == "json" else "\n".join(
        ["Congress Status", "Legacy:"] +
        [f"  {k}: {v}" for k, v in payload["legacy"].items()] +
        ["Derived:"] +
        [f"  {k}: {v}" for k, v in payload["derived"].items()]
    ))


def cmd_congress_init(args):
    result = run_legacy_cli(
        repo_root() / "legacy-congress" / "cppi" / "cli.py",
        ["init"],
        {"CPPI_DB_PATH": args.congress_legacy_db},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_congress_ingest(args):
    legacy_args = ["ingest", "--days", str(args.days)]
    if args.house_only:
        legacy_args.append("--house-only")
    if args.senate_only:
        legacy_args.append("--senate-only")
    if args.bulk:
        legacy_args.append("--bulk")
    result = run_legacy_cli(
        repo_root() / "legacy-congress" / "cppi" / "cli.py",
        legacy_args,
        {"CPPI_DB_PATH": args.congress_legacy_db},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_congress_parse(args):
    legacy_args = ["parse"]
    if args.force:
        legacy_args.append("--force")
    result = run_legacy_cli(
        repo_root() / "legacy-congress" / "cppi" / "cli.py",
        legacy_args,
        {"CPPI_DB_PATH": args.congress_legacy_db},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_combined_report(args):
    init_db(args.db)
    with get_connection(args.db) as conn:
        text, payload = build_combined_report(conn)
    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(text)


def cmd_combined_build(args):
    result = build_from_derived(repo_root(), args.db, lookback_window=args.window)
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(f"combined built={result.combined_count} blocked={result.blocked_count} run_id={result.run_id}")


def cmd_run(args):
    reference_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
    result = run_unified_pipeline(
        repo_root=repo_root(),
        derived_db_path=args.db,
        insider_legacy_db_path=args.insider_legacy_db,
        congress_legacy_db_path=args.congress_legacy_db,
        reference_date=reference_date,
        lookback_window=args.window,
        artifact_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
    )
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(
            f"run insider_results={result.insider['imported_result_count']} "
            f"congress_results={result.congress['imported_result_count']} "
            f"combined={result.combined['combined_count']}"
        )
        if result.artifact_paths:
            print("")
            print("Artifacts:")
            for key, value in sorted(result.artifact_paths.items()):
                print(f"- {key}: {value}")


def cmd_status(args):
    result = derived_status(args.db)
    if args.format == "json":
        print(json.dumps(result, indent=2))
    else:
        lines = [
            f"runs: {result['runs']}",
            f"normalized: {result['normalized']}",
            f"source_results: {result['source_results']}",
            f"combined_results: {result['combined_results']}",
            f"failed_runs: {result['failed_runs']}",
            "source_result_counts:",
            f"  insider: {result['source_result_counts']['insider']}",
            f"  congress: {result['source_result_counts']['congress']}",
        ]
        if result["recent_runs"]:
            lines.append("recent_runs:")
            for row in result["recent_runs"]:
                lines.append(f"  {row['source']} {row['run_type']} {row['status']} {row['run_id']}")
        print("\n".join(lines))


def cmd_doctor(args):
    db_path = Path(args.db)
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        writable = True
    except Exception:
        writable = False
    init_db(str(db_path))
    with get_connection(str(db_path)) as conn:
        failed_runs = fetch_failed_runs(conn)
        db_counts = {
            "runs": int(conn.execute("SELECT COUNT(*) AS c FROM runs").fetchone()["c"]),
            "normalized": int(conn.execute("SELECT COUNT(*) AS c FROM normalized_transactions").fetchone()["c"]),
            "source_results": int(conn.execute("SELECT COUNT(*) AS c FROM signal_results").fetchone()["c"]),
            "combined_results": int(conn.execute("SELECT COUNT(*) AS c FROM combined_results").fetchone()["c"]),
        }
    checks = {
        "repo_root_exists": repo_root().exists(),
        "legacy_insider_exists": (repo_root() / "legacy-insider").exists(),
        "legacy_congress_exists": (repo_root() / "legacy-congress").exists(),
        "legacy_insider_db_parent_exists": Path(args.insider_legacy_db).parent.exists(),
        "legacy_congress_db_parent_exists": Path(args.congress_legacy_db).parent.exists(),
        "derived_db_parent_writable": writable,
        "fixtures_exist": (repo_root() / "tests" / "fixtures" / "vertical_slice").exists(),
        "db_connectable": True,
        "failed_runs_present": len(failed_runs) > 0,
    }
    overall = all(v for k, v in checks.items() if k != "failed_runs_present")
    if args.format == "json":
        print(json.dumps({"ok": overall, "checks": checks, "db_counts": db_counts}, indent=2))
    else:
        print("\n".join([f"ok: {overall}"] + [f"{k}: {v}" for k, v in checks.items()] + [f"{k}: {v}" for k, v in db_counts.items()]))
    raise SystemExit(0 if overall else 2)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="signals")
    parser.add_argument("--db", default=str(default_derived_db()))
    parser.add_argument("--insider-legacy-db", default=str(default_insider_legacy_db()))
    parser.add_argument("--congress-legacy-db", default=str(default_congress_legacy_db()))
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument("--artifacts-dir", default=None)
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run insider import, congress import, and combined build")
    run_parser.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    run_parser.add_argument("--window", type=int, default=90, help="Lookback window in days")
    run_parser.set_defaults(func=cmd_run)

    slice_parser = subparsers.add_parser("slice", help="Run the narrow vertical slice")
    slice_sub = slice_parser.add_subparsers(dest="slice_command")
    slice_run = slice_sub.add_parser("run", help="Execute the vertical slice")
    slice_run.set_defaults(func=cmd_slice_run)

    insider = subparsers.add_parser("insider", help="Insider engine commands")
    insider_sub = insider.add_subparsers(dest="insider_command")
    insider_fixture = insider_sub.add_parser("fixture-run", help="Run the insider fixture through the shared slice flow")
    insider_fixture.set_defaults(func=cmd_insider_fixture)
    insider_ingest = insider_sub.add_parser("ingest", help="Run the legacy insider ingest command")
    insider_ingest.add_argument("--csv", required=True, help="Path to legacy insider universe CSV")
    insider_ingest.add_argument("--max-filings", type=int, default=None, help="Max filings per company")
    insider_ingest.add_argument("--start-date", dest="start_date", default=None, help="Historical backfill start date")
    insider_ingest.add_argument("--end-date", dest="end_date", default=None, help="Historical backfill end date")
    insider_ingest.add_argument("--async", dest="use_async", action="store_true", help="Use async mode in legacy insider ingest")
    insider_ingest.add_argument("--concurrency", type=int, default=5, help="Async concurrency")
    insider_ingest.set_defaults(func=cmd_insider_ingest)
    insider_parse = insider_sub.add_parser("parse", help="Run the legacy insider parse command")
    insider_parse.set_defaults(func=cmd_insider_parse)
    insider_classify = insider_sub.add_parser("classify", help="Run the legacy insider classify command")
    insider_classify.set_defaults(func=cmd_insider_classify)
    insider_run = insider_sub.add_parser("run-legacy", help="Run the legacy insider full pipeline command")
    insider_run.add_argument("--csv", required=True, help="Path to legacy insider universe CSV")
    insider_run.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    insider_run.add_argument("--max-filings", type=int, default=None, help="Max filings per company")
    insider_run.set_defaults(func=cmd_insider_run_legacy)
    insider_score = insider_sub.add_parser("score", help="Run the legacy insider score flow and import derived results")
    insider_score.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    insider_score.set_defaults(func=cmd_insider_score)
    insider_rewrite_ingest = insider_sub.add_parser("rewrite-ingest", help="Run the rewritten insider SEC ingest flow without the legacy insider DB")
    insider_rewrite_ingest.add_argument("--csv", required=True, help="Path to company universe CSV")
    insider_rewrite_ingest.add_argument("--cache-dir", default=str(default_insider_rewrite_cache()), help="Cache directory for SEC company map and raw Form 4 XML")
    insider_rewrite_ingest.add_argument("--max-filings", type=int, default=None, help="Maximum filings per company")
    insider_rewrite_ingest.add_argument("--start-date", dest="start_date", default=None, help="Historical backfill start date")
    insider_rewrite_ingest.add_argument("--end-date", dest="end_date", default=None, help="Historical backfill end date")
    insider_rewrite_ingest.add_argument("--sec-user-agent", required=True, help="SEC-compliant user agent")
    insider_rewrite_ingest.set_defaults(func=cmd_insider_rewrite_ingest)
    insider_rewrite = insider_sub.add_parser("rewrite-score", help="Run the rewritten insider XML-to-derived score flow without the legacy insider DB")
    insider_rewrite.add_argument("--xml-dir", default=str(default_insider_xml_cache()), help="Directory containing raw Form 4 XML files")
    insider_rewrite.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    insider_rewrite.set_defaults(func=cmd_insider_rewrite_score)
    insider_rewrite_run = insider_sub.add_parser("rewrite-run", help="Run rewritten insider live SEC ingest followed by rewritten XML-to-derived scoring")
    insider_rewrite_run.add_argument("--csv", required=True, help="Path to company universe CSV")
    insider_rewrite_run.add_argument("--cache-dir", default=str(default_insider_rewrite_cache()), help="Cache directory for SEC company map and raw Form 4 XML")
    insider_rewrite_run.add_argument("--max-filings", type=int, default=None, help="Maximum filings per company")
    insider_rewrite_run.add_argument("--start-date", dest="start_date", default=None, help="Historical backfill start date")
    insider_rewrite_run.add_argument("--end-date", dest="end_date", default=None, help="Historical backfill end date")
    insider_rewrite_run.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    insider_rewrite_run.add_argument("--sec-user-agent", required=True, help="SEC-compliant user agent")
    insider_rewrite_run.set_defaults(func=cmd_insider_rewrite_run)
    insider_report = insider_sub.add_parser("report", help="Render persisted insider results without recomputing")
    insider_report.set_defaults(func=cmd_source_report, source_name="insider")
    insider_status = insider_sub.add_parser("status", help="Show insider legacy + derived status")
    insider_status.set_defaults(func=cmd_insider_status)

    congress = subparsers.add_parser("congress", help="Congress engine commands")
    congress_sub = congress.add_subparsers(dest="congress_command")
    congress_fixture = congress_sub.add_parser("fixture-run", help="Run the congress fixture through the shared slice flow")
    congress_fixture.set_defaults(func=cmd_congress_fixture)
    congress_init = congress_sub.add_parser("init", help="Run the legacy congress init command")
    congress_init.set_defaults(func=cmd_congress_init)
    congress_ingest = congress_sub.add_parser("ingest", help="Run the legacy congress ingest command")
    congress_ingest.add_argument("--days", type=int, default=90, help="Lookback days")
    congress_ingest.add_argument("--house-only", action="store_true", help="Only ingest House filings")
    congress_ingest.add_argument("--senate-only", action="store_true", help="Only ingest Senate filings")
    congress_ingest.add_argument("--bulk", action="store_true", help="Enable legacy bulk House ingestion")
    congress_ingest.set_defaults(func=cmd_congress_ingest)
    congress_parse = congress_sub.add_parser("parse", help="Run the legacy congress parse command")
    congress_parse.add_argument("--force", action="store_true", help="Force re-parsing in the legacy congress engine")
    congress_parse.set_defaults(func=cmd_congress_parse)
    congress_score = congress_sub.add_parser("score", help="Run the legacy congress score flow and import derived entity results")
    congress_score.add_argument("--window", type=int, default=90, help="Lookback window in days")
    congress_score.set_defaults(func=cmd_congress_score)
    congress_rewrite_ingest_house = congress_sub.add_parser("rewrite-ingest-house", help="Download real House PTR PDFs directly into the rewrite cache without the legacy congress DB")
    congress_rewrite_ingest_house.add_argument("--cache-dir", default=str(default_congress_rewrite_cache()), help="Rewrite cache root for House PDFs and FD XML")
    congress_rewrite_ingest_house.add_argument("--days", type=int, default=90, help="Lookback days for FD XML PTR filtering")
    congress_rewrite_ingest_house.add_argument("--max-filings", type=int, default=None, help="Maximum PTR PDFs to fetch")
    congress_rewrite_ingest_house.add_argument("--force", action="store_true", help="Force re-download of cached PDFs")
    congress_rewrite_ingest_house.set_defaults(func=cmd_congress_rewrite_ingest_house)
    congress_rewrite_house = congress_sub.add_parser("rewrite-score-house", help="Run direct congress House PDF parsing/scoring from cached text PDFs without the legacy congress DB")
    congress_rewrite_house.add_argument("--pdf-dir", default=str(repo_root() / "legacy-congress" / "cache" / "pdfs" / "house"), help="Directory containing cached House PTR PDFs")
    congress_rewrite_house.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    congress_rewrite_house.add_argument("--window", type=int, default=90, help="Lookback window in days")
    congress_rewrite_house.add_argument("--max-files", type=int, default=None, help="Maximum PDFs to process")
    congress_rewrite_house.set_defaults(func=cmd_congress_rewrite_score_house)
    congress_rewrite_run_house = congress_sub.add_parser("rewrite-run-house", help="Run direct House PTR download followed by direct House PDF scoring")
    congress_rewrite_run_house.add_argument("--cache-dir", default=str(default_congress_rewrite_cache()), help="Rewrite cache root for House PDFs and FD XML")
    congress_rewrite_run_house.add_argument("--days", type=int, default=90, help="Lookback days for FD XML PTR filtering")
    congress_rewrite_run_house.add_argument("--max-filings", type=int, default=None, help="Maximum PTR PDFs to fetch")
    congress_rewrite_run_house.add_argument("--force", action="store_true", help="Force re-download of cached PDFs")
    congress_rewrite_run_house.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    congress_rewrite_run_house.add_argument("--window", type=int, default=90, help="Lookback window in days")
    congress_rewrite_run_house.add_argument("--score-max-files", type=int, default=None, help="Maximum cached PDFs to score")
    congress_rewrite_run_house.set_defaults(func=cmd_congress_rewrite_run_house)
    congress_report = congress_sub.add_parser("report", help="Render persisted congress results without recomputing")
    congress_report.set_defaults(func=cmd_source_report, source_name="congress")
    congress_status = congress_sub.add_parser("status", help="Show congress legacy + derived status")
    congress_status.set_defaults(func=cmd_congress_status)

    combined = subparsers.add_parser("combined", help="Combined overlay commands")
    combined_sub = combined.add_subparsers(dest="combined_command")
    combined_build = combined_sub.add_parser("build-fixture", help="Build the combined overlay from the fixture-backed slice")
    combined_build.set_defaults(func=cmd_combined_fixture)
    combined_build_real = combined_sub.add_parser("build", help="Build the combined overlay from persisted derived source results")
    combined_build_real.add_argument("--window", type=int, default=90, help="Lookback window in days")
    combined_build_real.set_defaults(func=cmd_combined_build)
    combined_report = combined_sub.add_parser("report", help="Render persisted combined results without recomputing")
    combined_report.set_defaults(func=cmd_combined_report)

    status = subparsers.add_parser("status", help="Show derived-store status")
    status.set_defaults(func=cmd_status)

    doctor = subparsers.add_parser("doctor", help="Run workspace health checks")
    doctor.set_defaults(func=cmd_doctor)
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
