from __future__ import annotations

import argparse
import json
import sys
import warnings
from datetime import datetime
from pathlib import Path

from signals.combined.service import build_from_derived
from signals.congress.service import (
    get_legacy_status as get_congress_legacy_status,
    run_legacy_score_into_derived as run_congress_legacy_score_into_derived,
)
from signals.congress.ingest import ingest_house_ptrs_direct
from signals.congress.direct_service import run_direct_house_pdfs_into_derived
from signals.congress.senate_direct import (
    ingest_senate_ptrs_direct,
    run_direct_senate_html_into_derived,
)
from signals.congress.diagnostics import build_congress_candidate_discovery
from signals.core.derived_db import fetch_failed_runs, get_connection, init_db
from signals.core.legacy_loader import legacy_congress_root, legacy_insider_root
from signals.core.legacy_subprocess import run_legacy_cli
from signals.core.pipeline import run_unified_pipeline
from signals.core.pipeline import run_direct_pipeline
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
from signals.insider.diagnostics import build_insider_candidate_discovery
from signals.insider.ingest import ingest_universe_direct
from signals.reporting.formatters import render_json, render_text
from signals.reporting.service import build_combined_report, build_source_report


def _emit_progress(stage: str, payload: dict):
    event = payload.get("event")
    if event == "company_completed":
        message = (
            f"[progress] {stage} "
            f"{payload.get('index')}/{payload.get('companies_total')} "
            f"{payload.get('ticker')} downloaded={payload.get('downloaded')} "
            f"total_downloaded={payload.get('total_downloaded')}"
        )
    elif event == "company_skipped":
        message = (
            f"[progress] {stage} "
            f"{payload.get('index')}/{payload.get('companies_total')} "
            f"{payload.get('ticker')} skipped={payload.get('reason')}"
        )
    elif event in {"start", "finished"}:
        message = (
            f"[progress] {stage} {event} "
            f"completed={payload.get('companies_completed', 0)}/{payload.get('companies_total', 0)} "
            f"remaining={payload.get('remaining_companies', 0)} "
            f"total_downloaded={payload.get('total_new_filings', 0)}"
        )
    else:
        summary_keys = [
            "run_id",
            "downloaded_count",
            "downloaded_ptr_count",
            "imported_result_count",
            "imported_normalized_count",
            "combined_count",
            "blocked_count",
            "xml_count",
            "pdf_count",
            "html_count",
        ]
        parts = [f"{key}={payload[key]}" for key in summary_keys if key in payload]
        message = f"[progress] {stage}" + (f" {' '.join(parts)}" if parts else "")
    print(message, file=sys.stderr, flush=True)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_derived_db() -> Path:
    return repo_root() / "data" / "derived" / "disclosure_signals_derived.db"


def default_insider_legacy_db() -> Path:
    return legacy_insider_root() / "insider_signal.db"


def default_insider_xml_cache() -> Path:
    return default_insider_rewrite_cache() / "filings"


def default_insider_rewrite_cache() -> Path:
    return repo_root() / "data" / "rewrite_cache" / "insider"


def default_congress_legacy_db() -> Path:
    return legacy_congress_root() / "data" / "cppi.db"


def default_congress_rewrite_cache() -> Path:
    return repo_root() / "data" / "rewrite_cache" / "congress"


def _compat_warning(cmd: str):
    warnings.warn(
        f"Command '{cmd}' is part of the legacy compatibility surface and is slated for removal. "
        "Use the direct rewrite flows instead.",
        DeprecationWarning,
        stacklevel=2,
    )


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
    _compat_warning("insider score")
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
        resume=not getattr(args, "no_resume", False),
        progress_callback=lambda payload: _emit_progress("insider_ingest", payload),
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
        resume=not getattr(args, "no_resume", False),
        progress_callback=lambda payload: _emit_progress("insider_ingest", payload),
    )
    _emit_progress("insider_score_start", {"filings_dir": ingest_result["filings_dir"]})
    score_result = run_direct_xml_into_derived(
        repo_root=repo_root(),
        derived_db_path=args.db,
        xml_dir=ingest_result["filings_dir"],
        reference_date=reference_date,
    )
    _emit_progress("insider_score_done", score_result.to_dict())
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


def cmd_insider_candidate_discovery(args):
    init_db(args.db)
    with get_connection(args.db) as conn:
        run_id = args.run_id
        if not run_id:
            row = conn.execute(
                """
                SELECT run_id
                FROM runs
                WHERE run_type = 'direct_xml_score'
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                raise SystemExit("no direct_xml_score runs found in derived DB")
            run_id = row["run_id"]
        payload = build_insider_candidate_discovery(conn, run_id=run_id, limit=args.limit)
    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(f"insider candidate discovery run_id={payload['run_id']}")
        for item in payload["candidates"]:
            print(
                f"- {item['normalized_name']} count={item['count']} "
                f"instruments={item['instrument_types']} examples={item['raw_examples']}"
            )


def cmd_insider_parse(args):
    _compat_warning("insider parse")
    result = run_legacy_cli(
        legacy_insider_root() / "cli.py",
        ["parse"],
        {"DB_PATH": args.insider_legacy_db, "SKIP_CONFIG_VALIDATION": "1"},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_insider_ingest(args):
    _compat_warning("insider ingest")
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
        legacy_insider_root() / "cli.py",
        legacy_args,
        {"DB_PATH": args.insider_legacy_db, "SKIP_CONFIG_VALIDATION": "1"},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_insider_classify(args):
    _compat_warning("insider classify")
    result = run_legacy_cli(
        legacy_insider_root() / "cli.py",
        ["classify"],
        {"DB_PATH": args.insider_legacy_db, "SKIP_CONFIG_VALIDATION": "1"},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_insider_run_legacy(args):
    _compat_warning("insider run-legacy")
    legacy_args = ["run", "--csv", args.csv]
    if args.date:
        legacy_args.extend(["--date", args.date])
    if args.max_filings is not None:
        legacy_args.extend(["--max-filings", str(args.max_filings)])
    result = run_legacy_cli(
        legacy_insider_root() / "cli.py",
        legacy_args,
        {"DB_PATH": args.insider_legacy_db, "SKIP_CONFIG_VALIDATION": "1"},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_congress_score(args):
    _compat_warning("congress score")
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


def cmd_congress_rewrite_ingest_senate(args):
    result = ingest_senate_ptrs_direct(
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
            f"congress-direct-senate-ingest searched={result.searched_count} "
            f"downloaded={result.downloaded_ptr_count} skipped_paper={result.skipped_paper_count} "
            f"failed={result.failed_count} html_dir={result.html_dir}"
        )


def cmd_congress_rewrite_score_senate(args):
    reference_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
    result = run_direct_senate_html_into_derived(
        repo_root=repo_root(),
        derived_db_path=args.db,
        html_dir=args.html_dir,
        reference_date=reference_date,
        window_days=args.window,
        max_files=args.max_files,
    )
    if args.format == "json":
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(
            f"congress-direct-senate html_count={result.html_count} "
            f"normalized={result.imported_normalized_count} "
            f"results={result.imported_result_count} skipped={result.skipped_count} "
            f"run_id={result.run_id}"
        )


def cmd_congress_rewrite_run_senate(args):
    ingest = ingest_senate_ptrs_direct(
        repo_root=repo_root(),
        cache_dir=args.cache_dir,
        days=args.days,
        max_filings=args.max_filings,
        force=args.force,
    )
    reference_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
    score = run_direct_senate_html_into_derived(
        repo_root=repo_root(),
        derived_db_path=args.db,
        html_dir=ingest.html_dir,
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
            f"congress-direct-senate-run searched={ingest.searched_count} downloaded={ingest.downloaded_ptr_count} "
            f"html_count={score.html_count} normalized={score.imported_normalized_count} "
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


def cmd_congress_candidate_discovery(args):
    init_db(args.db)
    with get_connection(args.db) as conn:
        run_id = args.run_id
        if not run_id:
            run_type = "direct_senate_score" if args.branch == "senate" else "direct_house_score"
            row = conn.execute(
                """
                SELECT run_id
                FROM runs
                WHERE run_type = ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (run_type,),
            ).fetchone()
            if row is None:
                raise SystemExit(f"no {run_type} runs found in derived DB")
            run_id = row["run_id"]
        payload = build_congress_candidate_discovery(conn, run_id=run_id, limit=args.limit)
    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(f"congress candidate discovery run_id={payload['run_id']}")
        for item in payload["candidates"]:
            print(
                f"- {item['normalized_name']} count={item['count']} "
                f"categories={item['asset_categories']} examples={item['raw_examples']}"
            )


def cmd_congress_init(args):
    _compat_warning("congress init")
    result = run_legacy_cli(
        legacy_congress_root() / "cppi" / "cli.py",
        ["init"],
        {"CPPI_DB_PATH": args.congress_legacy_db},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_congress_ingest(args):
    _compat_warning("congress ingest")
    legacy_args = ["ingest", "--days", str(args.days)]
    if args.house_only:
        legacy_args.append("--house-only")
    if args.senate_only:
        legacy_args.append("--senate-only")
    if args.bulk:
        legacy_args.append("--bulk")
    result = run_legacy_cli(
        legacy_congress_root() / "cppi" / "cli.py",
        legacy_args,
        {"CPPI_DB_PATH": args.congress_legacy_db},
    )
    if args.format == "json":
        print(json.dumps({"stdout": result.stdout, "stderr": result.stderr}, indent=2))
    else:
        print(result.stdout.rstrip())


def cmd_congress_parse(args):
    _compat_warning("congress parse")
    legacy_args = ["parse"]
    if args.force:
        legacy_args.append("--force")
    result = run_legacy_cli(
        legacy_congress_root() / "cppi" / "cli.py",
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
    if not args.legacy:
        missing = []
        if not args.csv:
            missing.append("--csv")
        if not args.sec_user_agent:
            missing.append("--sec-user-agent")
        if missing:
            raise SystemExit(f"run requires {' and '.join(missing)} unless --legacy is set")
        result = run_direct_pipeline(
            repo_root=repo_root(),
            derived_db_path=args.db,
            insider_csv_path=args.csv,
            insider_user_agent=args.sec_user_agent,
            insider_cache_dir=args.insider_cache_dir,
            congress_cache_dir=args.congress_cache_dir,
            reference_date=reference_date,
            lookback_window=args.window,
            insider_max_filings=args.insider_max_filings,
            house_days=args.house_days,
            house_max_filings=args.house_max_filings,
            senate_days=args.senate_days,
            senate_max_filings=args.senate_max_filings,
            artifact_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
            progress_callback=_emit_progress,
        )
    else:
        _compat_warning("run --legacy")
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
        if not args.legacy:
            insider_count = result.insider["score"]["imported_result_count"]
        else:
            insider_count = result.insider["imported_result_count"]
        print(
            f"run insider_results={insider_count} "
            f"congress_results={result.congress['imported_result_count']} "
            f"combined={result.combined['combined_count']}"
        )
        if result.artifact_paths:
            print("")
            print("Artifacts:")
            for key, value in sorted(result.artifact_paths.items()):
                print(f"- {key}: {value}")


def cmd_run_direct(args):
    args.legacy = False
    cmd_run(args)


def cmd_validate_live(args):
    reference_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
    if not args.csv or not args.sec_user_agent:
        raise SystemExit("validate-live requires --csv and --sec-user-agent")
    result = run_direct_pipeline(
        repo_root=repo_root(),
        derived_db_path=args.db,
        insider_csv_path=args.csv,
        insider_user_agent=args.sec_user_agent,
        insider_cache_dir=args.insider_cache_dir,
        congress_cache_dir=args.congress_cache_dir,
        reference_date=reference_date,
        lookback_window=args.window,
        insider_max_filings=args.insider_max_filings,
        house_days=args.house_days,
        house_max_filings=args.house_max_filings,
        senate_days=args.senate_days,
        senate_max_filings=args.senate_max_filings,
        artifact_dir=Path(args.artifacts_dir) if args.artifacts_dir else None,
        progress_callback=_emit_progress,
    )
    payload = {
        "analysis": result.analysis,
        "artifact_paths": result.artifact_paths,
        "run": result.to_dict(),
    }
    if args.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        analysis = result.analysis or {}
        summary = analysis.get("summary", {})
        assessment = analysis.get("assessment", {})
        print(
            f"validate-live overlap={summary.get('overlap_subject_count', 0)} "
            f"combined={summary.get('combined_count', 0)} "
            f"blocked={summary.get('blocked_count', 0)} "
            f"readiness={assessment.get('readiness', 'unknown')}"
        )
        if result.artifact_paths:
            print("")
            print("Artifacts:")
            for key, value in sorted(result.artifact_paths.items()):
                if key.startswith("production_confidence") or key in {"overlay_diagnostics", "run_summary"}:
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
        "legacy_insider_exists": legacy_insider_root().exists(),
        "legacy_congress_exists": legacy_congress_root().exists(),
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


def cmd_validate(args):
    from signals.analysis.validation import (
        run_transaction_validation, render_transaction_validation_markdown,
        run_baseline_comparison, render_baseline_comparison_markdown,
        run_regime_analysis, render_regime_analysis_markdown,
    )
    forward_days = [int(d) for d in args.forward_days.split(",")] if args.forward_days else [5, 10, 20, 60]
    report = run_transaction_validation(
        db_path=args.db,
        forward_days=forward_days,
        source_filter=args.source or None,
        min_date=args.min_date,
        max_date=args.max_date,
    )
    if args.format == "json":
        payload = {"transaction_validation": report}
        if args.baseline:
            payload["baseline_comparison"] = run_baseline_comparison(args.db, forward_days, args.min_date, args.max_date)
        if args.regime:
            payload["regime_analysis"] = run_regime_analysis(args.db, forward_days, args.min_date, args.max_date)
        print(json.dumps(payload, indent=2))
    else:
        print(render_transaction_validation_markdown(report))
        if args.baseline:
            print()
            print(render_baseline_comparison_markdown(run_baseline_comparison(args.db, forward_days, args.min_date, args.max_date)))
        if args.regime:
            print()
            print(render_regime_analysis_markdown(run_regime_analysis(args.db, forward_days, args.min_date, args.max_date)))


def cmd_brief(args):
    from signals.analysis.daily_brief import build_daily_brief, render_daily_brief_markdown
    reference_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now()
    brief = build_daily_brief(args.db, reference_date=reference_date, include_sectors=args.sectors)
    if args.format == "json":
        print(json.dumps(brief, indent=2))
    else:
        print(render_daily_brief_markdown(brief))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="signals")
    parser.add_argument("--db", default=str(default_derived_db()))
    parser.add_argument("--insider-legacy-db", default=str(default_insider_legacy_db()))
    parser.add_argument("--congress-legacy-db", default=str(default_congress_legacy_db()))
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument("--artifacts-dir", default=None)
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run the unified pipeline; uses direct rewrite flows by default")
    run_parser.add_argument("--legacy", action="store_true", help="Use the legacy-backed unified pipeline instead of the direct rewrite flows")
    run_parser.add_argument("--csv", default=None, help="Path to company universe CSV for direct insider ingest")
    run_parser.add_argument("--sec-user-agent", default=None, help="SEC-compliant user agent for direct insider ingest")
    run_parser.add_argument("--insider-cache-dir", default=str(default_insider_rewrite_cache()), help="Rewrite cache directory for insider SEC XML")
    run_parser.add_argument("--congress-cache-dir", default=str(default_congress_rewrite_cache()), help="Rewrite cache root for House PDFs, FD XML, and Senate HTML")
    run_parser.add_argument("--insider-max-filings", type=int, default=None, help="Maximum insider filings per company for direct mode")
    run_parser.add_argument("--house-days", type=int, default=90, help="House ingest lookback days for direct mode")
    run_parser.add_argument("--house-max-filings", type=int, default=None, help="Maximum House filings to fetch and score for direct mode")
    run_parser.add_argument("--senate-days", type=int, default=365, help="Senate ingest lookback days for direct mode")
    run_parser.add_argument("--senate-max-filings", type=int, default=None, help="Maximum Senate filings to fetch and score for direct mode")
    run_parser.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    run_parser.add_argument("--window", type=int, default=90, help="Lookback window in days")
    run_parser.set_defaults(func=cmd_run)

    run_direct = subparsers.add_parser("run-direct", help="Compatibility alias for the direct unified pipeline")
    run_direct.add_argument("--csv", required=True, help="Path to company universe CSV for direct insider ingest")
    run_direct.add_argument("--sec-user-agent", required=True, help="SEC-compliant user agent for direct insider ingest")
    run_direct.add_argument("--insider-cache-dir", default=str(default_insider_rewrite_cache()), help="Rewrite cache directory for insider SEC XML")
    run_direct.add_argument("--congress-cache-dir", default=str(default_congress_rewrite_cache()), help="Rewrite cache root for House PDFs, FD XML, and Senate HTML")
    run_direct.add_argument("--insider-max-filings", type=int, default=None, help="Maximum insider filings per company")
    run_direct.add_argument("--house-days", type=int, default=90, help="House ingest lookback days")
    run_direct.add_argument("--house-max-filings", type=int, default=None, help="Maximum House filings to fetch and score")
    run_direct.add_argument("--senate-days", type=int, default=365, help="Senate ingest lookback days")
    run_direct.add_argument("--senate-max-filings", type=int, default=None, help="Maximum Senate filings to fetch and score")
    run_direct.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    run_direct.add_argument("--window", type=int, default=90, help="Lookback window in days")
    run_direct.set_defaults(legacy=False)
    run_direct.set_defaults(func=cmd_run_direct)

    validate_live = subparsers.add_parser("validate-live", help="Run a larger direct live-universe validation and emit a production-confidence report")
    validate_live.add_argument("--csv", required=True, help="Path to company universe CSV for direct insider ingest")
    validate_live.add_argument("--sec-user-agent", required=True, help="SEC-compliant user agent for direct insider ingest")
    validate_live.add_argument("--insider-cache-dir", default=str(default_insider_rewrite_cache()), help="Rewrite cache directory for insider SEC XML")
    validate_live.add_argument("--congress-cache-dir", default=str(default_congress_rewrite_cache()), help="Rewrite cache root for House PDFs, FD XML, and Senate HTML")
    validate_live.add_argument("--insider-max-filings", type=int, default=None, help="Maximum insider filings per company")
    validate_live.add_argument("--house-days", type=int, default=90, help="House ingest lookback days")
    validate_live.add_argument("--house-max-filings", type=int, default=None, help="Maximum House filings to fetch and score")
    validate_live.add_argument("--senate-days", type=int, default=365, help="Senate ingest lookback days")
    validate_live.add_argument("--senate-max-filings", type=int, default=None, help="Maximum Senate filings to fetch and score")
    validate_live.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    validate_live.add_argument("--window", type=int, default=90, help="Lookback window in days")
    validate_live.set_defaults(func=cmd_validate_live)

    slice_parser = subparsers.add_parser("slice", help="Run the narrow vertical slice")
    slice_sub = slice_parser.add_subparsers(dest="slice_command")
    slice_run = slice_sub.add_parser("run", help="Execute the vertical slice")
    slice_run.set_defaults(func=cmd_slice_run)

    insider = subparsers.add_parser("insider", help="Insider engine commands")
    insider_sub = insider.add_subparsers(dest="insider_command")
    insider_fixture = insider_sub.add_parser("fixture-run", help="Run the insider fixture through the shared slice flow")
    insider_fixture.set_defaults(func=cmd_insider_fixture)
    insider_rewrite_ingest = insider_sub.add_parser("rewrite-ingest", help="Run the rewritten insider SEC ingest flow without the legacy insider DB")
    insider_rewrite_ingest.add_argument("--csv", required=True, help="Path to company universe CSV")
    insider_rewrite_ingest.add_argument("--cache-dir", default=str(default_insider_rewrite_cache()), help="Cache directory for SEC company map and raw Form 4 XML")
    insider_rewrite_ingest.add_argument("--max-filings", type=int, default=None, help="Maximum filings per company")
    insider_rewrite_ingest.add_argument("--start-date", dest="start_date", default=None, help="Historical backfill start date")
    insider_rewrite_ingest.add_argument("--end-date", dest="end_date", default=None, help="Historical backfill end date")
    insider_rewrite_ingest.add_argument("--no-resume", action="store_true", help="Ignore any existing ingest checkpoint state in the cache dir")
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
    insider_rewrite_run.add_argument("--no-resume", action="store_true", help="Ignore any existing ingest checkpoint state in the cache dir")
    insider_rewrite_run.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    insider_rewrite_run.add_argument("--sec-user-agent", required=True, help="SEC-compliant user agent")
    insider_rewrite_run.set_defaults(func=cmd_insider_rewrite_run)
    insider_report = insider_sub.add_parser("report", help="Render persisted insider results without recomputing")
    insider_report.set_defaults(func=cmd_source_report, source_name="insider")
    insider_candidates = insider_sub.add_parser("candidate-discovery", help="Show top unresolved insider issuer candidates worth reviewing")
    insider_candidates.add_argument("--run-id")
    insider_candidates.add_argument("--limit", type=int, default=10)
    insider_candidates.set_defaults(func=cmd_insider_candidate_discovery)
    insider_ingest = insider_sub.add_parser("ingest", help="Deprecated compatibility alias for `compat insider ingest`")
    insider_ingest.add_argument("--csv", required=True)
    insider_ingest.add_argument("--max-filings", type=int)
    insider_ingest.add_argument("--start-date")
    insider_ingest.add_argument("--end-date")
    insider_ingest.add_argument("--async", dest="use_async", action="store_true")
    insider_ingest.add_argument("--concurrency", type=int, default=5)
    insider_ingest.set_defaults(func=cmd_insider_ingest)
    insider_parse = insider_sub.add_parser("parse", help="Deprecated compatibility alias for `compat insider parse`")
    insider_parse.set_defaults(func=cmd_insider_parse)
    insider_classify = insider_sub.add_parser("classify", help="Deprecated compatibility alias for `compat insider classify`")
    insider_classify.set_defaults(func=cmd_insider_classify)
    insider_run_legacy = insider_sub.add_parser("run-legacy", help="Deprecated compatibility alias for `compat insider run-legacy`")
    insider_run_legacy.add_argument("--csv", required=True)
    insider_run_legacy.add_argument("--date")
    insider_run_legacy.add_argument("--max-filings", type=int)
    insider_run_legacy.set_defaults(func=cmd_insider_run_legacy)
    insider_score = insider_sub.add_parser("score", help="Deprecated compatibility alias for `compat insider score`")
    insider_score.add_argument("--date")
    insider_score.set_defaults(func=cmd_insider_score)
    insider_status = insider_sub.add_parser("status", help="Show insider legacy + derived status")
    insider_status.set_defaults(func=cmd_insider_status)

    congress = subparsers.add_parser("congress", help="Congress engine commands")
    congress_sub = congress.add_subparsers(dest="congress_command")
    congress_fixture = congress_sub.add_parser("fixture-run", help="Run the congress fixture through the shared slice flow")
    congress_fixture.set_defaults(func=cmd_congress_fixture)
    congress_rewrite_ingest_house = congress_sub.add_parser("rewrite-ingest-house", help="Download real House PTR PDFs directly into the rewrite cache without the legacy congress DB")
    congress_rewrite_ingest_house.add_argument("--cache-dir", default=str(default_congress_rewrite_cache()), help="Rewrite cache root for House PDFs and FD XML")
    congress_rewrite_ingest_house.add_argument("--days", type=int, default=90, help="Lookback days for FD XML PTR filtering")
    congress_rewrite_ingest_house.add_argument("--max-filings", type=int, default=None, help="Maximum PTR PDFs to fetch")
    congress_rewrite_ingest_house.add_argument("--force", action="store_true", help="Force re-download of cached PDFs")
    congress_rewrite_ingest_house.set_defaults(func=cmd_congress_rewrite_ingest_house)
    congress_rewrite_house = congress_sub.add_parser("rewrite-score-house", help="Run direct congress House PDF parsing/scoring from cached text PDFs without the legacy congress DB")
    congress_rewrite_house.add_argument("--pdf-dir", default=str(default_congress_rewrite_cache() / "pdfs" / "house"), help="Directory containing cached House PTR PDFs")
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
    congress_rewrite_ingest_senate = congress_sub.add_parser("rewrite-ingest-senate", help="Download real Senate electronic PTR HTML directly into the rewrite cache without the legacy congress DB")
    congress_rewrite_ingest_senate.add_argument("--cache-dir", default=str(default_congress_rewrite_cache()), help="Rewrite cache root for Senate HTML")
    congress_rewrite_ingest_senate.add_argument("--days", type=int, default=90, help="Lookback days for Senate search")
    congress_rewrite_ingest_senate.add_argument("--max-filings", type=int, default=None, help="Maximum Senate filings to fetch")
    congress_rewrite_ingest_senate.add_argument("--force", action="store_true", help="Force re-download of cached filings")
    congress_rewrite_ingest_senate.set_defaults(func=cmd_congress_rewrite_ingest_senate)
    congress_rewrite_score_senate = congress_sub.add_parser("rewrite-score-senate", help="Run direct congress Senate electronic HTML scoring without the legacy congress DB")
    congress_rewrite_score_senate.add_argument("--html-dir", default=str(default_congress_rewrite_cache() / "pdfs" / "senate"), help="Directory containing cached Senate PTR HTML files")
    congress_rewrite_score_senate.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    congress_rewrite_score_senate.add_argument("--window", type=int, default=90, help="Lookback window in days")
    congress_rewrite_score_senate.add_argument("--max-files", type=int, default=None, help="Maximum HTML files to process")
    congress_rewrite_score_senate.set_defaults(func=cmd_congress_rewrite_score_senate)
    congress_rewrite_run_senate = congress_sub.add_parser("rewrite-run-senate", help="Run direct Senate electronic filing download followed by direct Senate scoring")
    congress_rewrite_run_senate.add_argument("--cache-dir", default=str(default_congress_rewrite_cache()), help="Rewrite cache root for Senate HTML")
    congress_rewrite_run_senate.add_argument("--days", type=int, default=90, help="Lookback days for Senate search")
    congress_rewrite_run_senate.add_argument("--max-filings", type=int, default=None, help="Maximum Senate filings to fetch")
    congress_rewrite_run_senate.add_argument("--force", action="store_true", help="Force re-download of cached filings")
    congress_rewrite_run_senate.add_argument("--date", default=None, help="Reference date YYYY-MM-DD")
    congress_rewrite_run_senate.add_argument("--window", type=int, default=90, help="Lookback window in days")
    congress_rewrite_run_senate.add_argument("--score-max-files", type=int, default=None, help="Maximum cached HTML files to score")
    congress_rewrite_run_senate.set_defaults(func=cmd_congress_rewrite_run_senate)
    congress_report = congress_sub.add_parser("report", help="Render persisted congress results without recomputing")
    congress_report.set_defaults(func=cmd_source_report, source_name="congress")
    congress_candidates = congress_sub.add_parser("candidate-discovery", help="Show top unresolved House issuer candidates worth adding to the canonical map")
    congress_candidates.add_argument("--branch", choices=["house", "senate"], default="house")
    congress_candidates.add_argument("--run-id")
    congress_candidates.add_argument("--limit", type=int, default=10)
    congress_candidates.set_defaults(func=cmd_congress_candidate_discovery)
    congress_init = congress_sub.add_parser("init", help="Deprecated compatibility alias for `compat congress init`")
    congress_init.set_defaults(func=cmd_congress_init)
    congress_ingest = congress_sub.add_parser("ingest", help="Deprecated compatibility alias for `compat congress ingest`")
    congress_ingest.add_argument("--days", type=int, default=90)
    congress_ingest.add_argument("--house-only", action="store_true")
    congress_ingest.add_argument("--senate-only", action="store_true")
    congress_ingest.add_argument("--bulk", action="store_true")
    congress_ingest.set_defaults(func=cmd_congress_ingest)
    congress_parse = congress_sub.add_parser("parse", help="Deprecated compatibility alias for `compat congress parse`")
    congress_parse.add_argument("--force", action="store_true")
    congress_parse.set_defaults(func=cmd_congress_parse)
    congress_score = congress_sub.add_parser("score", help="Deprecated compatibility alias for `compat congress score`")
    congress_score.add_argument("--window", type=int, default=90)
    congress_score.set_defaults(func=cmd_congress_score)
    congress_status = congress_sub.add_parser("status", help="Show congress legacy + derived status")
    congress_status.set_defaults(func=cmd_congress_status)

    compat = subparsers.add_parser("compat", help="Legacy compatibility commands (slated for removal)")
    compat_sub = compat.add_subparsers(dest="compat_command")

    ins_compat = compat_sub.add_parser("insider", help="Legacy insider commands")
    ins_compat_sub = ins_compat.add_subparsers(dest="insider_compat_command")
    
    ins_ingest = ins_compat_sub.add_parser("ingest", help="Legacy insider ingest")
    ins_ingest.add_argument("--csv", required=True)
    ins_ingest.add_argument("--max-filings", type=int)
    ins_ingest.add_argument("--start-date")
    ins_ingest.add_argument("--end-date")
    ins_ingest.add_argument("--async", dest="use_async", action="store_true")
    ins_ingest.add_argument("--concurrency", type=int, default=5)
    ins_ingest.set_defaults(func=cmd_insider_ingest)

    ins_parse = ins_compat_sub.add_parser("parse")
    ins_parse.set_defaults(func=cmd_insider_parse)

    ins_classify = ins_compat_sub.add_parser("classify")
    ins_classify.set_defaults(func=cmd_insider_classify)

    ins_run = ins_compat_sub.add_parser("run-legacy")
    ins_run.add_argument("--csv", required=True)
    ins_run.add_argument("--date")
    ins_run.add_argument("--max-filings", type=int)
    ins_run.set_defaults(func=cmd_insider_run_legacy)

    ins_score = ins_compat_sub.add_parser("score")
    ins_score.add_argument("--date")
    ins_score.set_defaults(func=cmd_insider_score)

    cong_compat = compat_sub.add_parser("congress", help="Legacy congress commands")
    cong_compat_sub = cong_compat.add_subparsers(dest="congress_compat_command")

    cong_init = cong_compat_sub.add_parser("init")
    cong_init.set_defaults(func=cmd_congress_init)

    cong_ingest = cong_compat_sub.add_parser("ingest")
    cong_ingest.add_argument("--days", type=int, default=90)
    cong_ingest.add_argument("--house-only", action="store_true")
    cong_ingest.add_argument("--senate-only", action="store_true")
    cong_ingest.add_argument("--bulk", action="store_true")
    cong_ingest.set_defaults(func=cmd_congress_ingest)

    cong_parse = cong_compat_sub.add_parser("parse")
    cong_parse.add_argument("--force", action="store_true")
    cong_parse.set_defaults(func=cmd_congress_parse)

    cong_score = cong_compat_sub.add_parser("score")
    cong_score.add_argument("--window", type=int, default=90)
    cong_score.set_defaults(func=cmd_congress_score)

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

    validate = subparsers.add_parser("validate", help="Run forward-return validation on persisted signals")
    validate.add_argument("--source", choices=["insider", "congress"], default=None, help="Filter by source")
    validate.add_argument("--min-date", default=None, help="Minimum execution date YYYY-MM-DD")
    validate.add_argument("--max-date", default=None, help="Maximum execution date YYYY-MM-DD")
    validate.add_argument("--forward-days", default=None, help="Comma-separated forward windows (default: 5,10,20,60)")
    validate.add_argument("--baseline", action="store_true", help="Include trivial baseline comparison")
    validate.add_argument("--regime", action="store_true", help="Include market regime analysis")
    validate.set_defaults(func=cmd_validate)

    brief = subparsers.add_parser("brief", help="Generate high-signal daily brief")
    brief.add_argument("--date", default=None, help="Reference date YYYY-MM-DD (default: today)")
    brief.add_argument("--sectors", action="store_true", help="Include sector summary (requires yfinance)")
    brief.set_defaults(func=cmd_brief)

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
