from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, UTC


def utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class RunRecord:
    run_id: str
    run_type: str
    source: str
    status: str
    params_json: str
    code_version: str
    method_versions_json: str
    started_at: str
    ended_at: str | None = None
    summary_json: str | None = None


def make_run(run_type: str, source: str, code_version: str, params: dict, method_versions: dict) -> RunRecord:
    return RunRecord(
        run_id=str(uuid.uuid4()),
        run_type=run_type,
        source=source,
        status="STARTED",
        params_json=json.dumps(params, sort_keys=True),
        code_version=code_version,
        method_versions_json=json.dumps(method_versions, sort_keys=True),
        started_at=utcnow_iso(),
    )

