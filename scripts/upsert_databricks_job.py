#!/usr/bin/env python3
"""Upsert Databricks Job from databricks/jobs/data_quality_pipeline.json and optionally run it.

Stdlib only (urllib). Env:
  DATABRICKS_HOST   workspace URL (https://...)
  DATABRICKS_TOKEN  PAT / OAuth token (never printed)
  RUN_JOB           true|false — default true (CI after deploy)
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

JOB_NAME = "data-quality-forge-pipeline"
JOB_DEF_REL = Path("databricks/jobs/data_quality_pipeline.json")
LIST_PAGE_SIZE = 25
# Optional short poll after run-now (seconds). Exit 0 after start even if still running.
WAIT_BUDGET_SEC = 180
POLL_INTERVAL_SEC = 10


def _die(msg: str, code: int = 1) -> None:
    print(f"::error::{msg}", file=sys.stderr)
    sys.exit(code)


def _truthy(value: str | None, default: bool = True) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _host_and_token() -> tuple[str, str]:
    host = os.environ.get("DATABRICKS_HOST", "").strip().rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    missing = []
    if not host:
        missing.append("DATABRICKS_HOST")
    if not token:
        missing.append("DATABRICKS_TOKEN")
    if missing:
        _die(
            "Missing required environment variable(s): "
            + ", ".join(missing)
            + ". Set them as GitHub Actions secrets (never commit tokens)."
        )
    if not host.startswith(("http://", "https://")):
        host = "https://" + host
    return host, token


def _api(
    host: str,
    token: str,
    method: str,
    path: str,
    payload: dict | None = None,
    query: dict | None = None,
) -> dict:
    url = host + path
    if query:
        url += "?" + urllib.parse.urlencode({k: v for k, v in query.items() if v is not None})
    data = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read().decode("utf-8", errors="replace")
            status = response.status
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        # Never echo Authorization / token; detail is API JSON only.
        _die(f"{method} {path} failed with HTTP {exc.code}: {detail}")
    except urllib.error.URLError as exc:
        _die(f"Could not reach Databricks host: {exc.reason}")
    if status < 200 or status >= 300:
        _die(f"{method} {path} failed with HTTP {status}: {body}")
    if not body.strip():
        return {}
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        _die(f"{method} {path} returned non-JSON body: {body[:500]}")


def _load_settings(repo_root: Path) -> dict:
    path = repo_root / JOB_DEF_REL
    if not path.is_file():
        _die(f"Job definition not found: {path}")
    try:
        settings = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        _die(f"Invalid JSON in {path}: {exc}")
    if settings.get("name") != JOB_NAME:
        _die(f"Expected job name {JOB_NAME!r} in {path}, got {settings.get('name')!r}")
    return settings


def _find_job_id(host: str, token: str, name: str) -> int | None:
    page_token = None
    while True:
        query: dict = {"limit": LIST_PAGE_SIZE}
        if page_token:
            query["page_token"] = page_token
        result = _api(host, token, "GET", "/api/2.1/jobs/list", query=query)
        for job in result.get("jobs") or []:
            settings = job.get("settings") or {}
            if settings.get("name") == name:
                job_id = job.get("job_id")
                if job_id is None:
                    _die(f"Matched job named {name!r} but job_id missing: {job}")
                return int(job_id)
        page_token = result.get("next_page_token")
        if not page_token:
            return None


def _upsert(host: str, token: str, settings: dict) -> int:
    existing = _find_job_id(host, token, JOB_NAME)
    if existing is not None:
        print(f"Found existing job {JOB_NAME!r} (job_id={existing}); resetting settings.")
        _api(
            host,
            token,
            "POST",
            "/api/2.1/jobs/reset",
            payload={"job_id": existing, "new_settings": settings},
        )
        return existing
    print(f"No job named {JOB_NAME!r}; creating.")
    created = _api(host, token, "POST", "/api/2.1/jobs/create", payload=settings)
    job_id = created.get("job_id")
    if job_id is None:
        _die(f"jobs/create succeeded but no job_id in response: {created}")
    print(f"Created job_id={job_id}")
    return int(job_id)


def _run_now(host: str, token: str, job_id: int) -> int:
    result = _api(host, token, "POST", "/api/2.1/jobs/run-now", payload={"job_id": job_id})
    run_id = result.get("run_id")
    if run_id is None:
        _die(f"jobs/run-now succeeded but no run_id: {result}")
    run_id = int(run_id)
    runs_url = f"{host}/#job/{job_id}/run/{run_id}"
    print(f"Started run_id={run_id}")
    print(f"Run page (workspace UI): {runs_url}")
    return run_id


def _maybe_wait(host: str, token: str, run_id: int) -> None:
    """Poll briefly for a terminal state; do not fail the deploy if still running."""
    deadline = time.monotonic() + WAIT_BUDGET_SEC
    terminal = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
    while time.monotonic() < deadline:
        info = _api(
            host,
            token,
            "GET",
            "/api/2.1/jobs/runs/get",
            query={"run_id": run_id},
        )
        state = (info.get("state") or {})
        life = state.get("life_cycle_state") or "UNKNOWN"
        result_state = state.get("result_state")
        print(f"Run {run_id} life_cycle_state={life} result_state={result_state}")
        if life in terminal:
            if result_state and result_state != "SUCCESS":
                _die(
                    f"Job run {run_id} finished with result_state={result_state}: "
                    f"{state.get('state_message') or ''}"
                )
            print(f"Run {run_id} completed successfully.")
            return
        time.sleep(POLL_INTERVAL_SEC)
    print(
        f"Run {run_id} still in progress after ~{WAIT_BUDGET_SEC}s; "
        "exiting 0 (check the run page in Databricks)."
    )


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    host, token = _host_and_token()
    settings = _load_settings(repo_root)
    run_job = _truthy(os.environ.get("RUN_JOB"), default=True)

    job_id = _upsert(host, token, settings)
    print(f"Upserted job_id={job_id} name={JOB_NAME}")

    if not run_job:
        print("RUN_JOB is false; skipping run-now.")
        return

    run_id = _run_now(host, token, job_id)
    _maybe_wait(host, token, run_id)


if __name__ == "__main__":
    main()
