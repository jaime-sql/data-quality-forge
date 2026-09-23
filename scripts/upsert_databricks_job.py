#!/usr/bin/env python3
"""Upsert a Databricks Lakeflow/DLT Pipeline and optionally start an update.

Stdlib only (urllib). Env:
  DATABRICKS_HOST      workspace URL (https://...)
  DATABRICKS_TOKEN     PAT (never printed)
  RUN_JOB              true|false — start a pipeline update after upsert (default true)
  DATABRICKS_CATALOG   Unity Catalog name (default: workspace)
  DATABRICKS_SCHEMA    Schema / target (default: data_quality_forge)
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

PIPELINE_NAME = "data-quality-forge-pipeline"
PIPELINE_DEF_REL = Path("databricks/pipelines/data_quality_pipeline.json")
# Legacy path still accepted if someone has not pulled the new folder yet
LEGACY_JOB_DEF_REL = Path("databricks/jobs/data_quality_pipeline.json")
LIST_PAGE_SIZE = 25
WAIT_BUDGET_SEC = 300
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
        with urllib.request.urlopen(request, timeout=180) as response:
            body = response.read().decode("utf-8", errors="replace")
            status = response.status
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
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
    path = repo_root / PIPELINE_DEF_REL
    if not path.is_file():
        _die(f"Pipeline definition not found: {path}")
    try:
        settings = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        _die(f"Invalid JSON in {path}: {exc}")
    if settings.get("name") != PIPELINE_NAME:
        _die(f"Expected pipeline name {PIPELINE_NAME!r} in {path}, got {settings.get('name')!r}")

    catalog = os.environ.get("DATABRICKS_CATALOG", "").strip()
    schema = os.environ.get("DATABRICKS_SCHEMA", "").strip()
    if catalog:
        settings["catalog"] = catalog
    if schema:
        settings["schema"] = schema
    return settings


def _find_pipeline_id(host: str, token: str, name: str) -> str | None:
    page_token = None
    while True:
        query: dict = {"max_results": LIST_PAGE_SIZE}
        if page_token:
            query["page_token"] = page_token
        result = _api(host, token, "GET", "/api/2.0/pipelines", query=query)
        for pipe in result.get("statuses") or result.get("pipelines") or []:
            # list returns {pipeline_id, name, state, ...} under statuses on some APIs
            if pipe.get("name") == name:
                pid = pipe.get("pipeline_id")
                if not pid:
                    _die(f"Matched pipeline named {name!r} but pipeline_id missing: {pipe}")
                return str(pid)
        page_token = result.get("next_page_token")
        if not page_token:
            return None


def _api_allow_fail(
    host: str,
    token: str,
    method: str,
    path: str,
    payload: dict | None = None,
) -> tuple[int, dict | str]:
    """Like _api but returns (status, body) instead of dying on HTTP errors."""
    url = host + path
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
        with urllib.request.urlopen(request, timeout=180) as response:
            body = response.read().decode("utf-8", errors="replace")
            status = response.status
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(detail)
        except json.JSONDecodeError:
            return exc.code, detail
    except urllib.error.URLError as exc:
        _die(f"Could not reach Databricks host: {exc.reason}")
    if not body.strip():
        return status, {}
    try:
        return status, json.loads(body)
    except json.JSONDecodeError:
        return status, body


def _storage_fallback(settings: dict) -> dict:
    """Hive-style pipeline when Unity Catalog catalog/schema is unavailable."""
    fb = {
        "name": settings["name"],
        "storage": "dbfs:/pipelines/data-quality-forge",
        "target": settings.get("schema") or "data_quality_forge",
        "libraries": settings["libraries"],
        "channel": settings.get("channel", "CURRENT"),
        "continuous": False,
        "development": True,
        "photon": True,
    }
    return fb


def _upsert(host: str, token: str, settings: dict) -> str:
    existing = _find_pipeline_id(host, token, PIPELINE_NAME)
    attempts = [settings, _storage_fallback(settings)]
    if existing is not None:
        print(f"Found existing pipeline {PIPELINE_NAME!r} (pipeline_id={existing}); updating.")
        last_err = None
        for payload_base in attempts:
            payload = dict(payload_base)
            payload["id"] = existing
            status, body = _api_allow_fail(
                host, token, "PUT", f"/api/2.0/pipelines/{existing}", payload=payload
            )
            if 200 <= status < 300:
                print(f"Updated pipeline with keys: {sorted(k for k in payload if k != 'id')}")
                return existing
            last_err = body
            print(f"Update attempt failed HTTP {status}; trying fallback settings…")
        _die(f"Could not update pipeline {existing}: {last_err}")

    print(f"No pipeline named {PIPELINE_NAME!r}; creating.")
    last_err = None
    for payload in attempts:
        status, body = _api_allow_fail(host, token, "POST", "/api/2.0/pipelines", payload=payload)
        if 200 <= status < 300 and isinstance(body, dict):
            pipeline_id = body.get("pipeline_id")
            if pipeline_id:
                print(f"Created pipeline_id={pipeline_id} with keys: {sorted(payload)}")
                return str(pipeline_id)
        last_err = body
        print(f"Create attempt failed HTTP {status}; trying fallback settings…")
    _die(f"Could not create pipeline: {last_err}")


def _start_update(host: str, token: str, pipeline_id: str) -> str:
    result = _api(
        host,
        token,
        "POST",
        f"/api/2.0/pipelines/{pipeline_id}/updates",
        payload={"full_refresh": True},
    )
    update_id = result.get("update_id")
    if not update_id:
        _die(f"pipelines/.../updates succeeded but no update_id: {result}")
    update_id = str(update_id)
    ui = f"{host}/#joblist/pipelines/{pipeline_id}/updates/{update_id}"
    print(f"Started update_id={update_id}")
    print(f"Pipeline update UI: {ui}")
    return update_id


def _maybe_wait(host: str, token: str, pipeline_id: str, update_id: str) -> None:
    deadline = time.monotonic() + WAIT_BUDGET_SEC
    terminal = {"COMPLETED", "FAILED", "CANCELED"}
    while time.monotonic() < deadline:
        info = _api(
            host,
            token,
            "GET",
            f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}",
        )
        state = (info.get("update") or info).get("state") or "UNKNOWN"
        print(f"Update {update_id} state={state}")
        if state in terminal:
            if state != "COMPLETED":
                _die(f"Pipeline update {update_id} finished with state={state}: {info}")
            print(f"Update {update_id} completed successfully.")
            return
        time.sleep(POLL_INTERVAL_SEC)
    print(
        f"Update {update_id} still in progress after ~{WAIT_BUDGET_SEC}s; "
        "exiting 0 (check the pipeline UI in Databricks)."
    )


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    host, token = _host_and_token()
    settings = _load_settings(repo_root)
    run_update = _truthy(os.environ.get("RUN_JOB"), default=True)

    pipeline_id = _upsert(host, token, settings)
    print(f"Upserted pipeline_id={pipeline_id} name={PIPELINE_NAME}")

    if not run_update:
        print("RUN_JOB is false; skipping pipeline update.")
        return

    update_id = _start_update(host, token, pipeline_id)
    _maybe_wait(host, token, pipeline_id, update_id)


if __name__ == "__main__":
    main()
