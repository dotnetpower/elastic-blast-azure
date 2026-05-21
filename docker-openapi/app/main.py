"""ElasticBLAST on Azure — OpenAPI Server v3.1.

Runs inside AKS as a pod. Self-contained: stores job state in K8s ConfigMaps,
optionally forwards events to Control Plane via webhook.
"""

from __future__ import annotations

import configparser
import glob
import gzip
import hashlib
import hmac
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
import zipfile
from datetime import datetime, timezone
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, Literal, Optional
from urllib.parse import urlparse

import uvicorn
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

from util import run_cancellable, safe_exec

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("elb-openapi")

# ── Configuration (env var → fallback) ─────────────────────────────────────
CLUSTER_NAME = os.environ.get("ELB_CLUSTER_NAME", "elastic-blast-on-azure")
STORAGE_ACCOUNT = os.environ.get("ELB_STORAGE_ACCOUNT", "stgelb")
RESOURCE_GROUP = os.environ.get("ELB_RESOURCE_GROUP", "rg-elb")
AZURE_REGION = os.environ.get("ELB_AZURE_REGION", "koreacentral")
ACR_NAME = os.environ.get("ELB_ACR_NAME", "elbacr01")
ACR_RESOURCE_GROUP = os.environ.get("ELB_ACR_RESOURCE_GROUP", "rg-elbacr-01")
MACHINE_TYPE = os.environ.get("ELB_MACHINE_TYPE", "Standard_E16s_v5")
NUM_NODES = int(os.environ.get("ELB_NUM_NODES", "3"))
CONTROL_PLANE_URL = os.environ.get("CONTROL_PLANE_URL", "")  # optional webhook
INTERNAL_TOKEN = os.environ.get("ELB_OPENAPI_INTERNAL_TOKEN", "").strip()
# API gating token. When set, all mutating / data-egress endpoints require
# the header ``X-ELB-API-Token: <value>``. When empty, the service requires
# ``ELB_OPENAPI_ALLOW_UNAUTHENTICATED=1`` to start — fail-closed by default
# so a misconfigured deployment cannot accidentally expose unauthenticated
# job submission, deletion, or result download.
API_TOKEN = os.environ.get("ELB_OPENAPI_API_TOKEN", "").strip()
ALLOW_UNAUTHENTICATED = os.environ.get("ELB_OPENAPI_ALLOW_UNAUTHENTICATED", "").strip().lower() in {"1", "true", "yes"}
if not API_TOKEN and not ALLOW_UNAUTHENTICATED:
    # We log instead of raising at import time so the pod can still expose
    # /healthz for readiness probes — but auth-gated endpoints will reject
    # all requests until configured. See ``require_api_token`` below.
    logger.error(
        "ELB_OPENAPI_API_TOKEN is not configured and "
        "ELB_OPENAPI_ALLOW_UNAUTHENTICATED is not set. All authenticated "
        "endpoints will return 503 until one of these env vars is provided."
    )
VERSION = "3.3.0"

MAX_ACTIVE_SUBMISSIONS = max(1, int(os.environ.get("ELB_OPENAPI_MAX_ACTIVE_SUBMISSIONS", "1")))
DISPATCH_INTERVAL_SECONDS = max(1, int(os.environ.get("ELB_OPENAPI_DISPATCH_INTERVAL_SECONDS", "5")))
WATCHDOG_INTERVAL_SECONDS = max(5, int(os.environ.get("ELB_OPENAPI_WATCHDOG_INTERVAL_SECONDS", "60")))
SUBMIT_STUCK_SECONDS = max(60, int(os.environ.get("ELB_OPENAPI_SUBMIT_STUCK_SECONDS", "7200")))
PENDING_STUCK_SECONDS = max(300, int(os.environ.get("ELB_OPENAPI_PENDING_STUCK_SECONDS", "1800")))
RUNNING_IDLE_SECONDS = max(300, int(os.environ.get("ELB_OPENAPI_RUNNING_IDLE_SECONDS", "10800")))
FINALIZER_STUCK_SECONDS = max(300, int(os.environ.get("ELB_OPENAPI_FINALIZER_STUCK_SECONDS", "1800")))
BACKGROUND_DISABLED = os.environ.get("ELB_OPENAPI_DISABLE_BACKGROUND", "").lower() in {"1", "true", "yes"}

_BLOB_URL_RE = re.compile(r"^https://[a-z0-9]+\.blob\.core\.windows\.net/[a-z0-9][-a-z0-9]*/.*$")
_VALID_PROGRAMS = frozenset({"blastp", "blastn", "blastx", "psiblast", "rpsblast", "rpstblastn", "tblastn", "tblastx"})
_CM_LABEL = "elb-job=true"
_CM_PREFIX = "elb-job-"
_TERMINAL_STATES = {"completed", "failed", "cancelled"}
_ACTIVE_STATES = {"dispatching", "submitting", "running"}
_QUEUED_STATES = {"queued"}
_PRIORITY_LABELS = {"low": 25, "normal": 50, "high": 75, "urgent": 100}
_SUBMISSION_SOURCES = {"dashboard", "external_api", "terminal", "system"}
_DEFAULT_EXTERNAL_SOURCE = "external_api"
_RESULT_FILE_RE = re.compile(r"(?P<name>[^\s;]+\.(?:xml|out)(?:\.gz)?)", re.IGNORECASE)

_TOOL_PATH = "/opt/venv/bin"
if os.path.isdir(_TOOL_PATH):
    current_path = os.environ.get("PATH", "")
    if _TOOL_PATH not in current_path.split(os.pathsep):
        os.environ["PATH"] = f"{_TOOL_PATH}{os.pathsep}{current_path}" if current_path else _TOOL_PATH


# ── Workload Identity bootstrap ───────────────────────────────────────────
def _wi_az_login() -> None:
    """Log in to Azure CLI using Workload Identity federated token, if available.

    This enables `elastic-blast` (which shells out to `az account show`) and
    other `az`-dependent helpers to authenticate without an interactive login.
    Runs once at startup.

    Notes
    -----
    * azcopy is configured separately via ``AZCOPY_AUTO_LOGIN_TYPE=WORKLOAD``
      (set in the Dockerfile) and does NOT depend on this function.
    * ``--allow-no-subscriptions`` is required because the Workload Identity
      MI typically only has data-plane RBAC (Storage Blob Data Contributor on
      a single account) and no subscription-scope role. Without this flag
      ``az login`` fails with ``ERROR: No subscriptions found``.
    """
    client_id = os.environ.get("AZURE_CLIENT_ID", "")
    tenant_id = os.environ.get("AZURE_TENANT_ID", "")
    token_file = os.environ.get("AZURE_FEDERATED_TOKEN_FILE", "")
    # Half-configured WI is a deployment bug — surface it loudly so it is
    # caught in pod logs rather than silently degrading at job-submit time.
    if token_file and not client_id:
        logger.error(
            "Workload Identity token file is mounted but AZURE_CLIENT_ID is empty. "
            "The ServiceAccount is likely missing the "
            "'azure.workload.identity/client-id' annotation."
        )
        return
    if not (client_id and tenant_id and token_file):
        if not os.environ.get("KUBERNETES_SERVICE_HOST"):
            logger.info("Workload Identity env vars not set outside Kubernetes, skipping az login")
            return
        logger.info("Workload Identity env vars not set, trying Azure CLI MSI login")
        try:
            safe_exec(["az", "login", "--identity", "--allow-no-subscriptions"], timeout=60)
            logger.info("az login succeeded via Managed Identity fallback")
        except Exception as exc:
            logger.warning("az login via Managed Identity fallback failed: %s", str(exc)[:1000])
        return
    if not os.path.isfile(token_file):
        logger.warning("Federated token file not found: %s", token_file)
        return
    try:
        with open(token_file) as f:
            token = f.read().strip()
        safe_exec([
            "az", "login", "--service-principal",
            "-u", client_id, "-t", tenant_id,
            "--federated-token", token,
            "--allow-no-subscriptions",
        ], timeout=30)
        logger.info("az login succeeded via Workload Identity (client=%s)", client_id[:8])
    except Exception as exc:
        # Log the full error — earlier truncation hid the real cause
        # ("No subscriptions found") for hours of diagnosis.
        logger.warning("az login via Workload Identity failed: %s", str(exc)[:1000])

_wi_az_login()

def _ensure_az_login() -> None:
    try:
        safe_exec(["az", "account", "show", "--query", "user.name", "--output", "tsv"], timeout=30)
        return
    except Exception:
        pass
    try:
        safe_exec(["az", "login", "--identity", "--allow-no-subscriptions"], timeout=60)
        safe_exec(["az", "account", "show", "--query", "user.name", "--output", "tsv"], timeout=30)
    except Exception as exc:
        raise RuntimeError(f"Azure CLI login unavailable: {str(exc)[:500]}") from exc

# ── App ────────────────────────────────────────────────────────────────────
tags_metadata = [
    {"name": "System", "description": "Health checks and configuration"},
    {"name": "Cluster", "description": "AKS cluster status"},
    {"name": "Jobs", "description": "BLAST job submission and monitoring"},
]

app = FastAPI(
    title="ElasticBLAST on Azure",
    description=(
        "REST API for running ElasticBLAST searches on Azure Kubernetes Service.\n\n"
        "**Two submission modes:**\n"
        "- **Mode A** — Advanced: provide full Azure Blob URLs for db, queries, results\n"
        "- **Mode B** — Simple: provide inline FASTA + short DB name + optional taxonomy filter\n\n"
        "Job state persists in K8s ConfigMaps — survives pod restarts."
    ),
    version=VERSION,
    openapi_url="/openapi.json",
    openapi_tags=tags_metadata,
)

# Security headers middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response as StarletteResponse


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response: StarletteResponse = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response

app.add_middleware(SecurityHeadersMiddleware)

# CORS — Control Plane SPA origin. Fail-closed: only the configured origin
# is allowed. A wildcard CORS policy combined with the API-token auth model
# would let a hostile site read API responses on behalf of any logged-in
# operator, so we never default to ``*``.
_cors_origins: list[str] = []
if CONTROL_PLANE_URL:
    from urllib.parse import urlparse
    _parsed = urlparse(CONTROL_PLANE_URL)
    _origin = f"{_parsed.scheme}://{_parsed.netloc}"
    _cors_origins = [_origin]
elif ALLOW_UNAUTHENTICATED:
    # Local development opt-in only.
    logger.warning(
        "CONTROL_PLANE_URL is unset; CORS defaults to '*' because "
        "ELB_OPENAPI_ALLOW_UNAUTHENTICATED=1 (development mode)."
    )
    _cors_origins = ["*"]
else:
    logger.warning(
        "CONTROL_PLANE_URL is unset; CORS will reject all cross-origin "
        "requests. Set CONTROL_PLANE_URL to enable the Control Plane SPA."
    )

from starlette.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-ELB-API-Token", "X-ELB-Internal-Token"],
)


# ── Authentication ─────────────────────────────────────────────────────────
def require_api_token(
    x_elb_api_token: Optional[str] = Header(None, alias="X-ELB-API-Token"),
) -> None:
    """FastAPI dependency: require ``X-ELB-API-Token`` on protected endpoints.

    Behaviour matrix
    ----------------
    * ``ELB_OPENAPI_API_TOKEN`` set (recommended) → request must carry a
      matching token. Comparison uses :func:`hmac.compare_digest` to avoid
      timing leaks.
    * ``ELB_OPENAPI_API_TOKEN`` unset and
      ``ELB_OPENAPI_ALLOW_UNAUTHENTICATED=1`` → access is granted (local
      development only). A warning is logged on startup.
    * Otherwise (the default) → return ``503 Service Unavailable`` so the
      operator notices the missing configuration instead of silently
      exposing the API.
    """
    if API_TOKEN:
        if not x_elb_api_token or not hmac.compare_digest(x_elb_api_token, API_TOKEN):
            raise HTTPException(401, "missing or invalid X-ELB-API-Token")
        return
    if ALLOW_UNAUTHENTICATED:
        return
    raise HTTPException(
        503,
        "API authentication is not configured. Set ELB_OPENAPI_API_TOKEN or, "
        "for local development, ELB_OPENAPI_ALLOW_UNAUTHENTICATED=1.",
    )


# ── ConfigMap-based job storage ────────────────────────────────────────────

def _cm_name(job_id: str) -> str:
    return f"{_CM_PREFIX}{job_id}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(value: Any) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _duration_seconds(start: Any, end: Any | None = None) -> int | None:
    start_ts = _parse_ts(start)
    if start_ts <= 0:
        return None
    end_ts = _parse_ts(end) if end else time.time()
    if end_ts <= 0:
        return None
    return max(0, int(end_ts - start_ts))


def _age_seconds(value: Any) -> float:
    ts = _parse_ts(value)
    return max(0.0, time.time() - ts) if ts else 0.0


def _safe_label_value(value: Any, default: str = "unknown") -> str:
    raw = re.sub(r"[^a-z0-9_.-]", "-", str(value or default).lower()).strip(".-_")
    return (raw or default)[:63]


def _normalise_priority(priority: int | str | None) -> int:
    if isinstance(priority, str):
        label = priority.strip().lower()
        if label in _PRIORITY_LABELS:
            return _PRIORITY_LABELS[label]
        try:
            priority = int(label)
        except ValueError:
            return _PRIORITY_LABELS["normal"]
    if priority is None:
        return _PRIORITY_LABELS["normal"]
    return max(0, min(100, int(priority)))


def _normalise_submission_source(value: str | None) -> str:
    source = str(value or _DEFAULT_EXTERNAL_SOURCE).strip().lower()
    if source not in _SUBMISSION_SOURCES:
        raise HTTPException(400, "submission_source must be dashboard, external_api, terminal, or system")
    return source


def _effective_submission_source(value: str | None, internal_token: str | None) -> str:
    source = _normalise_submission_source(value)
    if source == _DEFAULT_EXTERNAL_SOURCE:
        return source
    if not INTERNAL_TOKEN or not internal_token or not hmac.compare_digest(internal_token, INTERNAL_TOKEN):
        raise HTTPException(403, "trusted submission_source requires internal authentication")
    return source


def _safe_detail_value(value: str | None, *, max_len: int = 128) -> str:
    raw = str(value or "").strip()
    if len(raw) > max_len:
        raise HTTPException(400, f"value must be at most {max_len} characters")
    if re.search(r"[\x00-\x1f]", raw):
        raise HTTPException(400, "value must not contain control characters")
    return raw


def _job_id_from_idempotency_key(key: str) -> str:
    cleaned = key.strip()
    if not cleaned or len(cleaned) > 256:
        raise HTTPException(400, "idempotency_key must be 1-256 characters")
    return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()[:12]


def _db_name_from_value(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme == "https":
        parts = [p for p in parsed.path.split("/") if p]
        return parts[-1] if parts else "unknown"
    return value.strip().strip("/").split("/")[-1] or "unknown"


@lru_cache(maxsize=1)
def _blast_version_detail() -> dict[str, str]:
    env_version = os.environ.get("ELB_BLAST_VERSION", "").strip()
    for cmd in (["blastn", "-version"], ["blastp", "-version"]):
        try:
            proc = safe_exec(cmd, timeout=5)
            text = (proc.stdout or proc.stderr or "").strip()
            first = text.splitlines()[0].strip() if text else ""
            if first:
                version = first.replace("blastn: ", "").replace("blastp: ", "")
                return {"version": version, "detail": text, "source": "blast_plus_binary"}
        except Exception:
            continue
    if env_version:
        return {"version": env_version, "detail": env_version, "source": "ELB_BLAST_VERSION"}
    return {"version": "unknown", "detail": "", "source": "not_available"}


def _db_version_detail(db_name: str) -> dict[str, str]:
    env_key = "ELB_DB_VERSION_" + re.sub(r"[^A-Z0-9]", "_", db_name.upper())
    version = os.environ.get(env_key, "").strip() or os.environ.get("ELB_DB_VERSION", "").strip()
    if version:
        return {"version": version, "source": env_key if os.environ.get(env_key) else "ELB_DB_VERSION"}
    safe_db = re.sub(r"[^A-Za-z0-9._-]", "", db_name)
    if safe_db:
        # Use a per-call unique tmp path: concurrent /v1/jobs/{id}/status
        # polls for jobs sharing the same db name would otherwise race on
        # the same fixed file (the metadata file is downloaded fresh each
        # call so there is no cache benefit to a stable name).
        local_path = f"/tmp/{safe_db}-metadata-{uuid.uuid4().hex}.json"
        metadata_url = f"{_blob_base()}/blast-db/{safe_db}/{safe_db}-nucl-metadata.json"
        try:
            _azcopy_login()
            _cleanup_tmp(local_path)
            safe_exec(["azcopy", "cp", metadata_url, local_path], timeout=60)
            with open(local_path, encoding="utf-8") as handle:
                metadata = json.load(handle)
            files = metadata.get("files") if isinstance(metadata.get("files"), list) else []
            source_version = ""
            for item in files:
                match = re.search(r"/(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})/", str(item))
                if match:
                    source_version = match.group(1)
                    break
            metadata_version = str(metadata.get("version") or "").strip()
            effective_version = source_version or metadata_version
            if effective_version:
                detail = {
                    "metadata_version": metadata_version,
                    "source_version": source_version,
                    "dbtype": str(metadata.get("dbtype") or ""),
                    "number_of_sequences": str(metadata.get("number-of-sequences") or ""),
                    "number_of_letters": str(metadata.get("number-of-letters") or ""),
                }
                return {"version": effective_version, "detail": json.dumps(detail, sort_keys=True), "source": "blastdb_metadata"}
        except Exception as exc:
            logger.debug("DB version metadata unavailable for %s: %s", db_name, str(exc)[:200])
        finally:
            _cleanup_tmp(local_path)
    return {"version": "unknown", "source": "not_available"}


def _blast_version_from_result(job_info: dict[str, Any]) -> dict[str, str]:
    results_url = str(job_info.get("results", "")).rstrip("/")
    if not results_url:
        return {"version": "unknown", "detail": "", "source": "not_available"}
    files = _list_result_files(job_info)
    if not files:
        return {"version": "unknown", "detail": "", "source": "not_available"}
    first = files[0]
    filename = _safe_result_filename(str(first.get("filename", "")))
    blob_path = _safe_result_blob_path(str(first.get("blob_path", "")), filename)
    # Per-call unique tmp file to avoid concurrent-poll races where two
    # threads download into the same path.
    local_path = f"/tmp/{job_info.get('job_id', 'job')}-{uuid.uuid4().hex}-{filename}"
    try:
        _azcopy_login()
        _cleanup_tmp(local_path)
        safe_exec(["azcopy", "cp", f"{results_url}/{blob_path}", local_path], timeout=120)
        opener = gzip.open if filename.endswith(".gz") else open
        with opener(local_path, "rt", encoding="utf-8", errors="ignore") as handle:
            sample = handle.read(4096)
        match = re.search(r"<BlastOutput_version>([^<]+)</BlastOutput_version>", sample)
        if not match:
            return {"version": "unknown", "detail": "", "source": "not_available"}
        detail = match.group(1).strip()
        version = re.sub(r"^BLAST[A-Z]*\s+", "", detail).strip() or detail
        return {"version": version, "detail": detail, "source": "result_xml"}
    except Exception as exc:
        logger.debug("BLAST version result metadata unavailable for %s: %s", job_info.get("job_id"), str(exc)[:200])
        return {"version": "unknown", "detail": "", "source": "not_available"}
    finally:
        _cleanup_tmp(local_path)


def _ensure_job_defaults(job_id: str, data: dict[str, Any]) -> dict[str, Any]:
    now = _now_iso()
    data.setdefault("job_id", job_id)
    data.setdefault("status", "queued")
    data.setdefault("priority", _PRIORITY_LABELS["normal"])
    data["priority"] = _normalise_priority(data.get("priority"))
    data.setdefault("created_at", now)
    data.setdefault("queued_at", data.get("created_at", now))
    data.setdefault("updated_at", data.get("created_at", now))
    data.setdefault("last_progress_at", data.get("updated_at", now))
    data.setdefault("attempt", 0)
    return data

def _save_job_cm(job_id: str, data: dict[str, Any]) -> bool:
    """Create or update a ConfigMap for a job."""
    cm_name = _cm_name(job_id)
    data = _ensure_job_defaults(job_id, dict(data))
    payload = json.dumps(data, default=str)
    try:
        labels = {
            "elb-job": "true",
            "job-id": job_id,
            "status": _safe_label_value(data.get("status")),
            "priority": str(_normalise_priority(data.get("priority"))),
        }
        manifest = {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": cm_name, "labels": labels},
            "data": {"job": payload},
        }
        proc_input = json.dumps(manifest)
        # subprocess is hoisted to module-level imports; calling kubectl
        # apply with stdin avoids leaving the manifest on disk and the
        # earlier dry-run-validate pass was unused output.
        subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=proc_input, capture_output=True, text=True, timeout=10, check=True,
        )
        return True
    except Exception as exc:
        logger.warning("Failed to save ConfigMap for %s: %s", job_id, str(exc)[:200])
        return False

def _load_all_jobs_cm() -> dict[str, dict[str, Any]]:
    """Load all job ConfigMaps from K8s."""
    jobs: dict[str, dict[str, Any]] = {}
    try:
        proc = safe_exec(["kubectl", "get", "configmap", "-l", _CM_LABEL, "-o", "json"], timeout=10)
        data = json.loads(proc.stdout)
        for item in data.get("items", []):
            job_data = item.get("data", {}).get("job", "{}")
            parsed = json.loads(job_data)
            jid = parsed.get("job_id", item["metadata"]["name"].replace(_CM_PREFIX, ""))
            jobs[jid] = _ensure_job_defaults(jid, parsed)
    except Exception as exc:
        logger.warning("Failed to load job ConfigMaps: %s", str(exc)[:200])
    return jobs

def _load_job_cm(job_id: str) -> dict[str, Any] | None:
    """Load a single job ConfigMap."""
    try:
        proc = safe_exec(["kubectl", "get", "configmap", _cm_name(job_id), "-o", "json"], timeout=5)
        data = json.loads(proc.stdout)
        return _ensure_job_defaults(job_id, json.loads(data.get("data", {}).get("job", "{}")))
    except Exception:
        return None

def _delete_job_cm(job_id: str) -> None:
    try:
        safe_exec(["kubectl", "delete", "configmap", _cm_name(job_id)], timeout=10)
    except Exception as exc:
        logger.warning("Failed to delete ConfigMap %s: %s", job_id, str(exc)[:200])

# ── In-memory cache (hot path, synced with ConfigMap) ──────────────────────
_jobs: dict[str, dict[str, Any]] = {}
_jobs_lock = Lock()
_cm_loaded = False
_job_threads: dict[str, Thread] = {}
_job_cancel_events: dict[str, Event] = {}
_threads_lock = Lock()
_background_started = False
_background_stop = Event()

def _ensure_loaded() -> None:
    """Lazy-load from ConfigMaps on first access."""
    global _cm_loaded
    if _cm_loaded:
        return
    loaded = _load_all_jobs_cm()
    with _jobs_lock:
        if _cm_loaded:
            return
        _jobs.update(loaded)
        _cm_loaded = True
        logger.info("Loaded %d jobs from ConfigMaps", len(loaded))
    _reconcile_recovered_jobs()


def _has_alive_thread(job_id: str) -> bool:
    with _threads_lock:
        thread = _job_threads.get(job_id)
    return bool(thread and thread.is_alive())


def _reconcile_recovered_jobs() -> None:
    with _jobs_lock:
        recovered = [
            dict(job) for job in _jobs.values()
            if job.get("status") in {"dispatching", "submitting"} and not _has_alive_thread(job.get("job_id", ""))
        ]
    for job in recovered:
        job_id = job["job_id"]
        refreshed = _refresh_job_status(job_id) or job
        summary = refreshed.get("k8s_summary") or {}
        if refreshed.get("status") in {"dispatching", "submitting"} and not summary.get("total") and not summary.get("submit_failed"):
            _update_job(
                job_id,
                status="queued",
                phase="recovered",
                queued_at=_now_iso(),
                last_progress_at=_now_iso(),
                error="",
            )


def _save_job(job_id: str, data: dict[str, Any], *, require_persist: bool = False) -> dict[str, Any]:
    data = _ensure_job_defaults(job_id, dict(data))
    data["updated_at"] = _now_iso()
    with _jobs_lock:
        _jobs[job_id] = data
    persisted = _save_job_cm(job_id, data)
    if require_persist and not persisted:
        with _jobs_lock:
            _jobs.pop(job_id, None)
        raise HTTPException(503, "failed to persist job queue state")
    return data


def _update_job(job_id: str, **updates: Any) -> dict[str, Any] | None:
    with _jobs_lock:
        current = _jobs.get(job_id)
        if not current:
            return None
        data = dict(current)
        data.update(updates)
        data["updated_at"] = _now_iso()
        _jobs[job_id] = data
    _save_job_cm(job_id, data)
    return data


def _active_job_count_unlocked() -> int:
    return sum(1 for job in _jobs.values() if job.get("status") in _ACTIVE_STATES)


def _queued_position(job_id: str) -> int | None:
    with _jobs_lock:
        queued = _sorted_queued_jobs_unlocked()
    for idx, job in enumerate(queued, start=1):
        if job.get("job_id") == job_id:
            return idx
    return None


def _sorted_queued_jobs_unlocked() -> list[dict[str, Any]]:
    queued = [job for job in _jobs.values() if job.get("status") in _QUEUED_STATES]
    return sorted(
        queued,
        key=lambda job: (-_normalise_priority(job.get("priority")), str(job.get("created_at", ""))),
    )


def _claim_next_job() -> dict[str, Any] | None:
    with _jobs_lock:
        if _active_job_count_unlocked() >= MAX_ACTIVE_SUBMISSIONS:
            return None
        queued = _sorted_queued_jobs_unlocked()
        if not queued:
            return None
        job = dict(queued[0])
        job["status"] = "dispatching"
        job["started_at"] = job.get("started_at") or _now_iso()
        job["last_progress_at"] = _now_iso()
        job["updated_at"] = _now_iso()
        _jobs[job["job_id"]] = job
    _save_job_cm(job["job_id"], job)
    return job


def _write_config_file(job_id: str, config_text: str) -> str:
    job_dir = f"/tmp/elb-jobs/{job_id}"
    os.makedirs(job_dir, exist_ok=True)
    cfg_path = os.path.join(job_dir, "config.ini")
    with open(cfg_path, "w", encoding="utf-8") as f:
        f.write(config_text)
    return cfg_path


def _start_job_thread(job: dict[str, Any]) -> None:
    job_id = job["job_id"]
    with _threads_lock:
        existing = _job_threads.get(job_id)
        if existing and existing.is_alive():
            return
        cancel_event = _job_cancel_events.setdefault(job_id, Event())
        cancel_event.clear()
        thread = Thread(target=_run_submit_bg, args=(job_id,), daemon=True)
        _job_threads[job_id] = thread
        thread.start()


def _dispatcher_once() -> bool:
    _ensure_loaded()
    claimed = _claim_next_job()
    if not claimed:
        return False
    _start_job_thread(claimed)
    return True


def _dispatcher_loop() -> None:
    while not _background_stop.is_set():
        try:
            while _dispatcher_once():
                pass
        except Exception as exc:
            logger.warning("dispatcher loop failed: %s", str(exc)[:300])
        _background_stop.wait(DISPATCH_INTERVAL_SECONDS)


def _watchdog_loop() -> None:
    while not _background_stop.is_set():
        try:
            _watchdog_once()
        except Exception as exc:
            logger.warning("watchdog loop failed: %s", str(exc)[:300])
        _background_stop.wait(WATCHDOG_INTERVAL_SECONDS)


def _start_background_threads() -> None:
    global _background_started
    if BACKGROUND_DISABLED or _background_started:
        return
    _background_started = True
    Thread(target=_dispatcher_loop, name="elb-openapi-dispatcher", daemon=True).start()
    Thread(target=_watchdog_loop, name="elb-openapi-watchdog", daemon=True).start()


@app.on_event("startup")
def _on_startup() -> None:
    _start_background_threads()

# ── Webhook (optional) ────────────────────────────────────────────────────

def _webhook_notify(job_id: str, data: dict[str, Any]) -> None:
    """Webhook to Control Plane with exponential backoff (3 attempts). Failure is non-fatal.

    Security:
    * The destination URL is fixed to ``CONTROL_PLANE_URL`` from the
      operator-controlled environment. We additionally **enforce HTTPS**
      (HTTP is only permitted for ``localhost`` / ``127.0.0.1`` to keep
      local-dev ergonomic) so that intermediate networks cannot tamper
      with or read the event stream.
    * If ``ELB_OPENAPI_INTERNAL_TOKEN`` is configured it is sent as
      ``Authorization: Bearer <token>`` so the Control Plane can
      authenticate inbound webhooks (defence in depth — Control Plane
      should still verify on its side).
    """
    if not CONTROL_PLANE_URL:
        return
    import time
    from urllib.parse import urlparse
    parsed = urlparse(CONTROL_PLANE_URL)
    is_local = parsed.hostname in ("localhost", "127.0.0.1", "::1")
    if parsed.scheme != "https" and not is_local:
        # Refuse to send job state to a non-TLS remote endpoint to avoid
        # credential / job-id disclosure on the wire. This is a hard
        # configuration error; surface it loudly but stay non-fatal.
        logger.error(
            "Webhook suppressed for job %s: CONTROL_PLANE_URL must use https "
            "(got scheme=%r).", job_id, parsed.scheme,
        )
        return
    body = json.dumps({"job_id": job_id, **data}).encode()
    headers = {"Content-Type": "application/json"}
    if INTERNAL_TOKEN:
        headers["Authorization"] = f"Bearer {INTERNAL_TOKEN}"
    for attempt in range(3):
        try:
            import urllib.request
            req = urllib.request.Request(
                f"{CONTROL_PLANE_URL}/api/blast/register-external-job",
                data=body, headers=headers,
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
            logger.info("Webhook sent for job %s (attempt %d)", job_id, attempt + 1)
            return
        except Exception as exc:
            if attempt < 2:
                time.sleep(2 ** attempt)  # 1s, 2s backoff
            else:
                logger.debug("Webhook failed for %s after 3 attempts (non-fatal): %s", job_id, str(exc)[:100])

# ── Helpers ────────────────────────────────────────────────────────────────

def _azcopy_login() -> None:
    login_type = os.environ.get("AZCOPY_AUTO_LOGIN_TYPE", "").strip().upper()
    if login_type == "WORKLOAD":
        has_workload_identity = all(
            os.environ.get(name)
            for name in ("AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE")
        )
        if has_workload_identity:
            return
        logger.warning(
            "AZCOPY_AUTO_LOGIN_TYPE=WORKLOAD is set but Workload Identity env vars "
            "are incomplete; falling back to azcopy MSI login"
        )
        os.environ.pop("AZCOPY_AUTO_LOGIN_TYPE", None)
        os.environ.pop("AZCOPY_TENANT_ID", None)
    elif login_type:
        return
    try:
        safe_exec(["azcopy", "login", "--identity"], timeout=30)
    except Exception as exc:
        raise RuntimeError(f"azcopy auth failed: {str(exc)[:200]}") from exc

def _validate_blob_url(url: str, field: str) -> None:
    if not _BLOB_URL_RE.match(url):
        raise ValueError(f"{field} must be a valid Azure Blob URL")
    parsed = urlparse(url)
    if parsed.query or parsed.fragment:
        raise ValueError(f"{field} must not include query strings, fragments, or SAS tokens")
    # Defence-in-depth: reject path traversal segments even though Azure
    # would refuse the request anyway. Blocks `https://x.blob.core.windows.net/c/../../etc/passwd`
    # before the credential is ever attached, and prevents log entries
    # for those URLs from misleading operators.
    path_parts = (parsed.path or "").split("/")
    if any(p in {"", ".", ".."} for p in path_parts[3:]):
        # path_parts[0]='' (leading /), [1]=container, [2:]=blob name segments
        raise ValueError(f"{field} contains path traversal or empty segments")


def _validate_short_blob_name(value: str, field: str) -> None:
    if not value or value.startswith("/") or ".." in value or "?" in value or "#" in value:
        raise ValueError(f"{field} must be a safe blob path without query strings or traversal")

def _sanitize_job_id(job_id: str) -> str:
    cleaned = re.sub(r"[^a-f0-9]", "", job_id.lower())
    if len(cleaned) < 6 or len(cleaned) > 12:
        raise HTTPException(status_code=400, detail="Invalid job_id format")
    return cleaned

def _blob_base() -> str:
    return f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"

_CONFIG_SECRET_PARTS = (
    "key", "secret", "token", "password", "passwd", "pwd",
    "credential", "sas", "signature", "sig",
    "connection_string", "connectionstring",
)


def _redact_config(cfg: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    """Mask any value whose key looks like a secret, in case the deployment
    accidentally ships a stale ``elb-cfg.ini`` containing a storage account
    key, SAS token, or other credential. The endpoint is auth-gated, but
    operators may still legitimately copy the response into chat or tickets,
    so leaking secrets through this surface is unacceptable.
    """
    redacted: dict[str, dict[str, str]] = {}
    for section, items in cfg.items():
        out: dict[str, str] = {}
        for k, v in items.items():
            lower_k = k.lower()
            if any(part in lower_k for part in _CONFIG_SECRET_PARTS):
                out[k] = "***REDACTED***"
            else:
                out[k] = v
        redacted[section] = out
    return redacted


def _resolve_config() -> dict[str, dict[str, str]]:
    config = configparser.ConfigParser()
    ini = os.path.join(os.path.dirname(__file__), "elb-cfg.ini")
    if os.path.isfile(ini):
        config.read(ini, encoding="utf-8")
    if not config.has_section("cloud-provider"): config.add_section("cloud-provider")
    config["cloud-provider"]["azure-region"] = AZURE_REGION
    config["cloud-provider"]["azure-resource-group"] = RESOURCE_GROUP
    config["cloud-provider"]["azure-storage-account"] = STORAGE_ACCOUNT
    config["cloud-provider"]["azure-acr-name"] = ACR_NAME
    config["cloud-provider"]["azure-acr-resource-group"] = ACR_RESOURCE_GROUP
    if not config.has_section("cluster"): config.add_section("cluster")
    config["cluster"]["name"] = CLUSTER_NAME
    config["cluster"]["machine-type"] = MACHINE_TYPE
    config["cluster"]["num-nodes"] = str(NUM_NODES)
    if not config.has_section("blast"): config.add_section("blast")
    return {s: dict(config[s]) for s in config.sections()}

def _cleanup_tmp(*paths: str) -> None:
    for p in paths:
        try:
            if os.path.isdir(p): shutil.rmtree(p, ignore_errors=True)
            elif os.path.isfile(p): os.remove(p)
        except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════════════════

# Versioned router — all business endpoints live under /v1.
# Authentication is enforced at the router level so every current and future
# endpoint inherits the same access policy. Truly public probes (e.g.
# ``/healthz`` on the root app) bypass this dependency by design.
v1 = APIRouter(prefix="/v1", dependencies=[Depends(require_api_token)])

# ── Root redirect ──────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def root_redirect():
    """Redirect root to Swagger UI."""
    from starlette.responses import RedirectResponse
    return RedirectResponse(url="/docs")


# ── System ─────────────────────────────────────────────────────────────────

@app.get("/healthz", tags=["System"], summary="Liveness probe")
async def healthz():
    """Lightweight liveness probe for Kubernetes. Always returns 200 if the process is alive."""
    return {"status": "ok"}

@v1.get("/health", tags=["System"], summary="Detailed health check")
async def health():
    """Check Kubernetes connectivity, Azure auth, and show active configuration."""
    checks: dict[str, Any] = {}
    try:
        proc = safe_exec("kubectl get nodes --no-headers", timeout=10)
        checks["kubernetes"] = {"status": "ok", "nodes": len([l for l in proc.stdout.strip().split("\n") if l])}
    except Exception as e:
        checks["kubernetes"] = {"status": "error", "message": str(e)[:200]}
    try:
        from azure.identity import DefaultAzureCredential
        dac = DefaultAzureCredential()
        token = dac.get_token("https://management.azure.com/.default")
        checks["azure_auth"] = {"status": "ok", "method": "DefaultAzureCredential"}
    except ImportError:
        # Fallback if azure-identity not installed
        try:
            safe_exec("az account show", timeout=10)
            checks["azure_auth"] = {"status": "ok", "method": "az-cli"}
        except Exception as e:
            checks["azure_auth"] = {"status": "error", "message": str(e)[:200]}
    except Exception as e:
        checks["azure_auth"] = {"status": "error", "message": str(e)[:200]}
    return {
        "status": "healthy" if all(c["status"] == "ok" for c in checks.values()) else "degraded",
        "version": VERSION,
        "config": {"cluster": CLUSTER_NAME, "storage_account": STORAGE_ACCOUNT, "resource_group": RESOURCE_GROUP, "region": AZURE_REGION},
        "checks": checks,
    }

@v1.get("/config", tags=["System"], summary="Active configuration")
async def get_config():
    """Get the effective ElasticBLAST configuration. Environment variables override INI defaults.

    Secret-looking values (keys, tokens, SAS, connection strings) are
    redacted so accidental leaks via this endpoint are impossible.
    """
    return JSONResponse(content=_redact_config(_resolve_config()))

# ── Cluster ────────────────────────────────────────────────────────────────

@v1.get("/cluster", tags=["Cluster"], summary="Cluster overview")
async def get_cluster():
    """Get AKS cluster summary: nodes with instance types, pods with phases, and status counts."""
    result: dict[str, Any] = {"cluster_name": CLUSTER_NAME}
    try:
        proc = safe_exec("kubectl get nodes -o json", timeout=15)
        data = json.loads(proc.stdout)
        result["nodes"] = [{
            "name": i["metadata"]["name"],
            "status": next((c["type"] for c in i.get("status",{}).get("conditions",[]) if c.get("status")=="True" and c["type"]=="Ready"), "NotReady"),
            "instance_type": i["metadata"].get("labels",{}).get("node.kubernetes.io/instance-type","unknown"),
        } for i in data.get("items",[])]
    except Exception as e:
        result["nodes_error"] = str(e)[:200]
    try:
        proc = safe_exec("kubectl get pods -o json", timeout=15)
        data = json.loads(proc.stdout)
        pods = [{"name": i["metadata"]["name"], "phase": i["status"].get("phase","Unknown"), "node": i["spec"].get("nodeName","")} for i in data.get("items",[])]
        summary: dict[str,int] = {}
        for p in pods: summary[p["phase"]] = summary.get(p["phase"],0)+1
        result["pods"] = pods
        result["pod_summary"] = summary
    except Exception as e:
        result["pods_error"] = str(e)[:200]
    return result

# ── Jobs Models ────────────────────────────────────────────────────────────

class BlastOptions(BaseModel):
    """BLAST search parameters."""
    evalue: Optional[float] = Field(None, description="E-value threshold. Server default if omitted.", examples=[0.05])
    max_target_seqs: Optional[int] = Field(None, description="Maximum number of hits to return.", examples=[100])
    outfmt: Optional[str] = Field(None, description="Output format string (default: '7').", examples=["7"])
    extra: Optional[str] = Field(None, description="Additional BLAST CLI options as raw string.")


class ExternalBlastOptions(BaseModel):
    """External API BLAST options.

    The external result pipeline requires BLAST XML (`outfmt=5`) because
    `Hsp_hseq` is needed for FASTA generation.
    """

    outfmt: int = Field(5, description="Fixed to BLAST XML format 5")
    word_size: int = Field(28, ge=1)
    dust: bool = Field(True)
    evalue: float = Field(10.0, gt=0)
    max_target_seqs: int = Field(500, ge=1)


class ExternalSubmitRequest(BaseModel):
    query_fasta: str = Field(..., min_length=1)
    db: str = Field(..., min_length=1)
    program: str = Field("blastn")
    taxid: Optional[int] = Field(None)
    is_inclusive: Optional[bool] = Field(None)
    options: ExternalBlastOptions = Field(default_factory=ExternalBlastOptions)
    priority: int | str = Field(50)
    batch_len: Optional[int] = Field(None, ge=1, le=1_000_000_000)
    idempotency_key: Optional[str] = Field(None, min_length=1, max_length=256)
    resource_profile: str = Field("standard")
    submission_source: str = Field(_DEFAULT_EXTERNAL_SOURCE)
    external_correlation_id: Optional[str] = Field(None, max_length=128)

_MODE_A_EXAMPLE = {
    "summary": "Mode A — Blob URL (advanced)",
    "description": "Provide full Azure Blob Storage URLs for database, queries, and results.",
    "value": {
        "program": "blastn",
        "db": "https://stgelb0509.blob.core.windows.net/blast-db/16S_ribosomal_RNA",
        "queries": "https://stgelb0509.blob.core.windows.net/queries/sample.fa",
        "results": "https://stgelb0509.blob.core.windows.net/results/run-001",
        "options": "-evalue 0.01 -outfmt 7",
    },
}

_MODE_B_EXAMPLE = {
    "summary": "Mode B — Inline FASTA (simple)",
    "description": "Provide FASTA text inline. Server uploads query and resolves DB/results URLs automatically.",
    "value": {
        "program": "blastn",
        "db": "16S_ribosomal_RNA",
        "query_fasta": ">NC_003310.1\nATGCATGCATGCATGCATGCATGCATGCATGC\nGCATGCATGCATGCATGCATGCATGCATGCAT",
        "blast_options": {"evalue": 0.05, "max_target_seqs": 100},
    },
}

_MODE_B_TAXID_EXAMPLE = {
    "summary": "Mode B — with Taxonomy filter",
    "description": "Filter BLAST results by organism taxonomy. is_inclusive=true searches within the taxid, false excludes it.",
    "value": {
        "program": "blastn",
        "db": "16S_ribosomal_RNA",
        "query_fasta": ">NC_003310.1\nATGCATGCATGCATGCATGCATGCATGCATGC",
        "taxid": 10244,
        "is_inclusive": True,
        "blast_options": {"evalue": 0.05, "max_target_seqs": 100},
    },
}

class JobSubmitRequest(BaseModel):
    """Unified BLAST job submission.

    **Mode A** (Blob URL): provide `db`, `queries`, `results` as full Azure Blob URLs.

    **Mode B** (Inline FASTA): provide `query_fasta` + short `db` name.
    Server auto-uploads query to blob and resolves all URLs.
    Optionally filter by `taxid` + `is_inclusive`.
    """
    model_config = {"json_schema_extra": {"examples": [_MODE_A_EXAMPLE["value"], _MODE_B_EXAMPLE["value"], _MODE_B_TAXID_EXAMPLE["value"]]}}

    program: str = Field("blastn", description="BLAST program (blastn, blastp, blastx, tblastn, etc.)", examples=["blastn", "blastp", "blastx"])
    db: str = Field(..., description="Full Blob URL (mode A) or short DB name like '16S_ribosomal_RNA' (mode B)", examples=["16S_ribosomal_RNA"])
    cluster_name: Optional[str] = Field(None, description="AKS cluster name. Uses server default if omitted.")
    # Mode A
    queries: Optional[str] = Field(None, description="Query sequences Blob URL (mode A only)")
    results: Optional[str] = Field(None, description="Results destination Blob URL (mode A only)")
    options: Optional[str] = Field(None, description="Raw BLAST CLI options string (mode A only)")
    # Mode B
    query_fasta: Optional[str] = Field(None, description="Inline FASTA text (mode B). Server auto-uploads to blob storage.")
    taxid: Optional[int] = Field(None, description="NCBI Taxonomy ID for organism filtering", examples=[10244, 9606])
    is_inclusive: Optional[bool] = Field(None, description="true: search within taxid only. false: exclude taxid. Ignored without taxid.")
    blast_options: Optional[BlastOptions] = Field(None, description="Structured BLAST options (mode B). Easier than raw option string.")
    priority: int | str = Field(50, description="Queue priority. Accepts 0-100 or low, normal, high, urgent. Running jobs are not preempted.")
    batch_len: Optional[int] = Field(None, ge=1, le=1_000_000_000, description="Optional ElasticBLAST blast.batch-len override for query batching.")
    idempotency_key: Optional[str] = Field(None, min_length=1, max_length=256, description="Stable caller key. Replays return the same job handle instead of creating duplicate work.")
    resource_profile: str = Field("standard", description="Server-side sizing policy label, e.g. standard or core_nt_safe.")
    submission_source: str = Field(_DEFAULT_EXTERNAL_SOURCE, description="Effective source of the submission: dashboard, external_api, terminal, or system.")
    external_correlation_id: Optional[str] = Field(None, max_length=128, description="Caller-side correlation id, e.g. dashboard job id.")

# ── Jobs — Submit ──────────────────────────────────────────────────────────

def _build_options(opts: BlastOptions | None, taxid: int | None, inclusive: bool | None) -> str:
    parts: list[str] = []
    if opts:
        if opts.evalue is not None: parts.append(f"-evalue {opts.evalue}")
        if opts.max_target_seqs is not None: parts.append(f"-max_target_seqs {opts.max_target_seqs}")
        if opts.outfmt is not None: parts.append(f"-outfmt {opts.outfmt}")
        if opts.extra: parts.append(opts.extra)
    if not parts:
        parts.append("-evalue 0.01 -outfmt 7")
    if taxid is not None:
        option = "-taxids" if inclusive is not False else "-negative_taxids"
        parts.append(f"{option} {taxid}")
    return " ".join(parts)


def _build_external_options(opts: ExternalBlastOptions, taxid: int | None, inclusive: bool | None) -> str:
    if opts.outfmt != 5:
        raise HTTPException(400, "options.outfmt is fixed to 5 because the result pipeline requires BLAST XML")
    parts = [
        "-outfmt 5",
        f"-word_size {opts.word_size}",
        f"-evalue {opts.evalue}",
        f"-max_target_seqs {opts.max_target_seqs}",
        "-dust yes" if opts.dust else "-dust no",
    ]
    if taxid is not None:
        option = "-taxids" if inclusive is not False else "-negative_taxids"
        parts.append(f"{option} {taxid}")
    return " ".join(parts)

def _upload_fasta(job_id: str, fasta: str) -> str:
    # job_id is reused across idempotent replays, so two concurrent submits
    # for the same idempotency_key would race on the same /tmp path. Append
    # a per-call uuid so each upload has its own staging file.
    local = f"/tmp/query-{job_id}-{uuid.uuid4().hex}.fa"
    url = f"{_blob_base()}/queries/{job_id}.fa"
    with open(local, "w") as f: f.write(fasta)
    try:
        _azcopy_login()
        safe_exec(["azcopy", "cp", local, url], timeout=60)
    finally:
        _cleanup_tmp(local)
    return url

def _last_json(stdout: str) -> dict[str, Any] | None:
    for line in reversed(stdout.splitlines()):
        candidate = line.strip()
        if not candidate.startswith("{") or not candidate.endswith("}"):
            continue
        try:
            decoded = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, dict):
            return decoded
    return None


def _ensure_elb_scripts_configmap() -> None:
    required_scripts = {
        "blast-run-aks.sh",
        "elb-finalizer-aks.sh",
        "init-db-download-aks.sh",
        "query-download-ssd-aks.sh",
        "results-export-aks.sh",
    }
    try:
        existing = safe_exec(["kubectl", "get", "configmap", "elb-scripts", "-o", "json"], timeout=10)
        data = json.loads(existing.stdout or "{}").get("data", {})
        if required_scripts.issubset(set(data)):
            return
    except Exception:
        pass
    scripts_dir = files("elastic_blast").joinpath("templates/scripts")
    scripts_path = Path(str(scripts_dir))
    if not all((scripts_path / name).is_file() for name in required_scripts):
        missing = sorted(name for name in required_scripts if not (scripts_path / name).is_file())
        raise RuntimeError(f"Installed ElasticBLAST scripts are incomplete: {missing}")
    # Build the ConfigMap manifest in-process and apply via stdin so we
    # don't fork a shell. The previous ``sh -lc 'kubectl ... | kubectl ...'``
    # pattern was vulnerable to a path-with-spaces in ``scripts_dir`` and
    # added a shell parsing layer to the trusted-input chain.
    dry_run = subprocess.run(
        ["kubectl", "create", "configmap", "elb-scripts",
         f"--from-file={scripts_path}",
         "--dry-run=client", "-o", "yaml"],
        capture_output=True, text=True, timeout=30, check=True,
    )
    subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=dry_run.stdout, capture_output=True, text=True, timeout=60, check=True,
    )


def _run_submit_bg(job_id: str) -> None:
    with _jobs_lock:
        job = dict(_jobs.get(job_id, {}))
    if not job:
        return
    config_text = job.get("config_ini", "")
    if not config_text:
        _update_job(job_id, status="failed", error="missing persisted config_ini")
        return

    cfg_path = _write_config_file(job_id, config_text)
    cancel_event = _job_cancel_events.setdefault(job_id, Event())
    _update_job(
        job_id,
        status="submitting",
        cfg_path=cfg_path,
        attempt=int(job.get("attempt", 0)) + 1,
        last_progress_at=_now_iso(),
    )
    try:
        _ensure_az_login()
        _ensure_elb_scripts_configmap()
        result = run_cancellable(
            ["elastic-blast", "submit", "--cfg", cfg_path],
            timeout=None,
            stop_event=cancel_event,
        )
        payload = _last_json(result.stdout or "") or {}
        details = payload.get("details") if isinstance(payload.get("details"), dict) else {}
        if payload.get("decision") == "already_done" and details.get("terminal") == "SUCCESS":
            status = "completed"
        elif payload.get("decision") == "already_done" and details.get("terminal") == "FAILURE":
            status = "failed"
        else:
            status = "running"
        _update_job(
            job_id,
            status=status,
            phase="submitted" if status == "running" else status,
            elb_job_id=payload.get("correlation_id") or job_id,
            submit_result=payload,
            stdout_tail=(result.stdout or "")[-2000:],
            stderr_tail=(result.stderr or "")[-2000:],
            last_progress_at=_now_iso(),
        )
        _webhook_notify(job_id, {"event": "submitted", "status": status})
        logger.info("Job %s submitted", job_id)
    except Exception as e:
        if cancel_event.is_set():
            _update_job(job_id, status="cancelled", phase="cancelled", error=str(e)[:500])
            _webhook_notify(job_id, {"event": "cancelled", "error": str(e)[:200]})
            logger.warning("Job %s cancelled: %s", job_id, str(e)[:200])
        else:
            _update_job(job_id, status="failed", phase="submit_failed", error=str(e)[:500])
            _webhook_notify(job_id, {"event": "failed", "error": str(e)[:200]})
            logger.error("Job %s failed: %s", job_id, str(e)[:200])
    finally:
        _dispatcher_once()


def _job_marker_phase(results_url: str) -> str | None:
    if not results_url:
        return None
    try:
        _azcopy_login()
        proc = safe_exec(["azcopy", "ls", f"{results_url}/metadata/"], timeout=10)
    except Exception:
        return None
    if "SUCCESS.txt" in proc.stdout:
        return "completed"
    if "FAILURE.txt" in proc.stdout:
        return "failed"
    return None


def _k8s_job_summary(elb_job_id: str) -> dict[str, Any]:
    empty = {"total": 0, "succeeded": 0, "failed": 0, "active": 0, "submit_failed": 0, "finalizer_active": 0}
    try:
        proc = safe_exec(["kubectl", "get", "jobs", "-l", f"elb-job-id={elb_job_id}", "-o", "json"], timeout=15)
        data = json.loads(proc.stdout)
    except Exception as exc:
        return {**empty, "error": str(exc)[:200]}

    items = data.get("items", [])
    if not items:
        try:
            proc = safe_exec(["kubectl", "get", "jobs", "-o", "json"], timeout=15)
            fallback = json.loads(proc.stdout)
            items = [
                item
                for item in fallback.get("items", [])
                if item.get("metadata", {}).get("labels", {}).get("app") in {"blast", "submit", "finalizer"}
            ]
        except Exception:
            items = []

    summary = dict(empty)
    for item in items:
        labels = item.get("metadata", {}).get("labels", {})
        app_label = labels.get("app", "")
        status = item.get("status", {})
        if app_label == "blast":
            summary["total"] += 1
            summary["succeeded"] += status.get("succeeded", 0) or 0
            summary["failed"] += status.get("failed", 0) or 0
            summary["active"] += status.get("active", 0) or 0
        elif app_label == "submit":
            summary["submit_failed"] += status.get("failed", 0) or 0
        elif app_label == "finalizer":
            summary["finalizer_active"] += status.get("active", 0) or 0
    return summary


def _k8s_pod_stuck_reason(elb_job_id: str) -> str | None:
    try:
        proc = safe_exec(["kubectl", "get", "pods", "-l", f"elb-job-id={elb_job_id}", "-o", "json"], timeout=15)
        data = json.loads(proc.stdout)
    except Exception:
        return None

    items = data.get("items", [])
    if not items:
        try:
            proc = safe_exec(["kubectl", "get", "pods", "-o", "json"], timeout=15)
            fallback = json.loads(proc.stdout)
            items = [
                item
                for item in fallback.get("items", [])
                if item.get("metadata", {}).get("labels", {}).get("app") in {"blast", "submit", "finalizer"}
            ]
        except Exception:
            items = []

    now = time.time()
    for item in items:
        meta = item.get("metadata", {})
        status = item.get("status", {})
        pod_name = meta.get("name", "unknown")
        phase = status.get("phase", "")
        if phase == "Failed":
            return f"pod {pod_name} failed: {status.get('reason', 'Failed')}"
        if phase == "Pending":
            created = _parse_ts(meta.get("creationTimestamp"))
            if created and now - created > PENDING_STUCK_SECONDS:
                reasons = []
                for condition in status.get("conditions", []):
                    if condition.get("type") == "PodScheduled" and condition.get("status") == "False":
                        reasons.append(condition.get("reason", "unscheduled"))
                        reasons.append(condition.get("message", "")[:200])
                return f"pod {pod_name} pending too long: {' '.join(r for r in reasons if r)}"
        for container in status.get("containerStatuses", []):
            state = container.get("state", {})
            waiting = state.get("waiting", {})
            terminated = state.get("terminated", {})
            reason = waiting.get("reason") or terminated.get("reason")
            if reason in {"OOMKilled", "Evicted", "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"}:
                return f"pod {pod_name} container {container.get('name')} is {reason}"
    return None


def _refresh_job_status(job_id: str) -> dict[str, Any] | None:
    with _jobs_lock:
        job = dict(_jobs.get(job_id, {}))
    if not job:
        return None
    if job.get("status") in _TERMINAL_STATES or job.get("status") == "queued":
        return job

    marker = _job_marker_phase(job.get("results", ""))
    if marker:
        return _update_job(job_id, status=marker, phase=marker, last_progress_at=_now_iso())

    elb_job_id = job.get("elb_job_id") or job_id
    summary = _k8s_job_summary(elb_job_id)
    stuck_reason = _k8s_pod_stuck_reason(elb_job_id)
    previous_summary = job.get("k8s_summary")
    updates: dict[str, Any] = {"k8s_summary": summary}
    if summary != previous_summary:
        updates["last_progress_at"] = _now_iso()

    if stuck_reason:
        updates.update({"status": "failed", "phase": "stuck_cancelled", "error": stuck_reason})
        refreshed = _update_job(job_id, **updates)
        _cancel_job(job_id, stuck_reason, terminal_status="failed")
        return refreshed
    if summary.get("submit_failed"):
        updates.update({"status": "failed", "phase": "submit_failed", "error": "submit job failed before creating BLAST jobs"})
    elif summary.get("failed"):
        updates.update({"status": "failed", "phase": "blast_failed", "error": "one or more BLAST jobs failed"})
    elif summary.get("total", 0) > 0:
        if summary.get("succeeded", 0) >= summary.get("total", 0) and summary.get("total", 0) > 0:
            if _list_result_files(job):
                updates.update({"status": "completed", "phase": "completed", "completed_at": _now_iso()})
            else:
                updates.update({"status": "running", "phase": "finalizing"})
        elif summary.get("active", 0) > 0:
            updates.update({"status": "running", "phase": "running"})
        else:
            updates.update({"status": "running", "phase": "pending"})
    else:
        if _list_result_files(job):
            updates.update({"status": "completed", "phase": "completed", "completed_at": _now_iso()})
        else:
            updates.update({"phase": "submitting"})
    return _update_job(job_id, **updates)


def _cancel_job(job_id: str, reason: str, *, terminal_status: str = "cancelled") -> None:
    event = _job_cancel_events.setdefault(job_id, Event())
    event.set()
    with _jobs_lock:
        job = dict(_jobs.get(job_id, {}))
    cfg_path = job.get("cfg_path")
    should_delete_remote = job.get("status") in _ACTIVE_STATES or bool(cfg_path)
    if should_delete_remote and not cfg_path and job.get("config_ini"):
        cfg_path = _write_config_file(job_id, job["config_ini"])
    if should_delete_remote and cfg_path and os.path.isfile(cfg_path):
        try:
            safe_exec(["elastic-blast", "delete", "--cfg", cfg_path], timeout=300)
        except Exception as exc:
            logger.warning("delete after cancel failed for %s: %s", job_id, str(exc)[:300])
    _update_job(job_id, status=terminal_status, phase="cancelled" if terminal_status == "cancelled" else "stuck_cancelled", error=reason)
    _webhook_notify(job_id, {"event": terminal_status, "error": reason[:200]})


def _watchdog_once() -> None:
    _ensure_loaded()
    with _jobs_lock:
        candidates = [dict(job) for job in _jobs.values() if job.get("status") in _ACTIVE_STATES]
    for job in candidates:
        job_id = job["job_id"]
        refreshed = _refresh_job_status(job_id) or job
        status = refreshed.get("status")
        phase = refreshed.get("phase", "")
        if status in _TERMINAL_STATES:
            continue
        if phase == "submitting" and _age_seconds(refreshed.get("started_at")) > SUBMIT_STUCK_SECONDS:
            _cancel_job(job_id, "submit produced no BLAST jobs before stuck timeout", terminal_status="failed")
        elif phase == "finalizing" and _age_seconds(refreshed.get("last_progress_at")) > FINALIZER_STUCK_SECONDS:
            _cancel_job(job_id, "finalizer did not write a terminal marker before stuck timeout", terminal_status="failed")
        elif status == "running" and phase == "pending" and _age_seconds(refreshed.get("last_progress_at")) > RUNNING_IDLE_SECONDS:
            _cancel_job(job_id, "BLAST jobs remained pending without progress before stuck timeout", terminal_status="failed")


def _external_status(job_info: dict[str, Any]) -> str:
    status = str(job_info.get("status", "queued"))
    if status == "completed":
        return "success"
    if status in {"failed", "cancelled"}:
        return "failed"
    if status == "queued":
        return "queued"
    return "running"


def _progress_pct(job_info: dict[str, Any]) -> int:
    public_status = _external_status(job_info)
    if public_status == "success":
        return 100
    if public_status == "queued":
        return 0
    summary = job_info.get("k8s_summary") if isinstance(job_info.get("k8s_summary"), dict) else {}
    total = int(summary.get("total", 0) or 0)
    succeeded = int(summary.get("succeeded", 0) or 0)
    if total <= 0:
        return 5
    return max(1, min(99, int((succeeded / total) * 100)))


def _list_result_files(job_info: dict[str, Any]) -> list[dict[str, Any]]:
    existing = job_info.get("result_files")
    if isinstance(existing, list) and existing:
        return existing
    results_url = str(job_info.get("results", ""))
    if not results_url:
        return []
    try:
        _azcopy_login()
        proc = safe_exec(["azcopy", "ls", results_url, "--machine-readable"], timeout=60)
    except Exception:
        return []
    files: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line in (proc.stdout or "").splitlines():
        match = _RESULT_FILE_RE.search(line)
        if not match:
            continue
        blob_path = match.group("name")
        name = blob_path.split("/")[-1]
        if not name.startswith("batch_"):
            continue
        if name in seen:
            continue
        seen.add(name)
        size_match = re.search(r"(?:Content Length|contentLength|Size):\s*(\d+)", line, re.IGNORECASE)
        size_bytes = int(size_match.group(1)) if size_match else 0
        files.append({
            "file_id": f"result-{len(files) + 1:03d}",
            "filename": name,
            "blob_path": blob_path,
            "format": "blast_xml" if re.search(r"\.(?:xml|out)(?:\.gz)?$", name, re.IGNORECASE) else "blast_result",
            "size_bytes": size_bytes,
        })
    if files:
        _update_job(job_info["job_id"], result_files=files)
    return files


def _external_job_payload(job_info: dict[str, Any]) -> dict[str, Any]:
    public_status = _external_status(job_info)
    if public_status == "success":
        updates: dict[str, Any] = {}
        if str(job_info.get("blast_version", "unknown")) == "unknown":
            blast_version = _blast_version_from_result(job_info)
            if blast_version.get("version") != "unknown":
                updates["blast_version"] = blast_version["version"]
                updates["blast_version_detail"] = blast_version
        if str(job_info.get("db_version", "unknown")) == "unknown":
            db_version = _db_version_detail(job_info.get("db_name") or _db_name_from_value(str(job_info.get("db", ""))))
            if db_version.get("version") != "unknown":
                updates["db_version"] = db_version["version"]
                updates["db_version_detail"] = db_version
        if updates:
            job_info = _update_job(job_info["job_id"], **updates)
    payload: dict[str, Any] = {
        "job_id": job_info["job_id"],
        "status": public_status,
        "created_at": job_info.get("created_at", ""),
        "queued_at": job_info.get("queued_at", ""),
        "started_at": job_info.get("started_at", ""),
        "updated_at": job_info.get("updated_at", ""),
        "blast_version": job_info.get("blast_version", "unknown"),
        "blast_version_detail": job_info.get("blast_version_detail", {"version": job_info.get("blast_version", "unknown")}),
        "db_name": job_info.get("db_name") or _db_name_from_value(str(job_info.get("db", ""))),
        "db_version": job_info.get("db_version", "unknown"),
        "db_version_detail": job_info.get("db_version_detail", {"version": job_info.get("db_version", "unknown")}),
        "submission_source": job_info.get("submission_source", _DEFAULT_EXTERNAL_SOURCE),
        "external_correlation_id": job_info.get("external_correlation_id", ""),
    }
    summary = job_info.get("k8s_summary") if isinstance(job_info.get("k8s_summary"), dict) else {}
    if summary:
        payload["execution"] = {
            "shard_count": int(summary.get("total", 0) or 0),
            "shards_succeeded": int(summary.get("succeeded", 0) or 0),
            "shards_active": int(summary.get("active", 0) or 0),
            "shards_failed": int(summary.get("failed", 0) or 0),
        }
    terminal_at = job_info.get("completed_at") or job_info.get("failed_at") or job_info.get("updated_at")
    elapsed_end = terminal_at if public_status in {"success", "failed"} else None
    elapsed_seconds = _duration_seconds(job_info.get("created_at"), elapsed_end)
    if elapsed_seconds is not None:
        payload["elapsed_seconds"] = elapsed_seconds
    queue_wait_seconds = _duration_seconds(job_info.get("queued_at"), job_info.get("started_at"))
    if queue_wait_seconds is not None:
        payload["queue_wait_seconds"] = queue_wait_seconds
    run_seconds = _duration_seconds(job_info.get("started_at"), elapsed_end)
    if run_seconds is not None:
        payload["run_seconds"] = run_seconds
    if public_status == "queued":
        payload["queue_position"] = _queued_position(job_info["job_id"])
    elif public_status == "running":
        payload["progress_pct"] = _progress_pct(job_info)
    elif public_status == "success":
        payload["completed_at"] = job_info.get("completed_at") or job_info.get("updated_at", "")
        files = _list_result_files(job_info)
        result_payload: dict[str, Any] = {"files": files}
        if "hit_count" in job_info:
            result_payload["hit_count"] = int(job_info.get("hit_count", 0) or 0)
        payload["result"] = result_payload
    else:
        payload["failed_at"] = job_info.get("failed_at") or job_info.get("updated_at", "")
        payload["error"] = {
            "code": job_info.get("error_code") or ("CANCELLED" if job_info.get("status") == "cancelled" else "BLAST_FAILED"),
            "message": job_info.get("error") or "BLAST job failed",
        }
    return payload


def _get_job_or_404(job_id: str) -> dict[str, Any]:
    job_id = _sanitize_job_id(job_id)
    with _jobs_lock:
        job_info = _jobs.get(job_id)
    if not job_info:
        job_info = _load_job_cm(job_id)
        if job_info:
            with _jobs_lock:
                _jobs[job_id] = job_info
    if not job_info:
        raise HTTPException(404, f"Job {job_id} not found")
    if job_info.get("status") not in _TERMINAL_STATES and job_info.get("status") != "queued":
        job_info = _refresh_job_status(job_id) or job_info
    return job_info


def _resolve_result_file(job_info: dict[str, Any], file_id: str) -> dict[str, Any]:
    if not re.match(r"^[A-Za-z0-9._-]{1,128}$", file_id):
        raise HTTPException(400, "Invalid file_id")
    for item in _list_result_files(job_info):
        if item.get("file_id") == file_id:
            return item
    raise HTTPException(404, f"File {file_id} not found")


def _safe_result_filename(value: str) -> str:
    filename = str(value or "").strip()
    if not re.match(r"^[A-Za-z0-9._-]{1,128}\.(?:xml|out)(?:\.gz)?$", filename, re.IGNORECASE):
        raise HTTPException(400, "Invalid result filename")
    return filename


def _safe_result_blob_path(value: str, fallback_filename: str) -> str:
    blob_path = str(value or fallback_filename).strip().lstrip("/")
    if ".." in blob_path or "?" in blob_path or "#" in blob_path:
        raise HTTPException(400, "Invalid result blob path")
    if not re.match(r"^[A-Za-z0-9._/-]{1,512}\.(?:xml|out)(?:\.gz)?$", blob_path, re.IGNORECASE):
        raise HTTPException(400, "Invalid result blob path")
    if not blob_path.split("/")[-1].startswith("batch_"):
        raise HTTPException(400, "Invalid result blob path")
    return blob_path


@v1.post("/jobs", tags=["Jobs"], status_code=202, summary="Submit a BLAST search",
          openapi_extra={"requestBody": {"content": {"application/json": {"examples": {
              "mode_a": _MODE_A_EXAMPLE, "mode_b": _MODE_B_EXAMPLE, "mode_b_taxid": _MODE_B_TAXID_EXAMPLE,
          }}}}})
async def submit_job(req: JobSubmitRequest, x_elb_internal_token: Optional[str] = Header(None, alias="X-ELB-Internal-Token")):
    """Submit a BLAST search job. Mode is auto-detected:

    - **Mode A** — if `queries` and `results` are provided as full Blob URLs
    - **Mode B** — if `query_fasta` is provided (inline FASTA text)

    Returns a `job_id` for polling status via `GET /jobs/{job_id}/status`.
    """
    _ensure_loaded()
    if req.program not in _VALID_PROGRAMS:
        raise HTTPException(400, f"Invalid program. Must be: {', '.join(sorted(_VALID_PROGRAMS))}")
    submission_source = _effective_submission_source(req.submission_source, x_elb_internal_token)
    external_correlation_id = _safe_detail_value(req.external_correlation_id)

    if req.idempotency_key:
        job_id = _job_id_from_idempotency_key(f"{submission_source}:{req.idempotency_key}")
        with _jobs_lock:
            existing = _jobs.get(job_id)
        if existing:
            return {
                "job_id": job_id,
                "status": existing.get("status", "unknown"),
                "submission_source": existing.get("submission_source", submission_source),
                "external_correlation_id": existing.get("external_correlation_id", external_correlation_id),
                "blast_version": existing.get("blast_version", "unknown"),
                "db_name": existing.get("db_name", "unknown"),
                "db_version": existing.get("db_version", "unknown"),
                "message": "Existing job returned for idempotency_key.",
                "status_url": f"/v1/jobs/{job_id}/status",
            }
    else:
        job_id = uuid.uuid4().hex[:12]
    is_b = req.query_fasta is not None

    if is_b:
        if not req.query_fasta.strip():
            raise HTTPException(400, "query_fasta must not be empty")
        try: queries_url = _upload_fasta(job_id, req.query_fasta)
        except Exception as e: raise HTTPException(503, f"Upload failed: {str(e)[:200]}")
        if _BLOB_URL_RE.match(req.db):
            try: _validate_blob_url(req.db, "db")
            except ValueError as e: raise HTTPException(400, str(e))
            db_url = req.db
        else:
            try: _validate_short_blob_name(req.db, "db")
            except ValueError as e: raise HTTPException(400, str(e))
            db_url = f"{_blob_base()}/blast-db/{req.db}/{req.db}"
        results_url = f"{_blob_base()}/results/{job_id}"
        opts = _build_options(req.blast_options, req.taxid, req.is_inclusive)
    else:
        if not req.queries or not req.results:
            raise HTTPException(400, "Mode A requires queries and results Blob URLs")
        try:
            _validate_blob_url(req.db, "db"); _validate_blob_url(req.queries, "queries"); _validate_blob_url(req.results, "results")
        except ValueError as e: raise HTTPException(400, str(e))
        db_url, queries_url, results_url = req.db, req.queries, req.results
        opts = req.options or "-evalue 0.01 -outfmt 7"

    try: _azcopy_login()
    except RuntimeError as e: raise HTTPException(503, str(e)[:300])

    config = configparser.ConfigParser()
    ini = os.path.join(os.path.dirname(__file__), "elb-cfg.ini")
    if os.path.isfile(ini): config.read(ini, encoding="utf-8")
    cluster = req.cluster_name or CLUSTER_NAME
    for s in ("cloud-provider","cluster","blast"):
        if not config.has_section(s): config.add_section(s)
    config["cloud-provider"]["azure-region"] = AZURE_REGION
    config["cloud-provider"]["azure-resource-group"] = RESOURCE_GROUP
    config["cloud-provider"]["azure-storage-account"] = STORAGE_ACCOUNT
    config["cloud-provider"]["azure-acr-name"] = ACR_NAME
    config["cloud-provider"]["azure-acr-resource-group"] = ACR_RESOURCE_GROUP
    config["cluster"]["name"] = cluster
    config["cluster"]["machine-type"] = MACHINE_TYPE
    config["cluster"]["num-nodes"] = str(NUM_NODES)
    config["blast"]["program"] = req.program
    config["blast"]["db"] = db_url
    config["blast"]["queries"] = queries_url
    config["blast"]["results"] = results_url
    config["blast"]["options"] = opts
    if req.batch_len is not None:
        config["blast"]["batch-len"] = str(req.batch_len)

    from io import StringIO
    config_buf = StringIO()
    config.write(config_buf)
    config_text = config_buf.getvalue()

    db_name = _db_name_from_value(req.db)
    blast_version = _blast_version_detail()
    db_version = _db_version_detail(db_name)
    job_data = {
        "job_id": job_id, "status": "queued", "mode": "B" if is_b else "A",
        "created_at": _now_iso(), "queued_at": _now_iso(),
        "priority": _normalise_priority(req.priority),
        "idempotency_key": req.idempotency_key or "",
        "resource_profile": req.resource_profile,
        "submission_source": submission_source,
        "external_correlation_id": external_correlation_id,
        "cluster_name": cluster, "program": req.program, "db": db_url,
        "db_name": db_name,
        "db_version": db_version["version"],
        "db_version_detail": db_version,
        "blast_version": blast_version["version"],
        "blast_version_detail": blast_version,
        "results": results_url, "config_ini": config_text,
    }
    _save_job(job_id, job_data, require_persist=True)

    dispatched = _dispatcher_once()
    logger.info("Job %s queued (mode %s priority=%s dispatched=%s)", job_id, "B" if is_b else "A", req.priority, dispatched)
    position = _queued_position(job_id)
    with _jobs_lock:
        current_status = _jobs.get(job_id, {}).get("status")
    status = current_status or ("dispatching" if dispatched else "queued")
    return {
        "job_id": job_id,
        "status": status,
        "queue_position": position,
        "submission_source": submission_source,
        "external_correlation_id": external_correlation_id,
        "created_at": job_data["created_at"],
        "blast_version": blast_version["version"],
        "blast_version_detail": blast_version,
        "db_name": db_name,
        "db_version": db_version["version"],
        "db_version_detail": db_version,
        "message": f"Poll GET /v1/jobs/{job_id}/status for progress.",
        "status_url": f"/v1/jobs/{job_id}/status",
    }

# ── Jobs — List ────────────────────────────────────────────────────────────

@v1.get("/jobs", tags=["Jobs"], summary="List all jobs")
async def list_jobs():
    """List all tracked BLAST jobs. State is persisted in K8s ConfigMaps."""
    _ensure_loaded()
    with _jobs_lock:
        items = list(_jobs.items())
    return {"jobs": [
        {"job_id": jid, "status": i["status"], "mode": i.get("mode","A"),
         "created_at": i.get("created_at",""), "program": i.get("program",""),
         "cluster_name": i.get("cluster_name",""), "db": i.get("db",""),
         "priority": i.get("priority", _PRIORITY_LABELS["normal"]),
         "queue_position": _queued_position(jid)}
        for jid, i in items
    ], "count": len(items)}

# ── Jobs — Status ──────────────────────────────────────────────────────────

@v1.get("/jobs/{job_id}/status", tags=["Jobs"], summary="Get job status")
async def get_job_status(job_id: str):
    """Get detailed job status by polling Kubernetes jobs and blob storage markers."""
    _ensure_loaded()
    job_id = _sanitize_job_id(job_id)
    with _jobs_lock: job_info = _jobs.get(job_id)
    if not job_info:
        job_info = _load_job_cm(job_id)
        if job_info:
            with _jobs_lock: _jobs[job_id] = job_info
    if not job_info: raise HTTPException(404, f"Job {job_id} not found")
    if job_info.get("status") not in _TERMINAL_STATES and job_info.get("status") != "queued":
        job_info = _refresh_job_status(job_id) or job_info

    return {
        "job_id": job_id,
        "status": job_info.get("status", "unknown"),
        "phase": job_info.get("phase", job_info.get("status", "unknown")),
        "queue_position": _queued_position(job_id),
        "created_at": job_info.get("created_at", ""),
        "updated_at": job_info.get("updated_at", ""),
        "last_progress_at": job_info.get("last_progress_at", ""),
        "program": job_info.get("program", ""),
        "db": job_info.get("db", ""),
        "resource_profile": job_info.get("resource_profile", "standard"),
        "error": job_info.get("error", ""),
        "kubernetes": {"summary": job_info.get("k8s_summary", {})},
    }

# ── Jobs — Delete ──────────────────────────────────────────────────────────

@v1.delete("/jobs/{job_id}", tags=["Jobs"], summary="Delete a job")
async def delete_job(job_id: str):
    """Delete a tracked job, clean up K8s resources and ConfigMap."""
    _ensure_loaded()
    job_id = _sanitize_job_id(job_id)
    with _jobs_lock: job_info = _jobs.get(job_id)
    if not job_info:
        job_info = _load_job_cm(job_id)
    if not job_info: raise HTTPException(404, f"Job {job_id} not found")
    with _jobs_lock:
        _jobs[job_id] = job_info

    _cancel_job(job_id, "deleted by API request", terminal_status="cancelled")
    cfg_path = job_info.get("cfg_path", "")
    if cfg_path:
        _cleanup_tmp(os.path.dirname(cfg_path))
    with _jobs_lock:
        _jobs.pop(job_id, None)
    _delete_job_cm(job_id)
    return {"job_id": job_id, "status": "deleted"}

# ── Jobs — Results ─────────────────────────────────────────────────────────

_MERGED_RESULTS_BLOB = "merged_results.out.gz"


@v1.get("/jobs/{job_id}/results", tags=["Jobs"], summary="Download results")
async def download_results(
    job_id: str,
    content: Literal["full", "merged", "xml"] = Query(
        default="full",
        description=(
            "Result packaging mode. 'full' (default, backward-compatible) returns "
            "a ZIP of every shard *.out.gz / *.out from the results container. "
            "'merged' returns a ZIP containing only merged_results.out.gz "
            "(404 if the merger has not uploaded it yet). 'xml' streams the "
            "gunzipped BLAST XML (outfmt=5) from merged_results.out.gz as "
            "application/xml."
        ),
    ),
):
    """Download BLAST results for a completed job.

    - ``content=full`` (default): every shard's ``*.out.gz``/``*.out`` packed
      into ``blast-results-<job_id>.zip``. This preserves the original
      contract.
    - ``content=merged``: a ZIP that contains only ``merged_results.out.gz``
      from the sharded merger. Returns 404 when the merger has not yet
      published that file.
    - ``content=xml``: the gunzipped BLAST XML (outfmt=5) from
      ``merged_results.out.gz``, served as ``application/xml``. Returns 404
      when the merger output is not yet available.
    """
    _ensure_loaded()
    job_id = _sanitize_job_id(job_id)
    with _jobs_lock: job_info = _jobs.get(job_id)
    if not job_info: job_info = _load_job_cm(job_id)
    if not job_info: raise HTTPException(404, f"Job {job_id} not found")
    results_url = job_info.get("results", "").rstrip("/")
    if not results_url: raise HTTPException(404, "No results URL")

    work_dir = f"/tmp/results-{job_id}-{uuid.uuid4().hex}"
    zip_path = f"/tmp/results-{job_id}-{uuid.uuid4().hex}.zip"
    xml_path = f"/tmp/results-{job_id}-{uuid.uuid4().hex}.xml"
    try:
        os.makedirs(work_dir, exist_ok=True)
        _azcopy_login()

        if content == "full":
            safe_exec(
                ["azcopy", "cp", f"{results_url}/*", work_dir,
                 "--recursive", "--include-pattern", "*.out.gz;*.out"],
                timeout=300,
            )
            files = glob.glob(os.path.join(work_dir, "**", "*.out*"), recursive=True)
            if not files:
                raise HTTPException(404, "No result files found")
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in files:
                    zf.write(f, os.path.relpath(f, work_dir))
            return FileResponse(
                zip_path,
                filename=f"blast-results-{job_id}.zip",
                media_type="application/zip",
                background=BackgroundTask(_cleanup_tmp, work_dir, zip_path),
            )

        # content == 'merged' or 'xml' — both need merged_results.out.gz.
        # Use azcopy's include-pattern so the call succeeds (with no file)
        # when the merger has not uploaded yet, instead of returning a
        # non-zero exit. Then verify locally and 404 if missing.
        safe_exec(
            ["azcopy", "cp", f"{results_url}/*", work_dir,
             "--recursive", "--include-pattern", _MERGED_RESULTS_BLOB],
            timeout=300,
        )
        candidates = glob.glob(
            os.path.join(work_dir, "**", _MERGED_RESULTS_BLOB), recursive=True
        )
        if not candidates:
            raise HTTPException(
                404,
                f"{_MERGED_RESULTS_BLOB} is not available for this job yet",
            )
        merged_local = candidates[0]

        if content == "merged":
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(merged_local, _MERGED_RESULTS_BLOB)
            return FileResponse(
                zip_path,
                filename=f"blast-results-{job_id}-merged.zip",
                media_type="application/zip",
                background=BackgroundTask(_cleanup_tmp, work_dir, zip_path),
            )

        # content == 'xml'
        with gzip.open(merged_local, "rb") as src, open(xml_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        return FileResponse(
            xml_path,
            filename=f"blast-results-{job_id}.xml",
            media_type="application/xml",
            background=BackgroundTask(_cleanup_tmp, work_dir, xml_path),
        )
    except HTTPException:
        raise
    except Exception as e:
        _cleanup_tmp(work_dir, zip_path, xml_path)
        raise HTTPException(500, str(e)[:500])


# ── External ElasticBLAST API facade ───────────────────────────────────────

external_v1 = APIRouter(
    prefix="/api/v1/elastic-blast",
    tags=["External ElasticBLAST"],
    dependencies=[Depends(require_api_token)],
)


@external_v1.post("/submit", status_code=202, summary="Submit an external ElasticBLAST job")
async def external_submit(req: ExternalSubmitRequest) -> dict[str, Any]:
    """Public submit contract for direct API callers.

    The external contract always uses inline FASTA and BLAST XML output
    (`outfmt=5`) so the downstream parser can recover `Hsp_hseq`.
    """

    _build_external_options(req.options, req.taxid, req.is_inclusive)
    blast_options = BlastOptions(
        evalue=req.options.evalue,
        max_target_seqs=req.options.max_target_seqs,
        outfmt="5",
        extra=f"-word_size {req.options.word_size} {'-dust yes' if req.options.dust else '-dust no'}",
    )
    internal = JobSubmitRequest(
        program=req.program,
        db=req.db,
        query_fasta=req.query_fasta,
        blast_options=blast_options,
        taxid=req.taxid,
        is_inclusive=req.is_inclusive,
        priority=req.priority,
        idempotency_key=req.idempotency_key,
        resource_profile=req.resource_profile,
        batch_len=req.batch_len,
        submission_source=_DEFAULT_EXTERNAL_SOURCE,
        external_correlation_id=_safe_detail_value(req.external_correlation_id),
    )
    response = await submit_job(internal)
    job_info = _get_job_or_404(response["job_id"])
    payload = _external_job_payload(job_info)
    payload["status"] = "queued" if payload["status"] == "running" and job_info.get("status") in {"dispatching", "submitting"} else payload["status"]
    return payload


@external_v1.get("/jobs/{job_id}", summary="Get external ElasticBLAST job status")
async def external_job_status(job_id: str) -> dict[str, Any]:
    return _external_job_payload(_get_job_or_404(job_id))


@external_v1.get("/jobs/{job_id}/files/{file_id}", summary="Download an external ElasticBLAST result file")
async def external_download_file(job_id: str, file_id: str):
    job_info = _get_job_or_404(job_id)
    if _external_status(job_info) != "success":
        raise HTTPException(409, "Result files are available only after the job succeeds")
    item = _resolve_result_file(job_info, file_id)
    filename = _safe_result_filename(item["filename"])
    blob_path = _safe_result_blob_path(item.get("blob_path", ""), filename)
    results_url = str(job_info.get("results", "")).rstrip("/")
    if not results_url:
        raise HTTPException(404, "No results URL")
    work_dir = f"/tmp/external-results-{job_id}-{file_id}-{uuid.uuid4().hex}"
    local_path = os.path.join(work_dir, filename)
    try:
        os.makedirs(work_dir, exist_ok=True)
        _azcopy_login()
        safe_exec(["azcopy", "cp", f"{results_url}/{blob_path}", local_path], timeout=300)
        media_type = "application/gzip" if filename.endswith(".gz") else "application/xml"
        return FileResponse(
            local_path,
            filename=filename,
            media_type=media_type,
            background=BackgroundTask(_cleanup_tmp, work_dir),
        )
    except HTTPException:
        _cleanup_tmp(work_dir)
        raise
    except Exception as exc:
        _cleanup_tmp(work_dir)
        raise HTTPException(500, str(exc)[:500])

# ── Register versioned router ──────────────────────────────────────────────
app.include_router(v1)
app.include_router(external_v1)

# ── Entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # ``reload=True`` is a development-only convenience that re-imports the
    # module on every file change. It must never be enabled in production
    # because the autoreloader (a) keeps a parent process holding stale state
    # and (b) widens the attack surface by re-executing arbitrary files. Gate
    # it on an explicit env opt-in so a misconfigured image cannot ship with
    # reload enabled.
    _dev_reload = os.environ.get("ELB_OPENAPI_DEV_RELOAD", "").strip().lower() in {"1", "true", "yes"}
    _bind_host = os.environ.get("ELB_OPENAPI_BIND_HOST", "0.0.0.0").strip() or "0.0.0.0"
    _bind_port = int(os.environ.get("ELB_OPENAPI_BIND_PORT", "8000"))
    uvicorn.run(
        f"{Path(__file__).stem}:app",
        host=_bind_host,
        port=_bind_port,
        reload=_dev_reload,
    )
