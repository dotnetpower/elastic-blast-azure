"""ElasticBLAST on Azure — OpenAPI Server v3.1.

Runs inside AKS as a pod. Self-contained: stores job state in K8s ConfigMaps,
optionally forwards events to Control Plane via webhook.
"""

from __future__ import annotations

import base64
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
from collections import OrderedDict
from datetime import datetime, timezone
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from threading import BoundedSemaphore, Event, Lock, Thread
from typing import Any, Literal, Optional
from urllib.parse import urlparse

import uvicorn
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

import submit_coordination as _coord
from helpers import (
    _age_seconds,
    _cache_trim,
    _cleanup_tmp,
    _db_name_from_value,
    _decode_jobs_cursor,
    _duration_seconds,
    _encode_jobs_cursor,
    _job_id_from_idempotency_key,
    _last_json,
    _molecule_label,
    _now_iso,
    _parse_ts,
    _redact_config,
    _resolve_molecule_type,
    _safe_detail_value,
    _safe_label_value,
    _safe_result_blob_path,
    _safe_result_filename,
    _sanitize_job_id,
    _validate_short_blob_name,
)
from schemas import (
    _MODE_A_EXAMPLE,
    _MODE_B_EXAMPLE,
    _MODE_B_TAXID_EXAMPLE,
    _PASSTHROUGH_MAX_KEY_LEN,
    _PASSTHROUGH_MAX_KEYS,
    _PASSTHROUGH_MAX_TOTAL_BYTES,
    _PASSTHROUGH_MAX_VALUE_LEN,
    BlastOptions,
    DatabaseList,
    DatabaseListItem,
    DatabaseMetadata,
    ExternalBlastOptions,
    ExternalSubmitRequest,
    JobSubmitRequest,
    _sanitize_passthrough,
)
from schemas import (
    DEFAULT_EXTERNAL_SOURCE as _DEFAULT_EXTERNAL_SOURCE,
)
from util import run_cancellable, safe_exec

try:
    import eta as _eta
except Exception:  # pragma: no cover - ETA overlay is optional
    _eta = None

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
VERSION = "3.7.6"

# /v1/ready hard time budget. The endpoint exists so external callers can
# pre-flight whether a POST /v1/jobs is likely to succeed without paying the
# heavy /v1/health cost (DefaultAzureCredential token + full kubectl get nodes).
# Kept short on purpose: every probe inside uses kubectl --request-timeout=1s
# so a stopped AKS API server fails the whole call inside the budget.
# 2.5s instead of 3.0s so the dashboard's 5.0s client timeout has a clean
# 2× safety margin against Container Apps cold-path jitter.
READY_BUDGET_SECONDS = max(1.0, float(os.environ.get("ELB_OPENAPI_READY_BUDGET_SECONDS", "2.5")))
# Label selector for the BLAST workload node pool. Empty disables the check
# (e.g. autoscale-only clusters where the pool spins up after the first job).
WORKLOAD_POOL_LABEL = os.environ.get("ELB_OPENAPI_WORKLOAD_POOL_LABEL", "workload=blast").strip()
# Optional pool *name* the Cluster Autoscaler must actually own for the
# autoscaler-pending degraded state to apply. When set, the
# ``cluster-autoscaler-status`` ConfigMap body is inspected and must mention
# this pool (case-insensitive substring match against ``.data.status``);
# otherwise the autoscaler ConfigMap existence alone is enough (legacy
# behaviour, preserved for backward compatibility on single-pool clusters).
WORKLOAD_POOL_NAME = os.environ.get("ELB_OPENAPI_WORKLOAD_POOL_NAME", "").strip()
# When the workload pool has zero Ready nodes, /v1/ready normally fails with
# 'no_workload_nodes'. On clusters where the Cluster Autoscaler is enabled
# the pool can legitimately be at zero between jobs — the very act of
# POST /v1/jobs is what scales the pool back up. Set
# ELB_OPENAPI_AUTOSCALER_AWARE_READY=1 (the default) so /v1/ready degrades
# the workload_pool check to a non-fatal 'autoscaler_pending' info entry
# in that case instead of 503-ing the probe.
READY_AUTOSCALER_AWARE = os.environ.get(
    "ELB_OPENAPI_AUTOSCALER_AWARE_READY", "1"
).strip().lower() in {"1", "true", "yes"}
# Per-token rate limit for /v1/ready. The probe is intentionally cheap but
# a token holder can still poll it as a cluster-state oracle. 30 req/min /
# token is generous for a pre-flight check and cuts the enumeration surface.
READY_RATE_LIMIT_PER_MINUTE = max(
    1, int(os.environ.get("ELB_OPENAPI_READY_RATE_LIMIT_PER_MINUTE", "30"))
)
# Hard cap on the number of distinct (token | per-IP-anonymous) buckets that
# can co-exist in ``_READY_RATE_BUCKETS``. The dict is ordered + accessed
# LRU-style, so the oldest bucket evicts when the cap is reached. Bounds
# memory permanently on long-running pods that have served many distinct
# tokens / IPs (the original GC-on-empty path was a no-op because the prod
# code always appended a timestamp before the GC check could run — issue
# #20 P1 #3).
READY_RATE_BUCKETS_MAX = max(
    16, int(os.environ.get("ELB_OPENAPI_READY_RATE_BUCKETS_MAX", "4096"))
)
# Hash the cluster name in the /v1/ready response so the probe does not
# leak the AKS cluster identifier to anyone holding a token. Off by default
# to preserve the existing contract; flip to 1 in shared environments.
READY_MASK_CLUSTER_NAME = os.environ.get(
    "ELB_OPENAPI_READY_MASK_CLUSTER_NAME", "0"
).strip().lower() in {"1", "true", "yes"}

MAX_ACTIVE_SUBMISSIONS = max(1, int(os.environ.get("ELB_OPENAPI_MAX_ACTIVE_SUBMISSIONS", "1")))
# Cap concurrent azcopy subprocesses spawned by the submit path. submit_job /
# external_submit run in FastAPI's threadpool (default 40 sync slots), and each
# Mode-B submit spawns ~2 azcopy processes (FASTA upload + the *uncached* DB-
# metadata fetch). Left unbounded, a ~50-submit burst could fan out to dozens of
# concurrent azcopy processes and OOM the pod under its memory limit — the very
# #54 failure re-created via a new path. A small semaphore keeps the submit path
# many-x more parallel than the old serialized (single-event-loop) model while
# bounding peak subprocess memory well under the limit; the admit cap still
# governs how many jobs actually run.
AZCOPY_CONCURRENCY = max(1, int(os.environ.get("ELB_OPENAPI_AZCOPY_CONCURRENCY", "8")))
_azcopy_slots = BoundedSemaphore(AZCOPY_CONCURRENCY)
DISPATCH_INTERVAL_SECONDS = max(1, int(os.environ.get("ELB_OPENAPI_DISPATCH_INTERVAL_SECONDS", "5")))
WATCHDOG_INTERVAL_SECONDS = max(5, int(os.environ.get("ELB_OPENAPI_WATCHDOG_INTERVAL_SECONDS", "60")))
SUBMIT_STUCK_SECONDS = max(60, int(os.environ.get("ELB_OPENAPI_SUBMIT_STUCK_SECONDS", "7200")))
PENDING_STUCK_SECONDS = max(300, int(os.environ.get("ELB_OPENAPI_PENDING_STUCK_SECONDS", "1800")))
RUNNING_IDLE_SECONDS = max(300, int(os.environ.get("ELB_OPENAPI_RUNNING_IDLE_SECONDS", "10800")))
FINALIZER_STUCK_SECONDS = max(300, int(os.environ.get("ELB_OPENAPI_FINALIZER_STUCK_SECONDS", "1800")))
# #62: a dispatching/submitting job whose in-process submit thread died (pod
# restart / cluster stop mid-submit) must release its MAX_ACTIVE_SUBMISSIONS
# slot within one watchdog tick, not after SUBMIT_STUCK_SECONDS (2h). Otherwise
# a few post-restart zombies hold every slot and the dispatcher wedges
# (throughput -> 0). RECLAIM_GRACE_SECONDS keeps the watchdog from racing a job
# that was just claimed (status=dispatching) but whose submit thread has not
# started yet; SUBMIT_MAX_RETRIES bounds the requeue loop so a job that keeps
# losing its thread (cluster still flaky / cold-staging then stops again)
# eventually fails instead of re-sticking the dispatcher forever.
RECLAIM_GRACE_SECONDS = max(5, int(os.environ.get("ELB_OPENAPI_RECLAIM_GRACE_SECONDS", "45")))
SUBMIT_MAX_RETRIES = max(1, int(os.environ.get("ELB_OPENAPI_SUBMIT_MAX_RETRIES", "3")))
# Grace window for the SUCCESS marker to become consistent with the result
# blob listing. The finalizer uploads every artifact before writing
# metadata/SUCCESS.txt, so when the marker is visible the artifacts are already
# durably stored; only the azcopy List that /jobs/{id}/results relies on can
# briefly lag. Within this window a marker-complete job is held at "finalizing"
# until the listing catches up (eliminating the completed→/results 404 race);
# past the window the marker is trusted so a listing that never catches up
# cannot wedge the job in a non-terminal state forever.
RESULTS_VISIBILITY_GRACE_SECONDS = max(0, int(os.environ.get("ELB_OPENAPI_RESULTS_VISIBILITY_GRACE_SECONDS", "120")))
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
    {"name": "Databases", "description": "BLAST database catalogue and version metadata"},
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


# BLAST+ version pinned by the ElasticBLAST release this OpenAPI plane ships
# alongside. The OpenAPI container does NOT install the BLAST+ binaries
# (they only live on the in-cluster search pods), so the binary-probe branch
# below always fails inside the deployed pod and the function used to fall
# through to ``"unknown"``. That nullified blast_version in every dashboard
# job payload until the result XML branch in ``_blast_version_from_result``
# happened to fire -- which never does for ``-outfmt 6/7`` (tabular) runs.
# Keep this in sync with the ``ELB_DOCKER_VERSION`` comment in
# elastic-blast-azure src/elastic_blast/constants.py (currently 2.17.0).
_BLAST_PLUS_PINNED_VERSION = "2.17.0+"


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
    # Last-resort: the BLAST+ version pinned by this ElasticBLAST release.
    # Better than "unknown" for the typical tabular-outfmt run where neither
    # the binary nor the XML result file can supply the value. ``source`` makes
    # the provenance visible so the dashboard / a curious operator can tell it
    # apart from a probed value.
    return {
        "version": _BLAST_PLUS_PINNED_VERSION,
        "detail": _BLAST_PLUS_PINNED_VERSION,
        "source": "elastic_blast_release_pin",
    }


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
            with _azcopy_slots:
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
                # Return ``detail`` as a nested object, NOT a JSON-encoded
                # string. The dashboard surfaces this verbatim under
                # ``payload.external.db_version_detail.detail``; serialising
                # to a string here forced the SPA to render an escaped JSON
                # blob in the job details panel instead of structured fields.
                return {"version": effective_version, "detail": detail, "source": "blastdb_metadata"}
        except Exception as exc:
            logger.debug("DB version metadata unavailable for %s: %s", db_name, str(exc)[:200])
        finally:
            _cleanup_tmp(local_path)
    return {"version": "unknown", "source": "not_available"}


def _storage_oauth_token() -> str:
    """Return a short-lived OAuth bearer token for the Azure Blob data plane.

    Uses ``DefaultAzureCredential`` which already runs inside the pod under
    Workload Identity. The token is acquired fresh on each call — listing
    the database catalogue is infrequent so caching adds complexity for no
    real benefit.
    """
    from azure.identity import DefaultAzureCredential
    cred = DefaultAzureCredential()
    return cred.get_token("https://storage.azure.com/.default").token


_NON_DATABASE_PREFIXES: frozenset[str] = frozenset(
    {"metadata", "custom-db-build", ".staging", "custom_db"}
)
_SHARD_PREFIX_RE = re.compile(r"^\d+shards$")


def _is_database_prefix(name: str) -> bool:
    """Return ``True`` only for prefixes that represent a real BLAST DB.

    The ``blast-db`` container also carries non-database top-level
    prefixes — ``{N}shards/`` (prepare-db shard layouts), ``metadata/``
    (oracle staging area), ``.staging/``, ``custom-db-build/``, and
    ``custom_db/`` (each custom DB lives one level deeper). Filtering
    here mirrors the dashboard's ``list_databases`` skip-rules so that
    ``GET /v1/databases`` only surfaces user-actionable database names.
    """
    if not name or name in _NON_DATABASE_PREFIXES:
        return False
    if _SHARD_PREFIX_RE.match(name):
        return False
    return True


def _list_blast_database_names(
    container: str = "blast-db",
    timeout: int = 30,
) -> tuple[list[str], str]:
    """List the top-level prefixes (database names) inside ``container``.

    Calls the Azure Blob REST API with ``delimiter=/`` so the response only
    enumerates the first-level "directories" — the database names. Avoids
    fetching the entire blob list, which on a populated cluster runs into
    tens of thousands of entries.

    Drops non-database prefixes (shard layouts, oracle metadata, custom-db
    staging) via :func:`_is_database_prefix` so callers receive only real
    BLAST databases.

    Results are cached in-process with TTL
    ``ELB_OPENAPI_DB_LIST_TTL_SECONDS`` (default 120s). The cache cannot be
    bypassed from a normal request — the catalogue is a fixed shape per
    pod and the cost of one stale-by-up-to-120s response is preferable to
    a footgun for callers.

    Returns ``(names, cache_status)`` where ``cache_status`` is ``HIT`` or
    ``MISS``. Raises ``FileNotFoundError`` if the container does not exist;
    other HTTP errors propagate as :class:`requests.HTTPError`.
    """
    now = time.time()
    if _DB_LIST_TTL_SECONDS > 0:
        with _db_list_lock:
            cached = _db_list_cache.get(container)
        if cached and (now - cached["fetched_at"]) < _DB_LIST_TTL_SECONDS:
            return list(cached["names"]), "HIT"

    import xml.etree.ElementTree as ET

    import requests

    token = _storage_oauth_token()
    url = f"{_blob_base()}/{container}"
    headers = {
        "Authorization": f"Bearer {token}",
        "x-ms-version": "2020-04-08",
    }
    names: list[str] = []
    marker: str | None = None
    for _ in range(20):  # safety cap; one container should never need 20 pages
        params: dict[str, str] = {
            "restype": "container",
            "comp": "list",
            "delimiter": "/",
        }
        if marker:
            params["marker"] = marker
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        if resp.status_code == 404:
            raise FileNotFoundError(f"Container {container!r} not found")
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        for prefix in root.findall(".//BlobPrefix/Name"):
            value = (prefix.text or "").strip().rstrip("/")
            if _is_database_prefix(value):
                names.append(value)
        next_marker = root.find("NextMarker")
        marker = (
            next_marker.text.strip()
            if next_marker is not None and next_marker.text
            else ""
        )
        if not marker:
            break
    sorted_names = sorted(set(names))
    if _DB_LIST_TTL_SECONDS > 0:
        with _db_list_lock:
            _db_list_cache[container] = {"names": list(sorted_names), "fetched_at": now}
    return sorted_names, "MISS"


# ── BLAST database metadata cache ──────────────────────────────────────────
#
# Metadata blobs (``{db}-nucl-metadata.json`` / ``-prot-metadata.json``)
# are read repeatedly by ``GET /v1/databases/{db_name}``. They're tiny
# (~50–100 KB) but change at most weekly when the NCBI snapshot rolls,
# so we keep an in-process TTL+ETag cache to keep the endpoint sub-ms
# on cache hits and 304-fast on revalidation.
#
# The cache is access-time LRU (``OrderedDict.move_to_end`` on hit) so
# that frequently read databases survive eviction even when their last
# *fetch* was long ago. Negative results (genuine blob 404) are recorded
# separately with a shorter TTL so a hostile or buggy caller cannot
# amplify a missing name into a stream of Storage 404s.

_DB_METADATA_TTL_SECONDS = max(
    0, int(os.environ.get("ELB_OPENAPI_DB_METADATA_TTL_SECONDS", "600"))
)
_DB_LIST_TTL_SECONDS = max(
    0, int(os.environ.get("ELB_OPENAPI_DB_LIST_TTL_SECONDS", "120"))
)
_DB_CACHE_MAX_ENTRIES = max(
    8, int(os.environ.get("ELB_OPENAPI_DB_CACHE_MAX_ENTRIES", "128"))
)
_DB_NEGATIVE_TTL_SECONDS = max(
    0, int(os.environ.get("ELB_OPENAPI_DB_NEGATIVE_TTL_SECONDS", "60"))
)

_db_metadata_cache: "OrderedDict[tuple[str, str], dict[str, Any]]" = OrderedDict()
_db_negative_cache: "OrderedDict[tuple[str, str], float]" = OrderedDict()
_db_list_cache: dict[str, dict[str, Any]] = {}
_db_metadata_lock = Lock()
_db_list_lock = Lock()


class _MetadataFetchError(Exception):
    """Wraps a transient (network / 5xx / parse) failure from Storage.

    Callers distinguish this from ``return None`` (genuine "not found")
    so a Storage outage surfaces as 503 instead of being mistaken for a
    missing database.
    """

    def __init__(self, message: str, *, transient: bool = True) -> None:
        super().__init__(message)
        self.transient = transient


def _fetch_blob_with_etag(
    url: str,
    etag: str | None,
    timeout: int = 30,
) -> tuple[int, dict[str, Any] | None, str | None]:
    """GET a small JSON blob with optional ``If-None-Match``.

    Returns ``(status_code, parsed_json_or_none, new_etag_or_none)``.
    ``304`` returns ``(304, None, original_etag)``; ``404`` returns
    ``(404, None, None)``; all other failures raise
    :class:`_MetadataFetchError` so the caller can distinguish a
    transient outage from a genuinely missing blob.
    """
    import requests

    try:
        token = _storage_oauth_token()
    except Exception as exc:  # auth failure is transient from our POV
        raise _MetadataFetchError(f"storage auth failed: {exc!s:.200}") from exc
    headers: dict[str, str] = {
        "Authorization": f"Bearer {token}",
        "x-ms-version": "2020-04-08",
    }
    if etag:
        headers["If-None-Match"] = etag
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise _MetadataFetchError(f"network error fetching {url}: {exc!s:.200}") from exc
    if resp.status_code == 304:
        return 304, None, etag
    if resp.status_code == 404:
        return 404, None, None
    if resp.status_code >= 500 or resp.status_code == 429:
        raise _MetadataFetchError(
            f"storage {resp.status_code} for {url}: {resp.text[:200]}",
        )
    if resp.status_code >= 400:
        # 401/403 etc. are configuration errors, not transient — still surface.
        raise _MetadataFetchError(
            f"storage {resp.status_code} for {url}: {resp.text[:200]}",
            transient=False,
        )
    new_etag = resp.headers.get("ETag")
    try:
        parsed = resp.json()
    except ValueError as exc:
        raise _MetadataFetchError(f"invalid JSON from {url}: {exc!s:.200}") from exc
    return resp.status_code, parsed, new_etag


_SNAPSHOT_RE = re.compile(r"/(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})/")


def _normalise_metadata(
    db_name: str,
    raw: dict[str, Any],
    molecule_type_raw: str,
    *,
    container: str = "blast-db",
) -> dict[str, Any]:
    """Project the raw NCBI metadata JSON into the public API schema.

    Unknown molecule types raise :class:`ValueError` (propagated from
    :func:`_resolve_molecule_type`) so the caller can return a 500
    instead of mislabelling the database. A missing snapshot date is
    logged as a warning — the snapshot field falls back to ``"unknown"``
    so existing clients keep working, but operators can grep the log
    for an NCBI path-format change.
    """
    molecule_type, molecule_label = _resolve_molecule_type(molecule_type_raw)
    files_list = raw.get("files") if isinstance(raw.get("files"), list) else []
    source_version = ""
    for item in files_list:
        match = _SNAPSHOT_RE.search(str(item))
        if match:
            source_version = match.group(1)
            break
    if not source_version:
        logger.warning(
            "snapshot regex did not match any entry in metadata.files for %s ("
            "first=%r); NCBI path format may have changed",
            db_name,
            (str(files_list[0]) if files_list else ""),
        )
    schema_version = str(raw.get("version") or "").strip()
    last_updated = str(
        raw.get("last-updated") or raw.get("last_updated") or raw.get("date") or ""
    ).strip()
    title = ""
    for source_key in ("description", "display_name", "title", "name"):
        value = raw.get(source_key)
        if isinstance(value, str) and value.strip():
            title = value.strip()
            break
    return {
        "name": db_name,
        "container": container,
        "title": title,
        "dbtype": str(raw.get("dbtype") or "").strip(),
        "molecule_type": molecule_type,
        "molecule_label": molecule_label,
        "snapshot": source_version or "unknown",
        "last_updated": last_updated or None,
        "number_of_sequences": raw.get("number-of-sequences"),
        "number_of_letters": raw.get("number-of-letters"),
        "number_of_volumes": raw.get("number-of-volumes"),
        "bytes_total": raw.get("bytes-total"),
        "bytes_to_cache": raw.get("bytes-to-cache"),
        "metadata_schema_version": schema_version,
    }


def _project_with_cached_at(metadata: dict[str, Any], fetched_at: float) -> dict[str, Any]:
    out = dict(metadata)
    out["cached_at"] = datetime.fromtimestamp(fetched_at, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    return out


def _database_metadata(
    db_name: str,
    *,
    container: str = "blast-db",
    timeout: int = 30,
) -> tuple[dict[str, Any] | None, str]:
    """Read a BLAST database's metadata (nucleotide or protein).

    Hits an in-process cache first. On miss / expiry, performs a direct
    HTTPS ``GET`` against the metadata blob (no ``azcopy`` subprocess);
    on TTL expiry uses ``If-None-Match`` so unchanged blobs return
    ``304 Not Modified`` and avoid the JSON parse.

    Returns ``(normalised_metadata_or_none, cache_status)`` where
    ``cache_status`` is one of ``HIT``, ``REVALIDATE``, ``MISS`` or
    ``NEGATIVE_HIT``. ``None`` is returned only when both metadata
    blobs are genuinely 404; transient outages raise
    :class:`_MetadataFetchError`.
    """
    from copy import deepcopy

    safe_db = re.sub(r"[^A-Za-z0-9._-]", "", db_name)
    if not safe_db:
        return None, "MISS"
    key = (container, safe_db)
    now = time.time()

    cached_snapshot: dict[str, Any] | None = None
    cached_suffix: str | None = None
    cached_etag: str | None = None
    cached_molecule: str | None = None
    with _db_metadata_lock:
        cached = _db_metadata_cache.get(key)
        if cached and _DB_METADATA_TTL_SECONDS > 0 and (
            now - cached.get("fetched_at", 0.0)
        ) < _DB_METADATA_TTL_SECONDS:
            _db_metadata_cache.move_to_end(key)
            projected = _project_with_cached_at(cached["metadata"], cached["fetched_at"])
            return projected, "HIT"
        # Negative cache: a previous full lookup confirmed both suffixes 404.
        if _DB_NEGATIVE_TTL_SECONDS > 0:
            neg_until = _db_negative_cache.get(key)
            if neg_until is not None and neg_until > now:
                _db_negative_cache.move_to_end(key)
                return None, "NEGATIVE_HIT"
            if neg_until is not None and neg_until <= now:
                _db_negative_cache.pop(key, None)
        if cached is not None:
            cached_suffix = cached.get("suffix")
            cached_etag = cached.get("etag")
            cached_molecule = cached.get("molecule_type_raw")
            cached_snapshot = {
                "metadata": deepcopy(cached["metadata"]),
                "etag": cached.get("etag"),
                "suffix": cached.get("suffix"),
                "molecule_type_raw": cached.get("molecule_type_raw"),
                "fetched_at": cached.get("fetched_at", 0.0),
            }

    # Try the previously-seen suffix first to avoid an extra 404 round-trip.
    candidates: list[tuple[str, str]] = []
    if cached_snapshot and cached_suffix and cached_molecule:
        candidates.append((cached_molecule, cached_suffix))
    for molecule_type_raw, suffix in (
        ("nucl", "-nucl-metadata.json"),
        ("prot", "-prot-metadata.json"),
    ):
        if (molecule_type_raw, suffix) not in candidates:
            candidates.append((molecule_type_raw, suffix))

    saw_404 = False
    last_transient: _MetadataFetchError | None = None
    for molecule_type_raw, suffix in candidates:
        url = f"{_blob_base()}/{container}/{safe_db}/{safe_db}{suffix}"
        send_etag = cached_etag if cached_suffix == suffix else None
        try:
            status, parsed, etag = _fetch_blob_with_etag(
                url, send_etag, timeout=timeout,
            )
        except _MetadataFetchError as exc:
            if exc.transient:
                last_transient = exc
                logger.warning(
                    "transient metadata fetch failure for %s%s: %s",
                    safe_db,
                    suffix,
                    exc,
                )
                continue
            # Non-transient (e.g. 401/403) — fail loud immediately.
            raise
        if status == 304 and cached_snapshot is not None:
            refreshed_metadata = cached_snapshot["metadata"]
            refreshed = {
                "metadata": refreshed_metadata,
                "etag": cached_snapshot["etag"],
                "suffix": cached_snapshot["suffix"],
                "molecule_type_raw": cached_snapshot["molecule_type_raw"],
                "fetched_at": now,
            }
            with _db_metadata_lock:
                if _DB_METADATA_TTL_SECONDS > 0:
                    _db_metadata_cache[key] = {
                        **refreshed,
                        "metadata": deepcopy(refreshed_metadata),
                    }
                    _db_metadata_cache.move_to_end(key)
                    _cache_trim(_db_metadata_cache, _DB_CACHE_MAX_ENTRIES)
            return _project_with_cached_at(refreshed_metadata, now), "REVALIDATE"
        if status == 404:
            saw_404 = True
            continue
        if parsed is None:
            last_transient = _MetadataFetchError(
                f"empty body from {url} (status={status})",
            )
            continue
        try:
            metadata = _normalise_metadata(
                safe_db, parsed, molecule_type_raw, container=container,
            )
        except ValueError as exc:
            logger.error(
                "normalise_metadata failed for %s%s: %s", safe_db, suffix, exc,
            )
            raise _MetadataFetchError(str(exc), transient=False) from exc
        entry = {
            "metadata": metadata,
            "etag": etag,
            "suffix": suffix,
            "molecule_type_raw": molecule_type_raw,
            "fetched_at": now,
        }
        with _db_metadata_lock:
            if _DB_METADATA_TTL_SECONDS > 0:
                _db_metadata_cache[key] = {
                    **entry,
                    "metadata": deepcopy(metadata),
                }
                _db_metadata_cache.move_to_end(key)
                _cache_trim(_db_metadata_cache, _DB_CACHE_MAX_ENTRIES)
            # New positive answer invalidates any stale negative entry.
            _db_negative_cache.pop(key, None)
        return _project_with_cached_at(metadata, now), "MISS"

    # No candidate succeeded.
    if last_transient is not None:
        # Any transient error during the lookup outweighs a sibling 404 — we
        # do NOT know whether the failed suffix was the right one, so we
        # must not synthesise a stable negative cache entry. Re-raise so
        # the caller turns this into a 503.
        raise last_transient
    if saw_404 and _DB_NEGATIVE_TTL_SECONDS > 0:
        with _db_metadata_lock:
            _db_negative_cache[key] = now + _DB_NEGATIVE_TTL_SECONDS
            _db_negative_cache.move_to_end(key)
            _cache_trim(_db_negative_cache, _DB_CACHE_MAX_ENTRIES)
    return None, "MISS"


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
        if refreshed.get("status") in {"dispatching", "submitting"}:
            # Delegate to the SAME bounded reclaim the watchdog uses (#62) so a
            # job that keeps losing its thread across pod restarts is failed once
            # its SUBMIT_MAX_RETRIES budget is spent instead of being requeued
            # unconditionally here. An unbounded requeue in this startup pass
            # would otherwise resurrect a job the watchdog already failed and
            # re-wedge the dispatcher across restarts (live-validated 2026-06-21).
            _reclaim_dead_thread_job(job_id, refreshed)


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
        _eta_snapshot = None
        if (
            _eta is not None
            and _eta.enabled()
            and updates.get("status") == "completed"
            and not current.get("eta_recorded")
        ):
            data["eta_recorded"] = True
            _jobs[job_id] = data
            _eta_snapshot = [dict(v) for v in _jobs.values()]
        else:
            _jobs[job_id] = data
    _save_job_cm(job_id, data)
    if _eta_snapshot is not None:
        try:
            _eta.record_sample(data, _eta_snapshot)
        except Exception:
            pass
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


def _warm_database_cache() -> None:
    """Pre-fetch the database catalogue and per-database metadata.

    Cold-start latency on the ``/v1/databases/*`` endpoints used to spike
    to ~400 ms × N because the in-process cache is wiped on every pod
    restart. This helper runs once during startup (in a daemon thread)
    so the first user-facing request hits a warm cache. Failures are
    logged but never fatal — the pod must still come up if Storage is
    transiently unreachable.
    """
    if os.environ.get("ELB_OPENAPI_DISABLE_WARMUP", "").strip() == "1":
        logger.info("database warmup disabled via env (ELB_OPENAPI_DISABLE_WARMUP=1)")
        return
    try:
        names, _ = _list_blast_database_names()
    except FileNotFoundError:
        logger.info("warmup: blast-db container not yet provisioned")
        return
    except Exception as exc:
        logger.warning("warmup: list failed (non-fatal): %s", str(exc)[:200])
        return
    if not names:
        logger.info("warmup: no databases found")
        return
    warmed = 0
    for name in names:
        try:
            meta, status = _database_metadata(name)
        except _MetadataFetchError as exc:
            logger.warning(
                "warmup: metadata fetch for %s failed (non-fatal): %s",
                name, exc,
            )
            continue
        except Exception as exc:  # defensive — never let warmup crash startup
            logger.warning(
                "warmup: unexpected error for %s (non-fatal): %s",
                name, str(exc)[:200],
            )
            continue
        if meta is not None:
            warmed += 1
        logger.info("warmup: %s -> %s", name, status if meta else "404")
    logger.info("warmup: %d/%d databases primed", warmed, len(names))


@app.on_event("startup")
def _on_startup() -> None:
    _start_background_threads()
    # Run cache warmup in a daemon thread so an unreachable Storage path
    # does not block uvicorn startup or its readiness probe.
    Thread(
        target=_warm_database_cache,
        name="elb-openapi-warm-databases",
        daemon=True,
    ).start()

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


def _blob_base() -> str:
    return f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"


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

# ═══════════════════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════════════════

# ── /v1/ready helpers ──────────────────────────────────────────────────────
# Per-token rate limit + in-process metric counters live here so the route
# body stays declarative. All state is process-local; if the pod is replaced
# the counters reset, which is the same behaviour as in-cluster Prometheus
# exporters started from a fresh process.
_READY_RATE_LOCK = Lock()
_READY_RATE_BUCKETS: "OrderedDict[str, list[float]]" = OrderedDict()
_READY_METRICS_LOCK = Lock()
_READY_METRICS: dict[str, int] = {
    "ok": 0,
    "k8s_unreachable": 0,
    "no_workload_nodes": 0,
    "workload_pool_check_failed": 0,
    "openapi_pod_not_ready": 0,
    "openapi_pod_check_failed": 0,
    "rate_limited": 0,
    "autoscaler_pending": 0,
}


def _ready_token_bucket_check(token_or_anon: str) -> bool:
    """Return True if the request is within budget, False to deny.

    Uses a fixed 60-second sliding window. The bucket key is the SHA-256 of
    the raw token so a leaked log line cannot replay the token. ``anonymous``
    (when ELB_OPENAPI_ALLOW_UNAUTHENTICATED=1 in dev) is bucketed separately,
    and the route additionally suffixes the client IP so a single noisy
    laptop cannot DoS every other unauthenticated caller.

    Memory bound: ``_READY_RATE_BUCKETS`` is an ``OrderedDict`` accessed in
    LRU order. When the dict grows past ``READY_RATE_BUCKETS_MAX`` the
    least-recently-used bucket evicts. This replaces the original GC-on-empty
    path which was unreachable in the prod code (the append happened before
    the empty check) — issue #20 P1 #3.
    """
    now = time.monotonic()
    key = hashlib.sha256(token_or_anon.encode("utf-8", "ignore")).hexdigest()
    with _READY_RATE_LOCK:
        bucket = _READY_RATE_BUCKETS.get(key)
        if bucket is None:
            bucket = []
            _READY_RATE_BUCKETS[key] = bucket
        else:
            # LRU touch — mark this bucket as most-recently-used.
            _READY_RATE_BUCKETS.move_to_end(key)
        # Drop timestamps older than 60s.
        cutoff = now - 60.0
        while bucket and bucket[0] < cutoff:
            bucket.pop(0)
        if len(bucket) >= READY_RATE_LIMIT_PER_MINUTE:
            return False
        bucket.append(now)
        # LRU evict the oldest bucket entries when over the soft cap. The
        # dict is bounded so a long-running pod that has served many distinct
        # tokens / IPs cannot accumulate unbounded SHA-256 keys.
        while len(_READY_RATE_BUCKETS) > READY_RATE_BUCKETS_MAX:
            _READY_RATE_BUCKETS.popitem(last=False)
    return True


def _ready_record_metric(code: str) -> None:
    with _READY_METRICS_LOCK:
        _READY_METRICS[code] = _READY_METRICS.get(code, 0) + 1


def _ready_masked_cluster_name() -> str:
    if not READY_MASK_CLUSTER_NAME:
        return CLUSTER_NAME
    # 16-char prefix of SHA-256 is a stable opaque identifier the caller
    # can correlate across calls without learning the real cluster name.
    digest = hashlib.sha256(CLUSTER_NAME.encode("utf-8", "ignore")).hexdigest()
    return f"sha256:{digest[:16]}"


def _anonymous_client_ip(request: Request) -> str:
    """Return the real anonymous-caller IP for per-IP bucket keying.

    The /v1/ready route runs behind an in-pod nginx + the Container Apps
    ingress, so ``request.client.host`` is always the proxy's IP and every
    unauthenticated caller would collapse into one shared bucket; one noisy
    laptop could then DoS the whole anonymous slot. We trust the
    ``X-Forwarded-For`` first hop (the closest reverse-proxy boundary) and
    fall back to ``X-Real-IP`` then ``request.client.host`` so the function
    still returns something when the route is called via TestClient or in a
    deployment that strips the proxy headers.

    Returns ``"unknown"`` when no source is available so the caller key stays
    a non-empty string. Issue #20 P1 #2.
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        first = xff.split(",", 1)[0].strip()
        if first:
            return first
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        real_ip = real_ip.strip()
        if real_ip:
            return real_ip
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


_AUTOSCALER_NAME_LINE_RE = re.compile(
    r"^\s*(?:pool\s+)?name\s*:\s*(\S+)\s*$", re.IGNORECASE
)


def _autoscaler_status_mentions_pool(body: str, pool_name: str) -> bool:
    """Return True iff the cluster-autoscaler-status body declares a
    node-group whose ``Name:`` (or ``Pool Name:``) field exactly matches
    ``pool_name`` (case-insensitive, anchored on the field line).

    The previous implementation did ``WORKLOAD_POOL_NAME.lower() in body.lower()``
    which matched any line containing the substring — e.g. ``blast`` matched
    ``blastlogs`` and ``warmupblast``, ``pool`` matched ``systempool``.
    Issue #20 P3 #8.
    """
    target = pool_name.strip().lower()
    if not target:
        return False
    for line in body.splitlines():
        m = _AUTOSCALER_NAME_LINE_RE.match(line)
        if m and m.group(1).lower() == target:
            return True
    return False


def _autoscaler_enabled_for_workload_pool() -> bool:
    """Best-effort check: is Cluster Autoscaler active on a pool that could
    host BLAST workload nodes?

    We look at the cluster-autoscaler-status ConfigMap published by the
    autoscaler add-on. The cheap probe is "the ConfigMap exists" — the
    autoscaler writes it on startup. Returning True here lets /v1/ready
    degrade a zero-Ready workload pool into ``autoscaler_pending`` instead
    of failing with ``no_workload_nodes`` (which would be a false-positive
    on autoscale-to-zero clusters).

    When ``ELB_OPENAPI_WORKLOAD_POOL_NAME`` is set, the probe additionally
    requires the autoscaler status body to declare a node group whose
    ``Name:`` field *exactly* matches that pool (case-insensitive, anchored
    on the field line — see :func:`_autoscaler_status_mentions_pool`). This
    catches the multi-pool case where the autoscaler is enabled on a
    different pool (e.g. the system pool) and would otherwise falsely
    degrade a real no-Ready-workload-nodes outage into
    ``autoscaler_pending``.

    Probe budget: when ``WORKLOAD_POOL_NAME`` is set the kubectl probe
    fetches the full ``.data.status`` body (typically a few KiB on a
    multi-pool cluster) instead of the lighter ``-o name`` ConfigMap
    existence check. Parsing is O(lines) and stays well under the
    ``READY_BUDGET_SECONDS`` (2.5 s) envelope, but the network round-trip
    is the dominant cost (~50–200 ms on a healthy cluster). Leave
    ``WORKLOAD_POOL_NAME`` unset on single-pool clusters to keep the
    cheap path.

    Errors are swallowed and treated as "no autoscaler" so a probe failure
    cannot turn a real outage into a soft warning.
    """
    if not READY_AUTOSCALER_AWARE:
        return False
    try:
        if not WORKLOAD_POOL_NAME:
            safe_exec(
                "kubectl get configmap cluster-autoscaler-status "
                "-n kube-system --request-timeout=1s -o name",
                timeout=2,
            )
            return True
        proc = safe_exec(
            "kubectl get configmap cluster-autoscaler-status "
            "-n kube-system --request-timeout=1s "
            "-o jsonpath={.data.status}",
            timeout=2,
        )
        return _autoscaler_status_mentions_pool(proc.stdout or "", WORKLOAD_POOL_NAME)
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════
# ROUTES (router definition)
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

@v1.get("/ready", tags=["System"], summary="Submit-path readiness probe")
async def ready(
    request: Request,
    x_elb_api_token: Optional[str] = Header(None, alias="X-ELB-API-Token"),
):
    """Cheap readiness signal for callers about to POST /v1/jobs.

    Three independent probes share a hard ``READY_BUDGET_SECONDS`` budget:
      * ``k8s_api``       — ``kubectl get --raw /readyz`` (API server reachable)
      * ``workload_pool`` — at least one node Ready under ``WORKLOAD_POOL_LABEL``
      * ``openapi_pod``   — ``elb-openapi`` Deployment has ``readyReplicas >= 1``

    Returns 200 with ``{"ready": true, "checks": {...}, ...}`` only when every
    probe passes. Otherwise returns 503 with ``{"ready": false, "code": ..,
    "message": ..., "checks": {...}}`` where ``code`` is one of
    ``k8s_unreachable`` / ``no_workload_nodes`` / ``workload_pool_check_failed``
    / ``openapi_pod_not_ready`` / ``openapi_pod_check_failed``. The endpoint
    deliberately does NOT call ``DefaultAzureCredential.get_token`` so AKS
    stopped → caller sees a transport timeout / 503 from the upstream proxy,
    never an opaque 30s ARM hang.

    When ``ELB_OPENAPI_AUTOSCALER_AWARE_READY=1`` (default) and the workload
    pool reports zero Ready nodes *and* the Cluster Autoscaler ConfigMap is
    present in kube-system, the probe degrades the workload_pool check to
    ``autoscaler_pending`` (still ready=True) so autoscale-to-zero pools
    don't produce false negatives.

    Subject to a per-token sliding-window rate limit
    (``ELB_OPENAPI_READY_RATE_LIMIT_PER_MINUTE``, default 30/min).
    """
    # Per-token sliding-window rate limit. Burned before any kubectl call so
    # a token holder cannot use the probe as a free polling oracle. When the
    # caller is anonymous (dev-only ELB_OPENAPI_ALLOW_UNAUTHENTICATED=1), the
    # bucket is additionally keyed on the *real* client IP so one noisy
    # laptop cannot DoS every other unauthenticated caller through the
    # shared ``anonymous`` slot. ``request.client.host`` behind the in-pod
    # nginx + Container Apps ingress is always the proxy IP, so we read
    # ``X-Forwarded-For`` (first hop) first and fall back to ``X-Real-IP``
    # then ``request.client.host`` — issue #20 P1 #2.
    if x_elb_api_token:
        token_key = x_elb_api_token
    else:
        token_key = f"anonymous:{_anonymous_client_ip(request)}"
    if not _ready_token_bucket_check(token_key):
        _ready_record_metric("rate_limited")
        return JSONResponse(
            status_code=429,
            content={
                "ready": False,
                "code": "rate_limited",
                "message": (
                    f"/v1/ready rate limit reached "
                    f"({READY_RATE_LIMIT_PER_MINUTE}/min). Retry after 60s."
                ),
                "limit_per_minute": READY_RATE_LIMIT_PER_MINUTE,
                "version": VERSION,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            headers={"Retry-After": "60"},
        )

    checks: dict[str, Any] = {}
    code: Optional[str] = None
    message: Optional[str] = None

    try:
        safe_exec("kubectl get --raw /readyz --request-timeout=1s", timeout=2)
        checks["k8s_api"] = {"status": "ok"}
    except Exception as e:
        checks["k8s_api"] = {"status": "error", "message": str(e)[:200]}
        code = "k8s_unreachable"
        message = f"K8s API server not reachable ({type(e).__name__})"

    if code is None:
        if not WORKLOAD_POOL_LABEL:
            checks["workload_pool"] = {"status": "ok", "skipped": "label_disabled"}
        else:
            try:
                proc = safe_exec(
                    f"kubectl get nodes -l {WORKLOAD_POOL_LABEL} -o json --request-timeout=1s",
                    timeout=2,
                )
                data = json.loads(proc.stdout)
                ready_nodes = sum(
                    1
                    for n in data.get("items", [])
                    if any(
                        c.get("type") == "Ready" and c.get("status") == "True"
                        for c in n.get("status", {}).get("conditions", [])
                    )
                )
                checks["workload_pool"] = {
                    "status": "ok" if ready_nodes else "error",
                    "ready_nodes": ready_nodes,
                    "label": WORKLOAD_POOL_LABEL,
                }
                if not ready_nodes:
                    # Autoscale-to-zero clusters legitimately report 0 Ready
                    # nodes between jobs. Degrade to a non-fatal status so
                    # the probe does not flap on every idle period.
                    if _autoscaler_enabled_for_workload_pool():
                        checks["workload_pool"]["status"] = "ok"
                        checks["workload_pool"]["degraded"] = "autoscaler_pending"
                        _ready_record_metric("autoscaler_pending")
                    else:
                        code = "no_workload_nodes"
                        message = (
                            f"No Ready nodes match label '{WORKLOAD_POOL_LABEL}' "
                            "and Cluster Autoscaler ConfigMap is not present"
                        )
            except Exception as e:
                checks["workload_pool"] = {"status": "error", "message": str(e)[:200]}
                code = "workload_pool_check_failed"
                message = f"Workload pool probe failed ({type(e).__name__})"

    if code is None:
        try:
            proc = safe_exec(
                "kubectl get deploy elb-openapi -o json --request-timeout=1s",
                timeout=2,
            )
            data = json.loads(proc.stdout)
            ready_replicas = int((data.get("status") or {}).get("readyReplicas") or 0)
            checks["openapi_pod"] = {
                "status": "ok" if ready_replicas else "error",
                "ready_replicas": ready_replicas,
            }
            if not ready_replicas:
                code = "openapi_pod_not_ready"
                message = "elb-openapi Deployment has zero ready replicas"
        except Exception as e:
            checks["openapi_pod"] = {"status": "error", "message": str(e)[:200]}
            code = "openapi_pod_check_failed"
            message = f"openapi pod probe failed ({type(e).__name__})"

    payload: dict[str, Any] = {
        "version": VERSION,
        "cluster_name": _ready_masked_cluster_name(),
        "checks": checks,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "budget_seconds": READY_BUDGET_SECONDS,
    }
    if code:
        _ready_record_metric(code)
        payload.update({"ready": False, "code": code, "message": message})
        return JSONResponse(status_code=503, content=payload)
    _ready_record_metric("ok")
    payload["ready"] = True
    return payload


@v1.get(
    "/ready/metrics",
    tags=["System"],
    summary="In-process counters for /v1/ready outcomes",
)
async def ready_metrics():
    """Return a snapshot of the per-outcome counter for ``/v1/ready``.

    The counters are process-local and reset on pod restart, so operators
    should scrape them on a short interval (e.g. every 60 s from a sidecar
    or a Container Apps `cron` rule) if long-term aggregation is needed.
    Authenticated via the same router-level ``require_api_token`` dep, so
    only token holders can read them — same posture as the rest of /v1.
    """
    with _READY_METRICS_LOCK:
        snapshot = dict(_READY_METRICS)
    snapshot["version"] = VERSION
    snapshot["rate_limit_per_minute"] = READY_RATE_LIMIT_PER_MINUTE
    snapshot["timestamp"] = datetime.now(timezone.utc).isoformat()
    return snapshot

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

# ── Databases ──────────────────────────────────────────────────────────────

@v1.get(
    "/databases",
    tags=["Databases"],
    summary="List prepared BLAST databases",
    response_model=DatabaseList,
)
def list_blast_databases(response: StarletteResponse):
    """List databases prepared under the workspace ``blast-db`` container.

    Each entry is a top-level prefix (e.g. ``core_nt``, ``nr``,
    ``swissprot``). Call ``GET /v1/databases/{db_name}`` for the
    molecule type, version, and sequence counts.

    The endpoint is a plain ``def`` (run in a threadpool by FastAPI)
    because the underlying Azure Blob list is synchronous. Exposing it
    as ``async def`` would let one blocking ``requests.get`` choke the
    single-worker uvicorn event loop.
    """
    try:
        names, cache_status = _list_blast_database_names()
    except FileNotFoundError:
        response.headers["X-Cache"] = "MISS"
        return DatabaseList(databases=[], count=0)
    except Exception as exc:
        raise HTTPException(503, f"Storage list failed: {str(exc)[:200]}") from exc
    response.headers["X-Cache"] = cache_status
    items = [DatabaseListItem(name=n) for n in names]
    return DatabaseList(databases=items, count=len(items))


@v1.get(
    "/databases/{db_name}",
    tags=["Databases"],
    summary="Get BLAST database metadata",
    response_model=DatabaseMetadata,
)
def get_blast_database(db_name: str, response: StarletteResponse):
    """Return the molecule type, version, and metadata for one database.

    Sync route on purpose (see :func:`list_blast_databases`).
    """
    safe = re.sub(r"[^A-Za-z0-9._-]", "", db_name)
    if not safe or safe != db_name:
        raise HTTPException(
            400,
            "db_name must contain only alphanumerics, '.', '_', '-'.",
        )
    try:
        meta, cache_status = _database_metadata(safe)
    except _MetadataFetchError as exc:
        # Transient Storage outage / auth failure / parse error.
        logger.error("metadata fetch failed for %s: %s", safe, exc)
        raise HTTPException(
            503,
            f"Storage metadata fetch failed: {exc!s:.200}",
            headers={"X-Cache": "BYPASS"},
        ) from exc
    if not meta:
        raise HTTPException(
            404,
            f"Database {db_name!r} not found or metadata unavailable.",
            headers={"X-Cache": cache_status},
        )
    response.headers["X-Cache"] = cache_status
    return meta

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
        with _azcopy_slots:
            safe_exec(["azcopy", "cp", local, url], timeout=60)
    finally:
        _cleanup_tmp(local)
    return url


def _discover_elb_job_id_from_submit_output(job_id: str, stdout: str) -> str:
    if not stdout:
        return ""
    patterns = (
        rf"/results/(?:\d{{4}}/\d{{2}}/\d{{2}}/)?{re.escape(job_id)}/(?P<elb_job_id>job-[A-Za-z0-9_-]+)/metadata/",
        r"\b(?P<elb_job_id>job-[0-9a-f]{32})\b",
    )
    for pattern in patterns:
        match = re.search(pattern, stdout)
        if match:
            return match.group("elb_job_id")
    return ""


def _effective_elb_job_id(job_info: dict[str, Any]) -> str:
    job_id = str(job_info.get("job_id") or "")
    current = str(job_info.get("elb_job_id") or "")
    if current.startswith("job-"):
        return current
    discovered = _discover_elb_job_id_from_submit_output(
        job_id,
        "\n".join(
            str(job_info.get(key) or "")
            for key in ("stdout_tail", "stderr_tail")
        ),
    )
    if discovered:
        _update_job(job_id, elb_job_id=discovered)
        return discovered
    return current or job_id


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
        # Cross-path submit coordination (Gate A Lease + Gate B ceiling). Shared
        # with the dashboard control plane so both paths honour the same
        # concurrency ceiling. No-op (returns None) unless BLAST_COORD_BACKEND=k8s.
        run_slot = _coord.acquire_run_slot(job_id, stop_event=cancel_event)
        # When coordination holds Gate A, hard-cap the submit subprocess BELOW
        # the Lease TTL so an overrunning submit is killed (failed) before the
        # Lease can be reclaimed by another path — mirrors the dashboard's
        # submit_exec_timeout < lease_ttl invariant. The disabled path keeps the
        # legacy unbounded timeout.
        submit_timeout = (
            _coord.submit_exec_timeout_seconds() if run_slot is not None else None
        )
        try:
            result = run_cancellable(
                ["elastic-blast", "submit", "--cfg", cfg_path],
                timeout=submit_timeout,
                stop_event=cancel_event,
            )
        finally:
            # Release Gate A as soon as the submit critical section is done; the
            # job's own finalizer now counts toward Gate B for everyone else.
            _coord.release_run_slot(run_slot)
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
            elb_job_id=(
                payload.get("correlation_id")
                or _discover_elb_job_id_from_submit_output(job_id, result.stdout or "")
                or job_id
            ),
            submit_result=payload,
            stdout_tail=(result.stdout or "")[-2000:],
            stderr_tail=(result.stderr or "")[-2000:],
            last_progress_at=_now_iso(),
        )
        _webhook_notify(job_id, {"event": "submitted", "status": status})
        logger.info("Job %s submitted", job_id)
    except _coord.SubmitSlotBusyTimeout as e:
        # Lease busy / cluster at the concurrency ceiling past the wait budget.
        # This is NOT a failure: requeue so the dispatcher retries on a later
        # tick. The watchdog bounds total queued lifetime.
        _update_job(
            job_id,
            status="queued",
            phase="waiting_for_capacity",
            last_progress_at=_now_iso(),
        )
        logger.info("Job %s requeued (capacity wait): %s", job_id, str(e)[:200])
    except Exception as e:
        if cancel_event.is_set():
            _update_job(job_id, status="cancelled", phase="cancelled", error=str(e)[:500])
            _webhook_notify(job_id, {"event": "cancelled", "error": str(e)[:200]})
            logger.warning("Job %s cancelled: %s", job_id, str(e)[:200])
        else:
            _update_job(job_id, status="failed", phase="submit_failed", error=str(e)[:8000])
            _webhook_notify(job_id, {"event": "failed", "error": str(e)[:200]})
            logger.error("Job %s failed: %s", job_id, str(e)[:200])
    finally:
        _dispatcher_once()


def _job_marker_phase(results_url: str, elb_job_id: str = "") -> str | None:
    if not results_url:
        return None
    base = results_url.rstrip("/")
    candidates = [f"{base}/metadata/"]
    if elb_job_id.startswith("job-"):
        candidates.insert(0, f"{base}/{elb_job_id}/metadata/")
    for marker_url in candidates:
        try:
            _azcopy_login()
            proc = safe_exec(["azcopy", "ls", marker_url], timeout=10)
        except Exception:
            continue
        if "SUCCESS.txt" in proc.stdout:
            return "completed"
        if "FAILURE.txt" in proc.stdout:
            return "failed"
    return None


def _k8s_job_summary(elb_job_id: str) -> dict[str, Any]:
    # ``failed`` / ``submit_failed`` count *pod* retry attempts: Kubernetes
    # increments a Job's ``.status.failed`` on every failed pod, including
    # transient failures it is still retrying within ``backoffLimit``. Using
    # those raw counts to declare the run dead misreports an in-flight retry as
    # ``blast_failed``. The ``*_terminal`` counters below only fire when the Job
    # itself carries a ``Failed`` condition (``backoffLimit`` exhausted — the
    # Job can no longer succeed), so phase decisions can distinguish "a pod
    # failed once and is being retried" from "the Job has permanently failed".
    empty = {
        "total": 0,
        "succeeded": 0,
        "failed": 0,
        "active": 0,
        "submit_failed": 0,
        "finalizer_active": 0,
        "failed_terminal": 0,
        "submit_failed_terminal": 0,
    }
    try:
        proc = safe_exec(["kubectl", "get", "jobs", "-l", f"elb-job-id={elb_job_id}", "-o", "json"], timeout=15)
        data = json.loads(proc.stdout)
    except Exception as exc:
        return {**empty, "error": str(exc)[:200]}

    items = data.get("items", [])
    summary = dict(empty)
    for item in items:
        labels = item.get("metadata", {}).get("labels", {})
        app_label = labels.get("app", "")
        status = item.get("status", {})
        conditions = {
            c.get("type"): c.get("status")
            for c in status.get("conditions", [])
            if isinstance(c, dict)
        }
        job_failed_terminal = conditions.get("Failed") == "True"
        if app_label == "blast":
            summary["total"] += 1
            summary["succeeded"] += status.get("succeeded", 0) or 0
            summary["failed"] += status.get("failed", 0) or 0
            summary["active"] += status.get("active", 0) or 0
            if job_failed_terminal:
                summary["failed_terminal"] += 1
        elif app_label == "submit":
            summary["submit_failed"] += status.get("failed", 0) or 0
            if job_failed_terminal:
                summary["submit_failed_terminal"] += 1
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


def _notify_terminal_transition(job_id: str, updates: dict[str, Any]) -> None:
    """Best-effort webhook on natural running -> terminal flips.

    ``_refresh_job_status`` only reaches this point after the early-out for
    already-terminal/queued jobs, so any ``status`` in ``_TERMINAL_STATES``
    sitting in ``updates`` is a true running->terminal transition that the
    dashboard would otherwise only learn about via its own poll cadence. The
    submit/cancel paths already notify on their own terminal events; this
    closes the gap for the most common case (natural completion) and for
    K8s-derived failure detection (``stuck_reason`` is handled by
    ``_cancel_job`` which already notifies). Failures here are swallowed --
    the webhook is best-effort and must never block status persistence.
    """
    new_status = str(updates.get("status") or "")
    if new_status not in _TERMINAL_STATES:
        return
    payload: dict[str, Any] = {"event": new_status, "status": new_status}
    error = updates.get("error")
    if error:
        payload["error"] = str(error)[:200]
    # Attach the same derived runtime fields the /v1/jobs LIST surfaces so the
    # dashboard's sibling-stats cache can be populated on the webhook fast path
    # (instead of waiting for the next /v1/jobs sync tick to discover them).
    # Mirrors the _runtime_block helper inside list_jobs (~L3061). Best-effort:
    # a missing job snapshot must never block the webhook.
    try:
        with _jobs_lock:
            job_snap = dict(_jobs.get(job_id, {}))
        if job_snap:
            merged = {**job_snap, **updates}
            started_at = merged.get("started_at") or ""
            terminal_at = (
                merged.get("completed_at")
                or merged.get("failed_at")
                or merged.get("updated_at")
            )
            payload["started_at"] = started_at
            payload["elapsed_seconds"] = _duration_seconds(
                merged.get("created_at"), terminal_at
            )
            payload["queue_wait_seconds"] = (
                _duration_seconds(merged.get("queued_at"), started_at)
                if started_at
                else None
            )
            payload["run_seconds"] = (
                _duration_seconds(started_at, terminal_at) if started_at else None
            )
    except Exception:
        # Stats are a nice-to-have on the webhook; the dashboard will fall back
        # to the next list-sync to populate them if anything goes sideways here.
        pass
    try:
        _webhook_notify(job_id, payload)
    except Exception:
        pass


def _snapshot_k8s_summary_for_terminal(
    job: dict[str, Any], elb_job_id: str
) -> dict[str, Any] | None:
    """Return a fresh k8s job summary to attach to a terminal-flip update.

    The marker-driven completion paths used to skip the kubectl call entirely,
    so a job that flipped to ``completed`` via the success marker landed in
    the dashboard with ``execution.shard_count = 0`` and
    ``shards_succeeded = 0`` (issue #18) because the execution counters in
    ``_external_job_payload`` are populated from ``k8s_summary``. Snapshot it
    one last time on the terminal transition so the recorded summary reflects
    the final fan-out. Returns ``None`` if the kubectl call failed AND the
    existing summary is usable -- never overwrite a real summary with an
    error stub.
    """
    snapshot = _k8s_job_summary(elb_job_id)
    if snapshot.get("error"):
        existing = job.get("k8s_summary") or {}
        if isinstance(existing, dict) and existing.get("total"):
            return None
    return snapshot


def _refresh_job_status(job_id: str) -> dict[str, Any] | None:
    with _jobs_lock:
        job = dict(_jobs.get(job_id, {}))
    if not job:
        return None
    if job.get("status") in _TERMINAL_STATES or job.get("status") == "queued":
        return job

    elb_job_id = _effective_elb_job_id(job)
    marker = _job_marker_phase(job.get("results", ""), elb_job_id)
    if marker == "failed":
        updates: dict[str, Any] = {
            "status": "failed",
            "phase": "failed",
            "last_progress_at": _now_iso(),
        }
        summary_snapshot = _snapshot_k8s_summary_for_terminal(job, elb_job_id)
        if summary_snapshot is not None:
            updates["k8s_summary"] = summary_snapshot
        result = _update_job(job_id, **updates)
        _notify_terminal_transition(job_id, updates)
        return result
    if marker == "completed":
        # The finalizer uploads every result artifact (shard ``batch_*.out.gz``
        # and, in DB-partitioned runs, ``merged_results.out.gz``) *before* it
        # writes the ``metadata/SUCCESS.txt`` marker. So when the marker is
        # visible the artifacts are already durably stored — but the azcopy
        # List that ``GET /jobs/{job_id}/results`` relies on can briefly lag
        # behind the marker write. Gate ``completed`` on the same result
        # listing the download path uses (mirroring the Kubernetes-summary
        # branch below) so status never flips to ``completed`` while
        # ``/results`` would still 404.
        if _list_result_files(job):
            updates = {
                "status": "completed",
                "phase": "completed",
                "completed_at": _now_iso(),
                "last_progress_at": _now_iso(),
            }
            # Snapshot the final K8s fan-out so the dashboard's
            # ``execution.shard_count`` / ``shards_succeeded`` projection has
            # the real numbers instead of the stale (often empty) value left
            # over from the last polling pass before the marker landed.
            summary_snapshot = _snapshot_k8s_summary_for_terminal(job, elb_job_id)
            if summary_snapshot is not None:
                updates["k8s_summary"] = summary_snapshot
            result = _update_job(job_id, **updates)
            _notify_terminal_transition(job_id, updates)
            return result
        # Marker present but the listing has not caught up yet. Hold at
        # ``finalizing`` and re-check on the next poll; this self-heals within
        # seconds. Bound the hold by RESULTS_VISIBILITY_GRACE_SECONDS so a
        # listing that never catches up cannot wedge a SUCCESS-marked job in a
        # non-terminal state forever — past the grace window we trust the
        # marker (the artifacts are durably written per the finalizer contract).
        seen_at = job.get("success_marker_seen_at") or _now_iso()
        if _age_seconds(seen_at) > RESULTS_VISIBILITY_GRACE_SECONDS:
            updates = {
                "status": "completed",
                "phase": "completed",
                "completed_at": _now_iso(),
                "last_progress_at": _now_iso(),
            }
            summary_snapshot = _snapshot_k8s_summary_for_terminal(job, elb_job_id)
            if summary_snapshot is not None:
                updates["k8s_summary"] = summary_snapshot
            result = _update_job(job_id, **updates)
            _notify_terminal_transition(job_id, updates)
            return result
        return _update_job(
            job_id,
            status="running",
            phase="finalizing",
            success_marker_seen_at=seen_at,
            last_progress_at=_now_iso(),
        )

    elb_job_id = _effective_elb_job_id(job)
    summary = _k8s_job_summary(elb_job_id)
    stuck_reason = _k8s_pod_stuck_reason(elb_job_id)
    previous_summary = job.get("k8s_summary")
    updates = {"k8s_summary": summary}
    if summary != previous_summary:
        updates["last_progress_at"] = _now_iso()

    if stuck_reason:
        updates.update({"status": "failed", "phase": "stuck_cancelled", "error": stuck_reason})
        refreshed = _update_job(job_id, **updates)
        # ``_cancel_job`` already fires the failure webhook -- do not double-notify.
        _cancel_job(job_id, stuck_reason, terminal_status="failed")
        return refreshed
    if summary.get("submit_failed_terminal"):
        updates.update({"status": "failed", "phase": "submit_failed", "error": "submit job failed before creating BLAST jobs"})
    elif summary.get("failed_terminal"):
        updates.update({"status": "failed", "phase": "blast_failed", "error": "one or more BLAST jobs failed"})
    elif summary.get("total", 0) > 0:
        if summary.get("succeeded", 0) >= summary.get("total", 0) and summary.get("total", 0) > 0:
            if _list_result_files(job):
                updates.update({"status": "completed", "phase": "completed", "completed_at": _now_iso()})
            else:
                updates.update({"status": "running", "phase": "finalizing"})
        elif summary.get("active", 0) > 0:
            # A pod that failed transiently (counted in ``failed``) but is still
            # being retried within ``backoffLimit`` keeps ``active`` > 0. Treat
            # the job as running, not failed — only a terminal ``Failed`` Job
            # condition (``failed_terminal`` above) flips to ``blast_failed``.
            updates.update({"status": "running", "phase": "running"})
        else:
            # No pod is active and not every shard has succeeded yet, but no Job
            # carries a terminal ``Failed`` condition — Kubernetes is between
            # retry attempts (a transient pod failure is bumping ``failed``).
            # Hold at ``pending`` and re-poll instead of declaring the run dead.
            updates.update({"status": "running", "phase": "pending"})
    else:
        if _list_result_files(job):
            updates.update({"status": "completed", "phase": "completed", "completed_at": _now_iso()})
        else:
            updates.update({"phase": "submitting"})
    result = _update_job(job_id, **updates)
    _notify_terminal_transition(job_id, updates)
    return result


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


def _reclaim_dead_thread_job(job_id: str, refreshed: dict[str, Any]) -> bool:
    """Reclaim a dispatching/submitting job whose submit thread is dead (#62).

    A pod restart (e.g. the AKS cluster was stopped mid-submit) loses every
    in-process submit thread, so a recovered job can sit in ``submitting``
    forever, permanently holding one of the ``MAX_ACTIVE_SUBMISSIONS`` dispatch
    slots and wedging the dispatcher (throughput -> 0). The watchdog calls this
    every tick so the slot is reclaimed within ``WATCHDOG_INTERVAL_SECONDS``
    instead of after ``SUBMIT_STUCK_SECONDS`` (2h).

    Returns True when the slot was released (the job was requeued or failed),
    False when the job is left untouched because its submit already created
    BLAST k8s work -- re-submitting would duplicate jobs, so the normal status
    refresh is left to carry it to running/terminal.

    Bounded by ``SUBMIT_MAX_RETRIES``: a job that keeps losing its thread (the
    cluster is still flaky and stops again) is requeued at most that many times,
    then failed, so it cannot re-stick the dispatcher indefinitely. Mirrors the
    startup-only :func:`_reconcile_recovered_jobs` requeue, adding the retry
    bound and continuous (every-tick) operation.
    """
    summary = refreshed.get("k8s_summary") or {}
    if summary.get("total") or summary.get("submit_failed"):
        return False
    attempt = int(refreshed.get("attempt", 0) or 0)
    if attempt < SUBMIT_MAX_RETRIES:
        _update_job(
            job_id,
            status="queued",
            phase="recovered",
            queued_at=_now_iso(),
            last_progress_at=_now_iso(),
            error="",
        )
        logger.info(
            "watchdog reclaimed dead-thread job %s -> queued (attempt %d/%d)",
            job_id, attempt, SUBMIT_MAX_RETRIES,
        )
    else:
        _cancel_job(
            job_id,
            f"submit thread died with no BLAST jobs after {SUBMIT_MAX_RETRIES} attempts",
            terminal_status="failed",
        )
        logger.error(
            "watchdog failed dead-thread job %s after %d attempts (slot released)",
            job_id, SUBMIT_MAX_RETRIES,
        )
    return True


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
        # #62: a dispatching/submitting job whose submit thread died (pod restart
        # lost the in-process thread) holds a MAX_ACTIVE slot. Reclaim it within
        # one tick rather than waiting SUBMIT_STUCK_SECONDS (2h). The started_at
        # grace avoids racing a job that was just claimed but whose thread has
        # not started yet; an alive thread (a legitimately cold-staging submit)
        # is never touched.
        if (
            status in {"dispatching", "submitting"}
            and not _has_alive_thread(job_id)
            and _age_seconds(refreshed.get("started_at")) > RECLAIM_GRACE_SECONDS
        ):
            if _reclaim_dead_thread_job(job_id, refreshed):
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
    _pt = job_info.get("passthrough")
    if isinstance(_pt, dict) and _pt:
        payload["passthrough"] = _pt
    summary = job_info.get("k8s_summary") if isinstance(job_info.get("k8s_summary"), dict) else {}
    effective_elb_job_id = _effective_elb_job_id(job_info)
    if effective_elb_job_id.startswith("job-") and job_info.get("elb_job_id") != effective_elb_job_id:
        fresh_summary = _k8s_job_summary(effective_elb_job_id)
        updated = _update_job(
            job_info["job_id"],
            elb_job_id=effective_elb_job_id,
            k8s_summary=fresh_summary,
            last_progress_at=_now_iso(),
        )
        if updated:
            job_info = updated
        summary = fresh_summary
    # Expose the elastic-blast job id (the ``job-<hash>`` stamped on the
    # in-cluster k8s objects via the ``elb-job-id`` label / ``BLAST_ELB_JOB_ID``
    # env). The dashboard only knows the OpenAPI ``job_id`` and cannot otherwise
    # map an external job to its BLAST pods, so without this it can render the
    # execution-step timeline but never stream the raw pod logs. Emit only a
    # GENUINE discovered id: ``_effective_elb_job_id`` falls back to the OpenAPI
    # ``job_id`` when none has been discovered yet, so guard on it differing from
    # ``job_id`` to avoid handing the dashboard a non-existent pod selector.
    if effective_elb_job_id.startswith("job-") and effective_elb_job_id != str(
        job_info.get("job_id") or ""
    ):
        payload["elb_job_id"] = effective_elb_job_id
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
        if _eta is not None and _eta.enabled():
            with _jobs_lock:
                _eta_jobs = [dict(v) for v in _jobs.values()]
            _eta_out = _eta.compute_eta(job_info, _eta_jobs, MAX_ACTIVE_SUBMISSIONS)
            if _eta_out:
                payload["eta"] = _eta_out
    elif public_status == "running":
        payload["progress_pct"] = _progress_pct(job_info)
        if _eta is not None and _eta.enabled():
            with _jobs_lock:
                _eta_jobs = [dict(v) for v in _jobs.values()]
            _eta_out = _eta.compute_eta(job_info, _eta_jobs, MAX_ACTIVE_SUBMISSIONS)
            if _eta_out:
                payload["eta"] = _eta_out
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


@lru_cache(maxsize=None)
def _elb_recognises_cluster_param(param_name: str) -> bool:
    """Return True when the bundled elastic-blast accepts a ``[cluster]`` config param.

    During a rolling OpenAPI image rebuild this app code (COPYed into the image)
    can begin emitting a new optional ``[cluster]`` optimisation hint before the
    bundled elastic-blast (pinned by the Dockerfile ``ELB_REF`` git clone) is
    advanced to a revision that recognises it. elastic-blast's config validator
    (``ElasticBlastConfig._validate_config_parser``) rejects ANY parameter not
    present in its dataclass mappings with ``Unrecognized configuration
    parameter "<name>" in section "cluster"`` — turning an optional hint into a
    hard submit failure.

    Mirror elastic-blast's own mapping (the same source the validator scans) so
    we only write a param it will accept. On any import/introspection failure,
    fail CLOSED (treat as unsupported) so a partially-installed or version-skewed
    runtime can never hard-fail submit over an optional hint.
    """
    try:
        from elastic_blast.constants import CFG_CLUSTER
        from elastic_blast.elb_config import (
            AZUREConfig,
            BlastConfig,
            ClusterConfig,
            TimeoutsConfig,
        )
    except Exception:
        return False
    for cls in (AZUREConfig, BlastConfig, ClusterConfig, TimeoutsConfig):
        for info in getattr(cls, "mapping", {}).values():
            if (
                info is not None
                and info.section == CFG_CLUSTER
                and info.param_name == param_name
            ):
                return True
    return False


_RESULTS_PREFIX_RE = re.compile(r"^\d{4}/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/$")


def _validate_results_prefix(value: Optional[str]) -> str:
    """Return a safe date-tiered results sub-prefix (``YYYY/MM/DD/``) or ``''``.

    The dashboard's date-tiered storage layout (elb-dashboard
    ``STORAGE_DATE_LAYOUT_ENABLED``) asks the sibling to write a Mode B job's
    results under ``results/<YYYY/MM/DD>/<job_id>/`` instead of the flat
    ``results/<job_id>/``. Only an exact ``YYYY/MM/DD/`` shape with a real month
    (01-12) and day (01-31) is accepted so a hostile / malformed value can never
    inject ``..`` traversal, an absolute path, extra segments, or a nonsensical
    bucket (e.g. ``9999/99/99``) that redirect / scatter writes. Empty / missing
    keeps the legacy flat layout, so old callers are unaffected.
    """
    if not value:
        return ""
    v = str(value).strip().strip("/")
    if not v:
        return ""
    v = f"{v}/"
    if not _RESULTS_PREFIX_RE.match(v):
        # Log the rejection so a misbehaving caller is diagnosable; the value is
        # caller-supplied and bounded, but truncate defensively before logging.
        logger.warning("rejected results_prefix (not a YYYY/MM/DD/ date path): %r", str(value)[:64])
        raise HTTPException(400, "results_prefix must be an exact YYYY/MM/DD/ date path")
    return v


@v1.post("/jobs", tags=["Jobs"], status_code=202, summary="Submit a BLAST search",
          openapi_extra={"requestBody": {"content": {"application/json": {"examples": {
              "mode_a": _MODE_A_EXAMPLE, "mode_b": _MODE_B_EXAMPLE, "mode_b_taxid": _MODE_B_TAXID_EXAMPLE,
          }}}}})
# Deliberately a plain ``def`` (run in FastAPI's threadpool), NOT ``async def``:
# the body does blocking I/O (FASTA upload via azcopy, ConfigMap persistence in
# ``_save_job``, DB-version metadata fetch) with ZERO awaits. As an async handler
# every submit blocked the single asyncio event loop for its whole duration,
# which serialised the submit path (~9s/job observed) and — under a concurrent
# 50-submit burst — starved the ``/healthz`` readiness probe so the pod flapped
# to NotReady (dashboard issue #54). Running in the threadpool lets concurrent
# submits proceed in parallel and keeps the event loop free for probes; the
# server-side admit cap (ELB_OPENAPI_MAX_ACTIVE_SUBMISSIONS) still bounds how
# many jobs actually dispatch. Mirrors the same rationale already applied to the
# ``/jobs/{id}/status`` handler.
def submit_job(req: JobSubmitRequest, x_elb_internal_token: Optional[str] = Header(None, alias="X-ELB-Internal-Token")):
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
    # Caller-supplied fields beyond the schema (e.g. request_id) — preserved so
    # they can be echoed on status/result for correlation. Bounded for safety.
    passthrough = _sanitize_passthrough(getattr(req, "model_extra", None))

    if req.idempotency_key:
        job_id = _job_id_from_idempotency_key(f"{submission_source}:{req.idempotency_key}")
        with _jobs_lock:
            existing = _jobs.get(job_id)
        if existing:
            replay: dict[str, Any] = {
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
            # Echo the ORIGINAL job's pass-through (a replay must return the
            # same handle, not the replay caller's fields).
            existing_passthrough = existing.get("passthrough")
            if isinstance(existing_passthrough, dict) and existing_passthrough:
                replay["passthrough"] = existing_passthrough
            return replay
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
        # Date-tiered results layout (dashboard STORAGE_DATE_LAYOUT_ENABLED):
        # when the caller forwards a ``YYYY/MM/DD/`` prefix, write results under
        # results/<prefix><job_id>/ so the blob layout matches the dashboard's
        # native date tiering. Empty prefix => legacy flat results/<job_id>/.
        _results_prefix = _validate_results_prefix(getattr(req, "results_prefix", None))
        results_url = f"{_blob_base()}/results/{_results_prefix}{job_id}"
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
    # Dashboard policy: OpenAPI submissions use AKS node-local SSD,
    # not the historical shared PV/PVC path.
    config["cluster"]["exp-use-local-ssd"] = "true"
    config["cluster"]["reuse"] = "true"
    # Skip re-staging warmed DB shards onto node-local SSD when the dashboard
    # already ran an explicit warmup (app=elb-db-warmup) for this DB on the
    # cluster. kubernetes.py:_dashboard_warmup_jobs_ready verifies every expected
    # shard has a succeeded warmup job (and falls back to init-ssd otherwise), so
    # this is safe to leave on unconditionally; the env lever is an escape hatch
    # — set ELB_OPENAPI_SKIP_WARMED_SSD_INIT=0 to force the historical init-ssd
    # staging path on every submit.
    #
    # Only emit the hint when the bundled elastic-blast actually recognises it:
    # during a rolling rebuild this app code can run ahead of the pinned
    # elastic-blast (ELB_REF), and writing an unrecognised param hard-fails the
    # whole submit with "Unrecognized configuration parameter". See
    # _elb_recognises_cluster_param.
    if os.environ.get("ELB_OPENAPI_SKIP_WARMED_SSD_INIT", "1").strip().lower() not in {"0", "false", "no"}:
        if _elb_recognises_cluster_param("exp-skip-warmed-ssd-init"):
            config["cluster"]["exp-skip-warmed-ssd-init"] = "true"
        else:
            logger.warning(
                "bundled elastic-blast does not recognise "
                "'exp-skip-warmed-ssd-init'; omitting the optional warmed-SSD-"
                "init skip hint to avoid a hard submit failure (version skew — "
                "rebuild elb-openapi with a newer ELB_REF to restore it)."
            )
    config["blast"]["db"] = db_url
    config["blast"]["queries"] = queries_url
    config["blast"]["results"] = results_url
    config["blast"]["options"] = opts
    if req.batch_len is not None:
        config["blast"]["batch-len"] = str(req.batch_len)

    # Dashboard concurrency lever (default-OFF): ELB_OPENAPI_NUM_CPUS pins the
    # elastic-blast [cluster] num-cpus. elastic-blast derives the shard pod CPU
    # limit (= num-cpus) and request (= num-cpus - 2) from it, so lowering this
    # raises how many shard pods co-schedule per node (request is the binding
    # constraint). Unset => elastic-blast keeps its profile default
    # (threads_per_pod, currently 8 -> request 6 -> 2 jobs/node), i.e. unchanged
    # behaviour. Search space / sharding / num-nodes are untouched, so NCBI
    # parity (-searchsp) is independent of this knob.
    _elb_num_cpus = os.environ.get("ELB_OPENAPI_NUM_CPUS", "").strip()
    if _elb_num_cpus:
        try:
            _elb_num_cpus_val = int(_elb_num_cpus)
        except ValueError:
            _elb_num_cpus_val = 0
        if _elb_num_cpus_val >= 1:
            config["cluster"]["num-cpus"] = str(_elb_num_cpus_val)

    db_name = _db_name_from_value(req.db)
    profile = str(req.resource_profile or "").strip().lower()
    if db_name == "core_nt" and profile in {"core_nt_precise", "precise", "core_nt_safe"}:
        partitions = max(1, min(NUM_NODES, 10))
        config["blast"]["db-partitions"] = str(partitions)
        config["blast"]["db-partition-prefix"] = (
            f"{_blob_base()}/blast-db/{partitions}shards/core_nt_shard_"
        )
        if "-searchsp" not in opts and "-dbsize" not in opts:
            config["blast"]["options"] = f"{opts} -searchsp 32156241807668"

    from io import StringIO
    config_buf = StringIO()
    config.write(config_buf)
    config_text = config_buf.getvalue()

    blast_version = _blast_version_detail()
    db_version = _db_version_detail(db_name)
    job_data = {
        "job_id": job_id, "status": "queued", "mode": "B" if is_b else "A",
        "query_seqs": (_eta.parse_query_features(req.query_fasta)[0] if (_eta is not None and _eta.enabled() and is_b) else 0),
        "query_bases": (_eta.parse_query_features(req.query_fasta)[1] if (_eta is not None and _eta.enabled() and is_b) else 0),
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
    if passthrough:
        job_data["passthrough"] = passthrough
    _save_job(job_id, job_data, require_persist=True)

    dispatched = _dispatcher_once()
    logger.info("Job %s queued (mode %s priority=%s dispatched=%s)", job_id, "B" if is_b else "A", req.priority, dispatched)
    position = _queued_position(job_id)
    with _jobs_lock:
        current_status = _jobs.get(job_id, {}).get("status")
    status = current_status or ("dispatching" if dispatched else "queued")
    response: dict[str, Any] = {
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
    if passthrough:
        response["passthrough"] = passthrough
    return response

# ── Jobs — List ────────────────────────────────────────────────────────────

@v1.get("/jobs", tags=["Jobs"], summary="List all jobs")
async def list_jobs(
    limit: Optional[int] = Query(
        None, ge=1, le=500,
        description="Max jobs to return, most-recent first. Omit for the full list.",
    ),
    cursor: Optional[str] = Query(
        None, description="Opaque pagination cursor from a previous response's next_cursor.",
    ),
):
    """List tracked BLAST jobs. State is persisted in K8s ConfigMaps.

    Without ``limit`` the full list is returned in insertion order (unchanged
    legacy behaviour). With ``limit`` the jobs are ordered most-recent first and
    a ``next_cursor`` is returned for stable keyset pagination; an unparseable
    ``cursor`` is ignored (degrades to the first page).
    """
    _ensure_loaded()
    with _jobs_lock:
        items = list(_jobs.items())
    # The list view feeds the dashboard's BlastJobs page; exposing started_at
    # (and the derived elapsed/run/queue-wait seconds) lets the row's
    # "Elapsed" / "Duration" timer skip the queue-wait portion instead of
    # counting wall-clock from `created_at`. Mirrors the same computation the
    # /v1/jobs/{id}/status detail handler does (see L2677-L2685) so list +
    # detail agree byte-for-byte; the helper inlines it because we only need
    # the three derived ints, not the full status payload.
    def _runtime_block(i: dict[str, Any]) -> dict[str, Any]:
        status = i.get("status", "")
        terminal_at = (
            i.get("completed_at")
            or i.get("failed_at")
            or i.get("updated_at")
        )
        elapsed_end = terminal_at if status in _TERMINAL_STATES else None
        # `_duration_seconds(start, end=<falsy>)` falls back to `time.time()`,
        # which silently turns an empty `started_at` into a wall-clock since
        # `queued_at` — exactly the bug this whole block is meant to fix. Gate
        # the queue-wait + run helpers on a populated `started_at` so a still-
        # queued row reports None (the dashboard then keeps the "Queued for"
        # timer counting against `created_at`).
        started_at = i.get("started_at") or ""
        return {
            "started_at": started_at,
            "updated_at": i.get("updated_at", ""),
            "elapsed_seconds": _duration_seconds(i.get("created_at"), elapsed_end),
            "queue_wait_seconds": (
                _duration_seconds(i.get("queued_at"), started_at)
                if started_at
                else None
            ),
            "run_seconds": (
                _duration_seconds(started_at, elapsed_end) if started_at else None
            ),
        }
    summaries = [
        {"job_id": jid, "status": i["status"], "mode": i.get("mode", "A"),
         "created_at": i.get("created_at", ""), "program": i.get("program", ""),
         "cluster_name": i.get("cluster_name", ""), "db": i.get("db", ""),
         "elb_job_id": i.get("elb_job_id", ""),
         "priority": i.get("priority", _PRIORITY_LABELS["normal"]),
         "queue_position": _queued_position(jid),
         **_runtime_block(i)}
        for jid, i in items
    ]
    total = len(summaries)
    if limit is None:
        # Legacy unpaginated response (backward compatible). next_cursor /
        # has_more are additive so existing parsers are unaffected.
        return {"jobs": summaries, "count": total, "next_cursor": None, "has_more": False}
    # Most-recent first; tie-break on job_id so the (created_at, job_id) keyset
    # cursor is a total order with no page overlap or gaps.
    summaries.sort(key=lambda s: (s["created_at"], s["job_id"]), reverse=True)
    if cursor:
        decoded = _decode_jobs_cursor(cursor)
        if decoded is not None:
            summaries = [
                s for s in summaries if (s["created_at"], s["job_id"]) < decoded
            ]
    page = summaries[:limit]
    has_more = len(summaries) > limit
    next_cursor = (
        _encode_jobs_cursor(page[-1]["created_at"], page[-1]["job_id"])
        if has_more and page else None
    )
    return {"jobs": page, "count": total, "next_cursor": next_cursor, "has_more": has_more}

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

    _status_payload: dict[str, Any] = {
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
    if _eta is not None and _eta.enabled() and job_info.get("status") in {"queued", "dispatching", "submitting", "running"}:
        with _jobs_lock:
            _eta_jobs = [dict(v) for v in _jobs.values()]
        _eta_out = _eta.compute_eta(job_info, _eta_jobs, MAX_ACTIVE_SUBMISSIONS)
        if _eta_out:
            _status_payload["eta"] = _eta_out
    return _status_payload
    _pt = job_info.get("passthrough")
    if isinstance(_pt, dict) and _pt:
        _status_payload["passthrough"] = _pt
    if _eta is not None and _eta.enabled() and job_info.get("status") in {"queued", "dispatching", "submitting", "running"}:
        with _jobs_lock:
            _eta_jobs = [dict(v) for v in _jobs.values()]
        _eta_out = _eta.compute_eta(job_info, _eta_jobs, MAX_ACTIVE_SUBMISSIONS)
        if _eta_out:
            _status_payload["eta"] = _eta_out
    return _status_payload

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
# Plain ``def`` for the same reason as ``submit_job`` (its only delegate): keep
# the blocking submit path off the asyncio event loop so a concurrent burst does
# not serialise or starve readiness (issue #54).
def external_submit(req: ExternalSubmitRequest) -> dict[str, Any]:
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
    response = submit_job(internal)
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
