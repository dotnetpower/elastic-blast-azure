"""ElasticBLAST on Azure — OpenAPI Server v3.1.

Runs inside AKS as a pod. Self-contained: stores job state in K8s ConfigMaps,
optionally forwards events to Control Plane via webhook.
"""

from __future__ import annotations

import configparser
import glob
import json
import logging
import os
import re
import shutil
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, APIRouter
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask
from util import safe_exec

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("elb-openapi")

# ── Configuration (env var → fallback) ─────────────────────────────────────
CLUSTER_NAME = os.environ.get("ELB_CLUSTER_NAME", "elastic-blast-on-azure")
STORAGE_ACCOUNT = os.environ.get("ELB_STORAGE_ACCOUNT", "stgelb")
RESOURCE_GROUP = os.environ.get("ELB_RESOURCE_GROUP", "rg-elb")
AZURE_REGION = os.environ.get("ELB_AZURE_REGION", "koreacentral")
MACHINE_TYPE = os.environ.get("ELB_MACHINE_TYPE", "Standard_E16s_v5")
NUM_NODES = int(os.environ.get("ELB_NUM_NODES", "3"))
CONTROL_PLANE_URL = os.environ.get("CONTROL_PLANE_URL", "")  # optional webhook
VERSION = "3.3.0"

_BLOB_URL_RE = re.compile(r"^https://[a-z0-9]+\.blob\.core\.windows\.net/[a-z0-9][-a-z0-9]*/.*$")
_VALID_PROGRAMS = frozenset({"blastp", "blastn", "blastx", "psiblast", "rpsblast", "rpstblastn", "tblastn", "tblastx"})
_CM_LABEL = "elb-job=true"
_CM_PREFIX = "elb-job-"


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
        logger.info("Workload Identity env vars not set, skipping az login")
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

# CORS — Control Plane SPA origin. CONTROL_PLANE_URL takes priority;
# fall back to wildcard only in development. Production deployments should
# always set CONTROL_PLANE_URL.
_cors_origins: list[str] = ["*"]
if CONTROL_PLANE_URL:
    from urllib.parse import urlparse
    _parsed = urlparse(CONTROL_PLANE_URL)
    _origin = f"{_parsed.scheme}://{_parsed.netloc}"
    _cors_origins = [_origin]

from starlette.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

# ── ConfigMap-based job storage ────────────────────────────────────────────

def _cm_name(job_id: str) -> str:
    return f"{_CM_PREFIX}{job_id}"

def _save_job_cm(job_id: str, data: dict[str, Any]) -> None:
    """Create or update a ConfigMap for a job."""
    cm_name = _cm_name(job_id)
    payload = json.dumps(data, default=str)
    try:
        # Try patch first (update)
        safe_exec([
            "kubectl", "create", "configmap", cm_name,
            f"--from-literal=data={payload}",
            "--dry-run=client", "-o", "json",
        ], timeout=5)
        # Apply with labels
        manifest = {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": cm_name, "labels": {"elb-job": "true", "job-id": job_id}},
            "data": {"job": payload},
        }
        proc_input = json.dumps(manifest)
        import subprocess
        subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=proc_input, capture_output=True, text=True, timeout=10, check=True,
        )
    except Exception as exc:
        logger.warning("Failed to save ConfigMap for %s: %s", job_id, str(exc)[:200])

def _load_all_jobs_cm() -> dict[str, dict[str, Any]]:
    """Load all job ConfigMaps from K8s."""
    jobs: dict[str, dict[str, Any]] = {}
    try:
        proc = safe_exec(f"kubectl get configmap -l {_CM_LABEL} -o json", timeout=10)
        data = json.loads(proc.stdout)
        for item in data.get("items", []):
            job_data = item.get("data", {}).get("job", "{}")
            parsed = json.loads(job_data)
            jid = parsed.get("job_id", item["metadata"]["name"].replace(_CM_PREFIX, ""))
            jobs[jid] = parsed
    except Exception as exc:
        logger.warning("Failed to load job ConfigMaps: %s", str(exc)[:200])
    return jobs

def _load_job_cm(job_id: str) -> dict[str, Any] | None:
    """Load a single job ConfigMap."""
    try:
        proc = safe_exec(f"kubectl get configmap {_cm_name(job_id)} -o json", timeout=5)
        data = json.loads(proc.stdout)
        return json.loads(data.get("data", {}).get("job", "{}"))
    except Exception:
        return None

def _delete_job_cm(job_id: str) -> None:
    try:
        safe_exec(f"kubectl delete configmap {_cm_name(job_id)}", timeout=10)
    except Exception as exc:
        logger.warning("Failed to delete ConfigMap %s: %s", job_id, str(exc)[:200])

# ── In-memory cache (hot path, synced with ConfigMap) ──────────────────────
_jobs: dict[str, dict[str, Any]] = {}
_jobs_lock = Lock()
_cm_loaded = False

def _ensure_loaded() -> None:
    """Lazy-load from ConfigMaps on first access."""
    global _cm_loaded
    if _cm_loaded:
        return
    with _jobs_lock:
        if _cm_loaded:
            return
        loaded = _load_all_jobs_cm()
        _jobs.update(loaded)
        _cm_loaded = True
        logger.info("Loaded %d jobs from ConfigMaps", len(loaded))

# ── Webhook (optional) ────────────────────────────────────────────────────

def _webhook_notify(job_id: str, data: dict[str, Any]) -> None:
    """Webhook to Control Plane with exponential backoff (3 attempts). Failure is non-fatal."""
    if not CONTROL_PLANE_URL:
        return
    import time
    body = json.dumps({"job_id": job_id, **data}).encode()
    for attempt in range(3):
        try:
            import urllib.request
            req = urllib.request.Request(
                f"{CONTROL_PLANE_URL}/api/blast/register-external-job",
                data=body, headers={"Content-Type": "application/json"},
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
    if os.environ.get("AZCOPY_AUTO_LOGIN_TYPE"):
        return
    try:
        safe_exec("azcopy login --identity", timeout=30)
    except Exception as exc:
        raise RuntimeError(f"azcopy auth failed: {str(exc)[:200]}") from exc

def _validate_blob_url(url: str, field: str) -> None:
    if not _BLOB_URL_RE.match(url):
        raise ValueError(f"{field} must be a valid Azure Blob URL")

def _sanitize_job_id(job_id: str) -> str:
    cleaned = re.sub(r"[^a-f0-9]", "", job_id.lower())
    if len(cleaned) < 6 or len(cleaned) > 12:
        raise HTTPException(status_code=400, detail="Invalid job_id format")
    return cleaned

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

# Versioned router — all business endpoints live under /v1
v1 = APIRouter(prefix="/v1")

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
    """Get the effective ElasticBLAST configuration. Environment variables override INI defaults."""
    return JSONResponse(content=_resolve_config())

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

# ── Jobs — Submit ──────────────────────────────────────────────────────────

def _build_entrez_query(taxid: int, inclusive: bool) -> str:
    base = f"txid{taxid}[Organism:exp]"
    return base if inclusive else f"NOT {base}"

def _build_options(opts: BlastOptions | None, taxid: int | None, inclusive: bool | None) -> str:
    parts: list[str] = []
    if opts:
        if opts.evalue is not None: parts.append(f"-evalue {opts.evalue}")
        if opts.max_target_seqs is not None: parts.append(f"-max_target_seqs {opts.max_target_seqs}")
        if opts.outfmt is not None: parts.append(f'-outfmt "{opts.outfmt}"')
        if opts.extra: parts.append(opts.extra)
    if not parts:
        parts.append("-evalue 0.01 -outfmt 7")
    if taxid is not None:
        eq = _build_entrez_query(taxid, inclusive if inclusive is not None else True)
        parts.append(f'-entrez_query "{eq}"')
    return " ".join(parts)

def _upload_fasta(job_id: str, fasta: str) -> str:
    local = f"/tmp/query-{job_id}.fa"
    url = f"{_blob_base()}/queries/{job_id}.fa"
    with open(local, "w") as f: f.write(fasta)
    try:
        _azcopy_login()
        safe_exec(["azcopy", "cp", local, url], timeout=60)
    finally:
        _cleanup_tmp(local)
    return url

def _run_submit_bg(job_id: str, cfg_path: str) -> None:
    try:
        result = safe_exec(["elastic-blast", "submit", "--cfg", cfg_path], timeout=1800)
        with _jobs_lock:
            _jobs[job_id]["status"] = "submitted"
            if result.stdout:
                m = re.search(r"job-([a-f0-9]+)", result.stdout)
                if m: _jobs[job_id]["elb_job_id"] = m.group(1)
        _save_job_cm(job_id, _jobs[job_id])
        _webhook_notify(job_id, {"event": "submitted", "status": "submitted"})
        logger.info("Job %s submitted", job_id)
    except Exception as e:
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["error"] = str(e)[:500]
        _save_job_cm(job_id, _jobs[job_id])
        _webhook_notify(job_id, {"event": "failed", "error": str(e)[:200]})
        logger.error("Job %s failed: %s", job_id, str(e)[:200])


@v1.post("/jobs", tags=["Jobs"], status_code=202, summary="Submit a BLAST search",
          openapi_extra={"requestBody": {"content": {"application/json": {"examples": {
              "mode_a": _MODE_A_EXAMPLE, "mode_b": _MODE_B_EXAMPLE, "mode_b_taxid": _MODE_B_TAXID_EXAMPLE,
          }}}}})
async def submit_job(req: JobSubmitRequest):
    """Submit a BLAST search job. Mode is auto-detected:

    - **Mode A** — if `queries` and `results` are provided as full Blob URLs
    - **Mode B** — if `query_fasta` is provided (inline FASTA text)

    Returns a `job_id` for polling status via `GET /jobs/{job_id}/status`.
    """
    _ensure_loaded()
    if req.program not in _VALID_PROGRAMS:
        raise HTTPException(400, f"Invalid program. Must be: {', '.join(sorted(_VALID_PROGRAMS))}")

    job_id = uuid.uuid4().hex[:8]
    is_b = req.query_fasta is not None

    if is_b:
        if not req.query_fasta.strip():
            raise HTTPException(400, "query_fasta must not be empty")
        try: queries_url = _upload_fasta(job_id, req.query_fasta)
        except Exception as e: raise HTTPException(503, f"Upload failed: {str(e)[:200]}")
        db_url = req.db if _BLOB_URL_RE.match(req.db) else f"{_blob_base()}/blast-db/{req.db}"
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
    config["cluster"]["name"] = cluster
    config["cluster"]["machine-type"] = MACHINE_TYPE
    config["cluster"]["num-nodes"] = str(NUM_NODES)
    config["blast"]["program"] = req.program
    config["blast"]["db"] = db_url
    config["blast"]["queries"] = queries_url
    config["blast"]["results"] = results_url
    config["blast"]["options"] = opts

    job_dir = f"/tmp/elb-jobs/{job_id}"
    os.makedirs(job_dir, exist_ok=True)
    cfg_path = os.path.join(job_dir, "config.ini")
    with open(cfg_path, "w") as f: config.write(f)

    job_data = {
        "job_id": job_id, "status": "submitting", "mode": "B" if is_b else "A",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "cluster_name": cluster, "program": req.program, "db": db_url,
        "results": results_url, "cfg_path": cfg_path,
    }
    with _jobs_lock:
        _jobs[job_id] = job_data
    _save_job_cm(job_id, job_data)

    Thread(target=_run_submit_bg, args=(job_id, cfg_path), daemon=True).start()
    logger.info("Job %s started (mode %s)", job_id, "B" if is_b else "A")
    return {"job_id": job_id, "status": "submitting", "message": "Poll GET /v1/jobs/{job_id}/status for progress."}

# ── Jobs — List ────────────────────────────────────────────────────────────

@v1.get("/jobs", tags=["Jobs"], summary="List all jobs")
async def list_jobs():
    """List all tracked BLAST jobs. State is persisted in K8s ConfigMaps."""
    _ensure_loaded()
    with _jobs_lock:
        return {"jobs": [
            {"job_id": jid, "status": i["status"], "mode": i.get("mode","A"),
             "created_at": i.get("created_at",""), "program": i.get("program",""),
             "cluster_name": i.get("cluster_name",""), "db": i.get("db","")}
            for jid, i in _jobs.items()
        ], "count": len(_jobs)}

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

    if job_info["status"] == "submitting":
        return {"job_id": job_id, "phase": "submitting", "message": "Submitting to AKS", "created_at": job_info.get("created_at","")}
    if job_info["status"] == "failed":
        return {"job_id": job_id, "phase": "failed", "error": job_info.get("error","Unknown"), "created_at": job_info.get("created_at","")}

    k8s: dict[str,Any] = {}
    try:
        proc = safe_exec("kubectl get jobs -o json", timeout=15)
        data = json.loads(proc.stdout)
        total=succeeded=failed=active=0
        elb_id = job_info.get("elb_job_id","")
        for item in data.get("items",[]):
            name=item["metadata"]["name"]; labels=item["metadata"].get("labels",{}); st=item.get("status",{})
            if elb_id and labels.get("ElasticBLAST","")!=elb_id:
                if not any(kw in name for kw in ("blast","batch","init")): continue
            if "blast" in name or "batch" in name:
                total+=1; succeeded+=st.get("succeeded",0) or 0; failed+=st.get("failed",0) or 0; active+=st.get("active",0) or 0
        k8s["summary"]={"total":total,"succeeded":succeeded,"failed":failed,"active":active}
        k8s["phase"]="initializing" if total==0 else "failed" if failed>0 else "completed" if succeeded==total else "running" if active>0 else "pending"
    except Exception as e:
        k8s["phase"]="unknown"; k8s["error"]=str(e)[:200]

    results_url = job_info.get("results","")
    if results_url:
        try:
            _azcopy_login()
            proc = safe_exec(["azcopy","ls",f"{results_url}/metadata/"], timeout=10)
            if "SUCCESS.txt" in proc.stdout: k8s["phase"]="completed"
            elif "FAILURE.txt" in proc.stdout: k8s["phase"]="failed"
        except Exception: pass

    return {"job_id": job_id, "phase": k8s.get("phase","unknown"), "created_at": job_info.get("created_at",""),
            "program": job_info.get("program",""), "db": job_info.get("db",""), "kubernetes": k8s}

# ── Jobs — Delete ──────────────────────────────────────────────────────────

@v1.delete("/jobs/{job_id}", tags=["Jobs"], summary="Delete a job")
async def delete_job(job_id: str):
    """Delete a tracked job, clean up K8s resources and ConfigMap."""
    _ensure_loaded()
    job_id = _sanitize_job_id(job_id)
    with _jobs_lock: job_info = _jobs.pop(job_id, None)
    if not job_info:
        job_info = _load_job_cm(job_id)
    if not job_info: raise HTTPException(404, f"Job {job_id} not found")

    cfg_path = job_info.get("cfg_path","")
    if cfg_path and os.path.isfile(cfg_path):
        try: safe_exec(["elastic-blast","delete","--cfg",cfg_path], timeout=120)
        except Exception as e: logger.warning("delete %s failed: %s", job_id, str(e)[:200])
    if cfg_path: _cleanup_tmp(os.path.dirname(cfg_path))
    _delete_job_cm(job_id)
    return {"job_id": job_id, "status": "deleted"}

# ── Jobs — Results ─────────────────────────────────────────────────────────

@v1.get("/jobs/{job_id}/results", tags=["Jobs"], summary="Download results")
async def download_results(job_id: str):
    """Download BLAST results for a completed job as a ZIP archive."""
    _ensure_loaded()
    job_id = _sanitize_job_id(job_id)
    with _jobs_lock: job_info = _jobs.get(job_id)
    if not job_info: job_info = _load_job_cm(job_id)
    if not job_info: raise HTTPException(404, f"Job {job_id} not found")
    results_url = job_info.get("results","")
    if not results_url: raise HTTPException(404, "No results URL")

    work_dir = f"/tmp/results-{job_id}"
    zip_path = f"/tmp/results-{job_id}.zip"
    try:
        os.makedirs(work_dir, exist_ok=True)
        _azcopy_login()
        safe_exec(["azcopy","cp",f"{results_url}/*",work_dir,"--recursive","--include-pattern","*.out.gz;*.out"], timeout=300)
        files = glob.glob(os.path.join(work_dir,"**","*.out*"), recursive=True)
        if not files: raise HTTPException(404, "No result files found")
        with zipfile.ZipFile(zip_path,"w",zipfile.ZIP_DEFLATED) as zf:
            for f in files: zf.write(f, os.path.relpath(f, work_dir))
        return FileResponse(zip_path, filename=f"blast-results-{job_id}.zip", media_type="application/zip",
                           background=BackgroundTask(_cleanup_tmp, work_dir, zip_path))
    except HTTPException: raise
    except Exception as e:
        _cleanup_tmp(work_dir, zip_path)
        raise HTTPException(500, str(e)[:500])

# ── Register versioned router ──────────────────────────────────────────────
app.include_router(v1)

# ── Entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(f"{Path(__file__).stem}:app", host="0.0.0.0", port=8000, reload=True)
