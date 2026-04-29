import configparser
import glob
import json
import os
import re
import shutil
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, Thread
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask
from util import safe_exec

# ── Configuration ──────────────────────────────────────────────────────────
CLUSTER_NAME = os.environ.get('ELB_CLUSTER_NAME', 'elastic-blast-on-azure')
STORAGE_ACCOUNT = os.environ.get('ELB_STORAGE_ACCOUNT', 'stgelb')
RESOURCE_GROUP = os.environ.get('ELB_RESOURCE_GROUP', 'rg-elb-koc')

# Validate Blob URL pattern: https://<account>.blob.core.windows.net/<container>/...
_BLOB_URL_PATTERN = re.compile(
    r'^https://[a-z0-9]+\.blob\.core\.windows\.net/[a-z0-9][-a-z0-9]*/.*$'
)

app = FastAPI(
    title="ElasticBlast on Azure OpenAPI",
    description="REST API for running ElasticBLAST searches on Azure Kubernetes Service",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# ── In-memory job tracking ─────────────────────────────────────────────────
# job_id -> {status, created_at, config, cluster_name, ...}
_MAX_TRACKED_JOBS = 1000
_jobs: dict[str, dict] = {}
_jobs_lock = Lock()


def _evict_old_jobs():
    """Remove oldest completed/failed jobs when limit exceeded. Must hold _jobs_lock."""
    if len(_jobs) <= _MAX_TRACKED_JOBS:
        return
    # Sort by created_at, remove oldest completed/failed first
    removable = [
        (jid, info) for jid, info in _jobs.items()
        if info.get('status') in ('submitted', 'failed')
    ]
    removable.sort(key=lambda x: x[1].get('created_at', ''))
    to_remove = len(_jobs) - _MAX_TRACKED_JOBS
    for jid, _ in removable[:to_remove]:
        cfg_path = _jobs[jid].get('cfg_path', '')
        if cfg_path and os.path.isfile(cfg_path):
            try:
                os.remove(cfg_path)
            except OSError:
                pass
        del _jobs[jid]


def _azcopy_login():
    """Login azcopy using available credentials (Managed Identity or CLI)."""
    login_type = os.environ.get('AZCOPY_AUTO_LOGIN_TYPE', '')
    if login_type:
        return
    try:
        safe_exec('azcopy login --identity', timeout=30)
    except Exception as e:
        raise RuntimeError(f'azcopy authentication failed: {str(e)[:200]}')


def _validate_blob_url(url: str, field_name: str) -> None:
    """Validate that a URL is a legitimate Azure Blob Storage URL."""
    if not _BLOB_URL_PATTERN.match(url):
        raise ValueError(f'{field_name} must be a valid Azure Blob Storage URL '
                         f'(https://<account>.blob.core.windows.net/<container>/...)')


def _sanitize_job_id(job_id: str) -> str:
    """Sanitize job_id: must be 8-char hex only."""
    cleaned = re.sub(r'[^a-f0-9]', '', job_id.lower())
    if len(cleaned) != 8:
        raise HTTPException(status_code=400, detail="Invalid job_id format")
    return cleaned


# ── Health & Info ──────────────────────────────────────────────────────────

@app.get("/")
async def read_root():
    return {"message": "ElasticBlast on Azure OpenAPI", "version": "2.0.0"}


@app.get('/health')
async def health():
    """Health check: verify kubectl and Azure auth connectivity."""
    checks = {}
    try:
        proc = safe_exec('kubectl get nodes --no-headers', timeout=10)
        node_count = len([l for l in proc.stdout.strip().split('\n') if l])
        checks['kubernetes'] = {'status': 'ok', 'nodes': node_count}
    except Exception as e:
        checks['kubernetes'] = {'status': 'error', 'message': str(e)[:200]}
    try:
        safe_exec('az account show', timeout=10)
        checks['azure_auth'] = {'status': 'ok'}
    except Exception as e:
        checks['azure_auth'] = {'status': 'error', 'message': str(e)[:200]}
    overall = 'healthy' if all(c['status'] == 'ok' for c in checks.values()) else 'degraded'
    return {"status": overall, "checks": checks}


@app.get('/ping')
async def ping():
    return {"message": "pong"}


# ── Cluster Status ─────────────────────────────────────────────────────────

@app.get('/cluster/nodes')
async def get_cluster_nodes():
    """Get AKS cluster node status."""
    try:
        proc = safe_exec('kubectl get nodes -o json', timeout=15)
        data = json.loads(proc.stdout)
        nodes = []
        for item in data.get('items', []):
            node = {
                'name': item['metadata']['name'],
                'status': next(
                    (c['type'] for c in item.get('status', {}).get('conditions', [])
                     if c.get('status') == 'True' and c['type'] == 'Ready'),
                    'NotReady'
                ),
                'instance_type': item['metadata'].get('labels', {}).get(
                    'node.kubernetes.io/instance-type', 'unknown'),
            }
            nodes.append(node)
        return {"nodes": nodes, "count": len(nodes)}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e)[:300])


@app.get('/cluster/pods')
async def get_cluster_pods():
    """Get pod status with summary counts."""
    try:
        proc = safe_exec('kubectl get pods -o json', timeout=15)
        data = json.loads(proc.stdout)
        pods = []
        for item in data.get('items', []):
            pods.append({
                'name': item['metadata']['name'],
                'phase': item['status'].get('phase', 'Unknown'),
                'node': item['spec'].get('nodeName', ''),
                'labels': item['metadata'].get('labels', {}),
            })
        summary = {}
        for p in pods:
            phase = p['phase']
            summary[phase] = summary.get(phase, 0) + 1
        return {"pods": pods, "summary": summary, "total": len(pods)}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e)[:300])


# ── Job Submission ─────────────────────────────────────────────────────────

_VALID_PROGRAMS = {'blastp', 'blastn', 'blastx', 'psiblast', 'rpsblast', 'rpstblastn', 'tblastn', 'tblastx'}


class BlastRequest(BaseModel):
    """BLAST search configuration for job submission."""
    cluster_name: Optional[str] = Field(None, description="AKS cluster name (uses default if not set)")
    program: str = Field('blastn', description="BLAST program (blastn, blastp, blastx, etc.)")
    db: str = Field(..., description="BLAST database URL in Azure Blob Storage")
    queries: str = Field(..., description="Query sequences URL in Azure Blob Storage")
    results: str = Field(..., description="Results destination URL in Azure Blob Storage")
    options: str = Field('-evalue 0.01 -outfmt 7', description="BLAST command-line options")


def _run_submit_background(job_id: str, cfg_path: str):
    """Run elastic-blast submit in a background thread."""
    try:
        result = safe_exec(
            ['elastic-blast', 'submit', '--cfg', cfg_path],
            timeout=1800
        )
        with _jobs_lock:
            _jobs[job_id]['status'] = 'submitted'
            if result.stdout:
                match = re.search(r"job-([a-f0-9]+)", result.stdout)
                if match:
                    _jobs[job_id]['elb_job_id'] = match.group(1)
    except Exception as e:
        with _jobs_lock:
            _jobs[job_id]['status'] = 'failed'
            _jobs[job_id]['error'] = str(e)[:500]


@app.post('/jobs')
async def submit_job(blast: BlastRequest):
    """Submit a new BLAST search job. Returns a job_id for status polling.
    
    Submission runs asynchronously — poll GET /jobs/{job_id}/status for progress.
    """
    # Validate Blob URLs
    try:
        _validate_blob_url(blast.db, 'db')
        _validate_blob_url(blast.queries, 'queries')
        _validate_blob_url(blast.results, 'results')
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if blast.program not in _VALID_PROGRAMS:
        raise HTTPException(status_code=400,
                            detail=f'Invalid program: {blast.program}. Must be one of: {", ".join(sorted(_VALID_PROGRAMS))}')

    job_id = uuid.uuid4().hex[:8]

    try:
        _azcopy_login()
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e)[:300])

    # Build INI config from request
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "elb-cfg.ini"), encoding='utf-8')

    cluster_name = blast.cluster_name or config.get('cluster', 'name', fallback=CLUSTER_NAME)
    config['cluster']['name'] = cluster_name

    config['blast']['program'] = blast.program
    config['blast']['db'] = blast.db
    config['blast']['queries'] = blast.queries
    config['blast']['results'] = blast.results
    config['blast']['options'] = blast.options

    cfg_path = os.path.join(os.path.dirname(__file__), f"job-{job_id}.ini")
    with open(cfg_path, "w") as configfile:
        config.write(configfile)

    # Track job
    with _jobs_lock:
        _evict_old_jobs()
        _jobs[job_id] = {
            'status': 'submitting',
            'created_at': datetime.now(timezone.utc).isoformat(),
            'cluster_name': cluster_name,
            'program': blast.program,
            'db': blast.db,
            'results': blast.results,
            'cfg_path': cfg_path,
        }

    # Submit in background thread (avoids HTTP timeout)
    thread = Thread(target=_run_submit_background, args=(job_id, cfg_path), daemon=True)
    thread.start()

    return JSONResponse(content={
        "job_id": job_id,
        "status": "submitting",
        "message": "BLAST search submission started. Poll GET /jobs/{job_id}/status for progress.",
    }, status_code=202)


# ── Job Status ─────────────────────────────────────────────────────────────

@app.get('/jobs')
async def list_jobs():
    """List all tracked jobs with their current status."""
    with _jobs_lock:
        jobs_list = []
        for jid, info in _jobs.items():
            jobs_list.append({
                'job_id': jid,
                'status': info['status'],
                'created_at': info.get('created_at', ''),
                'program': info.get('program', ''),
                'cluster_name': info.get('cluster_name', ''),
            })
    return {"jobs": jobs_list, "count": len(jobs_list)}


@app.get('/jobs/{job_id}/status')
async def get_job_status(job_id: str):
    """Get detailed status of a specific job by polling Kubernetes."""
    job_id = _sanitize_job_id(job_id)

    with _jobs_lock:
        job_info = _jobs.get(job_id)

    if not job_info:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    # If still submitting, return early
    if job_info['status'] == 'submitting':
        return {
            'job_id': job_id,
            'phase': 'submitting',
            'message': 'Job is being submitted to AKS cluster',
            'created_at': job_info.get('created_at', ''),
        }

    # If submission failed, return error
    if job_info['status'] == 'failed':
        return {
            'job_id': job_id,
            'phase': 'failed',
            'error': job_info.get('error', 'Unknown error'),
            'created_at': job_info.get('created_at', ''),
        }

    # Check K8s job status via kubectl — filter by job labels
    k8s_status = {'blast_jobs': {}, 'init_jobs': {}}
    try:
        # Get jobs matching this elastic-blast run
        proc = safe_exec('kubectl get jobs -o json', timeout=15)
        data = json.loads(proc.stdout)

        total = 0
        succeeded = 0
        failed = 0
        active = 0

        elb_job_id = job_info.get('elb_job_id', '')

        for item in data.get('items', []):
            name = item['metadata']['name']
            labels = item['metadata'].get('labels', {})
            status = item.get('status', {})

            # Filter by elb_job_id if available, otherwise include all blast/batch jobs
            if elb_job_id and labels.get('ElasticBLAST', '') != elb_job_id:
                if 'blast' not in name and 'batch' not in name and 'init' not in name:
                    continue

            job_entry = {
                'succeeded': status.get('succeeded', 0) or 0,
                'failed': status.get('failed', 0) or 0,
                'active': status.get('active', 0) or 0,
                'start_time': status.get('startTime', ''),
                'completion_time': status.get('completionTime', ''),
            }

            if 'blast' in name or 'batch' in name:
                k8s_status['blast_jobs'][name] = job_entry
                total += 1
                succeeded += job_entry['succeeded']
                failed += job_entry['failed']
                active += job_entry['active']
            elif 'init' in name or 'setup' in name:
                k8s_status['init_jobs'][name] = job_entry

        k8s_status['summary'] = {
            'total': total,
            'succeeded': succeeded,
            'failed': failed,
            'active': active,
            'pending': max(0, total - succeeded - failed - active),
        }

        if total == 0:
            phase = 'initializing'
        elif failed > 0:
            phase = 'failed'
        elif succeeded == total:
            phase = 'completed'
        elif active > 0:
            phase = 'running'
        else:
            phase = 'pending'

        k8s_status['phase'] = phase

    except Exception as e:
        k8s_status['error'] = str(e)[:300]
        k8s_status['phase'] = 'unknown'

    # Check for SUCCESS/FAILURE markers in blob storage
    results_url = job_info.get('results', '')
    if results_url:
        try:
            _azcopy_login()
            proc = safe_exec(['azcopy', 'ls', f'{results_url}/metadata/'], timeout=10)
            if 'SUCCESS.txt' in proc.stdout:
                k8s_status['phase'] = 'success'
            elif 'FAILURE.txt' in proc.stdout:
                k8s_status['phase'] = 'failure'
        except Exception:
            pass  # Marker check is best-effort

    return {
        'job_id': job_id,
        'phase': k8s_status.get('phase', 'unknown'),
        'created_at': job_info.get('created_at', ''),
        'program': job_info.get('program', ''),
        'db': job_info.get('db', ''),
        'kubernetes': k8s_status,
    }


# ── Results Download ───────────────────────────────────────────────────────

@app.get('/jobs/{job_id}/results')
async def download_results(job_id: str):
    """Download results for a completed job as a ZIP file."""
    job_id = _sanitize_job_id(job_id)

    with _jobs_lock:
        job_info = _jobs.get(job_id)

    if not job_info:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    results_url = job_info.get('results', '')
    if not results_url:
        raise HTTPException(status_code=404, detail="No results URL configured for this job")

    work_dir = f'/tmp/results-{job_id}'
    zip_path = f'/tmp/results-{job_id}.zip'

    try:
        os.makedirs(work_dir, exist_ok=True)
        _azcopy_login()

        # Download results from Blob Storage
        safe_exec(
            ['azcopy', 'cp', f'{results_url}/*', work_dir,
             '--recursive', '--include-pattern', '*.out.gz;*.out'],
            timeout=300
        )

        # Create ZIP
        result_files = glob.glob(os.path.join(work_dir, '**', '*.out*'), recursive=True)
        if not result_files:
            raise HTTPException(status_code=404, detail="No result files found")

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for fpath in result_files:
                arcname = os.path.relpath(fpath, work_dir)
                zipf.write(fpath, arcname)

        return FileResponse(
            zip_path,
            filename=f'blast-results-{job_id}.zip',
            media_type='application/zip',
            background=BackgroundTask(_cleanup_tmp, work_dir, zip_path),
        )

    except HTTPException:
        raise
    except Exception as e:
        _cleanup_tmp(work_dir, zip_path)
        raise HTTPException(status_code=500, detail=str(e)[:500])


def _cleanup_tmp(*paths: str):
    """Remove temporary files and directories."""
    for p in paths:
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            elif os.path.isfile(p):
                os.remove(p)
        except Exception:
            pass


# ── ElasticBLAST Config ───────────────────────────────────────────────────

@app.get('/config')
async def get_elb_config():
    """Get the current base ElasticBLAST configuration."""
    try:
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), "elb-cfg.ini"), encoding='utf-8')
        result = {section: dict(config[section]) for section in config.sections()}
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)[:300])


if __name__ == '__main__':
    uvicorn.run(f"{Path(__file__).stem}:app", host="0.0.0.0", port=8000, reload=True)
