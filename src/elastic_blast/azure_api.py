"""
elastic_blast/azure_api.py — Public, programmatic API surface for Azure/AKS

This is the entry point that external services (e.g., elb-dashboard) and
the CLI both call. It wraps `ElasticBlastAzure` lifecycle methods with:

  * structured results / structured errors (`azure_api_types`)
  * idempotency keyed on `elb_job_id`
  * in-process backpressure (semaphore + bounded queue)
  * capacity reporting before submission
  * health checks for liveness/readiness probes
  * correlation-ID-aware structured logging

Concurrency model:
  * Many threads in one process call submit_search() concurrently.
  * `_submission_gate` caps concurrent in-flight submissions per process.
    Excess requests are queued up to `queue_size`; beyond that they are
    rejected with a CapacityVerdict so callers can apply jittered retry.
  * Multi-process safety is delegated to AKS / Azure (per-job Job names,
    elb-job-id label scoping); the process-level gate is purely a local
    safety throttle.

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

from __future__ import annotations

import contextvars
import logging
import os
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Optional

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError  # type: ignore

from .azure_api_types import (
    AzureApiError, CapacityReport, CapacityVerdict, ErrorCategory,
    HealthReport, HealthStatus, SearchPhase, StatusResult, SubmitDecision,
    SubmitResult,
)
from .constants import (
    AKS_PROVISIONING_STATE, CLUSTER_ERROR, ElbStatus, INPUT_ERROR,
    DEPENDENCY_ERROR,
)
from .elb_config import ElasticBlastConfig
from .util import SafeExecError, UserReportError

# We import the implementation lazily inside functions to avoid importing
# the heavy `azure` module at API import time.

# ---------------------------------------------------------------------------
# Correlation-ID context
# ---------------------------------------------------------------------------

_correlation_id: contextvars.ContextVar[Optional[str]] = \
    contextvars.ContextVar('elb_correlation_id', default=None)


def get_correlation_id() -> Optional[str]:
    """Return the correlation id active for the current task/thread, if any."""
    return _correlation_id.get()


@contextmanager
def correlation_scope(correlation_id: str):
    """Bind a correlation id (typically elb_job_id) for the duration of a block.

    Use in API handlers:
        with correlation_scope(cfg.azure.elb_job_id):
            return submit_search(cfg)
    """
    token = _correlation_id.set(correlation_id)
    try:
        yield
    finally:
        _correlation_id.reset(token)


class _CorrelationFilter(logging.Filter):
    """logging.Filter that attaches the active correlation id to every record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.elb_correlation_id = _correlation_id.get() or '-'
        return True


def install_correlation_log_filter(logger: Optional[logging.Logger] = None) -> None:
    """Attach the correlation filter to a logger (root by default).

    Idempotent: a second call does not duplicate the filter.
    """
    target = logger or logging.getLogger()
    for f in target.filters:
        if isinstance(f, _CorrelationFilter):
            return
    target.addFilter(_CorrelationFilter())


# ---------------------------------------------------------------------------
# Submission backpressure gate
# ---------------------------------------------------------------------------

class SubmissionGate:
    """Bounded concurrency gate around submit_search().

    Semantics:
      * `max_concurrent` requests can hold a slot at once.
      * Up to `max_concurrent + queue_size` callers may *try* to acquire;
        callers beyond that bound get a False return.
      * `acquire(timeout)` returns True if a slot was obtained within the
        timeout, False otherwise.

    The gate is purely advisory at the process level. AKS does the real
    serialization on shared resources (PVC, ConfigMap).
    """

    def __init__(self, max_concurrent: int, queue_size: int):
        if max_concurrent < 1:
            raise ValueError('max_concurrent must be >= 1')
        if queue_size < 0:
            raise ValueError('queue_size must be >= 0')
        self._max_concurrent = max_concurrent
        self._queue_size = queue_size
        self._sem = threading.Semaphore(max_concurrent)
        self._waiters_lock = threading.Lock()
        self._waiters = 0   # number of callers currently in acquire()

    def settings(self) -> Dict[str, int]:
        return {
            'max_concurrent': self._max_concurrent,
            'queue_size': self._queue_size,
        }

    @property
    def waiters(self) -> int:
        with self._waiters_lock:
            return self._waiters

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Try to acquire a slot. Returns False if the queue is overloaded."""
        with self._waiters_lock:
            if self._waiters >= self._max_concurrent + self._queue_size:
                return False
            self._waiters += 1
        try:
            return self._sem.acquire(timeout=timeout) if timeout is not None \
                else self._sem.acquire()
        finally:
            with self._waiters_lock:
                self._waiters -= 1

    def release(self) -> None:
        self._sem.release()

    @contextmanager
    def slot(self, timeout: Optional[float] = None):
        ok = self.acquire(timeout=timeout)
        if not ok:
            raise AzureApiError(ErrorCategory.CAPACITY,
                'Local submission gate is full; retry later',
                retry_after_seconds=_jittered_retry(30),
                details={'gate': self.settings(), 'waiters': self.waiters})
        try:
            yield
        finally:
            self.release()


def _jittered_retry(base: int) -> int:
    """Return a retry-after hint with mild jitter (avoid thundering herd)."""
    # Use deterministic jitter source so tests stay stable when patched.
    import random
    return base + random.randint(0, max(1, base // 2))


# Module-level singleton; instantiated lazily so importers can override
# ELB_AZURE_MAX_CONCURRENT / ELB_AZURE_QUEUE_SIZE before first use.
_gate_lock = threading.Lock()
_gate: Optional[SubmissionGate] = None


def get_submission_gate() -> SubmissionGate:
    global _gate
    if _gate is None:
        with _gate_lock:
            if _gate is None:
                max_c = max(1, int(os.environ.get('ELB_AZURE_MAX_CONCURRENT', '8')))
                qsz = max(0, int(os.environ.get('ELB_AZURE_QUEUE_SIZE', '64')))
                _gate = SubmissionGate(max_c, qsz)
    return _gate


def reset_submission_gate_for_tests() -> None:
    """Clear the module-level gate so tests can install their own."""
    global _gate
    with _gate_lock:
        _gate = None


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------

_HTTP_TRANSIENT = {408, 429, 500, 502, 503, 504}


def _map_exception(exc: BaseException, *, correlation_id: Optional[str]) -> AzureApiError:
    """Translate any exception into an AzureApiError with a stable category."""
    if isinstance(exc, AzureApiError):
        if exc.correlation_id is None:
            exc.correlation_id = correlation_id
        return exc

    if isinstance(exc, ResourceNotFoundError):
        return AzureApiError(ErrorCategory.NOT_FOUND, str(exc),
                             correlation_id=correlation_id, cause=exc)

    if isinstance(exc, HttpResponseError):
        status = getattr(exc, 'status_code', None)
        msg = getattr(exc, 'message', None) or str(exc)
        if status == 429:
            return AzureApiError(ErrorCategory.CAPACITY,
                f'Azure throttled the request: {msg}',
                retry_after_seconds=_jittered_retry(30),
                correlation_id=correlation_id, cause=exc)
        if status in (401, 403):
            return AzureApiError(ErrorCategory.AUTH, msg,
                                 correlation_id=correlation_id, cause=exc)
        if status == 404:
            return AzureApiError(ErrorCategory.NOT_FOUND, msg,
                                 correlation_id=correlation_id, cause=exc)
        if status == 409:
            return AzureApiError(ErrorCategory.CONFLICT, msg,
                                 correlation_id=correlation_id, cause=exc)
        if status in _HTTP_TRANSIENT:
            return AzureApiError(ErrorCategory.TRANSIENT, msg,
                retry_after_seconds=_jittered_retry(15),
                correlation_id=correlation_id, cause=exc)
        if status is not None and 400 <= status < 500:
            return AzureApiError(ErrorCategory.INVALID, msg,
                                 correlation_id=correlation_id, cause=exc)
        return AzureApiError(ErrorCategory.PERMANENT, msg,
                             correlation_id=correlation_id, cause=exc)

    if isinstance(exc, UserReportError):
        rc = getattr(exc, 'returncode', None)
        msg = getattr(exc, 'message', str(exc))
        if rc == DEPENDENCY_ERROR:
            return AzureApiError(ErrorCategory.PERMANENT, msg,
                                 correlation_id=correlation_id, cause=exc)
        if rc == INPUT_ERROR:
            return AzureApiError(ErrorCategory.INVALID, msg,
                                 correlation_id=correlation_id, cause=exc)
        # CLUSTER_ERROR and friends — usually transient (cluster may recover)
        return AzureApiError(ErrorCategory.TRANSIENT, msg,
            retry_after_seconds=_jittered_retry(60),
            correlation_id=correlation_id, cause=exc)

    if isinstance(exc, SafeExecError):
        return AzureApiError(ErrorCategory.TRANSIENT,
            f'Subprocess failed: {exc.message}',
            retry_after_seconds=_jittered_retry(15),
            correlation_id=correlation_id, cause=exc)

    return AzureApiError(ErrorCategory.INTERNAL, f'{type(exc).__name__}: {exc}',
                         correlation_id=correlation_id, cause=exc)


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

def _has_terminal_marker(cfg: ElasticBlastConfig, *,
                          marker: str) -> bool:
    """Probe Blob Storage for an existing terminal marker (SUCCESS.txt /
    FAILURE.txt). True if present.

    We keep this best-effort: if Azure SDK fails, we treat it as 'no marker'
    and proceed. The finalizer is itself idempotent on the marker side.
    """
    try:
        from .azure_traits import azure_blob_exists
        from .constants import ELB_METADATA_DIR
        marker_url = (f'{cfg.cluster.results}/{cfg.azure.elb_job_id}/'
                      f'{ELB_METADATA_DIR}/{marker}')
        return azure_blob_exists(marker_url)
    except Exception as e:
        logging.debug(f'Idempotency probe for {marker} failed (treating as absent): {e}')
        return False


def _has_active_submission_jobs(cfg: ElasticBlastConfig) -> bool:
    """Return True if any Job carrying this elb-job-id is currently active
    on the cluster. Used to detect "submission already running" replays."""
    if cfg.cluster.dry_run:
        return False
    ctx = getattr(cfg.appstate, 'k8s_ctx', None)
    if not ctx:
        return False
    from .util import safe_exec, handle_error
    cmd = [
        'kubectl', f'--context={ctx}', 'get', 'jobs',
        '-l', f'elb-job-id={cfg.azure.elb_job_id}',
        '-o', 'jsonpath={.items[?(@.status.active)].metadata.name}',
    ]
    try:
        out = handle_error(safe_exec(cmd).stdout).strip()
    except SafeExecError:
        return False
    return bool(out)


# ---------------------------------------------------------------------------
# Capacity check
# ---------------------------------------------------------------------------

def check_capacity(cfg: ElasticBlastConfig) -> CapacityReport:
    """Inspect whether a fresh submission would be served right now.

    Looks at:
      1. The local submission gate (process-level backpressure)
      2. The AKS cluster provisioning state (if it exists)
      3. The number of other in-flight submissions on that cluster

    Quota check (Azure Compute vCPU per region) is intentionally NOT done
    here unconditionally — it is an extra ARM round-trip that we only do
    on the slow path (when we're about to create a new cluster). If the
    caller wants it eagerly, set ELB_CAPACITY_PROBE_QUOTA=1.
    """
    from .azure import _get_clients, check_cluster

    gate = get_submission_gate()
    settings = gate.settings()
    waiters = gate.waiters
    report = CapacityReport(
        verdict=CapacityVerdict.AVAILABLE,
        region=str(cfg.azure.region),
        cluster_name=cfg.cluster.name,
        queue_depth=waiters,
        max_concurrent=settings['max_concurrent'],
    )

    # Local gate
    if waiters >= settings['max_concurrent'] + settings['queue_size']:
        report.verdict = CapacityVerdict.EXHAUSTED_RETRY_LATER
        report.retry_after_seconds = _jittered_retry(30)
        report.reasons.append('local submission gate at queue limit')
        return report
    if waiters >= settings['max_concurrent']:
        report.verdict = CapacityVerdict.DEGRADED
        report.reasons.append('local submission gate full; new requests will queue')

    # AKS cluster state
    try:
        state = check_cluster(cfg) or ''
    except Exception as e:  # noqa: BLE001 — capacity probe must not raise
        report.verdict = CapacityVerdict.UNKNOWN
        report.reasons.append(f'AKS state probe failed: {e}')
        return report
    report.cluster_state = state
    if state in (AKS_PROVISIONING_STATE.FAILED.value,):
        report.verdict = CapacityVerdict.EXHAUSTED_INTERVENTION_REQUIRED
        report.reasons.append('AKS cluster is in FAILED state')
        return report
    if state in (AKS_PROVISIONING_STATE.UPDATING.value,
                 AKS_PROVISIONING_STATE.CREATING.value,
                 AKS_PROVISIONING_STATE.STARTING.value):
        if report.verdict == CapacityVerdict.AVAILABLE:
            report.verdict = CapacityVerdict.DEGRADED
        report.retry_after_seconds = max(report.retry_after_seconds or 0,
                                         _jittered_retry(60))
        report.reasons.append(f'AKS cluster transitioning: {state}')

    # Active submissions on the cluster
    try:
        active = _count_active_submissions_on_cluster(cfg)
        report.active_submissions = active
    except Exception as e:  # noqa: BLE001
        report.reasons.append(f'cluster job enumeration failed: {e}')

    # Optional Azure compute quota probe
    if os.environ.get('ELB_CAPACITY_PROBE_QUOTA') == '1':
        try:
            limit, used = _probe_compute_quota(cfg)
            report.quota_limit_cores = limit
            report.quota_used_cores = used
            if limit and used and used >= limit:
                report.verdict = CapacityVerdict.EXHAUSTED_INTERVENTION_REQUIRED
                report.reasons.append(
                    f'Azure compute vCPU quota exhausted in {cfg.azure.region} '
                    f'({used}/{limit}) — request quota increase or change region')
        except Exception as e:  # noqa: BLE001
            report.reasons.append(f'quota probe failed: {e}')

    return report


def _count_active_submissions_on_cluster(cfg: ElasticBlastConfig) -> int:
    """Count distinct elb-job-id Jobs with active>0 on the cluster.

    Returns 0 on any error so capacity reporting is best-effort.
    """
    ctx = getattr(cfg.appstate, 'k8s_ctx', None)
    if not ctx or cfg.cluster.dry_run:
        return 0
    from .util import safe_exec, handle_error
    jsonpath = (
        '{range .items[*]}'
        '{.metadata.labels.elb-job-id}{"\\t"}{.status.active}{"\\n"}'
        '{end}'
    )
    cmd = [
        'kubectl', f'--context={ctx}', 'get', 'jobs',
        '-l', 'elb-job-id', '-o', f'jsonpath={jsonpath}',
    ]
    try:
        out = handle_error(safe_exec(cmd).stdout)
    except SafeExecError:
        return 0
    seen = set()
    for line in out.splitlines():
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        jid, active = parts[0], parts[1]
        if jid and active and active != '0':
            seen.add(jid)
    return len(seen)


def _probe_compute_quota(cfg: ElasticBlastConfig):
    """Return (limit_cores, used_cores) for the configured region.

    Uses the Compute Usage API. Best-effort — caller catches Exception.
    """
    from .azure import _get_clients
    clients = _get_clients()
    limit = used = 0
    for usage in clients.compute.usage.list(str(cfg.azure.region)):
        # Total Regional vCPUs is the relevant aggregate quota.
        if usage.name and usage.name.value == 'cores':
            limit = int(usage.limit or 0)
            used = int(usage.current_value or 0)
            break
    return limit, used


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def health_check() -> HealthReport:
    """Liveness/readiness probe.

    Checks:
      kubectl_available     — kubectl binary is on PATH
      azcopy_available      — azcopy binary is on PATH
      azure_credential      — DefaultAzureCredential can mint a token
    Does NOT touch any specific cluster — that is done by check_capacity().
    """
    import shutil
    checks: Dict[str, bool] = {}
    messages: Dict[str, str] = {}

    checks['kubectl_available'] = shutil.which('kubectl') is not None
    if not checks['kubectl_available']:
        messages['kubectl_available'] = 'kubectl not on PATH'

    checks['azcopy_available'] = shutil.which('azcopy') is not None
    if not checks['azcopy_available']:
        messages['azcopy_available'] = 'azcopy not on PATH'

    try:
        from azure.identity import DefaultAzureCredential  # type: ignore
        DefaultAzureCredential().get_token('https://management.azure.com/.default')
        checks['azure_credential'] = True
    except Exception as e:  # noqa: BLE001
        checks['azure_credential'] = False
        messages['azure_credential'] = f'cannot mint Azure token: {e}'

    if all(checks.values()):
        status = HealthStatus.OK
    elif checks.get('kubectl_available') and checks.get('azure_credential'):
        # azcopy missing degrades data movement only; control plane works.
        status = HealthStatus.DEGRADED
    else:
        status = HealthStatus.UNHEALTHY
    return HealthReport(status=status, checks=checks, messages=messages)


# ---------------------------------------------------------------------------
# Submission API
# ---------------------------------------------------------------------------

def submit_search(cfg: ElasticBlastConfig, *,
                  query_batches=None,
                  query_length: int = 0,
                  one_stage_cloud_query_split: bool = False,
                  idempotency_key: Optional[str] = None,
                  gate_timeout_seconds: float = 0.0) -> SubmitResult:
    """Submit an ElasticBLAST search via the structured API.

    `idempotency_key`, when provided, becomes the elb_job_id, allowing a
    safe replay of the same submit with the same outcome. NOTE: this
    mutates `cfg.azure.elb_job_id` on the caller's config object — do NOT
    share a single `ElasticBlastConfig` instance across concurrent submit
    calls. Construct a fresh cfg per request.

    `gate_timeout_seconds` controls how long to wait for a backpressure
    slot before returning REJECTED_CAPACITY. Default 0 = fail fast (the
    caller's HTTP timeout should govern queueing semantics).
    """
    install_correlation_log_filter()
    if idempotency_key:
        cfg.azure.elb_job_id = idempotency_key

    correlation_id = cfg.azure.elb_job_id
    cluster_name = cfg.cluster.name

    with correlation_scope(correlation_id):
        # 1. Idempotency: if a previous submission already finished, return
        #    immediately rather than touching the cluster.
        if _has_terminal_marker(cfg, marker='SUCCESS.txt'):
            return SubmitResult(
                decision=SubmitDecision.ALREADY_DONE,
                correlation_id=correlation_id, cluster_name=cluster_name,
                message='SUCCESS.txt already present for this idempotency key',
                details={'terminal': 'SUCCESS'})
        if _has_terminal_marker(cfg, marker='FAILURE.txt'):
            return SubmitResult(
                decision=SubmitDecision.ALREADY_DONE,
                correlation_id=correlation_id, cluster_name=cluster_name,
                message='FAILURE.txt already present for this idempotency key',
                details={'terminal': 'FAILURE'})

        # 2. If cluster knows about active jobs for this id, the caller is
        #    replaying mid-flight — return RESUMED.
        if _has_active_submission_jobs(cfg):
            return SubmitResult(
                decision=SubmitDecision.RESUMED,
                correlation_id=correlation_id, cluster_name=cluster_name,
                message='Submission already in flight; returning resumed handle')

        # 3. Backpressure gate.
        gate = get_submission_gate()
        try:
            with gate.slot(timeout=gate_timeout_seconds):
                # 4. Defer to the existing implementation.
                from .azure import ElasticBlastAzure  # local import avoids cycle
                elb = ElasticBlastAzure(cfg, create=True)
                try:
                    elb.submit(query_batches or [], query_length,
                               one_stage_cloud_query_split)
                except Exception as e:  # noqa: BLE001
                    api_err = _map_exception(e, correlation_id=correlation_id)
                    if api_err.category == ErrorCategory.INVALID:
                        return SubmitResult(
                            decision=SubmitDecision.REJECTED_INVALID,
                            correlation_id=correlation_id,
                            cluster_name=cluster_name,
                            message=api_err.message, details=api_err.details)
                    if api_err.category == ErrorCategory.CAPACITY:
                        return SubmitResult(
                            decision=SubmitDecision.REJECTED_CAPACITY,
                            correlation_id=correlation_id,
                            cluster_name=cluster_name,
                            message=api_err.message,
                            retry_after_seconds=api_err.retry_after_seconds,
                            details=api_err.details)
                    if api_err.category in (ErrorCategory.PERMANENT,
                                              ErrorCategory.AUTH,
                                              ErrorCategory.NOT_FOUND):
                        return SubmitResult(
                            decision=SubmitDecision.REJECTED_PERMANENT,
                            correlation_id=correlation_id,
                            cluster_name=cluster_name,
                            message=api_err.message, details=api_err.details)
                    # Transient/internal: re-raise as structured error so
                    # the HTTP handler returns 5xx.
                    raise api_err
                return SubmitResult(
                    decision=SubmitDecision.ACCEPTED,
                    correlation_id=correlation_id, cluster_name=cluster_name,
                    message='submission accepted')
        except AzureApiError as e:
            # The gate raises CAPACITY when full; map to the typed result.
            if e.category == ErrorCategory.CAPACITY:
                return SubmitResult(
                    decision=SubmitDecision.REJECTED_CAPACITY,
                    correlation_id=correlation_id, cluster_name=cluster_name,
                    message=e.message,
                    retry_after_seconds=e.retry_after_seconds,
                    queue_position=gate.waiters,
                    details=e.details)
            raise


def get_status(cfg: ElasticBlastConfig) -> StatusResult:
    """Structured status wrapper around ElasticBlastAzure.check_status()."""
    correlation_id = cfg.azure.elb_job_id
    with correlation_scope(correlation_id):
        try:
            from .azure import ElasticBlastAzure
            elb = ElasticBlastAzure(cfg, create=False)
            status, counts, messages = elb.check_status()
        except Exception as e:  # noqa: BLE001
            raise _map_exception(e, correlation_id=correlation_id) from e
        return StatusResult(
            correlation_id=correlation_id,
            phase=_map_phase(status),
            pending=int(counts.get('pending', 0)),
            running=int(counts.get('running', 0)),
            succeeded=int(counts.get('succeeded', 0)),
            failed=int(counts.get('failed', 0)),
            message=messages.get('error', '') if messages else '',
            results_uri=f'{cfg.cluster.results}/{cfg.azure.elb_job_id}',
            cluster_provisioning_state=_safe_state(cfg),
        )


def delete_search(cfg: ElasticBlastConfig, *, force: bool = False) -> Dict[str, Any]:
    """Delete the cluster / cleanup.

    `force=True` bypasses the multi-submission guard. The flag is passed
    explicitly to the underlying delete helper rather than via os.environ,
    so concurrent callers in the same process cannot interfere with each
    other's force semantics.
    """
    correlation_id = cfg.azure.elb_job_id
    with correlation_scope(correlation_id):
        from .azure import (delete_cluster_with_cleanup,
                            ElasticBlastAzure)
        elb = ElasticBlastAzure(cfg, create=False)
        try:
            if cfg.cluster.reuse:
                # Reuse mode keeps the cluster — only clean up jobs.
                elb._cleanup_jobs_only()
            else:
                delete_cluster_with_cleanup(cfg, force=force)
        except Exception as e:  # noqa: BLE001
            raise _map_exception(e, correlation_id=correlation_id) from e
        return {
            'correlation_id': correlation_id,
            'cluster_name': cfg.cluster.name,
            'forced': force,
        }


def _map_phase(elb_status: ElbStatus) -> SearchPhase:
    """Map the internal ElbStatus enum to the API's SearchPhase."""
    return _PHASE_MAP.get(elb_status, SearchPhase.UNKNOWN)


_PHASE_MAP = {
    ElbStatus.SUBMITTING: SearchPhase.INITIALIZING,
    ElbStatus.RUNNING: SearchPhase.RUNNING,
    ElbStatus.SUCCESS: SearchPhase.SUCCEEDED,
    ElbStatus.FAILURE: SearchPhase.FAILED,
    ElbStatus.UNKNOWN: SearchPhase.UNKNOWN,
}


def _safe_state(cfg: ElasticBlastConfig) -> str:
    """Best-effort cluster state read, never raises."""
    try:
        from .azure import check_cluster
        return check_cluster(cfg) or ''
    except Exception:  # noqa: BLE001
        return ''
