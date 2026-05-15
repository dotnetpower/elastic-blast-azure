"""
elastic_blast/azure_cli_glue.py — CLI <-> azure_api adapter

Purpose:
    Lets `bin/elastic-blast` emit machine-readable JSON output and honor
    idempotency / correlation-id / capacity semantics for Azure submissions
    *without* modifying the upstream submit/status/delete command modules.

Used by `bin/elastic-blast` only when:
    * the active CSP is Azure, AND
    * the user (or an integration like elb-dashboard) passed `--json`
      and/or any of `--idempotency-key`, `--correlation-id`, `--force`.

Exit code conventions (Azure JSON mode):
    0   success / accepted / resumed / already_done
    1   INPUT_ERROR (validation, malformed request)            -> REJECTED_INVALID
    8   CLUSTER_ERROR (transient)                              -> TRANSIENT
    10  NOT_READY_ERROR (capacity exhausted, retry later)      -> REJECTED_CAPACITY
    7   DEPENDENCY_ERROR (auth, dep missing, hard quota)       -> REJECTED_PERMANENT
    255 UNKNOWN_ERROR (internal bug)                           -> INTERNAL

The last line of stdout is always a single JSON object so consumers can
parse it without touching the multi-line log stream above it.

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

from __future__ import annotations

import dataclasses
import json
import logging
import re
import sys
from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict, Iterator, Optional

from . import azure_api
from .azure_api_types import (
    AzureApiError, ErrorCategory, SubmitDecision, SubmitResult,
)
from .constants import (
    CLUSTER_ERROR, CSP, DEPENDENCY_ERROR, INPUT_ERROR, NOT_READY_ERROR,
    UNKNOWN_ERROR,
)
from .elb_config import ElasticBlastConfig

LOGGER = logging.getLogger(__name__)

# Idempotency keys end up as a K8s label value (`elb-job-id=<key>`) AND a
# Job-name suffix. Both must be DNS-1123-safe: lowercase alphanumeric +
# `-`, start/end alphanumeric, length <= 63. We accept upper case but
# normalize to lowercase, and reject anything outside the alphabet.
# The optional middle group lets us accept single-character keys too.
_IDEMPOTENCY_KEY_RE = re.compile(
    r'^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$')
_IDEMPOTENCY_KEY_MAX = 63


def validate_idempotency_key(key: str) -> str:
    """Return a normalized DNS-1123-safe idempotency key, or raise.

    The key is lowercased; rejects empty, too-long, or non-alphanumeric/
    `-`/`_`/`.` content. The exception is `AzureApiError` with category
    INVALID so the caller can surface a structured 400 response.
    """
    if not isinstance(key, str) or not key:
        raise AzureApiError(ErrorCategory.INVALID,
            'idempotency_key is required and must be a non-empty string')
    if len(key) > _IDEMPOTENCY_KEY_MAX:
        raise AzureApiError(ErrorCategory.INVALID,
            f'idempotency_key too long ({len(key)} > {_IDEMPOTENCY_KEY_MAX})')
    if not _IDEMPOTENCY_KEY_RE.match(key):
        raise AzureApiError(ErrorCategory.INVALID,
            'idempotency_key must match DNS-1123 (alphanumeric, `-`, `_`, `.`; '
            'must start and end with alphanumeric)')
    return key.lower()

# Map ErrorCategory -> CLI exit code
_CATEGORY_EXIT = {
    ErrorCategory.TRANSIENT: CLUSTER_ERROR,
    ErrorCategory.CAPACITY: NOT_READY_ERROR,
    ErrorCategory.INVALID: INPUT_ERROR,
    ErrorCategory.AUTH: DEPENDENCY_ERROR,
    ErrorCategory.NOT_FOUND: INPUT_ERROR,
    ErrorCategory.CONFLICT: CLUSTER_ERROR,
    ErrorCategory.PERMANENT: DEPENDENCY_ERROR,
    ErrorCategory.INTERNAL: UNKNOWN_ERROR,
}

# Map SubmitDecision -> CLI exit code (only non-zero ones)
_DECISION_EXIT = {
    SubmitDecision.REJECTED_CAPACITY: NOT_READY_ERROR,
    SubmitDecision.REJECTED_INVALID: INPUT_ERROR,
    SubmitDecision.REJECTED_PERMANENT: DEPENDENCY_ERROR,
}


def is_azure(cfg: ElasticBlastConfig) -> bool:
    """True if the active config targets the Azure CSP."""
    try:
        return cfg.cloud_provider.cloud == CSP.AZURE
    except Exception:  # noqa: BLE001
        return False


def _to_jsonable(obj: Any) -> Any:
    """Convert a dataclass / Enum / nested structure to JSON-safe values."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _to_jsonable(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    return obj


def emit_json(payload: Dict[str, Any]) -> None:
    """Write a single-line JSON object to stdout.

    Consumers (e.g., elb-dashboard's terminal_run) parse the LAST stdout
    line. Keeping it on one line avoids fragile multi-line parsing.

    Broken-pipe safe: if the consumer closed stdout we silently log and
    move on rather than spew a traceback into the parent process.
    """
    line = json.dumps(_to_jsonable(payload), separators=(',', ':'),
                      sort_keys=True, default=str)
    try:
        sys.stdout.write(line + '\n')
        sys.stdout.flush()
    except BrokenPipeError:
        LOGGER.debug('emit_json: stdout pipe closed; payload dropped')
    except (OSError, ValueError) as e:
        # ValueError for closed file; OSError for other I/O failures.
        LOGGER.debug(f'emit_json: write failed ({type(e).__name__}); payload dropped')


def _resolve_correlation_id(args, cfg: ElasticBlastConfig) -> str:
    """Pick the correlation id: CLI flag > existing elb_job_id."""
    cid = getattr(args, 'correlation_id', None)
    if cid:
        return str(cid)
    return cfg.azure.elb_job_id


@contextmanager
def correlation(args, cfg: ElasticBlastConfig) -> Iterator[str]:
    """Bind a correlation scope around a CLI command for Azure runs."""
    if not is_azure(cfg):
        yield ''
        return
    cid = _resolve_correlation_id(args, cfg)
    azure_api.install_correlation_log_filter()
    with azure_api.correlation_scope(cid):
        yield cid


# ---------------------------------------------------------------------------
# Submit
# ---------------------------------------------------------------------------

def _wrap_submit_result(result: SubmitResult) -> Dict[str, Any]:
    return {
        'kind': 'submit_result',
        **_to_jsonable(result),
    }


def _early_idempotency_probe(cfg: ElasticBlastConfig,
                              correlation_id: str) -> Optional[SubmitResult]:
    """Run the cheap idempotency probes BEFORE the heavy submit pipeline.

    Mirrors azure_api.submit_search()'s probes but does so without invoking
    `ElasticBlastAzure(create=True)` or touching the cluster — that lets
    the CLI short-circuit query splitting / config writing / etc.
    """
    cluster_name = cfg.cluster.name
    try:
        if azure_api._has_terminal_marker(cfg, marker='SUCCESS.txt'):
            return SubmitResult(
                decision=SubmitDecision.ALREADY_DONE,
                correlation_id=correlation_id,
                cluster_name=cluster_name,
                message='SUCCESS.txt already present for this idempotency key',
                details={'terminal': 'SUCCESS'})
        if azure_api._has_terminal_marker(cfg, marker='FAILURE.txt'):
            return SubmitResult(
                decision=SubmitDecision.ALREADY_DONE,
                correlation_id=correlation_id,
                cluster_name=cluster_name,
                message='FAILURE.txt already present for this idempotency key',
                details={'terminal': 'FAILURE'})
    except Exception as e:  # noqa: BLE001
        LOGGER.debug(f'idempotency probe failed (advisory): {e}')

    try:
        if azure_api._has_active_submission_jobs(cfg):
            return SubmitResult(
                decision=SubmitDecision.RESUMED,
                correlation_id=correlation_id,
                cluster_name=cluster_name,
                message='Submission already in flight; returning resumed handle')
    except Exception as e:  # noqa: BLE001
        LOGGER.debug(f'active-jobs probe failed (advisory): {e}')

    return None


def submit_command(args, cfg: ElasticBlastConfig, clean_up_stack,
                    *, default_submit) -> int:
    """Replacement for `commands.submit.submit` when Azure JSON mode is on.

    Strategy:
      1. Apply --idempotency-key onto cfg (mutates elb_job_id in-place).
      2. Probe terminal markers / active jobs; if hit, emit JSON and exit.
      3. Otherwise delegate to upstream `default_submit` for the heavy
         lifting (validation, query split, cloud upload, cluster create).
      4. On success: emit ACCEPTED JSON.
      5. On failure: map exception -> JSON error + exit code.
    """
    json_mode = bool(getattr(args, 'json', False))
    idem = getattr(args, 'idempotency_key', None)
    if idem:
        # Validate BEFORE mutating cfg so a bad key never leaks into the
        # cluster as an unsearchable label.
        try:
            normalized = validate_idempotency_key(idem)
        except AzureApiError as e:
            return _emit_error_envelope(e, cfg.azure.elb_job_id, json_mode)
        previous = cfg.azure.elb_job_id
        cfg.azure.elb_job_id = normalized
        # Audit the mutation so postmortems can correlate the key.
        LOGGER.info(
            f'idempotency_key applied: elb_job_id {previous!r} -> {normalized!r}')
    correlation_id = _resolve_correlation_id(args, cfg)

    # Phase 1: short-circuit on idempotency hits (only when JSON mode is
    # active — we don't want to silently change the legacy CLI behavior).
    if json_mode:
        early = _early_idempotency_probe(cfg, correlation_id)
        if early is not None:
            emit_json(_wrap_submit_result(early))
            return 0

    # Phase 2: run the upstream submit pipeline.
    try:
        rc = default_submit(args, cfg, clean_up_stack)
    except AzureApiError as e:
        return _emit_error_envelope(e, correlation_id, json_mode)
    except Exception as e:  # noqa: BLE001
        api_err = azure_api._map_exception(e, correlation_id=correlation_id)
        if json_mode:
            emit_json({'kind': 'error', **api_err.to_dict()})
        # Re-raise so bin/elastic-blast's existing handler logs it.
        raise

    # Phase 3: success -> structured ACCEPTED.
    if json_mode and rc == 0:
        result = SubmitResult(
            decision=SubmitDecision.ACCEPTED,
            correlation_id=correlation_id,
            cluster_name=cfg.cluster.name,
            message='submission accepted')
        emit_json(_wrap_submit_result(result))
    return rc


def _emit_error_envelope(api_err: AzureApiError,
                         correlation_id: str,
                         json_mode: bool) -> int:
    """Emit a JSON error envelope and return a stable exit code.

    Does NOT call sys.exit; the caller decides whether to return or raise.
    """
    if api_err.correlation_id is None:
        api_err.correlation_id = correlation_id
    if json_mode:
        emit_json({'kind': 'error', **api_err.to_dict()})
    LOGGER.error(f'[{api_err.category.value}] {api_err.message}')
    return _CATEGORY_EXIT.get(api_err.category, UNKNOWN_ERROR)


# Backward-compat alias (deprecated naming kept for any external import).
_emit_error_and_exit = _emit_error_envelope


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def status_command(args, cfg: ElasticBlastConfig, clean_up_stack,
                    *, default_status) -> int:
    """Route to azure_api.get_status when --json is set; otherwise default."""
    if not bool(getattr(args, 'json', False)):
        return default_status(args, cfg, clean_up_stack)
    correlation_id = _resolve_correlation_id(args, cfg)
    try:
        result = azure_api.get_status(cfg)
    except AzureApiError as e:
        return _emit_error_envelope(e, correlation_id, True)
    except Exception as e:  # noqa: BLE001
        api_err = azure_api._map_exception(e, correlation_id=correlation_id)
        return _emit_error_envelope(api_err, correlation_id, True)
    emit_json({'kind': 'status_result', **_to_jsonable(result)})
    return 0


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

def delete_command(args, cfg: ElasticBlastConfig, clean_up_stack,
                    *, default_delete) -> int:
    """Route to azure_api.delete_search when --json is set.

    `--force` is honored in either path: when JSON mode is off we still
    pass force= via the explicit kwarg by setting ELB_FORCE_DELETE for
    backward compat with the upstream delete code path.
    """
    json_mode = bool(getattr(args, 'json', False))
    force = bool(getattr(args, 'force', False))

    if not json_mode:
        # Non-JSON path: still honor --force without touching upstream code.
        # (delete_cluster_with_cleanup checks ELB_FORCE_DELETE for legacy
        # callers; this preserves single-process semantics.)
        import os
        if force:
            os.environ['ELB_FORCE_DELETE'] = '1'
        try:
            return default_delete(args, cfg, clean_up_stack)
        finally:
            if force:
                os.environ.pop('ELB_FORCE_DELETE', None)

    correlation_id = _resolve_correlation_id(args, cfg)
    try:
        result = azure_api.delete_search(cfg, force=force)
    except AzureApiError as e:
        return _emit_error_envelope(e, correlation_id, True)
    except Exception as e:  # noqa: BLE001
        api_err = azure_api._map_exception(e, correlation_id=correlation_id)
        return _emit_error_envelope(api_err, correlation_id, True)
    emit_json({'kind': 'delete_result', **result})
    return 0


# ---------------------------------------------------------------------------
# Capacity / health (read-only sub-actions exposed via JSON mode)
# ---------------------------------------------------------------------------

def capacity_command(cfg: ElasticBlastConfig) -> int:
    """Emit a CapacityReport as JSON; never raises."""
    correlation_id = _resolve_correlation_id_safe(cfg)
    try:
        report = azure_api.check_capacity(cfg)
    except AzureApiError as e:
        return _emit_error_envelope(e, correlation_id, True)
    except Exception as e:  # noqa: BLE001
        api_err = azure_api._map_exception(e, correlation_id=correlation_id)
        return _emit_error_envelope(api_err, correlation_id, True)
    emit_json({'kind': 'capacity_report', **_to_jsonable(report)})
    return 0


def health_command() -> int:
    """Emit a HealthReport as JSON; never raises."""
    try:
        report = azure_api.health_check()
    except Exception as e:  # noqa: BLE001
        emit_json({'kind': 'error', 'category': 'internal',
                   'message': f'{type(e).__name__}: {e}',
                   'retry_after_seconds': None,
                   'details': {}, 'correlation_id': None})
        return UNKNOWN_ERROR
    emit_json({'kind': 'health_report', **_to_jsonable(report)})
    return 0


def _resolve_correlation_id_safe(cfg: ElasticBlastConfig) -> str:
    try:
        return cfg.azure.elb_job_id
    except Exception:  # noqa: BLE001
        return ''
