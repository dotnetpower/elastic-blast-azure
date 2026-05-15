"""
elastic_blast/azure_api_types.py — Public API types for Azure/AKS submissions

Designed for use both from the CLI and from external services
(e.g., elb-dashboard) calling ElasticBLAST as an API. All exceptions and
results are structured so that callers can:
  * distinguish transient vs. permanent failures
  * decide whether to retry, when to retry, and at what rate
  * trace a request end-to-end via correlation IDs
  * expose meaningful errors to end users without leaking internals

ISO/IEC 25010 reliability characteristics targeted:
  - Maturity:        explicit error categories, no silent swallow
  - Availability:    capacity reports + retry-after hints
  - Fault tolerance: idempotency, structured retry semantics
  - Recoverability:  resumable submissions keyed by elb_job_id

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

class ErrorCategory(str, Enum):
    """Coarse error categories used by the API surface.

    Callers map these to HTTP status codes / retry policies:
        TRANSIENT   -> 503, retry with backoff
        CAPACITY    -> 429, retry after `retry_after_seconds`
        INVALID     -> 400, do not retry
        AUTH        -> 401/403, do not retry
        NOT_FOUND   -> 404, do not retry
        CONFLICT    -> 409, caller must reconcile state
        PERMANENT   -> 500, do not retry without intervention
        INTERNAL    -> 500, our bug, do not retry without intervention
    """
    TRANSIENT = 'transient'
    CAPACITY = 'capacity'
    INVALID = 'invalid'
    AUTH = 'auth'
    NOT_FOUND = 'not_found'
    CONFLICT = 'conflict'
    PERMANENT = 'permanent'
    INTERNAL = 'internal'


class AzureApiError(Exception):
    """Structured exception raised by the Azure API surface.

    Attributes:
        category:           ErrorCategory for retry decisions.
        message:            Human-readable summary, safe to surface to users.
        retry_after_seconds: Hint to the caller; None means "do not retry".
        details:            Machine-readable extras (cluster name, quota, etc.).
        correlation_id:     elb_job_id or generated request id.
        cause:              Original exception, if any.
    """

    def __init__(self, category: ErrorCategory, message: str, *,
                 retry_after_seconds: Optional[int] = None,
                 details: Optional[Dict[str, Any]] = None,
                 correlation_id: Optional[str] = None,
                 cause: Optional[BaseException] = None):
        super().__init__(message)
        self.category = category
        self.message = message
        self.retry_after_seconds = retry_after_seconds
        self.details = details or {}
        self.correlation_id = correlation_id
        self.cause = cause

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for transport (e.g., HTTP response body)."""
        return {
            'category': self.category.value,
            'message': self.message,
            'retry_after_seconds': self.retry_after_seconds,
            'details': self.details,
            'correlation_id': self.correlation_id,
        }


# ---------------------------------------------------------------------------
# Submission lifecycle
# ---------------------------------------------------------------------------

class SubmitDecision(str, Enum):
    """The disposition of a submit() call from the API server's perspective."""

    ACCEPTED = 'accepted'
    """Request was accepted and will run; poll for status."""

    RESUMED = 'resumed'
    """Request matched an existing in-progress submission (idempotent retry)."""

    ALREADY_DONE = 'already_done'
    """A previous submission with this idempotency key is already complete."""

    QUEUED = 'queued'
    """Capacity gate is full; request is buffered. Poll for status."""

    REJECTED_CAPACITY = 'rejected_capacity'
    """Capacity gate is full beyond the queue limit; client must retry later."""

    REJECTED_INVALID = 'rejected_invalid'
    """Request is malformed; do not retry without changes."""

    REJECTED_PERMANENT = 'rejected_permanent'
    """Cluster/region/quota cannot serve this request; do not retry as-is."""


@dataclass
class SubmitResult:
    """Structured outcome of submit_search().

    `status_url` is a hint for callers that want to provide a polling URL
    to end users; it is opaque (the API server constructs the actual URL).
    """
    decision: SubmitDecision
    correlation_id: str            # elb_job_id used for the submission
    cluster_name: str
    message: str = ''
    retry_after_seconds: Optional[int] = None
    queue_position: Optional[int] = None
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.decision in (SubmitDecision.REJECTED_CAPACITY,
                                  SubmitDecision.REJECTED_INVALID,
                                  SubmitDecision.REJECTED_PERMANENT,
                                  SubmitDecision.ALREADY_DONE)


# ---------------------------------------------------------------------------
# Status reporting
# ---------------------------------------------------------------------------

class SearchPhase(str, Enum):
    """Coarse lifecycle phases reported to API callers."""
    PENDING = 'pending'              # accepted but cluster not yet ready
    INITIALIZING = 'initializing'    # cluster provisioning / DB download
    RUNNING = 'running'              # at least one BLAST job active
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    UNKNOWN = 'unknown'              # we cannot determine state right now


@dataclass
class StatusResult:
    correlation_id: str
    phase: SearchPhase
    pending: int = 0
    running: int = 0
    succeeded: int = 0
    failed: int = 0
    message: str = ''
    cluster_provisioning_state: str = ''
    results_uri: str = ''
    details: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Capacity reporting
# ---------------------------------------------------------------------------

class CapacityVerdict(str, Enum):
    AVAILABLE = 'available'
    """Capacity is available right now."""

    DEGRADED = 'degraded'
    """Capacity is tight; submission may queue or be slow."""

    EXHAUSTED_RETRY_LATER = 'exhausted_retry_later'
    """No capacity now, but is expected to recover. Retry after hint."""

    EXHAUSTED_INTERVENTION_REQUIRED = 'exhausted_intervention_required'
    """Hard quota / region constraint; client must change region or quota
    before retrying."""

    UNKNOWN = 'unknown'
    """We could not determine capacity (Azure SDK error, etc.)."""


@dataclass
class CapacityReport:
    verdict: CapacityVerdict
    region: str
    cluster_name: str
    cluster_state: str = ''
    active_submissions: int = 0          # other elb-job-id Jobs running
    queue_depth: int = 0                 # in-process backpressure queue
    max_concurrent: int = 0              # in-process semaphore limit
    available_cores: Optional[int] = None
    quota_limit_cores: Optional[int] = None
    quota_used_cores: Optional[int] = None
    retry_after_seconds: Optional[int] = None
    reasons: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class HealthStatus(str, Enum):
    OK = 'ok'
    DEGRADED = 'degraded'
    UNHEALTHY = 'unhealthy'


@dataclass
class HealthReport:
    status: HealthStatus
    checks: Dict[str, bool] = field(default_factory=dict)   # check_name -> pass/fail
    messages: Dict[str, str] = field(default_factory=dict)  # check_name -> detail
