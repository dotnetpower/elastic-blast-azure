"""Cross-path submit coordination for the elb-openapi service (Phase 1).

The dashboard control plane (``elb-dashboard``) and this OpenAPI service both
run ``elastic-blast submit`` against the *same* AKS cluster + ``default``
namespace. Each one only serialises submits within its own process (the
dashboard via a Redis lock, this service via the in-memory
``MAX_ACTIVE_SUBMISSIONS`` dispatcher) — neither lock is visible to the other,
so the two paths can run ``submit`` simultaneously and race on the shared
ServiceAccount / Secret / PVC / Job objects and clobber the shared
``elb-scripts`` ConfigMap.

This module closes that gap with a cluster-visible coordinator that BOTH repos
share, mirroring ``elb-dashboard``'s
``api/services/k8s/submit_lease.py`` + ``api/services/k8s/blast_status.py``
contract exactly so the same "3 concurrent" ceiling is enforced from both
sides:

* **Gate A — Lease mutex.** A ``coordination.k8s.io/v1`` Lease named
  ``elb-blast-submit-default`` (one per namespace) is acquired *before* the
  ``elastic-blast submit`` critical section and released *after* it. Acquire
  uses ``resourceVersion`` CAS so exactly one of two racing acquirers wins;
  the loser sees BUSY and retries. Release is conditional (only if we still
  hold it) so a submit that overran the TTL cannot clobber a newer holder.

* **Gate B — run-concurrency ceiling.** While holding Gate A, count the
  distinct ``elb-job-id`` values among non-terminal ``app=finalizer`` Jobs
  in the SAME (``default``) namespace the Lease locks — NOT cluster-wide
  (a fresh, uncached read). Gate A and Gate B MUST scope to the identical
  namespace or the ceiling is enforced against a different population than it
  serialises. If the count is already at ``BLAST_MAX_RUN_CONCURRENCY`` the slot
  is full → release Gate A and wait.

Everything here speaks ``kubectl`` via ``subprocess`` to match the rest of the
OpenAPI service (which already manages ConfigMaps/Jobs through ``kubectl``).
The Lease and the Gate-B count are both pinned to the ``default`` namespace to
match the dashboard's pinned namespace — the two paths MUST count the same
population or "3" becomes fiction.

Coordination is **OFF by default** (``BLAST_COORD_BACKEND`` unset / ``redis``)
so this is an additive, no-op change until both repos are deployed and the env
flag is flipped (sibling first — see the rollout order in this repo's design
doc ``docs/submit-coordination.md`` and the cross-repo tracking issue
dotnetpower/elastic-blast-azure#1).

Env knobs (identical names + defaults to ``elb-dashboard``):
    BLAST_COORD_BACKEND            redis   (k8s enables this coordinator)
    BLAST_MAX_RUN_CONCURRENCY      3
    BLAST_SUBMIT_LEASE_TTL_SECONDS 900
    BLAST_LEASE_CLOCK_SKEW_SECONDS 30
    BLAST_FINALIZER_GRACE_SECONDS  300
    BLAST_CAPACITY_WAIT_MAX_SECONDS 1800

Deployment requirement: the OpenAPI pod's ServiceAccount must be able to
``get``/``create``/``update`` ``coordination.k8s.io`` Leases and ``list``
``batch`` Jobs in the ``default`` namespace. A 403 here is surfaced loudly
(fail-closed) rather than silently retried.

Validation: ``cd docker-openapi && python -m pytest tests/test_submit_coordination.py -q``.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event
from typing import Any, Optional

logger = logging.getLogger("elb-openapi")

UTC = timezone.utc

# ── Contract constants (MUST match elb-dashboard) ──────────────────────────
# api/services/blast/coordination.py: SUBMIT_COORDINATION_NAMESPACE = "default"
_NAMESPACE = "default"
# api/services/k8s/submit_lease.py: submit_lease_name(ns) -> "elb-blast-submit-<ns>"
_LEASE_NAME_PREFIX = "elb-blast-submit"
_LEASE_API_GROUP = "coordination.k8s.io/v1"
# api/services/blast/coordination.py
_FINALIZER_APP = "finalizer"  # FINALIZER_LABEL_SELECTOR = "app=finalizer"
_COMPANION_APPS = ("blast", "submit")  # sorted({"submit","blast"}) for selector parity
_JOB_ID_LABEL = "elb-job-id"  # FINALIZER_JOB_ID_LABEL

# ── Defaults (MUST match elb-dashboard) ────────────────────────────────────
_DEFAULT_MAX_RUN_CONCURRENCY = 3
_DEFAULT_LEASE_TTL_SECONDS = 900
_DEFAULT_LEASE_CLOCK_SKEW_SECONDS = 30
_DEFAULT_FINALIZER_GRACE_SECONDS = 300
_DEFAULT_CAPACITY_WAIT_MAX_SECONDS = 1800

# How often the capacity wait re-checks Gate A / Gate B while blocked.
_SLOT_POLL_SECONDS = 5.0


class SubmitSlotBusyTimeout(Exception):
    """No run slot acquired within the capacity wait budget → caller requeues.

    NOT a failure: the cluster is simply at the concurrency ceiling (or another
    path holds the Lease). The dispatcher re-claims the job on its next tick.
    """


class SubmitCoordinationError(Exception):
    """A genuine coordination/API failure (RBAC, transport, malformed JSON).

    Fail-closed: the caller treats this as a submit failure rather than
    silently proceeding, because proceeding would bypass the cross-path mutex.
    """


@dataclass(frozen=True)
class RunSlot:
    """Handle for an acquired Gate-A Lease, passed back to release it."""

    name: str
    namespace: str
    holder: str


# ── Env helpers (read each call so monkeypatch/runtime flips take effect) ──
def _int_env(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


def coordination_backend() -> str:
    raw = (os.environ.get("BLAST_COORD_BACKEND") or "").strip().lower()
    return "k8s" if raw == "k8s" else "redis"


def coordination_enabled() -> bool:
    return coordination_backend() == "k8s"


def max_run_concurrency() -> int:
    return _int_env("BLAST_MAX_RUN_CONCURRENCY", _DEFAULT_MAX_RUN_CONCURRENCY, minimum=1)


def lease_ttl_seconds() -> int:
    return _int_env("BLAST_SUBMIT_LEASE_TTL_SECONDS", _DEFAULT_LEASE_TTL_SECONDS, minimum=1)


def lease_clock_skew_seconds() -> int:
    return _int_env("BLAST_LEASE_CLOCK_SKEW_SECONDS", _DEFAULT_LEASE_CLOCK_SKEW_SECONDS, minimum=0)


def finalizer_grace_seconds() -> int:
    return _int_env("BLAST_FINALIZER_GRACE_SECONDS", _DEFAULT_FINALIZER_GRACE_SECONDS, minimum=0)


def capacity_wait_max_seconds() -> int:
    return _int_env(
        "BLAST_CAPACITY_WAIT_MAX_SECONDS", _DEFAULT_CAPACITY_WAIT_MAX_SECONDS, minimum=0
    )


def lease_name(namespace: str = _NAMESPACE) -> str:
    return f"{_LEASE_NAME_PREFIX}-{namespace}"


def new_holder_identity(source: str = "openapi") -> str:
    return f"{source}-{uuid.uuid4().hex}"


# ── Time helpers (MUST match elb-dashboard's MicroTime format) ─────────────
def _now() -> datetime:
    return datetime.now(UTC)


def _micro_time(value: datetime) -> str:
    """Render a Kubernetes ``MicroTime`` (RFC3339 with microseconds, ``Z``)."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def _parse_k8s_time(raw: Any) -> Optional[datetime]:
    if not isinstance(raw, str) or not raw:
        return None
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _is_expired(spec: dict[str, Any], now: datetime, skew: int) -> bool:
    """Mirror elb-dashboard submit_lease._is_expired (fail-closed on unparseable).

    A Lease is expired only when ``now > renewTime + leaseDurationSeconds + skew``.
    A present-but-unparseable ``renewTime`` is treated as STILL HELD (we cannot
    prove it dead, so taking it over would risk a concurrent submit). A missing
    ``renewTime`` field means the Lease was never stamped → available.
    """
    renew_raw = spec.get("renewTime")
    renew = _parse_k8s_time(renew_raw)
    if renew is None:
        if isinstance(renew_raw, str) and renew_raw.strip():
            logger.info("submit lease renewTime unparseable; treating as held: %r", renew_raw)
            return False
        return True
    try:
        ttl = int(spec.get("leaseDurationSeconds") or lease_ttl_seconds())
    except (TypeError, ValueError):
        ttl = lease_ttl_seconds()
    elapsed = (now - renew).total_seconds()
    return elapsed > (ttl + skew)


# ── kubectl plumbing ───────────────────────────────────────────────────────
def _kubectl(
    args: list[str], *, input_text: Optional[str] = None, timeout: float = 15.0
) -> subprocess.CompletedProcess:
    """Run ``kubectl`` and return the CompletedProcess WITHOUT raising on non-zero.

    Callers inspect ``returncode``/``stderr`` to distinguish BUSY (AlreadyExists /
    CAS conflict) from genuine errors, so this deliberately does not use
    ``safe_exec`` (which raises on non-zero).
    """
    try:
        return subprocess.run(
            ["kubectl", *args],
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise SubmitCoordinationError(f"kubectl timed out: {' '.join(args)}") from exc
    except Exception as exc:  # pragma: no cover - defensive
        raise SubmitCoordinationError(f"kubectl exec error: {type(exc).__name__}: {exc}") from exc


def _is_conflict(stderr: str) -> bool:
    low = stderr.lower()
    return (
        "operation cannot be fulfilled" in low
        or "please apply your changes to the latest version" in low
        or "conflict" in low
    )


def _is_already_exists(stderr: str) -> bool:
    low = stderr.lower()
    return "alreadyexists" in low or "already exists" in low


def _is_not_found(stderr: str) -> bool:
    low = stderr.lower()
    return "notfound" in low or "not found" in low


def _lease_body(name: str, namespace: str, holder: str, ttl: int) -> dict[str, Any]:
    stamp = _micro_time(_now())
    return {
        "apiVersion": _LEASE_API_GROUP,
        "kind": "Lease",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "holderIdentity": holder,
            "leaseDurationSeconds": ttl,
            "acquireTime": stamp,
            "renewTime": stamp,
        },
    }


def _get_lease(name: str, namespace: str) -> Optional[dict[str, Any]]:
    proc = _kubectl(
        ["get", "lease", name, "-n", namespace, "--ignore-not-found", "-o", "json"]
    )
    if proc.returncode != 0:
        raise SubmitCoordinationError(
            f"lease GET failed ({proc.returncode}): {proc.stderr.strip()[:200]} — "
            "check the OpenAPI ServiceAccount can manage coordination.k8s.io Leases"
        )
    text = (proc.stdout or "").strip()
    if not text:
        return None  # --ignore-not-found returns empty when the Lease is absent
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SubmitCoordinationError(f"lease GET returned non-JSON: {exc}") from exc


def _create_lease(name: str, namespace: str, holder: str, ttl: int) -> Optional[RunSlot]:
    body = json.dumps(_lease_body(name, namespace, holder, ttl))
    proc = _kubectl(["create", "-f", "-"], input_text=body)
    if proc.returncode == 0:
        return RunSlot(name=name, namespace=namespace, holder=holder)
    if _is_already_exists(proc.stderr):
        return None  # someone created it first → BUSY, re-read next attempt
    raise SubmitCoordinationError(
        f"lease CREATE failed ({proc.returncode}): {proc.stderr.strip()[:200]}"
    )


def _replace_lease(
    name: str, namespace: str, holder: str, ttl: int, resource_version: Any
) -> Optional[RunSlot]:
    body = _lease_body(name, namespace, holder, ttl)
    if resource_version:
        body["metadata"]["resourceVersion"] = str(resource_version)
    proc = _kubectl(["replace", "-f", "-"], input_text=json.dumps(body))
    if proc.returncode == 0:
        return RunSlot(name=name, namespace=namespace, holder=holder)
    if _is_conflict(proc.stderr) or _is_not_found(proc.stderr):
        return None  # lost the CAS race / Lease vanished → BUSY, retry
    raise SubmitCoordinationError(
        f"lease PUT failed ({proc.returncode}): {proc.stderr.strip()[:200]}"
    )


def _acquire_lease_once(
    name: str, namespace: str, holder: str, ttl: int, skew: int
) -> Optional[RunSlot]:
    """One Gate-A attempt. Returns a RunSlot, or None if BUSY (live other holder)."""
    lease = _get_lease(name, namespace)
    if lease is None:
        return _create_lease(name, namespace, holder, ttl)
    spec = lease.get("spec") or {}
    current_holder = str(spec.get("holderIdentity") or "")
    resource_version = (lease.get("metadata") or {}).get("resourceVersion")
    now = _now()
    if current_holder == holder:
        return _replace_lease(name, namespace, holder, ttl, resource_version)  # renew
    if not current_holder or _is_expired(spec, now, skew):
        return _replace_lease(name, namespace, holder, ttl, resource_version)  # CAS takeover
    return None  # live holder, not us → BUSY


def _release_lease(slot: RunSlot) -> None:
    """Conditionally clear the Lease — only if we still hold it (best-effort)."""
    for _attempt in range(2):
        try:
            lease = _get_lease(slot.name, slot.namespace)
        except SubmitCoordinationError as exc:
            logger.info("submit lease release skipped: %s", str(exc)[:120])
            return
        if lease is None:
            return
        spec = lease.get("spec") or {}
        if str(spec.get("holderIdentity") or "") != slot.holder:
            return  # newer holder took over — do NOT clobber
        metadata = lease.get("metadata") or {}
        body = {
            "apiVersion": _LEASE_API_GROUP,
            "kind": "Lease",
            "metadata": {
                "name": slot.name,
                "namespace": slot.namespace,
                "resourceVersion": metadata.get("resourceVersion"),
            },
            "spec": {
                "holderIdentity": "",
                "leaseDurationSeconds": spec.get("leaseDurationSeconds"),
                "acquireTime": spec.get("acquireTime"),
                "renewTime": spec.get("renewTime"),
            },
        }
        proc = _kubectl(["replace", "-f", "-"], input_text=json.dumps(body))
        if proc.returncode == 0:
            return
        if _is_conflict(proc.stderr):
            continue  # someone modified it; re-GET and re-check holder once
        logger.info("submit lease release skipped: PUT %s", proc.stderr.strip()[:120])
        return


# ── Gate B — run-concurrency ceiling ───────────────────────────────────────
def _job_label(job: dict[str, Any], name: str) -> str:
    return str(((job.get("metadata", {}) or {}).get("labels", {}) or {}).get(name) or "")


def _finalizer_is_terminal(job: dict[str, Any]) -> bool:
    """Fail-closed terminal test, identical to elb-dashboard blast_status.

    Terminal ONLY when ``succeeded > 0`` OR ``completionTime`` set OR a
    ``Complete``/``Failed`` condition is ``status == "True"``. A bare
    ``failed > 0`` is treated as STILL ACTIVE (the Job controller is still
    retrying), the fail-closed direction for an admission gate.
    """
    status = job.get("status", {}) or {}
    if not isinstance(status, dict):
        return False
    try:
        if int(status.get("succeeded", 0) or 0) > 0:
            return True
    except (TypeError, ValueError):
        return False
    if status.get("completionTime"):
        return True
    conditions = status.get("conditions", []) or []
    if isinstance(conditions, list):
        for cond in conditions:
            if not isinstance(cond, dict):
                continue
            if cond.get("type") in ("Complete", "Failed") and (
                str(cond.get("status", "")).lower() == "true"
            ):
                return True
    return False


def _job_age_seconds(job: dict[str, Any], now: float) -> Optional[float]:
    raw = (job.get("metadata", {}) or {}).get("creationTimestamp")
    if not raw:
        return None
    parsed = _parse_k8s_time(raw)
    if parsed is None:
        return None
    return max(0.0, now - parsed.timestamp())


def count_active_blast_submissions(namespace: str = _NAMESPACE) -> int:
    """Gate B count — distinct active submit units (mirrors elb-dashboard).

    Counts distinct ``elb-job-id`` among NON-TERMINAL ``app=finalizer`` Jobs
    (one finalizer per submit). Fresh/uncached read (admission decision).
    Fail-closed: a read failure RAISES so the caller releases Gate A and waits,
    never returning a low count that over-admits.

    Phantom guard: a lone finalizer (no live ``app=submit``/``app=blast``
    companion) older than ``BLAST_FINALIZER_GRACE_SECONDS`` is an orphaned
    phantom and NOT counted; younger ones are counted to cover the async
    ``app=blast`` batch-creation lag.
    """
    selector = f"app in ({_FINALIZER_APP},{','.join(_COMPANION_APPS)})"
    proc = _kubectl(
        ["get", "jobs", "-n", namespace, "-l", selector, "-o", "json"], timeout=15.0
    )
    if proc.returncode != 0:
        raise SubmitCoordinationError(
            f"gate B jobs list failed ({proc.returncode}): {proc.stderr.strip()[:200]}"
        )
    try:
        items = json.loads(proc.stdout or "{}").get("items", []) or []
    except json.JSONDecodeError as exc:
        raise SubmitCoordinationError(f"gate B jobs list non-JSON: {exc}") from exc

    import time as _time

    now = _time.time()
    grace = finalizer_grace_seconds()
    companion_apps = set(_COMPANION_APPS)
    live_companion_ids: set[str] = set()
    finalizers: list[dict[str, Any]] = []
    for job in items:
        app = _job_label(job, "app")
        if _finalizer_is_terminal(job):
            continue
        if app == _FINALIZER_APP:
            finalizers.append(job)
        elif app in companion_apps:
            job_id = _job_label(job, _JOB_ID_LABEL)
            if job_id:
                live_companion_ids.add(job_id)

    counted: set[str] = set()
    for job in finalizers:
        job_id = _job_label(job, _JOB_ID_LABEL)
        if not job_id:
            synthetic = (job.get("metadata", {}) or {}).get("uid") or (
                job.get("metadata", {}) or {}
            ).get("name")
            counted.add(f"__unlabeled__:{synthetic or id(job)}")
            continue
        if job_id in counted:
            continue
        if job_id in live_companion_ids:
            counted.add(job_id)
            continue
        age = _job_age_seconds(job, now)
        if age is None or age < grace:
            counted.add(job_id)  # within async-creation grace → still a live slot
        # else: companion-less + past grace → phantom, not counted
    return len(counted)


# ── Capacity wait (interruptible) ──────────────────────────────────────────
def _slot_wait(stop_event: Optional[Event], seconds: float) -> None:
    """Sleep ``seconds`` but wake early if ``stop_event`` is set (cancel)."""
    if stop_event is not None:
        stop_event.wait(seconds)
    else:  # pragma: no cover - exercised indirectly
        import time as _time

        _time.sleep(seconds)


# ── Public API ─────────────────────────────────────────────────────────────
def acquire_run_slot(
    job_id: str, *, stop_event: Optional[Event] = None, source: str = "openapi"
) -> Optional[RunSlot]:
    """Acquire a cross-path run slot before ``elastic-blast submit``.

    Returns ``None`` when coordination is disabled (the caller submits without
    the gate, preserving legacy behaviour). When enabled, returns a
    :class:`RunSlot` to release after the submit critical section, or raises:

    * :class:`SubmitSlotBusyTimeout` — Lease busy / capacity full past the
      ``BLAST_CAPACITY_WAIT_MAX_SECONDS`` budget → caller requeues (not a
      failure).
    * :class:`SubmitCoordinationError` — RBAC / API / cancellation → caller
      fails (or cancels) the job fail-closed.
    """
    if not coordination_enabled():
        return None

    holder = new_holder_identity(source)
    name = lease_name(_NAMESPACE)
    ttl = lease_ttl_seconds()
    skew = lease_clock_skew_seconds()
    ceiling = max_run_concurrency()
    budget = capacity_wait_max_seconds()

    import time as _time

    deadline = _time.monotonic() + budget
    while True:
        if stop_event is not None and stop_event.is_set():
            raise SubmitCoordinationError("cancelled during capacity wait")

        slot = _acquire_lease_once(name, _NAMESPACE, holder, ttl, skew)
        if slot is not None:
            # Gate A held — evaluate Gate B WHILE holding it (fresh read).
            try:
                active = count_active_blast_submissions(_NAMESPACE)
            except Exception:
                _release_lease(slot)  # fail-closed: never hold the Lease on error
                raise
            if active < ceiling:
                logger.info(
                    "run slot acquired job=%s holder=%s gateB=%d/%d",
                    job_id,
                    holder,
                    active,
                    ceiling,
                )
                return slot
            # Capacity full — release Gate A so another waiter can progress.
            _release_lease(slot)
            logger.info(
                "run slot full job=%s gateB=%d/%d — waiting", job_id, active, ceiling
            )

        if _time.monotonic() >= deadline:
            raise SubmitSlotBusyTimeout(
                f"no run slot within {budget}s (lease busy or capacity at {ceiling})"
            )
        _slot_wait(stop_event, _SLOT_POLL_SECONDS)


def release_run_slot(slot: Optional[RunSlot]) -> None:
    """Release a slot acquired by :func:`acquire_run_slot` (no-op if ``None``)."""
    if slot is None:
        return
    try:
        _release_lease(slot)
    except Exception as exc:  # pragma: no cover - release is best-effort
        logger.info("run slot release skipped: %s", type(exc).__name__)
