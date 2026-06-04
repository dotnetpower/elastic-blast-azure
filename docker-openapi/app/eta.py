"""Self-learning per-job ETA for the elb-openapi BLAST queue (build-context overlay).

Responsibility: Predict per-job start/finish times for queued and running BLAST
jobs by (a) learning the run time of a job online, keyed on (db, query-size
bucket, cold/warm), and (b) simulating the C-server submission queue to translate
that per-job estimate into queue-aware start/finish projections.
Edit boundaries: This file is copied verbatim into the sibling docker-openapi
build context as ``app/eta.py`` by ``scripts/dev/patch-openapi-build-context.py``.
It must stay import-safe with no hard dependency on Azure SDKs (lazy import +
in-memory fallback) and must never raise into the request path — every public
entry point swallows its own errors and degrades to "no ETA".
Key entry points: ``parse_query_features``, ``record_sample``, ``compute_eta``,
``enabled``.
Risky contracts: ``compute_eta`` consumes a snapshot of the openapi ``_jobs``
dict (list of job dicts) and the integer server count ``C`` (=
MAX_ACTIVE_SUBMISSIONS); it returns ``None`` when ETA is disabled or the target
is terminal. The learning store is an aggregate-per-(db,bucket,cold/warm) row, so
reads/writes are O(1) and survive pod restarts via Azure Table.
Validation: ``uv run python -m pytest scripts/dev/openapi-overlays/test_eta.py``.
"""

# ruff: noqa: S110
#   try/except/pass is deliberate: this overlay must never raise into the
#   openapi request path; learning-store and table failures degrade silently.

from __future__ import annotations

import heapq
import logging
import math
import os
import threading
import time
from datetime import UTC, datetime
from typing import Any

_logger = logging.getLogger("eta")

# ── Tunables (all env-overridable, all default-safe) ────────────────────────

_TRUE = {"1", "true", "yes", "on"}


def enabled() -> bool:
    """ETA is strictly opt-in. Unset => byte-identical legacy behaviour."""
    return os.environ.get("ELB_OPENAPI_ETA_ENABLED", "").strip().lower() in _TRUE


# Conservative bias: predict the ~p65 of the learned distribution so consumers
# tend to finish *earlier* than promised rather than later.
_P65_Z = float(os.environ.get("ELB_OPENAPI_ETA_BIAS_Z", "0.385"))

# Below this many samples a bucket is "cold" and we fall back to the coarser
# estimate (warm-only, then db default).
_MIN_SAMPLES = max(1, int(os.environ.get("ELB_OPENAPI_ETA_MIN_SAMPLES", "3")))

# Adaptive EWMA window: early on behaves like a plain running mean, later like an
# exponential moving average so the estimate keeps tracking drift.
_EWMA_WINDOW = max(2, int(os.environ.get("ELB_OPENAPI_ETA_EWMA_WINDOW", "30")))

# If no other job was active in the prior gap, the cluster may have been
# auto-stopped/idle => the job pays node spin-up. Learned separately (cold pk).
_COLD_GAP_SECONDS = max(60, int(os.environ.get("ELB_OPENAPI_ETA_COLD_GAP_SECONDS", "600")))

# Per-db fallback when a bucket has too few samples to trust.
_DEFAULT_RUN_SECONDS = {"core_nt": 110.0}
_GLOBAL_DEFAULT_RUN_SECONDS = float(
    os.environ.get("ELB_OPENAPI_ETA_DEFAULT_RUN_SECONDS", "120")
)

_ACTIVE_STATES = {"dispatching", "submitting", "running"}
_QUEUED_STATES = {"queued"}

_TABLE_NAME = os.environ.get("ELB_OPENAPI_ETA_TABLE", "elbopenapietastats")

# Cross-replica freshness: a cached row older than this is re-read from the
# Table on the next access so a second openapi replica's learning propagates.
_CACHE_TTL_SECONDS = max(0, int(os.environ.get("ELB_OPENAPI_ETA_CACHE_TTL_SECONDS", "15")))

# Bounded retries for the ETag optimistic-merge update (cross-replica safe).
_UPDATE_MAX_RETRIES = max(1, int(os.environ.get("ELB_OPENAPI_ETA_UPDATE_RETRIES", "4")))


# ── Time helpers ────────────────────────────────────────────────────────────


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except (ValueError, TypeError):
        return None


def _seconds_between(start: Any, end: Any) -> float | None:
    a, b = _parse_iso(start), _parse_iso(end)
    if a is None or b is None:
        return None
    return max(0.0, (b - a).total_seconds())


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


# ── Feature extraction ──────────────────────────────────────────────────────


def parse_query_features(query_fasta: str | None) -> tuple[int, int]:
    """Return (sequence_count, total_residues) from an inline FASTA string.

    Best-effort and cheap. Mode A (blob-only) submissions pass ``None`` => (0, 0)
    which maps to the ``unknown`` bucket and the db-level fallback estimate.
    """
    if not query_fasta:
        return (0, 0)
    seqs = 0
    bases = 0
    for line in query_fasta.splitlines():
        if not line:
            continue
        if line[0] == ">":
            seqs += 1
        else:
            bases += len(line.strip())
    if seqs == 0 and bases > 0:
        seqs = 1
    return (seqs, bases)


def _bases_bucket(bases: int) -> str:
    """Coarse log-ish buckets so nearby query sizes share learning."""
    if bases <= 0:
        return "u"
    for hi, label in ((1_000, "b0"), (10_000, "b1"), (100_000, "b2"), (1_000_000, "b3")):
        if bases < hi:
            return label
    return "b4"


def _seqs_bucket(seqs: int) -> str:
    if seqs <= 0:
        return "u"
    for hi, label in ((2, "s1"), (10, "s2"), (100, "s3"), (1_000, "s4")):
        if seqs < hi:
            return label
    return "s5"


def bucket_for(seqs: int, bases: int) -> str:
    return f"{_seqs_bucket(seqs)}.{_bases_bucket(bases)}"


def _partition_key(db: str, bucket: str, cold: bool) -> str:
    state = "cold" if cold else "warm"
    safe_db = (db or "unknown").replace("|", "_")
    return f"{safe_db}|{bucket}|{state}"


# ── Learning store (aggregate row per (db, bucket, cold/warm)) ──────────────


class _Store:
    """O(1) online aggregate store. Azure Table when available, else in-memory.

    Each row holds an adaptive EWMA mean and variance plus a sample count. The
    store never raises into callers — table failures silently fall back to the
    in-memory mirror so the request path is unaffected.

    Cross-replica safety: with more than one openapi replica every ``update``
    re-reads the Table row and writes back with an ETag ``match_condition`` so
    two replicas merging samples for the same (db, bucket, cold/warm) key never
    clobber each other (bounded optimistic-merge retries). Reads keep a short
    TTL so a peer replica's learning propagates within ``_CACHE_TTL_SECONDS``.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._mem: dict[str, dict[str, float]] = {}
        self._mem_ts: dict[str, float] = {}
        self._client: Any = None
        self._client_tried = False

    # -- table client (lazy, optional) --
    def _table(self) -> Any:
        if self._client_tried:
            return self._client
        self._client_tried = True
        conn = os.environ.get("ELB_OPENAPI_ETA_TABLE_CONN") or os.environ.get(
            "AZURE_STORAGE_CONNECTION_STRING"
        )
        account = os.environ.get("STORAGE_ACCOUNT") or os.environ.get(
            "AZURE_STORAGE_ACCOUNT"
        )
        try:
            from azure.data.tables import TableServiceClient  # type: ignore

            if conn:
                svc = TableServiceClient.from_connection_string(conn)
            elif account:
                from azure.identity import DefaultAzureCredential  # type: ignore

                svc = TableServiceClient(
                    endpoint=f"https://{account}.table.core.windows.net",
                    credential=DefaultAzureCredential(),
                )
            else:
                # ETA is enabled but no Table backing is configured: learning is
                # per-replica and lost on restart. Warn once so an operator can
                # diagnose "ETA never gets more confident".
                _logger.warning(
                    "eta: enabled but no Table backing configured "
                    "(set ELB_OPENAPI_ETA_TABLE_CONN or STORAGE_ACCOUNT); "
                    "learning is in-memory only and not shared across replicas"
                )
                return None
            try:
                svc.create_table_if_not_exists(_TABLE_NAME)
            except Exception:
                pass
            self._client = svc.get_table_client(_TABLE_NAME)
            _logger.info("eta: Table store initialised (table=%s)", _TABLE_NAME)
        except Exception as exc:  # pragma: no cover - import/auth failure path
            _logger.warning("eta: Table store unavailable, using in-memory store: %s", exc)
            self._client = None
        return self._client

    @staticmethod
    def _row_from_entity(ent: Any) -> dict[str, float]:
        return {
            "count": float(ent.get("count", 0) or 0),
            "mean": float(ent.get("mean", 0) or 0),
            "var": float(ent.get("var", 0) or 0),
        }

    def _cache_put(self, pk: str, row: dict[str, float]) -> None:
        with self._lock:
            self._mem[pk] = dict(row)
            self._mem_ts[pk] = time.monotonic()

    def get(self, pk: str) -> dict[str, float] | None:
        with self._lock:
            cached = self._mem.get(pk)
            fresh = (
                cached is not None
                and (time.monotonic() - self._mem_ts.get(pk, 0.0)) < _CACHE_TTL_SECONDS
            )
        if cached is not None and fresh:
            return dict(cached)
        client = self._table()
        if client is None:
            # No Table: the in-memory mirror is the only truth (per-replica).
            return dict(cached) if cached is not None else None
        try:
            ent = client.get_entity(partition_key=pk, row_key="agg")
            row = self._row_from_entity(ent)
            self._cache_put(pk, row)
            return row
        except Exception:
            # Row missing or read failed: keep serving the stale cache if any.
            return dict(cached) if cached is not None else None

    @staticmethod
    def _merge(row: dict[str, float], run_seconds: float) -> dict[str, float]:
        count = row["count"]
        # Running mean until the window fills (count=0 => alpha=1 => mean=x,
        # var=0), then floors at 1/window => exponential moving average.
        alpha = 1.0 / min(count + 1.0, float(_EWMA_WINDOW))
        delta = run_seconds - row["mean"]
        mean = row["mean"] + alpha * delta
        var = (1.0 - alpha) * (row["var"] + alpha * delta * delta)
        return {"count": count + 1.0, "mean": mean, "var": max(0.0, var)}

    def update(self, pk: str, run_seconds: float) -> None:
        client = self._table()
        if client is None:
            # In-memory only: merge onto the local mirror under the lock.
            with self._lock:
                base = self._mem.get(pk) or {"count": 0.0, "mean": 0.0, "var": 0.0}
                new_row = self._merge(base, run_seconds)
                self._mem[pk] = new_row
                self._mem_ts[pk] = time.monotonic()
            return

        # ETag optimistic-merge so concurrent replicas never clobber each other.
        from azure.core import MatchConditions  # type: ignore
        from azure.core.exceptions import (  # type: ignore
            HttpResponseError,
            ResourceModifiedError,
            ResourceNotFoundError,
        )

        for _ in range(_UPDATE_MAX_RETRIES):
            etag = None
            base = {"count": 0.0, "mean": 0.0, "var": 0.0}
            try:
                ent = client.get_entity(partition_key=pk, row_key="agg")
                base = self._row_from_entity(ent)
                etag = ent.metadata.get("etag") if hasattr(ent, "metadata") else None
            except ResourceNotFoundError:
                etag = None
            except Exception:
                # Read failed entirely: degrade to a best-effort blind upsert.
                etag = None

            new_row = self._merge(base, run_seconds)
            entity = {
                "PartitionKey": pk,
                "RowKey": "agg",
                "count": new_row["count"],
                "mean": new_row["mean"],
                "var": new_row["var"],
            }
            try:
                if etag is not None:
                    client.update_entity(
                        entity,
                        mode="replace",
                        etag=etag,
                        match_condition=MatchConditions.IfNotModified,
                    )
                else:
                    # Create-if-absent: a racing creator triggers a conflict that
                    # we retry as an update on the next loop.
                    client.create_entity(entity)
                self._cache_put(pk, new_row)
                return
            except (ResourceModifiedError, ResourceNotFoundError):
                continue  # peer replica won the race; re-read and merge again
            except HttpResponseError as exc:
                if getattr(exc, "status_code", None) == 409:
                    continue  # create raced with a peer; retry as update
                break
            except Exception:
                break
        # All retries exhausted: keep the local mirror moving so this replica
        # still learns even if the shared write keeps losing the race.
        with self._lock:
            base = self._mem.get(pk) or {"count": 0.0, "mean": 0.0, "var": 0.0}
            new_row = self._merge(base, run_seconds)
            self._mem[pk] = new_row
            self._mem_ts[pk] = time.monotonic()


_store = _Store()


# ── Prediction (Layer 1) ────────────────────────────────────────────────────


def _db_default(db: str) -> float:
    return _DEFAULT_RUN_SECONDS.get(db, _GLOBAL_DEFAULT_RUN_SECONDS)


def predict(db: str, bucket: str, cold: bool) -> tuple[float, str, dict[str, Any]]:
    """Return (estimated_run_seconds, confidence, basis).

    Fallback chain: exact (db,bucket,cold/warm) row -> warm row for the bucket ->
    db default constant. The estimate is biased to ~p65 (mean + z*sigma).
    """
    tried: list[str] = []
    for use_cold in ([cold] if cold else [False]) + ([False] if cold else []):
        pk = _partition_key(db, bucket, use_cold)
        if pk in tried:
            continue
        tried.append(pk)
        row = _store.get(pk)
        if row and row["count"] >= _MIN_SAMPLES:
            sigma = math.sqrt(max(0.0, row["var"]))
            est = max(1.0, row["mean"] + _P65_Z * sigma)
            conf = "high" if row["count"] >= 10 else "medium"
            basis = {
                "db": db,
                "bucket": bucket,
                "cold": use_cold,
                "samples": int(row["count"]),
                "mean_seconds": round(row["mean"], 1),
            }
            return (est, conf, basis)
    est = _db_default(db)
    return (
        est,
        "low",
        {"db": db, "bucket": bucket, "cold": cold, "samples": 0, "basis": "default"},
    )


# ── Cold detection + per-job run estimate ───────────────────────────────────


def _job_was_cold(job: dict[str, Any], jobs: list[dict[str, Any]]) -> bool:
    """Heuristic: cold if no other job was active shortly before this started."""
    started = _parse_iso(job.get("started_at"))
    if started is None:
        return False
    jid = job.get("job_id")
    for other in jobs:
        if other.get("job_id") == jid:
            continue
        o_start = _parse_iso(other.get("started_at"))
        o_end = _parse_iso(
            other.get("completed_at") or other.get("failed_at") or other.get("updated_at")
        )
        if o_start is None:
            continue
        # Overlap or finished within the cold-gap window before this job started.
        if o_start <= started and (o_end is None or o_end >= started):
            return False
        if o_end is not None and 0 <= (started - o_end).total_seconds() < _COLD_GAP_SECONDS:
            return False
    return True


def _features(job: dict[str, Any]) -> tuple[str, str]:
    db = str(job.get("db_name") or job.get("db") or "unknown")
    seqs = int(job.get("query_seqs", 0) or 0)
    bases = int(job.get("query_bases", 0) or 0)
    return db, bucket_for(seqs, bases)


def _estimate_run(
    job: dict[str, Any], jobs: list[dict[str, Any]]
) -> tuple[float, str, dict[str, Any]]:
    db, bucket = _features(job)
    cold = _job_was_cold(job, jobs)
    return predict(db, bucket, cold)


# ── Recording (called on job completion) ────────────────────────────────────


def record_sample(job: dict[str, Any], jobs: list[dict[str, Any]]) -> None:
    """Persist one (features -> run_seconds) observation. Best-effort, no raise."""
    if not enabled():
        return
    try:
        run_seconds = _seconds_between(job.get("started_at"), job.get("completed_at"))
        if run_seconds is None or run_seconds <= 0:
            return
        db, bucket = _features(job)
        cold = _job_was_cold(job, jobs)
        _store.update(_partition_key(db, bucket, cold), run_seconds)
    except Exception:
        return


# ── Queue simulation (Layer 2) ──────────────────────────────────────────────


def _sorted_queued(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def key(j: dict[str, Any]) -> tuple[int, str]:
        return (-int(j.get("priority", 50) or 50), str(j.get("created_at", "")))

    return sorted(
        (j for j in jobs if str(j.get("status")) in _QUEUED_STATES),
        key=key,
    )


def compute_eta(
    target: dict[str, Any],
    jobs: list[dict[str, Any]],
    server_count: int,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """Queue-aware ETA for ``target``. Returns None when disabled or terminal.

    Simulates ``server_count`` (= MAX_ACTIVE_SUBMISSIONS) servers with a min-heap
    of free-times: running jobs seed the heap with their remaining time, then the
    priority-sorted queue is assigned to the earliest-free server in turn.
    """
    if not enabled():
        return None
    try:
        now = now or _now()
        status = str(target.get("status"))
        C = max(1, int(server_count))

        # Running target: ETA is just its own remaining time.
        if status in _ACTIVE_STATES:
            est, conf, basis = _estimate_run(target, jobs)
            elapsed = _seconds_between(target.get("started_at"), _iso(now)) or 0.0
            remaining = max(0.0, est - elapsed)
            finish = now + _timedelta(remaining)
            return {
                "remaining_seconds": round(remaining, 1),
                "estimated_finish_seconds": round(remaining, 1),
                "estimated_finish_at": _iso(finish),
                "confidence": conf,
                "basis": basis,
            }

        if status not in _QUEUED_STATES:
            return None

        # Seed servers with the remaining time of currently active jobs.
        heap: list[float] = []
        active = [j for j in jobs if str(j.get("status")) in _ACTIVE_STATES]
        for j in active:
            est, _, _ = _estimate_run(j, jobs)
            elapsed = _seconds_between(j.get("started_at"), _iso(now)) or 0.0
            heapq.heappush(heap, max(0.0, est - elapsed))
        while len(heap) < C:
            heapq.heappush(heap, 0.0)
        # If more active than servers (shouldn't happen), keep the C earliest.
        while len(heap) > C:
            # Drop the largest by rebuilding from the C smallest.
            smallest = heapq.nsmallest(C, heap)
            heap = list(smallest)
            heapq.heapify(heap)
            break

        queued = _sorted_queued(jobs)
        target_id = target.get("job_id")
        jobs_ahead = 0
        target_basis: dict[str, Any] = {}
        target_conf = "low"
        start_s = finish_s = None
        for idx, j in enumerate(queued):
            free = heapq.heappop(heap)
            est, conf, basis = _estimate_run(j, jobs)
            j_finish = free + est
            heapq.heappush(heap, j_finish)
            if j.get("job_id") == target_id:
                jobs_ahead = idx + len(active)
                start_s, finish_s = free, j_finish
                target_conf, target_basis = conf, basis
                break

        if start_s is None:
            # Target not found in queue snapshot; degrade to its own estimate.
            est, target_conf, target_basis = _estimate_run(target, jobs)
            start_s, finish_s = 0.0, est
            jobs_ahead = len(active)

        return {
            "jobs_ahead": jobs_ahead,
            "estimated_start_seconds": round(start_s, 1),
            "estimated_finish_seconds": round(finish_s, 1),
            "estimated_start_at": _iso(now + _timedelta(start_s)),
            "estimated_finish_at": _iso(now + _timedelta(finish_s)),
            "confidence": target_conf,
            "basis": target_basis,
        }
    except Exception:
        return None


def _timedelta(seconds: float):
    from datetime import timedelta

    return timedelta(seconds=max(0.0, seconds))
