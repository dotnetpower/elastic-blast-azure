"""Unit tests for the elb-openapi cross-path submit coordinator (Phase 1).

Every test monkeypatches ``submit_coordination._kubectl`` so the suite never
touches a real cluster. The contract under test mirrors elb-dashboard's
``submit_lease`` + ``blast_status`` (Gate A Lease CAS + Gate B finalizer
counting) so both repos enforce the same concurrency ceiling.

Validation: ``cd docker-openapi && python -m pytest tests/test_submit_coordination.py -q``.
"""

from __future__ import annotations

import json
import subprocess
from typing import Any, Callable

import pytest
import submit_coordination as sc


def _proc(returncode: int = 0, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["kubectl"], returncode=returncode, stdout=stdout, stderr=stderr)


@pytest.fixture(autouse=True)
def _enable_k8s(monkeypatch):
    """Default every test to the k8s backend with a short wait budget."""
    monkeypatch.setenv("BLAST_COORD_BACKEND", "k8s")
    monkeypatch.setenv("BLAST_CAPACITY_WAIT_MAX_SECONDS", "0")
    # Never actually sleep in the capacity loop.
    monkeypatch.setattr(sc, "_slot_wait", lambda *_a, **_k: None)


# ── env / config ───────────────────────────────────────────────────────────
def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv("BLAST_COORD_BACKEND", raising=False)
    assert sc.coordination_enabled() is False
    assert sc.acquire_run_slot("job-1") is None


def test_redis_backend_disables(monkeypatch):
    monkeypatch.setenv("BLAST_COORD_BACKEND", "redis")
    assert sc.coordination_enabled() is False


def test_contract_constants_and_defaults():
    assert sc.lease_name("default") == "elb-blast-submit-default"
    assert sc._LEASE_API_GROUP == "coordination.k8s.io/v1"
    assert sc.max_run_concurrency() == 3
    assert sc.lease_ttl_seconds() == 900
    assert sc.lease_clock_skew_seconds() == 30
    assert sc.finalizer_grace_seconds() == 300
    holder = sc.new_holder_identity()
    assert holder.startswith("openapi-") and len(holder) > len("openapi-")


# ── submit exec timeout (submit_exec_timeout < lease_ttl invariant) ─────────
def test_submit_exec_timeout_default_follows_ttl(monkeypatch):
    monkeypatch.delenv("BLAST_OPENAPI_SUBMIT_EXEC_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("BLAST_SUBMIT_LEASE_TTL_SECONDS", raising=False)
    # Default TTL 900 → cap 900 - 120 = 780, strictly below the TTL.
    assert sc.submit_exec_timeout_seconds() == 780
    assert sc.submit_exec_timeout_seconds() < sc.lease_ttl_seconds()
    # Raising the TTL widens the cap automatically.
    monkeypatch.setenv("BLAST_SUBMIT_LEASE_TTL_SECONDS", "1200")
    assert sc.submit_exec_timeout_seconds() == 1080
    assert sc.submit_exec_timeout_seconds() < sc.lease_ttl_seconds()


def test_submit_exec_timeout_explicit_override(monkeypatch):
    monkeypatch.setenv("BLAST_OPENAPI_SUBMIT_EXEC_TIMEOUT_SECONDS", "500")
    assert sc.submit_exec_timeout_seconds() == 500
    # Garbage override falls back to the TTL-derived default.
    monkeypatch.setenv("BLAST_OPENAPI_SUBMIT_EXEC_TIMEOUT_SECONDS", "not-a-number")
    assert sc.submit_exec_timeout_seconds() == 780


def test_acquire_fails_closed_when_exec_timeout_ge_ttl(monkeypatch):
    # An override at/above the TTL re-opens the takeover race → refuse to admit.
    monkeypatch.setenv("BLAST_SUBMIT_LEASE_TTL_SECONDS", "900")
    monkeypatch.setenv("BLAST_OPENAPI_SUBMIT_EXEC_TIMEOUT_SECONDS", "900")
    # kubectl must never be reached — the guard fires before any cluster call.
    monkeypatch.setattr(
        sc,
        "_kubectl",
        _route_kubectl({}),  # any verb → AssertionError if invoked
    )
    monkeypatch.setattr(sc, "count_active_blast_submissions", lambda *_a, **_k: 0)
    with pytest.raises(sc.SubmitCoordinationError):
        sc.acquire_run_slot("job-1")


# ── _is_expired (fail-closed) ───────────────────────────────────────────────
def test_is_expired_missing_renewtime_is_available():
    assert sc._is_expired({}, sc._now(), 30) is True


def test_is_expired_unparseable_renewtime_held():
    # Cannot prove it dead → must NOT take over.
    assert sc._is_expired({"renewTime": "garbage"}, sc._now(), 30) is False


def test_is_expired_fresh_lease_held():
    now = sc._now()
    spec = {"renewTime": sc._micro_time(now), "leaseDurationSeconds": 900}
    assert sc._is_expired(spec, now, 30) is False


def test_is_expired_old_lease_expired():
    from datetime import timedelta

    now = sc._now()
    old = now - timedelta(seconds=1000)
    spec = {"renewTime": sc._micro_time(old), "leaseDurationSeconds": 900}
    assert sc._is_expired(spec, now, 30) is True  # 1000 > 900 + 30


def test_is_expired_within_skew_held():
    from datetime import timedelta

    now = sc._now()
    old = now - timedelta(seconds=920)
    spec = {"renewTime": sc._micro_time(old), "leaseDurationSeconds": 900}
    assert sc._is_expired(spec, now, 30) is False  # 920 < 900 + 30


# ── Gate A — acquire ────────────────────────────────────────────────────────
def _route_kubectl(handlers: dict[str, Callable[[list[str], str | None], subprocess.CompletedProcess]]):
    """Build a _kubectl stub that dispatches on the kubectl verb (args[0])."""

    def _stub(args, *, input_text=None, timeout=15.0):
        verb = args[0]
        handler = handlers.get(verb)
        if handler is None:
            raise AssertionError(f"unexpected kubectl verb: {verb} {args}")
        return handler(args, input_text)

    return _stub


def test_acquire_creates_when_lease_absent(monkeypatch):
    created = {}

    def _create(args, input_text):
        created["body"] = json.loads(input_text)
        return _proc(0, stdout="lease/elb-blast-submit-default created")

    monkeypatch.setattr(
        sc,
        "_kubectl",
        _route_kubectl(
            {
                "get": lambda a, i: _proc(0, stdout=""),  # --ignore-not-found → empty
                "create": _create,
                # Gate B: no jobs.
            }
        ),
    )
    # Gate B returns 0.
    monkeypatch.setattr(sc, "count_active_blast_submissions", lambda *_a, **_k: 0)

    slot = sc.acquire_run_slot("job-1")
    assert slot is not None
    assert slot.name == "elb-blast-submit-default"
    assert slot.namespace == "default"
    assert created["body"]["spec"]["holderIdentity"] == slot.holder
    assert created["body"]["spec"]["leaseDurationSeconds"] == 900


def test_acquire_busy_when_live_holder_times_out(monkeypatch):
    live_lease = {
        "metadata": {"name": "elb-blast-submit-default", "resourceVersion": "7"},
        "spec": {
            "holderIdentity": "celery-deadbeef",
            "leaseDurationSeconds": 900,
            "renewTime": sc._micro_time(sc._now()),  # fresh → live
        },
    }
    monkeypatch.setattr(
        sc,
        "_kubectl",
        _route_kubectl({"get": lambda a, i: _proc(0, stdout=json.dumps(live_lease))}),
    )
    monkeypatch.setattr(sc, "count_active_blast_submissions", lambda *_a, **_k: 0)
    with pytest.raises(sc.SubmitSlotBusyTimeout):
        sc.acquire_run_slot("job-1")


def test_acquire_takes_over_expired_lease(monkeypatch):
    from datetime import timedelta

    expired = {
        "metadata": {"name": "elb-blast-submit-default", "resourceVersion": "12"},
        "spec": {
            "holderIdentity": "celery-stale",
            "leaseDurationSeconds": 900,
            "renewTime": sc._micro_time(sc._now() - timedelta(seconds=1000)),
        },
    }
    replaced = {}

    def _replace(args, input_text):
        replaced["body"] = json.loads(input_text)
        return _proc(0)

    monkeypatch.setattr(
        sc,
        "_kubectl",
        _route_kubectl(
            {
                "get": lambda a, i: _proc(0, stdout=json.dumps(expired)),
                "replace": _replace,
            }
        ),
    )
    monkeypatch.setattr(sc, "count_active_blast_submissions", lambda *_a, **_k: 0)
    slot = sc.acquire_run_slot("job-1")
    assert slot is not None
    assert replaced["body"]["metadata"]["resourceVersion"] == "12"  # CAS guard


def test_acquire_cas_conflict_then_timeout(monkeypatch):
    expired = {
        "metadata": {"name": "elb-blast-submit-default", "resourceVersion": "1"},
        "spec": {"holderIdentity": "", "leaseDurationSeconds": 900},
    }
    monkeypatch.setattr(
        sc,
        "_kubectl",
        _route_kubectl(
            {
                "get": lambda a, i: _proc(0, stdout=json.dumps(expired)),
                "replace": lambda a, i: _proc(1, stderr="Operation cannot be fulfilled on leases ..."),
            }
        ),
    )
    monkeypatch.setattr(sc, "count_active_blast_submissions", lambda *_a, **_k: 0)
    with pytest.raises(sc.SubmitSlotBusyTimeout):
        sc.acquire_run_slot("job-1")


def test_acquire_get_forbidden_is_fatal(monkeypatch):
    monkeypatch.setattr(
        sc,
        "_kubectl",
        _route_kubectl({"get": lambda a, i: _proc(1, stderr="leases.coordination.k8s.io is forbidden")}),
    )
    with pytest.raises(sc.SubmitCoordinationError):
        sc.acquire_run_slot("job-1")


def test_acquire_releases_lease_when_gate_b_read_fails(monkeypatch):
    absent_then_present = {"n": 0}
    released = {"called": False}

    def _get(args, input_text):
        return _proc(0, stdout="")  # absent → create path

    def _create(args, input_text):
        return _proc(0)

    def _replace(args, input_text):
        released["called"] = True  # conditional release PUT
        return _proc(0)

    def _gate_b(*_a, **_k):
        raise sc.SubmitCoordinationError("jobs list 500")

    # Release does a GET (holder check) then a replace. Route GET to return our lease.
    our_holder = {"holder": None}
    lease_obj = {
        "metadata": {"name": "elb-blast-submit-default", "resourceVersion": "3"},
        "spec": {"holderIdentity": None, "leaseDurationSeconds": 900},
    }

    def _get_router(args, input_text):
        # First GET (acquire) → absent. Later GET (release) → our lease.
        absent_then_present["n"] += 1
        if absent_then_present["n"] == 1:
            return _proc(0, stdout="")
        lease_obj["spec"]["holderIdentity"] = our_holder["holder"]
        return _proc(0, stdout=json.dumps(lease_obj))

    monkeypatch.setattr(
        sc,
        "_kubectl",
        _route_kubectl({"get": _get_router, "create": _create, "replace": _replace}),
    )
    monkeypatch.setattr(sc, "count_active_blast_submissions", _gate_b)

    # Capture the holder used so the release GET returns a matching identity.
    real_new_holder = sc.new_holder_identity

    def _capture_holder(source="openapi"):
        our_holder["holder"] = real_new_holder(source)
        return our_holder["holder"]

    monkeypatch.setattr(sc, "new_holder_identity", _capture_holder)

    with pytest.raises(sc.SubmitCoordinationError):
        sc.acquire_run_slot("job-1")
    assert released["called"] is True  # Gate A was released on Gate B failure


# ── Gate B — counting ───────────────────────────────────────────────────────
def _job(app: str, job_id: str = "", *, age_s: int = 10, status: dict[str, Any] | None = None, uid: str = ""):
    from datetime import timedelta

    created = sc._now() - timedelta(seconds=age_s)
    labels = {"app": app}
    if job_id:
        labels["elb-job-id"] = job_id
    meta: dict[str, Any] = {"labels": labels, "creationTimestamp": sc._micro_time(created)}
    if uid:
        meta["uid"] = uid
    return {"metadata": meta, "status": status or {}}


def _gate_b_with(jobs, monkeypatch):
    monkeypatch.setattr(
        sc,
        "_kubectl",
        lambda args, **_k: _proc(0, stdout=json.dumps({"items": jobs})),
    )


def test_gate_b_counts_distinct_companion_backed(monkeypatch):
    jobs = [
        _job("finalizer", "j1"),
        _job("blast", "j1"),  # companion → j1 live
        _job("finalizer", "j2"),
        _job("submit", "j2"),  # companion → j2 live
    ]
    _gate_b_with(jobs, monkeypatch)
    assert sc.count_active_blast_submissions() == 2


def test_gate_b_skips_terminal_finalizers(monkeypatch):
    jobs = [
        _job("finalizer", "done", status={"succeeded": 1}),
        _job("finalizer", "alsodone", status={"completionTime": "2026-01-01T00:00:00Z"}),
        _job("finalizer", "failedcond", status={"conditions": [{"type": "Failed", "status": "True"}]}),
        _job("finalizer", "live", age_s=5),  # within grace → counted
    ]
    _gate_b_with(jobs, monkeypatch)
    assert sc.count_active_blast_submissions() == 1


def test_gate_b_bare_failed_still_active(monkeypatch):
    # failed>0 with no terminal condition → still retrying → fail-closed counted.
    jobs = [_job("finalizer", "retry", status={"failed": 2})]
    _gate_b_with(jobs, monkeypatch)
    assert sc.count_active_blast_submissions() == 1


def test_gate_b_phantom_past_grace_not_counted(monkeypatch):
    # Lone finalizer, no companion, older than 300s grace → phantom.
    jobs = [_job("finalizer", "ghost", age_s=400)]
    _gate_b_with(jobs, monkeypatch)
    assert sc.count_active_blast_submissions() == 0


def test_gate_b_young_lone_finalizer_counted(monkeypatch):
    jobs = [_job("finalizer", "fresh", age_s=10)]  # async companion lag grace
    _gate_b_with(jobs, monkeypatch)
    assert sc.count_active_blast_submissions() == 1


def test_gate_b_unlabeled_finalizer_counted(monkeypatch):
    jobs = [_job("finalizer", "", uid="abc")]  # no elb-job-id → synthetic slot
    _gate_b_with(jobs, monkeypatch)
    assert sc.count_active_blast_submissions() == 1


def test_gate_b_read_failure_raises(monkeypatch):
    monkeypatch.setattr(sc, "_kubectl", lambda args, **_k: _proc(1, stderr="boom"))
    with pytest.raises(sc.SubmitCoordinationError):
        sc.count_active_blast_submissions()


def test_acquire_capacity_full_then_timeout(monkeypatch):
    # Lease acquirable but Gate B already at ceiling → release + timeout.
    absent = {"metadata": {}, "spec": {}}
    released = {"n": 0}

    def _get(args, input_text):
        return _proc(0, stdout="")  # absent → create

    def _create(args, input_text):
        return _proc(0)

    def _replace(args, input_text):
        # release path: GET returns absent below, so replace shouldn't be hit
        return _proc(0)

    # Make release a no-op observer.
    monkeypatch.setattr(sc, "_release_lease", lambda slot: released.__setitem__("n", released["n"] + 1))
    monkeypatch.setattr(
        sc,
        "_kubectl",
        _route_kubectl({"get": _get, "create": _create, "replace": _replace}),
    )
    monkeypatch.setattr(sc, "count_active_blast_submissions", lambda *_a, **_k: 3)
    with pytest.raises(sc.SubmitSlotBusyTimeout):
        sc.acquire_run_slot("job-1")
    assert released["n"] >= 1  # Gate A released because capacity was full


# ── release ─────────────────────────────────────────────────────────────────
def test_release_skips_when_holder_differs(monkeypatch):
    other = {
        "metadata": {"name": "elb-blast-submit-default", "resourceVersion": "9"},
        "spec": {"holderIdentity": "celery-other", "leaseDurationSeconds": 900},
    }
    puts = {"n": 0}

    def _router(args, input_text=None):
        if args[0] == "get":
            return _proc(0, stdout=json.dumps(other))
        puts["n"] += 1
        return _proc(0)

    monkeypatch.setattr(sc, "_kubectl", _router)
    sc.release_run_slot(sc.RunSlot(name="elb-blast-submit-default", namespace="default", holder="openapi-me"))
    assert puts["n"] == 0  # never clobbered the newer holder


def test_release_clears_when_we_hold(monkeypatch):
    mine = {
        "metadata": {"name": "elb-blast-submit-default", "resourceVersion": "9"},
        "spec": {"holderIdentity": "openapi-me", "leaseDurationSeconds": 900},
    }
    cleared = {}

    def _router(args, input_text=None):
        if args[0] == "get":
            return _proc(0, stdout=json.dumps(mine))
        cleared["body"] = json.loads(input_text)
        return _proc(0)

    monkeypatch.setattr(sc, "_kubectl", _router)
    sc.release_run_slot(sc.RunSlot(name="elb-blast-submit-default", namespace="default", holder="openapi-me"))
    assert cleared["body"]["spec"]["holderIdentity"] == ""


def test_release_none_is_noop():
    sc.release_run_slot(None)  # must not raise
