"""Unit tests for the dead-thread submit-slot reclaim watchdog (#62).

Pins the contract that a ``dispatching``/``submitting`` job whose in-process
submit thread died (a pod restart after the AKS cluster was stopped mid-submit
loses every thread) is reclaimed within one ``_watchdog_once`` tick instead of
being held for ``SUBMIT_STUCK_SECONDS`` (2h) and wedging the dispatcher
(throughput -> 0). The load-bearing safety contract is that a job whose submit
thread is still ALIVE (a legitimately cold-staging submit waiting for nodes) is
never touched, so the reclaim can never cancel healthy work.

The functions under test (``_reclaim_dead_thread_job`` and ``_watchdog_once``)
never touch a real cluster here: ``_has_alive_thread``, ``_refresh_job_status``,
``_age_seconds``, ``_cancel_job`` and the ConfigMap persist are monkeypatched.

Validation: ``cd docker-openapi && python -m pytest tests/test_watchdog_reclaim.py -q``.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def main_module(monkeypatch):
    monkeypatch.setenv("ELB_OPENAPI_API_TOKEN", "test-token")
    monkeypatch.setenv("ELB_CLUSTER_NAME", "test-cluster")
    monkeypatch.setenv("ELB_OPENAPI_DISABLE_BACKGROUND", "1")

    import main  # noqa: PLC0415

    importlib.reload(main)
    # These unit tests must never shell out to kubectl / azcopy / elastic-blast.
    monkeypatch.setattr(main, "_ensure_loaded", lambda: None)
    monkeypatch.setattr(main, "_save_job_cm", lambda *_a, **_kw: True)
    return main


def _register(
    main,
    *,
    job_id: str = "job-test",
    status: str = "submitting",
    phase: str = "submitting",
    attempt: int = 1,
    summary: dict | None = None,
) -> str:
    main._jobs[job_id] = {
        "job_id": job_id,
        "status": status,
        "phase": phase,
        "started_at": "2026-06-21T00:00:00+00:00",
        "attempt": attempt,
        "k8s_summary": summary or {},
    }
    return job_id


def _patch_common(main, monkeypatch, *, alive: bool, age: float) -> None:
    monkeypatch.setattr(main, "_has_alive_thread", lambda _jid: alive)
    monkeypatch.setattr(main, "_refresh_job_status", lambda jid: dict(main._jobs[jid]))
    monkeypatch.setattr(main, "_age_seconds", lambda _ts: age)


# ── reclaim: the deadlock-breaking path ─────────────────────────────────────


def test_watchdog_reclaims_dead_thread_submitting_to_queued(main_module, monkeypatch):
    """No alive thread + no k8s work + past the grace -> requeue, slot released."""
    main = main_module
    job_id = _register(main, summary={"total": 0, "submit_failed": 0})
    _patch_common(main, monkeypatch, alive=False, age=120)

    main._watchdog_once()

    assert main._jobs[job_id]["status"] == "queued"
    assert main._jobs[job_id]["phase"] == "recovered"


def test_watchdog_fails_dead_thread_after_max_retries(main_module, monkeypatch):
    """A job that keeps losing its thread is failed once the retry budget is spent."""
    main = main_module
    # SUBMIT_MAX_RETRIES default is 3, so attempt=3 is the give-up boundary.
    job_id = _register(main, attempt=3, summary={"total": 0, "submit_failed": 0})
    _patch_common(main, monkeypatch, alive=False, age=120)

    seen: dict = {}

    def _spy_cancel(jid, reason, *, terminal_status="cancelled"):
        seen.update(jid=jid, terminal_status=terminal_status, reason=reason)
        main._jobs[jid]["status"] = terminal_status

    monkeypatch.setattr(main, "_cancel_job", _spy_cancel)

    main._watchdog_once()

    assert seen["terminal_status"] == "failed"
    assert main._jobs[job_id]["status"] == "failed"


# ── safety: never touch healthy work ────────────────────────────────────────


def test_watchdog_never_touches_alive_thread_job(main_module, monkeypatch):
    """A legitimately cold-staging submit (thread alive) must be left running."""
    main = main_module
    job_id = _register(main, summary={"total": 0, "submit_failed": 0})
    _patch_common(main, monkeypatch, alive=True, age=120)

    main._watchdog_once()

    assert main._jobs[job_id]["status"] == "submitting"


def test_watchdog_grace_skips_just_dispatched(main_module, monkeypatch):
    """A just-claimed job (no thread yet) within the grace window is untouched."""
    main = main_module
    job_id = _register(main, status="dispatching", phase="", summary={})
    # age below RECLAIM_GRACE_SECONDS (default 45) -> too young to reclaim.
    _patch_common(main, monkeypatch, alive=False, age=10)

    main._watchdog_once()

    assert main._jobs[job_id]["status"] == "dispatching"


def test_watchdog_leaves_job_that_already_created_k8s_work(main_module, monkeypatch):
    """Dead thread but submit already created BLAST jobs -> never re-submit.

    Re-queuing would duplicate the k8s Jobs; the normal status refresh carries
    it to running/terminal instead. Here ``age`` stays below
    ``SUBMIT_STUCK_SECONDS`` so the legacy 2h timeout does not fire either.
    """
    main = main_module
    job_id = _register(main, summary={"total": 3, "submit_failed": 0})
    _patch_common(main, monkeypatch, alive=False, age=120)

    main._watchdog_once()

    assert main._jobs[job_id]["status"] == "submitting"


# ── helper-level contract ───────────────────────────────────────────────────


def test_reclaim_helper_returns_false_when_k8s_work_exists(main_module):
    main = main_module
    refreshed = {"job_id": "j", "attempt": 0, "k8s_summary": {"total": 2}}
    assert main._reclaim_dead_thread_job("j", refreshed) is False


def test_reclaim_helper_returns_false_when_submit_failed_present(main_module):
    main = main_module
    refreshed = {"job_id": "j", "attempt": 0, "k8s_summary": {"total": 0, "submit_failed": 1}}
    assert main._reclaim_dead_thread_job("j", refreshed) is False


# ── startup reconcile shares the SAME bounded reclaim (#62 live-validation fix) ──


def test_reconcile_requeues_dead_thread_under_budget(main_module, monkeypatch):
    """Startup reconcile requeues a dead-thread zombie that is under budget."""
    main = main_module
    job_id = _register(main, attempt=1, summary={"total": 0, "submit_failed": 0})
    monkeypatch.setattr(main, "_has_alive_thread", lambda _jid: False)
    monkeypatch.setattr(main, "_refresh_job_status", lambda jid: dict(main._jobs[jid]))

    main._reconcile_recovered_jobs()

    assert main._jobs[job_id]["status"] == "queued"
    assert main._jobs[job_id]["phase"] == "recovered"


def test_reconcile_respects_retry_bound_and_fails(main_module, monkeypatch):
    """Startup reconcile must NOT unconditionally requeue a job past its retry
    budget — otherwise it resurrects a job the watchdog already failed and
    re-wedges the dispatcher across restarts (the live-validated 2026-06-21 bug).
    """
    main = main_module
    job_id = _register(main, attempt=3, summary={"total": 0, "submit_failed": 0})
    monkeypatch.setattr(main, "_has_alive_thread", lambda _jid: False)
    monkeypatch.setattr(main, "_refresh_job_status", lambda jid: dict(main._jobs[jid]))

    seen: dict = {}

    def _spy_cancel(jid, reason, *, terminal_status="cancelled"):
        seen.update(jid=jid, terminal_status=terminal_status)
        main._jobs[jid]["status"] = terminal_status

    monkeypatch.setattr(main, "_cancel_job", _spy_cancel)

    main._reconcile_recovered_jobs()

    assert seen.get("terminal_status") == "failed"
    assert main._jobs[job_id]["status"] == "failed"
