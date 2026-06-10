"""Unit tests for job phase classification from Kubernetes Job status.

Pins the contract that a *transient* pod retry (Kubernetes bumping a Job's
``.status.failed`` while still retrying within ``backoffLimit``) is NOT reported
as ``blast_failed``. Only a Job that carries a terminal ``Failed`` condition
(``backoffLimit`` exhausted) flips the run to ``blast_failed`` / ``submit_failed``.

The functions under test (``_k8s_job_summary`` and ``_refresh_job_status``) shell
out to ``kubectl`` via ``main.safe_exec``; every test monkeypatches that one
function with canned ``kubectl get jobs -o json`` output so the suite never
touches a real cluster.

Validation: ``cd docker-openapi && python -m pytest tests/test_job_phase.py -q``.
"""

from __future__ import annotations

import importlib
import json
import subprocess
from typing import Any

import pytest


@pytest.fixture
def main_module(monkeypatch):
    monkeypatch.setenv("ELB_OPENAPI_API_TOKEN", "test-token")
    monkeypatch.setenv("ELB_CLUSTER_NAME", "test-cluster")
    monkeypatch.setenv("ELB_OPENAPI_DISABLE_BACKGROUND", "1")

    import main  # noqa: PLC0415

    importlib.reload(main)
    return main


def _blast_job(*, succeeded: int = 0, failed: int = 0, active: int = 0, terminal_failed: bool = False) -> dict[str, Any]:
    status: dict[str, Any] = {"succeeded": succeeded, "failed": failed, "active": active}
    if terminal_failed:
        status["conditions"] = [
            {"type": "FailureTarget", "status": "True"},
            {"type": "Failed", "status": "True"},
        ]
    return {"metadata": {"labels": {"app": "blast"}}, "status": status}


def _submit_job(*, failed: int = 0, terminal_failed: bool = False) -> dict[str, Any]:
    status: dict[str, Any] = {"failed": failed}
    if terminal_failed:
        status["conditions"] = [{"type": "Failed", "status": "True"}]
    return {"metadata": {"labels": {"app": "submit"}}, "status": status}


def _patch_jobs(monkeypatch, main, items: list[dict[str, Any]]) -> None:
    def _fake(cmd, *_a, **_kw) -> subprocess.CompletedProcess:
        text = cmd if isinstance(cmd, str) else " ".join(cmd)
        if text.startswith("kubectl get jobs"):
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=json.dumps({"items": items}), stderr="")
        if text.startswith("kubectl get pods"):
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=json.dumps({"items": []}), stderr="")
        raise AssertionError(f"safe_exec received an unstubbed command: {text}")

    monkeypatch.setattr(main, "safe_exec", _fake)


# ── _k8s_job_summary: terminal vs transient ────────────────────────────────


def test_summary_counts_transient_retry_without_terminal_flag(main_module, monkeypatch):
    _patch_jobs(monkeypatch, main_module, [_blast_job(failed=1, active=1)])
    summary = main_module._k8s_job_summary("job-abc")
    assert summary["failed"] == 1  # raw pod retry count still surfaced
    assert summary["active"] == 1
    assert summary["failed_terminal"] == 0  # no terminal Failed condition


def test_summary_flags_terminal_failure(main_module, monkeypatch):
    _patch_jobs(monkeypatch, main_module, [_blast_job(failed=1, terminal_failed=True)])
    summary = main_module._k8s_job_summary("job-abc")
    assert summary["failed_terminal"] == 1


def test_summary_flags_terminal_submit_failure(main_module, monkeypatch):
    _patch_jobs(monkeypatch, main_module, [_submit_job(failed=1, terminal_failed=True)])
    summary = main_module._k8s_job_summary("job-abc")
    assert summary["submit_failed_terminal"] == 1
    assert summary["failed_terminal"] == 0


# ── _refresh_job_status: phase decision ────────────────────────────────────


def _register_running_job(main, job_id: str = "job-test") -> None:
    main._jobs[job_id] = {
        "job_id": job_id,
        "elb_job_id": "job-deadbeefdeadbeefdeadbeefdeadbeef",
        "status": "running",
        "phase": "running",
        "results": "",
    }


def test_transient_retry_does_not_flip_to_blast_failed(main_module, monkeypatch):
    """The regression: a pod failed once and is being retried (active=1).

    Before the fix this reported ``blast_failed``; now it stays ``running``.
    """
    monkeypatch.setattr(main_module, "_job_marker_phase", lambda *_a, **_kw: None)
    monkeypatch.setattr(main_module, "_list_result_files", lambda *_a, **_kw: [])
    _patch_jobs(monkeypatch, main_module, [_blast_job(failed=1, active=1)])
    _register_running_job(main_module)

    refreshed = main_module._refresh_job_status("job-test")
    assert refreshed["status"] == "running"
    assert refreshed["phase"] == "running"


def test_between_retries_holds_pending(main_module, monkeypatch):
    """failed>0, active=0, no terminal Failed condition → still not dead."""
    monkeypatch.setattr(main_module, "_job_marker_phase", lambda *_a, **_kw: None)
    monkeypatch.setattr(main_module, "_list_result_files", lambda *_a, **_kw: [])
    _patch_jobs(monkeypatch, main_module, [_blast_job(failed=2, active=0)])
    _register_running_job(main_module)

    refreshed = main_module._refresh_job_status("job-test")
    assert refreshed["status"] == "running"
    assert refreshed["phase"] == "pending"


def test_terminal_failure_reports_blast_failed(main_module, monkeypatch):
    monkeypatch.setattr(main_module, "_job_marker_phase", lambda *_a, **_kw: None)
    monkeypatch.setattr(main_module, "_list_result_files", lambda *_a, **_kw: [])
    _patch_jobs(monkeypatch, main_module, [_blast_job(failed=3, active=0, terminal_failed=True)])
    _register_running_job(main_module)

    refreshed = main_module._refresh_job_status("job-test")
    assert refreshed["status"] == "failed"
    assert refreshed["phase"] == "blast_failed"


def test_retry_then_success_completes(main_module, monkeypatch):
    """A pod that failed once but ultimately succeeded must complete cleanly."""
    monkeypatch.setattr(main_module, "_job_marker_phase", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        main_module,
        "_list_result_files",
        lambda *_a, **_kw: [{"name": "batch_000.out.gz"}],
    )
    _patch_jobs(monkeypatch, main_module, [_blast_job(succeeded=1, failed=1, active=0)])
    _register_running_job(main_module)

    refreshed = main_module._refresh_job_status("job-test")
    assert refreshed["status"] == "completed"
    assert refreshed["phase"] == "completed"
