"""Unit tests for the external-payload hardening landed for elb-dashboard.

The dashboard consumes ``/v1/jobs/{id}/status`` and reacts to the
``register-external-job`` webhook. This suite pins three contracts that the
dashboard depends on:

1. ``_db_version_detail`` returns ``detail`` as a **nested object**, not a
   JSON-encoded string -- the SPA renders the field verbatim and an escaped
   blob is unreadable.
2. ``_blast_version_detail`` falls back to the pinned ElasticBLAST BLAST+
   release (``2.17.0+``) when neither the binary probe nor the env override
   yields a value, so the dashboard never displays ``"unknown"`` for a
   completed tabular-outfmt run.
3. ``_refresh_job_status`` fires the ``register-external-job`` webhook on a
   natural running -> terminal flip (not only on submit / cancel / explicit
   failure) AND snapshots ``k8s_summary`` on the marker-driven completion
   path so the dashboard's ``execution.shard_count`` projection is non-zero.

Validation:
``cd docker-openapi && python -m pytest tests/test_external_payload_hardening.py -q``.
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
    monkeypatch.delenv("ELB_BLAST_VERSION", raising=False)

    import main  # noqa: PLC0415

    importlib.reload(main)
    # The lru_cache on _blast_version_detail otherwise persists across tests
    # within the same process.
    main._blast_version_detail.cache_clear()
    return main


# ── #10: db_version_detail returns nested dict, not JSON string ──────────


def test_db_version_detail_returns_dict_not_string(main_module, monkeypatch):
    """Pin contract: ``detail`` must be a dict so the SPA can render fields.

    Regression: prior code did ``json.dumps(detail, sort_keys=True)`` which
    forced the dashboard's job details panel to display an escaped JSON
    blob under ``payload.external.db_version_detail.detail``.
    """

    def _fake_safe_exec(cmd, *_a, **_kw):
        # azcopy login + cp; the cp call drops a metadata file at the path
        # passed as cmd[3].
        if isinstance(cmd, list) and len(cmd) >= 4 and cmd[0] == "azcopy" and cmd[1] == "cp":
            local_path = cmd[3]
            with open(local_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "version": "1.2",
                        "dbtype": "nucl",
                        "number-of-sequences": "100",
                        "number-of-letters": "12345",
                        "files": [
                            "https://example.blob.core.windows.net/db/2024-01-15-00-00-00/x.nhr",
                        ],
                    },
                    f,
                )
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(main_module, "safe_exec", _fake_safe_exec)
    monkeypatch.setattr(main_module, "_azcopy_login", lambda: None)
    monkeypatch.setattr(main_module, "_blob_base", lambda: "https://example.blob.core.windows.net")

    result = main_module._db_version_detail("nt")
    assert result["version"] == "2024-01-15-00-00-00"
    assert result["source"] == "blastdb_metadata"
    detail = result["detail"]
    assert isinstance(detail, dict), f"detail must be a dict, got {type(detail).__name__}"
    assert detail["dbtype"] == "nucl"
    assert detail["metadata_version"] == "1.2"
    assert detail["source_version"] == "2024-01-15-00-00-00"


# ── #9: blast_version pinned fallback ────────────────────────────────────


def test_blast_version_falls_back_to_pinned_release(main_module, monkeypatch):
    """When the binary is absent AND no env override, return the pinned version.

    The OpenAPI container does NOT ship the BLAST+ binaries, so the binary
    probe always fails inside the deployed pod; falling through to
    ``"unknown"`` made every dashboard job payload show
    ``blast_version: "unknown"`` for tabular-outfmt runs.
    """

    def _binary_missing(*_a, **_kw):
        raise FileNotFoundError("blastn not on PATH")

    monkeypatch.setattr(main_module, "safe_exec", _binary_missing)
    monkeypatch.delenv("ELB_BLAST_VERSION", raising=False)
    main_module._blast_version_detail.cache_clear()

    result = main_module._blast_version_detail()
    assert result["version"] == main_module._BLAST_PLUS_PINNED_VERSION
    assert result["source"] == "elastic_blast_release_pin"
    # The pinned constant must reflect a real release format so the dashboard
    # can render it (we do NOT silently return the literal string "unknown").
    assert result["version"] and result["version"] != "unknown"


def test_blast_version_env_override_still_wins(main_module, monkeypatch):
    """Env override path is preserved unchanged."""

    def _binary_missing(*_a, **_kw):
        raise FileNotFoundError("no binary")

    monkeypatch.setattr(main_module, "safe_exec", _binary_missing)
    monkeypatch.setenv("ELB_BLAST_VERSION", "2.16.0+")
    main_module._blast_version_detail.cache_clear()

    result = main_module._blast_version_detail()
    assert result["version"] == "2.16.0+"
    assert result["source"] == "ELB_BLAST_VERSION"


# ── #16/#17/#18: webhook on natural completion + summary snapshot ────────


def _register_running_job(main, job_id: str = "job-test") -> None:
    main._jobs[job_id] = {
        "job_id": job_id,
        "elb_job_id": "job-deadbeefdeadbeefdeadbeefdeadbeef",
        "status": "running",
        "phase": "running",
        "results": "https://example.blob.core.windows.net/results/job-test",
    }


def _capture_webhooks(monkeypatch, main) -> list[tuple[str, dict[str, Any]]]:
    captured: list[tuple[str, dict[str, Any]]] = []

    def _record(job_id: str, data: dict[str, Any]) -> None:
        captured.append((job_id, dict(data)))

    monkeypatch.setattr(main, "_webhook_notify", _record)
    return captured


def test_external_job_payload_exposes_elb_job_id(main_module, monkeypatch):
    """The public /v1/jobs payload exposes the elastic-blast ``job-<hash>`` id.

    The dashboard only knows the OpenAPI ``job_id`` and cannot otherwise map an
    external job to its in-cluster BLAST pods (labelled ``elb-job-id``), so
    without this it can render the step timeline but never stream the raw pod
    logs.
    """
    monkeypatch.setattr(main_module, "_progress_pct", lambda *_a, **_kw: 42)
    monkeypatch.setattr(main_module, "_k8s_job_summary", lambda *_a, **_kw: {})
    job_info = {
        "job_id": "job-test",
        "status": "running",
        "elb_job_id": "job-deadbeefdeadbeefdeadbeefdeadbeef",
        "created_at": "2026-06-17T00:00:00Z",
        "program": "blastn",
        "db": "core_nt",
    }
    payload = main_module._external_job_payload(job_info)
    assert payload["elb_job_id"] == "job-deadbeefdeadbeefdeadbeefdeadbeef"


def test_external_job_payload_omits_elb_job_id_when_unknown(main_module, monkeypatch):
    """No ``elb_job_id`` key until a valid ``job-`` id has been discovered."""
    monkeypatch.setattr(main_module, "_progress_pct", lambda *_a, **_kw: 42)
    monkeypatch.setattr(main_module, "_k8s_job_summary", lambda *_a, **_kw: {})
    monkeypatch.setattr(
        main_module, "_discover_elb_job_id_from_submit_output", lambda *_a, **_kw: ""
    )
    job_info = {
        "job_id": "job-test2",
        "status": "running",
        "created_at": "2026-06-17T00:00:00Z",
        "program": "blastn",
        "db": "core_nt",
    }
    payload = main_module._external_job_payload(job_info)
    assert "elb_job_id" not in payload


def test_marker_completion_fires_webhook_and_snapshots_summary(main_module, monkeypatch):
    """The dominant case: success marker landed, listing ready → completed.

    Pins three things at once:
    * status flips to ``completed``
    * webhook fires with ``event=completed``
    * ``k8s_summary`` is refreshed at the flip (not the stale prior value)
    """
    captured = _capture_webhooks(monkeypatch, main_module)
    monkeypatch.setattr(main_module, "_job_marker_phase", lambda *_a, **_kw: "completed")
    monkeypatch.setattr(
        main_module,
        "_list_result_files",
        lambda *_a, **_kw: [{"name": "batch_000.out.gz"}],
    )
    monkeypatch.setattr(
        main_module,
        "_k8s_job_summary",
        lambda _elb: {
            "total": 4,
            "succeeded": 4,
            "failed": 0,
            "active": 0,
            "submit_failed": 0,
            "finalizer_active": 0,
            "failed_terminal": 0,
            "submit_failed_terminal": 0,
        },
    )
    _register_running_job(main_module)

    refreshed = main_module._refresh_job_status("job-test")
    assert refreshed["status"] == "completed"
    assert refreshed["phase"] == "completed"
    # k8s_summary was snapshotted at flip-time (issue #18).
    assert refreshed["k8s_summary"]["succeeded"] == 4
    assert refreshed["k8s_summary"]["total"] == 4
    # Webhook fired once with the terminal status (issues #16/#17).
    assert len(captured) == 1
    job_id, payload = captured[0]
    assert job_id == "job-test"
    assert payload["event"] == "completed"
    assert payload["status"] == "completed"


def test_marker_failure_fires_webhook(main_module, monkeypatch):
    captured = _capture_webhooks(monkeypatch, main_module)
    monkeypatch.setattr(main_module, "_job_marker_phase", lambda *_a, **_kw: "failed")
    monkeypatch.setattr(main_module, "_list_result_files", lambda *_a, **_kw: [])
    monkeypatch.setattr(
        main_module,
        "_k8s_job_summary",
        lambda _elb: {"total": 0, "succeeded": 0, "failed": 0, "active": 0},
    )
    _register_running_job(main_module)

    refreshed = main_module._refresh_job_status("job-test")
    assert refreshed["status"] == "failed"
    assert len(captured) == 1
    assert captured[0][1]["event"] == "failed"


def test_k8s_summary_success_path_fires_webhook(main_module, monkeypatch):
    """The kubectl-summary-driven completion path also notifies."""
    captured = _capture_webhooks(monkeypatch, main_module)
    monkeypatch.setattr(main_module, "_job_marker_phase", lambda *_a, **_kw: None)
    monkeypatch.setattr(
        main_module,
        "_list_result_files",
        lambda *_a, **_kw: [{"name": "batch_000.out.gz"}],
    )

    def _fake_safe_exec(cmd, *_a, **_kw):
        text = cmd if isinstance(cmd, str) else " ".join(cmd)
        if text.startswith("kubectl get jobs"):
            items = [
                {
                    "metadata": {"labels": {"app": "blast"}},
                    "status": {"succeeded": 2, "failed": 0, "active": 0},
                }
            ]
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout=json.dumps({"items": items}), stderr=""
            )
        if text.startswith("kubectl get pods"):
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout=json.dumps({"items": []}), stderr=""
            )
        raise AssertionError(f"unstubbed: {text}")

    monkeypatch.setattr(main_module, "safe_exec", _fake_safe_exec)
    _register_running_job(main_module)

    refreshed = main_module._refresh_job_status("job-test")
    assert refreshed["status"] == "completed"
    assert len(captured) == 1
    assert captured[0][1]["event"] == "completed"


def test_running_transition_does_not_fire_webhook(main_module, monkeypatch):
    """Non-terminal updates must NOT emit a webhook (noise + dashboard cost)."""
    captured = _capture_webhooks(monkeypatch, main_module)
    monkeypatch.setattr(main_module, "_job_marker_phase", lambda *_a, **_kw: None)
    monkeypatch.setattr(main_module, "_list_result_files", lambda *_a, **_kw: [])

    def _fake_safe_exec(cmd, *_a, **_kw):
        text = cmd if isinstance(cmd, str) else " ".join(cmd)
        if text.startswith("kubectl get jobs"):
            items = [
                {
                    "metadata": {"labels": {"app": "blast"}},
                    "status": {"succeeded": 0, "failed": 0, "active": 2},
                }
            ]
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout=json.dumps({"items": items}), stderr=""
            )
        if text.startswith("kubectl get pods"):
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout=json.dumps({"items": []}), stderr=""
            )
        raise AssertionError(f"unstubbed: {text}")

    monkeypatch.setattr(main_module, "safe_exec", _fake_safe_exec)
    _register_running_job(main_module)

    refreshed = main_module._refresh_job_status("job-test")
    assert refreshed["status"] == "running"
    assert captured == []


def test_marker_completion_preserves_existing_summary_when_kubectl_fails(
    main_module, monkeypatch
):
    """If kubectl errors at the terminal flip, do not overwrite a real summary.

    The marker indicates the run finished; the dashboard would rather see the
    last good fan-out counts than an empty error stub for the executive view.
    """
    _capture_webhooks(monkeypatch, main_module)
    monkeypatch.setattr(main_module, "_job_marker_phase", lambda *_a, **_kw: "completed")
    monkeypatch.setattr(
        main_module,
        "_list_result_files",
        lambda *_a, **_kw: [{"name": "batch_000.out.gz"}],
    )
    monkeypatch.setattr(
        main_module,
        "_k8s_job_summary",
        lambda _elb: {
            "total": 0,
            "succeeded": 0,
            "failed": 0,
            "active": 0,
            "error": "kubectl unavailable",
        },
    )
    _register_running_job(main_module)
    main_module._jobs["job-test"]["k8s_summary"] = {
        "total": 3,
        "succeeded": 3,
        "failed": 0,
        "active": 0,
    }

    refreshed = main_module._refresh_job_status("job-test")
    assert refreshed["status"] == "completed"
    # The existing real summary is preserved (no error stub overwrite).
    assert refreshed["k8s_summary"]["total"] == 3
    assert refreshed["k8s_summary"]["succeeded"] == 3
    assert "error" not in refreshed["k8s_summary"]
