"""Tests for keyset pagination on ``GET /v1/jobs`` (dashboard issue #51 unblock).

Without ``limit`` the endpoint returns the full list in insertion order
(unchanged legacy behaviour). With ``limit`` it orders jobs most-recent first and
returns an opaque ``next_cursor`` for stable keyset pagination; an unparseable
cursor degrades to the first page. The dashboard proxy folds this cursor into its
combined ``page.next_cursor``.

Validation:
``cd docker-openapi && python -m pytest tests/test_jobs_pagination.py -q``.
"""

from __future__ import annotations

import importlib
import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("ELB_OPENAPI_API_TOKEN", "test-token")
os.environ.setdefault("ELB_OPENAPI_DISABLE_BACKGROUND", "1")

_HEADERS = {"X-ELB-API-Token": "test-token"}


@pytest.fixture
def main_module(monkeypatch):
    monkeypatch.setenv("ELB_OPENAPI_API_TOKEN", "test-token")
    monkeypatch.setenv("ELB_OPENAPI_DISABLE_BACKGROUND", "1")
    import main  # noqa: PLC0415

    importlib.reload(main)
    # Seed five jobs with strictly increasing created_at, oldest first inserted.
    jobs = {}
    for n in range(5):
        jid = f"job{n}"
        jobs[jid] = {
            "job_id": jid,
            "status": "completed",
            "created_at": f"2026-06-19T08:0{n}:00Z",
        }
    monkeypatch.setattr(main, "_jobs", jobs)
    monkeypatch.setattr(main, "_cm_loaded", True)  # skip ConfigMap load
    return main


def test_legacy_no_limit_returns_full_list(main_module) -> None:
    client = TestClient(main_module.app)
    resp = client.get("/v1/jobs", headers=_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 5
    assert len(body["jobs"]) == 5
    # Additive fields present but signal "no pagination".
    assert body["next_cursor"] is None
    assert body["has_more"] is False
    # Insertion order preserved for legacy callers.
    assert [j["job_id"] for j in body["jobs"]] == ["job0", "job1", "job2", "job3", "job4"]


def test_limit_orders_most_recent_first_with_cursor(main_module) -> None:
    client = TestClient(main_module.app)
    resp = client.get("/v1/jobs?limit=2", headers=_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 5  # total, not page size
    assert [j["job_id"] for j in body["jobs"]] == ["job4", "job3"]  # most-recent first
    assert body["has_more"] is True
    assert body["next_cursor"]


def test_cursor_walk_covers_all_without_overlap(main_module) -> None:
    client = TestClient(main_module.app)
    seen: list[str] = []
    cursor = None
    for _ in range(10):  # bounded guard against an infinite loop
        url = "/v1/jobs?limit=2" + (f"&cursor={cursor}" if cursor else "")
        body = client.get(url, headers=_HEADERS).json()
        seen.extend(j["job_id"] for j in body["jobs"])
        cursor = body["next_cursor"]
        if not cursor:
            break
    assert seen == ["job4", "job3", "job2", "job1", "job0"]
    assert len(seen) == len(set(seen))  # no overlap


def test_bad_cursor_degrades_to_first_page(main_module) -> None:
    client = TestClient(main_module.app)
    resp = client.get("/v1/jobs?limit=2&cursor=not-a-valid-cursor", headers=_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert [j["job_id"] for j in body["jobs"]] == ["job4", "job3"]


def test_list_jobs_exposes_runtime_block(monkeypatch) -> None:
    """``GET /v1/jobs`` must expose started_at + (elapsed/run/queue-wait)
    seconds for every row, matching the detail handler. The dashboard's
    BlastJobs list view reads these to skip the queue-wait portion when
    computing the 'Elapsed' / 'Duration' badge; without them the badge falls
    back to wall-clock from ``created_at`` and shows 20+ minutes for a job
    whose actual BLAST run was 3-4 minutes (a queue-position artefact, not
    runtime)."""
    import importlib
    monkeypatch.setenv("ELB_OPENAPI_API_TOKEN", "test-token")
    monkeypatch.setenv("ELB_OPENAPI_DISABLE_BACKGROUND", "1")
    import main  # noqa: PLC0415

    importlib.reload(main)
    # Terminal completed: started 30 s after enqueue, ran for 200 s.
    completed = {
        "job_id": "done",
        "status": "completed",
        "created_at": "2026-06-27T05:00:00Z",
        "queued_at": "2026-06-27T05:00:00Z",
        "started_at": "2026-06-27T05:00:30Z",
        "updated_at": "2026-06-27T05:03:50Z",
        "completed_at": "2026-06-27T05:03:50Z",
    }
    # Still queued: no started_at; run_seconds and queue_wait_seconds must be
    # None so the dashboard does not render runtime against a job that has
    # never executed.
    queued = {
        "job_id": "wait",
        "status": "queued",
        "created_at": "2026-06-27T05:05:00Z",
        "queued_at": "2026-06-27T05:05:00Z",
        "updated_at": "2026-06-27T05:05:01Z",
    }
    monkeypatch.setattr(main, "_jobs", {"done": completed, "wait": queued})
    monkeypatch.setattr(main, "_cm_loaded", True)
    client = TestClient(main.app)
    resp = client.get("/v1/jobs", headers=_HEADERS)
    assert resp.status_code == 200
    by_id = {j["job_id"]: j for j in resp.json()["jobs"]}
    done_row = by_id["done"]
    assert done_row["started_at"] == "2026-06-27T05:00:30Z"
    assert done_row["updated_at"] == "2026-06-27T05:03:50Z"
    # queue_wait = started - queued = 30 s; run = updated - started = 200 s;
    # elapsed = updated - created = 230 s. Mirrors detail.
    assert done_row["queue_wait_seconds"] == 30
    assert done_row["run_seconds"] == 200
    assert done_row["elapsed_seconds"] == 230
    wait_row = by_id["wait"]
    assert wait_row["started_at"] == ""
    # No started_at -> run_seconds + queue_wait_seconds report None
    # (BlastJobs falls back to created_at for the "Queued for" timer).
    assert wait_row["run_seconds"] is None
    assert wait_row["queue_wait_seconds"] is None
    # elapsed_seconds mirrors the detail handler: a non-terminal row leaves
    # elapsed_end=None and _duration_seconds falls through to time.time(),
    # so a still-queued row reports the live wall-clock since created_at.
    # The SPA does not read this for the queued badge (it computes
    # "Queued for" itself from created_at), so the value is informational.
    assert isinstance(wait_row["elapsed_seconds"], int)
    assert wait_row["elapsed_seconds"] >= 0
