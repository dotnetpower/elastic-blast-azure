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
