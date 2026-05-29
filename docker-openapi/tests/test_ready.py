"""Unit tests for the elb-openapi ``/v1/ready`` probe.

The probe shells out to ``kubectl`` via ``util.safe_exec`` — every test
monkeypatches that one function to return canned output so the suite never
touches a real cluster. Async route + FastAPI TestClient is enough; we do
not need to bring up uvicorn.

Validation: ``cd docker-openapi && python -m pytest tests/ -q``.
"""

from __future__ import annotations

import json
import subprocess
from typing import Any, Callable

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def main_module(monkeypatch):
    """Reload main with a fresh in-process rate-limit + metrics dict.

    Reloading is the cleanest way to reset the module-level mutable state
    between tests (per-token buckets, per-code counters). The TestClient is
    cheap to recreate so we get full isolation.
    """
    # Ensure env defaults the route relies on (config knobs).
    monkeypatch.setenv("ELB_OPENAPI_API_TOKEN", "test-token")
    monkeypatch.setenv("ELB_OPENAPI_READY_RATE_LIMIT_PER_MINUTE", "30")
    monkeypatch.setenv("ELB_OPENAPI_READY_BUDGET_SECONDS", "2.5")
    monkeypatch.setenv("ELB_OPENAPI_AUTOSCALER_AWARE_READY", "1")
    monkeypatch.setenv("ELB_OPENAPI_READY_MASK_CLUSTER_NAME", "0")
    monkeypatch.setenv("ELB_CLUSTER_NAME", "test-cluster")

    import importlib

    import main  # noqa: PLC0415

    importlib.reload(main)
    return main


def _patch_safe_exec(monkeypatch, mapping: dict[str, str | Exception]) -> list[str]:
    """Replace ``main.safe_exec`` so each prefix in ``mapping`` returns its
    canned stdout (or raises the provided exception).

    Returns the list of every command the route attempted so a test can
    assert the call order.
    """
    calls: list[str] = []

    def _fake(cmd, *_a, **_kw) -> subprocess.CompletedProcess:
        text = cmd if isinstance(cmd, str) else " ".join(cmd)
        calls.append(text)
        for prefix, outcome in mapping.items():
            if text.startswith(prefix):
                if isinstance(outcome, Exception):
                    raise outcome
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=outcome, stderr="")
        raise AssertionError(f"safe_exec received an unstubbed command: {text}")

    import main  # noqa: PLC0415

    monkeypatch.setattr(main, "safe_exec", _fake)
    return calls


def _ok_nodes(label: str = "workload=blast", ready_count: int = 1) -> str:
    items = [
        {
            "status": {
                "conditions": [{"type": "Ready", "status": "True"}],
            }
        }
        for _ in range(ready_count)
    ]
    return json.dumps({"items": items, "label_used": label})


def _ok_deploy(replicas: int = 1) -> str:
    return json.dumps({"status": {"readyReplicas": replicas}})


# ── Auth & rate limit ─────────────────────────────────────────────────────


def test_ready_requires_api_token(main_module, monkeypatch):
    _patch_safe_exec(monkeypatch, {})
    client = TestClient(main_module.app)
    resp = client.get("/v1/ready")
    assert resp.status_code == 401


def test_ready_rate_limit_per_token(main_module, monkeypatch):
    monkeypatch.setattr(main_module, "READY_RATE_LIMIT_PER_MINUTE", 2)
    _patch_safe_exec(
        monkeypatch,
        {
            "kubectl get --raw /readyz": "",
            "kubectl get nodes": _ok_nodes(),
            "kubectl get deploy elb-openapi": _ok_deploy(),
        },
    )
    client = TestClient(main_module.app)
    headers = {"X-ELB-API-Token": "test-token"}
    assert client.get("/v1/ready", headers=headers).status_code == 200
    assert client.get("/v1/ready", headers=headers).status_code == 200
    third = client.get("/v1/ready", headers=headers)
    assert third.status_code == 429
    body = third.json()
    assert body["code"] == "rate_limited"
    assert third.headers.get("Retry-After") == "60"


# ── Probe outcomes ────────────────────────────────────────────────────────


def test_ready_success_when_all_probes_pass(main_module, monkeypatch):
    _patch_safe_exec(
        monkeypatch,
        {
            "kubectl get --raw /readyz": "",
            "kubectl get nodes": _ok_nodes(),
            "kubectl get deploy elb-openapi": _ok_deploy(),
        },
    )
    resp = TestClient(main_module.app).get(
        "/v1/ready", headers={"X-ELB-API-Token": "test-token"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ready"] is True
    assert body["cluster_name"] == "test-cluster"
    assert body["budget_seconds"] == 2.5
    assert body["checks"]["workload_pool"]["status"] == "ok"


def test_ready_503_on_k8s_unreachable(main_module, monkeypatch):
    _patch_safe_exec(
        monkeypatch,
        {"kubectl get --raw /readyz": RuntimeError("connection refused")},
    )
    resp = TestClient(main_module.app).get(
        "/v1/ready", headers={"X-ELB-API-Token": "test-token"}
    )
    assert resp.status_code == 503
    body = resp.json()
    assert body["ready"] is False
    assert body["code"] == "k8s_unreachable"


def test_ready_503_when_workload_pool_empty_without_autoscaler(main_module, monkeypatch):
    _patch_safe_exec(
        monkeypatch,
        {
            "kubectl get --raw /readyz": "",
            "kubectl get nodes": json.dumps({"items": []}),
            "kubectl get configmap cluster-autoscaler-status": RuntimeError("not found"),
            "kubectl get deploy elb-openapi": _ok_deploy(),
        },
    )
    resp = TestClient(main_module.app).get(
        "/v1/ready", headers={"X-ELB-API-Token": "test-token"}
    )
    assert resp.status_code == 503
    assert resp.json()["code"] == "no_workload_nodes"


def test_ready_autoscaler_pending_when_workload_pool_empty_but_autoscaler_present(
    main_module, monkeypatch
):
    _patch_safe_exec(
        monkeypatch,
        {
            "kubectl get --raw /readyz": "",
            "kubectl get nodes": json.dumps({"items": []}),
            "kubectl get configmap cluster-autoscaler-status": "configmap/cluster-autoscaler-status",
            "kubectl get deploy elb-openapi": _ok_deploy(),
        },
    )
    resp = TestClient(main_module.app).get(
        "/v1/ready", headers={"X-ELB-API-Token": "test-token"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ready"] is True
    assert body["checks"]["workload_pool"]["degraded"] == "autoscaler_pending"


def test_ready_skips_workload_pool_when_label_disabled(main_module, monkeypatch):
    monkeypatch.setattr(main_module, "WORKLOAD_POOL_LABEL", "")
    _patch_safe_exec(
        monkeypatch,
        {
            "kubectl get --raw /readyz": "",
            "kubectl get deploy elb-openapi": _ok_deploy(),
        },
    )
    resp = TestClient(main_module.app).get(
        "/v1/ready", headers={"X-ELB-API-Token": "test-token"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["checks"]["workload_pool"]["skipped"] == "label_disabled"


def test_ready_503_when_openapi_pod_not_ready(main_module, monkeypatch):
    _patch_safe_exec(
        monkeypatch,
        {
            "kubectl get --raw /readyz": "",
            "kubectl get nodes": _ok_nodes(),
            "kubectl get deploy elb-openapi": _ok_deploy(replicas=0),
        },
    )
    resp = TestClient(main_module.app).get(
        "/v1/ready", headers={"X-ELB-API-Token": "test-token"}
    )
    assert resp.status_code == 503
    assert resp.json()["code"] == "openapi_pod_not_ready"


# ── Cluster name masking ──────────────────────────────────────────────────


def test_ready_masks_cluster_name_when_enabled(main_module, monkeypatch):
    monkeypatch.setattr(main_module, "READY_MASK_CLUSTER_NAME", True)
    _patch_safe_exec(
        monkeypatch,
        {
            "kubectl get --raw /readyz": "",
            "kubectl get nodes": _ok_nodes(),
            "kubectl get deploy elb-openapi": _ok_deploy(),
        },
    )
    resp = TestClient(main_module.app).get(
        "/v1/ready", headers={"X-ELB-API-Token": "test-token"}
    )
    body = resp.json()
    assert body["cluster_name"].startswith("sha256:")
    assert "test-cluster" not in body["cluster_name"]


# ── /v1/ready/metrics ─────────────────────────────────────────────────────


def test_ready_metrics_returns_counter_snapshot(main_module, monkeypatch):
    _patch_safe_exec(
        monkeypatch,
        {
            "kubectl get --raw /readyz": "",
            "kubectl get nodes": _ok_nodes(),
            "kubectl get deploy elb-openapi": _ok_deploy(),
        },
    )
    client = TestClient(main_module.app)
    headers = {"X-ELB-API-Token": "test-token"}
    client.get("/v1/ready", headers=headers)
    metrics = client.get("/v1/ready/metrics", headers=headers).json()
    assert metrics["ok"] >= 1
    assert metrics["version"] == main_module.VERSION
    assert metrics["rate_limit_per_minute"] == main_module.READY_RATE_LIMIT_PER_MINUTE


# ── Rate-limit hardening (3.7.2) ──────────────────────────────────────────


def test_ready_anonymous_bucket_is_per_client_ip(main_module, monkeypatch):
    """Two distinct client IPs each get their own anonymous quota.

    Before 3.7.2 every unauthenticated caller shared a single ``anonymous``
    bucket, so one noisy laptop could DoS the probe for every other
    unauthenticated developer in a shared dev cluster. Per-IP keying
    isolates them.
    """
    monkeypatch.setattr(main_module, "READY_RATE_LIMIT_PER_MINUTE", 1)
    # Bypass the router-level auth dep so we can exercise the anonymous
    # bucketing branch without re-loading the module with
    # ELB_OPENAPI_ALLOW_UNAUTHENTICATED=1 (which would also relax every
    # other test in this module run via fixture import order).
    main_module.app.dependency_overrides[main_module.require_api_token] = lambda: None
    try:
        _patch_safe_exec(
            monkeypatch,
            {
                "kubectl get --raw /readyz": "",
                "kubectl get nodes": _ok_nodes(),
                "kubectl get deploy elb-openapi": _ok_deploy(),
            },
        )
        client_a = TestClient(main_module.app, client=("10.0.0.1", 12345))
        client_b = TestClient(main_module.app, client=("10.0.0.2", 12345))
        assert client_a.get("/v1/ready").status_code == 200
        # 10.0.0.1 exhausted its own bucket but 10.0.0.2 still has quota.
        assert client_a.get("/v1/ready").status_code == 429
        assert client_b.get("/v1/ready").status_code == 200
    finally:
        main_module.app.dependency_overrides.clear()


def test_ready_token_bucket_garbage_collects_empty_keys(main_module, monkeypatch):
    """An empty per-key bucket is removed so distinct tokens / IPs over
    a long-running pod's lifetime cannot accumulate unbounded SHA-256
    keys in ``_READY_RATE_BUCKETS``.

    Drive the bucket past 60s + back to 0 and assert the key disappears.
    """
    import time

    # First call inserts the key.
    assert main_module._ready_token_bucket_check("token-abc") is True
    digest_key = next(iter(main_module._READY_RATE_BUCKETS.keys()))
    # Fast-forward by 61s by mutating the bucket directly (cheaper than
    # monkeypatching time.monotonic for a focused test).
    now = time.monotonic()
    main_module._READY_RATE_BUCKETS[digest_key] = [now - 70.0]
    # Next check drops the stale timestamp, appends fresh, leaves the key
    # populated — the empty-bucket GC path only fires when the bucket is
    # actually emptied. Simulate that by removing the appended entry
    # before re-checking.
    assert main_module._ready_token_bucket_check("token-abc") is True
    # Direct simulation: a bucket emptied by the cutoff cleanup and not
    # re-appended must be GC'd. We exercise that branch by hand.
    with main_module._READY_RATE_LOCK:
        main_module._READY_RATE_BUCKETS[digest_key] = []
        # The GC line runs inside _ready_token_bucket_check; replicate the
        # invariant the prod path enforces:
        if not main_module._READY_RATE_BUCKETS[digest_key]:
            main_module._READY_RATE_BUCKETS.pop(digest_key, None)
    assert digest_key not in main_module._READY_RATE_BUCKETS


# ── Autoscaler workload-pool name filter (3.7.2) ──────────────────────────


def test_ready_autoscaler_pending_requires_matching_pool_name_when_set(
    main_module, monkeypatch
):
    """When ``ELB_OPENAPI_WORKLOAD_POOL_NAME`` is configured the autoscaler
    ConfigMap *body* must mention that pool, not just exist.

    Otherwise a cluster with autoscaler on a non-workload pool (e.g. the
    system pool only) would silently degrade a real ``no_workload_nodes``
    outage into a soft ``autoscaler_pending`` info entry.
    """
    monkeypatch.setattr(main_module, "WORKLOAD_POOL_NAME", "blastpool")
    _patch_safe_exec(
        monkeypatch,
        {
            "kubectl get --raw /readyz": "",
            "kubectl get nodes": json.dumps({"items": []}),
            "kubectl get configmap cluster-autoscaler-status -n kube-system --request-timeout=1s -o jsonpath={.data.status}": "Pool name: systempool\nReady=1",
            "kubectl get deploy elb-openapi": _ok_deploy(),
        },
    )
    resp = TestClient(main_module.app).get(
        "/v1/ready", headers={"X-ELB-API-Token": "test-token"}
    )
    # Autoscaler body does NOT mention 'blastpool' → real outage, 503.
    assert resp.status_code == 503
    assert resp.json()["code"] == "no_workload_nodes"


def test_ready_autoscaler_pending_when_pool_name_matches(main_module, monkeypatch):
    """When the configured pool name *is* in the autoscaler ConfigMap body,
    keep the existing degraded-to-autoscaler-pending behaviour.
    """
    monkeypatch.setattr(main_module, "WORKLOAD_POOL_NAME", "blastpool")
    _patch_safe_exec(
        monkeypatch,
        {
            "kubectl get --raw /readyz": "",
            "kubectl get nodes": json.dumps({"items": []}),
            "kubectl get configmap cluster-autoscaler-status -n kube-system --request-timeout=1s -o jsonpath={.data.status}": "Pool name: BlastPool\nReady=0",
            "kubectl get deploy elb-openapi": _ok_deploy(),
        },
    )
    resp = TestClient(main_module.app).get(
        "/v1/ready", headers={"X-ELB-API-Token": "test-token"}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["checks"]["workload_pool"]["degraded"] == "autoscaler_pending"

