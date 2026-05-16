import asyncio
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


APP_DIR = Path(__file__).resolve().parents[2] / "docker-openapi" / "app"


def load_openapi_module(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ELB_OPENAPI_DISABLE_BACKGROUND", "1")
    sys.path.insert(0, str(APP_DIR))
    spec = importlib.util.spec_from_file_location("elb_openapi_main", APP_DIR / "main.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["elb_openapi_main"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def openapi(monkeypatch: pytest.MonkeyPatch):
    module = load_openapi_module(monkeypatch)
    monkeypatch.setattr(module, "_save_job_cm", lambda *args, **kwargs: True)
    monkeypatch.setattr(module, "_load_all_jobs_cm", lambda: {})
    monkeypatch.setattr(module, "_load_job_cm", lambda job_id: None)
    monkeypatch.setattr(module, "_delete_job_cm", lambda job_id: None)
    monkeypatch.setattr(module, "_azcopy_login", lambda: None)
    monkeypatch.setattr(module, "_webhook_notify", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "MAX_ACTIVE_SUBMISSIONS", 1)
    module._jobs.clear()
    module._job_threads.clear()
    module._job_cancel_events.clear()
    module._cm_loaded = True
    return module


def test_claim_next_job_uses_priority(openapi):
    openapi._jobs["aaaaaaaaaaaa"] = openapi._ensure_job_defaults(
        "aaaaaaaaaaaa",
        {"job_id": "aaaaaaaaaaaa", "status": "queued", "priority": 10, "created_at": "2026-01-01T00:00:00+00:00"},
    )
    openapi._jobs["bbbbbbbbbbbb"] = openapi._ensure_job_defaults(
        "bbbbbbbbbbbb",
        {"job_id": "bbbbbbbbbbbb", "status": "queued", "priority": 90, "created_at": "2026-01-02T00:00:00+00:00"},
    )

    claimed = openapi._claim_next_job()

    assert claimed["job_id"] == "bbbbbbbbbbbb"
    assert openapi._jobs["bbbbbbbbbbbb"]["status"] == "dispatching"
    assert openapi._jobs["aaaaaaaaaaaa"]["status"] == "queued"


def test_submit_queues_when_capacity_is_full(openapi):
    openapi._jobs["111111111111"] = openapi._ensure_job_defaults(
        "111111111111",
        {"job_id": "111111111111", "status": "running", "priority": 50},
    )
    request = openapi.JobSubmitRequest(
        program="blastn",
        db="https://example.blob.core.windows.net/blast-db/16S_ribosomal_RNA",
        queries="https://example.blob.core.windows.net/queries/sample.fa",
        results="https://example.blob.core.windows.net/results/run-001",
        priority=95,
    )

    response = asyncio.run(openapi.submit_job(request))

    assert response["status"] == "queued"
    assert response["queue_position"] == 1
    assert openapi._jobs[response["job_id"]]["priority"] == 95


def test_submit_accepts_named_priority(openapi):
    openapi._jobs["111111111111"] = openapi._ensure_job_defaults(
        "111111111111",
        {"job_id": "111111111111", "status": "running", "priority": 50},
    )
    request = openapi.JobSubmitRequest(
        program="blastn",
        db="https://example.blob.core.windows.net/blast-db/16S_ribosomal_RNA",
        queries="https://example.blob.core.windows.net/queries/sample.fa",
        results="https://example.blob.core.windows.net/results/run-001",
        priority="urgent",
    )

    response = asyncio.run(openapi.submit_job(request))

    assert response["status"] == "queued"
    assert openapi._jobs[response["job_id"]]["priority"] == 100


def test_run_submit_uses_structured_cli_without_wall_clock_timeout(openapi, monkeypatch: pytest.MonkeyPatch):
    captured = {}

    def fake_run_cancellable(cmd, timeout=None, stop_event=None, **kwargs):
        captured["cmd"] = cmd
        captured["timeout"] = timeout
        captured["stop_event"] = stop_event
        return subprocess.CompletedProcess(cmd, 0, '{"decision":"accepted","correlation_id":"aaaaaaaaaaaa"}\n', "")

    monkeypatch.setattr(openapi, "run_cancellable", fake_run_cancellable)
    monkeypatch.setattr(openapi, "_dispatcher_once", lambda: False)
    openapi._jobs["aaaaaaaaaaaa"] = openapi._ensure_job_defaults(
        "aaaaaaaaaaaa",
        {
            "job_id": "aaaaaaaaaaaa",
            "status": "dispatching",
            "config_ini": "[cloud-provider]\n[cluster]\n[blast]\nprogram = blastn\n",
            "results": "https://example.blob.core.windows.net/results/run-001",
        },
    )

    openapi._run_submit_bg("aaaaaaaaaaaa")

    assert captured["timeout"] is None
    assert captured["stop_event"] is not None
    assert captured["cmd"][:4] == ["elastic-blast", "submit", "--cfg", captured["cmd"][3]]
    assert "--json" not in captured["cmd"]
    assert "--idempotency-key" not in captured["cmd"]
    assert "--correlation-id" not in captured["cmd"]
    assert openapi._jobs["aaaaaaaaaaaa"]["status"] == "running"


def test_idempotency_key_maps_to_safe_job_id(openapi):
    job_id = openapi._job_id_from_idempotency_key("Lab-Request-42/core_nt")

    assert len(job_id) == 12
    assert openapi._sanitize_job_id(job_id) == job_id


def test_cancel_queued_job_does_not_call_elastic_blast_delete(openapi, monkeypatch: pytest.MonkeyPatch):
    calls = []
    monkeypatch.setattr(openapi, "safe_exec", lambda *args, **kwargs: calls.append((args, kwargs)))
    openapi._jobs["aaaaaaaaaaaa"] = openapi._ensure_job_defaults(
        "aaaaaaaaaaaa",
        {"job_id": "aaaaaaaaaaaa", "status": "queued", "config_ini": "[blast]\nprogram = blastn\n"},
    )

    openapi._cancel_job("aaaaaaaaaaaa", "deleted by API request")

    assert calls == []
    assert openapi._jobs["aaaaaaaaaaaa"]["status"] == "cancelled"


def test_submit_requires_queue_persistence(openapi, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(openapi, "_save_job_cm", lambda *args, **kwargs: False)
    request = openapi.JobSubmitRequest(
        program="blastn",
        db="https://example.blob.core.windows.net/blast-db/16S_ribosomal_RNA",
        queries="https://example.blob.core.windows.net/queries/sample.fa",
        results="https://example.blob.core.windows.net/results/run-001",
    )

    with pytest.raises(openapi.HTTPException) as exc:
        asyncio.run(openapi.submit_job(request))

    assert exc.value.status_code == 503
    assert openapi._jobs == {}


def test_recovered_submitting_job_without_thread_is_requeued(openapi, monkeypatch: pytest.MonkeyPatch):
    job = openapi._ensure_job_defaults(
        "aaaaaaaaaaaa",
        {
            "job_id": "aaaaaaaaaaaa",
            "status": "submitting",
            "phase": "submitting",
            "config_ini": "[blast]\nprogram = blastn\n",
            "results": "https://example.blob.core.windows.net/results/run-001",
        },
    )
    monkeypatch.setattr(openapi, "_load_all_jobs_cm", lambda: {"aaaaaaaaaaaa": job})
    monkeypatch.setattr(openapi, "_job_marker_phase", lambda results_url: None)
    monkeypatch.setattr(
        openapi,
        "_k8s_job_summary",
        lambda elb_job_id: {"total": 0, "succeeded": 0, "failed": 0, "active": 0, "submit_failed": 0, "finalizer_active": 0},
    )
    monkeypatch.setattr(openapi, "_k8s_pod_stuck_reason", lambda elb_job_id: None)
    openapi._jobs.clear()
    openapi._cm_loaded = False

    openapi._ensure_loaded()

    assert openapi._jobs["aaaaaaaaaaaa"]["status"] == "queued"
    assert openapi._jobs["aaaaaaaaaaaa"]["phase"] == "recovered"


def test_submit_rejects_sas_query_in_blob_url(openapi):
    request = openapi.JobSubmitRequest(
        program="blastn",
        db="https://example.blob.core.windows.net/blast-db/16S_ribosomal_RNA?sig=secret",
        queries="https://example.blob.core.windows.net/queries/sample.fa",
        results="https://example.blob.core.windows.net/results/run-001",
    )

    with pytest.raises(openapi.HTTPException) as exc:
        asyncio.run(openapi.submit_job(request))

    assert exc.value.status_code == 400


def test_delete_loaded_job_runs_remote_cancel(openapi, monkeypatch: pytest.MonkeyPatch):
    calls = []
    job = openapi._ensure_job_defaults(
        "aaaaaaaaaaaa",
        {
            "job_id": "aaaaaaaaaaaa",
            "status": "running",
            "cfg_path": "/tmp/elb-openapi-test-config.ini",
            "config_ini": "[blast]\nprogram = blastn\n",
        },
    )
    monkeypatch.setattr(openapi, "_load_job_cm", lambda job_id: job)
    monkeypatch.setattr(openapi.os.path, "isfile", lambda path: True)
    monkeypatch.setattr(openapi, "safe_exec", lambda *args, **kwargs: calls.append((args, kwargs)))
    monkeypatch.setattr(openapi, "_cleanup_tmp", lambda *args, **kwargs: None)
    openapi._jobs.clear()

    response = asyncio.run(openapi.delete_job("aaaaaaaaaaaa"))

    assert response["status"] == "deleted"
    assert any(call[0][0][:2] == ["elastic-blast", "delete"] for call in calls)


def test_external_submit_contract_returns_versions_and_source(openapi, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(openapi, "_blast_version_detail", lambda: {"version": "2.17.0+", "detail": "blastn: 2.17.0+", "source": "test"})
    monkeypatch.setattr(openapi, "_db_version_detail", lambda db_name: {"version": "2026-05-02", "source": "test"})
    monkeypatch.setattr(openapi, "_upload_fasta", lambda job_id, fasta: f"https://example.blob.core.windows.net/queries/{job_id}.fa")
    monkeypatch.setattr(openapi, "_dispatcher_once", lambda: False)
    client = TestClient(openapi.app)

    response = client.post(
        "/api/v1/elastic-blast/submit",
        json={
            "query_fasta": ">q1\nATGCATGCATGC",
            "db": "core_nt",
            "program": "blastn",
            "taxid": 3431483,
            "is_inclusive": False,
            "options": {"outfmt": 5, "word_size": 28, "dust": True, "evalue": 0.05, "max_target_seqs": 500},
            "batch_len": 462,
            "idempotency_key": "external-request-1",
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "queued"
    assert body["submission_source"] == "external_api"
    assert body["blast_version"] == "2.17.0+"
    assert body["db_name"] == "core_nt"
    assert body["db_version"] == "2026-05-02"
    job = openapi._jobs[body["job_id"]]
    assert job["submission_source"] == "external_api"
    assert "-outfmt 5" in job["config_ini"]
    assert "-negative_taxids 3431483" in job["config_ini"]
    assert "batch-len = 462" in job["config_ini"]


def test_external_submit_cannot_spoof_dashboard_source(openapi, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(openapi, "_upload_fasta", lambda job_id, fasta: f"https://example.blob.core.windows.net/queries/{job_id}.fa")
    monkeypatch.setattr(openapi, "_dispatcher_once", lambda: False)
    client = TestClient(openapi.app)

    response = client.post(
        "/api/v1/elastic-blast/submit",
        json={
            "query_fasta": ">q1\nATGCATGCATGC",
            "db": "core_nt",
            "program": "blastn",
            "submission_source": "dashboard",
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert body["submission_source"] == "external_api"
    assert openapi._jobs[body["job_id"]]["submission_source"] == "external_api"


def test_external_correlation_id_is_not_idempotency_key(openapi, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(openapi, "_upload_fasta", lambda job_id, fasta: f"https://example.blob.core.windows.net/queries/{job_id}.fa")
    monkeypatch.setattr(openapi, "_dispatcher_once", lambda: False)
    client = TestClient(openapi.app)

    payload = {
        "query_fasta": ">q1\nATGCATGCATGC",
        "db": "core_nt",
        "program": "blastn",
        "external_correlation_id": "same-correlation-id",
    }
    first = client.post("/api/v1/elastic-blast/submit", json=payload)
    second = client.post("/api/v1/elastic-blast/submit", json=payload)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["job_id"] != second.json()["job_id"]
    assert first.json()["external_correlation_id"] == "same-correlation-id"


def test_internal_submit_rejects_trusted_source_without_token(openapi):
    client = TestClient(openapi.app)

    response = client.post(
        "/v1/jobs",
        json={
            "program": "blastn",
            "db": "https://example.blob.core.windows.net/blast-db/core_nt",
            "queries": "https://example.blob.core.windows.net/queries/sample.fa",
            "results": "https://example.blob.core.windows.net/results/run-001",
            "submission_source": "dashboard",
        },
    )

    assert response.status_code == 403


def test_external_submit_rejects_non_xml_outfmt(openapi):
    client = TestClient(openapi.app)

    response = client.post(
        "/api/v1/elastic-blast/submit",
        json={
            "query_fasta": ">q1\nATGCATGCATGC",
            "db": "core_nt",
            "program": "blastn",
            "options": {"outfmt": 6},
        },
    )

    assert response.status_code == 400


def test_external_status_maps_success_result_files(openapi, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        openapi,
        "_list_result_files",
        lambda job_info: [{"file_id": "result-xml-001", "filename": "blast_result.xml", "format": "blast_xml", "size_bytes": 123}],
    )
    openapi._jobs["aaaaaaaaaaaa"] = openapi._ensure_job_defaults(
        "aaaaaaaaaaaa",
        {
            "job_id": "aaaaaaaaaaaa",
            "status": "completed",
            "created_at": "2026-05-12T10:00:00Z",
            "updated_at": "2026-05-12T10:05:00Z",
            "blast_version": "2.17.0+",
            "db_name": "core_nt",
            "db_version": "2026-05-02",
            "results": "https://example.blob.core.windows.net/results/aaaaaaaaaaaa",
            "hit_count": 500,
            "k8s_summary": {"total": 3, "succeeded": 3, "failed": 0, "active": 0},
        },
    )
    client = TestClient(openapi.app)

    response = client.get("/api/v1/elastic-blast/jobs/aaaaaaaaaaaa")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["elapsed_seconds"] == 300
    assert body["execution"]["shard_count"] == 3
    assert body["execution"]["shards_succeeded"] == 3
    assert body["result"]["hit_count"] == 500
    assert body["result"]["files"][0]["file_id"] == "result-xml-001"


def test_external_status_maps_failure_error(openapi):
    openapi._jobs["aaaaaaaaaaaa"] = openapi._ensure_job_defaults(
        "aaaaaaaaaaaa",
        {
            "job_id": "aaaaaaaaaaaa",
            "status": "failed",
            "created_at": "2026-05-12T10:00:00Z",
            "updated_at": "2026-05-12T10:02:00Z",
            "error_code": "QUERY_TOO_SHORT",
            "error": "Query sequence length is below minimum threshold",
        },
    )
    client = TestClient(openapi.app)

    response = client.get("/api/v1/elastic-blast/jobs/aaaaaaaaaaaa")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "failed"
    assert body["error"]["code"] == "QUERY_TOO_SHORT"
    assert "minimum" in body["error"]["message"]


def test_external_download_rejects_unsafe_result_filename(openapi, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        openapi,
        "_list_result_files",
        lambda job_info: [{"file_id": "result-xml-001", "filename": "bad\"name.xml", "format": "blast_xml", "size_bytes": 123}],
    )
    openapi._jobs["aaaaaaaaaaaa"] = openapi._ensure_job_defaults(
        "aaaaaaaaaaaa",
        {
            "job_id": "aaaaaaaaaaaa",
            "status": "completed",
            "results": "https://example.blob.core.windows.net/results/aaaaaaaaaaaa",
        },
    )
    client = TestClient(openapi.app)

    response = client.get("/api/v1/elastic-blast/jobs/aaaaaaaaaaaa/files/result-xml-001")

    assert response.status_code == 400