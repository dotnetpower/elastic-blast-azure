"""API and state-machine tests for the dashboard-compatible OpenAPI contracts."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def main_module(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ELB_OPENAPI_API_TOKEN", "test-token")
    monkeypatch.setenv("ELB_CLUSTER_NAME", "test-cluster")
    monkeypatch.setenv("ELB_OPENAPI_DISABLE_BACKGROUND", "1")

    import main

    return importlib.reload(main)


def _resolved_context() -> dict[str, Any]:
    return {
        "status": "resolved",
        "rid": "ABCDEFGH",
        "database": "core_nt",
        "reference_query_id": "q1",
        "submitted_query_id": "q1",
        "query_length": 4,
        "active_source_version": "generation-1",
        "web_blast_statistical_context": {
            "filtered_database_letters": 1000,
            "filtered_database_sequences": 10,
            "length_adjustment": 1,
            "effective_search_space": 2970,
            "scoring_search_space": 3000,
            "result_database_letters": 1000,
        },
        "query_effective_search_spaces": [2970],
        "expected_filter": {
            "taxid": 1,
            "is_inclusive": True,
            "verified_from_result": False,
        },
        "evidence": {"source": "test"},
        "warnings": [],
        "internal_only": "response model must remove this",
    }


def _payload() -> dict[str, Any]:
    return {
        "rid": "ABCDEFGH",
        "query_fasta": ">q1\nACGT\n",
        "db": "core_nt",
        "taxid": 1,
    }


def _install_active_database(main_module, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        main_module._exact_oracle,
        "read_active_database",
        lambda **_kwargs: SimpleNamespace(
            total_letters=1000,
            total_sequences=10,
            source_version="generation-1",
        ),
    )
    monkeypatch.setattr(main_module, "_blob_base", lambda: "https://example.invalid")
    monkeypatch.setattr(main_module, "_storage_oauth_token", lambda: "test-credential")


def test_reference_context_endpoint_requires_token_and_filters_response(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_active_database(main_module, monkeypatch)
    calls: list[dict[str, Any]] = []

    def resolve(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return _resolved_context()

    monkeypatch.setattr(main_module._reference_context, "resolve_reference_context", resolve)
    client = TestClient(main_module.app)

    unauthenticated = client.post("/v1/web-blast/statistical-context", json=_payload())
    authenticated = client.post(
        "/v1/web-blast/statistical-context",
        json=_payload(),
        headers={"X-ELB-API-Token": "test-token"},
    )

    assert unauthenticated.status_code == 401
    assert authenticated.status_code == 200
    assert "internal_only" not in authenticated.json()
    assert calls == [
        {
            "rid": "ABCDEFGH",
            "query_fasta": ">q1\nACGT\n",
            "active_total_letters": 1000,
            "active_total_sequences": 10,
            "active_source_version": "generation-1",
            "taxid": 1,
            "is_inclusive": True,
        }
    ]


def test_reference_context_endpoint_accepts_zero_length_adjustment(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_active_database(main_module, monkeypatch)
    resolved = _resolved_context()
    resolved["web_blast_statistical_context"] = {
        **resolved["web_blast_statistical_context"],
        "length_adjustment": 0,
        "effective_search_space": 4000,
        "scoring_search_space": 4000,
    }
    resolved["query_effective_search_spaces"] = [4000]
    monkeypatch.setattr(
        main_module._reference_context,
        "resolve_reference_context",
        lambda **_kwargs: resolved,
    )

    response = TestClient(main_module.app).post(
        "/v1/web-blast/statistical-context",
        json=_payload(),
        headers={"X-ELB-API-Token": "test-token"},
    )

    assert response.status_code == 200
    assert response.json()["web_blast_statistical_context"]["length_adjustment"] == 0


@pytest.mark.parametrize(
    ("error_name", "status_code", "error_code", "retry_after"),
    [
        ("ReferenceContextNotReady", 409, "reference_not_ready", "30"),
        ("ReferenceContextUnavailable", 503, "reference_unavailable", None),
        ("ReferenceContextError", 422, "reference_invalid", None),
    ],
)
def test_reference_context_endpoint_maps_resolver_errors(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
    error_name: str,
    status_code: int,
    error_code: str,
    retry_after: str | None,
) -> None:
    _install_active_database(main_module, monkeypatch)
    error_type = getattr(main_module._reference_context, error_name)

    def fail(**_kwargs: Any) -> None:
        raise error_type("test failure")

    monkeypatch.setattr(main_module._reference_context, "resolve_reference_context", fail)
    response = TestClient(main_module.app).post(
        "/v1/web-blast/statistical-context",
        json=_payload(),
        headers={"X-ELB-API-Token": "test-token"},
    )

    assert response.status_code == status_code
    assert response.json()["detail"]["code"] == error_code
    assert response.headers.get("Retry-After") == retry_after


def test_reference_context_endpoint_sanitizes_active_metadata_failure(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fail(**_kwargs: Any) -> None:
        raise RuntimeError("sensitive storage detail")

    monkeypatch.setattr(main_module._exact_oracle, "read_active_database", fail)
    response = TestClient(main_module.app).post(
        "/v1/web-blast/statistical-context",
        json=_payload(),
        headers={"X-ELB-API-Token": "test-token"},
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "active_database_unavailable"
    assert "sensitive storage detail" not in response.text
    assert "error_type=RuntimeError" in caplog.text
    assert "sensitive storage detail" not in caplog.text


def test_openapi_schema_publishes_reference_and_readiness_contracts(main_module) -> None:
    schema = main_module.app.openapi()
    operation = schema["paths"]["/v1/web-blast/statistical-context"]["post"]
    request = schema["components"]["schemas"]["WebBlastStatisticalContextRequest"]
    status = schema["components"]["schemas"]["JobStatusResponse"]

    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/WebBlastStatisticalContextResponse"
    }
    assert request["properties"]["rid"]["pattern"] == "^[A-Z0-9]{8,16}$"
    assert {"results_ready", "results_ready_at", "merged_at", "db_partitions"} <= set(
        status["properties"]
    )


def _install_job_update_fake(main_module, monkeypatch: pytest.MonkeyPatch) -> None:
    def update(job_id: str, **values: Any) -> dict[str, Any]:
        main_module._jobs[job_id] = {**main_module._jobs[job_id], **values}
        return dict(main_module._jobs[job_id])

    monkeypatch.setattr(main_module, "_update_job", update)
    monkeypatch.setattr(main_module, "_notify_terminal_transition", lambda *_args: None)
    monkeypatch.setattr(main_module, "_snapshot_k8s_summary_for_terminal", lambda *_args: None)
    monkeypatch.setattr(main_module, "_effective_elb_job_id", lambda _job: "job-" + "a" * 32)
    monkeypatch.setattr(main_module, "_job_marker_phase", lambda *_args: "completed")


def _partitioned_job() -> dict[str, Any]:
    return {
        "job_id": "job-test",
        "status": "running",
        "phase": "running",
        "results": "https://example.invalid/results/job-test",
        "db_partitions": 10,
    }


def test_partitioned_job_stays_finalizing_without_canonical_result(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_module._jobs.clear()
    main_module._jobs["job-test"] = _partitioned_job()
    _install_job_update_fake(main_module, monkeypatch)
    monkeypatch.setattr(main_module, "_list_result_files", lambda _job: [])
    monkeypatch.setattr(main_module, "_age_seconds", lambda _value: 121)

    result = main_module._refresh_job_status("job-test")

    assert result["status"] == "running"
    assert result["phase"] == "finalizing"


def test_partitioned_job_fails_after_finalizer_deadline(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_module._jobs.clear()
    main_module._jobs["job-test"] = {
        **_partitioned_job(),
        "success_marker_seen_at": "2026-01-01T00:00:00Z",
    }
    _install_job_update_fake(main_module, monkeypatch)
    monkeypatch.setattr(main_module, "_list_result_files", lambda _job: [])
    monkeypatch.setattr(
        main_module,
        "_age_seconds",
        lambda _value: main_module.PARTITIONED_RESULT_FINALIZER_DEADLINE_SECONDS + 1,
    )

    result = main_module._refresh_job_status("job-test")

    assert result["status"] == "failed"
    assert result["phase"] == "finalizer_failed"


def test_partitioned_job_completes_with_canonical_result(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_module._jobs.clear()
    main_module._jobs["job-test"] = _partitioned_job()
    _install_job_update_fake(main_module, monkeypatch)
    monkeypatch.setattr(
        main_module,
        "_list_result_files",
        lambda _job: [{"filename": "merged_results.out.gz"}],
    )

    result = main_module._refresh_job_status("job-test")

    assert result["status"] == "completed"
    assert result["phase"] == "completed"


def test_public_payload_exposes_selection_and_partition_provenance(main_module) -> None:
    payload = main_module._external_job_payload(
        {
            "job_id": "job-test",
            "status": "running",
            "created_at": "2026-01-01T00:00:00Z",
            "db": "core_nt",
            "result_selection_policy": "diversity_aware",
            "db_partitions": 10,
        }
    )

    assert payload["result_selection_policy"] == "diversity_aware"
    assert payload["db_partitions"] == 10


@pytest.mark.parametrize("context_location", ["nested", "legacy_top_level"])
def test_diversity_selection_rejects_reference_context_before_side_effects(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
    context_location: str,
) -> None:
    context = {
        "filtered_database_letters": 100,
        "filtered_database_sequences": 10,
        "length_adjustment": 0,
        "effective_search_space": 400,
        "scoring_search_space": 400,
        "result_database_letters": 100,
    }
    request_data: dict[str, Any] = {
        "program": "blastn",
        "db": "core_nt",
        "query_fasta": ">q1\nACGT\n",
        "blast_options": {
            "outfmt": "5",
            "result_selection_policy": "diversity_aware",
        },
    }
    if context_location == "nested":
        request_data["blast_options"]["web_blast_statistical_context"] = context
    else:
        request_data["web_blast_statistical_context"] = context
    request = main_module.JobSubmitRequest(**request_data)
    monkeypatch.setattr(main_module, "_ensure_loaded", lambda: None)
    monkeypatch.setattr(
        main_module,
        "_upload_fasta",
        lambda *_args: pytest.fail("selection validation must precede upload"),
    )

    with pytest.raises(
        main_module.HTTPException,
        match="web_blast_statistical_context requires native_top_n",
    ) as error:
        main_module.submit_job(request)

    assert error.value.status_code == 400
