"""API and state-machine tests for the dashboard-compatible OpenAPI contracts."""

from __future__ import annotations

import importlib
import configparser
from io import StringIO
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


def test_sequence_diversity_rejects_reference_context_with_typed_422(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = main_module.JobSubmitRequest(
        program="blastn",
        db="core_nt",
        query_fasta=">q1\nACGT\n",
        resource_profile="core_nt_safe",
        blast_options={
            "outfmt": "7 qseqid saccver sseq qstart qend evalue bitscore",
            "result_selection_policy": "sequence_diversity",
            "web_blast_statistical_context": {
                "filtered_database_letters": 100,
                "filtered_database_sequences": 10,
                "length_adjustment": 0,
                "effective_search_space": 400,
                "scoring_search_space": 400,
                "result_database_letters": 100,
            },
        },
    )
    monkeypatch.setattr(main_module, "_ensure_loaded", lambda: None)
    monkeypatch.setattr(
        main_module,
        "_upload_fasta",
        lambda *_args: pytest.fail("selection validation must precede upload"),
    )

    with pytest.raises(main_module.HTTPException) as error:
        main_module.submit_job(request)

    assert error.value.status_code == 422
    assert error.value.detail["code"] == "sequence_diversity_incompatible_context"
    assert error.value.detail["retryable"] is False


@pytest.mark.parametrize(
    ("blast_options", "expected_code", "expected_missing"),
    [
        (
            {
                "outfmt": "5",
                "max_target_seqs": 2,
                "result_selection_policy": "sequence_diversity",
            },
            "sequence_diversity_invalid_outfmt",
            [],
        ),
        (
            {
                "outfmt": "7 qseqid saccver qstart qend evalue bitscore",
                "max_target_seqs": 2,
                "result_selection_policy": "sequence_diversity",
            },
            "sequence_diversity_missing_fields",
            ["sseq"],
        ),
    ],
)
def test_sequence_diversity_rejects_invalid_request_before_side_effects(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
    blast_options: dict[str, Any],
    expected_code: str,
    expected_missing: list[str],
) -> None:
    request = main_module.JobSubmitRequest(
        program="blastn",
        db="core_nt",
        query_fasta=">q1\nACGT\n",
        resource_profile="core_nt_safe",
        blast_options=blast_options,
    )
    monkeypatch.setattr(main_module, "_ensure_loaded", lambda: None)
    monkeypatch.setattr(
        main_module,
        "_upload_fasta",
        lambda *_args: pytest.fail("selection validation must precede upload"),
    )

    with pytest.raises(main_module.HTTPException) as error:
        main_module.submit_job(request)

    assert error.value.status_code == 422
    assert error.value.detail["code"] == expected_code
    assert error.value.detail.get("missing_fields", []) == expected_missing
    assert error.value.detail["retryable"] is False


def test_sequence_diversity_openapi_schema_exposes_enum_and_pool_bounds(
    main_module,
) -> None:
    schema = TestClient(main_module.app).get("/openapi.json").json()
    options = schema["components"]["schemas"]["BlastOptions"]["properties"]

    assert options["result_selection_policy"]["enum"] == [
        "native_top_n",
        "diversity_aware",
        "sequence_diversity",
    ]
    assert options["candidate_pool_size"]["minimum"] == 1
    assert "maximum" not in options["candidate_pool_size"]


def test_sequence_diversity_http_validation_has_stable_error_shape(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(main_module, "_ensure_loaded", lambda: None)
    monkeypatch.setattr(
        main_module,
        "_upload_fasta",
        lambda *_args: pytest.fail("selection validation must precede upload"),
    )
    client = TestClient(main_module.app)
    base = {
        "program": "blastn",
        "db": "core_nt",
        "query_fasta": ">q1\nACGT\n",
        "resource_profile": "core_nt_safe",
    }

    wrong_type = client.post(
        "/v1/jobs",
        headers={"X-ELB-API-Token": "test-token"},
        json={
            **base,
            "blast_options": {
                "outfmt": "7 qseqid saccver sseq qstart qend evalue bitscore",
                "result_selection_policy": "sequence_diversity",
                "candidate_pool_size": True,
            },
        },
    )
    missing_field = client.post(
        "/v1/jobs",
        headers={"X-ELB-API-Token": "test-token"},
        json={
            **base,
            "blast_options": {
                "outfmt": "7 qseqid saccver qstart qend evalue bitscore",
                "result_selection_policy": "sequence_diversity",
            },
        },
    )

    assert wrong_type.status_code == 422
    assert wrong_type.json() == {
        "detail": {
            "code": "sequence_diversity_invalid_candidate_pool",
            "message": "candidate_pool_size must be a positive integer",
            "retryable": False,
        }
    }
    assert missing_field.status_code == 422
    assert missing_field.json() == {
        "detail": {
            "code": "sequence_diversity_missing_fields",
            "message": (
                "sequence_diversity requires query identity, accession, sseq, "
                "qstart, qend, evalue, bitscore, and score in the effective "
                "tabular outfmt"
            ),
            "missing_fields": ["sseq"],
            "retryable": False,
        }
    }

    xml_facade = client.post(
        "/api/v1/elastic-blast/submit",
        headers={"X-ELB-API-Token": "test-token"},
        json={
            "query_fasta": ">q1\nACGT\n",
            "db": "core_nt",
            "blast_options": {
                "result_selection_policy": "sequence_diversity"
            },
        },
    )
    assert xml_facade.status_code == 422
    assert xml_facade.json() == {
        "detail": {
            "code": "sequence_diversity_invalid_outfmt",
            "message": (
                "sequence_diversity requires tabular outfmt 6 or 7 via /v1/jobs"
            ),
            "retryable": False,
        }
    }


@pytest.mark.parametrize(
    ("max_target_seqs", "candidate_pool_size", "applied_pool_size"),
    [(3, None, 2_000), (10_000, 20_000, 20_000)],
)
def test_sequence_diversity_submit_separates_group_target_from_shard_pool(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
    max_target_seqs: int,
    candidate_pool_size: int | None,
    applied_pool_size: int,
) -> None:
    saved: dict[str, Any] = {}
    active = SimpleNamespace(
        source_version="generation-1",
        db_prefix="core_nt/generations/generation-1/core_nt",
        shard_layout_prefix="core_nt/generations/generation-1/shards",
        total_letters=1000,
        total_sequences=10,
        search_space=3000,
    )
    monkeypatch.setattr(main_module, "_ensure_loaded", lambda: None)
    monkeypatch.setattr(
        main_module, "_upload_fasta", lambda *_args: "https://example.invalid/queries/q.fa"
    )
    monkeypatch.setattr(main_module, "_azcopy_login", lambda: None)
    monkeypatch.setattr(main_module, "_blob_base", lambda: "https://example.invalid")
    monkeypatch.setattr(main_module, "_storage_oauth_token", lambda: "test-token")
    monkeypatch.setattr(
        main_module, "_blast_version_detail", lambda: {"version": "2.17.0+"}
    )
    monkeypatch.setattr(
        main_module,
        "_db_version_detail",
        lambda _db: {"version": "generation-1", "detail": {}},
    )
    monkeypatch.setattr(
        main_module._exact_oracle, "read_active_database", lambda **_kwargs: active
    )
    monkeypatch.setattr(
        main_module._exact_oracle,
        "prepare_web_blast_statistics",
        lambda **kwargs: (kwargs["options"], None),
    )
    monkeypatch.setattr(
        main_module._exact_oracle,
        "select_web_blast_partitions",
        lambda _statistics, *, default_partitions: min(2, default_partitions),
    )
    monkeypatch.setattr(
        main_module._exact_oracle,
        "preserve_or_set_search_space",
        lambda options, _search_space: options,
    )
    monkeypatch.setattr(
        main_module,
        "_save_job",
        lambda job_id, data, *, require_persist: saved.update(
            {"job_id": job_id, "data": data, "require_persist": require_persist}
        ),
    )
    monkeypatch.setattr(main_module, "_dispatcher_once", lambda: False)
    monkeypatch.setattr(main_module, "_queued_position", lambda _job_id: 1)
    blast_options: dict[str, Any] = {
        "outfmt": "7 qseqid saccver sseq qstart qend evalue bitscore",
        "max_target_seqs": max_target_seqs,
        "result_selection_policy": "sequence_diversity",
    }
    if candidate_pool_size is not None:
        blast_options["candidate_pool_size"] = candidate_pool_size
    request = main_module.JobSubmitRequest(
        program="blastn",
        db="core_nt",
        query_fasta=">q1\nACGT\n",
        resource_profile="core_nt_safe",
        blast_options=blast_options,
    )

    response = main_module.submit_job(request)

    assert response["status"] == "queued"
    assert saved["require_persist"] is True
    job_data = saved["data"]
    config = configparser.ConfigParser()
    config.read_file(StringIO(job_data["config_ini"]))
    assert f"-max_target_seqs {applied_pool_size}" in config["blast"]["options"]
    assert config["blast"]["requested-max-target-seqs"] == str(max_target_seqs)
    assert config["blast"]["candidate-pool-size-requested"] == str(
        candidate_pool_size or 0
    )
    assert config["blast"]["result-selection-policy"] == "sequence_diversity"
    assert job_data["requested_sequence_groups"] == max_target_seqs
    assert job_data["candidate_pool_size_requested_per_shard"] == candidate_pool_size
    assert job_data["candidate_pool_size_applied_per_shard"] == applied_pool_size


def _sequence_idempotent_request(main_module, *, outfmt: str):
    return main_module.JobSubmitRequest(
        program="blastn",
        db="core_nt",
        query_fasta=">q1\nACGT\n",
        resource_profile="core_nt_safe",
        idempotency_key="sequence-key",
        blast_options={
            "outfmt": outfmt,
            "max_target_seqs": 3,
            "result_selection_policy": "sequence_diversity",
        },
    )


def test_sequence_diversity_validation_precedes_idempotency_replay(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = main_module._job_id_from_idempotency_key("external_api:sequence-key")
    monkeypatch.setattr(
        main_module,
        "_jobs",
        {job_id: {"status": "completed", "result_selection_policy": "native_top_n"}},
    )
    monkeypatch.setattr(main_module, "_ensure_loaded", lambda: None)

    with pytest.raises(main_module.HTTPException) as error:
        main_module.submit_job(_sequence_idempotent_request(main_module, outfmt="5"))

    assert error.value.status_code == 422
    assert error.value.detail["code"] == "sequence_diversity_invalid_outfmt"


def test_sequence_diversity_rejects_idempotency_key_bound_to_native_job(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = main_module._job_id_from_idempotency_key("external_api:sequence-key")
    monkeypatch.setattr(
        main_module,
        "_jobs",
        {job_id: {"status": "completed", "result_selection_policy": "native_top_n"}},
    )
    monkeypatch.setattr(main_module, "_ensure_loaded", lambda: None)
    monkeypatch.setattr(
        main_module,
        "_upload_fasta",
        lambda *_args: pytest.fail("idempotency conflict must precede upload"),
    )

    with pytest.raises(main_module.HTTPException) as error:
        main_module.submit_job(
            _sequence_idempotent_request(
                main_module,
                outfmt="7 qseqid saccver sseq qstart qend evalue bitscore",
            )
        )

    assert error.value.status_code == 409
    assert error.value.detail == {
        "code": "sequence_diversity_idempotency_conflict",
        "message": "idempotency_key is already bound to different result-selection semantics",
        "retryable": False,
    }


def test_sequence_diversity_idempotent_replay_returns_matching_job(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = main_module._job_id_from_idempotency_key("external_api:sequence-key")
    monkeypatch.setattr(
        main_module,
        "_jobs",
        {
            job_id: {
                "status": "completed",
                "result_selection_policy": "sequence_diversity",
                "requested_sequence_groups": 3,
                "candidate_pool_size_applied_per_shard": 2000,
            }
        },
    )
    monkeypatch.setattr(main_module, "_ensure_loaded", lambda: None)
    monkeypatch.setattr(
        main_module,
        "_upload_fasta",
        lambda *_args: pytest.fail("matching replay must not upload"),
    )

    response = main_module.submit_job(
        _sequence_idempotent_request(
            main_module,
            outfmt="7 qseqid saccver sseq qstart qend evalue bitscore",
        )
    )

    assert response["job_id"] == job_id
    assert response["status"] == "completed"
    assert response["message"] == "Existing job returned for idempotency_key."


def test_sequence_diversity_rejects_top_level_policy_before_side_effects(
    main_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = main_module.JobSubmitRequest(
        program="blastn",
        db="core_nt",
        query_fasta=">q1\nACGT\n",
        result_selection_policy="sequence_diversity",
    )
    monkeypatch.setattr(main_module, "_ensure_loaded", lambda: None)
    monkeypatch.setattr(
        main_module,
        "_upload_fasta",
        lambda *_args: pytest.fail("top-level policy rejection must precede upload"),
    )

    with pytest.raises(main_module.HTTPException) as error:
        main_module.submit_job(request)

    assert error.value.status_code == 422
    assert error.value.detail["code"] == "sequence_diversity_requires_structured_options"
