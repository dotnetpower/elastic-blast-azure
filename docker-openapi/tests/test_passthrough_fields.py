"""Unit tests for caller-supplied pass-through fields on POST /v1/jobs.

A caller may send fields beyond the ``JobSubmitRequest`` schema (e.g.
``request_id``). The service preserves them (bounded) and echoes them back under
``passthrough`` on the submit, status (``/v1/jobs/{id}/status``), and external
status/result (``/api/v1/elastic-blast/jobs/{id}``) payloads so the caller can
correlate. This suite pins:

1. ``_sanitize_passthrough`` bounds key count / key+value length / total size and
   flattens complex values, returning ``{}`` for nothing usable.
2. ``JobSubmitRequest`` keeps unknown fields (``model_extra``) instead of
   dropping them.
3. ``get_job_status`` (``_status_payload``) and ``_external_job_payload`` echo a
   stored ``passthrough`` and omit the key when there is none.

Validation:
``cd docker-openapi && python -m pytest tests/test_passthrough_fields.py -q``.
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
    return main


# ── _sanitize_passthrough bounds ───────────────────────────────────────────


def test_sanitize_keeps_scalar_fields(main_module):
    out = main_module._sanitize_passthrough(
        {"request_id": "req-123", "n": 5, "flag": True, "nothing": None}
    )
    assert out == {"request_id": "req-123", "n": 5, "flag": True, "nothing": None}


def test_sanitize_returns_empty_for_non_dict_or_empty(main_module):
    assert main_module._sanitize_passthrough(None) == {}
    assert main_module._sanitize_passthrough({}) == {}
    assert main_module._sanitize_passthrough("not-a-dict") == {}


def test_sanitize_bounds_value_length(main_module):
    out = main_module._sanitize_passthrough({"request_id": "x" * 5000})
    assert len(out["request_id"]) == main_module._PASSTHROUGH_MAX_VALUE_LEN


def test_sanitize_caps_key_count(main_module):
    big = {f"k{i}": i for i in range(main_module._PASSTHROUGH_MAX_KEYS + 20)}
    out = main_module._sanitize_passthrough(big)
    assert len(out) <= main_module._PASSTHROUGH_MAX_KEYS


def test_sanitize_flattens_complex_value_to_bounded_string(main_module):
    out = main_module._sanitize_passthrough({"meta": {"nested": [1, 2, 3]}})
    assert isinstance(out["meta"], str)
    assert "nested" in out["meta"]


def test_sanitize_drops_blank_keys(main_module):
    out = main_module._sanitize_passthrough({"   ": "v", "ok": "1"})
    assert out == {"ok": "1"}


def test_sanitize_total_size_budget(main_module):
    # Each value is at the per-value cap; the total budget must stop well before
    # all keys are accepted.
    chunk = "y" * main_module._PASSTHROUGH_MAX_VALUE_LEN
    big = {f"k{i}": chunk for i in range(main_module._PASSTHROUGH_MAX_KEYS)}
    out = main_module._sanitize_passthrough(big)
    total = sum(len(k) + len(str(v)) for k, v in out.items())
    assert total <= main_module._PASSTHROUGH_MAX_TOTAL_BYTES


# ── JobSubmitRequest keeps unknown fields ──────────────────────────────────


def test_submit_request_preserves_unknown_fields(main_module):
    req = main_module.JobSubmitRequest(
        db="16S_ribosomal_RNA",
        query_fasta=">s\nACGT",
        request_id="req-abc",
        my_tag="hello",
    )
    assert req.model_extra == {"request_id": "req-abc", "my_tag": "hello"}


# ── status / external payload echo ─────────────────────────────────────────


def _job(**extra):
    base = {
        "job_id": "abc123",
        "status": "queued",
        "created_at": "2026-06-15T00:00:00+00:00",
        "program": "blastn",
        "db": "https://x/blast-db/16S_ribosomal_RNA",
        "db_name": "16S_ribosomal_RNA",
        "results": "",
    }
    base.update(extra)
    return base


def test_status_payload_echoes_passthrough(main_module, monkeypatch):
    job = _job(passthrough={"request_id": "req-xyz"})
    main_module._jobs["abc123"] = job
    monkeypatch.setattr(main_module, "_refresh_job_status", lambda *_a, **_k: job)

    import asyncio

    payload = asyncio.run(main_module.get_job_status("abc123"))
    assert payload["passthrough"] == {"request_id": "req-xyz"}


def test_status_payload_omits_passthrough_when_absent(main_module, monkeypatch):
    job = _job()
    main_module._jobs["abc123"] = job
    monkeypatch.setattr(main_module, "_refresh_job_status", lambda *_a, **_k: job)

    import asyncio

    payload = asyncio.run(main_module.get_job_status("abc123"))
    assert "passthrough" not in payload


def test_external_payload_echoes_passthrough(main_module, monkeypatch):
    monkeypatch.setattr(main_module, "_list_result_files", lambda *_a, **_k: [])
    job = _job(status="queued", passthrough={"request_id": "req-ext", "tier": 2})
    payload = main_module._external_job_payload(job)
    assert payload["passthrough"] == {"request_id": "req-ext", "tier": 2}


def test_external_payload_omits_passthrough_when_absent(main_module, monkeypatch):
    monkeypatch.setattr(main_module, "_list_result_files", lambda *_a, **_k: [])
    payload = main_module._external_job_payload(_job(status="queued"))
    assert "passthrough" not in payload
