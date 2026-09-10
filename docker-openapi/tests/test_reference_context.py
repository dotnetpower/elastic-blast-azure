"""Unit tests for the bounded reference-context resolver."""

from __future__ import annotations

from typing import Any

import pytest
import reference_context


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    reference_context.clear_reference_context_cache()
    yield
    reference_context.clear_reference_context_cache()


def _xml1(*, query_length: int = 4, database_letters: int = 1000) -> bytes:
    return (
        b"<BlastOutput><BlastOutput_program>blastn</BlastOutput_program>"
        b"<BlastOutput_db>core_nt</BlastOutput_db>"
        b"<BlastOutput_query-ID>q1</BlastOutput_query-ID>"
        + f"<BlastOutput_query-len>{query_length}</BlastOutput_query-len>".encode()
        + b"<BlastOutput_iterations><Iteration><Iteration_stat><Statistics>"
        b"<Statistics_db-num>10</Statistics_db-num>"
        + f"<Statistics_db-len>{database_letters}</Statistics_db-len>".encode()
        + b"</Statistics></Iteration_stat></Iteration></BlastOutput_iterations>"
        b"</BlastOutput>"
    )


def _xml2(*, length_adjustment: int = 1, effective_space: int = 2970) -> bytes:
    return (
        b"<BlastOutput2><report><results><search><stat><Statistics>"
        b"<db-num>10</db-num><db-len>1000</db-len>"
        + f"<hsp-len>{length_adjustment}</hsp-len>".encode()
        + f"<eff-space>{effective_space}</eff-space>".encode()
        + b"</Statistics></stat></search></results></report></BlastOutput2>"
    )


def _derive(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "rid": "ABCDEFGH",
        "query_fasta": ">q1\nACGT\n",
        "xml1_payload": _xml1(),
        "xml2_payload": _xml2(),
        "active_total_letters": 1000,
        "active_total_sequences": 10,
        "active_source_version": "generation-1",
        "taxid": 1,
        "is_inclusive": False,
    }
    kwargs.update(overrides)
    return reference_context.derive_reference_context(**kwargs)


def test_derive_reference_context_validates_integer_relationships() -> None:
    resolved = _derive()

    assert resolved["web_blast_statistical_context"] == {
        "filtered_database_letters": 1000,
        "filtered_database_sequences": 10,
        "length_adjustment": 1,
        "effective_search_space": 2970,
        "scoring_search_space": 3000,
        "result_database_letters": 1000,
    }
    assert resolved["query_effective_search_spaces"] == [2970]
    assert resolved["expected_filter"] == {
        "taxid": 1,
        "is_inclusive": False,
        "verified_from_result": False,
    }
    assert resolved["evidence"]["query_content_verified"] is False
    assert resolved["evidence"]["submission_options_verified"] is False
    assert resolved["evidence"]["active_source_version_verified"] is False


def test_derive_reference_context_rejects_query_mismatch() -> None:
    with pytest.raises(reference_context.ReferenceContextError, match="query length"):
        _derive(query_fasta=">q1\nACGTA\n")


def test_derive_reference_context_accepts_zero_length_adjustment() -> None:
    resolved = _derive(
        xml2_payload=_xml2(length_adjustment=0, effective_space=4000),
    )

    assert resolved["web_blast_statistical_context"]["length_adjustment"] == 0
    assert resolved["web_blast_statistical_context"]["effective_search_space"] == 4000


def test_reference_context_rejects_xml_entities() -> None:
    payload = (
        b'<!DOCTYPE BlastOutput [<!ENTITY injected "blocked">]>'
        b"<BlastOutput><BlastOutput_program>&injected;</BlastOutput_program></BlastOutput>"
    )

    with pytest.raises(reference_context.ReferenceContextError, match="forbidden XML features"):
        reference_context._parse_xml(payload, source="NCBI XML1")


def test_reference_context_reports_waiting_result() -> None:
    with pytest.raises(reference_context.ReferenceContextNotReady, match="retry after 30 seconds"):
        reference_context._parse_xml(b"Status=WAITING", source="NCBI XML1")


def test_resolver_cache_returns_isolated_payloads() -> None:
    responses = {"XML": _xml1(), "XML2": _xml2()}
    calls: list[str] = []

    def fetch(_rid: str, format_type: str) -> bytes:
        calls.append(format_type)
        return responses[format_type]

    kwargs = {
        "rid": "ABCDEFGH",
        "query_fasta": ">q1\nACGT\n",
        "active_total_letters": 1000,
        "active_total_sequences": 10,
        "active_source_version": "generation-1",
        "fetch_payload": fetch,
    }
    first = reference_context.resolve_reference_context(**kwargs)
    first["web_blast_statistical_context"]["effective_search_space"] = -1
    second = reference_context.resolve_reference_context(**kwargs)

    assert second["web_blast_statistical_context"]["effective_search_space"] == 2970
    assert calls == ["XML", "XML2"]


def test_resolver_fails_fast_when_fetch_gate_is_busy(monkeypatch: pytest.MonkeyPatch) -> None:
    class BusyLock:
        def acquire(self, *, timeout: float) -> bool:
            assert timeout == reference_context._FETCH_LOCK_WAIT_SECONDS
            return False

        def release(self) -> None:
            raise AssertionError("an unacquired gate must not be released")

    monkeypatch.setattr(reference_context, "_FETCH_LOCK", BusyLock())

    with pytest.raises(reference_context.ReferenceContextUnavailable, match="resolver is busy"):
        reference_context.resolve_reference_context(
            rid="ABCDEFGH",
            query_fasta=">q1\nACGT\n",
            active_total_letters=1000,
            active_total_sequences=10,
            active_source_version="generation-1",
            fetch_payload=lambda _rid, _format: b"",
        )
