"""Unit tests for active-generation and exact-oracle helpers."""

from __future__ import annotations

from typing import Any

import exact_oracle
import pytest


class Response:
    def __init__(
        self,
        status_code: int = 200,
        *,
        payload: Any = None,
        content: bytes = b"",
        size: int = 10,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.content = content
        self.headers = {"Content-Length": str(size)}

    def json(self) -> Any:
        return self._payload


def _credential() -> str:
    return "unit-test-credential"


def _active_metadata() -> dict[str, Any]:
    generation = "generation-1"
    return {
        "active_generation": {
            "id": generation,
            "prefix": f"core_nt/generations/{generation}/core_nt",
        },
        "active_prefix": f"core_nt/generations/{generation}/core_nt",
        "shard_layout_prefix": f"core_nt/generations/{generation}/shards",
        "total_letters": 1_000_000,
        "total_sequences": 100,
        "total_bytes": 2_000_000,
    }


def test_read_active_database_uses_one_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        exact_oracle.requests,
        "get",
        lambda _url, **_kwargs: Response(payload=_active_metadata()),
    )

    active = exact_oracle.read_active_database(
        blob_base="https://account.blob.core.windows.net",
        db_name="core_nt",
        token=_credential(),
    )

    assert active.source_version == "generation-1"
    assert active.db_prefix == "core_nt/generations/generation-1/core_nt"
    assert active.shard_layout_prefix == "core_nt/generations/generation-1/shards"
    assert active.total_letters == 1_000_000
    assert active.total_sequences == 100
    assert active.total_bytes == 2_000_000


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"active_generation": {"id": "generation-1"}},
        {
            **_active_metadata(),
            "active_prefix": "core_nt/core_nt",
        },
    ],
)
def test_read_active_database_fails_closed_on_incomplete_metadata(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict[str, Any],
) -> None:
    monkeypatch.setattr(
        exact_oracle.requests,
        "get",
        lambda _url, **_kwargs: Response(payload=payload),
    )

    with pytest.raises(exact_oracle.ExactOracleUnavailable):
        exact_oracle.read_active_database(
            blob_base="https://account.blob.core.windows.net",
            db_name="core_nt",
            token=_credential(),
        )


def test_read_one_shard_layout_rejects_oversized_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = "".join(
        f"core_nt.{index:04d}\n" for index in range(exact_oracle._MAX_PARTS + 1)
    ).encode()
    responses = iter(
        (
            Response(content=manifest),
            Response(content=b"DBLIST core_nt.00\n"),
            Response(content=(b"0" * 64) + b" 1\n"),
            Response(content=b"DBLIST core_nt.00\n"),
        )
    )
    monkeypatch.setattr(exact_oracle.requests, "get", lambda *_a, **_k: next(responses))
    active = exact_oracle.ActiveDatabase(
        source_version="generation-1",
        db_prefix="core_nt/generations/generation-1/core_nt",
        shard_layout_prefix="core_nt/generations/generation-1/shards",
        total_letters=1_000_000,
        total_sequences=100,
        search_space=123,
        total_bytes=2_000_000,
    )

    with pytest.raises(exact_oracle.ExactOracleUnavailable, match="volume limit"):
        exact_oracle.read_one_shard_layout(
            blob_base="https://account.blob.core.windows.net",
            db_name="core_nt",
            active_database=active,
            token=_credential(),
        )


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ("-evalue 1e-5 -outfmt 5", "-evalue 1e-5 -outfmt 5"),
        (
            "-evalue 1e-5 -outfmt 6",
            "-evalue 1e-5 -outfmt 6 std staxids sscinames stitle qcovs score",
        ),
        (
            "-outfmt '7 std staxids' -dust yes",
            "-outfmt 7 std staxids sscinames stitle qcovs score -dust yes",
        ),
    ],
)
def test_ensure_tabular_raw_score(options: str, expected: str) -> None:
    assert exact_oracle.ensure_tabular_raw_score(options) == expected


def test_preserve_query_specific_search_space() -> None:
    result = exact_oracle.preserve_or_set_search_space(
        "-outfmt 5 -searchsp 123456 -dust yes",
        999999,
    )

    assert result == "-outfmt 5 -searchsp 123456 -dust yes"


def test_set_active_fallback_search_space() -> None:
    result = exact_oracle.preserve_or_set_search_space(
        "-outfmt '7 std score' -dust yes",
        999999,
    )

    assert result == "-outfmt '7 std score' -dust yes -searchsp 999999"


def test_prepare_statistics_accepts_zero_length_adjustment() -> None:
    active = exact_oracle.ActiveDatabase(
        source_version="generation-1",
        db_prefix="core_nt/generations/generation-1/core_nt",
        shard_layout_prefix="core_nt/generations/generation-1/shards",
        total_letters=100,
        total_sequences=10,
        search_space=400,
    )
    context = {
        "filtered_database_letters": 100,
        "filtered_database_sequences": 10,
        "length_adjustment": 0,
        "effective_search_space": 400,
        "scoring_search_space": 400,
        "result_database_letters": 100,
    }

    options, statistics = exact_oracle.prepare_web_blast_statistics(
        context=context,
        query_fasta=">query-1\nACGT\n",
        active_database=active,
        options="-outfmt 5",
    )

    assert options == "-outfmt 5 -dbsize 100 -searchsp 400"
    assert statistics is not None
    assert statistics.length_adjustment == 0


def test_validate_execution_options_returns_bounded_provenance() -> None:
    evidence = exact_oracle.validate_web_blast_execution_options(
        "-outfmt 5 -word_size 28 -dust yes -soft_masking false -evalue 0.05 "
        "-max_target_seqs 500 -negative_taxids 1 -dbsize 1000 -searchsp 2970",
        program="blastn",
    )

    assert evidence == {
        "filter_semantics": "blast_taxonomy_filter",
        "filter_mode": "exclude",
        "filter_taxids": [1],
        "candidate_budget": 500,
        "candidate_budget_verified": True,
        "scoring_profile": "megablast_web_default",
        "scoring_profile_verified": True,
    }


def test_validate_execution_options_rejects_unsupported_program() -> None:
    with pytest.raises(exact_oracle.ExactOracleUnavailable, match="requires blastn"):
        exact_oracle.validate_web_blast_execution_options(
            "-outfmt 5 -dbsize 1000 -searchsp 2970",
            program="blastp",
        )
