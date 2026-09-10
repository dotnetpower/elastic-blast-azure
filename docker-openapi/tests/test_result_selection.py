"""Contract tests for opt-in sequence-diversity request planning."""

from __future__ import annotations

import pytest

import exact_oracle
from result_selection import (
    ResultSelectionValidationError,
    prepare_sequence_diversity_options,
)
from schemas import BlastOptions


def _required_outfmt(*, include_score: bool = True) -> str:
    fields = "qseqid saccver sseq qstart qend evalue bitscore"
    return f"7 {fields}{' score' if include_score else ''}"


def test_existing_policy_defaults_and_serialization_are_unchanged() -> None:
    options = BlastOptions()

    assert options.result_selection_policy == "native_top_n"
    assert options.model_dump(exclude_none=True) == {
        "result_selection_policy": "native_top_n"
    }
    assert BlastOptions(result_selection_policy="diversity_aware").model_dump(
        exclude_none=True
    ) == {"result_selection_policy": "diversity_aware"}


def test_sequence_diversity_uses_effective_score_enrichment() -> None:
    effective = exact_oracle.ensure_tabular_raw_score(
        f"-outfmt {_required_outfmt(include_score=False)} -max_target_seqs 3"
    )

    plan = prepare_sequence_diversity_options(
        effective,
        max_target_seqs=3,
        candidate_pool_size=None,
    )

    assert " score" in plan.options
    assert plan.options.endswith("-max_target_seqs 2000")
    assert plan.requested_sequence_groups == 3
    assert plan.candidate_pool_size_requested_per_shard is None
    assert plan.candidate_pool_size_applied_per_shard == 2000


@pytest.mark.parametrize(
    ("outfmt", "code", "missing"),
    [
        ("5", "sequence_diversity_invalid_outfmt", []),
        (
            "7 qseqid saccver qstart qend evalue bitscore score",
            "sequence_diversity_missing_fields",
            ["sseq"],
        ),
    ],
)
def test_sequence_diversity_rejects_unsupported_effective_outfmt(
    outfmt: str,
    code: str,
    missing: list[str],
) -> None:
    with pytest.raises(ResultSelectionValidationError) as error:
        prepare_sequence_diversity_options(
            f"-outfmt {outfmt} -max_target_seqs 2",
            max_target_seqs=2,
            candidate_pool_size=10,
        )

    assert error.value.detail()["code"] == code
    assert error.value.detail().get("missing_fields", []) == missing
    assert error.value.detail()["retryable"] is False


@pytest.mark.parametrize("candidate_pool_size", [0, 1, 5001])
def test_sequence_diversity_rejects_invalid_candidate_pool(
    candidate_pool_size: int,
) -> None:
    with pytest.raises(ResultSelectionValidationError) as error:
        prepare_sequence_diversity_options(
            f"-outfmt {_required_outfmt()} -max_target_seqs 2",
            max_target_seqs=2,
            candidate_pool_size=candidate_pool_size,
        )

    assert error.value.code == "sequence_diversity_invalid_candidate_pool"
    assert error.value.detail()["retryable"] is False
