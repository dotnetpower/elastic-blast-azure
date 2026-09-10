"""Validate opt-in result-selection policies for OpenAPI submissions.

This module owns only request-time policy validation and bounded candidate-pool
planning. Runtime result grouping remains in ``merge-sharded-results.sh``.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass

SEQUENCE_DIVERSITY_DEFAULT_CANDIDATE_POOL_SIZE = 2_000
SEQUENCE_DIVERSITY_MAX_CANDIDATE_POOL_SIZE = 5_000
SEQUENCE_IDENTITY_MODE = "aligned_sequence_query_span"
SEQUENCE_IDENTITY_VERSION = 1

_STD_TABULAR_FIELDS = (
    "qseqid",
    "sseqid",
    "pident",
    "length",
    "mismatch",
    "gapopen",
    "qstart",
    "qend",
    "sstart",
    "send",
    "evalue",
    "bitscore",
)
_QUERY_FIELDS = frozenset({"qseqid", "qacc", "qaccver", "qgi"})
_ACCESSION_FIELDS = frozenset({"sseqid", "sacc", "saccver", "sgi"})
_REQUIRED_FIELDS = ("query_identity", "accession", "sseq", "qstart", "qend", "evalue", "bitscore", "score")


class ResultSelectionValidationError(ValueError):
    """A safe, machine-readable permanent request rejection."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        missing_fields: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.missing_fields = missing_fields

    def detail(self) -> dict[str, object]:
        detail: dict[str, object] = {
            "code": self.code,
            "message": self.message,
            "retryable": False,
        }
        if self.missing_fields:
            detail["missing_fields"] = list(self.missing_fields)
        return detail


@dataclass(frozen=True)
class SequenceDiversityPlan:
    """Validated execution options for one sequence-diversity request."""

    options: str
    requested_sequence_groups: int
    candidate_pool_size_requested_per_shard: int | None
    candidate_pool_size_applied_per_shard: int


def _outfmt_spec(options: str) -> str:
    try:
        tokens = shlex.split(options or "")
    except ValueError as exc:
        raise ResultSelectionValidationError(
            "sequence_diversity_invalid_outfmt",
            "sequence_diversity BLAST options could not be parsed",
        ) from exc
    spec = ""
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "-outfmt" and index + 1 < len(tokens):
            parts: list[str] = []
            cursor = index + 1
            while cursor < len(tokens) and not tokens[cursor].startswith("-"):
                parts.append(tokens[cursor])
                cursor += 1
            spec = " ".join(parts)
            index = cursor
            continue
        if token.startswith("-outfmt="):
            spec = token.split("=", 1)[1]
        index += 1
    return spec.strip().strip("'\"")


def _expanded_fields(spec: str) -> tuple[str, tuple[str, ...]]:
    parts = spec.split()
    if not parts or parts[0] not in {"6", "7"}:
        raise ResultSelectionValidationError(
            "sequence_diversity_invalid_outfmt",
            "sequence_diversity supports only tabular BLAST outfmt 6 or 7",
        )
    fields: list[str] = []
    for field in parts[1:] or ["std"]:
        if field.lower() == "std":
            fields.extend(_STD_TABULAR_FIELDS)
        else:
            fields.append(field.lower())
    return parts[0], tuple(fields)


def _missing_fields(fields: tuple[str, ...]) -> tuple[str, ...]:
    present = set(fields)
    missing: list[str] = []
    for required in _REQUIRED_FIELDS:
        if required == "query_identity":
            available = bool(present & _QUERY_FIELDS)
        elif required == "accession":
            available = bool(present & _ACCESSION_FIELDS)
        else:
            available = required in present
        if not available:
            missing.append(required)
    return tuple(missing)


def _replace_max_target_seqs(options: str, value: int) -> str:
    try:
        tokens = shlex.split(options or "")
    except ValueError as exc:
        raise ResultSelectionValidationError(
            "sequence_diversity_invalid_outfmt",
            "sequence_diversity BLAST options could not be parsed",
        ) from exc
    output: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "-max_target_seqs":
            index += 2 if index + 1 < len(tokens) else 1
            continue
        if token.startswith("-max_target_seqs="):
            index += 1
            continue
        output.append(token)
        index += 1
    output.extend(("-max_target_seqs", str(value)))
    return " ".join(output)


def prepare_sequence_diversity_options(
    effective_options: str,
    *,
    max_target_seqs: int | None,
    candidate_pool_size: int | None,
) -> SequenceDiversityPlan:
    """Validate effective fields and separate final-group N from shard cap."""
    requested_groups = 500 if max_target_seqs is None else max_target_seqs
    if (
        isinstance(requested_groups, bool)
        or not isinstance(requested_groups, int)
        or requested_groups <= 0
        or requested_groups > SEQUENCE_DIVERSITY_MAX_CANDIDATE_POOL_SIZE
    ):
        raise ResultSelectionValidationError(
            "sequence_diversity_invalid_candidate_pool",
            "sequence_diversity max_target_seqs must be between 1 and 5000",
        )

    requested_pool = candidate_pool_size
    if requested_pool is None:
        applied_pool = max(
            SEQUENCE_DIVERSITY_DEFAULT_CANDIDATE_POOL_SIZE,
            requested_groups,
        )
    elif isinstance(requested_pool, bool) or not isinstance(requested_pool, int):
        raise ResultSelectionValidationError(
            "sequence_diversity_invalid_candidate_pool",
            "candidate_pool_size must be a positive integer",
        )
    else:
        applied_pool = requested_pool

    if applied_pool <= 0 or applied_pool > SEQUENCE_DIVERSITY_MAX_CANDIDATE_POOL_SIZE:
        raise ResultSelectionValidationError(
            "sequence_diversity_invalid_candidate_pool",
            "candidate_pool_size must be between 1 and 5000",
        )
    if applied_pool < requested_groups:
        raise ResultSelectionValidationError(
            "sequence_diversity_invalid_candidate_pool",
            "candidate_pool_size must be greater than or equal to max_target_seqs",
        )

    _code, fields = _expanded_fields(_outfmt_spec(effective_options))
    missing = _missing_fields(fields)
    if missing:
        raise ResultSelectionValidationError(
            "sequence_diversity_missing_fields",
            "sequence_diversity requires query identity, accession, sseq, qstart, "
            "qend, evalue, bitscore, and score in the effective tabular outfmt",
            missing_fields=missing,
        )

    return SequenceDiversityPlan(
        options=_replace_max_target_seqs(effective_options, applied_pool),
        requested_sequence_groups=requested_groups,
        candidate_pool_size_requested_per_shard=requested_pool,
        candidate_pool_size_applied_per_shard=applied_pool,
    )
