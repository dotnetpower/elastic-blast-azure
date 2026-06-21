"""Pydantic request/response models for the ElasticBLAST OpenAPI server.

This module is the API *contract* layer: it declares the data shapes the
service accepts and returns, plus the small bounding helper that protects
the job store from oversized caller-supplied pass-through fields. It holds
no application logic, no I/O, and no Azure/Kubernetes coupling so it can be
imported (and reasoned about) independently of ``main``.
"""

from __future__ import annotations

import json
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

# Default ``submission_source`` for externally submitted jobs. Lives here (a
# plain literal, not an env-derived value) so the request models can default
# to it without importing ``main`` — which would create an import cycle.
DEFAULT_EXTERNAL_SOURCE = "external_api"


# ── Databases ──────────────────────────────────────────────────────────────

class DatabaseListItem(BaseModel):
    """Lightweight catalogue entry returned by ``GET /v1/databases``."""

    name: str = Field(..., description="Database name as stored under the blast-db container.")


class DatabaseList(BaseModel):
    """Catalogue of prepared BLAST databases."""

    databases: list[DatabaseListItem]
    count: int = Field(..., ge=0)
    container: str = Field("blast-db", description="Source container.")


class DatabaseMetadata(BaseModel):
    """Full metadata for a single BLAST database (v3.5.0 schema).

    Breaking changes vs. the previous ``DatabaseMetadata`` payload:

    - ``description`` was renamed to ``title`` (the source JSON's
      ``description`` field is a one-line title, not a long blurb).
    - ``version`` was renamed to ``snapshot`` to disambiguate the
      NCBI-snapshot timestamp from the metadata-schema version. The
      schema version is now ``metadata_schema_version`` (previously
      ``metadata_version``).
    - ``molecule_type`` now carries the lowercase natural value
      (``dna`` / ``protein``) instead of the abbreviated
      (``nucl`` / ``prot``). A new ``molecule_label`` field carries
      the display label (``mixed DNA`` / ``protein``).
    - New fields: ``container``, ``last_updated``, ``number_of_volumes``,
      ``bytes_total``, ``bytes_to_cache``, ``cached_at``.
    """

    name: str
    container: str = Field("blast-db", description="Source container.")
    title: str = Field(
        "", description="Short one-line title from the source metadata."
    )
    dbtype: str = Field(
        "", description="Raw BLAST dbtype string (e.g. 'Nucleotide', 'Protein')."
    )
    molecule_type: Literal["dna", "protein"] = Field(
        ..., description="Sequence molecule type (lowercase natural value)."
    )
    molecule_label: str = Field(
        "", description="Display label for the molecule type (e.g. 'mixed DNA')."
    )
    snapshot: str = Field(
        ..., description="NCBI snapshot timestamp embedded in the source file paths."
    )
    last_updated: Optional[str] = Field(
        None, description="Source metadata's last-updated timestamp (ISO 8601)."
    )
    number_of_sequences: Optional[int] = None
    number_of_letters: Optional[int] = None
    number_of_volumes: Optional[int] = None
    bytes_total: Optional[int] = None
    bytes_to_cache: Optional[int] = None
    metadata_schema_version: str = Field(
        "", description="Schema version of the source metadata JSON."
    )
    cached_at: str = Field(
        ...,
        description="UTC timestamp (ISO 8601) when this payload was loaded into the server's in-process cache.",
    )


# ── Jobs ───────────────────────────────────────────────────────────────────

class BlastOptions(BaseModel):
    """BLAST search parameters."""
    evalue: Optional[float] = Field(None, description="E-value threshold. Server default if omitted.", examples=[0.05])
    max_target_seqs: Optional[int] = Field(None, description="Maximum number of hits to return.", examples=[100])
    outfmt: Optional[str] = Field(None, description="Output format string (default: '7').", examples=["7"])
    extra: Optional[str] = Field(None, description="Additional BLAST CLI options as raw string.")


class ExternalBlastOptions(BaseModel):
    """External API BLAST options.

    The external result pipeline requires BLAST XML (`outfmt=5`) because
    `Hsp_hseq` is needed for FASTA generation.
    """

    outfmt: int = Field(5, description="Fixed to BLAST XML format 5")
    word_size: int = Field(28, ge=1)
    dust: bool = Field(True)
    evalue: float = Field(10.0, gt=0)
    max_target_seqs: int = Field(500, ge=1)


class ExternalSubmitRequest(BaseModel):
    query_fasta: str = Field(..., min_length=1)
    db: str = Field(..., min_length=1)
    program: str = Field("blastn")
    taxid: Optional[int] = Field(None)
    is_inclusive: Optional[bool] = Field(None)
    options: ExternalBlastOptions = Field(default_factory=ExternalBlastOptions)
    priority: int | str = Field(50)
    batch_len: Optional[int] = Field(None, ge=1, le=1_000_000_000)
    idempotency_key: Optional[str] = Field(None, min_length=1, max_length=256)
    resource_profile: str = Field("standard")
    submission_source: str = Field(DEFAULT_EXTERNAL_SOURCE)
    external_correlation_id: Optional[str] = Field(None, max_length=128)

_MODE_A_EXAMPLE = {
    "summary": "Mode A — Blob URL (advanced)",
    "description": "Provide full Azure Blob Storage URLs for database, queries, and results.",
    "value": {
        "program": "blastn",
        "db": "https://stgelb0509.blob.core.windows.net/blast-db/16S_ribosomal_RNA",
        "queries": "https://stgelb0509.blob.core.windows.net/queries/sample.fa",
        "results": "https://stgelb0509.blob.core.windows.net/results/run-001",
        "options": "-evalue 0.01 -outfmt 7",
    },
}

# Real E. coli K-12 MG1655 16S ribosomal RNA, partial (NCBI NR_024570.1, first ~540 bp).
# Used by the Mode B examples so they actually hit the 16S_ribosomal_RNA database
# (the previous synthetic "ATGC..." repeat against a bacterial 16S DB returned no hits).
_SAMPLE_16S_FASTA = (
    ">NR_024570.1 Escherichia coli str. K-12 substr. MG1655 16S ribosomal RNA, partial sequence\n"
    "AAATTGAAGAGTTTGATCATGGCTCAGATTGAACGCTGGCGGCAGGCCTAACACATGCAA\n"
    "GTCGAACGGTAACAGGAAGAAGCTTGCTTCTTTGCTGACGAGTGGCGGACGGGTGAGTAA\n"
    "TGTCTGGGAAACTGCCTGATGGAGGGGGATAACTACTGGAAACGGTAGCTAATACCGCAT\n"
    "AACGTCGCAAGACCAAAGAGGGGGACCTTCGGGCCTCTTGCCATCGGATGTGCCCAGATG\n"
    "GGATTAGCTAGTAGGTGGGGTAACGGCTCACCTAGGCGACGATCCCTAGCTGGTCTGAGA\n"
    "GGATGACCAGCCACACTGGAACTGAGACACGGTCCAGACTCCTACGGGAGGCAGCAGTGG\n"
    "GGAATATTGCACAATGGGCGCAAGCCTGATGCAGCCATGCCGCGTGTATGAAGAAGGCCT\n"
    "TCGGGTTGTAAAGTACTTTCAGCGGGGAGGAAGGGAGTAAAGTTAATACCTTTGCTCATT\n"
    "GACGTTACCCGCAGAAGAAGCACCGGCTAACTCCGTGCCAGCAGCCGCGGTAATACGGAG\n"
)

_MODE_B_EXAMPLE = {
    "summary": "Mode B — Inline FASTA (simple)",
    "description": "Provide FASTA text inline. Server uploads query and resolves DB/results URLs automatically. Query is E. coli K-12 16S rRNA partial (NR_024570.1) against the 16S_ribosomal_RNA database. outfmt is 5 (BLAST XML) because the result pipeline requires it.",
    "value": {
        "program": "blastn",
        "db": "16S_ribosomal_RNA",
        "query_fasta": _SAMPLE_16S_FASTA,
        "blast_options": {"evalue": 0.05, "max_target_seqs": 100, "outfmt": "5"},
    },
}

_MODE_B_TAXID_EXAMPLE = {
    "summary": "Mode B — with Taxonomy filter",
    "description": "Filter BLAST results by organism taxonomy. is_inclusive=true searches within the taxid, false excludes it. Query is E. coli K-12 16S rRNA (NR_024570.1) filtered to taxid 562 (Escherichia coli). outfmt is 5 (BLAST XML) because the result pipeline requires it.",
    "value": {
        "program": "blastn",
        "db": "16S_ribosomal_RNA",
        "query_fasta": _SAMPLE_16S_FASTA,
        "taxid": 562,
        "is_inclusive": True,
        "blast_options": {"evalue": 0.05, "max_target_seqs": 100, "outfmt": "5"},
    },
}

class JobSubmitRequest(BaseModel):
    """Unified BLAST job submission.

    **Mode A** (Blob URL): provide `db`, `queries`, `results` as full Azure Blob URLs.

    **Mode B** (Inline FASTA): provide `query_fasta` + short `db` name.
    Server auto-uploads query to blob and resolves all URLs.
    Optionally filter by `taxid` + `is_inclusive`.

    Any field a caller sends beyond this schema (e.g. `request_id`) is preserved
    verbatim (bounded) under `passthrough` on the submit / status / result
    payloads so an external caller can correlate. See `_sanitize_passthrough`.
    """
    model_config = {
        # Keep unknown fields (e.g. a caller's request_id) instead of dropping
        # them, so they can be echoed back on status/result. They are read via
        # `model_extra` and bounded by `_sanitize_passthrough` before storage.
        "extra": "allow",
        "json_schema_extra": {"examples": [_MODE_A_EXAMPLE["value"], _MODE_B_EXAMPLE["value"], _MODE_B_TAXID_EXAMPLE["value"]]},
    }

    program: str = Field("blastn", description="BLAST program (blastn, blastp, blastx, tblastn, etc.)", examples=["blastn", "blastp", "blastx"])
    db: str = Field(..., description="Full Blob URL (mode A) or short DB name like '16S_ribosomal_RNA' (mode B)", examples=["16S_ribosomal_RNA"])
    cluster_name: Optional[str] = Field(None, description="AKS cluster name. Uses server default if omitted.")
    # Mode A
    queries: Optional[str] = Field(None, description="Query sequences Blob URL (mode A only)")
    results: Optional[str] = Field(None, description="Results destination Blob URL (mode A only)")
    options: Optional[str] = Field(None, description="Raw BLAST CLI options string (mode A only)")
    # Mode B
    query_fasta: Optional[str] = Field(None, description="Inline FASTA text (mode B). Server auto-uploads to blob storage.")
    taxid: Optional[int] = Field(None, description="NCBI Taxonomy ID for organism filtering", examples=[10244, 9606])
    is_inclusive: Optional[bool] = Field(None, description="true: search within taxid only. false: exclude taxid. Ignored without taxid.")
    blast_options: Optional[BlastOptions] = Field(None, description="Structured BLAST options (mode B). Easier than raw option string.")
    priority: int | str = Field(50, description="Queue priority. Accepts 0-100 or low, normal, high, urgent. Running jobs are not preempted.")
    batch_len: Optional[int] = Field(None, ge=1, le=1_000_000_000, description="Optional ElasticBLAST blast.batch-len override for query batching.")
    idempotency_key: Optional[str] = Field(None, min_length=1, max_length=256, description="Stable caller key. Replays return the same job handle instead of creating duplicate work.")
    resource_profile: str = Field("standard", description="Server-side sizing policy label, e.g. standard or core_nt_safe.")
    submission_source: str = Field(DEFAULT_EXTERNAL_SOURCE, description="Effective source of the submission: dashboard, external_api, terminal, or system.")
    external_correlation_id: Optional[str] = Field(None, max_length=128, description="Caller-side correlation id, e.g. dashboard job id.")

# Bounds for caller-supplied pass-through fields. Job state is persisted in a K8s
# ConfigMap (~1 MiB cap) and echoed on every status/result poll, so an oversized
# or hostile producer must not be able to bloat either. Values are flattened to
# JSON scalars / bounded strings so the payload stays small and flat.
_PASSTHROUGH_MAX_KEYS = 32
_PASSTHROUGH_MAX_KEY_LEN = 128
_PASSTHROUGH_MAX_VALUE_LEN = 1024
_PASSTHROUGH_MAX_TOTAL_BYTES = 16384


def _sanitize_passthrough(extra: Any) -> dict[str, Any]:
    """Bound + flatten the caller-supplied fields beyond the submit schema.

    ``extra`` is ``JobSubmitRequest.model_extra`` — the dict of fields a caller
    sent that the model does not declare (e.g. ``request_id``). They are kept so
    the submit / status / result payloads can echo them back for correlation,
    but bounded first: at most ``_PASSTHROUGH_MAX_KEYS`` keys, each key/value
    length capped, complex (dict/list) values flattened to a bounded JSON string,
    and a total-size budget. Returns ``{}`` when there is nothing usable, so a
    job submitted without extra fields stores and echoes no ``passthrough`` key.
    """
    if not isinstance(extra, dict) or not extra:
        return {}
    out: dict[str, Any] = {}
    total = 0
    for raw_key, raw_value in extra.items():
        if len(out) >= _PASSTHROUGH_MAX_KEYS:
            break
        key = str(raw_key).strip()[:_PASSTHROUGH_MAX_KEY_LEN]
        if not key:
            continue
        if raw_value is None or isinstance(raw_value, (bool, int, float)):
            value: Any = raw_value
        elif isinstance(raw_value, str):
            value = raw_value[:_PASSTHROUGH_MAX_VALUE_LEN]
        else:
            try:
                value = json.dumps(raw_value, default=str)[:_PASSTHROUGH_MAX_VALUE_LEN]
            except (TypeError, ValueError):
                value = str(raw_value)[:_PASSTHROUGH_MAX_VALUE_LEN]
        total += len(key) + len(str(value))
        if total > _PASSTHROUGH_MAX_TOTAL_BYTES:
            break
        out[key] = value
    return out
