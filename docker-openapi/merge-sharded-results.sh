#!/bin/bash
# Merge DB-partitioned BLAST outputs into one deterministic result file.

set -euo pipefail

if [ "$#" -lt 6 ]; then
    echo "Usage: $0 <input-tsv> <output-gz> <report-json> <num-shards> <blast-program> <blast-options>" >&2
    exit 2
fi

INPUT_TSV="$1"
OUTPUT_GZ="$2"
REPORT_JSON="$3"
NUM_SHARDS="$4"
BLAST_PROGRAM="$5"
BLAST_OPTIONS="$6"

python3 - "$INPUT_TSV" "$OUTPUT_GZ" "$REPORT_JSON" "$NUM_SHARDS" "$BLAST_PROGRAM" "$BLAST_OPTIONS" <<'PY'
import copy
import gzip
import json
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path

BLAST_EVALUE_EPSILON = 1.0e-180
WEB_BLAST_STATISTICS_MAX_BYTES = 16 * 1024
SEQUENCE_IDENTITY_MODE = "aligned_sequence_query_span"
SEQUENCE_IDENTITY_VERSION = 1
SEQUENCE_DIVERSITY_MAX_CANDIDATE_POOL_SIZE = 5_000
SEQUENCE_GROUP_REPORT_LIMIT = 5_000
SEQUENCE_SOURCE_MARKER = "# ELB source-shard:"


def result_selection_policy():
    value = os.environ.get("ELB_RESULT_SELECTION_POLICY", "native_top_n").strip()
    if value not in {"native_top_n", "diversity_aware", "sequence_diversity"}:
        raise ValueError(f"Unsupported result selection policy: {value}")
    return value


def optional_positive_env(name):
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def load_web_blast_statistics():
    path_value = os.environ.get("ELB_WEB_BLAST_STATISTICS_FILE", "").strip()
    if not path_value:
        return None
    path = Path(path_value)
    if not path.is_file() or path.stat().st_size > WEB_BLAST_STATISTICS_MAX_BYTES:
        raise ValueError("Web BLAST statistics manifest is missing or oversized")
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise ValueError("Web BLAST statistics manifest is invalid") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("Web BLAST statistics manifest schema is unsupported")
    integer_fields = (
        "query_length",
        "filtered_database_letters",
        "filtered_database_sequences",
        "length_adjustment",
        "effective_search_space",
        "scoring_search_space",
        "result_database_letters",
        "active_database_letters",
        "active_database_sequences",
    )
    for field in integer_fields:
        try:
            value = int(payload.get(field) or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Web BLAST statistics field {field} is invalid") from exc
        if value <= 0:
            raise ValueError(f"Web BLAST statistics field {field} is invalid")
        payload[field] = value
    if not str(payload.get("query_id") or "").strip():
        raise ValueError("Web BLAST statistics query_id is invalid")
    if not str(payload.get("active_source_version") or "").strip():
        raise ValueError("Web BLAST statistics active_source_version is invalid")
    return payload


def option_scalar(options_text, flag):
    tokens = shlex.split(options_text or "")
    values = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == flag:
            if index + 1 >= len(tokens) or tokens[index + 1].startswith("-"):
                raise ValueError(f"{flag} requires a scalar value")
            values.append(tokens[index + 1])
            index += 2
            continue
        if token.startswith(f"{flag}="):
            values.append(token.split("=", 1)[1])
        index += 1
    if len(values) != 1:
        raise ValueError(f"Web BLAST exact merge requires exactly one {flag} value")
    try:
        value = int(values[0])
    except ValueError as exc:
        raise ValueError(f"{flag} must be a positive integer") from exc
    if value <= 0:
        raise ValueError(f"{flag} must be a positive integer")
    return value


def validate_web_blast_statistics(payload, query_order, queries, blast_options):
    if len(query_order) != 1:
        raise ValueError("Web BLAST statistics require exactly one merged query")
    query_id = query_order[0]
    item = queries[query_id]
    template = item["template"]
    try:
        query_length = int(text_at(template, "Iteration_query-len", "0"))
    except ValueError as exc:
        raise ValueError("Merged query length is invalid") from exc
    query_def_id = text_at(template, "Iteration_query-def").strip().split(None, 1)[0]
    manifest_query_id = str(payload["query_id"]).strip()
    if manifest_query_id not in {query_id, query_def_id}:
        raise ValueError("Web BLAST statistics query identity does not match the result")
    if query_length != payload["query_length"]:
        raise ValueError("Web BLAST statistics query length does not match the result")
    if (
        item["db_len"] != payload["active_database_letters"]
        or item["db_num"] != payload["active_database_sequences"]
    ):
        raise ValueError("Web BLAST statistics active database does not match shard totals")
    length_adjustment = payload["length_adjustment"]
    effective_query_length = query_length - length_adjustment
    effective_database_length = (
        payload["filtered_database_letters"]
        - payload["filtered_database_sequences"] * length_adjustment
    )
    if effective_query_length <= 0 or effective_database_length <= 0:
        raise ValueError("Web BLAST statistics contain invalid effective lengths")
    if (
        effective_query_length * effective_database_length
        != payload["effective_search_space"]
    ):
        raise ValueError("Web BLAST reported effective search space is inconsistent")
    if (
        effective_query_length * payload["filtered_database_letters"]
        != payload["scoring_search_space"]
    ):
        raise ValueError("Web BLAST scoring search space is inconsistent")
    if payload["result_database_letters"] not in {
        payload["filtered_database_letters"],
        payload["active_database_letters"],
    }:
        raise ValueError("Web BLAST result database length is inconsistent")
    if option_scalar(blast_options, "-dbsize") != payload["filtered_database_letters"]:
        raise ValueError("BLAST -dbsize does not match Web BLAST statistics")
    if option_scalar(blast_options, "-searchsp") != payload["scoring_search_space"]:
        raise ValueError("BLAST -searchsp does not match Web BLAST scoring space")


def _accession_base(accession):
    if not accession:
        return accession
    if "." not in accession:
        return accession
    head, tail = accession.rsplit(".", 1)
    return head if tail.isdigit() else accession


def load_tie_order_oracle(warnings, candidate_accessions=None):
    oracle_path = os.environ.get("ELB_TIE_ORDER_FILE", "").strip()
    if not oracle_path:
        return None, {}, 0, []
    path = Path(oracle_path)
    if not path.exists():
        warnings.append(f"Tie-order oracle file was not found: {oracle_path}")
        return oracle_path, {}, 0, []

    db_order = tie_order_oracle_source() == "db_order"
    wanted_accessions = (
        observed_accession_keys(candidate_accessions or ())
        if db_order and candidate_accessions is not None
        else None
    )
    order = {}
    accessions = []
    unique_accessions = 0
    parsed_accessions = 0
    logical_oid_rank = -1
    previous_oid_key = None
    with path.open() as oracle_file:
        for raw_line in oracle_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            tokens = re.split(r"[\t, ]+", line)
            explicit_oid_key = None
            if (
                len(tokens) >= 3
                and re.fullmatch(r"[0-9]{2}", tokens[0])
                and tokens[1].isdigit()
            ):
                explicit_oid_key = (tokens[0], int(tokens[1]))
                accession = tokens[2]
            elif len(tokens) >= 12:
                accession = tokens[1]
            elif len(tokens) >= 2 and tokens[0].isdigit():
                accession = tokens[1]
            else:
                accession = tokens[0]
            if not accession:
                continue
            if explicit_oid_key is not None:
                if explicit_oid_key != previous_oid_key:
                    logical_oid_rank += 1
                    previous_oid_key = explicit_oid_key
                accession_rank = logical_oid_rank
            elif wanted_accessions is not None:
                accession_rank = parsed_accessions
            else:
                accession_rank = unique_accessions
            parsed_accessions += 1
            base = _accession_base(accession)
            if wanted_accessions is not None and not (
                accession in wanted_accessions or base in wanted_accessions
            ):
                continue
            if accession not in order:
                order[accession] = accession_rank
                if wanted_accessions is None:
                    accessions.append(accession)
                    unique_accessions += 1
            if base and base not in order:
                order[base] = order[accession]
    oracle_accession_count = (
        parsed_accessions if wanted_accessions is not None else unique_accessions
    )
    if oracle_accession_count:
        warnings.append(
            "Tie-order oracle is enabled; ties are ordered by the supplied same-snapshot accession list"
        )
        if wanted_accessions is not None:
            warnings.append(
                "DB-order oracle was streamed; only candidate accession ranks were retained in memory"
            )
    else:
        warnings.append(f"Tie-order oracle file contained no usable accessions: {oracle_path}")
    return oracle_path, order, oracle_accession_count, accessions


def observed_accession_keys(accessions):
    keys = set()
    for accession in accessions:
        if not accession:
            continue
        keys.add(accession)
        base = _accession_base(accession)
        if base:
            keys.add(base)
    return keys


def oracle_missing_accessions(oracle_accessions, observed_keys):
    missing = []
    for accession in oracle_accessions:
        base = _accession_base(accession)
        if accession not in observed_keys and (not base or base not in observed_keys):
            missing.append(accession)
    return missing


def strict_oracle_enabled():
    return os.environ.get("ELB_TIE_ORDER_STRICT", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def tie_order_oracle_source():
    source = os.environ.get("ELB_TIE_ORDER_SOURCE", "").strip().lower()
    return source if source in {"query", "db_order"} else "query"


def blast_evalue_sort_key(evalue):
    # Mirrors NCBI BLAST core s_EvalueComp: values below 1e-180 compare equal.
    return 0.0 if evalue < BLAST_EVALUE_EPSILON else evalue


def deterministic_tie_order_enabled():
    # Opt-in (default OFF). When enabled, ties within an identical
    # (evalue, bitscore) score class are broken by subject accession instead
    # of input/shard concatenation order. This makes the selected set AND its
    # ordering reproducible across reruns regardless of which shard finished
    # first -- a reproducibility requirement for validated diagnostic
    # pipelines. A tie-order oracle, when present, still takes precedence.
    return os.environ.get("ELB_DETERMINISTIC_TIE_ORDER", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def diversity_aware_cutoff_limit():
    # Default auto (None); set to 0 to restore strict top-N selection or to a
    # positive integer k for a fixed reservation. Auto mode preserves the
    # lower-scoring unique-subject proportion observed in the merged shard
    # candidate pool instead of allowing a cross-shard top-score tie class to
    # consume the entire result window.
    raw = os.environ.get("ELB_DIVERSITY_AWARE_CUTOFF", "").strip()
    if not raw or raw.lower() == "auto":
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else 0


def diversity_reservation_mode(limit, strict_oracle, db_order_exact=False):
    if strict_oracle:
        return "strict_oracle"
    if db_order_exact:
        return "db_order_exact"
    if limit is None:
        return "proportional"
    return "fixed" if limit > 0 else "off"


def tie_break_sort_component(tie_order, accession, ordinal):
    # Tertiary tie-break component used after (evalue, -bitscore). The oracle,
    # when active, always wins. Otherwise, in deterministic mode the subject
    # accession provides a stable, rerun-independent order; the input ordinal
    # is kept as the final disambiguator.
    if tie_order:
        return oracle_sort_key(
            tie_order,
            accession,
            ordinal,
            reverse=tie_order_oracle_source() == "db_order",
        )
    if deterministic_tie_order_enabled():
        return (0, accession)
    return (0, ordinal)


def ranking_basis_label(tie_order, raw_score_available=False):
    if tie_order and tie_order_oracle_source() == "db_order" and raw_score_available:
        return "blast_evalue_raw_score_db_oid_desc"
    if tie_order:
        return "evalue_bitscore_oracle_ordinal"
    if deterministic_tie_order_enabled():
        return "evalue_bitscore_accession_ordinal"
    return "evalue_bitscore_ordinal"


def selection_equivalence_label(tie_order, strict_oracle, raw_score_available):
    if strict_oracle and tie_order:
        return "strict_query_oracle"
    if (
        tie_order
        and tie_order_oracle_source() == "db_order"
        and raw_score_available
    ):
        return "full_db_hitlist_exact"
    return "heuristic"


def apply_diversity_reservation(selected, sorted_hits, limit, subject_key):
    # Replace the tail of a saturated selection window with the best
    # lower-scoring near-miss hits. Only acts when the ENTIRE selected window
    # is a single tied (evalue, bitscore) score class -- i.e. informative
    # lower-scoring subjects were pushed out purely by the max_target_seqs
    # cutoff. Both tabular and XML hit tuples begin with
    # (evalue, -bitscore).
    top_class = (selected[0][0], selected[0][1])
    if any((hit[0], hit[1]) != top_class for hit in selected):
        return selected, 0, 0

    # Count each subject once using its best-ranked row/Hit. A lower-ranked HSP
    # for an already seen subject is not a new variant candidate. Formats with
    # no subject column intentionally fall back to row identity.
    candidate_hits = []
    candidate_subjects = set()
    for hit in sorted_hits:
        subject = subject_key(hit)
        if subject and subject in candidate_subjects:
            continue
        if subject:
            candidate_subjects.add(subject)
        candidate_hits.append(hit)

    selected_subjects = set()
    selected_candidate_count = 0
    for hit in selected:
        subject = subject_key(hit)
        if subject and subject in selected_subjects:
            continue
        if subject:
            selected_subjects.add(subject)
        selected_candidate_count += 1

    top_candidates = [
        hit for hit in candidate_hits if (hit[0], hit[1]) == top_class
    ]
    if len(top_candidates) <= selected_candidate_count:
        return selected, 0, 0

    near_misses = [
        hit for hit in candidate_hits if (hit[0], hit[1]) != top_class
    ]
    if not near_misses:
        return selected, 0, 0

    if limit is None:
        # Preserve the lower-score share represented in the shard candidate
        # pool. Integer ceiling guarantees at least one near-miss whenever a
        # real tied-class overflow and a lower-scoring candidate coexist.
        candidate_count = len(top_candidates) + len(near_misses)
        reserve_limit = (
            len(selected) * len(near_misses) + candidate_count - 1
        ) // candidate_count
    else:
        reserve_limit = limit
    # Never replace every top-class hit. At N=1 this intentionally reserves
    # zero slots so the result count stays within max_target_seqs.
    reserve = min(reserve_limit, len(near_misses), max(0, len(selected) - 1))
    if reserve <= 0:
        return selected, 0, len(near_misses)
    kept = selected[: len(selected) - reserve]
    return kept + near_misses[:reserve], reserve, len(near_misses)


def oracle_rank(order, accession):
    return order.get(accession, order.get(_accession_base(accession)))


def oracle_sort_key(order, accession, fallback, reverse=False):
    if not order:
        return (0, fallback)
    rank = oracle_rank(order, accession)
    if rank is None:
        return (1, fallback)
    return (0, -rank if reverse else rank)


def unmapped_oracle_accessions(order, accessions):
    return sorted({accession for accession in accessions if oracle_rank(order, accession) is None})


def tabular_subject_hits(connection, query_id):
    # Select one best-ranked HSP per subject on disk. The first ordinal remains
    # the stable fallback even when another HSP supplies the subject's best
    # e-value/score, matching the historical in-memory grouping contract.
    rows = connection.execute(
        """
        SELECT evalue_key, negative_score, first_ordinal, accession,
               subject_key, display_evalue, display_bitscore, raw_score
        FROM (
            SELECT evalue_key, negative_score, ordinal, accession, subject_key,
                   display_evalue, display_bitscore, raw_score,
                   MIN(ordinal) OVER (PARTITION BY subject_key) AS first_ordinal,
                   ROW_NUMBER() OVER (
                       PARTITION BY subject_key
                       ORDER BY evalue_key, negative_score, ordinal
                   ) AS subject_rank
            FROM tabular_hits
            WHERE query_id = ?
        )
        WHERE subject_rank = 1
        """,
        (query_id,),
    )
    return list(rows)


def selected_tabular_row_offsets(connection, query_id, subject_keys):
    offsets = {key: [] for key in subject_keys}
    # Stay below SQLite's conservative host-parameter limit while fetching
    # every HSP offset for only the subjects selected for final output.
    for start in range(0, len(subject_keys), 400):
        chunk = subject_keys[start : start + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = connection.execute(
            f"""
            SELECT subject_key, row_offset
            FROM tabular_hits
            WHERE query_id = ? AND subject_key IN ({placeholders})
            ORDER BY ordinal
            """,
            (query_id, *chunk),
        )
        for subject_key, row_offset in rows:
            offsets[subject_key].append(row_offset)
    return offsets


def apply_sequence_tie_ranks(connection, tie_order):
    if not tie_order:
        return
    updates = []
    for (accession,) in connection.execute(
        "SELECT DISTINCT accession FROM tabular_hits WHERE accession != ''"
    ):
        rank = oracle_rank(tie_order, accession)
        if rank is not None:
            updates.append((rank, accession))
        if len(updates) >= 1000:
            connection.executemany(
                "UPDATE tabular_hits SET tie_rank = ? WHERE accession = ?",
                updates,
            )
            updates.clear()
    if updates:
        connection.executemany(
            "UPDATE tabular_hits SET tie_rank = ? WHERE accession = ?",
            updates,
        )
    connection.commit()


def sequence_order_sql(tie_order):
    if tie_order:
        direction = "DESC" if tie_order_oracle_source() == "db_order" else "ASC"
        return f"tie_rank IS NULL, tie_rank {direction}, ordinal"
    if deterministic_tie_order_enabled():
        return "accession, ordinal"
    return "ordinal"


def sequence_diversity_representatives(connection, query_id, limit, tie_order):
    tie_sql = sequence_order_sql(tie_order)
    return list(
        connection.execute(
            f"""
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY query_id, normalized_sseq, qstart, qend
                           ORDER BY evalue_key, negative_score, {tie_sql}
                       ) AS group_rank
                FROM tabular_hits
                WHERE query_id = ?
            ),
            group_counts AS (
                SELECT query_id, normalized_sseq, qstart, qend,
                       COUNT(DISTINCT accession) AS accession_count,
                       COUNT(*) AS source_row_count
                FROM tabular_hits
                WHERE query_id = ?
                GROUP BY query_id, normalized_sseq, qstart, qend
            )
            SELECT ranked.evalue_key, ranked.negative_score, ranked.ordinal,
                   ranked.accession, ranked.row_offset, ranked.display_evalue,
                   ranked.display_bitscore, ranked.raw_score,
                   group_counts.accession_count, group_counts.source_row_count
            FROM ranked
            JOIN group_counts USING (query_id, normalized_sseq, qstart, qend)
            WHERE ranked.group_rank = 1
            ORDER BY ranked.evalue_key, ranked.negative_score, {tie_sql}
            LIMIT ?
            """,
            (query_id, query_id, limit),
        )
    )


def observed_sequence_counts(connection):
    observed_groups = connection.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM tabular_hits
            GROUP BY query_id, normalized_sseq, qstart, qend
        )
        """
    ).fetchone()[0]
    observed_subjects = connection.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT 1
            FROM tabular_hits
            GROUP BY query_id, accession
        )
        """
    ).fetchone()[0]
    return int(observed_groups), int(observed_subjects)


def candidate_pool_saturated_shards(connection, candidate_pool_size):
    return int(
        connection.execute(
            """
            SELECT COUNT(DISTINCT source_shard)
            FROM (
                SELECT source_shard, query_id
                FROM tabular_hits
                WHERE source_shard != ''
                GROUP BY source_shard, query_id
                HAVING COUNT(DISTINCT accession) >= ?
            )
            """,
            (candidate_pool_size,),
        ).fetchone()[0]
    )


# Field-aware tabular column resolution. The shard merge historically assumed
# the BLAST `std` column order (qseqid=0, sseqid=1, evalue=10, bitscore=11). An
# extended/reordered outfmt such as
# `-outfmt "7 sseqid staxids sstrand pident evalue bitscore ..."` (the layout
# that surfaces subject taxids/names) breaks every one of those fixed positions,
# so the group/rank/oracle columns are resolved BY NAME from the outfmt
# specifier instead. A plain `6`/`7` or a `std`-prefixed layout resolves back to
# the exact historical positions, so existing runs are byte-identical.
_STD_TABULAR_FIELDS = [
    "qseqid", "sseqid", "pident", "length", "mismatch", "gapopen",
    "qstart", "qend", "sstart", "send", "evalue", "bitscore",
]
# Query / subject identity codes that can serve as the per-query group key and
# the tie-order oracle accession respectively (BLAST+ accepts several aliases).
_QUERY_FIELD_CODES = {"qseqid", "qacc", "qaccver", "qgi"}
_SUBJECT_FIELD_CODES = {"sseqid", "sacc", "saccver", "sgi"}


def expand_outfmt_fields(spec):
    """Return the ordered list of tabular column field codes for an outfmt spec.

    `spec` is the full `-outfmt` value (with or without the leading numeric
    code), e.g. "7 std staxids" or "sseqid staxids evalue bitscore". An empty
    spec (plain `-outfmt 6`/`7`) resolves to the standard 12 columns. The `std`
    token expands in place to those 12 codes, matching BLAST+ semantics.
    """
    tokens = (spec or "").strip().strip("'\"").split()
    if tokens and tokens[0].isdigit():
        tokens = tokens[1:]
    if not tokens:
        return list(_STD_TABULAR_FIELDS)
    fields = []
    for tok in tokens:
        if tok == "std":
            fields.extend(_STD_TABULAR_FIELDS)
        else:
            fields.append(tok.lower())
    return fields


def resolve_tabular_columns(spec, warnings):
    """Resolve group/rank/oracle column indices BY NAME from a tabular outfmt.

    Returns ``(qseqid_idx, evalue_idx, bitscore_idx, score_idx, subject_idx)``
    where the query, raw-score, and subject indices may be ``None``. Raises
    ``ValueError`` when evalue or bitscore is absent. A
    missing query column means the caller merges every hit as a single query
    group (correct only for single-query searches); a missing subject column
    disables the tie-order oracle / deterministic accession tie-break.
    """
    fields = expand_outfmt_fields(spec)

    def first_index(codes):
        for i, field in enumerate(fields):
            if field in codes:
                return i
        return None

    qseqid_idx = first_index(_QUERY_FIELD_CODES)
    evalue_idx = first_index({"evalue"})
    bitscore_idx = first_index({"bitscore"})
    score_idx = first_index({"score"})
    subject_idx = first_index(_SUBJECT_FIELD_CODES)
    if evalue_idx is None or bitscore_idx is None:
        raise ValueError(
            "sharded tabular merge requires evalue and bitscore columns in the "
            f"-outfmt specifier; resolved fields={fields}"
        )
    if qseqid_idx is None:
        warnings.append(
            "outfmt has no query column; all hits are merged as a single query "
            "group (correct only for single-query searches)"
        )
    if subject_idx is None:
        warnings.append(
            "outfmt has no subject accession column; the tie-order oracle and "
            "deterministic accession tie-break are disabled, and max_target_seqs "
            "plus diversity reservation operate on rows instead of subjects"
        )
    return qseqid_idx, evalue_idx, bitscore_idx, score_idx, subject_idx


def resolve_sequence_diversity_columns(spec):
    fields = expand_outfmt_fields(spec)

    def first_index(codes):
        for index, field in enumerate(fields):
            if field in codes:
                return index
        return None

    resolved = {
        "query_identity": first_index(_QUERY_FIELD_CODES),
        "accession": first_index(_SUBJECT_FIELD_CODES),
        "sseq": first_index({"sseq"}),
        "qstart": first_index({"qstart"}),
        "qend": first_index({"qend"}),
        "evalue": first_index({"evalue"}),
        "bitscore": first_index({"bitscore"}),
        "score": first_index({"score"}),
    }
    missing = [field for field, index in resolved.items() if index is None]
    if missing:
        raise ValueError(
            "sequence_diversity requires effective tabular fields: "
            + ", ".join(missing)
        )
    return resolved


def xml_subject_accession(hit):
    accession = text_at(hit, "Hit_accession")
    if accession:
        return accession
    hit_id = text_at(hit, "Hit_id")
    if "|" in hit_id:
        parts = [part for part in hit_id.split("|") if part]
        return parts[-1] if parts else hit_id
    return hit_id


def parse_max_target_seqs(options_text):
    warnings = []
    max_hits = 500
    try:
        tokens = shlex.split(options_text or "")
    except ValueError as exc:
        raise ValueError(f"Could not parse BLAST options: {exc}") from exc

    for idx, token in enumerate(tokens):
        value = None
        if token == "-max_target_seqs" and idx + 1 < len(tokens):
            value = tokens[idx + 1]
        elif token.startswith("-max_target_seqs="):
            value = token.split("=", 1)[1]
        if value is None:
            continue
        try:
            parsed = int(value)
        except ValueError:
            raise ValueError(f"max_target_seqs must be an integer, got {value}")
        if parsed <= 0:
            raise ValueError(f"max_target_seqs must be positive, got {value}")
        return parsed, warnings
    return max_hits, warnings


def resolve_result_max_target_seqs(candidate_pool_size):
    raw = os.environ.get("ELB_REQUESTED_MAX_TARGET_SEQS", "").strip()
    if not raw:
        return candidate_pool_size
    try:
        requested = int(raw)
    except ValueError as exc:
        raise ValueError(f"requested max_target_seqs must be an integer, got {raw}") from exc
    if requested <= 0:
        raise ValueError(f"requested max_target_seqs must be positive, got {raw}")
    if requested > candidate_pool_size:
        raise ValueError(
            "requested max_target_seqs cannot exceed the shard candidate pool "
            f"({requested} > {candidate_pool_size})"
        )
    return requested


def parse_outfmt(options_text):
    try:
        tokens = shlex.split(options_text or "")
    except ValueError as exc:
        raise ValueError(f"Could not parse BLAST options: {exc}") from exc
    outfmt = "6"
    for idx, token in enumerate(tokens):
        if token == "-outfmt" and idx + 1 < len(tokens):
            outfmt = tokens[idx + 1]
        elif token.startswith("-outfmt="):
            outfmt = token.split("=", 1)[1]
    return outfmt.strip().split(maxsplit=1)[0] or "6"


def parse_outfmt_spec(options_text):
    """Return the FULL `-outfmt` value (numeric code + field codes), or "".

    Unlike :func:`parse_outfmt` (which returns only the leading code so the
    dispatcher can pick xml vs tabular), this keeps the entire specifier so the
    tabular merge can resolve its group/rank/oracle columns by field name.

    The canonical wire format is UNQUOTED — quotes break the raw YAML
    substitution elastic-blast uses to inject ``ELB_BLAST_OPTIONS``. So a
    multi-token specifier arrives as separate ``shlex`` tokens
    (``-outfmt 7 sseqid staxids``) and is rejoined here by collecting every
    token after ``-outfmt`` up to the next ``-flag`` (BLAST format field codes
    never start with ``-``). A quoted input (``-outfmt "7 sseqid"``) already
    arrives as one token and is handled by the same loop, so both forms resolve
    to the full specifier.
    """
    try:
        tokens = shlex.split(options_text or "")
    except ValueError:
        return ""
    spec = ""
    i = 0
    n = len(tokens)
    while i < n:
        token = tokens[i]
        if token == "-outfmt" and i + 1 < n:
            parts = []
            j = i + 1
            while j < n and not tokens[j].startswith("-"):
                parts.append(tokens[j])
                j += 1
            spec = " ".join(parts)
            i = j
            continue
        if token.startswith("-outfmt="):
            spec = token.split("=", 1)[1]
        i += 1
    return spec.strip()


def merge_tabular(
    input_tsv,
    output_gz,
    report_json,
    num_shards,
    blast_program,
    max_hits,
    candidate_pool_size,
    warnings,
    outfmt="6",
    outfmt_spec="",
):
    selection_policy = result_selection_policy()
    expected_shards = int(num_shards)
    succeeded_shards = optional_positive_env("ELB_SUCCEEDED_SHARDS") or expected_shards
    if selection_policy == "sequence_diversity" and succeeded_shards != expected_shards:
        raise ValueError(
            "sequence_diversity requires every expected shard to succeed "
            f"({succeeded_shards} of {expected_shards})"
        )
    oracle_path = os.environ.get("ELB_TIE_ORDER_FILE", "").strip() or None
    db_order_requested = bool(oracle_path) and tie_order_oracle_source() == "db_order"
    # Resolve the group / rank / oracle columns BY NAME from the outfmt
    # specifier (handles reordered + extended layouts like
    # `7 sseqid staxids ... evalue bitscore ...`). For a plain or `std`-prefixed
    # layout these resolve back to the historical positions (qseqid=0,
    # sseqid=1, evalue=10, bitscore=11), so existing runs are byte-identical.
    qseqid_idx, evalue_idx, bitscore_idx, score_idx, subject_idx = resolve_tabular_columns(
        outfmt_spec, warnings
    )
    sequence_columns = (
        resolve_sequence_diversity_columns(outfmt_spec)
        if selection_policy == "sequence_diversity"
        else None
    )
    if sequence_columns is not None:
        qseqid_idx = sequence_columns["query_identity"]
        subject_idx = sequence_columns["accession"]
        evalue_idx = sequence_columns["evalue"]
        bitscore_idx = sequence_columns["bitscore"]
        score_idx = sequence_columns["score"]
    # A subject accession column is required for the tie-order oracle and the
    # deterministic accession tie-break; without it, neither can run.
    if subject_idx is None:
        if db_order_requested:
            raise ValueError(
                "DB-order exact merge requires a subject accession column in outfmt"
            )
    if db_order_requested and score_idx is None:
        raise ValueError(
            "DB-order exact tabular merge requires the raw score column in outfmt"
        )
    # Lowest column count a data row must have for every resolved index to be
    # addressable (mirrors the historical `< 12` guard for the std layout).
    required_indexes = [
        idx
        for idx in (qseqid_idx, evalue_idx, bitscore_idx, score_idx, subject_idx)
        if idx is not None
    ]
    if sequence_columns is not None:
        required_indexes.extend(sequence_columns.values())
    min_required_cols = max(required_indexes) + 1
    unsupported_rows = 0
    total_input_rows = 0
    ordinal = 0
    # Capture the authoritative `# Fields:` header BLAST itself wrote into the
    # shard outputs (outfmt 7 only). Reusing it verbatim makes the merged
    # output self-describing for EXTENDED / reordered layouts — the merge
    # re-ranks by the resolved evalue/bitscore positions and re-emits the full
    # row, so trailing columns (staxids, sstrand, qseq, sseq, …) are preserved;
    # this keeps the header in sync with them. Plain outfmt 6 input carries no
    # comment lines, so `captured_fields` stays None and the standard 12-field
    # fallback below applies (unchanged behaviour).
    captured_fields = None

    input_path = Path(input_tsv)
    database_fd, database_name = tempfile.mkstemp(
        prefix="merge-tabular-", suffix=".sqlite3", dir=input_path.parent
    )
    os.close(database_fd)
    database_path = Path(database_name)
    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute(
        """
        CREATE TABLE tabular_hits (
            query_id TEXT NOT NULL,
            subject_key TEXT NOT NULL,
            accession TEXT NOT NULL,
            evalue_key REAL NOT NULL,
            negative_score REAL NOT NULL,
            ordinal INTEGER NOT NULL,
            display_evalue REAL NOT NULL,
            display_bitscore REAL NOT NULL,
            raw_score REAL,
            row_offset INTEGER NOT NULL,
            source_shard TEXT NOT NULL,
            normalized_sseq TEXT NOT NULL,
            qstart TEXT NOT NULL,
            qend TEXT NOT NULL,
            tie_rank INTEGER
        )
        """
    )
    insert_rows = []
    source_shard = ""
    observed_source_shards = set()
    if input_path.exists():
        with input_path.open("rb") as handle:
            while True:
                row_offset = handle.tell()
                raw_line = handle.readline()
                if not raw_line:
                    break
                line = raw_line.decode().rstrip("\n")
                if not line:
                    continue
                if line.startswith("#"):
                    if line.startswith(SEQUENCE_SOURCE_MARKER):
                        source_shard = line[len(SEQUENCE_SOURCE_MARKER) :].strip()
                        if source_shard:
                            observed_source_shards.add(source_shard)
                        continue
                    if captured_fields is None and line.startswith("# Fields:"):
                        candidate = line[len("# Fields:") :].strip()
                        if candidate:
                            captured_fields = candidate
                    continue
                total_input_rows += 1
                cols = line.split("\t")
                if len(cols) < min_required_cols:
                    unsupported_rows += 1
                    continue
                try:
                    evalue = float(cols[evalue_idx])
                    bitscore = float(cols[bitscore_idx])
                    raw_score = float(cols[score_idx]) if score_idx is not None else None
                except ValueError:
                    unsupported_rows += 1
                    continue
                ranking_score = raw_score if raw_score is not None else bitscore
                group_key = cols[qseqid_idx] if qseqid_idx is not None else ""
                accession = cols[subject_idx] if subject_idx is not None else ""
                subject_key = f"subject:{accession}" if accession else f"row:{ordinal}"
                normalized_sseq = (
                    cols[sequence_columns["sseq"]].upper().replace("-", "")
                    if sequence_columns is not None
                    else ""
                )
                qstart = cols[sequence_columns["qstart"]] if sequence_columns is not None else ""
                qend = cols[sequence_columns["qend"]] if sequence_columns is not None else ""
                insert_rows.append(
                    (
                        group_key,
                        subject_key,
                        accession,
                        blast_evalue_sort_key(evalue),
                        -ranking_score,
                        ordinal,
                        evalue,
                        bitscore,
                        raw_score,
                        row_offset,
                        source_shard,
                        normalized_sseq,
                        qstart,
                        qend,
                        None,
                    )
                )
                ordinal += 1
                if len(insert_rows) >= 1000:
                    connection.executemany(
                        "INSERT INTO tabular_hits VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        insert_rows,
                    )
                    insert_rows.clear()
    if insert_rows:
        connection.executemany(
            "INSERT INTO tabular_hits VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            insert_rows,
        )
    connection.commit()
    if sequence_columns is not None and unsupported_rows:
        connection.close()
        database_path.unlink(missing_ok=True)
        raise ValueError(
            "sequence_diversity cannot group malformed or incomplete HSP rows "
            f"({unsupported_rows} rows)"
        )
    connection.execute(
        "CREATE INDEX tabular_hits_subject_idx "
        "ON tabular_hits(query_id, subject_key, ordinal)"
    )
    if sequence_columns is not None:
        connection.execute(
            "CREATE INDEX tabular_hits_sequence_idx "
            "ON tabular_hits(query_id, normalized_sseq, qstart, qend, ordinal)"
        )
    candidate_accessions = (
        row[0]
        for row in connection.execute(
            "SELECT DISTINCT accession FROM tabular_hits WHERE accession != ''"
        )
    )
    (
        oracle_path,
        tie_order,
        oracle_unique_accessions,
        oracle_accessions,
    ) = load_tie_order_oracle(warnings, candidate_accessions)
    if sequence_columns is not None:
        apply_sequence_tie_ranks(connection, tie_order)
    strict_oracle = bool(tie_order) and strict_oracle_enabled()
    db_order_exact = db_order_requested
    if db_order_exact and total_input_rows and not tie_order:
        raise ValueError("DB-order oracle does not cover any candidate subjects")
    if subject_idx is None:
        strict_oracle = False
        tie_order = {}
        db_order_exact = False
    if strict_oracle:
        warnings.append("Strict tie-order oracle is enabled; non-oracle hits are excluded")
    query_ids = [
        row[0]
        for row in connection.execute(
            "SELECT DISTINCT query_id FROM tabular_hits ORDER BY query_id"
        )
    ]

    if unsupported_rows:
        warnings.append("Some rows were skipped because they were not outfmt 6 compatible")

    resolved_fields = expand_outfmt_fields(outfmt_spec)
    fields = captured_fields or (
        ", ".join(resolved_fields)
        if resolved_fields != _STD_TABULAR_FIELDS
        else (
            "query acc.ver, subject acc.ver, % identity, alignment length, mismatches, "
            "gap opens, q. start, q. end, s. start, s. end, evalue, bit score"
        )
    )
    blast_label = blast_program.upper() if blast_program else "BLAST"
    tie_break_count = 0
    tie_cutoff_overflow_count = 0
    tie_cutoff_queries = []
    oracle_missing_queries = []
    diversity_limit = (
        0 if strict_oracle or db_order_exact else diversity_aware_cutoff_limit()
    )
    diversity_mode = diversity_reservation_mode(
        diversity_limit, strict_oracle, db_order_exact
    )
    diversity_reserved_count = 0
    diversity_candidate_count = 0
    diversity_queries = []
    total_input_subjects = 0
    total_output_subjects = 0
    total_output_rows = 0
    sequence_group_counts = []
    sequence_groups_seen = 0
    sequence_shortfall = False

    with input_path.open("rb") as row_source, gzip.open(output_gz, "wt") as out:
        for query_id in query_ids:
            if sequence_columns is not None:
                observed_for_query = int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM (
                            SELECT 1 FROM tabular_hits
                            WHERE query_id = ?
                            GROUP BY normalized_sseq, qstart, qend
                        )
                        """,
                        (query_id,),
                    ).fetchone()[0]
                )
                selected = sequence_diversity_representatives(
                    connection, query_id, max_hits, tie_order
                )
                sequence_shortfall = sequence_shortfall or observed_for_query < max_hits
                out.write(f"# {blast_label}\n")
                out.write(f"# Query: {query_id}\n")
                out.write(f"# Database: merged from {num_shards} shards\n")
                out.write(f"# Fields: {fields}\n")
                out.write(f"# {len(selected)} hits found\n")
                for hit in selected:
                    sequence_groups_seen += 1
                    row_source.seek(hit[4])
                    row = row_source.readline().decode().rstrip("\n")
                    out.write(row + "\n")
                    total_output_rows += 1
                    if len(sequence_group_counts) < SEQUENCE_GROUP_REPORT_LIMIT:
                        sequence_group_counts.append(
                            {
                                "sequence_group_ordinal": sequence_groups_seen,
                                "sequence_group_accession_count": int(hit[8]),
                                "sequence_group_source_row_count": int(hit[9]),
                            }
                        )
                total_input_subjects += int(
                    connection.execute(
                        "SELECT COUNT(DISTINCT accession) FROM tabular_hits WHERE query_id = ?",
                        (query_id,),
                    ).fetchone()[0]
                )
                total_output_subjects += len(selected)
                continue
            hits = tabular_subject_hits(connection, query_id)
            total_input_subjects += len(hits)
            if db_order_exact:
                unmapped = unmapped_oracle_accessions(
                    tie_order, (hit[3] for hit in hits)
                )
                if unmapped:
                    raise ValueError(
                        "DB-order oracle does not cover all candidate subjects; "
                        f"query={query_id!r} missing={len(unmapped)} "
                        f"first={unmapped[:10]}"
                    )
            if strict_oracle:
                observed_keys = observed_accession_keys(
                    hit[3] for hit in hits
                )
                missing_accessions = oracle_missing_accessions(oracle_accessions, observed_keys)
                if missing_accessions:
                    oracle_missing_queries.append(
                        {
                            "query_id": query_id,
                            "missing_count": len(missing_accessions),
                            "first_missing_accessions": missing_accessions[:20],
                        }
                    )
                hits = [
                    hit
                    for hit in hits
                    if oracle_sort_key(tie_order, hit[3], hit[2])[0] == 0
                ]
            pair_counts = Counter((hit[0], hit[1]) for hit in hits)
            tie_break_count += sum(count - 1 for count in pair_counts.values() if count > 1)
            sorted_hits = sorted(
                hits,
                key=lambda hit: (
                    hit[0],
                    hit[1],
                    tie_break_sort_component(
                        tie_order, hit[3], hit[2]
                    ),
                    hit[2],
                ),
            )
            selected = sorted_hits[:max_hits]
            if selected and len(sorted_hits) > len(selected):
                cutoff_signature = (selected[-1][0], selected[-1][1])
                cutoff_input_count = sum(
                    1 for hit in sorted_hits if (hit[0], hit[1]) == cutoff_signature
                )
                cutoff_selected_count = sum(
                    1 for hit in selected if (hit[0], hit[1]) == cutoff_signature
                )
                cutoff_overflow = max(0, cutoff_input_count - cutoff_selected_count)
                if cutoff_overflow:
                    tie_cutoff_overflow_count += cutoff_overflow
                    if len(tie_cutoff_queries) < 10:
                        cutoff_item = {
                            "query_id": query_id,
                            "evalue": selected[-1][5],
                            "bitscore": selected[-1][6],
                            "tie_input_count": cutoff_input_count,
                            "tie_selected_count": cutoff_selected_count,
                            "tie_overflow_count": cutoff_overflow,
                        }
                        if selected[-1][7] is not None:
                            cutoff_item["score"] = selected[-1][7]
                        tie_cutoff_queries.append(cutoff_item)
            # Diversity-aware reservation runs AFTER cutoff detection so the
            # truncation report still reflects the pristine strict top-N window.
            if diversity_limit != 0 and selected and len(sorted_hits) > len(selected):
                selected, reserved, candidates = apply_diversity_reservation(
                    selected,
                    sorted_hits,
                    diversity_limit,
                    lambda hit: hit[3],
                )
                if reserved:
                    diversity_reserved_count += reserved
                    diversity_candidate_count += candidates
                    if len(diversity_queries) < 10:
                        diversity_queries.append(
                            {
                                "query_id": query_id,
                                "candidate_count": candidates,
                                "reservation_mode": diversity_mode,
                                "reserved_count": reserved,
                            }
                        )
            out.write(f"# {blast_label}\n")
            out.write(f"# Query: {query_id}\n")
            out.write(f"# Database: merged from {num_shards} shards\n")
            out.write(f"# Fields: {fields}\n")
            out.write(f"# {len(selected)} hits found\n")
            selected_offsets = selected_tabular_row_offsets(
                connection, query_id, [hit[4] for hit in selected]
            )
            for hit in selected:
                for row_offset in selected_offsets[hit[4]]:
                    row_source.seek(row_offset)
                    row = row_source.readline().decode().rstrip("\n")
                    out.write(row + "\n")
                    total_output_rows += 1
            total_output_subjects += len(selected)

    if tie_break_count:
        if db_order_exact:
            warnings.append(
                "Ties were resolved with the BLAST full-DB raw-score and OID comparator"
            )
        else:
            warnings.append(
                "Ties were resolved deterministically but may not match full-DB BLAST internal order"
            )
    if tie_cutoff_overflow_count and not db_order_exact:
        warnings.append(
            "The max_target_seqs cutoff splits a tied score class; strict Web BLAST "
            "ordering may require original BLAST DB subject order"
        )
    if diversity_reserved_count:
        warnings.append(
            "Diversity-aware cutoff reserved lower-scoring near-miss subjects; "
            "the displayed set preserves shard candidate-pool composition and is "
            "not the strict top max_target_seqs by score"
        )

    report = {
        "outfmt": int(str(outfmt).strip().split(maxsplit=1)[0]) if str(outfmt).strip() else 6,
        "format": "blast_tabular",
        "fields": fields,
        "resolved_columns": {
            "qseqid": qseqid_idx,
            "evalue": evalue_idx,
            "bitscore": bitscore_idx,
            "subject": subject_idx,
            **({"score": score_idx} if score_idx is not None else {}),
        },
        "max_target_seqs": max_hits,
        "candidate_pool_size": candidate_pool_size,
        "queries": len(query_ids),
        # Keep the historical tabular `*_hits` row semantics for report
        # consumers; the new `*_subjects` fields carry max_target_seqs units.
        "total_input_hits": total_input_rows,
        "total_input_rows": total_input_rows,
        "total_input_subjects": total_input_subjects,
        "total_output_hits": total_output_rows,
        "total_output_rows": total_output_rows,
        "total_output_subjects": total_output_subjects,
        "unsupported_rows": unsupported_rows,
        "tie_break_count": tie_break_count,
        "tie_cutoff_overflow_count": tie_cutoff_overflow_count,
        "tie_cutoff_queries": tie_cutoff_queries,
        "diversity_reserved_count": diversity_reserved_count,
        "diversity_candidate_count": diversity_candidate_count,
        "diversity_reservation_mode": diversity_mode,
        "diversity_queries": diversity_queries,
        "num_shards": int(num_shards),
        "ranking_basis": ranking_basis_label(tie_order, score_idx is not None),
        "selection_equivalence": selection_equivalence_label(
            tie_order, strict_oracle, score_idx is not None
        ),
        "tie_order_oracle_path": oracle_path,
        "tie_order_oracle_source": tie_order_oracle_source() if tie_order else None,
        "tie_order_oracle_accessions": oracle_unique_accessions,
        "tie_order_oracle_strict": strict_oracle,
        "tie_order_oracle_missing_count": sum(
            item["missing_count"] for item in oracle_missing_queries
        ),
        "tie_order_oracle_missing_queries": oracle_missing_queries,
        "warnings": warnings,
        "result_selection_policy_requested": selection_policy,
        "result_selection_policy_applied": selection_policy,
    }
    if sequence_columns is not None:
        observed_groups, observed_subjects = observed_sequence_counts(connection)
        saturated_shards = candidate_pool_saturated_shards(
            connection, candidate_pool_size
        )
        shortfall_reasons = []
        if saturated_shards:
            shortfall_reasons.append("candidate_pool_saturated")
        if not ordinal:
            shortfall_reasons.append("no_candidates_observed")
        elif sequence_shortfall:
            shortfall_reasons.append("insufficient_unique_groups_in_observed_pool")
        report.update(
            {
                "sequence_identity_mode": SEQUENCE_IDENTITY_MODE,
                "sequence_identity_version": SEQUENCE_IDENTITY_VERSION,
                "requested_sequence_groups": max_hits,
                "returned_sequence_groups": total_output_subjects,
                "candidate_pool_size_requested_per_shard": optional_positive_env(
                    "ELB_CANDIDATE_POOL_SIZE_REQUESTED"
                ),
                "candidate_pool_size_applied_per_shard": candidate_pool_size,
                "observed_candidate_rows": ordinal,
                "observed_candidate_subjects": observed_subjects,
                "observed_sequence_groups": observed_groups,
                "expected_shards": expected_shards,
                "succeeded_shards": succeeded_shards,
                "candidate_pool_saturated_shards": saturated_shards,
                "observed_pool_complete": (
                    len(observed_source_shards) == expected_shards
                    and saturated_shards == 0
                ),
                "shortfall_reasons": shortfall_reasons,
                "sequence_group_counts": sequence_group_counts,
                "sequence_group_counts_truncated": (
                    total_output_subjects > len(sequence_group_counts)
                ),
            }
        )
        report["ranking_basis"] = "blast_evalue_raw_score_existing_order_ordinal"
        report["selection_equivalence"] = "observed_candidate_pool"
        report["diversity_reservation_mode"] = "not_applicable"
    Path(report_json).write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    connection.close()
    database_path.unlink(missing_ok=True)
    return total_output_subjects, len(query_ids)


def text_at(element, path, default=""):
    found = element.find(path)
    return found.text if found is not None and found.text is not None else default


def int_at(element, path):
    text = text_at(element, path)
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def set_child_text(element, path, value):
    child = element.find(path)
    if child is None:
        child = ET.SubElement(element, path)
    child.text = str(value)


def derive_hsp_len(query_len, db_len, db_num, eff_space):
    for hsp_len in range(query_len + 1):
        if (query_len - hsp_len) * (db_len - (db_num * hsp_len)) == eff_space:
            return hsp_len
    return None


def normalize_sharded_db_name(db_name):
    stripped = (db_name or "").strip()
    if not stripped:
        return stripped
    return re.sub(r"_shard_\d+$", "", stripped)


def _length_adjustment_blast_options(options_text):
    tokens = shlex.split(options_text or "")
    valued = {"-task", "-reward", "-penalty", "-gapopen", "-gapextend"}
    out = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token in valued and index + 1 < len(tokens):
            out.extend((token, tokens[index + 1]))
            index += 2
            continue
        if any(token.startswith(f"{flag}=") for flag in valued):
            out.append(token)
        elif token == "-ungapped":
            out.append(token)
            if index + 1 < len(tokens) and not tokens[index + 1].startswith("-"):
                out.append(tokens[index + 1])
                index += 1
        index += 1
    return out


def recalibrate_full_db_hsp_lengths(queries, blast_program, options_text, warnings):
    """Return native full-DB length adjustments for exact blastn XML merges."""
    if blast_program != "blastn":
        warnings.append(
            "Full-DB Statistics_hsp-len recalibration is currently available for blastn only"
        )
        return {}
    blastn = shutil.which("blastn")
    makeblastdb = shutil.which("makeblastdb")
    if not blastn or not makeblastdb:
        warnings.append(
            "BLAST tools were unavailable for full-DB Statistics_hsp-len recalibration"
        )
        return {}

    grouped = defaultdict(list)
    for query_id, item in queries.items():
        try:
            query_len = int(text_at(item["template"], "Iteration_query-len", "0"))
        except ValueError:
            query_len = 0
        if query_len > 0 and item["db_len"] > 0 and item["db_num"] > 0:
            grouped[(item["db_len"], item["db_num"])].append((query_id, query_len))
    calibrated = {}
    try:
        relevant_options = _length_adjustment_blast_options(options_text)
    except ValueError as exc:
        warnings.append(f"Could not parse options for HSP-length recalibration: {exc}")
        return {}

    for group_index, ((db_len, db_num), query_items) in enumerate(grouped.items()):
        try:
            with tempfile.TemporaryDirectory(prefix="elb-hsp-len-") as temp_dir:
                root = Path(temp_dir)
                query_path = root / "queries.fa"
                query_path.write_text(
                    "".join(
                        f">q{index}\n{('ACGT' * ((length + 3) // 4))[:length]}\n"
                        for index, (_query_id, length) in enumerate(query_items)
                    )
                )
                max_length = max(length for _query_id, length in query_items)
                subject_path = root / "tiny.fa"
                subject_path.write_text(
                    f">dummy\n{('ACGT' * ((max_length + 3) // 4))[:max_length]}\n"
                )
                tiny_db = root / "tiny"
                subprocess.run(
                    [
                        makeblastdb,
                        "-in",
                        str(subject_path),
                        "-dbtype",
                        "nucl",
                        "-parse_seqids",
                        "-out",
                        str(tiny_db),
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=60,
                )
                alias = root / "virtual.nal"
                alias.write_text(
                    f"TITLE full-db-stats-{group_index}\n"
                    "DBLIST tiny\n"
                    f"NSEQ {db_num}\n"
                    f"LENGTH {db_len}\n"
                )
                output = root / "stats.xml"
                subprocess.run(
                    [
                        blastn,
                        "-query",
                        str(query_path),
                        "-db",
                        str(root / "virtual"),
                        "-outfmt",
                        "5",
                        "-max_target_seqs",
                        "1",
                        "-dust",
                        "no",
                        *relevant_options,
                        "-out",
                        str(output),
                    ],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=60,
                )
                root_xml = ET.parse(output).getroot()
                iterations = root_xml.findall("./BlastOutput_iterations/Iteration")
                if len(iterations) != len(query_items):
                    raise ValueError("BLAST statistics probe returned an unexpected query count")
                for (query_id, _length), iteration in zip(
                    query_items, iterations, strict=True
                ):
                    stats = iteration.find("./Iteration_stat/Statistics")
                    hsp_len = int_at(stats, "Statistics_hsp-len") if stats is not None else None
                    if hsp_len is None:
                        raise ValueError("BLAST statistics probe omitted Statistics_hsp-len")
                    calibrated[query_id] = hsp_len
        except (OSError, subprocess.SubprocessError, ET.ParseError, ValueError) as exc:
            warnings.append(
                "Full-DB Statistics_hsp-len recalibration failed for "
                f"db_len={db_len} db_num={db_num}: {type(exc).__name__}"
            )
    return calibrated


def hit_rank(hit):
    best_key = (float("inf"), float("inf"))
    best_evalue = float("inf")
    best_bitscore = float("-inf")
    best_raw_score = float("-inf")
    hsp_count = 0
    for hsp in hit.findall("./Hit_hsps/Hsp"):
        hsp_count += 1
        try:
            evalue = float(text_at(hsp, "Hsp_evalue", "inf"))
            bitscore = float(text_at(hsp, "Hsp_bit-score", "-inf"))
            raw_score = float(text_at(hsp, "Hsp_score", "-inf"))
        except ValueError:
            continue
        key = (blast_evalue_sort_key(evalue), -raw_score)
        if key < best_key:
            best_key = key
            best_evalue = evalue
            best_bitscore = bitscore
            best_raw_score = raw_score
    return (
        best_key[0],
        best_key[1],
        hsp_count,
        best_evalue,
        best_bitscore,
        best_raw_score,
    )


def merge_xml(
    input_tsv,
    output_gz,
    report_json,
    num_shards,
    max_hits,
    candidate_pool_size,
    warnings,
    blast_program,
    blast_options,
):
    oracle_path = os.environ.get("ELB_TIE_ORDER_FILE", "").strip() or None
    db_order_requested = bool(oracle_path) and tie_order_oracle_source() == "db_order"
    input_root = Path(input_tsv).parent
    output_path = Path(output_gz).resolve()
    xml_files = []
    for shard_idx in range(int(num_shards)):
        shard_dir = input_root / f"shard_{shard_idx:02d}"
        xml_files.extend(
            path for path in sorted(shard_dir.glob("*.out.gz"))
            if path.resolve() != output_path
        )
    if not xml_files:
        raise ValueError("No shard XML result files found")

    base_root = None
    iterations_node = None
    queries = {}
    query_order = []
    total_input_hits = 0
    total_input_hsps = 0
    malformed_xml_count = 0
    unsupported_records = 0
    ordinal = 0

    for xml_file in xml_files:
        try:
            with gzip.open(xml_file, "rb") as handle:
                root = ET.parse(handle).getroot()
        except (OSError, ET.ParseError) as exc:
            raise ValueError(f"Malformed XML result {xml_file.name}: {exc}") from exc
        if root.tag != "BlastOutput":
            raise ValueError(f"Unexpected XML root {root.tag}: {xml_file.name}")
        if base_root is None:
            base_root = copy.deepcopy(root)
            iterations_node = base_root.find("BlastOutput_iterations")
            if iterations_node is None:
                iterations_node = ET.SubElement(base_root, "BlastOutput_iterations")
            iterations_node.clear()
            db_node = base_root.find("BlastOutput_db")
            if db_node is not None:
                db_node.text = normalize_sharded_db_name(db_node.text)
            warnings.append(
                "BlastOutput top-level metadata is normalized from the first valid shard; "
                "per-query Statistics db length/count are merged across shards"
            )

        for iteration in root.findall("./BlastOutput_iterations/Iteration"):
            query_id = text_at(iteration, "Iteration_query-ID") or text_at(iteration, "Iteration_query-def")
            query_id = query_id.strip()
            if not query_id:
                unsupported_records += 1
                continue
            if query_id not in queries:
                template = copy.deepcopy(iteration)
                hits_node = template.find("Iteration_hits")
                if hits_node is None:
                    hits_node = ET.SubElement(template, "Iteration_hits")
                hits_node.clear()
                queries[query_id] = {
                    "template": template,
                    "hits": [],
                    "db_len": 0,
                    "db_num": 0,
                    "eff_spaces": Counter(),
                    "missing_stats": 0,
                }
                query_order.append(query_id)
            statistics = iteration.find("./Iteration_stat/Statistics")
            db_len = int_at(statistics, "Statistics_db-len") if statistics is not None else None
            db_num = int_at(statistics, "Statistics_db-num") if statistics is not None else None
            eff_space = int_at(statistics, "Statistics_eff-space") if statistics is not None else None
            if db_len is None or db_num is None:
                queries[query_id]["missing_stats"] += 1
            else:
                queries[query_id]["db_len"] += db_len
                queries[query_id]["db_num"] += db_num
            if eff_space is not None:
                queries[query_id]["eff_spaces"][eff_space] += 1
            for hit in iteration.findall("./Iteration_hits/Hit"):
                (
                    evalue_key,
                    negative_score,
                    hsp_count,
                    display_evalue,
                    display_bitscore,
                    raw_score,
                ) = hit_rank(hit)
                if hsp_count == 0:
                    unsupported_records += 1
                    continue
                total_input_hits += 1
                total_input_hsps += hsp_count
                queries[query_id]["hits"].append(
                    (
                        evalue_key,
                        negative_score,
                        ordinal,
                        copy.deepcopy(hit),
                        display_evalue,
                        display_bitscore,
                        hsp_count,
                        raw_score,
                    )
                )
                ordinal += 1

    if base_root is None or iterations_node is None:
        raise ValueError("No valid BLAST XML results found")
    if unsupported_records:
        warnings.append("Some XML records were skipped because query or HSP metadata was incomplete")

    candidate_accessions = (
        xml_subject_accession(hit[3])
        for item in queries.values()
        for hit in item["hits"]
    )
    (
        oracle_path,
        tie_order,
        oracle_unique_accessions,
        oracle_accessions,
    ) = load_tie_order_oracle(warnings, candidate_accessions)
    strict_oracle = bool(tie_order) and strict_oracle_enabled()
    db_order_exact = db_order_requested
    if db_order_exact and total_input_hits and not tie_order:
        raise ValueError("DB-order oracle does not cover any candidate subjects")
    if strict_oracle:
        warnings.append("Strict tie-order oracle is enabled; non-oracle hits are excluded")

    web_blast_statistics = load_web_blast_statistics()
    if web_blast_statistics is not None:
        if not db_order_exact:
            raise ValueError("Web BLAST statistics require a same-generation DB-order oracle")
        validate_web_blast_statistics(
            web_blast_statistics,
            query_order,
            queries,
            blast_options,
        )
        warnings.append(
            "Web BLAST taxonomy-filtered statistics were validated against runtime options"
        )

    calibrated_hsp_lengths = (
        recalibrate_full_db_hsp_lengths(
            queries, blast_program, blast_options, warnings
        )
        if db_order_exact and web_blast_statistics is None
        else {}
    )

    tie_break_count = 0
    tie_cutoff_overflow_count = 0
    tie_cutoff_queries = []
    oracle_missing_queries = []
    diversity_limit = (
        0 if strict_oracle or db_order_exact else diversity_aware_cutoff_limit()
    )
    diversity_mode = diversity_reservation_mode(
        diversity_limit, strict_oracle, db_order_exact
    )
    diversity_reserved_count = 0
    diversity_candidate_count = 0
    diversity_queries = []
    total_output_hits = 0
    total_output_hsps = 0
    for query_id in query_order:
        item = queries[query_id]
        hits = item["hits"]
        if db_order_exact:
            unmapped = unmapped_oracle_accessions(
                tie_order, (xml_subject_accession(hit[3]) for hit in hits)
            )
            if unmapped:
                raise ValueError(
                    "DB-order oracle does not cover all candidate subjects; "
                    f"query={query_id!r} missing={len(unmapped)} "
                    f"first={unmapped[:10]}"
                )
        if strict_oracle:
            observed_keys = observed_accession_keys(xml_subject_accession(hit[3]) for hit in hits)
            missing_accessions = oracle_missing_accessions(oracle_accessions, observed_keys)
            if missing_accessions:
                oracle_missing_queries.append(
                    {
                        "query_id": query_id,
                        "missing_count": len(missing_accessions),
                        "first_missing_accessions": missing_accessions[:20],
                    }
                )
            hits = [
                hit
                for hit in hits
                if oracle_sort_key(tie_order, xml_subject_accession(hit[3]), hit[2])[0] == 0
            ]
        pair_counts = Counter((hit[0], hit[1]) for hit in hits)
        tie_break_count += sum(count - 1 for count in pair_counts.values() if count > 1)
        sorted_hits = sorted(
            hits,
            key=lambda hit: (
                hit[0],
                hit[1],
                tie_break_sort_component(tie_order, xml_subject_accession(hit[3]), hit[2]),
                hit[2],
            ),
        )
        selected = sorted_hits[:max_hits]
        if selected and len(sorted_hits) > len(selected):
            cutoff_signature = (selected[-1][0], selected[-1][1])
            cutoff_input_count = sum(
                1 for hit in sorted_hits if (hit[0], hit[1]) == cutoff_signature
            )
            cutoff_selected_count = sum(
                1 for hit in selected if (hit[0], hit[1]) == cutoff_signature
            )
            cutoff_overflow = max(0, cutoff_input_count - cutoff_selected_count)
            if cutoff_overflow:
                tie_cutoff_overflow_count += cutoff_overflow
                if len(tie_cutoff_queries) < 10:
                    tie_cutoff_queries.append(
                        {
                            "query_id": query_id,
                            "evalue": selected[-1][4],
                            "bitscore": selected[-1][5],
                            "score": selected[-1][7],
                            "hsp_count": selected[-1][6],
                            "tie_input_count": cutoff_input_count,
                            "tie_selected_count": cutoff_selected_count,
                            "tie_overflow_count": cutoff_overflow,
                        }
                    )
        if diversity_limit != 0 and selected and len(sorted_hits) > len(selected):
            selected, reserved, candidates = apply_diversity_reservation(
                selected,
                sorted_hits,
                diversity_limit,
                lambda hit: xml_subject_accession(hit[3]),
            )
            if reserved:
                diversity_reserved_count += reserved
                diversity_candidate_count += candidates
                if len(diversity_queries) < 10:
                    diversity_queries.append(
                        {
                            "query_id": query_id,
                            "candidate_count": candidates,
                            "reservation_mode": diversity_mode,
                            "reserved_count": reserved,
                        }
                    )
        template = item["template"]
        hits_node = template.find("Iteration_hits")
        if hits_node is None:
            hits_node = ET.SubElement(template, "Iteration_hits")
        hits_node.clear()
        for index, selected_hit in enumerate(selected, start=1):
            hit = selected_hit[3]
            hit_num = hit.find("Hit_num")
            if hit_num is not None:
                hit_num.text = str(index)
            hits_node.append(hit)
            total_output_hits += 1
            total_output_hsps += len(hit.findall("./Hit_hsps/Hsp"))

        statistics = template.find("./Iteration_stat/Statistics")
        if statistics is None:
            iteration_stat = template.find("Iteration_stat")
            if iteration_stat is None:
                iteration_stat = ET.SubElement(template, "Iteration_stat")
            statistics = ET.SubElement(iteration_stat, "Statistics")
        if web_blast_statistics is not None:
            set_child_text(
                statistics,
                "Statistics_db-len",
                web_blast_statistics["result_database_letters"],
            )
            set_child_text(
                statistics,
                "Statistics_db-num",
                web_blast_statistics["filtered_database_sequences"],
            )
            set_child_text(
                statistics,
                "Statistics_hsp-len",
                web_blast_statistics["length_adjustment"],
            )
            set_child_text(
                statistics,
                "Statistics_eff-space",
                web_blast_statistics["effective_search_space"],
            )
        elif item["db_len"] and item["db_num"]:
            set_child_text(statistics, "Statistics_db-len", item["db_len"])
            set_child_text(statistics, "Statistics_db-num", item["db_num"])
            if len(item["eff_spaces"]) == 1:
                eff_space = next(iter(item["eff_spaces"]))
                set_child_text(statistics, "Statistics_eff-space", eff_space)
                if query_id in calibrated_hsp_lengths:
                    set_child_text(
                        statistics,
                        "Statistics_hsp-len",
                        calibrated_hsp_lengths[query_id],
                    )
                else:
                    try:
                        query_len = int(text_at(template, "Iteration_query-len", "0"))
                    except ValueError:
                        query_len = 0
                    hsp_len = derive_hsp_len(
                        query_len, item["db_len"], item["db_num"], eff_space
                    )
                    if hsp_len is not None:
                        set_child_text(statistics, "Statistics_hsp-len", hsp_len)
                    else:
                        warnings.append(
                            f"Could not derive merged HSP length for query {query_id}; "
                            "kept the first shard value"
                        )
            elif item["eff_spaces"]:
                warnings.append(
                    f"Shard effective search spaces differ for query {query_id}; "
                    "kept the first shard value"
                )
        if item["missing_stats"]:
            warnings.append(f"Some shard statistics were missing for query {query_id}")
        iterations_node.append(template)

    if tie_break_count:
        if db_order_exact:
            warnings.append(
                "Ties were resolved with the BLAST full-DB raw-score and OID comparator"
            )
        else:
            warnings.append(
                "Ties were resolved deterministically but may not match full-DB BLAST internal order"
            )
    if tie_cutoff_overflow_count and not db_order_exact:
        warnings.append(
            "The max_target_seqs cutoff splits a tied score class; strict Web BLAST "
            "ordering may require original BLAST DB subject order"
        )
    if diversity_reserved_count:
        warnings.append(
            "Diversity-aware cutoff reserved lower-scoring near-miss subjects; "
            "the displayed set preserves shard candidate-pool composition and is "
            "not the strict top max_target_seqs by score"
        )

    with gzip.open(output_gz, "wb") as handle:
        ET.ElementTree(base_root).write(handle, encoding="utf-8", xml_declaration=True)

    report = {
        "outfmt": 5,
        "format": "blast_xml",
        "max_target_seqs": max_hits,
        "candidate_pool_size": candidate_pool_size,
        "queries": len(query_order),
        "total_input_hits": total_input_hits,
        "total_input_subjects": total_input_hits,
        "total_output_hits": total_output_hits,
        "total_output_subjects": total_output_hits,
        "total_input_hsps": total_input_hsps,
        "total_output_hsps": total_output_hsps,
        "unsupported_records": unsupported_records,
        "malformed_xml_count": malformed_xml_count,
        "tie_break_count": tie_break_count,
        "tie_cutoff_overflow_count": tie_cutoff_overflow_count,
        "tie_cutoff_queries": tie_cutoff_queries,
        "diversity_reserved_count": diversity_reserved_count,
        "diversity_candidate_count": diversity_candidate_count,
        "diversity_reservation_mode": diversity_mode,
        "diversity_queries": diversity_queries,
        "num_shards": int(num_shards),
        "ranking_basis": (
            "blast_evalue_raw_score_db_oid_desc"
            if db_order_exact
            else "best_hsp_evalue_raw_score_oracle_ordinal"
            if tie_order
            else "best_hsp_evalue_raw_score_accession_ordinal"
            if deterministic_tie_order_enabled()
            else "best_hsp_evalue_raw_score_ordinal"
        ),
        "selection_equivalence": selection_equivalence_label(
            tie_order, strict_oracle, True
        ),
        "statistics_equivalence": (
            "web_blast_exact"
            if web_blast_statistics is not None
            else "full_db_exact"
            if db_order_exact and len(calibrated_hsp_lengths) == len(query_order)
            else "partial"
        ),
        "web_blast_statistical_context": web_blast_statistics,
        "tie_order_oracle_path": oracle_path,
        "tie_order_oracle_source": tie_order_oracle_source() if tie_order else None,
        "tie_order_oracle_accessions": oracle_unique_accessions,
        "tie_order_oracle_strict": strict_oracle,
        "tie_order_oracle_missing_count": sum(
            item["missing_count"] for item in oracle_missing_queries
        ),
        "tie_order_oracle_missing_queries": oracle_missing_queries,
        "result_selection_policy_requested": result_selection_policy(),
        "result_selection_policy_applied": result_selection_policy(),
        "warnings": warnings,
    }
    Path(report_json).write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    return total_output_hits, len(query_order)


input_tsv, output_gz, report_json, num_shards, blast_program, blast_options = sys.argv[1:]
try:
    num_shards_value = int(num_shards)
except ValueError as exc:
    raise ValueError("num_shards must be an integer") from exc
if not 1 <= num_shards_value <= 1024:
    raise ValueError("num_shards must be between 1 and 1024")
num_shards = str(num_shards_value)
candidate_pool_size, warnings = parse_max_target_seqs(blast_options)
max_hits = resolve_result_max_target_seqs(candidate_pool_size)
outfmt = parse_outfmt(blast_options)
outfmt_spec = parse_outfmt_spec(blast_options)
selection_policy = result_selection_policy()
if selection_policy == "sequence_diversity" and outfmt not in ("6", "7"):
    raise ValueError("sequence_diversity supports only tabular BLAST outfmt 6 or 7")
if (
    selection_policy == "sequence_diversity"
    and candidate_pool_size > SEQUENCE_DIVERSITY_MAX_CANDIDATE_POOL_SIZE
):
    raise ValueError("sequence_diversity candidate pool cannot exceed 5000 per shard")
if outfmt == "5":
    total_hits, query_count = merge_xml(
        input_tsv,
        output_gz,
        report_json,
        num_shards,
        max_hits,
        candidate_pool_size,
        warnings,
        blast_program,
        blast_options,
    )
elif outfmt in ("6", "7"):
    # outfmt 6/7 share the same tabular data rows (7 only adds comment lines,
    # which the merge skips and re-emits). The merge resolves its group/rank/
    # oracle columns by NAME from the full specifier, so reordered + extended
    # layouts (e.g. `7 sseqid staxids ... evalue bitscore ...`) merge correctly.
    total_hits, query_count = merge_tabular(
        input_tsv,
        output_gz,
        report_json,
        num_shards,
        blast_program,
        max_hits,
        candidate_pool_size,
        warnings,
        outfmt=outfmt, outfmt_spec=outfmt_spec,
    )
else:
    raise ValueError(f"Unsupported sharded merge outfmt: {outfmt}")
print(
    f"Merged {total_hits} hits from {query_count} queries "
    f"with outfmt={outfmt} max_target_seqs={max_hits}",
    file=sys.stderr,
)
PY
