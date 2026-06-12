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
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path


def _accession_base(accession):
    if not accession:
        return accession
    if "." not in accession:
        return accession
    head, tail = accession.rsplit(".", 1)
    return head if tail.isdigit() else accession


def load_tie_order_oracle(warnings):
    oracle_path = os.environ.get("ELB_TIE_ORDER_FILE", "").strip()
    if not oracle_path:
        return None, {}, 0, []
    path = Path(oracle_path)
    if not path.exists():
        warnings.append(f"Tie-order oracle file was not found: {oracle_path}")
        return oracle_path, {}, 0, []

    order = {}
    accessions = []
    unique_accessions = 0
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        tokens = re.split(r"[\t, ]+", line)
        if len(tokens) >= 12:
            accession = tokens[1]
        elif len(tokens) >= 2 and tokens[0].isdigit():
            accession = tokens[1]
        else:
            accession = tokens[0]
        if not accession:
            continue
        if accession not in order:
            order[accession] = unique_accessions
            accessions.append(accession)
            unique_accessions += 1
        base = _accession_base(accession)
        if base and base not in order:
            order[base] = order[accession]
    if unique_accessions:
        warnings.append(
            "Tie-order oracle is enabled; ties are ordered by the supplied same-snapshot accession list"
        )
    else:
        warnings.append(f"Tie-order oracle file contained no usable accessions: {oracle_path}")
    return oracle_path, order, unique_accessions, accessions


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
    # Opt-in (default OFF / 0). When set to a positive integer k, and the
    # selected max_target_seqs window is entirely filled by a SINGLE tied
    # (evalue, bitscore) score class while lower-scoring hits exist below the
    # cutoff, the last min(k, available) slots are replaced by the best
    # lower-scoring (more informative, e.g. 1-mismatch) hits. This preserves
    # near-miss subjects that a strict max_target_seqs cutoff would otherwise
    # drop -- the case where 100 perfect matches push out a single SNP-bearing
    # variant. Default 0 preserves standard BLAST max_target_seqs semantics.
    raw = os.environ.get("ELB_DIVERSITY_AWARE_CUTOFF", "").strip()
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError:
        return 0
    return value if value > 0 else 0


def tie_break_sort_component(tie_order, accession, ordinal):
    # Tertiary tie-break component used after (evalue, -bitscore). The oracle,
    # when active, always wins. Otherwise, in deterministic mode the subject
    # accession provides a stable, rerun-independent order; the input ordinal
    # is kept as the final disambiguator.
    if tie_order:
        return oracle_sort_key(tie_order, accession, ordinal)
    if deterministic_tie_order_enabled():
        return (0, accession)
    return (0, ordinal)


def ranking_basis_label(tie_order):
    if tie_order:
        return "evalue_bitscore_oracle_ordinal"
    if deterministic_tie_order_enabled():
        return "evalue_bitscore_accession_ordinal"
    return "evalue_bitscore_ordinal"


def apply_diversity_reservation(selected, sorted_hits, limit):
    # Replace the tail of a saturated selection window with the best
    # lower-scoring near-miss hits. Only acts when the ENTIRE selected window
    # is a single tied (evalue, bitscore) score class -- i.e. informative
    # lower-scoring subjects were pushed out purely by the max_target_seqs
    # cutoff. Hit tuple layout: (evalue, -bitscore, ordinal, line).
    top_class = (selected[0][0], selected[0][1])
    if any((hit[0], hit[1]) != top_class for hit in selected):
        return selected, 0
    near_misses = [
        hit for hit in sorted_hits[len(selected):] if (hit[0], hit[1]) != top_class
    ]
    if not near_misses:
        return selected, 0
    reserve = min(limit, len(near_misses), len(selected))
    if reserve <= 0:
        return selected, 0
    kept = selected[: len(selected) - reserve]
    return kept + near_misses[:reserve], reserve


def oracle_sort_key(order, accession, fallback):
    if not order:
        return (0, fallback)
    rank = order.get(accession, order.get(_accession_base(accession)))
    if rank is None:
        return (1, fallback)
    return (0, rank)


def tabular_subject_accession(line, subject_idx=1):
    cols = line.split("\t")
    return cols[subject_idx] if len(cols) > subject_idx else ""


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

    Returns ``(qseqid_idx, evalue_idx, bitscore_idx, subject_idx)`` where the
    query and subject indices may be ``None``. Raises ``ValueError`` when evalue
    or bitscore is absent (the merge cannot re-rank shard hits without them). A
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
            "deterministic accession tie-break are disabled"
        )
    return qseqid_idx, evalue_idx, bitscore_idx, subject_idx


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


def merge_tabular(input_tsv, output_gz, report_json, num_shards, blast_program, max_hits, warnings, outfmt="6", outfmt_spec=""):
    oracle_path, tie_order, oracle_unique_accessions, oracle_accessions = load_tie_order_oracle(warnings)
    strict_oracle = bool(tie_order) and strict_oracle_enabled()
    if strict_oracle:
        warnings.append("Strict tie-order oracle is enabled; non-oracle hits are excluded")
    # Resolve the group / rank / oracle columns BY NAME from the outfmt
    # specifier (handles reordered + extended layouts like
    # `7 sseqid staxids ... evalue bitscore ...`). For a plain or `std`-prefixed
    # layout these resolve back to the historical positions (qseqid=0,
    # sseqid=1, evalue=10, bitscore=11), so existing runs are byte-identical.
    qseqid_idx, evalue_idx, bitscore_idx, subject_idx = resolve_tabular_columns(
        outfmt_spec, warnings
    )
    # A subject accession column is required for the tie-order oracle and the
    # deterministic accession tie-break; without it, neither can run.
    if subject_idx is None:
        strict_oracle = False
        tie_order = {}
    oracle_subject_idx = subject_idx if subject_idx is not None else 1
    # Lowest column count a data row must have for every resolved index to be
    # addressable (mirrors the historical `< 12` guard for the std layout).
    min_required_cols = max(
        idx for idx in (qseqid_idx, evalue_idx, bitscore_idx, subject_idx) if idx is not None
    ) + 1
    query_hits = defaultdict(list)
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
    if input_path.exists():
        with input_path.open() as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                if line.startswith("#"):
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
                except ValueError:
                    unsupported_rows += 1
                    continue
                group_key = cols[qseqid_idx] if qseqid_idx is not None else ""
                query_hits[group_key].append((evalue, -bitscore, ordinal, line))
                ordinal += 1

    if unsupported_rows:
        warnings.append("Some rows were skipped because they were not outfmt 6 compatible")

    fields = captured_fields or (
        "query acc.ver, subject acc.ver, % identity, alignment length, mismatches, "
        "gap opens, q. start, q. end, s. start, s. end, evalue, bit score"
    )
    blast_label = blast_program.upper() if blast_program else "BLAST"
    tie_break_count = 0
    tie_cutoff_overflow_count = 0
    tie_cutoff_queries = []
    oracle_missing_queries = []
    diversity_limit = diversity_aware_cutoff_limit()
    diversity_reserved_count = 0
    diversity_queries = []
    total_output_hits = 0

    with gzip.open(output_gz, "wt") as out:
        for query_id in sorted(query_hits):
            hits = query_hits[query_id]
            if strict_oracle:
                observed_keys = observed_accession_keys(
                    tabular_subject_accession(hit[3], oracle_subject_idx) for hit in hits
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
                    if oracle_sort_key(tie_order, tabular_subject_accession(hit[3], oracle_subject_idx), hit[2])[0] == 0
                ]
            pair_counts = Counter((hit[0], hit[1]) for hit in hits)
            tie_break_count += sum(count - 1 for count in pair_counts.values() if count > 1)
            sorted_hits = sorted(
                hits,
                key=lambda hit: (
                    hit[0],
                    hit[1],
                    tie_break_sort_component(
                        tie_order, tabular_subject_accession(hit[3], oracle_subject_idx), hit[2]
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
                        tie_cutoff_queries.append(
                            {
                                "query_id": query_id,
                                "evalue": cutoff_signature[0],
                                "bitscore": -cutoff_signature[1],
                                "tie_input_count": cutoff_input_count,
                                "tie_selected_count": cutoff_selected_count,
                                "tie_overflow_count": cutoff_overflow,
                            }
                        )
            # Diversity-aware reservation (opt-in) runs AFTER cutoff detection so
            # the truncation report still reflects the real top-class overflow.
            if diversity_limit and selected and len(sorted_hits) > len(selected):
                selected, reserved = apply_diversity_reservation(
                    selected, sorted_hits, diversity_limit
                )
                if reserved:
                    diversity_reserved_count += reserved
                    if len(diversity_queries) < 10:
                        diversity_queries.append(
                            {"query_id": query_id, "reserved_count": reserved}
                        )
            out.write(f"# {blast_label}\n")
            out.write(f"# Query: {query_id}\n")
            out.write(f"# Database: merged from {num_shards} shards\n")
            out.write(f"# Fields: {fields}\n")
            out.write(f"# {len(selected)} hits found\n")
            for hit in selected:
                out.write(hit[3] + "\n")
            total_output_hits += len(selected)

    if tie_break_count:
        warnings.append(
            "Ties were resolved deterministically but may not match full-DB BLAST internal order"
        )
    if tie_cutoff_overflow_count:
        warnings.append(
            "The max_target_seqs cutoff splits a tied score class; strict Web BLAST "
            "ordering may require original BLAST DB subject order"
        )
    if diversity_reserved_count:
        warnings.append(
            "Diversity-aware cutoff reserved slots for lower-scoring near-miss hits; "
            "the displayed set is not the strict top max_target_seqs by score"
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
        },
        "max_target_seqs": max_hits,
        "queries": len(query_hits),
        "total_input_hits": total_input_rows,
        "total_output_hits": total_output_hits,
        "unsupported_rows": unsupported_rows,
        "tie_break_count": tie_break_count,
        "tie_cutoff_overflow_count": tie_cutoff_overflow_count,
        "tie_cutoff_queries": tie_cutoff_queries,
        "diversity_reserved_count": diversity_reserved_count,
        "diversity_queries": diversity_queries,
        "num_shards": int(num_shards),
        "ranking_basis": ranking_basis_label(tie_order),
        "tie_order_oracle_path": oracle_path,
        "tie_order_oracle_accessions": oracle_unique_accessions,
        "tie_order_oracle_strict": strict_oracle,
        "tie_order_oracle_missing_count": sum(
            item["missing_count"] for item in oracle_missing_queries
        ),
        "tie_order_oracle_missing_queries": oracle_missing_queries,
        "warnings": warnings,
    }
    Path(report_json).write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    return total_output_hits, len(query_hits)


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


def hit_rank(hit):
    best_evalue = float("inf")
    best_bitscore = float("-inf")
    hsp_count = 0
    for hsp in hit.findall("./Hit_hsps/Hsp"):
        hsp_count += 1
        try:
            best_evalue = min(best_evalue, float(text_at(hsp, "Hsp_evalue", "inf")))
            best_bitscore = max(best_bitscore, float(text_at(hsp, "Hsp_bit-score", "-inf")))
        except ValueError:
            continue
    return best_evalue, -best_bitscore, hsp_count


def merge_xml(input_tsv, output_gz, report_json, num_shards, max_hits, warnings):
    oracle_path, tie_order, oracle_unique_accessions, oracle_accessions = load_tie_order_oracle(warnings)
    strict_oracle = bool(tie_order) and strict_oracle_enabled()
    if strict_oracle:
        warnings.append("Strict tie-order oracle is enabled; non-oracle hits are excluded")
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
                evalue, negative_bitscore, hsp_count = hit_rank(hit)
                if hsp_count == 0:
                    unsupported_records += 1
                    continue
                total_input_hits += 1
                total_input_hsps += hsp_count
                queries[query_id]["hits"].append((evalue, negative_bitscore, -hsp_count, ordinal, copy.deepcopy(hit)))
                ordinal += 1

    if base_root is None or iterations_node is None:
        raise ValueError("No valid BLAST XML results found")
    if unsupported_records:
        warnings.append("Some XML records were skipped because query or HSP metadata was incomplete")

    tie_break_count = 0
    tie_cutoff_overflow_count = 0
    tie_cutoff_queries = []
    oracle_missing_queries = []
    total_output_hits = 0
    total_output_hsps = 0
    for query_id in query_order:
        item = queries[query_id]
        hits = item["hits"]
        if strict_oracle:
            observed_keys = observed_accession_keys(xml_subject_accession(hit[4]) for hit in hits)
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
                if oracle_sort_key(tie_order, xml_subject_accession(hit[4]), hit[3])[0] == 0
            ]
        pair_counts = Counter((hit[0], hit[1], hit[2]) for hit in hits)
        tie_break_count += sum(count - 1 for count in pair_counts.values() if count > 1)
        sorted_hits = sorted(
            hits,
            key=lambda hit: (
                hit[0],
                hit[1],
                hit[2],
                tie_break_sort_component(tie_order, xml_subject_accession(hit[4]), hit[3]),
                hit[3],
            ),
        )
        selected = sorted_hits[:max_hits]
        if selected and len(sorted_hits) > len(selected):
            cutoff_signature = (selected[-1][0], selected[-1][1], selected[-1][2])
            cutoff_input_count = sum(
                1 for hit in sorted_hits if (hit[0], hit[1], hit[2]) == cutoff_signature
            )
            cutoff_selected_count = sum(
                1 for hit in selected if (hit[0], hit[1], hit[2]) == cutoff_signature
            )
            cutoff_overflow = max(0, cutoff_input_count - cutoff_selected_count)
            if cutoff_overflow:
                tie_cutoff_overflow_count += cutoff_overflow
                if len(tie_cutoff_queries) < 10:
                    tie_cutoff_queries.append(
                        {
                            "query_id": query_id,
                            "evalue": cutoff_signature[0],
                            "bitscore": -cutoff_signature[1],
                            "hsp_count": -cutoff_signature[2],
                            "tie_input_count": cutoff_input_count,
                            "tie_selected_count": cutoff_selected_count,
                            "tie_overflow_count": cutoff_overflow,
                        }
                    )
        template = item["template"]
        hits_node = template.find("Iteration_hits")
        if hits_node is None:
            hits_node = ET.SubElement(template, "Iteration_hits")
        hits_node.clear()
        for index, selected_hit in enumerate(selected, start=1):
            hit = selected_hit[4]
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
        if item["db_len"] and item["db_num"]:
            set_child_text(statistics, "Statistics_db-len", item["db_len"])
            set_child_text(statistics, "Statistics_db-num", item["db_num"])
            if len(item["eff_spaces"]) == 1:
                eff_space = next(iter(item["eff_spaces"]))
                set_child_text(statistics, "Statistics_eff-space", eff_space)
                try:
                    query_len = int(text_at(template, "Iteration_query-len", "0"))
                except ValueError:
                    query_len = 0
                hsp_len = derive_hsp_len(query_len, item["db_len"], item["db_num"], eff_space)
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
        warnings.append(
            "Ties were resolved deterministically but may not match full-DB BLAST internal order"
        )
    if tie_cutoff_overflow_count:
        warnings.append(
            "The max_target_seqs cutoff splits a tied score class; strict Web BLAST "
            "ordering may require original BLAST DB subject order"
        )

    with gzip.open(output_gz, "wb") as handle:
        ET.ElementTree(base_root).write(handle, encoding="utf-8", xml_declaration=True)

    report = {
        "outfmt": 5,
        "format": "blast_xml",
        "max_target_seqs": max_hits,
        "queries": len(query_order),
        "total_input_hits": total_input_hits,
        "total_output_hits": total_output_hits,
        "total_input_hsps": total_input_hsps,
        "total_output_hsps": total_output_hsps,
        "unsupported_records": unsupported_records,
        "malformed_xml_count": malformed_xml_count,
        "tie_break_count": tie_break_count,
        "tie_cutoff_overflow_count": tie_cutoff_overflow_count,
        "tie_cutoff_queries": tie_cutoff_queries,
        "num_shards": int(num_shards),
        "ranking_basis": (
            "best_hsp_evalue_bitscore_oracle_ordinal"
            if tie_order
            else (
                "best_hsp_evalue_bitscore_accession_ordinal"
                if deterministic_tie_order_enabled()
                else "best_hsp_evalue_bitscore_ordinal"
            )
        ),
        "tie_order_oracle_path": oracle_path,
        "tie_order_oracle_accessions": oracle_unique_accessions,
        "tie_order_oracle_strict": strict_oracle,
        "tie_order_oracle_missing_count": sum(
            item["missing_count"] for item in oracle_missing_queries
        ),
        "tie_order_oracle_missing_queries": oracle_missing_queries,
        "warnings": warnings,
    }
    Path(report_json).write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    return total_output_hits, len(query_order)


input_tsv, output_gz, report_json, num_shards, blast_program, blast_options = sys.argv[1:]
max_hits, warnings = parse_max_target_seqs(blast_options)
outfmt = parse_outfmt(blast_options)
outfmt_spec = parse_outfmt_spec(blast_options)
if outfmt == "5":
    total_hits, query_count = merge_xml(input_tsv, output_gz, report_json, num_shards, max_hits, warnings)
elif outfmt in ("6", "7"):
    # outfmt 6/7 share the same tabular data rows (7 only adds comment lines,
    # which the merge skips and re-emits). The merge resolves its group/rank/
    # oracle columns by NAME from the full specifier, so reordered + extended
    # layouts (e.g. `7 sseqid staxids ... evalue bitscore ...`) merge correctly.
    total_hits, query_count = merge_tabular(
        input_tsv, output_gz, report_json, num_shards, blast_program, max_hits, warnings,
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
