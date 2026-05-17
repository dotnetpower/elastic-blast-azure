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
import re
import shlex
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path


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


def merge_tabular(input_tsv, output_gz, report_json, num_shards, blast_program, max_hits, warnings):
    query_hits = defaultdict(list)
    unsupported_rows = 0
    total_input_rows = 0
    ordinal = 0

    input_path = Path(input_tsv)
    if input_path.exists():
        with input_path.open() as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                if not line or line.startswith("#"):
                    continue
                total_input_rows += 1
                cols = line.split("\t")
                if len(cols) < 12:
                    unsupported_rows += 1
                    continue
                try:
                    evalue = float(cols[10])
                    bitscore = float(cols[11])
                except ValueError:
                    unsupported_rows += 1
                    continue
                query_hits[cols[0]].append((evalue, -bitscore, ordinal, line))
                ordinal += 1

    if unsupported_rows:
        warnings.append("Some rows were skipped because they were not outfmt 6 compatible")

    fields = (
        "query acc.ver, subject acc.ver, % identity, alignment length, mismatches, "
        "gap opens, q. start, q. end, s. start, s. end, evalue, bit score"
    )
    blast_label = blast_program.upper() if blast_program else "BLAST"
    tie_break_count = 0
    total_output_hits = 0

    with gzip.open(output_gz, "wt") as out:
        for query_id in sorted(query_hits):
            hits = query_hits[query_id]
            pair_counts = Counter((hit[0], hit[1]) for hit in hits)
            tie_break_count += sum(count - 1 for count in pair_counts.values() if count > 1)
            selected = sorted(hits, key=lambda hit: (hit[0], hit[1], hit[2]))[:max_hits]
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

    report = {
        "outfmt": 6,
        "format": "blast_tabular",
        "max_target_seqs": max_hits,
        "queries": len(query_hits),
        "total_input_hits": total_input_rows,
        "total_output_hits": total_output_hits,
        "unsupported_rows": unsupported_rows,
        "tie_break_count": tie_break_count,
        "num_shards": int(num_shards),
        "ranking_basis": "evalue_bitscore_ordinal",
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
    total_output_hits = 0
    total_output_hsps = 0
    for query_id in query_order:
        item = queries[query_id]
        hits = item["hits"]
        pair_counts = Counter((hit[0], hit[1], hit[2]) for hit in hits)
        tie_break_count += sum(count - 1 for count in pair_counts.values() if count > 1)
        selected = sorted(hits, key=lambda hit: (hit[0], hit[1], hit[2], hit[3]))[:max_hits]
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
        "num_shards": int(num_shards),
        "ranking_basis": "best_hsp_evalue_bitscore_ordinal",
        "warnings": warnings,
    }
    Path(report_json).write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    return total_output_hits, len(query_order)


input_tsv, output_gz, report_json, num_shards, blast_program, blast_options = sys.argv[1:]
max_hits, warnings = parse_max_target_seqs(blast_options)
outfmt = parse_outfmt(blast_options)
if outfmt == "5":
    total_hits, query_count = merge_xml(input_tsv, output_gz, report_json, num_shards, max_hits, warnings)
elif outfmt == "6":
    total_hits, query_count = merge_tabular(input_tsv, output_gz, report_json, num_shards, blast_program, max_hits, warnings)
else:
    raise ValueError(f"Unsupported sharded merge outfmt: {outfmt}")
print(
    f"Merged {total_hits} hits from {query_count} queries "
    f"with outfmt={outfmt} max_target_seqs={max_hits}",
    file=sys.stderr,
)
PY
