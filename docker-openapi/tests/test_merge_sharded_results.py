"""Boundary tests for the checked-in shard-result merge helper."""

from __future__ import annotations

import gzip
import json
import os
import resource
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "merge-sharded-results.sh"


def _run_sequence_diversity_merge(
    tmp_path: Path,
    rows: list[str],
    *,
    max_target_seqs: int = 2,
    candidate_pool_size: int | None = None,
    num_shards: int = 2,
    extra_env: dict[str, str] | None = None,
) -> tuple[list[str], dict[str, object]]:
    input_tsv = tmp_path / "hits.tsv"
    output_gz = tmp_path / "merged.out.gz"
    report_json = tmp_path / "merge-report.json"
    input_tsv.write_text("\n".join(rows) + ("\n" if rows else ""))
    subprocess.run(  # noqa: S603 -- executes the checked-in merge helper.
        [
            "/bin/bash",
            str(SCRIPT),
            str(input_tsv),
            str(output_gz),
            str(report_json),
            str(num_shards),
            "blastn",
            (
                "-outfmt 6 qseqid saccver sseq qstart qend evalue bitscore score "
                f"-max_target_seqs {candidate_pool_size or max_target_seqs}"
            ),
        ],
        check=True,
        env={
            **os.environ,
            "ELB_RESULT_SELECTION_POLICY": "sequence_diversity",
            "ELB_REQUESTED_MAX_TARGET_SEQS": str(max_target_seqs),
            **(
                {"ELB_CANDIDATE_POOL_SIZE_REQUESTED": str(candidate_pool_size)}
                if candidate_pool_size is not None
                else {}
            ),
            **(extra_env or {}),
        },
    )
    with gzip.open(output_gz, "rt") as handle:
        output_rows = [
            line.rstrip("\n")
            for line in handle
            if line.strip() and not line.startswith("#")
        ]
    return output_rows, json.loads(report_json.read_text())


def _run_legacy_tabular_merge(
    tmp_path: Path,
    rows: list[str],
    *,
    policy: str | None = None,
    diversity_cutoff: str | None = None,
    max_target_seqs: int = 2,
) -> tuple[list[str], dict[str, object]]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    input_tsv = tmp_path / "hits.tsv"
    output_gz = tmp_path / "merged.out.gz"
    report_json = tmp_path / "merge-report.json"
    input_tsv.write_text("\n".join(rows) + "\n")
    env = os.environ.copy()
    env.pop("ELB_RESULT_SELECTION_POLICY", None)
    env.pop("ELB_DIVERSITY_AWARE_CUTOFF", None)
    if policy is not None:
        env["ELB_RESULT_SELECTION_POLICY"] = policy
    if diversity_cutoff is not None:
        env["ELB_DIVERSITY_AWARE_CUTOFF"] = diversity_cutoff
    subprocess.run(  # noqa: S603 -- executes the checked-in merge helper.
        [
            "/bin/bash",
            str(SCRIPT),
            str(input_tsv),
            str(output_gz),
            str(report_json),
            "2",
            "blastn",
            (
                "-outfmt 6 qseqid saccver sseq qstart qend evalue bitscore score "
                f"-max_target_seqs {max_target_seqs}"
            ),
        ],
        check=True,
        env=env,
    )
    with gzip.open(output_gz, "rt") as handle:
        output_rows = [
            line.rstrip("\n")
            for line in handle
            if line.strip() and not line.startswith("#")
        ]
    return output_rows, json.loads(report_json.read_text())


def _row(
    accession: str,
    sequence: str,
    *,
    query: str = "q1",
    qstart: str = "1",
    qend: str = "4",
    evalue: str = "1e-20",
    bitscore: str = "80",
    score: str = "90",
) -> str:
    return "\t".join(
        (query, accession, sequence, qstart, qend, evalue, bitscore, score)
    )


def test_unspecified_policy_preserves_native_top_n_output(tmp_path: Path) -> None:
    first = _row("acc-a", "AAAA", evalue="1e-30", score="100")
    second_hsp = _row("acc-a", "CCCC", evalue="1e-10", score="70")
    excluded = _row("acc-b", "GGGG", evalue="1e-20", score="90")

    output_rows, report = _run_legacy_tabular_merge(
        tmp_path,
        [first, second_hsp, excluded],
        max_target_seqs=1,
    )

    assert output_rows == [first, second_hsp]
    assert report["result_selection_policy_requested"] == "native_top_n"
    assert report["result_selection_policy_applied"] == "native_top_n"


def test_explicit_native_top_n_matches_unspecified_output(tmp_path: Path) -> None:
    rows = [
        _row("acc-a", "AAAA", evalue="1e-30", score="100"),
        _row("acc-a", "CCCC", evalue="1e-10", score="70"),
        _row("acc-b", "GGGG", evalue="1e-20", score="90"),
    ]

    implicit_rows, _ = _run_legacy_tabular_merge(
        tmp_path / "implicit", rows, max_target_seqs=1
    )
    explicit_rows, report = _run_legacy_tabular_merge(
        tmp_path / "explicit",
        rows,
        policy="native_top_n",
        max_target_seqs=1,
    )

    assert explicit_rows == implicit_rows
    with gzip.open(tmp_path / "implicit" / "merged.out.gz", "rb") as implicit:
        implicit_bytes = implicit.read()
    with gzip.open(tmp_path / "explicit" / "merged.out.gz", "rb") as explicit:
        explicit_bytes = explicit.read()
    assert explicit_bytes == implicit_bytes
    assert report["result_selection_policy_applied"] == "native_top_n"


def test_diversity_aware_existing_accession_selection_is_unchanged(
    tmp_path: Path,
) -> None:
    rows = [
        _row("acc-a", "AAAA", evalue="1e-30", score="100"),
        _row("acc-b", "CCCC", evalue="1e-30", score="100"),
        _row("acc-c", "GGGG", evalue="1e-30", score="100"),
        _row("acc-near", "TTTT", evalue="1e-20", score="90"),
    ]

    output_rows, report = _run_legacy_tabular_merge(
        tmp_path,
        rows,
        policy="diversity_aware",
        diversity_cutoff="1",
        max_target_seqs=2,
    )

    assert [row.split("\t")[1] for row in output_rows] == ["acc-a", "acc-near"]
    assert report["diversity_reserved_count"] == 1
    assert report["result_selection_policy_applied"] == "diversity_aware"


def test_sequence_diversity_groups_normalized_sequence_across_accessions(
    tmp_path: Path,
) -> None:
    rows = [
        "q1\tacc-a\ta-cg\t1\t4\t1e-20\t80\t90",
        "q1\tacc-b\tACG\t1\t4\t1e-30\t70\t85",
        "q1\tacc-c\tA-C-G\t1\t4\t1e-30\t70\t84",
    ]

    output_rows, report = _run_sequence_diversity_merge(tmp_path, rows)

    assert output_rows == [rows[1]]
    assert report["returned_sequence_groups"] == 1
    assert report["sequence_group_counts"] == [
        {
            "sequence_group_accession_count": 3,
            "sequence_group_ordinal": 1,
            "sequence_group_source_row_count": 3,
        }
    ]


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("ACGN", "ACGR"),
        ("ACG", "CGT"),
    ],
)
def test_sequence_diversity_compares_ambiguity_and_reverse_complement_literally(
    tmp_path: Path,
    left: str,
    right: str,
) -> None:
    output_rows, report = _run_sequence_diversity_merge(
        tmp_path,
        [_row("acc-a", left), _row("acc-b", right)],
    )

    assert len(output_rows) == 2
    assert report["observed_sequence_groups"] == 2


def test_sequence_diversity_allows_one_accession_in_multiple_groups(
    tmp_path: Path,
) -> None:
    output_rows, report = _run_sequence_diversity_merge(
        tmp_path,
        [_row("acc-a", "AAAA"), _row("acc-a", "CCCC")],
    )

    assert len(output_rows) == 2
    assert [row.split("\t")[1] for row in output_rows] == ["acc-a", "acc-a"]
    assert report["observed_candidate_subjects"] == 1
    assert report["observed_sequence_groups"] == 2


def test_sequence_diversity_keeps_different_query_spans_separate(
    tmp_path: Path,
) -> None:
    output_rows, report = _run_sequence_diversity_merge(
        tmp_path,
        [
            _row("acc-a", "AAAA", qstart="1", qend="4"),
            _row("acc-b", "AAAA", qstart="2", qend="5"),
        ],
    )

    assert len(output_rows) == 2
    assert report["observed_sequence_groups"] == 2


def test_sequence_diversity_groups_the_same_signature_across_shards(
    tmp_path: Path,
) -> None:
    output_rows, report = _run_sequence_diversity_merge(
        tmp_path,
        [
            "# ELB source-shard:00",
            _row("acc-a", "A-A-A-A", evalue="1e-20"),
            "# ELB source-shard:01",
            _row("acc-b", "aaaa", evalue="1e-30"),
        ],
    )

    assert [row.split("\t")[1] for row in output_rows] == ["acc-b"]
    assert report["observed_sequence_groups"] == 1
    assert report["sequence_group_counts"][0]["sequence_group_accession_count"] == 2


def test_sequence_diversity_representative_uses_existing_comparator(
    tmp_path: Path,
) -> None:
    rows = [
        _row("eval-worse", "AAAA", evalue="1e-20", score="200"),
        _row("eval-best", "A-A-A-A", evalue="1e-30", score="100"),
        _row("score-low", "CCCC", evalue="1e-10", score="80"),
        _row("score-high", "C-C-C-C", evalue="1e-10", score="90"),
        _row("first", "GGGG", evalue="1e-10", score="70"),
        _row("second", "G-G-G-G", evalue="1e-10", score="70"),
    ]

    output_rows, _report = _run_sequence_diversity_merge(
        tmp_path, rows, max_target_seqs=3
    )

    assert [row.split("\t")[1] for row in output_rows] == [
        "eval-best",
        "score-high",
        "first",
    ]


def test_sequence_diversity_reports_group_shortfall(tmp_path: Path) -> None:
    output_rows, report = _run_sequence_diversity_merge(
        tmp_path,
        [_row("acc-a", "AAAA"), _row("acc-b", "CCCC")],
        max_target_seqs=3,
    )

    assert len(output_rows) == 2
    assert report["requested_sequence_groups"] == 3
    assert report["returned_sequence_groups"] == 2
    assert report["shortfall_reasons"] == [
        "insufficient_unique_groups_in_observed_pool"
    ]


def test_sequence_diversity_reports_zero_results(tmp_path: Path) -> None:
    output_rows, report = _run_sequence_diversity_merge(
        tmp_path,
        [],
        max_target_seqs=3,
    )

    assert output_rows == []
    assert report["observed_candidate_rows"] == 0
    assert report["observed_candidate_subjects"] == 0
    assert report["observed_sequence_groups"] == 0
    assert report["requested_sequence_groups"] == 3
    assert report["returned_sequence_groups"] == 0
    assert report["shortfall_reasons"] == ["no_candidates_observed"]


def test_sequence_diversity_reports_candidate_pool_saturation(
    tmp_path: Path,
) -> None:
    rows = [
        "# ELB source-shard:00",
        _row("acc-a", "AAAA"),
        _row("acc-b", "A-A-A-A"),
        "# ELB source-shard:01",
        _row("acc-c", "AAAA"),
        _row("acc-d", "A-A-A-A"),
    ]

    output_rows, report = _run_sequence_diversity_merge(
        tmp_path,
        rows,
        max_target_seqs=2,
        candidate_pool_size=2,
    )

    assert len(output_rows) == 1
    assert report["candidate_pool_size_requested_per_shard"] == 2
    assert report["candidate_pool_size_applied_per_shard"] == 2
    assert report["expected_shards"] == 2
    assert report["succeeded_shards"] == 2
    assert report["candidate_pool_saturated_shards"] == 2
    assert report["observed_pool_complete"] is False
    assert report["shortfall_reasons"] == [
        "candidate_pool_saturated",
        "insufficient_unique_groups_in_observed_pool",
    ]


def test_sequence_diversity_report_counts_match_observed_rows(tmp_path: Path) -> None:
    rows = [
        _row("acc-a", "AAAA", evalue="1e-30"),
        _row("acc-b", "A-A-A-A", evalue="1e-20"),
        _row("acc-a", "CCCC", evalue="1e-10"),
    ]

    output_rows, report = _run_sequence_diversity_merge(tmp_path, rows)

    assert len(output_rows) == 2
    assert report["observed_candidate_rows"] == 3
    assert report["observed_candidate_subjects"] == 2
    assert report["observed_sequence_groups"] == 2
    assert report["sequence_group_counts"] == [
        {
            "sequence_group_accession_count": 2,
            "sequence_group_ordinal": 1,
            "sequence_group_source_row_count": 2,
        },
        {
            "sequence_group_accession_count": 1,
            "sequence_group_ordinal": 2,
            "sequence_group_source_row_count": 1,
        },
    ]
    assert report["sequence_group_counts_truncated"] is False
    assert report["ranking_basis"] == "blast_evalue_raw_score_existing_order_ordinal"
    assert report["selection_equivalence"] == "observed_candidate_pool"
    assert report["diversity_reservation_mode"] == "not_applicable"


def test_sequence_diversity_rejects_malformed_hsp_row(tmp_path: Path) -> None:
    input_tsv = tmp_path / "hits.tsv"
    input_tsv.write_text(
        "# ELB source-shard:00\n"
        + _row("acc-a", "AAAA")
        + "\nq1\tacc-b\tCCCC\t1\t4\tnot-an-evalue\t80\t90\n"
    )
    proc = subprocess.run(  # noqa: S603 -- executes the checked-in merge helper.
        [
            "/bin/bash",
            str(SCRIPT),
            str(input_tsv),
            str(tmp_path / "merged.out.gz"),
            str(tmp_path / "merge-report.json"),
            "1",
            "blastn",
            "-outfmt 6 qseqid saccver sseq qstart qend evalue bitscore score "
            "-max_target_seqs 2",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "ELB_RESULT_SELECTION_POLICY": "sequence_diversity",
            "ELB_REQUESTED_MAX_TARGET_SEQS": "2",
            "ELB_SUCCEEDED_SHARDS": "1",
        },
    )

    assert proc.returncode != 0
    assert "cannot group malformed or incomplete HSP rows" in proc.stderr
    assert not (tmp_path / "merged.out.gz").exists()


def test_sequence_diversity_rejects_missing_expected_shard(tmp_path: Path) -> None:
    input_tsv = tmp_path / "hits.tsv"
    input_tsv.write_text(_row("acc-a", "AAAA") + "\n")
    proc = subprocess.run(  # noqa: S603 -- executes the checked-in merge helper.
        [
            "/bin/bash",
            str(SCRIPT),
            str(input_tsv),
            str(tmp_path / "merged.out.gz"),
            str(tmp_path / "merge-report.json"),
            "2",
            "blastn",
            "-outfmt 6 qseqid saccver sseq qstart qend evalue bitscore score "
            "-max_target_seqs 2",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "ELB_RESULT_SELECTION_POLICY": "sequence_diversity",
            "ELB_REQUESTED_MAX_TARGET_SEQS": "2",
            "ELB_SUCCEEDED_SHARDS": "1",
        },
    )

    assert proc.returncode != 0
    assert "every expected shard" in proc.stderr
    assert not (tmp_path / "merged.out.gz").exists()


def test_sequence_diversity_merge_accepts_pool_above_legacy_limit(
    tmp_path: Path,
) -> None:
    input_tsv = tmp_path / "hits.tsv"
    input_tsv.write_text(_row("acc-a", "AAAA") + "\n")
    proc = subprocess.run(  # noqa: S603 -- executes the checked-in merge helper.
        [
            "/bin/bash",
            str(SCRIPT),
            str(input_tsv),
            str(tmp_path / "merged.out.gz"),
            str(tmp_path / "merge-report.json"),
            "1",
            "blastn",
            "-outfmt 6 qseqid saccver sseq qstart qend evalue bitscore score "
            "-max_target_seqs 5001",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "ELB_RESULT_SELECTION_POLICY": "sequence_diversity",
            "ELB_REQUESTED_MAX_TARGET_SEQS": "2",
        },
    )

    assert proc.returncode == 0, proc.stderr
    report = json.loads((tmp_path / "merge-report.json").read_text())
    assert report["candidate_pool_size"] == 5_001
    assert report["returned_sequence_groups"] == 1


def test_sequence_diversity_large_pool_uses_disk_backed_bounded_memory(
    tmp_path: Path,
) -> None:
    input_tsv = tmp_path / "hits.tsv"
    output_gz = tmp_path / "merged.out.gz"
    report_json = tmp_path / "merge-report.json"
    sequence = "A" * 16_384
    with input_tsv.open("w") as handle:
        handle.write("# ELB source-shard:00\n")
        for index in range(6_000):
            handle.write(_row(f"acc-{index:05d}", f"{sequence}{index:06d}") + "\n")

    memory_limit = 96 * 1024 * 1024

    def limit_address_space() -> None:
        resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))

    proc = subprocess.run(  # noqa: S603 -- executes the checked-in merge helper.
        [
            "/bin/bash",
            str(SCRIPT),
            str(input_tsv),
            str(output_gz),
            str(report_json),
            "1",
            "blastn",
            "-outfmt 6 qseqid saccver sseq qstart qend evalue bitscore score "
            "-max_target_seqs 6000",
        ],
        check=False,
        capture_output=True,
        text=True,
        preexec_fn=limit_address_space,
        env={
            **os.environ,
            "ELB_RESULT_SELECTION_POLICY": "sequence_diversity",
            "ELB_REQUESTED_MAX_TARGET_SEQS": "6000",
            "ELB_SUCCEEDED_SHARDS": "1",
        },
    )

    assert proc.returncode == 0, proc.stderr
    report = json.loads(report_json.read_text())
    assert report["observed_candidate_rows"] == 6_000
    assert report["observed_sequence_groups"] == 6_000
    assert report["returned_sequence_groups"] == 6_000
    assert len(report["sequence_group_counts"]) == 5_000
    assert report["sequence_group_counts_truncated"] is True


@pytest.mark.parametrize("num_shards", ["not-an-integer", "0", "1025"])
def test_merge_rejects_invalid_shard_count(tmp_path: Path, num_shards: str) -> None:
    proc = subprocess.run(  # noqa: S603 -- executes the checked-in merge helper.
        [
            "/bin/bash",
            str(SCRIPT),
            str(tmp_path / "hits.tsv"),
            str(tmp_path / "merged.out.gz"),
            str(tmp_path / "merge-report.json"),
            num_shards,
            "blastn",
            "-outfmt 6 std score -max_target_seqs 10",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert proc.returncode != 0
    assert "num_shards" in proc.stderr
