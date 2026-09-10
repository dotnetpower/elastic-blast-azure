"""Boundary tests for the checked-in shard-result merge helper."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "merge-sharded-results.sh"


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
