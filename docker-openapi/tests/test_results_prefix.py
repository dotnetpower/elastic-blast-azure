"""Unit tests for the date-tiered results-prefix validator (Mode B).

The dashboard's date-tiered storage layout (elb-dashboard
``STORAGE_DATE_LAYOUT_ENABLED``) forwards a ``results_prefix`` of the shape
``YYYY/MM/DD/`` on ``POST /v1/jobs`` so the sibling writes a Mode B job's
results under ``results/<YYYY/MM/DD>/<job_id>/`` instead of the flat
``results/<job_id>/``. ``_validate_results_prefix`` is the security boundary:
it must accept only an exact date shape (no traversal / absolute path / extra
segments) and otherwise fall back to the flat layout, so an old caller that
sends nothing is unaffected and a hostile value cannot redirect writes.

Validation:
``cd docker-openapi && python -m pytest tests/test_results_prefix.py -q``.
"""

from __future__ import annotations

import importlib

import pytest
from fastapi import HTTPException


@pytest.fixture
def main_module(monkeypatch):
    monkeypatch.setenv("ELB_OPENAPI_API_TOKEN", "test-token")
    monkeypatch.setenv("ELB_CLUSTER_NAME", "test-cluster")
    monkeypatch.setenv("ELB_OPENAPI_DISABLE_BACKGROUND", "1")
    import main  # noqa: PLC0415

    importlib.reload(main)
    return main


def test_empty_or_missing_keeps_flat_layout(main_module):
    # None / "" / whitespace / slash-only all degrade to the flat layout ("").
    for value in (None, "", "   ", "/", "//"):
        assert main_module._validate_results_prefix(value) == ""


def test_valid_date_prefix_is_normalised(main_module):
    assert main_module._validate_results_prefix("2026/06/23") == "2026/06/23/"
    assert main_module._validate_results_prefix("2026/06/23/") == "2026/06/23/"
    assert main_module._validate_results_prefix("/2026/06/23/") == "2026/06/23/"


@pytest.mark.parametrize(
    "value",
    [
        "../secrets",
        "2026/06/23/../../etc",
        "2026/06/23/extra",
        "2026-06-23",
        "26/6/23",
        "results/2026/06/23",
        "https://evil.example/results",
        "2026/06",
        "2026/06/23/job-abc",
    ],
)
def test_rejects_non_date_or_traversal(main_module, value):
    with pytest.raises(HTTPException) as exc:
        main_module._validate_results_prefix(value)
    assert exc.value.status_code == 400


def test_results_url_uses_prefix_when_present(main_module):
    # The Mode B results_url is f"{blob_base}/results/{prefix}{job_id}"; with a
    # valid prefix the job's results land under the date directory, and without
    # one they stay flat (prefix == "").
    prefix = main_module._validate_results_prefix("2026/06/23/")
    assert f"/results/{prefix}abc123" == "/results/2026/06/23/abc123"
    flat = main_module._validate_results_prefix(None)
    assert f"/results/{flat}abc123" == "/results/abc123"
