"""Regression guard: the BLAST submit handlers must run in FastAPI's threadpool.

``POST /v1/jobs`` (``submit_job``) and ``POST /api/v1/elastic-blast/submit``
(``external_submit``) do blocking I/O — FASTA upload via ``azcopy``, ConfigMap
persistence in ``_save_job``, and DB-version metadata fetches — with no awaits.
They must therefore be declared as plain ``def`` so FastAPI dispatches them to
its anyio threadpool instead of running them on the single asyncio event loop.

Declaring them ``async def`` (the prior state) blocked the event loop for the
whole duration of every submit, which serialised the submit path (~9s/job) and
starved the ``/healthz`` readiness probe under a concurrent 50-submit burst,
flapping the pod to NotReady (dashboard issue #54). This suite fails if either
handler regresses to a coroutine function.

Validation:
``cd docker-openapi && python -m pytest tests/test_submit_threadpool.py -q``.
"""

from __future__ import annotations

import inspect
import os

import pytest

os.environ.setdefault("ELB_OPENAPI_API_TOKEN", "test-token")
os.environ.setdefault("ELB_OPENAPI_DISABLE_BACKGROUND", "1")

try:
    import main  # noqa: PLC0415
except Exception as exc:  # pragma: no cover - import guard
    pytest.skip(f"main import failed: {exc}", allow_module_level=True)


def test_submit_job_is_not_a_coroutine_function() -> None:
    """``submit_job`` must be a plain ``def`` (threadpool), not ``async def``."""
    assert not inspect.iscoroutinefunction(main.submit_job), (
        "submit_job must stay a plain def so FastAPI runs it in the threadpool; "
        "as async def its blocking I/O serialises submits and starves readiness "
        "under burst (issue #54)."
    )


def test_external_submit_is_not_a_coroutine_function() -> None:
    """``external_submit`` delegates to ``submit_job`` and must also be sync."""
    assert not inspect.iscoroutinefunction(main.external_submit), (
        "external_submit must stay a plain def for the same reason as "
        "submit_job (its only delegate); see issue #54."
    )


def test_azcopy_concurrency_is_bounded() -> None:
    """The submit-path azcopy fan-out must stay bounded under a burst.

    Running the submit handlers in the threadpool (the issue #54 fix) lets up to
    ~40 submits run in parallel, each spawning ~2 azcopy subprocesses. The
    ``_azcopy_slots`` semaphore caps concurrent azcopy so a burst cannot OOM the
    pod under its memory limit. This guards against removing the bound.
    """
    import threading

    assert isinstance(main._azcopy_slots, threading.BoundedSemaphore)
    assert main.AZCOPY_CONCURRENCY >= 1
    # Default must stay well under FastAPI's threadpool (40) so the bound is real.
    assert main.AZCOPY_CONCURRENCY <= 32
