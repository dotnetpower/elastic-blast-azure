"""Regression tests for the optional ``[cluster]`` param version-skew guard.

Covers ``main._elb_recognises_cluster_param`` — the guard that prevents the
OpenAPI app from writing an optional ``exp-skip-warmed-ssd-init`` hint that the
bundled elastic-blast (pinned by the Dockerfile ``ELB_REF``) does not recognise.
Writing an unrecognised param hard-fails the whole ``elastic-blast submit`` with
``Unrecognized configuration parameter``, so during a rolling rebuild the guard
must omit the hint instead of breaking submit.
"""

from __future__ import annotations

import sys
import types

import main


class _FakeParamInfo:
    def __init__(self, section: str, param_name: str) -> None:
        self.section = section
        self.param_name = param_name


def _install_fake_elastic_blast(monkeypatch, *, cluster_param: str | None) -> None:
    """Inject a minimal fake ``elastic_blast`` package into ``sys.modules``.

    ``cluster_param`` is registered in the cluster section mapping when given,
    mirroring elastic-blast's real ``ClusterConfig.mapping`` shape so the guard
    sees a recognised param.
    """
    pkg = types.ModuleType("elastic_blast")
    constants = types.ModuleType("elastic_blast.constants")
    constants.CFG_CLUSTER = "cluster"
    elb_config = types.ModuleType("elastic_blast.elb_config")

    class _ClusterConfig:
        mapping = {}

    if cluster_param is not None:
        _ClusterConfig.mapping = {
            "skip_warmed_ssd_init": _FakeParamInfo("cluster", cluster_param),
        }

    class _Empty:
        mapping: dict[str, object] = {}

    elb_config.AZUREConfig = _Empty
    elb_config.BlastConfig = _Empty
    elb_config.ClusterConfig = _ClusterConfig
    elb_config.TimeoutsConfig = _Empty
    pkg.constants = constants
    pkg.elb_config = elb_config

    monkeypatch.setitem(sys.modules, "elastic_blast", pkg)
    monkeypatch.setitem(sys.modules, "elastic_blast.constants", constants)
    monkeypatch.setitem(sys.modules, "elastic_blast.elb_config", elb_config)


def test_guard_recognises_mapped_cluster_param(monkeypatch) -> None:
    _install_fake_elastic_blast(monkeypatch, cluster_param="exp-skip-warmed-ssd-init")
    main._elb_recognises_cluster_param.cache_clear()
    try:
        assert main._elb_recognises_cluster_param("exp-skip-warmed-ssd-init") is True
        assert main._elb_recognises_cluster_param("totally-bogus-param") is False
    finally:
        main._elb_recognises_cluster_param.cache_clear()


def test_guard_fails_closed_when_param_unmapped(monkeypatch) -> None:
    # Bundled elastic-blast predates the param (no cluster mapping entry).
    _install_fake_elastic_blast(monkeypatch, cluster_param=None)
    main._elb_recognises_cluster_param.cache_clear()
    try:
        assert main._elb_recognises_cluster_param("exp-skip-warmed-ssd-init") is False
    finally:
        main._elb_recognises_cluster_param.cache_clear()


def test_guard_fails_closed_when_elastic_blast_missing(monkeypatch) -> None:
    # Setting the package to None forces ImportError on `import elastic_blast.*`,
    # mirroring a partially-installed runtime. The guard must fail CLOSED.
    monkeypatch.setitem(sys.modules, "elastic_blast", None)
    monkeypatch.setitem(sys.modules, "elastic_blast.constants", None)
    monkeypatch.setitem(sys.modules, "elastic_blast.elb_config", None)
    main._elb_recognises_cluster_param.cache_clear()
    try:
        assert main._elb_recognises_cluster_param("exp-skip-warmed-ssd-init") is False
    finally:
        main._elb_recognises_cluster_param.cache_clear()
