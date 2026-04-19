# Tests for azure_optimizer.py — Optimization profiles and predictions

"""
Unit tests for Azure optimization profiles, prediction, and profile application.

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

import os
from unittest.mock import patch, MagicMock
import pytest
from elastic_blast.azure_optimizer import (
    OptimizationProfile, get_profile, predict, predict_all_profiles, apply_profile
)


class TestGetProfile:
    """Tests for get_profile()."""

    def test_default_is_balanced(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('ELB_OPTIMIZATION', None)
            assert get_profile() == OptimizationProfile.BALANCED

    def test_cost_profile(self):
        with patch.dict(os.environ, {'ELB_OPTIMIZATION': 'cost'}):
            assert get_profile() == OptimizationProfile.COST

    def test_performance_profile(self):
        with patch.dict(os.environ, {'ELB_OPTIMIZATION': 'performance'}):
            assert get_profile() == OptimizationProfile.PERFORMANCE

    def test_invalid_falls_back_to_balanced(self):
        with patch.dict(os.environ, {'ELB_OPTIMIZATION': 'turbo'}):
            assert get_profile() == OptimizationProfile.BALANCED


class TestPredict:
    """Tests for predict()."""

    def test_cost_is_cheapest(self):
        cost = predict(OptimizationProfile.COST, query_size_gb=1, db_size_gb=50)
        perf = predict(OptimizationProfile.PERFORMANCE, query_size_gb=1, db_size_gb=50)
        assert cost.estimated_cost < perf.estimated_cost

    def test_performance_is_fastest(self):
        # Large workload where VM power matters more than node count
        cost = predict(OptimizationProfile.COST, query_size_gb=100, db_size_gb=500)
        perf = predict(OptimizationProfile.PERFORMANCE, query_size_gb=100, db_size_gb=500)
        assert perf.estimated_hours < cost.estimated_hours

    def test_spot_used_for_cost_and_balanced(self):
        cost = predict(OptimizationProfile.COST, query_size_gb=1, db_size_gb=50)
        balanced = predict(OptimizationProfile.BALANCED, query_size_gb=1, db_size_gb=50)
        perf = predict(OptimizationProfile.PERFORMANCE, query_size_gb=1, db_size_gb=50)
        assert cost.use_spot is True
        assert balanced.use_spot is True
        assert perf.use_spot is False

    def test_large_db_selects_better_vm(self):
        p = predict(OptimizationProfile.COST, query_size_gb=1, db_size_gb=500)
        assert p.vm_type == 'Standard_E16s_v3'  # large_db_vm for cost

    def test_prediction_str_includes_key_info(self):
        p = predict(OptimizationProfile.BALANCED, query_size_gb=1, db_size_gb=50)
        s = str(p)
        assert 'BALANCED' in s
        assert '$' in s
        assert 'Estimated time' in s
        assert 'Pods:' in s

    def test_zero_query_doesnt_crash(self):
        p = predict(OptimizationProfile.BALANCED, query_size_gb=0, db_size_gb=10)
        assert p.estimated_hours > 0


class TestPredictAllProfiles:
    """Tests for predict_all_profiles()."""

    def test_output_contains_all_profiles(self):
        output = predict_all_profiles(query_size_gb=1, db_size_gb=50)
        assert 'COST' in output
        assert 'BALANCED' in output
        assert 'PERFORMANCE' in output
        assert '±30%' in output

    def test_output_shows_estimates(self):
        output = predict_all_profiles(query_size_gb=1, db_size_gb=50)
        assert 'Time:' in output
        assert 'Cost:' in output
        assert '$' in output


class TestApplyProfile:
    """Tests for apply_profile()."""

    def test_cost_enables_spot(self):
        """Cost profile enables reuse but does NOT auto-enable spot (system pool limitation)."""
        cfg = MagicMock()
        cfg.cluster.use_preemptible = False
        cfg.cluster.num_nodes = 1
        cfg.cluster.machine_type = 'Standard_E32s_v3'
        cfg.cluster.reuse = False
        cfg.blast.batch_len = 100000

        with patch.dict(os.environ, {'ELB_QUERY_SIZE_GB': '1', 'ELB_DB_SIZE_GB': '50'}):
            pred = apply_profile(cfg, OptimizationProfile.COST)

        assert cfg.cluster.reuse is True
        # Spot not auto-enabled for system pool — requires user nodepool
        assert pred.use_spot is True  # prediction still shows Spot is ideal

    def test_performance_does_not_force_spot(self):
        cfg = MagicMock()
        cfg.cluster.use_preemptible = False
        cfg.cluster.num_nodes = 10
        cfg.cluster.machine_type = 'Standard_E64bs_v5'
        cfg.cluster.reuse = False
        cfg.blast.batch_len = 100000

        with patch.dict(os.environ, {'ELB_QUERY_SIZE_GB': '1', 'ELB_DB_SIZE_GB': '50'}):
            pred = apply_profile(cfg, OptimizationProfile.PERFORMANCE)

        assert cfg.cluster.use_preemptible is False
        assert pred.use_spot is False
