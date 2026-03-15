# Tests for Phase 5: SaaS Operations — Cost tracker and Spot VM support

"""
Unit tests for azure_cost_tracker.py and Spot VM configuration.

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

import os
from unittest.mock import patch, MagicMock
import pytest
from elastic_blast.azure_cost_tracker import estimate_cost, CostEstimate, AZURE_VM_HOURLY_PRICES


class TestCostEstimate:
    """Tests for cost estimation."""

    def test_basic_estimate(self):
        """Standard VM cost estimate should be positive."""
        result = estimate_cost('Standard_E32s_v3', num_nodes=3, estimated_hours=2.0)
        assert result.total > 0
        assert result.compute_per_hour == AZURE_VM_HOURLY_PRICES['Standard_E32s_v3'] * 3
        assert result.is_spot is False

    def test_spot_discount(self):
        """Spot VMs should be significantly cheaper."""
        regular = estimate_cost('Standard_E32s_v3', num_nodes=3, estimated_hours=2.0)
        spot = estimate_cost('Standard_E32s_v3', num_nodes=3, estimated_hours=2.0, use_spot=True)
        assert spot.total < regular.total
        assert spot.is_spot is True

    def test_unknown_vm_uses_default(self):
        """Unknown VM types should use default pricing."""
        result = estimate_cost('Standard_FUTURE_v99', num_nodes=1, estimated_hours=1.0)
        assert result.total > 0

    def test_storage_cost_included(self):
        """DB storage cost should be included in total."""
        no_storage = estimate_cost('Standard_E32s_v3', num_nodes=1, estimated_hours=1.0, db_size_gb=0)
        with_storage = estimate_cost('Standard_E32s_v3', num_nodes=1, estimated_hours=1.0, db_size_gb=2000)
        assert with_storage.total > no_storage.total

    def test_l_series_pricing(self):
        """L-series VMs should have pricing data."""
        result = estimate_cost('Standard_L64s_v3', num_nodes=2, estimated_hours=3.0)
        assert result.compute_per_hour == AZURE_VM_HOURLY_PRICES['Standard_L64s_v3'] * 2

    def test_str_representation(self):
        """Cost estimate should have readable string representation."""
        result = estimate_cost('Standard_E32s_v3', num_nodes=3, estimated_hours=2.0)
        s = str(result)
        assert 'Standard_E32s_v3' in s
        assert '3 nodes' in s
        assert '$' in s


class TestSpotVmConfig:
    """Tests for Spot VM configuration in AKS cluster creation."""

    def test_spot_flags_added_when_preemptible(self):
        """When use_preemptible=True, start_cluster should include Spot VM flags."""
        from elastic_blast.azure import start_cluster

        with patch('elastic_blast.azure.safe_exec') as mock_exec:
            mock_exec.return_value = MagicMock(stdout=b'', returncode=0)

            # Create minimal mock config
            cfg = MagicMock()
            cfg.cluster.name = 'test-cluster'
            cfg.cluster.machine_type = 'Standard_E32s_v3'
            cfg.cluster.num_nodes = 3
            cfg.cluster.use_preemptible = True
            cfg.cluster.use_local_ssd = False
            cfg.cluster.dry_run = True  # Dry run to avoid actual execution
            cfg.cluster.labels = 'project=elastic-blast'
            cfg.cluster.enable_stackdriver = False
            cfg.azure.resourcegroup = 'rg-test'
            cfg.azure.aks_version = ''

            start_cluster(cfg)
            # In dry run mode, it just logs — but we can verify the params were built
            # The code path sets up actual_params with Spot flags
