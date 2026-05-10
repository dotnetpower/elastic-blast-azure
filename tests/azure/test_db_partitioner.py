"""Tests for azure_db_partitioner module."""

import pytest
from unittest.mock import MagicMock, patch

from elastic_blast.azure_traits import (
    compute_partition_plan,
    apply_auto_partition,
    _derive_partition_prefix,
    PartitionPlan,
    MIN_DB_SIZE_FOR_PARTITION_GB,
    DB_RAM_FRACTION,
    MIN_PARTITIONS,
    MAX_PARTITIONS,
)
from elastic_blast.constants import SYSTEM_MEMORY_RESERVE


class TestComputePartitionPlan:
    """Tests for compute_partition_plan()."""

    def test_small_db_no_partition(self):
        """DB smaller than MIN_DB_SIZE_FOR_PARTITION_GB should not be partitioned."""
        plan = compute_partition_plan(
            db_size_gb=5.0, node_ram_gb=256, num_nodes=3,
            db_url='https://stgelb.blob.core.windows.net/blast-db/small_db/small_db')
        assert plan.db_partitions == 0
        assert plan.db_partition_prefix == ''
        assert 'too small' in plan.reason

    def test_db_fits_in_single_node(self):
        """DB that fits in a single node's RAM should not be partitioned."""
        plan = compute_partition_plan(
            db_size_gb=50.0, node_ram_gb=256, num_nodes=3,
            db_url='https://stgelb.blob.core.windows.net/blast-db/db/db')
        usable = (256 - SYSTEM_MEMORY_RESERVE) * DB_RAM_FRACTION
        assert 50.0 <= usable  # sanity check
        assert plan.db_partitions == 0
        assert 'fits in single node' in plan.reason

    def test_large_db_needs_partition(self):
        """DB larger than node RAM should be partitioned."""
        plan = compute_partition_plan(
            db_size_gb=269.0, node_ram_gb=128, num_nodes=5,
            db_url='https://stgelb.blob.core.windows.net/blast-db/core_nt/core_nt')
        usable = (128 - SYSTEM_MEMORY_RESERVE) * DB_RAM_FRACTION
        expected_min = int(269.0 / usable) + 1  # ceil
        assert plan.db_partitions >= expected_min
        assert plan.db_partitions >= MIN_PARTITIONS
        assert plan.db_partition_prefix != ''
        assert 'exceeds node RAM' in plan.reason

    def test_local_ssd_clamps_to_num_nodes(self):
        """In local-SSD mode, partitions >= num_nodes."""
        plan = compute_partition_plan(
            db_size_gb=269.0, node_ram_gb=128, num_nodes=10,
            db_url='https://stgelb.blob.core.windows.net/blast-db/core_nt/core_nt',
            use_local_ssd=True)
        assert plan.db_partitions >= 10

    def test_pv_mode_no_node_clamp(self):
        """In PV mode, partitions may be fewer than num_nodes."""
        plan = compute_partition_plan(
            db_size_gb=269.0, node_ram_gb=256, num_nodes=20,
            db_url='https://stgelb.blob.core.windows.net/blast-db/core_nt/core_nt',
            use_local_ssd=False)
        # DB is 269 GB, node usable ~190 GB -> needs ~2 partitions
        assert plan.db_partitions >= MIN_PARTITIONS
        # Should NOT clamp to 20 nodes in PV mode
        assert plan.db_partitions < 20

    def test_partition_count_capped(self):
        """Partition count should not exceed MAX_PARTITIONS."""
        plan = compute_partition_plan(
            db_size_gb=5000.0, node_ram_gb=8, num_nodes=200,
            db_url='https://stgelb.blob.core.windows.net/blast-db/huge/huge',
            use_local_ssd=True)
        assert plan.db_partitions <= MAX_PARTITIONS

    def test_custom_prefix_override(self):
        """partition_prefix_override should be used instead of derived prefix."""
        custom = 'https://stgelb.blob.core.windows.net/blast-db/custom/shard_'
        plan = compute_partition_plan(
            db_size_gb=500.0, node_ram_gb=64, num_nodes=10,
            db_url='https://stgelb.blob.core.windows.net/blast-db/nt/nt',
            partition_prefix_override=custom)
        assert plan.db_partition_prefix == custom

    def test_invalid_db_size(self):
        """Negative DB size should raise ValueError."""
        with pytest.raises(ValueError, match='DB size must be positive'):
            compute_partition_plan(
                db_size_gb=-1.0, node_ram_gb=256, num_nodes=3,
                db_url='https://x/db')

    def test_invalid_node_ram(self):
        """Zero node RAM should raise ValueError."""
        with pytest.raises(ValueError, match='Node RAM must be positive'):
            compute_partition_plan(
                db_size_gb=100.0, node_ram_gb=0, num_nodes=3,
                db_url='https://x/db')

    def test_invalid_num_nodes(self):
        """Zero nodes should raise ValueError."""
        with pytest.raises(ValueError, match='Number of nodes must be positive'):
            compute_partition_plan(
                db_size_gb=100.0, node_ram_gb=256, num_nodes=0,
                db_url='https://x/db')

    def test_per_node_gb_correct(self):
        """per_node_gb should be db_size / partitions."""
        plan = compute_partition_plan(
            db_size_gb=300.0, node_ram_gb=64, num_nodes=10,
            db_url='https://stgelb.blob.core.windows.net/blast-db/nt/nt')
        if plan.db_partitions > 0:
            expected = round(300.0 / plan.db_partitions, 1)
            assert plan.per_node_gb == expected

    def test_tiny_node_ram_raises(self):
        """Node RAM <= SYSTEM_MEMORY_RESERVE should raise ValueError."""
        with pytest.raises(ValueError, match='too small'):
            compute_partition_plan(
                db_size_gb=100.0, node_ram_gb=2.0, num_nodes=3,
                db_url='https://x/db/db')

    def test_max_partitions_emits_warning_when_shard_exceeds_ram(self, caplog):
        """When capped at MAX_PARTITIONS but shard still > node RAM, should log warning."""
        import logging
        with caplog.at_level(logging.WARNING):
            plan = compute_partition_plan(
                db_size_gb=5000.0, node_ram_gb=8, num_nodes=200,
                db_url='https://stgelb.blob.core.windows.net/blast-db/huge/huge',
                use_local_ssd=True)
            assert plan.db_partitions == MAX_PARTITIONS
            # per_node_gb = 5000/100 = 50, usable = (8-2)*0.75 = 4.5
            assert plan.per_node_gb > (8 - SYSTEM_MEMORY_RESERVE) * DB_RAM_FRACTION
        assert 'capped at' in caplog.text

    def test_empty_prefix_from_bad_url_raises(self):
        """URL with no slashes should raise ValueError (cannot derive prefix)."""
        with pytest.raises(ValueError, match='Cannot derive partition prefix'):
            compute_partition_plan(
                db_size_gb=500.0, node_ram_gb=64, num_nodes=10,
                db_url='noslashurl')


class TestDerivePartitionPrefix:
    """Tests for _derive_partition_prefix()."""

    def test_standard_url(self):
        """Standard DB URL should derive correct prefix."""
        url = 'https://stgelb.blob.core.windows.net/blast-db/core_nt/core_nt'
        prefix = _derive_partition_prefix(url, 10)
        assert prefix == 'https://stgelb.blob.core.windows.net/blast-db/10shards/core_nt_shard_'

    def test_different_partition_count(self):
        """Partition count should be reflected in directory name."""
        url = 'https://stgelb.blob.core.windows.net/blast-db/nt/nt'
        prefix = _derive_partition_prefix(url, 5)
        assert '5shards' in prefix
        assert prefix.endswith('nt_shard_')

    def test_deep_path(self):
        """URL with deeper path should still work."""
        url = 'https://stgelb.blob.core.windows.net/blast-db/databases/v5/swissprot/swissprot'
        prefix = _derive_partition_prefix(url, 3)
        assert prefix == 'https://stgelb.blob.core.windows.net/blast-db/databases/v5/3shards/swissprot_shard_'


class TestApplyAutoPartition:
    """Tests for apply_auto_partition()."""

    def _make_cfg(self, db_size_gb=269.0, machine_type='Standard_E16s_v3',
                  num_nodes=5, use_local_ssd=True, db_partitions=0,
                  db_partition_prefix='', db_auto_partition=True):
        """Create a mock config object."""
        cfg = MagicMock()
        cfg.blast.db_auto_partition = db_auto_partition
        cfg.blast.db_partitions = db_partitions
        cfg.blast.db_partition_prefix = db_partition_prefix
        cfg.blast.db = 'https://stgelb.blob.core.windows.net/blast-db/core_nt/core_nt'
        cfg.blast.db_metadata = MagicMock()
        cfg.blast.db_metadata.bytes_to_cache = int(db_size_gb * (1024 ** 3))
        cfg.cluster.machine_type = machine_type
        cfg.cluster.num_nodes = num_nodes
        cfg.cluster.use_local_ssd = use_local_ssd
        return cfg

    @patch('elastic_blast.azure_traits.get_machine_properties')
    def test_auto_partition_applied(self, mock_props):
        """Auto-partition should set db_partitions and db_partition_prefix."""
        mock_props.return_value = MagicMock(memory=128)
        cfg = self._make_cfg(db_size_gb=269.0, num_nodes=5)
        plan = apply_auto_partition(cfg)
        assert plan is not None
        assert plan.db_partitions > 0
        assert cfg.blast.db_partitions == plan.db_partitions
        assert cfg.blast.db_partition_prefix == plan.db_partition_prefix

    @patch('elastic_blast.azure_traits.get_machine_properties')
    def test_skip_when_manual_partitions_set(self, mock_props):
        """Should skip when db_partitions is already > 0."""
        cfg = self._make_cfg(db_partitions=10)
        plan = apply_auto_partition(cfg)
        assert plan is None

    @patch('elastic_blast.azure_traits.get_machine_properties')
    def test_skip_when_no_metadata(self, mock_props):
        """Should skip when db_metadata is None."""
        cfg = self._make_cfg()
        cfg.blast.db_metadata = None
        plan = apply_auto_partition(cfg)
        assert plan is None

    @patch('elastic_blast.azure_traits.get_machine_properties')
    def test_small_db_no_partition(self, mock_props):
        """Small DB should return plan with 0 partitions."""
        mock_props.return_value = MagicMock(memory=256)
        cfg = self._make_cfg(db_size_gb=5.0)
        plan = apply_auto_partition(cfg)
        assert plan is not None
        assert plan.db_partitions == 0
        assert cfg.blast.db_partitions == 0

    @patch('elastic_blast.azure_traits.get_machine_properties')
    def test_preserves_existing_prefix(self, mock_props):
        """Should use existing db_partition_prefix if already set."""
        mock_props.return_value = MagicMock(memory=128)
        custom = 'https://stgelb.blob.core.windows.net/blast-db/custom/shard_'
        cfg = self._make_cfg(db_size_gb=269.0, db_partition_prefix=custom)
        plan = apply_auto_partition(cfg)
        assert plan is not None
        assert plan.db_partition_prefix == custom

    @patch('elastic_blast.azure_traits.get_machine_properties')
    def test_unknown_machine_type_returns_none(self, mock_props):
        """Unknown machine type should return None (not crash)."""
        mock_props.side_effect = NotImplementedError('Unknown VM')
        cfg = self._make_cfg(machine_type='Standard_Unknown_v99')
        plan = apply_auto_partition(cfg)
        assert plan is None

    @patch('elastic_blast.azure_traits.get_machine_properties')
    def test_tiny_ram_returns_none(self, mock_props):
        """Node RAM too small to compute plan should return None (not crash)."""
        mock_props.return_value = MagicMock(memory=1.0)  # < SYSTEM_MEMORY_RESERVE
        cfg = self._make_cfg(db_size_gb=269.0)
        plan = apply_auto_partition(cfg)
        assert plan is None
        # Config should NOT be modified
        assert cfg.blast.db_partitions == 0
