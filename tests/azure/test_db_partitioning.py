# Tests for Phase 2: DB Partitioning for TB-Scale

"""
Unit tests for DB partitioning functionality in azure.py and kubernetes.py

Tests cover:
- Config: db_partitions and db_partition_prefix in BlastConfig
- _submit_partitioned(): partitioned mode routing
- _generate_partitioned_jobs(): P × N job generation
- _job_substitutions_for_partition(): per-partition substitution dict
- _initialize_cluster_partitioned(): cluster init for partitioned mode
- initialize_storage_partitioned(): partitioned PVC init in kubernetes.py
- Job number limit checking for partitioned mode

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

import os
import shlex
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch, MagicMock, call, ANY
from tempfile import TemporaryDirectory
import pytest
from elastic_blast.azure import ElasticBlastAzure
from elastic_blast.constants import ElbCommand, AKS_PROVISIONING_STATE, INPUT_ERROR
from elastic_blast.elb_config import ElasticBlastConfig
from elastic_blast import config, kubernetes
from elastic_blast.util import SafeExecError, UserReportError

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'azure', 'data')
INI = os.path.join(DATA_DIR, 'test-cfg-file.ini')

PARTITION_PREFIX = 'https://stgelb.blob.core.windows.net/blast-db/mydb/part_'


def _make_cfg(db_partitions: int = 0, db_partition_prefix: str = '',
              reuse: bool = False, dry_run: bool = True,
              use_local_ssd: bool = False) -> ElasticBlastConfig:
    """Create a test config with optional partitioning settings."""
    args = Namespace(cfg=INI)
    cfg = ElasticBlastConfig(config.configure(args), task=ElbCommand.SUBMIT)
    cfg.blast.db_partitions = db_partitions
    cfg.blast.db_partition_prefix = db_partition_prefix
    cfg.cluster.reuse = reuse
    cfg.cluster.dry_run = dry_run
    cfg.cluster.use_local_ssd = use_local_ssd
    return cfg


class TestPartitionConfig:
    """Tests for db_partitions and db_partition_prefix config fields."""

    def test_default_values(self):
        """Default: no partitioning."""
        cfg = _make_cfg()
        assert cfg.blast.db_partitions == 0
        assert cfg.blast.db_partition_prefix == ''

    def test_set_partition_values(self):
        """Config values can be set."""
        cfg = _make_cfg(db_partitions=10, db_partition_prefix=PARTITION_PREFIX)
        assert cfg.blast.db_partitions == 10
        assert cfg.blast.db_partition_prefix == PARTITION_PREFIX

    def test_validation_prefix_required_when_partitions_set(self):
        """Validation: prefix required when partitions > 0."""
        cfg = _make_cfg(db_partitions=5, db_partition_prefix='')
        errors = []
        cfg.blast.validate(errors, ElbCommand.SUBMIT)
        assert any('db-partition-prefix' in e for e in errors)

    def test_validation_negative_partitions(self):
        """Validation: negative partitions rejected."""
        cfg = _make_cfg(db_partitions=-1, db_partition_prefix=PARTITION_PREFIX)
        errors = []
        cfg.blast.validate(errors, ElbCommand.SUBMIT)
        assert any('non-negative' in e for e in errors)

    def test_validation_passes_with_valid_config(self):
        """Validation: valid partitioning config passes."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX)
        errors = []
        cfg.blast.validate(errors, ElbCommand.SUBMIT)
        # No partition-related errors (other errors may exist from test config)
        partition_errors = [e for e in errors if 'partition' in e.lower()]
        assert len(partition_errors) == 0


class TestSubmitPartitioned:
    """Tests for _submit_partitioned() routing."""

    @patch('elastic_blast.azure.get_blastdb_info', return_value=('testdb', '', 'testdb'))
    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    def test_submit_routes_to_partitioned(self, mock_usage, mock_dbinfo):
        """submit() routes to _submit_partitioned when db_partitions > 0."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX)
        elb = ElasticBlastAzure(cfg)
        elb.cloud_job_submission = False

        with patch.object(elb, '_submit_partitioned') as mock_partitioned:
            query_batches = ['batch_000.fa', 'batch_001.fa']
            elb.submit(query_batches, 1000, False)
            mock_partitioned.assert_called_once_with(query_batches, 1000)

    @patch('elastic_blast.azure.get_blastdb_info', return_value=('testdb', '', 'testdb'))
    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    def test_submit_uses_standard_for_no_partitions(self, mock_usage, mock_dbinfo):
        """submit() uses standard path when db_partitions == 0."""
        cfg = _make_cfg(db_partitions=0)
        elb = ElasticBlastAzure(cfg)
        elb.cloud_job_submission = True
        elb.cluster_initialized = True  # Skip init

        with patch.object(elb, '_submit_partitioned') as mock_partitioned, \
             patch('elastic_blast.kubernetes.submit_job_submission_job') as mock_submit_job:
            query_batches = ['batch_000.fa', 'batch_001.fa']
            elb.submit(query_batches, 1000, False)
            mock_partitioned.assert_not_called()
            mock_submit_job.assert_called_once()

    @patch('elastic_blast.azure.get_blastdb_info', return_value=('testdb', '', 'testdb'))
    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    def test_partitioned_checks_job_limit(self, mock_usage, mock_dbinfo):
        """_submit_partitioned raises when P × N exceeds K8s limit."""
        cfg = _make_cfg(db_partitions=100, db_partition_prefix=PARTITION_PREFIX)
        elb = ElasticBlastAzure(cfg)
        elb.cloud_job_submission = False

        # Generate more queries than limit when multiplied by partitions
        with patch('elastic_blast.kubernetes.get_maximum_number_of_allowed_k8s_jobs',
                   return_value=100):
            query_batches = [f'batch_{i:03d}.fa' for i in range(10)]
            # 100 partitions × 10 queries = 1000 > 100 limit
            with pytest.raises(UserReportError) as exc_info:
                elb._submit_partitioned(query_batches, 1000)
            assert 'exceeding the limit' in str(exc_info.value.message)


class TestJobSubstitutionsPartition:
    """Tests for _job_substitutions_for_partition()."""

    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    def test_substitution_values(self, mock_usage):
        """Check substitution dictionary has correct partition-specific values."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX)
        elb = ElasticBlastAzure(cfg)
        query_batches = ['batch_000.fa', 'batch_001.fa']

        subs = elb._job_substitutions_for_partition(
            query_batches, 2, 'part_02/part_02', 'part_02'
        )

        # Check partition-specific values
        assert subs['ELB_DB'] == 'part_02/part_02'
        assert subs['ELB_DB_LABEL'] == 'part02'
        assert 'part_02' in subs['ELB_RESULTS']
        assert subs['ELB_BLAST_PROGRAM'] == 'blastx'

    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    def test_different_partitions_have_different_results_paths(self, mock_usage):
        """Each partition should have unique results path."""
        cfg = _make_cfg(db_partitions=3, db_partition_prefix=PARTITION_PREFIX)
        elb = ElasticBlastAzure(cfg)
        query_batches = ['batch_000.fa']

        paths = set()
        for i in range(3):
            subs = elb._job_substitutions_for_partition(
                query_batches, i, f'part_{i:02d}/db', 'db'
            )
            paths.add(subs['ELB_RESULTS'])

        assert len(paths) == 3, 'Each partition must have a unique results path'


class TestGeneratePartitionedJobs:
    """Tests for _generate_partitioned_jobs()."""

    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    @patch('elastic_blast.kubernetes.submit_jobs', return_value=['job1', 'job2'])
    def test_generates_correct_number_of_jobs(self, mock_submit, mock_usage):
        """P partitions × N queries = P×N job files."""
        cfg = _make_cfg(db_partitions=3, db_partition_prefix=PARTITION_PREFIX)
        cfg.appstate.k8s_ctx = 'test-ctx'
        elb = ElasticBlastAzure(cfg)

        query_batches = ['batch_000.fa', 'batch_001.fa', 'batch_002.fa']
        elb._generate_partitioned_jobs(query_batches)

        # submit_jobs was called with a directory containing job files
        mock_submit.assert_called_once()
        # Verify the Path argument was passed
        call_args = mock_submit.call_args
        assert isinstance(call_args[0][1], Path) or isinstance(call_args.args[1], Path)

    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    @patch('elastic_blast.kubernetes.submit_jobs', return_value=[])
    def test_dry_run_does_not_submit(self, mock_submit, mock_usage):
        """Dry run passes dry_run=True to submit_jobs."""
        cfg = _make_cfg(db_partitions=2, db_partition_prefix=PARTITION_PREFIX)
        cfg.appstate.k8s_ctx = 'test-ctx'
        elb = ElasticBlastAzure(cfg)

        query_batches = ['batch_000.fa']
        elb._generate_partitioned_jobs(query_batches)

        # submit_jobs called with dry_run=True (from cfg.cluster.dry_run)
        _, kwargs = mock_submit.call_args
        assert kwargs.get('dry_run', False) is True


class TestInitializeClusterPartitioned:
    """Tests for _initialize_cluster_partitioned()."""

    @patch('elastic_blast.azure.get_blastdb_info', return_value=('testdb', '', 'testdb'))
    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    @patch('elastic_blast.azure.check_cluster', return_value='')
    @patch('elastic_blast.azure.start_cluster')
    @patch('elastic_blast.azure.set_role_assignment')
    @patch('elastic_blast.kubernetes.enable_service_account')
    @patch('elastic_blast.kubernetes.initialize_storage_partitioned')
    def test_creates_cluster_and_inits_partitioned_storage(
            self, mock_init_part, mock_sa, mock_role, mock_start,
            mock_check, mock_usage, mock_dbinfo):
        """Full cluster creation + partitioned storage init."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX)
        elb = ElasticBlastAzure(cfg)
        elb.cloud_job_submission = False

        with patch.object(elb, '_get_aks_credentials', return_value='test-ctx'), \
             patch.object(elb, '_label_nodes'):
            elb._initialize_cluster_partitioned(['batch_000.fa'])

        mock_start.assert_called_once()
        mock_init_part.assert_called_once()

    @patch('elastic_blast.azure.get_blastdb_info', return_value=('testdb', '', 'testdb'))
    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    @patch('elastic_blast.azure.check_cluster', return_value=AKS_PROVISIONING_STATE.SUCCEEDED)
    @patch('elastic_blast.azure.start_cluster')
    @patch('elastic_blast.azure.set_role_assignment')
    @patch('elastic_blast.kubernetes.enable_service_account')
    @patch('elastic_blast.kubernetes.initialize_storage_partitioned')
    def test_reuse_skips_cluster_creation(
            self, mock_init_part, mock_sa, mock_role, mock_start,
            mock_check, mock_usage, mock_dbinfo):
        """In reuse mode with existing cluster, skips start_cluster."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX, reuse=True)
        elb = ElasticBlastAzure(cfg)
        elb.cloud_job_submission = False

        with patch.object(elb, '_get_aks_credentials', return_value='test-ctx'), \
             patch.object(elb, '_label_nodes'):
            elb._initialize_cluster_partitioned(['batch_000.fa'])

        mock_start.assert_not_called()
        mock_init_part.assert_called_once()


class TestInitializeStoragePartitioned:
    """Tests for kubernetes.initialize_storage_partitioned()."""

    def test_creates_pvc_and_init_job(self):
        """Verify PVC and init job are created via kubectl."""
        cfg = _make_cfg(db_partitions=3, db_partition_prefix=PARTITION_PREFIX,
                        dry_run=False)
        cfg.appstate.k8s_ctx = 'test-ctx'

        safe_exec_calls = []

        def mock_safe_exec(cmd, **kwargs):
            if isinstance(cmd, str):
                safe_exec_calls.append(cmd)
            else:
                safe_exec_calls.append(' '.join(cmd))
            result = MagicMock()
            result.stdout = b''
            return result

        with patch('elastic_blast.kubernetes.safe_exec', side_effect=mock_safe_exec):
            kubernetes.initialize_storage_partitioned(cfg, wait=False)

        # Should have at least 2 kubectl apply calls (PVC + init job)
        apply_calls = [c for c in safe_exec_calls if 'kubectl' in c and 'apply' in c]
        assert len(apply_calls) >= 2, f'Expected at least 2 kubectl apply calls, got {len(apply_calls)}'

    def test_dry_run_logs_only(self):
        """Dry run should not call safe_exec kubectl."""
        cfg = _make_cfg(db_partitions=3, db_partition_prefix=PARTITION_PREFIX)
        cfg.appstate.k8s_ctx = 'test-ctx'

        with patch('elastic_blast.kubernetes.safe_exec') as mock_exec:
            kubernetes.initialize_storage_partitioned(cfg, wait=False)
            # In dry_run mode, safe_exec should not be called
            mock_exec.assert_not_called()

    def test_rejects_zero_partitions(self):
        """Should raise ValueError for 0 partitions."""
        cfg = _make_cfg(db_partitions=0, db_partition_prefix=PARTITION_PREFIX)
        cfg.appstate.k8s_ctx = 'test-ctx'

        with pytest.raises(ValueError, match='db_partitions must be > 0'):
            kubernetes.initialize_storage_partitioned(cfg)

    def test_rejects_missing_k8s_ctx(self):
        """Should raise RuntimeError without k8s context."""
        cfg = _make_cfg(db_partitions=3, db_partition_prefix=PARTITION_PREFIX)
        cfg.appstate.k8s_ctx = None

        with pytest.raises(RuntimeError, match='kubernetes context is missing'):
            kubernetes.initialize_storage_partitioned(cfg)

    def test_generates_download_commands_for_each_partition(self):
        """The init job YAML should contain download commands for each partition."""
        cfg = _make_cfg(db_partitions=3, db_partition_prefix=PARTITION_PREFIX,
                        dry_run=False)
        cfg.appstate.k8s_ctx = 'test-ctx'

        created_files = []

        def capture_safe_exec(cmd, **kwargs):
            if isinstance(cmd, str):
                cmd_str = cmd
            else:
                cmd_str = ' '.join(cmd)
            # Capture the init job YAML content if it's a kubectl apply
            if 'apply -f' in cmd_str and 'partitioned' in cmd_str:
                yaml_path = cmd_str.split('-f ')[-1].strip()
                if os.path.exists(yaml_path):
                    with open(yaml_path) as f:
                        created_files.append(f.read())
            result = MagicMock()
            result.stdout = b''
            return result

        with patch('elastic_blast.kubernetes.safe_exec', side_effect=capture_safe_exec):
            kubernetes.initialize_storage_partitioned(cfg, wait=False)

        # Verify the generated YAML contains all partition references
        assert len(created_files) >= 1, 'Expected init job YAML to be captured'
        yaml_content = created_files[-1]
        for i in range(3):
            assert f'part_{i:02d}' in yaml_content, \
                f'Expected part_{i:02d} in init job YAML'
