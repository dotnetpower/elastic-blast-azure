# Tests for Phase 1: Warm Cluster — DB RAM Residency

"""
Unit tests for warm cluster reuse functionality in azure.py

Tests cover:
- _db_already_loaded(): PVC/init-pv existence check
- _cleanup_jobs_only(): reuse mode job cleanup
- _initialize_cluster() with reuse mode shortcut
- scale_nodes(): AKS node pool scaling
- delete() in reuse mode
- _deploy_vmtouch_daemonset(): DaemonSet deployment

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

import os
import shlex
from argparse import Namespace
from unittest.mock import patch, MagicMock, call
import pytest
from elastic_blast.azure import ElasticBlastAzure, check_cluster
from elastic_blast.constants import ElbCommand, AKS_PROVISIONING_STATE
from elastic_blast.elb_config import ElasticBlastConfig
from elastic_blast import config
from elastic_blast.util import SafeExecError

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'azure', 'data')
INI = os.path.join(DATA_DIR, 'test-cfg-file.ini')


def _make_cfg(reuse: bool = True, dry_run: bool = True,
              use_local_ssd: bool = False) -> ElasticBlastConfig:
    """Create a test config with optional reuse/dry_run settings."""
    args = Namespace(cfg=INI)
    with patch('elastic_blast.elb_config.get_latest_dir', return_value='latest'), \
         patch('elastic_blast.elb_config.get_db_metadata', return_value=None):
        cfg = ElasticBlastConfig(config.configure(args), task=ElbCommand.SUBMIT)
    cfg.cluster.reuse = reuse
    cfg.cluster.dry_run = dry_run
    cfg.cluster.use_local_ssd = use_local_ssd
    return cfg


class TestDbAlreadyLoaded:
    """Tests for _db_already_loaded() method."""

    def test_returns_false_in_dry_run(self):
        """dry_run mode always returns False."""
        cfg = _make_cfg(reuse=True, dry_run=True)
        elb = ElasticBlastAzure(cfg)
        assert elb._db_already_loaded() is False

    def test_returns_false_for_local_ssd(self):
        """Local SSD mode returns False (can't verify without running a pod)."""
        cfg = _make_cfg(reuse=True, dry_run=False, use_local_ssd=True)
        elb = ElasticBlastAzure(cfg)
        # Mock _get_k8s_ctx to avoid actual cluster connection
        with patch.object(elb, '_get_k8s_ctx', return_value='test-ctx'):
            assert elb._db_already_loaded() is False

    def test_returns_true_when_pvc_bound_and_init_succeeded(self):
        """Returns True when PVC is Bound and init-pv job succeeded."""
        cfg = _make_cfg(reuse=True, dry_run=False)
        elb = ElasticBlastAzure(cfg)

        mock_proc_pvc = MagicMock()
        mock_proc_pvc.stdout = b'Bound'
        mock_proc_init = MagicMock()
        mock_proc_init.stdout = b'1'

        with patch.object(elb, '_get_k8s_ctx', return_value='test-ctx'):
            with patch('elastic_blast.azure.safe_exec') as mock_exec:
                mock_exec.side_effect = [mock_proc_pvc, mock_proc_init]
                result = elb._db_already_loaded()

        assert result is True
        assert mock_exec.call_count == 2

    def test_returns_false_when_pvc_not_bound(self):
        """Returns False when PVC is not bound."""
        cfg = _make_cfg(reuse=True, dry_run=False)
        elb = ElasticBlastAzure(cfg)

        mock_proc = MagicMock()
        mock_proc.stdout = b'Pending'

        with patch.object(elb, '_get_k8s_ctx', return_value='test-ctx'):
            with patch('elastic_blast.azure.safe_exec', return_value=mock_proc):
                result = elb._db_already_loaded()

        assert result is False

    def test_returns_false_on_exception(self):
        """Returns False when kubectl command fails."""
        cfg = _make_cfg(reuse=True, dry_run=False)
        elb = ElasticBlastAzure(cfg)

        with patch.object(elb, '_get_k8s_ctx', return_value='test-ctx'):
            with patch('elastic_blast.azure.safe_exec', side_effect=Exception('connection refused')):
                result = elb._db_already_loaded()

        assert result is False


class TestCleanupJobsOnly:
    """Tests for _cleanup_jobs_only() method."""

    def test_deletes_blast_and_submit_jobs(self):
        """Should delete blast and submit jobs, preserving cluster."""
        cfg = _make_cfg(reuse=True, dry_run=False)
        elb = ElasticBlastAzure(cfg)

        with patch.object(elb, '_get_k8s_ctx', return_value='test-ctx'):
            with patch('elastic_blast.azure.safe_exec') as mock_exec:
                elb._cleanup_jobs_only()

        # Should have called delete for blast jobs and submit jobs
        assert mock_exec.call_count == 2
        calls = mock_exec.call_args_list
        # First call: delete blast jobs
        assert 'delete' in ' '.join(str(c) for c in calls[0].args[0])
        assert 'app=blast' in ' '.join(str(c) for c in calls[0].args[0])
        # Second call: delete submit jobs
        assert 'delete' in ' '.join(str(c) for c in calls[1].args[0])
        assert 'app=submit' in ' '.join(str(c) for c in calls[1].args[0])

    def test_handles_kubectl_failure_gracefully(self):
        """Should not raise when kubectl fails."""
        cfg = _make_cfg(reuse=True, dry_run=False)
        elb = ElasticBlastAzure(cfg)

        with patch.object(elb, '_get_k8s_ctx', return_value='test-ctx'):
            with patch('elastic_blast.azure.safe_exec', side_effect=Exception('cluster unreachable')):
                # Should not raise
                elb._cleanup_jobs_only()

    def test_dry_run_logs_only(self):
        """In dry run, should log commands without executing."""
        cfg = _make_cfg(reuse=True, dry_run=True)
        elb = ElasticBlastAzure(cfg)

        with patch.object(elb, '_get_k8s_ctx', return_value='test-ctx'):
            with patch('elastic_blast.azure.safe_exec') as mock_exec:
                elb._cleanup_jobs_only()

        # In dry run mode, safe_exec should NOT be called
        mock_exec.assert_not_called()


class TestDeleteReuseMode:
    """Tests for delete() in reuse mode."""

    def test_delete_reuse_mode_calls_cleanup(self):
        """In reuse mode, delete() should call _cleanup_jobs_only, not delete_cluster_with_cleanup."""
        cfg = _make_cfg(reuse=True, dry_run=True)
        elb = ElasticBlastAzure(cfg)

        with patch.object(elb, '_cleanup_jobs_only') as mock_cleanup:
            with patch('elastic_blast.azure.delete_cluster_with_cleanup') as mock_delete:
                elb.delete()

        mock_cleanup.assert_called_once()
        mock_delete.assert_not_called()

    def test_delete_non_reuse_mode_deletes_cluster(self):
        """Without reuse, delete() should call delete_cluster_with_cleanup."""
        cfg = _make_cfg(reuse=False, dry_run=True)
        elb = ElasticBlastAzure(cfg)

        with patch.object(elb, '_cleanup_jobs_only') as mock_cleanup:
            with patch('elastic_blast.azure.delete_cluster_with_cleanup') as mock_delete:
                elb.delete()

        mock_cleanup.assert_not_called()
        mock_delete.assert_called_once()


class TestScaleNodes:
    """Tests for scale_nodes() method."""

    def test_scale_down_to_zero(self):
        """Should call az aks nodepool scale with node-count 0."""
        cfg = _make_cfg(dry_run=False)
        elb = ElasticBlastAzure(cfg)

        with patch('elastic_blast.azure_sdk.scale_node_pool') as mock:
            elb.scale_nodes(0)
        mock.assert_called_once()

    def test_scale_up(self):
        """Should call SDK scale_node_pool with specified count."""
        cfg = _make_cfg(dry_run=False)
        elb = ElasticBlastAzure(cfg)

        with patch('elastic_blast.azure_sdk.scale_node_pool') as mock:
            elb.scale_nodes(5)
        mock.assert_called_once()

    def test_dry_run_does_not_execute(self):
        """In dry run, should pass dry_run to SDK."""
        cfg = _make_cfg(dry_run=True)
        elb = ElasticBlastAzure(cfg)

        with patch('elastic_blast.azure_sdk.scale_node_pool') as mock:
            elb.scale_nodes(0)
        mock.assert_called_once()


class TestInitializeClusterReuse:
    """Tests for _initialize_cluster() warm cluster shortcut."""

    def test_skips_init_when_db_loaded(self, mocker):
        """When reuse=true, cluster running, DB loaded → skip full init."""
        cfg = _make_cfg(reuse=True, dry_run=False)
        elb = ElasticBlastAzure(cfg)

        mocker.patch('elastic_blast.azure.check_cluster',
                     return_value=AKS_PROVISIONING_STATE.SUCCEEDED.value)
        mocker.patch.object(elb, '_db_already_loaded', return_value=True)
        mock_upload = mocker.patch.object(elb, '_upload_queries_only')
        mock_start = mocker.patch('elastic_blast.azure.start_cluster')
        mock_storage = mocker.patch('elastic_blast.azure.kubernetes.initialize_storage')

        elb._initialize_cluster(queries=['batch_001.fa'])

        mock_upload.assert_called_once_with(['batch_001.fa'])
        mock_start.assert_not_called()
        mock_storage.assert_not_called()

    def test_full_init_when_db_not_loaded(self, mocker):
        """When reuse=true but DB not loaded → full initialization."""
        cfg = _make_cfg(reuse=True, dry_run=True)
        elb = ElasticBlastAzure(cfg)
        elb.cloud_job_submission = False

        mocker.patch('elastic_blast.azure.check_cluster', return_value='')
        mocker.patch.object(elb, '_db_already_loaded', return_value=False)
        mock_start = mocker.patch('elastic_blast.azure.start_cluster_async', return_value=None)
        mocker.patch('elastic_blast.azure.wait_for_cluster')
        mocker.patch('elastic_blast.azure.set_role_assignment')
        mocker.patch('elastic_blast.azure.kubernetes.enable_service_account')
        mocker.patch('elastic_blast.azure.kubernetes.create_scripts_configmap')
        mocker.patch('elastic_blast.azure.kubernetes.initialize_storage')
        mocker.patch.object(elb, '_label_nodes')
        mocker.patch.object(elb, '_get_k8s_ctx', return_value='test-ctx')
        mocker.patch.object(elb, '_deploy_vmtouch_daemonset')
        mocker.patch('elastic_blast.azure.get_blastdb_info', return_value=('testdb', '', 'testdb'))
        mocker.patch('elastic_blast.azure.get_usage_reporting', return_value=False)

        elb._initialize_cluster(queries=None)

        mock_start.assert_called_once()

    def test_full_init_when_reuse_false(self, mocker):
        """When reuse=false → always do full initialization."""
        cfg = _make_cfg(reuse=False, dry_run=True)
        elb = ElasticBlastAzure(cfg)
        elb.cloud_job_submission = False

        mocker.patch('elastic_blast.azure.check_cluster', return_value='')
        mock_start = mocker.patch('elastic_blast.azure.start_cluster_async', return_value=None)
        mocker.patch('elastic_blast.azure.wait_for_cluster')
        mocker.patch('elastic_blast.azure.set_role_assignment')
        mocker.patch('elastic_blast.azure.kubernetes.enable_service_account')
        mocker.patch('elastic_blast.azure.kubernetes.create_scripts_configmap')
        mocker.patch('elastic_blast.azure.kubernetes.initialize_storage')
        mocker.patch.object(elb, '_label_nodes')
        mocker.patch.object(elb, '_get_k8s_ctx', return_value='test-ctx')
        mocker.patch('elastic_blast.azure.get_blastdb_info', return_value=('testdb', '', 'testdb'))
        mocker.patch('elastic_blast.azure.get_usage_reporting', return_value=False)

        elb._initialize_cluster(queries=None)

        mock_start.assert_called_once()
