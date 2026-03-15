# 

"""
Unit tests for azure module

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

import json
import os
import subprocess
from argparse import Namespace
from unittest.mock import patch, MagicMock

import pytest  # type: ignore

from elastic_blast.azure import (
    ElasticBlastAzure,
    get_disks,
    get_snapshots,
    delete_disk,
    delete_snapshot,
    get_aks_clusters,
    get_aks_credentials,
    delete_cluster_with_cleanup,
    check_cluster,
    remove_split_query,
    check_prerequisites,
)
from elastic_blast import azure
from elastic_blast import kubernetes
from elastic_blast import config
from elastic_blast.constants import (
    CLUSTER_ERROR,
    ElbCommand,
    ElbStatus,
    AKS_PROVISIONING_STATE,
)
from elastic_blast.util import SafeExecError, UserReportError
from elastic_blast.elb_config import ElasticBlastConfig
from elastic_blast.db_metadata import DbMetadata
from tests.utils import MockedCompletedProcess, AZURE_DISKS, AZURE_SNAPSHOTS, AKS_CLUSTERS

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
INI = os.path.join(DATA_DIR, 'test-cfg-file.ini')

DB_METADATA = DbMetadata(version='1',
                         dbname='some-name',
                         dbtype='Protein',
                         description='A test database',
                         number_of_letters=25,
                         number_of_sequences=25,
                         files=[],
                         last_updated='some-date',
                         bytes_total=25,
                         bytes_to_cache=25,
                         number_of_volumes=1)


def _make_cfg(dry_run: bool = True) -> ElasticBlastConfig:
    """Create a test config from the Azure INI file.
    
    Mocks get_latest_dir and get_db_metadata to avoid Azure Storage
    connections during ElasticBlastConfig construction.
    """
    args = Namespace(cfg=INI)
    with patch('elastic_blast.elb_config.get_latest_dir', return_value='latest'), \
         patch('elastic_blast.elb_config.get_db_metadata', return_value=DB_METADATA):
        cfg = ElasticBlastConfig(config.configure(args), task=ElbCommand.SUBMIT)
    cfg.cluster.dry_run = dry_run
    return cfg


# ---------------------------------------------------------------------------
# Azure Disk tests
# ---------------------------------------------------------------------------

class TestGetDisks:
    """Tests for azure.get_disks()."""

    def test_returns_disk_names(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_disks', return_value=['disk-1', 'disk-2']):
            assert get_disks(cfg) == ['disk-1', 'disk-2']

    def test_returns_empty_list(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_disks', return_value=[]):
            assert get_disks(cfg) == []

    def test_raises_runtime_error_on_bad_json(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_disks', side_effect=RuntimeError('SDK error')):
            with pytest.raises(RuntimeError):
                get_disks(cfg)

    def test_dry_run_returns_empty(self):
        cfg = _make_cfg(dry_run=True)
        with patch('elastic_blast.azure_sdk.get_disks', return_value=[]) as m:
            assert get_disks(cfg, dry_run=True) == []
            m.assert_called_once()


class TestGetSnapshots:
    """Tests for azure.get_snapshots()."""

    def test_returns_snapshot_names(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_snapshots', return_value=['snap-1']):
            assert get_snapshots(cfg) == ['snap-1']

    def test_raises_runtime_error_on_bad_json(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_snapshots', side_effect=RuntimeError('SDK error')):
            with pytest.raises(RuntimeError):
                get_snapshots(cfg)


class TestDeleteDisk:
    """Tests for azure.delete_disk()."""

    def test_calls_az_disk_delete(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.delete_disk') as mock:
            delete_disk('my-disk', cfg)
        mock.assert_called_once_with(cfg.azure.resourcegroup, 'my-disk')

    def test_empty_name_raises_value_error(self):
        cfg = _make_cfg()
        with pytest.raises(ValueError, match='No disk name'):
            delete_disk('', cfg)

    def test_none_cfg_raises_value_error(self):
        with pytest.raises(ValueError, match='No application config'):
            delete_disk('some-disk', None)

    def test_propagates_safe_exec_error(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.delete_disk',
                   side_effect=Exception('disk not found')):
            with pytest.raises(Exception):
                delete_disk('nonexistent-disk', cfg)


class TestDeleteSnapshot:
    """Tests for azure.delete_snapshot()."""

    def test_calls_az_snapshot_delete(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.delete_snapshot') as mock:
            delete_snapshot('my-snap', cfg)
        mock.assert_called_once_with(cfg.azure.resourcegroup, 'my-snap')

    def test_empty_name_raises_value_error(self):
        cfg = _make_cfg()
        with pytest.raises(ValueError, match='No snapshot name'):
            delete_snapshot('', cfg)


# ---------------------------------------------------------------------------
# AKS Cluster tests
# ---------------------------------------------------------------------------

class TestGetAksClusters:
    """Tests for azure.get_aks_clusters()."""

    def test_returns_cluster_names(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_aks_clusters', return_value=['cluster-a', 'cluster-b']):
            assert get_aks_clusters(cfg) == ['cluster-a', 'cluster-b']

    def test_returns_empty_list(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_aks_clusters', return_value=[]):
            assert get_aks_clusters(cfg) == []

    def test_raises_runtime_error_on_bad_json(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_aks_clusters', side_effect=RuntimeError('parse error')):
            with pytest.raises(RuntimeError):
                get_aks_clusters(cfg)

    def test_calls_az_aks_list_with_resource_group(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_aks_clusters', return_value=[]) as mock:
            get_aks_clusters(cfg)
        mock.assert_called_once_with(cfg.azure.resourcegroup, cfg.cluster.dry_run)


class TestGetAksCredentials:
    """Tests for azure.get_aks_credentials()."""

    def test_returns_kubernetes_context(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_aks_credentials', return_value='my-aks-context'):
            assert get_aks_credentials(cfg) == 'my-aks-context'

    def test_propagates_error_for_nonexistent_cluster(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_aks_credentials',
                   side_effect=UserReportError(1, 'cluster not found')):
            with pytest.raises(UserReportError):
                get_aks_credentials(cfg)


# ---------------------------------------------------------------------------
# delete_cluster_with_cleanup tests
# ---------------------------------------------------------------------------

class TestDeleteClusterWithCleanup:
    """Tests for azure.delete_cluster_with_cleanup()."""

    def test_successful_cleanup_and_deletion(self, mocker):
        """Full cluster deletion: get credentials, delete k8s resources, delete cluster."""
        cfg = _make_cfg(dry_run=False)

        mocker.patch('elastic_blast.azure.check_cluster',
                     return_value=AKS_PROVISIONING_STATE.SUCCEEDED)
        mocker.patch('elastic_blast.azure._get_resource_ids',
                     return_value=MagicMock(disks=[], snapshots=[]))
        mocker.patch('elastic_blast.azure.get_aks_credentials', return_value='test-ctx')
        mocker.patch('elastic_blast.azure.kubernetes.check_server')
        mocker.patch('elastic_blast.azure.kubernetes.get_persistent_disks', return_value=[])
        mocker.patch('elastic_blast.azure.kubernetes.get_volume_snapshots', return_value=[])
        mocker.patch('elastic_blast.azure.kubernetes.delete_all', return_value=[])
        mocker.patch('elastic_blast.azure.get_disks', return_value=[])
        mocker.patch('elastic_blast.azure.get_snapshots', return_value=[])
        mocker.patch('elastic_blast.azure.remove_split_query')
        mocker.patch('elastic_blast.azure.delete_cluster', return_value=cfg.cluster.name)

        delete_cluster_with_cleanup(cfg)
        azure.delete_cluster.assert_called_with(cfg)

    def test_no_cluster_raises_error(self, mocker):
        """Raises UserReportError when cluster not found and not dry_run."""
        cfg = _make_cfg(dry_run=False)

        mocker.patch('elastic_blast.azure.check_cluster', return_value='')
        mocker.patch('elastic_blast.azure._get_resource_ids',
                     return_value=MagicMock(disks=[], snapshots=[]))
        # Mock ElasticBlastAzure constructor — _status_from_results returns UNKNOWN
        mock_elb = MagicMock()
        mock_elb._status_from_results.return_value = ElbStatus.UNKNOWN
        mocker.patch('elastic_blast.azure.ElasticBlastAzure', return_value=mock_elb)

        with pytest.raises(UserReportError) as errinfo:
            delete_cluster_with_cleanup(cfg)
        assert errinfo.value.returncode == CLUSTER_ERROR

    def test_dry_run_no_cluster_returns(self, mocker):
        """In dry_run, returns without error when cluster not found."""
        cfg = _make_cfg(dry_run=True)

        mocker.patch('elastic_blast.azure.check_cluster', return_value='')
        mocker.patch('elastic_blast.azure._get_resource_ids',
                     return_value=MagicMock(disks=[], snapshots=[]))

        # Should not raise
        delete_cluster_with_cleanup(cfg)

    def test_failed_kubectl_still_deletes_cluster(self, mocker):
        """Cluster deletion proceeds when kubectl communication fails."""
        cfg = _make_cfg(dry_run=False)

        mocker.patch('elastic_blast.azure.check_cluster',
                     return_value=AKS_PROVISIONING_STATE.SUCCEEDED)
        mocker.patch('elastic_blast.azure._get_resource_ids',
                     return_value=MagicMock(disks=[], snapshots=[]))
        mocker.patch('elastic_blast.azure.get_aks_credentials',
                     side_effect=Exception('connection refused'))
        mocker.patch('elastic_blast.azure.get_disks', return_value=[])
        mocker.patch('elastic_blast.azure.get_snapshots', return_value=[])
        mocker.patch('elastic_blast.azure.remove_split_query')
        mocker.patch('elastic_blast.azure.delete_cluster', return_value=cfg.cluster.name)

        delete_cluster_with_cleanup(cfg)
        azure.delete_cluster.assert_called_with(cfg)

    def test_disk_left_after_k8s_delete_is_cleaned(self, mocker):
        """Disks remaining after k8s delete_all are cleaned up."""
        cfg = _make_cfg(dry_run=False)
        leaked_disk = 'leaked-disk-1'

        mocker.patch('elastic_blast.azure.check_cluster',
                     return_value=AKS_PROVISIONING_STATE.SUCCEEDED)
        mocker.patch('elastic_blast.azure._get_resource_ids',
                     return_value=MagicMock(disks=[leaked_disk], snapshots=[]))
        mocker.patch('elastic_blast.azure.get_aks_credentials', return_value='test-ctx')
        mocker.patch('elastic_blast.azure.kubernetes.check_server')
        mocker.patch('elastic_blast.azure.kubernetes.get_persistent_disks',
                     return_value=[leaked_disk])
        mocker.patch('elastic_blast.azure.kubernetes.get_volume_snapshots', return_value=[])
        mocker.patch('elastic_blast.azure.kubernetes.delete_all', return_value=[])
        mocker.patch('elastic_blast.azure.get_disks', return_value=[leaked_disk])
        mocker.patch('elastic_blast.azure.delete_disk')
        mocker.patch('elastic_blast.azure.get_snapshots', return_value=[])
        mocker.patch('elastic_blast.azure.remove_split_query')
        mocker.patch('elastic_blast.azure.delete_cluster', return_value=cfg.cluster.name)

        delete_cluster_with_cleanup(cfg)
        azure.delete_disk.assert_called_with(leaked_disk, cfg)

    def test_handles_cluster_starting_status(self, mocker):
        """Waits when cluster is Starting, then proceeds to delete."""
        cfg = _make_cfg(dry_run=False)
        call_count = [0]

        def status_transition(ignored_cfg):
            call_count[0] += 1
            if call_count[0] == 1:
                return AKS_PROVISIONING_STATE.STARTING.value
            return AKS_PROVISIONING_STATE.SUCCEEDED.value

        mocker.patch('elastic_blast.azure.check_cluster', side_effect=status_transition)
        mocker.patch('elastic_blast.azure._get_resource_ids',
                     return_value=MagicMock(disks=[], snapshots=[]))
        mocker.patch('elastic_blast.azure.get_aks_credentials', return_value='test-ctx')
        mocker.patch('elastic_blast.azure.kubernetes.check_server')
        mocker.patch('elastic_blast.azure.kubernetes.get_persistent_disks', return_value=[])
        mocker.patch('elastic_blast.azure.kubernetes.get_volume_snapshots', return_value=[])
        mocker.patch('elastic_blast.azure.kubernetes.delete_all', return_value=[])
        mocker.patch('elastic_blast.azure.get_disks', return_value=[])
        mocker.patch('elastic_blast.azure.get_snapshots', return_value=[])
        mocker.patch('elastic_blast.azure.remove_split_query')
        mocker.patch('elastic_blast.azure.delete_cluster', return_value=cfg.cluster.name)
        mocker.patch('time.sleep')

        delete_cluster_with_cleanup(cfg)
        assert azure.check_cluster.call_count > 1
        azure.delete_cluster.assert_called()


# ---------------------------------------------------------------------------
# remove_split_query tests
# ---------------------------------------------------------------------------

class TestRemoveSplitQuery:
    """Tests for azure.remove_split_query()."""

    def test_calls_azcopy_rm(self):
        """remove_split_query invokes azcopy rm for Azure results."""
        cfg = _make_cfg(dry_run=False)

        with patch('elastic_blast.azure.safe_exec') as mock_exec:
            remove_split_query(cfg)

        mock_exec.assert_called_once()
        cmd = mock_exec.call_args[0][0]
        cmd_str = ' '.join(str(arg) for arg in cmd) if isinstance(cmd, list) else str(cmd)
        assert 'azcopy' in cmd_str
        assert 'rm' in cmd_str

    def test_dry_run_does_not_execute(self):
        """In dry_run mode, remove_split_query does not call safe_exec."""
        cfg = _make_cfg(dry_run=True)

        with patch('elastic_blast.azure.safe_exec') as mock_exec:
            remove_split_query(cfg)

        mock_exec.assert_not_called()


# ---------------------------------------------------------------------------
# _decode helper tests
# ---------------------------------------------------------------------------

class TestDecode:
    """Tests for ElasticBlastAzure._decode() helper."""

    def test_bytes(self):
        from elastic_blast.azure import ElasticBlastAzure
        assert ElasticBlastAzure._decode(b'hello') == 'hello'

    def test_str(self):
        from elastic_blast.azure import ElasticBlastAzure
        assert ElasticBlastAzure._decode('hello') == 'hello'

    def test_none(self):
        from elastic_blast.azure import ElasticBlastAzure
        assert ElasticBlastAzure._decode(None) == ''

    def test_bytes_with_invalid_utf8(self):
        from elastic_blast.azure import ElasticBlastAzure
        result = ElasticBlastAzure._decode(b'\xff\xfe')
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# Cluster name validation tests
# ---------------------------------------------------------------------------

class TestClusterNameValidation:
    """Tests for AKS cluster name validation in azure_sdk."""

    def test_valid_name(self):
        from elastic_blast.azure_sdk import start_cluster
        start_cluster('rg', 'elb-test-01', location='eastus',
                      machine_type='Standard_D8s_v3', num_nodes=1, dry_run=True)

    def test_uppercase_rejected(self):
        from elastic_blast.azure_sdk import start_cluster
        with pytest.raises(ValueError, match='Invalid AKS cluster name'):
            start_cluster('rg', 'ELB-Test', location='eastus',
                          machine_type='Standard_D8s_v3', num_nodes=1, dry_run=True)

    def test_spaces_rejected(self):
        from elastic_blast.azure_sdk import start_cluster
        with pytest.raises(ValueError, match='Invalid AKS cluster name'):
            start_cluster('rg', 'elb test', location='eastus',
                          machine_type='Standard_D8s_v3', num_nodes=1, dry_run=True)


# ---------------------------------------------------------------------------
# check_prerequisites tests
# ---------------------------------------------------------------------------

class TestCheckPrerequisites:
    """Tests for azure.check_prerequisites()."""

    def test_succeeds_when_all_tools_present(self):
        with patch('elastic_blast.azure_sdk.check_prerequisites'):
            check_prerequisites()

    def test_raises_when_az_missing(self):
        with patch('elastic_blast.azure_sdk.check_prerequisites',
                   side_effect=UserReportError(1, 'Azure authentication failed')):
            with pytest.raises(UserReportError):
                check_prerequisites()

    def test_raises_when_kubectl_missing(self):
        with patch('elastic_blast.azure_sdk.check_prerequisites',
                   side_effect=UserReportError(1, "kubectl doesn't work")):
            with pytest.raises(UserReportError):
                check_prerequisites()

    def test_raises_when_azcopy_missing(self):
        with patch('elastic_blast.azure_sdk.check_prerequisites',
                   side_effect=UserReportError(1, 'azcopy is not installed')):
            with pytest.raises(UserReportError, match='azcopy'):
                check_prerequisites()


# ---------------------------------------------------------------------------
# Integration tests — require Azure credentials and may create resources
# ---------------------------------------------------------------------------

SKIP = not os.getenv('RUN_ALL_TESTS')


@pytest.fixture
def provide_cluster():
    """Create an AKS cluster before and delete it after a test."""
    args = Namespace(cfg=INI)
    cfg = ElasticBlastConfig(config.configure(args), task=ElbCommand.SUBMIT)
    cfg.cluster.name = cfg.cluster.name + f'-{os.environ["USER"]}' + '-02'
    yield cfg

    # teardown
    name = cfg.cluster.name
    try:
        if name in azure.get_aks_clusters(cfg):
            cmd = f'az aks delete --resource-group {cfg.azure.resourcegroup} --name {name} --yes'
            azure.safe_exec(cmd.split())
    except Exception:
        pass


@pytest.mark.skipif(SKIP, reason='Requires Azure credentials and may create AKS resources')
def test_get_aks_credentials_real(provide_cluster):
    """Test that azure.get_aks_credentials does not raise exceptions when
    a cluster is present."""
    cfg = provide_cluster
    azure.get_aks_credentials(cfg)


@pytest.mark.skipif(SKIP, reason='Requires Azure credentials and may create AKS resources')
def test_set_role_assignment_real(provide_cluster):
    """Test that azure.set_role_assignment does not raise exceptions when
    a cluster is present."""
    cfg = provide_cluster
    azure.set_role_assignment(cfg)


@pytest.mark.skipif(SKIP, reason='Requires Azure credentials and may create AKS resources')
def test_get_aks_credentials_no_cluster_real():
    """Test that SafeExecError is raised when getting credentials of a
    non-existent AKS cluster."""
    args = Namespace(cfg=INI)
    cfg = ElasticBlastConfig(config.configure(args), task=ElbCommand.SUBMIT)
    cfg.cluster.name = 'some-strange-cluster-name'
    assert cfg.cluster.name not in azure.get_aks_clusters(cfg)
    with pytest.raises(SafeExecError):
        azure.get_aks_credentials(cfg)