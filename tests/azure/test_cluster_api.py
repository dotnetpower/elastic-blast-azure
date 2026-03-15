#                           PUBLIC DOMAIN NOTICE
#              National Center for Biotechnology Information
#  
# This software is a "United States Government Work" under the
# terms of the United States Copyright Act.  It was written as part of
# the authors' official duties as United States Government employees and
# thus cannot be copyrighted.  This software is freely available
# to the public for use.  The National Library of Medicine and the U.S.
# Government have not placed any restriction on its use or reproduction.
#   
# Although all reasonable efforts have been taken to ensure the accuracy
# and reliability of the software and data, the NLM and the U.S.
# Government do not and cannot warrant the performance or results that
# may be obtained by using this software or data.  The NLM and the U.S.
# Government disclaim all warranties, express or implied, including
# warranties of performance, merchantability or fitness for any particular
# purpose.
#   
# Please cite NCBI in any work or product based on this material.

"""
Unit tests for Azure AKS cluster API functions (check_cluster, start_cluster,
delete_cluster, get_aks_clusters).

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

import json
import os
import time
from argparse import Namespace
from unittest.mock import patch, MagicMock

import pytest

from elastic_blast.azure import (
    check_cluster,
    start_cluster,
    delete_cluster,
    get_aks_clusters,
)
from elastic_blast import config
from elastic_blast.constants import ElbCommand, AKS_PROVISIONING_STATE
from elastic_blast.elb_config import ElasticBlastConfig
from elastic_blast.util import SafeExecError
from elastic_blast.db_metadata import DbMetadata

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
INI = os.path.join(TEST_DATA_DIR, 'test-cfg-file.ini')

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
# Mocked unit tests
# ---------------------------------------------------------------------------

class TestCheckCluster:
    """Tests for azure.check_cluster() — delegates to SDK."""

    def test_returns_succeeded_for_running_cluster(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.check_cluster', return_value='Succeeded'):
            assert check_cluster(cfg) == AKS_PROVISIONING_STATE.SUCCEEDED.value

    def test_returns_empty_for_no_cluster(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.check_cluster', return_value=''):
            assert check_cluster(cfg) == ''

    def test_returns_creating_for_provisioning_cluster(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.check_cluster', return_value='Creating'):
            assert check_cluster(cfg) == AKS_PROVISIONING_STATE.CREATING.value

    def test_dry_run_returns_empty(self):
        cfg = _make_cfg(dry_run=True)
        with patch('elastic_blast.azure_sdk.check_cluster', return_value=''):
            assert check_cluster(cfg) == ''

    def test_calls_az_aks_list_with_resource_group(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.check_cluster', return_value='Succeeded') as m:
            check_cluster(cfg)
        m.assert_called_once_with(cfg.azure.resourcegroup, cfg.cluster.name, cfg.cluster.dry_run)


class TestGetAksClusters:
    """Tests for azure.get_aks_clusters() — delegates to SDK."""

    def test_returns_cluster_names(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_aks_clusters', return_value=['cluster-1', 'cluster-2']):
            assert get_aks_clusters(cfg) == ['cluster-1', 'cluster-2']

    def test_returns_empty_list_when_no_clusters(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_aks_clusters', return_value=[]):
            assert get_aks_clusters(cfg) == []

    def test_raises_runtime_error_on_bad_json(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.get_aks_clusters', side_effect=RuntimeError('parse error')):
            with pytest.raises(RuntimeError):
                get_aks_clusters(cfg)


class TestDeleteCluster:
    """Tests for azure.delete_cluster() — delegates to SDK."""

    def test_calls_az_aks_delete(self):
        cfg = _make_cfg(dry_run=False)
        mock_poller = MagicMock()
        mock_poller.result.return_value = None
        with patch('elastic_blast.azure_sdk.delete_cluster', return_value=mock_poller):
            result = delete_cluster(cfg)
        assert result == cfg.cluster.name

    def test_dry_run_does_not_execute(self):
        cfg = _make_cfg(dry_run=True)
        with patch('elastic_blast.azure_sdk.delete_cluster', return_value=None):
            result = delete_cluster(cfg)
        assert result == cfg.cluster.name

    def test_propagates_safe_exec_error(self):
        cfg = _make_cfg(dry_run=False)
        with patch('elastic_blast.azure_sdk.delete_cluster',
                   side_effect=Exception('cluster not found')):
            with pytest.raises(Exception):
                delete_cluster(cfg)


# ---------------------------------------------------------------------------
# Integration tests — require Azure credentials and may create AKS resources
# ---------------------------------------------------------------------------

SKIP = not os.getenv('RUN_ALL_TESTS')


@pytest.fixture(scope="module")
def get_cluster_name():
    """Generate a unique cluster name for integration tests."""
    str_current_time = str(int(time.time()))
    uniq_cluster_name = "pytest-" + str_current_time
    if 'USER' in os.environ:
        uniq_cluster_name += "-" + os.environ['USER']
    else:
        uniq_cluster_name += "-" + str(os.getpid())
    return uniq_cluster_name


@pytest.mark.skipif(SKIP, reason='Requires Azure credentials and may create AKS resources')
@pytest.mark.skip(reason="Integration test — assumes test ordering, may leak resources")
def test_start_cluster_integration(get_cluster_name):
    """Integration test: create an AKS cluster."""
    cfg = _make_cfg(dry_run=False)
    cfg.cluster.name = get_cluster_name
    created_name = start_cluster(cfg)
    assert cfg.cluster.name == created_name


@pytest.mark.skipif(SKIP, reason='Requires Azure credentials and may create AKS resources')
@pytest.mark.skip(reason="Integration test — assumes test ordering, may leak resources")
def test_cluster_presence_integration(get_cluster_name):
    """Integration test: check AKS cluster status."""
    cfg = _make_cfg(dry_run=False)
    cfg.cluster.name = get_cluster_name
    status = check_cluster(cfg)
    assert status == AKS_PROVISIONING_STATE.SUCCEEDED


@pytest.mark.skipif(SKIP, reason='Requires Azure credentials and may create AKS resources')
@pytest.mark.skip(reason="Integration test — assumes test ordering, may leak resources")
def test_delete_cluster_integration(get_cluster_name):
    """Integration test: delete an AKS cluster."""
    cfg = _make_cfg(dry_run=False)
    cfg.cluster.name = get_cluster_name
    deleted_name = delete_cluster(cfg)
    assert deleted_name == cfg.cluster.name


@pytest.mark.skipif(SKIP, reason='Requires Azure credentials and may create AKS resources')
@pytest.mark.skip(reason="Integration test — assumes test ordering, may leak resources")
def test_cluster_deletion_integration(get_cluster_name):
    """Integration test: verify cluster is gone after deletion."""
    cfg = _make_cfg(dry_run=False)
    cfg.cluster.name = get_cluster_name
    status = check_cluster(cfg)
    assert status == ''
