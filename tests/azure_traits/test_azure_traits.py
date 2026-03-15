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
Unit tests for elastic_blast.azure_traits

Author: Victor Joukov joukovv@ncbi.nlm.nih.gov
"""
from elastic_blast.azure_traits import get_machine_properties, get_instance_type_offerings
from elastic_blast.base import InstanceProperties
import os
import pytest

def test_ram():
    assert get_machine_properties('Standard_E32s_v3') == InstanceProperties(32, 256)

def test_l_series_storage_optimized():
    """L-series VMs for TB-scale BLAST DB with large NVMe."""
    assert get_machine_properties('Standard_L32s_v3') == InstanceProperties(32, 256)
    assert get_machine_properties('Standard_L64s_v3') == InstanceProperties(64, 512)
    assert get_machine_properties('Standard_L80s_v3') == InstanceProperties(80, 640)

def test_e_series_v5():
    """E-series v5 VMs with NVMe temp storage."""
    assert get_machine_properties('Standard_E32bs_v5') == InstanceProperties(32, 256)
    assert get_machine_properties('Standard_E64bs_v5') == InstanceProperties(64, 512)

def test_unsupported_instance_type_optimal():
    with pytest.raises(NotImplementedError):
        get_machine_properties('optimal')

def test_not_found():
    with pytest.raises(NotImplementedError):
        get_machine_properties('Standard_NONEXISTENT_32')
        
@pytest.mark.skipif(not os.getenv('RUN_ALL_TESTS'),
                    reason='Requires Azure CLI credentials (az vm list-sizes)')
def test_get_instance_type_offerings():
    result = get_instance_type_offerings('eastus')
    assert result != None
    assert len(result) > 0
    assert 'Standard_HB120rs_v3' in [item['name'] for item in result]
    assert 'Standard_HC44rs' in [item['name'] for item in result]
    assert 'Standard_HB60rs' in [item['name'] for item in result]
    assert 'Standard_D16s_v3' in [item['name'] for item in result]
    assert 'Standard_D32s_v3' in [item['name'] for item in result]
    assert 'Standard_D64s_v3' in [item['name'] for item in result]
    assert 'Standard_E16s_v3' in [item['name'] for item in result]
    assert 'Standard_E32s_v3' in [item['name'] for item in result]
    assert 'Standard_E64s_v3' in [item['name'] for item in result]
    assert 'Standard_E64is_v3' in [item['name'] for item in result]
