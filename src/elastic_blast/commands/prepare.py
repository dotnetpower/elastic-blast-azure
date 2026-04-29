"""
elb/commands/prepare.py - Command to prepare AKS cluster with sharded DB and warmup

Creates the AKS cluster, downloads DB shards to local SSDs (or PVC),
and optionally warms the cache. Does NOT submit BLAST jobs.

Usage:
    elastic-blast prepare --cfg config.ini

Author: Moon Hyuk Choi
"""
import os
import logging
from elastic_blast.elasticblast_factory import ElasticBlastFactory
from elastic_blast.util import UserReportError, check_user_provided_blastdb_exists, ElbSupportedPrograms
from elastic_blast.constants import CSP, INPUT_ERROR, BLASTDB_ERROR, ElbCommand
from elastic_blast.elb_config import ElasticBlastConfig
from elastic_blast.resources.quotas.quota_check import check_resource_quotas


def prepare(args, cfg: ElasticBlastConfig, clean_up_stack):
    """Entry point to prepare an AKS cluster with DB shards (no BLAST execution)."""
    dry_run = cfg.cluster.dry_run
    cfg.validate(ElbCommand.PREPARE, dry_run)

    if cfg.cloud_provider.cloud != CSP.AZURE:
        raise UserReportError(returncode=INPUT_ERROR,
                              message='The prepare command is only supported on Azure.')

    from elastic_blast.azure_sdk import check_prerequisites as azure_check_prerequisites
    azure_check_prerequisites()

    if os.getenv('TEAMCITY_VERSION') is None:
        check_resource_quotas(cfg)

    # Verify BLAST database is accessible
    sas_token = cfg.azure.get_sas_token()
    try:
        check_user_provided_blastdb_exists(
            cfg.blast.db,
            ElbSupportedPrograms().get_db_mol_type(cfg.blast.program),
            cfg.cluster.db_source,
            gcp_prj=None,
            sas_token=sas_token
        )
    except ValueError as err:
        raise UserReportError(returncode=BLASTDB_ERROR, message=str(err))

    elastic_blast = ElasticBlastFactory(cfg, True, clean_up_stack)
    elastic_blast.prepare()

    logging.info('Cluster preparation complete. Use "elastic-blast submit" with reuse=true to run BLAST jobs.')
    return 0
