# Declare the License

"""
Help functions to access Azure resources and manipulate parameters and environment

Authors: Moon Hyuk Choi moonchoi@microsoft.com
"""

import os
from pathlib import Path
import shlex
from subprocess import check_call
import subprocess
from tempfile import TemporaryDirectory
import time
import logging
import json
import shutil
from timeit import default_timer as timer
from typing import Any, DefaultDict, Dict, Optional, List, Tuple
import uuid
from collections import defaultdict
from tenacity import retry, stop_after_attempt, wait_exponential

from .base import MemoryStr, QuerySplittingResults

from .subst import substitute_params

from .filehelper import open_for_write_immediate
from .jobs import read_job_template, write_job_files
from .util import ElbSupportedPrograms, safe_exec, UserReportError, SafeExecError, safe_exec_print
from .util import validate_gcp_disk_name, get_blastdb_info, get_usage_reporting
from .util import is_newer_version, handle_error

from . import kubernetes
from .constants import CLUSTER_ERROR, ELB_NUM_JOBS_SUBMITTED, ELB_METADATA_DIR, K8S_JOB_SUBMIT_JOBS
from .constants import ELB_STATE_DISK_ID_FILE, DEPENDENCY_ERROR
from .constants import ELB_QUERY_BATCH_DIR, ELB_DFLT_MIN_NUM_NODES
from .constants import K8S_JOB_CLOUD_SPLIT_SSD, K8S_JOB_INIT_PV
from .constants import K8S_JOB_BLAST, K8S_JOB_GET_BLASTDB, K8S_JOB_IMPORT_QUERY_BATCHES
from .constants import K8S_JOB_LOAD_BLASTDB_INTO_RAM, K8S_JOB_RESULTS_EXPORT, K8S_UNINITIALIZED_CONTEXT
from .constants import ELB_DOCKER_IMAGE_AZURE, ELB_QUERY_LENGTH, INPUT_ERROR
from .constants import ElbExecutionMode, ElbStatus
from .constants import AKS_PROVISIONING_STATE

from .constants import ELB_DFLT_AKS_ACR_NAME, ELB_DFLT_AKS_ACR_RESOURCE_GROUP
from .constants import STATUS_MESSAGE_ERROR

from .constants import ELB_DFLT_BLAST_JOB_AKS_TEMPLATE, ELB_LOCAL_SSD_BLAST_JOB_AKS_TEMPLATE
from .elb_config import ElasticBlastConfig, ResourceIds
from .elasticblast import ElasticBlast
from .gcp_traits import enable_gcp_api
from . import VERSION

class ElasticBlastAzure(ElasticBlast):
    """ Implementation of core ElasticBLAST functionality in Azure. """
    def __init__(self, cfg: ElasticBlastConfig, create=False, cleanup_stack: Optional[List[Any]]=None):
        super().__init__(cfg, create, cleanup_stack)
        self.query_files: List[str] = []
        self.cluster_initialized = False
        self.apis_enabled = False
        self.auto_shutdown = not 'ELB_DISABLE_AUTO_SHUTDOWN' in os.environ

    def cloud_query_split(self, query_files: List[str]) -> None:
        """ Submit the query sequences for splitting to the cloud.
            Initialize cluster with cloud split job
            Parameters:
                query_files     - list files containing query sequence data to split
        """
        if self.dry_run:
            return
        self.query_files = query_files
        logging.debug("Initialize cluster with cloud split")
        self._initialize_cluster()
        self.cluster_initialized = True

    def wait_for_cloud_query_split(self) -> None:
        """ Wait for cloud split """
        if not self.query_files:
            # This is QuerySplitMode.CLIENT - no need to wait
            return
        k8s_ctx = self._get_aks_credentials()
        kubectl = f'kubectl --context={k8s_ctx}'
        job_to_wait = K8S_JOB_CLOUD_SPLIT_SSD if self.cfg.cluster.use_local_ssd else K8S_JOB_INIT_PV

        while True:
            cmd = f"{kubectl} get job {job_to_wait} -o jsonpath=" "'{.items[?(@.status.active)].metadata.name}'"
            if self.dry_run:
                logging.debug(cmd)
                return
            else:
                logging.debug(f'Waiting for job {job_to_wait}')
                proc = safe_exec(cmd)
                res = handle_error(proc.stdout)
            if not res:
                # Job's not active, check it did not fail
                cmd = f"{kubectl} get job {job_to_wait} -o jsonpath=" "'{.items[?(@.status.failed)].metadata.name}'"
                proc = safe_exec(cmd)
                res = handle_error(proc.stdout)
                if res:
                    if job_to_wait == K8S_JOB_INIT_PV:
                        # Assume BLASTDB error, as it is more likely to occur than copying files to PV when importing queries
                        msg = 'BLASTDB initialization failed, please run '
                        msg += f'"elastic-blast status --gcp-project {self.cfg.gcp.project} '
                        msg += f'--gcp-region {self.cfg.gcp.region} --gcp-zone '
                        msg += f'{self.cfg.gcp.zone} --results {self.cfg.cluster.name}" '
                        msg += 'for further details'
                    else:
                        msg = 'Cloud query splitting or upload of its results from SSD failed'
                    raise UserReportError(returncode=CLUSTER_ERROR, message=msg)
                else:
                    return
            time.sleep(30)

    def upload_query_length(self, query_length: int) -> None:
        """ Save query length in a metadata file in GS """
        if query_length <= 0: return
        fname = os.path.join(self.cfg.cluster.results, self.cfg.azure.elb_job_id, ELB_METADATA_DIR, ELB_QUERY_LENGTH)
        print(f'\033[33m[2/5] Upload query length file: {fname}\033[0m')
        sas_token = self.cfg.azure.get_sas_token()
        with open_for_write_immediate(fname, sas_token=sas_token) as f:
            f.write(str(query_length))
        # Note: if cloud split is used this file is uploaded
        # by the run script in the 1st stage

    def _check_job_number_limit(self, queries: Optional[List[str]], query_length) -> None:
        """ Check that resulting number of jobs does not exceed Kubernetes limit """
        if not queries:
            # Nothing to check, the job number is still unknown
            return
        k8s_job_limit = kubernetes.get_maximum_number_of_allowed_k8s_jobs(self.dry_run)
        if len(queries) > k8s_job_limit:
            batch_len = self.cfg.blast.batch_len
            suggested_batch_len = int(query_length / k8s_job_limit) + 1
            msg = 'Your ElasticBLAST search has failed and its computing resources will be deleted.\n' \
                  f'The batch size specified ({batch_len}) led to creating {len(queries)} kubernetes jobs, which exceeds the limit on number of jobs ({k8s_job_limit}).' \
                  f' Please increase the batch-len parameter to at least {suggested_batch_len} and repeat the search.'
            raise UserReportError(INPUT_ERROR, msg)

    def _submit_partitioned(self, query_batches: List[str], query_length) -> None:
        """DB-partitioned mode: search each partition independently against all queries.
        
        Architecture:
            P partitions × N query batches = P×N total batch jobs
            Each partition's jobs produce results in a per-partition subdirectory.
            Results are organized as: {results}/part_00/, {results}/part_01/, etc.
        """
        cfg = self.cfg
        num_partitions = cfg.blast.db_partitions

        if not self.cluster_initialized:
            # Check total job count: partitions × queries
            if query_batches:
                total_jobs = len(query_batches) * num_partitions
                k8s_job_limit = kubernetes.get_maximum_number_of_allowed_k8s_jobs(self.dry_run)
                if total_jobs > k8s_job_limit:
                    msg = f'DB-partitioned search would create {total_jobs} jobs ' \
                          f'({num_partitions} partitions × {len(query_batches)} queries), ' \
                          f'exceeding the limit of {k8s_job_limit}. ' \
                          f'Reduce db-partitions or increase batch-len.'
                    raise UserReportError(INPUT_ERROR, msg)

            self.query_files = []
            logging.debug(f"Initialize cluster for partitioned DB search ({num_partitions} partitions)")
            self._initialize_cluster_partitioned(query_batches)
            self.cluster_initialized = True

        self._generate_partitioned_jobs(query_batches)
        self.cleanup_stack.clear()
        self.cleanup_stack.append(lambda: kubernetes.collect_k8s_logs(cfg))

    def _initialize_cluster_partitioned(self, queries: Optional[List[str]]) -> None:
        """Initialize cluster for partitioned DB search.
        Creates AKS cluster and downloads all DB partitions to PVC."""
        cfg, clean_up_stack = self.cfg, self.cleanup_stack

        print(f'\033[33m[3/5] Initialize cluster (partitioned DB: {cfg.blast.db_partitions} partitions)\033[0m')

        logging.info('Starting cluster for partitioned DB search')
        clean_up_stack.append(lambda: logging.debug('Before creating cluster'))
        if not cfg.cluster.reuse:
            clean_up_stack.append(lambda: delete_cluster_with_cleanup(cfg))
        clean_up_stack.append(lambda: kubernetes.collect_k8s_logs(cfg))

        aks_status = check_cluster(cfg)
        if not cfg.cluster.reuse or aks_status == '':
            start_cluster(cfg)
        clean_up_stack.append(lambda: logging.debug('After creating cluster'))

        self._get_aks_credentials()
        self._label_nodes()

        if not cfg.cluster.reuse or aks_status == '':
            set_role_assignment(cfg)

        if self.cloud_job_submission or self.auto_shutdown:
            kubernetes.enable_service_account(cfg)

        print(f'\033[33m[4/5] Initializing partitioned storage ({cfg.blast.db_partitions} partitions)\033[0m')
        logging.info(f'Initializing partitioned storage: {cfg.blast.db_partitions} partitions')

        kubernetes.initialize_storage_partitioned(
            cfg, self.query_files,
            ElbExecutionMode.NOWAIT if self.cloud_job_submission else ElbExecutionMode.WAIT
        )

        print(f'\033[33m[5/5] Done (partitioned init)\033[0m')

    def _generate_partitioned_jobs(self, query_batches: List[str]) -> None:
        """Generate and submit BLAST batch jobs for each DB partition.
        
        For each partition i (0..P-1), generates N batch jobs where
        each job searches one query batch against partition i.
        Results for partition i go to: {results}/part_{i:02d}/
        """
        cfg = self.cfg
        num_partitions = cfg.blast.db_partitions
        partition_prefix = cfg.blast.db_partition_prefix

        base_results = os.path.join(cfg.cluster.results, cfg.azure.elb_job_id)
        sas_token = cfg.azure.get_sas_token()

        all_job_files = []
        with TemporaryDirectory() as job_path:
            for part_idx in range(num_partitions):
                # Derive partition DB name from prefix: e.g., "swissprot_part_00"
                partition_db_name = os.path.basename(f'{partition_prefix}{part_idx:02d}')
                # DB path on PVC: part_XX/db_name
                partition_db_on_pvc = f'part_{part_idx:02d}/{partition_db_name}'

                subs = self._job_substitutions_for_partition(
                    query_batches, part_idx, partition_db_on_pvc, partition_db_name
                )
                job_template_text = read_job_template(cfg=cfg)

                job_files = write_job_files(
                    job_path, f'part{part_idx:02d}_batch_',
                    job_template_text, query_batches, **subs
                )
                all_job_files.extend(job_files)

            total_jobs = len(all_job_files)
            logging.info(f'Submitting {total_jobs} partitioned jobs '
                         f'({num_partitions} partitions × {len(query_batches)} queries)')

            assert cfg.appstate.k8s_ctx
            start = timer()
            job_names = kubernetes.submit_jobs(cfg.appstate.k8s_ctx, Path(job_path), dry_run=self.dry_run)
            end = timer()
            logging.debug(f'RUNTIME submit-partitioned-jobs {end-start} seconds')

        # Signal total number of jobs submitted
        with open_for_write_immediate(os.path.join(base_results, ELB_METADATA_DIR,
                                                    ELB_NUM_JOBS_SUBMITTED), sas_token=sas_token) as f:
            f.write(str(total_jobs))

    def _job_substitutions_for_partition(self, query_batches: List[str],
                                         partition_idx: int, partition_db: str,
                                         partition_db_name: str) -> Dict[str, str]:
        """Prepare substitution dictionary for a specific partition's batch jobs.
        
        Args:
            query_batches: list of query batch file paths
            partition_idx: 0-based partition index
            partition_db: DB path on PVC (e.g., "part_00/swissprot_part_00")
            partition_db_name: DB name for labeling (e.g., "swissprot_part_00")
        """
        cfg = self.cfg
        usage_reporting = get_usage_reporting()
        blast_program = cfg.blast.program

        # CPU allocation
        if len(query_batches) == cfg.cluster.num_nodes:
            num_cpu_req = cfg.cluster.num_cpus - 2
        else:
            num_cpu_req = ((cfg.cluster.num_nodes * cfg.cluster.num_cpus) // 4) - 2

        # Results go to partition-specific subdirectory
        base_results = os.path.join(cfg.cluster.results, cfg.azure.elb_job_id)
        partition_results = os.path.join(base_results, f'part_{partition_idx:02d}')

        subs = {
            'ELB_BLAST_PROGRAM': blast_program,
            'ELB_DB': partition_db,
            'ELB_DB_LABEL': f'part{partition_idx:02d}',
            'ELB_MEM_REQUEST': str(cfg.cluster.mem_request),
            'ELB_MEM_LIMIT': str(cfg.cluster.mem_limit),
            'ELB_BLAST_OPTIONS': cfg.blast.options,
            'ELB_BLAST_TIMEOUT': str(cfg.timeouts.blast_k8s * 60),
            'ELB_RESULTS': partition_results,
            'ELB_NUM_CPUS_REQ': str(num_cpu_req),
            'ELB_NUM_CPUS': str(cfg.cluster.num_cpus),
            'ELB_DB_MOL_TYPE': str(ElbSupportedPrograms().get_db_mol_type(blast_program)),
            'ELB_DOCKER_IMAGE': cfg.azure.elb_docker_image,
            'ELB_TIMEFMT': '%s%N',
            'BLAST_ELB_JOB_ID': cfg.azure.elb_job_id,
            'BLAST_ELB_VERSION': VERSION,
            'BLAST_USAGE_REPORT': str(usage_reporting).lower(),
            'K8S_JOB_GET_BLASTDB': K8S_JOB_GET_BLASTDB,
            'K8S_JOB_LOAD_BLASTDB_INTO_RAM': K8S_JOB_LOAD_BLASTDB_INTO_RAM,
            'K8S_JOB_IMPORT_QUERY_BATCHES': K8S_JOB_IMPORT_QUERY_BATCHES,
            'K8S_JOB_SUBMIT_JOBS': K8S_JOB_SUBMIT_JOBS,
            'K8S_JOB_BLAST': K8S_JOB_BLAST,
            'K8S_JOB_RESULTS_EXPORT': K8S_JOB_RESULTS_EXPORT,
            'ELB_AZURE_RESOURCE_GROUP': cfg.azure.resourcegroup,
            'ELB_METADATA_DIR': ELB_METADATA_DIR,
        }
        return subs

    def submit(self, query_batches: List[str], query_length, one_stage_cloud_query_split: bool) -> None:
        """ Submit query batches to cluster
            Parameters:
                query_batches               - list of bucket names of queries to submit
                query_length                - total query length
                one_stage_cloud_query_split - do the query split in the cloud as a part
                                              of executing a regular job """
        # Can't use one stage cloud split for GCP, should never happen
        assert(not one_stage_cloud_query_split)

        # DB-partitioned mode: each partition searched independently against all queries
        if self.cfg.blast.db_partitions > 0:
            return self._submit_partitioned(query_batches, query_length)

        if not self.cluster_initialized:
            self._check_job_number_limit(query_batches, query_length)
            self.query_files = []  # No cloud split
            logging.debug("Initialize cluster with NO cloud split")
            self._initialize_cluster(query_batches)
            self.cluster_initialized = True
        if self.cloud_job_submission:
            kubernetes.submit_job_submission_job(self.cfg)
        else:
            self._generate_and_submit_jobs(query_batches)
            if self.cfg.cluster.num_nodes != 1:
                logging.info('Enable autoscaling')
                cmd = f'gcloud container clusters update {self.cfg.cluster.name} --enable-autoscaling --node-pool default-pool --min-nodes 0 --max-nodes {self.cfg.cluster.num_nodes} --project {self.cfg.gcp.project} --zone {self.cfg.gcp.zone}'
                if self.dry_run:
                    logging.info(cmd)
                else:
                    safe_exec(cmd)
                logging.info('Done enabling autoscaling')

            if not self.cfg.cluster.use_local_ssd:
                if not self.cfg.appstate.k8s_ctx:
                    raise RuntimeError('K8s context not set')
                kubernetes.wait_for_pvc(self.cfg.appstate.k8s_ctx, 'blast-dbs-pvc')
                # save persistent disk id
                disk_ids = kubernetes.get_persistent_disks(self.cfg.appstate.k8s_ctx)
                logging.debug(f'New persistent disk id: {disk_ids}')
                self.cfg.appstate.resources.disks += disk_ids
                dest = os.path.join(self.cfg.cluster.results, self.cfg.azure.elb_job_id, ELB_METADATA_DIR,
                                    ELB_STATE_DISK_ID_FILE)
                sas_token = self.cfg.azure.get_sas_token()
                with open_for_write_immediate(dest) as f:
                    f.write(self.cfg.appstate.resources.to_json(), sas_token=sas_token)

                kubernetes.label_persistent_disk(self.cfg, 'blast-dbs-pvc')
                kubernetes.delete_volume_snapshots(self.cfg.appstate.k8s_ctx)

        self.cleanup_stack.clear()
        self.cleanup_stack.append(lambda: kubernetes.collect_k8s_logs(self.cfg))
    
    def run_command(self, cmd: str) -> str:
        """ Run a command in the context of the cluster """
        k8s_ctx = self._get_aks_credentials()
        context = f'--context={k8s_ctx}'
        cmd = f'{cmd} {context}'
        if self.dry_run:
            logging.info(cmd)
        else:
            proc = safe_exec(shlex.split(cmd))
            return handle_error(proc.stdout)

    def check_status(self, extended=False) -> Tuple[ElbStatus, Dict[str, int], Dict[str, str]]:
        """ Check execution status of ElasticBLAST search
        Parameters:
            extended - do we need verbose information about jobs
        Returns:
            tuple of
                status - cluster status, ElbStatus
                counts - job counts for all job states
                verbose_result - a dictionary with enrties: label, detailed info about jobs
        """
        try:
            return self._check_status(extended)
        except SafeExecError as err:
            # cluster is not valid, return empty result
            msg = err.message.strip()
            logging.info(msg)
            return ElbStatus.UNKNOWN, defaultdict(int), {STATUS_MESSAGE_ERROR: msg} if msg else {}

    def _check_status(self, extended=False) -> Tuple[ElbStatus, Dict[str, int], Dict[str, str]]:
        # We cache only status from gone cluster - it can't change anymore
        if self.cached_status:
            return self.cached_status, self.cached_counts, {STATUS_MESSAGE_ERROR: self.cached_failure_message} if self.cached_failure_message else {}
        counts: DefaultDict[str, int] = defaultdict(int)
        # self._enable_gcp_apis()
        status = self._status_from_results()
        if status != ElbStatus.UNKNOWN:
            return status, self.cached_counts, {STATUS_MESSAGE_ERROR: self.cached_failure_message} if self.cached_failure_message else {}

        aks_status : str = check_cluster(self.cfg)
        if not aks_status:
            return ElbStatus.UNKNOWN, {}, {STATUS_MESSAGE_ERROR: f'Cluster "{self.cfg.cluster.name}" was not found'}

        logging.debug(f'AKS status: {aks_status}')
        
        if aks_status in [AKS_PROVISIONING_STATE.UPDATING, AKS_PROVISIONING_STATE.CREATING, AKS_PROVISIONING_STATE.STARTING]:
            return ElbStatus.SUBMITTING, {}, {}

        if aks_status != AKS_PROVISIONING_STATE.SUCCEEDED:
            # TODO: This behavior is consistent with current tests, consider returning a value
            # as follows, and changing test in tests/app/test_elasticblast.py::test_cluster_error
            # return ElbStatus.DELETING, {}, ''
            raise UserReportError(returncode=CLUSTER_ERROR,
                            message=f'Cluster "{self.cfg.cluster.name}" exists, but is not responding. '
                                'It may be still initializing, please try checking status again in a few minutes.')

        k8s_ctx = self._get_aks_credentials()
        selector = 'app=blast'
        kubectl = f'kubectl --context={k8s_ctx}'

        # if we need name of the job in the future add NAME:.metadata.name to custom-columns
        # get status of jobs (pending/running, succeeded, failed)
        cmd = f'{kubectl} get jobs -o custom-columns=STATUS:.status.conditions[0].type -l {selector}'.split()
        if self.dry_run:
            logging.debug(cmd)
        else:
            proc = safe_exec(cmd)
            for line in handle_error(proc.stdout).split('\n'):
                if not line or line.startswith('STATUS'):
                    continue
                if line.startswith('Complete'):
                    counts['succeeded'] += 1
                elif line.startswith('Failed'):
                    counts['failed'] += 1
                else:
                    counts['pending'] += 1
                
        # get number of running pods
        cmd = f'{kubectl} get pods -o custom-columns=STATUS:.status.phase -l {selector}'.split()
        if self.dry_run:
            logging.info(cmd)
        else:
            proc = safe_exec(cmd)
            for line in handle_error(proc.stdout).split('\n'):
                if line == 'Running':
                    counts['running'] += 1

        # correct number of pending jobs: running jobs were counted twice,
        # as running and pending
        counts['pending'] -= counts['running']
        status = ElbStatus.UNKNOWN
        if counts['failed'] > 0:
            status = ElbStatus.FAILURE
        elif counts['running'] > 0 or counts['pending'] > 0:
            status = ElbStatus.RUNNING
        elif counts['succeeded']:
            status = ElbStatus.SUCCESS
        else:
            # check init-pv and submit-jobs status
            status = ElbStatus.SUBMITTING
            pending, succeeded, failed = self._job_status_by_app('setup')
            if failed > 0:
                status = ElbStatus.FAILURE
            elif pending == 0:
                pending, succeeded, failed = self._job_status_by_app('submit')
                if failed > 0:
                    status = ElbStatus.FAILURE

        return status, counts, {}
    
    def _job_status_by_app(self, app):
        """ get status of jobs (pending/running, succeeded, failed) by app """
        pending = 0
        succeeded = 0
        failed = 0
        selector = f'app={app}'
        k8s_ctx = self._get_aks_credentials()
        kubectl = f'kubectl --context={k8s_ctx}'
        cmd = f'{kubectl} get jobs -o custom-columns=STATUS:.status.conditions[0].type -l {selector}'.split()
        if self.dry_run:
            logging.debug(cmd)
        else:
            try:
                proc = safe_exec(cmd)
            except SafeExecError as err:
                logging.debug(f'Error "{err.message}" in command "{cmd}"')
                return 0, 0, 0
            for line in handle_error(proc.stdout).split('\n'):
                if not line or line.startswith('STATUS'):
                    continue
                if line.startswith('Complete'):
                    succeeded += 1
                elif line.startswith('Failed'):
                    failed += 1
                else:
                    pending += 1
        return pending, succeeded, failed


    def delete(self):
        """Delete cluster and associated resources.
        In reuse mode, only clean up jobs/data, preserving the cluster and PVCs."""
        if self.cfg.cluster.reuse:
            logging.info('Reuse mode: preserving cluster and PVCs, cleaning up jobs only')
            self._cleanup_jobs_only()
            return
        delete_cluster_with_cleanup(self.cfg)

    def _cleanup_jobs_only(self) -> None:
        """In reuse mode: delete completed/failed BLAST jobs but preserve cluster, PVCs, and DB.
        This allows the next search to skip DB initialization entirely."""
        cfg = self.cfg
        dry_run = cfg.cluster.dry_run
        try:
            k8s_ctx = self._get_aks_credentials()
            kubectl = f'kubectl --context={k8s_ctx}'
            # Delete BLAST batch jobs only (app=blast), preserve init-pv PVC/data
            cmd = f'{kubectl} delete jobs -l app=blast --ignore-not-found=true'
            if dry_run:
                logging.info(cmd)
            else:
                safe_exec(shlex.split(cmd))
            # Delete submit-jobs pod
            cmd = f'{kubectl} delete jobs -l app=submit --ignore-not-found=true'
            if dry_run:
                logging.info(cmd)
            else:
                safe_exec(shlex.split(cmd))
            logging.info('Reuse mode cleanup complete: jobs deleted, cluster and PVCs preserved')
        except Exception as e:
            logging.warning(f'Cleanup in reuse mode failed: {e}')

    def _db_already_loaded(self) -> bool:
        """Check if the BLAST DB is already loaded on the PV/NVMe in the cluster.
        Returns True if init-pv has previously completed successfully and PVC exists."""
        cfg = self.cfg
        if cfg.cluster.dry_run:
            return False
        try:
            k8s_ctx = self._get_aks_credentials()
            kubectl = f'kubectl --context={k8s_ctx}'
            # Check if PVC exists and is bound
            if cfg.cluster.use_local_ssd:
                # For local SSD mode, we can't easily verify DB presence
                # without running a pod; conservatively return False
                return False
            cmd = f'{kubectl} get pvc blast-dbs-pvc-rwm -o jsonpath={{.status.phase}}'
            proc = safe_exec(shlex.split(cmd))
            pvc_status = proc.stdout.strip() if proc.stdout else ''
            if pvc_status != 'Bound':
                logging.debug(f'PVC not bound (status={pvc_status}), DB not loaded')
                return False
            # Check if init-pv job completed successfully
            cmd = f'{kubectl} get job init-pv -o jsonpath={{.status.succeeded}}'
            proc = safe_exec(shlex.split(cmd))
            succeeded = proc.stdout.strip() if proc.stdout else ''
            if succeeded == '1':
                logging.info('Reuse mode: DB already loaded on PV (init-pv succeeded, PVC bound)')
                return True
            logging.debug(f'init-pv job succeeded count: {succeeded}')
            return False
        except Exception as e:
            logging.debug(f'DB existence check failed: {e}')
            return False

    def _upload_queries_only(self, queries: Optional[List[str]]) -> None:
        """In reuse mode with DB already loaded: only upload new query batches to PV.
        Skip DB download (init-pv) entirely."""
        cfg = self.cfg
        k8s_ctx = self._get_aks_credentials()
        sas_token = cfg.azure.get_sas_token()

        if self.cloud_job_submission:
            # Upload the job template and batch list for submit-jobs Pod
            subs = self.job_substitutions(queries)
            template_name = ELB_LOCAL_SSD_BLAST_JOB_AKS_TEMPLATE if cfg.cluster.use_local_ssd else ELB_DFLT_BLAST_JOB_AKS_TEMPLATE
            job_template = read_job_template(template_name=template_name, cfg=cfg)
            s = substitute_params(job_template, subs)
            bucket_job_template = os.path.join(cfg.cluster.results, cfg.azure.elb_job_id,
                                               ELB_METADATA_DIR, 'job.yaml.template')
            with open_for_write_immediate(bucket_job_template, sas_token=sas_token) as f:
                f.write(s)

        # Import query batches to existing PV (skip DB download)
        logging.info('Reuse mode: uploading query batches only (DB already loaded)')
        kubernetes.import_query_batches_only(cfg)

    def scale_nodes(self, node_count: int) -> None:
        """Scale AKS node pool to specified count.
        Use 0 to scale down (cost savings), >0 to scale up.
        In scale-down: PVCs are preserved, DB will need re-caching on scale-up."""
        cfg = self.cfg
        rg = cfg.azure.resourcegroup
        name = cfg.cluster.name
        dry_run = cfg.cluster.dry_run
        cmd = f'az aks nodepool scale --resource-group {rg} --cluster-name {name} ' \
              f'--name nodepool1 --node-count {node_count} --no-wait'
        if dry_run:
            logging.info(cmd)
        else:
            logging.info(f'Scaling AKS node pool to {node_count} nodes')
            safe_exec(shlex.split(cmd))

    def _initialize_cluster(self, queries: Optional[List[str]]):
        """ Creates a k8s cluster, connects to it and initializes the persistent disk.
        In reuse mode with DB already loaded, skips DB initialization and only uploads queries. """
        cfg, query_files, clean_up_stack = self.cfg, self.query_files, self.cleanup_stack
        pd_size = MemoryStr(cfg.cluster.pd_size).asGB()
        
        print(f'\033[33m[3/5] Initialize cluster\033[0m')
        
        # Check for warm cluster reuse: cluster exists + DB already on PV
        if cfg.cluster.reuse:
            aks_status = check_cluster(cfg)
            if aks_status == AKS_PROVISIONING_STATE.SUCCEEDED and self._db_already_loaded():
                print(f'\033[32m[3/5] Reuse mode: cluster running, DB already loaded — skipping init\033[0m')
                logging.info('Warm cluster reuse: skipping cluster creation and DB initialization')
                self._upload_queries_only(queries)
                clean_up_stack.append(lambda: kubernetes.collect_k8s_logs(cfg))
                return
        
        # TODO: Azure disk quota check not yet implemented
        logging.info('Starting cluster')
        clean_up_stack.append(lambda: logging.debug('Before creating cluster'))
        if not cfg.cluster.reuse:
            clean_up_stack.append(lambda: delete_cluster_with_cleanup(cfg))
        clean_up_stack.append(lambda: kubernetes.collect_k8s_logs(cfg))
        if self.cloud_job_submission:
            subs = self.job_substitutions(queries)            
            
            template_name = ELB_LOCAL_SSD_BLAST_JOB_AKS_TEMPLATE if cfg.cluster.use_local_ssd else ELB_DFLT_BLAST_JOB_AKS_TEMPLATE
            job_template = read_job_template(template_name=template_name, cfg=cfg)
            s = substitute_params(job_template, subs)
            bucket_job_template = os.path.join(cfg.cluster.results, self.cfg.azure.elb_job_id, ELB_METADATA_DIR, 'job.yaml.template')
            sas_token = self.cfg.azure.get_sas_token()
            with open_for_write_immediate(bucket_job_template, sas_token=sas_token) as f:
                f.write(s)
        aks_status = check_cluster(cfg)
        if not cfg.cluster.reuse or aks_status == '':
            start_cluster(cfg)
        clean_up_stack.append(lambda: logging.debug('After creating cluster'))

        self._get_aks_credentials()

        self._label_nodes()

        if not cfg.cluster.reuse or aks_status == '':
            set_role_assignment(cfg)

        if self.cloud_job_submission or self.auto_shutdown:
            kubernetes.enable_service_account(cfg)

        print(f'\033[33m[4/5] Initializing storage\033[0m')
        logging.info('Initializing storage')
        clean_up_stack.append(lambda: logging.debug('Before initializing storage'))
        
        kubernetes.initialize_storage(cfg, query_files,
            ElbExecutionMode.NOWAIT if self.cloud_job_submission else ElbExecutionMode.WAIT)
        clean_up_stack.append(lambda: logging.debug('After initializing storage'))

        # Deploy vmtouch DaemonSet in reuse mode to keep DB in RAM across searches
        if cfg.cluster.reuse and not cfg.cluster.use_local_ssd:
            self._deploy_vmtouch_daemonset()

        if not self.auto_shutdown:
            logging.debug('Disabling janitor')
        else:
            # TODO: need to implement submit_janitor_cronjob
            print(f'\033[33m[5/5] Done\033[0m')
            pass
            # kubernetes.submit_janitor_cronjob(cfg)

    def _deploy_vmtouch_daemonset(self) -> None:
        """Deploy vmtouch DaemonSet to keep BLAST DB cached in RAM.
        Uses 80% of available RAM instead of hardcoded 5GB."""
        cfg = self.cfg
        k8s_ctx = self._get_aks_credentials()
        kubectl = f'kubectl --context={k8s_ctx}'
        dry_run = cfg.cluster.dry_run

        # Check if already deployed
        cmd = f'{kubectl} get daemonset vmtouch-db-cache --ignore-not-found -o name'
        if not dry_run:
            try:
                proc = safe_exec(shlex.split(cmd))
                if proc.stdout and proc.stdout.strip():
                    logging.info('vmtouch DaemonSet already deployed, skipping')
                    return
            except Exception:
                pass

        sas_token = cfg.azure.get_sas_token()
        db, _, _ = get_blastdb_info(cfg.blast.db, None, sas_token=sas_token)
        program = cfg.blast.program

        subs = {
            'ELB_DOCKER_IMAGE': cfg.azure.elb_docker_image,
            'ELB_DB': db,
            'ELB_DB_MOL_TYPE': str(ElbSupportedPrograms().get_db_mol_type(program)),
        }

        from importlib.resources import files as pkg_files
        ref = pkg_files('elastic_blast').joinpath('templates/vmtouch-daemonset-aks.yaml.template')
        template_text = ref.read_text()
        daemonset_yaml = substitute_params(template_text, subs)

        with TemporaryDirectory() as d:
            ds_file = os.path.join(d, 'vmtouch-daemonset.yaml')
            with open(ds_file, 'w') as f:
                f.write(daemonset_yaml)
            cmd = f'{kubectl} apply -f {ds_file}'
            if dry_run:
                logging.info(cmd)
            else:
                safe_exec(shlex.split(cmd))
                logging.info('Deployed vmtouch DaemonSet for DB RAM caching')

    def _label_nodes(self):
        """ Label nodes by ordinal numbers for proper initialization.

            When we use local SSD the storage of each node should be
            initialized individually (as opposed to the case of persistent
            volumes). For this we create number of jobs and assign every init-ssd
            job to corresponding node using affinity label of form ordinal:{number}.
            See src/elastic_blast/templates/job-init-local-ssd.yaml.template
        """
        use_local_ssd = self.cfg.cluster.use_local_ssd
        dry_run = self.cfg.cluster.dry_run
        k8s_ctx = self._get_aks_credentials()
        kubectl = f'kubectl --context={k8s_ctx}'
        if use_local_ssd:
            # Label nodes in the cluster for affinity
            # cmd = kubectl + " get nodes -o jsonpath={.items[*]['metadata.name']}"
            cmd = kubectl + " get nodes -o jsonpath='{.items[*].metadata.name}'"
            if dry_run:
                logging.info(cmd)
                res = ' '.join([f'gke-node-{i}' for i in range(self.cfg.cluster.num_nodes)])
            else:
                proc = safe_exec(cmd)
                res = handle_error(proc.stdout)
            for i, name in enumerate(res.split()):
                name = name.replace("'","")
                cmd = f'{kubectl} label nodes {name} ordinal={i} --overwrite'
                if dry_run:
                    logging.info(cmd)
                else:
                    safe_exec(cmd)
    
    def job_substitutions(self, query_batches) -> Dict[str, str]:
        """ Prepare substitution dictionary for job generation """
        cfg = self.cfg
        usage_reporting = get_usage_reporting()
        sas_token = cfg.azure.get_sas_token()
        db, _, db_label = get_blastdb_info(cfg.blast.db,
                                           None, sas_token=sas_token)

        blast_program = cfg.blast.program
        
        # get optimized cpu
        if len(query_batches) == self.cfg.cluster.num_nodes:
            num_cpu_req = self.cfg.cluster.num_cpus - 2
        else:
            num_cpu_req = ((self.cfg.cluster.num_nodes * self.cfg.cluster.num_cpus) // 4) - 2

        # prepare substitution for current template
        # TODO consider template using cfg variables directly as, e.g. ${blast.program}
        subs = {
            'ELB_BLAST_PROGRAM': blast_program,
            'ELB_DB': db,
            'ELB_DB_LABEL': db_label,
            'ELB_MEM_REQUEST': str(cfg.cluster.mem_request),
            'ELB_MEM_LIMIT': str(cfg.cluster.mem_limit),
            'ELB_BLAST_OPTIONS': cfg.blast.options,
            # FIXME: EB-210
            'ELB_BLAST_TIMEOUT': str(cfg.timeouts.blast_k8s * 60),
            'ELB_RESULTS': os.path.join(cfg.cluster.results, cfg.azure.elb_job_id),
            # 'ELB_NUM_CPUS_REQ': str(cfg.cluster.num_cpus // 4), 
            'ELB_NUM_CPUS_REQ': str(num_cpu_req),
            'ELB_NUM_CPUS': str(cfg.cluster.num_cpus),
            'ELB_DB_MOL_TYPE': str(ElbSupportedPrograms().get_db_mol_type(blast_program)),
            'ELB_DOCKER_IMAGE': cfg.azure.elb_docker_image,
            'ELB_TIMEFMT': '%s%N',  # timestamp in nanoseconds
            'BLAST_ELB_JOB_ID': cfg.azure.elb_job_id, #uuid.uuid4().hex,
            'BLAST_ELB_VERSION': VERSION,
            'BLAST_USAGE_REPORT': str(usage_reporting).lower(),
            'K8S_JOB_GET_BLASTDB' : K8S_JOB_GET_BLASTDB,
            'K8S_JOB_LOAD_BLASTDB_INTO_RAM' : K8S_JOB_LOAD_BLASTDB_INTO_RAM,
            'K8S_JOB_IMPORT_QUERY_BATCHES' : K8S_JOB_IMPORT_QUERY_BATCHES,
            'K8S_JOB_SUBMIT_JOBS' : K8S_JOB_SUBMIT_JOBS,
            'K8S_JOB_BLAST' : K8S_JOB_BLAST,
            'K8S_JOB_RESULTS_EXPORT' : K8S_JOB_RESULTS_EXPORT,
            'ELB_AZURE_RESOURCE_GROUP': cfg.azure.resourcegroup,
            'ELB_METADATA_DIR': ELB_METADATA_DIR,
        }
        return subs


    def _generate_and_submit_jobs(self, queries: List[str]):
        cfg, clean_up_stack = self.cfg, self.cleanup_stack
        subs = self.job_substitutions()
        job_template_text = read_job_template(cfg=cfg)
        with TemporaryDirectory() as job_path:
            job_files = write_job_files(job_path, 'batch_', job_template_text, queries, **subs)
            logging.debug(f'Generated {len(job_files)} job files')
            if len(job_files) > 0:
                logging.debug(f'Job #1 file: {job_files[0]}')
                logging.debug('Command to run in the pod:')
                with open(job_files[0]) as f:
                    for line in f:
                        if line.find('-query') >= 0:
                            logging.debug(line.strip())
                            break

            logging.info('Submitting jobs to cluster')
            clean_up_stack.append(lambda: logging.debug('Before submission computational jobs'))
            # Should never happen, cfg.appstate.k8s_ctx should always be initialized properly
            # by the time of this call 
            assert(cfg.appstate.k8s_ctx)
            start = timer()
            job_names = kubernetes.submit_jobs(cfg.appstate.k8s_ctx, Path(job_path), dry_run=self.dry_run)
            end = timer()
            logging.debug(f'RUNTIME submit-jobs {end-start} seconds')
            logging.debug(f'SPEED to submit-jobs {len(job_names)/(end-start):.2f} jobs/second')
            clean_up_stack.append(lambda: logging.debug('After submission computational jobs'))
            if job_names:
                logging.debug(f'Job #1 name: {job_names[0]}')
                
            sas_token = self.cfg.azure.get_sas_token()
            # Signal janitor job to start checking for results
            with open_for_write_immediate(os.path.join(cfg.cluster.results, self.cfg.azure.elb_job_id, ELB_METADATA_DIR, ELB_NUM_JOBS_SUBMITTED), sas_token=sas_token) as f:
                f.write(str(len(job_names)))


    def get_disk_quota(self) -> Tuple[float, float]:
        """ Get the Persistent Disk SSD quota (SSD_TOTAL_GB)
            Returns tuple of limit and usage in GB """
        cmd = f'gcloud compute regions describe {self.cfg.gcp.region} --project {self.cfg.gcp.project} --format json'
        limit = 1e9
        usage = 0.0
        if self.cfg.cluster.dry_run:
            logging.info(cmd)
        else:
            # The execution of this command requires serviceusage.quotas.get permission
            # so it can be unsuccessful for some users
            p = safe_exec(cmd)
            if p.stdout:
                res = json.loads(p.stdout.decode())
                if 'quotas' in res:
                    for quota in res['quotas']:
                        if quota['metric'] == 'SSD_TOTAL_GB':
                            limit = float(quota['limit'])
                            usage = float(quota['usage'])
                            break
        return limit, usage

    def _enable_gcp_apis(self) -> None:
        """ Enables GCP APIs only once per object initialization """
        if not self.apis_enabled:
            enable_gcp_api(self.cfg.gcp.project, self.cfg.cluster.dry_run)
            self.apis_enabled = True

    def _get_aks_credentials(self) -> str:
        """ Memoized get_gke_credentials """
        if not self.cfg.appstate.k8s_ctx:
            self.cfg.appstate.k8s_ctx = get_aks_credentials(self.cfg)
        return self.cfg.appstate.k8s_ctx


def set_gcp_project(project: str) -> None:
    """Set current GCP project in gcloud environment, raises
    util.SafeExecError on problems with running command line gcloud"""
    cmd = f'gcloud config set project {project}'
    safe_exec(cmd)


def get_disks(cfg: ElasticBlastConfig, dry_run: bool = False) -> List[str]:
    """Return a list of disk names in the current GCP project.
    Raises:
        util.SafeExecError on problems with command line gcloud,
        RuntimeError when gcloud results cannot be parsed"""
    cmd = f'gcloud compute disks list --format json --project {cfg.gcp.project}'
    if dry_run:
        logging.info(cmd)
        return list()

    p = safe_exec(cmd)
    try:
        disks = json.loads(p.stdout.decode())
    except Exception as err:
        raise RuntimeError('Error when parsing listing of GCP disks' + str(err))
    if disks is None:
        raise RuntimeError('Improperly read gcloud disk listing')
    return [i['name'] for i in disks]


def get_snapshots(cfg: ElasticBlastConfig, dry_run: bool = False) -> List[str]:
    """Return a list of volume snapshot names in the current GCP project.
    Raises:
        util.SafeExecError on problems with command line gcloud,
        RuntimeError when gcloud results cannot be parsed"""
    cmd = f'gcloud compute snapshots list --format json --project {cfg.gcp.project}'
    if dry_run:
        logging.info(cmd)
        return list()

    p = safe_exec(cmd)
    try:
        snapshots = json.loads(p.stdout.decode())
    except Exception as err:
        raise RuntimeError('Error when parsing listing of GCP snapshots' + str(err))
    if snapshots is None:
        raise RuntimeError('Improperly read gcloud disk listing')
    return [i['name'] for i in snapshots]


def delete_disk(name: str, cfg: ElasticBlastConfig) -> None:
    """Delete a persistent disk.

    Arguments:
        name: Disk name
        cfg: Application config

    Raises:
        util.SafeExecError on problems with command line tools
        ValueError if disk name is empty"""
    if not name:
        raise ValueError('No disk name provided')
    if not cfg:
        raise ValueError('No application config provided')
    cmd = f'gcloud compute disks delete -q {name} --project {cfg.gcp.project}  --zone {cfg.gcp.zone}'
    safe_exec(cmd)


def delete_snapshot(name: str, cfg: ElasticBlastConfig) -> None:
    """Delete a volume snapshot.

    Arguments:
        name: Volume snapshot name
        cfg: Application config

    Raises:
        util.SafeExecError on problems with command line tools
        ValueError if disk name is empty"""
    if not name:
        raise ValueError('No disk name provided')
    if not cfg:
        raise ValueError('No application config provided')
    cmd = f'gcloud compute snaphots delete -q {name} --project {cfg.gcp.project}  --zone {cfg.gcp.zone}'
    safe_exec(cmd)


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)) # type: ignore
def _get_resource_ids(cfg: ElasticBlastConfig) -> ResourceIds:
    """ Try to get the GCP persistent disk ID from elastic-blast records"""
    retval = ResourceIds()
    if cfg.appstate.resources.disks and cfg.appstate.resources.snapshots:
        retval = cfg.appstate.resources
        logging.debug(f'GCP disk ID {retval.disks}')
        logging.debug(f'GCP volume snapshot ID {retval.snapshots}')
        # no need to get disk id from GS if we already have it
        return retval

    disk_id_on_gcs = os.path.join(cfg.cluster.results, cfg.azure.elb_job_id, ELB_METADATA_DIR, ELB_STATE_DISK_ID_FILE)
    cmd = f'gsutil -q stat {disk_id_on_gcs}'
    try:
        safe_exec(cmd)
    except Exception as e:
        logging.debug(f'{disk_id_on_gcs} not found')
        return retval

    cmd = f'gsutil -q cat {disk_id_on_gcs}'
    try:
        p = safe_exec(cmd)
        retval = ResourceIds.from_json(p.stdout.decode())

        err = p.stderr.decode()
        if retval.disks or retval.snapshots:
            logging.debug(f"Retrieved GCP resource IDs {retval} from {disk_id_on_gcs}")
            try:
                for disk_id in retval.disks:
                    validate_gcp_disk_name(disk_id)
            except ValueError:
                logging.error(f'GCP disk ID "{disk_id}" retrieved from {disk_id_on_gcs} is invalid.')
        else:
            raise RuntimeError('Persistent disk id stored in GS is empty')
    except Exception as e:
        logging.error(f'Unable to read {disk_id_on_gcs}: {e}')
        raise

    logging.debug(f'Fetched resource IDs {retval}')
    return retval


def delete_cluster_with_cleanup(cfg: ElasticBlastConfig) -> None:
    """Delete GKE cluster along with persistent disk

    Arguments:
        cfg: Config parameters"""

    dry_run = cfg.cluster.dry_run
    try_kubernetes = True
    pds = []
    snapshots = []
    try:
        resources = _get_resource_ids(cfg)
        pds = resources.disks
        snapshots = resources.snapshots
    except Exception as e:
        logging.error(f'Unable to read disk id from GS: {e}')
    else:
        logging.debug(f'PD id {" ".join(pds)}')
        logging.debug(f'Snapshot id {" ".join(snapshots)}')

    # determine the course of action based on cluster status
    while True:
        status : str = check_cluster(cfg)
        if not status:
            msg = f'Cluster {cfg.cluster.name} was not found'
            if cfg.cluster.dry_run:
                logging.error(msg)
                return
            else:
                # TODO: to avoid this hack make delete_cluster_with_cleanup
                # a method of ElasticBlastGcp
                elastic_blast = ElasticBlastAzure(cfg, False)
                status = elastic_blast._status_from_results()
                if status == ElbStatus.UNKNOWN:
                    raise UserReportError(returncode=CLUSTER_ERROR, message=msg)
                # Check for status of gone cluster, delete data if
                # necessary
                remove_split_query(cfg)
                return
                
        logging.debug(f'Cluster status "{status}"')

        if status == AKS_PROVISIONING_STATE.SUCCEEDED:
            break
        # if error, there is something wrong with the cluster, kubernetes will
        # likely not work
        if status == AKS_PROVISIONING_STATE.FAILED or status == AKS_PROVISIONING_STATE.STOPPING or AKS_PROVISIONING_STATE.DELETING:
            try_kubernetes = False
            break
        
        # if cluster is provisioning or undergoing software updates, wait
        # until it is active,
        if status ==  AKS_PROVISIONING_STATE.STARTING or status == AKS_PROVISIONING_STATE.UPDATING:
            time.sleep(10)
            continue
        # if cluster is already being deleted, nothing to do, exit with an error
        if status == AKS_PROVISIONING_STATE.STOPPING:
            raise UserReportError(returncode=CLUSTER_ERROR,
                                  message=f"cluster '{cfg.cluster.name}' is already being deleted")

        # for unrecognized cluster status exit the loop and the code below
        # will delete the cluster
        logging.warning(f'Unrecognized cluster status {status}')
        break

    if try_kubernetes:
        try:
            cfg.appstate.k8s_ctx = get_aks_credentials(cfg)
            kubernetes.check_server(cfg.appstate.k8s_ctx, dry_run)
        except Exception as e:
            logging.warning(f'Connection to Kubernetes cluster failed.\tDetails: {e}')
            # Can't do anything kubernetes without cluster credentials
            try_kubernetes = False

    if try_kubernetes:
        k8s_ctx = cfg.appstate.k8s_ctx
        # This should never happen when calling the elastic-blast script, as
        # the k8s context is set as part of calling gcloud container clusters get credentials
        # This check is to pacify the mypy type checker and to alert those
        # using the API directly of missing pre-conditions
        assert(k8s_ctx)

        try:
            # get cluster's persistent disk in case they leak
            pds = kubernetes.get_persistent_disks(k8s_ctx, dry_run)
        except Exception as e:
            logging.warning(f'kubernetes.get_persistent_disks failed.\tDetails: {e}')

        try:
            # get cluster's volume snapshots in case they leak
            snapshots = kubernetes.get_volume_snapshots(k8s_ctx, dry_run)
        except Exception as e:
            logging.warning(f'kubernetes.get_volume_snapshots failed.\tDetails: {e}')

        try:
            # delete all k8s jobs, persistent volumes and volume claims
            # this should delete persistent disks
            deleted = kubernetes.delete_all(k8s_ctx, dry_run)
            logging.debug(f'Deleted k8s objects {" ".join(deleted)}')
            disks = get_disks(cfg, dry_run)
            for i in pds:
                if i in disks:
                    logging.debug(f'PD {i} still present after deleting k8s jobs and PVCs')
                else:
                    logging.debug(f'PD {i} was deleted by deleting k8s PVC')

            all_snapshots = get_snapshots(cfg, dry_run)
            for i in snapshots:
                if i in all_snapshots:
                    logging.debug(f'Snapshot {i} still present after deleting k8s jobs and volume snapshots')
                else:
                    logging.debug(f'Snapshot {i} was deleted by deleting k8s volume snapshots')
        except Exception as e:
            # nothing to do the above fails, the code below will take care of
            # persistent disk leak
            logging.warning(f'kubernetes.delete_all failed.\tDetails: {e}')

    if pds:
        try:
            # delete persistent disks if they are still in GCP, this may be faster
            # than deleting a non-existent disk
            disks = get_disks(cfg, dry_run)
            for i in pds:
                if i in disks:
                    logging.debug(f'PD {i} still present after cluster deletion, deleting again')
                    delete_disk(i, cfg)
            all_snapshots = get_snapshots(cfg, dry_run)
            for i in snapshots:
                if i in all_snapshots:
                    logging.debug(f'Snapshot {i} still present after cluster deletion, deleting again')
                    delete_snapshot(i, cfg)
        except Exception as e:
            logging.error(getattr(e, 'message', repr(e)))
            # if the above failed, try deleting each disk unconditionally to
            # minimize resource leak
            for i in pds:
                try:
                    delete_disk(i, cfg)
                except Exception as e:
                    logging.error(getattr(e, 'message', repr(e)))
            for i in snapshots:
                try:
                    delete_snapshot(i, cfg)
                except Exception as e:
                    logging.error(getattr(e, 'message', repr(e)))
        finally:
            disks = get_disks(cfg, dry_run)
            for i in pds:
                if i in disks:
                    msg = f'ElasticBLAST was not able to delete persistent disk "{i}". ' \
                        'Leaving it may cause additional charges from the cloud provider. ' \
                        'You can verify that the disk still exists using this command:\n' \
                        f'gcloud compute disks list --project {cfg.gcp.project} | grep {i}\n' \
                        f'and delete it with:\ngcloud compute disks delete {i} --project {cfg.gcp.project} --zone {cfg.gcp.zone}'
                    logging.error(msg)

            all_snapshots = get_snapshots(cfg, dry_run)
            for i in snapshots:
                if i in all_snapshots:
                    msg = f'ElasticBLAST was not able to delete volume snapshot "{i}". ' \
                        'Leaving it may cause additional charges from the cloud provider. ' \
                        'You can verify that the disk still exists using this command:\n' \
                        f'gcloud compute disks snapshots --project {cfg.gcp.project} | grep {i}\n' \
                        f'and delete it with:\ngcloud compute snapshots delete {i} --project {cfg.gcp.project} --zone {cfg.gcp.zone}'
                    logging.error(msg)
                    # Remove the exception for now, as we want to delete the cluster always!
                    #raise UserReportError(returncode=CLUSTER_ERROR, msg)

    remove_split_query(cfg)
    delete_cluster(cfg)


def get_aks_clusters(cfg: ElasticBlastConfig) -> List[str]:
    """Return a list of GKE cluster names.

    Arguments:
        cfg: configuration object

    Raises:
        util.SafeExecError on problems with command line gcloud
        RuntimeError on problems parsing gcloud JSON output"""
    # cmd = f'gcloud container clusters list --format json --project {cfg.gcp.project}'
    cmd = f'az aks list --query "[].name" -o json'
    p = safe_exec(cmd)
    try:
        clusters = json.loads(handle_error(p.stdout))
    except Exception as err:
        raise RuntimeError(f'Error when parsing JSON listing of GKE clusters: {str(err)}')
    return [i['name'] for i in clusters]


def get_aks_credentials(cfg: ElasticBlastConfig) -> str:
    """Connect to a AKS cluster.

    Arguments:
        cfg: configuration object

    Returns:
        The kubernetes current context

    Raises:
        util.SafeExecError on problems with command line aks"""
    cmd: List[str] = 'az aks get-credentials'.split()
    cmd.append('--resource-group')
    cmd.append(f'{cfg.azure.resourcegroup}')
    cmd.append('--name')
    cmd.append(cfg.cluster.name)
    cmd.append('--overwrite-existing')    
    
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        safe_exec(cmd)

    cmd = 'kubectl config current-context'.split()
    retval = K8S_UNINITIALIZED_CONTEXT
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        p = safe_exec(cmd)
        retval = handle_error(p.stdout).strip()
    return retval

def set_role_assignment(cfg: ElasticBlastConfig):
    """Set role assignment for the managed identity of the AKS cluster.

    Arguments:
        cfg: configuration object

    Raises:
        util.SafeExecError on problems with command line aks"""
    # get storage account id
    cmd: List[str] = 'az storage account show'.split()
    cmd.append('--name')
    cmd.append(cfg.azure.storage_account)
    cmd.append('--resource-group')
    cmd.append(cfg.azure.resourcegroup)
    cmd.append('--query')
    cmd.append('id')
    cmd.append('-o')
    cmd.append('tsv')
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        p = safe_exec(cmd)
        sa_id = handle_error(p.stdout).strip()
    
    # get kubeletidentity
    cmd: List[str] = 'az aks show'.split()
    cmd.append('--name')
    cmd.append(cfg.cluster.name)
    cmd.append('--resource-group')
    cmd.append(cfg.azure.resourcegroup)
    cmd.append('--query')
    cmd.append('identityProfile.kubeletidentity.clientId')
    cmd.append('-o')
    cmd.append('tsv')
    
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        p = safe_exec(cmd)
        aks_kubelet_id = handle_error(p.stdout).strip()
        
    # Storage Blob Data Contributor role assign
    cmd: List[str] = 'az role assignment create'.split()
    cmd.append('--role')
    cmd.append('Storage Blob Data Contributor')
    cmd.append('--assignee')
    cmd.append(aks_kubelet_id)
    cmd.append('--scope')
    cmd.append(sa_id)
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        safe_exec(cmd)
    
    # get acr id
    cmd: List[str] = 'az acr show'.split()
    cmd.append('--name')
    cmd.append(cfg.azure.acr_name)
    cmd.append('--resource-group')
    cmd.append(cfg.azure.acr_resourcegroup)
    cmd.append('--query')
    cmd.append('id')
    cmd.append('-o')
    cmd.append('tsv')
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        p = safe_exec(cmd)
        acr_id = handle_error(p.stdout).strip()
    
    # AcrPull role assignment
    cmd: List[str] = 'az role assignment create'.split()
    cmd.append('--role')
    cmd.append('AcrPull')
    cmd.append('--assignee')
    cmd.append(aks_kubelet_id)
    cmd.append('--scope')
    cmd.append(acr_id)
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        safe_exec(cmd)
        
    # get nodeResourceGroup
    cmd: List[str] = 'az aks show'.split()
    cmd.append('--name')
    cmd.append(cfg.cluster.name)
    cmd.append('--resource-group')
    cmd.append(cfg.azure.resourcegroup)
    cmd.append('--query')
    cmd.append('nodeResourceGroup')
    cmd.append('-o')
    cmd.append('tsv')
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        p = safe_exec(cmd)
        node_resourcegroup = handle_error(p.stdout).strip()
        
    # get subscription id
    cmd: List[str] = 'az account show'.split()
    cmd.append('--query')
    cmd.append('id')
    cmd.append('-o')
    cmd.append('tsv')
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        p = safe_exec(cmd)
        subscription_id = handle_error(p.stdout).strip()
        
    # assign Controbutor role to nodeResourceGroup, to allow the cluster to create resources(disk) in the nodeResourceGroup
    cmd: List[str] = 'az role assignment create'.split()
    cmd.append('--role')
    cmd.append('Contributor')
    cmd.append('--assignee')
    cmd.append(aks_kubelet_id)
    cmd.append('--scope')
    # cmd.append(f'/subscriptions/{subscription_id}/resourceGroups/{node_resourcegroup}')
    cmd.append(f'/subscriptions/{subscription_id}')
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        safe_exec(cmd)


def check_cluster(cfg: ElasticBlastConfig) -> str:
    """ Check if cluster specified by configuration is running.
    Returns cluster status in AKS - Creating, Succeeded, Updating, Deleting, Failed, Canceled, Provisioning, Stopped, Stopping, Resuming -
    if there is such cluster, empty string otherwise.
    All possible exceptions will be passed to upper level.
    """
    cluster_name = cfg.cluster.name
    
    # TODO: A timeout occurs when AKS is in a stopped state. or check nameserver in /etc/resolv.conf
    query = f'[?name==\'{cluster_name}\']' + '.{ProvisioningState:provisioningState}'
    cmd = f'az aks list --resource-group {cfg.azure.resourcegroup} --query "{query}" -o tsv'
    retval = ''
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        out = safe_exec(shlex.split(cmd), timeout=10)
        retval = out.stdout.strip()
    return retval


def start_cluster(cfg: ElasticBlastConfig):
    """ Starts cluster as specified by configuration.
    All possible exceptions will be passed to upper level.

    Per https://cloud.google.com/kubernetes-engine/docs/how-to/creating-a-regional-cluster#create-regional-single-zone-nodepool
    this function creates a (standard GKE) regional cluster with a single-zone node pool
    """

    cluster_name = ''
    machine_type = ''
    num_nodes = 1

    # .. get values from config and raise exception if missing
    if cfg.cluster.name is not None:
        cluster_name = cfg.cluster.name
    else:
        raise ValueError('Configuration error: missing cluster name in [cluster] sections')
    
    if cfg.cluster.machine_type is not None:
        machine_type = cfg.cluster.machine_type
    else:
        raise ValueError('Configuration error: missing machine-type in [cluster] sections')
    
    if cfg.cluster.num_nodes is not None:
        num_nodes = cfg.cluster.num_nodes
    else:
        raise ValueError('Configuration error: missing num-nodes in [cluster] sections')

    # ask for cheaper nodes
    use_preemptible = cfg.cluster.use_preemptible
    use_local_ssd = cfg.cluster.use_local_ssd
    dry_run = cfg.cluster.dry_run
    
    # install k8s-extension
    logging.info('Installing k8s-extension')
    cmd: List[str] = 'az extension add'.split()
    cmd.append('--upgrade')
    cmd.append('--name')
    cmd.append('k8s-extension')
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        safe_exec(cmd)
    

    # https://learn.microsoft.com/en-us/cli/azure/aks?view=azure-cli-latest#az-aks-create
    actual_params = ["az", "aks", "create"]
    actual_params.append('--auto-upgrade-channel')
    actual_params.append('none')
    actual_params.append('--resource-group')
    actual_params.append(f'{cfg.azure.resourcegroup}')
    actual_params.append('--name')
    actual_params.append(f'{cluster_name}')
    actual_params.append('--generate-ssh-keys')

    actual_params.append('--node-vm-size')
    actual_params.append(machine_type)

    actual_params.append('--node-count')
    actual_params.append(str(num_nodes))
    actual_params.append('--min-count')
    actual_params.append(str(num_nodes))
    actual_params.append('--max-count')
    actual_params.append(str(num_nodes*3))
    actual_params.append('--enable-cluster-autoscaler')
    
    actual_params.append('--node-osdisk-type')
    actual_params.append('Managed') # Managed | Ephemeral, Premium SSD LRS
    
    #service mesh
    # actual_params.append('--enable-azure-service-mesh')
    
    #enable managed identity
    actual_params.append('--enable-managed-identity')
    
    # enable container storage
    # actual_params.append('--enable-azure-container-storage')
    # actual_params.append('azureDisk')
    
    # Autoscaling for clusters with local SSD works only by shrinking
    # so to support it we start cluster with maximum nodes.
    # Thus the nodes are properly initialized and autoscaler
    # later can remove them if/when they're not needed.
    # if use_local_ssd:
    #     actual_params.append(str(cfg.cluster.num_nodes))
    # else:
    #     actual_params.append(str(ELB_DFLT_MIN_NUM_NODES))
    
    # https://learn.microsoft.com/en-us/azure/aks/azure-blob-csi?tabs=NFS#before-you-begin
    if not use_local_ssd:
        actual_params.append('--enable-blob-driver')

    if use_preemptible:
        actual_params.append('--preemptible')

    # https://cloud.google.com/stackdriver/pricing
    # if cfg.cluster.enable_stackdriver:
    #     actual_params.append('--enable-stackdriver-kubernetes')

    
    # FIXME: labels, in future will be provided by config or run-time
    tags = cfg.cluster.labels
    actual_params.append('--tags')
    actual_params.append(tags)

    # if use_local_ssd:
    #     actual_params.append('--local-ssd-count')
    #     actual_params.append('1')

    # if cfg.azure.network is not None:
    #     actual_params.append(f'--network={cfg.azure.network}')
    # if cfg.azure.subnet is not None:
    #     actual_params.append(f'--subnetwork={cfg.azure.subnet}')

    if cfg.azure.aks_version:
        actual_params.append('--kubernetes-version')
        actual_params.append(f'{cfg.azure.aks_version}')

    start = timer()
    if dry_run:
        logging.info(' '.join(actual_params))
    else:
        logging.info('create aks cluster: ' + ' '.join(actual_params))
        print(f'\033[32m create aks cluster: {" ".join(actual_params)}\033[0m')
        safe_exec(actual_params, timeout=1800) # 30 minutes
        # safe_exec_print(actual_params)
        
    end = timer()
    logging.debug(f'RUNTIME cluster-create {end-start} seconds')
    
    

    return cluster_name


def delete_cluster(cfg: ElasticBlastConfig):
    cluster_name = cfg.cluster.name
    actual_params = ["az", "aks", "delete"]
    actual_params.append('--resource-group')
    actual_params.append(f'{cfg.azure.resourcegroup}')
    actual_params.append('--name')
    actual_params.append(f'{cluster_name}')
    start = timer()
    if cfg.cluster.dry_run:
        logging.info(actual_params)
    else:
        safe_exec(actual_params)
    end = timer()
    logging.debug(f'RUNTIME cluster-delete {end-start} seconds')
    return cluster_name


def check_prerequisites() -> None:
    """ Check that necessary tools, gcloud, gsutil, gke-gcloud-auth-plugin and kubectl
    are available if necessary.
    If execution of one of these tools is unsuccessful
    it will throw UserReportError exception."""
    try:
        p = safe_exec('gcloud --version')
    except SafeExecError as e:
        message = f"Required pre-requisite 'gcloud' doesn't work, check installation of GCP SDK.\nDetails: {e.message}"
        raise UserReportError(DEPENDENCY_ERROR, message)
    logging.debug(f'{":".join(p.stdout.decode().split())}')

    try:
        # client=true prevents kubectl from addressing server which can be down at the moment
        p = safe_exec('kubectl version --output=json --client=true')
    except SafeExecError as e:
        message = f"Required pre-requisite 'kubectl' doesn't work, check Kubernetes installation.\nDetails: {e.message}"
        raise UserReportError(DEPENDENCY_ERROR, message)
    logging.debug(f'{":".join(p.stdout.decode().split())}')

    version_data = json.loads(p.stdout.decode())
    kubectl_version = version_data["clientVersion"]["major"] + "."
    kubectl_version += version_data["clientVersion"]["minor"]
    is_newer_than_1_25 = True
    try : is_newer_than_1_25 = is_newer_version(kubectl_version, "1.25")
    except ValueError: pass # ignore version parsing errors
    if is_newer_than_1_25 and shutil.which("gke-gcloud-auth-plugin") is None:
        message = f"Missing dependency 'gke-gcloud-auth-plugin', "
        message += "for more information, please see "
        message += "https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke"
        raise UserReportError(DEPENDENCY_ERROR, message)

    # Check we have gsutil available
    try:
        p = safe_exec('gsutil --version')
    except SafeExecError as e:
        message = f"Required pre-requisite 'gsutil' doesn't work, check installation of GCP SDK.\nDetails: {e.message}\nNote: this is because your query is located on GS, you may try another location"
        raise UserReportError(DEPENDENCY_ERROR, message)
    logging.debug(f'{":".join(p.stdout.decode().split())}')


def remove_split_query(cfg: ElasticBlastConfig) -> None:
    """ Remove split query from user's results bucket """
    _remove_ancillary_data(cfg, ELB_QUERY_BATCH_DIR)


# TODO: implement this function
def _remove_ancillary_data(cfg: ElasticBlastConfig, bucket_prefix: str) -> None:
    """ Removes ancillary data from the end user's result bucket
    cfg: Configuration object
    bucket_prefix: path that follows the users' bucket name (looks like a file system directory)
    """
    return
    dry_run = cfg.cluster.dry_run
    out_path = os.path.join(cfg.cluster.results, bucket_prefix, '*')
    cmd = f'gsutil -mq rm {out_path}'
    if dry_run:
        logging.info(cmd)
    else:
        # This command is a part of clean-up process, there is no benefit in reporting
        # its failure except logging it
        try:
            safe_exec(cmd)
        except SafeExecError as e:
            message = e.message.strip().translate(str.maketrans('\n', '|'))
            logging.warning(message)

