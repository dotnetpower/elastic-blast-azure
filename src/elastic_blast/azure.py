"""
elastic_blast/azure.py — Azure AKS implementation of ElasticBLAST

Manages the lifecycle of BLAST searches on Azure Kubernetes Service:
cluster creation, DB initialization, job submission, status checking, and cleanup.

Authors: Moon Hyuk Choi moonchoi@microsoft.com
"""

import os
import shlex
import time
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from timeit import default_timer as timer
from typing import Any, DefaultDict, Dict, Optional, List, Tuple
from collections import defaultdict
from tenacity import retry, stop_after_attempt, wait_exponential

from .base import MemoryStr
from .subst import substitute_params
from . import azure_monitor as monitor
from .filehelper import open_for_write_immediate
from .jobs import read_job_template, write_job_files
from .util import (ElbSupportedPrograms, safe_exec, UserReportError, SafeExecError,
                   get_blastdb_info, get_usage_reporting, handle_error)
from . import kubernetes
from .constants import (
    CLUSTER_ERROR, DEPENDENCY_ERROR, INPUT_ERROR,
    ELB_NUM_JOBS_SUBMITTED, ELB_METADATA_DIR, ELB_STATE_DISK_ID_FILE,
    ELB_QUERY_BATCH_DIR, ELB_QUERY_LENGTH,
    K8S_JOB_CLOUD_SPLIT_SSD, K8S_JOB_INIT_PV, K8S_JOB_BLAST,
    K8S_JOB_GET_BLASTDB, K8S_JOB_IMPORT_QUERY_BATCHES,
    K8S_JOB_LOAD_BLASTDB_INTO_RAM, K8S_JOB_RESULTS_EXPORT, K8S_JOB_SUBMIT_JOBS,
    ELB_DFLT_BLAST_JOB_AKS_TEMPLATE, ELB_LOCAL_SSD_BLAST_JOB_AKS_TEMPLATE,
    ElbExecutionMode, ElbStatus, AKS_PROVISIONING_STATE, STATUS_MESSAGE_ERROR,
)
from .elb_config import ElasticBlastConfig, ResourceIds
from .elasticblast import ElasticBlast
from . import VERSION
from . import azure_sdk as sdk


# ---------------------------------------------------------------------------
# ElasticBlastAzure — main class
# ---------------------------------------------------------------------------

class ElasticBlastAzure(ElasticBlast):
    """Azure AKS implementation of ElasticBLAST."""

    # Storage class for PVC creation.
    # Resolved from: (1) cfg [cluster] storage-class, (2) ELB_STORAGE_CLASS env var, (3) default
    # Values: azureblob-nfs-premium (default), azure-netapp-ultra (ANF)
    STORAGE_CLASS = os.environ.get('ELB_STORAGE_CLASS', 'azureblob-nfs-premium')

    def __init__(self, cfg: ElasticBlastConfig, create=False,
                 cleanup_stack: Optional[List[Any]] = None):
        super().__init__(cfg, create, cleanup_stack)
        # Override STORAGE_CLASS from config if set
        if cfg.cluster.storage_class:
            self.STORAGE_CLASS = cfg.cluster.storage_class
        self.query_files: List[str] = []
        self.cluster_initialized = False
        self.auto_shutdown = 'ELB_DISABLE_AUTO_SHUTDOWN' not in os.environ

    def _safe_collect_logs(self) -> None:
        """Collect K8s logs only if the kubectl context is available."""
        if self.cfg.appstate.k8s_ctx:
            kubernetes.collect_k8s_logs(self.cfg)

    # -- Query splitting ----------------------------------------------------

    def cloud_query_split(self, query_files: List[str]) -> None:
        """Submit query sequences for cloud-based splitting."""
        if self.dry_run:
            return
        self.query_files = query_files
        self._initialize_cluster()
        self.cluster_initialized = True

    def wait_for_cloud_query_split(self) -> None:
        """Block until cloud query split job completes."""
        if not self.query_files:
            return
        kubectl = self._kubectl()
        job = K8S_JOB_CLOUD_SPLIT_SSD if self.cfg.cluster.use_local_ssd else K8S_JOB_INIT_PV

        while True:
            if self.dry_run:
                return
            active = self._kubectl_jsonpath(f'get job {job}',
                                            '{.items[?(@.status.active)].metadata.name}')
            if not active:
                failed = self._kubectl_jsonpath(f'get job {job}',
                                               '{.items[?(@.status.failed)].metadata.name}')
                if failed:
                    msg = ('BLASTDB initialization failed' if job == K8S_JOB_INIT_PV
                           else 'Cloud query splitting failed')
                    raise UserReportError(returncode=CLUSTER_ERROR, message=msg)
                return
            time.sleep(30)

    def upload_query_length(self, query_length: int) -> None:
        """Upload query length metadata to Blob Storage."""
        if query_length <= 0:
            return
        fname = self._metadata_path(ELB_QUERY_LENGTH)
        with open_for_write_immediate(fname) as f:
            f.write(str(query_length))

    # -- Job submission -----------------------------------------------------

    def submit(self, query_batches: List[str], query_length: int,
               one_stage_cloud_query_split: bool) -> None:
        """Submit BLAST batch jobs to the AKS cluster."""
        if one_stage_cloud_query_split:
            raise UserReportError(returncode=INPUT_ERROR,
                message='One-stage cloud query split is not supported on Azure.')

        # Apply optimization profile and show prediction
        self._show_optimization_prediction(query_batches, query_length)

        if self.cfg.blast.db_partitions > 0:
            return self._submit_partitioned(query_batches, query_length)

        if not self.cluster_initialized:
            self._check_job_number_limit(query_batches, query_length)
            self.query_files = []
            self._initialize_cluster(query_batches)
            self.cluster_initialized = True

        if self.cloud_job_submission:
            kubernetes.submit_job_submission_job(self.cfg)
        else:
            self._generate_and_submit_jobs(query_batches)
            if not self.cfg.cluster.use_local_ssd:
                self._save_persistent_disk_ids()

        # Deploy finalizer job to auto-detect completion and upload status marker
        self._submit_finalizer_job()

        self.cleanup_stack.clear()
        self.cleanup_stack.append(lambda: self._safe_collect_logs())

    # -- Status & lifecycle -------------------------------------------------

    def check_status(self, extended=False) -> Tuple[ElbStatus, Dict[str, int], Dict[str, str]]:
        """Check execution status of the ElasticBLAST search."""
        try:
            return self._check_status(extended)
        except SafeExecError as err:
            msg = err.message.strip()
            logging.info(msg)
            return ElbStatus.UNKNOWN, defaultdict(int), {STATUS_MESSAGE_ERROR: msg} if msg else {}

    def delete(self):
        """Delete cluster and resources. In reuse mode, only clean up jobs."""
        if self.cfg.cluster.reuse:
            self._cleanup_jobs_only()
        else:
            delete_cluster_with_cleanup(self.cfg)

    def run_command(self, cmd: str) -> str:
        """Run a kubectl command in the cluster context."""
        full_cmd = f'{cmd} --context={self._get_k8s_ctx()}'
        if self.dry_run:
            logging.info(full_cmd)
            return ''
        return handle_error(safe_exec(shlex.split(full_cmd)).stdout)

    def scale_nodes(self, node_count: int) -> None:
        """Scale AKS node pool. Use 0 to scale down for cost savings."""
        cfg = self.cfg
        sdk.scale_node_pool(cfg.azure.resourcegroup, cfg.cluster.name,
                            'nodepool1', node_count, cfg.cluster.dry_run)

    def get_disk_quota(self) -> Tuple[float, float]:
        """Azure disk quota (not yet implemented). Returns (limit_gb, usage_gb)."""
        return 1e9, 0.0

    def merge_partitioned_results(self) -> List[str]:
        """Merge results from DB-partitioned search into main results directory."""
        num_partitions = self.cfg.blast.db_partitions
        if num_partitions <= 0:
            return []

        base = os.path.join(self.cfg.cluster.results, self.cfg.azure.elb_job_id)
        merged: List[str] = []
        for i in range(num_partitions):
            src = os.path.join(base, f'part_{i:02d}', '*.out.gz')
            cmd = ['azcopy', 'cp', src, f'{base}/', '--recursive']
            if self.cfg.cluster.dry_run:
                logging.info(f'dry-run: {" ".join(cmd)}')
            else:
                try:
                    _retry_exec(cmd)
                    merged.append(src)
                except SafeExecError as e:
                    logging.warning(f'Failed to merge partition {i}: {e.message}')
        return merged

    # -- Internal helpers ---------------------------------------------------

    @staticmethod
    def _decode(value) -> str:
        """Safely decode bytes to str. Handles None, bytes, and str."""
        if value is None:
            return ''
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        return str(value)

    def _get_k8s_ctx(self) -> str:
        """Get or cache kubectl context."""
        if not self.cfg.appstate.k8s_ctx:
            self.cfg.appstate.k8s_ctx = get_aks_credentials(self.cfg)
        return self.cfg.appstate.k8s_ctx

    def _kubectl(self) -> str:
        """Return kubectl command prefix with context."""
        return f'kubectl --context={self._get_k8s_ctx()}'

    def _kubectl_jsonpath(self, resource_cmd: str, jsonpath: str) -> str:
        """Run kubectl with jsonpath output and return result."""
        cmd = f'{self._kubectl()} {resource_cmd} -o jsonpath=\'{jsonpath}\''
        proc = safe_exec(cmd)
        return handle_error(proc.stdout)

    def _metadata_path(self, filename: str) -> str:
        """Return full blob path for a metadata file."""
        return os.path.join(self.cfg.cluster.results, self.cfg.azure.elb_job_id,
                            ELB_METADATA_DIR, filename)

    def _results_path(self) -> str:
        """Return base results path including job_id."""
        return os.path.join(self.cfg.cluster.results, self.cfg.azure.elb_job_id)

    def _job_template_name(self) -> str:
        """Return correct job template name based on storage mode."""
        if self.cfg.cluster.use_local_ssd:
            return ELB_LOCAL_SSD_BLAST_JOB_AKS_TEMPLATE
        return ELB_DFLT_BLAST_JOB_AKS_TEMPLATE

    def _build_substitutions(self, query_batches: List[str], *,
                              db: str = '', db_label: str = '',
                              results_path: str = '') -> Dict[str, str]:
        """Build template substitution dictionary.

        Shared by both standard and partitioned job generation.
        """
        cfg = self.cfg
        if not db:
            db, _, db_label = get_blastdb_info(cfg.blast.db)
        if not results_path:
            results_path = self._results_path()

        program = cfg.blast.program
        # CPU allocation: fewer CPUs when 1 job per node, else quarter of total
        # Cap at num_cpus - 2 to ensure request never exceeds per-node limit
        if len(query_batches) == cfg.cluster.num_nodes:
            cpu_req = cfg.cluster.num_cpus - 2
        else:
            cpu_req = ((cfg.cluster.num_nodes * cfg.cluster.num_cpus) // 4) - 2
        cpu_req = min(cpu_req, cfg.cluster.num_cpus - 2)
        cpu_req = max(0, cpu_req)

        return {
            'ELB_BLAST_PROGRAM': program,
            'ELB_DB': db,
            'ELB_DB_LABEL': db_label,
            'ELB_MEM_REQUEST': str(cfg.cluster.mem_request),
            'ELB_MEM_LIMIT': str(cfg.cluster.mem_limit),
            'ELB_BLAST_OPTIONS': cfg.blast.options,
            'ELB_BLAST_TIMEOUT': str(cfg.timeouts.blast_k8s * 60),
            'ELB_RESULTS': results_path,
            'ELB_NUM_CPUS_REQ': str(cpu_req),
            'ELB_NUM_CPUS': str(cfg.cluster.num_cpus),
            'ELB_DB_MOL_TYPE': str(ElbSupportedPrograms().get_db_mol_type(program)),
            'ELB_DOCKER_IMAGE': cfg.azure.elb_docker_image,
            'ELB_TIMEFMT': '%s%N',
            'BLAST_ELB_JOB_ID': cfg.azure.elb_job_id,
            'BLAST_ELB_VERSION': VERSION,
            'BLAST_USAGE_REPORT': str(get_usage_reporting()).lower(),
            'K8S_JOB_GET_BLASTDB': K8S_JOB_GET_BLASTDB,
            'K8S_JOB_LOAD_BLASTDB_INTO_RAM': K8S_JOB_LOAD_BLASTDB_INTO_RAM,
            'K8S_JOB_IMPORT_QUERY_BATCHES': K8S_JOB_IMPORT_QUERY_BATCHES,
            'K8S_JOB_SUBMIT_JOBS': K8S_JOB_SUBMIT_JOBS,
            'K8S_JOB_BLAST': K8S_JOB_BLAST,
            'K8S_JOB_RESULTS_EXPORT': K8S_JOB_RESULTS_EXPORT,
            'ELB_AZURE_RESOURCE_GROUP': cfg.azure.resourcegroup,
            'ELB_METADATA_DIR': ELB_METADATA_DIR,
        }

    # Keep backward-compatible names
    def job_substitutions(self, query_batches) -> Dict[str, str]:
        return self._build_substitutions(query_batches)

    def _job_substitutions_for_partition(self, query_batches, partition_idx, partition_db, partition_db_name):
        return self._build_substitutions(
            query_batches, db=partition_db, db_label=f'part{partition_idx:02d}',
            results_path=os.path.join(self._results_path(), f'part_{partition_idx:02d}')
        )

    def _submit_finalizer_job(self) -> None:
        """Submit the finalizer Job that waits for BLAST completion and uploads status."""
        cfg = self.cfg
        if cfg.cluster.dry_run:
            logging.info('dry-run: would submit elb-finalizer job')
            return

        subs = {
            'ELB_DOCKER_IMAGE': cfg.azure.elb_docker_image,
            'ELB_RESULTS': self._results_path(),
            'ELB_AZURE_RESOURCE_GROUP': cfg.azure.resourcegroup,
            'ELB_CLUSTER_NAME': cfg.cluster.name,
            'ELB_REUSE_CLUSTER': 'true' if cfg.cluster.reuse else 'false',
            'ELB_METADATA_DIR': ELB_METADATA_DIR,
            'ELB_SERVICE_ACCOUNT': 'default',
        }

        from importlib.resources import files as pkg_files
        template = pkg_files('elastic_blast').joinpath(
            'templates/elb-finalizer-aks.yaml.template').read_text()
        rendered = substitute_params(template, subs)

        with TemporaryDirectory() as d:
            path = os.path.join(d, 'elb-finalizer.yaml')
            with open(path, 'w') as f:
                f.write(rendered)
            kubectl = self._kubectl()
            safe_exec(shlex.split(f'{kubectl} apply -f {path}'))
            logging.info('Submitted elb-finalizer job')

    def _show_optimization_prediction(self, query_batches: List[str], query_length: int) -> None:
        """Apply optimization profile and display time/cost prediction."""
        from . import azure_optimizer as optimizer

        profile = optimizer.get_profile()
        query_size_gb = query_length / 1e9 if query_length > 0 else 0.1

        # Get actual DB size from metadata if available
        db_size_gb = 10.0  # default
        if self.cfg.blast.db_metadata and hasattr(self.cfg.blast.db_metadata, 'bytes_to_cache'):
            db_size_gb = self.cfg.blast.db_metadata.bytes_to_cache / (1024 ** 3)
        elif os.environ.get('ELB_DB_SIZE_GB'):
            db_size_gb = float(os.environ['ELB_DB_SIZE_GB'])

        num_batches = len(query_batches) if query_batches else None

        # Show all profiles comparison
        comparison = optimizer.predict_all_profiles(
            query_size_gb=query_size_gb, db_size_gb=db_size_gb,
            batch_len=self.cfg.blast.batch_len, num_batches=num_batches)
        logging.info(comparison)
        if os.isatty(1):
            print(comparison)

        pred = optimizer.apply_profile(self.cfg, profile,
                                       query_size_gb=query_size_gb,
                                       db_size_gb=db_size_gb)

        # Detailed prediction log
        logging.info(f'\n{"=" * 60}\n'
                     f'  ElasticBLAST Execution Plan\n'
                     f'{"=" * 60}\n'
                     f'{pred}\n'
                     f'{"=" * 60}')

    def _check_job_number_limit(self, queries: Optional[List[str]], query_length) -> None:
        """Raise if the number of jobs exceeds the Kubernetes limit."""
        if not queries:
            return
        limit = kubernetes.get_maximum_number_of_allowed_k8s_jobs(self.dry_run)
        if len(queries) > limit:
            suggested = int(query_length / limit) + 1
            raise UserReportError(INPUT_ERROR,
                f'Batch size led to {len(queries)} jobs, exceeding limit of {limit}. '
                f'Increase batch-len to at least {suggested}.')

    # -- Cluster initialization ---------------------------------------------

    def _initialize_cluster(self, queries: Optional[List[str]] = None):
        """Create AKS cluster, configure IAM, initialize storage.

        Cluster creation runs asynchronously — query template upload happens
        in parallel while the cluster is provisioning.
        """
        cfg = self.cfg

        # Warm cluster shortcut
        if cfg.cluster.reuse:
            status = check_cluster(cfg)
            if status == AKS_PROVISIONING_STATE.SUCCEEDED.value and self._db_already_loaded():
                logging.info('Warm cluster reuse: skipping init')
                self._upload_queries_only(queries)
                self.cleanup_stack.append(lambda: self._safe_collect_logs())
                return

        # Full initialization
        if not cfg.cluster.reuse:
            self.cleanup_stack.append(lambda: delete_cluster_with_cleanup(cfg, allow_missing=True))
        self.cleanup_stack.append(lambda: self._safe_collect_logs())

        # Start cluster creation asynchronously
        aks_status = check_cluster(cfg)
        poller = None
        if not cfg.cluster.reuse or not aks_status:
            poller = start_cluster_async(cfg)

        # Do work while cluster is provisioning (saves 5-10 min)
        if self.cloud_job_submission:
            self._upload_job_template(queries)

        # Wait for cluster to be ready
        if poller:
            wait_for_cluster(cfg, poller)

        self._get_k8s_ctx()
        self._label_nodes()

        if not cfg.cluster.reuse or not aks_status:
            set_role_assignment(cfg)

        if self.cloud_job_submission or self.auto_shutdown:
            kubernetes.enable_service_account(cfg)

        kubernetes.create_scripts_configmap(cfg.appstate.k8s_ctx, cfg.cluster.dry_run)

        # Apply ANF StorageClass if configured (persistent DB across cluster lifecycles)
        if self.STORAGE_CLASS == 'azure-netapp-ultra':
            self._apply_anf_storage_class()

        logging.info('Initializing storage')
        wait_mode = ElbExecutionMode.NOWAIT if self.cloud_job_submission else ElbExecutionMode.WAIT
        kubernetes.initialize_storage(cfg, self.query_files, wait_mode)

        if cfg.cluster.reuse and not cfg.cluster.use_local_ssd:
            self._deploy_vmtouch_daemonset()

    def _initialize_cluster_partitioned(self, queries: Optional[List[str]]) -> None:
        """Initialize cluster for DB-partitioned search."""
        cfg = self.cfg

        if not cfg.cluster.reuse:
            self.cleanup_stack.append(lambda: delete_cluster_with_cleanup(cfg, allow_missing=True))
        self.cleanup_stack.append(lambda: self._safe_collect_logs())

        aks_status = check_cluster(cfg)
        if not cfg.cluster.reuse or not aks_status:
            start_cluster(cfg)

        self._get_k8s_ctx()
        self._label_nodes()

        if not cfg.cluster.reuse or not aks_status:
            set_role_assignment(cfg)

        if self.cloud_job_submission or self.auto_shutdown:
            kubernetes.enable_service_account(cfg)

        kubernetes.create_scripts_configmap(cfg.appstate.k8s_ctx, cfg.cluster.dry_run)

        wait_mode = ElbExecutionMode.NOWAIT if self.cloud_job_submission else ElbExecutionMode.WAIT
        kubernetes.initialize_storage_partitioned(cfg, self.query_files, wait_mode)

    # -- Partitioned search -------------------------------------------------

    def _submit_partitioned(self, query_batches: List[str], query_length) -> None:
        """Submit jobs for DB-partitioned mode: P partitions x N queries."""
        cfg = self.cfg
        num_partitions = cfg.blast.db_partitions

        if not self.cluster_initialized:
            if query_batches:
                total = len(query_batches) * num_partitions
                limit = kubernetes.get_maximum_number_of_allowed_k8s_jobs(self.dry_run)
                if total > limit:
                    raise UserReportError(INPUT_ERROR,
                        f'Partitioned search would create {total} jobs '
                        f'({num_partitions} x {len(query_batches)}), exceeding limit {limit}.')
            self.query_files = []
            self._initialize_cluster_partitioned(query_batches)
            self.cluster_initialized = True

        self._generate_partitioned_jobs(query_batches)
        self.cleanup_stack.clear()
        self.cleanup_stack.append(lambda: self._safe_collect_logs())

    def _generate_partitioned_jobs(self, query_batches: List[str]) -> None:
        """Generate and submit BLAST jobs for each DB partition."""
        cfg = self.cfg
        prefix = cfg.blast.db_partition_prefix
        base = self._results_path()

        all_files = []
        with TemporaryDirectory() as job_path:
            for i in range(cfg.blast.db_partitions):
                db_name = os.path.basename(f'{prefix}{i:02d}')
                db_on_pvc = f'part_{i:02d}/{db_name}'
                subs = self._build_substitutions(
                    query_batches,
                    db=db_on_pvc, db_label=f'part{i:02d}',
                    results_path=os.path.join(base, f'part_{i:02d}'),
                )
                template = read_job_template(cfg=cfg)
                files = write_job_files(job_path, f'part{i:02d}_batch_',
                                        template, query_batches, **subs)
                all_files.extend(files)

            total = len(all_files)
            logging.info(f'Submitting {total} partitioned jobs')
            assert cfg.appstate.k8s_ctx
            start = timer()
            kubernetes.submit_jobs(cfg.appstate.k8s_ctx, Path(job_path), dry_run=self.dry_run)
            logging.debug(f'RUNTIME submit-partitioned-jobs {timer() - start:.1f}s')

        with open_for_write_immediate(self._metadata_path(ELB_NUM_JOBS_SUBMITTED)) as f:
            f.write(str(total))

    # -- Standard job submission --------------------------------------------

    def _generate_and_submit_jobs(self, queries: List[str]):
        """Generate BLAST batch job YAMLs and submit to cluster."""
        cfg = self.cfg
        subs = self._build_substitutions(queries)
        template = read_job_template(cfg=cfg)

        with TemporaryDirectory() as job_path:
            job_files = write_job_files(job_path, 'batch_', template, queries, **subs)
            logging.info(f'Submitting {len(job_files)} jobs to cluster')

            assert cfg.appstate.k8s_ctx
            start = timer()
            job_names = kubernetes.submit_jobs(cfg.appstate.k8s_ctx, Path(job_path),
                                               dry_run=self.dry_run)
            elapsed = timer() - start
            logging.debug(f'RUNTIME submit-jobs {elapsed:.1f}s '
                         f'({len(job_names) / max(elapsed, 0.1):.0f} jobs/sec)')

            with open_for_write_immediate(self._metadata_path(ELB_NUM_JOBS_SUBMITTED)) as f:
                f.write(str(len(job_names)))

            monitor.track_search_submitted(
                job_id=cfg.azure.elb_job_id, program=cfg.blast.program,
                db=cfg.blast.db, num_jobs=len(job_names),
                num_nodes=cfg.cluster.num_nodes, machine_type=cfg.cluster.machine_type)

    def _save_persistent_disk_ids(self) -> None:
        """Save persistent disk IDs to Blob Storage metadata."""
        ctx = self.cfg.appstate.k8s_ctx
        if not ctx:
            raise RuntimeError('K8s context not set')
        kubernetes.wait_for_pvc(ctx, 'blast-dbs-pvc')

        disk_ids = kubernetes.get_persistent_disks(ctx)
        self.cfg.appstate.resources.disks += disk_ids
        with open_for_write_immediate(self._metadata_path(ELB_STATE_DISK_ID_FILE)) as f:
            f.write(self.cfg.appstate.resources.to_json())

        kubernetes.label_persistent_disk(self.cfg, 'blast-dbs-pvc')
        kubernetes.delete_volume_snapshots(ctx)

    def _upload_job_template(self, queries: Optional[List[str]]) -> None:
        """Upload rendered job template to Blob Storage."""
        subs = self._build_substitutions(queries or [])
        template = read_job_template(template_name=self._job_template_name(), cfg=self.cfg)
        rendered = substitute_params(template, subs)
        path = os.path.join(self._results_path(), ELB_METADATA_DIR, 'job.yaml.template')
        with open_for_write_immediate(path) as f:
            f.write(rendered)

    def _apply_anf_storage_class(self) -> None:
        """Apply Azure NetApp Files StorageClass to enable persistent DB volumes."""
        kubectl = self._kubectl()
        from importlib.resources import files as pkg_files
        sc_path = pkg_files('elastic_blast').joinpath('templates/storage-aks-anf.yaml')
        with TemporaryDirectory() as d:
            dest = os.path.join(d, 'storage-aks-anf.yaml')
            with open(dest, 'w') as f:
                f.write(sc_path.read_text())
            if self.cfg.cluster.dry_run:
                logging.info(f'Would apply ANF StorageClass from {dest}')
            else:
                safe_exec(shlex.split(f'{kubectl} apply -f {dest}'))
                logging.info('Applied azure-netapp-ultra StorageClass')

    # -- Status check -------------------------------------------------------

    def _check_status(self, extended=False) -> Tuple[ElbStatus, Dict[str, int], Dict[str, str]]:
        """Internal status check implementation."""
        if self.cached_status:
            result = {STATUS_MESSAGE_ERROR: self.cached_failure_message} if self.cached_failure_message else {}
            return self.cached_status, self.cached_counts, result

        status = self._status_from_results()
        if status != ElbStatus.UNKNOWN:
            result = {STATUS_MESSAGE_ERROR: self.cached_failure_message} if self.cached_failure_message else {}
            return status, self.cached_counts, result

        aks_status = check_cluster(self.cfg)
        if not aks_status:
            return (ElbStatus.UNKNOWN, {},
                    {STATUS_MESSAGE_ERROR: f'Cluster "{self.cfg.cluster.name}" was not found'})

        TRANSITIONAL = {AKS_PROVISIONING_STATE.UPDATING.value,
                        AKS_PROVISIONING_STATE.CREATING.value,
                        AKS_PROVISIONING_STATE.STARTING.value}
        if aks_status in TRANSITIONAL:
            return ElbStatus.SUBMITTING, {}, {}

        if aks_status != AKS_PROVISIONING_STATE.SUCCEEDED.value:
            raise UserReportError(returncode=CLUSTER_ERROR,
                message=f'Cluster "{self.cfg.cluster.name}" is not responding. '
                        'Try again in a few minutes.')

        counts = self._count_blast_jobs()
        status = self._derive_status(counts)
        return status, counts, {}

    def _count_blast_jobs(self) -> DefaultDict[str, int]:
        """Count BLAST job statuses via kubectl."""
        counts: DefaultDict[str, int] = defaultdict(int)
        kubectl = self._kubectl()

        if not self.dry_run:
            proc = safe_exec(f'{kubectl} get jobs -o custom-columns='
                            f'STATUS:.status.conditions[0].type -l app=blast'.split())
            for line in handle_error(proc.stdout).split('\n'):
                if not line or line.startswith('STATUS'):
                    continue
                if line.startswith('Complete'):
                    counts['succeeded'] += 1
                elif line.startswith('Failed'):
                    counts['failed'] += 1
                else:
                    counts['pending'] += 1

            proc = safe_exec(f'{kubectl} get pods -o custom-columns='
                            f'STATUS:.status.phase -l app=blast'.split())
            for line in handle_error(proc.stdout).split('\n'):
                if line == 'Running':
                    counts['running'] += 1

            counts['pending'] -= counts['running']
        return counts

    def _derive_status(self, counts: DefaultDict[str, int]) -> ElbStatus:
        """Derive overall status from job counts."""
        if counts['failed'] > 0:
            return ElbStatus.FAILURE
        if counts['running'] > 0 or counts['pending'] > 0:
            return ElbStatus.RUNNING
        if counts['succeeded'] > 0:
            return ElbStatus.SUCCESS

        # No blast jobs yet — check setup/submit
        for app in ('setup', 'submit'):
            _, _, failed = self._job_status_by_app(app)
            if failed > 0:
                return ElbStatus.FAILURE
        return ElbStatus.SUBMITTING

    def _job_status_by_app(self, app: str) -> Tuple[int, int, int]:
        """Count pending/succeeded/failed jobs for a given app label."""
        pending = succeeded = failed = 0
        if self.dry_run:
            return pending, succeeded, failed
        try:
            proc = safe_exec(f'{self._kubectl()} get jobs -o custom-columns='
                            f'STATUS:.status.conditions[0].type -l app={app}'.split())
            for line in handle_error(proc.stdout).split('\n'):
                if not line or line.startswith('STATUS'):
                    continue
                if line.startswith('Complete'):
                    succeeded += 1
                elif line.startswith('Failed'):
                    failed += 1
                else:
                    pending += 1
        except SafeExecError:
            pass
        return pending, succeeded, failed

    # -- Cleanup ------------------------------------------------------------

    def _cleanup_jobs_only(self) -> None:
        """In reuse mode: delete BLAST/submit jobs, preserve cluster and PVCs."""
        try:
            kubectl = self._kubectl()
            for label in ('app=blast', 'app=submit'):
                cmd = f'{kubectl} delete jobs -l {label} --ignore-not-found=true'
                if self.cfg.cluster.dry_run:
                    logging.info(cmd)
                else:
                    safe_exec(shlex.split(cmd))
            logging.info('Reuse cleanup: jobs deleted, cluster preserved')
        except Exception as e:
            logging.warning(f'Reuse cleanup failed: {e}')

    # -- Warm cluster -------------------------------------------------------

    def _db_already_loaded(self) -> bool:
        """Check if BLAST DB is already present on the PV."""
        if self.cfg.cluster.dry_run or self.cfg.cluster.use_local_ssd:
            return False
        try:
            kubectl = self._kubectl()
            proc = safe_exec(shlex.split(
                f'{kubectl} get pvc blast-dbs-pvc-rwm -o jsonpath={{.status.phase}}'))
            if self._decode(proc.stdout).strip() != 'Bound':
                return False
            proc = safe_exec(shlex.split(
                f'{kubectl} get job init-pv -o jsonpath={{.status.succeeded}}'))
            return self._decode(proc.stdout).strip() == '1'
        except Exception:
            return False

    def _upload_queries_only(self, queries: Optional[List[str]]) -> None:
        """Warm cluster: upload queries without re-downloading DB."""
        if self.cloud_job_submission:
            self._upload_job_template(queries)
        kubernetes.import_query_batches_only(self.cfg)

    def _deploy_vmtouch_daemonset(self) -> None:
        """Deploy DaemonSet to keep BLAST DB cached in RAM (80% of available)."""
        kubectl = self._kubectl()
        dry_run = self.cfg.cluster.dry_run

        # Skip if already deployed
        if not dry_run:
            try:
                proc = safe_exec(shlex.split(
                    f'{kubectl} get daemonset vmtouch-db-cache --ignore-not-found -o name'))
                if self._decode(proc.stdout).strip():
                    return
            except Exception:
                pass

        db, _, _ = get_blastdb_info(self.cfg.blast.db)
        subs = {
            'ELB_DOCKER_IMAGE': self.cfg.azure.elb_docker_image,
            'ELB_DB': db,
            'ELB_DB_MOL_TYPE': str(ElbSupportedPrograms().get_db_mol_type(self.cfg.blast.program)),
        }

        from importlib.resources import files as pkg_files
        template = pkg_files('elastic_blast').joinpath(
            'templates/vmtouch-daemonset-aks.yaml.template').read_text()
        yaml_content = substitute_params(template, subs)

        with TemporaryDirectory() as d:
            path = os.path.join(d, 'vmtouch-daemonset.yaml')
            with open(path, 'w') as f:
                f.write(yaml_content)
            if dry_run:
                logging.info(f'Would apply {path}')
            else:
                safe_exec(shlex.split(f'{kubectl} apply -f {path}'))

    def _label_nodes(self) -> None:
        """Label nodes with ordinal index for local-SSD affinity."""
        if not self.cfg.cluster.use_local_ssd:
            return
        kubectl = self._kubectl()
        if self.cfg.cluster.dry_run:
            return
        proc = safe_exec(f"{kubectl} get nodes -o jsonpath='{{.items[*].metadata.name}}'")
        for i, name in enumerate(handle_error(proc.stdout).replace("'", "").split()):
            safe_exec(f'{kubectl} label nodes {name} ordinal={i} --overwrite')


# ---------------------------------------------------------------------------
# Module-level functions (thin wrappers around azure_sdk)
# ---------------------------------------------------------------------------

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _retry_exec(cmd):
    """Execute a command with exponential backoff retry."""
    return safe_exec(cmd)


def set_gcp_project(project: str) -> None:
    """No-op for Azure. Retained for interface compatibility."""
    pass


def get_disks(cfg: ElasticBlastConfig, dry_run: bool = False) -> List[str]:
    return sdk.get_disks(cfg.azure.resourcegroup, dry_run)


def get_snapshots(cfg: ElasticBlastConfig, dry_run: bool = False) -> List[str]:
    return sdk.get_snapshots(cfg.azure.resourcegroup, dry_run)


def delete_disk(name: str, cfg: ElasticBlastConfig) -> None:
    if not name:
        raise ValueError('No disk name provided')
    if not cfg:
        raise ValueError('No application config provided')
    sdk.delete_disk(cfg.azure.resourcegroup, name)


def delete_snapshot(name: str, cfg: ElasticBlastConfig) -> None:
    if not name:
        raise ValueError('No snapshot name provided')
    if not cfg:
        raise ValueError('No application config provided')
    sdk.delete_snapshot(cfg.azure.resourcegroup, name)


def get_aks_clusters(cfg: ElasticBlastConfig) -> List[str]:
    return sdk.get_aks_clusters(cfg.azure.resourcegroup, cfg.cluster.dry_run)


def get_aks_credentials(cfg: ElasticBlastConfig) -> str:
    return sdk.get_aks_credentials(cfg.azure.resourcegroup, cfg.cluster.name,
                                   cfg.cluster.dry_run)


def set_role_assignment(cfg: ElasticBlastConfig):
    sdk.set_role_assignments(cfg.azure.resourcegroup, cfg.cluster.name,
                             cfg.azure.storage_account,
                             cfg.azure.acr_name, cfg.azure.acr_resourcegroup,
                             cfg.cluster.dry_run)


def check_cluster(cfg: ElasticBlastConfig) -> str:
    return sdk.check_cluster(cfg.azure.resourcegroup, cfg.cluster.name,
                             cfg.cluster.dry_run)


def start_cluster(cfg: ElasticBlastConfig) -> str:
    """Create AKS cluster. Blocks until provisioning completes."""
    poller = start_cluster_async(cfg)
    wait_for_cluster(cfg, poller)
    return cfg.cluster.name


def start_cluster_async(cfg: ElasticBlastConfig):
    """Start AKS cluster creation (non-blocking). Returns poller or None."""
    name = cfg.cluster.name
    if not name:
        raise ValueError('Missing cluster name')
    if not cfg.cluster.machine_type:
        raise ValueError('Missing machine-type')

    tags = {}
    if cfg.cluster.labels:
        for pair in cfg.cluster.labels.split(','):
            k, _, v = pair.partition('=')
            tags[k.strip()] = v.strip()

    return sdk.start_cluster(
        resource_group=cfg.azure.resourcegroup, cluster_name=name,
        location=cfg.azure.region, machine_type=cfg.cluster.machine_type,
        num_nodes=cfg.cluster.num_nodes or 1,
        use_local_ssd=cfg.cluster.use_local_ssd,
        use_spot=cfg.cluster.use_preemptible, tags=tags,
        k8s_version=cfg.azure.aks_version or None,
        dry_run=cfg.cluster.dry_run,
    )


def wait_for_cluster(cfg: ElasticBlastConfig, poller) -> None:
    """Block until AKS cluster is ready."""
    if poller is None:
        return
    start = timer()
    logging.info(f'Waiting for AKS cluster {cfg.cluster.name}...')
    try:
        poller.result()
    except Exception as e:
        elapsed = timer() - start
        logging.debug(f'RUNTIME cluster-create-failed {elapsed:.1f}s')
        raise UserReportError(CLUSTER_ERROR,
            f'AKS cluster {cfg.cluster.name} creation failed after {elapsed:.0f}s: {e}') from e
    elapsed = timer() - start
    logging.debug(f'RUNTIME cluster-create {elapsed:.1f}s')
    monitor.track_cluster_created(
        cluster_name=cfg.cluster.name, duration_s=elapsed,
        num_nodes=cfg.cluster.num_nodes or 1, machine_type=cfg.cluster.machine_type)


def delete_cluster(cfg: ElasticBlastConfig) -> str:
    """Delete AKS cluster. Blocks until deletion completes."""
    name = cfg.cluster.name
    start = timer()
    poller = sdk.delete_cluster(cfg.azure.resourcegroup, name, cfg.cluster.dry_run)
    if poller:
        poller.result()
    elapsed = timer() - start
    logging.debug(f'RUNTIME cluster-delete {elapsed:.1f}s')
    monitor.track_cluster_deleted(cluster_name=name, duration_s=elapsed)
    return name


def check_prerequisites() -> None:
    sdk.check_prerequisites()


# ---------------------------------------------------------------------------
# Resource cleanup
# ---------------------------------------------------------------------------

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _get_resource_ids(cfg: ElasticBlastConfig) -> ResourceIds:
    """Fetch persistent disk/snapshot IDs from Blob Storage metadata."""
    if cfg.appstate.resources.disks and cfg.appstate.resources.snapshots:
        return cfg.appstate.resources
    path = os.path.join(cfg.cluster.results, cfg.azure.elb_job_id,
                        ELB_METADATA_DIR, ELB_STATE_DISK_ID_FILE)
    try:
        from .filehelper import open_for_read
        with open_for_read(path) as f:
            retval = ResourceIds.from_json(f.read())
        if retval.disks or retval.snapshots:
            return retval
    except Exception as e:
        logging.debug(f'Unable to read {path}: {e}')
    return ResourceIds()


def delete_cluster_with_cleanup(cfg: ElasticBlastConfig, allow_missing: bool = False) -> None:
    """Delete AKS cluster and clean up persistent disks/snapshots.

    Args:
        allow_missing: If True, skip gracefully when cluster not found.
                       Used by cleanup stack to avoid hiding original errors.
    """
    dry_run = cfg.cluster.dry_run
    pds, snapshots = _collect_resource_ids(cfg)

    # Wait for cluster to reach a stable state
    try_kubernetes = _wait_for_cluster_ready(cfg, allow_missing=allow_missing)

    if try_kubernetes:
        pds, snapshots = _cleanup_k8s_resources(cfg, pds, snapshots, dry_run)

    _cleanup_leaked_disks(cfg, pds, snapshots, dry_run)
    remove_split_query(cfg)
    delete_cluster(cfg)


def _collect_resource_ids(cfg: ElasticBlastConfig) -> Tuple[List[str], List[str]]:
    """Gather disk and snapshot IDs from metadata."""
    try:
        res = _get_resource_ids(cfg)
        return res.disks, res.snapshots
    except Exception as e:
        logging.error(f'Unable to read resource IDs: {e}')
        return [], []


def _wait_for_cluster_ready(cfg: ElasticBlastConfig, allow_missing: bool = False) -> bool:
    """Wait for AKS cluster to reach a terminal state. Returns True if K8s is usable.

    Args:
        allow_missing: If True, return False instead of raising when cluster not found.
                       Used by cleanup stack where throwing would hide the original error.
    """
    while True:
        status = check_cluster(cfg)
        if not status:
            if cfg.cluster.dry_run:
                return False
            if allow_missing:
                logging.info(f'Cluster {cfg.cluster.name} not found, nothing to delete')
                return False
            elb = ElasticBlastAzure(cfg, False)
            result_status = elb._status_from_results()
            if result_status == ElbStatus.UNKNOWN:
                raise UserReportError(CLUSTER_ERROR,
                    f'Cluster {cfg.cluster.name} was not found')
            remove_split_query(cfg)
            return False

        if status == AKS_PROVISIONING_STATE.SUCCEEDED.value:
            return True
        if status in (AKS_PROVISIONING_STATE.FAILED.value,
                      AKS_PROVISIONING_STATE.STOPPING.value,
                      AKS_PROVISIONING_STATE.DELETING.value):
            return False
        if status in (AKS_PROVISIONING_STATE.STARTING.value,
                      AKS_PROVISIONING_STATE.UPDATING.value):
            time.sleep(10)
            continue

        logging.warning(f'Unrecognized cluster status: {status}')
        return True


def _cleanup_k8s_resources(cfg, pds, snapshots, dry_run):
    """Delete K8s objects and collect resource IDs."""
    try:
        cfg.appstate.k8s_ctx = get_aks_credentials(cfg)
        kubernetes.check_server(cfg.appstate.k8s_ctx, dry_run)
    except Exception as e:
        logging.warning(f'K8s connection failed: {e}')
        return pds, snapshots

    ctx = cfg.appstate.k8s_ctx
    assert ctx

    try:
        pds = kubernetes.get_persistent_disks(ctx, dry_run)
    except Exception as e:
        logging.warning(f'get_persistent_disks failed: {e}')

    try:
        snapshots = kubernetes.get_volume_snapshots(ctx, dry_run)
    except Exception as e:
        logging.warning(f'get_volume_snapshots failed: {e}')

    try:
        kubernetes.delete_all(ctx, dry_run)
    except Exception as e:
        logging.warning(f'kubernetes.delete_all failed: {e}')

    return pds, snapshots


def _cleanup_leaked_disks(cfg, pds, snapshots, dry_run):
    """Delete any disks/snapshots that survived K8s cleanup."""
    rg = cfg.azure.resourcegroup
    for disk in pds:
        try:
            if disk in get_disks(cfg, dry_run):
                delete_disk(disk, cfg)
        except Exception as e:
            logging.error(f'Failed to delete disk {disk}: {e}')
            _warn_leaked_resource('disk', disk, rg,
                f'az disk list --resource-group {rg} --query "[?name==\'{disk}\']" -o table',
                f'az disk delete -y --name {disk} --resource-group {rg}')

    for snap in snapshots:
        try:
            if snap in get_snapshots(cfg, dry_run):
                delete_snapshot(snap, cfg)
        except Exception as e:
            logging.error(f'Failed to delete snapshot {snap}: {e}')
            _warn_leaked_resource('snapshot', snap, rg,
                f'az snapshot list --resource-group {rg} --query "[?name==\'{snap}\']" -o table',
                f'az snapshot delete --name {snap} --resource-group {rg}')


def _warn_leaked_resource(kind: str, name: str, rg: str, check_cmd: str, delete_cmd: str):
    """Log a warning about a resource that couldn't be cleaned up."""
    logging.error(
        f'Could not delete {kind} "{name}". This may cause charges.\n'
        f'Verify: {check_cmd}\nDelete: {delete_cmd}')


def remove_split_query(cfg: ElasticBlastConfig) -> None:
    """Remove split query data from Blob Storage."""
    _remove_ancillary_data(cfg, ELB_QUERY_BATCH_DIR)


def _remove_ancillary_data(cfg: ElasticBlastConfig, bucket_prefix: str) -> None:
    """Remove data under a result subdirectory."""
    path = os.path.join(cfg.cluster.results, cfg.azure.elb_job_id, bucket_prefix)
    cmd = f'azcopy rm "{path}" --recursive=true'
    if cfg.cluster.dry_run:
        logging.info(cmd)
    else:
        try:
            safe_exec(shlex.split(cmd))
        except SafeExecError as e:
            logging.warning(e.message.strip().replace('\n', '|'))
