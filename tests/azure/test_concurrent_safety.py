"""
Regression tests for multi-request concurrency safety in the Azure path.

Covers fixes from 2026-05-15:
- AZUREConfig.elb_job_id must be per-instance (dataclass field default_factory)
- BLAST_ELB_JOB_ID and BLAST_ELB_JOB_ID_SHORT must be substituted in all
  AKS Job templates that have static names
- Rendered Job names must be DNS-1123 compliant and <=63 chars
- elb-finalizer-aks.sh must scope its kubectl wait/get to elb-job-id label
- _cleanup_stale_jobs / _cleanup_jobs_only must use elb-job-id label selector

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

import re
from importlib.resources import files as pkg_files

import pytest  # type: ignore

from elastic_blast.elb_config import AZUREConfig
from elastic_blast.subst import substitute_params


# Templates that have static metadata.name and therefore must include the
# BLAST_ELB_JOB_ID_SHORT suffix to be safe under concurrent submissions.
_PER_SUBMISSION_TEMPLATES = [
    'elb-finalizer-aks.yaml.template',
    'job-submit-jobs-aks.yaml.template',
    'blast-batch-job-aks.yaml.template',
    'blast-batch-job-local-ssd-aks.yaml.template',
    'blast-batch-job-shard-ssd-aks.yaml.template',
]

# Templates that have job/daemonset names that are intentionally cluster-shared
# (e.g., init-pv, init-ssd, vmtouch, create-workspace). These names must NOT
# include the per-submission suffix because the underlying resource (PVC or
# /workspace hostPath) can only host one DB at a time per cluster.
_CLUSTER_SHARED_NAMED_TEMPLATES = [
    'job-init-pv-aks.yaml.template',
    'job-init-pv-partitioned-aks.yaml.template',
    'job-init-local-ssd-aks.yaml.template',
    'job-init-ssd-shard-aks.yaml.template',
]

# All AKS templates expected to carry the elb-job-id label for cleanup scoping.
_LABELED_TEMPLATES = _PER_SUBMISSION_TEMPLATES + _CLUSTER_SHARED_NAMED_TEMPLATES


def _common_subs():
    job_id = 'job-' + 'a' * 32
    return {
        'BLAST_ELB_JOB_ID': job_id,
        'BLAST_ELB_JOB_ID_SHORT': job_id[-8:],
        'ELB_BLAST_PROGRAM': 'blastn',
        'ELB_DB_LABEL': 'pdbnt',
        'JOB_NUM': '0',
        'ELB_DB': 'pdbnt',
        'ELB_DOCKER_IMAGE': 'x',
        'ELB_RESULTS': 'http://x/y',
        'ELB_AZURE_RESOURCE_GROUP': 'rg',
        'ELB_CLUSTER_NAME': 'c',
        'ELB_REUSE_CLUSTER': 'false',
        'ELB_METADATA_DIR': 'metadata',
        'ELB_SERVICE_ACCOUNT': 'default',
        'ELB_DB_PARTITIONS': '0',
        'ELB_SHARD_IDX': '00',
        'ELB_PD_SIZE': '100Gi',
        'ELB_MEM_REQUEST': '4G',
        'ELB_MEM_LIMIT': '8G',
        'ELB_BLAST_OPTIONS': '',
        'ELB_BLAST_TIMEOUT': '600',
        'ELB_NUM_CPUS_REQ': '2',
        'ELB_NUM_CPUS': '4',
        'ELB_DB_MOL_TYPE': 'nucl',
        'ELB_TIMEFMT': '%s%N',
        'BLAST_ELB_VERSION': '1.5.0',
        'BLAST_USAGE_REPORT': 'true',
        'K8S_JOB_GET_BLASTDB': 'a',
        'K8S_JOB_LOAD_BLASTDB_INTO_RAM': 'b',
        'K8S_JOB_IMPORT_QUERY_BATCHES': 'c',
        'K8S_JOB_SUBMIT_JOBS': 'd',
        'K8S_JOB_BLAST': 'e',
        'K8S_JOB_RESULTS_EXPORT': 'f',
        'INPUT_QUERY': 'None',
        'BATCH_LEN': '100000',
        'COPY_ONLY': '1',
        'TIMEOUT': '600',
        'NODE_ORDINAL': '0',
        'ELB_IMAGE_QS': 'q',
        'ELB_DB_PATH': '',
        'ELB_TAX_DB_PATH': '',
        'ELB_BLASTDB_SRC': 'NCBI',
        'ELB_TAXIDLIST': '',
        'ELB_SC_NAME': 'managed-csi',
        'ELB_NUM_PARTITIONS': '4',
        'ELB_PARTITION_PREFIX': 'p',
        'QUERY_BATCHES': 'http://x/q',
        'ELB_NUM_NODES': '3',
        'ELB_USE_LOCAL_SSD': 'true',
        'ELB_LABELS': 'k=v',
        'GCP_PROJECT_OPT': '',
    }


def _read_template(name: str) -> str:
    return pkg_files('elastic_blast').joinpath(f'templates/{name}').read_text()


def _extract_metadata_names(rendered: str):
    """Yield each top-level metadata.name occurring at indent level 2 (Job/DaemonSet name)."""
    for m in re.finditer(r'^  name:\s*(\S+)\s*$', rendered, re.MULTILINE):
        yield m.group(1)


# ---------------------------------------------------------------------------
# AZUREConfig.elb_job_id must be unique per instance
# ---------------------------------------------------------------------------

def test_azureconfig_elb_job_id_is_per_instance():
    """Two AZUREConfig instances in the same process must have different
    elb_job_id values. Regression for the dataclass field eval-once bug."""
    a = AZUREConfig(region='koreacentral')
    b = AZUREConfig(region='koreacentral')
    assert a.elb_job_id != b.elb_job_id
    assert a.elb_job_id.startswith('job-')
    assert b.elb_job_id.startswith('job-')
    # Hex part is 32 chars
    assert len(a.elb_job_id) == len('job-') + 32


def test_azureconfig_elb_job_id_short_suffix_well_formed():
    """The 8-char suffix used in K8s Job names must be hex and DNS-1123 safe."""
    cfg = AZUREConfig(region='koreacentral')
    short = cfg.elb_job_id[-8:]
    assert len(short) == 8
    assert re.match(r'^[0-9a-f]{8}$', short)


# ---------------------------------------------------------------------------
# Per-submission templates: name must include the SHORT suffix and label
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('template', _PER_SUBMISSION_TEMPLATES)
def test_per_submission_template_renders_with_unique_name(template):
    subs = _common_subs()
    rendered = substitute_params(_read_template(template), subs)
    short = subs['BLAST_ELB_JOB_ID_SHORT']
    names = list(_extract_metadata_names(rendered))
    assert names, f'{template}: no top-level metadata.name found'
    for n in names:
        assert short in n, (
            f'{template}: name {n!r} missing per-submission suffix '
            f'{short!r} — concurrent submissions will collide')


@pytest.mark.parametrize('template', _LABELED_TEMPLATES)
def test_template_has_elb_job_id_label(template):
    subs = _common_subs()
    rendered = substitute_params(_read_template(template), subs)
    job_id = subs['BLAST_ELB_JOB_ID']
    assert f'elb-job-id: "{job_id}"' in rendered, (
        f'{template}: missing or unsubstituted elb-job-id label')


@pytest.mark.parametrize('template', _LABELED_TEMPLATES)
def test_rendered_names_are_dns1123_and_under_63_chars(template):
    subs = _common_subs()
    rendered = substitute_params(_read_template(template), subs)
    for n in _extract_metadata_names(rendered):
        assert len(n) <= 63, f'{template}: name {n!r} exceeds 63 chars ({len(n)})'
        assert re.match(r'^[a-z0-9]([a-z0-9-]*[a-z0-9])?$', n), (
            f'{template}: name {n!r} is not DNS-1123 compliant')


# ---------------------------------------------------------------------------
# Cluster-shared named templates: name must NOT include the SHORT suffix
# (init-pv / init-ssd map onto a single cluster-wide PVC or hostPath)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('template', _CLUSTER_SHARED_NAMED_TEMPLATES)
def test_cluster_shared_template_keeps_static_name(template):
    subs = _common_subs()
    rendered = substitute_params(_read_template(template), subs)
    short = subs['BLAST_ELB_JOB_ID_SHORT']
    # init-ssd templates use NODE_ORDINAL in the name, but never the SHORT id.
    for n in _extract_metadata_names(rendered):
        assert short not in n, (
            f'{template}: static cluster-shared name {n!r} unexpectedly '
            f'embeds per-submission suffix; this would hide the underlying '
            f'PVC / hostPath data race instead of preventing it')


# ---------------------------------------------------------------------------
# Finalizer script must scope kubectl to its own submission's BLAST jobs
# ---------------------------------------------------------------------------

def test_finalizer_script_uses_elb_job_id_label_selector():
    """Regression for the bug where the finalizer waited on app=blast,
    matching every BLAST job in the cluster (including other concurrent
    submissions) and writing SUCCESS/FAILURE based on their state."""
    script = pkg_files('elastic_blast').joinpath(
        'templates/scripts/elb-finalizer-aks.sh').read_text()
    # Must reference the env var and build a label selector with elb-job-id
    assert 'BLAST_ELB_JOB_ID' in script, (
        'finalizer script must reference BLAST_ELB_JOB_ID')
    assert 'elb-job-id=${BLAST_ELB_JOB_ID}' in script, (
        'finalizer script must filter kubectl by elb-job-id label')
    # Must NOT use bare `app=blast` for kubectl wait/get (would race)
    bad = re.findall(r'kubectl\s+(?:wait|get\s+jobs)\b[^\n]*-l\s+app=blast(?!\S)',
                     script)
    assert not bad, (
        f'finalizer script still uses unscoped `app=blast` selector: {bad}')


def test_finalizer_script_guards_zero_blast_jobs():
    """Regression for the 'no jobs = SUCCESS' race: the finalizer must
    confirm at least one BLAST job exists before declaring success.
    Otherwise a finalizer that races ahead of submit-jobs writes
    SUCCESS.txt before any BLAST work runs."""
    script = pkg_files('elastic_blast').joinpath(
        'templates/scripts/elb-finalizer-aks.sh').read_text()
    # Must inspect job count before waiting
    assert 'blast_count' in script, (
        'finalizer must count BLAST jobs before declaring SUCCESS')
    # Must short-circuit to FAILURE if no BLAST jobs ever appear
    assert 'BLAST_APPEAR_TIMEOUT' in script, (
        'finalizer must bound how long it waits for BLAST jobs to appear')
    assert 'no BLAST jobs appeared' in script, (
        'finalizer must produce a clear FAILURE reason when blast_count==0')
    # Must also catch submit-jobs Job failure (cause of zero BLAST jobs)
    assert 'submit_failed' in script, (
        'finalizer must detect submit-jobs failure as zero-jobs cause')


def test_finalizer_script_is_idempotent_on_existing_marker():
    """If a previous attempt already wrote SUCCESS.txt or FAILURE.txt the
    finalizer must exit without touching it, so retries can't flip the
    terminal state recorded for the user."""
    script = pkg_files('elastic_blast').joinpath(
        'templates/scripts/elb-finalizer-aks.sh').read_text()
    assert 'azcopy list' in script, (
        'finalizer must probe for an existing marker before writing one')
    assert 'SUCCESS.txt already present' in script
    assert 'FAILURE.txt already present' in script


def test_finalizer_job_backoff_limit_is_zero():
    """The finalizer is not safe to retry — backoffLimit must be 0 so a
    second pod cannot observe different cluster state and overwrite the
    first attempt's marker."""
    tmpl = pkg_files('elastic_blast').joinpath(
        'templates/elb-finalizer-aks.yaml.template').read_text()
    m = re.search(r'^\s*backoffLimit:\s*(\d+)\s*$', tmpl, re.MULTILINE)
    assert m, 'elb-finalizer template must declare backoffLimit'
    assert int(m.group(1)) == 0, (
        f'finalizer backoffLimit must be 0, got {m.group(1)} — retries '
        f'can race with the original attempt and corrupt the SUCCESS/FAILURE '
        f'marker')


def test_finalizer_template_passes_blast_elb_job_id_env():
    """The finalizer container must receive BLAST_ELB_JOB_ID so the script
    above can build the scoped label selector."""
    rendered = substitute_params(_read_template('elb-finalizer-aks.yaml.template'),
                                 _common_subs())
    # Match the env entry: name: BLAST_ELB_JOB_ID followed by value: "<job-id>"
    assert re.search(
        r'-\s+name:\s+BLAST_ELB_JOB_ID\s*\n\s*value:\s*"job-a{32}"',
        rendered), 'finalizer container missing BLAST_ELB_JOB_ID env var'


# ---------------------------------------------------------------------------
# Cleanup methods must use elb-job-id label selector
# ---------------------------------------------------------------------------

def test_cleanup_stale_jobs_filters_by_elb_job_id():
    """_cleanup_stale_jobs must include elb-job-id=<id> in the kubectl
    delete selector so it cannot delete other submissions' running jobs."""
    src = pkg_files('elastic_blast').joinpath('azure.py').read_text()
    # Pull the function body
    m = re.search(r'def _cleanup_stale_jobs\(self\)[^\n]*\n(?:\s{4,}.*\n)+', src)
    assert m, '_cleanup_stale_jobs function not found'
    body = m.group(0)
    assert 'elb-job-id=' in body, (
        '_cleanup_stale_jobs must filter by elb-job-id label')
    assert 'app=' in body and ',' in body, (
        '_cleanup_stale_jobs must combine app=X with elb-job-id selector')


def test_cleanup_jobs_only_filters_by_elb_job_id():
    src = pkg_files('elastic_blast').joinpath('azure.py').read_text()
    m = re.search(r'def _cleanup_jobs_only\(self\)[^\n]*\n(?:\s{4,}.*\n)+', src)
    assert m, '_cleanup_jobs_only function not found'
    body = m.group(0)
    assert 'elb-job-id=' in body, (
        '_cleanup_jobs_only must filter by elb-job-id label')


# ---------------------------------------------------------------------------
# apply_profile() must NOT mutate process-global os.environ
# ---------------------------------------------------------------------------

def test_apply_profile_does_not_set_global_env_vars(monkeypatch):
    """apply_profile() previously set AZCOPY_CONCURRENCY_VALUE and
    ELB_SKIP_DB_VERIFY in os.environ, which raced between concurrent
    submissions in the same process."""
    from unittest.mock import MagicMock
    from elastic_blast.azure import apply_profile, OptimizationProfile

    monkeypatch.delenv('AZCOPY_CONCURRENCY_VALUE', raising=False)
    monkeypatch.delenv('ELB_SKIP_DB_VERIFY', raising=False)
    monkeypatch.setenv('ELB_QUERY_SIZE_GB', '1')
    monkeypatch.setenv('ELB_DB_SIZE_GB', '50')

    cfg = MagicMock()
    cfg.cluster.use_preemptible = False
    cfg.cluster.num_nodes = 1
    cfg.cluster.machine_type = 'Standard_E32s_v5'
    cfg.cluster.reuse = False
    cfg.blast.batch_len = 100000

    apply_profile(cfg, OptimizationProfile.COST)

    import os as _os
    assert 'AZCOPY_CONCURRENCY_VALUE' not in _os.environ, (
        'apply_profile must not mutate os.environ (multi-request race)')
    assert 'ELB_SKIP_DB_VERIFY' not in _os.environ, (
        'apply_profile must not mutate os.environ (multi-request race)')


# ---------------------------------------------------------------------------
# delete_cluster_with_cleanup must refuse if other submissions are active
# ---------------------------------------------------------------------------

def test_delete_cluster_aborts_when_other_submission_active(monkeypatch):
    """Calling delete on a cluster while another submission has active
    BLAST jobs must abort with a clear UserReportError, not silently
    destroy the other search's data."""
    from unittest.mock import MagicMock, patch
    from elastic_blast.util import UserReportError

    monkeypatch.delenv('ELB_FORCE_DELETE', raising=False)

    cfg = MagicMock()
    cfg.cluster.dry_run = False
    cfg.cluster.name = 'shared-cluster'
    cfg.azure.elb_job_id = 'job-mine'
    cfg.appstate.k8s_ctx = 'ctx'

    fake_proc = MagicMock()
    # Two other submissions, one of which has 1 active BLAST job
    fake_proc.stdout = (
        b'job-mine\t\t1\t\n'
        b'job-other\t1\t\t\n'
        b'job-other2\t\t\t1\n'
    )

    with patch('elastic_blast.azure.safe_exec', return_value=fake_proc):
        from elastic_blast.azure import _abort_if_other_submissions_active
        with pytest.raises(UserReportError) as exc:
            _abort_if_other_submissions_active(cfg)
        assert 'job-other' in str(exc.value.message)
        assert 'ELB_FORCE_DELETE' in str(exc.value.message)


def test_delete_cluster_allows_when_only_my_submission(monkeypatch):
    """If the only active jobs belong to my elb-job-id, do NOT abort —
    they are this submission's own jobs that the cluster delete will
    legitimately tear down."""
    from unittest.mock import MagicMock, patch

    monkeypatch.delenv('ELB_FORCE_DELETE', raising=False)

    cfg = MagicMock()
    cfg.cluster.dry_run = False
    cfg.cluster.name = 'mine'
    cfg.azure.elb_job_id = 'job-mine'
    cfg.appstate.k8s_ctx = 'ctx'

    fake_proc = MagicMock()
    fake_proc.stdout = b'job-mine\t3\t\t\n'

    with patch('elastic_blast.azure.safe_exec', return_value=fake_proc):
        from elastic_blast.azure import _abort_if_other_submissions_active
        # Should NOT raise
        _abort_if_other_submissions_active(cfg)


def test_delete_cluster_guard_skipped_without_kube_context(monkeypatch):
    """If the kubectl context has never been resolved, the guard must
    skip silently rather than trying to fetch credentials (which would
    rewrite ~/.kube/config as a side effect of a delete pre-flight)."""
    from unittest.mock import MagicMock, patch

    monkeypatch.delenv('ELB_FORCE_DELETE', raising=False)

    cfg = MagicMock()
    cfg.cluster.dry_run = False
    cfg.cluster.name = 'cold'
    cfg.azure.elb_job_id = 'job-mine'
    cfg.appstate.k8s_ctx = None  # never resolved

    with patch('elastic_blast.azure.safe_exec') as exec_mock:
        from elastic_blast.azure import _abort_if_other_submissions_active
        _abort_if_other_submissions_active(cfg)
        # We must NOT shell out for kubectl when no context is cached.
        exec_mock.assert_not_called()


def test_delete_cluster_force_override(monkeypatch):
    """ELB_FORCE_DELETE=1 must bypass the multi-submission guard."""
    from unittest.mock import MagicMock, patch

    monkeypatch.setenv('ELB_FORCE_DELETE', '1')

    with patch('elastic_blast.azure._abort_if_other_submissions_active') as guard, \
         patch('elastic_blast.azure._collect_resource_ids', return_value=([], [])), \
         patch('elastic_blast.azure._wait_for_cluster_ready', return_value=False), \
         patch('elastic_blast.azure._cleanup_leaked_disks'), \
         patch('elastic_blast.azure.remove_split_query'), \
         patch('elastic_blast.azure.delete_cluster'):
        cfg = MagicMock()
        cfg.cluster.dry_run = False
        from elastic_blast.azure import delete_cluster_with_cleanup
        delete_cluster_with_cleanup(cfg, allow_missing=False)
        guard.assert_not_called()
