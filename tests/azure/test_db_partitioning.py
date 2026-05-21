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

import gzip
import json
import os
import subprocess
import xml.etree.ElementTree as ET
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch, MagicMock
from tempfile import TemporaryDirectory
import pytest
from elastic_blast.azure import ElasticBlastAzure
from elastic_blast.constants import ElbCommand, AKS_PROVISIONING_STATE
from elastic_blast.elb_config import ElasticBlastConfig
from elastic_blast.db_metadata import DbMetadata
from elastic_blast import config, kubernetes
from elastic_blast.util import UserReportError

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'azure', 'data')
INI = os.path.join(DATA_DIR, 'test-cfg-file.ini')

PARTITION_PREFIX = 'https://stgelb.blob.core.windows.net/blast-db/mydb/part_'

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


def _make_cfg(db_partitions: int = 0, db_partition_prefix: str = '',
              reuse: bool = False, dry_run: bool = True,
              use_local_ssd: bool = False,
              skip_warmed_ssd_init: bool = False) -> ElasticBlastConfig:
    """Create a test config with optional partitioning settings.
    
    Mocks get_latest_dir and get_db_metadata to avoid Azure Storage
    connections during ElasticBlastConfig construction.
    """
    args = Namespace(cfg=INI)
    with patch('elastic_blast.elb_config.get_latest_dir', return_value='latest'), \
         patch('elastic_blast.elb_config.get_db_metadata', return_value=DB_METADATA):
        cfg = ElasticBlastConfig(config.configure(args), task=ElbCommand.SUBMIT)
    cfg.blast.db_partitions = db_partitions
    cfg.blast.db_partition_prefix = db_partition_prefix
    cfg.cluster.reuse = reuse
    cfg.cluster.dry_run = dry_run
    cfg.cluster.use_local_ssd = use_local_ssd
    cfg.cluster.skip_warmed_ssd_init = skip_warmed_ssd_init
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

    def test_cluster_skip_warmed_ssd_init_default(self):
        """Warm SSD init skip is opt-in."""
        cfg = _make_cfg()
        assert cfg.cluster.skip_warmed_ssd_init is False

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
        cfg.blast.options = '-outfmt 6'
        errors = []
        cfg.blast.validate(errors, ElbCommand.SUBMIT)
        # No partition-related errors (other errors may exist from test config)
        partition_errors = [e for e in errors if 'partition' in e.lower()]
        assert len(partition_errors) == 0

    def test_partitioned_mode_allows_xml_outfmt_5(self):
        """Partitioned merge accepts BLAST XML outfmt 5."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX)
        cfg.blast.options = '-outfmt 5'
        errors = []
        cfg.blast.validate(errors, ElbCommand.SUBMIT)
        assert not any('requires outfmt' in e for e in errors)

    def test_partitioned_mode_allows_tabular_std_outfmt_6(self):
        """Partitioned merge accepts default std-prefixed tabular outfmt 6."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX)
        cfg.blast.options = '-outfmt "6 std qlen"'
        errors = []
        cfg.blast.validate(errors, ElbCommand.SUBMIT)
        assert not any('requires outfmt' in e for e in errors)

    def test_partitioned_mode_rejects_custom_outfmt_6(self):
        """Partitioned merge rejects custom outfmt 6 column layouts."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX)
        cfg.blast.options = '-outfmt "6 qseqid sseqid evalue bitscore"'
        errors = []
        cfg.blast.validate(errors, ElbCommand.SUBMIT)
        assert any('requires outfmt' in e for e in errors)

    def test_partitioned_mode_rejects_unsupported_outfmt(self):
        """Partitioned merge rejects formats without a merge implementation."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX)
        cfg.blast.options = '-outfmt 7'
        errors = []
        cfg.blast.validate(errors, ElbCommand.SUBMIT)
        assert any('requires outfmt 5' in e for e in errors)

    def test_partitioned_mode_allows_extended_outfmt_6(self):
        """Partitioned merge accepts outfmt 6 with extra tabular fields."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX)
        cfg.blast.options = '-outfmt "6 std qcovs"'
        errors = []
        cfg.blast.validate(errors, ElbCommand.SUBMIT)
        assert not any('requires outfmt' in e for e in errors)


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
            assert 'exceeding limit' in str(exc_info.value.message)

    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    def test_partitioned_submit_always_deploys_finalizer(self, mock_usage):
        """Partitioned finalizer is required for merge even without auto-shutdown."""
        cfg = _make_cfg(db_partitions=2, db_partition_prefix=PARTITION_PREFIX)
        elb = ElasticBlastAzure(cfg)
        elb.cloud_job_submission = False
        elb.auto_shutdown = False
        elb.cluster_initialized = True

        with patch.object(elb, '_generate_partitioned_jobs') as mock_generate, \
             patch('elastic_blast.kubernetes.enable_service_account') as mock_sa, \
             patch.object(elb, '_submit_finalizer_job') as mock_finalizer:
            elb._submit_partitioned(['batch_000.fa'], 1000)

        mock_generate.assert_called_once_with(['batch_000.fa'])
        mock_sa.assert_called_once_with(cfg)
        mock_finalizer.assert_called_once_with()


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
    @patch('elastic_blast.azure.start_cluster_async')
    @patch('elastic_blast.azure.wait_for_cluster')
    @patch('elastic_blast.azure.set_role_assignment')
    @patch('elastic_blast.kubernetes.enable_service_account')
    @patch('elastic_blast.kubernetes.create_scripts_configmap')
    @patch('elastic_blast.kubernetes.initialize_storage_partitioned')
    def test_creates_cluster_and_inits_partitioned_storage(
            self, mock_init_part, mock_configmap, mock_sa, mock_role, mock_wait,
            mock_start_async, mock_check, mock_usage, mock_dbinfo):
        """Full cluster creation + partitioned storage init (async path)."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX)
        elb = ElasticBlastAzure(cfg)
        elb.cloud_job_submission = False

        with patch.object(elb, '_get_k8s_ctx', return_value='test-ctx'), \
             patch.object(elb, '_label_nodes'):
            elb._initialize_cluster_partitioned(['batch_000.fa'])

        mock_start_async.assert_called_once()
        mock_wait.assert_called_once()
        mock_sa.assert_called_once()
        mock_configmap.assert_called_once()
        mock_init_part.assert_called_once()

    @patch('elastic_blast.azure.get_blastdb_info', return_value=('testdb', '', 'testdb'))
    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    @patch('elastic_blast.azure.check_cluster', return_value=AKS_PROVISIONING_STATE.SUCCEEDED)
    @patch('elastic_blast.azure.start_cluster_async')
    @patch('elastic_blast.azure.wait_for_cluster')
    @patch('elastic_blast.azure.set_role_assignment')
    @patch('elastic_blast.kubernetes.enable_service_account')
    @patch('elastic_blast.kubernetes.create_scripts_configmap')
    @patch('elastic_blast.kubernetes.initialize_storage_partitioned')
    def test_reuse_skips_cluster_creation(
            self, mock_init_part, mock_configmap, mock_sa, mock_role, mock_wait,
            mock_start_async, mock_check, mock_usage, mock_dbinfo):
        """In reuse mode with existing cluster, skips start_cluster_async."""
        cfg = _make_cfg(db_partitions=4, db_partition_prefix=PARTITION_PREFIX, reuse=True)
        elb = ElasticBlastAzure(cfg)
        elb.cloud_job_submission = False

        with patch.object(elb, '_get_k8s_ctx', return_value='test-ctx'), \
             patch.object(elb, '_label_nodes'):
            elb._initialize_cluster_partitioned(['batch_000.fa'])

        mock_start_async.assert_not_called()
        mock_wait.assert_not_called()
        mock_sa.assert_called_once()
        mock_configmap.assert_called_once()
        mock_init_part.assert_called_once()


class TestFinalizerPartitionedResults:
    """Tests for partitioned result finalizer wiring and merge helper."""

    @staticmethod
    def _xml_result(query_id, hits):
        hit_xml = []
        for idx, (subject, evalue, bitscore) in enumerate(hits, start=1):
            hit_xml.append(f"""
                    <Hit>
                        <Hit_num>{idx}</Hit_num>
                        <Hit_id>{subject}</Hit_id>
                        <Hit_def>{subject}</Hit_def>
                        <Hit_accession>{subject}</Hit_accession>
                        <Hit_len>100</Hit_len>
                        <Hit_hsps>
                            <Hsp>
                                <Hsp_num>1</Hsp_num>
                                <Hsp_bit-score>{bitscore}</Hsp_bit-score>
                                <Hsp_score>{int(bitscore)}</Hsp_score>
                                <Hsp_evalue>{evalue}</Hsp_evalue>
                                <Hsp_query-from>1</Hsp_query-from>
                                <Hsp_query-to>10</Hsp_query-to>
                                <Hsp_hit-from>1</Hsp_hit-from>
                                <Hsp_hit-to>10</Hsp_hit-to>
                                <Hsp_identity>10</Hsp_identity>
                                <Hsp_align-len>10</Hsp_align-len>
                                <Hsp_qseq>AAAAAAAAAA</Hsp_qseq>
                                <Hsp_hseq>AAAAAAAAAA</Hsp_hseq>
                                <Hsp_midline>||||||||||</Hsp_midline>
                            </Hsp>
                        </Hit_hsps>
                    </Hit>""")
        return f"""<?xml version=\"1.0\"?>
<BlastOutput>
    <BlastOutput_program>blastn</BlastOutput_program>
    <BlastOutput_version>BLASTN 2.17.0+</BlastOutput_version>
    <BlastOutput_reference>reference</BlastOutput_reference>
    <BlastOutput_db>shard-db</BlastOutput_db>
    <BlastOutput_query-ID>{query_id}</BlastOutput_query-ID>
    <BlastOutput_query-def>{query_id}</BlastOutput_query-def>
    <BlastOutput_query-len>10</BlastOutput_query-len>
    <BlastOutput_param><Parameters /></BlastOutput_param>
    <BlastOutput_iterations>
        <Iteration>
            <Iteration_iter-num>1</Iteration_iter-num>
            <Iteration_query-ID>{query_id}</Iteration_query-ID>
            <Iteration_query-def>{query_id}</Iteration_query-def>
            <Iteration_query-len>10</Iteration_query-len>
            <Iteration_hits>{''.join(hit_xml)}
            </Iteration_hits>
            <Iteration_stat><Statistics /></Iteration_stat>
        </Iteration>
    </BlastOutput_iterations>
</BlastOutput>
"""

    @patch('elastic_blast.azure.get_usage_reporting', return_value=False)
    def test_finalizer_template_includes_blast_options(self, mock_usage):
        """Finalizer receives original BLAST options for max_target_seqs parsing."""
        cfg = _make_cfg(db_partitions=3, db_partition_prefix=PARTITION_PREFIX,
                        dry_run=False)
        cfg.blast.options = '-outfmt 6 -max_target_seqs 25'
        cfg.appstate.k8s_ctx = 'test-ctx'
        elb = ElasticBlastAzure(cfg)
        created_files = []

        def capture_safe_exec(cmd, **kwargs):
            cmd_str = ' '.join(cmd) if isinstance(cmd, list) else cmd
            yaml_path = cmd_str.split('-f ')[-1].strip()
            if os.path.exists(yaml_path):
                with open(yaml_path) as handle:
                    created_files.append(handle.read())
            result = MagicMock()
            result.stdout = b''
            return result

        with patch('elastic_blast.azure.safe_exec', side_effect=capture_safe_exec):
            elb._submit_finalizer_job()

        assert len(created_files) == 1
        yaml_content = created_files[0]
        assert 'name: ELB_BLAST_OPTIONS' in yaml_content
        assert '-outfmt 6 -max_target_seqs 25' in yaml_content
        assert 'name: ELB_DB_PARTITIONS' in yaml_content
        assert '"3"' in yaml_content

    def test_merge_script_respects_max_target_seqs_and_writes_report(self):
        """Merge helper keeps the best N tabular hits per query and reports stats."""
        script = Path(__file__).parents[2] / 'src/elastic_blast/templates/scripts/merge-sharded-results.sh'
        rows = [
            'q1\tsubject_c\t99\t10\t0\t0\t1\t10\t1\t10\t1e-20\t90\t44',
            'q1\tsubject_a\t99\t10\t0\t0\t1\t10\t1\t10\t1e-30\t70\t55',
            'q1\tsubject_b\t99\t10\t0\t0\t1\t10\t1\t10\t1e-20\t100\t66',
            'q2\tsubject_d\t99\t10\t0\t0\t1\t10\t1\t10\t1e-10\t80',
            'not\tenough\tcolumns',
        ]
        with TemporaryDirectory() as tmpdir:
            input_tsv = Path(tmpdir) / 'all_hits.tsv'
            output_gz = Path(tmpdir) / 'merged.out.gz'
            report_json = Path(tmpdir) / 'merge-report.json'
            input_tsv.write_text('\n'.join(rows) + '\n')

            subprocess.run([
                'bash', str(script), str(input_tsv), str(output_gz),
                str(report_json), '2', 'blastp', '-outfmt 6 -max_target_seqs 2',
            ], check=True)

            with gzip.open(output_gz, 'rt') as handle:
                merged = handle.read()
            report = json.loads(report_json.read_text())

        assert '# BLASTP' in merged
        assert 'subject_a' in merged
        assert 'subject_b' in merged
        assert '55' in merged
        assert '66' in merged
        assert 'subject_c' not in merged
        assert report['max_target_seqs'] == 2
        assert report['queries'] == 2
        assert report['total_input_hits'] == 5
        assert report['total_output_hits'] == 3
        assert report['unsupported_rows'] == 1

    def test_merge_script_writes_valid_xml_for_outfmt_5(self):
        """Merge helper keeps XML valid and applies max_target_seqs per query."""
        script = Path(__file__).parents[2] / 'src/elastic_blast/templates/scripts/merge-sharded-results.sh'
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            for shard, hits in {
                'shard_00': [('subject_slow', '1e-10', 80.0), ('subject_best', '1e-30', 70.0)],
                'shard_01': [('subject_bit', '1e-20', 100.0)],
            }.items():
                shard_dir = root / shard
                shard_dir.mkdir()
                with gzip.open(shard_dir / 'batch.out.gz', 'wt') as handle:
                    handle.write(self._xml_result('Query_1', hits))
            input_tsv = root / 'all_hits.tsv'
            input_tsv.write_text('')
            output_gz = root / 'merged.out.gz'
            report_json = root / 'merge-report.json'

            subprocess.run([
                'bash', str(script), str(input_tsv), str(output_gz),
                str(report_json), '2', 'blastn', '-outfmt 5 -max_target_seqs 2',
            ], check=True)

            with gzip.open(output_gz, 'rt') as handle:
                tree = ET.parse(handle)
            subjects = [node.text for node in tree.findall('.//Hit_id')]
            report = json.loads(report_json.read_text())

        assert subjects == ['subject_best', 'subject_bit']
        assert report['outfmt'] == 5
        assert report['format'] == 'blast_xml'
        assert report['max_target_seqs'] == 2
        assert report['queries'] == 1
        assert report['total_input_hits'] == 3
        assert report['total_output_hits'] == 2
        assert report['ranking_basis'] == 'best_hsp_evalue_bitscore_ordinal'

    def test_merge_script_rejects_malformed_xml_shard(self):
        """Malformed XML shard output is fatal to avoid partial success markers."""
        script = Path(__file__).parents[2] / 'src/elastic_blast/templates/scripts/merge-sharded-results.sh'
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shard_dir = root / 'shard_00'
            shard_dir.mkdir()
            with gzip.open(shard_dir / 'batch.out.gz', 'wt') as handle:
                handle.write('<BlastOutput><BlastOutput_iterations>')
            input_tsv = root / 'all_hits.tsv'
            input_tsv.write_text('')
            output_gz = root / 'merged.out.gz'
            report_json = root / 'merge-report.json'

            result = subprocess.run([
                'bash', str(script), str(input_tsv), str(output_gz),
                str(report_json), '1', 'blastn', '-outfmt 5 -max_target_seqs 2',
            ], check=False, capture_output=True, text=True)
            output_exists = output_gz.exists()

        assert result.returncode != 0
        assert 'Malformed XML result' in result.stderr
        assert not output_exists

    def test_merge_script_rejects_empty_xml_query_id(self):
        """XML records without query identity are audited and skipped."""
        script = Path(__file__).parents[2] / 'src/elastic_blast/templates/scripts/merge-sharded-results.sh'
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shard_dir = root / 'shard_00'
            shard_dir.mkdir()
            with gzip.open(shard_dir / 'batch.out.gz', 'wt') as handle:
                handle.write(self._xml_result('', [('subject_a', '1e-30', 70.0)]))
            input_tsv = root / 'all_hits.tsv'
            input_tsv.write_text('')
            output_gz = root / 'merged.out.gz'
            report_json = root / 'merge-report.json'

            subprocess.run([
                'bash', str(script), str(input_tsv), str(output_gz),
                str(report_json), '1', 'blastn', '-outfmt 5 -max_target_seqs 2',
            ], check=True)
            report = json.loads(report_json.read_text())

        assert report['queries'] == 0
        assert report['unsupported_records'] == 1
        assert report['total_output_hits'] == 0

    def test_merge_script_xml_tie_prefers_more_hsps(self):
        """XML tie handling prefers richer hits before falling back to ordinal."""
        script = Path(__file__).parents[2] / 'src/elastic_blast/templates/scripts/merge-sharded-results.sh'
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            shard_dir = root / 'shard_00'
            shard_dir.mkdir()
            one_hsp = self._xml_result('Query_1', [('subject_one_hsp', '1e-20', 80.0)])
            two_hsp = self._xml_result('Query_1', [('subject_two_hsp', '1e-20', 80.0)]).replace(
                '</Hit_hsps>',
                '''<Hsp>
                <Hsp_num>2</Hsp_num>
                <Hsp_bit-score>75.0</Hsp_bit-score>
                <Hsp_score>75</Hsp_score>
                <Hsp_evalue>1e-19</Hsp_evalue>
                <Hsp_query-from>1</Hsp_query-from>
                <Hsp_query-to>10</Hsp_query-to>
                <Hsp_hit-from>1</Hsp_hit-from>
                <Hsp_hit-to>10</Hsp_hit-to>
                <Hsp_identity>10</Hsp_identity>
                <Hsp_align-len>10</Hsp_align-len>
                <Hsp_qseq>AAAAAAAAAA</Hsp_qseq>
                <Hsp_hseq>AAAAAAAAAA</Hsp_hseq>
                <Hsp_midline>||||||||||</Hsp_midline>
              </Hsp></Hit_hsps>''',
            )
            with gzip.open(shard_dir / 'a.out.gz', 'wt') as handle:
                handle.write(one_hsp)
            with gzip.open(shard_dir / 'b.out.gz', 'wt') as handle:
                handle.write(two_hsp)
            input_tsv = root / 'all_hits.tsv'
            input_tsv.write_text('')
            output_gz = root / 'merged.out.gz'
            report_json = root / 'merge-report.json'

            subprocess.run([
                'bash', str(script), str(input_tsv), str(output_gz),
                str(report_json), '1', 'blastn', '-outfmt 5 -max_target_seqs 1',
            ], check=True)
            with gzip.open(output_gz, 'rt') as handle:
                tree = ET.parse(handle)

        assert [node.text for node in tree.findall('.//Hit_id')] == ['subject_two_hsp']

    def test_finalizer_fails_incomplete_shard_downloads(self):
        """Finalizer script must not publish success when any shard output is missing."""
        script = Path(__file__).parents[2] / 'src/elastic_blast/templates/scripts/elb-finalizer-aks.sh'
        content = script.read_text()
        assert 'MISSING_SHARD_COUNT' in content
        assert 'READ_FAILURE_COUNT' in content
        assert 'incomplete shard results' in content


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

    def test_uses_template_with_configmap_scripts(self):
        """The init job YAML uses template with ConfigMap-mounted scripts."""
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

        # Verify the generated YAML uses ConfigMap scripts + correct env vars
        assert len(created_files) >= 1, 'Expected init job YAML to be captured'
        yaml_content = created_files[-1]
        # Template uses ConfigMap-mounted script instead of inline shell
        assert 'init-db-partitioned-aks.sh' in yaml_content, \
            'Expected ConfigMap script reference'
        assert 'elb-scripts' in yaml_content, \
            'Expected ConfigMap name in volumes'
        # Env vars pass partition info to the script
        assert 'ELB_NUM_PARTITIONS' in yaml_content
        assert '"3"' in yaml_content, \
            'Expected num_partitions=3 in env var'
        assert PARTITION_PREFIX in yaml_content, \
            'Expected partition prefix in env var'


class TestInitializeLocalSsdSharded:
    """Tests for kubernetes.initialize_local_ssd_sharded()."""

    def test_skips_init_jobs_when_dashboard_warmup_ready(self):
        """Dashboard-warmed shards can skip redundant init-ssd jobs."""
        cfg = _make_cfg(db_partitions=3, db_partition_prefix=PARTITION_PREFIX,
                        reuse=True, dry_run=False, use_local_ssd=True,
                        skip_warmed_ssd_init=True)
        cfg.appstate.k8s_ctx = 'test-ctx'
        cfg.blast.db = 'RNAvirome.S2.RDRP'

        warmup_payload = {
            'items': [
                {
                    'metadata': {
                        'labels': {
                            'app': 'elb-db-warmup',
                            'db': 'RNAvirome.S2.RDRP',
                            'shard': shard,
                        },
                        'annotations': {'elb.dashboard/source-version': 'v1'},
                    },
                    'status': {'succeeded': 1},
                }
                for shard in ('00', '01', '02')
            ]
        }
        safe_exec_calls = []

        def mock_safe_exec(cmd, **kwargs):
            cmd_str = cmd if isinstance(cmd, str) else ' '.join(cmd)
            safe_exec_calls.append(cmd_str)
            result = MagicMock()
            if 'get jobs -l app=elb-db-warmup' in cmd_str:
                result.stdout = json.dumps(warmup_payload).encode()
            else:
                result.stdout = b''
            return result

        with patch('elastic_blast.kubernetes.safe_exec', side_effect=mock_safe_exec):
            kubernetes.initialize_local_ssd_sharded(cfg, wait=False)

        assert any('get jobs -l app=elb-db-warmup,db=RNAvirome.S2.RDRP' in c
                   for c in safe_exec_calls)
        assert not any('apply -f' in c for c in safe_exec_calls)


class TestCreateScriptsConfigMap:
    """Tests for kubernetes.create_scripts_configmap()."""

    def test_creates_configmap_with_scripts(self):
        """ConfigMap should contain all .sh script files."""
        created_files = []

        def capture_safe_exec(cmd, **kwargs):
            if isinstance(cmd, str):
                cmd_str = cmd
            else:
                cmd_str = ' '.join(cmd)
            if 'apply -f' in cmd_str:
                yaml_path = cmd_str.split('-f ')[-1].strip()
                if os.path.exists(yaml_path):
                    with open(yaml_path) as f:
                        created_files.append(f.read())
            result = MagicMock()
            result.stdout = b''
            return result

        with patch('elastic_blast.kubernetes.safe_exec', side_effect=capture_safe_exec):
            kubernetes.create_scripts_configmap('test-ctx', dry_run=False)

        assert len(created_files) == 1, 'Expected ConfigMap YAML to be captured'
        cm_yaml = created_files[0]
        assert 'kind: ConfigMap' in cm_yaml
        assert 'name: elb-scripts' in cm_yaml
        # Check that the partitioning scripts are included.
        expected_scripts = [
            'init-db-download-aks.sh',
            'blast-vmtouch-aks.sh',
            'blast-run-aks.sh',
            'results-export-aks.sh',
            'query-download-ssd-aks.sh',
            'init-db-partitioned-aks.sh',
            'merge-sharded-results.sh',
        ]
        for script in expected_scripts:
            assert script in cm_yaml, f'Expected {script} in ConfigMap'

    def test_dry_run_does_not_call_kubectl(self):
        """Dry run should log but not execute kubectl."""
        with patch('elastic_blast.kubernetes.safe_exec') as mock_exec:
            kubernetes.create_scripts_configmap('test-ctx', dry_run=True)
            mock_exec.assert_not_called()

    def test_scripts_contain_valid_shebang(self):
        """All script files should have proper shebang line."""
        from importlib_resources import files as pkg_files
        scripts_dir = pkg_files('elastic_blast').joinpath('templates/scripts')
        for script_file in scripts_dir.iterdir():
            if script_file.name.endswith('.sh'):
                content = script_file.read_text()
                assert content.startswith('#!/bin/bash'), \
                    f'{script_file.name} missing #!/bin/bash shebang'


class TestMergePartitionedResults:
    """Tests for merge_partitioned_results()."""

    def test_merge_copies_partition_results(self):
        """Merging should call azcopy for each partition."""
        cfg = _make_cfg(db_partitions=3, db_partition_prefix=PARTITION_PREFIX, dry_run=False)
        elb = ElasticBlastAzure.__new__(ElasticBlastAzure)
        elb.cfg = cfg
        elb.dry_run = False
        elb.cleanup_stack = []

        with patch('elastic_blast.azure.safe_exec') as mock_exec:
            result = elb.merge_partitioned_results()
            assert mock_exec.call_count == 3
            assert len(result) == 3

    def test_merge_skips_when_no_partitions(self):
        """Should return empty list when not a partitioned search."""
        cfg = _make_cfg(db_partitions=0, dry_run=False)
        elb = ElasticBlastAzure.__new__(ElasticBlastAzure)
        elb.cfg = cfg
        elb.dry_run = False
        elb.cleanup_stack = []

        result = elb.merge_partitioned_results()
        assert result == []

    def test_merge_dry_run_logs_only(self):
        """Dry run should log commands without executing."""
        cfg = _make_cfg(db_partitions=2, db_partition_prefix=PARTITION_PREFIX, dry_run=True)
        elb = ElasticBlastAzure.__new__(ElasticBlastAzure)
        elb.cfg = cfg
        elb.dry_run = True
        elb.cleanup_stack = []

        with patch('elastic_blast.azure.safe_exec') as mock_exec:
            result = elb.merge_partitioned_results()
            mock_exec.assert_not_called()
            assert result == []
