"""
Tests for the public Azure API surface (azure_api / azure_api_types).

Covers:
  * submit_search idempotency (terminal marker, in-flight resume)
  * SubmissionGate backpressure semantics
  * Capacity report verdicts
  * health_check probe categories
  * Error mapping (HTTP status → ErrorCategory)
  * Correlation ID propagation through logs

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

from __future__ import annotations

import logging
import threading
import time
from unittest.mock import MagicMock, patch

import pytest  # type: ignore

from elastic_blast import azure_api
from elastic_blast.azure_api import (
    SubmissionGate, _map_exception, check_capacity, correlation_scope,
    delete_search, get_correlation_id, get_status, get_submission_gate,
    health_check, install_correlation_log_filter,
    reset_submission_gate_for_tests, submit_search,
)
from elastic_blast.azure_api_types import (
    AzureApiError, CapacityVerdict, ErrorCategory, HealthStatus,
    SearchPhase, SubmitDecision,
)


@pytest.fixture(autouse=True)
def _isolated_gate(monkeypatch):
    """Each test gets a fresh module-level submission gate."""
    monkeypatch.delenv('ELB_AZURE_MAX_CONCURRENT', raising=False)
    monkeypatch.delenv('ELB_AZURE_QUEUE_SIZE', raising=False)
    monkeypatch.delenv('ELB_FORCE_DELETE', raising=False)
    monkeypatch.delenv('ELB_CAPACITY_PROBE_QUOTA', raising=False)
    reset_submission_gate_for_tests()
    yield
    reset_submission_gate_for_tests()


def _stub_cfg(elb_job_id='job-abcdef0123456789abcdef0123456789'):
    cfg = MagicMock()
    cfg.azure.elb_job_id = elb_job_id
    cfg.azure.region = 'koreacentral'
    cfg.cluster.name = 'elastic-blast'
    cfg.cluster.results = 'https://stg.blob.core.windows.net/blast-db/results'
    cfg.cluster.dry_run = False
    cfg.appstate.k8s_ctx = 'ctx'
    cfg.blast.program = 'blastn'
    cfg.blast.db = 'pdbnt'
    return cfg


# ---------------------------------------------------------------------------
# SubmissionGate
# ---------------------------------------------------------------------------

class TestSubmissionGate:
    def test_basic_acquire_release(self):
        g = SubmissionGate(max_concurrent=2, queue_size=0)
        assert g.acquire(timeout=0.1)
        assert g.acquire(timeout=0.1)
        assert not g.acquire(timeout=0)
        g.release()
        assert g.acquire(timeout=0.1)

    def test_queue_capacity_limit_rejects_overflow(self):
        g = SubmissionGate(max_concurrent=1, queue_size=1)
        assert g.acquire(timeout=0.1)
        # 2nd waiter is allowed in the queue (would block) — simulate via thread
        ev = threading.Event()
        results = []

        def waiter():
            ok = g.acquire(timeout=0.5)
            results.append(ok)
            if ok:
                g.release()
            ev.set()

        t = threading.Thread(target=waiter)
        t.start()
        # While the waiter is queued, the next caller must be rejected.
        # Give the waiter a moment to enter acquire() and increment _waiters.
        time.sleep(0.05)
        rejected = g.acquire(timeout=0)
        assert rejected is False, 'overflow caller must not get a slot'
        g.release()  # let the waiter proceed
        ev.wait(timeout=2)
        t.join(timeout=2)
        assert results == [True]

    def test_slot_context_manager_raises_capacity_error(self):
        g = SubmissionGate(max_concurrent=1, queue_size=0)
        # Hold the only slot
        assert g.acquire(timeout=0.1)
        with pytest.raises(AzureApiError) as exc:
            with g.slot(timeout=0):
                pass
        assert exc.value.category == ErrorCategory.CAPACITY
        assert exc.value.retry_after_seconds and exc.value.retry_after_seconds > 0

    def test_invalid_settings(self):
        with pytest.raises(ValueError):
            SubmissionGate(max_concurrent=0, queue_size=0)
        with pytest.raises(ValueError):
            SubmissionGate(max_concurrent=1, queue_size=-1)


def test_get_submission_gate_honors_env(monkeypatch):
    monkeypatch.setenv('ELB_AZURE_MAX_CONCURRENT', '4')
    monkeypatch.setenv('ELB_AZURE_QUEUE_SIZE', '12')
    reset_submission_gate_for_tests()
    g = get_submission_gate()
    assert g.settings() == {'max_concurrent': 4, 'queue_size': 12}


# ---------------------------------------------------------------------------
# Correlation ID
# ---------------------------------------------------------------------------

class TestCorrelation:
    def test_scope_sets_and_clears(self):
        assert get_correlation_id() is None
        with correlation_scope('job-1'):
            assert get_correlation_id() == 'job-1'
            with correlation_scope('job-2'):
                assert get_correlation_id() == 'job-2'
            assert get_correlation_id() == 'job-1'
        assert get_correlation_id() is None

    def test_log_filter_attaches_id(self):
        logger = logging.getLogger('elastic_blast.test_corr')
        logger.handlers.clear()
        install_correlation_log_filter(logger)
        captured: list = []

        class _H(logging.Handler):
            def emit(self, record):
                captured.append(getattr(record, 'elb_correlation_id', None))

        logger.addHandler(_H())
        logger.setLevel(logging.INFO)
        with correlation_scope('job-X'):
            logger.info('hello')
        logger.info('outside')
        assert captured == ['job-X', '-']
        # Idempotent
        before = len(logger.filters)
        install_correlation_log_filter(logger)
        assert len(logger.filters) == before


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------

class TestErrorMapping:
    def test_passthrough(self):
        e = AzureApiError(ErrorCategory.AUTH, 'no token')
        out = _map_exception(e, correlation_id='cid')
        assert out is e
        assert e.correlation_id == 'cid'

    @pytest.mark.parametrize('status,category,is_retry', [
        (429, ErrorCategory.CAPACITY, True),
        (401, ErrorCategory.AUTH, False),
        (403, ErrorCategory.AUTH, False),
        (404, ErrorCategory.NOT_FOUND, False),
        (409, ErrorCategory.CONFLICT, False),
        (500, ErrorCategory.TRANSIENT, True),
        (503, ErrorCategory.TRANSIENT, True),
        (400, ErrorCategory.INVALID, False),
    ])
    def test_http_status_mapping(self, status, category, is_retry):
        from azure.core.exceptions import HttpResponseError  # type: ignore
        exc = HttpResponseError(message='boom')
        exc.status_code = status  # type: ignore
        out = _map_exception(exc, correlation_id='cid')
        assert out.category == category
        assert (out.retry_after_seconds is not None) == is_retry

    def test_user_report_error_mapping(self):
        from elastic_blast.util import UserReportError
        from elastic_blast.constants import (CLUSTER_ERROR, INPUT_ERROR,
                                              DEPENDENCY_ERROR)
        out = _map_exception(UserReportError(returncode=INPUT_ERROR, message='bad'),
                             correlation_id='cid')
        assert out.category == ErrorCategory.INVALID
        out = _map_exception(UserReportError(returncode=DEPENDENCY_ERROR, message='no kubectl'),
                             correlation_id='cid')
        assert out.category == ErrorCategory.PERMANENT
        out = _map_exception(UserReportError(returncode=CLUSTER_ERROR, message='unstable'),
                             correlation_id='cid')
        assert out.category == ErrorCategory.TRANSIENT
        assert out.retry_after_seconds and out.retry_after_seconds > 0

    def test_unknown_exception_is_internal(self):
        out = _map_exception(RuntimeError('weird'), correlation_id='cid')
        assert out.category == ErrorCategory.INTERNAL
        assert out.correlation_id == 'cid'


# ---------------------------------------------------------------------------
# submit_search idempotency
# ---------------------------------------------------------------------------

class TestSubmitIdempotency:
    def test_returns_already_done_on_success_marker(self):
        cfg = _stub_cfg()
        with patch('elastic_blast.azure_api._has_terminal_marker',
                   side_effect=lambda c, marker: marker == 'SUCCESS.txt'):
            result = submit_search(cfg)
        assert result.decision == SubmitDecision.ALREADY_DONE
        assert 'SUCCESS' in (result.details.get('terminal') or '')

    def test_returns_already_done_on_failure_marker(self):
        cfg = _stub_cfg()
        with patch('elastic_blast.azure_api._has_terminal_marker',
                   side_effect=lambda c, marker: marker == 'FAILURE.txt'):
            result = submit_search(cfg)
        assert result.decision == SubmitDecision.ALREADY_DONE
        assert result.details.get('terminal') == 'FAILURE'

    def test_returns_resumed_when_jobs_active(self):
        cfg = _stub_cfg()
        with patch('elastic_blast.azure_api._has_terminal_marker', return_value=False), \
             patch('elastic_blast.azure_api._has_active_submission_jobs', return_value=True):
            result = submit_search(cfg)
        assert result.decision == SubmitDecision.RESUMED

    def test_idempotency_key_overrides_elb_job_id(self):
        cfg = _stub_cfg(elb_job_id='job-original')
        with patch('elastic_blast.azure_api._has_terminal_marker', return_value=False), \
             patch('elastic_blast.azure_api._has_active_submission_jobs', return_value=True):
            result = submit_search(cfg, idempotency_key='job-from-caller')
        assert cfg.azure.elb_job_id == 'job-from-caller'
        assert result.correlation_id == 'job-from-caller'


# ---------------------------------------------------------------------------
# submit_search backpressure
# ---------------------------------------------------------------------------

class TestSubmitBackpressure:
    def test_rejects_when_gate_full(self, monkeypatch):
        monkeypatch.setenv('ELB_AZURE_MAX_CONCURRENT', '1')
        monkeypatch.setenv('ELB_AZURE_QUEUE_SIZE', '0')
        reset_submission_gate_for_tests()
        gate = get_submission_gate()
        assert gate.acquire(timeout=0.1)
        cfg = _stub_cfg()
        with patch('elastic_blast.azure_api._has_terminal_marker', return_value=False), \
             patch('elastic_blast.azure_api._has_active_submission_jobs', return_value=False):
            r = submit_search(cfg, gate_timeout_seconds=0)
        assert r.decision == SubmitDecision.REJECTED_CAPACITY
        assert r.retry_after_seconds and r.retry_after_seconds > 0


# ---------------------------------------------------------------------------
# submit_search delegation + error mapping
# ---------------------------------------------------------------------------

class TestSubmitDelegation:
    def test_accepted_when_underlying_submit_succeeds(self):
        cfg = _stub_cfg()
        fake_elb = MagicMock()
        with patch('elastic_blast.azure_api._has_terminal_marker', return_value=False), \
             patch('elastic_blast.azure_api._has_active_submission_jobs', return_value=False), \
             patch('elastic_blast.azure.ElasticBlastAzure', return_value=fake_elb):
            r = submit_search(cfg)
        assert r.decision == SubmitDecision.ACCEPTED
        fake_elb.submit.assert_called_once()

    def test_invalid_input_maps_to_rejected_invalid(self):
        from elastic_blast.util import UserReportError
        from elastic_blast.constants import INPUT_ERROR
        cfg = _stub_cfg()
        fake_elb = MagicMock()
        fake_elb.submit.side_effect = UserReportError(returncode=INPUT_ERROR,
                                                     message='bad query')
        with patch('elastic_blast.azure_api._has_terminal_marker', return_value=False), \
             patch('elastic_blast.azure_api._has_active_submission_jobs', return_value=False), \
             patch('elastic_blast.azure.ElasticBlastAzure', return_value=fake_elb):
            r = submit_search(cfg)
        assert r.decision == SubmitDecision.REJECTED_INVALID
        assert 'bad query' in r.message

    def test_dependency_error_maps_to_rejected_permanent(self):
        from elastic_blast.util import UserReportError
        from elastic_blast.constants import DEPENDENCY_ERROR
        cfg = _stub_cfg()
        fake_elb = MagicMock()
        fake_elb.submit.side_effect = UserReportError(returncode=DEPENDENCY_ERROR,
                                                     message='no azcopy')
        with patch('elastic_blast.azure_api._has_terminal_marker', return_value=False), \
             patch('elastic_blast.azure_api._has_active_submission_jobs', return_value=False), \
             patch('elastic_blast.azure.ElasticBlastAzure', return_value=fake_elb):
            r = submit_search(cfg)
        assert r.decision == SubmitDecision.REJECTED_PERMANENT

    def test_transient_error_propagates_as_api_error(self):
        from elastic_blast.util import UserReportError
        from elastic_blast.constants import CLUSTER_ERROR
        cfg = _stub_cfg()
        fake_elb = MagicMock()
        fake_elb.submit.side_effect = UserReportError(returncode=CLUSTER_ERROR,
                                                     message='cluster busy')
        with patch('elastic_blast.azure_api._has_terminal_marker', return_value=False), \
             patch('elastic_blast.azure_api._has_active_submission_jobs', return_value=False), \
             patch('elastic_blast.azure.ElasticBlastAzure', return_value=fake_elb):
            with pytest.raises(AzureApiError) as exc:
                submit_search(cfg)
        assert exc.value.category == ErrorCategory.TRANSIENT
        assert exc.value.retry_after_seconds and exc.value.retry_after_seconds > 0
        assert exc.value.correlation_id == cfg.azure.elb_job_id


# ---------------------------------------------------------------------------
# Capacity check
# ---------------------------------------------------------------------------

class TestCapacityCheck:
    def test_available_when_clean(self):
        cfg = _stub_cfg()
        with patch('elastic_blast.azure.check_cluster', return_value='Succeeded'), \
             patch('elastic_blast.azure_api._count_active_submissions_on_cluster',
                   return_value=0):
            r = check_capacity(cfg)
        assert r.verdict == CapacityVerdict.AVAILABLE
        assert r.cluster_state == 'Succeeded'

    def test_degraded_when_cluster_transitioning(self):
        cfg = _stub_cfg()
        with patch('elastic_blast.azure.check_cluster', return_value='Creating'), \
             patch('elastic_blast.azure_api._count_active_submissions_on_cluster',
                   return_value=0):
            r = check_capacity(cfg)
        assert r.verdict == CapacityVerdict.DEGRADED
        assert r.retry_after_seconds and r.retry_after_seconds > 0

    def test_intervention_when_cluster_failed(self):
        cfg = _stub_cfg()
        with patch('elastic_blast.azure.check_cluster', return_value='Failed'), \
             patch('elastic_blast.azure_api._count_active_submissions_on_cluster',
                   return_value=0):
            r = check_capacity(cfg)
        assert r.verdict == CapacityVerdict.EXHAUSTED_INTERVENTION_REQUIRED

    def test_unknown_when_state_probe_raises(self):
        cfg = _stub_cfg()
        with patch('elastic_blast.azure.check_cluster',
                   side_effect=RuntimeError('AKS down')):
            r = check_capacity(cfg)
        assert r.verdict == CapacityVerdict.UNKNOWN
        assert any('AKS state probe failed' in s for s in r.reasons)

    def test_exhausted_when_local_gate_at_queue_limit(self, monkeypatch):
        monkeypatch.setenv('ELB_AZURE_MAX_CONCURRENT', '1')
        monkeypatch.setenv('ELB_AZURE_QUEUE_SIZE', '0')
        reset_submission_gate_for_tests()
        gate = get_submission_gate()
        assert gate.acquire(timeout=0.1)

        # Need a second waiter so waiters >= max+queue
        ev = threading.Event()

        def waiter():
            ok = gate.acquire(timeout=1)
            if ok:
                gate.release()
            ev.set()

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.05)  # let waiter enter acquire()

        cfg = _stub_cfg()
        with patch('elastic_blast.azure.check_cluster', return_value='Succeeded'):
            r = check_capacity(cfg)
        assert r.verdict == CapacityVerdict.EXHAUSTED_RETRY_LATER
        gate.release()
        ev.wait(timeout=2)
        t.join(timeout=2)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class TestHealth:
    def test_ok_when_all_checks_pass(self):
        with patch('shutil.which', return_value='/usr/bin/x'), \
             patch('azure.identity.DefaultAzureCredential') as cred:
            cred.return_value.get_token.return_value = MagicMock()
            r = health_check()
        assert r.status == HealthStatus.OK
        assert all(r.checks.values())

    def test_degraded_when_only_azcopy_missing(self):
        def which(name):
            return None if name == 'azcopy' else '/usr/bin/' + name
        with patch('shutil.which', side_effect=which), \
             patch('azure.identity.DefaultAzureCredential') as cred:
            cred.return_value.get_token.return_value = MagicMock()
            r = health_check()
        assert r.status == HealthStatus.DEGRADED
        assert r.checks['kubectl_available'] is True
        assert r.checks['azcopy_available'] is False

    def test_unhealthy_when_kubectl_missing(self):
        def which(name):
            return None if name == 'kubectl' else '/usr/bin/' + name
        with patch('shutil.which', side_effect=which), \
             patch('azure.identity.DefaultAzureCredential') as cred:
            cred.return_value.get_token.return_value = MagicMock()
            r = health_check()
        assert r.status == HealthStatus.UNHEALTHY


# ---------------------------------------------------------------------------
# get_status / delete_search wrappers
# ---------------------------------------------------------------------------

class TestStatusWrapper:
    def test_maps_phase(self):
        from elastic_blast.constants import ElbStatus
        cfg = _stub_cfg()
        fake_elb = MagicMock()
        fake_elb.check_status.return_value = (ElbStatus.RUNNING,
                                              {'pending': 1, 'running': 2,
                                               'succeeded': 3, 'failed': 0},
                                              {})
        with patch('elastic_blast.azure.ElasticBlastAzure', return_value=fake_elb), \
             patch('elastic_blast.azure_api._safe_state', return_value='Succeeded'):
            r = get_status(cfg)
        assert r.phase == SearchPhase.RUNNING
        assert r.pending == 1 and r.running == 2 and r.succeeded == 3
        assert r.cluster_provisioning_state == 'Succeeded'

    def test_propagates_api_error_on_underlying_failure(self):
        from elastic_blast.util import UserReportError
        from elastic_blast.constants import CLUSTER_ERROR
        cfg = _stub_cfg()
        fake_elb = MagicMock()
        fake_elb.check_status.side_effect = UserReportError(returncode=CLUSTER_ERROR,
                                                           message='temporary')
        with patch('elastic_blast.azure.ElasticBlastAzure', return_value=fake_elb):
            with pytest.raises(AzureApiError) as exc:
                get_status(cfg)
        assert exc.value.category == ErrorCategory.TRANSIENT


class TestDeleteWrapper:
    def test_force_passes_through_to_helper(self, monkeypatch):
        """force=True must reach delete_cluster_with_cleanup as a kwarg,
        without touching os.environ (which would race between concurrent
        delete callers in the same process)."""
        monkeypatch.delenv('ELB_FORCE_DELETE', raising=False)
        cfg = _stub_cfg()
        cfg.cluster.reuse = False
        fake_elb = MagicMock()
        with patch('elastic_blast.azure.ElasticBlastAzure', return_value=fake_elb), \
             patch('elastic_blast.azure.delete_cluster_with_cleanup') as dcc:
            delete_search(cfg, force=True)
        dcc.assert_called_once()
        _, kwargs = dcc.call_args
        assert kwargs.get('force') is True
        # Env must be untouched
        import os as _os
        assert 'ELB_FORCE_DELETE' not in _os.environ

    def test_reuse_mode_uses_jobs_only_cleanup(self):
        cfg = _stub_cfg()
        cfg.cluster.reuse = True
        fake_elb = MagicMock()
        with patch('elastic_blast.azure.ElasticBlastAzure', return_value=fake_elb), \
             patch('elastic_blast.azure.delete_cluster_with_cleanup') as dcc:
            delete_search(cfg, force=True)
        dcc.assert_not_called()
        fake_elb._cleanup_jobs_only.assert_called_once()

    def test_force_false_does_not_pass_force(self):
        cfg = _stub_cfg()
        cfg.cluster.reuse = False
        with patch('elastic_blast.azure.ElasticBlastAzure', return_value=MagicMock()), \
             patch('elastic_blast.azure.delete_cluster_with_cleanup') as dcc:
            delete_search(cfg, force=False)
        _, kwargs = dcc.call_args
        assert kwargs.get('force') is False


# ---------------------------------------------------------------------------
# jsonpath escape lock-in (kubectl wants literal \t/\n inside the string)
# ---------------------------------------------------------------------------

class TestJsonpathEscape:
    def test_count_active_submissions_jsonpath_uses_literal_backslash(self):
        """The kubectl jsonpath expression must contain the *literal* two
        characters backslash + t (not a real tab character). Otherwise
        kubectl renders the field separator as a tab embedded in the
        expression — the output then has no separators and parsing fails."""
        cfg = _stub_cfg()
        captured = {}

        def fake_safe_exec(cmd, *a, **kw):
            captured['cmd'] = cmd
            proc = MagicMock()
            proc.stdout = b''
            return proc

        with patch('elastic_blast.util.safe_exec', fake_safe_exec):
            azure_api._count_active_submissions_on_cluster(cfg)

        # The -o argument is the last item; verify escape sequence
        out_arg = next(a for a in captured['cmd'] if a.startswith('jsonpath='))
        assert '\\t' in out_arg, (
            'jsonpath must include literal backslash-t for kubectl')
        assert '\t' not in out_arg, (
            'jsonpath must NOT contain an actual tab character')


# ---------------------------------------------------------------------------
# Independent submissions in the same process do not interfere
# ---------------------------------------------------------------------------

class TestConcurrentSubmits:
    def test_each_submit_gets_its_own_correlation_id(self, monkeypatch):
        """Two threads calling submit_search with different cfgs must end
        up with different correlation ids in their results — no cross-talk
        through the contextvar."""
        import os as _os
        monkeypatch.setenv('ELB_AZURE_MAX_CONCURRENT', '4')
        monkeypatch.setenv('ELB_AZURE_QUEUE_SIZE', '0')
        reset_submission_gate_for_tests()
        cfg_a = _stub_cfg('job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
        cfg_b = _stub_cfg('job-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')

        results = {}
        def run(name, cfg):
            with patch('elastic_blast.azure_api._has_terminal_marker', return_value=False), \
                 patch('elastic_blast.azure_api._has_active_submission_jobs', return_value=False), \
                 patch('elastic_blast.azure.ElasticBlastAzure', return_value=MagicMock()):
                results[name] = submit_search(cfg)

        t1 = threading.Thread(target=run, args=('a', cfg_a))
        t2 = threading.Thread(target=run, args=('b', cfg_b))
        t1.start(); t2.start()
        t1.join(timeout=5); t2.join(timeout=5)
        assert results['a'].correlation_id != results['b'].correlation_id
        assert results['a'].decision == SubmitDecision.ACCEPTED
        assert results['b'].decision == SubmitDecision.ACCEPTED
