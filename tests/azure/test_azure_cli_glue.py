"""
Tests for elastic_blast.azure_cli_glue — the CLI <-> azure_api adapter.

Covers:
  * JSON envelope shape & schema for submit/status/delete/capacity/health
  * Exit-code mapping per ErrorCategory and SubmitDecision
  * Idempotency short-circuit (terminal marker hit, active jobs hit)
  * Correlation context binding
  * Non-Azure passthrough (no JSON, no glue)

Author: Moon Hyuk Choi moonchoi@microsoft.com
"""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock, patch

import pytest  # type: ignore

from elastic_blast import azure_cli_glue
from elastic_blast.azure_api_types import (
    AzureApiError, CapacityReport, CapacityVerdict, ErrorCategory,
    HealthReport, HealthStatus, SearchPhase, StatusResult, SubmitDecision,
    SubmitResult,
)
from elastic_blast.constants import (
    CLUSTER_ERROR, CSP, DEPENDENCY_ERROR, INPUT_ERROR, NOT_READY_ERROR,
    UNKNOWN_ERROR,
)


def _azure_cfg(elb_job_id='job-deadbeefdeadbeefdeadbeefdeadbeef'):
    cfg = MagicMock()
    cfg.cloud_provider.cloud = CSP.AZURE
    cfg.azure.elb_job_id = elb_job_id
    cfg.cluster.name = 'elastic-blast'
    cfg.cluster.results = 'https://stg.blob.core.windows.net/blast-db/results'
    cfg.cluster.reuse = False
    cfg.cluster.dry_run = False
    cfg.appstate.k8s_ctx = 'ctx'
    return cfg


def _gcp_cfg():
    cfg = MagicMock()
    cfg.cloud_provider.cloud = CSP.GCP
    return cfg


def _last_json_line(captured: str) -> dict:
    """Pull the LAST non-empty line of stdout and JSON-decode it.

    Mirrors what elb-dashboard's terminal_run does: `result["stdout"]`
    .splitlines()[-1] -> json.loads.
    """
    lines = [ln for ln in captured.strip().splitlines() if ln.strip()]
    assert lines, f'no stdout captured: {captured!r}'
    return json.loads(lines[-1])


# ---------------------------------------------------------------------------
# Helper / introspection
# ---------------------------------------------------------------------------

def test_is_azure_true_for_azure_cfg():
    assert azure_cli_glue.is_azure(_azure_cfg()) is True


def test_is_azure_false_for_gcp():
    assert azure_cli_glue.is_azure(_gcp_cfg()) is False


def test_is_azure_swallows_attribute_errors():
    cfg = MagicMock()
    cfg.cloud_provider = None  # accessing .cloud will raise
    assert azure_cli_glue.is_azure(cfg) is False


# ---------------------------------------------------------------------------
# JSON emitter
# ---------------------------------------------------------------------------

def test_to_jsonable_handles_dataclass_enum_nested():
    result = SubmitResult(
        decision=SubmitDecision.ACCEPTED,
        correlation_id='cid', cluster_name='c', message='m',
        retry_after_seconds=42, queue_position=3,
        details={'nested': {'k': SubmitDecision.QUEUED}})
    out = azure_cli_glue._to_jsonable(result)
    assert out['decision'] == 'accepted'
    assert out['retry_after_seconds'] == 42
    assert out['details']['nested']['k'] == 'queued'


def test_emit_json_writes_single_line(capsys):
    azure_cli_glue.emit_json({'kind': 'x', 'a': 1, 'b': [1, 2]})
    captured = capsys.readouterr().out
    assert captured.count('\n') == 1
    payload = json.loads(captured.strip())
    assert payload == {'kind': 'x', 'a': 1, 'b': [1, 2]}


def test_emit_json_keys_sorted_for_diff_stability(capsys):
    # Stable key ordering helps consumers that diff snapshots.
    azure_cli_glue.emit_json({'b': 2, 'a': 1, 'kind': 'z'})
    out = capsys.readouterr().out.strip()
    assert out.index('"a"') < out.index('"b"') < out.index('"kind"')


# ---------------------------------------------------------------------------
# Correlation
# ---------------------------------------------------------------------------

def test_correlation_uses_cli_flag_when_set():
    cfg = _azure_cfg(elb_job_id='cfg-id')
    args = MagicMock(correlation_id='cli-id')
    with azure_cli_glue.correlation(args, cfg) as cid:
        assert cid == 'cli-id'
        from elastic_blast import azure_api
        assert azure_api.get_correlation_id() == 'cli-id'


def test_correlation_falls_back_to_elb_job_id():
    cfg = _azure_cfg(elb_job_id='cfg-id')
    args = MagicMock(correlation_id=None)
    with azure_cli_glue.correlation(args, cfg) as cid:
        assert cid == 'cfg-id'


def test_correlation_noop_for_non_azure():
    cfg = _gcp_cfg()
    args = MagicMock(correlation_id='ignored')
    with azure_cli_glue.correlation(args, cfg) as cid:
        assert cid == ''


# ---------------------------------------------------------------------------
# Submit: idempotency short-circuit
# ---------------------------------------------------------------------------

def test_submit_returns_already_done_on_success_marker(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, idempotency_key=None, correlation_id=None)
    sentinel_default = MagicMock(side_effect=AssertionError('must not call'))
    with patch.object(azure_cli_glue.azure_api, '_has_terminal_marker',
                      side_effect=lambda _cfg, *, marker: marker == 'SUCCESS.txt'):
        rc = azure_cli_glue.submit_command(
            args, cfg, [], default_submit=sentinel_default)
    assert rc == 0
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['kind'] == 'submit_result'
    assert payload['decision'] == 'already_done'
    assert payload['details']['terminal'] == 'SUCCESS'


def test_submit_returns_already_done_on_failure_marker(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, idempotency_key=None, correlation_id=None)
    with patch.object(azure_cli_glue.azure_api, '_has_terminal_marker',
                      side_effect=lambda _cfg, *, marker: marker == 'FAILURE.txt'):
        rc = azure_cli_glue.submit_command(
            args, cfg, [], default_submit=MagicMock())
    assert rc == 0
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['details']['terminal'] == 'FAILURE'


def test_submit_returns_resumed_when_active_jobs_present(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, idempotency_key=None, correlation_id=None)
    with patch.object(azure_cli_glue.azure_api, '_has_terminal_marker',
                      return_value=False), \
         patch.object(azure_cli_glue.azure_api, '_has_active_submission_jobs',
                      return_value=True):
        rc = azure_cli_glue.submit_command(
            args, cfg, [], default_submit=MagicMock(side_effect=AssertionError))
    assert rc == 0
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['decision'] == 'resumed'


def test_submit_idempotency_key_overrides_elb_job_id():
    cfg = _azure_cfg(elb_job_id='original-id')
    args = MagicMock(json=False, idempotency_key='new-key', correlation_id=None)
    with patch.object(azure_cli_glue.azure_api, '_has_terminal_marker',
                      return_value=False), \
         patch.object(azure_cli_glue.azure_api, '_has_active_submission_jobs',
                      return_value=False):
        default = MagicMock(return_value=0)
        azure_cli_glue.submit_command(
            args, cfg, [], default_submit=default)
    assert cfg.azure.elb_job_id == 'new-key'
    default.assert_called_once()


def test_submit_idempotency_probes_skipped_when_json_off(capsys):
    """Without --json we don't change CLI behavior even with an idem key."""
    cfg = _azure_cfg()
    args = MagicMock(json=False, idempotency_key='k', correlation_id=None)
    default = MagicMock(return_value=0)
    with patch.object(azure_cli_glue.azure_api, '_has_terminal_marker',
                      side_effect=AssertionError('should not probe')) as probe:
        rc = azure_cli_glue.submit_command(
            args, cfg, [], default_submit=default)
    assert rc == 0
    probe.assert_not_called()


# ---------------------------------------------------------------------------
# Submit: success/failure JSON envelopes
# ---------------------------------------------------------------------------

def test_submit_emits_accepted_envelope_on_success(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, idempotency_key=None, correlation_id='trace-1')
    with patch.object(azure_cli_glue.azure_api, '_has_terminal_marker',
                      return_value=False), \
         patch.object(azure_cli_glue.azure_api, '_has_active_submission_jobs',
                      return_value=False):
        rc = azure_cli_glue.submit_command(
            args, cfg, [], default_submit=lambda *a, **kw: 0)
    assert rc == 0
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['kind'] == 'submit_result'
    assert payload['decision'] == 'accepted'
    assert payload['correlation_id'] == 'trace-1'
    assert payload['cluster_name'] == 'elastic-blast'


def test_submit_maps_azureapierror_to_capacity_exit_code(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, idempotency_key=None, correlation_id=None)
    err = AzureApiError(ErrorCategory.CAPACITY, 'queue full',
                        retry_after_seconds=30)
    with patch.object(azure_cli_glue.azure_api, '_has_terminal_marker',
                      return_value=False), \
         patch.object(azure_cli_glue.azure_api, '_has_active_submission_jobs',
                      return_value=False):
        rc = azure_cli_glue.submit_command(
            args, cfg, [],
            default_submit=MagicMock(side_effect=err))
    assert rc == NOT_READY_ERROR
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['kind'] == 'error'
    assert payload['category'] == 'capacity'
    assert payload['retry_after_seconds'] == 30


def test_submit_maps_invalid_to_input_error(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, idempotency_key=None, correlation_id=None)
    err = AzureApiError(ErrorCategory.INVALID, 'bad config')
    with patch.object(azure_cli_glue.azure_api, '_has_terminal_marker',
                      return_value=False), \
         patch.object(azure_cli_glue.azure_api, '_has_active_submission_jobs',
                      return_value=False):
        rc = azure_cli_glue.submit_command(
            args, cfg, [], default_submit=MagicMock(side_effect=err))
    assert rc == INPUT_ERROR


def test_submit_maps_auth_to_dependency_error(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, idempotency_key=None, correlation_id=None)
    err = AzureApiError(ErrorCategory.AUTH, 'forbidden')
    with patch.object(azure_cli_glue.azure_api, '_has_terminal_marker',
                      return_value=False), \
         patch.object(azure_cli_glue.azure_api, '_has_active_submission_jobs',
                      return_value=False):
        rc = azure_cli_glue.submit_command(
            args, cfg, [], default_submit=MagicMock(side_effect=err))
    assert rc == DEPENDENCY_ERROR


def test_submit_unmapped_exception_re_raises_for_legacy_handler(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, idempotency_key=None, correlation_id=None)
    with patch.object(azure_cli_glue.azure_api, '_has_terminal_marker',
                      return_value=False), \
         patch.object(azure_cli_glue.azure_api, '_has_active_submission_jobs',
                      return_value=False):
        with pytest.raises(RuntimeError, match='boom'):
            azure_cli_glue.submit_command(
                args, cfg, [],
                default_submit=MagicMock(side_effect=RuntimeError('boom')))
    # JSON envelope still emitted before re-raise.
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['kind'] == 'error'
    assert payload['category'] == 'internal'


# ---------------------------------------------------------------------------
# Status routing
# ---------------------------------------------------------------------------

def test_status_passthrough_when_json_off():
    cfg = _azure_cfg()
    args = MagicMock(json=False, correlation_id=None)
    default = MagicMock(return_value=0)
    rc = azure_cli_glue.status_command(args, cfg, [], default_status=default)
    assert rc == 0
    default.assert_called_once()


def test_status_emits_structured_json(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, correlation_id=None)
    fake = StatusResult(
        correlation_id='cid', phase=SearchPhase.RUNNING,
        pending=1, running=2, succeeded=3, failed=0,
        results_uri='https://x/y',
        cluster_provisioning_state='Succeeded')
    with patch.object(azure_cli_glue.azure_api, 'get_status', return_value=fake):
        rc = azure_cli_glue.status_command(
            args, cfg, [], default_status=MagicMock(side_effect=AssertionError))
    assert rc == 0
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['kind'] == 'status_result'
    assert payload['phase'] == 'running'
    assert payload['running'] == 2


def test_status_maps_not_found_to_input_error_exit(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, correlation_id=None)
    err = AzureApiError(ErrorCategory.NOT_FOUND, 'no such job')
    with patch.object(azure_cli_glue.azure_api, 'get_status', side_effect=err):
        rc = azure_cli_glue.status_command(
            args, cfg, [], default_status=MagicMock())
    assert rc == INPUT_ERROR
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['category'] == 'not_found'


# ---------------------------------------------------------------------------
# Delete routing
# ---------------------------------------------------------------------------

def test_delete_passthrough_sets_force_env(monkeypatch):
    cfg = _azure_cfg()
    args = MagicMock(json=False, correlation_id=None, force=True)
    seen = {}
    def default(_a, _c, _s):
        import os
        seen['env'] = os.environ.get('ELB_FORCE_DELETE')
        return 0
    monkeypatch.delenv('ELB_FORCE_DELETE', raising=False)
    rc = azure_cli_glue.delete_command(args, cfg, [], default_delete=default)
    assert rc == 0
    assert seen['env'] == '1'
    # cleaned up afterwards
    import os
    assert 'ELB_FORCE_DELETE' not in os.environ


def test_delete_passthrough_no_env_when_force_false(monkeypatch):
    cfg = _azure_cfg()
    args = MagicMock(json=False, correlation_id=None, force=False)
    seen = {}
    def default(_a, _c, _s):
        import os
        seen['env'] = os.environ.get('ELB_FORCE_DELETE')
        return 0
    monkeypatch.delenv('ELB_FORCE_DELETE', raising=False)
    azure_cli_glue.delete_command(args, cfg, [], default_delete=default)
    assert seen['env'] is None


def test_delete_emits_structured_json_with_force(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, correlation_id=None, force=True)
    with patch.object(azure_cli_glue.azure_api, 'delete_search',
                      return_value={'correlation_id': 'cid',
                                    'cluster_name': 'c',
                                    'forced': True}) as mock_del:
        rc = azure_cli_glue.delete_command(
            args, cfg, [], default_delete=MagicMock(side_effect=AssertionError))
    assert rc == 0
    mock_del.assert_called_once()
    _, kwargs = mock_del.call_args
    assert kwargs == {'force': True}
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['kind'] == 'delete_result'
    assert payload['forced'] is True


def test_delete_json_mode_propagates_no_force_when_not_set(capsys):
    cfg = _azure_cfg()
    args = MagicMock(json=True, correlation_id=None, force=False)
    with patch.object(azure_cli_glue.azure_api, 'delete_search',
                      return_value={'forced': False}) as mock_del:
        azure_cli_glue.delete_command(
            args, cfg, [], default_delete=MagicMock())
    _, kwargs = mock_del.call_args
    assert kwargs == {'force': False}


# ---------------------------------------------------------------------------
# Capacity / health (read-only sub-actions)
# ---------------------------------------------------------------------------

def test_capacity_command_emits_report(capsys):
    cfg = _azure_cfg()
    rep = CapacityReport(
        verdict=CapacityVerdict.AVAILABLE, region='koreacentral',
        cluster_name='elastic-blast', cluster_state='Succeeded',
        active_submissions=1, queue_depth=0, max_concurrent=8)
    with patch.object(azure_cli_glue.azure_api, 'check_capacity',
                      return_value=rep):
        rc = azure_cli_glue.capacity_command(cfg)
    assert rc == 0
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['kind'] == 'capacity_report'
    assert payload['verdict'] == 'available'
    assert payload['active_submissions'] == 1


def test_capacity_command_handles_internal_error(capsys):
    cfg = _azure_cfg()
    with patch.object(azure_cli_glue.azure_api, 'check_capacity',
                      side_effect=RuntimeError('bad')):
        rc = azure_cli_glue.capacity_command(cfg)
    assert rc == UNKNOWN_ERROR
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['category'] == 'internal'


def test_health_command_emits_report(capsys):
    rep = HealthReport(
        status=HealthStatus.OK,
        checks={'kubectl': True, 'azcopy': True, 'credential': True},
        messages={})
    with patch.object(azure_cli_glue.azure_api, 'health_check',
                      return_value=rep):
        rc = azure_cli_glue.health_command()
    assert rc == 0
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['kind'] == 'health_report'
    assert payload['status'] == 'ok'
    assert payload['checks']['kubectl'] is True


def test_health_command_never_raises(capsys):
    with patch.object(azure_cli_glue.azure_api, 'health_check',
                      side_effect=RuntimeError('exploded')):
        rc = azure_cli_glue.health_command()
    assert rc == UNKNOWN_ERROR
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['category'] == 'internal'


# ---------------------------------------------------------------------------
# Exit-code map lock-in
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('category,expected', [
    (ErrorCategory.TRANSIENT, CLUSTER_ERROR),
    (ErrorCategory.CAPACITY, NOT_READY_ERROR),
    (ErrorCategory.INVALID, INPUT_ERROR),
    (ErrorCategory.AUTH, DEPENDENCY_ERROR),
    (ErrorCategory.NOT_FOUND, INPUT_ERROR),
    (ErrorCategory.CONFLICT, CLUSTER_ERROR),
    (ErrorCategory.PERMANENT, DEPENDENCY_ERROR),
    (ErrorCategory.INTERNAL, UNKNOWN_ERROR),
])
def test_category_exit_map_lockin(category, expected):
    assert azure_cli_glue._CATEGORY_EXIT[category] == expected


# ---------------------------------------------------------------------------
# Hardening: idempotency-key validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('key', [
    'job-deadbeef',
    'JOB-ABCDEF1234',                  # uppercase normalized to lowercase
    'a',                                # single char (>=2 required by regex? no, single OK)
    'a' * 63,                           # max length
    'job_with_dots.and-dashes',
])
def test_validate_idempotency_key_accepts_valid(key):
    out = azure_cli_glue.validate_idempotency_key(key)
    # Always lowercased.
    assert out == key.lower()
    # And DNS-1123 friendly.
    assert all(c.isalnum() or c in '-._' for c in out)


@pytest.mark.parametrize('bad', [
    '',
    '-leading-dash',
    'trailing-dash-',
    'has space',
    'has/slash',
    'has:colon',
    'has$shell',
    'a' * 64,                           # one over max
])
def test_validate_idempotency_key_rejects_invalid(bad):
    with pytest.raises(AzureApiError) as ei:
        azure_cli_glue.validate_idempotency_key(bad)
    assert ei.value.category == ErrorCategory.INVALID


def test_validate_idempotency_key_rejects_non_string():
    with pytest.raises(AzureApiError):
        azure_cli_glue.validate_idempotency_key(None)  # type: ignore[arg-type]
    with pytest.raises(AzureApiError):
        azure_cli_glue.validate_idempotency_key(12345)  # type: ignore[arg-type]


def test_submit_rejects_invalid_idempotency_key_with_structured_error(capsys):
    cfg = _azure_cfg(elb_job_id='original-id')
    args = MagicMock(json=True, idempotency_key='bad/key',
                     correlation_id=None)
    default = MagicMock(side_effect=AssertionError('must not run'))
    rc = azure_cli_glue.submit_command(args, cfg, [], default_submit=default)
    assert rc == INPUT_ERROR
    payload = _last_json_line(capsys.readouterr().out)
    assert payload['kind'] == 'error'
    assert payload['category'] == 'invalid'
    # cfg was NOT mutated when validation failed.
    assert cfg.azure.elb_job_id == 'original-id'


def test_submit_normalizes_uppercase_key_to_lowercase():
    cfg = _azure_cfg()
    args = MagicMock(json=False, idempotency_key='UPPER-KEY-1234',
                     correlation_id=None)
    azure_cli_glue.submit_command(args, cfg, [],
        default_submit=MagicMock(return_value=0))
    assert cfg.azure.elb_job_id == 'upper-key-1234'


def test_submit_logs_elb_job_id_mutation(caplog):
    cfg = _azure_cfg(elb_job_id='before-id')
    args = MagicMock(json=False, idempotency_key='after-id',
                     correlation_id=None)
    with caplog.at_level('INFO', logger='elastic_blast.azure_cli_glue'):
        azure_cli_glue.submit_command(args, cfg, [],
            default_submit=MagicMock(return_value=0))
    msgs = ' '.join(r.message for r in caplog.records)
    assert 'before-id' in msgs and 'after-id' in msgs


# ---------------------------------------------------------------------------
# Hardening: emit_json broken-pipe safety
# ---------------------------------------------------------------------------

def test_emit_json_swallows_broken_pipe(monkeypatch):
    class _Pipe:
        def write(self, _s):
            raise BrokenPipeError(32, 'pipe')
        def flush(self):
            raise BrokenPipeError(32, 'pipe')
    monkeypatch.setattr(azure_cli_glue.sys, 'stdout', _Pipe())
    # Should NOT raise.
    azure_cli_glue.emit_json({'kind': 'x'})


def test_emit_json_swallows_oserror(monkeypatch):
    class _Closed:
        def write(self, _s):
            raise OSError(9, 'bad fd')
        def flush(self):
            pass
    monkeypatch.setattr(azure_cli_glue.sys, 'stdout', _Closed())
    azure_cli_glue.emit_json({'kind': 'x'})  # must not raise


# ---------------------------------------------------------------------------
# Hardening: backward-compat alias
# ---------------------------------------------------------------------------

def test_emit_error_helper_alias_kept_for_external_callers():
    # We renamed _emit_error_and_exit -> _emit_error_envelope; the old
    # name must remain importable so any external script keeps working.
    assert azure_cli_glue._emit_error_and_exit is azure_cli_glue._emit_error_envelope
