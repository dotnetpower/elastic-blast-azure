import logging
import os
import shlex
import signal
import subprocess
import tempfile
import time
from threading import Event
from typing import Dict, List, Optional, Union


# Substring match (case-insensitive) — when found in an env var name the
# value is replaced with a fixed placeholder before the dict is logged.
# Defence in depth so a future caller passing storage keys, SAS tokens or
# bearer tokens via ``env=`` cannot leak them through DEBUG logs.
_SECRET_ENV_NAME_PARTS = (
    "key", "secret", "token", "password", "passwd", "pwd",
    "credential", "sas", "signature", "sig=",
    "azure_storage", "aws_secret", "aws_session",
    "connection_string", "connectionstring",
)


def _redact_env_for_log(env: Dict[str, str]) -> Dict[str, str]:
    """Return a copy of ``env`` with values of secret-looking keys masked."""
    redacted: Dict[str, str] = {}
    for key, value in env.items():
        lower = key.lower()
        if any(part in lower for part in _SECRET_ENV_NAME_PARTS):
            redacted[key] = "***REDACTED***"
        else:
            redacted[key] = value
    return redacted


def safe_exec(cmd: Union[List[str], str], env: Optional[Dict[str, str]] = None, timeout: Optional[float] = 600) -> subprocess.CompletedProcess:
    """Wrapper around subprocess.run that raises SafeExecError on errors from
    command line with error messages assembled from all available information

    Arguments:
        cmd: Command line
        env: Environment variables to set. Current environment will also be
        copied. Variables in env take priority if they appear in both
        os.environ and env.
    """
    if isinstance(cmd, str):
        # ``shlex.split`` honours quotes and escapes; the previous
        # ``cmd.split()`` corrupted any argument containing spaces or shell
        # metacharacters and quietly produced wrong argv.
        cmd = shlex.split(cmd)
    if not isinstance(cmd, list):
        raise ValueError('safe_exec "cmd" argument must be a list or string')

    run_env = None
    if env:
        run_env = os.environ | env
    try:
        logging.debug(' '.join(cmd))
        if env:
            logging.debug(_redact_env_for_log(env))
        p = subprocess.run(cmd, check=True, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, env=run_env, universal_newlines=True, timeout=timeout)
        
    except subprocess.CalledProcessError as e:
        msg = f'The command "{" ".join(e.cmd)}" returned with exit code {e.returncode}\n{handle_error(e.stderr)}\n{handle_error(e.stdout)}'
        if e.output is not None:
            msg = '\n'.join([msg, f'{handle_error(e.output)}'])
        raise RuntimeError(msg) from e
    except subprocess.TimeoutExpired as e:
        cmd_str = " ".join(e.cmd) if isinstance(e.cmd, list) else str(e.cmd)
        raise RuntimeError(f'Command timed out after {e.timeout}s: {cmd_str}') from e
   
    return p


def run_cancellable(
    cmd: Union[List[str], str],
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
    stop_event: Optional[Event] = None,
    poll_interval: float = 1.0,
) -> subprocess.CompletedProcess:
    """Run a long command that can be cancelled by another thread.

    Unlike ``safe_exec``, this helper is intended for ElasticBLAST submissions
    that may legitimately run for many hours. It has no default wall-clock
    timeout, writes stdout/stderr to temporary files to avoid pipe deadlocks,
    and kills the whole process group when ``stop_event`` is set.
    """

    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    if not isinstance(cmd, list):
        raise ValueError('run_cancellable "cmd" argument must be a list or string')

    run_env = os.environ | env if env else None
    deadline = time.monotonic() + timeout if timeout is not None else None

    with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stdout_file, \
            tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stderr_file:
        logging.debug(' '.join(cmd))
        proc = subprocess.Popen(
            cmd,
            stdout=stdout_file,
            stderr=stderr_file,
            env=run_env,
            universal_newlines=True,
            start_new_session=True,
        )

        def _read_outputs() -> tuple[str, str]:
            stdout_file.seek(0)
            stderr_file.seek(0)
            return stdout_file.read(), stderr_file.read()

        def _terminate(reason: str) -> None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
                proc.wait(timeout=10)
            except Exception:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    pass
                proc.wait(timeout=10)
            stdout, stderr = _read_outputs()
            raise RuntimeError(f'{reason}: {" ".join(cmd)}\n{handle_error(stderr)}\n{handle_error(stdout)}')

        while True:
            rc = proc.poll()
            if rc is not None:
                stdout, stderr = _read_outputs()
                if rc != 0:
                    msg = (
                        f'The command "{" ".join(cmd)}" returned with exit code {rc}\n'
                        f'{handle_error(stderr)}\n{handle_error(stdout)}'
                    )
                    raise RuntimeError(msg)
                return subprocess.CompletedProcess(cmd, rc, stdout, stderr)

            if stop_event is not None and stop_event.is_set():
                _terminate('Command cancelled')

            if deadline is not None and time.monotonic() >= deadline:
                _terminate(f'Command timed out after {timeout}s')

            time.sleep(poll_interval)

def handle_error(exp_obj):
    """Handle error and decode stderr if necessary."""
    
    if isinstance(exp_obj, bytes):
        exp_obj = exp_obj.decode()
    return exp_obj