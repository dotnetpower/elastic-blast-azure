#!/usr/bin/env python3
# ruff: noqa: E501
"""Patch the vendored elastic-blast-azure clone for dashboard sharded runs.

Responsibility: Patch the vendored elastic-blast-azure clone for dashboard sharded runs
Edit boundaries: Keep terminal-side behavior here; api/worker callers should use service
wrappers.
Key entry points: `_replace_once`, `_replace_once_unless_present`,
`_replace_all_unless_present`, `patch_azure_py`, `patch_azure_cli_glue`,
`patch_finalizer_template`, `patch_finalizer_script`,
`patch_kubectl_transient_retries`, `patch_disk_backed_monolithic_mode`,
`patch_requested_max_target_seqs`, `patch_aks_job_ttl`
Risky contracts: Do not expose terminal services directly to the internet or log secrets.
Validation: `uv run pytest -q api/tests/test_terminal_toolchain.py
api/tests/test_terminal_command_guard.py api/tests/test_terminal_patch_elastic_blast.py`.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path


def _replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected one match in {path}, found {count}")
    path.write_text(text.replace(old, new, 1))


def _replace_once_unless_present(
    path: Path, old: str, new: str, marker: str, *, allow_absent: bool = False
) -> None:
    text = path.read_text()
    if marker in text:
        return
    count = text.count(old)
    if count == 0 and allow_absent:
        return
    if count != 1:
        raise RuntimeError(f"expected one match in {path}, found {count}")
    path.write_text(text.replace(old, new, 1))


def _replace_all_unless_present(path: Path, old: str, new: str, marker: str) -> None:
    text = path.read_text()
    if marker in text:
        return
    count = text.count(old)
    if count < 1:
        raise RuntimeError(f"expected at least one match in {path}, found {count}")
    path.write_text(text.replace(old, new))


def _replace_block_once_unless_present(
    path: Path,
    *,
    start_marker: str,
    end_marker: str,
    replacement: str,
    marker: str,
) -> None:
    """Replace one bounded source block while failing closed on layout drift."""

    text = path.read_text()
    if marker in text:
        return
    if text.count(start_marker) != 1 or text.count(end_marker) != 1:
        raise RuntimeError(
            f"expected one bounded block in {path}, found "
            f"start={text.count(start_marker)} end={text.count(end_marker)}"
        )
    start = text.index(start_marker)
    end = text.index(end_marker, start) + len(end_marker)
    path.write_text(text[:start] + replacement + text[end:])


def patch_kubectl_transient_retries(root: Path) -> None:
    """Retry bounded, replay-safe kubectl operations on transient API failures."""
    path = root / "src/elastic_blast/util.py"
    _replace_once_unless_present(
        path,
        "import subprocess\nimport datetime\n",
        "import subprocess\nimport time\nimport datetime\n",
        "import time\nimport datetime\n",
    )
    _replace_once_unless_present(
        path,
        "def safe_exec(cmd: list[str] | str, env: dict[str, str] | None = None, timeout:",
        "def _safe_exec_once(cmd: list[str] | str, env: dict[str, str] | None = None, timeout:",
        "def _safe_exec_once(cmd:",
    )
    wrapper = '''

_KUBECTL_RETRYABLE_VERBS = frozenset({"apply", "delete", "get", "label", "logs"})
_KUBECTL_TRANSIENT_MARKERS = (
    "serviceunavailable",
    "too many requests",
    "toomanyrequests",
    "server is currently unable to handle the request",
    "internalerror",
    "gateway timeout",
    "connection reset",
    "unexpected eof",
    "tls handshake timeout",
    "i/o timeout",
)
_KUBECTL_NON_RETRYABLE_MARKERS = (
    "forbidden",
    "unauthorized",
    "authentication required",
    "permission denied",
    "invalid argument",
    "unknown flag",
)


def _kubectl_verb(argv: list[str]) -> str:
    options_with_values = {
        "--cluster",
        "--context",
        "--kubeconfig",
        "--namespace",
        "--request-timeout",
        "--server",
        "--token",
        "--user",
    }
    skip_value = False
    for arg in argv[1:]:
        if skip_value:
            skip_value = False
            continue
        if arg in options_with_values:
            skip_value = True
            continue
        if arg.startswith("-"):
            continue
        return arg
    return ""


def _kubectl_transient_failure(cmd: list[str] | str, exc: SafeExecError) -> bool:
    argv = cmd.split() if isinstance(cmd, str) else list(cmd)
    if not argv or os.path.basename(argv[0]) != "kubectl":
        return False
    verb = _kubectl_verb(argv)
    if verb not in _KUBECTL_RETRYABLE_VERBS:
        return False
    error = str(exc).lower()
    if any(marker in error for marker in _KUBECTL_NON_RETRYABLE_MARKERS):
        return False
    if verb == "delete" and not any(
        arg == "--ignore-not-found" or arg.startswith("--ignore-not-found=")
        for arg in argv
    ):
        return False
    if verb == "label" and "--overwrite" not in argv:
        return False
    return any(marker in error for marker in _KUBECTL_TRANSIENT_MARKERS) or bool(
        re.search(r"(?:^|[^0-9])(429|500|502|503|504)(?:[^0-9]|$)", error)
    )


def safe_exec(cmd: list[str] | str, env: dict[str, str] | None = None,
              timeout: float | None = 60) -> subprocess.CompletedProcess:
    """Run a command and retry replay-safe kubectl calls on transient failures."""
    import logging as retry_logging

    if not isinstance(cmd, (list, str)):
        return _safe_exec_once(cmd, env=env, timeout=timeout)
    argv = cmd.split() if isinstance(cmd, str) else list(cmd)
    if not argv or os.path.basename(argv[0]) != "kubectl":
        return _safe_exec_once(cmd, env=env, timeout=timeout)
    try:
        attempts = max(1, min(int(os.getenv("ELB_KUBECTL_TRANSIENT_ATTEMPTS", "6")), 6))
    except ValueError:
        attempts = 6
    try:
        deadline_seconds = max(
            1.0,
            min(float(os.getenv("ELB_KUBECTL_TRANSIENT_DEADLINE_SECONDS", "180")), 600.0),
        )
    except ValueError:
        deadline_seconds = 180.0
    started_at = time.monotonic()
    last_error: SafeExecError | None = None
    for attempt in range(1, attempts + 1):
        remaining = deadline_seconds - (time.monotonic() - started_at)
        if remaining <= 0:
            if last_error is not None:
                raise last_error
            raise SafeExecError(
                deadline_seconds,
                f"kubectl retry deadline exceeded after {deadline_seconds:g}s",
            )
        attempt_timeout = remaining if timeout is None else min(float(timeout), remaining)
        try:
            return _safe_exec_once(cmd, env=env, timeout=attempt_timeout)
        except SafeExecError as exc:
            last_error = exc
            if attempt >= attempts or not _kubectl_transient_failure(cmd, exc):
                raise
            delay = min(4, 2 ** (attempt - 1))
            remaining = deadline_seconds - (time.monotonic() - started_at)
            if remaining <= delay:
                raise
            retry_logging.warning(
                "Transient Kubernetes API failure; retrying kubectl verb=%s "
                "attempt %d/%d in %ds deadline=%gs",
                _kubectl_verb(argv),
                attempt + 1,
                attempts,
                delay,
                deadline_seconds,
            )
            time.sleep(delay)
    raise AssertionError("unreachable kubectl retry state")
'''
    _replace_once_unless_present(
        path,
        "    return p\n\ndef safe_exec_print(",
        f"    return p\n{wrapper}\ndef safe_exec_print(",
        "def _kubectl_transient_failure(",
    )


def patch_azure_py(root: Path) -> None:
    path = root / "src/elastic_blast/azure.py"
    _replace_once_unless_present(
        path,
        (
            "        # Deploy finalizer\n"
            "        if self.auto_shutdown:\n"
            "            self._submit_finalizer_job()\n"
        ),
        (
            "        # Deploy finalizer. In partitioned/sharded mode this is also the\n"
            "        # result-merger and terminal marker writer, not just an "
            "auto-shutdown hook.\n"
            "        self._submit_finalizer_job()\n"
        ),
        "result-merger and terminal marker writer",
        allow_absent=True,
    )
    _replace_once_unless_present(
        path,
        (
            "            'ELB_DB_PARTITIONS': str(cfg.blast.db_partitions) "
            "if cfg.blast.db_partitions > 0 else '0',\n"
            "            'ELB_BLAST_PROGRAM': cfg.blast.program,\n"
        ),
        (
            "            'ELB_DB_PARTITIONS': str(cfg.blast.db_partitions) "
            "if cfg.blast.db_partitions > 0 else '0',\n"
            "            'ELB_BLAST_PROGRAM': cfg.blast.program,\n"
            "            'ELB_BLAST_OPTIONS': cfg.blast.options,\n"
        ),
        "'ELB_BLAST_OPTIONS': cfg.blast.options",
    )
    _replace_once_unless_present(
        path,
        (
            "            'ELB_BLAST_PROGRAM': cfg.blast.program,\n"
            "            'ELB_BLAST_OPTIONS': cfg.blast.options,\n"
        ),
        (
            "            'ELB_BLAST_PROGRAM': cfg.blast.program,\n"
            "            'ELB_BLAST_OPTIONS': cfg.blast.options,\n"
            "            'ELB_REQUESTED_MAX_TARGET_SEQS': (\n"
            "                str(cfg.blast.requested_max_target_seqs)\n"
            "                if cfg.blast.requested_max_target_seqs > 0 else ''\n"
            "            ),\n"
        ),
        "'ELB_REQUESTED_MAX_TARGET_SEQS': (",
    )
    _replace_once_unless_present(
        path,
        (
            "            'ELB_REQUESTED_MAX_TARGET_SEQS': (\n"
            "                str(cfg.blast.requested_max_target_seqs)\n"
            "                if cfg.blast.requested_max_target_seqs > 0 else ''\n"
            "            ),\n"
        ),
        (
            "            'ELB_REQUESTED_MAX_TARGET_SEQS': (\n"
            "                str(cfg.blast.requested_max_target_seqs)\n"
            "                if cfg.blast.requested_max_target_seqs > 0 else ''\n"
            "            ),\n"
            "            'ELB_RESULT_SELECTION_POLICY': cfg.blast.result_selection_policy,\n"
        ),
        "'ELB_RESULT_SELECTION_POLICY': cfg.blast.result_selection_policy",
    )
    _replace_once_unless_present(
        path,
        "            'ELB_RESULT_SELECTION_POLICY': cfg.blast.result_selection_policy,\n",
        (
            "            'ELB_RESULT_SELECTION_POLICY': cfg.blast.result_selection_policy,\n"
            "            'ELB_CANDIDATE_POOL_SIZE_REQUESTED': (\n"
            "                str(cfg.blast.candidate_pool_size_requested)\n"
            "                if cfg.blast.candidate_pool_size_requested > 0 else ''\n"
            "            ),\n"
        ),
        "'ELB_CANDIDATE_POOL_SIZE_REQUESTED': (",
    )
    _replace_once_unless_present(
        path,
        (
            "        subs = {\n"
            "            'ELB_DOCKER_IMAGE': cfg.azure.elb_docker_image,\n"
            "            'ELB_RESULTS': self._results_path(),\n"
        ),
        (
            "        subs = {\n"
            "            'ELB_DOCKER_IMAGE': cfg.azure.elb_docker_image,\n"
            "            'ELB_FINALIZER_DOCKER_IMAGE': cfg.azure.cjs_docker_image,\n"
            "            'ELB_RESULTS': self._results_path(),\n"
        ),
        "'ELB_FINALIZER_DOCKER_IMAGE': cfg.azure.cjs_docker_image",
    )


def patch_partitioned_outfmt_gate(root: Path) -> None:
    """Allow tabular ``-outfmt 6``/``7`` (incl. extended layouts) for sharding.

    Upstream ``elb_config.py`` rejects every partitioned outfmt other than 5,
    6, or ``6 std...``. The dashboard shard merge
    (``merge-sharded-results.sh``) is field-aware: it resolves its group/rank/
    oracle columns BY NAME from the full ``-outfmt`` specifier and re-emits its
    own comment header, so any tabular ``6``/``7`` layout merges correctly as
    long as it carries ``evalue`` + ``bitscore`` (the merge fail-closes
    otherwise with a clear error). Widen the gate so the dashboard's New Search
    taxonomy toggle and a hand-written extended layout
    (e.g. ``7 qseqid sseqid staxids sstrand pident evalue bitscore``) can run
    sharded on both the internal and OpenAPI execution planes. outfmt 5 still
    rejects extended fields (the XML path has no field-list concept).
    """
    path = root / "src/elastic_blast/elb_config.py"
    _replace_once_unless_present(
        path,
        (
            "            if (\n"
            "                outfmt_code not in {'5', '6'}\n"
            "                or (outfmt_code == '5' and outfmt_extended)\n"
            "                or (outfmt_code == '6' and outfmt_extended and not "
            "outfmt_extended.startswith('std'))\n"
            "            ):\n"
            "                errors.append(\n"
            "                    'Partitioned BLAST requires outfmt 5 without extended fields, '\n"
            "                    'outfmt 6, or \"6 std...\"; '\n"
            "                    f'{outfmt} is not supported for merge')\n"
        ),
        (
            "            if (\n"
            "                outfmt_code not in {'5', '6', '7'}\n"
            "                or (outfmt_code == '5' and outfmt_extended)\n"
            "            ):\n"
            "                errors.append(\n"
            "                    'Partitioned BLAST requires outfmt 5 without extended fields, '\n"
            "                    'or tabular outfmt 6/7 (optionally with an extended field list); '\n"
            "                    f'{outfmt} is not supported for merge')\n"
        ),
        "outfmt_code not in {'5', '6', '7'}",
    )


def patch_disk_backed_monolithic_mode(root: Path) -> None:
    """Add an explicit, isolated one-partition local-SSD execution mode.

    A full ``core_nt`` database exceeds E16 RAM but fits its node-local disk.
    This mode keeps the pod memory limit in force, skips only the conservative
    database-fit-in-RAM check, disables eager page-cache warming, and uses a
    cache directory distinct from ordinary multi-shard runs.
    """

    config_path = root / "src/elastic_blast/elb_config.py"
    _replace_once_unless_present(
        config_path,
        "    db_partition_prefix: str = ''\n\n    # database metadata, not part of config\n",
        (
            "    db_partition_prefix: str = ''\n"
            "    disk_backed_monolithic: bool = False\n\n"
            "    # database metadata, not part of config\n"
        ),
        "disk_backed_monolithic: bool = False",
    )
    _replace_once_unless_present(
        config_path,
        (
            "               'db_partitions': ParamInfo(CFG_BLAST, CFG_BLAST_DB_PARTITIONS),\n"
            "               'db_partition_prefix': ParamInfo(CFG_BLAST, CFG_BLAST_DB_PARTITION_PREFIX)}\n"
        ),
        (
            "               'db_partitions': ParamInfo(CFG_BLAST, CFG_BLAST_DB_PARTITIONS),\n"
            "               'db_partition_prefix': ParamInfo(CFG_BLAST, CFG_BLAST_DB_PARTITION_PREFIX),\n"
            "               'disk_backed_monolithic': ParamInfo(CFG_BLAST, 'disk-backed-monolithic')}\n"
        ),
        "'disk_backed_monolithic': ParamInfo(CFG_BLAST, 'disk-backed-monolithic')",
    )
    _replace_once_unless_present(
        config_path,
        (
            "        if self.db_partitions < 0:\n"
            "            errors.append(f'db-partitions must be non-negative, got {self.db_partitions}')\n"
        ),
        (
            "        if self.db_partitions < 0:\n"
            "            errors.append(f'db-partitions must be non-negative, got {self.db_partitions}')\n"
            "        if self.disk_backed_monolithic and self.db_partitions != 1:\n"
            "            errors.append(\n"
            "                'disk-backed-monolithic requires exactly one DB partition'\n"
            "            )\n"
        ),
        "disk-backed-monolithic requires exactly one DB partition",
    )
    _replace_once_unless_present(
        config_path,
        (
            "        if task == ElbCommand.SUBMIT:\n"
            "            # validate number of CPUs and memory limit for searching a batch\n"
            "            # of queries\n"
        ),
        (
            "        if task == ElbCommand.SUBMIT:\n"
            "            if self.blast.disk_backed_monolithic and (\n"
            "                self.cloud_provider.cloud != CSP.AZURE\n"
            "                or not self.cluster.use_local_ssd\n"
            "            ):\n"
            "                errors.append(\n"
            "                    'disk-backed-monolithic requires Azure node-local SSD storage'\n"
            "                )\n"
            "            # validate number of CPUs and memory limit for searching a batch\n"
            "            # of queries\n"
        ),
        "disk-backed-monolithic requires Azure node-local SSD storage",
    )
    _replace_once_unless_present(
        config_path,
        "                if self.blast.db_metadata:\n",
        (
            "                disk_backed_monolithic = (\n"
            "                    self.blast.disk_backed_monolithic\n"
            "                    and self.cloud_provider.cloud == CSP.AZURE\n"
            "                    and self.cluster.use_local_ssd\n"
            "                    and self.blast.db_partitions == 1\n"
            "                )\n"
            "                if self.blast.db_metadata and not disk_backed_monolithic:\n"
        ),
        "if self.blast.db_metadata and not disk_backed_monolithic:",
    )

    azure_path = root / "src/elastic_blast/azure.py"
    _replace_once_unless_present(
        azure_path,
        (
            "        program = cfg.blast.program\n"
            "        # CPU allocation: fewer CPUs when 1 job per node, else quarter of total\n"
        ),
        (
            "        program = cfg.blast.program\n"
            "        disk_backed_monolithic = bool(cfg.blast.disk_backed_monolithic)\n"
            "        # CPU allocation: fewer CPUs when 1 job per node, else quarter of total\n"
        ),
        "disk_backed_monolithic = bool(cfg.blast.disk_backed_monolithic)",
    )
    _replace_once_unless_present(
        azure_path,
        (
            "            'ELB_MEM_LIMIT': str(cfg.cluster.mem_limit),\n"
            "            'ELB_BLAST_OPTIONS': cfg.blast.options,\n"
        ),
        (
            "            'ELB_MEM_LIMIT': str(cfg.cluster.mem_limit),\n"
            "            'ELB_DB_HOST_PATH': (\n"
            "                '/workspace/blast-monolithic'\n"
            "                if disk_backed_monolithic else '/workspace/blast'\n"
            "            ),\n"
            "            'ELB_VMTOUCH_DISABLE': '1' if disk_backed_monolithic else '0',\n"
            "            'ELB_BLAST_OPTIONS': cfg.blast.options,\n"
        ),
        "'ELB_DB_HOST_PATH': (",
    )

    kubernetes_path = root / "src/elastic_blast/kubernetes.py"
    _replace_once_unless_present(
        kubernetes_path,
        ("    if _dashboard_warmup_jobs_ready(cfg, db, num_shards):\n        return\n"),
        (
            "    if (\n"
            "        not cfg.blast.disk_backed_monolithic\n"
            "        and _dashboard_warmup_jobs_ready(cfg, db, num_shards)\n"
            "    ):\n"
            "        return\n"
        ),
        "not cfg.blast.disk_backed_monolithic",
    )
    _replace_once_unless_present(
        kubernetes_path,
        (
            "        'ELB_PARTITION_PREFIX': partition_prefix,\n"
            "        'ELB_RESULTS': results_bucket,\n"
        ),
        (
            "        'ELB_PARTITION_PREFIX': partition_prefix,\n"
            "        'ELB_DB_HOST_PATH': (\n"
            "            '/workspace/blast-monolithic'\n"
            "            if cfg.blast.disk_backed_monolithic else '/workspace/blast'\n"
            "        ),\n"
            "        'ELB_RESULTS': results_bucket,\n"
        ),
        "if cfg.blast.disk_backed_monolithic else '/workspace/blast'",
    )

    search_template = (
        root / "src/elastic_blast/templates/blast-batch-job-shard-ssd-aks.yaml.template"
    )
    _replace_once_unless_present(
        search_template,
        (
            "      - name: blast-dbs\n"
            "        hostPath:\n"
            '          path: "/workspace"\n'
            "          type: DirectoryOrCreate\n"
        ),
        (
            "      - name: blast-dbs\n"
            "        hostPath:\n"
            '          path: "${ELB_DB_HOST_PATH}"\n'
            "          type: DirectoryOrCreate\n"
        ),
        'path: "${ELB_DB_HOST_PATH}"',
    )
    _replace_once_unless_present(
        search_template,
        (
            "        - name: blast-dbs\n"
            "          mountPath: /blast/blastdb\n"
            "          subPath: blast\n"
        ),
        (
            "        - name: blast-dbs\n"
            "          # ELB isolated DB host path mount\n"
            "          mountPath: /blast/blastdb\n"
        ),
        "ELB isolated DB host path mount",
    )

    init_template = root / "src/elastic_blast/templates/job-init-ssd-shard-aks.yaml.template"
    _replace_once_unless_present(
        init_template,
        (
            "      - name: blastdb\n"
            "        hostPath:\n"
            '          path: "/workspace"\n'
            "      - name: scripts\n"
        ),
        (
            "      - name: blastdb\n"
            "        hostPath:\n"
            '          path: "${ELB_DB_HOST_PATH}"\n'
            "          type: DirectoryOrCreate\n"
            "      - name: query-cache\n"
            "        hostPath:\n"
            '          path: "/workspace/queries"\n'
            "          type: DirectoryOrCreate\n"
            "      - name: scripts\n"
        ),
        'path: "${ELB_DB_HOST_PATH}"',
    )
    _replace_once_unless_present(
        init_template,
        (
            "        - name: blastdb\n"
            "          mountPath: /blast/blastdb\n"
            "          subPath: blast\n"
        ),
        (
            "        - name: blastdb\n"
            "          # ELB isolated DB host path mount\n"
            "          mountPath: /blast/blastdb\n"
        ),
        "ELB isolated DB host path mount",
    )
    _replace_once_unless_present(
        init_template,
        (
            "        - name: blastdb\n"
            "          mountPath: /blast/queries\n"
            "          subPath: queries\n"
            "          readOnly: false\n"
        ),
        (
            "        - name: query-cache\n"
            "          mountPath: /blast/queries\n"
            "          readOnly: false\n"
        ),
        "name: query-cache\n          mountPath: /blast/queries",
    )


def patch_requested_max_target_seqs(root: Path) -> None:
    """Add an internal final-result cap distinct from the shard candidate pool.

    The runtime model uses zero only as the optional-field sentinel. The API
    serializer emits this key only with a validated positive value.
    """

    config_path = root / "src/elastic_blast/elb_config.py"
    _replace_once_unless_present(
        config_path,
        "    disk_backed_monolithic: bool = False\n\n",
        ("    disk_backed_monolithic: bool = False\n    requested_max_target_seqs: int = 0\n\n"),
        "requested_max_target_seqs: int = 0",
    )
    _replace_once_unless_present(
        config_path,
        "    requested_max_target_seqs: int = 0\n\n",
        (
            "    requested_max_target_seqs: int = 0\n"
            "    result_selection_policy: str = 'native_top_n'\n\n"
        ),
        "result_selection_policy: str = 'native_top_n'",
    )
    _replace_once_unless_present(
        config_path,
        "    result_selection_policy: str = 'native_top_n'\n\n",
        (
            "    result_selection_policy: str = 'native_top_n'\n"
            "    candidate_pool_size_requested: int = 0\n\n"
        ),
        "candidate_pool_size_requested: int = 0",
    )
    _replace_once_unless_present(
        config_path,
        (
            "               'disk_backed_monolithic': "
            "ParamInfo(CFG_BLAST, 'disk-backed-monolithic')}\n"
        ),
        (
            "               'disk_backed_monolithic': "
            "ParamInfo(CFG_BLAST, 'disk-backed-monolithic'),\n"
            "               'requested_max_target_seqs': "
            "ParamInfo(CFG_BLAST, 'requested-max-target-seqs')}\n"
        ),
        "'requested_max_target_seqs': ParamInfo(CFG_BLAST, 'requested-max-target-seqs')",
    )
    _replace_once_unless_present(
        config_path,
        (
            "               'requested_max_target_seqs': "
            "ParamInfo(CFG_BLAST, 'requested-max-target-seqs')}\n"
        ),
        (
            "               'requested_max_target_seqs': "
            "ParamInfo(CFG_BLAST, 'requested-max-target-seqs'),\n"
            "               'result_selection_policy': "
            "ParamInfo(CFG_BLAST, 'result-selection-policy')}\n"
        ),
        "'result_selection_policy': ParamInfo(CFG_BLAST, 'result-selection-policy')",
    )
    _replace_once_unless_present(
        config_path,
        (
            "               'result_selection_policy': "
            "ParamInfo(CFG_BLAST, 'result-selection-policy')}\n"
        ),
        (
            "               'result_selection_policy': "
            "ParamInfo(CFG_BLAST, 'result-selection-policy'),\n"
            "               'candidate_pool_size_requested': "
            "ParamInfo(CFG_BLAST, 'candidate-pool-size-requested')}\n"
        ),
        "'candidate_pool_size_requested': ParamInfo(CFG_BLAST, 'candidate-pool-size-requested')",
    )
    _replace_once_unless_present(
        config_path,
        (
            "        if self.disk_backed_monolithic and self.db_partitions != 1:\n"
            "            errors.append(\n"
            "                'disk-backed-monolithic requires exactly one DB partition'\n"
            "            )\n"
        ),
        (
            "        if self.requested_max_target_seqs < 0:\n"
            "            errors.append(\n"
            "                'requested-max-target-seqs must be non-negative'\n"
            "            )\n"
            "        if self.disk_backed_monolithic and self.db_partitions != 1:\n"
            "            errors.append(\n"
            "                'disk-backed-monolithic requires exactly one DB partition'\n"
            "            )\n"
        ),
        "requested-max-target-seqs must be non-negative",
    )
    _replace_once_unless_present(
        config_path,
        (
            "        if self.requested_max_target_seqs < 0:\n"
            "            errors.append(\n"
            "                'requested-max-target-seqs must be non-negative'\n"
            "            )\n"
        ),
        (
            "        if self.requested_max_target_seqs < 0:\n"
            "            errors.append(\n"
            "                'requested-max-target-seqs must be non-negative'\n"
            "            )\n"
            "        if self.result_selection_policy not in {'native_top_n', 'diversity_aware'}:\n"
            "            errors.append(\n"
            "                'result-selection-policy must be native_top_n or diversity_aware'\n"
            "            )\n"
        ),
        "self.result_selection_policy not in",
    )
    _replace_once_unless_present(
        config_path,
        (
            "        if self.result_selection_policy not in {'native_top_n', 'diversity_aware'}:\n"
            "            errors.append(\n"
            "                'result-selection-policy must be native_top_n or diversity_aware'\n"
            "            )\n"
        ),
        (
            "        if self.result_selection_policy not in {\n"
            "            'native_top_n', 'diversity_aware', 'sequence_diversity'\n"
            "        }:\n"
            "            errors.append(\n"
            "                'result-selection-policy must be native_top_n, diversity_aware, '\n"
            "                'or sequence_diversity'\n"
            "            )\n"
            "        if not 0 <= self.candidate_pool_size_requested <= 5000:\n"
            "            errors.append(\n"
            "                'candidate-pool-size-requested must be between 0 and 5000'\n"
            "            )\n"
        ),
        "candidate-pool-size-requested must be between 0 and 5000",
    )


def patch_azure_cli_glue(root: Path) -> None:
    path = root / "src/elastic_blast/azure_cli_glue.py"
    _replace_once_unless_present(
        path,
        ("    # Phase 3: success -> structured ACCEPTED.\n    if json_mode and rc == 0:\n"),
        (
            "    # Phase 3: success -> structured ACCEPTED.\n"
            "    if json_mode and rc == 0:\n"
            "        # Dashboard JSON submit has its own log/state collectors.\n"
            "        # Avoid running ElasticBLAST's post-submit cleanup hook here,\n"
            "        # because it can keep the submit process open while K8s work\n"
            "        # is already running or even completed.\n"
            "        clean_up_stack.clear()\n"
        ),
        "Dashboard JSON submit has its own log/state collectors",
    )


def _azure_traits_paths(root: Path) -> list[Path]:
    paths = [root / "src/elastic_blast/azure_traits.py"]
    for pattern in (
        "venv/lib/python*/site-packages/elastic_blast/azure_traits.py",
        ".venv/lib/python*/site-packages/elastic_blast/azure_traits.py",
    ):
        paths.extend(root.glob(pattern))
    return sorted({path for path in paths if path.exists()})


def patch_azure_traits(root: Path) -> None:
    machine_entries = (
        "    # D/E-series v7 AMD (dashboard availability fallback)\n"
        "    'Standard_D2as_v7': {'cpu': 2, 'memory': 8},\n"
        "    'Standard_D4as_v7': {'cpu': 4, 'memory': 16},\n"
        "    'Standard_E16as_v7': {'cpu': 16, 'memory': 128},\n"
        "    'Standard_E32as_v7': {'cpu': 32, 'memory': 256},\n"
        "    'Standard_E48as_v7': {'cpu': 48, 'memory': 384},\n"
    )
    price_entries = (
        "    # D/E-series v7 AMD (dashboard availability fallback)\n"
        "    'Standard_D2as_v7': 0.096,\n"
        "    'Standard_D4as_v7': 0.192,\n"
        "    'Standard_E16as_v7': 1.008,\n"
        "    'Standard_E32as_v7': 2.016,\n"
        "    'Standard_E48as_v7': 3.024,\n"
    )
    for path in _azure_traits_paths(root):
        _replace_once_unless_present(
            path,
            "    'Standard_D8s_v3': {'cpu': 8, 'memory': 32},  # 8 vCPU, 32 GB RAM\n",
            (
                "    'Standard_D8s_v3': {'cpu': 8, 'memory': 32},  # 8 vCPU, 32 GB RAM\n"
                f"{machine_entries}"
            ),
            "'Standard_E32as_v7': {'cpu': 32, 'memory': 256}",
        )
        _replace_once_unless_present(
            path,
            "    'Standard_D64s_v3': 3.072,\n",
            f"    'Standard_D64s_v3': 3.072,\n{price_entries}",
            "'Standard_E32as_v7': 2.016",
            allow_absent=True,
        )


def patch_finalizer_template(root: Path) -> None:
    path = root / "src/elastic_blast/templates/elb-finalizer-aks.yaml.template"
    _replace_once_unless_present(
        path,
        "        image: ${ELB_DOCKER_IMAGE}\n",
        "        image: ${ELB_FINALIZER_DOCKER_IMAGE}\n",
        "image: ${ELB_FINALIZER_DOCKER_IMAGE}",
    )
    _replace_once_unless_present(
        path,
        (
            "        - name: ELB_BLAST_PROGRAM\n"
            '          value: "${ELB_BLAST_PROGRAM}"\n'
            "        - name: BLAST_ELB_JOB_ID\n"
        ),
        (
            "        - name: ELB_BLAST_PROGRAM\n"
            '          value: "${ELB_BLAST_PROGRAM}"\n'
            "        - name: ELB_BLAST_OPTIONS\n"
            '          value: "${ELB_BLAST_OPTIONS}"\n'
            "        - name: BLAST_ELB_JOB_ID\n"
        ),
        "name: ELB_BLAST_OPTIONS",
    )
    _replace_once_unless_present(
        path,
        (
            "        - name: ELB_BLAST_OPTIONS\n"
            "          value: >-\n"
            "            ${ELB_BLAST_OPTIONS}\n"
            "        - name: BLAST_ELB_JOB_ID\n"
        ),
        (
            "        - name: ELB_BLAST_OPTIONS\n"
            "          value: >-\n"
            "            ${ELB_BLAST_OPTIONS}\n"
            "        - name: ELB_REQUESTED_MAX_TARGET_SEQS\n"
            '          value: "${ELB_REQUESTED_MAX_TARGET_SEQS}"\n'
            "        - name: BLAST_ELB_JOB_ID\n"
        ),
        "name: ELB_REQUESTED_MAX_TARGET_SEQS",
        allow_absent=True,
    )
    _replace_once_unless_present(
        path,
        (
            "        - name: ELB_BLAST_OPTIONS\n"
            '          value: "${ELB_BLAST_OPTIONS}"\n'
            "        - name: BLAST_ELB_JOB_ID\n"
        ),
        (
            "        - name: ELB_BLAST_OPTIONS\n"
            '          value: "${ELB_BLAST_OPTIONS}"\n'
            "        - name: ELB_REQUESTED_MAX_TARGET_SEQS\n"
            '          value: "${ELB_REQUESTED_MAX_TARGET_SEQS}"\n'
            "        - name: BLAST_ELB_JOB_ID\n"
        ),
        "name: ELB_REQUESTED_MAX_TARGET_SEQS",
    )
    _replace_once_unless_present(
        path,
        (
            "        - name: ELB_REQUESTED_MAX_TARGET_SEQS\n"
            '          value: "${ELB_REQUESTED_MAX_TARGET_SEQS}"\n'
        ),
        (
            "        - name: ELB_REQUESTED_MAX_TARGET_SEQS\n"
            '          value: "${ELB_REQUESTED_MAX_TARGET_SEQS}"\n'
            "        - name: ELB_RESULT_SELECTION_POLICY\n"
            '          value: "${ELB_RESULT_SELECTION_POLICY}"\n'
        ),
        "name: ELB_RESULT_SELECTION_POLICY",
    )
    _replace_once_unless_present(
        path,
        (
            "        - name: ELB_RESULT_SELECTION_POLICY\n"
            '          value: "${ELB_RESULT_SELECTION_POLICY}"\n'
        ),
        (
            "        - name: ELB_RESULT_SELECTION_POLICY\n"
            '          value: "${ELB_RESULT_SELECTION_POLICY}"\n'
            "        - name: ELB_CANDIDATE_POOL_SIZE_REQUESTED\n"
            '          value: "${ELB_CANDIDATE_POOL_SIZE_REQUESTED}"\n'
        ),
        "name: ELB_CANDIDATE_POOL_SIZE_REQUESTED",
    )
    _replace_once_unless_present(
        path,
        "      restartPolicy: Never\n  # The finalizer writes terminal SUCCESS/FAILURE markers",
        (
            "      restartPolicy: Never\n"
            "      tolerations:\n"
            "      - key: workload\n"
            "        operator: Equal\n"
            "        value: blast\n"
            "        effect: NoSchedule\n"
            "      - key: CriticalAddonsOnly\n"
            "        operator: Exists\n"
            "        effect: NoSchedule\n"
            "  # The finalizer writes terminal SUCCESS/FAILURE markers"
        ),
        "key: CriticalAddonsOnly",
    )


def patch_finalizer_script(root: Path, merge_script_source: Path) -> None:
    path = root / "src/elastic_blast/templates/scripts/elb-finalizer-aks.sh"
    merge_script_target = path.parent / "merge-sharded-results.sh"
    merge_script_target.write_text(merge_script_source.read_text())

    _replace_once_unless_present(
        path,
        (
            'MARKER_DIR="${ELB_RESULTS}/${ELB_METADATA_DIR}"\n'
            "if azcopy login --identity >/dev/null 2>&1; then\n"
            '    if azcopy list "${MARKER_DIR}/SUCCESS.txt" '
            ">/dev/null 2>&1; then\n"
            '        if [ "${ELB_DB_PARTITIONS:-0}" -gt 0 ]; then\n'
            '            if azcopy list "${ELB_RESULTS}/merged_results.out.gz" '
            ">/dev/null 2>&1 && \\\n"
            '               azcopy list "${ELB_RESULTS}/merge-report.json" '
            ">/dev/null 2>&1; then\n"
            '                echo "SUCCESS.txt and merge artifacts already '
            'present; skipping finalizer"\n'
            "                exit 0\n"
            "            fi\n"
            '            echo "SUCCESS.txt already present but merge artifacts '
            'are missing; continuing merge"\n'
            "        else\n"
            '            echo "SUCCESS.txt already present at ${MARKER_DIR}; '
            'skipping finalizer"\n'
            "            exit 0\n"
            "        fi\n"
            "    fi\n"
            '    if azcopy list "${MARKER_DIR}/FAILURE.txt" '
            ">/dev/null 2>&1; then\n"
            '        echo "FAILURE.txt already present at ${MARKER_DIR}; '
            'skipping finalizer"\n'
            "        exit 0\n"
            "    fi\n"
            "fi\n"
        ),
        (
            'MARKER_DIR="${ELB_RESULTS}/${ELB_METADATA_DIR}"\n'
            "blob_exists() {\n"
            "    local output\n"
            '    output=$(azcopy list "$1" 2>/dev/null || true)\n'
            "    printf '%s\\n' \"$output\" | grep -Ev '^(INFO:|$)' >/dev/null\n"
            "}\n"
            "if azcopy login --identity >/dev/null 2>&1; then\n"
            '    if blob_exists "${MARKER_DIR}/SUCCESS.txt"; then\n'
            '        if [ "${ELB_DB_PARTITIONS:-0}" -gt 0 ]; then\n'
            '            if blob_exists "${ELB_RESULTS}/merged_results.out.gz" && \\\n'
            '               blob_exists "${ELB_RESULTS}/merge-report.json"; then\n'
            '                echo "SUCCESS.txt and merge artifacts already '
            'present; skipping finalizer"\n'
            "                exit 0\n"
            "            fi\n"
            '            echo "SUCCESS.txt already present but merge artifacts '
            'are missing; continuing merge"\n'
            "        else\n"
            '            echo "SUCCESS.txt already present at ${MARKER_DIR}; '
            'skipping finalizer"\n'
            "            exit 0\n"
            "        fi\n"
            "    fi\n"
            '    if blob_exists "${MARKER_DIR}/FAILURE.txt"; then\n'
            '        echo "FAILURE.txt already present at ${MARKER_DIR}; '
            'skipping finalizer"\n'
            "        exit 0\n"
            "    fi\n"
            "fi\n"
        ),
        "blob_exists()",
    )
    _replace_once_unless_present(
        path,
        (
            '            if ! azcopy cp "${SHARD_DIR}/*.out.gz" "$LOCAL_DIR/" '
            "--log-level=ERROR 2>/dev/null; then\n"
        ),
        (
            '            if ! azcopy cp "${SHARD_DIR}/*" "$LOCAL_DIR/" '
            '--include-pattern "*.out.gz" --log-level=ERROR 2>/dev/null; then\n'
        ),
        '--include-pattern "*.out.gz"',
    )
    # Preserve the per-shard ``# Fields:`` comment line when concatenating
    # shard outputs into MERGE_INPUT. Upstream strips every comment with
    # ``awk '!/^#/'``, which means the authoritative outfmt 7 field list
    # (``... bit score, subject tax ids, subject sci names``) never reaches
    # merge-sharded-results.sh. The merge then falls back to the standard
    # 12-field header even though the data rows carry the extended staxids /
    # sscinames columns, so the results parser — which derives columns from
    # the ``# Fields:`` line — silently drops them and the dashboard shows an
    # empty Scientific Name. Keeping every ``# Fields:`` line (the merge
    # captures the first and ignores the rest) makes the merged header match
    # the extended data rows. Plain outfmt 6 input carries no comment lines,
    # so this is a no-op for the standard layout.
    _replace_once_unless_present(
        path,
        ('                    if ! zcat "$f" | awk \'!/^#/\' >> "$MERGE_INPUT"; then\n'),
        (
            "                    if ! zcat \"$f\" | awk '/^# Fields:/ || !/^#/' "
            '>> "$MERGE_INPUT"; then\n'
        ),
        "awk '/^# Fields:/ || !/^#/'",
    )
    _replace_once_unless_present(
        path,
        (
            "                    if ! zcat \"$f\" | awk '/^# Fields:/ || !/^#/' "
            '>> "$MERGE_INPUT"; then\n'
        ),
        (
            '                    if [ "${ELB_RESULT_SELECTION_POLICY:-native_top_n}" = '
            '"sequence_diversity" ]; then\n'
            '                        printf \'# ELB source-shard:%s\\n\' "$SHARD" '
            '>> "$MERGE_INPUT"\n'
            "                    fi\n"
            "                    if ! zcat \"$f\" | awk '/^# Fields:/ || !/^#/' "
            '>> "$MERGE_INPUT"; then\n'
        ),
        "# ELB source-shard:%s",
    )
    _replace_once_unless_present(
        path,
        '        TOTAL_ROWS=$(wc -l < "$MERGE_INPUT" 2>/dev/null || echo 0)\n',
        (
            '        export ELB_SUCCEEDED_SHARDS="$DOWNLOAD_SUCCESS_COUNT"\n'
            '        TOTAL_ROWS=$(wc -l < "$MERGE_INPUT" 2>/dev/null || echo 0)\n'
        ),
        'export ELB_SUCCEEDED_SHARDS="$DOWNLOAD_SUCCESS_COUNT"',
    )
    _replace_once_unless_present(
        path,
        (
            '        TOTAL_ROWS=$(wc -l < "$MERGE_INPUT" 2>/dev/null || echo 0)\n'
            '        echo "Downloaded $SHARD_COUNT shard files, $TOTAL_ROWS tabular rows"\n\n'
            "        if ! /scripts/merge-sharded-results.sh \\\n"
        ),
        (
            '        TOTAL_ROWS=$(wc -l < "$MERGE_INPUT" 2>/dev/null || echo 0)\n'
            '        echo "Downloaded $SHARD_COUNT shard files, $TOTAL_ROWS tabular rows"\n\n'
            '        ORACLE_FILE="$MERGE_DIR/tie-order-oracle.txt"\n'
            '        ORACLE_SEARCH_BASES="$ELB_RESULTS"\n'
            '        ORACLE_PARENT_RESULTS="${ELB_RESULTS%/job-*}"\n'
            '        if [ "$ORACLE_PARENT_RESULTS" != "$ELB_RESULTS" ]; then\n'
            '            ORACLE_SEARCH_BASES="$ORACLE_SEARCH_BASES $ORACLE_PARENT_RESULTS"\n'
            "        fi\n"
            '        WEB_STATS_FILE="$MERGE_DIR/web-blast-statistics.json"\n'
            "        WEB_STATS_REQUIRED=0\n"
            '        case " ${ELB_BLAST_OPTIONS:-} " in\n'
            '            *" -dbsize "*|*" -dbsize="*) WEB_STATS_REQUIRED=1 ;;\n'
            "        esac\n"
            "        for WEB_STATS_BASE in $ORACLE_SEARCH_BASES; do\n"
            '            [ -n "${ELB_WEB_BLAST_STATISTICS_FILE:-}" ] && break\n'
            '            WEB_STATS_BLOB="${WEB_STATS_BASE}/${ELB_METADATA_DIR}/web-blast-statistics.json"\n'
            '            if blob_exists "$WEB_STATS_BLOB"; then\n'
            '                if azcopy cp "$WEB_STATS_BLOB" "$WEB_STATS_FILE" '
            "--log-level=ERROR </dev/null 2>/dev/null; then\n"
            '                    export ELB_WEB_BLAST_STATISTICS_FILE="$WEB_STATS_FILE"\n'
            '                    echo "Using Web BLAST statistics from ${WEB_STATS_BLOB}"\n'
            "                fi\n"
            "            fi\n"
            "        done\n"
            '        if [ "$WEB_STATS_REQUIRED" -eq 1 ] && '
            '[ -z "${ELB_WEB_BLAST_STATISTICS_FILE:-}" ]; then\n'
            '            echo "ERROR: -dbsize requires a Web BLAST statistics manifest"\n'
            "            exit 1\n"
            "        fi\n\n"
            '        case "${ELB_RESULT_SELECTION_POLICY:-native_top_n}" in\n'
            '            diversity_aware)\n'
            '                export ELB_DIVERSITY_AWARE_CUTOFF="auto"\n'
            '                ORACLE_SEARCH_BASES=""\n'
            '                echo "Using diversity-aware proportional result selection"\n'
            '                ;;\n'
            '            sequence_diversity)\n'
            '                ORACLE_SEARCH_BASES=""\n'
            '                echo "Using sequence-diversity result selection"\n'
            '                ;;\n'
            '        esac\n\n'
            "        for ORACLE_BASE in $ORACLE_SEARCH_BASES; do\n"
            '            [ -n "${ELB_TIE_ORDER_FILE:-}" ] && break\n'
            '            ORACLE_BLOB="${ORACLE_BASE}/${ELB_METADATA_DIR}/tie-order-oracle.txt"\n'
            '            if blob_exists "$ORACLE_BLOB"; then\n'
            '                if azcopy cp "$ORACLE_BLOB" "$ORACLE_FILE" '
            "--log-level=ERROR 2>/dev/null; then\n"
            '                    export ELB_TIE_ORDER_FILE="$ORACLE_FILE"\n'
            '                    export ELB_TIE_ORDER_BASE="$ORACLE_BASE"\n'
            '                    export ELB_TIE_ORDER_SOURCE="query"\n'
            '                    echo "Using tie-order oracle from ${ORACLE_BLOB}"\n'
            "                else\n"
            '                    echo "WARNING: tie-order oracle exists but could not be '
            'downloaded: ${ORACLE_BLOB}"\n'
            "                fi\n"
            "            fi\n"
            "        done\n\n"
            '        if [ -z "${ELB_TIE_ORDER_FILE:-}" ]; then\n'
            "            for ORACLE_BASE in $ORACLE_SEARCH_BASES; do\n"
            '                [ -n "${ELB_TIE_ORDER_FILE:-}" ] && break\n'
            '                ORACLE_URLS_BLOB="${ORACLE_BASE}/${ELB_METADATA_DIR}/'
            'tie-order-oracle-urls.txt"\n'
            '                if blob_exists "$ORACLE_URLS_BLOB"; then\n'
            '                    ORACLE_URLS_FILE="$MERGE_DIR/tie-order-oracle-urls.txt"\n'
            '                    ORACLE_PART_DIR="$MERGE_DIR/tie-order-oracle-parts"\n'
            '                    mkdir -p "$ORACLE_PART_DIR"\n'
            '                    if azcopy cp "$ORACLE_URLS_BLOB" "$ORACLE_URLS_FILE" '
            "--log-level=ERROR 2>/dev/null; then\n"
            "                        idx=0\n"
            "                        while IFS= read -r part_url; do\n"
            '                            [ -z "$part_url" ] && continue\n'
            '                            part_file=$(printf "%s/part-%06d.txt" '
            '"$ORACLE_PART_DIR" "$idx")\n'
            '                            if ! azcopy cp "$part_url" "$part_file" '
            "--log-level=ERROR </dev/null 2>/dev/null; then\n"
            '                                echo "WARNING: tie-order oracle part could not '
            'be downloaded: ${part_url}"\n'
            '                                rm -f "$part_file"\n'
            "                            fi\n"
            "                            idx=$((idx + 1))\n"
            '                        done < "$ORACLE_URLS_FILE"\n'
            "                        ORACLE_EXPECTED_PARTS=$(grep -cve '^$' \"$ORACLE_URLS_FILE\" || true)\n"
            '                        ORACLE_DOWNLOADED_PARTS=$(find "$ORACLE_PART_DIR" -type f '
            '-name "part-*.txt" | wc -l | tr -d " ")\n'
            '                        if [ "$ORACLE_EXPECTED_PARTS" -le 0 ] || '
            '[ "$ORACLE_DOWNLOADED_PARTS" -ne "$ORACLE_EXPECTED_PARTS" ]; then\n'
            '                            echo "ERROR: DB-order oracle parts incomplete: '
            'expected=${ORACLE_EXPECTED_PARTS} downloaded=${ORACLE_DOWNLOADED_PARTS}"\n'
            "                            exit 1\n"
            "                        fi\n"
            '                        if [ "$ORACLE_DOWNLOADED_PARTS" -gt 0 ]; then\n'
            '                            find "$ORACLE_PART_DIR" -type f '
            '-name "part-*.txt" | sort | xargs cat > "$ORACLE_FILE"\n'
            '                            export ELB_TIE_ORDER_FILE="$ORACLE_FILE"\n'
            '                            export ELB_TIE_ORDER_BASE="$ORACLE_BASE"\n'
            '                            export ELB_TIE_ORDER_SOURCE="db_order"\n'
            '                            echo "Using DB-order tie oracle parts from '
            '${ORACLE_URLS_BLOB}"\n'
            "                        fi\n"
            "                    fi\n"
            "                fi\n"
            "            done\n"
            "        fi\n"
            '        if [ -n "${ELB_TIE_ORDER_FILE:-}" ]; then\n'
            '            ORACLE_STRICT_BLOB="${ELB_TIE_ORDER_BASE:-$ELB_RESULTS}/'
            "${ELB_METADATA_DIR}/"
            'tie-order-oracle-strict.txt"\n'
            '            if blob_exists "$ORACLE_STRICT_BLOB"; then\n'
            '                export ELB_TIE_ORDER_STRICT="1"\n'
            "            fi\n"
            "        fi\n\n"
            "        if ! /scripts/merge-sharded-results.sh \\\n"
        ),
        "ELB_TIE_ORDER_FILE",
    )
    _replace_once_unless_present(
        path,
        (
            '        if [ "$ORACLE_PARENT_RESULTS" != "$ELB_RESULTS" ]; then\n'
            '            ORACLE_SEARCH_BASES="$ORACLE_SEARCH_BASES $ORACLE_PARENT_RESULTS"\n'
            "        fi\n"
            "        for ORACLE_BASE in $ORACLE_SEARCH_BASES; do\n"
        ),
        (
            '        if [ "$ORACLE_PARENT_RESULTS" != "$ELB_RESULTS" ]; then\n'
            '            ORACLE_SEARCH_BASES="$ORACLE_SEARCH_BASES $ORACLE_PARENT_RESULTS"\n'
            "        fi\n"
            '        case "${ELB_RESULT_SELECTION_POLICY:-native_top_n}" in\n'
            '            diversity_aware)\n'
            '                export ELB_DIVERSITY_AWARE_CUTOFF="auto"\n'
            '                ORACLE_SEARCH_BASES=""\n'
            '                echo "Using diversity-aware proportional result selection"\n'
            '                ;;\n'
            '            sequence_diversity)\n'
            '                ORACLE_SEARCH_BASES=""\n'
            '                echo "Using sequence-diversity result selection"\n'
            '                ;;\n'
            '        esac\n'
            "        for ORACLE_BASE in $ORACLE_SEARCH_BASES; do\n"
        ),
        "Using sequence-diversity result selection",
        allow_absent=True,
    )
    _replace_once_unless_present(
        path,
        (
            '        if [ "${ELB_RESULT_SELECTION_POLICY:-native_top_n}" = "diversity_aware" ]; then\n'
            '            export ELB_DIVERSITY_AWARE_CUTOFF="auto"\n'
            '            ORACLE_SEARCH_BASES=""\n'
            '            echo "Using diversity-aware proportional result selection"\n'
            "        fi\n"
        ),
        (
            '        case "${ELB_RESULT_SELECTION_POLICY:-native_top_n}" in\n'
            '            diversity_aware)\n'
            '                export ELB_DIVERSITY_AWARE_CUTOFF="auto"\n'
            '                ORACLE_SEARCH_BASES=""\n'
            '                echo "Using diversity-aware proportional result selection"\n'
            '                ;;\n'
            '            sequence_diversity)\n'
            '                ORACLE_SEARCH_BASES=""\n'
            '                echo "Using sequence-diversity result selection"\n'
            '                ;;\n'
            '        esac\n'
        ),
        "Using sequence-diversity result selection",
    )

    text = path.read_text()
    if (
        "MERGE_OUTFMT=$(python3" in text
        and '"$MERGE_INPUT" "$MERGE_OUTPUT" "$MERGE_REPORT"' in text
    ):
        return
    if '"$MAX_HITS" "$MERGE_INPUT" "$MERGE_OUTPUT"' in text:
        raise RuntimeError(
            "elastic-blast-azure finalizer has the legacy tabular merge patch; "
            "update the cloned runtime to the XML-aware finalizer before building"
        )

    raise RuntimeError(
        "elastic-blast-azure finalizer is not XML-aware; update the cloned runtime "
        "before building the dashboard terminal image"
    )


_HARDENED_INIT_DB_SHARD_AKS_SCRIPT = r"""
#!/bin/bash
set -euo pipefail

echo "BASH version ${BASH_VERSION}"
echo "Shard download: idx=${ELB_SHARD_IDX} prefix=${ELB_PARTITION_PREFIX} db=${ELB_DB}"

if [ -n "${STARTUP_DELAY:-}" ]; then
    echo "Waiting ${STARTUP_DELAY}s for workspace initialization"
    sleep "${STARTUP_DELAY}"
fi

cd "${ELB_BLASTDB_DIR:-/blast/blastdb}"

ORIG_DB="$ELB_DB"
if [[ "$ELB_DB" =~ ^(.+)_shard_([0-9]+)$ ]]; then
        ORIG_DB="${BASH_REMATCH[1]}"
fi
if [[ ! "$ELB_DB" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,126}$ ]]; then
    echo "ERROR: unsafe shard DB name for cache markers: ${ELB_DB}"
    exit 64
fi
CACHE_COMPLETE=".elb-cache.${ELB_DB}.complete"
CACHE_SOURCE_VERSION=".elb-cache.${ELB_DB}.source-version"
CACHE_MANIFEST=".elb-cache.${ELB_DB}.manifest"
CACHE_LAYOUT_SHA=".elb-cache.${ELB_DB}.layout-sha256"
if [ "${ELB_STAGE_LOCK_HELD:-0}" = "1" ]; then
    if ! flock -n 9; then
        echo "ERROR: inherited stage lock descriptor is unavailable"
        exit 70
    fi
    echo "STAGE_LOCK_REUSE file=.elb-stage.lock"
else
    STAGE_LOCK_WAIT_SECONDS="${ELB_STAGE_LOCK_TIMEOUT_SECONDS:-2400}"
    case "$STAGE_LOCK_WAIT_SECONDS" in
      ''|*[!0-9]*) echo "ERROR: invalid stage lock timeout: ${STAGE_LOCK_WAIT_SECONDS}"; exit 64 ;;
    esac
    if [ "${#STAGE_LOCK_WAIT_SECONDS}" -gt 4 ] \
            || [ "$STAGE_LOCK_WAIT_SECONDS" -lt 1 ] \
            || [ "$STAGE_LOCK_WAIT_SECONDS" -gt 5400 ]; then
            echo "ERROR: stage lock timeout must be between 1 and 5400 seconds"
            exit 64
    fi
    if ! command -v flock >/dev/null 2>&1; then
        echo "ERROR: flock is required for safe node-local DB staging"
        exit 69
    fi
    STAGE_LOCK_FILE=".elb-stage.lock"
    exec 9>"$STAGE_LOCK_FILE"
    echo "STAGE_LOCK_WAIT file=${STAGE_LOCK_FILE} timeout=${STAGE_LOCK_WAIT_SECONDS}s"
    STAGE_LOCK_WAIT_STARTED=$(date +%s)
    if ! flock -w "$STAGE_LOCK_WAIT_SECONDS" 9; then
        echo "ERROR: stage lock timeout file=${STAGE_LOCK_FILE} waited_seconds=$(( $(date +%s) - STAGE_LOCK_WAIT_STARTED ))"
        exit 75
    fi
    export ELB_STAGE_LOCK_HELD=1
    echo "STAGE_LOCK_ACQUIRED file=${STAGE_LOCK_FILE} waited_seconds=$(( $(date +%s) - STAGE_LOCK_WAIT_STARTED ))"
fi
if [ -f .download-complete ]; then
    echo "CACHE_MIGRATE invalidating legacy global completion marker"
    rm -f .download-complete
fi
rm -f /tmp/elb-stage-result

start=$(date +%s)
log_runtime() {
    local ts
    ts=$(date +'%F %T')
    printf '%s RUNTIME %s %f seconds\n' "$ts" "$1" "$2"
}

azcopy login --identity || { echo "ERROR: azcopy login failed"; exit 1; }
export AZCOPY_CONCURRENCY_VALUE=${AZCOPY_CONCURRENCY_VALUE:-16}
export AZCOPY_BUFFER_GB=${AZCOPY_BUFFER_GB:-2}

retry_azcopy() {
    local max_attempts=3 attempt=1 wait_sec=5
    while [ "$attempt" -le "$max_attempts" ]; do
        if azcopy "$@"; then return 0; fi
        echo "azcopy attempt ${attempt}/${max_attempts} failed, retrying in ${wait_sec}s..."
        sleep "$wait_sec"
        wait_sec=$((wait_sec * 2))
        attempt=$((attempt + 1))
    done
    echo "ERROR: azcopy failed after ${max_attempts} attempts"
    return 1
}

SHARD_URL="${ELB_PARTITION_PREFIX}${ELB_SHARD_IDX}/"
MANIFEST_URL="${SHARD_URL}${ELB_DB}.manifest"
NAL_URL="${SHARD_URL}${ELB_DB}.nal"
LAYOUT_URL="${SHARD_URL}${ELB_DB}.layout"
echo "Downloading manifest: ${MANIFEST_URL}"
retry_azcopy cp "${MANIFEST_URL}" /tmp/manifest.txt --log-level=ERROR || {
    echo "ERROR: manifest download failed"
    exit 1
}
retry_azcopy cp "${NAL_URL}" /tmp/shard.nal --log-level=ERROR || {
    echo "ERROR: shard alias download failed"
    rm -f "$CACHE_COMPLETE"
    exit 1
}

valid_volume_name() {
    local volume="$1" suffix
    if [ "$volume" = "$ORIG_DB" ]; then
        return 0
    fi
    if [[ "$volume" != "$ORIG_DB".* ]]; then
        return 1
    fi
    suffix="${volume#"$ORIG_DB"}"
    [[ "$suffix" =~ ^\.[0-9]+$ ]]
}

mapfile -t VOLUMES < /tmp/manifest.txt
if [ "${#VOLUMES[@]}" -lt 1 ]; then
    echo "ERROR: shard manifest is empty"
    rm -f "$CACHE_COMPLETE"
    exit 65
fi
declare -A SEEN_VOLUMES=()
for volume in "${VOLUMES[@]}"; do
    if ! valid_volume_name "$volume"; then
        echo "ERROR: invalid volume name in shard manifest: ${volume}"
        rm -f "$CACHE_COMPLETE"
        exit 65
    fi
    if [ -n "${SEEN_VOLUMES[$volume]+x}" ]; then
        echo "ERROR: duplicate volume name in shard manifest: ${volume}"
        rm -f "$CACHE_COMPLETE"
        exit 65
    fi
    SEEN_VOLUMES["$volume"]=1
done
echo "Volumes: ${VOLUMES[*]}"

# Legacy layouts use `<container>/<N>shards/<db>_shard_`; immutable layouts
# insert `<db>/generations/<id>/shards/` before that suffix. Resolve the full
# database root from either shape instead of accidentally treating `shards/`
# as the generation root.
if [[ "${ELB_PARTITION_PREFIX}" =~ ^(.+)/shards/[0-9]+shards/[^/]+_shard_$ ]]; then
    DB_BASE_URL="${BASH_REMATCH[1]}/"
    DB_URL="${DB_BASE_URL}"
else
    DB_BASE_URL=$(echo "${ELB_PARTITION_PREFIX}" | sed 's|/[^/]*/[^/]*$|/|')
    DB_URL="${DB_BASE_URL}${ORIG_DB}/"
fi
echo "DB base URL: ${DB_URL}"

EXPECTED_SOURCE_VERSION="${ELB_DB_SOURCE_VERSION:-}"
# The immutable path itself is authoritative. Pin the expected generation even
# when an older ElasticBLAST template did not inject ELB_DB_SOURCE_VERSION.
if [ -z "$EXPECTED_SOURCE_VERSION" ] \
        && [[ "${ELB_PARTITION_PREFIX}" =~ /generations/([^/]+)/shards/ ]]; then
    EXPECTED_SOURCE_VERSION="${BASH_REMATCH[1]}"
    echo "DB source version derived from immutable shard path: ${EXPECTED_SOURCE_VERSION}"
fi
METADATA_SOURCE_VERSION=""
SHARD_LAYOUT_SCHEMA="0"
if [[ "${ELB_PARTITION_PREFIX}" =~ ^(https://[^/]+/[^/]+)/ ]]; then
    CONTAINER_ROOT_URL="${BASH_REMATCH[1]}/"
else
    echo "ERROR: shard prefix does not identify an Azure container root"
    exit 65
fi
METADATA_URL="${ELB_METADATA_URL:-${CONTAINER_ROOT_URL}${ORIG_DB}-metadata.json}"
echo "Resolving DB metadata: ${METADATA_URL}"
if retry_azcopy cp "${METADATA_URL}" /tmp/db-metadata.json --log-level=ERROR; then
    if command -v python3 >/dev/null 2>&1; then
        METADATA_SOURCE_VERSION=$(python3 -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    print(str(json.load(handle).get("source_version") or ""))
' /tmp/db-metadata.json 2>/dev/null || true)
        SHARD_LAYOUT_SCHEMA=$(python3 -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    value = json.load(handle).get("shard_layout_schema", 0)
print(value if type(value) is int else "invalid")
' /tmp/db-metadata.json 2>/dev/null || printf invalid)
    else
        METADATA_SOURCE_VERSION=$(sed -n \
            's/.*"source_version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
            /tmp/db-metadata.json | head -1)
        SHARD_LAYOUT_SCHEMA=$(sed -n \
            's/.*"shard_layout_schema"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' \
            /tmp/db-metadata.json | head -1)
        SHARD_LAYOUT_SCHEMA="${SHARD_LAYOUT_SCHEMA:-0}"
    fi
else
    echo "ERROR: DB metadata lookup failed after retries; refusing unversioned shard staging"
    rm -f "$CACHE_COMPLETE"
    exit 75
fi
case "$SHARD_LAYOUT_SCHEMA" in
    ''|*[!0-9]*) echo "ERROR: invalid shard_layout_schema: ${SHARD_LAYOUT_SCHEMA}"; rm -f "$CACHE_COMPLETE"; exit 65 ;;
esac
if [ "${#SHARD_LAYOUT_SCHEMA}" -gt 2 ] || [ "$SHARD_LAYOUT_SCHEMA" -gt 1 ]; then
    echo "ERROR: unsupported shard_layout_schema: ${SHARD_LAYOUT_SCHEMA}"
    rm -f "$CACHE_COMPLETE"
    exit 65
fi
if [ -n "$EXPECTED_SOURCE_VERSION" ]; then
    if [ -z "$METADATA_SOURCE_VERSION" ]; then
        echo "ERROR: DB metadata is missing source_version required by this Job"
        rm -f "$CACHE_COMPLETE"
        exit 75
    fi
    if [ "$EXPECTED_SOURCE_VERSION" != "$METADATA_SOURCE_VERSION" ]; then
        echo "ERROR: DB source version changed after Job creation" \
            "expected=${EXPECTED_SOURCE_VERSION} actual=${METADATA_SOURCE_VERSION}"
        rm -f "$CACHE_COMPLETE"
        exit 75
    fi
fi
if [ -z "$EXPECTED_SOURCE_VERSION" ]; then
    EXPECTED_SOURCE_VERSION="$METADATA_SOURCE_VERSION"
fi
if [ -n "$EXPECTED_SOURCE_VERSION" ]; then
    echo "DB source version: ${EXPECTED_SOURCE_VERSION}"
else
    echo "WARNING: DB metadata did not contain source_version"
fi

LAYOUT_AVAILABLE="0"
rm -f /tmp/shard-layout.txt
if [ "$SHARD_LAYOUT_SCHEMA" -ge 1 ]; then
    if ! retry_azcopy cp "${LAYOUT_URL}" /tmp/shard-layout.txt --log-level=ERROR; then
        echo "ERROR: schema ${SHARD_LAYOUT_SCHEMA} requires shard layout metadata"
        rm -f "$CACHE_COMPLETE"
        exit 65
    fi
elif ! azcopy cp "${LAYOUT_URL}" /tmp/shard-layout.txt --log-level=ERROR; then
    echo "WARNING: LEGACY_LAYOUT no authoritative disk-size metadata; preflight is degraded"
    rm -f /tmp/shard-layout.txt
fi
if [ -f /tmp/shard-layout.txt ]; then
    layout_extra=""
    if ! read -r EXPECTED_LAYOUT_SHA REQUIRED_BYTES layout_extra < /tmp/shard-layout.txt \
            || [ -n "$layout_extra" ]; then
        echo "ERROR: malformed shard layout metadata"
        rm -f "$CACHE_COMPLETE"
        exit 65
    fi
    case "$EXPECTED_LAYOUT_SHA" in
    *[!0-9a-f]*) echo "ERROR: invalid shard layout digest"; rm -f "$CACHE_COMPLETE"; exit 65 ;;
    esac
    case "$REQUIRED_BYTES" in
    ''|*[!0-9]*) echo "ERROR: invalid shard required_bytes"; rm -f "$CACHE_COMPLETE"; exit 65 ;;
    esac
    if [ "${#EXPECTED_LAYOUT_SHA}" -ne 64 ] \
            || [ "${#REQUIRED_BYTES}" -gt 18 ] \
            || [ "$REQUIRED_BYTES" -lt 1 ]; then
        echo "ERROR: shard layout metadata values are out of range"
        rm -f "$CACHE_COMPLETE"
        exit 65
    fi
    if ! command -v sha256sum >/dev/null 2>&1; then
        echo "ERROR: sha256sum is required for shard layout validation"
        rm -f "$CACHE_COMPLETE"
        exit 69
    fi
    ACTUAL_LAYOUT_SHA=$( \
        { cat /tmp/manifest.txt; printf '\0'; cat /tmp/shard.nal; } \
        | sha256sum | awk '{print $1}'
    )
    if [ "$ACTUAL_LAYOUT_SHA" != "$EXPECTED_LAYOUT_SHA" ]; then
        echo "ERROR: shard layout digest mismatch"
        rm -f "$CACHE_COMPLETE"
        exit 65
    fi
    LAYOUT_AVAILABLE="1"
    echo "LAYOUT_VERIFIED sha256=${EXPECTED_LAYOUT_SHA} required_bytes=${REQUIRED_BYTES}"
fi

write_volpaths() {
    local volpaths=""
    for volume in "${VOLUMES[@]}"; do
        [ -n "$volpaths" ] && volpaths="$volpaths "
        volpaths="${volpaths}$(pwd)/${volume}"
    done
    echo "VOLPATHS=${volpaths}" > /tmp/shard_volpaths.txt
    echo "Volume paths: ${volpaths}"
}

commit_layout_markers() {
    cp /tmp/manifest.txt "${CACHE_MANIFEST}.tmp"
    mv "${CACHE_MANIFEST}.tmp" "$CACHE_MANIFEST"
    if [ "$LAYOUT_AVAILABLE" = "1" ]; then
        printf '%s' "$EXPECTED_LAYOUT_SHA" > "${CACHE_LAYOUT_SHA}.tmp"
        mv "${CACHE_LAYOUT_SHA}.tmp" "$CACHE_LAYOUT_SHA"
    else
        rm -f "$CACHE_LAYOUT_SHA"
    fi
}

rm -f "${CACHE_COMPLETE}.tmp" "${CACHE_SOURCE_VERSION}.tmp" \
    "${CACHE_LAYOUT_SHA}.tmp" "${CACHE_MANIFEST}.tmp" "./${ELB_DB}.nal.tmp"
if [ -f "$CACHE_COMPLETE" ] && [ -z "$EXPECTED_SOURCE_VERSION" ]; then
    echo "CACHE_UNVERIFIED expected source version is unavailable"
    rm -f "$CACHE_COMPLETE"
fi
if find . -maxdepth 1 -name '.azDownload-*' | grep -q .; then
    echo "CLEANUP partial downloads"
    find . -maxdepth 1 -name '.azDownload-*' -exec rm -rf {} +
fi

payload_ext="nsq"
if [ "${ELB_DB_MOL_TYPE:-nucl}" = "prot" ]; then
    payload_ext="psq"
fi
missing_volume="0"
if [ -f "$CACHE_COMPLETE" ]; then
    for volume in "${VOLUMES[@]}"; do
        if [ ! -s "${volume}.${payload_ext}" ]; then
            missing_volume="1"
            echo "CACHE_INCOMPLETE missing ${volume}.${payload_ext}"
        fi
    done
    if [ "$missing_volume" != "0" ]; then
        rm -f "$CACHE_COMPLETE"
    fi
fi

# Self-heal caches staged before the `.nos`/`.not` taxonomy filter index was
# added to the download set. The DB-level OUTPUT taxonomy files `${ORIG_DB}.ntf`
# /`.nto` and the FILTER index `${ORIG_DB}.nos`/`.not` are siblings: a
# taxonomy-capable DB (core_nt) ships all four, a non-taxonomy DB ships none. So
# if `.ntf` is present locally but `.not`/`.nos` are not, this cache predates the
# fix and any `-taxids`/`-negative_taxids` search would abort with blastn
# exit 255 ("the file must exist: '<db>.not'"). Invalidate so the corrected
# pattern below re-stages them. Non-taxonomy DBs (no local `.ntf`) are untouched.
if [ -f "$CACHE_COMPLETE" ] && [ -s "${ORIG_DB}.ntf" ] \
    && { [ ! -s "${ORIG_DB}.not" ] || [ ! -s "${ORIG_DB}.nos" ]; }; then
    echo "CACHE_INCOMPLETE missing taxonomy filter index ${ORIG_DB}.not/.nos"
    rm -f "$CACHE_COMPLETE"
fi

if [ -f "$CACHE_COMPLETE" ] && [ -n "$EXPECTED_SOURCE_VERSION" ]; then
    if [ ! -f "$CACHE_SOURCE_VERSION" ]; then
        echo "CACHE_STALE missing source-version marker"
        rm -f "$CACHE_COMPLETE"
    elif [ "$(cat "$CACHE_SOURCE_VERSION")" != "$EXPECTED_SOURCE_VERSION" ]; then
        echo "CACHE_STALE source-version mismatch"
        rm -f "$CACHE_COMPLETE"
    fi
fi

if [ -f "$CACHE_COMPLETE" ]; then
    if [ ! -f "$CACHE_MANIFEST" ]; then
        echo "CACHE_STALE missing shard-manifest marker"
        rm -f "$CACHE_COMPLETE"
    elif ! cmp -s /tmp/manifest.txt "$CACHE_MANIFEST"; then
        echo "CACHE_STALE shard manifest mismatch"
        rm -f "$CACHE_COMPLETE"
    fi
fi
if [ -f "$CACHE_COMPLETE" ]; then
    if [ ! -f "./${ELB_DB}.nal" ]; then
        echo "CACHE_STALE missing shard alias"
        rm -f "$CACHE_COMPLETE"
    elif ! cmp -s /tmp/shard.nal "./${ELB_DB}.nal"; then
        echo "CACHE_STALE shard alias mismatch"
        rm -f "$CACHE_COMPLETE"
    fi
fi
if [ -f "$CACHE_COMPLETE" ] && [ "$LAYOUT_AVAILABLE" = "1" ]; then
    if [ ! -f "$CACHE_LAYOUT_SHA" ]; then
        echo "CACHE_STALE missing shard-layout marker"
        rm -f "$CACHE_COMPLETE"
    elif [ "$(cat "$CACHE_LAYOUT_SHA")" != "$EXPECTED_LAYOUT_SHA" ]; then
        echo "CACHE_STALE shard layout mismatch"
        rm -f "$CACHE_COMPLETE"
    fi
fi

# Integrity gate (mirrors api/services/warmup/scripts.py): the file-presence and
# taxonomy checks above miss a cache whose volumes exist but disagree with the
# alias/LMDB metadata, which fails the search with "Input db vol does not match
# lmdb vol". blastdbcmd -info reads that vol<->lmdb<->alias consistency; a
# failing probe means the staged DB is corrupt, so invalidate the marker and
# re-download rather than skip onto a broken cache.
if [ -f "$CACHE_COMPLETE" ]; then
    if ! blastdbcmd -db "$ELB_DB" -info >/dev/null 2>&1; then
        echo "CACHE_CORRUPT blastdbcmd integrity probe failed - invalidating"
        rm -f "$CACHE_COMPLETE"
    fi
fi

if [ -f "$CACHE_COMPLETE" ]; then
    echo "DOWNLOAD_SKIP existing shard=${ELB_SHARD_IDX}"
    commit_layout_markers
    write_volpaths
    printf '%s' skipped > /tmp/elb-stage-result
    exit 0
fi

remove_volume_payloads() {
    local volume candidate
    for volume in "$@"; do
        for candidate in "${volume}".*; do
            if [ -e "$candidate" ] || [ -L "$candidate" ]; then
                rm -f -- "$candidate"
            fi
        done
    done
}

# A failed or stale cache is rebuilt from a clean set of files while the
# exclusive node-local lock is held. The current manifest is trusted only after
# strict name validation above. A prior committed manifest may contribute stale
# volumes, but only names belonging to this same DB are eligible for deletion.
remove_volume_payloads "${VOLUMES[@]}"
for previous_manifest in "$CACHE_MANIFEST" .download-manifest; do
    if [ -f "$previous_manifest" ]; then
        mapfile -t PREVIOUS_VOLUMES < "$previous_manifest"
        for previous_volume in "${PREVIOUS_VOLUMES[@]}"; do
            if valid_volume_name "$previous_volume"; then
                remove_volume_payloads "$previous_volume"
            else
                echo "WARNING: ignoring unsafe volume in previous cache manifest"
            fi
        done
    fi
done
# Shared taxonomy files have no DB prefix and may still be required by another
# prepared database in this flat node-local cache. Preserve them here; the
# transfer below overwrites them when the current DB prefix supplies a newer
# authoritative copy.
rm -f -- "${ORIG_DB}.ndb" "${ORIG_DB}.ntf" "${ORIG_DB}.nto" \
    "${ORIG_DB}.nos" "${ORIG_DB}.not"

if [ "$LAYOUT_AVAILABLE" = "1" ]; then
    RESERVE_BYTES="${ELB_STAGE_DISK_RESERVE_BYTES:-}"
    if [ -z "$RESERVE_BYTES" ]; then
        RESERVE_BYTES=$(( REQUIRED_BYTES / 20 ))
        if [ "$RESERVE_BYTES" -lt 1073741824 ]; then
            RESERVE_BYTES=1073741824
        fi
    fi
    case "$RESERVE_BYTES" in
      ''|*[!0-9]*) echo "ERROR: invalid ELB_STAGE_DISK_RESERVE_BYTES"; exit 64 ;;
    esac
    if [ "${#RESERVE_BYTES}" -gt 18 ]; then
        echo "ERROR: ELB_STAGE_DISK_RESERVE_BYTES is out of range"
        exit 64
    fi
    AVAILABLE_BYTES=$(df -B1 --output=avail . | tail -n 1 | tr -d '[:space:]')
    case "$AVAILABLE_BYTES" in
      ''|*[!0-9]*) echo "ERROR: unable to determine node-local available bytes"; exit 74 ;;
    esac
    TOTAL_REQUIRED_BYTES=$(( REQUIRED_BYTES + RESERVE_BYTES ))
    if [ "$TOTAL_REQUIRED_BYTES" -lt "$REQUIRED_BYTES" ]; then
        echo "ERROR: disk preflight byte calculation overflow"
        exit 65
    fi
    echo "DISK_PREFLIGHT required_bytes=${REQUIRED_BYTES} reserve_bytes=${RESERVE_BYTES} available_bytes=${AVAILABLE_BYTES}"
    if [ "$AVAILABLE_BYTES" -lt "$TOTAL_REQUIRED_BYTES" ]; then
        echo "ERROR: insufficient node-local disk required_bytes=${REQUIRED_BYTES}" \
            "reserve_bytes=${RESERVE_BYTES} available_bytes=${AVAILABLE_BYTES};" \
            "free node disk space or use a larger node OS disk"
        exit 28
    fi
else
    echo "WARNING: DISK_PREFLIGHT_SKIP legacy shard layout has no authoritative byte count"
fi

PATTERN=""
for VOL in "${VOLUMES[@]}"; do
    [ -n "$PATTERN" ] && PATTERN="${PATTERN};"
    PATTERN="${PATTERN}${VOL}.*"
done
# DB-prefix taxonomy index files. `.ndb;.ntf;.nto` cover the `staxids`/`sscinames`
# OUTPUT lookup, but the `-taxids`/`-negative_taxids` taxonomy FILTER additionally
# memory-maps `${ORIG_DB}.nos` and `${ORIG_DB}.not` (the seqid->taxid index). Omitting
# them makes blastn abort with exit 255 ("the file must exist: '<db>.not'") on any
# sharded run that carries a taxon include/exclude filter, while non-filtered and
# OUTPUT-only (outfmt 7 staxids) runs still succeed. Keep all five in the pattern.
PATTERN="${PATTERN};taxdb.btd;taxdb.bti;taxonomy4blast.sqlite3;${ORIG_DB}.ndb;${ORIG_DB}.ntf;${ORIG_DB}.nto;${ORIG_DB}.nos;${ORIG_DB}.not"
echo "Downloading with pattern: ${PATTERN}"

retry_azcopy cp "${DB_URL}*" . \
    --include-pattern "${PATTERN}" \
    --block-size-mb=256 \
    --overwrite=true \
    --log-level=WARNING

find . -maxdepth 1 -name '.azDownload-*' -exec rm -rf {} +

end=$(date +%s)
log_runtime "download-shard-${ELB_SHARD_IDX}" $((end - start))

payload_count=$(find . -maxdepth 1 -name "*.${payload_ext}" ! -name '.azDownload-*' | wc -l)
echo "DB files downloaded: ${payload_count} .${payload_ext} files"
echo "Total size: $(du -sh . 2>/dev/null | cut -f1)"
if [ "$payload_count" = "0" ]; then
    echo "ERROR: no ${payload_ext} volume files downloaded"
    exit 1
fi
for volume in "${VOLUMES[@]}"; do
    if [ ! -s "${volume}.${payload_ext}" ]; then
        echo "ERROR: required payload is missing after download: ${volume}.${payload_ext}"
        exit 1
    fi
done
if [ ! -s taxdb.btd ] || [ ! -s taxdb.bti ]; then
    echo "TAXDB_SKIP taxdb files not present in DB prefix"
fi
if [ -s "${ORIG_DB}.ntf" ] \
    && { [ ! -s "${ORIG_DB}.not" ] || [ ! -s "${ORIG_DB}.nos" ]; }; then
    echo "ERROR: downloaded taxonomy filter index is incomplete ${ORIG_DB}.not/.nos"
    exit 1
fi
cp /tmp/shard.nal "./${ELB_DB}.nal.tmp"
mv "./${ELB_DB}.nal.tmp" "./${ELB_DB}.nal"
if ! blastdbcmd -db "$ELB_DB" -info >/dev/null 2>&1; then
    echo "ERROR: downloaded DB failed blastdbcmd integrity probe"
    exit 1
fi

write_volpaths
commit_layout_markers
if [ -n "$EXPECTED_SOURCE_VERSION" ]; then
    printf '%s' "$EXPECTED_SOURCE_VERSION" > "${CACHE_SOURCE_VERSION}.tmp"
    mv "${CACHE_SOURCE_VERSION}.tmp" "$CACHE_SOURCE_VERSION"
else
    rm -f "$CACHE_SOURCE_VERSION"
fi
printf '%s' ok > "${CACHE_COMPLETE}.tmp"
mv "${CACHE_COMPLETE}.tmp" "$CACHE_COMPLETE"
rm -f .download-source-version .download-layout-sha256 .download-manifest
printf '%s' downloaded > /tmp/elb-stage-result

pkill -f azcopy 2>/dev/null || true
rm -rf /root/.azcopy 2>/dev/null || true
""".strip()


def _init_shard_script_paths(root: Path) -> list[Path]:
    source_path = root / "src/elastic_blast/templates/scripts/init-db-shard-aks.sh"
    paths = [source_path]
    for pattern in (
        "venv/lib/python*/site-packages/elastic_blast/templates/scripts/init-db-shard-aks.sh",
        ".venv/lib/python*/site-packages/elastic_blast/templates/scripts/init-db-shard-aks.sh",
    ):
        paths.extend(root.glob(pattern))
    return sorted({path for path in paths if path.exists()})


def patch_init_shard_script(root: Path) -> None:
    paths = _init_shard_script_paths(root)
    if not paths:
        raise RuntimeError(f"init-db-shard-aks.sh not found under {root}")
    for path in paths:
        path.write_text(_HARDENED_INIT_DB_SHARD_AKS_SCRIPT + "\n")


_INIT_DB_DOWNLOAD_LOCK_ANCHOR = "fi\n\nstart=$(date +%s)\n"
_INIT_DB_DOWNLOAD_LOCK_BLOCK = r"""# ELB DB writer lock (added by patch_elastic_blast.py).
# Only the local-SSD init template opts in. PV-backed uses of this shared script
# retain upstream behaviour because filesystem lock semantics vary by PV type.
if [ "${ELB_DB_WRITER_LOCK:-0}" = "1" ]; then
    STAGE_LOCK_WAIT_SECONDS="${ELB_STAGE_LOCK_TIMEOUT_SECONDS:-2400}"
    case "$STAGE_LOCK_WAIT_SECONDS" in
      ''|*[!0-9]*) echo "ERROR: invalid stage lock timeout: ${STAGE_LOCK_WAIT_SECONDS}"; exit 64 ;;
    esac
    if [ "${#STAGE_LOCK_WAIT_SECONDS}" -gt 4 ] \
            || [ "$STAGE_LOCK_WAIT_SECONDS" -lt 1 ] \
            || [ "$STAGE_LOCK_WAIT_SECONDS" -gt 5400 ]; then
        echo "ERROR: stage lock timeout must be between 1 and 5400 seconds"
        exit 64
    fi
    if ! command -v flock >/dev/null 2>&1; then
        echo "ERROR: flock is required for safe node-local DB staging"
        exit 69
    fi
    STAGE_LOCK_FILE=".elb-stage.lock"
    exec 9>"$STAGE_LOCK_FILE"
    echo "STAGE_LOCK_WAIT file=${STAGE_LOCK_FILE} timeout=${STAGE_LOCK_WAIT_SECONDS}s"
    STAGE_LOCK_WAIT_STARTED=$(date +%s)
    if ! flock -w "$STAGE_LOCK_WAIT_SECONDS" 9; then
        echo "ERROR: stage lock timeout file=${STAGE_LOCK_FILE} waited_seconds=$(( $(date +%s) - STAGE_LOCK_WAIT_STARTED ))"
        exit 75
    fi
    export ELB_STAGE_LOCK_HELD=1
    echo "STAGE_LOCK_ACQUIRED file=${STAGE_LOCK_FILE} waited_seconds=$(( $(date +%s) - STAGE_LOCK_WAIT_STARTED ))"
fi

"""

_INIT_DB_DOWNLOAD_VERIFY_ANCHOR = (
    "# Clean up azcopy background processes to ensure container exits cleanly.\n"
)
_INIT_DB_DOWNLOAD_VERIFY_BLOCK = r"""# A local-SSD writer must not report its
# init Job successful after a partial or corrupt transfer. PV-backed callers do
# not opt in and retain the upstream verification policy.
if [ "${ELB_DB_WRITER_LOCK:-0}" = "1" ]; then
    if ! blastdbcmd -info -db "$ELB_DB" -dbtype "$ELB_DB_MOL_TYPE" >/dev/null 2>&1; then
        echo "ERROR: local-SSD DB writer integrity check failed db=${ELB_DB}" >&2
        exit 76
    fi
    echo "DB_WRITER_CACHE_VERIFIED db=${ELB_DB}"
fi

"""


def patch_init_db_download_writer_lock(root: Path) -> None:
    """Opt the non-sharded local-SSD init path into the cache writer lock."""

    paths = []
    source = root / "src/elastic_blast/templates/scripts/init-db-download-aks.sh"
    if source.exists():
        paths.append(source)
    for pattern in (
        "venv/lib/python*/site-packages/elastic_blast/templates/scripts/init-db-download-aks.sh",
        ".venv/lib/python*/site-packages/elastic_blast/templates/scripts/init-db-download-aks.sh",
    ):
        paths.extend(root.glob(pattern))
    if not paths:
        raise RuntimeError(f"init-db-download-aks.sh not found under {root}")
    for path in sorted(set(paths)):
        _replace_once_unless_present(
            path,
            _INIT_DB_DOWNLOAD_LOCK_ANCHOR,
            "fi\n\n" + _INIT_DB_DOWNLOAD_LOCK_BLOCK + "start=$(date +%s)\n",
            "ELB DB writer lock (added by patch_elastic_blast.py)",
        )
        _replace_once_unless_present(
            path,
            _INIT_DB_DOWNLOAD_VERIFY_ANCHOR,
            _INIT_DB_DOWNLOAD_VERIFY_BLOCK + _INIT_DB_DOWNLOAD_VERIFY_ANCHOR,
            "DB_WRITER_CACHE_VERIFIED db=${ELB_DB}",
        )

    template = root / "src/elastic_blast/templates/job-init-local-ssd-aks.yaml.template"
    _replace_once_unless_present(
        template,
        (
            "        - name: ELB_SKIP_DB_VERIFY\n"
            '          value: "true"\n'
            "        - name: STARTUP_DELAY\n"
        ),
        (
            "        - name: ELB_SKIP_DB_VERIFY\n"
            '          value: "true"\n'
            "        - name: ELB_DB_WRITER_LOCK\n"
            '          value: "1"\n'
            "        - name: STARTUP_DELAY\n"
        ),
        "name: ELB_DB_WRITER_LOCK",
    )


# ---------------------------------------------------------------------------
# blast-run-aks.sh: inject a vmtouch step immediately before the blastn
# invocation.
#
# The upstream AKS variant of `blast-run-aks.sh` (unlike the NCBI reference
# `splitq_download_db_search`) skips vmtouch entirely. That left every BLAST
# search pod paying the full mmap-fault cost from cold SSD on the first
# query — and the separate warmup-Job vmtouch step that this dashboard used
# to ship was a 1-second noop on already-cached pages with no mmap holder
# (see docs/features_change/2026-06/2026-06-06-warmup-drop-fake-vmtouch.md).
#
# Restoring vmtouch *inside* the search pod fixes both:
#  * the pages it touches stay resident under memory pressure because the
#    `blastn` process that follows holds an active mmap on the same files
#    (the kernel deprioritises eviction of pages with active mappings);
#  * the work is colocated with `blastn` on the same node by elastic-blast's
#    own `nodeSelector: { ordinal: ${ELB_SHARD_IDX} }` pin, so the vmtouch
#    cost is paid exactly once per shard per pod and applies to the right
#    files.
#
# The patch is idempotent (guarded by the literal `ELB vmtouch warm step`
# marker) so re-running `patch_elastic_blast.py` against an already-patched
# tree is a no-op, matching the rest of this file's contract.
# ---------------------------------------------------------------------------

_BLAST_RUN_AKS_VMTOUCH_ANCHOR = 'start=$(date +%s)\necho "run start'
_BLAST_RUN_AKS_VMTOUCH_BLOCK = r"""# ELB vmtouch warm step (added by patch_elastic_blast.py).
# Touches the DB shard volume files into the page cache before BLAST starts so
# the first mmap fault path is RAM-resident. `blastn` then holds those pages
# under an active mapping for the duration of the search, which keeps the
# kernel from reclaiming them. ELB_VMTOUCH_DISABLE=1 skips the step.
if [ "${ELB_VMTOUCH_DISABLE:-0}" != "1" ] && command -v vmtouch >/dev/null 2>&1; then
    if command -v blastdb_path >/dev/null 2>&1; then
        vm_start=$(date +%s)
        # vmtouch -m caps the per-FILE size it will touch (it skips any single
        # volume file larger than this), not a cumulative cache budget. BLAST
        # DB volumes are typically GB-scale per file so 60% of MemAvailable
        # leaves any realistic volume well under the cap while still acting
        # as a safety rail for a pathologically large single file.
        elb_vmtouch_awk='/MemAvailable/ {printf "%dG", int($2/1024/1024*0.6)}'
        ELB_VMTOUCH_MEM=${ELB_VMTOUCH_MEM:-$(awk "$elb_vmtouch_awk" /proc/meminfo)}
        echo "vmtouch warm: db=${ELB_DB} mol=${ELB_DB_MOL_TYPE} budget=${ELB_VMTOUCH_MEM}"
        # Touch volumes serially with -t (read into cache, no daemon, no
        # mlock). The next `blastn` mmap reference is what actually keeps
        # the pages resident.
        blastdb_path -dbtype "$ELB_DB_MOL_TYPE" -db "$ELB_DB" -getvolumespath 2>/dev/null \
            | tr ' ' '\n' \
            | xargs -r -n1 vmtouch -tqm "$ELB_VMTOUCH_MEM" || true
        vm_end=$(date +%s)
        # Emit the runtime line BOTH on stdout (pod log) and into the
        # $BLAST_RUNTIME file so it ships to Blob via the existing
        # results-export-aks.sh `BLAST_RUNTIME-${JOB_NUM}.out` upload. That
        # lets the SPA later surface per-shard vmtouch timing without
        # plumbing a new artefact path.
        vm_db_label="vmtouch-${ELB_DB//\//-}"
        vm_runtime_line=$(printf 'RUNTIME %s %f seconds' "$vm_db_label" $((vm_end - vm_start)))
        echo "$vm_runtime_line"
        echo "$vm_runtime_line" >> "$BLAST_RUNTIME"
    fi
fi

"""

_BLAST_RUN_AKS_READER_LOCK_ACQUIRE_ANCHOR = (
    'if [[ ! -s "$RESULTS_DIR/BLASTDB_LENGTH.out" ]]; then\n'
)
_BLAST_RUN_AKS_READER_LOCK_ACQUIRE_BLOCK = r"""# ELB DB reader lock (added by patch_elastic_blast.py).
# Sharded local-SSD staging takes an exclusive flock on the same file. Hold a
# shared lock from the first DB metadata read through BLAST process completion
# so a cache repair or source-generation refresh cannot replace mmap payloads
# underneath an active search. Other readers remain concurrent.
if [ "${ELB_DB_READER_LOCK:-0}" = "1" ]; then
    READER_LOCK_WAIT_SECONDS="${ELB_DB_READER_LOCK_TIMEOUT_SECONDS:-5400}"
    case "$READER_LOCK_WAIT_SECONDS" in
      ''|*[!0-9]*) echo "ERROR: invalid DB reader lock timeout: ${READER_LOCK_WAIT_SECONDS}" >&2; exit 64 ;;
    esac
    if [ "${#READER_LOCK_WAIT_SECONDS}" -gt 4 ] \
            || [ "$READER_LOCK_WAIT_SECONDS" -lt 1 ] \
            || [ "$READER_LOCK_WAIT_SECONDS" -gt 5400 ]; then
        echo "ERROR: DB reader lock timeout must be between 1 and 5400 seconds" >&2
        exit 64
    fi
    if ! command -v flock >/dev/null 2>&1; then
        echo "ERROR: flock is required for safe node-local DB reads" >&2
        exit 69
    fi
    READER_LOCK_FILE=".elb-stage.lock"
    if ! exec 8>>"$READER_LOCK_FILE"; then
        echo "ERROR: unable to open DB reader lock file=${READER_LOCK_FILE}" >&2
        exit 73
    fi
    echo "DB_READER_LOCK_WAIT file=${READER_LOCK_FILE} timeout=${READER_LOCK_WAIT_SECONDS}s"
    READER_LOCK_WAIT_STARTED=$(date +%s)
    if ! flock -s -w "$READER_LOCK_WAIT_SECONDS" 8; then
        echo "ERROR: DB reader lock timeout file=${READER_LOCK_FILE} waited_seconds=$(( $(date +%s) - READER_LOCK_WAIT_STARTED ))" >&2
        exit 75
    fi
    ELB_DB_READER_LOCK_HELD=1
    echo "DB_READER_LOCK_ACQUIRED file=${READER_LOCK_FILE} waited_seconds=$(( $(date +%s) - READER_LOCK_WAIT_STARTED ))"
    if [[ "$ELB_DB" =~ _shard_[0-9]+$ ]]; then
        if [[ ! "$ELB_DB" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] \
                || [ "${#ELB_DB}" -gt 127 ]; then
            echo "ERROR: unsafe shard DB name for cache marker: ${ELB_DB}" >&2
            flock -u 8 || true
            exec 8>&-
            ELB_DB_READER_LOCK_HELD=0
            exit 64
        fi
        READER_CACHE_COMPLETE=".elb-cache.${ELB_DB}.complete"
        if [ ! -f "$READER_CACHE_COMPLETE" ]; then
            echo "ERROR: DB shard completion marker is missing db=${ELB_DB}" >&2
            flock -u 8 || true
            exec 8>&-
            ELB_DB_READER_LOCK_HELD=0
            exit 76
        fi
    fi
    if ! blastdbcmd -db "$ELB_DB" -info >/dev/null 2>&1; then
        echo "ERROR: DB cache integrity check failed after reader lock acquisition db=${ELB_DB}" >&2
        flock -u 8 || true
        exec 8>&-
        ELB_DB_READER_LOCK_HELD=0
        exit 76
    fi
    echo "DB_READER_CACHE_VERIFIED db=${ELB_DB}"
fi

"""
_BLAST_RUN_AKS_READER_LOCK_RELEASE_ANCHOR = "BLAST_EXIT_CODE=$?\n"
_BLAST_RUN_AKS_READER_LOCK_RELEASE_BLOCK = r"""BLAST_EXIT_CODE=$?
if [ "${ELB_DB_READER_LOCK_HELD:-0}" = "1" ]; then
    flock -u 8 || true
    exec 8>&-
    ELB_DB_READER_LOCK_HELD=0
    echo "DB_READER_LOCK_RELEASED file=${READER_LOCK_FILE}"
fi
"""


def _blast_run_aks_script_paths(root: Path) -> list[Path]:
    source_path = root / "src/elastic_blast/templates/scripts/blast-run-aks.sh"
    paths = [source_path]
    for pattern in (
        "venv/lib/python*/site-packages/elastic_blast/templates/scripts/blast-run-aks.sh",
        ".venv/lib/python*/site-packages/elastic_blast/templates/scripts/blast-run-aks.sh",
    ):
        paths.extend(root.glob(pattern))
    return sorted({path for path in paths if path.exists()})


def patch_blast_run_aks_script(root: Path) -> None:
    paths = _blast_run_aks_script_paths(root)
    if not paths:
        raise RuntimeError(f"blast-run-aks.sh not found under {root}")
    for path in paths:
        _replace_once_unless_present(
            path,
            _BLAST_RUN_AKS_VMTOUCH_ANCHOR,
            _BLAST_RUN_AKS_VMTOUCH_BLOCK + _BLAST_RUN_AKS_VMTOUCH_ANCHOR,
            "ELB vmtouch warm step",
        )
        patch_blast_run_aks_outfmt_argv(path)
        patch_blast_run_aks_reader_lock(path)


def patch_blast_run_aks_reader_lock(path: Path) -> None:
    """Fence sharded local-SSD cache writers for the BLAST read lifetime."""

    _replace_once_unless_present(
        path,
        _BLAST_RUN_AKS_READER_LOCK_ACQUIRE_ANCHOR,
        _BLAST_RUN_AKS_READER_LOCK_ACQUIRE_BLOCK + _BLAST_RUN_AKS_READER_LOCK_ACQUIRE_ANCHOR,
        "ELB DB reader lock (added by patch_elastic_blast.py)",
    )
    _replace_once_unless_present(
        path,
        _BLAST_RUN_AKS_READER_LOCK_RELEASE_ANCHOR,
        _BLAST_RUN_AKS_READER_LOCK_RELEASE_BLOCK,
        "DB_READER_LOCK_RELEASED file=${READER_LOCK_FILE}",
    )


# ---------------------------------------------------------------------------
# blast-run-aks.sh: pass BLAST options as a quote-safe argv array so a
# multi-token `-outfmt` specifier (e.g. `-outfmt 7 std staxids sstrand qseq
# sseq`, needed to surface subject taxids/names) reaches `blastn` as a SINGLE
# argument instead of being word-split into stray positional args.
#
# The canonical wire format is UNQUOTED — quotes break the raw YAML
# substitution elastic-blast uses to inject ELB_BLAST_OPTIONS into the pod env,
# so we cannot rely on shell quotes to group the specifier. Instead we rebuild
# an argv array from ELB_BLAST_OPTIONS, rejoining every token after `-outfmt`
# up to the next `-flag` (BLAST format field codes never start with `-`, and
# every other BLAST option takes a single-token value — only `-outfmt` is
# multi-token). For a single-token `-outfmt 5` (every job today) the array is
# byte-identical to the previous unquoted `$ELB_BLAST_OPTIONS` word-splitting,
# so existing runs are unchanged; only a multi-token specifier behaves
# differently (correctly grouped). No `eval`, no quotes — deterministic and
# unit-testable in isolation.
# ---------------------------------------------------------------------------

_BLAST_RUN_AKS_ARGV_ANCHOR = (
    "# shellcheck disable=SC2086\n"
    'TIME="$DATE_NOW run start $JOB_NUM $ELB_BLAST_PROGRAM $ELB_DB %e %U %S %P" \\\n'
)
_BLAST_RUN_AKS_ARGV_BLOCK = r"""# ELB outfmt argv rebuild (added by patch_elastic_blast.py).
# Rejoin a multi-token -outfmt specifier into a single argv element so it
# survives to blastn intact. Byte-identical to plain word-splitting for the
# single-token -outfmt every current job uses.
#
# Hardening: split ELB_BLAST_OPTIONS with glob DISABLED (set -f) and a known
# IFS so a stray glob metacharacter in the options can never expand a BLAST
# flag into matching filenames (the previous unquoted `$ELB_BLAST_OPTIONS`
# expansion did glob — this is strictly safer for the no-glob inputs BLAST
# options actually carry). The original noglob state is restored afterwards.
ELB_BLAST_ARGV=()
_elb_had_noglob=0
case "$-" in *f*) _elb_had_noglob=1 ;; esac
_elb_saved_ifs="$IFS"
set -f
IFS=$' \t\n'
# shellcheck disable=SC2206
_elb_opt_tokens=( $ELB_BLAST_OPTIONS )
IFS="$_elb_saved_ifs"
[ "$_elb_had_noglob" -eq 1 ] || set +f
_elb_i=0
while [ "$_elb_i" -lt "${#_elb_opt_tokens[@]}" ]; do
    _elb_tok="${_elb_opt_tokens[$_elb_i]}"
    if [ "$_elb_tok" = "-outfmt" ]; then
        ELB_BLAST_ARGV+=( "-outfmt" )
        _elb_i=$((_elb_i + 1))
        _elb_spec=""
        _elb_have_spec=0
        while [ "$_elb_i" -lt "${#_elb_opt_tokens[@]}" ] && [ "${_elb_opt_tokens[$_elb_i]:0:1}" != "-" ]; do
            if [ "$_elb_have_spec" -eq 0 ]; then
                _elb_spec="${_elb_opt_tokens[$_elb_i]}"
                _elb_have_spec=1
            else
                _elb_spec="$_elb_spec ${_elb_opt_tokens[$_elb_i]}"
            fi
            _elb_i=$((_elb_i + 1))
        done
        if [ "$_elb_have_spec" -eq 1 ]; then
            ELB_BLAST_ARGV+=( "$_elb_spec" )
        fi
    else
        ELB_BLAST_ARGV+=( "$_elb_tok" )
        _elb_i=$((_elb_i + 1))
    fi
done

"""


def patch_blast_run_aks_outfmt_argv(path: Path) -> None:
    """Rebuild BLAST options into a quote-safe argv array (multi-token outfmt).

    Skips gracefully when the TIME= invocation anchor is absent (e.g. a partial
    test stub or a layout this patch does not recognise), and raises only when
    the anchor is present but the invocation line has drifted — so a real
    upstream change cannot silently leave the rebuilt array unused.
    """
    text = path.read_text()
    if "ELB outfmt argv rebuild" in text:
        return
    if _BLAST_RUN_AKS_ARGV_ANCHOR not in text:
        return
    invocation_old = '-num_threads "$ELB_NUM_CPUS" \\\n$ELB_BLAST_OPTIONS \\\n2>"$ERROR_FILE"'
    invocation_new = '-num_threads "$ELB_NUM_CPUS" \\\n"${ELB_BLAST_ARGV[@]}" \\\n2>"$ERROR_FILE"'
    if invocation_old not in text:
        raise RuntimeError(
            "blast-run-aks.sh has the argv anchor but the blastn invocation line "
            "drifted; update patch_blast_run_aks_outfmt_argv before building"
        )
    text = text.replace(
        _BLAST_RUN_AKS_ARGV_ANCHOR,
        _BLAST_RUN_AKS_ARGV_BLOCK + _BLAST_RUN_AKS_ARGV_ANCHOR,
        1,
    )
    text = text.replace(invocation_old, invocation_new, 1)
    path.write_text(text)


def patch_aks_workload_tolerations(root: Path) -> None:
    templates = {
        "blast-batch-job-aks.yaml.template": "OnFailure",
        "blast-batch-job-local-ssd-aks.yaml.template": "OnFailure",
        "blast-batch-job-shard-ssd-aks.yaml.template": "OnFailure",
        "job-init-pv-aks.yaml.template": "Never",
        "job-init-pv-partitioned-aks.yaml.template": "Never",
        "job-init-local-ssd-aks.yaml.template": "Never",
        "job-init-ssd-shard-aks.yaml.template": "Never",
        "job-submit-jobs-aks.yaml.template": "Never",
        "vmtouch-daemonset-aks.yaml.template": "Always",
    }
    tolerations = """      tolerations:
      - key: workload
        operator: Equal
        value: blast
        effect: NoSchedule
"""
    node_selector = """      nodeSelector:
        workload: blast
"""
    for name, restart_policy in templates.items():
        path = root / "src/elastic_blast/templates" / name
        text = path.read_text()
        if "key: workload" not in text:
            old = f"      restartPolicy: {restart_policy}\n"
            new = f"      restartPolicy: {restart_policy}\n{tolerations}"
            _replace_once(path, old, new)
            text = path.read_text()
        duplicate = tolerations + node_selector + "      nodeSelector:\n"
        if duplicate in text:
            text = text.replace(
                duplicate,
                tolerations + "      nodeSelector:\n        workload: blast\n",
                1,
            )
        elif tolerations + "      nodeSelector:\n        workload: blast\n" in text:
            continue
        elif tolerations + "      nodeSelector:\n" in text:
            text = text.replace(
                tolerations + "      nodeSelector:\n",
                tolerations + "      nodeSelector:\n        workload: blast\n",
                1,
            )
        else:
            text = text.replace(tolerations, tolerations + node_selector, 1)
        path.write_text(text)


def patch_sharded_reader_lock_opt_in(root: Path) -> None:
    """Enable the DB reader lock for both node-local SSD search modes."""

    templates = root / "src/elastic_blast/templates"
    for name in (
        "blast-batch-job-local-ssd-aks.yaml.template",
        "blast-batch-job-shard-ssd-aks.yaml.template",
    ):
        path = templates / name
        _replace_once_unless_present(
            path,
            (
                "        - name: ELB_DB_MOL_TYPE\n"
                '          value: "${ELB_DB_MOL_TYPE}"\n'
                "        - name: QUERY_DIR\n"
            ),
            (
                "        - name: ELB_DB_MOL_TYPE\n"
                '          value: "${ELB_DB_MOL_TYPE}"\n'
                "        - name: ELB_DB_READER_LOCK\n"
                '          value: "1"\n'
                "        - name: QUERY_DIR\n"
            ),
            "name: ELB_DB_READER_LOCK",
        )
        _replace_once_unless_present(
            path,
            (
                "        - name: ELB_DB_READER_LOCK\n"
                '          value: "1"\n'
                "        - name: QUERY_DIR\n"
            ),
            (
                "        - name: ELB_DB_READER_LOCK\n"
                '          value: "1"\n'
                "        - name: ELB_VMTOUCH_DISABLE\n"
                '          value: "${ELB_VMTOUCH_DISABLE}"\n'
                "        - name: QUERY_DIR\n"
            ),
            "name: ELB_VMTOUCH_DISABLE",
        )


def patch_aks_job_ttl(root: Path) -> None:
    """Auto-delete completed BLAST Jobs after a bounded TTL.

    The pinned upstream ref predates upstream commit ba8075b1 (which added
    ``ttlSecondsAfterFinished`` to the batch-job templates), so finished
    ``blastn-batch-*`` Jobs are never garbage-collected on the persistent
    dashboard cluster -- they accumulate by the thousand and hold node
    ephemeral-storage. The per-job ``elb-finalizer-*`` Jobs accumulate the same
    way (one per completed search). Inject a literal ``ttlSecondsAfterFinished``
    at the Job.spec level into the three batch-job templates AND the finalizer
    template so the Kubernetes TTL-after-finished controller deletes each
    completed Job (and its pods) automatically.

    Safe by construction: the dashboard derives job status from persisted
    jobstate Table rows + the Storage ``SUCCESS.txt`` marker and reads results
    straight from Storage blobs, so deleting the finished k8s Job does not
    affect the Blast Jobs listing, job detail, or result retrieval. The TTL only
    governs GC AFTER the Job reaches a terminal state -- it does not change
    retry behaviour, so the finalizer's ``backoffLimit: 0`` (it is not safely
    retryable) is preserved. The warmup (``warm-*``) and init-ssd Jobs back the
    node-local DB cache and are managed by the dashboard warmup reconciler
    (which relies on the Job objects existing), so they are intentionally
    untouched.

    A literal value (not the upstream ``${ELB_JOB_TTL_SECONDS}`` template
    variable) is used on purpose: the pinned ref's ``azure.py`` builds batch-job
    substitutions across two separate dicts that do not provide that key, so a
    ``${...}`` placeholder would render unsubstituted and yield an invalid
    integer. Build-time override via the ``ELB_JOB_TTL_SECONDS`` env var
    (digits, seconds; default 1800 = 30 min).
    """
    raw = os.environ.get("ELB_JOB_TTL_SECONDS", "1800")
    if not raw.isdigit():
        print(
            f"warning: ELB_JOB_TTL_SECONDS={raw!r} is not numeric; using 1800",
            file=sys.stderr,
        )
    ttl = raw if raw.isdigit() else "1800"
    ttl_line = f"  ttlSecondsAfterFinished: {ttl}\n"
    deadline_raw = os.environ.get("ELB_FINALIZER_ACTIVE_DEADLINE_SECONDS", "1800")
    if not deadline_raw.isdigit() or int(deadline_raw) <= 0:
        print(
            "warning: ELB_FINALIZER_ACTIVE_DEADLINE_SECONDS="
            f"{deadline_raw!r} is not a positive integer; using 1800",
            file=sys.stderr,
        )
        deadline_raw = "1800"
    deadline_line = f"  activeDeadlineSeconds: {deadline_raw}\n"
    templates = [
        "blast-batch-job-aks.yaml.template",
        "blast-batch-job-local-ssd-aks.yaml.template",
        "blast-batch-job-shard-ssd-aks.yaml.template",
        "elb-finalizer-aks.yaml.template",
    ]
    for name in templates:
        path = root / "src/elastic_blast/templates" / name
        text = path.read_text()
        insertion = ""
        if "ttlSecondsAfterFinished" not in text:
            insertion += ttl_line
        if name == "elb-finalizer-aks.yaml.template" and "activeDeadlineSeconds" not in text:
            insertion += deadline_line
        if not insertion:
            continue
        # Anchor on the REAL Job.spec field: line-start + 2-space indent +
        # `backoffLimit:` + a numeric value. A line-anchored numeric match can
        # never hit a prose comment that merely mentions "backoffLimit" (e.g.
        # the finalizer template's "K8s default backoffLimit of 6"), so the
        # insertion point is deterministic rather than accidentally safe.
        matches = list(re.finditer(r"^  backoffLimit: *\d+ *$", text, re.MULTILINE))
        if not matches:
            raise RuntimeError(
                f"{name}: no Job.spec 'backoffLimit: <n>' anchor for "
                "ttlSecondsAfterFinished insertion"
            )
        insert_at = matches[-1].start()  # the last (sole) real field
        path.write_text(text[:insert_at] + insertion + text[insert_at:])


def patch_unique_init_ssd_job_names(root: Path) -> None:
    templates = [
        "job-init-local-ssd-aks.yaml.template",
        "job-init-ssd-shard-aks.yaml.template",
    ]
    for name in templates:
        path = root / "src/elastic_blast/templates" / name
        _replace_once_unless_present(
            path,
            "  name: init-ssd-${NODE_ORDINAL}\n",
            "  name: init-ssd-${BLAST_ELB_JOB_ID}-${NODE_ORDINAL}\n",
            "name: init-ssd-${BLAST_ELB_JOB_ID}-${NODE_ORDINAL}",
        )


def patch_create_workspace_daemonset_tolerations(root: Path) -> None:
    # The create-workspace DaemonSet (kube-system) bind-mounts a hostPath and
    # creates /workspace on every node so the init-ssd Jobs can later mount it.
    # Upstream ships it without tolerations, so it cannot land on the blast pool
    # nodes (taint workload=blast:NoSchedule). When the init-ssd Job is then
    # scheduled on a blast node, kubelet fails to bind-mount /workspace and the
    # pod sticks in CreateContainerConfigError with
    # "stat /workspace: no such file or directory". Add the matching toleration
    # so the DaemonSet runs on the blast pool too.
    templates = [
        "job-init-local-ssd-aks.yaml.template",
        "job-init-ssd-shard-aks.yaml.template",
    ]
    old = (
        "          type: DirectoryOrCreate\n      nodeSelector:\n        kubernetes.io/os: linux\n"
    )
    new = (
        "          type: DirectoryOrCreate\n"
        "      tolerations:\n"
        "      - key: workload\n"
        "        operator: Equal\n"
        "        value: blast\n"
        "        effect: NoSchedule\n"
        "      nodeSelector:\n"
        "        kubernetes.io/os: linux\n"
    )
    marker = (
        "      tolerations:\n"
        "      - key: workload\n"
        "        operator: Equal\n"
        "        value: blast\n"
        "        effect: NoSchedule\n"
        "      nodeSelector:\n"
        "        kubernetes.io/os: linux\n"
    )
    for name in templates:
        path = root / "src/elastic_blast/templates" / name
        _replace_once_unless_present(path, old, new, marker)


def patch_init_job_wait_filters(root: Path) -> None:
    path = root / "src/elastic_blast/kubernetes.py"
    _replace_once_unless_present(
        path,
        (
            "            cmd = f'kubectl --context={cfg.appstate.k8s_ctx} "
            "get jobs -o jsonpath=' \\\n"
        ),
        (
            "            cmd = f'kubectl --context={cfg.appstate.k8s_ctx} "
            "get jobs -l elb-job-id={cfg.azure.elb_job_id} -o jsonpath=' \\\n"
        ),
        "get jobs -l elb-job-id={cfg.azure.elb_job_id} -o jsonpath=",
        allow_absent=True,
    )
    _replace_once_unless_present(
        path,
        (
            "            cmd = f'kubectl --context={cfg.appstate.k8s_ctx} "
            "get jobs -l app=setup -o jsonpath=' \\\n"
        ),
        (
            "            cmd = f'kubectl --context={cfg.appstate.k8s_ctx} "
            "get jobs -l app=setup,elb-job-id={cfg.azure.elb_job_id} "
            "-o jsonpath=' \\\n"
        ),
        "get jobs -l app=setup,elb-job-id={cfg.azure.elb_job_id} -o jsonpath=",
        allow_absent=True,
    )
    _replace_all_unless_present(
        path,
        "cmd = f'kubectl --context={cfg.appstate.k8s_ctx} delete jobs -l app=setup'",
        (
            "cmd = f'kubectl --context={cfg.appstate.k8s_ctx} "
            "delete jobs --ignore-not-found=true "
            "-l app=setup,elb-job-id={cfg.azure.elb_job_id}'"
        ),
        "delete jobs --ignore-not-found=true -l app=setup,elb-job-id={cfg.azure.elb_job_id}",
    )


def patch_init_job_retry_tolerance(root: Path) -> None:
    """Treat only a terminal Kubernetes Job condition as init failure.

    ``status.failed`` counts failed Pods, including Pods that a Job with a
    ``backoffLimit`` is still retrying. The pinned runtime aborts the complete
    submit as soon as that counter becomes non-zero. Keep polling retryable
    Jobs and fail only after Kubernetes sets the Job's ``Failed`` condition.
    """

    path = root / "src/elastic_blast/kubernetes.py"
    helper = '''def _elb_init_job_states(raw: str) -> tuple[set[str], set[str], set[str]]:
    """Return pending, terminal-failed, and succeeded Job names."""
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as err:
        raise RuntimeError(f'Invalid init Job status response: {err}') from err
    items = payload.get('items') if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise RuntimeError('Invalid init Job status response: items must be a list')
    pending: set[str] = set()
    failed: set[str] = set()
    succeeded: set[str] = set()
    for job in items:
        if not isinstance(job, dict):
            raise RuntimeError('Invalid init Job status response: Job must be an object')
        metadata = job.get('metadata') or {}
        status = job.get('status') or {}
        name = str(metadata.get('name') or '')
        if not name or not isinstance(status, dict):
            raise RuntimeError('Invalid init Job status response: Job name/status is missing')
        conditions = status.get('conditions') or []
        if not isinstance(conditions, list):
            raise RuntimeError(f'Invalid init Job status response for {name}: conditions')
        terminal_failed = any(
            isinstance(condition, dict)
            and condition.get('type') == 'Failed'
            and str(condition.get('status') or '').lower() == 'true'
            for condition in conditions
        )
        complete = any(
            isinstance(condition, dict)
            and condition.get('type') == 'Complete'
            and str(condition.get('status') or '').lower() == 'true'
            for condition in conditions
        )
        try:
            succeeded_count = int(status.get('succeeded') or 0)
        except (TypeError, ValueError) as err:
            raise RuntimeError(
                f'Invalid init Job status response for {name}: succeeded count'
            ) from err
        if terminal_failed:
            failed.add(name)
        elif complete or succeeded_count > 0:
            succeeded.add(name)
        else:
            # Includes a failed Pod that is between Kubernetes backoff retries.
            pending.add(name)
    return pending, failed, succeeded


def _wait_for_elb_init_jobs(
    cfg: ElasticBlastConfig,
    *,
    selector: str,
    expected_job_names: set[str],
    timeout_seconds: int,
    failure_prefix: str,
    dry_run: bool,
) -> None:
    """Wait for the exact init Job set within one wall-clock deadline."""
    if not expected_job_names:
        raise ValueError('Expected init Job set must not be empty')
    deadline = timer() + timeout_seconds
    while True:
        remaining = deadline - timer()
        if remaining <= 0:
            missing = ' '.join(sorted(expected_job_names))
            raise TimeoutError(f'{failure_prefix} timed out: {missing}')
        cmd = (
            f'kubectl --context={cfg.appstate.k8s_ctx} get jobs '
            f'-l {selector} -o json'
        )
        if dry_run:
            logging.info(cmd)
            return
        proc = safe_exec(cmd, timeout=max(1, min(20, remaining)))
        pending, failed, succeeded = _elb_init_job_states(handle_error(proc.stdout))
        pending &= expected_job_names
        failed &= expected_job_names
        succeeded &= expected_job_names
        missing = expected_job_names - pending - failed - succeeded
        logging.debug(
            'Init Jobs pending=%s missing=%s succeeded=%s',
            ' '.join(sorted(pending)),
            ' '.join(sorted(missing)),
            ' '.join(sorted(succeeded)),
        )
        if failed:
            names = ' '.join(sorted(failed))
            try:
                logs = safe_exec(
                    f'kubectl --context={cfg.appstate.k8s_ctx} logs '
                    f'-l {selector} --all-containers=true --prefix=true --tail=-1',
                    timeout=max(1, min(20, deadline - timer())),
                )
                for line in handle_error(logs.stdout).split('\\n'):
                    if line:
                        logging.error(line)
            except Exception as err:
                logging.error('Failed to collect init Job logs: %s', err)
            raise RuntimeError(f'{failure_prefix}: {names}')
        if expected_job_names.issubset(succeeded):
            logging.debug('Init Jobs succeeded: %s', ' '.join(sorted(succeeded)))
            return
        remaining = deadline - timer()
        if remaining > 0:
            time.sleep(min(20, remaining))


'''
    _replace_once_unless_present(
        path,
        "def initialize_local_ssd(cfg: ElasticBlastConfig,",
        helper + "def initialize_local_ssd(cfg: ElasticBlastConfig,",
        "def _wait_for_elb_init_jobs(",
    )
    _replace_block_once_unless_present(
        path,
        start_marker="        # wait for multiple jobs\n",
        end_marker="            raise TimeoutError(f'{d} jobs timed out')\n",
        replacement="""        # Wait for the exact init Job set. A non-zero status.failed is only a
        # failed-Pod count while Kubernetes still has backoff retries available.
        if cfg.cloud_provider.cloud == CSP.AZURE:
            init_selector = f'elb-job-id={cfg.azure.elb_job_id}'
            expected_init_jobs = {
                f'init-ssd-{cfg.azure.elb_job_id}-{n}' for n in range(num_nodes)
            }
        else:
            init_selector = 'app=setup'
            expected_init_jobs = {f'init-ssd-{n}' for n in range(num_nodes)}
        _wait_for_elb_init_jobs(
            cfg,
            selector=init_selector,
            expected_job_names=expected_init_jobs,
            timeout_seconds=init_blastdb_minutes_timeout * 60,
            failure_prefix='Local SSD initialization jobs failed',
            dry_run=dry_run,
        )
""",
        marker="failure_prefix='Local SSD initialization jobs failed'",
    )
    _replace_block_once_unless_present(
        path,
        start_marker="        # Wait for all init jobs to complete\n",
        end_marker="            raise TimeoutError('Shard init jobs timed out')\n",
        replacement="""        # Scope to this runtime generation and require every expected shard Job.
        init_selector = f'app=setup,elb-job-id={cfg.azure.elb_job_id}'
        expected_init_jobs = {
            f'init-ssd-{cfg.azure.elb_job_id}-{n}'
            for n in range(min(num_shards, num_nodes))
        }
        _wait_for_elb_init_jobs(
            cfg,
            selector=init_selector,
            expected_job_names=expected_init_jobs,
            timeout_seconds=init_blastdb_minutes_timeout * 60,
            failure_prefix='Shard init jobs failed',
            dry_run=dry_run,
        )
""",
        marker="failure_prefix='Shard init jobs failed'",
    )


def verify_runtime_identity_templates(root: Path) -> None:
    """Fail closed when log-correlation labels drift out of AKS templates."""

    templates = (
        "job-init-ssd-shard-aks.yaml.template",
        "blast-batch-job-shard-ssd-aks.yaml.template",
        "elb-finalizer-aks.yaml.template",
    )
    marker = 'elb-job-id: "${BLAST_ELB_JOB_ID}"'
    for name in templates:
        path = root / "src/elastic_blast/templates" / name
        count = path.read_text().count(marker)
        if count < 2:
            raise RuntimeError(f"{name}: expected runtime identity label on Job and Pod metadata")


def main() -> int:
    if len(sys.argv) not in {2, 3}:
        print(
            "usage: patch_elastic_blast.py /path/to/elastic-blast-azure [merge-script]",
            file=sys.stderr,
        )
        return 2
    root = Path(sys.argv[1]).resolve()
    merge_script_source = (
        Path(sys.argv[2]).resolve()
        if len(sys.argv) == 3
        else Path(__file__).with_name("merge-sharded-results.sh")
    )
    if not (root / "src/elastic_blast").is_dir():
        print(f"not an elastic-blast-azure source tree: {root}", file=sys.stderr)
        return 2
    if not merge_script_source.is_file():
        print(f"merge script not found: {merge_script_source}", file=sys.stderr)
        return 2

    patch_azure_py(root)
    patch_partitioned_outfmt_gate(root)
    patch_disk_backed_monolithic_mode(root)
    patch_requested_max_target_seqs(root)
    patch_azure_cli_glue(root)
    patch_kubectl_transient_retries(root)
    patch_azure_traits(root)
    patch_finalizer_template(root)
    patch_finalizer_script(root, merge_script_source)
    patch_init_shard_script(root)
    patch_init_db_download_writer_lock(root)
    patch_blast_run_aks_script(root)
    patch_aks_workload_tolerations(root)
    patch_sharded_reader_lock_opt_in(root)
    patch_aks_job_ttl(root)
    patch_unique_init_ssd_job_names(root)
    patch_create_workspace_daemonset_tolerations(root)
    patch_init_job_wait_filters(root)
    patch_init_job_retry_tolerance(root)
    verify_runtime_identity_templates(root)
    print("patched elastic-blast-azure finalizer for sharded result merge")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
