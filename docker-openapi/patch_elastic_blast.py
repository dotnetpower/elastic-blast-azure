#!/usr/bin/env python3
# ruff: noqa: E501
"""Patch the vendored elastic-blast-azure clone for dashboard sharded runs.

Responsibility: Patch the vendored elastic-blast-azure clone for dashboard sharded runs
Edit boundaries: Keep terminal-side behavior here; api/worker callers should use service
wrappers.
Key entry points: `_replace_once`, `_replace_once_unless_present`,
`_replace_all_unless_present`, `patch_azure_py`, `patch_azure_cli_glue`,
`patch_finalizer_template`, `patch_finalizer_script`
Risky contracts: Do not expose terminal services directly to the internet or log secrets.
Validation: `uv run pytest -q api/tests/test_terminal_toolchain.py
api/tests/test_terminal_command_guard.py`.
"""

from __future__ import annotations

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


def patch_azure_cli_glue(root: Path) -> None:
    path = root / "src/elastic_blast/azure_cli_glue.py"
    _replace_once_unless_present(
        path,
        (
            "    # Phase 3: success -> structured ACCEPTED.\n"
            "    if json_mode and rc == 0:\n"
        ),
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
            "    'Standard_D64s_v3': 3.072,\n" f"{price_entries}",
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
            '--log-level=ERROR 2>/dev/null; then\n'
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
        (
            "                    if ! zcat \"$f\" | awk '!/^#/' "
            '>> "$MERGE_INPUT"; then\n'
        ),
        (
            "                    if ! zcat \"$f\" | awk '/^# Fields:/ || !/^#/' "
            '>> "$MERGE_INPUT"; then\n'
        ),
        "awk '/^# Fields:/ || !/^#/'",
    )
    _replace_once_unless_present(
        path,
        (
            '        TOTAL_ROWS=$(wc -l < "$MERGE_INPUT" 2>/dev/null || echo 0)\n'
            '        echo "Downloaded $SHARD_COUNT shard files, $TOTAL_ROWS tabular rows"\n\n'
            '        if ! /scripts/merge-sharded-results.sh \\\n'
        ),
        (
            '        TOTAL_ROWS=$(wc -l < "$MERGE_INPUT" 2>/dev/null || echo 0)\n'
            '        echo "Downloaded $SHARD_COUNT shard files, $TOTAL_ROWS tabular rows"\n\n'
            '        ORACLE_FILE="$MERGE_DIR/tie-order-oracle.txt"\n'
            '        ORACLE_SEARCH_BASES="$ELB_RESULTS"\n'
            '        ORACLE_PARENT_RESULTS="${ELB_RESULTS%/job-*}"\n'
            '        if [ "$ORACLE_PARENT_RESULTS" != "$ELB_RESULTS" ]; then\n'
            '            ORACLE_SEARCH_BASES="$ORACLE_SEARCH_BASES $ORACLE_PARENT_RESULTS"\n'
            '        fi\n'
            '        for ORACLE_BASE in $ORACLE_SEARCH_BASES; do\n'
            '            [ -n "${ELB_TIE_ORDER_FILE:-}" ] && break\n'
            '            ORACLE_BLOB="${ORACLE_BASE}/${ELB_METADATA_DIR}/tie-order-oracle.txt"\n'
            '            if blob_exists "$ORACLE_BLOB"; then\n'
            '                if azcopy cp "$ORACLE_BLOB" "$ORACLE_FILE" '
            '--log-level=ERROR 2>/dev/null; then\n'
            '                    export ELB_TIE_ORDER_FILE="$ORACLE_FILE"\n'
            '                    export ELB_TIE_ORDER_BASE="$ORACLE_BASE"\n'
            '                    echo "Using tie-order oracle from ${ORACLE_BLOB}"\n'
            '                else\n'
            '                    echo "WARNING: tie-order oracle exists but could not be '
            'downloaded: ${ORACLE_BLOB}"\n'
            '                fi\n'
            '            fi\n'
            '        done\n\n'
            '        if [ -z "${ELB_TIE_ORDER_FILE:-}" ]; then\n'
            '            for ORACLE_BASE in $ORACLE_SEARCH_BASES; do\n'
            '                [ -n "${ELB_TIE_ORDER_FILE:-}" ] && break\n'
            '                ORACLE_URLS_BLOB="${ORACLE_BASE}/${ELB_METADATA_DIR}/'
            'tie-order-oracle-urls.txt"\n'
            '                if blob_exists "$ORACLE_URLS_BLOB"; then\n'
            '                    ORACLE_URLS_FILE="$MERGE_DIR/tie-order-oracle-urls.txt"\n'
            '                    ORACLE_PART_DIR="$MERGE_DIR/tie-order-oracle-parts"\n'
            '                    mkdir -p "$ORACLE_PART_DIR"\n'
            '                    if azcopy cp "$ORACLE_URLS_BLOB" "$ORACLE_URLS_FILE" '
            '--log-level=ERROR 2>/dev/null; then\n'
            '                        idx=0\n'
            '                        while IFS= read -r part_url; do\n'
            '                            [ -z "$part_url" ] && continue\n'
            '                            part_file=$(printf "%s/part-%06d.txt" '
            '"$ORACLE_PART_DIR" "$idx")\n'
            '                            if ! azcopy cp "$part_url" "$part_file" '
            '--log-level=ERROR 2>/dev/null; then\n'
            '                                echo "WARNING: tie-order oracle part could not '
            'be downloaded: ${part_url}"\n'
            '                                rm -f "$part_file"\n'
            '                            fi\n'
            '                            idx=$((idx + 1))\n'
            '                        done < "$ORACLE_URLS_FILE"\n'
            '                        if find "$ORACLE_PART_DIR" -type f '
            '-name "part-*.txt" | grep -q .; then\n'
            '                            find "$ORACLE_PART_DIR" -type f '
            '-name "part-*.txt" | sort | xargs cat > "$ORACLE_FILE"\n'
            '                            export ELB_TIE_ORDER_FILE="$ORACLE_FILE"\n'
            '                            export ELB_TIE_ORDER_BASE="$ORACLE_BASE"\n'
            '                            echo "Using DB-order tie oracle parts from '
            '${ORACLE_URLS_BLOB}"\n'
            '                        fi\n'
            '                    fi\n'
            '                fi\n'
            '            done\n'
            '        fi\n'
            '        if [ -n "${ELB_TIE_ORDER_FILE:-}" ]; then\n'
            '            ORACLE_STRICT_BLOB="${ELB_TIE_ORDER_BASE:-$ELB_RESULTS}/'
            '${ELB_METADATA_DIR}/'
            'tie-order-oracle-strict.txt"\n'
            '            if blob_exists "$ORACLE_STRICT_BLOB"; then\n'
            '                export ELB_TIE_ORDER_STRICT="1"\n'
            '            fi\n'
            '        fi\n\n'
            '        if ! /scripts/merge-sharded-results.sh \\\n'
        ),
        "ELB_TIE_ORDER_FILE",
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
echo "Downloading manifest: ${MANIFEST_URL}"
retry_azcopy cp "${MANIFEST_URL}" /tmp/manifest.txt --log-level=ERROR || {
    echo "ERROR: manifest download failed"
    exit 1
}
retry_azcopy cp "${NAL_URL}" "./${ELB_DB}.nal" --log-level=ERROR || true
VOLUMES=$(cat /tmp/manifest.txt)
echo "Volumes: ${VOLUMES}"

DB_BASE_URL=$(echo "${ELB_PARTITION_PREFIX}" | sed 's|/[^/]*/[^/]*$|/|')
ORIG_DB=$(echo "${ELB_DB}" | sed 's/_shard_[0-9]*$//')
DB_URL="${DB_BASE_URL}${ORIG_DB}/"
echo "DB base URL: ${DB_URL}"

EXPECTED_SOURCE_VERSION="${ELB_DB_SOURCE_VERSION:-}"
if [ -z "$EXPECTED_SOURCE_VERSION" ]; then
    METADATA_URL="${DB_BASE_URL}${ORIG_DB}-metadata.json"
    echo "Resolving DB source version: ${METADATA_URL}"
    if retry_azcopy cp "${METADATA_URL}" /tmp/db-metadata.json --log-level=ERROR; then
        if command -v python3 >/dev/null 2>&1; then
            EXPECTED_SOURCE_VERSION=$(python3 -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    print(str(json.load(handle).get("source_version") or ""))
' /tmp/db-metadata.json 2>/dev/null || true)
        else
            EXPECTED_SOURCE_VERSION=$(sed -n \
                's/.*"source_version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
                /tmp/db-metadata.json | head -1)
        fi
        if [ -n "$EXPECTED_SOURCE_VERSION" ]; then
            echo "DB source version: ${EXPECTED_SOURCE_VERSION}"
        else
            echo "WARNING: DB metadata did not contain source_version"
        fi
    else
        echo "WARNING: DB metadata source-version lookup failed;" \
            "cache freshness marker will not be checked"
    fi
fi

write_volpaths() {
    local volpaths=""
    for volume in $VOLUMES; do
        [ -n "$volpaths" ] && volpaths="$volpaths "
        volpaths="${volpaths}$(pwd)/${volume}"
    done
    echo "VOLPATHS=${volpaths}" > /tmp/shard_volpaths.txt
    echo "Volume paths: ${volpaths}"
}

if find . -maxdepth 1 -name '.azDownload-*' | grep -q .; then
    echo "CLEANUP partial downloads"
    find . -maxdepth 1 -name '.azDownload-*' -exec rm -rf {} +
fi

payload_ext="nsq"
if [ "${ELB_DB_MOL_TYPE:-nucl}" = "prot" ]; then
    payload_ext="psq"
fi
missing_volume="0"
if [ -f .download-complete ]; then
    for volume in $VOLUMES; do
        if [ ! -s "${volume}.${payload_ext}" ]; then
            missing_volume="1"
            echo "CACHE_INCOMPLETE missing ${volume}.${payload_ext}"
        fi
    done
    if [ "$missing_volume" != "0" ]; then
        rm -f .download-complete
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
if [ -f .download-complete ] && [ -s "${ORIG_DB}.ntf" ] \
    && { [ ! -s "${ORIG_DB}.not" ] || [ ! -s "${ORIG_DB}.nos" ]; }; then
    echo "CACHE_INCOMPLETE missing taxonomy filter index ${ORIG_DB}.not/.nos"
    rm -f .download-complete
fi

if [ -f .download-complete ] && [ -n "$EXPECTED_SOURCE_VERSION" ]; then
    if [ ! -f .download-source-version ]; then
        echo "CACHE_STALE missing source-version marker"
        rm -f .download-complete
    elif [ "$(cat .download-source-version)" != "$EXPECTED_SOURCE_VERSION" ]; then
        echo "CACHE_STALE source-version mismatch"
        rm -f .download-complete
    fi
fi

if [ -f .download-complete ]; then
    echo "DOWNLOAD_SKIP existing shard=${ELB_SHARD_IDX}"
    write_volpaths
    exit 0
fi

PATTERN=""
for VOL in $VOLUMES; do
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
if [ ! -s taxdb.btd ] || [ ! -s taxdb.bti ]; then
    echo "TAXDB_SKIP taxdb files not present in DB prefix"
fi

write_volpaths
printf '%s' ok > .download-complete
if [ -n "$EXPECTED_SOURCE_VERSION" ]; then
    printf '%s' "$EXPECTED_SOURCE_VERSION" > .download-source-version
fi

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
    '# shellcheck disable=SC2086\n'
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
        if "nodeSelector:\n        workload: blast" not in text:
            insert_at = text.index(tolerations) + len(tolerations)
            text = text[:insert_at] + node_selector + text[insert_at:]
            path.write_text(text)


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
            "  name: init-ssd-${BLAST_ELB_JOB_ID_SHORT}-${NODE_ORDINAL}\n",
            "name: init-ssd-${BLAST_ELB_JOB_ID_SHORT}-${NODE_ORDINAL}",
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
        "          type: DirectoryOrCreate\n"
        "      nodeSelector:\n"
        "        kubernetes.io/os: linux\n"
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
    )
    _replace_all_unless_present(
        path,
        "cmd = f'kubectl --context={cfg.appstate.k8s_ctx} delete jobs -l app=setup'",
        (
            "cmd = f'kubectl --context={cfg.appstate.k8s_ctx} "
            "delete jobs -l app=setup,elb-job-id={cfg.azure.elb_job_id}'"
        ),
        "delete jobs -l app=setup,elb-job-id={cfg.azure.elb_job_id}",
    )


def patch_init_job_retry_tolerance(root: Path) -> None:
    # The sharded local-SSD init wait loop aborts the WHOLE submit the instant
    # any init Job shows .status.failed >= 1. But .status.failed is the FAILED-
    # POD counter, and these init Jobs carry a backoffLimit: a transient pod
    # failure (e.g. a throttled azcopy in import-query-batches) is retried by
    # k8s and the Job ultimately SUCCEEDS. Concurrent submits make those
    # transient failures common, so ~1-in-6 submits aborted even though every
    # init Job eventually completed (live-confirmed: the Job ended
    # backoffLimit=3 failed=1 succeeded=1 = Complete). Treat a Job as failed
    # ONLY when its own `Failed` condition is set (backoffLimit exhausted), and
    # finish only when nothing is active AND nothing is pending a retry.
    path = root / "src/elastic_blast/kubernetes.py"
    _replace_once_unless_present(
        path,
        (
            "            if failed:\n"
            "                raise RuntimeError(f'Shard init jobs failed: {failed}')\n"
            "            if not active:\n"
            "                logging.debug(f'Shard init jobs succeeded: {succeeded}')\n"
            "                break\n"
        ),
        (
            "            active_set = set(active.split())\n"
            "            failed_set = set(failed.split())\n"
            "            succeeded_set = set(succeeded.split())\n"
            "            fatal = []\n"
            "            for jn in sorted(failed_set - succeeded_set - active_set):\n"
            "                try:\n"
            "                    cond = safe_exec(\n"
            "                        f'kubectl --context={cfg.appstate.k8s_ctx} get job {jn} '\n"
            "                        '-o jsonpath={.status.conditions[*].type}')\n"
            "                except Exception:\n"
            "                    continue\n"
            "                if 'Failed' in handle_error(cond.stdout).split():\n"
            "                    fatal.append(jn)\n"
            "            if fatal:\n"
            "                names = ' '.join(fatal)\n"
            "                raise RuntimeError(f'Shard init jobs failed: {names}')\n"
            "            if not active_set and not (failed_set - succeeded_set):\n"
            "                logging.debug(f'Shard init jobs succeeded: {succeeded}')\n"
            "                break\n"
        ),
        "failed_set = set(failed.split())",
    )
    # Same retry-intolerance exists in the NON-sharded local-SSD init wait loop
    # (initialize_local_ssd). The customer path is sharded, but fix it too so a
    # single-DB local-SSD submit is equally resilient.
    _replace_once_unless_present(
        path,
        (
            "            if failed:\n"
            "                proc = safe_exec(f'kubectl --context={cfg.appstate.k8s_ctx} logs -l app=setup')\n"
            "                for line in handle_error(proc.stdout).split('\\n'):\n"
            "                    logging.debug(line)\n"
            "                raise RuntimeError(f'Local SSD initialization jobs failed: {failed}')\n"
            "            if not active:\n"
            "                logging.debug(f'Local SSD initialization jobs succeeded: {succeeded}')\n"
            "                break\n"
        ),
        (
            "            active_set = set(active.split())\n"
            "            failed_set = set(failed.split())\n"
            "            succeeded_set = set(succeeded.split())\n"
            "            fatal = []\n"
            "            for jn in sorted(failed_set - succeeded_set - active_set):\n"
            "                try:\n"
            "                    cond = safe_exec(\n"
            "                        f'kubectl --context={cfg.appstate.k8s_ctx} get job {jn} '\n"
            "                        '-o jsonpath={.status.conditions[*].type}')\n"
            "                except Exception:\n"
            "                    continue\n"
            "                if 'Failed' in handle_error(cond.stdout).split():\n"
            "                    fatal.append(jn)\n"
            "            if fatal:\n"
            "                proc = safe_exec(f'kubectl --context={cfg.appstate.k8s_ctx} logs -l app=setup')\n"
            "                for line in handle_error(proc.stdout).split('\\n'):\n"
            "                    logging.debug(line)\n"
            "                names = ' '.join(fatal)\n"
            "                raise RuntimeError(f'Local SSD initialization jobs failed: {names}')\n"
            "            if not active_set and not (failed_set - succeeded_set):\n"
            "                logging.debug(f'Local SSD initialization jobs succeeded: {succeeded}')\n"
            "                break\n"
        ),
        "raise RuntimeError(f'Local SSD initialization jobs failed: {names}')",
    )


def patch_qs_docker_version(root: Path) -> None:
    # Bump the query-split (qs) image tag so the cluster pulls a NEW tag. The
    # init-ssd Job has no imagePullPolicy (=> IfNotPresent), so overwriting the
    # old tag would not reliably reach cache-warm nodes; a fresh tag forces a
    # clean pull. The new tag carries the import-query-batches azcopy retry
    # hardening in docker-qs/run.sh.
    path = root / "src/elastic_blast/constants.py"
    _replace_once_unless_present(
        path,
        "ELB_QS_DOCKER_VERSION = '0.1.4'\n",
        "ELB_QS_DOCKER_VERSION = '0.1.5'\n",
        "ELB_QS_DOCKER_VERSION = '0.1.5'",
    )


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
    patch_azure_cli_glue(root)
    patch_azure_traits(root)
    patch_finalizer_template(root)
    patch_finalizer_script(root, merge_script_source)
    patch_init_shard_script(root)
    patch_blast_run_aks_script(root)
    patch_aks_workload_tolerations(root)
    patch_unique_init_ssd_job_names(root)
    patch_create_workspace_daemonset_tolerations(root)
    patch_init_job_wait_filters(root)
    patch_init_job_retry_tolerance(root)
    patch_qs_docker_version(root)
    print("patched elastic-blast-azure finalizer for sharded result merge")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
