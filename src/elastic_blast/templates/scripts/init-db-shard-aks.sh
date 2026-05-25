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
PATTERN="${PATTERN};taxdb.btd;taxdb.bti;taxonomy4blast.sqlite3;${ORIG_DB}.ndb;${ORIG_DB}.ntf;${ORIG_DB}.nto"
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
