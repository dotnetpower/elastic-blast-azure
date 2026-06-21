"""Pure, self-contained helpers for the ElasticBLAST OpenAPI server.

Everything here is free of I/O, Azure/Kubernetes coupling, and module-level
configuration state: timestamp math, value normalisation, input validation,
small parsing utilities, and opaque cursor (de)serialisation. Keeping these
out of ``main`` shrinks the application module and lets each helper be unit
tested in isolation.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import time
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import urlparse

from fastapi import HTTPException


# ── Timestamps & durations ─────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(value: Any) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _duration_seconds(start: Any, end: Any | None = None) -> int | None:
    start_ts = _parse_ts(start)
    if start_ts <= 0:
        return None
    end_ts = _parse_ts(end) if end else time.time()
    if end_ts <= 0:
        return None
    return max(0, int(end_ts - start_ts))


def _age_seconds(value: Any) -> float:
    ts = _parse_ts(value)
    return max(0.0, time.time() - ts) if ts else 0.0


# ── Value normalisation & validation ───────────────────────────────────────

def _safe_label_value(value: Any, default: str = "unknown") -> str:
    raw = re.sub(r"[^a-z0-9_.-]", "-", str(value or default).lower()).strip(".-_")
    return (raw or default)[:63]


def _safe_detail_value(value: str | None, *, max_len: int = 128) -> str:
    raw = str(value or "").strip()
    if len(raw) > max_len:
        raise HTTPException(400, f"value must be at most {max_len} characters")
    if re.search(r"[\x00-\x1f]", raw):
        raise HTTPException(400, "value must not contain control characters")
    return raw


def _job_id_from_idempotency_key(key: str) -> str:
    cleaned = key.strip()
    if not cleaned or len(cleaned) > 256:
        raise HTTPException(400, "idempotency_key must be 1-256 characters")
    return hashlib.sha256(cleaned.encode("utf-8")).hexdigest()[:12]


def _db_name_from_value(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme == "https":
        parts = [p for p in parsed.path.split("/") if p]
        return parts[-1] if parts else "unknown"
    return value.strip().strip("/").split("/")[-1] or "unknown"


def _validate_short_blob_name(value: str, field: str) -> None:
    if not value or value.startswith("/") or ".." in value or "?" in value or "#" in value:
        raise ValueError(f"{field} must be a safe blob path without query strings or traversal")


def _sanitize_job_id(job_id: str) -> str:
    cleaned = re.sub(r"[^a-f0-9]", "", job_id.lower())
    if len(cleaned) < 6 or len(cleaned) > 12:
        raise HTTPException(status_code=400, detail="Invalid job_id format")
    return cleaned


def _safe_result_filename(value: str) -> str:
    filename = str(value or "").strip()
    if not re.match(r"^[A-Za-z0-9._-]{1,128}\.(?:xml|out)(?:\.gz)?$", filename, re.IGNORECASE):
        raise HTTPException(400, "Invalid result filename")
    return filename


def _safe_result_blob_path(value: str, fallback_filename: str) -> str:
    blob_path = str(value or fallback_filename).strip().lstrip("/")
    if ".." in blob_path or "?" in blob_path or "#" in blob_path:
        raise HTTPException(400, "Invalid result blob path")
    if not re.match(r"^[A-Za-z0-9._/-]{1,512}\.(?:xml|out)(?:\.gz)?$", blob_path, re.IGNORECASE):
        raise HTTPException(400, "Invalid result blob path")
    if not blob_path.split("/")[-1].startswith("batch_"):
        raise HTTPException(400, "Invalid result blob path")
    return blob_path


# ── Molecule type mapping ──────────────────────────────────────────────────

_MOLECULE_TYPE_MAP: dict[str, tuple[Literal["dna", "protein"], str]] = {
    "nucl": ("dna", "mixed DNA"),
    "nucleotide": ("dna", "mixed DNA"),
    "nucleotides": ("dna", "mixed DNA"),
    "dna": ("dna", "mixed DNA"),
    "prot": ("protein", "protein"),
    "protein": ("protein", "protein"),
    "proteins": ("protein", "protein"),
}


def _resolve_molecule_type(
    molecule_type_raw: str | None,
) -> tuple[Literal["dna", "protein"], str]:
    """Map a raw molecule token to ``(molecule_type, molecule_label)``.

    Unknown tokens raise ``ValueError`` rather than silently falling
    back to ``protein`` so a future NCBI molecule class surfaces as an
    operator-visible 500 instead of a silently mislabelled response.
    """
    key = (molecule_type_raw or "").casefold()
    if not key:
        raise ValueError("empty molecule_type")
    try:
        return _MOLECULE_TYPE_MAP[key]
    except KeyError as exc:
        raise ValueError(f"unsupported molecule_type: {molecule_type_raw!r}") from exc


def _molecule_label(molecule_type_raw: str | None) -> str:
    """Friendly label for the BLAST molecule type (matches the dashboard).

    Unknown values pass through unchanged. Use :func:`_resolve_molecule_type`
    inside the v1 metadata path where unknown values should surface as an
    error rather than a guess.
    """
    if not molecule_type_raw:
        return ""
    try:
        _, label = _resolve_molecule_type(molecule_type_raw)
    except ValueError:
        return molecule_type_raw
    return label


# ── Cache & parsing utilities ──────────────────────────────────────────────

def _cache_trim(cache: "OrderedDict[Any, Any]", limit: int) -> None:
    """Trim ``cache`` down to ``limit`` entries, evicting the LRU end.

    Callers must hold the cache's lock. ``OrderedDict.popitem(last=False)``
    removes the least-recently-touched entry, so callers that promote on
    hit via ``move_to_end`` get true access-LRU behaviour.
    """
    while len(cache) > limit:
        cache.popitem(last=False)


def _last_json(stdout: str) -> dict[str, Any] | None:
    for line in reversed(stdout.splitlines()):
        candidate = line.strip()
        if not candidate.startswith("{") or not candidate.endswith("}"):
            continue
        try:
            decoded = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(decoded, dict):
            return decoded
    return None


def _cleanup_tmp(*paths: str) -> None:
    for p in paths:
        try:
            if os.path.isdir(p): shutil.rmtree(p, ignore_errors=True)
            elif os.path.isfile(p): os.remove(p)
        except Exception: pass


# ── Config redaction ───────────────────────────────────────────────────────

_CONFIG_SECRET_PARTS = (
    "key", "secret", "token", "password", "passwd", "pwd",
    "credential", "sas", "signature", "sig",
    "connection_string", "connectionstring",
)


def _redact_config(cfg: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    """Mask any value whose key looks like a secret, in case the deployment
    accidentally ships a stale ``elb-cfg.ini`` containing a storage account
    key, SAS token, or other credential. The endpoint is auth-gated, but
    operators may still legitimately copy the response into chat or tickets,
    so leaking secrets through this surface is unacceptable.
    """
    redacted: dict[str, dict[str, str]] = {}
    for section, items in cfg.items():
        out: dict[str, str] = {}
        for k, v in items.items():
            lower_k = k.lower()
            if any(part in lower_k for part in _CONFIG_SECRET_PARTS):
                out[k] = "***REDACTED***"
            else:
                out[k] = v
        redacted[section] = out
    return redacted


# ── Pagination cursor ──────────────────────────────────────────────────────

# Opaque keyset cursor for /v1/jobs pagination: base64 of
# "<created_at>\x1f<job_id>". The dashboard proxy folds this into its combined
# page cursor (dotnetpower/elb-dashboard#51); callers that omit ``limit`` get the
# full unpaginated list, so older clients are unaffected.
def _encode_jobs_cursor(created_at: str, job_id: str) -> str:
    raw = f"{created_at}\x1f{job_id}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_jobs_cursor(cursor: str) -> tuple[str, str] | None:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        created_at, job_id = raw.split("\x1f", 1)
        return created_at, job_id
    except Exception:
        return None
