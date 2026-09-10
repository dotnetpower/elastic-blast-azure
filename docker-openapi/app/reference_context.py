"""Resolve validated Web BLAST statistical context from a completed NCBI RID.

Responsibility: Fetch bounded XML/XML2 evidence from the fixed NCBI BLAST URL and derive the
six-field statistical context required by precise single-query execution.
Edit boundaries: Keep HTTP, XML parsing, and deterministic context derivation here; the OpenAPI
route owns authentication and HTTP error mapping.
Key entry points: `derive_reference_context`, `resolve_reference_context`,
`clear_reference_context_cache`.
Risky contracts: Never submit or poll an NCBI search, never accept a caller-controlled URL, never
guess filtered counts, and fail closed when query length, database counts, or integer formulas
differ. Result formats cannot prove original query content, submit options, or source-version ID.
Validation: `uv run pytest -q api/tests/test_openapi_reference_context.py`.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import os
import re
import threading
import time
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from zipfile import BadZipFile, ZipFile

import requests
from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException

NCBI_BLAST_URL = "https://blast.ncbi.nlm.nih.gov/blast/Blast.cgi"
_RID_RE = re.compile(r"^[A-Z0-9]{8,16}$")
_MAX_RESPONSE_BYTES = 128 * 1024 * 1024
_MAX_XML2_MEMBERS = 16
_UINT32_MODULUS = 1 << 32
_CACHE_TTL_SECONDS = 6 * 60 * 60
_CACHE_MAX_ENTRIES = 128
_FETCH_LOCK_WAIT_SECONDS = 2.0
_FETCH_LOCK = threading.Lock()
_CACHE_LOCK = threading.Lock()
_CACHE: dict[tuple[object, ...], tuple[float, dict[str, Any]]] = {}
_LAST_REQUEST_AT = 0.0


class ReferenceContextError(ValueError):
    """Raised when reference evidence cannot produce an exact context."""


class ReferenceContextNotReady(ReferenceContextError):
    """Raised when an NCBI RID has not reached a result-bearing state."""


class ReferenceContextUnavailable(RuntimeError):
    """Raised when the fixed NCBI result endpoint cannot be reached safely."""


@dataclass(frozen=True, slots=True)
class _Xml1Evidence:
    program: str
    database: str
    query_id: str
    query_length: int
    database_sequences: int
    database_letters_raw: int


@dataclass(frozen=True, slots=True)
class _Xml2Evidence:
    database_sequences: int
    database_letters_raw: int
    length_adjustment: int
    effective_search_space: int


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _first_text(root: ET.Element, name: str) -> str:
    for node in root.iter():
        if _local_name(node.tag) == name:
            return (node.text or "").strip()
    return ""


def _one_node(root: ET.Element, name: str, *, source: str) -> ET.Element:
    nodes = [node for node in root.iter() if _local_name(node.tag) == name]
    if len(nodes) != 1:
        raise ReferenceContextError(
            f"{source} must contain exactly one {name} block; found {len(nodes)}"
        )
    return nodes[0]


def _positive_int(root: ET.Element, name: str, *, source: str) -> int:
    value = _first_text(root, name)
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ReferenceContextError(f"{source} {name} must be an integer") from exc
    if parsed <= 0:
        raise ReferenceContextError(f"{source} {name} must be positive")
    return parsed


def _nonnegative_int(root: ET.Element, name: str, *, source: str) -> int:
    value = _first_text(root, name)
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ReferenceContextError(f"{source} {name} must be an integer") from exc
    if parsed < 0:
        raise ReferenceContextError(f"{source} {name} must be non-negative")
    return parsed


def _bounded_decompress(payload: bytes, *, source: str) -> bytes:
    if len(payload) > _MAX_RESPONSE_BYTES:
        raise ReferenceContextError(f"{source} exceeds the 128 MiB compressed response limit")
    if not payload.startswith(b"\x1f\x8b"):
        return payload
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as stream:
        expanded = stream.read(_MAX_RESPONSE_BYTES + 1)
    if len(expanded) > _MAX_RESPONSE_BYTES:
        raise ReferenceContextError(f"{source} expands beyond 128 MiB")
    return expanded


def _parse_xml(payload: bytes, *, source: str) -> ET.Element:
    decoded = _bounded_decompress(payload, source=source)
    try:
        return ET.fromstring(decoded)
    except DefusedXmlException as exc:
        raise ReferenceContextError(f"{source} contains forbidden XML features") from exc
    except ET.ParseError as exc:
        sample = decoded[:4096].decode("utf-8", errors="replace")
        status = re.search(r"Status\s*=\s*([A-Za-z]+)", sample)
        if status and status.group(1).upper() == "WAITING":
            raise ReferenceContextNotReady("NCBI RID is not ready; retry after 30 seconds") from exc
        raise ReferenceContextError(f"{source} is not valid BLAST XML") from exc


def _parse_xml1(payload: bytes) -> _Xml1Evidence:
    root = _parse_xml(payload, source="NCBI XML1")
    if _local_name(root.tag) != "BlastOutput":
        raise ReferenceContextError("NCBI XML1 root must be BlastOutput")
    statistics = _one_node(root, "Statistics", source="NCBI XML1")
    return _Xml1Evidence(
        program=_first_text(root, "BlastOutput_program"),
        database=_first_text(root, "BlastOutput_db"),
        query_id=(
            _first_text(root, "BlastOutput_query-ID") or _first_text(root, "BlastOutput_query-def")
        ),
        query_length=_positive_int(root, "BlastOutput_query-len", source="NCBI XML1"),
        database_sequences=_positive_int(statistics, "Statistics_db-num", source="NCBI XML1"),
        database_letters_raw=_positive_int(statistics, "Statistics_db-len", source="NCBI XML1"),
    )


def _xml2_documents(payload: bytes) -> tuple[bytes, ...]:
    decoded = _bounded_decompress(payload, source="NCBI XML2")
    if not decoded.startswith(b"PK\x03\x04"):
        return (decoded,)
    try:
        with ZipFile(io.BytesIO(decoded)) as archive:
            members = [
                member
                for member in archive.infolist()
                if not member.is_dir() and member.filename.lower().endswith(".xml")
            ]
            if not members or len(members) > _MAX_XML2_MEMBERS:
                raise ReferenceContextError("NCBI XML2 ZIP has an invalid XML member count")
            if sum(member.file_size for member in members) > _MAX_RESPONSE_BYTES:
                raise ReferenceContextError("NCBI XML2 ZIP expands beyond 128 MiB")
            documents = tuple(archive.read(member) for member in members)
    except BadZipFile as exc:
        raise ReferenceContextError("NCBI XML2 ZIP is invalid") from exc
    results = tuple(document for document in documents if b"<BlastOutput2" in document)
    if not results:
        raise ReferenceContextError("NCBI XML2 ZIP contains no BlastOutput2 result")
    return results


def _parse_xml2(payload: bytes) -> _Xml2Evidence:
    roots: list[ET.Element] = []
    for document in _xml2_documents(payload):
        try:
            roots.append(ET.fromstring(document))
        except DefusedXmlException as exc:
            raise ReferenceContextError("NCBI XML2 result contains forbidden XML features") from exc
        except ET.ParseError as exc:
            raise ReferenceContextError("NCBI XML2 result is not valid XML") from exc
    statistics = [
        node for root in roots for node in root.iter() if _local_name(node.tag) == "Statistics"
    ]
    if len(statistics) != 1:
        raise ReferenceContextError(
            f"NCBI XML2 must contain exactly one Statistics block; found {len(statistics)}"
        )
    node = statistics[0]
    return _Xml2Evidence(
        database_sequences=_positive_int(node, "db-num", source="NCBI XML2"),
        database_letters_raw=_positive_int(node, "db-len", source="NCBI XML2"),
        length_adjustment=_nonnegative_int(node, "hsp-len", source="NCBI XML2"),
        effective_search_space=_positive_int(node, "eff-space", source="NCBI XML2"),
    )


def _single_query_metadata(query_fasta: str) -> tuple[str, int]:
    records: list[tuple[str, int]] = []
    query_id = ""
    length = 0
    for raw_line in query_fasta.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith(">"):
            if query_id:
                records.append((query_id, length))
            query_id = line[1:].strip().split(None, 1)[0]
            length = 0
            if not query_id:
                raise ReferenceContextError("query FASTA header is missing an id")
            continue
        if not query_id:
            raise ReferenceContextError("query FASTA data appeared before its header")
        length += len("".join(line.split()))
    if query_id:
        records.append((query_id, length))
    if len(records) != 1 or records[0][1] <= 0:
        raise ReferenceContextError("reference context requires exactly one non-empty FASTA query")
    return records[0]


def _normalise_database(value: str) -> str:
    return value.rstrip("/").rsplit("/", 1)[-1].lower()


def _normalise_result_database_letters(
    raw_value: int,
    *,
    filtered_letters: int,
    active_total_letters: int,
) -> tuple[int, str]:
    candidates = {filtered_letters, active_total_letters}
    matches = sorted(
        candidate
        for candidate in candidates
        if candidate == raw_value or candidate % _UINT32_MODULUS == raw_value
    )
    if len(matches) != 1:
        raise ReferenceContextError(
            "NCBI result database length does not match the filtered or active database"
        )
    result = matches[0]
    encoding = "exact" if result == raw_value else "uint32_wrapped"
    return result, encoding


def derive_reference_context(
    *,
    rid: str,
    query_fasta: str,
    xml1_payload: bytes,
    xml2_payload: bytes,
    active_total_letters: int,
    active_total_sequences: int,
    active_source_version: str,
    taxid: int | None = None,
    is_inclusive: bool | None = None,
) -> dict[str, Any]:
    """Derive the exact six-field context from paired XML1/XML2 evidence."""
    canonical_rid = rid.strip().upper()
    if not _RID_RE.fullmatch(canonical_rid):
        raise ReferenceContextError("RID must contain 8-16 uppercase letters or digits")
    query_id, query_length = _single_query_metadata(query_fasta)
    xml1 = _parse_xml1(xml1_payload)
    xml2 = _parse_xml2(xml2_payload)
    if xml1.program.lower() != "blastn":
        raise ReferenceContextError("reference result must use blastn")
    if _normalise_database(xml1.database) != "core_nt":
        raise ReferenceContextError("reference result must use core_nt")
    if xml1.query_length != query_length:
        raise ReferenceContextError("reference query length does not match the submitted FASTA")
    if xml2.length_adjustment >= query_length:
        raise ReferenceContextError("reference length adjustment must be smaller than query length")
    if xml2.database_letters_raw != xml1.database_letters_raw:
        raise ReferenceContextError("XML1 and XML2 result database lengths differ")
    if xml2.database_sequences not in {
        xml1.database_sequences,
        active_total_sequences,
    }:
        raise ReferenceContextError("XML1 and XML2 database sequence counts are inconsistent")

    effective_query_length = query_length - xml2.length_adjustment
    effective_database_length, remainder = divmod(
        xml2.effective_search_space,
        effective_query_length,
    )
    if remainder:
        raise ReferenceContextError("reference effective search space is not integral")
    filtered_letters = effective_database_length + xml1.database_sequences * xml2.length_adjustment
    if filtered_letters > active_total_letters:
        raise ReferenceContextError("reference filtered letters exceed the active database")
    if xml1.database_sequences > active_total_sequences:
        raise ReferenceContextError("reference filtered sequences exceed the active database")
    scoring_search_space = effective_query_length * filtered_letters
    result_database_letters, length_encoding = _normalise_result_database_letters(
        xml1.database_letters_raw,
        filtered_letters=filtered_letters,
        active_total_letters=active_total_letters,
    )
    context = {
        "filtered_database_letters": filtered_letters,
        "filtered_database_sequences": xml1.database_sequences,
        "length_adjustment": xml2.length_adjustment,
        "effective_search_space": xml2.effective_search_space,
        "scoring_search_space": scoring_search_space,
        "result_database_letters": result_database_letters,
    }
    expected_effective = effective_query_length * (
        filtered_letters - xml1.database_sequences * xml2.length_adjustment
    )
    if expected_effective != xml2.effective_search_space:
        raise ReferenceContextError("derived reference statistics failed validation")
    return {
        "status": "resolved",
        "rid": canonical_rid,
        "database": "core_nt",
        "reference_query_id": xml1.query_id,
        "submitted_query_id": query_id,
        "query_length": query_length,
        "active_source_version": active_source_version,
        "web_blast_statistical_context": context,
        "query_effective_search_spaces": [xml2.effective_search_space],
        "expected_filter": {
            "taxid": taxid,
            "is_inclusive": is_inclusive,
            "verified_from_result": False,
        },
        "evidence": {
            "xml1_sha256": hashlib.sha256(xml1_payload).hexdigest(),
            "xml2_sha256": hashlib.sha256(xml2_payload).hexdigest(),
            "result_database_length_encoding": length_encoding,
            "query_content_verified": False,
            "submission_options_verified": False,
            "active_source_version_verified": False,
        },
        "warnings": [
            "The RID result formats do not prove the original query content, taxonomy "
            "expression, all submission options, or source-version identity; verify those "
            "inputs match before submitting this context."
        ],
    }


def _response_bytes(response: requests.Response, *, label: str) -> bytes:
    if response.status_code == 429:
        raise ReferenceContextUnavailable("NCBI BLAST is rate limiting requests")
    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        raise ReferenceContextUnavailable(f"NCBI {label} request failed") from exc
    declared = response.headers.get("Content-Length", "")
    if declared.isdigit() and int(declared) > _MAX_RESPONSE_BYTES:
        raise ReferenceContextError(f"NCBI {label} response exceeds 128 MiB")
    payload = bytearray()
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        payload.extend(chunk)
        if len(payload) > _MAX_RESPONSE_BYTES:
            raise ReferenceContextError(f"NCBI {label} response exceeds 128 MiB")
    return bytes(payload)


def _request_interval_seconds() -> float:
    raw = os.environ.get("NCBI_BLAST_REQUEST_INTERVAL_SECONDS", "10").strip()
    try:
        return max(0.0, min(float(raw), 60.0))
    except ValueError:
        return 10.0


def _fetch_payload(rid: str, format_type: str) -> bytes:
    global _LAST_REQUEST_AT
    interval = _request_interval_seconds()
    remaining = interval - (time.monotonic() - _LAST_REQUEST_AT)
    if remaining > 0:
        time.sleep(remaining)
    params = {
        "CMD": "Get",
        "RID": rid,
        "FORMAT_TYPE": format_type,
        "TOOL": os.environ.get("NCBI_TOOL", "elb-dashboard"),
    }
    email = os.environ.get("NCBI_EMAIL", "").strip()
    if email:
        params["EMAIL"] = email
    api_key = os.environ.get("NCBI_API_KEY", "").strip()
    if api_key:
        params["API_KEY"] = api_key
    try:
        with requests.get(
            NCBI_BLAST_URL,
            params=params,
            headers={"User-Agent": "elb-dashboard/1.0"},
            timeout=(5, 45),
            allow_redirects=False,
            stream=True,
        ) as response:
            payload = _response_bytes(response, label=format_type)
    except ReferenceContextError:
        raise
    except requests.RequestException as exc:
        raise ReferenceContextUnavailable("NCBI BLAST result service is unavailable") from exc
    finally:
        _LAST_REQUEST_AT = time.monotonic()
    return payload


def clear_reference_context_cache() -> None:
    """Clear the bounded in-process resolver cache (tests and operator refresh)."""
    with _CACHE_LOCK:
        _CACHE.clear()


def resolve_reference_context(
    *,
    rid: str,
    query_fasta: str,
    active_total_letters: int,
    active_total_sequences: int,
    active_source_version: str,
    taxid: int | None = None,
    is_inclusive: bool | None = None,
    fetch_payload: Callable[[str, str], bytes] | None = None,
) -> dict[str, Any]:
    """Fetch one completed RID and return a validated, cacheable context."""
    canonical_rid = rid.strip().upper()
    cache_key = (
        canonical_rid,
        hashlib.sha256(query_fasta.encode("utf-8")).hexdigest(),
        active_source_version,
        active_total_letters,
        active_total_sequences,
        taxid,
        is_inclusive,
    )
    now = time.monotonic()
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if cached and cached[0] > now:
            return deepcopy(cached[1])
    fetch = fetch_payload or _fetch_payload
    if not _FETCH_LOCK.acquire(timeout=_FETCH_LOCK_WAIT_SECONDS):
        raise ReferenceContextUnavailable(
            "Reference context resolver is busy; retry shortly"
        )
    try:
        with _CACHE_LOCK:
            cached = _CACHE.get(cache_key)
            if cached and cached[0] > time.monotonic():
                return deepcopy(cached[1])
        xml1_payload = fetch(canonical_rid, "XML")
        xml2_payload = fetch(canonical_rid, "XML2")
        resolved = derive_reference_context(
            rid=canonical_rid,
            query_fasta=query_fasta,
            xml1_payload=xml1_payload,
            xml2_payload=xml2_payload,
            active_total_letters=active_total_letters,
            active_total_sequences=active_total_sequences,
            active_source_version=active_source_version,
            taxid=taxid,
            is_inclusive=is_inclusive,
        )
        with _CACHE_LOCK:
            if len(_CACHE) >= _CACHE_MAX_ENTRIES:
                oldest = min(_CACHE, key=lambda key: _CACHE[key][0])
                _CACHE.pop(oldest, None)
            _CACHE[cache_key] = (
                time.monotonic() + _CACHE_TTL_SECONDS,
                deepcopy(resolved),
            )
        return deepcopy(resolved)
    finally:
        _FETCH_LOCK.release()
