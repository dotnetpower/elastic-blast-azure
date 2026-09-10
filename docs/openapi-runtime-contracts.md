# OpenAPI Runtime Contracts

This document records the dashboard-compatible OpenAPI behavior incorporated into this repository on 2026-09-10. The source natively includes the behavior validated in the `elb-openapi:4.55` dashboard image, plus source-level validation and input-bound hardening found during publication review. It retains the OpenAPI application version `3.7.6`.

The two version identifiers have different scopes:

- `3.7.6` is the sibling OpenAPI application's API version.
- `4.55` is the immutable image lineage used by `dotnetpower/elb-dashboard`.

## Result readiness

A partitioned job remains `running` with phase `finalizing` until the canonical `merged_results.out.gz` artifact is discoverable. A success marker or completed shard Jobs alone are not sufficient.

A ready partitioned result exposes:

- `status=completed`
- `results_ready=true`
- `results_ready_at`
- `merged_at`

The merge finalizer has a 30-minute active deadline. A partitioned run that does not publish its canonical merge within that bound becomes `failed` with phase `finalizer_failed`; it does not remain in an unbounded wait and does not fail open to `completed`.

## Result selection

`blast_options.result_selection_policy` accepts two values:

| Value | Contract |
| --- | --- |
| `native_top_n` | Default. Uses the BLAST score, raw-score, and database-order comparator. It does not promise representation from lower score classes when one tied class fills the result window. |
| `diversity_aware` | Reserves a proportional share for distinct lower-score subjects. This is heuristic and does not claim native full-database top-N membership. |

The selected policy and effective partition count are retained in job state and returned in public status payloads.

## Tabular output fields

NCBI ElasticBLAST does not model arbitrary multi-token `outfmt` columns as separate structured parameters. This implementation respects that design and uses the supported raw options contract. The compatibility layer preserves caller fields and appends missing fields needed by merge ranking and result analysis.

For `outfmt 7`, the authoritative `# Fields:` header is preserved. Consumers should map columns by that header rather than by a fixed count. Missing compatibility fields are appended in this stable order:

1. `staxids`
2. `sscinames`
3. `stitle`
4. `qcovs`
5. `score`

Fields already present in the caller's layout are not duplicated or reordered.

## Active database and search space

Precise `core_nt` execution reads one active-generation metadata record and validates all immutable database and shard paths against that generation. Normal callers omit:

- `blast_options.db_effective_search_space`
- raw `-searchsp`
- raw `-dbsize`

The service resolves the active-generation value. An explicit typed value is accepted only through the structured field and conflicts with raw `-searchsp` or `-dbsize` instead of silently choosing one.

Exact native selection requires a ready, same-generation DB-order oracle. Oracle metadata and runtime statistical context are written without exposing Storage credentials or SAS URLs.

## Reference statistical context

Authenticated callers can use:

```text
POST /v1/web-blast/statistical-context
```

The request supplies a completed NCBI RID and the exact single-record FASTA associated with that result. The resolver fetches XML1 and XML2 only from the fixed NCBI result endpoint and derives:

- filtered database letters
- filtered database sequences
- length adjustment
- reported effective search space
- scoring search space
- result database letters

A length adjustment of zero is valid for short-query contexts. All other
statistical fields remain positive integers, and the formulas are validated
before any runtime option is accepted.

The endpoint does not submit a search and does not poll a pending RID. It returns:

- `409 reference_not_ready` with `Retry-After: 30` for a pending result
- `422 reference_invalid` for inconsistent evidence
- retryable `503` errors for external-result, active-metadata, or bounded-concurrency unavailability

Result formats cannot prove the original query content, taxonomy expression, every submit option, or source-version identity. The response reports those provenance limitations explicitly; callers must verify matching inputs before reusing the context.

## Security and resource bounds

The reference endpoint inherits the `/v1` router's `X-ELB-API-Token` dependency. External XML handling uses:

- a fixed destination URL and disabled redirects
- connect and read timeouts
- a 128 MiB compressed and expanded response limit
- at most 16 XML2 ZIP members
- `defusedxml==0.7.1`
- process-wide request pacing
- a 128-entry, six-hour cache with isolated copies
- a two-second fetch-gate acquisition bound

Storage OAuth credentials and optional external API credentials never appear in response payloads or exception messages.

## Runtime hardening

The source also includes the runtime guards used by the validated dashboard image:

- canonical result-path validation
- content-verified ElasticBLAST script ConfigMap reconciliation
- bounded script payload size and post-apply verification
- canonical runtime job-id discovery and propagation
- node-local database validation before warmed-cache reuse
- reader/writer locking for shared database paths
- finalizer deadlines and terminal failure projection
- disk-backed shard-result merge and bounded oracle processing
- a 1,024-entry ceiling for shard-layout volumes and merge shard counts

The Dockerfile asserts that these contracts are present in source, system-Python, and Azure CLI virtual-environment copies before the image can complete its build.

## Validation

Run the standalone OpenAPI tests from the repository root:

```bash
ELB_OPENAPI_ALLOW_UNAUTHENTICATED=1 \
PYTHONPATH=docker-openapi/app \
python -m pytest -q docker-openapi/tests
```

The added contract suites cover:

- `docker-openapi/tests/test_reference_context.py`: reference-context derivation,
	XML hardening, cache isolation, and bounded contention
- `docker-openapi/tests/test_reference_context_endpoint.py`: endpoint
	authentication, typed response filtering, sanitized failures, canonical-merge
	readiness, the 30-minute failure boundary, and selection-policy conflicts
- `docker-openapi/tests/test_exact_oracle.py`: active-generation metadata,
	bounded layout validation, and exact-oracle option validation
- the existing `docker-openapi/tests/` suite: pagination, passthrough fields,
	result prefixes, readiness probes, watchdog recovery, and external payloads

Publication validation completed with `149 passed` across the complete
`docker-openapi/tests` suite. The Python runtime files compiled under Python
3.11, the merge helper passed `bash -n`, the new modules passed Ruff, and a
second dashboard-patcher application produced no source diff.

The earlier dashboard deployment validation for image `4.55` recorded:

- ACR build run `de9t`
- image digest `sha256:6876e5370f3edc3816451e71bdeee100bf69c5da47ec93332fb359acb0342b5c`
- AKS deployment generation 55 at 1/1 Ready and Available with zero restarts
- live `200` health/schema, `401` missing-token, and `422` request-validation checks
- zero request 5xx responses, exceptions, high-severity traces, or failed dependencies during rollout validation

The additional zero-adjustment and input-bound source hardening described above
was validated locally before publication. It does not imply that the already
running immutable `4.55` digest was rebuilt or redeployed.

See the dashboard-side records for the deployment evidence and client guidance:

- [OpenAPI reference-context resolver](https://github.com/dotnetpower/elb-dashboard/blob/main/docs/features_change/2026-09/2026-09-09-openapi-reference-context-resolver.md)
- [OpenAPI result-readiness and selection contracts](https://github.com/dotnetpower/elb-dashboard/blob/main/docs/features_change/2026-09/2026-09-09-openapi-result-contract-hardening.md)
- [API Reference user guide](https://github.com/dotnetpower/elb-dashboard/blob/main/docs/user-guide/api-reference.md)
