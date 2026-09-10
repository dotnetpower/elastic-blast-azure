# OpenAPI Runtime Contracts

This document records the dashboard-compatible OpenAPI behavior incorporated into this repository on 2026-09-10. The source natively includes the behavior validated in the deployed `elb-openapi:4.55` dashboard image plus the opt-in sequence-diversity source contract targeted for `elb-openapi:4.56`. Image `4.56` has not been built or deployed. The OpenAPI application version remains `3.7.6`.

The two version identifiers have different scopes:

- `3.7.6` is the sibling OpenAPI application's API version.
- `4.55` is the immutable image currently deployed by `dotnetpower/elb-dashboard`.
- `4.56` is the future source target for this contract; it is not runtime evidence.

## Result readiness

A partitioned job remains `running` with phase `finalizing` until the canonical `merged_results.out.gz` artifact is discoverable. A success marker or completed shard Jobs alone are not sufficient.

A ready partitioned result exposes:

- `status=completed`
- `results_ready=true`
- `results_ready_at`
- `merged_at`

The merge finalizer has a 30-minute active deadline. A partitioned run that does not publish its canonical merge within that bound becomes `failed` with phase `finalizer_failed`; it does not remain in an unbounded wait and does not fail open to `completed`.

## Result selection

`blast_options.result_selection_policy` accepts three values:

| Value | Contract |
| --- | --- |
| `native_top_n` | Default. Uses the BLAST score, raw-score, and database-order comparator. It does not promise representation from lower score classes when one tied class fills the result window. |
| `diversity_aware` | Reserves a proportional share for distinct lower-score subjects. This is heuristic and does not claim native full-database top-N membership. |
| `sequence_diversity` | Opt-in for partitioned tabular output only. Groups observed HSP rows by aligned subject sequence and query span, selects one representative row per group with the existing BLAST comparator, then returns at most `max_target_seqs` groups per query. |

The selected policy and effective partition count are retained in job state and returned in public status payloads.

Omitting the policy remains identical to explicit `native_top_n`. The
`diversity_aware` accession-based algorithm is unchanged. No unsupported
sequence-diversity request falls back to either existing policy.

### Sequence identity

Sequence-diversity identity is versioned as:

```text
sequence_identity_mode = aligned_sequence_query_span
sequence_identity_version = 1
signature = (query identity, uppercase(sseq with ASCII '-' removed), qstart, qend)
```

Whitespace and other characters are compared literally. Ambiguity symbols are
compared literally after uppercasing. The implementation does not reverse
complement sequences and does not reorder `qstart` and `qend`. Subject accession
is not part of the signature, so one group may contain several accessions and
one accession may occur in several groups.

The representative comparator is the existing merger comparator: smaller
e-value, then higher raw score when available (otherwise higher bit score), then
existing database/source order or stable input order, with original ordinal as
the final fallback. Only the representative HSP row is written to the canonical
merged output for this policy; the existing policies continue to emit every HSP
row for each selected accession.

### Candidate pool

`blast_options.candidate_pool_size` is a positive integer used only by
`sequence_diversity`. It is the per-shard BLAST subject cap, separate from the
final per-query group count in `max_target_seqs`. The server default is `2000`
when omitted. There is no fixed server maximum: an explicit value remains a
finite per-request bound and must be greater than or equal to
`max_target_seqs`. Non-positive values, non-integers, and use with another
policy are rejected instead of clamped. Larger pools proportionally increase
BLAST output, storage, and merge work.

The merger keeps row bodies in the input spool and ranking/signature metadata
in a file-backed SQLite database. It does not collect the candidate pool or
aligned sequences in an unbounded Python list. The per-group detail list in the
merge report is capped at 5,000 entries and reports truncation explicitly; this
observability bound does not truncate the canonical result or aggregate counts.

Local canonical output and report files are completed under a persistent
advisory lock using same-directory temporary files. A duplicate finalizer waits
for at most 30 minutes and reuses a fresh completed pair from the lock owner;
otherwise it performs the merge itself. The shell forwards termination signals
to the Python merge process, which closes and removes registered SQLite and
temporary artifacts before releasing the lock. Gzip headers retain the
canonical filename and atomic replacement preserves the target mode.

The outer finalizer validates the completed gzip stream, validates the XML root
when applicable, uploads the named output and report, and writes the durable
success marker last. A local report alone is never completion evidence.

### Merge report

The additive sequence-diversity report fields are:

- `result_selection_policy_requested` and `result_selection_policy_applied`
- `sequence_identity_mode` and `sequence_identity_version`
- `requested_sequence_groups` and `returned_sequence_groups`
- `candidate_pool_size_requested_per_shard` and
	`candidate_pool_size_applied_per_shard`
- `observed_candidate_rows`, `observed_candidate_subjects`, and
	`observed_sequence_groups`
- `expected_shards`, `succeeded_shards`, and
	`candidate_pool_saturated_shards`
- `observed_pool_complete` and `shortfall_reasons`

`observed_pool_complete=true` means every expected shard provenance marker was
observed and no shard/query candidate set reached the configured cap. It does
not prove that the database was exhausted or that no unobserved candidate
exists. Supported shortfall reasons are
`candidate_pool_saturated`, `insufficient_unique_groups_in_observed_pool`, and
`no_candidates_observed`; the report never claims `database_exhausted`.

Per-group metadata includes `sequence_group_accession_count` and
`sequence_group_source_row_count`. The current runtime has no stable deduplicated
exact-HSP identity beyond source-row ordinal, so it deliberately omits
`sequence_group_hsp_count` rather than inventing one. Group accession counts are
not globally additive because the same accession can belong to several groups.

The finalizer runs only after every expected shard has produced a readable
result. Missing or unreadable shards create a terminal failure marker and no
canonical merged result; partial candidates are never presented as complete.

## Tabular output fields

NCBI ElasticBLAST does not model arbitrary multi-token `outfmt` columns as separate structured parameters. This implementation respects that design and uses the supported raw options contract. The compatibility layer preserves caller fields and appends missing fields needed by merge ranking and result analysis.

For `outfmt 7`, the authoritative `# Fields:` header is preserved. Consumers should map columns by that header rather than by a fixed count. Missing compatibility fields are appended in this stable order:

1. `staxids`
2. `sscinames`
3. `stitle`
4. `qcovs`
5. `score`

Fields already present in the caller's layout are not duplicated or reordered.

For `sequence_diversity`, the caller must supply an effective tabular outfmt 6
or 7 containing query identity (`qseqid`, `qacc`, `qaccver`, or `qgi`), subject
accession (`sseqid`, `sacc`, `saccver`, or `sgi`), `sseq`, `qstart`, `qend`,
`evalue`, and `bitscore`. The server appends `score` before validation. It does
not silently add the other semantic fields.

Invalid combinations return HTTP 422 with a bounded detail object. Stable codes
include `sequence_diversity_invalid_outfmt`,
`sequence_diversity_missing_fields`,
`sequence_diversity_invalid_candidate_pool`,
`sequence_diversity_requires_structured_options`,
`sequence_diversity_requires_sharded_merge`, and
`sequence_diversity_incompatible_context`. Reusing an idempotency key already
bound to different policy, final-group, or applied-pool semantics returns HTTP
409 `sequence_diversity_idempotency_conflict`. These errors set
`retryable=false`; `missing_fields` is present when applicable. Request or
sequence content is not included in the error.

## Result downloads

The existing `GET /v1/jobs/{job_id}/results` modes are unchanged:

- `content=full` returns a ZIP containing every shard `*.out.gz` / `*.out`.
- `content=merged` returns a ZIP containing only `merged_results.out.gz`.
- `content=xml` gunzips `merged_results.out.gz` and serves `application/xml`.

Sequence diversity does not add a raw download mode and cannot be combined with
outfmt 5/XML. One job has one policy-applied canonical merged result. Comparing
native and sequence-diversity output requires two jobs submitted with different
policies.

## Downstream interpretation

Sequence groups are selected before downstream accession collapse, accession
union-coverage calculation, and identity/coverage filtering. Consequently,
`max_target_seqs=N` does not guarantee N surviving accessions after those
downstream operations. Group counts cannot reconstruct accession-level union
coverage.

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
- disk-backed sequence-signature grouping with an explicit per-request shard cap
- a 1,024-entry ceiling for shard-layout volumes and merge shard counts

The Dockerfile asserts that these contracts are present in source, system-Python, and Azure CLI virtual-environment copies before the image can complete its build.

## Validation

Run the standalone OpenAPI tests from the repository root:

```bash
ELB_OPENAPI_ALLOW_UNAUTHENTICATED=1 \
PYTHONPATH=docker-openapi/app \
python -m pytest -q docker-openapi/tests
```

The sequence-diversity source contract passed `188` tests in an isolated
environment created from `docker-openapi/app/requirements.txt` and
`docker-openapi/requirements-dev.txt`. The merge helper also passed shell syntax
validation, changed Python files passed Ruff, and the runtime modules compiled.
This is local source evidence only; no `4.56` image was built or deployed.

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
