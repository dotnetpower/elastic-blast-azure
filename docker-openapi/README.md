# ElasticBLAST docker image

This directory contains the tools needed to build the docker image used to
run BLAST in ElasticBLAST.

The `Makefile` contains targets to build, test and deploy the docker image in
various repositories.

If you have `docker` available, run `make azure-build` to build the image, and `make
check` to test it locally.

## Runtime contracts

This source includes the result-readiness, result-selection, active-generation,
and reference-context behavior validated by the ElasticBLAST dashboard runtime.
See [OpenAPI Runtime Contracts](../docs/openapi-runtime-contracts.md) for the
public API contract, compatibility rules, security bounds, and validation
evidence.

The authenticated reference-context endpoint is:

```text
POST /v1/web-blast/statistical-context
```

Partitioned tabular requests can opt into aligned-sequence grouping:

```json
{
	"blast_options": {
		"outfmt": "7 qseqid saccver sseq qstart qend evalue bitscore",
		"max_target_seqs": 100,
		"candidate_pool_size": 2000,
		"result_selection_policy": "sequence_diversity"
	},
	"resource_profile": "core_nt_safe"
}
```

The server appends raw `score`, rejects missing semantic fields or XML with
HTTP 422, and writes one representative HSP per observed sequence signature to
the canonical merged result. Existing `native_top_n`, `diversity_aware`, and
`full` / `merged` / `xml` download behavior is unchanged. See the runtime
contract for signature, count, candidate-pool, and downstream filtering
semantics. The candidate pool defaults to `2000`; larger positive finite values
are accepted without a fixed server maximum.

kubectl create deployment elb-openapi --image=elbacr.azurecr.io/elb-openapi:0.2
kubectl expose deployment elb-openapi --type=LoadBalancer --port=80 --target-port=8000