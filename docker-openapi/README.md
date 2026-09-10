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

kubectl create deployment elb-openapi --image=elbacr.azurecr.io/elb-openapi:0.2
kubectl expose deployment elb-openapi --type=LoadBalancer --port=80 --target-port=8000