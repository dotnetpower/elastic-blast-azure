# Cross-path submit coordination (Kubernetes Lease)

## Why

Two independent code paths can run `elastic-blast submit` against the **same**
AKS cluster and `default` namespace:

1. The **dashboard control plane** (`elb-dashboard`) submit path.
2. This **OpenAPI service** (`docker-openapi`) submit path.

Each path historically enforced its own concurrency limit that was invisible to
the other — the dashboard used a Redis lock, this service used the in-process
`ELB_OPENAPI_MAX_ACTIVE_SUBMISSIONS` gate. Because neither limit was visible to
the other path, both could run `elastic-blast submit` simultaneously and race on
the shared service account / secret / PVC / Job objects and clobber the shared
`elb-scripts` ConfigMap.

The fix is a **shared, cluster-visible coordinator** built on a Kubernetes
[Lease](https://kubernetes.io/docs/concepts/architecture/leases/) object plus a
live count of active BLAST submissions. Both repositories implement the **same**
two-gate contract so they honour one global ceiling.

## Two gates

* **Gate A — Lease mutex.** A single `coordination.k8s.io/v1` Lease named
  `elb-blast-submit` in the `default` namespace serialises the submit critical
  section. Acquisition is a compare-and-swap on `resourceVersion`; a stale Lease
  (no renew within `ttl + skew`) is reclaimed. The Lease is released as soon as
  the `elastic-blast submit` call returns — the job's own finalizer Job then
  represents it for Gate B.
* **Gate B — run-concurrency ceiling.** While holding Gate A, the coordinator
  counts active BLAST submissions on the cluster (Jobs labelled
  `app in (finalizer,blast,submit)`, excluding terminal Jobs and phantom
  finalizers past the grace window). If the count is already at the ceiling
  (`BLAST_MAX_RUN_CONCURRENCY`, default 3) the holder releases Gate A and waits,
  then retries — so the global number of concurrent submits never exceeds the
  ceiling regardless of which path issued them.

## Behaviour

* **Disabled by default.** Coordination is a no-op unless
  `BLAST_COORD_BACKEND=k8s`. With the backend unset or `redis`,
  `acquire_run_slot()` returns `None` and the submit path is unchanged.
* **Bounded waits.** Each acquire attempt waits at most
  `BLAST_CAPACITY_WAIT_MAX_SECONDS` (default 1800s) for a slot; on expiry the job
  is **requeued** (`status=queued`, `phase=waiting_for_capacity`) rather than
  failed, and the dispatcher retries it on a later tick. The existing watchdog
  `ELB_OPENAPI_SUBMIT_STUCK_SECONDS` (default 7200s) bounds the total queued
  lifetime, so a cluster that never frees capacity eventually fails the job
  instead of looping forever.
* **Fail-closed.** A Kubernetes API error while reading the Lease or counting
  Jobs raises `SubmitCoordinationError`, which fails the job rather than silently
  admitting an uncoordinated submit.

## Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `BLAST_COORD_BACKEND` | unset (`redis`) | `k8s` enables coordination; anything else is a no-op. |
| `BLAST_MAX_RUN_CONCURRENCY` | `3` | Gate B ceiling — max concurrent submits across both paths. |
| `BLAST_SUBMIT_LEASE_TTL_SECONDS` | `900` | Lease staleness threshold before reclaim. |
| `BLAST_LEASE_CLOCK_SKEW_SECONDS` | `30` | Clock-skew allowance added to the TTL. |
| `BLAST_FINALIZER_GRACE_SECONDS` | `300` | Age before a lone finalizer Job is treated as phantom. |
| `BLAST_CAPACITY_WAIT_MAX_SECONDS` | `1800` | Max wait per acquire attempt before requeue. |
| `BLAST_OPENAPI_SUBMIT_EXEC_TIMEOUT_SECONDS` | `lease_ttl − 120` | Hard cap on the coordinated `submit` subprocess. MUST stay `< lease_ttl` (fail-closed at acquire). OpenAPI-only. |

The first six names and defaults are identical to the dashboard control plane so
the two paths agree on the ceiling and the staleness windows. The last one is
OpenAPI-only — the dashboard pins its submit subprocess at a fixed 600s, while
this service derives the cap from the TTL so long-submit operators raise a single
number (`BLAST_SUBMIT_LEASE_TTL_SECONDS`) and the exec cap follows.

## Required RBAC (LOAD-BEARING)

When `BLAST_COORD_BACKEND=k8s`, the OpenAPI service account needs the following
permissions in the `default` namespace. **Coordination fails closed (jobs fail)
if these are missing**, so grant them before flipping the backend on:

* `coordination.k8s.io` Leases — `get`, `create`, `update`.
* `batch` Jobs — `list` (label-selector reads for Gate B).

Example Role + RoleBinding (apply to the namespace the service account runs in
and to `default` if different):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: elb-blast-submit-coordinator
  namespace: default
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["list"]
```

## Rollout order (LOAD-BEARING)

Deploy this service's Phase 1 coordinator **first**. It is additive and safe
while the dashboard stays on its Redis backend (this path simply enforces the
shared ceiling against the live Job count). **Only after** this is live should
the dashboard be flipped to `BLAST_COORD_BACKEND=k8s`. Flipping the dashboard
first opens a transient window where neither path sees the other's lock.

## Long submits and the Lease TTL — the `submit_exec_timeout < lease_ttl` invariant

Gate A is held for the duration of the `elastic-blast submit` call and released
immediately after, with **no renewal heartbeat** — this matches the dashboard
contract exactly (`api/services/blast/k8s_gate.py` releases in its `finally`).
Without a bound, a single `elastic-blast submit` running longer than
`BLAST_SUBMIT_LEASE_TTL_SECONDS + BLAST_LEASE_CLOCK_SKEW_SECONDS` (default
900 + 30 = 930s) would let the Lease be reclaimed by another path while the
first submit is still creating objects — defeating Gate A's mutual exclusion.

The dashboard closes this with a **load-bearing ordering invariant**
(`docs/research/blast-submit-coordination.md` §4.3): its submit subprocess is
hard-capped (600s) strictly below the Lease TTL (900s), so an overrunning submit
is killed (failed) *before* the Lease can be reclaimed. This service enforces
the same invariant:

* When coordination holds Gate A, the `submit` subprocess is run with a finite
  `timeout` from `submit_exec_timeout_seconds()` (default `lease_ttl − 120`,
  always `< lease_ttl`). An overrun raises and fails the job before the
  `renewTime + ttl + skew` reclaim point — never a silent concurrent submit.
* `acquire_run_slot()` **fails closed** if a misconfigured
  `BLAST_OPENAPI_SUBMIT_EXEC_TIMEOUT_SECONDS` is `>= lease_ttl`, refusing to
  admit rather than running unprotected.
* The coordination-disabled path (`BLAST_COORD_BACKEND != k8s`) keeps the legacy
  unbounded `timeout=None`, so non-coordinated deployments are unaffected.

For genuinely long submits, raise `BLAST_SUBMIT_LEASE_TTL_SECONDS`: the exec cap
follows it up automatically and the invariant holds by construction. A renewal
heartbeat (an alternative to capping) remains out of scope for v1 and, if added,
must land in both repos together to keep the contract aligned.

