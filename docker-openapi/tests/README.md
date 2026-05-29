# docker-openapi tests

Unit tests for the `elb-openapi` FastAPI service. The tests do **not**
touch a live Kubernetes API — every call to `kubectl` (`util.safe_exec`) is
replaced with canned stdout via `monkeypatch`.

## Setup

```bash
cd docker-openapi
python -m venv .venv && source .venv/bin/activate
pip install -r app/requirements.txt -r requirements-dev.txt
```

## Run

```bash
python -m pytest tests/ -q
```

The `tests/conftest.py` adds `docker-openapi/app/` to `sys.path` so
`from main import app` resolves without packaging the service. Each test
that mutates `safe_exec` does so via `monkeypatch`, so state cannot leak
between tests.

## Coverage so far

* `/v1/ready` — happy path, every documented failure code, autoscaler-aware
  skip, label-disabled skip, cluster-name masking, and per-token
  sliding-window rate limit.
* `/v1/ready/metrics` — counter snapshot is exposed and increments after a
  probe completes.

When adding routes that hit `kubectl` or `azure.identity`, follow the same
pattern: pin the side-effecting helper with `monkeypatch.setattr` and assert
on `resp.status_code` + `resp.json()`.
