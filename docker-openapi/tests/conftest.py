"""Pytest configuration for the docker-openapi service.

Adds ``docker-openapi/app`` to ``sys.path`` so ``from app import main`` works
without requiring the service to be installed as a package. Also sets the
minimum env vars ``main`` needs at import time (token + allow-unauth so the
TestClient can issue requests without hitting the real K8s API).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Make `app/` importable as the top-level `main` module.
_HERE = Path(__file__).resolve().parent
_APP_DIR = _HERE.parent / "app"
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

# Default env so importing main.py does not trip on missing config. Each
# test that needs a different value uses monkeypatch.setenv.
os.environ.setdefault("ELB_OPENAPI_API_TOKEN", "test-token")
os.environ.setdefault("ELB_CLUSTER_NAME", "test-cluster")
os.environ.setdefault("ELB_OPENAPI_DISABLE_BACKGROUND", "1")
