import os
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
# Local dependency directory (pip --target install) when used.
deps = ROOT / ".pydeps"
if deps.is_dir():
    sys.path.insert(0, str(deps))

from app.main import create_app  # noqa: E402


def _wait_state(client, build_id, expected, timeout=30.0):
    """Poll until build reaches one of the expected states."""
    if isinstance(expected, str):
        expected = {expected}
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        resp = client.get(f"/builds/{build_id}")
        assert resp.status_code == 200
        last = resp.json()
        if last["state"] in expected:
            return last
        time.sleep(0.1)
    raise AssertionError(
        f"build {build_id} did not reach {expected}; last state={last}"
    )


@pytest.fixture(scope="session")
def client():
    app = create_app()
    with TestClient(app) as c:
        yield c


@pytest.fixture
def build_id():
    return f"t-{os.urandom(6).hex()}"
