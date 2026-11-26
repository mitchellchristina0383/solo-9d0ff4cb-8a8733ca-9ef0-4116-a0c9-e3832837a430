"""Unit tests for the tail buffer and procfs group helpers."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
deps = ROOT / ".pydeps"
if deps.is_dir():
    sys.path.insert(0, str(deps))

from app.buffers import TailBuffer  # noqa: E402
from app.procfs import group_alive, group_pids  # noqa: E402


def test_tailbuffer_keeps_latest_bytes_within_limit():
    buf = TailBuffer(max_bytes=100)
    buf.append(b"a" * 60)
    buf.append(b"b" * 60)
    data, total = buf.snapshot()
    assert total == 120
    assert len(data) == 100
    # The retained content is the tail: newest bytes win.
    assert data.endswith(b"b" * 60)
    assert data.startswith(b"a" * 40)


def test_tailbuffer_small_input_not_truncated():
    buf = TailBuffer(max_bytes=100)
    buf.append(b"hello ")
    buf.append(b"world")
    data, total = buf.snapshot()
    assert data == b"hello world"
    assert total == 11
    assert len(data) <= 100


def test_tailbuffer_single_oversized_chunk():
    buf = TailBuffer(max_bytes=50)
    buf.append(b"x" * 200)
    data, total = buf.snapshot()
    assert len(data) == 50
    assert total == 200
    assert data == b"x" * 50


def test_tailbuffer_many_tiny_chunks_preserve_order():
    buf = TailBuffer(max_bytes=10)
    for i in range(20):
        buf.append(bytes([65 + (i % 26)]))
    data, total = buf.snapshot()
    assert total == 20
    assert len(data) == 10
    # Last ten appended bytes, in order.
    assert data == bytes(65 + (i % 26) for i in range(10, 20))


def test_group_pids_detects_own_new_session():
    """A child in its own session/group is discoverable via /proc."""
    import subprocess
    import time

    p = subprocess.Popen(
        [sys.executable, "-c", "import time;time.sleep(2)"],
        start_new_session=True,
    )
    try:
        # Wait until it is visible.
        deadline = time.monotonic() + 3.0
        pids = []
        while time.monotonic() < deadline:
            pids = group_pids(p.pid)
            if pids:
                break
            time.sleep(0.02)
        assert p.pid in pids
        assert group_alive(p.pid) is True
    finally:
        p.terminate()
        p.wait(timeout=5)

    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline and group_alive(p.pid):
        time.sleep(0.02)
    assert group_alive(p.pid) is False
    assert group_pids(p.pid) == []


def test_group_pids_unknown_group_is_empty():
    # A PGID that almost certainly does not exist.
    fake_pgid = 2_000_000
    assert group_pids(fake_pgid) == []
    assert group_alive(fake_pgid) is False
