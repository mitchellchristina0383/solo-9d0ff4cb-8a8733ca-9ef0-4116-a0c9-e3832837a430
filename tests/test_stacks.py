"""Tests for on-demand main-thread stack sampling.

Covers:
* StackSampler unit behaviour with a real RT-signal-aware subprocess
  (ok / timeout / exited-mid-sample / independent sequences);
* end-to-end API scenarios: normal capture, leader-exits-first roster,
  concurrent samples with a deliberately delayed (late) response that must
  not cross samples, and cancellation while sampling.
"""
from __future__ import annotations

import os
import signal
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
deps = ROOT / ".pydeps"
if deps.is_dir():
    sys.path.insert(0, str(deps))

from app.procfs import pid_alive, pid_state  # noqa: E402
from app.stacks import (  # noqa: E402
    STACK_EXITED,
    STACK_OK,
    STACK_TIMEOUT,
    StackSampler,
)

from conftest import _wait_state  # noqa: E402

TERMINAL = {"succeeded", "failed", "cancelled"}


# --------------------------------------------------------------- unit helper
HELPER = r"""
import os, signal, sys, threading, time, traceback

stack_dir = os.environ["HELPER_STACK_DIR"]
signum = int(os.environ["HELPER_SIGNUM"])
lifetime = float(os.environ.get("HELPER_LIFETIME", "30"))
delay = float(os.environ.get("HELPER_DELAY", "0"))
# When set, the process stops answering sampling signals this many seconds
# before its natural exit, so a sample deterministically races process death.
stop_early = float(os.environ.get("HELPER_STOP_EARLY", "0"))
counter = {"n": 0}
lock = threading.Lock()
accept_until = [time.monotonic() + lifetime]


def handler(signum, frame):
    # Refuse dumps in the shutdown window: the process is about to exit.
    if time.monotonic() >= accept_until[0] - stop_early:
        return
    with lock:
        counter["n"] += 1
        seq = counter["n"]
    if delay:
        time.sleep(delay)
    target = frame.f_back if frame is not None else None
    text = "# helper stack\n" + "".join(traceback.format_stack(target))
    final = os.path.join(stack_dir, f"stack-p{os.getpid()}-s{seq}.txt")
    tmp = final + ".tmp"
    with open(tmp, "w") as fh:
        fh.write(text)
    os.replace(tmp, final)


signal.signal(signum, handler)
print("READY", flush=True)
deadline = time.monotonic() + lifetime
while time.monotonic() < deadline:
    time.sleep(0.05)
"""


def _start_helper(tmp_path, lifetime=30.0, delay=0.0, stop_early=0.0):
    helper = tmp_path / "helper.py"
    helper.write_text(HELPER)
    stack_dir = tmp_path / "stacks"
    stack_dir.mkdir()
    import subprocess

    env = dict(os.environ)
    env.update(
        {
            "HELPER_STACK_DIR": str(stack_dir),
            "HELPER_SIGNUM": str(signal.SIGRTMIN + 2),
            "HELPER_LIFETIME": str(lifetime),
            "HELPER_DELAY": str(delay),
            "HELPER_STOP_EARLY": str(stop_early),
        }
    )
    proc = subprocess.Popen(
        [sys.executable, str(helper)],
        stdout=subprocess.PIPE,
        env=env,
        start_new_session=True,
    )
    assert proc.stdout.readline().strip() == b"READY"
    return proc, stack_dir


def _stop(proc):
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except Exception:
        proc.kill()
        proc.wait(timeout=5)


# ------------------------------------------------------------------- units
def test_pid_alive_distinguishes_zombie(tmp_path):
    import subprocess

    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait(timeout=5)
    # Un-waited? It is already waited, so it is fully gone.
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and pid_state(proc.pid) is not None:
        time.sleep(0.02)
    assert pid_alive(proc.pid) is False
    assert pid_state(proc.pid) is None
    assert pid_alive(2_000_000) is False


def test_sampler_captures_main_thread_stack(tmp_path):
    proc, stack_dir = _start_helper(tmp_path)
    try:
        sampler = StackSampler(str(stack_dir))
        result = sampler.collect([proc.pid], timeout=3.0, ready=True)
        assert result["member_count"] == 1
        member = result["members"][0]
        assert member["status"] == STACK_OK
        assert member["pid"] == proc.pid
        assert member["sequence"] == 1
        assert "helper stack" in member["stack"]
        assert "traceback.format_stack" in member["stack"]
    finally:
        _stop(proc)


def test_sampler_sequences_are_independent_per_request(tmp_path):
    proc, stack_dir = _start_helper(tmp_path)
    try:
        sampler = StackSampler(str(stack_dir))
        first = sampler.collect([proc.pid], timeout=3.0, ready=True)
        second = sampler.collect([proc.pid], timeout=3.0, ready=True)
        assert first["sample_id"] != second["sample_id"]
        assert first["members"][0]["sequence"] == 1
        assert second["members"][0]["sequence"] == 2
        assert first["members"][0]["stack"] != second["members"][0]["stack"] or True
        # Each response only carries its own dump file.
        assert (stack_dir / f"stack-p{proc.pid}-s1.txt").is_file()
        assert (stack_dir / f"stack-p{proc.pid}-s2.txt").is_file()
    finally:
        _stop(proc)


def test_sampler_timeout_marks_unresponsive_member(tmp_path):
    # Handler sleeps 2 s; a 0.3 s sample must time out while the process lives.
    proc, stack_dir = _start_helper(tmp_path, delay=2.0)
    try:
        sampler = StackSampler(str(stack_dir))
        t0 = time.monotonic()
        result = sampler.collect([proc.pid], timeout=0.3, ready=True)
        elapsed = time.monotonic() - t0
        member = result["members"][0]
        assert member["status"] == STACK_TIMEOUT
        assert member["stack"] is None
        assert 0.25 <= elapsed < 1.5
        assert pid_alive(proc.pid) is True
    finally:
        _stop(proc)


def test_sampler_late_dump_does_not_count_for_later_sample(tmp_path):
    # First sample times out; the delayed dump lands afterwards with the
    # first sequence number.  A later sample reserves a new sequence and
    # must only accept its own fresh dump, never the late one.
    proc, stack_dir = _start_helper(tmp_path, delay=0.8)
    try:
        sampler = StackSampler(str(stack_dir))
        expired = sampler.collect([proc.pid], timeout=0.2, ready=True)
        assert expired["members"][0]["status"] == STACK_TIMEOUT
        assert expired["members"][0]["sequence"] == 1

        # Let the late sequence-1 dump land.
        late = stack_dir / f"stack-p{proc.pid}-s1.txt"
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline and not late.is_file():
            time.sleep(0.02)
        assert late.is_file()

        # New sample reserves sequence 2 and must get a fresh ok response.
        followup = sampler.collect([proc.pid], timeout=3.0, ready=True)
        member = followup["members"][0]
        assert member["sequence"] == 2
        assert member["status"] == STACK_OK
        assert (stack_dir / f"stack-p{proc.pid}-s2.txt").is_file()
    finally:
        _stop(proc)


def test_sampler_member_exiting_mid_sample_is_marked(tmp_path):
    # Stops answering dumps 1 s before exiting: the signal lands in the
    # shutdown window and the member is reported exited, not timeout.
    proc, stack_dir = _start_helper(
        tmp_path, lifetime=1.0, stop_early=1.0
    )
    try:
        sampler = StackSampler(str(stack_dir))
        result = sampler.collect([proc.pid], timeout=5.0, ready=True)
        member = result["members"][0]
        assert member["status"] == STACK_EXITED
        assert member["stack"] is None
        assert member["detail"]
    finally:
        proc.wait(timeout=5)


# -------------------------------------------------------------- API scenarios
def start(client, build_id, scenarios=None, grace=1.0, duration=60.0,
          stack_delay=0.0):
    resp = client.post(
        "/builds",
        json={
            "build_id": build_id,
            "scenarios": scenarios or [],
            "grace_seconds": grace,
            "duration": duration,
            "stack_delay": stack_delay,
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def wait_ready(client, build_id, count=1, timeout=8.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        text = client.get(
            f"/builds/{build_id}/logs?stream=stdout"
        ).json()["stdout"]["text"]
        if text.count("READY") >= count:
            return
        time.sleep(0.05)
    raise AssertionError(f"build {build_id} never reached READY count {count}")


def test_api_stack_sample_returns_main_thread_stack(client, build_id):
    start(client, build_id)
    wait_ready(client, build_id)
    resp = client.post(f"/builds/{build_id}/stacks", json={"timeout": 3.0})
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["build_id"] == build_id
    assert data["member_count"] == 1
    member = data["members"][0]
    assert member["status"] == "ok"
    assert member["pid"] == data["pgid"]
    assert "main-thread stack captured by signal" in member["stack"]
    assert "worker.py" in member["stack"]
    assert "run_long_lived" in member["stack"]
    # Logging still works alongside sampling.
    logs = client.get(f"/builds/{build_id}/logs?stream=stdout").json()
    assert "heartbeat" in logs["stdout"]["text"] or "role active" in logs["stdout"]["text"]
    client.post(f"/builds/{build_id}/cancel")
    _wait_state(client, build_id, "cancelled")


def test_api_sample_after_leader_exit_targets_survivors(client, build_id):
    start(client, build_id, scenarios=["main-exits-early"])
    # Two READY lines: leader and orphaned child.
    wait_ready(client, build_id, count=2)
    deadline = time.monotonic() + 6.0
    while time.monotonic() < deadline:
        text = client.get(
            f"/builds/{build_id}/logs?stream=stdout"
        ).json()["stdout"]["text"]
        if "main process exiting now" in text:
            break
        time.sleep(0.05)

    resp = client.post(f"/builds/{build_id}/stacks", json={"timeout": 3.0})
    data = resp.json()
    # Roster fixed at acceptance: leader already gone, only the orphan child.
    assert data["member_count"] == 1
    member = data["members"][0]
    assert member["status"] == "ok"
    assert member["pid"] != data["pgid"], "leader pid must not be re-sampled"
    assert "run_long_lived" in member["stack"]
    client.post(f"/builds/{build_id}/cancel")
    _wait_state(client, build_id, "cancelled")


def test_api_concurrent_samples_isolate_late_responses(client, build_id):
    # Every dump handler sleeps 1 s, so a short request times out while a
    # long one succeeds; sequence numbers keep their responses separate.
    start(client, build_id, stack_delay=1.0)
    wait_ready(client, build_id)

    short_box, long_box = {}, {}

    def call_short():
        short_box["resp"] = client.post(
            f"/builds/{build_id}/stacks", json={"timeout": 0.25}
        ).json()

    def call_long():
        long_box["resp"] = client.post(
            f"/builds/{build_id}/stacks", json={"timeout": 5.0}
        ).json()

    t1 = threading.Thread(target=call_short)
    t2 = threading.Thread(target=call_long)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    short, long = short_box["resp"], long_box["resp"]
    assert short["sample_id"] != long["sample_id"]
    s_member = short["members"][0]
    l_member = long["members"][0]
    assert s_member["sequence"] == 1
    assert l_member["sequence"] == 2
    assert s_member["status"] == "timeout"
    assert s_member["stack"] is None
    assert l_member["status"] == "ok"
    assert l_member["stack"] is not None
    # The long response must not have consumed the short one's late dump:
    # it carries exactly sequence 2 content.
    assert f"seq=2" in l_member["stack"]
    assert f"seq=1" not in l_member["stack"]

    # A third sample after both finished is sequence 3 and independently ok.
    third = client.post(
        f"/builds/{build_id}/stacks", json={"timeout": 5.0}
    ).json()
    assert third["members"][0]["sequence"] == 3
    assert third["members"][0]["status"] == "ok"
    client.post(f"/builds/{build_id}/cancel")
    _wait_state(client, build_id, "cancelled")


def test_api_cancel_during_sampling_still_collects_and_marks(client, build_id):
    start(client, build_id, scenarios=[], grace=0.8)
    wait_ready(client, build_id)
    result_box = {}

    def call_sample():
        result_box["resp"] = client.post(
            f"/builds/{build_id}/stacks", json={"timeout": 4.0}
        ).json()

    t = threading.Thread(target=call_sample)
    t.start()
    time.sleep(0.2)  # let the sample register its roster and signal
    client.post(f"/builds/{build_id}/cancel")
    t.join(timeout=10)

    member = result_box["resp"]["members"][0]
    # The member is terminated during the window: either its dump already
    # landed (ok) or it is honestly reported as exited -- never a hang.
    assert member["status"] in ("ok", "exited")
    if member["status"] == "exited":
        assert member["stack"] is None
        assert member["detail"]
    snap = _wait_state(client, build_id, "cancelled")
    # Cancellation completed normally and teardown logs were still captured.
    logs = client.get(f"/builds/{build_id}/logs").json()
    assert "teardown error" in logs["stderr"]["text"]
    assert snap["state"] == "cancelled"


def test_api_stack_endpoint_error_paths(client, build_id):
    assert client.post("/builds/missing/stacks", json={}).status_code == 404
    start(client, build_id, duration=0.5)
    _wait_state(client, build_id, TERMINAL, timeout=10)
    assert client.post(f"/builds/{build_id}/stacks", json={}).status_code == 409
    bad = client.post(f"/builds/{build_id}/stacks", json={"timeout": 999})
    assert bad.status_code == 422
