"""End-to-end cancellation scenarios against the FastAPI app.

The sample worker is a real subprocess tree; these tests exercise genuine
SIGTERM/SIGKILL behaviour, /proc group liveness and pipe EOF handling.
Grace periods are deliberately short to keep the suite fast.
"""
import time

from conftest import _wait_state

TERMINAL = {"succeeded", "failed", "cancelled"}


def start(client, build_id, scenarios=None, grace=0.8, duration=60.0):
    resp = client.post(
        "/builds",
        json={
            "build_id": build_id,
            "scenarios": scenarios or [],
            "grace_seconds": grace,
            "duration": duration,
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def wait_running(client, build_id, timeout=5.0):
    """Wait until the build reaches running state with the worker ready."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = client.get(f"/builds/{build_id}").json()
        if snap["state"] == "running":
            logs = client.get(f"/builds/{build_id}/logs?stream=stdout").json()
            if "READY" in logs["stdout"]["text"]:
                return snap
        time.sleep(0.05)
    raise AssertionError(f"build {build_id} never became ready")


def test_health_and_docs(client):
    assert client.get("/health").json()["status"] == "ok"
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200


def test_natural_success(client, build_id):
    start(client, build_id, scenarios=[], grace=1.0, duration=0.8)
    snap = _wait_state(client, build_id, "succeeded", timeout=15.0)
    assert snap["leader_exit"]["kind"] == "exit"
    assert snap["leader_exit"]["code"] == 0
    assert snap["cancellation"] is None
    logs = client.get(f"/builds/{build_id}/logs").json()
    assert "build started" in logs["stdout"]["text"]
    assert logs["stdout"]["truncated"] is False


def test_graceful_cancel_not_forced(client, build_id):
    """Default worker exits on SIGTERM: cancellation stays graceful."""
    start(client, build_id, scenarios=[], grace=1.0)
    wait_running(client, build_id)
    info = client.post(f"/builds/{build_id}/cancel").json()
    assert info["state"] == "cancelling"
    snap = _wait_state(client, build_id, "cancelled", timeout=15.0)
    assert snap["cancellation"]["forced"] is False
    assert snap["cancellation"]["notified_at"] is not None
    assert snap["cancellation"]["force_killed_at"] is None
    # Teardown error line must be captured on the stderr tail.
    logs = client.get(f"/builds/{build_id}/logs").json()
    assert "teardown error" in logs["stderr"]["text"]
    assert "received signal 15" in logs["stdout"]["text"]


def test_force_kill_after_grace(client, build_id):
    """ignore-term: group survives SIGTERM, must be SIGKILLed at grace end."""
    start(client, build_id, scenarios=["ignore-term"], grace=0.8)
    wait_running(client, build_id)
    t0 = time.monotonic()
    client.post(f"/builds/{build_id}/cancel")
    snap = _wait_state(client, build_id, "cancelled", timeout=15.0)
    elapsed = time.monotonic() - t0
    assert snap["cancellation"]["forced"] is True
    assert snap["cancellation"]["force_killed_at"] is not None
    # The grace period really elapsed; SIGKILL is not sent immediately.
    assert elapsed >= 0.7
    logs = client.get(f"/builds/{build_id}/logs").json()
    assert "awaiting SIGKILL" in logs["stderr"]["text"]


def test_main_exits_early_group_tracked(client, build_id):
    """Main leader exits first; the remaining same-group child is handled."""
    start(client, build_id, scenarios=["main-exits-early"], grace=1.0)
    # Wait until the leader has actually exited and the orphaned child is
    # emitting its own output (proves its inherited pipe is still drained).
    deadline = time.monotonic() + 8.0
    while time.monotonic() < deadline:
        logs = client.get(f"/builds/{build_id}/logs?stream=stdout").json()
        text = logs["stdout"]["text"]
        if "main process exiting now" in text and "heartbeat" in text:
            break
        time.sleep(0.05)
    mid = client.get(f"/builds/{build_id}").json()
    assert mid["state"] == "running", "build must stay running while group lives"
    client.post(f"/builds/{build_id}/cancel")
    snap = _wait_state(client, build_id, "cancelled", timeout=15.0)
    assert snap["cancellation"]["forced"] is False
    logs = client.get(f"/builds/{build_id}/logs").json()
    assert "main process exiting now" in logs["stdout"]["text"]
    # The orphaned child's output must still be drained.
    assert "heartbeat" in logs["stdout"]["text"]

def test_main_exits_early_plus_ignore_term_force_kills_orphan(client, build_id):
    start(
        client,
        build_id,
        scenarios=["main-exits-early", "ignore-term"],
        grace=0.8,
    )
    # The orphan only becomes signal-safe once it printed its own READY.
    deadline = time.monotonic() + 8.0
    while time.monotonic() < deadline:
        text = client.get(
            f"/builds/{build_id}/logs?stream=stdout"
        ).json()["stdout"]["text"]
        if text.count("READY") >= 2:
            break
        time.sleep(0.05)
    client.post(f"/builds/{build_id}/cancel")
    snap = _wait_state(client, build_id, "cancelled", timeout=15.0)
    assert snap["cancellation"]["forced"] is True


def test_noisy_teardown_tails_are_split_and_capped(client, build_id):
    """~250 KiB on each stream: both tails capped at 32 KiB, never mixed."""
    start(client, build_id, scenarios=["noisy-teardown"], grace=2.0)
    wait_running(client, build_id)
    client.post(f"/builds/{build_id}/cancel")
    snap = _wait_state(client, build_id, "cancelled", timeout=20.0)
    assert snap["cancellation"]["forced"] is False
    logs = client.get(f"/builds/{build_id}/logs").json()
    limit = 32 * 1024
    for stream in ("stdout", "stderr"):
        entry = logs[stream]
        assert entry["retained_bytes"] <= limit
        assert entry["total_bytes"] > limit
        assert entry["truncated"] is True
    # Tail content is the *latest* output; streams stay strictly separated.
    assert "teardown-stdout" in logs["stdout"]["text"]
    assert "teardown-stderr" not in logs["stdout"]["text"]
    assert "teardown-stderr" in logs["stderr"]["text"]
    assert "teardown-stdout" not in logs["stderr"]["text"]
    # The final teardown error is within the retained stderr tail.
    assert "teardown error" in logs["stderr"]["text"]
    # Stream filter parameter.
    only_err = client.get(f"/builds/{build_id}/logs?stream=stderr").json()
    assert "stdout" not in only_err and "stderr" in only_err


def test_repeated_cancel_uses_first_request(client, build_id):
    """Second cancel must not restart or alter the first shutdown sequence."""
    start(client, build_id, scenarios=["ignore-term"], grace=1.2)
    wait_running(client, build_id)
    first = client.post(
        f"/builds/{build_id}/cancel", json={"grace_seconds": 1.2}
    ).json()["cancellation"]
    second = client.post(
        f"/builds/{build_id}/cancel", json={"grace_seconds": 0.0}
    ).json()["cancellation"]
    # First request's grace is retained; second call shares the record.
    assert second["grace_seconds"] == 1.2
    assert second["requested_at"] == first["requested_at"]
    snap = _wait_state(client, build_id, "cancelled", timeout=15.0)
    assert snap["cancellation"]["forced"] is True
    assert snap["cancellation"]["requested_at"] == first["requested_at"]


def test_parallel_builds_are_isolated(client, build_id):
    """Other builds keep running while one build is torn down."""
    other_id = build_id + "-peer"
    start(client, build_id, scenarios=["ignore-term"], grace=0.6)
    start(client, other_id, scenarios=[], grace=1.0, duration=60.0)
    wait_running(client, build_id)
    wait_running(client, other_id)
    client.post(f"/builds/{build_id}/cancel")
    _wait_state(client, build_id, "cancelled", timeout=15.0)
    peer = client.get(f"/builds/{other_id}").json()
    assert peer["state"] == "running"
    # Clean up the peer gracefully.
    client.post(f"/builds/{other_id}/cancel")
    _wait_state(client, other_id, "cancelled", timeout=15.0)
    listing = client.get("/builds").json()["builds"]
    states = {b["build_id"]: b["state"] for b in listing}
    assert states[build_id] == "cancelled"
    assert states[other_id] == "cancelled"


def test_cancel_unknown_and_duplicate_id(client, build_id):
    assert client.post("/builds/missing/cancel").status_code == 404
    start(client, build_id, scenarios=[], grace=1.0)
    dup = client.post("/builds", json={"build_id": build_id, "scenarios": []})
    assert dup.status_code == 409
    bad = client.post(
        "/builds", json={"build_id": build_id + "x", "scenarios": ["nope"]}
    )
    assert bad.status_code == 422


def test_cancel_after_finish_returns_terminal_state(client, build_id):
    start(client, build_id, scenarios=[], grace=1.0, duration=0.6)
    _wait_state(client, build_id, "succeeded", timeout=15.0)
    resp = client.post(f"/builds/{build_id}/cancel")
    # A finished build stays finished; no state corruption.
    assert resp.json()["state"] == "succeeded"
