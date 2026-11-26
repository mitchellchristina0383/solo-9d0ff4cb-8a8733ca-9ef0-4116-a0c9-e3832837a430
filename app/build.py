"""Per-build supervisor.

A *build* is a process group created with ``start_new_session=True``.  One
supervisor thread per build owns the whole lifecycle:

* pumps stdout/stderr through separate 32 KiB tail buffers (reading continues
  for as long as any group member holds the pipe open);
* waits for either natural group exit or a cancellation request;
* on cancellation: notifies the whole group (SIGTERM), waits out a grace
  period, then force-kills survivors (SIGKILL);
* finishes only after the whole group has exited *and* both pipes reached EOF.

Cancellation is idempotent: the API may be called any number of times but the
shutdown sequence runs exactly once, and every caller observes the same
shared progress record.
"""
from __future__ import annotations

import fcntl
import logging
import os
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .buffers import TailBuffer
from .procfs import group_alive, group_pids
from .reaper import Reaper, describe_status
from .stacks import (
    STACK_DIR_ENVVAR,
    STACK_SIGNAL,
    STACK_SIGNAL_ENVVAR,
    StackSampler,
)

logger = logging.getLogger("buildsvc.build")

TAIL_MAX = 32 * 1024
POLL_INTERVAL = 0.05
READER_JOIN_TIMEOUT = 10.0
KILL_WAIT_TIMEOUT = 10.0
READY_TIMEOUT = 15.0
# Upper bound for waiting on in-flight stack samples at build finalization.
STACK_CLEANUP_WAIT = 35.0
READY_MARKER = b"READY"

# Build lifecycle states.
RUNNING = "running"
CANCELLING = "cancelling"
SUCCEEDED = "succeeded"
FAILED = "failed"
CANCELLED = "cancelled"
TERMINAL_STATES = {SUCCEEDED, FAILED, CANCELLED}


def _set_nonblocking(fd: int) -> None:
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


@dataclass
class CancellationInfo:
    grace_seconds: float
    requested_at: float
    notified_at: Optional[float] = None
    force_killed_at: Optional[float] = None
    forced: Optional[bool] = None  # None = sequence not finished yet
    note: Optional[str] = None


@dataclass
class Build:
    build_id: str
    scenario: str
    command: list[str]
    reaper: Reaper
    grace_default: float = 5.0
    stack_root: Optional[str] = None
    stack_delay: float = 0.0
    stdout: TailBuffer = field(default_factory=lambda: TailBuffer(TAIL_MAX))
    stderr: TailBuffer = field(default_factory=lambda: TailBuffer(TAIL_MAX))
    state: str = RUNNING
    pid: Optional[int] = None
    pgid: Optional[int] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    leader_exit: Optional[dict] = None
    error: Optional[str] = None
    cancel: Optional[CancellationInfo] = None

    def __post_init__(self) -> None:
        self._proc: Optional[subprocess.Popen] = None
        self._out_raw = None
        self._err_raw = None
        self._out_fd: Optional[int] = None
        self._err_fd: Optional[int] = None
        self._cancel_event = threading.Event()
        self._ready_event = threading.Event()
        self._ready_lock = threading.Lock()
        self._ready_count = 0
        self._done_event = threading.Event()
        self._supervisor: Optional[threading.Thread] = None
        self._stdout_thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None
        self._inflight_lock = threading.Lock()
        self._inflight = 0
        self._idle_event = threading.Event()
        self._idle_event.set()
        # Build-private directory for stack dumps; the worker learns it via
        # an env var and only writes inside it.
        stack_root = self.stack_root or os.path.join(
            tempfile.gettempdir(), "buildsvc-stacks"
        )
        self.stack_dir = os.path.join(stack_root, f"build-{self.build_id}-{uuid.uuid4().hex[:8]}")
        os.makedirs(self.stack_dir, exist_ok=True)
        self.sampler = StackSampler(self.stack_dir, signum=STACK_SIGNAL)

    # ------------------------------------------------------------------ start
    def start(self) -> None:
        self.started_at = time.time()
        env = dict(os.environ)
        env[STACK_DIR_ENVVAR] = self.stack_dir
        env[STACK_SIGNAL_ENVVAR] = str(STACK_SIGNAL)
        if self.stack_delay > 0:
            self.command = [*self.command, "--stack-delay", str(self.stack_delay)]
        self._proc = subprocess.Popen(
            self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,  # new process group, group id == leader pid
            close_fds=True,
            env=env,
        )
        self.pid = self._proc.pid
        self.pgid = self._proc.pid
        # Detach the Python buffered wrappers and read at the OS level instead:
        # BufferedReader.read() on a pipe performs an internal readahead that
        # asks for more bytes than are currently available and blocks inside
        # pipe_read until EOF, so a reader thread would observe nothing while
        # the process is alive even though data is sitting in the pipe.
        # select + non-blocking os.read returns data as soon as it arrives.
        self._out_raw = self._proc.stdout.detach()
        self._err_raw = self._proc.stderr.detach()
        self._out_fd = self._out_raw.fileno()
        self._err_fd = self._err_raw.fileno()
        _set_nonblocking(self._out_fd)
        _set_nonblocking(self._err_fd)
        self._stdout_thread = threading.Thread(
            target=self._pump,
            args=(self._out_fd, self._out_raw, self.stdout, "stdout", True),
            name=f"{self.build_id}-stdout",
            daemon=True,
        )
        self._stderr_thread = threading.Thread(
            target=self._pump,
            args=(self._err_fd, self._err_raw, self.stderr, "stderr", False),
            name=f"{self.build_id}-stderr",
            daemon=True,
        )
        self._stdout_thread.start()
        self._stderr_thread.start()
        self._supervisor = threading.Thread(
            target=self._supervise, name=f"{self.build_id}-supervisor", daemon=True
        )
        self._supervisor.start()

    def _pump(self, fd: int, raw, buffer: TailBuffer, label: str, watch_ready: bool) -> None:
        """Drain one pipe into its tail buffer until EOF.

        Uses select + non-blocking os.read so data is delivered immediately
        instead of being held by buffered-reader readahead for the pipe's
        lifetime.  Returns only when every writer closed the pipe (EOF),
        which is why the supervisor joins these threads before reporting the
        build as finished.
        """
        try:
            while True:
                ready, _, _ = select.select([fd], [], [], 1.0)
                if not ready:
                    continue
                try:
                    data = os.read(fd, 65536)
                except BlockingIOError:
                    continue
                except OSError as exc:
                    logger.warning(
                        "read error on %s of %s: %s", label, self.build_id, exc
                    )
                    return
                if not data:
                    return  # EOF: all group members closed this stream
                buffer.append(data)
                if watch_ready and not self._ready_event.is_set():
                    # Count READY markers seen across the whole stream so that
                    # every group member (leader plus any same-group children)
                    # must announce readiness.  A member only prints READY after
                    # installing its own SIGTERM handler, so signalling cannot
                    # hit a process still running with default disposition.
                    new_markers = data.count(READY_MARKER)
                    if new_markers:
                        with self._ready_lock:
                            self._ready_count += new_markers
                        if self._is_group_ready():
                            self._ready_event.set()
        except Exception:  # pragma: no cover - defensive
            logger.exception("log pump failed on %s of %s", label, self.build_id)
        finally:
            try:
                raw.close()  # closes the fd exactly once
            except OSError:  # pragma: no cover
                pass
            # If the process dies before printing READY unblock any waiter.
            self._ready_event.set()

    # ------------------------------------------------------------- supervision
    def _is_group_ready(self) -> bool:
        """Every currently-live group member has announced readiness.

        The READY count is cumulative over the group's stdout, and the leader
        prints READY before spawning any child, so:

        * simple build: one member, one READY -> 1 >= 1;
        * main-exits-early: once the child prints READY there are two markers
          for (briefly) two members; after the leader exits, two markers still
          cover the single survivor.  A cancel can therefore never reach a
          member whose SIGTERM handler is not yet installed.
        """
        if self.pgid is None:
            return False
        return self._ready_count >= len(group_pids(self.pgid))

    def _supervise(self) -> None:
        try:
            assert self.pgid is not None
            while group_alive(self.pgid):
                if self._cancel_event.is_set():
                    self._shutdown_group()
                    break
                self._cancel_event.wait(POLL_INTERVAL)

            # Whole group is gone: wait for the pipes to reach EOF so that all
            # output, including teardown errors, is captured before reporting.
            self._join_reader(self._stdout_thread)
            self._join_reader(self._stderr_thread)
        except Exception:
            logger.exception("supervisor crashed for %s", self.build_id)
            self.error = "supervisor error: " + traceback_text()
        finally:
            self._finalize()

    def _shutdown_group(self) -> None:
        """Run exactly once per build: graceful notify, grace, then SIGKILL."""
        assert self.cancel is not None
        pgid = self.pgid
        now = time.time()
        self.cancel.notified_at = now
        logger.info("build %s: notifying group %s (SIGTERM)", self.build_id, pgid)
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            # Every member exited between the liveness check and the signal.
            self.cancel.forced = False
            self.cancel.note = "group already gone at notify"
            return

        deadline = time.monotonic() + self.cancel.grace_seconds
        while time.monotonic() < deadline:
            if not group_alive(pgid):
                self.cancel.forced = False
                logger.info("build %s: group exited within grace", self.build_id)
                return
            time.sleep(POLL_INTERVAL)

        if not group_alive(pgid):
            self.cancel.forced = False
            return

        # Grace expired with live members: force-kill the whole group.
        self.cancel.forced = True
        self.cancel.force_killed_at = time.time()
        logger.warning(
            "build %s: grace expired, force-killing group %s", self.build_id, pgid
        )
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            self.cancel.forced = False
            return

        kill_deadline = time.monotonic() + KILL_WAIT_TIMEOUT
        while time.monotonic() < kill_deadline:
            if not group_alive(pgid):
                return
            time.sleep(POLL_INTERVAL)
        self.cancel.note = "group still visible in /proc after SIGKILL wait"

    def _join_reader(self, thread: Optional[threading.Thread]) -> None:
        if thread is not None:
            thread.join(timeout=READER_JOIN_TIMEOUT)

    def _finalize(self) -> None:
        raw = self.reaper.exit_status(self.pid) if self.pid is not None else None
        if raw is not None:
            self.leader_exit = describe_status(raw)
            # Stop Popen from complaining about an un-waited child; the global
            # reaper is the designated waiter for every PID.
            if self._proc is not None:
                if os.WIFEXITED(raw):
                    self._proc.returncode = os.WEXITSTATUS(raw)
                elif os.WIFSIGNALED(raw):
                    self._proc.returncode = -os.WTERMSIG(raw)
        self.finished_at = time.time()
        if self._cancel_event.is_set():
            self.state = CANCELLED
        elif self.leader_exit is not None and self.leader_exit.get("kind") == "exit" \
                and self.leader_exit.get("code") == 0:
            self.state = SUCCEEDED
        else:
            self.state = FAILED
        # Let any in-flight samples resolve (each bounded by its own timeout)
        # before deleting their dump directory.
        self._idle_event.wait(timeout=STACK_CLEANUP_WAIT)
        self._remove_stack_dir()
        self._done_event.set()
        logger.info("build %s finished: %s", self.build_id, self.state)

    # ------------------------------------------------------------------- API
    def request_cancel(self, grace_seconds: Optional[float] = None) -> CancellationInfo:
        """First call arms cancellation; later calls share its record.

        Blocks briefly until the build announced readiness (signal handlers
        installed); a cancel racing process startup must not hit the default
        SIGTERM disposition.
        """
        if self._cancel_event.is_set():
            return self.cancel  # type: ignore[return-value]
        ready = self._ready_event.wait(timeout=READY_TIMEOUT)
        note = None
        if not ready:
            if self.state in TERMINAL_STATES:
                # Build exited (e.g. crashed) before READY: nothing to shut
                # down; keep the record for observability without signalling.
                return CancellationInfo(
                    grace_seconds=float(
                        self.grace_default if grace_seconds is None else grace_seconds
                    ),
                    requested_at=time.time(),
                    note="build finished before readiness",
                )
            note = "ready marker not seen within timeout; proceeding anyway"
            logger.warning("build %s: %s", self.build_id, note)
        grace = self.grace_default if grace_seconds is None else grace_seconds
        self.cancel = CancellationInfo(
            grace_seconds=float(grace), requested_at=time.time(), note=note
        )
        if self.state not in TERMINAL_STATES:
            self.state = CANCELLING
        self._cancel_event.set()
        return self.cancel

    @property
    def cancellation_requested(self) -> bool:
        return self._cancel_event.is_set()

    def force_cancel(self, grace_seconds: Optional[float] = None) -> CancellationInfo:
        """Arm cancellation without the readiness handshake (shutdown path)."""
        if self._cancel_event.is_set():
            return self.cancel  # type: ignore[return-value]
        grace = self.grace_default if grace_seconds is None else grace_seconds
        self.cancel = CancellationInfo(
            grace_seconds=float(grace), requested_at=time.time(),
            note="service shutdown",
        )
        if self.state not in TERMINAL_STATES:
            self.state = CANCELLING
        self._cancel_event.set()
        return self.cancel

    def wait_done(self, timeout: Optional[float] = None) -> bool:
        return self._done_event.wait(timeout)

    # ------------------------------------------------------------- stack dumps
    def sample_stacks(self, timeout: float) -> dict:
        """Snapshot group membership now and sample every member once.

        The roster is fixed at acceptance time: a leader that already exited
        is absent, while surviving same-group children are included.  Log
        pumping and cancellation keep running while this blocks.
        """
        assert self.pgid is not None
        with self._inflight_lock:
            self._inflight += 1
            self._idle_event.clear()
        try:
            pids = group_pids(self.pgid)
            result = self.sampler.collect(
                pids=pids, timeout=timeout, ready=self._ready_event.is_set()
            )
        finally:
            with self._inflight_lock:
                self._inflight -= 1
                if self._inflight == 0:
                    self._idle_event.set()
        result["build_id"] = self.build_id
        result["pgid"] = self.pgid
        result["state"] = self.state
        return result

    def _remove_stack_dir(self) -> None:
        shutil.rmtree(self.stack_dir, ignore_errors=True)

    def snapshot(self) -> dict:
        return {
            "build_id": self.build_id,
            "scenario": self.scenario,
            "command": self.command,
            "state": self.state,
            "pid": self.pid,
            "pgid": self.pgid,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": (
                round((self.finished_at or time.time()) - (self.started_at or 0.0), 3)
            ),
            "leader_exit": self.leader_exit,
            "error": self.error,
            "cancellation": _cancel_snapshot(self.cancel),
        }


def _cancel_snapshot(info: Optional[CancellationInfo]) -> Optional[dict]:
    if info is None:
        return None
    return {
        "requested_at": info.requested_at,
        "notified_at": info.notified_at,
        "grace_seconds": info.grace_seconds,
        "force_killed_at": info.force_killed_at,
        "forced": info.forced,
        "note": info.note,
    }


def traceback_text() -> str:
    import traceback

    return traceback.format_exc(limit=3)


# --------------------------------------------------------------------- manager
class BuildManager:
    def __init__(self, reaper: Reaper, worker_path: Path) -> None:
        self._reaper = reaper
        self._worker_path = worker_path
        self._builds: dict[str, Build] = {}
        self._lock = threading.Lock()
        self._stack_root = os.path.join(
            tempfile.gettempdir(), "buildsvc-stacks-" + uuid.uuid4().hex[:8]
        )
        os.makedirs(self._stack_root, exist_ok=True)

    @property
    def stack_root(self) -> str:
        return self._stack_root

    def create(
        self,
        scenario: list[str],
        grace_default: float,
        name: Optional[str],
        stack_delay: float = 0.0,
    ) -> Build:
        build_id = name or f"build-{uuid.uuid4().hex[:12]}"
        command = [sys.executable, str(self._worker_path), *scenario]
        build = Build(
            build_id=build_id,
            scenario="+".join(scenario),
            command=command,
            reaper=self._reaper,
            grace_default=grace_default,
            stack_root=self._stack_root,
            stack_delay=stack_delay,
        )
        with self._lock:
            if build_id in self._builds:
                raise KeyError(build_id)
            self._builds[build_id] = build
        build.start()
        return build

    def get(self, build_id: str) -> Optional[Build]:
        return self._builds.get(build_id)

    def list_builds(self) -> list[Build]:
        return list(self._builds.values())

    def shutdown_all(self) -> None:
        """Best-effort cleanup on service shutdown.

        Does not wait on the readiness handshake: the process is going away
        anyway and a stuck READY must not delay process teardown.
        """
        for build in self._builds.values():
            if build.state in TERMINAL_STATES:
                continue
            build.force_cancel(grace_seconds=1.0)
        for build in self._builds.values():
            build.wait_done(timeout=KILL_WAIT_TIMEOUT + 5)
        shutil.rmtree(self._stack_root, ignore_errors=True)
