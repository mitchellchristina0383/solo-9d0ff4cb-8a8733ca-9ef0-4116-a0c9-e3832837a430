"""On-demand Python stack sampling for running build groups.

A *sample* asks every member of a build's process group for the stack of its
main thread.  The request is accepted against the membership snapshot taken
at acceptance time (``group_pids``), so a leader that exits before sampling
simply is not part of the roster while surviving same-group children still
are.

Mechanism
---------
Each group member runs the bundled sample worker, which installs a handler
for a queued real-time signal (:data:`STACK_SIGNAL`).  Real-time signals are
queued rather than coalesced, so two concurrent samples aimed at the same
PID produce two independently delivered notifications and two separate
dumps.

Every dump is written to the build-private stack directory as an atomically
renamed file ``stack-p<PID>-s<seq>.txt`` where ``seq`` is the 1-based count
of signals that PID has handled.  The sampler owns a per-PID sequence
counter: the *n*-th sample reserves sequence *n* and only accepts the file
carrying that exact sequence.  A dump arriving after its request already
timed out keeps its old sequence number and can never match a later
request's reserved sequence, which is what guarantees that every response
is attributable solely to the request that triggered it -- late responses
from one sample never leak into another.

Members that disappear mid-sample (including reaped zombies) are reported
as ``exited``; members still alive but without their dump at the deadline
are reported as ``timeout``.  Already collected stacks are always returned.
"""
from __future__ import annotations

import errno
import logging
import os
import signal
import threading
import time
import uuid
from typing import Optional

from .procfs import pid_alive, pid_state

logger = logging.getLogger("buildsvc.stacks")

# Member-level outcomes.
STACK_OK = "ok"
STACK_EXITED = "exited"
STACK_TIMEOUT = "timeout"
STACK_ERROR = "error"

# SIGRTMIN+2: keep clear of the lowest RT numbers some libcs reserve for
# pthread/threading use.  Real-time signals queue, so concurrent samples do
# not collapse into one delivery.
STACK_SIGNAL = signal.SIGRTMIN + 2
STACK_SIGNAL_ENVVAR = "BUILDSVC_STACK_SIGNAL"
STACK_DIR_ENVVAR = "BUILDSVC_STACK_DIR"

POLL_INTERVAL = 0.02
# Server-side ceiling for a single sample's wait, mirrored by the API model.
MAX_STACK_TIMEOUT = 30.0


class StackSampler:
    """Per-build sampler: owns the stack directory and per-PID sequences."""

    def __init__(self, stack_dir: str, signum: int = STACK_SIGNAL) -> None:
        self._stack_dir = stack_dir
        self._signum = signum
        self._seq_lock = threading.Lock()
        self._seq: dict[int, int] = {}

    @property
    def stack_dir(self) -> str:
        return self._stack_dir

    @property
    def signum(self) -> int:
        return self._signum

    def reserve_sequence(self, pid: int) -> int:
        """Reserve the next 1-based dump sequence for *pid*."""
        with self._seq_lock:
            nxt = self._seq.get(pid, 0) + 1
            self._seq[pid] = nxt
            return nxt

    def sample_path(self, pid: int, sequence: int) -> str:
        return os.path.join(
            self._stack_dir, f"stack-p{pid}-s{sequence}.txt"
        )

    def collect(
        self, pids: list[int], timeout: float, ready: bool
    ) -> dict:
        """Sample the given roster, waiting at most *timeout* seconds.

        *pids* is the membership snapshot fixed at request acceptance and is
        never re-based: members that vanish are reported as exited, and
        members spawned later are ignored.  Returns a JSON-friendly response
        containing exactly this sample's results.
        """
        sample_id = uuid.uuid4().hex
        started_at = time.time()
        deadline = time.monotonic() + max(timeout, 0.0)
        roster = {
            pid: {
                "pid": pid,
                "sequence": self.reserve_sequence(pid),
                "status": None,
                "stack": None,
                "detail": None,
            }
            for pid in pids
        }

        # First delivery attempt.  A process that dies between the snapshot
        # and the signal is accounted as exited right away.
        for pid, entry in roster.items():
            self._signal_one(pid, entry)

        while True:
            pending = False
            for entry in roster.values():
                if entry["status"] is not None:
                    continue
                pid = entry["pid"]
                path = self.sample_path(pid, entry["sequence"])
                try:
                    with open(path, "r", encoding="utf-8", errors="replace") as fh:
                        entry["stack"] = fh.read()
                    entry["status"] = STACK_OK
                    continue
                except FileNotFoundError:
                    pass
                except OSError as exc:  # pragma: no cover - defensive
                    entry["status"] = STACK_ERROR
                    entry["detail"] = f"read failed: {exc}"
                    continue
                if not pid_alive(pid):
                    entry["status"] = STACK_EXITED
                    state = pid_state(pid)
                    entry["detail"] = (
                        "process became zombie before responding"
                        if state == "Z"
                        else "process exited before responding"
                    )
                    continue
                pending = True

            if not pending:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                for entry in roster.values():
                    if entry["status"] is None:
                        entry["status"] = STACK_TIMEOUT
                        entry["detail"] = (
                            f"no stack dump within {timeout:.3f}s"
                        )
                break
            time.sleep(min(POLL_INTERVAL, remaining))

        members = [
            {
                "pid": entry["pid"],
                "sequence": entry["sequence"],
                "status": entry["status"],
                "stack": entry["stack"],
                "detail": entry["detail"],
            }
            for entry in sorted(roster.values(), key=lambda e: e["pid"])
        ]
        return {
            "sample_id": sample_id,
            "requested_at": started_at,
            "timeout_seconds": float(timeout),
            "group_ready": ready,
            "member_count": len(members),
            "members": members,
        }

    def _signal_one(self, pid: int, entry: dict) -> None:
        if not pid_alive(pid):
            entry["status"] = STACK_EXITED
            entry["detail"] = "process exited before signal"
            return
        try:
            os.kill(pid, self._signum)
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                entry["status"] = STACK_EXITED
                entry["detail"] = "process exited before signal"
            else:  # pragma: no cover - permission errors etc.
                entry["status"] = STACK_ERROR
                entry["detail"] = f"signal failed: {exc}"
