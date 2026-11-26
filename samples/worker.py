#!/usr/bin/env python3
"""Sample "build" script used by the cancellation service.

Stdlib only.  The scenarios are flags and can be combined:

  --main-exits-early   main process starts a long-lived child in the same
                       process group, then exits immediately (the supervisor
                       must keep tracking the remaining group members)
  --ignore-term        SIGTERM is logged and ignored, so the grace period
                       expires and the supervisor has to SIGKILL
  --noisy-teardown     on SIGTERM, emit far more than 32 KiB on BOTH stdout
                       and stderr while shutting down (exercises continued
                       log reading during the wait and tail truncation)
  --duration N         natural lifetime in seconds (default 60)
  --child              internal: run as the long-lived child role
  --stack-delay N      delay the stack-dump signal handler by N seconds,
                       producing a late response for sampling tests

Every teardown also writes an error line on stderr ("收尾时报的错").

When BUILDSVC_STACK_DIR is set (injected by the service) a handler for the
queued real-time signal BUILDSVC_STACK_SIGNAL (default SIGRTMIN+2) is
installed: it dumps the main-thread stack to an atomically renamed
stack-p<PID>-s<seq>.txt file in that directory.
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
import traceback

# Configuration injected by the build service for on-demand stack sampling.
STACK_DIR = os.environ.get("BUILDSVC_STACK_DIR")
try:
    STACK_SIGNAL = int(os.environ.get("BUILDSVC_STACK_SIGNAL", str(signal.SIGRTMIN + 2)))
except (TypeError, ValueError):  # pragma: no cover - defensive
    STACK_SIGNAL = signal.SIGRTMIN + 2
_stack_seq = 0
_stack_seq_lock = threading.Lock()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="sample build script")
    p.add_argument("--main-exits-early", action="store_true")
    p.add_argument("--ignore-term", action="store_true")
    p.add_argument("--noisy-teardown", action="store_true")
    p.add_argument("--duration", type=float, default=60.0)
    p.add_argument("--child", action="store_true")
    p.add_argument("--teardown-lines", type=int, default=4000)
    p.add_argument(
        "--stack-delay",
        type=float,
        default=0.0,
        help="seconds to wait inside the stack-dump signal handler before "
             "writing the dump (simulates a late/blocked response)",
    )
    return p.parse_args()


def log(msg: str, err: bool = False) -> None:
    stream = sys.stderr if err else sys.stdout
    print(f"[{time.strftime('%H:%M:%S')}] pid={_pid()} role={'child' if ARGS.child else 'main'}: {msg}",
          file=stream, flush=True)


def _pid() -> int:
    return os.getpid()


def make_handler(args: argparse.Namespace, state: dict):
    def handle_term(signum, frame) -> None:  # noqa: ANN001
        log(f"received signal {signum}, beginning teardown")
        if args.ignore_term:
            log("refusing to terminate (SIGTERM ignored); awaiting SIGKILL", err=True)
            return
        if args.noisy_teardown:
            # Far more than the 32 KiB tail limit, on both streams separately.
            log(f"emitting {args.teardown_lines} teardown lines on stdout and stderr")
            for i in range(args.teardown_lines):
                print(f"teardown-stdout line {i:06d}: " + "x" * 40, flush=True)
                print(f"teardown-stderr line {i:06d}: " + "y" * 40, file=sys.stderr, flush=True)
        # The error reported while wrapping up.
        log("teardown error: cleanup step reported failure code 7", err=True)
        state["terminated"] = True
        sys.exit(1)
    return handle_term


def run_long_lived(args: argparse.Namespace, state: dict) -> None:
    deadline = time.monotonic() + args.duration
    heartbeat = 0
    # Emit a line immediately so callers can confirm this role is alive and
    # its output is being drained without waiting for the first tick.
    log("role active; entering work loop")
    while time.monotonic() < deadline and not state["terminated"]:
        if heartbeat % 4 == 0:
            log(f"working... heartbeat={heartbeat}")
        heartbeat += 1
        time.sleep(0.25)
    if not state["terminated"]:
        log("work finished naturally")


def make_stack_handler(args: argparse.Namespace):
    """Handler for the queued stack-sampling real-time signal.

    Runs on the main thread (Python signals always do), so the dumped frames
    are the main-thread call stack.  Each invocation writes an atomically
    renamed file carrying this PID's 1-based dump sequence; the service
    matches a request to its reserved sequence and therefore never confuses
    a late dump of one sample with another sample's response.
    """

    def handle_stack(signum, frame) -> None:  # noqa: ANN001
        global _stack_seq
        with _stack_seq_lock:
            _stack_seq += 1
            seq = _stack_seq
        if args.stack_delay > 0:
            # Hold the handler for a while.  Real-time signals queue while the
            # handler runs, so concurrent samples still get separate dumps in
            # request order -- this only makes every response late.
            time.sleep(args.stack_delay)
        lines = [
            "# main-thread stack captured by signal\n",
            f"# pid={os.getpid()} seq={seq}\n",
        ]
        # Skip the handler frame itself; report the interrupted call stack.
        target = frame.f_back if frame is not None else None
        lines.extend(traceback.format_stack(target))
        text = "".join(lines)
        if not STACK_DIR:  # pragma: no cover - service always sets it
            return
        final = os.path.join(STACK_DIR, f"stack-p{os.getpid()}-s{seq}.txt")
        tmp = final + f".tmp{os.getpid()}-{seq}"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, final)

    return handle_stack


def install_stack_handler(args: argparse.Namespace) -> None:
    if STACK_DIR:
        signal.signal(STACK_SIGNAL, make_stack_handler(args))


def main() -> int:
    state = {"terminated": False}
    signal.signal(signal.SIGTERM, make_handler(ARGS, state))
    install_stack_handler(ARGS)
    # Signal handlers are installed before announcing readiness, so a cancel
    # that races process startup can never hit the default SIGTERM action.
    log("build started")
    print("READY", flush=True)

    if not ARGS.child and ARGS.main_exits_early:
        child_cmd = [sys.executable, __file__, "--child", "--duration", str(ARGS.duration)]
        for flag in ("--ignore-term", "--noisy-teardown"):
            if getattr(ARGS, flag.lstrip("-").replace("-", "_")):
                child_cmd.append(flag)
        if ARGS.stack_delay > 0:
            child_cmd += ["--stack-delay", str(ARGS.stack_delay)]
        child = subprocess.Popen(child_cmd)
        log(f"spawned long-lived child pid={child.pid}; main process exiting now")
        return 0

    run_long_lived(ARGS, state)
    log("build exiting with code 0")
    return 0


if __name__ == "__main__":
    ARGS = parse_args()
    sys.exit(main())
