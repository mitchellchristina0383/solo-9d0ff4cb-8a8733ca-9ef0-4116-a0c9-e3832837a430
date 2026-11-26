"""Global child-process reaper.

The service runs as a single process and may supervise several builds at
once.  Every build starts its processes in a dedicated process group, but
all of them are direct (or, after the leader dies, reparented) children of
*this* process.

Python's ``subprocess``/``Popen.wait()`` only works for the specific PID it
created, and a plain ``os.waitpid(-1, ...)`` from one build would steal the
exit status of another build's children.  Instead one global reaper:

* installs itself as a child subreaper (Linux ``PR_SET_CHILD_SUBREAPER``) so
  grandchildren whose parent exits get reparented to us instead of PID 1;
* receives ``SIGCHLD`` and calls ``os.waitpid(-1, WNOHANG)`` until no more
  zombies are left;
* records every reaped PID's exit information in a shared table.

Build supervisors look exit statuses up there instead of calling waitpid
themselves.
"""
from __future__ import annotations

import os
import signal
import threading
from typing import Optional

PR_SET_CHILD_SUBREAPER = 36


class Reaper:
    def __init__(self) -> None:
        self._statuses: dict[int, int] = {}
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def install(self) -> None:
        """Become a subreaper and start the reaper thread.

        Reaping is poll-driven (WNOHANG) so it also works when signal
        handlers cannot be installed, e.g. when the app's lifespan runs in a
        non-main thread under TestClient.  When we are on the main thread a
        SIGCHLD handler additionally wakes the poller immediately.
        """
        try:
            import ctypes

            libc = ctypes.CDLL("libc.so.6", use_errno=True)
            rc = libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
            if rc != 0:  # pragma: no cover - environment dependent
                errno = ctypes.get_errno()
                raise OSError(errno, os.strerror(errno))
        except (OSError, AttributeError):  # pragma: no cover - non-Linux
            # Without subreaper semantics orphaned grandchildren are reparented
            # to PID 1; the group-based liveness check still handles them.
            pass

        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGCHLD, lambda *_: self._wake.set())
        self._thread = threading.Thread(
            target=self._run, name="child-reaper", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        # Poll at this cadence; SIGCHLD (when available) only shortens it.
        poll = 0.1
        while not self._stop.is_set():
            self._reap_all()
            self._wake.wait(timeout=poll)
            self._wake.clear()

    def _reap_all(self) -> None:
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return  # no children at all
            except InterruptedError:
                continue
            if pid == 0:
                return
            with self._lock:
                self._statuses[pid] = status

    def exit_status(self, pid: int) -> Optional[int]:
        """Return raw wait status for *pid* once reaped, else None."""
        with self._lock:
            return self._statuses.get(pid)

    def shutdown(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        # Final best-effort sweep.
        try:
            self._reap_all()
        except Exception:  # pragma: no cover - defensive
            pass


# Process a raw wait status into a small JSON-friendly description.
def describe_status(raw: int) -> dict:
    if os.WIFEXITED(raw):
        return {"kind": "exit", "code": os.WEXITSTATUS(raw)}
    if os.WIFSIGNALED(raw):
        sig = os.WTERMSIG(raw)
        return {"kind": "signal", "signal": sig, "name": signal.Signals(sig).name}
    return {"kind": "unknown", "raw": raw}  # pragma: no cover
