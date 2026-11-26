"""Helpers for inspecting process groups via /proc."""
from __future__ import annotations

import os


def group_pids(pgid: int) -> list[int]:
    """Return PIDs currently in process group *pgid*.

    Zombies are included: they still occupy a PID until their parent reaps
    them, so they count as "not fully gone" for shutdown accounting.
    """
    pids: list[int] = []
    try:
        entries = os.listdir("/proc")
    except FileNotFoundError:  # pragma: no cover - non-Linux fallback
        return pids
    for name in entries:
        if not name.isdigit():
            continue
        pid = int(name)
        try:
            with open(f"/proc/{pid}/stat", "r") as fh:
                stat = fh.read()
        except (FileNotFoundError, ProcessLookupError):
            continue
        # /proc/<pid>/stat: pid (comm) state ppid pgrp ...
        # The comm field may contain spaces/parens, so split around it.
        right = stat.rfind(")")
        if right == -1:
            continue
        fields = stat[right + 2:].split()
        # fields[0] = state, fields[1] = ppid, fields[2] = pgrp
        if len(fields) < 3:
            continue
        try:
            if int(fields[2]) == pgid:
                pids.append(pid)
        except ValueError:
            continue
    return pids


def group_alive(pgid: int) -> bool:
    return bool(group_pids(pgid))
