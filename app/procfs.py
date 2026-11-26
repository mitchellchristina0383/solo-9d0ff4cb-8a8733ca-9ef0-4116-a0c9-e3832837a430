"""Helpers for inspecting process groups via /proc."""
from __future__ import annotations

import os
from typing import Optional


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


def pid_state(pid: int) -> Optional[str]:
    """Return the single-char /proc state of *pid*, or None if it is gone.

    State "Z" marks a zombie: still occupying a PID but already dead, which
    stack sampling treats as an exited member (no signal could reach it).
    """
    try:
        with open(f"/proc/{pid}/stat", "r") as fh:
            stat = fh.read()
    except (FileNotFoundError, ProcessLookupError):
        return None
    right = stat.rfind(")")
    if right == -1:
        return None
    fields = stat[right + 2:].split()
    if not fields:
        return None
    return fields[0]


def pid_alive(pid: int) -> bool:
    """True only for a live, non-zombie process."""
    state = pid_state(pid)
    return state is not None and state != "Z"
