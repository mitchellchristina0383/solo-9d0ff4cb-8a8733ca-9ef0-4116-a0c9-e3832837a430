"""In-process tail buffer: keeps at most *max_bytes* of the latest bytes.

Stdout and stderr of every build are captured through separate TailBuffer
instances so the two streams are never interleaved.
"""
from __future__ import annotations

import threading


class TailBuffer:
    def __init__(self, max_bytes: int = 32 * 1024) -> None:
        self._max_bytes = max_bytes
        self._chunks: list[bytes] = []
        self._size = 0
        self._total = 0  # total bytes ever appended (pre-truncation counter)
        self._lock = threading.Lock()

    def append(self, data: bytes) -> None:
        if not data:
            return
        with self._lock:
            self._chunks.append(data)
            self._size += len(data)
            self._total += len(data)
            self._trim_locked()

    def _trim_locked(self) -> None:
        while self._size > self._max_bytes and len(self._chunks) > 1:
            oldest = self._chunks[0]
            overflow = self._size - self._max_bytes
            if len(oldest) <= overflow:
                self._chunks.pop(0)
                self._size -= len(oldest)
            else:
                # Keep the tail of the oversized first chunk.
                self._chunks[0] = oldest[overflow:]
                self._size -= overflow
        if self._chunks and self._size > self._max_bytes:
            # A single chunk larger than the limit: keep only its tail.
            only = self._chunks[0]
            self._chunks = [only[-self._max_bytes:]]
            self._size = len(self._chunks[0])

    def snapshot(self) -> tuple[bytes, int]:
        """Return (retained tail bytes, total bytes ever written)."""
        with self._lock:
            return b"".join(self._chunks), self._total

    @property
    def max_bytes(self) -> int:
        return self._max_bytes
