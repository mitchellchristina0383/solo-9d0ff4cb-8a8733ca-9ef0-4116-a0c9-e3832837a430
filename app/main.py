"""FastAPI application: start/cancel sample builds, inspect state and logs."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .build import (
    CANCELLED,
    TERMINAL_STATES,
    Build,
    BuildManager,
)
from .reaper import Reaper

logger = logging.getLogger("buildsvc")

WORKER_PATH = Path(__file__).resolve().parent.parent / "samples" / "worker.py"
DEFAULT_GRACE = 5.0
MAX_GRACE = 60.0

# Scenario flags accepted from the API -> worker CLI flags.
SCENARIO_FLAGS = {
    "main-exits-early": "--main-exits-early",
    "ignore-term": "--ignore-term",
    "noisy-teardown": "--noisy-teardown",
}


class StartRequest(BaseModel):
    scenarios: list[str] = Field(
        default_factory=list,
        description="Subset of: main-exits-early, ignore-term, noisy-teardown",
    )
    duration: float = Field(default=60.0, ge=0.1, le=3600.0)
    grace_seconds: float = Field(default=DEFAULT_GRACE, ge=0.0, le=MAX_GRACE)
    build_id: Optional[str] = Field(default=None, max_length=64)


class CancelRequest(BaseModel):
    grace_seconds: Optional[float] = Field(default=None, ge=0.0, le=MAX_GRACE)


def _build_command(req: StartRequest) -> list[str]:
    unknown = [s for s in req.scenarios if s not in SCENARIO_FLAGS]
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"unknown scenarios {unknown}; valid: {sorted(SCENARIO_FLAGS)}",
        )
    cmd = [SCENARIO_FLAGS[s] for s in req.scenarios]
    cmd += ["--duration", str(req.duration)]
    return cmd


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        reaper = Reaper()
        reaper.install()
        app.state.reaper = reaper
        app.state.manager = BuildManager(reaper=reaper, worker_path=WORKER_PATH)
        logger.info("build service started (worker=%s)", WORKER_PATH)
        try:
            yield
        finally:
            app.state.manager.shutdown_all()
            reaper.shutdown()

    app = FastAPI(
        title="Build Cancellation Service",
        version="1.0.0",
        description=(
            "Single-instance service that runs sample builds as process groups. "
            "Cancel notifies the group (SIGTERM), waits a grace period, then "
            "SIGKILLs survivors. State is only reported as cancelled once the "
            "whole group exited and both log streams reached EOF."
        ),
        lifespan=lifespan,
    )

    def manager() -> BuildManager:
        return app.state.manager

    def get_build_or_404(build_id: str) -> Build:
        build = manager().get(build_id)
        if build is None:
            raise HTTPException(status_code=404, detail=f"build {build_id!r} not found")
        return build

    @app.get("/health")
    def health() -> dict:
        builds = manager().list_builds()
        return {
            "status": "ok",
            "builds_total": len(builds),
            "builds_active": sum(1 for b in builds if b.state not in TERMINAL_STATES),
        }

    @app.post("/builds", status_code=201)
    def start_build(req: StartRequest) -> dict:
        try:
            scenario_cmd = _build_command(req)
            build = manager().create(
                scenario=scenario_cmd,
                grace_default=req.grace_seconds,
                name=req.build_id,
            )
        except KeyError:
            raise HTTPException(status_code=409, detail="build_id already exists")
        return build.snapshot()

    @app.get("/builds")
    def list_builds() -> dict:
        return {"builds": [b.snapshot() for b in manager().list_builds()]}

    @app.get("/builds/{build_id}")
    def get_build(build_id: str) -> dict:
        return get_build_or_404(build_id).snapshot()

    @app.post("/builds/{build_id}/cancel")
    def cancel_build(build_id: str, req: CancelRequest | None = None) -> dict:
        build = get_build_or_404(build_id)
        grace = req.grace_seconds if req is not None else None
        first_request = not build.cancellation_requested
        info = build.request_cancel(grace_seconds=grace)
        return {
            "build_id": build_id,
            "state": build.state,
            "reused_existing_cancellation": not first_request,
            "cancellation": {
                "requested_at": info.requested_at,
                "notified_at": info.notified_at,
                "grace_seconds": info.grace_seconds,
                "force_killed_at": info.force_killed_at,
                "forced": info.forced,
                "note": info.note,
            },
        }

    @app.get("/builds/{build_id}/logs")
    def get_logs(build_id: str, stream: str = "all") -> dict:
        if stream not in ("all", "stdout", "stderr"):
            raise HTTPException(
                status_code=422, detail="stream must be one of all, stdout, stderr"
            )
        build = get_build_or_404(build_id)
        out_bytes, out_total = build.stdout.snapshot()
        err_bytes, err_total = build.stderr.snapshot()

        def entry(buf_bytes: bytes, total: int) -> dict:
            return {
                "text": buf_bytes.decode("utf-8", errors="replace"),
                "retained_bytes": len(buf_bytes),
                "total_bytes": total,
                "truncated": total > len(buf_bytes),
                "limit_bytes": build.stdout.max_bytes,
            }

        resp: dict = {"build_id": build_id, "state": build.state}
        if stream in ("all", "stdout"):
            resp["stdout"] = entry(out_bytes, out_total)
        if stream in ("all", "stderr"):
            resp["stderr"] = entry(err_bytes, err_total)
        return resp

    @app.post("/builds/{build_id}/wait")
    def wait_build(build_id: str, timeout: float = 30.0) -> dict:
        build = get_build_or_404(build_id)
        finished = build.wait_done(timeout=min(max(timeout, 0.0), 300.0))
        snap = build.snapshot()
        snap["finished_within_timeout"] = finished
        return snap

    return app


app = create_app()
