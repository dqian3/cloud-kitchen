"""API routes: jobs, clusters, events stream."""

import asyncio
import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from kitchend.core import adapters, jobs, ledger, spend, submission

router = APIRouter(prefix="/api")


@router.get("/experiments")
def list_experiments(project: str, request: Request):
    """The project's real experiment catalog, from its kitchen_adapter.py."""
    try:
        project_cfg = request.app.state.config.project(project)
    except KeyError as e:
        raise HTTPException(404, str(e))
    return adapters.catalog(project_cfg)


# --- jobs ---

class JobSubmit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project: str
    name: str | None = None        # what to call it in the queue
    experiments: list[str] = Field(default_factory=list)
    command: list[str] | None = None
    queue: str | None = None
    cluster: str | None = None     # daemon-managed lease: up before, down after
    after: int | None = None       # wait for this job to finish with data
    run_dir: str | None = None
    resume: bool = False
    priority: int = 0
    max_attempts: int = jobs.DEFAULT_MAX_ATTEMPTS
    retry_delay_secs: int = jobs.DEFAULT_RETRY_DELAY_SECS


@router.post("/jobs")
def submit_job(body: JobSubmit, request: Request):
    app = request.app
    try:
        project_cfg = app.state.config.project(body.project)
    except KeyError as e:
        raise HTTPException(404, str(e))
    spec = body.model_dump(exclude_none=True)
    try:
        specs = submission.prepare_specs(project_cfg, spec)
    except ValueError as e:
        raise HTTPException(422, str(e))
    try:
        ids = submission.enqueue_all(app.state.db, app.state.hub,
                                     app.state.scheduler, project_cfg, specs)
    except ValueError as e:
        raise HTTPException(422, str(e))
    return {"id": ids[0], "ids": ids}


@router.get("/jobs")
def list_jobs(request: Request, state: str | None = None, limit: int = 100):
    out = jobs.list_jobs(request.app.state.db, state=state, limit=limit)
    sched = request.app.state.scheduler
    for job in out:
        job["retry_in_s"] = sched.wait_seconds(job["id"])
        job["bringing_up"] = sched.bringing_up(job["id"])
        job["display_state"] = sched.display_state(job)
    return out


@router.get("/jobs/{job_id}")
def get_job(job_id: int, request: Request):
    job = jobs.get(request.app.state.db, job_id)
    if job is None:
        raise HTTPException(404, f"no job {job_id}")
    job["attempts_log"] = jobs.attempts(request.app.state.db, job_id)
    job["retry_in_s"] = request.app.state.scheduler.wait_seconds(job_id)
    job["bringing_up"] = request.app.state.scheduler.bringing_up(job_id)
    job["display_state"] = request.app.state.scheduler.display_state(job)
    return job


class Purge(BaseModel):
    outcomes: list[str] = list(jobs.PURGEABLE_OUTCOMES)
    project: str | None = None


@router.post("/jobs/purge")
def purge_jobs(body: Purge, request: Request):
    """Delete done jobs that produced nothing worth keeping: their rows,
    attempts, and driver logs. Never their files — measurement data is
    deleted through the ledger, one run at a time, on purpose."""
    app = request.app
    bad = [o for o in body.outcomes if o not in jobs.PURGEABLE_OUTCOMES]
    if bad:
        raise HTTPException(422, f"not purgeable: {bad}")
    project_id = None
    if body.project:
        try:
            project_id = jobs.ensure_project_row(
                app.state.db, app.state.config.project(body.project))
        except KeyError as e:
            raise HTTPException(404, str(e))
    ids = jobs.purge(app.state.db, app.state.hub, body.outcomes, project_id)
    for jid in ids:
        (app.state.runner.jobs_dir / f"{jid}.log").unlink(missing_ok=True)
    return {"purged": ids}


class Reorder(BaseModel):
    ids: list[int]


@router.post("/jobs/reorder")
def reorder_jobs(body: Reorder, request: Request):
    """Queued jobs dispatch in the given order (first = next). The running
    job is not part of the queue and can't be moved."""
    app = request.app
    try:
        jobs.reorder(app.state.db, app.state.hub, body.ids)
    except KeyError as e:
        raise HTTPException(404, f"no job {e}")
    except ValueError as e:
        raise HTTPException(409, str(e))
    app.state.scheduler.wake()
    return {"ok": True}


@router.get("/jobs/{job_id}/log")
def job_log(job_id: int, request: Request, tail: int = 200):
    return {"log": request.app.state.runner.tail_log(job_id, lines=tail)}


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: int, request: Request):
    try:
        state = await request.app.state.scheduler.cancel(job_id)
    except KeyError:
        raise HTTPException(404, f"no job {job_id}")
    return {"id": job_id, "state": state}


@router.delete("/jobs/{job_id}")
def delete_job(job_id: int, request: Request):
    """Delete a done job and its driver log. Its run directory stays: a
    directory holds measurements, and those are deleted through the ledger
    (DELETE /runs/{id}?delete_files=true), where what you name is the data."""
    app = request.app
    try:
        jobs.delete(app.state.db, app.state.hub, job_id)
    except KeyError:
        raise HTTPException(404, f"no job {job_id}")
    except ValueError as e:
        raise HTTPException(409, str(e))
    (app.state.runner.jobs_dir / f"{job_id}.log").unlink(missing_ok=True)
    return {"ok": True}


@router.post("/jobs/{job_id}/retry")
def retry_job(job_id: int, request: Request):
    """Try a waiting job again now: drop its delay and any cluster cooldown
    holding it. For when the reason it was waiting has gone away."""
    try:
        return request.app.state.scheduler.retry_now(job_id)
    except KeyError:
        raise HTTPException(404, f"no job {job_id}")
    except ValueError as e:
        raise HTTPException(409, str(e))


class Paused(BaseModel):
    paused: bool


@router.post("/pause")
def set_paused(body: Paused, request: Request):
    """Stop starting new jobs, without stopping the daemon. Anything already
    running is left alone; clusters, the ledger and the UI keep working."""
    request.app.state.scheduler.set_paused(body.paused)
    return {"paused": request.app.state.scheduler.paused()}


class Resubmit(BaseModel):
    resume: bool = True


@router.post("/jobs/{job_id}/resubmit")
def resubmit_job(job_id: int, body: Resubmit, request: Request):
    try:
        new_id = request.app.state.scheduler.resubmit(job_id, resume=body.resume)
    except KeyError:
        raise HTTPException(404, f"no job {job_id}")
    except ValueError as e:
        raise HTTPException(409, str(e))
    return {"id": new_id, "parent": job_id}


# --- clusters ---

class ClusterUp(BaseModel):
    ttl_minutes: int = Field(120, ge=1)


class ClusterDown(BaseModel):
    force: bool = False


@router.get("/spend")
def get_spend(request: Request, days: int = 7):
    """Cluster time and cost from the event log, probes apart from runs.

    GCP billing is not readable from here and keeps only each VM's latest
    start/stop, so this is the only record of what the queue has spent."""
    since = None
    if days > 0:
        since = request.app.state.db.query_one(
            "SELECT datetime('now', ?) AS t", (f"-{int(days)} days",))["t"]
    return spend.report(request.app.state.db, since=since)


@router.get("/clusters")
def list_clusters(request: Request):
    return request.app.state.clusters.snapshot()


@router.post("/clusters/{project}/{name}/up")
async def cluster_up(project: str, name: str, body: ClusterUp, request: Request):
    try:
        await request.app.state.clusters.up(
            f"{project}/{name}", ttl_minutes=body.ttl_minutes)
    except KeyError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(422, str(e))
    except RuntimeError as e:
        raise HTTPException(502, str(e))
    return {"ok": True}


@router.post("/clusters/{project}/{name}/down")
async def cluster_down(project: str, name: str, body: ClusterDown, request: Request):
    try:
        await request.app.state.clusters.down(f"{project}/{name}", force=body.force)
    except KeyError as e:
        raise HTTPException(404, str(e))
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}


@router.post("/clusters/{project}/{name}/refresh")
async def cluster_refresh(project: str, name: str, request: Request):
    try:
        statuses = await request.app.state.clusters.refresh_status(f"{project}/{name}")
    except KeyError as e:
        raise HTTPException(404, str(e))
    return {"vms": statuses}


# --- the daemon's own log: every state change, from the events table ---

@router.get("/logs")
def daemon_log(request: Request, limit: int = 200):
    """Newest daemon events (jobs and clusters), oldest first
    within the window — the audit trail behind the SSE stream."""
    rows = request.app.state.db.query(
        "SELECT id, ts, job_id, cluster_id, type, payload_json FROM events "
        "ORDER BY id DESC LIMIT ?", (min(int(limit), 1000),))
    out = []
    for r in reversed(rows):
        out.append({"id": r["id"], "ts": r["ts"], "type": r["type"],
                    "job_id": r["job_id"], "cluster_id": r["cluster_id"],
                    **json.loads(r["payload_json"])})
    return out


# --- run ledger ---

@router.get("/runs")
def list_runs(request: Request, project: str | None = None,
              experiment: str | None = None, tag: str | None = None,
              limit: int = 100):
    return ledger.list_runs(request.app.state.db, project=project,
                            experiment=experiment, tag=tag, limit=limit)


@router.get("/runs/{run_id}")
def get_run(run_id: int, request: Request):
    run = ledger.get_run(request.app.state.db, run_id)
    if run is None:
        raise HTTPException(404, f"no run {run_id}")
    return run


class TrialsAdd(BaseModel):
    trials: int = Field(ge=1)


@router.post("/runs/{run_id}/trials")
def add_trials(run_id: int, body: TrialsAdd, request: Request):
    """Append new numbered trials to a result using its source job."""
    try:
        return request.app.state.scheduler.add_trials(run_id, body.trials)
    except KeyError:
        raise HTTPException(404, f"no run {run_id}")
    except ValueError as e:
        raise HTTPException(409, str(e))


@router.post("/runs/{run_id}/points/{point_id}/retry")
def retry_point(run_id: int, point_id: int, request: Request):
    """Rerun and overwrite one exact dimension/rate/trial identity."""
    try:
        return request.app.state.scheduler.retry_point(run_id, point_id)
    except KeyError:
        raise HTTPException(404, f"no run {run_id}")
    except ValueError as e:
        raise HTTPException(409, str(e))


class NoteAdd(BaseModel):
    text: str


def _remove_run_dir(app, project_name, run_dir) -> str | None:
    """Delete a run directory if it is inside the project's runs roots.
    Returns the path removed, or None when there was nothing safe to
    remove. Raises HTTPException when the path exists but is out of
    bounds — silently keeping data the caller asked to delete would be
    worse than refusing."""
    import shutil
    if not run_dir:
        return None
    try:
        project_cfg = app.state.config.project(project_name)
    except KeyError as e:
        raise HTTPException(404, str(e))
    path = ledger.deletable_dir(project_cfg, run_dir)
    if path is None:
        if Path(run_dir).exists():
            raise HTTPException(
                409, f"{run_dir} is not inside {project_name}'s runs roots "
                     f"{list(project_cfg.runs_roots)}; delete it by hand")
        return None
    shutil.rmtree(path)
    return str(path)


@router.delete("/runs/{run_id}")
def delete_run(run_id: int, request: Request, delete_files: bool = False):
    """Remove a run from the ledger (points, tags, notes). With
    delete_files, its directory under the project's runs roots goes too —
    that is the measurement data, and it is not recoverable."""
    app = request.app
    run = ledger.get_run(app.state.db, run_id)
    if run is None:
        raise HTTPException(404, f"no run {run_id}")
    removed = (_remove_run_dir(app, run["project"], run["run_dir"])
               if delete_files else None)
    ledger.delete_run(app.state.db, run_id)
    app.state.hub.emit("run.deleted", run_id=run_id, removed_dir=removed)
    return {"ok": True, "removed_dir": removed}


@router.post("/runs/{run_id}/notes")
def add_note(run_id: int, body: NoteAdd, request: Request):
    if ledger.get_run(request.app.state.db, run_id) is None:
        raise HTTPException(404, f"no run {run_id}")
    note_id = ledger.add_note(request.app.state.db, run_id, body.text)
    return {"id": note_id}


class TagAdd(BaseModel):
    name: str


@router.post("/runs/{run_id}/tags")
def add_tag(run_id: int, body: TagAdd, request: Request):
    if ledger.get_run(request.app.state.db, run_id) is None:
        raise HTTPException(404, f"no run {run_id}")
    ledger.add_tag(request.app.state.db, run_id, body.name)
    return {"ok": True}


@router.delete("/runs/{run_id}/tags/{name}")
def remove_tag(run_id: int, name: str, request: Request):
    ledger.remove_tag(request.app.state.db, run_id, name)
    return {"ok": True}


class ScanRequest(BaseModel):
    project: str


@router.post("/runs/scan")
def scan_runs(body: ScanRequest, request: Request):
    """Backfill: index pre-existing run dirs under the project's runs roots."""
    try:
        project_cfg = request.app.state.config.project(body.project)
    except KeyError as e:
        raise HTTPException(404, str(e))
    return ledger.scan_project(request.app.state.db, project_cfg)


# --- SSE stream ---

@router.get("/stream")
async def stream(request: Request):
    hub = request.app.state.hub

    async def gen():
        last_id = request.headers.get("Last-Event-ID")
        if last_id:
            for ev in hub.replay_since(int(last_id)):
                yield f"id: {ev['id']}\ndata: {json.dumps(ev)}\n\n"
        q = hub.subscribe()
        try:
            while True:
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"id: {ev['id']}\ndata: {json.dumps(ev)}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            hub.unsubscribe(q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})
