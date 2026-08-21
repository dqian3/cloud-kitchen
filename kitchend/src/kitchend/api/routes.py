"""API routes: jobs, clusters, events stream."""

import asyncio
import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from kitchend.core import adapters, jobs, ledger, submission

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
    project: str
    experiments: list[str] = Field(default_factory=list)
    command: list[str] | None = None
    sweep: dict | None = None      # ad-hoc sweep params → adapter.oneoff()
    extra_flags: list[str] = Field(default_factory=list)
    queue: str | None = None
    cluster: str | None = None     # daemon-managed lease: up before, down after
    cluster_ttl_minutes: int = 60
    after: int | None = None       # wait for this job's retry chain to finish
    run_dir: str | None = None
    resume: bool = False
    priority: int = 0
    max_retries: int = 0
    retry_delay_secs: int = 600


@router.post("/jobs")
def submit_job(body: JobSubmit, request: Request):
    app = request.app
    try:
        project_cfg = app.state.config.project(body.project)
    except KeyError as e:
        raise HTTPException(404, str(e))
    spec = body.model_dump(exclude_none=True)
    try:
        submission.prepare_spec(project_cfg, spec)
    except ValueError as e:
        raise HTTPException(422, str(e))
    job_id = submission.enqueue(app.state.db, app.state.hub,
                                app.state.scheduler, project_cfg, spec)
    return {"id": job_id}


@router.get("/jobs")
def list_jobs(request: Request, state: str | None = None, limit: int = 100):
    return jobs.list_jobs(request.app.state.db, state=state, limit=limit)


@router.get("/jobs/{job_id}")
def get_job(job_id: int, request: Request):
    job = jobs.get(request.app.state.db, job_id)
    if job is None:
        raise HTTPException(404, f"no job {job_id}")
    return job


class JobEdit(BaseModel):
    experiments: list[str] | None = None
    command: list[str] | None = None
    extra_flags: list[str] | None = None
    queue: str | None = None
    cluster: str | None = None
    cluster_ttl_minutes: int | None = None
    after: int | None = None
    run_dir: str | None = None
    resume: bool | None = None
    priority: int | None = None
    max_retries: int | None = None
    retry_delay_secs: int | None = None


@router.patch("/jobs/{job_id}")
def edit_job(job_id: int, body: JobEdit, request: Request):
    """Edit a job while it is still queued (reorder via priority, change the
    spec). Running or finished jobs are immutable — cancel/resubmit instead."""
    app = request.app
    updates = body.model_dump(exclude_none=True)
    if not updates:
        raise HTTPException(422, "no fields to update")
    try:
        job = jobs.update_queued(app.state.db, app.state.hub, job_id, updates)
        # Validate the edited spec still builds a command.
        jobs.build_command(app.state.config.project(job["project"]), job["spec"])
    except KeyError:
        raise HTTPException(404, f"no job {job_id}")
    except ValueError as e:
        raise HTTPException(409, str(e))
    app.state.scheduler.wake()
    return job


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


class Resubmit(BaseModel):
    resume: bool = True


@router.post("/jobs/{job_id}/resubmit")
def resubmit_job(job_id: int, body: Resubmit, request: Request):
    try:
        new_id = request.app.state.scheduler.resubmit(job_id, resume=body.resume)
    except KeyError:
        raise HTTPException(404, f"no job {job_id}")
    return {"id": new_id, "parent": job_id}


# --- clusters ---

class ClusterUp(BaseModel):
    ttl_minutes: int = 120


class ClusterDown(BaseModel):
    force: bool = False


class ClusterExtend(BaseModel):
    lease_id: str
    ttl_minutes: int


@router.get("/clusters")
def list_clusters(request: Request):
    return request.app.state.clusters.snapshot()


@router.post("/clusters/{project}/{name}/up")
async def cluster_up(project: str, name: str, body: ClusterUp, request: Request):
    try:
        lease_id = await request.app.state.clusters.up(
            f"{project}/{name}", body.ttl_minutes)
    except KeyError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(422, str(e))
    return {"lease_id": lease_id, "ttl_minutes": body.ttl_minutes}


@router.post("/clusters/{project}/{name}/down")
async def cluster_down(project: str, name: str, body: ClusterDown, request: Request):
    try:
        await request.app.state.clusters.down(f"{project}/{name}", force=body.force)
    except KeyError as e:
        raise HTTPException(404, str(e))
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return {"ok": True}


@router.post("/clusters/{project}/{name}/extend")
def cluster_extend(project: str, name: str, body: ClusterExtend, request: Request):
    try:
        request.app.state.clusters.extend(
            f"{project}/{name}", body.lease_id, body.ttl_minutes)
    except KeyError as e:
        raise HTTPException(404, str(e))
    return {"ok": True}


@router.post("/clusters/{project}/{name}/refresh")
async def cluster_refresh(project: str, name: str, request: Request):
    try:
        statuses = await request.app.state.clusters.refresh_status(f"{project}/{name}")
    except KeyError as e:
        raise HTTPException(404, str(e))
    return {"vms": statuses}


# --- saved sweeps: one-offs promoted to reusable presets ---

class SweepSave(BaseModel):
    project: str
    name: str
    params: dict


@router.get("/sweeps")
def list_sweeps(project: str, request: Request):
    app = request.app
    try:
        project_cfg = app.state.config.project(project)
    except KeyError as e:
        raise HTTPException(404, str(e))
    project_id = jobs.ensure_project_row(app.state.db, project_cfg)
    return [
        {"id": r["id"], "name": r["name"], "created_at": r["created_at"],
         "params": json.loads(r["params_json"])}
        for r in app.state.db.query(
            "SELECT * FROM saved_sweeps WHERE project_id = ? ORDER BY name",
            (project_id,))
    ]


@router.post("/sweeps")
def save_sweep(body: SweepSave, request: Request):
    """Keep a one-off sweep's params under a name. Validates through the
    adapter now, so a preset that can't build a command is rejected at save
    rather than discovered at submit."""
    app = request.app
    try:
        project_cfg = app.state.config.project(body.project)
        adapters.oneoff_command(project_cfg, body.params)
    except KeyError as e:
        raise HTTPException(404, str(e))
    except ValueError as e:
        raise HTTPException(422, str(e))
    project_id = jobs.ensure_project_row(app.state.db, project_cfg)
    app.state.db.execute(
        "INSERT INTO saved_sweeps (project_id, name, params_json) "
        "VALUES (?, ?, ?) ON CONFLICT(project_id, name) "
        "DO UPDATE SET params_json = excluded.params_json",
        (project_id, body.name, json.dumps(body.params)))
    return {"ok": True}


@router.delete("/sweeps/{sweep_id}")
def delete_sweep(sweep_id: int, request: Request):
    request.app.state.db.execute("DELETE FROM saved_sweeps WHERE id = ?",
                                 (sweep_id,))
    return {"ok": True}


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


class NoteAdd(BaseModel):
    text: str


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
