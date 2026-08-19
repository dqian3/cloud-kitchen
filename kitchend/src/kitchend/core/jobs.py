"""Job store: rows in the jobs table plus command construction.

A job spec (stored verbatim as spec_json):

    {
      "project":     "aspen-bft",
      "experiments": ["aspen", "flutter"],     # driver args, or
      "command":     ["python3", "x.py"],      # explicit command (overrides driver)
      "extra_flags": ["--trials", "3"],
      "queue":       "main",                   # serialization key; default = project
      "run_dir":     null,                     # pinned on retry/resume
      "resume":      false,
      "priority":    0,
      "max_retries": 2,
      "retry_delay_secs": 600
    }

Exit-code contract (from run_experiment.py): 0 clean → succeeded; 2 degraded
but data written → degraded, never auto-retried; anything else → failed, with
bounded resume-retries.
"""

import json

QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
DEGRADED = "degraded"
FAILED = "failed"
CANCELED = "canceled"
INTERRUPTED = "interrupted"

ACTIVE_STATES = (QUEUED, RUNNING)


def ensure_project_row(db, project_cfg):
    row = db.query_one("SELECT id FROM projects WHERE name = ?", (project_cfg.name,))
    if row:
        return row["id"]
    return db.insert(
        "INSERT INTO projects (name, repo_path, adapter_path, runs_roots) "
        "VALUES (?, ?, ?, ?)",
        (project_cfg.name, str(project_cfg.repo_path),
         str(project_cfg.adapter_path) if project_cfg.adapter_path else None,
         json.dumps(list(project_cfg.runs_roots))),
    )


def submit(db, project_id, spec: dict) -> int:
    return db.insert(
        "INSERT INTO jobs (project_id, spec_json, run_dir, state, priority, retries_left) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (project_id, json.dumps(spec), spec.get("run_dir"), QUEUED,
         int(spec.get("priority", 0)), int(spec.get("max_retries", 0))),
    )


def get(db, job_id):
    row = db.query_one(
        "SELECT j.*, p.name AS project FROM jobs j "
        "JOIN projects p ON p.id = j.project_id WHERE j.id = ?", (job_id,))
    return _to_dict(row) if row else None


def list_jobs(db, state=None, limit=100):
    if state:
        rows = db.query(
            "SELECT j.*, p.name AS project FROM jobs j "
            "JOIN projects p ON p.id = j.project_id "
            "WHERE j.state = ? ORDER BY j.id DESC LIMIT ?", (state, limit))
    else:
        rows = db.query(
            "SELECT j.*, p.name AS project FROM jobs j "
            "JOIN projects p ON p.id = j.project_id "
            "ORDER BY j.id DESC LIMIT ?", (limit,))
    return [_to_dict(r) for r in rows]


def _to_dict(row):
    d = dict(row)
    d["spec"] = json.loads(d.pop("spec_json"))
    d.pop("estimate_json", None)
    return d


def queue_key(job: dict) -> str:
    return job["spec"].get("queue") or job["spec"].get("cluster") or job["project"]


def set_state(db, hub, job_id, state, **fields):
    sets = ["state = ?"]
    params = [state]
    for col, val in fields.items():
        sets.append(f"{col} = ?")
        params.append(val)
    params.append(job_id)
    db.execute(f"UPDATE jobs SET {', '.join(sets)} WHERE id = ?", params)
    hub.emit("job.state", job_id=job_id, state=state, **{
        k: v for k, v in fields.items() if k in ("exit_code", "pid", "run_dir")
    })


def update_queued(db, hub, job_id, updates: dict):
    """Edit a job that is still queued: spec fields and/or priority.

    The check-and-update runs under the DB lock via a conditional UPDATE, so
    a dispatch racing this edit either sees the old spec (job already
    running — the edit is refused) or the new one.
    """
    job = get(db, job_id)
    if job is None:
        raise KeyError(job_id)
    if job["state"] != QUEUED:
        raise ValueError(f"job {job_id} is {job['state']}, not queued")
    spec = dict(job["spec"])
    priority = updates.pop("priority", None)
    spec.update(updates)
    params = [json.dumps(spec), spec.get("run_dir"),
              int(spec.get("max_retries", 0))]
    sets = "spec_json = ?, run_dir = ?, retries_left = ?"
    if priority is not None:
        sets += ", priority = ?"
        params.append(int(priority))
        spec["priority"] = int(priority)
    params.append(job_id)
    cur = db.execute(
        f"UPDATE jobs SET {sets} WHERE id = ? AND state = 'queued'", params)
    if cur.rowcount == 0:
        raise ValueError(f"job {job_id} started while editing; not modified")
    hub.emit("job.edited", job_id=job_id, state=QUEUED)
    return get(db, job_id)


def build_command(project_cfg, spec: dict):
    """(argv, cwd) for a job. Explicit `command` wins; otherwise the project's
    driver + experiments + extra flags, with run-dir/resume flags appended."""
    if spec.get("command"):
        argv = list(spec["command"])
    else:
        if not project_cfg.driver:
            raise ValueError(
                f"project '{project_cfg.name}' has no driver configured and "
                "the job spec has no explicit command")
        argv = list(project_cfg.driver) + list(spec.get("experiments", []))
    argv += [str(f) for f in spec.get("extra_flags", [])]
    if spec.get("run_dir"):
        argv += [project_cfg.output_dir_flag, str(spec["run_dir"])]
    if spec.get("resume"):
        argv.append(project_cfg.resume_flag)
    cwd = project_cfg.repo_path / project_cfg.driver_cwd
    return argv, cwd


def recover_orphans(db, hub):
    """Mark jobs that were 'running' under a dead daemon as interrupted."""
    import os
    for row in db.query("SELECT id, pid FROM jobs WHERE state = ?", (RUNNING,)):
        pid = row["pid"]
        alive = False
        if pid:
            try:
                os.kill(pid, 0)
                alive = True
            except (ProcessLookupError, PermissionError):
                alive = False
        if not alive:
            set_state(db, hub, row["id"], INTERRUPTED,
                      finished_at=_now(db))


def _now(db):
    return db.query_one("SELECT datetime('now') AS t")["t"]
