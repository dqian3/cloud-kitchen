"""Job store: rows in the jobs table plus command construction.

One row per job, for its whole life. A job is `waiting`, `running`, or
`done`; only a done job has an `outcome`. Retrying does not make a new row —
it raises `attempts` and puts the job back to waiting — so the queue shows
one line per thing you asked for, however many times the daemon had to try.
Each driver invocation is recorded in job_attempts, which is where exit
codes and errors live.

A job spec (stored verbatim as spec_json):

    {
      "project":     "aspen-bft",
      "experiments": ["aspen", "flutter"],     # driver args, or
      "command":     ["python3", "x.py"],      # canonical argv after submission
      "queue":       "main",                   # display grouping; default = project
      "run_dir":     null,                     # assigned at first spawn
      "resume":      false,
      "priority":    0,
      "max_attempts": 20,
      "retry_delay_secs": 600
    }

Exit-code contract (from the drivers): 0 clean → ok; 2 degraded but data
written → degraded, never retried; anything else → another attempt, resuming
into the same run dir, until max_attempts.
"""

import json
from pathlib import Path

# Attempts are cheap next to a lost measurement: a failed run resumes into
# its directory, and a cluster that would not come up (a zone stockout) is
# probed again after a cooldown rather than given up on. Ten minutes avoids
# turning a regional stockout into enough create calls to trigger API limits.
DEFAULT_MAX_ATTEMPTS = 20
DEFAULT_RETRY_DELAY_SECS = 600

WAITING = "waiting"         # in the queue: waiting its turn, or for a cluster
RUNNING = "running"         # a driver process is alive
DONE = "done"

OK = "ok"
DEGRADED = "degraded"       # exit 2: data written, not retried
FAILED = "failed"           # attempts exhausted, or a non-retryable error
CANCELED = "canceled"

# Outcomes that produced no data worth keeping, so purge may drop the row.
PURGEABLE_OUTCOMES = (FAILED, CANCELED)


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
        "INSERT INTO jobs (project_id, spec_json, run_dir, state, priority, "
        "max_attempts) VALUES (?, ?, ?, ?, ?, ?)",
        (project_id, json.dumps(spec), spec.get("run_dir"), WAITING,
         int(spec.get("priority", 0)),
         int(spec.get("max_attempts", DEFAULT_MAX_ATTEMPTS))),
    )


def get(db, job_id):
    row = db.query_one(
        "SELECT j.*, p.name AS project FROM jobs j "
        "JOIN projects p ON p.id = j.project_id WHERE j.id = ?", (job_id,))
    return _with_queue(db, _to_dict(row)) if row else None


def _with_queue(db, job):
    """Attach the job's queue, so a caller sees how it serializes without
    knowing how queue keys are derived."""
    job["queue"] = queue_key(job)
    return job


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
    return [_with_queue(db, _to_dict(r)) for r in rows]


def attempts(db, job_id):
    """This job's driver invocations, oldest first."""
    return [dict(r) for r in db.query(
        "SELECT n, started_at, finished_at, exit_code, points, error "
        "FROM job_attempts WHERE job_id = ? ORDER BY n", (job_id,))]


def _to_dict(row):
    d = dict(row)
    d["spec"] = json.loads(d.pop("spec_json"))
    raw = d.pop("progress_json", None)
    d["progress"] = json.loads(raw) if raw else None
    d["done"] = d["state"] == DONE
    # One source of truth for clients: explicit resubmits resume immediately;
    # retries resume after their first attempt even though the original
    # submission spec remains unchanged.
    d["will_resume"] = bool(
        d.get("run_dir")
        and (d["spec"].get("resume") or d.get("attempts", 0) > 0))
    return d


def default_run_dir(project_cfg, job_id):
    """Output dir for a job whose spec didn't pin one: one directory per job.

    Assigned by the daemon rather than invented by the driver, so ingest
    knows where events.jsonl will appear; every attempt of this job resumes
    into the same directory.
    """
    import datetime
    if project_cfg.runs_roots:
        root = project_cfg.repo_path / project_cfg.runs_roots[0]
    else:
        root = project_cfg.repo_path / "runs"
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    return root / f"kitchen-job{job_id}-{stamp}"


def label(spec: dict, project_cfg=None) -> str:
    """What to call this job in a queue: the name it was submitted under, or
    the experiments it runs, or the name its driver was given. A raw argv is
    the last resort, and never the whole of it."""
    if spec.get("name"):
        return spec["name"]
    if spec.get("experiments"):
        return " ".join(spec["experiments"])
    argv = [str(a) for a in spec.get("command") or ()]
    flag = getattr(project_cfg, "name_flag", "--name")
    if flag and flag in argv:
        i = argv.index(flag)
        if i + 1 < len(argv):
            return argv[i + 1]
    script = next((a for a in argv if a.endswith(".py")), None)
    return script or (argv[0] if argv else "job")


def queue_key(job: dict) -> str:
    """What to group this job under in the UI.

    A label, nothing more. Dispatch is one global queue in priority order and
    never consults this.
    """
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
        k: v for k, v in fields.items()
        if k in ("outcome", "pid", "run_dir", "last_error")
    })


def finish(db, hub, job_id, outcome, last_error=None):
    """End a job: done, with the outcome that stuck."""
    set_state(db, hub, job_id, DONE, outcome=outcome, finished_at=_now(db),
              last_error=last_error)


def wait_again(db, hub, job_id, last_error=None):
    """Put a job back in the queue. The wait itself is the scheduler's, held
    per cluster in memory: only the job at the head runs, so the delay is
    about the cluster it could not have, and a restart costs one probe
    rather than a table to keep in step."""
    set_state(db, hub, job_id, WAITING, last_error=last_error)


def start_attempt(db, hub, job_id) -> int:
    """Record a driver invocation about to start; returns its number."""
    n = db.query_one("SELECT attempts + 1 AS n FROM jobs WHERE id = ?",
                     (job_id,))["n"]
    db.execute("UPDATE jobs SET attempts = ?, started_at = "
               "COALESCE(started_at, datetime('now')) WHERE id = ?", (n, job_id))
    db.insert("INSERT INTO job_attempts (job_id, n) VALUES (?, ?)", (job_id, n))
    hub.emit("job.attempt", job_id=job_id, n=n)
    return n


def end_attempt(db, job_id, exit_code=None, error=None, points=None) -> None:
    """Close the open attempt. `points` is what it finished, which is how
    the scheduler tells a flaky run from a broken environment."""
    db.execute(
        "UPDATE job_attempts SET finished_at = datetime('now'), "
        "exit_code = ?, error = ?, points = ? "
        "WHERE job_id = ? AND finished_at IS NULL",
        (exit_code, error, points, job_id))


def purge(db, hub, outcomes=PURGEABLE_OUTCOMES, project_id=None) -> list[int]:
    """Delete done jobs whose outcome produced nothing worth keeping. Rows
    only: measurement data is deleted through the ledger, never here."""
    sql = ("SELECT id FROM jobs WHERE state = 'done' AND outcome IN "
           f"({','.join('?' * len(outcomes))})")
    params = list(outcomes)
    if project_id is not None:
        sql += " AND project_id = ?"
        params.append(project_id)
    ids = [r["id"] for r in db.query(sql, params)]
    for job_id in ids:
        _detach_and_delete(db, job_id)
    if ids:
        hub.emit("jobs.purged", ids=ids, outcomes=list(outcomes))
    return ids


def delete(db, hub, job_id) -> None:
    """Delete one done job. An unfinished job must be canceled first."""
    job = get(db, job_id)
    if job is None:
        raise KeyError(job_id)
    if job["state"] != DONE:
        raise ValueError(f"job {job_id} is {job['state']}; cancel it first")
    _detach_and_delete(db, job_id)
    # The id goes in the payload, not the job_id column: that column is a
    # foreign key into the row we just deleted.
    hub.emit("job.deleted", deleted_job_id=job_id)


def _detach_and_delete(db, job_id) -> None:
    """Drop a job row and what points at it. The audit trail and ledger runs
    keep their history, with no job to point at."""
    # Keep the id in the payload: the row goes, but "which job spent this
    # cluster time" is exactly what the audit log is for.
    db.execute("UPDATE events SET job_id = NULL, payload_json = "
               "json_set(payload_json, '$.job_id', ?) WHERE job_id = ?",
               (job_id, job_id))
    db.execute("UPDATE runs SET job_id = NULL WHERE job_id = ?", (job_id,))
    db.execute("DELETE FROM job_attempts WHERE job_id = ?", (job_id,))
    db.execute("DELETE FROM jobs WHERE id = ?", (job_id,))


def _rank(db, ids: list[int]) -> None:
    """Write `ids` back as priorities N..1, so the list is the dispatch order.

    Priority is a dense rank, not a class: the only thing that reads it is
    `ORDER BY priority DESC, id ASC`, so any change of order is a renumber.
    """
    n = len(ids)
    for i, job_id in enumerate(ids):
        db.execute("UPDATE jobs SET priority = ? WHERE id = ? AND state = 'waiting'",
                   (n - i, job_id))


def waiting_order(db) -> list[int]:
    """Waiting job ids in dispatch order."""
    return [r["id"] for r in db.query(
        "SELECT id FROM jobs WHERE state = ? ORDER BY priority DESC, id ASC",
        (WAITING,))]


def reorder(db, hub, ids: list[int]) -> None:
    """Make the waiting jobs in `ids` dispatch in that order. Jobs that are
    running or done are refused."""
    for job_id in ids:
        job = get(db, job_id)
        if job is None:
            raise KeyError(job_id)
        if job["state"] != WAITING:
            raise ValueError(f"job {job_id} is {job['state']}, not waiting")
    _rank(db, ids)
    hub.emit("job.reordered", ids=list(ids))


def place(db, hub, job_id: int, after: int | None = None) -> int | None:
    """Position a freshly queued job, returning the job it now sits behind.

    `after` names the job to queue behind. Without it the job falls in behind
    the last one already queued for its own cluster, which is where the next
    piece of that cluster's work belongs — appending to the very end instead
    would put it behind every other cluster's queue, and a bare submit would
    mean something different before and after someone reorders (a new job
    defaults to priority 0, which sinks below a reordered set).
    """
    order = waiting_order(db)
    if job_id not in order:
        return None                      # already dispatched, or gone
    order.remove(job_id)
    if after is not None:
        if after not in order:
            raise ValueError(
                f"cannot queue after job {after}: it is not waiting")
        index = order.index(after) + 1
    else:
        after, index = None, len(order)
        cluster = (get(db, job_id) or {}).get("spec", {}).get("cluster")
        if cluster is not None:
            for i, other_id in enumerate(order):
                other = get(db, other_id)
                if other and other["spec"].get("cluster") == cluster:
                    after, index = other_id, i + 1
    order.insert(index, job_id)
    _rank(db, order)
    hub.emit("job.placed", job_id=job_id, after=after)
    return after


def build_command(project_cfg, spec: dict):
    """(argv, cwd) for a job, with runtime-owned output/resume flags.

    New submissions always carry one canonical `command`. The driver,
    driver_args, and extra_flags branches remain for jobs queued by older
    daemon versions so a restart does not invalidate existing work.
    """
    if spec.get("command"):
        argv = list(spec["command"])
    else:
        if not project_cfg.driver:
            raise ValueError(
                f"project '{project_cfg.name}' has no driver configured and "
                "the job spec has no explicit command")
        # driver_args (resolved from the experiment catalog at submit time)
        # wins over raw experiment names.
        args = spec.get("driver_args") or spec.get("experiments", [])
        argv = list(project_cfg.driver) + list(args)
    argv += [str(f) for f in spec.get("extra_flags", [])]
    if spec.get("run_dir"):
        argv += [project_cfg.output_dir_flag, str(spec["run_dir"])]
    if spec.get("resume"):
        argv.append(project_cfg.resume_flag)
    cwd = project_cfg.repo_path / project_cfg.driver_cwd
    # Fail at submit, not at dispatch: a job whose script isn't in the driver
    # checkout would otherwise bring a leased cluster up just to die on
    # "can't open file" (e.g. a native executor merged into the adapter's
    # worktree but not yet into the checkout jobs run in).
    script = next((a for a in argv[:3]
                   if a.endswith(".py") and not Path(a).is_absolute()), None)
    if script and cwd.is_dir() and not (cwd / script).exists():
        raise ValueError(
            f"{script} not found in {cwd} — is the change that adds it "
            "merged into the checkout jobs run in?")
    # Same for a checkout-relative interpreter (.venv/bin/python): a venv
    # that was never created there would fail the same way.
    interp = argv[0] if argv else ""
    if "/" in interp and not Path(interp).is_absolute() and cwd.is_dir() \
            and not (cwd / interp).exists():
        raise ValueError(
            f"{interp} not found in {cwd} — create the checkout's environment "
            "before submitting")
    return argv, cwd


def canonicalize_command(project_cfg, spec: dict) -> None:
    """Collapse every command input into the sole executable argv in-place.

    Catalog commands, classic driver arguments, and caller extra flags are
    submission-time inputs. Keeping them separate in a queued job made its
    displayed command differ from what dispatch actually ran. Runtime-owned
    output-dir and resume flags deliberately remain separate.
    """
    if spec.get("command"):
        argv = list(spec["command"])
    else:
        if not project_cfg.driver:
            raise ValueError(
                f"project '{project_cfg.name}' has no driver configured and "
                "the job spec has no explicit command")
        args = spec.get("driver_args") or spec.get("experiments", [])
        argv = list(project_cfg.driver) + list(args)
    argv += [str(f) for f in spec.get("extra_flags", [])]
    spec["command"] = [str(a) for a in argv]
    spec.pop("driver_args", None)
    spec.pop("extra_flags", None)


def added_trials_spec(source: dict, run_dir: str, trials: int,
                      trial_offset: int) -> dict:
    """Copy a job spec and make it run new trial numbers in its old dir."""
    if trials < 1:
        raise ValueError("trials must be >= 1")
    if trial_offset < 0:
        raise ValueError("trial offset must be >= 0")
    spec = dict(source)
    if spec.get("command"):
        # Include a legacy queued job's separate flags, then leave the new job
        # in canonical form.
        argv = list(spec["command"]) + list(spec.get("extra_flags") or ())
        argv = _without_cli_option(argv, "--trials")
        argv = _without_cli_option(argv, "--trial-offset")
        argv = _set_cli_option(argv, "--trials", trials)
        spec["command"] = _set_cli_option(argv, "--trial-offset", trial_offset)
        spec.pop("driver_args", None)
        spec.pop("extra_flags", None)
    else:
        # Compatibility for a classic driver job queued before commands were
        # canonicalized at submission.
        flags = _without_cli_option(list(spec.get("extra_flags") or ()),
                                    "--trials")
        flags = _without_cli_option(flags, "--trial-offset")
        flags = _set_cli_option(flags, "--trials", trials)
        spec["extra_flags"] = _set_cli_option(
            flags, "--trial-offset", trial_offset)
    spec["run_dir"] = str(run_dir)
    spec["resume"] = True
    spec.pop("after", None)
    base_name = source.get("name") or "experiment"
    spec["name"] = f"{base_name} (+{trials} trial{'s' if trials != 1 else ''})"
    spec["added_trials"] = {"count": trials, "offset": trial_offset}
    return spec


def retried_point_spec(source: dict, run_dir: str, point: dict) -> dict:
    """Copy a job spec and target one existing point for replacement."""
    trial = point.get("trial")
    if trial is None or int(trial) < 0:
        raise ValueError("point has no safe numbered trial to retry")
    target = {"dims": point.get("dims") or {}, "rate": point.get("rate"),
              "trial": int(trial)}
    spec = dict(source)
    argv = list(spec.get("command") or ()) + list(spec.get("extra_flags") or ())
    if not argv:
        raise ValueError("source job has no canonical command")
    for flag in ("--trials", "--trial-offset", "--retry-point"):
        argv = _without_cli_option(argv, flag)
    argv = _set_cli_option(argv, "--trials", 1)
    argv = _set_cli_option(argv, "--trial-offset", int(trial))
    spec["command"] = _set_cli_option(
        argv, "--retry-point", json.dumps(target, separators=(",", ":"),
                                           sort_keys=True))
    spec.pop("driver_args", None)
    spec.pop("extra_flags", None)
    spec.pop("after", None)
    spec["run_dir"] = str(run_dir)
    spec["resume"] = True
    base_name = source.get("name") or "experiment"
    spec["name"] = f"{base_name} (retry point trial {trial})"
    spec["retried_point"] = target
    return spec


def _set_cli_option(argv: list, flag: str, value) -> list:
    """Set the last-value-wins form of a CLI option without stale repeats."""
    return _without_cli_option(argv, flag) + [flag, str(value)]


def _without_cli_option(argv: list, flag: str) -> list:
    """Remove both ``--flag value`` and ``--flag=value`` forms."""
    out = []
    i = 0
    while i < len(argv):
        arg = str(argv[i])
        if arg == flag:
            i += 2 if i + 1 < len(argv) else 1
            continue
        if arg.startswith(flag + "="):
            i += 1
            continue
        out.append(argv[i])
        i += 1
    return out


def recover_orphans(db, hub):
    """A job left `running` by a dead daemon has lost its attempt, not its
    place: the attempt is closed and the job goes back to the queue (or ends
    if that was its last)."""
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
        if alive:
            continue
        end_attempt(db, row["id"], error="daemon died while it ran")
        job = get(db, row["id"])
        if job["attempts"] >= job["max_attempts"]:
            finish(db, hub, row["id"], FAILED,
                   last_error="daemon died while it ran")
        else:
            wait_again(db, hub, row["id"],
                       last_error="daemon died while it ran")


def _now(db):
    return db.query_one("SELECT datetime('now') AS t")["t"]
