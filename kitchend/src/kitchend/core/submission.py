"""Job submission: one resolution path shared by the HTTP API and MCP.

Takes a raw spec (experiments, an ad-hoc sweep, or an explicit command),
resolves it against the project's adapter, validates that it builds a
command, and enqueues it. Raises ValueError for anything the caller should
report as a bad request.
"""

from . import adapters, jobs


def prepare_spec(project_cfg, spec: dict) -> dict:
    """Resolve and validate a submission spec in place. Returns the spec."""
    if not spec.get("experiments") and not spec.get("command") \
            and not spec.get("sweep"):
        raise ValueError("spec needs experiments, a sweep, or an explicit command")
    # An ad-hoc sweep resolves to an explicit command at submit time, so a
    # later adapter edit can't silently change what a queued job will run.
    if spec.get("sweep"):
        if spec.get("experiments") or spec.get("command"):
            raise ValueError("sweep excludes experiments/command")
        argv, queue = adapters.oneoff_command(project_cfg, spec["sweep"])
        spec["command"] = argv
        if queue and not spec.get("queue"):
            spec["queue"] = f"{project_cfg.name}/{queue}"
    # Resolve against the project's catalog: expand aggregates, reject unknown
    # names and cross-cluster mixes, and route onto the cluster's queue.
    elif spec.get("experiments"):
        expanded, queue, driver_args = adapters.resolve_submission(
            project_cfg, spec["experiments"])
        spec["experiments"] = expanded
        spec["driver_args"] = driver_args
        if queue and not spec.get("queue"):
            spec["queue"] = f"{project_cfg.name}/{queue}"
    # Validate now so a bad spec fails at submit, not at dispatch.
    jobs.build_command(project_cfg, spec)
    return spec


def enqueue(db, hub, scheduler, project_cfg, spec: dict) -> int:
    project_id = jobs.ensure_project_row(db, project_cfg)
    job_id = jobs.submit(db, project_id, spec)
    hub.emit("job.state", job_id=job_id, state=jobs.QUEUED)
    scheduler.wake()
    return job_id
