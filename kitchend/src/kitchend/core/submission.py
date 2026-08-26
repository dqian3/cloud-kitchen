"""Job submission: one resolution path shared by the HTTP API and MCP.

Takes a raw spec (experiments, an ad-hoc sweep, or an explicit command) and
resolves it against the project's adapter into one or more job specs. A
submission is usually one job; catalog experiments that carry their own
native command fan out into sibling jobs — one per experiment, so each has
its own retries, lease, and progress — which the per-queue FIFO serializes
and same-cluster leases hand over between. Raises ValueError for anything
the caller should report as a bad request.
"""

from . import adapters, jobs


def prepare_specs(project_cfg, spec: dict) -> list[dict]:
    """Resolve and validate a submission. Returns the job specs it becomes."""
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
        # A sweep that names a daemon-configured cluster gets the managed
        # lease: the daemon brings the cluster up before the job and releases
        # it after (one-off executors assume VMs are running). Names that
        # don't match a configured cluster (a catalog alias, a local run)
        # only route the queue.
        cluster = spec["sweep"].get("cluster")
        if cluster and not spec.get("cluster") and \
                any(c.name == cluster for c in project_cfg.clusters):
            spec["cluster"] = cluster
    if spec.get("after") is not None:
        spec["after"] = int(spec["after"])

    specs = [spec]
    if spec.get("experiments") and not spec.get("command"):
        # Resolve against the project's catalog: expand aggregates, reject
        # unknown names and cross-cluster mixes, and route onto the cluster's
        # queue. Native (command-carrying) experiments become one job each.
        specs = []
        for plan in adapters.resolve_jobs(project_cfg, spec["experiments"]):
            s = {k: v for k, v in spec.items() if k != "experiments"}
            s["experiments"] = plan["experiments"]
            if plan.get("command"):
                s["command"] = plan["command"]
            if plan.get("driver_args") is not None:
                s["driver_args"] = plan["driver_args"]
            if plan.get("hosts"):
                # The VMs the lease starts; the rest of the cluster stays down.
                s["hosts"] = plan["hosts"]
            if plan.get("queue") and not s.get("queue"):
                s["queue"] = f"{project_cfg.name}/{plan['queue']}"
            # A native experiment assumes its cluster is already up, so a
            # queue naming a daemon-configured cluster brings the managed
            # lease with it. Classic driver experiments keep managing their
            # own VMs (interim mode) unless the submitter set `cluster`.
            if plan.get("command") and not s.get("cluster") and \
                    any(c.name == plan.get("queue")
                        for c in project_cfg.clusters):
                s["cluster"] = plan["queue"]
            specs.append(s)

    for s in specs:
        cluster = s.get("cluster")
        if cluster:
            if not any(c.name == cluster for c in project_cfg.clusters):
                raise ValueError(
                    f"project '{project_cfg.name}' has no cluster '{cluster}' "
                    f"configured; known: {[c.name for c in project_cfg.clusters]}")
            if not s.get("queue"):
                s["queue"] = f"{project_cfg.name}/{cluster}"
        # Stamp the label once, here, so the queue reads the same in the UI,
        # the CLI and MCP — and never as a wall of argv.
        s["name"] = jobs.label(s, project_cfg)
        # Validate now so a bad spec fails at submit, not at dispatch.
        jobs.build_command(project_cfg, s)
    return specs


def enqueue(db, hub, scheduler, project_cfg, spec: dict) -> int:
    project_id = jobs.ensure_project_row(db, project_cfg)
    job_id = jobs.submit(db, project_id, spec)
    hub.emit("job.state", job_id=job_id, state=jobs.WAITING)
    scheduler.wake()
    return job_id


def enqueue_all(db, hub, scheduler, project_cfg, specs: list[dict]) -> list[int]:
    return [enqueue(db, hub, scheduler, project_cfg, s) for s in specs]
