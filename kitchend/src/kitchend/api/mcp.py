"""MCP server, mounted in-process at /mcp on the daemon's own port.

Gives an agent the same surface the UI has — submit/monitor/cancel jobs,
cluster up/down, the run ledger — over the same DB and scheduler, so there
is exactly one writer. Spending money is gated: any action that starts VM
burn (cluster_up, or submitting a job routed onto a costed cluster) requires
`confirm_cost_usd` at or above the daemon's own estimate, so an agent must
state the cost it believes it is incurring before the daemon acts.

Transport: stateless streamable HTTP. DNS-rebinding protection is disabled
because the daemon binds loopback and is exposed only through `tailscale
serve` — the tailnet is the auth boundary, and the proxied Host header would
fail a loopback allowlist.
"""

import json

from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

from kitchend.core import adapters, jobs, ledger, submission


def require_confirmed_cost(estimate_usd, confirm_cost_usd, what: str) -> None:
    """The money gate. `estimate_usd` None means the action is free (no
    cost configured) and no confirmation is needed."""
    if estimate_usd is None:
        return
    if confirm_cost_usd is None:
        raise ValueError(
            f"{what} costs an estimated ${estimate_usd:.2f}; pass "
            f"confirm_cost_usd >= {estimate_usd:.2f} to proceed")
    if confirm_cost_usd < estimate_usd:
        raise ValueError(
            f"confirm_cost_usd ${confirm_cost_usd:.2f} is below the "
            f"estimated ${estimate_usd:.2f} for {what}")


def _job_cost_estimate(state, project_cfg, spec, est_hours):
    """$ estimate for a job: the burn rate of the cluster its queue maps to,
    times the caller's stated duration. Queues that map to no configured
    cluster (local/toy projects) are free."""
    queue = spec.get("queue") or ""
    if "/" not in queue:
        return None
    try:
        hourly = state.clusters.estimate_hourly(queue)
    except KeyError:
        return None
    if hourly is None:
        return None
    if not est_hours:
        raise ValueError(
            f"jobs on cluster '{queue}' cost ${hourly:.2f}/hr; pass "
            "est_hours (expected runtime) so the cost can be estimated")
    return hourly * float(est_hours)


def build_mcp(state) -> MCPServer:
    """`state` is the FastAPI app.state: config, db, hub, scheduler,
    clusters, runner."""
    mcp = MCPServer(
        "cloud-kitchen",
        instructions=(
            "Job queue, cluster manager, and run ledger for benchmark "
            "experiments. Cluster lifecycle is explicit and TTL-leased: "
            "bring a cluster up before submitting jobs that need it, and "
            "every dollar-burning call requires confirm_cost_usd. Job "
            "progress comes from get_job (points done/total, ETA); results "
            "land in the run ledger (list_runs/get_run). Record what a "
            "one-off showed with add_run_note."),
    )

    def _project(name):
        try:
            return state.config.project(name)
        except KeyError:
            raise ValueError(f"unknown project '{name}'; see list_projects")

    # --- catalog ---

    @mcp.tool()
    def list_projects() -> dict:
        """Configured projects, their clusters, and runs roots."""
        return {"projects": [{
            "name": p.name,
            "repo_path": str(p.repo_path),
            "clusters": [c.name for c in p.clusters],
            "has_driver": bool(p.driver),
        } for p in state.config.projects]}

    @mcp.tool()
    def list_experiments(project: str) -> dict:
        """The project's experiment catalog (from its kitchen_adapter.py),
        plus aggregates and saved one-off presets."""
        project_cfg = _project(project)
        cat = adapters.catalog(project_cfg)
        project_id = jobs.ensure_project_row(state.db, project_cfg)
        cat["saved_sweeps"] = [
            {"name": r["name"], "params": json.loads(r["params_json"])}
            for r in state.db.query(
                "SELECT name, params_json FROM saved_sweeps "
                "WHERE project_id = ? ORDER BY name", (project_id,))]
        return cat

    # --- jobs ---

    @mcp.tool()
    def submit_job(project: str, experiments: list[str] | None = None,
                   sweep: dict | None = None, queue: str | None = None,
                   priority: int = 0, max_retries: int = 0,
                   est_hours: float | None = None,
                   confirm_cost_usd: float | None = None) -> dict:
        """Queue a job. Give catalog `experiments` OR a one-off `sweep`
        (base, dims {name: [values]}, rates, rate_search, trials,
        duration_secs — translated by the project's adapter). Jobs whose
        queue maps to a costed cluster require est_hours plus
        confirm_cost_usd >= the returned estimate. The job does not bring
        clusters up; lease one first with cluster_up if the driver expects
        it running."""
        project_cfg = _project(project)
        spec = {"project": project, "priority": priority,
                "max_retries": max_retries}
        if experiments:
            spec["experiments"] = experiments
        if sweep:
            spec["sweep"] = sweep
        if queue:
            spec["queue"] = queue
        submission.prepare_spec(project_cfg, spec)
        estimate = _job_cost_estimate(state, project_cfg, spec, est_hours)
        require_confirmed_cost(estimate, confirm_cost_usd,
                               f"job on queue '{spec.get('queue')}'")
        job_id = submission.enqueue(state.db, state.hub, state.scheduler,
                                    project_cfg, spec)
        return {"job_id": job_id, "queue": spec.get("queue"),
                "estimate_usd": estimate}

    @mcp.tool()
    def get_job(job_id: int) -> dict:
        """One job with its live progress (points ok/dead/failed, current
        point, eta_secs) once its run emits events."""
        job = jobs.get(state.db, job_id)
        if job is None:
            raise ValueError(f"no job {job_id}")
        return job

    @mcp.tool()
    def list_jobs(job_state: str | None = None, limit: int = 20) -> dict:
        """Recent jobs, newest first. job_state filters on
        queued/running/succeeded/degraded/failed/canceled/interrupted."""
        return {"jobs": jobs.list_jobs(state.db, state=job_state,
                                       limit=limit)}

    @mcp.tool()
    def job_log(job_id: int, tail: int = 100) -> str:
        """The last `tail` lines of the job's driver output."""
        return state.runner.tail_log(job_id, lines=tail)

    @mcp.tool()
    async def cancel_job(job_id: int) -> dict:
        """Cancel a queued or running job (SIGINT first, so the run leaves a
        clean interrupt trail and can be resumed)."""
        return {"job_id": job_id,
                "state": await state.scheduler.cancel(job_id)}

    @mcp.tool()
    def resubmit_job(job_id: int, resume: bool = True) -> dict:
        """Resubmit a finished/failed/canceled job; resume=True reuses its
        run dir so completed points are skipped."""
        return {"job_id": state.scheduler.resubmit(job_id, resume=resume),
                "parent": job_id}

    # --- clusters ---

    @mcp.tool()
    def cluster_status() -> dict:
        """All clusters: state, VM count, burn rate, live leases."""
        return {"clusters": state.clusters.snapshot()}

    @mcp.tool()
    async def cluster_up(project: str, cluster: str, ttl_minutes: int,
                         confirm_cost_usd: float | None = None) -> dict:
        """Start a cluster under a TTL lease (auto-stops at expiry; extend
        with extend_lease). ttl_minutes is mandatory and confirm_cost_usd
        must cover burn_rate * ttl."""
        if ttl_minutes <= 0:
            raise ValueError("ttl_minutes must be positive")
        key = f"{project}/{cluster}"
        hourly = state.clusters.estimate_hourly(key)
        estimate = (hourly * ttl_minutes / 60) if hourly is not None else None
        require_confirmed_cost(estimate, confirm_cost_usd,
                               f"cluster {key} for {ttl_minutes} minutes")
        lease_id = await state.clusters.up(key, ttl_minutes, purpose="agent")
        return {"lease_id": lease_id, "ttl_minutes": ttl_minutes,
                "estimate_usd": estimate}

    @mcp.tool()
    async def cluster_down(project: str, cluster: str,
                           force: bool = False) -> dict:
        """Stop a cluster. Refuses while leases are live unless force=True."""
        await state.clusters.down(f"{project}/{cluster}", force=force)
        return {"ok": True}

    @mcp.tool()
    def extend_lease(project: str, cluster: str, lease_id: str,
                     ttl_minutes: int) -> dict:
        """Push a lease's expiry ttl_minutes from now."""
        state.clusters.extend(f"{project}/{cluster}", lease_id, ttl_minutes)
        return {"ok": True}

    # --- run ledger ---

    @mcp.tool()
    def list_runs(project: str | None = None, experiment: str | None = None,
                  tag: str | None = None, limit: int = 20) -> dict:
        """Recorded runs (paper sweeps and one-offs), newest first.
        dir_exists=0 means the directory was deleted but metrics survive."""
        return {"runs": ledger.list_runs(state.db, project=project,
                                         experiment=experiment, tag=tag,
                                         limit=limit)}

    @mcp.tool()
    def get_run(run_id: int) -> dict:
        """One run with per-point metrics, tags, and notes."""
        run = ledger.get_run(state.db, run_id)
        if run is None:
            raise ValueError(f"no run {run_id}")
        return run

    @mcp.tool()
    def add_run_note(run_id: int, text: str) -> dict:
        """Attach a note to a run — why it ran, what it showed."""
        if ledger.get_run(state.db, run_id) is None:
            raise ValueError(f"no run {run_id}")
        return {"note_id": ledger.add_note(state.db, run_id, text)}

    @mcp.tool()
    def tag_run(run_id: int, tag: str) -> dict:
        """Tag a run (e.g. 'paper', 'keeper', 'anomaly')."""
        if ledger.get_run(state.db, run_id) is None:
            raise ValueError(f"no run {run_id}")
        ledger.add_tag(state.db, run_id, tag)
        return {"ok": True}

    @mcp.tool()
    def scan_runs(project: str) -> dict:
        """Backfill the ledger from pre-existing run dirs on disk."""
        return ledger.scan_project(state.db, _project(project))

    return mcp


class _AtMountRoot:
    """Serve the sub-app's '/' route for any path under the mount.

    Mounting at '/mcp' hands the sub-app path '' for a request to bare
    '/mcp' (and '/' only for '/mcp/'); normalizing to '/' makes the
    canonical no-trailing-slash URL work.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            scope = dict(scope)
            scope["path"] = "/"
        await self.app(scope, receive, send)


def mcp_http_app(mcp: MCPServer):
    """The mountable ASGI app, serving at the mount root."""
    return _AtMountRoot(mcp.streamable_http_app(
        streamable_http_path="/", stateless_http=True, json_response=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=False),
    ))
