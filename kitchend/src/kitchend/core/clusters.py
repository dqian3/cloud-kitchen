"""Cluster manager: daemon-owned keep-alive, TTL leases, cost sessions.

The mechanics (dead-man switch, drain-before-start, flock, lease files) live
in kitchen.cluster; this layer adds what a daemon can: no terminal is
load-bearing, bring-ups carry a TTL, cost accrues in cluster_sessions rows,
and gcloud is re-polled so the DB reflects reality rather than intent.

While a cluster is up, one asyncio task ticks every minute: it re-arms the
dead-man switch each 30 minutes *only while a live lease exists*; when the
last lease expires or is released, it stops the VMs. If the daemon dies, the
switch on the VMs bounds the damage to the dead-man window.
"""

import asyncio
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from kitchen.cluster import ClusterState, KeepAlive, start_vms, stop_vms
from kitchen.remote import GCloudRemote, RemoteSettings


def _group_vms(group: dict) -> list[str]:
    """VMs of one role group: an explicit `vms` list, a single `vm`, or the
    generative `vm_prefix` + `count` naming (vsac corfu's client pool)."""
    if not isinstance(group, dict):
        return []
    if "vms" in group:
        return list(group["vms"])
    if "vm" in group and group["vm"]:
        return [group["vm"]]
    if "vm_prefix" in group and "count" in group:
        return [f"{group['vm_prefix']}{i}" for i in range(int(group["count"]))]
    return []


def vms_from_yaml(path: Path) -> list[str]:
    """Extract the VM list from a cluster YAML of any known shape."""
    with open(path) as f:
        d = yaml.safe_load(f) or {}
    if "vms" in d:
        return list(d["vms"])
    vms = []
    if "replica" in d or "durlog" in d:
        for role in ("replica", "sequencer", "durlog", "conslog"):
            vms += _group_vms(d.get(role, {}))
        for shard in d.get("shards", []):
            for k in ("primary_vm", "backup_vm"):
                if shard.get(k):
                    vms.append(shard[k])
        vms += _group_vms(d.get("client", {}))
        return vms
    raise ValueError(f"unrecognized cluster config shape: {path}")


@dataclass
class ManagedCluster:
    key: str                    # "<project>/<name>"
    project: str
    name: str
    config_path: Path
    hourly_usd: float | None
    remote: GCloudRemote
    state: ClusterState
    db_id: int
    create_cmd: tuple = ()      # provisioning argv (repo's setup script)
    create_cwd: Path | None = None
    vms: list[str] = field(default_factory=list)
    task: asyncio.Task | None = None
    keepalive: KeepAlive | None = None
    lease_handles: dict[str, object] = field(default_factory=dict)
    session_id: int | None = None
    last_status: dict = field(default_factory=dict)
    last_rearm: float = 0.0
    create_task: asyncio.Task | None = None
    create_log: list = field(default_factory=list)   # captured output lines
    create_rc: int | None = None                     # last run's exit code


class ClusterManager:
    REARM_INTERVAL_S = 30 * 60
    TICK_S = 60
    STATUS_POLL_S = 5 * 60

    def __init__(self, config, db, hub):
        self.config = config
        self.db = db
        self.hub = hub
        self.clusters: dict[str, ManagedCluster] = {}
        self._build_registry()
        # A daemon killed mid-transition leaves 'starting'/'stopping' rows
        # behind; this daemon isn't doing either, so say so. The VMs
        # themselves are deliberately left alone — refresh shows reality,
        # and a driver-managed run may legitimately own them.
        self.db.execute(
            "UPDATE clusters SET state = 'terminated' "
            "WHERE state IN ('starting', 'stopping')")

    def _build_registry(self):
        for p in self.config.projects:
            settings = RemoteSettings(
                gcp_project=p.gcp_project,
                tunnel_through_iap=p.tunnel_through_iap,
            )
            for c in p.clusters:
                key = f"{p.name}/{c.name}"
                config_path = p.repo_path / c.config
                row = self.db.query_one(
                    "SELECT id FROM clusters WHERE name = ? AND project_id = "
                    "(SELECT id FROM projects WHERE name = ?)", (c.name, p.name))
                if row:
                    db_id = row["id"]
                else:
                    from . import jobs as jobs_mod
                    project_id = jobs_mod.ensure_project_row(self.db, p)
                    db_id = self.db.insert(
                        "INSERT INTO clusters (project_id, name, config_path, "
                        "hourly_usd, state) VALUES (?, ?, ?, ?, 'terminated')",
                        (project_id, c.name, str(config_path), c.hourly_usd))
                self.clusters[key] = ManagedCluster(
                    key=key, project=p.name, name=c.name,
                    config_path=config_path, hourly_usd=c.hourly_usd,
                    remote=GCloudRemote(settings=settings,
                                        project=settings.gcp_project,
                                        tunnel_through_iap=settings.tunnel_through_iap),
                    state=ClusterState(f"{p.name}-{c.name}"),
                    db_id=db_id,
                    create_cmd=tuple(c.create_cmd),
                    create_cwd=p.repo_path / p.driver_cwd,
                )

    def _get(self, key) -> ManagedCluster:
        if key not in self.clusters:
            raise KeyError(f"unknown cluster: {key}")
        return self.clusters[key]

    def _vms(self, mc: ManagedCluster):
        if not mc.vms:
            mc.vms = vms_from_yaml(mc.config_path)
        return mc.vms

    def estimate_hourly(self, key) -> float | None:
        """Whole-cluster burn rate in $/hr, or None when no cost is
        configured. Reads the VM list if it hasn't been loaded yet, so the
        estimate exists before the cluster's first bring-up."""
        mc = self._get(key)
        if mc.hourly_usd is None:
            return None
        try:
            return mc.hourly_usd * len(self._vms(mc))
        except OSError:
            return None

    def _set_db_state(self, mc, state):
        self.db.execute(
            "UPDATE clusters SET state = ?, state_updated_at = datetime('now'), "
            "vm_count = ? WHERE id = ?", (state, len(mc.vms), mc.db_id))
        self.hub.emit("cluster.state", cluster_id=mc.db_id,
                      cluster=mc.key, state=state)

    # --- user/agent operations ---

    async def up(self, key, ttl_minutes: int, purpose="user"):
        mc = self._get(key)
        if ttl_minutes < 1:
            raise ValueError("ttl_minutes must be >= 1")
        vms = self._vms(mc)
        lease = mc.state.acquire_lease(purpose, ttl_s=ttl_minutes * 60)
        mc.lease_handles[lease.info.id] = lease
        self.db.insert(
            "INSERT INTO cluster_leases (cluster_id, holder_type, holder_id, "
            "expires_at) VALUES (?, ?, ?, datetime('now', ?))",
            (mc.db_id, purpose, lease.info.id, f"+{ttl_minutes * 60} seconds"))
        if mc.task is None or mc.task.done():
            self._set_db_state(mc, "starting")
            try:
                # A cluster that cannot fully start cannot run its job;
                # stop_on_partial stops the started subset rather than
                # holding it at cost. Pre-existing running VMs are untouched.
                await asyncio.to_thread(start_vms, mc.remote, vms,
                                        stop_on_partial=True)
            except Exception as e:
                self.hub.emit("cluster.error", cluster_id=mc.db_id,
                              cluster=mc.key, error=repr(e))
                lease.release()
                mc.lease_handles.pop(lease.info.id, None)
                self.db.execute(
                    "UPDATE cluster_leases SET released_at = datetime('now') "
                    "WHERE holder_id = ?", (lease.info.id,))
                self._set_db_state(mc, "terminated")
                raise RuntimeError(
                    f"cluster {key} failed to start (the VMs that did come "
                    f"up were stopped again): {e}") from e
            mc.keepalive = KeepAlive(mc.remote, vms, state=mc.state,
                                     stop_on_exit=False)
            mc.keepalive.acquire_lock()
            mc.last_rearm = time.monotonic()
            mc.session_id = self.db.insert(
                "INSERT INTO cluster_sessions (cluster_id, started_at, vm_count, "
                "hourly_usd) VALUES (?, datetime('now'), ?, ?)",
                (mc.db_id, len(vms), mc.hourly_usd))
            self._set_db_state(mc, "running")
            mc.task = asyncio.get_running_loop().create_task(self._tick_loop(mc))
        return lease.info.id

    async def down(self, key, force=False):
        mc = self._get(key)
        vms = self._vms(mc)
        live = mc.state.live_leases()
        if live and not force:
            holders = ", ".join(f"{l.purpose} ({l.id})" for l in live)
            raise RuntimeError(f"cluster {key} has live leases: {holders}")
        for lease in list(mc.lease_handles.values()):
            lease.release()
        mc.lease_handles.clear()
        for l in mc.state.live_leases():   # leases from other processes
            if force:
                l.path.unlink(missing_ok=True)
        await self._shutdown(mc, vms, reason="user")

    def extend(self, key, lease_id, ttl_minutes):
        mc = self._get(key)
        lease = mc.lease_handles.get(lease_id)
        if lease is None:
            raise KeyError(f"lease {lease_id} not held by the daemon")
        lease.renew(ttl_s=ttl_minutes * 60)
        self.db.execute(
            "UPDATE cluster_leases SET expires_at = datetime('now', ?) "
            "WHERE holder_id = ?", (f"+{ttl_minutes * 60} seconds", lease_id))
        self.hub.emit("cluster.lease", cluster_id=mc.db_id, cluster=mc.key,
                      action="extended", lease=lease_id)

    def release(self, key, lease_id):
        mc = self._get(key)
        lease = mc.lease_handles.pop(lease_id, None)
        if lease is not None:
            lease.release()
            self.db.execute(
                "UPDATE cluster_leases SET released_at = datetime('now') "
                "WHERE holder_id = ?", (lease_id,))
            self.hub.emit("cluster.lease", cluster_id=mc.db_id, cluster=mc.key,
                          action="released", lease=lease_id)

    async def create(self, key):
        """Provision the cluster's VMs via the repo's own setup script.

        This *creates* instances (money), so it only runs when the config
        declares a create_cmd, nothing else is creating, and the cluster
        isn't already being managed up. The scripts are restartable
        (existing VMs are skipped), so re-running after a partial stockout
        fills the gaps. Output is captured line by line into create_log,
        which the snapshot exposes for the UI to tail.
        """
        mc = self._get(key)
        if not mc.create_cmd:
            raise ValueError(f"cluster {key} has no create_cmd configured")
        if mc.create_task is not None and not mc.create_task.done():
            raise RuntimeError(f"cluster {key} is already being created")
        if mc.task is not None and not mc.task.done():
            raise RuntimeError(
                f"cluster {key} is up under daemon management; create is for "
                "provisioning VMs that don't exist yet")
        mc.create_log = []
        mc.create_rc = None
        self.hub.emit("cluster.create.started", cluster_id=mc.db_id,
                      cluster=mc.key, cmd=list(mc.create_cmd))
        mc.create_task = asyncio.get_running_loop().create_task(
            self._run_create(mc))

    async def _run_create(self, mc: ManagedCluster):
        try:
            proc = await asyncio.create_subprocess_exec(
                *mc.create_cmd, cwd=str(mc.create_cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT)
            async for raw in proc.stdout:
                line = raw.decode(errors="replace").rstrip()
                mc.create_log.append(line)
                if len(mc.create_log) > 500:
                    del mc.create_log[:100]
            mc.create_rc = await proc.wait()
        except Exception as e:
            mc.create_log.append(f"[daemon] create failed to run: {e!r}")
            mc.create_rc = -1
        # The setup scripts may regenerate the cluster YAML (n21 rewrites
        # its config); drop the cached VM list so the next read is fresh.
        mc.vms = []
        self.hub.emit("cluster.create.finished", cluster_id=mc.db_id,
                      cluster=mc.key, rc=mc.create_rc)

    async def refresh_status(self, key):
        mc = self._get(key)
        return await self.poll_status(mc)

    async def poll_status(self, mc: ManagedCluster):
        """Read VM states from gcloud and reconcile the displayed state.

        gcloud is the truth about VMs; the DB state describes daemon
        management. VMs running without a daemon lease show as 'unmanaged'
        (a driver-managed run, or leftovers) — they accrue cost, so they
        must be visible. The VMs themselves are never touched here.
        """
        vms = self._vms(mc)
        statuses = await asyncio.to_thread(mc.remote.vm_status, vms)
        mc.last_status = statuses
        if mc.task is not None and not mc.task.done():
            return statuses
        running = sum(1 for s in statuses.values() if s == "RUNNING")
        row = self.db.query_one(
            "SELECT state FROM clusters WHERE id = ?", (mc.db_id,))
        state = row["state"] if row else "terminated"
        if running and state == "terminated":
            self._set_db_state(mc, "unmanaged")
            self.hub.emit(
                "cluster.unmanaged", cluster_id=mc.db_id, cluster=mc.key,
                running=running,
                note="VMs running with no daemon lease are accruing cost")
        elif not running and state == "unmanaged":
            self._set_db_state(mc, "terminated")
        return statuses

    async def status_poll_loop(self):
        while True:
            for mc in self.clusters.values():
                try:
                    await self.poll_status(mc)
                except Exception as e:
                    self.hub.emit("cluster.error", cluster_id=mc.db_id,
                                  cluster=mc.key, error=f"status poll: {e!r}")
            await asyncio.sleep(self.STATUS_POLL_S)

    def snapshot(self):
        out = []
        for mc in self.clusters.values():
            row = self.db.query_one(
                "SELECT state, state_updated_at FROM clusters WHERE id = ?",
                (mc.db_id,))
            leases = [
                {"id": l.id, "purpose": l.purpose, "pid": l.pid,
                 "expires_in_s": max(0, int(l.expires_at - time.time()))}
                for l in mc.state.live_leases()
            ]
            running = sum(1 for s in (mc.last_status or {}).values()
                          if s == "RUNNING")
            burn = None
            if mc.hourly_usd is not None and mc.session_id is not None:
                burn = mc.hourly_usd * len(mc.vms)
            elif mc.hourly_usd is not None and running:
                burn = mc.hourly_usd * running
            creating = mc.create_task is not None and not mc.create_task.done()
            out.append({
                "key": mc.key, "project": mc.project, "name": mc.name,
                "state": row["state"] if row else "terminated",
                "state_updated_at": row["state_updated_at"] if row else None,
                "vm_count": len(mc.vms) if mc.vms else None,
                "vms_running": running if mc.last_status else None,
                "vms": mc.last_status or None,
                "leases": leases,
                "active": mc.task is not None and not mc.task.done(),
                "burn_usd_per_hr": burn,
                "session_cost_usd": self._session_cost(mc),
                "create": ({
                    "running": creating,
                    "rc": mc.create_rc,
                    "log_tail": mc.create_log[-30:],
                } if mc.create_cmd else None),
            })
        return out

    def _session_cost(self, mc):
        if mc.hourly_usd is None:
            return None
        row = self.db.query_one(
            "SELECT COALESCE(SUM((julianday(COALESCE(stopped_at, "
            "datetime('now'))) - julianday(started_at)) * 24 * vm_count * "
            "hourly_usd), 0) AS usd FROM cluster_sessions WHERE cluster_id = ?",
            (mc.db_id,))
        return round(row["usd"], 2) if row else None

    # --- the tick loop ---

    async def _tick_loop(self, mc: ManagedCluster):
        vms = self._vms(mc)
        try:
            while True:
                await asyncio.sleep(self.TICK_S)
                live = mc.state.live_leases()
                if not live:
                    self.hub.emit("cluster.lease", cluster_id=mc.db_id,
                                  cluster=mc.key, action="all_expired")
                    await self._shutdown(mc, vms, reason="leases_expired")
                    return
                if time.monotonic() - mc.last_rearm >= self.REARM_INTERVAL_S:
                    await asyncio.to_thread(mc.keepalive.rearm)
                    mc.last_rearm = time.monotonic()
                    self.hub.emit("cluster.keepalive", cluster_id=mc.db_id,
                                  cluster=mc.key,
                                  leases=[l.purpose for l in live])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.hub.emit("cluster.error", cluster_id=mc.db_id,
                          cluster=mc.key, error=repr(e))

    async def _shutdown(self, mc, vms, reason):
        if mc.task is not None and not mc.task.done() and \
                mc.task is not asyncio.current_task():
            mc.task.cancel()
        mc.task = None
        self._set_db_state(mc, "stopping")
        self.hub.emit("cluster.stopping", cluster_id=mc.db_id,
                      cluster=mc.key, reason=reason)
        try:
            await asyncio.to_thread(stop_vms, mc.remote, vms)
        finally:
            if mc.keepalive is not None:
                mc.keepalive.release_lock()
                mc.keepalive = None
            if mc.session_id is not None:
                self.db.execute(
                    "UPDATE cluster_sessions SET stopped_at = datetime('now') "
                    "WHERE id = ?", (mc.session_id,))
                mc.session_id = None
            self._set_db_state(mc, "terminated")

    async def shutdown_all(self):
        """Daemon exit: stop every cluster this daemon is keeping alive."""
        for mc in self.clusters.values():
            if mc.task is not None and not mc.task.done():
                try:
                    await self._shutdown(mc, self._vms(mc), reason="daemon_exit")
                except Exception as e:
                    self.hub.emit("cluster.error", cluster_id=mc.db_id,
                                  cluster=mc.key, error=repr(e))
