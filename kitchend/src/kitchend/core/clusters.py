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
from kitchen.remote import DockerRemote, GCloudRemote, RemoteSettings


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


def platform_from_yaml(path: Path) -> tuple[str, dict]:
    """(platform, raw config) — the daemon picks the remote backend from
    the cluster YAML, not the project: a docker fleet sits beside gcloud
    ones in the same project."""
    with open(path) as f:
        d = yaml.safe_load(f) or {}
    return d.get("platform", "gcloud"), d


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
    last_status: dict | None = None     # None until the first gcloud poll
    last_rearm: float = 0.0
    create_task: asyncio.Task | None = None
    create_log: list = field(default_factory=list)   # captured output lines
    create_rc: int | None = None                     # last run's exit code
    create_attempt: int = 0
    create_max_attempts: int = 0
    create_missing: list = field(default_factory=list)   # VMs still absent
    create_next_at: float | None = None              # epoch of next attempt
    hold_until: float = 0.0     # keep VMs up past the last lease until then
    hold_for: int | None = None  # the job the hand-off is for


class ClusterManager:
    REARM_INTERVAL_S = 30 * 60
    TICK_S = 60
    # gcloud status poll cadence by what the cluster is doing: fast while
    # it changes, slow when nothing should be happening.
    POLL_TRANSITIONAL_S = 10    # starting, stopping, provisioning
    POLL_RUNNING_S = 60         # running or unmanaged: VMs are billing
    POLL_IDLE_S = 10 * 60       # terminated

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
                remote = GCloudRemote(settings=settings,
                                      project=settings.gcp_project,
                                      tunnel_through_iap=settings.tunnel_through_iap)
                try:
                    platform, raw = platform_from_yaml(config_path)
                except OSError:
                    platform, raw = "gcloud", {}
                if platform == "docker":
                    compose = Path(raw.get("compose_file", "docker-compose.yml"))
                    if not compose.is_absolute():
                        compose = config_path.parent / compose
                    remote = DockerRemote(compose,
                                          project=raw.get("compose_project"))
                self.clusters[key] = ManagedCluster(
                    key=key, project=p.name, name=c.name,
                    config_path=config_path, hourly_usd=c.hourly_usd,
                    remote=remote,
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

    def overlaps(self, a: str, b: str) -> bool:
        """Do two clusters share any VM? (main is the head of the n51 fleet.)
        Leasing both at once would let one lease's release stop the other's
        running job."""
        if a == b:
            return False
        try:
            return bool(set(self._vms(self._get(a))) & set(self._vms(self._get(b))))
        except (KeyError, OSError, ValueError):
            return False

    def overlapping_active(self, key: str) -> list[str]:
        return [other.key for other in self.clusters.values()
                if other.task is not None and not other.task.done()
                and self.overlaps(key, other.key)]

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

    async def up(self, key, ttl_minutes: int, purpose="user", vms=None):
        """Lease the cluster, starting the VMs the holder needs.

        `vms` restricts the start to a subset of the cluster (a job whose
        largest point addresses 22 of 102 VMs); None means all of them. The
        keep-alive covers the union of every live lease's VMs, and the tick
        loop stops whatever is running when the last lease goes.
        """
        mc = self._get(key)
        if ttl_minutes < 1:
            raise ValueError("ttl_minutes must be >= 1")
        every = self._vms(mc)
        unknown = sorted(set(vms or ()) - set(every))
        if unknown:
            raise ValueError(f"cluster {key} has no VM {unknown}")
        vms = [v for v in every if v in set(vms)] if vms else every
        if mc.task is None or mc.task.done():
            busy = self.overlapping_active(key)
            if busy:
                raise RuntimeError(
                    f"cluster {key} shares VMs with {busy[0]}, which is up "
                    "under a lease; wait for it or use that cluster")
        lease = mc.state.acquire_lease(purpose, ttl_s=ttl_minutes * 60)
        mc.lease_handles[lease.info.id] = lease
        self.db.insert(
            "INSERT INTO cluster_leases (cluster_id, holder_type, holder_id, "
            "expires_at) VALUES (?, ?, ?, datetime('now', ?))",
            (mc.db_id, purpose, lease.info.id, f"+{ttl_minutes * 60} seconds"))
        fresh = mc.task is None or mc.task.done()
        if fresh:
            self._set_db_state(mc, "starting")
        # The same start whether the cluster is coming up for this lease or
        # is already up under another: start_vms skips RUNNING VMs, so a
        # handover only starts what the new holder needs beyond what the
        # last one left running (it may have released VMs its sweep stopped
        # addressing, or never needed them). A cluster that cannot fully
        # start cannot run its job; stop_on_partial stops the started subset
        # rather than holding it at cost.
        try:
            started = await asyncio.to_thread(start_vms, mc.remote, vms,
                                              drain_first=fresh,
                                              stop_on_partial=True)
        except Exception as e:
            self.hub.emit("cluster.error", cluster_id=mc.db_id,
                          cluster=mc.key, error=repr(e))
            lease.release()
            mc.lease_handles.pop(lease.info.id, None)
            self.db.execute(
                "UPDATE cluster_leases SET released_at = datetime('now') "
                "WHERE holder_id = ?", (lease.info.id,))
            if fresh:
                self._set_db_state(mc, "terminated")
            raise RuntimeError(
                f"cluster {key} failed to start (the VMs that did come "
                f"up were stopped again): {e}") from e
        if fresh:
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
        else:
            # The keep-alive must re-arm the dead-man timer on every VM any
            # live lease holds, including ones this lease just started.
            if mc.keepalive is not None:
                mc.keepalive.extend(vms)
            if started:
                self.hub.emit("cluster.restarted", cluster_id=mc.db_id,
                              cluster=mc.key, vms=started)
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

    def hold(self, key, seconds, for_job=None):
        """Keep the cluster up past its last lease for `seconds`: the next
        job in the queue uses these VMs, so hand them over instead of
        stopping and restarting them. The hold ends early when a new lease
        arrives; a job that never comes lets it lapse."""
        mc = self._get(key)
        mc.hold_until = time.time() + seconds
        mc.hold_for = for_job
        self.hub.emit("cluster.hold", cluster_id=mc.db_id, cluster=mc.key,
                      seconds=seconds, for_job=for_job)

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

    async def create(self, key, *, retry_delay_s=900, max_attempts=12,
                     stop_after=True):
        """Provision the cluster's VMs via the repo's own setup script,
        retrying until every VM in its config exists.

        This *creates* instances (money), so it only runs when the config
        declares a create_cmd, nothing else is creating, and the cluster
        isn't already being managed up. The scripts are restartable
        (existing VMs are skipped), so each attempt fills whatever a zone
        stockout left missing; attempts are retry_delay_s apart, up to
        max_attempts. Fresh VMs boot running: with stop_after they are
        stopped after each attempt so nothing accrues cost while the fleet
        waits on capacity. Output is captured line by line into create_log,
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
        mc.create_attempt = 0
        mc.create_max_attempts = max(1, int(max_attempts))
        mc.create_missing = []
        mc.create_next_at = None
        self.hub.emit("cluster.create.started", cluster_id=mc.db_id,
                      cluster=mc.key, cmd=list(mc.create_cmd),
                      max_attempts=mc.create_max_attempts)
        mc.create_task = asyncio.get_running_loop().create_task(
            self._run_create(mc, float(retry_delay_s), bool(stop_after)))

    def cancel_create(self, key):
        mc = self._get(key)
        if mc.create_task is None or mc.create_task.done():
            raise RuntimeError(f"cluster {key} is not being created")
        mc.create_task.cancel()
        mc.create_next_at = None
        self.hub.emit("cluster.create.canceled", cluster_id=mc.db_id,
                      cluster=mc.key, attempt=mc.create_attempt,
                      missing=len(mc.create_missing))

    async def _run_create_once(self, mc: ManagedCluster):
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
        except asyncio.CancelledError:
            raise
        except Exception as e:
            mc.create_log.append(f"[daemon] create failed to run: {e!r}")
            mc.create_rc = -1

    async def _run_create(self, mc: ManagedCluster, retry_delay_s, stop_after):
        while True:
            mc.create_attempt += 1
            mc.create_next_at = None
            mc.create_log.append(
                f"[daemon] provisioning attempt {mc.create_attempt}"
                f"/{mc.create_max_attempts}")
            await self._run_create_once(mc)
            # The setup scripts may regenerate the cluster YAML; re-read it.
            mc.vms = []
            vms = self._vms(mc)
            statuses = await asyncio.to_thread(mc.remote.vm_status, vms)
            mc.last_status = statuses
            mc.create_missing = [v for v in vms if v not in statuses]
            running = [v for v, s in statuses.items() if s == "RUNNING"]
            if stop_after and running:
                await asyncio.to_thread(mc.remote.vm_stop, running)
                mc.create_log.append(
                    f"[daemon] stopped {len(running)} freshly created VM(s)")
            self.hub.emit("cluster.create.attempt", cluster_id=mc.db_id,
                          cluster=mc.key, attempt=mc.create_attempt,
                          rc=mc.create_rc, missing=len(mc.create_missing))
            if not mc.create_missing or mc.create_attempt >= mc.create_max_attempts:
                break
            mc.create_next_at = time.time() + retry_delay_s
            mc.create_log.append(
                f"[daemon] {len(mc.create_missing)} VM(s) still missing; "
                f"next attempt in {int(retry_delay_s // 60)} min")
            await asyncio.sleep(retry_delay_s)
        self.hub.emit("cluster.create.finished", cluster_id=mc.db_id,
                      cluster=mc.key, rc=mc.create_rc,
                      attempts=mc.create_attempt,
                      missing=len(mc.create_missing))

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
        # Clusters may share VMs (main is the head of the n51 fleet); VMs
        # under another cluster's lease are managed, not stray.
        leased_elsewhere = set()
        for other in self.clusters.values():
            if other is not mc and other.task is not None and not other.task.done():
                leased_elsewhere.update(other.vms)
        running = sum(1 for v, s in statuses.items()
                      if s == "RUNNING" and v not in leased_elsewhere)
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

    def poll_interval(self, mc: ManagedCluster) -> float:
        if mc.create_task is not None and not mc.create_task.done():
            return self.POLL_TRANSITIONAL_S
        row = self.db.query_one("SELECT state FROM clusters WHERE id = ?",
                                (mc.db_id,))
        state = row["state"] if row else "terminated"
        if state in ("starting", "stopping"):
            return self.POLL_TRANSITIONAL_S
        if state in ("running", "unmanaged"):
            return self.POLL_RUNNING_S
        return self.POLL_IDLE_S

    async def status_poll_loop(self):
        due: dict[str, float] = {}
        while True:
            now = time.monotonic()
            for mc in self.clusters.values():
                if now < due.get(mc.key, 0.0):
                    continue
                try:
                    await self.poll_status(mc)
                except Exception as e:
                    self.hub.emit("cluster.error", cluster_id=mc.db_id,
                                  cluster=mc.key, error=f"status poll: {e!r}")
                due[mc.key] = time.monotonic() + self.poll_interval(mc)
            await asyncio.sleep(5)

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
                # What the session is paying for: the VMs the keep-alive
                # covers, not the whole config -- a subset lease starts fewer.
                held = mc.keepalive.vms if mc.keepalive is not None else mc.vms
                burn = mc.hourly_usd * len(held)
            elif mc.hourly_usd is not None and running:
                burn = mc.hourly_usd * running
            creating = mc.create_task is not None and not mc.create_task.done()
            try:
                est_hourly = self.estimate_hourly(mc.key)
            except Exception:
                est_hourly = None
            out.append({
                "key": mc.key, "project": mc.project, "name": mc.name,
                "state": row["state"] if row else "terminated",
                "state_updated_at": row["state_updated_at"] if row else None,
                "vm_count": len(mc.vms) if mc.vms else None,
                "vms_running": running if mc.last_status is not None else None,
                "vms": mc.last_status or None,
                "leases": leases,
                "active": mc.task is not None and not mc.task.done(),
                "hold_for": (mc.hold_for if time.time() < mc.hold_until
                             and not leases else None),
                "burn_usd_per_hr": burn,
                "est_usd_per_hr": est_hourly,   # whole-cluster rate if up
                "session_cost_usd": self._session_cost(mc),
                "create": ({
                    "running": creating,
                    "rc": mc.create_rc,
                    "attempt": mc.create_attempt,
                    "max_attempts": mc.create_max_attempts,
                    "missing": mc.create_missing,
                    "next_at": mc.create_next_at,
                    "log_tail": mc.create_log[-200:],
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
                if live:
                    mc.hold_until = 0.0
                    mc.hold_for = None
                elif time.time() < mc.hold_until:
                    continue    # handing over to the next job
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
            # The last poll ran mid-lease; re-read so the card doesn't
            # keep showing the VMs as up until the next poll.
            try:
                mc.last_status = await asyncio.to_thread(mc.remote.vm_status, vms)
            except Exception:
                pass

    async def shutdown_all(self):
        """Daemon exit: stop every cluster this daemon is keeping alive."""
        for mc in self.clusters.values():
            if mc.task is not None and not mc.task.done():
                try:
                    await self._shutdown(mc, self._vms(mc), reason="daemon_exit")
                except Exception as e:
                    self.hub.emit("cluster.error", cluster_id=mc.db_id,
                                  cluster=mc.key, error=repr(e))
