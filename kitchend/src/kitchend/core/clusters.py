"""Cluster manager: daemon-owned keep-alive and cost sessions.

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
import socket
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from kitchen.cluster import (ClusterState, KeepAlive, arm_shutdown,
                             start_vms, stop_vms)
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
    # Who is using this cluster: a job id, or 'user' for a manual
    # bring-up. The daemon is the only thing that starts these VMs, so
    # one field says what a directory of TTL'd lease files used to.
    used_by: str | None = None
    session_id: int | None = None
    last_status: dict | None = None     # None until the first gcloud poll
    last_rearm: float = 0.0
    # What would not start last time. The next bring-up asks for these
    # first; a successful start clears them.
    short_vms: list[str] = field(default_factory=list)
    create_task: asyncio.Task | None = None
    create_log: list = field(default_factory=list)   # captured output lines
    # Creation run from inside a bring-up has no create_task to watch, but
    # its output is worth the same as a manual create's.
    creating_for_job: bool = False
    create_rc: int | None = None                     # last run's exit code
    create_attempt: int = 0
    create_max_attempts: int = 0
    create_missing: list = field(default_factory=list)   # VMs still absent
    create_next_at: float | None = None              # epoch of next attempt
    # The ssh jump host, for fleets too large to give every VM its own IAP
    # tunnel. It is a VM like any other: it costs money while it runs, so it
    # comes up with the lease and goes down with it. `jump_remote` reaches it
    # directly — a remote that proxies through the jump cannot start the jump.
    jump_vm: str | None = None
    jump_port: int = 0
    jump_remote: GCloudRemote | None = None
    jump_proc: object | None = None      # the `start-iap-tunnel` subprocess


def _why_create_failed(log) -> str:
    """The provisioner's own explanation, from its output.

    It prints a reason per VM under an ERROR banner ("no zone in us-central1
    has n4-standard-16"); the exit code alone says only that something went
    wrong.
    """
    for line in reversed(list(log or [])[-60:]):
        line = line.strip()
        if ": Failed to create" in line or "no zone in" in line:
            return line.split(": ", 1)[-1] if ": " in line else line
    return ""


def _probe_set(vms, short=()) -> list:
    """The VMs to ask for before starting the fleet: the ones that would not
    start last time. Whatever was short is the likeliest thing to still be
    short, and asking costs a couple of VM-minutes where starting everything
    to rediscover it costs hundreds. Empty on a first bring-up, which starts
    the fleet outright.
    """
    short = set(short)
    return [v for v in vms if v in short]


class ClusterManager:
    REARM_INTERVAL_S = 30 * 60
    TICK_S = 60
    # gcloud status poll cadence by what the cluster is doing: fast while
    # it changes, slow when nothing should be happening.
    POLL_TRANSITIONAL_S = 10    # starting, stopping, provisioning
    POLL_RUNNING_S = 60         # running or unmanaged: VMs are billing
    POLL_IDLE_S = 10 * 60       # terminated
    # How long a fleet stays up with no lease while its queue still has work.
    # Long enough to ride out a failed bring-up and its 120s retry, short
    # enough that a genuinely stalled queue does not idle a cluster for long.
    HOLD_FOR_QUEUE_S = 15 * 60

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
            for c in p.clusters:
                # A cluster may live in its own GCP project: a fleet rebuilt
                # elsewhere moves without dragging its siblings along.
                settings = RemoteSettings(
                    gcp_project=c.gcp_project or p.gcp_project,
                    tunnel_through_iap=p.tunnel_through_iap,
                )
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
                try:
                    platform, raw = platform_from_yaml(config_path)
                except OSError:
                    platform, raw = "gcloud", {}
                # Cluster management opens sessions too: arming the dead-man
                # switch is an ssh to every VM, and the keep-alive repeats it
                # every 30 minutes. On a 102-VM fleet that is one gcloud
                # interpreter per VM, so it reads the cluster's jump host from
                # the same YAML key the driver does — one place, one answer.
                remote = GCloudRemote(settings=settings,
                                      project=settings.gcp_project,
                                      tunnel_through_iap=settings.tunnel_through_iap,
                                      proxy_jump=raw.get("proxy_jump"),
                                      ssh_user=raw.get("ssh_user"),
                                      ssh_key_file=raw.get("ssh_key_file"))
                jump_vm = raw.get("proxy_jump_vm")
                jump_port = 0
                jump_remote = None
                if jump_vm:
                    _, _, hostport = str(raw.get("proxy_jump", "")).rpartition("@")
                    _, _, port = hostport.partition(":")
                    jump_port = int(port or 22)
                    # No proxy_jump here on purpose: this is the remote that
                    # starts and stops the jump host itself.
                    jump_remote = GCloudRemote(
                        settings=settings, project=settings.gcp_project,
                        tunnel_through_iap=settings.tunnel_through_iap)
                if platform == "docker":
                    compose = Path(raw.get("compose_file", "docker-compose.yml"))
                    if not compose.is_absolute():
                        compose = config_path.parent / compose
                    remote = DockerRemote(compose,
                                          project=raw.get("compose_project"))
                self.clusters[key] = ManagedCluster(
                    jump_vm=jump_vm, jump_port=jump_port,
                    jump_remote=jump_remote,
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

    def is_up(self, key: str) -> bool:
        """Are this cluster's VMs running — under one of our leases or not?
        A cluster somebody else started is still up, and a scheduler holding
        a cooldown against it should stop waiting."""
        try:
            mc = self._get(key)
        except (KeyError, ValueError):
            return False
        row = self.db.query_one("SELECT state FROM clusters WHERE id = ?",
                                (mc.db_id,))
        return bool(row) and row["state"] in ("running", "unmanaged")

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

    async def up(self, key, purpose="user", vms=None):
        """Bring the cluster up and mark who is using it.

        `vms` restricts the start to a subset (a job whose largest point
        addresses 22 of 102 VMs); None means all of them. Nothing here decides
        when the cluster stops: the scheduler knows the job list, so it says
        when to keep the fleet and when to stop it.
        """
        mc = self._get(key)
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
        mc.used_by = purpose
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
        # A VM that does not exist yet cannot be started: create the missing
        # ones first, so a lease is one operation — have this cluster up —
        # rather than the caller knowing whether it was ever provisioned.
        try:
            await self._ensure_jump(mc)
            missing = await self._create_missing(mc, vms)
        except Exception:
            # The lease is already held at this point: give it back, or every
            # retry leaks one and the cluster never leaves 'starting'.
            self._mark_bringup_failed(mc, fresh)
            raise
        if missing:
            self.hub.emit("cluster.created_missing", cluster_id=mc.db_id,
                          cluster=mc.key, vms=missing)

        # Ask before paying: the VMs that failed last time cost a couple of
        # VM-minutes to retry, where starting the fleet to rediscover the
        # same shortage costs hundreds.
        probe = _probe_set(vms, mc.short_vms) if fresh else []
        try:
            if probe and len(probe) < len(vms):
                self.hub.emit("cluster.probe", cluster_id=mc.db_id,
                              cluster=mc.key, vms=probe,
                              short_last_time=list(mc.short_vms))
                await asyncio.to_thread(start_vms, mc.remote, probe,
                                        drain_first=False, stop_on_partial=True)
            started = await asyncio.to_thread(start_vms, mc.remote, vms,
                                              drain_first=fresh,
                                              stop_on_partial=True)
        except Exception as e:
            mc.short_vms = list(getattr(e, "failed_vms", None) or mc.short_vms)
            self.hub.emit("cluster.error", cluster_id=mc.db_id,
                          cluster=mc.key, error=repr(e))
            self._mark_bringup_failed(mc, fresh)
            raise RuntimeError(
                f"cluster {key} failed to start (the VMs that did come "
                f"up were stopped again): {e}") from e
        mc.short_vms = []
        # Everything past the start can raise too -- acquire_lock() does, when
        # a previous keep-alive never let go -- and the lease is already held.
        # Without this the failed bring-up kept it: thirteen accumulated on
        # one cluster while every retry failed the same way.
        try:
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
                # The keep-alive must re-arm the dead-man timer on every VM
                # any live lease holds, including ones this lease just
                # started.
                if mc.keepalive is not None:
                    mc.keepalive.extend(vms)
                if started:
                    self.hub.emit("cluster.restarted", cluster_id=mc.db_id,
                                  cluster=mc.key, vms=started)
        except Exception as e:
            self.hub.emit("cluster.error", cluster_id=mc.db_id,
                          cluster=mc.key, error=repr(e))
            self._mark_bringup_failed(mc, fresh)
            raise
        return lease.info.id

    async def down(self, key, force=False):
        mc = self._get(key)
        vms = self._vms(mc)
        if mc.used_by is not None and not force:
            raise RuntimeError(f"cluster {key} is in use by {mc.used_by}")
        mc.used_by = None
        await self._shutdown(mc, vms, reason="user")

    def release(self, key, _lease_id=None):
        """Note that nobody is using the cluster. Does not stop it -- the
        scheduler decides that from the queue."""
        mc = self._get(key)
        if mc.used_by is not None:
            mc.used_by = None
            self.hub.emit("cluster.released", cluster_id=mc.db_id,
                          cluster=mc.key)

    def _mark_bringup_failed(self, mc: ManagedCluster, fresh):
        """A bring-up that did not get off the ground leaves nobody using the
        cluster, and the fleet already came down with it."""
        mc.used_by = None
        if fresh:
            self._set_db_state(mc, "terminated")
            # The fleet is going down, so the jump host goes with it: it is a
            # VM like any other and bills while it runs.
            if mc.jump_vm:
                asyncio.get_running_loop().create_task(self._stop_jump(mc))

    async def _create_missing(self, mc: ManagedCluster, vms) -> list:
        """Create any of `vms` that do not exist, and return their names.

        The provisioning script skips VMs that are already there, so this is
        the same command a manual create runs; it just fires only when
        something is actually absent.
        """
        if not mc.create_cmd:
            return []
        statuses = await asyncio.to_thread(mc.remote.vm_status, vms)
        missing = [v for v in vms if statuses.get(v, "NOT_FOUND") == "NOT_FOUND"]
        if not missing:
            return []
        # Keep the previous attempt's output until this one produces some:
        # clearing here left the UI showing an empty log for a failed create
        # whose reason had just been overwritten.
        mc.create_log = list(mc.create_log)[-200:]
        mc.creating_for_job = True
        self.hub.emit("cluster.creating", cluster_id=mc.db_id, cluster=mc.key,
                      vms=missing, reason="missing for a lease")
        # A created VM boots running. If the rest of the fleet cannot be
        # created, this bring-up is over and none of them are wanted, so
        # they are stopped here: the caller only hands back the lease, and
        # a VM nothing is holding still bills.
        try:
            try:
                await self._run_create_once(mc)
            finally:
                mc.creating_for_job = False
            after = await asyncio.to_thread(mc.remote.vm_status, vms)
            made = [v for v in missing if after.get(v, "NOT_FOUND") != "NOT_FOUND"]
            if len(made) < len(missing):
                still = sorted(set(missing) - set(made))
                # Carry the provisioner's own reason. "could not be created"
                # sent us hunting through a UI whose create log had already
                # been cleared by the next attempt, when the script had said
                # plainly: no zone in us-central1 has n4-standard-16.
                why = _why_create_failed(mc.create_log)
                raise RuntimeError(
                    f"cluster {mc.key} is missing {len(still)} VM(s) that could "
                    f"not be created: {', '.join(still[:6])}"
                    + (" …" if len(still) > 6 else "")
                    + (f" — {why}" if why else ""))
        except Exception:
            await self._stop_whole_cluster(mc)
            raise
        return made

    async def _ensure_jump(self, mc: ManagedCluster) -> None:
        """Bring the jump host up and open the tunnel through it.

        Ordered before anything that touches the fleet: every session on a
        jumped cluster goes through here, including the ssh that arms the
        dead-man switch, so a fleet started first would be unreachable.
        """
        if not mc.jump_vm or mc.jump_remote is None:
            return
        if mc.jump_proc is not None and mc.jump_proc.poll() is None:
            return                                   # already up
        self.hub.emit("cluster.jump.starting", cluster_id=mc.db_id,
                      cluster=mc.key, vm=mc.jump_vm, port=mc.jump_port)
        # start_vms arms its dead-man too: the jump is a VM the daemon
        # started, so it is protected like the rest of the fleet.
        await asyncio.to_thread(start_vms, mc.jump_remote, [mc.jump_vm])
        zone = await asyncio.to_thread(mc.jump_remote._resolve_zone, mc.jump_vm)
        cmd = ["gcloud", "compute", "start-iap-tunnel", mc.jump_vm, "22",
               f"--local-host-port=localhost:{mc.jump_port}",
               f"--zone={zone}"]
        if mc.jump_remote.project:
            cmd.append(f"--project={mc.jump_remote.project}")
        mc.jump_proc = subprocess.Popen(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # The tunnel is not usable the moment the process exists; wait for
        # the local port to accept, or the first ssh fails on a race.
        deadline = time.time() + 60
        while time.time() < deadline:
            if mc.jump_proc.poll() is not None:
                mc.jump_proc = None
                raise RuntimeError(
                    f"iap tunnel to {mc.jump_vm} exited immediately")
            try:
                with socket.create_connection(("127.0.0.1", mc.jump_port), 1):
                    self.hub.emit("cluster.jump.up", cluster_id=mc.db_id,
                                  cluster=mc.key, vm=mc.jump_vm)
                    return
            except OSError:
                await asyncio.sleep(1)
        await self._stop_jump(mc)
        raise RuntimeError(
            f"iap tunnel to {mc.jump_vm} did not accept on port "
            f"{mc.jump_port} within 60s")

    async def _stop_jump(self, mc: ManagedCluster) -> None:
        """Close the tunnel and stop the jump host. It bills like any other
        VM, so it does not outlive the lease that needed it."""
        if mc.jump_proc is not None:
            try:
                mc.jump_proc.terminate()
            except Exception:
                pass
            mc.jump_proc = None
        if not mc.jump_vm or mc.jump_remote is None:
            return
        try:
            await asyncio.to_thread(mc.jump_remote.vm_stop, [mc.jump_vm])
            self.hub.emit("cluster.jump.down", cluster_id=mc.db_id,
                          cluster=mc.key, vm=mc.jump_vm)
        except Exception as e:
            # Never mask a teardown error with the jump's; the dead-man
            # switch armed at start is the backstop.
            self.hub.emit("cluster.error", cluster_id=mc.db_id, cluster=mc.key,
                          error=f"could not stop jump host {mc.jump_vm}: {e!r}")

    async def _stop_whole_cluster(self, mc: ManagedCluster) -> None:
        """Bring the whole configured fleet down after a failed bring-up.

        Not just what this attempt created or started: every partial cleanup
        so far has left something behind -- created-but-unstarted, probed but
        not covered by the later failure, running before the call and so
        deliberately untouched. The cluster is not usable in a half state
        anyway, so the only cleanup with no gaps is all of it.

        Never replaces the bring-up's own error: a failure here is reported,
        and anything that will not stop is armed so it powers itself off.
        """
        vms = self._vms(mc)
        if not vms:
            return
        try:
            statuses = await asyncio.to_thread(mc.remote.vm_status, vms)
            live = [v for v in vms if statuses.get(v, "NOT_FOUND")
                    not in ("NOT_FOUND", "TERMINATED", "STOPPING")]
            if not live:
                return
            self.hub.emit("cluster.stopping_partial", cluster_id=mc.db_id,
                          cluster=mc.key, vms=live,
                          reason="bring-up failed; stopping the whole fleet")
            left = await asyncio.to_thread(mc.remote.vm_stop, live)
            if left:
                self.hub.emit("cluster.error", cluster_id=mc.db_id,
                              cluster=mc.key,
                              error=f"could not stop {len(left)} VM(s): "
                                    f"{', '.join(left)}; arming their timers")
                await asyncio.to_thread(arm_shutdown, mc.remote, left)
        except Exception as e:
            self.hub.emit("cluster.error", cluster_id=mc.db_id, cluster=mc.key,
                          error=f"cleanup after a failed bring-up: {e!r}")

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

    TRANSITIONAL_VM_STATES = ("STOPPING", "STAGING", "PROVISIONING", "SUSPENDING")

    def poll_interval(self, mc: ManagedCluster) -> float:
        if mc.create_task is not None and not mc.create_task.done():
            return self.POLL_TRANSITIONAL_S
        # What the VMs are doing outranks what the row says: a cluster marked
        # terminated whose VMs are still STOPPING is mid-transition, and
        # polling it every ten minutes leaves the UI showing VMs long gone.
        if any(st in self.TRANSITIONAL_VM_STATES
               for st in (mc.last_status or {}).values()):
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
            creating = (mc.creating_for_job
                        or (mc.create_task is not None
                            and not mc.create_task.done()))
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
                # Kept for API compatibility; one entry when in use.
                "leases": ([{"purpose": mc.used_by}] if mc.used_by else []),
                "used_by": mc.used_by,
                "active": mc.task is not None and not mc.task.done(),
                # Up with nobody using it means the scheduler is keeping it
                # for the next job, not that it leaked.
                "kept_for_next": bool(
                    mc.task is not None and not mc.task.done()
                    and mc.used_by is None),
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
        """Keep a cluster alive while it is up. It does not decide when the
        cluster dies.

        The scheduler knows the job list, so it knows whether the next job
        wants this fleet; it says so by calling stop(). Leases with TTLs, an
        expiry check and a hold to bridge hand-offs were all ways of guessing
        that from in here, and the guess was wrong: taking a lease cleared the
        hold and nothing re-armed it, so one failed bring-up -- a lease taken
        and released in seconds -- tore down a fleet the queue still needed.
        """
        vms = self._vms(mc)
        try:
            while True:
                await asyncio.sleep(self.TICK_S)
                # Nothing else notices a dead tunnel, and every session on a
                # jumped cluster goes through it -- including the re-arm below.
                if mc.jump_vm and (mc.jump_proc is None
                                   or mc.jump_proc.poll() is not None):
                    self.hub.emit("cluster.jump.lost", cluster_id=mc.db_id,
                                  cluster=mc.key, vm=mc.jump_vm)
                    await self._ensure_jump(mc)
                if time.monotonic() - mc.last_rearm >= self.REARM_INTERVAL_S:
                    await asyncio.to_thread(mc.keepalive.rearm)
                    mc.last_rearm = time.monotonic()
                    self.hub.emit("cluster.keepalive", cluster_id=mc.db_id,
                                  cluster=mc.key, vms=len(vms))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.hub.emit("cluster.error", cluster_id=mc.db_id,
                          cluster=mc.key, error=repr(e))
        finally:
            if mc.keepalive is not None:
                mc.keepalive.release_lock()
                mc.keepalive = None

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
            await self._stop_jump(mc)
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
