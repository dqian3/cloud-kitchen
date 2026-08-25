"""The queue: per-queue FIFO, one running job per queue, bounded resume-retries.

A job's queue key is spec.queue (or spec.cluster, or the project name), so two
jobs that would fight over the same cluster serialize while different clusters
run in parallel, capped by max_concurrent_queues.

Exit-code policy (the drivers' existing contract):
  0 → succeeded
  2 → degraded: data was written; never auto-retried (resubmit-resume by hand)
  * → failed: retried up to max_retries, each retry resuming into the same
      run_dir so completed points are skipped.
"""

import asyncio
import json
import time

from . import jobs


class Scheduler:
    def __init__(self, config, db, hub, runner, clusters=None):
        self.config = config
        self.db = db
        self.hub = hub
        self.runner = runner
        self.clusters = clusters                      # ClusterManager, for
        self._tasks: dict[int, asyncio.Task] = {}     # spec.cluster leases
        self._running_queues: dict[str, int] = {}     # queue key -> job_id
        self._requeues: dict[int, asyncio.Task] = {}  # failed job -> retry timer
        self._cluster_jobs: dict[str, int] = {}       # leased cluster -> job_id
        # A cluster that failed to come up (a zone stockout) is held for the
        # failed job's retry delay, so the queue behind it does not burn one
        # partial start per job discovering the same thing.
        self._cluster_cooldown: dict[str, float] = {}  # cluster key -> monotonic
        self._wake = asyncio.Event()
        self._stopped = False
        # A retry timer lives only in the process that started it: failed
        # jobs still showing retries after a restart will never requeue, and
        # an `after` gate waiting on them would wait forever.
        self.db.execute(
            "UPDATE jobs SET retries_left = 0, retry_at = NULL "
            "WHERE state = ? AND retries_left > 0", (jobs.FAILED,))

    def wake(self):
        self._wake.set()

    async def loop(self):
        while not self._stopped:
            try:
                self._dispatch()
            except Exception as e:  # keep the scheduler alive on any bug
                self.hub.emit("scheduler.error", error=repr(e))
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    def stop(self):
        self._stopped = True
        self._wake.set()

    # --- dispatch ---

    def _dispatch(self):
        if len(self._running_queues) >= self.config.max_concurrent_queues:
            return
        for row in self.db.query(
            "SELECT j.id FROM jobs j WHERE j.state = ? "
            "ORDER BY j.priority DESC, j.id ASC", (jobs.QUEUED,)
        ):
            job = jobs.get(self.db, row["id"])
            gate = self._after_gate(job)
            if gate == "wait":
                continue
            if gate == "cancel":
                jobs.set_state(self.db, self.hub, job["id"], jobs.CANCELED,
                               finished_at=jobs._now(self.db))
                self.hub.emit("job.dependency_canceled", job_id=job["id"],
                              after=job["spec"].get("after"))
                continue
            key = jobs.queue_key(job)
            if key in self._running_queues:
                continue
            # Clusters that share VMs (main inside n51) must not be leased
            # by two jobs at once, whatever queues they sit on.
            cluster_key = (f"{job['project']}/{job['spec']['cluster']}"
                           if job["spec"].get("cluster") else None)
            if cluster_key and self.clusters is not None and any(
                    self.clusters.overlaps(cluster_key, held)
                    for held in self._cluster_jobs):
                continue
            if cluster_key and self._cooling(cluster_key):
                continue
            if cluster_key:
                self._cluster_jobs[cluster_key] = job["id"]
            self._running_queues[key] = job["id"]
            self._tasks[job["id"]] = asyncio.get_running_loop().create_task(
                self._execute(job, key))
            if len(self._running_queues) >= self.config.max_concurrent_queues:
                break

    def _cooling(self, cluster_key) -> bool:
        """Is this cluster, or one sharing its VMs, inside a bring-up
        cooldown? Expired entries are dropped as they are seen."""
        now = time.monotonic()
        for key, until in list(self._cluster_cooldown.items()):
            if until <= now:
                del self._cluster_cooldown[key]
                continue
            overlaps = getattr(self.clusters, "overlaps", None)
            if key == cluster_key or (overlaps and overlaps(cluster_key, key)):
                return True
        return False

    def next_queued(self):
        """The job that dispatches next (priority DESC, id ASC), or None."""
        row = self.db.query_one(
            "SELECT id FROM jobs WHERE state = ? "
            "ORDER BY priority DESC, id ASC LIMIT 1", (jobs.QUEUED,))
        return jobs.get(self.db, row["id"]) if row else None

    def _shares_cluster(self, a, b) -> bool:
        ca, cb = a["spec"].get("cluster"), b["spec"].get("cluster")
        if not ca or not cb or a["project"] != b["project"]:
            return False
        if ca == cb:
            return True
        return self.clusters is not None and self.clusters.overlaps(
            f"{a['project']}/{ca}", f"{b['project']}/{cb}")

    def _after_gate(self, job) -> str:
        """'ready' | 'wait' | 'cancel' for a job's `after` dependency.

        The dependency is judged at the end of the predecessor's retry chain:
        a failed job whose auto-retry is still queued means "wait", and a
        chain that ultimately produced data (succeeded/degraded) satisfies
        the dependent even though the first attempt failed. A chain that
        ended failed/canceled/interrupted cancels the dependent — silently
        burning cluster money after a broken predecessor is the worse
        default; resubmit the canceled job to run it anyway.
        """
        after = job["spec"].get("after")
        if after is None:
            return "ready"
        current = after
        while True:
            row = self.db.query_one(
                "SELECT id FROM jobs WHERE parent_job_id = ? "
                "ORDER BY id DESC LIMIT 1", (current,))
            if row is None:
                break
            current = row["id"]
        dep = jobs.get(self.db, current)
        if dep is None:
            return "cancel"     # dangling reference
        if dep["state"] in (jobs.SUCCEEDED, jobs.DEGRADED):
            return "ready"
        if dep["state"] in jobs.ACTIVE_STATES:
            return "wait"
        # failed / canceled / interrupted — but a scheduled retry may not
        # have inserted its row yet; retries_left > 0 on a failure means one
        # is coming, so keep waiting rather than canceling into the gap.
        if dep["state"] == jobs.FAILED and dep["retries_left"] > 0:
            return "wait"
        return "cancel"

    async def _execute(self, job, key):
        job_id = job["id"]
        try:
            project_cfg = self.config.project(job["project"])
            spec = dict(job["spec"])
            if not spec.get("run_dir"):
                spec["run_dir"] = str(jobs.default_run_dir(project_cfg, job_id))
                self.db.execute("UPDATE jobs SET run_dir = ? WHERE id = ?",
                                (spec["run_dir"], job_id))
            argv, cwd = jobs.build_command(project_cfg, spec)
        except (KeyError, ValueError) as e:
            jobs.set_state(self.db, self.hub, job_id, jobs.FAILED,
                           finished_at=jobs._now(self.db))
            self.hub.emit("job.error", job_id=job_id, error=str(e))
            self._release(key, job_id)
            return

        # Dispatched, but not running until the driver actually spawns: a
        # cluster coming up (or failing to) is 'starting'.
        jobs.set_state(self.db, self.hub, job_id, jobs.STARTING,
                       run_dir=spec.get("run_dir"))

        # Daemon-managed cluster: lease it up before the driver spawns (the
        # driver assumes VMs are running), keep the lease renewed while the
        # job lives, release it after. The tick loop stops the cluster ~a
        # minute after the last lease goes, so a chained job on the same
        # cluster picks the lease up without a VM cycle in between.
        cluster_key = lease_id = None
        ttl = int(spec.get("cluster_ttl_minutes", 60))
        if spec.get("cluster") and self.clusters is not None:
            cluster_key = f"{job['project']}/{spec['cluster']}"
            self.hub.emit("job.cluster", job_id=job_id, cluster=cluster_key,
                          action="acquiring")
            try:
                # A job that names its hosts (a committee sweep addressing
                # part of the fleet) leases only those; the rest stay down.
                subset = {"vms": spec["hosts"]} if spec.get("hosts") else {}
                lease_id = await self.clusters.up(cluster_key, ttl,
                                                  purpose=f"job-{job_id}",
                                                  **subset)
            except Exception as e:
                self.hub.emit("job.error", job_id=job_id,
                              error=f"cluster bring-up failed: {e!r}")
                delay = int(spec.get("retry_delay_secs", 600))
                self._cluster_cooldown[cluster_key] = time.monotonic() + delay
                self.hub.emit("cluster.cooldown", cluster=cluster_key,
                              job_id=job_id, delay_secs=delay)
                self._fail(job_id)
                self._release(key, job_id)
                return
            self.hub.emit("job.cluster", job_id=job_id, cluster=cluster_key,
                          action="leased", lease=lease_id)
            if jobs.get(self.db, job_id)["state"] == jobs.CANCELED:
                # Canceled while the cluster was coming up.
                self.clusters.release(cluster_key, lease_id)
                self._release(key, job_id)
                return

        jobs.set_state(self.db, self.hub, job_id, jobs.RUNNING,
                       started_at=jobs._now(self.db))
        extender = None
        if lease_id is not None:
            extender = asyncio.get_running_loop().create_task(
                self._keep_lease(cluster_key, lease_id, ttl))
        self.hub.emit("job.command", job_id=job_id, argv=argv, cwd=str(cwd))
        try:
            rc = await self.runner.run(
                job_id, argv, cwd,
                on_start=lambda pid: self.db.execute(
                    "UPDATE jobs SET pid = ? WHERE id = ?", (pid, job_id)),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # spawn failure (bad cwd, missing binary)
            jobs.set_state(self.db, self.hub, job_id, jobs.FAILED,
                           finished_at=jobs._now(self.db))
            self.hub.emit("job.error", job_id=job_id, error=repr(e))
            self._release(key, job_id)
            return
        finally:
            if extender is not None:
                extender.cancel()
            if lease_id is not None:
                nxt = self.next_queued()
                if nxt is not None and self._shares_cluster(job, nxt):
                    # The head of the queue runs on these VMs: keep them up
                    # for it rather than stop now and start again.
                    self.clusters.hold(cluster_key, 600, for_job=nxt["id"])
                    self.hub.emit("job.lease_handoff", job_id=job_id,
                                  cluster=cluster_key, to_job=nxt["id"])
                try:
                    self.clusters.release(cluster_key, lease_id)
                except Exception as e:
                    self.hub.emit("job.error", job_id=job_id,
                                  error=f"lease release failed: {e!r}")

        self._finish(job, key, rc)

    async def _keep_lease(self, cluster_key, lease_id, ttl_minutes):
        """Renew a job's cluster lease at half-TTL cadence while it runs, so
        a long job outlives its initial TTL but a dead daemon still lets the
        lease lapse within one TTL."""
        interval = max(60, ttl_minutes * 30)   # half the TTL, in seconds
        while True:
            await asyncio.sleep(interval)
            try:
                self.clusters.extend(cluster_key, lease_id, ttl_minutes)
            except Exception:
                return   # lease vanished (cluster forced down); stop renewing

    def _finish(self, job, key, rc):
        job_id = job["id"]
        now = jobs._now(self.db)
        current = jobs.get(self.db, job_id)
        if current["state"] == jobs.CANCELED:
            self._release(key, job_id)
            return
        if rc == 0:
            jobs.set_state(self.db, self.hub, job_id, jobs.SUCCEEDED,
                           exit_code=rc, finished_at=now)
        elif rc == 2:
            jobs.set_state(self.db, self.hub, job_id, jobs.DEGRADED,
                           exit_code=rc, finished_at=now)
        else:
            self._fail(job_id, exit_code=rc)
        self._release(key, job_id)

    def _fail(self, job_id, exit_code=None):
        """Mark a job failed and, if it has retries left, schedule one. Used
        for a non-zero exit and for a cluster that would not come up (a zone
        stockout clears with time, which is what the delay is for)."""
        current = jobs.get(self.db, job_id)
        now = jobs._now(self.db)
        jobs.set_state(self.db, self.hub, job_id, jobs.FAILED,
                       exit_code=exit_code, finished_at=now)
        retries_left = current["retries_left"]
        if retries_left <= 0:
            return
        delay = int(current["spec"].get("retry_delay_secs", 600))
        self.db.execute(
            "UPDATE jobs SET retry_at = datetime('now', ?) WHERE id = ?",
            (f"+{delay} seconds", job_id))
        self.hub.emit("job.retry_scheduled", job_id=job_id,
                      delay_secs=delay, retries_left=retries_left - 1)
        # `current`, not the dispatch-time snapshot: that predates the
        # daemon-assigned run_dir, and the retry must resume into it.
        self._requeues[job_id] = asyncio.get_running_loop().create_task(
            self._requeue_after(current, delay, retries_left - 1))

    async def _requeue_after(self, job, delay, retries_left):
        try:
            await asyncio.sleep(delay)
        finally:
            self._requeues.pop(job["id"], None)
            self.db.execute("UPDATE jobs SET retry_at = NULL WHERE id = ?",
                            (job["id"],))
        spec = dict(job["spec"])
        spec["resume"] = True
        if job.get("run_dir"):
            spec["run_dir"] = job["run_dir"]
        spec["max_retries"] = retries_left
        new_id = self.db.insert(
            "INSERT INTO jobs (project_id, spec_json, run_dir, state, priority, "
            "retries_left, parent_job_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (job["project_id"], json.dumps(spec), spec.get("run_dir"),
             jobs.QUEUED, job["priority"], retries_left, job["id"]),
        )
        self.hub.emit("job.state", job_id=new_id, state=jobs.QUEUED,
                      parent_job_id=job["id"])
        self.wake()

    def _release(self, key, job_id):
        if self._running_queues.get(key) == job_id:
            del self._running_queues[key]
        for ck, jid in list(self._cluster_jobs.items()):
            if jid == job_id:
                del self._cluster_jobs[ck]
        self._tasks.pop(job_id, None)
        self.wake()

    # --- user actions ---

    async def cancel(self, job_id) -> str:
        job = jobs.get(self.db, job_id)
        if job is None:
            raise KeyError(job_id)
        if job["state"] == jobs.QUEUED:
            jobs.set_state(self.db, self.hub, job_id, jobs.CANCELED,
                           finished_at=jobs._now(self.db))
            return jobs.CANCELED
        if job["state"] == jobs.STARTING:
            # The dispatch coroutine sees this after the lease returns and
            # releases it instead of spawning.
            jobs.set_state(self.db, self.hub, job_id, jobs.CANCELED,
                           finished_at=jobs._now(self.db))
            return jobs.CANCELED
        if job["state"] == jobs.RUNNING:
            jobs.set_state(self.db, self.hub, job_id, jobs.CANCELED)
            await self.runner.cancel(job_id)
            self.db.execute("UPDATE jobs SET finished_at = datetime('now') "
                            "WHERE id = ?", (job_id,))
            return jobs.CANCELED
        if job["state"] == jobs.FAILED and job["retries_left"] > 0:
            # Failed but a retry is pending: cancel the timer and end the
            # chain here, so nothing requeues and `after` gates release.
            timer = self._requeues.pop(job_id, None)
            if timer is not None:
                timer.cancel()
            self.db.execute("UPDATE jobs SET retries_left = 0, retry_at = NULL "
                            "WHERE id = ?", (job_id,))
            jobs.set_state(self.db, self.hub, job_id, jobs.CANCELED)
            return jobs.CANCELED
        return job["state"]

    def resubmit(self, job_id, resume=True) -> int:
        job = jobs.get(self.db, job_id)
        if job is None:
            raise KeyError(job_id)
        spec = dict(job["spec"])
        spec["resume"] = bool(resume)
        if resume and job.get("run_dir"):
            spec["run_dir"] = job["run_dir"]
        new_id = self.db.insert(
            "INSERT INTO jobs (project_id, spec_json, run_dir, state, priority, "
            "retries_left, parent_job_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (job["project_id"], json.dumps(spec), spec.get("run_dir"),
             jobs.QUEUED, job["priority"], int(spec.get("max_retries", 0)),
             job["id"]),
        )
        self.hub.emit("job.state", job_id=new_id, state=jobs.QUEUED,
                      parent_job_id=job["id"])
        self.wake()
        return new_id
