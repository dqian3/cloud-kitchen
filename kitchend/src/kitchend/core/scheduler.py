"""The queue: per-queue FIFO, one running job per queue, bounded attempts.

A job's queue key is spec.queue (or spec.cluster, or the project name), so two
jobs that would fight over the same cluster serialize while different clusters
run in parallel, capped by max_concurrent_queues.

A job stays one row from submit to done. Exit-code policy (the drivers'
existing contract):
  0 → done, ok
  2 → done, degraded: data was written; never retried automatically
  * → another attempt, resuming into the same run_dir so completed points are
      skipped, until max_attempts

A cluster that will not come up costs no attempt: nothing ran, so the job
stays waiting with its next attempt due, and the cluster is held down for
that delay so the rest of its queue does not probe it in turn.

Attempts stop early when two in a row die before finishing a point: that is a
broken environment, not a flaky run.
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
        self._cluster_jobs: dict[str, int] = {}       # leased cluster -> job_id
        # A cluster that failed to come up (a zone stockout) is held for the
        # waiting job's delay, so the queue behind it does not burn one
        # partial start per job discovering the same thing.
        self._cluster_cooldown: dict[str, float] = {}  # cluster key -> monotonic
        self._wake = asyncio.Event()
        self._stopped = False
        self._restore_cooldowns()
        # Only the front of the queue waits on a cluster (see jobs.reorder):
        # hold that invariant across a restart too.
        self.db.execute(
            "UPDATE jobs SET next_attempt_at = NULL WHERE state = ? AND id "
            "NOT IN (SELECT id FROM jobs WHERE state = ? "
            "        ORDER BY priority DESC, id ASC LIMIT 1)",
            (jobs.WAITING, jobs.WAITING))

    def _restore_cooldowns(self):
        """Rebuild the cluster cooldowns from the jobs a previous process
        left waiting, so a restart does not re-probe a cluster that has just
        failed to come up."""
        for row in self.db.query(
                "SELECT j.spec_json, p.name AS project, "
                "CAST((julianday(j.next_attempt_at) - julianday('now')) * 86400 "
                "AS INTEGER) AS left_s FROM jobs j "
                "JOIN projects p ON p.id = j.project_id "
                "WHERE j.state = ? AND j.next_attempt_at IS NOT NULL",
                (jobs.WAITING,)):
            cluster = (json.loads(row["spec_json"]) or {}).get("cluster")
            left = int(row["left_s"] or 0)
            if not cluster or left <= 0:
                continue
            key = f"{row['project']}/{cluster}"
            self._cluster_cooldown[key] = max(
                self._cluster_cooldown.get(key, 0), time.monotonic() + left)

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
            # next_attempt_at: a failed run waiting out its delay, or a
            # cluster that would not come up. Outlives a restart, unlike the
            # in-memory cooldown that holds the rest of the queue.
            "AND (j.next_attempt_at IS NULL "
            "     OR j.next_attempt_at <= datetime('now')) "
            "ORDER BY j.priority DESC, j.id ASC", (jobs.WAITING,)
        ):
            job = jobs.get(self.db, row["id"])
            gate = self._after_gate(job)
            if gate == "wait":
                continue
            if gate == "cancel":
                jobs.finish(self.db, self.hub, job["id"], jobs.CANCELED,
                            last_error=f"job {job['spec'].get('after')} it "
                                       "waited for did not produce data")
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

    def next_waiting(self):
        """The job that dispatches next (priority DESC, id ASC), or None."""
        row = self.db.query_one(
            "SELECT id FROM jobs WHERE state = ? "
            "ORDER BY priority DESC, id ASC LIMIT 1", (jobs.WAITING,))
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

        One row per job means there is no chain to walk: the dependency is
        ready when it is done and produced data, and a dependency that ended
        without data cancels the dependent — silently burning cluster money
        after a broken predecessor is the worse default; resubmit the
        canceled job to run it anyway.
        """
        after = job["spec"].get("after")
        if after is None:
            return "ready"
        dep = jobs.get(self.db, after)
        if dep is None:
            return "cancel"     # dangling reference
        if dep["state"] != jobs.DONE:
            return "wait"
        return "ready" if dep["outcome"] in (jobs.OK, jobs.DEGRADED) else "cancel"

    async def _execute(self, job, key):
        job_id = job["id"]
        try:
            project_cfg = self.config.project(job["project"])
            spec = dict(job["spec"])
            if not spec.get("run_dir"):
                # One directory per job: the first attempt names it, every
                # later one resumes into it.
                spec["run_dir"] = job.get("run_dir") or str(
                    jobs.default_run_dir(project_cfg, job_id))
                self.db.execute("UPDATE jobs SET run_dir = ? WHERE id = ?",
                                (spec["run_dir"], job_id))
            if job["attempts"]:
                # Every attempt after the first resumes: the points the last
                # one finished are on disk and must not be run again.
                spec["resume"] = True
            argv, cwd = jobs.build_command(project_cfg, spec)
        except (KeyError, ValueError) as e:
            jobs.finish(self.db, self.hub, job_id, jobs.FAILED, last_error=str(e))
            self.hub.emit("job.error", job_id=job_id, error=str(e))
            self._release(key, job_id)
            return

        # Daemon-managed cluster: lease it up before the driver spawns (the
        # driver assumes VMs are running), keep the lease renewed while the
        # job lives, release it after. The job stays `waiting` until the
        # driver actually spawns — a cluster coming up is the cluster's
        # business, and its state says so.
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
                error = f"cluster bring-up failed: {e!r}"
                self.hub.emit("job.error", job_id=job_id, error=error)
                delay = int(spec.get("retry_delay_secs",
                                     jobs.DEFAULT_RETRY_DELAY_SECS))
                self._cluster_cooldown[cluster_key] = time.monotonic() + delay
                self.hub.emit("cluster.cooldown", cluster=cluster_key,
                              job_id=job_id, delay_secs=delay)
                # No attempt is spent: nothing ran.
                jobs.wait_again(self.db, self.hub, job_id, delay, error)
                self._release(key, job_id)
                return
            self.hub.emit("job.cluster", job_id=job_id, cluster=cluster_key,
                          action="leased", lease=lease_id)
            if jobs.get(self.db, job_id)["state"] == jobs.DONE:
                # Canceled while the cluster was coming up.
                self.clusters.release(cluster_key, lease_id)
                self._release(key, job_id)
                return

        n = jobs.start_attempt(self.db, self.hub, job_id)
        jobs.set_state(self.db, self.hub, job_id, jobs.RUNNING)
        extender = None
        if lease_id is not None:
            extender = asyncio.get_running_loop().create_task(
                self._keep_lease(cluster_key, lease_id, ttl))
        self.hub.emit("job.command", job_id=job_id, argv=argv, cwd=str(cwd),
                      attempt=n)
        try:
            rc = await self.runner.run(
                job_id, argv, cwd,
                on_start=lambda pid: self.db.execute(
                    "UPDATE jobs SET pid = ? WHERE id = ?", (pid, job_id)),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # spawn failure (bad cwd, missing binary)
            jobs.end_attempt(self.db, job_id, error=repr(e))
            jobs.finish(self.db, self.hub, job_id, jobs.FAILED, last_error=repr(e))
            self.hub.emit("job.error", job_id=job_id, error=repr(e))
            self._release(key, job_id)
            return
        finally:
            if extender is not None:
                extender.cancel()
            if lease_id is not None:
                nxt = self.next_waiting()
                if nxt is not None and nxt["id"] != job_id \
                        and self._shares_cluster(job, nxt):
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
        current = jobs.get(self.db, job_id)
        points = ((current["progress"] or {}).get("points") or {}).get("done", 0)
        jobs.end_attempt(self.db, job_id, exit_code=rc, points=points)
        if current["state"] == jobs.DONE:      # canceled while it ran
            self._release(key, job_id)
            return
        if rc == 0:
            jobs.finish(self.db, self.hub, job_id, jobs.OK)
        elif rc == 2:
            jobs.finish(self.db, self.hub, job_id, jobs.DEGRADED)
        else:
            self._retry_or_fail(job_id, rc)
        self._release(key, job_id)

    def _retry_or_fail(self, job_id, exit_code):
        """A failed run gets another attempt in the same directory, unless it
        is out of attempts or the last two died before finishing a point — a
        missing toolchain or a bad config fails that way every time, and each
        attempt re-leases the cluster to learn the same thing."""
        job = jobs.get(self.db, job_id)
        error = f"exit code {exit_code}"
        if job["attempts"] >= job["max_attempts"]:
            jobs.finish(self.db, self.hub, job_id, jobs.FAILED,
                        last_error=f"{error}; out of attempts")
            return
        if self._dead_on_arrival(job_id):
            jobs.finish(self.db, self.hub, job_id, jobs.FAILED,
                        last_error=f"{error}; two attempts produced no points")
            self.hub.emit("job.gave_up", job_id=job_id,
                          reason="two attempts failed before their first point")
            return
        delay = int(job["spec"].get("retry_delay_secs",
                                    jobs.DEFAULT_RETRY_DELAY_SECS))
        jobs.wait_again(self.db, self.hub, job_id, delay, error)
        self.hub.emit("job.retry_scheduled", job_id=job_id, delay_secs=delay,
                      attempts_left=job["max_attempts"] - job["attempts"])

    def _dead_on_arrival(self, job_id) -> bool:
        """Did the last two attempts both end without a single point? A
        driver that reports no points at all (points NULL) is left alone."""
        rows = self.db.query(
            "SELECT points FROM job_attempts WHERE job_id = ? "
            "ORDER BY n DESC LIMIT 2", (job_id,))
        return len(rows) == 2 and all(r["points"] == 0 for r in rows)

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
        if job["state"] == jobs.DONE:
            return job["outcome"]
        if job["state"] == jobs.WAITING:
            # If a lease is coming up for it, _execute sees this and releases
            # it instead of spawning.
            jobs.finish(self.db, self.hub, job_id, jobs.CANCELED)
            return jobs.CANCELED
        jobs.end_attempt(self.db, job_id, error="canceled")
        jobs.finish(self.db, self.hub, job_id, jobs.CANCELED)
        await self.runner.cancel(job_id)
        return jobs.CANCELED

    def resubmit(self, job_id, resume=True) -> int:
        job = jobs.get(self.db, job_id)
        if job is None:
            raise KeyError(job_id)
        spec = dict(job["spec"])
        spec["resume"] = bool(resume)
        if resume and job.get("run_dir"):
            spec["run_dir"] = job["run_dir"]
        project_id = self.db.query_one(
            "SELECT project_id FROM jobs WHERE id = ?", (job_id,))["project_id"]
        new_id = jobs.submit(self.db, project_id, spec)
        self.hub.emit("job.state", job_id=new_id, state=jobs.WAITING,
                      resubmit_of=job_id)
        self.wake()
        return new_id
