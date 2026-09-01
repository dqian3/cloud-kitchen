"""The queue: one job at a time, in priority order, with bounded attempts.

One queue, not one per cluster: the clusters share VMs, so a second job could
not run anyway, and a queue each meant two heads retrying the same stockout on
their own timers.

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
import time

from . import jobs, ledger


class Scheduler:
    def __init__(self, config, db, hub, runner, clusters=None):
        self.config = config
        self.db = db
        self.hub = hub
        self.runner = runner
        self.clusters = clusters                      # ClusterManager, for
        self._tasks: dict[int, asyncio.Task] = {}
        self._running: int | None = None               # the job that owns it
        self._bringing_up: str | None = None           # its cluster, if any
        # A job that could not have its cluster waits before trying again.
        # Only the head runs, so the wait is the head's: nothing else was
        # going to probe anyway, and a restart costs one attempt.
        self._wait_until: dict[int, float] = {}        # job id -> monotonic
        self._wake = asyncio.Event()
        self._stopped = False
        last_pause = self.db.query_one(
            "SELECT type FROM events "
            "WHERE type IN ('scheduler.paused', 'scheduler.resumed') "
            "ORDER BY id DESC LIMIT 1")
        self._paused = bool(last_pause and last_pause["type"] == "scheduler.paused")

    # --- pause ---
    #
    # The latest pause/resume event is restored at startup. This keeps a
    # daemon restart from dispatching work before the operator can pause it
    # again, without adding a second source of queue state.

    def paused(self) -> bool:
        return self._paused

    def set_paused(self, value: bool):
        self._paused = bool(value)
        self.hub.emit("scheduler.paused" if value else "scheduler.resumed")
        self.wake()

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
        """Run the front of the queue, if anything can run.

        One queue, one job at a time. The head owns the daemon: if it is
        waiting out a failed attempt, nothing else tries — the clusters share
        VMs, so a job behind it would only ask the same question.
        """
        if self._running is not None:
            return
        # Paused: finish what is running, start nothing. Cluster polling, the
        # ledger and the UI all keep working.
        if self.paused():
            return
        for row in self.db.query(
            "SELECT j.id FROM jobs j WHERE j.state = ? "
            "ORDER BY j.priority DESC, j.id ASC", (jobs.WAITING,)
        ):
            job = jobs.get(self.db, row["id"])
            if self._waiting(job["id"]):
                return          # the head is waiting: so is everything else
            self._running = job["id"]
            self._bringing_up = (f"{job['project']}/{job['spec']['cluster']}"
                                 if job["spec"].get("cluster") else None)
            self._tasks[job["id"]] = asyncio.get_running_loop().create_task(
                self._execute(job))
            return

    def bringing_up(self, job_id):
        """The cluster this job is waiting on the daemon to bring up, if it
        is the one doing so. The job row stays `waiting` -- nothing has run
        and no attempt is spent -- and the UI renders this as `starting`."""
        if self._running != job_id:
            return None
        # Only while it is actually getting the cluster: once the driver
        # spawns, the job is running and the cluster is just up.
        job = jobs.get(self.db, job_id)
        return self._bringing_up if job and job["state"] == jobs.WAITING else None

    def wait_seconds(self, job_id):
        """Seconds until this job may try again, or None if it may now."""
        until = self._wait_until.get(job_id)
        if until is None:
            return None
        left = until - time.monotonic()
        return round(left) if left > 0 else None

    def display_state(self, job) -> str:
        """The queue state clients should show, derived from scheduler truth."""
        if job["state"] == jobs.DONE:
            return job["outcome"]
        if self.bringing_up(job["id"]):
            return "starting"
        if job["state"] == jobs.RUNNING:
            return "running"
        if self._waiting(job["id"]):
            return "retrying"
        return "queued"

    def _waiting(self, job_id) -> bool:
        until = self._wait_until.get(job_id)
        if until is None:
            return False
        if until <= time.monotonic():
            del self._wait_until[job_id]
            return False
        return True

    def _now(self) -> str:
        return self.db.query_one("SELECT datetime('now') AS t")["t"]

    def _note(self, job, reason) -> None:
        """Record why a job is sitting in the queue, without touching its
        attempts. Written only when it changes: dispatch runs every 5s."""
        if job.get("last_error") != reason:
            self.db.execute("UPDATE jobs SET last_error = ? WHERE id = ?",
                            (reason, job["id"]))
            self.hub.emit("job.blocked", job_id=job["id"], reason=reason)

    def next_waiting(self):
        """The head of the queue, or None.

        Read by the cluster keep-alive to decide whether the fleet now in use
        is the one the next job wants. The loop only guards against a job
        deleted between the query and the read.
        """
        for row in self.db.query(
            "SELECT id FROM jobs WHERE state = ? "
            "ORDER BY priority DESC, id ASC", (jobs.WAITING,)
        ):
            job = jobs.get(self.db, row["id"])
            if job is not None:
                return job
        return None

    def _outranked_by(self, job):
        """The job that should hold the queue slot instead of `job`, or None.

        Only asked in the window between claiming the slot and spawning the
        driver, while a cluster comes up. In that window the job row is still
        `waiting`, no attempt has been spent and nothing has run, so a reorder
        made during a bring-up can still take effect — the one moment where
        changing the queue order is free. Once the driver spawns the order is
        settled until the job ends.

        Uses `_dispatch`'s rule, so the slot only changes hands to a job that
        could actually take it: a retry-delayed job ahead of us stops the scan
        the way it stops dispatch — if nothing would run, there is no reason
        to give the fleet up.
        """
        for row in self.db.query(
            "SELECT j.id FROM jobs j WHERE j.state = ? "
            "ORDER BY j.priority DESC, j.id ASC", (jobs.WAITING,)
        ):
            if row["id"] == job["id"]:
                return None                     # still the head
            other = jobs.get(self.db, row["id"])
            if other is None:
                continue
            if self._waiting(other["id"]):
                return None
            return other
        return None

    def _shares_cluster(self, a, b) -> bool:
        """Would the next job use the fleet this one is finishing with?

        Same cluster, nothing subtler. Overlapping-but-different clusters
        (main's VMs are a subset of n51's) used to count, from when several
        holders could lease at once; now the next job brings up whichever
        cluster it names, so keeping a different one up buys nothing.
        """
        ca, cb = a["spec"].get("cluster"), b["spec"].get("cluster")
        return bool(ca) and ca == cb and a["project"] == b["project"]

    async def _execute(self, job):
        job_id = job["id"]
        try:
            await self._run(job)
        finally:
            # The queue slot goes back whatever happened — including a
            # job deleted mid-dispatch, whose bookkeeping raises. One
            # slot never returned stops the scheduler entirely.
            self._release(job_id)

    async def _run(self, job):
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
            return

        # Daemon-managed cluster: lease it up before the driver spawns (the
        # driver assumes VMs are running), keep the lease renewed while the
        # job lives, release it after. The job stays `waiting` until the
        # driver actually spawns — a cluster coming up is the cluster's
        # business, and its state says so.
        cluster_key = None
        using_cluster = False
        if spec.get("cluster") and self.clusters is not None:
            cluster_key = f"{job['project']}/{spec['cluster']}"
            self.hub.emit("job.cluster", job_id=job_id, cluster=cluster_key,
                          action="acquiring")
            try:
                # A job that names its hosts (a committee sweep addressing
                # part of the fleet) leases only those; the rest stay down.
                subset = {"vms": spec["hosts"]} if spec.get("hosts") else {}
                await self.clusters.up(cluster_key,
                                       purpose=f"job-{job_id}", **subset)
                using_cluster = True
            except Exception as e:
                error = f"cluster bring-up failed: {e!r}"
                self.hub.emit("job.error", job_id=job_id, error=error)
                delay = int(spec.get("retry_delay_secs",
                                     jobs.DEFAULT_RETRY_DELAY_SECS))
                # No attempt is spent: nothing ran.
                self._wait_until[job_id] = time.monotonic() + delay
                self.hub.emit("job.waiting", job_id=job_id, delay_secs=delay,
                              cluster=cluster_key)
                jobs.wait_again(self.db, self.hub, job_id, error)
                return
            self.hub.emit("job.cluster", job_id=job_id, cluster=cluster_key,
                          action="acquired")
            if jobs.get(self.db, job_id)["state"] == jobs.DONE:
                # Canceled while the cluster was coming up.
                await self._handoff_cluster(job, cluster_key)
                return
            # Reordered while the cluster came up. Bring-up is minutes on a
            # real fleet, and for all of it this job has done nothing but hold
            # the slot — so hand it over rather than spending the fleet on a
            # job the operator has since demoted. _handoff_cluster keeps the
            # VMs up when the new head wants the same cluster, which is the
            # usual case, so the swap costs nothing.
            ahead = self._outranked_by(job)
            if ahead is not None:
                self._note(job, f"yielded the queue slot to job {ahead['id']}: "
                                f"reordered while {cluster_key} was coming up")
                self.hub.emit("job.preempted", job_id=job_id,
                              by_job=ahead["id"], cluster=cluster_key)
                await self._handoff_cluster(job, cluster_key)
                return

        self._wait_until.pop(job_id, None)
        n = jobs.start_attempt(self.db, self.hub, job_id)
        jobs.set_state(self.db, self.hub, job_id, jobs.RUNNING)
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
            return
        else:
            # Record success/failure/retry before deciding the cluster
            # handoff. A failed job becomes WAITING here, so it can hand the
            # still-running cluster to its own next attempt instead of being
            # mistaken for a queue with no successor and stopping the VMs.
            self._finish(job, rc)
        finally:
            if using_cluster:
                await self._handoff_cluster(job, cluster_key)

    async def _handoff_cluster(self, job, cluster_key):
        """Release one job's ownership and keep the fleet for its successor."""
        job_id = job["id"]
        try:
            self.clusters.release(cluster_key)
        except Exception as e:
            self.hub.emit("job.error", job_id=job_id,
                          error=f"lease release failed: {e!r}")
        # The queue is known, so say what happens to the fleet rather than
        # leaving a timer to infer it. This also covers cancellation while
        # acquisition was in flight: acquiring a fleet must not bypass the
        # same-cluster handoff just because the driver never spawned.
        nxt = self.next_waiting()
        keep = nxt is not None and self._shares_cluster(job, nxt)
        if keep:
            self.hub.emit("job.cluster", job_id=job_id,
                          cluster=cluster_key, action="kept",
                          for_job=nxt["id"])
            return
        self.hub.emit("job.cluster", job_id=job_id,
                      cluster=cluster_key, action="stopping")
        try:
            await self.clusters.down(cluster_key, force=True)
        except Exception as e:
            self.hub.emit("job.error", job_id=job_id,
                          error=f"cluster stop failed: {e!r}")

    def _finish(self, job, rc):
        job_id = job["id"]
        current = jobs.get(self.db, job_id)
        if current is None:            # deleted while it ran; nothing to record
            return
        points = ((current["progress"] or {}).get("points") or {}).get("done", 0)
        jobs.end_attempt(self.db, job_id, exit_code=rc, points=points)
        if current["state"] == jobs.DONE:      # canceled while it ran
            return
        if rc == 0:
            jobs.finish(self.db, self.hub, job_id, jobs.OK)
        elif rc == 2:
            jobs.finish(self.db, self.hub, job_id, jobs.DEGRADED)
        else:
            self._retry_or_fail(job_id, rc)

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
        self._wait_until[job_id] = time.monotonic() + delay
        jobs.wait_again(self.db, self.hub, job_id, error)
        self.hub.emit("job.retry_scheduled", job_id=job_id, delay_secs=delay,
                      attempts_left=job["max_attempts"] - job["attempts"])

    def _dead_on_arrival(self, job_id) -> bool:
        """Did the last two attempts both end without a single point? A
        driver that reports no points at all (points NULL) is left alone."""
        rows = self.db.query(
            "SELECT points FROM job_attempts WHERE job_id = ? "
            "ORDER BY n DESC LIMIT 2", (job_id,))
        return len(rows) == 2 and all(r["points"] == 0 for r in rows)

    def _release(self, job_id):
        if self._running == job_id:
            self._running = None
            self._bringing_up = None
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

    def retry_now(self, job_id) -> dict:
        """Try a waiting job again immediately.

        Drops the wait its last attempt earned. Asking explicitly is a
        statement that
        the condition behind the wait has changed (capacity returned, a fleet
        was rebuilt), which the daemon cannot know on its own.
        """
        job = jobs.get(self.db, job_id)
        if job is None:
            raise KeyError(job_id)
        if job["state"] != jobs.WAITING:
            raise ValueError(f"job {job_id} is {job['state']}, not waiting")
        # A job stays WAITING while its cluster is coming up, so the state
        # check alone is not enough: without this, asking to retry during an
        # attempt cleared a wait that was not set and reported success while
        # changing nothing.
        if self._running == job_id:
            raise ValueError(
                f"job {job_id} is already trying: its cluster is coming up")
        self._wait_until.pop(job_id, None)
        self.hub.emit("job.retry_now", job_id=job_id)
        self.wake()
        return {"id": job_id}

    def resubmit(self, job_id, resume=True) -> int:
        job = jobs.get(self.db, job_id)
        if job is None:
            raise KeyError(job_id)
        spec = dict(job["spec"])
        spec["resume"] = bool(resume)
        if resume:
            if not job.get("run_dir"):
                raise ValueError(f"job {job_id} has no run directory to resume")
            spec["run_dir"] = job["run_dir"]
        else:
            # A source job may itself be a resumed job whose stored spec owns
            # a run_dir. Fresh means fresh: let `_run` allocate a new one.
            spec.pop("run_dir", None)
        project_id = self.db.query_one(
            "SELECT project_id FROM jobs WHERE id = ?", (job_id,))["project_id"]
        new_id = jobs.submit(self.db, project_id, spec)
        self.hub.emit("job.state", job_id=new_id, state=jobs.WAITING,
                      resubmit_of=job_id)
        self.wake()
        return new_id

    def add_trials(self, run_id: int, trials: int) -> dict:
        """Queue additional trial numbers into an existing result directory."""
        if trials < 1:
            raise ValueError("trials must be >= 1")
        run = ledger.get_run(self.db, run_id)
        if run is None:
            raise KeyError(run_id)
        if not run.get("dir_exists"):
            raise ValueError(f"run {run_id} no longer has a result directory")
        source_id = run.get("job_id")
        source = jobs.get(self.db, source_id) if source_id is not None else None
        if source is None or not source.get("run_dir"):
            raise ValueError(
                f"run {run_id} has no source job to resume; scanned results "
                "cannot have trials added automatically")
        if source["state"] != jobs.DONE:
            raise ValueError(f"run {run_id}'s job {source_id} is still {source['state']}")
        experiments = source["spec"].get("experiments") or []
        if len(experiments) > 1:
            raise ValueError(
                "the source job contains multiple experiments; adding trials "
                "to one result would also rerun the others")
        pending = self.db.query_one(
            "SELECT id FROM jobs WHERE run_dir = ? AND state != 'done' "
            "ORDER BY id DESC LIMIT 1", (source["run_dir"],))
        if pending:
            raise ValueError(
                f"job {pending['id']} is already extending this result")
        offset = ledger.next_trial(self.db, run_id)
        if offset is None:
            raise ValueError(
                f"run {run_id} has no numbered trials; its next offset "
                "cannot be determined safely")
        spec = jobs.added_trials_spec(source["spec"], source["run_dir"],
                                      trials, offset)
        project_id = self.db.query_one(
            "SELECT project_id FROM jobs WHERE id = ?", (source_id,))["project_id"]
        new_id = jobs.submit(self.db, project_id, spec)
        self.hub.emit("job.state", job_id=new_id, state=jobs.WAITING,
                      add_trials_to=run_id, trial_offset=offset, trials=trials)
        self.wake()
        return {"job_id": new_id, "run_id": run_id, "trials": trials,
                "trial_offset": offset, "parent": source_id}

    def retry_point(self, run_id: int, point_id: int) -> dict:
        """Queue one exact existing point to overwrite in its result dir."""
        run = ledger.get_run(self.db, run_id)
        if run is None:
            raise KeyError(run_id)
        if not run.get("dir_exists"):
            raise ValueError(f"run {run_id} no longer has a result directory")
        point = next((p for p in run["points"] if p["id"] == point_id), None)
        if point is None:
            raise ValueError(f"point {point_id} does not belong to run {run_id}")
        source_id = run.get("job_id")
        source = jobs.get(self.db, source_id) if source_id is not None else None
        if source is None or not source.get("run_dir"):
            raise ValueError(f"run {run_id} has no source job to resume")
        if source["state"] != jobs.DONE:
            raise ValueError(f"run {run_id}'s job {source_id} is still {source['state']}")
        if len(source["spec"].get("experiments") or []) > 1:
            raise ValueError("the source job contains multiple experiments")
        pending = self.db.query_one(
            "SELECT id FROM jobs WHERE run_dir = ? AND state != 'done' "
            "ORDER BY id DESC LIMIT 1", (source["run_dir"],))
        if pending:
            raise ValueError(f"job {pending['id']} is already extending this result")
        spec = jobs.retried_point_spec(source["spec"], source["run_dir"], point)
        project_id = self.db.query_one(
            "SELECT project_id FROM jobs WHERE id = ?", (source_id,))["project_id"]
        new_id = jobs.submit(self.db, project_id, spec)
        self.hub.emit("job.state", job_id=new_id, state=jobs.WAITING,
                      retry_point_in=run_id, point_id=point_id)
        self.wake()
        return {"job_id": new_id, "run_id": run_id, "point_id": point_id,
                "point": {"dims": point["dims"], "rate": point["rate"],
                          "trial": point["trial"]}, "parent": source_id}
