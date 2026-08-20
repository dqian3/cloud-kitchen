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

from . import jobs


class Scheduler:
    def __init__(self, config, db, hub, runner):
        self.config = config
        self.db = db
        self.hub = hub
        self.runner = runner
        self._tasks: dict[int, asyncio.Task] = {}     # job_id -> supervisor task
        self._running_queues: dict[str, int] = {}     # queue key -> job_id
        self._wake = asyncio.Event()
        self._stopped = False

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
            key = jobs.queue_key(job)
            if key in self._running_queues:
                continue
            self._running_queues[key] = job["id"]
            self._tasks[job["id"]] = asyncio.get_running_loop().create_task(
                self._execute(job, key))
            if len(self._running_queues) >= self.config.max_concurrent_queues:
                break

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

        jobs.set_state(self.db, self.hub, job_id, jobs.RUNNING,
                       started_at=jobs._now(self.db),
                       run_dir=spec.get("run_dir"))
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

        self._finish(job, key, rc)

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
            retries_left = current["retries_left"]
            if retries_left > 0:
                delay = int(job["spec"].get("retry_delay_secs", 600))
                jobs.set_state(self.db, self.hub, job_id, jobs.FAILED,
                               exit_code=rc, finished_at=now)
                self.hub.emit("job.retry_scheduled", job_id=job_id,
                              delay_secs=delay, retries_left=retries_left - 1)
                # `current`, not `job`: the dispatch-time snapshot predates the
                # daemon-assigned run_dir, and the retry must resume into it.
                asyncio.get_running_loop().create_task(
                    self._requeue_after(current, delay, retries_left - 1))
            else:
                jobs.set_state(self.db, self.hub, job_id, jobs.FAILED,
                               exit_code=rc, finished_at=now)
        self._release(key, job_id)

    async def _requeue_after(self, job, delay, retries_left):
        await asyncio.sleep(delay)
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
        if job["state"] == jobs.RUNNING:
            jobs.set_state(self.db, self.hub, job_id, jobs.CANCELED)
            await self.runner.cancel(job_id)
            self.db.execute("UPDATE jobs SET finished_at = datetime('now') "
                            "WHERE id = ?", (job_id,))
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
