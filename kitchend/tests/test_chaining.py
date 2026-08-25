"""Job chaining (`after`) and daemon-managed per-job cluster leases."""

import asyncio
import time

from kitchend.core import jobs


class FakeClusters:
    """Records lease traffic; up() resolves instantly."""

    def __init__(self, fail_up=False):
        self.fail_up = fail_up
        self.calls = []

    async def up(self, key, ttl_minutes, purpose="user"):
        if self.fail_up:
            raise RuntimeError("no capacity")
        self.calls.append(("up", key, purpose))
        return f"lease-{len(self.calls)}"

    def extend(self, key, lease_id, ttl_minutes):
        self.calls.append(("extend", key, lease_id))

    def release(self, key, lease_id):
        self.calls.append(("release", key, lease_id))

    def hold(self, key, seconds, for_job=None):
        self.calls.append(("hold", key, for_job))

    def overlaps(self, a, b):
        return False


def _submit(db, hub, config, script, **spec_kw):
    project_cfg = config.project("stub")
    project_id = jobs.ensure_project_row(db, project_cfg)
    spec = {"project": "stub", "experiments": [script], **spec_kw}
    job_id = jobs.submit(db, project_id, spec)
    hub.emit("job.state", job_id=job_id, state=jobs.QUEUED)
    return job_id


async def _run_until(scheduler, pred, timeout=10.0):
    task = asyncio.get_running_loop().create_task(scheduler.loop())
    scheduler.wake()
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if pred():
                return
            await asyncio.sleep(0.05)
        raise AssertionError("condition not reached before timeout")
    finally:
        scheduler.stop()
        task.cancel()


def _state(db, job_id):
    return jobs.get(db, job_id)["state"]


def test_after_waits_then_runs(env, tmp_path):
    config, db, hub, runner, scheduler = env
    marker = tmp_path / "order"

    async def main():
        first = _submit(db, hub, config, f"sleep 0.3 && echo A >> {marker}",
                        queue="a")
        # Different queue: only the dependency serializes them.
        second = _submit(db, hub, config, f"echo B >> {marker}", queue="b",
                         after=first)
        await _run_until(scheduler, lambda: all(
            _state(db, j) == jobs.SUCCEEDED for j in (first, second)))
        assert marker.read_text().splitlines() == ["A", "B"]

    asyncio.run(main())


def test_after_failed_chain_cancels_dependent(env):
    config, db, hub, runner, scheduler = env

    async def main():
        first = _submit(db, hub, config, "exit 1", queue="a", max_retries=0)
        second = _submit(db, hub, config, "exit 0", queue="b", after=first)
        await _run_until(scheduler,
                         lambda: _state(db, second) == jobs.CANCELED)
        assert _state(db, first) == jobs.FAILED

    asyncio.run(main())


def test_after_follows_retry_chain(env):
    config, db, hub, runner, scheduler = env

    async def main():
        # Fails once, then its auto-retry runs `exit 1` again... use a
        # marker so the retry succeeds: first attempt fails, retry passes.
        marker = config.projects[0].repo_path / "tried"
        first = _submit(db, hub, config,
                        f"test -e {marker} || {{ touch {marker}; exit 1; }}",
                        queue="a", max_retries=1, retry_delay_secs=0)
        second = _submit(db, hub, config, "exit 0", queue="b", after=first)
        await _run_until(scheduler,
                         lambda: _state(db, second) == jobs.SUCCEEDED)
        # The dependent ran only after the retry child produced data.
        retry_row = db.query("SELECT id FROM jobs WHERE parent_job_id = ?",
                             (first,))[0]
        assert _state(db, retry_row["id"]) == jobs.SUCCEEDED

    asyncio.run(main())


def test_degraded_chain_satisfies_dependent(env):
    config, db, hub, runner, scheduler = env

    async def main():
        first = _submit(db, hub, config, "exit 2", queue="a")
        second = _submit(db, hub, config, "exit 0", queue="b", after=first)
        await _run_until(scheduler,
                         lambda: _state(db, second) == jobs.SUCCEEDED)
        assert _state(db, first) == jobs.DEGRADED

    asyncio.run(main())


def test_cluster_lease_around_job(env):
    config, db, hub, runner, scheduler = env
    scheduler.clusters = FakeClusters()

    async def main():
        j = _submit(db, hub, config, "exit 0", cluster="main",
                    queue="stub/main")
        await _run_until(scheduler, lambda: _state(db, j) == jobs.SUCCEEDED)
        kinds = [c[0] for c in scheduler.clusters.calls]
        assert kinds == ["up", "release"]
        assert scheduler.clusters.calls[0][2] == f"job-{j}"

    asyncio.run(main())


def test_cluster_bringup_failure_leaves_the_job_queued(env):
    """A cluster that will not come up is not a run: the job goes back to
    queued with its next attempt due, no failed row and no retry spent."""
    config, db, hub, runner, scheduler = env
    scheduler.clusters = FakeClusters(fail_up=True)

    async def main():
        j = _submit(db, hub, config, "exit 0", cluster="main",
                    queue="stub/main", retry_delay_secs=600)

        def waited():
            row = jobs.get(db, j)
            return row["state"] == jobs.QUEUED and row["retry_at"]

        await _run_until(scheduler, waited)
        row = jobs.get(db, j)
        assert not runner.log_path(j).exists()   # the driver never spawned
        assert row["retries_left"] == jobs.DEFAULT_MAX_RETRIES
        # One row, still the original: nothing was added to the record.
        assert [r["id"] for r in jobs.list_jobs(db, limit=10)] == [j]

    asyncio.run(main())


def test_cluster_bringup_failure_retries_the_same_job(env):
    """A zone stockout is transient: after the cooldown the same job is
    dispatched again and runs once the cluster comes up."""
    config, db, hub, runner, scheduler = env
    fake = FakeClusters(fail_up=True)
    scheduler.clusters = fake

    async def main():
        j = _submit(db, hub, config, "exit 0", cluster="main",
                    queue="stub/main", retry_delay_secs=1)

        def waited_then_succeeded():
            if jobs.get(db, j)["retry_at"]:
                fake.fail_up = False   # capacity returned
            return _state(db, j) == jobs.SUCCEEDED

        await _run_until(scheduler, waited_then_succeeded)
        assert [c[0] for c in fake.calls] == ["up", "release"]
        assert jobs.get(db, j)["retry_at"] is None   # cleared on dispatch
        assert [r["id"] for r in jobs.list_jobs(db, limit=10)] == [j]

    asyncio.run(main())


def test_reorder_stops_a_deprioritized_job_waiting(env):
    """Only the front of the queue waits on a cluster: a job moved behind
    another drops its pending attempt instead of holding a retry it will not
    be the one to make."""
    config, db, hub, runner, scheduler = env
    scheduler.clusters = FakeClusters(fail_up=True)

    async def main():
        waiting = _submit(db, hub, config, "exit 0", cluster="main",
                          queue="stub/main", retry_delay_secs=600)
        other = _submit(db, hub, config, "exit 0", cluster="main",
                        queue="stub/main")
        await _run_until(scheduler, lambda: jobs.get(db, waiting)["retry_at"])
        jobs.reorder(db, hub, [other, waiting])
        assert jobs.get(db, waiting)["retry_at"] is None
        assert _state(db, waiting) == jobs.QUEUED

    asyncio.run(main())


def test_restart_keeps_the_cluster_cooling(env):
    """A daemon restart rebuilds the cooldown from the waiting job, so the
    cluster that just failed is not probed again on startup."""
    from kitchend.core.scheduler import Scheduler
    config, db, hub, runner, scheduler = env
    fake = FakeClusters(fail_up=True)
    scheduler.clusters = fake

    async def main():
        j = _submit(db, hub, config, "exit 0", cluster="main",
                    queue="stub/main", retry_delay_secs=600)
        await _run_until(scheduler, lambda: jobs.get(db, j)["retry_at"])
        fresh = Scheduler(config, db, hub, runner, clusters=fake)
        assert fresh._cooling("stub/main")
        assert jobs.get(db, j)["retry_at"] is not None   # still the front

    asyncio.run(main())


def test_cluster_bringup_failure_cools_the_cluster_down(env):
    """After one job finds the cluster will not come up, the jobs queued
    behind it on that cluster wait out the cooldown rather than each paying
    a partial start to learn the same thing."""
    config, db, hub, runner, scheduler = env
    fake = FakeClusters(fail_up=True)
    scheduler.clusters = fake

    async def main():
        first = _submit(db, hub, config, "exit 0", cluster="main",
                        queue="stub/main", retry_delay_secs=2)
        second = _submit(db, hub, config, "exit 0", cluster="main",
                         queue="stub/main")
        failed_at = {}

        def phases():
            if jobs.get(db, first)["retry_at"] and "t" not in failed_at:
                failed_at["t"] = time.monotonic()
                fake.fail_up = False       # capacity is back immediately...
            if "t" in failed_at and time.monotonic() - failed_at["t"] < 1.0:
                # ...but inside the cooldown neither job may be dispatched.
                assert _state(db, second) == jobs.QUEUED
                assert fake.calls == []
            return _state(db, second) == jobs.SUCCEEDED

        await _run_until(scheduler, phases)
        # It dispatched only after the 2 s cooldown lapsed.
        assert time.monotonic() - failed_at["t"] >= 2.0

    asyncio.run(main())


def test_pending_retry_survives_a_scheduler_restart(env):
    """A failed job whose retry is still pending when the daemon restarts is
    re-armed by the new scheduler; one whose retry already spawned is not."""
    from kitchend.core.scheduler import Scheduler
    config, db, hub, runner, scheduler = env
    project_cfg = config.project("stub")
    project_id = jobs.ensure_project_row(db, project_cfg)
    pending = jobs.submit(db, project_id, {"project": "stub", "experiments": ["exit 0"],
                                           "max_retries": 1, "retry_delay_secs": 1})
    spent = jobs.submit(db, project_id, {"project": "stub", "experiments": ["exit 0"],
                                         "max_retries": 1})
    db.execute("UPDATE jobs SET state = ?, retries_left = 1, "
               "retry_at = datetime('now', '+1 seconds') WHERE id = ?",
               (jobs.FAILED, pending))
    db.execute("UPDATE jobs SET state = ?, retries_left = 1, retry_at = NULL "
               "WHERE id = ?", (jobs.FAILED, spent))

    fresh = Scheduler(config, db, hub, runner)
    assert jobs.get(db, spent)["retries_left"] == 0       # zeroed at construction

    async def main():
        await _run_until(fresh, lambda: any(
            r.get("parent_job_id") == pending and r["state"] == jobs.SUCCEEDED
            for r in jobs.list_jobs(db, limit=10)))
        assert not any(r.get("parent_job_id") == spent
                       for r in jobs.list_jobs(db, limit=10))

    asyncio.run(main())


def test_lease_released_on_cancel(env):
    config, db, hub, runner, scheduler = env
    scheduler.clusters = FakeClusters()

    async def main():
        j = _submit(db, hub, config, "sleep 30", cluster="main",
                    queue="stub/main")
        await _run_until(scheduler, lambda: _state(db, j) == jobs.RUNNING)
        task = asyncio.get_running_loop().create_task(scheduler.loop())
        await scheduler.cancel(j)
        scheduler.stop()
        task.cancel()
        await asyncio.sleep(0.1)
        kinds = [c[0] for c in scheduler.clusters.calls]
        assert kinds == ["up", "release"]

    asyncio.run(main())
