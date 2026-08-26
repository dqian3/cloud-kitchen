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
    hub.emit("job.state", job_id=job_id, state=jobs.WAITING)
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


def _outcome(db, job_id):
    return jobs.get(db, job_id)["outcome"]


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
            _outcome(db, j) == jobs.OK for j in (first, second)))
        assert marker.read_text().splitlines() == ["A", "B"]

    asyncio.run(main())


def test_after_failure_cancels_dependent(env):
    config, db, hub, runner, scheduler = env

    async def main():
        first = _submit(db, hub, config, "exit 1", queue="a", max_attempts=1)
        second = _submit(db, hub, config, "exit 0", queue="b", after=first)
        await _run_until(scheduler,
                         lambda: _outcome(db, second) == jobs.CANCELED)
        assert _outcome(db, first) == jobs.FAILED

    asyncio.run(main())


def test_after_waits_out_the_retries(env):
    """The dependent waits while its predecessor is still trying, and runs
    once an attempt finally produces data."""
    config, db, hub, runner, scheduler = env

    async def main():
        marker = config.projects[0].repo_path / "tried"
        first = _submit(db, hub, config,
                        f"test -e {marker} || {{ touch {marker}; exit 1; }}",
                        queue="a", max_attempts=2, retry_delay_secs=0)
        second = _submit(db, hub, config, "exit 0", queue="b", after=first)
        seen = []

        def done():
            if _state(db, second) == jobs.WAITING and _state(db, first) != jobs.DONE:
                seen.append(True)      # it held while `first` was still trying
            return _outcome(db, second) == jobs.OK

        await _run_until(scheduler, done)
        assert seen
        assert jobs.get(db, first)["attempts"] == 2

    asyncio.run(main())


def test_degraded_predecessor_satisfies_dependent(env):
    config, db, hub, runner, scheduler = env

    async def main():
        first = _submit(db, hub, config, "exit 2", queue="a")
        second = _submit(db, hub, config, "exit 0", queue="b", after=first)
        await _run_until(scheduler, lambda: _outcome(db, second) == jobs.OK)
        assert _outcome(db, first) == jobs.DEGRADED

    asyncio.run(main())


def test_cluster_lease_around_job(env):
    config, db, hub, runner, scheduler = env
    scheduler.clusters = FakeClusters()

    async def main():
        j = _submit(db, hub, config, "exit 0", cluster="main",
                    queue="stub/main")
        await _run_until(scheduler, lambda: _state(db, j) == jobs.DONE)
        kinds = [c[0] for c in scheduler.clusters.calls]
        assert kinds == ["up", "release"]
        assert scheduler.clusters.calls[0][2] == f"job-{j}"

    asyncio.run(main())


def test_cluster_bringup_failure_costs_no_attempt(env):
    """A cluster that will not come up is not a run: the job stays waiting
    with its next attempt due, and nothing is written to its record."""
    config, db, hub, runner, scheduler = env
    scheduler.clusters = FakeClusters(fail_up=True)

    async def main():
        j = _submit(db, hub, config, "exit 0", cluster="main",
                    queue="stub/main", retry_delay_secs=600)

        def waited():
            row = jobs.get(db, j)
            return row["state"] == jobs.WAITING and row["next_attempt_at"]

        await _run_until(scheduler, waited)
        row = jobs.get(db, j)
        assert not runner.log_path(j).exists()   # the driver never spawned
        assert row["attempts"] == 0 and jobs.attempts(db, j) == []
        assert "bring-up failed" in row["last_error"]
        assert [r["id"] for r in jobs.list_jobs(db, limit=10)] == [j]

    asyncio.run(main())


def test_cluster_bringup_failure_retries_the_same_job(env):
    config, db, hub, runner, scheduler = env
    fake = FakeClusters(fail_up=True)
    scheduler.clusters = fake

    async def main():
        j = _submit(db, hub, config, "exit 0", cluster="main",
                    queue="stub/main", retry_delay_secs=1)

        def waited_then_ran():
            if jobs.get(db, j)["next_attempt_at"]:
                fake.fail_up = False   # capacity returned
            return _state(db, j) == jobs.DONE

        await _run_until(scheduler, waited_then_ran)
        assert _outcome(db, j) == jobs.OK
        assert jobs.get(db, j)["attempts"] == 1     # only the run that ran
        assert jobs.get(db, j)["next_attempt_at"] is None

    asyncio.run(main())


def test_cluster_bringup_failure_cools_the_cluster_down(env):
    """After one job finds the cluster will not come up, the jobs behind it
    wait out the cooldown rather than each paying a partial start to learn
    the same thing."""
    config, db, hub, runner, scheduler = env
    fake = FakeClusters(fail_up=True)
    scheduler.clusters = fake

    async def main():
        first = _submit(db, hub, config, "exit 0", cluster="main",
                        queue="stub/main", retry_delay_secs=2)
        second = _submit(db, hub, config, "exit 0", cluster="main",
                         queue="stub/main")
        held_at = {}

        def phases():
            if jobs.get(db, first)["next_attempt_at"] and "t" not in held_at:
                held_at["t"] = time.monotonic()
                fake.fail_up = False       # capacity is back immediately...
            if "t" in held_at and time.monotonic() - held_at["t"] < 1.0:
                # ...but inside the cooldown neither job may be dispatched.
                assert _state(db, second) == jobs.WAITING
                assert fake.calls == []
            return _outcome(db, second) == jobs.OK

        await _run_until(scheduler, phases)
        assert time.monotonic() - held_at["t"] >= 2.0

    asyncio.run(main())


def test_reorder_stops_a_deprioritized_job_waiting(env):
    """Only the front of the queue waits on a cluster: a job moved behind
    another drops its pending attempt."""
    config, db, hub, runner, scheduler = env
    scheduler.clusters = FakeClusters(fail_up=True)

    async def main():
        waiting = _submit(db, hub, config, "exit 0", cluster="main",
                          queue="stub/main", retry_delay_secs=600)
        other = _submit(db, hub, config, "exit 0", cluster="main",
                        queue="stub/main")
        await _run_until(scheduler,
                         lambda: jobs.get(db, waiting)["next_attempt_at"])
        jobs.reorder(db, hub, [other, waiting])
        assert jobs.get(db, waiting)["next_attempt_at"] is None
        assert _state(db, waiting) == jobs.WAITING

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
        await _run_until(scheduler,
                         lambda: jobs.get(db, j)["next_attempt_at"])
        fresh = Scheduler(config, db, hub, runner, clusters=fake)
        assert fresh._cooling("stub/main")
        assert jobs.get(db, j)["next_attempt_at"] is not None  # still the front

    asyncio.run(main())


def test_lease_released_on_cancel(env):
    config, db, hub, runner, scheduler = env

    class SlowClusters(FakeClusters):
        def __init__(self):
            super().__init__()
            self.gate = asyncio.Event()
            self.released = []

        async def up(self, key, ttl_minutes, purpose="user"):
            await self.gate.wait()
            return "lease-1"

        def release(self, key, lease_id):
            self.released.append(lease_id)

    fake = SlowClusters()
    scheduler.clusters = fake

    async def main():
        j = _submit(db, hub, config, "true", cluster="main", queue="stub/main")
        task = asyncio.get_running_loop().create_task(scheduler.loop())
        scheduler.wake()
        await asyncio.sleep(0.3)
        # Still waiting: the cluster is coming up, which is the cluster's
        # business, not a state of the job.
        assert _state(db, j) == jobs.WAITING
        assert await scheduler.cancel(j) == jobs.CANCELED
        fake.gate.set()
        await asyncio.sleep(0.3)
        assert _outcome(db, j) == jobs.CANCELED
        assert fake.released == ["lease-1"]    # given back, never spawned
        assert not runner.log_path(j).exists()
        scheduler.stop()
        task.cancel()

    asyncio.run(main())
