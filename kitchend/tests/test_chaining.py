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
            return row["state"] == jobs.WAITING and scheduler._waiting(j)

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
            if scheduler._waiting(j):
                fake.fail_up = False   # capacity returned
            return _state(db, j) == jobs.DONE

        await _run_until(scheduler, waited_then_ran)
        assert _outcome(db, j) == jobs.OK
        assert jobs.get(db, j)["attempts"] == 1     # only the run that ran
        assert not scheduler._waiting(j)

    asyncio.run(main())


def test_only_the_head_retries(env):
    """The wait belongs to the job that earned it. The job behind it is not
    dispatched either — one runs at a time — so nothing else probes a cluster
    that just refused."""
    config, db, hub, runner, scheduler = env
    fake = FakeClusters(fail_up=True)
    scheduler.clusters = fake

    async def main():
        first = _submit(db, hub, config, "exit 0", cluster="main",
                        queue="stub/main", retry_delay_secs=600)
        second = _submit(db, hub, config, "exit 0", cluster="main",
                         queue="stub/main")
        await _run_until(scheduler, lambda: scheduler._waiting(first))
        fake.calls.clear()
        await asyncio.sleep(0.4)
        assert scheduler._waiting(first)          # the head waits
        assert not scheduler._waiting(second)     # the one behind never tried
        assert fake.calls == []                   # and nothing probed

    asyncio.run(main())

def test_a_queue_waits_as_one(env):
    """The delay belongs to the queue: one job runs at a time, so every job
    in it waits out the same attempt rather than starting its own."""
    config, db, hub, runner, scheduler = env
    scheduler.clusters = FakeClusters(fail_up=True)

    async def main():
        waiting = _submit(db, hub, config, "exit 0", cluster="main",
                          queue="stub/main", retry_delay_secs=600)
        other = _submit(db, hub, config, "exit 0", cluster="main",
                        queue="stub/main")
        await _run_until(scheduler,
                         lambda: scheduler._waiting(waiting))
        # The wait is the queue's now, so reordering does not clear it: both
        # jobs sit behind the same delay, which is the point.
        jobs.reorder(db, hub, [other, waiting])
        assert _state(db, waiting) == jobs.WAITING
        assert scheduler._waiting(waiting)

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


def test_retry_now_drops_the_wait(env):
    """Asking explicitly says the reason for the wait has gone — a fleet
    rebuilt, capacity returned — which the daemon cannot see for itself."""
    config, db, hub, runner, scheduler = env
    fake = FakeClusters(fail_up=True)
    scheduler.clusters = fake

    async def main():
        j = _submit(db, hub, config, "exit 0", cluster="main",
                    queue="stub/main", retry_delay_secs=3600)
        await _run_until(scheduler, lambda: scheduler._waiting(j))

        scheduler.retry_now(j)
        assert not scheduler._waiting(j)

        # A job that is not waiting has nothing to retry.
        jobs.finish(db, hub, j, jobs.OK)
        try:
            scheduler.retry_now(j)
        except ValueError as e:
            assert "not waiting" in str(e)
        else:
            raise AssertionError("retry_now on a done job should be refused")

    asyncio.run(main())
