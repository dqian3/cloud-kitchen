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
        first = _submit(db, hub, config, "exit 1", queue="a")
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


def test_cluster_bringup_failure_fails_job(env):
    config, db, hub, runner, scheduler = env
    scheduler.clusters = FakeClusters(fail_up=True)

    async def main():
        j = _submit(db, hub, config, "exit 0", cluster="main",
                    queue="stub/main")
        await _run_until(scheduler, lambda: _state(db, j) == jobs.FAILED)
        # The driver never spawned.
        assert not runner.log_path(j).exists()

    asyncio.run(main())


def test_cluster_bringup_failure_retries_like_a_run_failure(env):
    """A zone stockout at bring-up is transient: with retries left the job
    is requeued (as a child resuming into its run dir) after the delay, and
    the retry succeeds once the cluster comes up."""
    config, db, hub, runner, scheduler = env
    fake = FakeClusters(fail_up=True)
    scheduler.clusters = fake

    async def main():
        # A one-second delay: with zero the retry dispatches in the same tick
        # as the failure, before the test can restore capacity.
        j = _submit(db, hub, config, "exit 0", cluster="main",
                    queue="stub/main", max_retries=1, retry_delay_secs=1)

        def retried_and_succeeded():
            if _state(db, j) == jobs.FAILED:
                fake.fail_up = False   # capacity returned
            return any(r.get("parent_job_id") == j and r["state"] == jobs.SUCCEEDED
                       for r in jobs.list_jobs(db, limit=10))

        await _run_until(scheduler, retried_and_succeeded)
        kinds = [c[0] for c in fake.calls]
        assert kinds == ["up", "release"]

    asyncio.run(main())


def test_cluster_bringup_failure_cools_the_cluster_down(env):
    """After one job finds the cluster will not come up, the jobs queued
    behind it on that cluster wait out the retry delay rather than each
    paying a partial start to learn the same thing."""
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
            if _state(db, first) == jobs.FAILED and "t" not in failed_at:
                failed_at["t"] = time.monotonic()
                fake.fail_up = False       # capacity is back immediately...
            if "t" in failed_at and time.monotonic() - failed_at["t"] < 1.0:
                # ...but inside the cooldown the second job must stay queued.
                assert _state(db, second) == jobs.QUEUED
                assert fake.calls == []
            return _state(db, second) == jobs.SUCCEEDED

        await _run_until(scheduler, phases)
        # It dispatched only after the 2 s cooldown lapsed.
        assert time.monotonic() - failed_at["t"] >= 2.0

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
