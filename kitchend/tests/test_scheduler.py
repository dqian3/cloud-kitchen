"""Scheduler end-to-end against real subprocesses (bash one-liners)."""

import asyncio
import time

from kitchend.core import jobs


def _submit(db, hub, config, script, **spec_kw):
    project_cfg = config.project("stub")
    project_id = jobs.ensure_project_row(db, project_cfg)
    spec = {"project": "stub", "experiments": [script], **spec_kw}
    job_id = jobs.submit(db, project_id, spec)
    hub.emit("job.state", job_id=job_id, state=jobs.QUEUED)
    return job_id


async def _run_until(db, scheduler, pred, timeout=10.0):
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


def test_exit_code_contract(env):
    config, db, hub, runner, scheduler = env

    async def main():
        ok = _submit(db, hub, config, "exit 0", queue="a")
        degraded = _submit(db, hub, config, "exit 2", queue="b")
        failed = _submit(db, hub, config, "exit 7", queue="c")
        await _run_until(db, scheduler, lambda: all(
            _state(db, j) not in jobs.ACTIVE_STATES
            for j in (ok, degraded, failed)))
        assert _state(db, ok) == jobs.SUCCEEDED
        assert _state(db, degraded) == jobs.DEGRADED
        assert _state(db, failed) == jobs.FAILED
        assert jobs.get(db, degraded)["exit_code"] == 2

    asyncio.run(main())


def test_retry_requeues_with_resume(env):
    config, db, hub, runner, scheduler = env

    async def main():
        failing = _submit(db, hub, config, "exit 1", queue="a",
                          max_retries=1, retry_delay_secs=0,
                          run_dir="/tmp/rd")
        def retried():
            rows = db.query("SELECT * FROM jobs WHERE parent_job_id = ?", (failing,))
            if not rows:
                return False
            child = jobs.get(db, rows[0]["id"])
            return child["state"] == jobs.FAILED
        await _run_until(db, scheduler, retried)
        child_row = db.query("SELECT id FROM jobs WHERE parent_job_id = ?", (failing,))[0]
        child = jobs.get(db, child_row["id"])
        assert child["spec"]["resume"] is True
        assert child["spec"]["run_dir"] == "/tmp/rd"
        assert child["spec"]["max_retries"] == 0
        # No grandchild: retries are bounded.
        assert not db.query("SELECT id FROM jobs WHERE parent_job_id = ?",
                            (child["id"],))

    asyncio.run(main())


def test_degraded_is_not_retried(env):
    config, db, hub, runner, scheduler = env

    async def main():
        j = _submit(db, hub, config, "exit 2", queue="a",
                    max_retries=3, retry_delay_secs=0)
        await _run_until(db, scheduler,
                         lambda: _state(db, j) == jobs.DEGRADED)
        await asyncio.sleep(0.2)
        assert not db.query("SELECT id FROM jobs WHERE parent_job_id = ?", (j,))

    asyncio.run(main())


def test_same_queue_serializes(env):
    config, db, hub, runner, scheduler = env

    async def main():
        marker = config.projects[0].repo_path / "marker"
        # Each job fails if the other is mid-flight: concurrent execution
        # would see the marker file existing.
        script = (f"test ! -e {marker} && touch {marker} && sleep 0.3 && "
                  f"rm {marker}")
        j1 = _submit(db, hub, config, script, queue="same")
        j2 = _submit(db, hub, config, script, queue="same")
        await _run_until(db, scheduler, lambda: all(
            _state(db, j) not in jobs.ACTIVE_STATES for j in (j1, j2)))
        assert _state(db, j1) == jobs.SUCCEEDED
        assert _state(db, j2) == jobs.SUCCEEDED

    asyncio.run(main())


def test_different_queues_run_concurrently(env):
    config, db, hub, runner, scheduler = env

    async def main():
        j1 = _submit(db, hub, config, "sleep 0.5", queue="a")
        j2 = _submit(db, hub, config, "sleep 0.5", queue="b")
        t0 = time.monotonic()
        await _run_until(db, scheduler, lambda: all(
            _state(db, j) == jobs.SUCCEEDED for j in (j1, j2)))
        # Serial would be >= 1.0s; allow generous slack for slow CI.
        assert time.monotonic() - t0 < 0.95

    asyncio.run(main())


def test_cancel_running_job(env):
    config, db, hub, runner, scheduler = env

    async def main():
        j = _submit(db, hub, config, "sleep 30", queue="a")
        await _run_until(db, scheduler,
                         lambda: _state(db, j) == jobs.RUNNING)
        task = asyncio.get_running_loop().create_task(scheduler.loop())
        await scheduler.cancel(j)
        scheduler.stop()
        task.cancel()
        assert _state(db, j) == jobs.CANCELED
        assert not runner.is_running(j)

    asyncio.run(main())


def test_recover_orphans(env):
    config, db, hub, runner, scheduler = env
    project_id = jobs.ensure_project_row(db, config.project("stub"))
    dead = jobs.submit(db, project_id, {"project": "stub", "experiments": ["x"]})
    db.execute("UPDATE jobs SET state = 'running', pid = 4194399 WHERE id = ?",
               (dead,))
    jobs.recover_orphans(db, hub)
    assert _state(db, dead) == jobs.INTERRUPTED


def test_cancel_reaches_pending_retry(env):
    config, db, hub, runner, scheduler = env

    async def main():
        failing = _submit(db, hub, config, "exit 1", queue="a",
                          max_retries=3, retry_delay_secs=600)
        await _run_until(db, scheduler,
                         lambda: _state(db, failing) == jobs.FAILED)
        # Failed, with a retry timer sleeping out its delay (the decrement
        # lands on the child it would create).
        assert jobs.get(db, failing)["retries_left"] == 3
        assert failing in scheduler._requeues
        assert await scheduler.cancel(failing) == jobs.CANCELED
        assert jobs.get(db, failing)["retries_left"] == 0
        assert failing not in scheduler._requeues
        await asyncio.sleep(0.05)
        # The chain ended here: no child was queued.
        assert not db.query("SELECT id FROM jobs WHERE parent_job_id = ?",
                            (failing,))

    asyncio.run(main())


def test_restart_clears_orphaned_retries(env):
    config, db, hub, runner, scheduler = env
    from kitchend.core.scheduler import Scheduler

    project_id = jobs.ensure_project_row(db, config.project("stub"))
    jid = jobs.submit(db, project_id, {"project": "stub", "experiments": ["x"]})
    db.execute("UPDATE jobs SET state = ?, retries_left = 2 WHERE id = ?",
               (jobs.FAILED, jid))
    Scheduler(config, db, hub, runner)     # a fresh daemon
    assert jobs.get(db, jid)["retries_left"] == 0
