"""Scheduler end-to-end against real subprocesses (bash one-liners)."""

import asyncio
import json
import time

from kitchend.core import jobs


def _submit(db, hub, config, script, **spec_kw):
    project_cfg = config.project("stub")
    project_id = jobs.ensure_project_row(db, project_cfg)
    spec = {"project": "stub", "experiments": [script], **spec_kw}
    job_id = jobs.submit(db, project_id, spec)
    hub.emit("job.state", job_id=job_id, state=jobs.WAITING)
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


def _outcome(db, job_id):
    return jobs.get(db, job_id)["outcome"]


def _done(db, *ids):
    return all(_state(db, i) == jobs.DONE for i in ids)


def test_exit_code_contract(env):
    config, db, hub, runner, scheduler = env
    ok = _submit(db, hub, config, "exit 0", queue="a")
    degraded = _submit(db, hub, config, "exit 2", queue="b")
    dead = _submit(db, hub, config, "exit 1", queue="c", max_attempts=1)

    async def main():
        await _run_until(db, scheduler, lambda: _done(db, ok, degraded, dead))
        assert _outcome(db, ok) == jobs.OK
        assert _outcome(db, degraded) == jobs.DEGRADED
        assert _outcome(db, dead) == jobs.FAILED
        # The attempt log carries the exit codes, not the job row.
        assert [a["exit_code"] for a in jobs.attempts(db, degraded)] == [2]

    asyncio.run(main())


def test_a_retry_reuses_the_same_row(env, tmp_path):
    """A failed run is tried again in place: same job, next attempt, resuming
    into the same directory. No second row appears in the queue."""
    config, db, hub, runner, scheduler = env
    marker = tmp_path / "tries"
    j = _submit(db, hub, config, f"echo x >> {marker}; test -s {marker} && "
                                 f"test $(wc -l < {marker}) -ge 2",
                queue="q", max_attempts=3, retry_delay_secs=0)

    async def main():
        await _run_until(db, scheduler, lambda: _state(db, j) == jobs.DONE)
        row = jobs.get(db, j)
        assert row["outcome"] == jobs.OK
        assert row["attempts"] == 2                      # failed once, then ok
        assert len(jobs.list_jobs(db, limit=10)) == 1    # one row, still
        assert [a["exit_code"] for a in jobs.attempts(db, j)] == [1, 0]

    asyncio.run(main())


def test_retry_resumes_into_the_same_dir(env):
    config, db, hub, runner, scheduler = env
    j = _submit(db, hub, config, "exit 1", queue="q", max_attempts=2,
                retry_delay_secs=0)
    seen = []
    hub.subscribe_test = None

    async def main():
        await _run_until(db, scheduler, lambda: _state(db, j) == jobs.DONE)
        cmds = [json.loads(r["payload_json"]) for r in db.query(
            "SELECT payload_json FROM events WHERE type = 'job.command' "
            "AND job_id = ? ORDER BY id", (j,))]
        assert len(cmds) == 2
        assert "--resume" not in cmds[0]["argv"]
        assert "--resume" in cmds[1]["argv"]       # the second attempt resumes
        assert cmds[0]["argv"][-2:] == cmds[1]["argv"][-3:-1]   # same run dir
        assert seen == []

    asyncio.run(main())


def test_degraded_is_not_retried(env):
    config, db, hub, runner, scheduler = env
    j = _submit(db, hub, config, "exit 2", queue="q", max_attempts=5,
                retry_delay_secs=0)

    async def main():
        await _run_until(db, scheduler, lambda: _state(db, j) == jobs.DONE)
        assert _outcome(db, j) == jobs.DEGRADED
        assert jobs.get(db, j)["attempts"] == 1

    asyncio.run(main())


def test_out_of_attempts_fails(env):
    config, db, hub, runner, scheduler = env
    j = _submit(db, hub, config, "exit 1", queue="q", max_attempts=2,
                retry_delay_secs=0)

    async def main():
        await _run_until(db, scheduler, lambda: _state(db, j) == jobs.DONE)
        row = jobs.get(db, j)
        assert row["outcome"] == jobs.FAILED and row["attempts"] == 2
        assert "out of attempts" in row["last_error"]

    asyncio.run(main())


def test_same_queue_serializes(env, tmp_path):
    config, db, hub, runner, scheduler = env
    marker = tmp_path / "order"
    a = _submit(db, hub, config, f"sleep 0.3 && echo A >> {marker}", queue="q")
    b = _submit(db, hub, config, f"echo B >> {marker}", queue="q")

    async def main():
        await _run_until(db, scheduler, lambda: _done(db, a, b))
        assert marker.read_text().splitlines() == ["A", "B"]

    asyncio.run(main())


def test_different_queues_run_concurrently(env, tmp_path):
    config, db, hub, runner, scheduler = env
    started = tmp_path / "started"
    j1 = _submit(db, hub, config, f"echo 1 >> {started}; sleep 0.4", queue="a")
    j2 = _submit(db, hub, config, f"echo 2 >> {started}; sleep 0.4", queue="b")

    async def main():
        await _run_until(db, scheduler, lambda: _done(db, j1, j2))
        assert sorted(started.read_text().split()) == ["1", "2"]

    asyncio.run(main())


def test_cancel_running_job(env, tmp_path):
    config, db, hub, runner, scheduler = env
    j = _submit(db, hub, config, "sleep 30", queue="q")

    async def main():
        await _run_until(db, scheduler, lambda: _state(db, j) == jobs.RUNNING)
        assert await scheduler.cancel(j) == jobs.CANCELED
        assert _outcome(db, j) == jobs.CANCELED
        assert jobs.attempts(db, j)[-1]["error"] == "canceled"

    asyncio.run(main())


def test_cancel_a_waiting_job_clears_its_next_attempt(env):
    config, db, hub, runner, scheduler = env
    j = _submit(db, hub, config, "exit 0", queue="q")
    jobs.wait_again(db, hub, j, "q", 600, "cluster bring-up failed")

    async def main():
        assert await scheduler.cancel(j) == jobs.CANCELED

    asyncio.run(main())
    row = jobs.get(db, j)
    assert row["state"] == jobs.DONE
    # And it can be dropped: nothing is pending for it any more.
    jobs.delete(db, hub, j)
    assert jobs.get(db, j) is None


def test_recover_orphans_returns_the_job_to_the_queue(env):
    """A daemon that dies mid-run costs the job its attempt, not its place."""
    config, db, hub, runner, scheduler = env
    j = _submit(db, hub, config, "true", queue="q", max_attempts=3,
                retry_delay_secs=600)
    jobs.start_attempt(db, hub, j)
    jobs.set_state(db, hub, j, jobs.RUNNING, pid=999_999)
    jobs.recover_orphans(db, hub)
    row = jobs.get(db, j)
    assert row["state"] == jobs.WAITING and row["next_attempt_at"]
    assert jobs.attempts(db, j)[-1]["error"] == "daemon died while it ran"

    spent = _submit(db, hub, config, "true", queue="q", max_attempts=1)
    jobs.start_attempt(db, hub, spent)
    jobs.set_state(db, hub, spent, jobs.RUNNING, pid=999_999)
    jobs.recover_orphans(db, hub)
    assert _outcome(db, spent) == jobs.FAILED   # that was its only attempt


def test_reorder_waiting_jobs(env):
    config, db, hub, runner, scheduler = env
    a = _submit(db, hub, config, "true", queue="q")
    b = _submit(db, hub, config, "true", queue="q")
    jobs.reorder(db, hub, [b, a])
    assert jobs.get(db, b)["priority"] > jobs.get(db, a)["priority"]
    jobs.set_state(db, hub, a, jobs.RUNNING)
    try:
        jobs.reorder(db, hub, [a, b])
    except ValueError as e:
        assert "not waiting" in str(e)
    else:
        raise AssertionError("reordering a running job should be refused")


def test_purge_drops_rows_without_data_and_keeps_files(env, tmp_path):
    config, db, hub, runner, scheduler = env
    run_dir = tmp_path / "runs" / "kitchen-job1"
    run_dir.mkdir(parents=True)
    (run_dir / "sweep_results.json").write_text("[]")
    dead = _submit(db, hub, config, "exit 1", queue="q")
    canceled = _submit(db, hub, config, "exit 1", queue="q")
    ok = _submit(db, hub, config, "true", queue="q")
    db.execute("UPDATE jobs SET run_dir = ? WHERE id = ?", (str(run_dir), dead))
    jobs.finish(db, hub, dead, jobs.FAILED)
    jobs.finish(db, hub, canceled, jobs.CANCELED)
    jobs.finish(db, hub, ok, jobs.OK)

    assert sorted(jobs.purge(db, hub)) == sorted([dead, canceled])
    assert jobs.get(db, ok) is not None
    assert (run_dir / "sweep_results.json").exists()   # purge never deletes data


def test_attempts_stop_after_two_pointless_ones(env):
    """A run that dies before its first point twice in a row is a broken
    environment, not a flaky run: the job ends instead of re-leasing the
    cluster for a third identical failure."""
    config, db, hub, runner, scheduler = env
    j = _submit(db, hub, config, "exit 1", queue="q", max_attempts=9,
                retry_delay_secs=0)

    async def main():
        await _run_until(db, scheduler, lambda: _state(db, j) == jobs.DONE)
        row = jobs.get(db, j)
        assert row["outcome"] == jobs.FAILED and row["attempts"] == 2
        assert "no points" in row["last_error"]

    asyncio.run(main())


def test_a_run_that_got_points_keeps_its_attempts(env):
    config, db, hub, runner, scheduler = env
    j = _submit(db, hub, config, "exit 1", queue="q", max_attempts=9,
                retry_delay_secs=600)
    # Progress from the first attempt: points landed, so a failure now is
    # worth retrying rather than giving up on.
    db.execute("UPDATE jobs SET progress_json = ? WHERE id = ?",
               (json.dumps({"points": {"done": 3}}), j))

    async def main():
        await _run_until(db, scheduler,
                         lambda: jobs.get(db, j)["next_attempt_at"] is not None)
        row = jobs.get(db, j)
        assert row["state"] == jobs.WAITING and row["attempts"] == 1
        assert jobs.attempts(db, j)[0]["points"] == 3

    asyncio.run(main())


def test_lease_handoff_holds_cluster_for_next_job(env):
    config, db, hub, runner, scheduler = env

    class FakeClusters:
        def __init__(self):
            self.holds = []
        async def up(self, key, ttl, purpose=None):
            return "lease"
        def release(self, key, lease_id):
            pass
        def extend(self, *a):
            pass
        def overlaps(self, a, b):
            return False
        def hold(self, key, seconds, for_job=None):
            self.holds.append((key, seconds, for_job))

    scheduler.clusters = FakeClusters()
    from dataclasses import replace
    from kitchend.config import ClusterConfig
    scheduler.config = replace(config, projects=(replace(
        config.project("stub"),
        clusters=(ClusterConfig(name="c", config="c.yaml"),)),))

    async def main():
        first = _submit(db, hub, config, "true", queue="q", cluster="c")
        second = _submit(db, hub, config, "true", queue="q", cluster="c")
        await _run_until(db, scheduler, lambda: _done(db, first, second))
        assert scheduler.clusters.holds[0][2] == second

    asyncio.run(main())
