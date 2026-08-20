"""Ingest: events.jsonl → per-job progress; dispatch-time run_dir assignment."""

import asyncio
import json
import time

from kitchen.events import EventEmitter

from kitchend.core import jobs
from kitchend.core.ingest import Ingester


def _make_job(db, config, run_dir, state="running"):
    project_id = jobs.ensure_project_row(db, config.project("stub"))
    job_id = jobs.submit(db, project_id,
                         {"project": "stub", "experiments": ["x"],
                          "run_dir": str(run_dir)})
    db.execute("UPDATE jobs SET state = ? WHERE id = ?", (state, job_id))
    return job_id


def test_ingest_folds_events_into_progress(env, tmp_path):
    config, db, hub, runner, scheduler = env
    run_dir = tmp_path / "rd"
    job_id = _make_job(db, config, run_dir)
    ingester = Ingester(db, hub)

    with EventEmitter(run_dir / "events.jsonl", "r1") as em:
        em.emit("run.started", argv=["x"], adapter="toy",
                experiments=["exp"], out_root=str(run_dir))
        em.emit("experiment.started", name="exp", est_points=4)
        em.emit("point.started", experiment="exp", dims={"payload": 16},
                rate=1000, trial=0, rel_dir="payload_16/rate_1000/trial_0")
        em.emit("point.finished", experiment="exp", dims={"payload": 16},
                rate=1000, trial=0, rel_dir="payload_16/rate_1000/trial_0",
                status="ok", duration_s=2.0,
                metrics={"throughput_msgs_per_sec": 900})
        em.emit("point.started", experiment="exp", dims={"payload": 16},
                rate=2000, trial=0, rel_dir="payload_16/rate_2000/trial_0")

    ingester.poll_once()
    job = jobs.get(db, job_id)
    p = job["progress"]
    assert p["experiment"] == "exp"
    assert p["est_points"] == 4
    assert p["points"]["ok"] == 1 and p["points"]["done"] == 1
    assert p["current"]["rate"] == 2000
    assert p["last_metrics"]["throughput_msgs_per_sec"] == 900
    # 1 executed point at 2.0s, 3 remaining → 6s.
    assert p["eta_secs"] == 6.0
    assert job["events_offset"] > 0
    assert db.query("SELECT id FROM events WHERE type = 'job.progress'")

    # Incremental: only new events are read; counts accumulate.
    offset_before = job["events_offset"]
    with EventEmitter(run_dir / "events.jsonl", "r1") as em:
        em.emit("point.finished", experiment="exp", dims={"payload": 16},
                rate=2000, trial=0, rel_dir="payload_16/rate_2000/trial_0",
                status="dead", duration_s=1.0, metrics={})
        em.emit("search.decision", dims={"payload": 16}, rate=1000,
                action="halve", note="delivered/offered 0.4")
    ingester.poll_once()
    job = jobs.get(db, job_id)
    p = job["progress"]
    assert p["points"]["dead"] == 1 and p["points"]["done"] == 2
    assert p["last_decision"]["action"] == "halve"
    assert job["events_offset"] > offset_before


def test_final_pass_after_job_finishes(env, tmp_path):
    config, db, hub, runner, scheduler = env
    run_dir = tmp_path / "rd"
    job_id = _make_job(db, config, run_dir)
    ingester = Ingester(db, hub)

    with EventEmitter(run_dir / "events.jsonl", "r1") as em:
        em.emit("run.started", argv=[], adapter="toy", experiments=["exp"],
                out_root=str(run_dir))
    ingester.poll_once()   # job now tracked

    # Job finishes; the trailing run.finished lands after the last poll.
    with EventEmitter(run_dir / "events.jsonl", "r1") as em:
        em.emit("run.finished", exit_code=0, points_total=2, points_ok=2,
                points_dead=0, points_failed=0)
    db.execute("UPDATE jobs SET state = 'succeeded' WHERE id = ?", (job_id,))
    ingester.poll_once()   # final pass for the just-finished job

    p = jobs.get(db, job_id)["progress"]
    assert p["run_state"] == "finished"
    assert p["totals_final"]["points_ok"] == 2
    # Untracked now: another poll reads nothing and changes nothing.
    ingester.poll_once()
    assert jobs.get(db, job_id)["progress"] == p


def test_new_run_in_same_dir_resets_progress(env, tmp_path):
    """A resumed job shares its predecessor's run_dir and events.jsonl; its
    progress must count only its own run, not the predecessor's points."""
    config, db, hub, runner, scheduler = env
    run_dir = tmp_path / "rd"
    job_id = _make_job(db, config, run_dir)
    ingester = Ingester(db, hub)

    with EventEmitter(run_dir / "events.jsonl", "r1") as em:
        em.emit("run.started", argv=[], adapter="toy", experiments=["exp"],
                out_root=str(run_dir))
        em.emit("point.finished", experiment="exp", dims={}, rate=1000,
                trial=0, rel_dir="rate_1000/trial_0", status="ok",
                duration_s=1.0, metrics={})
        em.emit("run.interrupted", signal="SIGINT", points_completed=1)
        # The resumed invocation appends to the same file.
        em.emit("run.started", argv=[], adapter="toy", experiments=["exp"],
                out_root=str(run_dir))
        em.emit("point.skipped", experiment="exp", dims={}, rate=1000,
                trial=0, rel_dir="rate_1000/trial_0", reason="resume")
        em.emit("point.finished", experiment="exp", dims={}, rate=2000,
                trial=0, rel_dir="rate_2000/trial_0", status="ok",
                duration_s=1.0, metrics={})
        em.emit("run.finished", exit_code=0, points_total=2, points_ok=2,
                points_dead=0, points_failed=0)
    ingester.poll_once()

    p = jobs.get(db, job_id)["progress"]
    assert p["points"] == {"ok": 1, "dead": 0, "failed": 0,
                           "skipped": 1, "done": 2}
    assert p["run_state"] == "finished"


def test_jobs_without_events_have_no_progress(env, tmp_path):
    config, db, hub, runner, scheduler = env
    job_id = _make_job(db, config, tmp_path / "empty")
    Ingester(db, hub).poll_once()
    assert jobs.get(db, job_id)["progress"] is None


def test_dispatch_assigns_run_dir(env):
    config, db, hub, runner, scheduler = env

    async def main():
        project_id = jobs.ensure_project_row(db, config.project("stub"))
        job_id = jobs.submit(db, project_id,
                             {"project": "stub", "experiments": ["exit 0"],
                              "queue": "a"})
        task = asyncio.get_running_loop().create_task(scheduler.loop())
        scheduler.wake()
        deadline = time.monotonic() + 10
        try:
            while time.monotonic() < deadline:
                if jobs.get(db, job_id)["state"] not in jobs.ACTIVE_STATES:
                    break
                await asyncio.sleep(0.05)
        finally:
            scheduler.stop()
            task.cancel()
        job = jobs.get(db, job_id)
        assert job["state"] == jobs.SUCCEEDED
        assert job["run_dir"] and f"kitchen-job{job_id}-" in job["run_dir"]
        # The assigned dir lives under the project's repo (no runs_roots
        # configured on the stub → <repo>/runs).
        assert job["run_dir"].startswith(str(config.projects[0].repo_path))

    asyncio.run(main())


def test_retry_resumes_into_assigned_run_dir(env):
    config, db, hub, runner, scheduler = env

    async def main():
        project_id = jobs.ensure_project_row(db, config.project("stub"))
        job_id = jobs.submit(db, project_id,
                             {"project": "stub", "experiments": ["exit 1"],
                              "queue": "a", "max_retries": 1,
                              "retry_delay_secs": 0})
        task = asyncio.get_running_loop().create_task(scheduler.loop())
        scheduler.wake()
        deadline = time.monotonic() + 10
        child = None
        try:
            while time.monotonic() < deadline:
                rows = db.query("SELECT id FROM jobs WHERE parent_job_id = ?",
                                (job_id,))
                if rows:
                    child = jobs.get(db, rows[0]["id"])
                    if child["state"] not in jobs.ACTIVE_STATES:
                        break
                await asyncio.sleep(0.05)
        finally:
            scheduler.stop()
            task.cancel()
        parent = jobs.get(db, job_id)
        assert parent["run_dir"]
        # The retry resumes into the dir the daemon assigned to the parent.
        assert child["spec"]["run_dir"] == parent["run_dir"]
        assert child["spec"]["resume"] is True

    asyncio.run(main())
