import asyncio
import json
import time

from kitchend.config import Config, ProjectConfig
from kitchend.core import jobs
from kitchend.core.db import Db
from kitchend.core.scheduler import Scheduler


class FakeHub:
    def __init__(self):
        self.events = []

    def emit(self, kind, **payload):
        self.events.append((kind, payload))


class FakeRunner:
    def __init__(self, exit_code):
        self.exit_code = exit_code
        self.calls = []

    async def run(self, job_id, argv, cwd, on_start=None):
        self.calls.append(list(argv))
        if on_start:
            on_start(1234)
        return self.exit_code


class FakeClusters:
    def __init__(self):
        self.calls = []

    async def up(self, key, purpose="user", **kwargs):
        self.calls.append(("up", key, purpose))

    def release(self, key):
        self.calls.append(("release", key))

    async def down(self, key, force=False):
        self.calls.append(("down", key, force))


def setup_scheduler(tmp_path, exit_code):
    project = ProjectConfig(name="p", repo_path=tmp_path)
    config = Config(db_path=tmp_path / "db.sqlite3",
                    jobs_dir=tmp_path / "jobs", projects=(project,))
    db = Db(config.db_path)
    hub = FakeHub()
    clusters = FakeClusters()
    runner = FakeRunner(exit_code)
    scheduler = Scheduler(config, db, hub, runner, clusters)
    project_id = jobs.ensure_project_row(db, project)
    return scheduler, db, clusters, project_id


def submit(db, project_id):
    return jobs.submit(db, project_id, {
        "project": "p",
        "command": ["fake-driver"],
        "cluster": "c",
        "max_attempts": 3,
        "retry_delay_secs": 120,
    })


def test_cluster_is_kept_for_next_job_on_same_cluster(tmp_path):
    scheduler, db, clusters, project_id = setup_scheduler(tmp_path, exit_code=0)
    first = submit(db, project_id)
    second = submit(db, project_id)

    asyncio.run(scheduler._run(jobs.get(db, first)))

    assert jobs.get(db, first)["outcome"] == jobs.OK
    assert jobs.get(db, second)["state"] == jobs.WAITING
    assert ("release", "p/c") in clusters.calls
    assert not any(call[0] == "down" for call in clusters.calls)


def test_cluster_is_kept_between_retry_attempts(tmp_path):
    scheduler, db, clusters, project_id = setup_scheduler(tmp_path, exit_code=1)
    job_id = submit(db, project_id)

    asyncio.run(scheduler._run(jobs.get(db, job_id)))

    job = jobs.get(db, job_id)
    assert job["state"] == jobs.WAITING
    assert job["attempts"] == 1
    assert ("release", "p/c") in clusters.calls
    assert not any(call[0] == "down" for call in clusters.calls)


def test_canceled_acquisition_hands_cluster_to_next_job(tmp_path):
    scheduler, db, clusters, project_id = setup_scheduler(tmp_path, exit_code=0)
    first_id = submit(db, project_id)
    second_id = submit(db, project_id)
    first = jobs.get(db, first_id)
    jobs.finish(db, scheduler.hub, first_id, jobs.CANCELED)

    asyncio.run(scheduler._handoff_cluster(first, "p/c"))

    assert jobs.get(db, second_id)["state"] == jobs.WAITING
    assert ("release", "p/c") in clusters.calls
    assert not any(call[0] == "down" for call in clusters.calls)


def test_retry_uses_same_directory_and_resume_flag(tmp_path):
    scheduler, db, _, project_id = setup_scheduler(tmp_path, exit_code=1)
    job_id = submit(db, project_id)

    asyncio.run(scheduler._run(jobs.get(db, job_id)))
    first = jobs.get(db, job_id)
    asyncio.run(scheduler._run(first))

    first_argv, retry_argv = scheduler.runner.calls
    assert "--resume" not in first_argv
    assert "--resume" in retry_argv
    assert retry_argv[retry_argv.index("--output-dir") + 1] == first["run_dir"]


def test_resubmit_resume_and_fresh_have_distinct_directories(tmp_path):
    scheduler, db, _, project_id = setup_scheduler(tmp_path, exit_code=0)
    old_dir = str(tmp_path / "existing-result")
    source = jobs.submit(db, project_id, {
        "project": "p", "command": ["fake-driver"],
        "run_dir": old_dir, "resume": True,
    })

    resumed = jobs.get(db, scheduler.resubmit(source, resume=True))
    fresh = jobs.get(db, scheduler.resubmit(source, resume=False))

    assert resumed["run_dir"] == old_dir
    assert resumed["spec"]["run_dir"] == old_dir
    assert resumed["spec"]["resume"] is True
    assert resumed["will_resume"] is True
    assert fresh["run_dir"] is None
    assert "run_dir" not in fresh["spec"]
    assert fresh["spec"]["resume"] is False
    assert fresh["will_resume"] is False


def test_add_trials_resumes_result_at_next_offset(tmp_path):
    scheduler, db, _, project_id = setup_scheduler(tmp_path, exit_code=0)
    job_dir = tmp_path / "existing-result"
    sweep_dir = job_dir / "exp"
    sweep_dir.mkdir(parents=True)
    source = jobs.submit(db, project_id, {
        "project": "p", "name": "exp", "experiments": ["exp"],
        "command": ["fake-driver", "--trials", "3"],
        "extra_flags": ["--trial-offset=0"], "run_dir": str(job_dir),
    })
    jobs.finish(db, scheduler.hub, source, jobs.OK)
    run_id = db.insert(
        "INSERT INTO runs (project_id, run_dir, experiment, job_id) "
        "VALUES (?, ?, ?, ?)",
        (project_id, str(sweep_dir), "exp", source))
    for trial in (0, 1, 2):
        db.insert(
            "INSERT INTO run_points (run_id, dims_json, rate, trial, metrics_json) "
            "VALUES (?, '{}', 1000, ?, '{}')", (run_id, trial))

    result = scheduler.add_trials(run_id, 2)
    added = jobs.get(db, result["job_id"])
    argv, _ = jobs.build_command(scheduler.config.project("p"), added["spec"])

    assert result["trial_offset"] == 3
    assert added["run_dir"] == str(job_dir)
    assert added["will_resume"] is True
    assert "extra_flags" not in added["spec"]
    assert argv.count("--trials") == 1
    assert argv[argv.index("--trials") + 1] == "2"
    assert argv.count("--trial-offset") == 1
    assert argv[argv.index("--trial-offset") + 1] == "3"
    assert argv[argv.index("--output-dir") + 1] == str(job_dir)
    assert "--resume" in argv


def test_add_trials_rejects_parallel_extension(tmp_path):
    scheduler, db, _, project_id = setup_scheduler(tmp_path, exit_code=0)
    job_dir = tmp_path / "existing-result"
    sweep_dir = job_dir / "exp"
    sweep_dir.mkdir(parents=True)
    source = jobs.submit(db, project_id, {
        "project": "p", "name": "exp", "experiments": ["exp"],
        "command": ["fake-driver"], "run_dir": str(job_dir),
    })
    jobs.finish(db, scheduler.hub, source, jobs.OK)
    run_id = db.insert(
        "INSERT INTO runs (project_id, run_dir, experiment, job_id) "
        "VALUES (?, ?, ?, ?)",
        (project_id, str(sweep_dir), "exp", source))
    db.insert(
        "INSERT INTO run_points (run_id, dims_json, rate, trial, metrics_json) "
        "VALUES (?, '{}', 1000, 0, '{}')", (run_id,))

    scheduler.add_trials(run_id, 1)

    try:
        scheduler.add_trials(run_id, 1)
        assert False, "expected a parallel extension to be rejected"
    except ValueError as e:
        assert "already extending" in str(e)


def test_retry_point_targets_exact_existing_identity(tmp_path):
    scheduler, db, _, project_id = setup_scheduler(tmp_path, exit_code=0)
    job_dir = tmp_path / "existing-result"
    sweep_dir = job_dir / "exp"
    sweep_dir.mkdir(parents=True)
    source = jobs.submit(db, project_id, {
        "project": "p", "name": "exp",
        "command": ["fake-driver", "--rates", "1000", "2000"],
        "run_dir": str(job_dir),
    })
    jobs.finish(db, scheduler.hub, source, jobs.OK)
    run_id = db.insert(
        "INSERT INTO runs (project_id, run_dir, experiment, job_id) "
        "VALUES (?, ?, ?, ?)",
        (project_id, str(sweep_dir), "exp", source))
    point_id = db.insert(
        "INSERT INTO run_points (run_id, dims_json, rate, trial, metrics_json) "
        "VALUES (?, ?, 2000, 3, '{}')", (run_id, '{"gamma":1.2}'))

    result = scheduler.retry_point(run_id, point_id)
    queued = jobs.get(db, result["job_id"])
    argv, _ = jobs.build_command(scheduler.config.project("p"), queued["spec"])

    assert queued["run_dir"] == str(job_dir)
    assert argv[argv.index("--trials") + 1] == "1"
    assert argv[argv.index("--trial-offset") + 1] == "3"
    target = json.loads(argv[argv.index("--retry-point") + 1])
    assert target == {"dims": {"gamma": 1.2}, "rate": 2000.0, "trial": 3}
    assert "--resume" in argv


def test_global_pause_survives_daemon_restart(tmp_path):
    scheduler, db, clusters, _ = setup_scheduler(tmp_path, exit_code=0)
    db.insert(
        "INSERT INTO events (type, payload_json) VALUES (?, ?)",
        ("scheduler.paused", "{}"),
    )

    restarted = Scheduler(
        scheduler.config, db, FakeHub(), scheduler.runner, clusters)

    assert restarted.paused() is True


# --- reordering while a cluster comes up ----------------------------------
#
# Bring-up is minutes on a real fleet. For all of it the head holds the queue
# slot with its row still `waiting`, no attempt spent and nothing run, so the
# order can still change; once the driver spawns it cannot.

class ReorderingClusters(FakeClusters):
    """Runs `during` inside up(), standing in for an operator reordering the
    queue while the fleet is still coming up."""

    def __init__(self, during):
        super().__init__()
        self.during = during

    async def up(self, key, purpose="user", **kwargs):
        await super().up(key, purpose, **kwargs)
        self.during()


def test_slot_is_handed_over_when_reordered_during_bringup(tmp_path):
    scheduler, db, _, project_id = setup_scheduler(tmp_path, exit_code=0)
    head = submit(db, project_id)
    other = submit(db, project_id)
    clusters = ReorderingClusters(
        lambda: jobs.reorder(db, scheduler.hub, [other, head]))
    scheduler.clusters = clusters

    asyncio.run(scheduler._run(jobs.get(db, head)))

    # It yielded: no driver, no attempt spent, still queued.
    assert scheduler.runner.calls == []
    assert jobs.get(db, head)["state"] == jobs.WAITING
    assert jobs.get(db, head)["attempts"] == 0
    assert any(k == "job.preempted" and p["by_job"] == other
               for k, p in scheduler.hub.events)
    # And the fleet stays up for the job that now leads, rather than bouncing.
    assert ("release", "p/c") in clusters.calls
    assert not any(call[0] == "down" for call in clusters.calls)


def test_head_keeps_the_slot_when_nothing_outranks_it(tmp_path):
    scheduler, db, clusters, project_id = setup_scheduler(tmp_path, exit_code=0)
    head = submit(db, project_id)
    submit(db, project_id)

    asyncio.run(scheduler._run(jobs.get(db, head)))

    assert len(scheduler.runner.calls) == 1
    assert jobs.get(db, head)["outcome"] == jobs.OK
    assert not any(k == "job.preempted" for k, _ in scheduler.hub.events)


def test_slot_is_not_yielded_to_a_retry_delayed_job(tmp_path):
    """A job that outranks the head but is waiting out a retry delay is not a
    reason to give up the slot -- yielding to it would leave the fleet up with
    nothing to run."""
    scheduler, db, _, project_id = setup_scheduler(tmp_path, exit_code=0)
    head = submit(db, project_id)
    delayed = submit(db, project_id)
    scheduler._wait_until[delayed] = time.monotonic() + 3600
    clusters = ReorderingClusters(
        lambda: jobs.reorder(db, scheduler.hub, [delayed, head]))
    scheduler.clusters = clusters

    asyncio.run(scheduler._run(jobs.get(db, head)))

    assert len(scheduler.runner.calls) == 1
    assert not any(k == "job.preempted" for k, _ in scheduler.hub.events)
