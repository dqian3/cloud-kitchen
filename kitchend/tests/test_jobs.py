from pathlib import Path

import pytest

from kitchend.config import ProjectConfig
from kitchend.core import jobs


def _project(tmp_path, **kw):
    defaults = dict(
        name="proj", repo_path=tmp_path,
        driver=("python3", "run.py"), driver_cwd="scripts",
    )
    defaults.update(kw)
    return ProjectConfig(**defaults)


def test_build_command_driver(tmp_path):
    p = _project(tmp_path)
    argv, cwd = jobs.build_command(p, {"experiments": ["aspen", "flutter"],
                                       "extra_flags": ["--trials", "3"]})
    assert argv == ["python3", "run.py", "aspen", "flutter", "--trials", "3"]
    assert cwd == tmp_path / "scripts"


def test_build_command_run_dir_and_resume(tmp_path):
    p = _project(tmp_path)
    argv, _ = jobs.build_command(p, {"experiments": ["aspen"],
                                     "run_dir": "/runs/x", "resume": True})
    assert argv[-3:] == ["--output-dir", "/runs/x", "--resume"]


def test_build_command_explicit_wins(tmp_path):
    p = _project(tmp_path)
    argv, _ = jobs.build_command(p, {"command": ["bash", "-c", "true"],
                                     "experiments": ["ignored"]})
    assert argv == ["bash", "-c", "true"]


def test_build_command_no_driver_errors(tmp_path):
    p = _project(tmp_path, driver=())
    with pytest.raises(ValueError, match="no driver"):
        jobs.build_command(p, {"experiments": ["aspen"]})


def test_queue_key_precedence():
    job = {"project": "p", "spec": {"queue": "q", "cluster": "c"}}
    assert jobs.queue_key(job) == "q"
    job = {"project": "p", "spec": {"cluster": "c"}}
    assert jobs.queue_key(job) == "c"
    job = {"project": "p", "spec": {}}
    assert jobs.queue_key(job) == "p"


def test_edit_queued_job(tmp_path):
    from kitchend.config import Config
    from kitchend.core.db import Db
    from kitchend.core.hub import EventHub

    p = _project(tmp_path)
    config = Config(db_path=tmp_path / "db.sqlite3", projects=(p,))
    db = Db(config.db_path)
    hub = EventHub(db)
    project_id = jobs.ensure_project_row(db, p)
    job_id = jobs.submit(db, project_id, {"project": "proj",
                                          "experiments": ["a"], "priority": 0})
    edited = jobs.update_queued(db, hub, job_id,
                                {"experiments": ["b", "c"], "priority": 5,
                                 "max_retries": 2})
    assert edited["spec"]["experiments"] == ["b", "c"]
    assert edited["priority"] == 5
    assert edited["retries_left"] == 2

    # Not editable once past queued.
    db.execute("UPDATE jobs SET state = 'running' WHERE id = ?", (job_id,))
    with pytest.raises(ValueError, match="not queued"):
        jobs.update_queued(db, hub, job_id, {"priority": 9})
    db.close()
