import pytest

from kitchend.config import Config, ProjectConfig
from kitchend.core.db import Db
from kitchend.core.hub import EventHub
from kitchend.core.runner import JobRunner
from kitchend.core.scheduler import Scheduler


@pytest.fixture
def env(tmp_path):
    """A daemon core wired to a temp DB with one stub project."""
    project = ProjectConfig(
        name="stub", repo_path=tmp_path, driver=("bash", "-c"), driver_cwd=".",
    )
    config = Config(
        db_path=tmp_path / "db.sqlite3",
        jobs_dir=tmp_path / "jobs",
        projects=(project,),
    )
    db = Db(config.db_path)
    hub = EventHub(db)
    runner = JobRunner(config.jobs_dir)
    scheduler = Scheduler(config, db, hub, runner)
    yield config, db, hub, runner, scheduler
    db.close()
