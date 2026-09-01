"""Where a newly queued job lands.

Priority is a dense rank read only by `ORDER BY priority DESC, id ASC`, so
placement is a renumber. The default matters: a bare submit used to take
priority 0, which sinks below any reordered set, so every new job arrived at
the very bottom of the queue regardless of which cluster it was for.
"""

import pytest

from kitchend.config import Config, ProjectConfig
from kitchend.core import jobs
from kitchend.core.db import Db


class FakeHub:
    def __init__(self):
        self.events = []

    def emit(self, kind, **payload):
        self.events.append((kind, payload))


def setup(tmp_path):
    project = ProjectConfig(name="p", repo_path=tmp_path)
    config = Config(db_path=tmp_path / "db.sqlite3", projects=(project,))
    db = Db(config.db_path)
    return db, FakeHub(), jobs.ensure_project_row(db, project)


def add(db, hub, project_id, cluster, after=None):
    job_id = jobs.submit(db, project_id, {
        "project": "p", "command": ["fake-driver"], "cluster": cluster})
    jobs.place(db, hub, job_id, after)
    return job_id


def test_new_job_lands_behind_its_own_cluster_not_at_the_end(tmp_path):
    db, hub, project_id = setup(tmp_path)
    main1 = add(db, hub, project_id, "main")
    n51 = add(db, hub, project_id, "n51")
    main2 = add(db, hub, project_id, "main")

    assert jobs.waiting_order(db) == [main1, main2, n51]


def test_a_first_job_for_a_cluster_goes_to_the_end(tmp_path):
    db, hub, project_id = setup(tmp_path)
    main = add(db, hub, project_id, "main")
    n51 = add(db, hub, project_id, "n51")

    assert jobs.waiting_order(db) == [main, n51]


def test_siblings_keep_their_submission_order(tmp_path):
    """A fan-out enqueues one job per experiment; each falls in behind the
    last, so the group stays in the order it was asked for."""
    db, hub, project_id = setup(tmp_path)
    first = add(db, hub, project_id, "main")
    rest = [add(db, hub, project_id, "main") for _ in range(3)]

    assert jobs.waiting_order(db) == [first, *rest]


def test_after_places_directly_behind_that_job(tmp_path):
    db, hub, project_id = setup(tmp_path)
    head = add(db, hub, project_id, "main")
    tail = add(db, hub, project_id, "main")
    inserted = add(db, hub, project_id, "main", after=head)

    assert jobs.waiting_order(db) == [head, inserted, tail]


def test_after_crosses_clusters_when_asked(tmp_path):
    """An explicit `after` outranks the same-cluster default."""
    db, hub, project_id = setup(tmp_path)
    main = add(db, hub, project_id, "main")
    n51 = add(db, hub, project_id, "n51")
    late = add(db, hub, project_id, "main", after=n51)

    assert jobs.waiting_order(db) == [main, n51, late]


def test_after_a_job_that_is_not_waiting_is_refused(tmp_path):
    db, hub, project_id = setup(tmp_path)
    done = add(db, hub, project_id, "main")
    jobs.finish(db, FakeHub(), done, jobs.OK)

    with pytest.raises(ValueError, match="not waiting"):
        add(db, hub, project_id, "main", after=done)


def test_placement_survives_a_reorder(tmp_path):
    """The bug this guards: reorder assigns N..1, so a later default submit at
    priority 0 fell below every reordered job."""
    db, hub, project_id = setup(tmp_path)
    a = add(db, hub, project_id, "main")
    b = add(db, hub, project_id, "n51")
    jobs.reorder(db, hub, [b, a])

    c = add(db, hub, project_id, "n51")

    assert jobs.waiting_order(db) == [b, c, a]


def test_a_refused_placement_queues_nothing(tmp_path):
    """A placement that cannot be honoured must not leave the job queued at
    the bottom -- which is the position `after` was given to avoid."""
    from kitchend.config import Config, ProjectConfig
    from kitchend.core import submission

    class FakeScheduler:
        def wake(self):
            pass

    db, hub, _ = setup(tmp_path)
    project = ProjectConfig(name="p", repo_path=tmp_path)
    before = jobs.waiting_order(db)

    with pytest.raises(ValueError, match="not waiting"):
        submission.enqueue(db, hub, FakeScheduler(), project, {
            "project": "p", "command": ["fake-driver"],
            "cluster": "main", "after": 999})

    assert jobs.waiting_order(db) == before
