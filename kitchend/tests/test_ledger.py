import json

from kitchend.config import ProjectConfig
from kitchend.core import jobs, ledger
from kitchend.core.db import Db


def test_scan_keeps_batch_size_points_distinct_and_reconciles(tmp_path):
    runs = tmp_path / "runs"
    sweep = runs / "job" / "zyzzyva"
    sweep.mkdir(parents=True)
    entries = [
        {"p": 1, "payload_size": 1024, "max_in_flight": 0,
         "batch_size": 1, "rate": 1000, "throughput_msgs_per_sec": 900},
        {"p": 1, "payload_size": 1024, "max_in_flight": 0,
         "batch_size": 50, "rate": 1000, "throughput_msgs_per_sec": 950},
    ]
    (sweep / "sweep_results.json").write_text(json.dumps(entries))

    project = ProjectConfig(name="p", repo_path=tmp_path,
                            runs_roots=("runs",))
    db = Db(tmp_path / "db.sqlite3")
    jobs.ensure_project_row(db, project)
    ledger.scan_project(db, project)
    run_id = db.query_one("SELECT id FROM runs")["id"]

    points = db.query("SELECT dims_json FROM run_points WHERE run_id = ?",
                      (run_id,))
    assert sorted(json.loads(row["dims_json"])["batch_size"]
                  for row in points) == [1, 50]

    # A refresh is a reconciliation, not an append.
    ledger.scan_project(db, project)
    assert db.query_one(
        "SELECT COUNT(*) AS n FROM run_points WHERE run_id = ?", (run_id,)
    )["n"] == 2


def test_job_tags_land_on_the_runs_the_job_produces(tmp_path):
    """A campaign is grouped by tagging its submission, not its dirs."""
    runs = tmp_path / "runs"
    sweep = runs / "kitchen-job1-x" / "fp_diagonal"
    sweep.mkdir(parents=True)
    project = ProjectConfig(name="p", repo_path=tmp_path, runs_roots=("runs",))
    db = Db(tmp_path / "db.sqlite3")
    project_id = jobs.ensure_project_row(db, project)
    job_id = jobs.submit(db, project_id, {
        "project": "p", "experiments": ["fp_diagonal"],
        "run_dir": str(runs / "kitchen-job1-x"),
        "tags": ["ladder-v2", "  "],   # blank names are not tags
    })

    ledger.apply_events(db, project_id, job_id, str(runs / "kitchen-job1-x"), [
        {"type": "point.finished", "ts": "2026-09-07T00:00:00",
         "data": {"experiment": "fp_diagonal", "rate": 10000, "trial": 0,
                  "dims": {"f": 1, "p": 1}, "rel_dir": "f_1/p_1/rate_10000",
                  "metrics": {"throughput_msgs_per_sec": 9900}}},
    ])

    tagged = ledger.list_runs(db, tag="ladder-v2")
    assert [r["run_dir"] for r in tagged] == [str(sweep)]
    assert tagged[0]["tags"] == ["ladder-v2"]
    assert ledger.list_runs(db, tag="nope") == []

    # Idempotent: a resume writing into the same dir re-applies the tag.
    ledger.apply_events(db, project_id, job_id, str(runs / "kitchen-job1-x"), [
        {"type": "point.finished", "ts": "2026-09-07T00:01:00",
         "data": {"experiment": "fp_diagonal", "rate": 35000, "trial": 0,
                  "dims": {"f": 1, "p": 1}, "rel_dir": "f_1/p_1/rate_35000",
                  "metrics": {"throughput_msgs_per_sec": 34000}}},
    ])
    assert ledger.list_runs(db, tag="ladder-v2")[0]["tags"] == ["ladder-v2"]


def test_scan_tags_a_backfilled_dir_from_its_job(tmp_path):
    runs = tmp_path / "runs"
    sweep = runs / "kitchen-job2-x" / "fp_diagonal"
    sweep.mkdir(parents=True)
    (sweep / "sweep_results.json").write_text(json.dumps(
        [{"f": 1, "p": 1, "rate": 10000, "throughput_msgs_per_sec": 9900}]))
    project = ProjectConfig(name="p", repo_path=tmp_path, runs_roots=("runs",))
    db = Db(tmp_path / "db.sqlite3")
    project_id = jobs.ensure_project_row(db, project)
    jobs.submit(db, project_id, {
        "project": "p", "run_dir": str(runs / "kitchen-job2-x"),
        "tags": ["ladder-v2"],
    })

    ledger.scan_project(db, project)
    assert [r["run_dir"] for r in ledger.list_runs(db, tag="ladder-v2")] \
        == [str(sweep)]
