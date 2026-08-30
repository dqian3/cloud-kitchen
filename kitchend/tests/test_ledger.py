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
    project_id = jobs.ensure_project_row(db, project)
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
