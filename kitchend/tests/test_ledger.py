"""Run ledger: live event feed, backfill scan, tags and notes."""

import json

from kitchend.core import jobs, ledger


def _job(db, config, run_dir):
    project_id = jobs.ensure_project_row(db, config.project("stub"))
    job_id = jobs.submit(db, project_id,
                         {"project": "stub", "experiments": ["x"],
                          "run_dir": str(run_dir)})
    return project_id, job_id


def _ev(type_, **data):
    return {"type": type_, "ts": "2026-08-20T12:00:00Z", "data": data}


def test_live_feed_creates_runs_and_points(env, tmp_path):
    config, db, hub, runner, scheduler = env
    out_root = tmp_path / "rd"
    project_id, job_id = _job(db, config, out_root)

    ledger.apply_events(db, project_id, job_id, str(out_root), [
        _ev("experiment.started", name="exp", est_points=2),
        _ev("point.finished", experiment="exp", dims={"payload": 16},
            rate=1000, trial=0, rel_dir="payload_16/rate_1000/trial_0",
            status="ok", duration_s=1.0,
            metrics={"throughput_msgs_per_sec": 900}),
        _ev("point.finished", experiment="exp", dims={"payload": 16},
            rate=2000, trial=0, rel_dir="payload_16/rate_2000/trial_0",
            status="dead", duration_s=1.0, metrics={"total_completed": 0}),
        _ev("experiment.finished", name="exp", status="degraded",
            detail="1 ok, 1 dead, 0 failed"),
    ])

    runs = ledger.list_runs(db)
    assert len(runs) == 1
    run = ledger.get_run(db, runs[0]["id"])
    assert run["experiment"] == "exp"
    assert run["run_dir"] == str(out_root / "exp")
    assert run["status"] == "degraded"
    assert run["n_points"] == 2
    assert run["job_id"] == job_id
    assert len(run["points"]) == 2
    by_rate = {p["rate"]: p for p in run["points"]}
    assert by_rate[1000]["metrics"]["throughput_msgs_per_sec"] == 900
    assert by_rate[2000]["metrics"]["status"] == "dead"
    assert by_rate[1000]["dims"] == {"payload": 16}


def test_refeed_and_resume_are_idempotent(env, tmp_path):
    config, db, hub, runner, scheduler = env
    out_root = tmp_path / "rd"
    project_id, job_id = _job(db, config, out_root)
    first = [
        _ev("experiment.started", name="exp", est_points=1),
        _ev("point.finished", experiment="exp", dims={}, rate=1000, trial=0,
            rel_dir="rate_1000/trial_0", status="ok", duration_s=1.0,
            metrics={"throughput_msgs_per_sec": 900}),
        _ev("run.interrupted", signal="SIGINT", points_completed=1),
    ]
    ledger.apply_events(db, project_id, job_id, str(out_root), first)
    run = ledger.list_runs(db)[0]
    assert run["status"] == "interrupted"

    # The resumed job re-reads the whole file: its predecessor's events plus
    # its own skip (metrics ride along) and finish. No duplicate rows, and
    # an old-style skip without metrics must not clobber recorded metrics.
    _, job2 = _job(db, config, out_root)
    ledger.apply_events(db, project_id, job2, str(out_root), first + [
        _ev("experiment.started", name="exp", est_points=1),
        _ev("point.skipped", experiment="exp", dims={}, rate=1000, trial=0,
            rel_dir="rate_1000/trial_0", reason="resume"),
        _ev("experiment.finished", name="exp", status="ok", detail="1 ok"),
    ])
    runs = ledger.list_runs(db)
    assert len(runs) == 1
    run = ledger.get_run(db, runs[0]["id"])
    assert run["status"] == "ok"
    assert run["job_id"] == job2
    assert len(run["points"]) == 1
    assert run["points"][0]["metrics"]["throughput_msgs_per_sec"] == 900


def test_backfill_scan(env, tmp_path):
    config, db, hub, runner, scheduler = env
    # Old-driver-shaped sweep dir under the stub repo's runs root.
    sweep = tmp_path / "runs" / "experiment_x" / "aspen"
    sweep.mkdir(parents=True)
    (sweep / "sweep_results.json").write_text(json.dumps([
        {"rate": 1000, "trial": 0, "total_completed": 500,
         "throughput_msgs_per_sec": 480.0, "payload_size": 16},
        {"rate": 4000, "trial": 0, "total_completed": 0,
         "throughput_msgs_per_sec": 0.0, "payload_size": 16},
    ]))
    (sweep / "provenance.jsonl").write_text(json.dumps(
        {"started_at": "2026-08-01T00:00:00", "git_commit": "abc123",
         "argv": ["sweep.py", "--rates", "1000"]}) + "\n")

    result = ledger.scan_project(db, config.project("stub"))
    assert result == {"added": 1, "updated": 0}
    run = ledger.get_run(db, ledger.list_runs(db)[0]["id"])
    assert run["experiment"] == "aspen"
    assert run["status"] == "degraded"       # one point committed nothing
    assert run["git_commit"] == "abc123"
    assert run["argv"] == ["sweep.py", "--rates", "1000"]
    assert run["n_points"] == 2
    assert run["points"][0]["metrics"]["payload_size"] == 16

    # Re-scan refreshes rather than duplicating.
    assert ledger.scan_project(db, config.project("stub")) == \
        {"added": 0, "updated": 1}


def test_tags_notes_and_filters(env, tmp_path):
    config, db, hub, runner, scheduler = env
    out_root = tmp_path / "rd"
    project_id, job_id = _job(db, config, out_root)
    ledger.apply_events(db, project_id, job_id, str(out_root), [
        _ev("experiment.started", name="exp-a"),
        _ev("experiment.finished", name="exp-a", status="ok", detail=""),
        _ev("experiment.started", name="exp-b"),
        _ev("experiment.finished", name="exp-b", status="ok", detail=""),
    ])
    runs = ledger.list_runs(db)
    a = next(r for r in runs if r["experiment"] == "exp-a")
    ledger.add_tag(db, a["id"], "paper")
    ledger.add_tag(db, a["id"], "paper")     # idempotent
    ledger.add_note(db, a["id"], "gamma test: no effect")

    assert [r["id"] for r in ledger.list_runs(db, tag="paper")] == [a["id"]]
    assert ledger.list_runs(db, experiment="exp-b")[0]["experiment"] == "exp-b"
    detail = ledger.get_run(db, a["id"])
    assert detail["tags"] == ["paper"]
    assert detail["notes"][0]["text"] == "gamma test: no effect"
    ledger.remove_tag(db, a["id"], "paper")
    assert ledger.get_run(db, a["id"])["tags"] == []

    # dir_exists tracks the filesystem: these sweep dirs were never created,
    # so listing flips the ingest-time default off.
    assert all(r["dir_exists"] == 0 for r in ledger.list_runs(db))

def test_delete_run_removes_points_tags_notes(tmp_path):
    from kitchend.core import ledger
    from kitchend.core.db import Db
    db = Db(tmp_path / "db.sqlite3")
    pid = db.insert("INSERT INTO projects (name, repo_path) VALUES ('p', '/x')")
    rid = db.insert(
        "INSERT INTO runs (project_id, experiment, run_dir, status, dir_exists) "
        "VALUES (?, 'e', '/x/run', 'ok', 0)", (pid,))
    db.insert("INSERT INTO run_points (run_id, dims_json, rate, trial, metrics_json) "
              "VALUES (?, '{}', 1.0, 0, '{}')", (rid,))
    db.insert("INSERT INTO notes (run_id, text) VALUES (?, 'n')", (rid,))
    assert ledger.delete_run(db, rid) is True
    assert ledger.get_run(db, rid) is None
    assert db.query("SELECT id FROM run_points WHERE run_id = ?", (rid,)) == []
    assert db.query("SELECT id FROM notes WHERE run_id = ?", (rid,)) == []
    assert ledger.delete_run(db, rid) is False
    db.close()
