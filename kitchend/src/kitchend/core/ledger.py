"""The run ledger: what ran, with what, and what came out.

Deliberately thin — a registry and metrics store, not a results browser.
Plotting and deep analysis stay in each repo's own analyze scripts over the
run dirs; the ledger's job is that a run (a paper sweep or a one-off launched
on a hunch) is findable later with its provenance, per-point metrics, and a
note about why it ran and what it showed — even after its directory has been
deleted to reclaim disk.

One `runs` row per sweep dir (`<out_root>/<experiment>`), fed two ways:
live, from the events the ingester is already reading; and by `scan_project`,
a backfill over pre-existing dirs keyed on their `sweep_results.json`.
"""

import json
import os
from pathlib import Path

from kitchen.run import default_point_dead

from . import jobs


# --- live feed (called by the ingester with each batch of run events) ---

def apply_events(db, project_id, job_id, out_root, events) -> None:
    """Fold one batch of a job's run events into the ledger. Idempotent:
    re-reading the same events (a resumed job shares its predecessor's
    events.jsonl) updates rows rather than duplicating them."""
    for event in events:
        etype = event.get("type")
        d = event.get("data") or {}
        if etype in ("point.finished", "point.skipped") and d.get("experiment"):
            sweep_dir = _sweep_dir(out_root, d["experiment"])
            run_id = _ensure_run(db, project_id, job_id, sweep_dir,
                                 d["experiment"], event.get("ts"))
            metrics = dict(d.get("metrics") or {})
            if d.get("status") and d["status"] != "ok":
                metrics["status"] = d["status"]
            summary_path = (os.path.join(sweep_dir, d["rel_dir"], "summary.json")
                            if d.get("rel_dir") else None)
            # A skip event from an emitter that predates metrics-on-skip must
            # not clobber the metrics its predecessor's run already recorded.
            keep = etype == "point.skipped" and not d.get("metrics")
            _upsert_point(db, run_id, d.get("dims") or {}, d.get("rate"),
                          d.get("trial"), metrics, summary_path,
                          keep_existing=keep)
        elif etype == "experiment.finished" and d.get("name"):
            # No row means the experiment finished without a point: there is
            # nothing to record, and a run entry with no data is noise.
            row = db.query_one("SELECT id FROM runs WHERE run_dir = ?",
                               (_sweep_dir(out_root, d["name"]),))
            if row:
                n = db.query_one("SELECT COUNT(*) AS n FROM run_points "
                                 "WHERE run_id = ?", (row["id"],))["n"]
                db.execute("UPDATE runs SET status = ?, n_points = ? "
                           "WHERE id = ?",
                           (d.get("status") or "ok", n, row["id"]))


def _sweep_dir(out_root, experiment) -> str:
    return str(Path(out_root) / experiment)


def _ensure_run(db, project_id, job_id, run_dir, experiment, ts) -> int:
    """The row for a sweep dir, created on its first point. status is a
    fact about the data (set when the experiment finishes, or by a scan),
    never about the job: a job that dies mid-sweep leaves the points it
    finished, not a run stuck in a state."""
    row = db.query_one("SELECT id FROM runs WHERE run_dir = ?", (run_dir,))
    if row:
        # A resume re-runs into the same dir: note who wrote it last.
        db.execute("UPDATE runs SET job_id = ?, dir_exists = 1, "
                   "indexed_at = datetime('now') WHERE id = ?",
                   (job_id, row["id"]))
        return row["id"]
    return db.insert(
        "INSERT INTO runs (project_id, run_dir, experiment, started_at, "
        "job_id, dir_exists, indexed_at) "
        "VALUES (?, ?, ?, ?, ?, 1, datetime('now'))",
        (project_id, run_dir, experiment, ts, job_id))


def _upsert_point(db, run_id, dims, rate, trial, metrics, summary_path,
                  keep_existing=False) -> None:
    dims_json = json.dumps(dims, sort_keys=True)
    row = db.query_one(
        "SELECT id FROM run_points WHERE run_id = ? AND dims_json = ? "
        "AND rate IS ? AND trial IS ?", (run_id, dims_json, rate, trial))
    metrics_json = json.dumps(metrics)
    if row:
        if not keep_existing:
            db.execute("UPDATE run_points SET metrics_json = ?, "
                       "summary_path = ? WHERE id = ?",
                       (metrics_json, summary_path, row["id"]))
    else:
        db.insert(
            "INSERT INTO run_points (run_id, dims_json, rate, trial, "
            "metrics_json, summary_path) VALUES (?, ?, ?, ?, ?, ?)",
            (run_id, dims_json, rate, trial, metrics_json, summary_path))


# --- backfill: index pre-existing run dirs ---

def scan_project(db, project_cfg) -> dict:
    """Walk the project's runs roots for sweep dirs (anything holding a
    sweep_results.json — both old-driver and SweepEngine output) and upsert
    them. Cheap enough to re-run whole; existing rows are refreshed."""
    project_id = jobs.ensure_project_row(db, project_cfg)
    added = updated = 0
    roots = project_cfg.runs_roots or ("runs",)
    for root in roots:
        base = project_cfg.repo_path / root
        if not base.is_dir():
            continue
        for results_path in sorted(base.rglob("sweep_results.json")):
            was_new = _index_sweep_dir(db, project_id, results_path.parent)
            if was_new is None:
                continue
            added += was_new
            updated += not was_new
    return {"added": added, "updated": updated}


def _index_sweep_dir(db, project_id, sweep_dir: Path) -> bool | None:
    """Upsert one sweep dir; True if newly added, False if refreshed, None
    if unreadable."""
    try:
        entries = json.loads((sweep_dir / "sweep_results.json").read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(entries, list):
        return None

    started_at = git_commit = argv = None
    try:
        lines = (sweep_dir / "provenance.jsonl").read_text().splitlines()
        if lines:
            first = json.loads(lines[0])
            started_at = first.get("started_at")
            git_commit = first.get("git_commit")
            argv = json.dumps(first.get("argv"))
    except (OSError, ValueError):
        pass

    failed = any(isinstance(e, dict) and "error" in e for e in entries)
    dead = any(isinstance(e, dict) and "error" not in e
               and default_point_dead(e) for e in entries)
    status = "failed" if failed else ("degraded" if dead else "ok")

    run_dir = str(sweep_dir)
    # A scanned dir may be a job's output: the daemon assigns <run_dir> and
    # the engine writes <run_dir>/<experiment>/, so the job owning it is the
    # one whose run_dir is this dir or its parent.
    job_row = db.query_one(
        "SELECT id FROM jobs WHERE run_dir IN (?, ?) ORDER BY id DESC LIMIT 1",
        (run_dir, str(sweep_dir.parent)))
    job_id = job_row["id"] if job_row else None
    row = db.query_one("SELECT id FROM runs WHERE run_dir = ?", (run_dir,))
    if row:
        run_id = row["id"]
        db.execute(
            "UPDATE runs SET status = ?, n_points = ?, started_at = "
            "COALESCE(started_at, ?), git_commit = COALESCE(git_commit, ?), "
            "argv = COALESCE(argv, ?), dir_exists = 1, "
            "job_id = COALESCE(job_id, ?), "
            "indexed_at = datetime('now') WHERE id = ?",
            (status, len(entries), started_at, git_commit, argv, job_id, run_id))
    else:
        run_id = db.insert(
            "INSERT INTO runs (project_id, run_dir, experiment, started_at, "
            "git_commit, argv, n_points, status, job_id, dir_exists, indexed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'))",
            (project_id, run_dir, sweep_dir.name, started_at, git_commit,
             argv, len(entries), status, job_id))
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        # Old-driver entries are flat summary dicts with dim tags mixed in;
        # there is no dimension table to split them with, so the whole entry
        # is the metrics record and rate/trial are lifted for querying. Its
        # position in the file is the identity: two entries that differ only
        # in a dim the ledger cannot name (a payload size) must not collapse
        # into one, and re-scanning the same file must not duplicate them.
        _upsert_point(db, run_id, {"i": i},
                      entry.get("rate"), entry.get("trial"), entry, None)
    return row is None


# --- queries, tags, notes ---

def list_runs(db, project=None, experiment=None, tag=None, limit=100):
    sql = ("SELECT r.*, p.name AS project, "
           "(SELECT GROUP_CONCAT(t.name) FROM run_tags rt "
           " JOIN tags t ON t.id = rt.tag_id WHERE rt.run_id = r.id) AS tags "
           "FROM runs r JOIN projects p ON p.id = r.project_id")
    where, params = [], []
    if project:
        where.append("p.name = ?")
        params.append(project)
    if experiment:
        where.append("r.experiment = ?")
        params.append(experiment)
    if tag:
        where.append("EXISTS (SELECT 1 FROM run_tags rt JOIN tags t "
                     "ON t.id = rt.tag_id WHERE rt.run_id = r.id "
                     "AND t.name = ?)")
        params.append(tag)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY r.id DESC LIMIT ?"
    params.append(limit)
    out = [_run_to_dict(r) for r in db.query(sql, params)]
    for run in out:   # metrics outlive the dir; the flag says which is which
        exists = os.path.isdir(run["run_dir"])
        if bool(run["dir_exists"]) != exists:
            db.execute("UPDATE runs SET dir_exists = ? WHERE id = ?",
                       (int(exists), run["id"]))
            run["dir_exists"] = int(exists)
    return out


def deletable_dir(project_cfg, run_dir: str):
    """The run's directory if it is safe to delete: it must sit strictly
    inside one of the project's configured runs roots. Anything else (a
    path outside them, a root itself, a symlink out) returns None — the
    ledger must never be able to remove arbitrary directories."""
    if not run_dir:
        return None
    path = Path(run_dir).expanduser()
    try:
        path = path.resolve(strict=True)
    except OSError:
        return None
    if not path.is_dir():
        return None
    for rel in (project_cfg.runs_roots or ()):
        try:
            root = (project_cfg.repo_path / rel).resolve(strict=True)
        except OSError:
            continue
        if path != root and root in path.parents:
            return path
    return None


def delete_run(db, run_id) -> bool:
    """Drop a run from the ledger: its points, tags, and notes. The run
    directory on disk is untouched (a scan would index it again)."""
    if db.query_one("SELECT id FROM runs WHERE id = ?", (run_id,)) is None:
        return False
    for table in ("run_points", "run_tags", "notes"):
        db.execute(f"DELETE FROM {table} WHERE run_id = ?", (run_id,))
    db.execute("DELETE FROM runs WHERE id = ?", (run_id,))
    return True


def get_run(db, run_id):
    row = db.query_one(
        "SELECT r.*, p.name AS project FROM runs r "
        "JOIN projects p ON p.id = r.project_id WHERE r.id = ?", (run_id,))
    if row is None:
        return None
    run = _run_to_dict(row)
    run["tags"] = [r["name"] for r in db.query(
        "SELECT t.name FROM run_tags rt JOIN tags t ON t.id = rt.tag_id "
        "WHERE rt.run_id = ? ORDER BY t.name", (run_id,))]
    run["notes"] = [dict(r) for r in db.query(
        "SELECT id, ts, text FROM notes WHERE run_id = ? ORDER BY id",
        (run_id,))]
    run["points"] = [{
        "id": r["id"],
        "dims": json.loads(r["dims_json"]),
        "rate": r["rate"],
        "trial": r["trial"],
        "metrics": json.loads(r["metrics_json"]),
    } for r in db.query(
        "SELECT * FROM run_points WHERE run_id = ? ORDER BY id", (run_id,))]
    run["builds"] = builds_of(run["run_dir"])
    # More than one build behind one curve: resume keeps the points already
    # on disk, so an attempt after a rebuild measures the rest with different
    # binaries and says nothing about it.
    run["mixed_build"] = len({b["git_commit"] for b in run["builds"]}) > 1
    return run


def builds_of(run_dir) -> list:
    """Every invocation that wrote into this run dir, with the build it ran.

    Read from provenance.jsonl rather than stored: the ledger keeps the first
    commit it saw, and a resumed run's later attempts are exactly what that
    misses.
    """
    out = []
    try:
        lines = (Path(run_dir) / "provenance.jsonl").read_text().splitlines()
    except OSError:
        return out
    for line in lines:
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        out.append({"started_at": rec.get("started_at"),
                    "git_commit": rec.get("git_commit"),
                    "git_branch": rec.get("git_branch"),
                    "git_dirty": bool(rec.get("git_dirty"))})
    return out


def _run_to_dict(row):
    d = dict(row)
    if "tags" in d:  # list_runs selects it; NULL means untagged, not absent
        d["tags"] = d["tags"].split(",") if d["tags"] else []
    if d.get("argv"):
        try:
            d["argv"] = json.loads(d["argv"])
        except ValueError:
            pass
    return d


def add_note(db, run_id, text) -> int:
    return db.insert("INSERT INTO notes (run_id, text) VALUES (?, ?)",
                     (run_id, text))


def add_tag(db, run_id, name) -> None:
    row = db.query_one("SELECT id FROM tags WHERE name = ?", (name,))
    tag_id = row["id"] if row else db.insert(
        "INSERT INTO tags (name) VALUES (?)", (name,))
    db.execute("INSERT OR IGNORE INTO run_tags (run_id, tag_id) "
               "VALUES (?, ?)", (run_id, tag_id))


def remove_tag(db, run_id, name) -> None:
    db.execute("DELETE FROM run_tags WHERE run_id = ? AND tag_id IN "
               "(SELECT id FROM tags WHERE name = ?)", (run_id, name))
