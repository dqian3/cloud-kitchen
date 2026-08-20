"""Tail each running job's <run_dir>/events.jsonl into a progress summary.

A native run (SweepEngine, or anything speaking the kitchen event contract)
appends structured events to its run dir. The daemon polls those files from
the per-job byte offset stored on the jobs row, folds new events into a
compact progress dict (points done, current point, ETA, last search
decision), persists both, and re-emits `job.progress` on the hub so the UI
updates live. Drivers that emit nothing (the interim aspen driver) never
produce a progress row and keep the log tail as their only signal.

Polling rather than inotify, matching kitchen.events.reader: it costs nothing
at this scale and inotify is unreliable on some filesystems (WSL2 included).
"""

import asyncio
import json
from pathlib import Path

from kitchen.events import read_all


def new_progress() -> dict:
    return {
        "experiment": None,
        "phase": None,
        "est_points": None,
        "points": {"ok": 0, "dead": 0, "failed": 0, "skipped": 0, "done": 0},
        "current": None,
        "last_decision": None,
        "last_metrics": None,
        "duration_sum_s": 0.0,
        "eta_secs": None,
        "run_state": None,
        "exit_code": None,
        "totals_final": None,
        "last_ts": None,
    }


def apply_event(p: dict, event: dict) -> None:
    """Fold one events.jsonl event into the progress dict. Unknown types are
    ignored — the contract says consumers must tolerate them."""
    etype = event.get("type")
    d = event.get("data") or {}
    p["last_ts"] = event.get("ts")
    if etype == "run.started":
        # A new invocation started writing to this run dir. A resumed or
        # retried job shares its predecessor's events.jsonl (same run_dir),
        # so counts folded before this point belong to a previous run —
        # start over rather than double-counting its points.
        p.clear()
        p.update(new_progress())
        p["last_ts"] = event.get("ts")
        p["run_state"] = "running"
    elif etype == "run.phase":
        p["phase"] = d.get("phase")
        p["experiment"] = d.get("experiment") or p["experiment"]
    elif etype == "experiment.started":
        p["experiment"] = d.get("name")
        if d.get("est_points"):
            p["est_points"] = (p["est_points"] or 0) + d["est_points"]
    elif etype == "point.started":
        p["current"] = {k: d.get(k)
                        for k in ("experiment", "dims", "rate", "trial",
                                  "rel_dir")}
    elif etype == "point.finished":
        status = {"ok": "ok", "dead": "dead"}.get(d.get("status"), "failed")
        p["points"][status] += 1
        p["points"]["done"] += 1
        p["current"] = None
        if d.get("duration_s"):
            p["duration_sum_s"] += d["duration_s"]
        if d.get("metrics"):
            p["last_metrics"] = d["metrics"]
    elif etype == "point.skipped":
        p["points"]["skipped"] += 1
        p["points"]["done"] += 1
    elif etype == "search.decision":
        p["last_decision"] = {k: d.get(k) for k in ("action", "rate", "note")}
    elif etype == "run.finished":
        p["run_state"] = "finished"
        p["exit_code"] = d.get("exit_code")
        p["totals_final"] = {k: d.get(k) for k in
                             ("points_total", "points_ok", "points_dead",
                              "points_failed")}
        p["current"] = None
    elif etype == "run.interrupted":
        p["run_state"] = "interrupted"
        p["current"] = None


def update_eta(p: dict) -> None:
    """ETA from the average executed-point duration. Resumed (skipped) points
    took no time and would drag the average, so they count toward done but
    not toward the divisor. Unknowable under a rate search (no est_points)."""
    executed = p["points"]["done"] - p["points"]["skipped"]
    est = p["est_points"]
    if not est or executed <= 0 or p["run_state"] != "running":
        p["eta_secs"] = None
        return
    remaining = max(0, est - p["points"]["done"])
    p["eta_secs"] = round(remaining * p["duration_sum_s"] / executed, 1)


class Ingester:
    """Polls running jobs' event files; one instance per daemon."""

    def __init__(self, db, hub, poll_s=1.0):
        self.db = db
        self.hub = hub
        self.poll_s = poll_s
        self._tracked: set[int] = set()   # jobs to give a final pass after
        self._stopped = False             # they leave 'running'

    async def loop(self):
        while not self._stopped:
            try:
                self.poll_once()
            except Exception as e:  # keep the ingester alive on any bug
                self.hub.emit("ingest.error", error=repr(e))
            await asyncio.sleep(self.poll_s)

    def stop(self):
        self._stopped = True

    def poll_once(self):
        # Besides running jobs, sweep anything that finished in the last few
        # seconds: a job fast enough to start and finish between two polls
        # (e.g. a resume where every point skips) is otherwise never seen in
        # 'running' at all. ingest_job reads from the stored offset, so the
        # repeat passes on an already-drained file are no-ops.
        rows = self.db.query(
            "SELECT id, state FROM jobs WHERE run_dir IS NOT NULL "
            "AND (state = 'running' "
            "     OR finished_at > datetime('now', '-10 seconds'))")
        running = {r["id"] for r in rows if r["state"] == "running"}
        recent = {r["id"] for r in rows}
        # _tracked additionally gives every job seen running one final read
        # after it leaves that state, so the tail of its event file
        # (run.finished included) is never lost.
        for job_id in sorted(recent | self._tracked):
            self.ingest_job(job_id)
        self._tracked = running

    def ingest_job(self, job_id) -> dict | None:
        """Read new events for one job; returns the updated progress, or None
        if there was nothing new."""
        row = self.db.query_one(
            "SELECT run_dir, events_offset, progress_json FROM jobs "
            "WHERE id = ?", (job_id,))
        if row is None or not row["run_dir"]:
            return None
        path = Path(row["run_dir"]) / "events.jsonl"
        events, offset = read_all(path, row["events_offset"] or 0)
        if not events:
            return None
        progress = (json.loads(row["progress_json"]) if row["progress_json"]
                    else new_progress())
        for event in events:
            apply_event(progress, event)
        update_eta(progress)
        self.db.execute(
            "UPDATE jobs SET events_offset = ?, progress_json = ? "
            "WHERE id = ?", (offset, json.dumps(progress), job_id))
        self.hub.emit("job.progress", job_id=job_id, progress=progress)
        return progress
