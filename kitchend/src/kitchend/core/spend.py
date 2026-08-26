"""What the clusters cost, derived from the event log.

The daemon cannot read GCP billing (the project bills to an account the user
has no permission on), and GCP itself keeps only the latest start/stop
timestamp per VM — so the event log is the only complete record of what ran
and for how long. Every bring-up leaves `cluster.state` transitions, so cost
is reconstructible for failed probes as well as for real runs, which is the
distinction that matters when a stockout has the queue probing all day.

The model, deliberately an upper bound and reported alongside VM-minutes so
it can be checked:

  starting → terminated   a probe: the VMs that did start, for the window
                          (the ones that failed cost nothing)
  starting → running      ramp-up: the whole cluster, for the window
  running/unmanaged → …   the whole cluster, until it stops

`gcloud compute instances describe` gives lastStart/lastStopTimestamp for the
most recent cycle, which is how the model was checked: 3m31s metered against
a 3m45s window.
"""

import json
from datetime import datetime

UP_STATES = ("running", "unmanaged", "stopping")


def report(db, since=None) -> dict:
    """Cluster time and cost, split into probes and runs.

    A probe is a bring-up that never reached `running` — the cluster was
    asked for and could not be had.
    """
    where = "WHERE type IN ('cluster.state', 'cluster.error', 'job.cluster')"
    params = []
    if since:
        where += " AND ts >= ?"
        params.append(since)
    rows = db.query(f"SELECT id, ts, job_id, type, payload_json FROM events "
                    f"{where} ORDER BY id", params)

    meta = {r["name"]: (r["vm_count"] or 0, r["hourly_usd"] or 0.0)
            for r in db.query("SELECT name, vm_count, hourly_usd FROM clusters")}
    names = {r["id"]: (json.loads(r["spec_json"]) or {}).get("name")
             for r in db.query("SELECT id, spec_json FROM jobs")}

    open_win = {}      # cluster key -> dict(start, reached_running, job_id, short)
    spans = []
    for r in rows:
        p = json.loads(r["payload_json"])
        key = p.get("cluster")
        if not key:
            continue
        if r["type"] == "job.cluster" and p.get("action") == "acquiring":
            # p["job_id"] is where a deleted job's id lands (see jobs.delete).
            open_win.setdefault(key, {}).update(
                job_id=r["job_id"] or p.get("job_id"))
            continue
        if r["type"] == "cluster.error":
            err = p.get("error") or ""
            if "failed for" in err:
                win = open_win.setdefault(key, {})
                win["short"] = int(err.split("failed for ")[1].split("/")[0])
            continue
        state = p.get("state")
        win = open_win.setdefault(key, {})
        if state == "starting":
            win.update(start=r["ts"], running_at=None, short=0)
        elif state in UP_STATES:
            win.setdefault("start", r["ts"])
            win.setdefault("running_at", r["ts"])
            if win.get("running_at") is None:
                win["running_at"] = r["ts"]
        elif state == "terminated" and win.get("start"):
            spans.append(_span(key, win, r["ts"], meta))
            open_win.pop(key, None)

    out = {"probes": _bucket(), "runs": _bucket(), "by_cluster": {},
           "by_job": {}, "by_day": {}}
    for s in spans:
        kind = "runs" if s["ran"] else "probes"
        _add(out[kind], s)
        _add(out["by_cluster"].setdefault(s["cluster"], _bucket()), s)
        _add(out["by_day"].setdefault(s["day"], _bucket()), s)
        if s["job_id"]:
            label = names.get(s["job_id"]) or f"job {s['job_id']}"
            _add(out["by_job"].setdefault(f"#{s['job_id']} {label}", _bucket()), s)
    out["total_usd"] = round(out["probes"]["usd"] + out["runs"]["usd"], 2)
    return out


def _span(key, win, end_ts, meta):
    size, hourly = meta.get(key.split("/")[-1], (0, 0.0))
    start = _parse(win["start"])
    secs = max(0.0, (_parse(end_ts) - start).total_seconds())
    ran = bool(win.get("running_at"))
    vms = size if ran else max(0, size - int(win.get("short") or 0))
    return {"cluster": key, "job_id": win.get("job_id"), "ran": ran,
            "secs": secs, "vms": vms, "usd": vms * secs / 3600 * hourly,
            "day": win["start"][:10]}


def _bucket():
    return {"n": 0, "vm_minutes": 0.0, "usd": 0.0}


def _add(b, s):
    b["n"] += 1
    b["vm_minutes"] = round(b["vm_minutes"] + s["vms"] * s["secs"] / 60, 1)
    b["usd"] = round(b["usd"] + s["usd"], 2)


def _parse(ts):
    return datetime.fromisoformat(ts)
