"""Event envelope and type registry.

Envelope:
    {"v": 1, "ts": "<ISO-8601 UTC>", "run_id": "...", "seq": N,
     "type": "<one of EVENT_TYPES>", "data": {...}}

Types and their expected data keys (informative, not enforced — consumers
must tolerate extras and absences):

  run.started         argv, adapter, experiments, out_root
  run.phase           phase (deploy|upload|sweep|analyze), experiment
  run.finished        exit_code, points_total, points_ok, points_dead, points_failed
  run.interrupted     signal, points_completed
  experiment.started  name, queue, est_points
  experiment.finished name, status (ok|degraded|failed), detail
  point.started       experiment, dims, rate, trial, rel_dir
  point.finished      experiment, dims, rate, trial, rel_dir, status
                      (ok|dead|error), duration_s, metrics
  point.retrying      attempt, max_attempts, reason
  point.skipped       reason (resume)
  search.decision     dims, rate, action (climb|halve|refine|abandon|replay), note
  cluster.state       cluster, state, vms, reason
  cluster.lease       cluster, action (acquired|released|expired|broken), holder
  cluster.keepalive   cluster, armed_until, interval_s
"""

import datetime

SCHEMA_VERSION = 1

EVENT_TYPES = frozenset({
    "run.started", "run.phase", "run.finished", "run.interrupted",
    "experiment.started", "experiment.finished",
    "point.started", "point.finished", "point.retrying", "point.skipped",
    "search.decision",
    "cluster.state", "cluster.lease", "cluster.keepalive",
})


def make_event(type: str, run_id: str, seq: int, data: dict) -> dict:
    if type not in EVENT_TYPES:
        raise ValueError(f"unknown event type: {type}")
    return {
        "v": SCHEMA_VERSION,
        "ts": datetime.datetime.now(datetime.timezone.utc)
              .isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "run_id": run_id,
        "seq": seq,
        "type": type,
        "data": data,
    }
