"""SweepEngine: loop order, dirs, resume, retry, events, exit codes."""

import json

import pytest

from kitchen.events import read_all
from kitchen.run import (RateSearchSpec, SweepEngine, SweepSpec,
                         point_rel_dir, run_experiments)


class FakeAdapter:
    """In-memory adapter: delivers min(rate, capacity), records every call.

    `errors_at` maps a rel_dir to how many launches should raise before one
    succeeds (for retry tests). `dead_above` makes high rates commit nothing.
    """
    name = "fake"

    def __init__(self, capacity=8000.0, dead_above=None, errors_at=None):
        self.capacity = capacity
        self.dead_above = dead_above
        self.errors_at = dict(errors_at or {})
        self.deploys = 0
        self.launches = []

    def deploy(self, ctx):
        self.deploys += 1

    def launch(self, ctx, point):
        self.launches.append(point.rel_dir)
        left = self.errors_at.get(point.rel_dir, 0)
        if left > 0:
            self.errors_at[point.rel_dir] = left - 1
            raise RuntimeError("injected launch failure")

    def analyze(self, ctx, point):
        rate = point.rate if point.rate is not None else 1000.0
        dead = self.dead_above is not None and rate >= self.dead_above
        delivered = 0.0 if dead else min(rate, self.capacity)
        return {
            "total_completed": int(delivered),
            "throughput_msgs_per_sec": delivered,
            "offered_rate": float(rate),
            "delivered_rate": delivered,
            "stats_window_secs": 1.0,
        }


def _events(out_root):
    events, _ = read_all(out_root / "events.jsonl")
    return events


def _types(events, type_):
    return [e for e in events if e["type"] == type_]


def test_static_sweep_layout_and_exit_code(tmp_path):
    spec = SweepSpec(name="exp", dims={"payload": (16, 1024)},
                     rates=(1000, 2000), trials=2)
    adapter = FakeAdapter()
    rc = run_experiments(adapter, [spec], tmp_path, argv=["test"])
    assert rc == 0
    assert adapter.deploys == 1
    # Trial outermost: trial 0 visits every point before trial 1 starts.
    assert adapter.launches == [
        "payload_16/rate_1000/trial_0", "payload_16/rate_2000/trial_0",
        "payload_1024/rate_1000/trial_0", "payload_1024/rate_2000/trial_0",
        "payload_16/rate_1000/trial_1", "payload_16/rate_2000/trial_1",
        "payload_1024/rate_1000/trial_1", "payload_1024/rate_2000/trial_1",
    ]
    summary = json.loads(
        (tmp_path / "exp/payload_16/rate_2000/trial_1/summary.json").read_text())
    assert summary["payload"] == 16
    assert summary["rate"] == 2000
    assert summary["trial"] == 1
    results = json.loads((tmp_path / "exp/sweep_results.json").read_text())
    assert len(results) == 8

    events = _events(tmp_path)
    assert _types(events, "run.started")[0]["data"]["experiments"] == ["exp"]
    assert _types(events, "experiment.started")[0]["data"]["est_points"] == 8
    assert len(_types(events, "point.started")) == 8
    finished = _types(events, "point.finished")
    assert all(e["data"]["status"] == "ok" for e in finished)
    run_finished = _types(events, "run.finished")[0]["data"]
    assert run_finished["exit_code"] == 0
    assert run_finished["points_ok"] == 8


def test_dead_points_mean_degraded(tmp_path):
    spec = SweepSpec(name="exp", rates=(1000, 50000))
    rc = run_experiments(FakeAdapter(dead_above=20000), [spec], tmp_path,
                         argv=["test"])
    assert rc == 2
    events = _events(tmp_path)
    statuses = [e["data"]["status"] for e in _types(events, "point.finished")]
    assert statuses == ["ok", "dead"]
    exp = _types(events, "experiment.finished")[0]["data"]
    assert exp["status"] == "degraded"


def test_error_beats_dead_and_retry_recovers(tmp_path):
    # Point at rate 2000 fails once, then succeeds on the retry.
    flaky = point_rel_dir({}, 2000, 0)
    spec = SweepSpec(name="exp", rates=(1000, 2000), max_attempts=2)
    adapter = FakeAdapter(errors_at={flaky: 1})
    rc = run_experiments(adapter, [spec], tmp_path, argv=["test"])
    assert rc == 0
    events = _events(tmp_path)
    retrying = _types(events, "point.retrying")
    assert len(retrying) == 1
    assert retrying[0]["data"]["attempt"] == 1
    statuses = [e["data"]["status"] for e in _types(events, "point.finished")]
    assert statuses == ["ok", "ok"]

    # Without retries the same failure is terminal, and errors outrank dead.
    spec2 = SweepSpec(name="exp2", rates=(2000, 50000))
    adapter2 = FakeAdapter(dead_above=20000,
                           errors_at={point_rel_dir({}, 2000, 0): 5})
    rc2 = run_experiments(adapter2, [spec2], tmp_path / "b", argv=["test"])
    assert rc2 == 1
    entry = json.loads(
        (tmp_path / "b/exp2/sweep_results.json").read_text())[0]
    assert "injected launch failure" in entry["error"]


def test_dead_not_retried_unless_opted_in(tmp_path):
    spec = SweepSpec(name="exp", rates=(50000,), max_attempts=3)
    adapter = FakeAdapter(dead_above=20000)
    run_experiments(adapter, [spec], tmp_path, argv=["test"])
    assert len(adapter.launches) == 1

    spec2 = SweepSpec(name="exp2", rates=(50000,), max_attempts=3,
                      retry_dead=True)
    adapter2 = FakeAdapter(dead_above=20000)
    run_experiments(adapter2, [spec2], tmp_path / "b", argv=["test"])
    assert len(adapter2.launches) == 3


def test_resume_skips_complete_points(tmp_path):
    spec = SweepSpec(name="exp", rates=(1000, 2000))
    run_experiments(FakeAdapter(), [spec], tmp_path, argv=["test"])

    # Second invocation resumes: nothing relaunches.
    resumed = SweepSpec(name="exp", rates=(1000, 2000), resume=True)
    adapter = FakeAdapter()
    rc = run_experiments(adapter, [resumed], tmp_path, argv=["test"])
    assert rc == 0
    assert adapter.launches == []
    events = _events(tmp_path)
    assert len(_types(events, "point.skipped")) == 2

    # A summary missing a required key is not complete: that point re-runs.
    victim = tmp_path / "exp/rate_1000/trial_0/summary.json"
    data = json.loads(victim.read_text())
    del data["offered_rate"]
    victim.write_text(json.dumps(data))
    adapter2 = FakeAdapter()
    partial = SweepSpec(name="exp", rates=(1000, 2000), resume=True,
                        resume_required_keys=("offered_rate",))
    run_experiments(adapter2, [partial], tmp_path, argv=["test"])
    assert adapter2.launches == ["rate_1000/trial_0"]


def test_rate_search_then_replay_on_later_trials(tmp_path):
    spec = SweepSpec(name="exp", search=RateSearchSpec(start=1000),
                     trials=2, duration_secs=1.0)
    adapter = FakeAdapter(capacity=8000)
    rc = run_experiments(adapter, [spec], tmp_path, argv=["test"])
    assert rc == 0
    searched = [1000, 2000, 4000, 8000, 16000, 10000, 12000, 14000]
    # Trial 0 searches; trial 1 replays the recorded rates in order.
    expected = ([point_rel_dir({}, r, 0) for r in searched]
                + [point_rel_dir({}, r, 1) for r in searched])
    assert adapter.launches == expected
    events = _events(tmp_path)
    actions = [e["data"]["action"] for e in _types(events, "search.decision")]
    assert actions == (["start"] + ["climb"] * 4 + ["refine"] * 3
                       + ["replay"] * 8)
    record = json.loads((tmp_path / "exp/searched_rates.json").read_text())
    assert record["points"][0]["complete"] is True
    assert record["points"][0]["rates"] == searched


def test_deploy_failure_fails_experiment_not_process(tmp_path):
    class BrokenDeploy(FakeAdapter):
        def deploy(self, ctx):
            raise RuntimeError("no cluster")

    rc = run_experiments(BrokenDeploy(), [SweepSpec(name="exp", rates=(1000,))],
                         tmp_path, argv=["test"])
    assert rc == 1
    exp = _types(_events(tmp_path), "experiment.finished")[0]["data"]
    assert exp["status"] == "failed"
    assert "no cluster" in exp["detail"]


def test_spec_validation():
    with pytest.raises(ValueError):
        SweepSpec(name="x", rates=(1,), search=RateSearchSpec())
    with pytest.raises(ValueError):
        SweepSpec(name="x", trials=0)
    assert SweepSpec(name="x", dims={"a": (1, 2), "b": (3,)},
                     rates=(1, 2)).est_points() == 4
    assert SweepSpec(name="x", search=RateSearchSpec()).est_points() is None
