"""MCP server: tools over the daemon core, and the cost-confirmation gate."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from mcp import Client

from kitchend.api.mcp import build_mcp, require_confirmed_cost
from kitchend.core import jobs, ledger


class FakeClusters:
    """Stands in for ClusterManager: one costed cluster, records calls."""

    def __init__(self, hourly=None):
        self.hourly = hourly
        self.up_calls = []

    def estimate_hourly(self, key):
        if key != "stub/main":
            raise KeyError(key)
        return self.hourly

    async def up(self, key, ttl_minutes, purpose="user"):
        self.up_calls.append((key, ttl_minutes, purpose))
        return "lease-1"

    def snapshot(self):
        return [{"key": "stub/main", "state": "terminated"}]


def _state(env, hourly=None):
    config, db, hub, runner, scheduler = env
    return SimpleNamespace(config=config, db=db, hub=hub, runner=runner,
                           scheduler=scheduler,
                           clusters=FakeClusters(hourly=hourly))


def _call(mcp, tool, args):
    async def run():
        async with Client(mcp) as c:
            return await c.call_tool(tool, args)
    return asyncio.run(run())


def _data(result):
    assert not result.is_error, result.content[0].text
    return json.loads(result.content[0].text)


def test_require_confirmed_cost():
    require_confirmed_cost(None, None, "free thing")          # free: no gate
    require_confirmed_cost(5.0, 5.0, "x")
    with pytest.raises(ValueError, match="pass confirm_cost_usd"):
        require_confirmed_cost(5.0, None, "x")
    with pytest.raises(ValueError, match="below the estimated"):
        require_confirmed_cost(5.0, 4.99, "x")


def test_list_projects_and_submit_free_job(env):
    mcp = build_mcp(_state(env))
    projects = _data(_call(mcp, "list_projects", {}))["projects"]
    assert projects[0]["name"] == "stub"

    # No queue → no cluster → free: queues without confirmation.
    out = _data(_call(mcp, "submit_job",
                      {"project": "stub", "experiments": ["exit 0"]}))
    job = jobs.get(env[1], out["job_id"])
    assert job["state"] == jobs.QUEUED
    assert out["estimate_usd"] is None


def test_submit_on_costed_cluster_requires_confirmation(env):
    mcp = build_mcp(_state(env, hourly=2.0))
    args = {"project": "stub", "experiments": ["exit 0"],
            "queue": "stub/main"}

    r = _call(mcp, "submit_job", args)
    assert r.is_error and "est_hours" in r.content[0].text

    r = _call(mcp, "submit_job", {**args, "est_hours": 3})
    assert r.is_error and "confirm_cost_usd >= 6.00" in r.content[0].text

    r = _call(mcp, "submit_job",
              {**args, "est_hours": 3, "confirm_cost_usd": 5.0})
    assert r.is_error and "below the estimated" in r.content[0].text

    out = _data(_call(mcp, "submit_job",
                      {**args, "est_hours": 3, "confirm_cost_usd": 6.0}))
    assert out["estimate_usd"] == 6.0
    assert jobs.get(env[1], out["job_id"])["state"] == jobs.QUEUED


def test_cluster_up_gate_and_lease(env):
    state = _state(env, hourly=4.0)
    mcp = build_mcp(state)

    r = _call(mcp, "cluster_up",
              {"project": "stub", "cluster": "main", "ttl_minutes": 90})
    assert r.is_error and "confirm_cost_usd >= 6.00" in r.content[0].text
    assert state.clusters.up_calls == []

    out = _data(_call(mcp, "cluster_up",
                      {"project": "stub", "cluster": "main",
                       "ttl_minutes": 90, "confirm_cost_usd": 6.0}))
    assert out["lease_id"] == "lease-1"
    assert state.clusters.up_calls == [("stub/main", 90, "agent")]


def test_run_ledger_tools(env, tmp_path):
    config, db, hub, runner, scheduler = env
    mcp = build_mcp(_state(env))
    project_id = jobs.ensure_project_row(db, config.project("stub"))
    job_id = jobs.submit(db, project_id, {"project": "stub",
                                          "experiments": ["x"]})
    ledger.apply_events(db, project_id, job_id, str(tmp_path), [
        {"type": "experiment.started", "ts": "t", "data": {"name": "exp"}},
        {"type": "point.finished", "ts": "t",
         "data": {"experiment": "exp", "dims": {}, "rate": 1000, "trial": 0,
                  "rel_dir": "rate_1000/trial_0", "status": "ok",
                  "metrics": {"throughput_msgs_per_sec": 900}}},
        {"type": "experiment.finished", "ts": "t",
         "data": {"name": "exp", "status": "ok"}},
    ])
    runs = _data(_call(mcp, "list_runs", {}))["runs"]
    assert runs[0]["experiment"] == "exp"
    _data(_call(mcp, "add_run_note",
                {"run_id": runs[0]["id"], "text": "looked healthy"}))
    _data(_call(mcp, "tag_run", {"run_id": runs[0]["id"], "tag": "keeper"}))
    run = _data(_call(mcp, "get_run", {"run_id": runs[0]["id"]}))
    assert run["notes"][0]["text"] == "looked healthy"
    assert run["tags"] == ["keeper"]
    assert run["points"][0]["metrics"]["throughput_msgs_per_sec"] == 900

    r = _call(mcp, "get_run", {"run_id": 999})
    assert r.is_error
