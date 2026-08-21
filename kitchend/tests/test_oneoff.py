"""One-off sweep submission: adapter oneoff() resolution."""

import sys

import pytest

from kitchend.config import ProjectConfig
from kitchend.core import adapters

TOY_ADAPTER = ("/home/dan/Projects/cloud-kitchen/kitchen/src/kitchen/run/"
               "kitchen_adapter.py")


@pytest.fixture
def toy_project(tmp_path):
    adapters.clear_cache()
    from pathlib import Path
    return ProjectConfig(name="toy", repo_path=tmp_path,
                         adapter_path=Path(TOY_ADAPTER))


def test_oneoff_builds_argv_and_queue(toy_project):
    argv, queue = adapters.oneoff_command(toy_project, {
        "base": "toy-search",
        "dims": {"payload": [16, 1024]},
        "rate_search": {"start": 500, "refine_steps": 2},
        "trials": 2,
        "duration_secs": 0.5,
    })
    assert argv[:3] == [sys.executable, "-m", "kitchen.run.toy"]
    assert "--rate-search" in argv
    i = argv.index("--dim")
    assert argv[i + 1] == "payload=16,1024"
    assert argv[argv.index("--rate-search-start") + 1] == "500"
    assert argv[argv.index("--trials") + 1] == "2"
    assert queue == "local"      # inherited from the base experiment


def test_oneoff_without_base(toy_project):
    argv, queue = adapters.oneoff_command(toy_project, {
        "rates": [1000, 2000], "queue": "elsewhere"})
    assert argv[argv.index("--rates"):][:3] == ["--rates", "1000", "2000"]
    assert queue == "elsewhere"


def test_oneoff_queue_from_cluster(toy_project):
    # A sweep that names its cluster lands on that cluster's queue, so it
    # serializes with the cluster's other jobs and gets cost-gated over MCP.
    _, queue = adapters.oneoff_command(
        toy_project, {"rates": [1000], "cluster": "main"})
    assert queue == "main"
    # Explicit queue still wins over cluster.
    _, queue = adapters.oneoff_command(
        toy_project, {"rates": [1000], "cluster": "main", "queue": "q"})
    assert queue == "q"


def test_oneoff_rejects_unknown_base(toy_project):
    with pytest.raises(ValueError, match="unknown base"):
        adapters.oneoff_command(toy_project, {"base": "nope"})


def test_prepare_spec_cluster_handling(toy_project, tmp_path):
    from dataclasses import replace

    from kitchend.config import ClusterConfig
    from kitchend.core import submission

    proj = replace(toy_project, clusters=(
        ClusterConfig(name="local", config="x.yaml"),), driver=("bash", "-c"))

    # A sweep naming a configured cluster inherits the managed lease and
    # the cluster's queue.
    (spec,) = submission.prepare_specs(
        proj, {"project": "toy", "sweep": {"rates": [1000], "cluster": "local"}})
    assert spec["cluster"] == "local"
    assert spec["queue"] == "toy/local"

    # A cluster name the daemon doesn't manage only routes the queue.
    (spec2,) = submission.prepare_specs(
        proj, {"project": "toy", "sweep": {"rates": [1000], "cluster": "alias"}})
    assert "cluster" not in spec2
    assert spec2["queue"] == "toy/alias"

    # An explicit unknown cluster is a submit-time error.
    with pytest.raises(ValueError, match="no cluster 'nope'"):
        submission.prepare_specs(proj, {"project": "toy",
                                        "experiments": ["toy-static"],
                                        "cluster": "nope"})


NATIVE_ADAPTER_SRC = '''
from kitchen.adapter import ExperimentInfo

class A:
    name = "nat"
    def experiments(self):
        return [
            ExperimentInfo(name="one", queue="main",
                           command=("python3", "native.py", "--name", "one")),
            ExperimentInfo(name="two", queue="main",
                           command=("python3", "native.py", "--name", "two")),
            ExperimentInfo(name="legacy", queue="main", args=("legacy",)),
        ]
    def aggregates(self):
        return {"all": ["one", "two", "legacy"]}

def get_adapter():
    return A()
'''


def test_native_catalog_submission_fans_out_and_leases(tmp_path):
    from kitchend.config import ClusterConfig, ProjectConfig
    from kitchend.core import adapters as _adapters, submission

    _adapters.clear_cache()
    ap = tmp_path / "kitchen_adapter.py"
    ap.write_text(NATIVE_ADAPTER_SRC)
    # The submit-time check requires the scripts to exist in the driver cwd.
    (tmp_path / "native.py").touch()
    (tmp_path / "run_experiment.py").touch()
    proj = ProjectConfig(name="nat", repo_path=tmp_path, adapter_path=ap,
                         driver=("python3", "run_experiment.py"),
                         clusters=(ClusterConfig(name="main", config="m.yaml"),))

    specs = submission.prepare_specs(
        proj, {"project": "nat", "experiments": ["all"], "after": 7})
    by_exps = {tuple(s["experiments"]): s for s in specs}
    assert set(by_exps) == {("one",), ("two",), ("legacy",)}
    # Native experiments: their own command, the cluster queue, and the
    # managed lease (they assume VMs are up). All inherit the chain gate.
    for name in ("one", "two"):
        s = by_exps[(name,)]
        assert s["command"][-2:] == ["--name", name]
        assert s["queue"] == "nat/main"
        assert s["cluster"] == "main"
        assert s["after"] == 7
    # The classic experiment keeps the driver and manages its own VMs.
    legacy = by_exps[("legacy",)]
    assert "command" not in legacy and "cluster" not in legacy
    assert legacy["driver_args"] == ["legacy"]
    assert legacy["queue"] == "nat/main"


def test_missing_native_script_rejected_at_submit(tmp_path):
    # A native command whose script isn't in the driver checkout must fail at
    # submit — not bring a leased cluster up just to die on "can't open file".
    from kitchend.config import ClusterConfig, ProjectConfig
    from kitchend.core import adapters as _adapters, submission

    _adapters.clear_cache()
    ap = tmp_path / "kitchen_adapter.py"
    ap.write_text(NATIVE_ADAPTER_SRC)
    (tmp_path / "run_experiment.py").touch()   # but no native.py
    proj = ProjectConfig(name="nat", repo_path=tmp_path, adapter_path=ap,
                         driver=("python3", "run_experiment.py"),
                         clusters=(ClusterConfig(name="main", config="m.yaml"),))
    with pytest.raises(ValueError, match="native.py not found"):
        submission.prepare_specs(proj, {"project": "nat",
                                        "experiments": ["one"]})


def test_oneoff_requires_hook(tmp_path):
    adapters.clear_cache()
    ap = tmp_path / "kitchen_adapter.py"
    ap.write_text(
        "class A:\n"
        "    name = 'bare'\n"
        "    def experiments(self): return []\n"
        "    def aggregates(self): return {}\n"
        "def get_adapter(): return A()\n")
    p = ProjectConfig(name="bare", repo_path=tmp_path, adapter_path=ap)
    with pytest.raises(ValueError, match="oneoff"):
        adapters.oneoff_command(p, {"rates": [1]})
