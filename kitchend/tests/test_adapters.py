from pathlib import Path

import pytest

from kitchend.config import ProjectConfig
from kitchend.core import adapters


ADAPTER_SRC = '''
from kitchen.adapter import (DimDisplay, DisplayInfo, ExperimentInfo,
                             MetricDisplay)

class A:
    name = "toy"
    def display(self):
        return DisplayInfo(
            dims=(DimDisplay(name="payload", unit="B", example="16,1024"),),
            metrics=(MetricDisplay(name="tput", label="delivered/s"),))
    def experiments(self):
        return [
            ExperimentInfo(name="alpha", description="a", queue="main",
                           args=("alpha",)),
            ExperimentInfo(name="beta", description="b", queue="main",
                           args=("beta",)),
            ExperimentInfo(name="gamma", description="g", queue="other",
                           args=("gamma",)),
            ExperimentInfo(name="preset", description="p", queue="main",
                           args=("--config", "x.yaml", "--n", "3")),
            ExperimentInfo(name="nat", description="native", queue="main",
                           command=("python3", "native.py", "--name", "nat")),
            ExperimentInfo(name="alpha_n4", description="variant", queue="main",
                           args=("alpha_n4",), group="alpha"),
        ]
    def aggregates(self):
        return {"pair": ["alpha", "beta"], "mixed": ["alpha", "nat", "beta"]}

def get_adapter():
    return A()
'''


@pytest.fixture
def project(tmp_path):
    adapters.clear_cache()
    ap = tmp_path / "kitchen_adapter.py"
    ap.write_text(ADAPTER_SRC)
    return ProjectConfig(name="toy", repo_path=tmp_path, adapter_path=ap,
                         driver=("python3", "run.py"))


def test_catalog(project):
    cat = adapters.catalog(project)
    assert cat["error"] is None
    assert [e["name"] for e in cat["experiments"]] == \
        ["alpha", "beta", "gamma", "preset", "nat", "alpha_n4"]
    by_name = {e["name"]: e for e in cat["experiments"]}
    # Display grouping passes through; ungrouped experiments carry "".
    assert by_name["alpha_n4"]["group"] == "alpha"
    assert by_name["alpha"]["group"] == ""
    assert cat["aggregates"] == {"pair": ["alpha", "beta"],
                                 "mixed": ["alpha", "nat", "beta"]}
    # display() passes through; empty labels fall back to the name.
    assert cat["display"]["dims"] == [
        {"name": "payload", "label": "payload", "unit": "B",
         "description": "", "example": "16,1024"}]
    assert cat["display"]["metrics"] == [
        {"name": "tput", "label": "delivered/s", "unit": ""}]


def test_catalog_without_display_hook(tmp_path):
    adapters.clear_cache()
    ap = tmp_path / "kitchen_adapter.py"
    ap.write_text(
        "class A:\n"
        "    name = 'bare'\n"
        "    def experiments(self): return []\n"
        "    def aggregates(self): return {}\n"
        "def get_adapter(): return A()\n")
    p = ProjectConfig(name="bare", repo_path=tmp_path, adapter_path=ap)
    assert adapters.catalog(p)["display"] is None


def test_resolve_aggregate_and_queue(project):
    (plan,) = adapters.resolve_jobs(project, ["pair"])
    assert plan["experiments"] == ["alpha", "beta"]
    assert plan["queue"] == "main"
    assert plan["driver_args"] == ["alpha", "beta"]


def test_resolve_rejects_unknown(project):
    with pytest.raises(ValueError, match="unknown experiment 'nope'"):
        adapters.resolve_jobs(project, ["nope"])


def test_resolve_rejects_cross_cluster(project):
    with pytest.raises(ValueError, match="multiple clusters"):
        adapters.resolve_jobs(project, ["alpha", "gamma"])


def test_preset_must_be_alone(project):
    (plan,) = adapters.resolve_jobs(project, ["preset"])
    assert plan["driver_args"] == ["--config", "x.yaml", "--n", "3"]
    with pytest.raises(ValueError, match="own arguments"):
        adapters.resolve_jobs(project, ["alpha", "preset"])


def test_native_experiment_is_its_own_job(project):
    (plan,) = adapters.resolve_jobs(project, ["nat"])
    assert plan["command"] == ["python3", "native.py", "--name", "nat"]
    assert plan["queue"] == "main"
    assert "driver_args" not in plan


def test_mixed_aggregate_fans_out(project):
    # A native experiment inside an aggregate becomes its own job; the
    # classic members still share one driver invocation.
    plans = adapters.resolve_jobs(project, ["mixed"])
    by_exps = {tuple(p["experiments"]): p for p in plans}
    assert set(by_exps) == {("nat",), ("alpha", "beta")}
    assert by_exps[("nat",)]["command"]
    assert by_exps[("alpha", "beta")]["driver_args"] == ["alpha", "beta"]


def test_missing_adapter_passthrough(tmp_path):
    p = ProjectConfig(name="raw", repo_path=tmp_path)
    (plan,) = adapters.resolve_jobs(p, ["anything"])
    assert plan == {"experiments": ["anything"], "queue": None,
                    "driver_args": ["anything"]}


def test_broken_adapter_reports_error(tmp_path):
    adapters.clear_cache()
    ap = tmp_path / "kitchen_adapter.py"
    ap.write_text("raise RuntimeError('boom')")
    p = ProjectConfig(name="broken", repo_path=tmp_path, adapter_path=ap)
    cat = adapters.catalog(p)
    assert "boom" in cat["error"]
