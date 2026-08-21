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
        ]
    def aggregates(self):
        return {"pair": ["alpha", "beta"]}

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
        ["alpha", "beta", "gamma", "preset"]
    assert cat["aggregates"] == {"pair": ["alpha", "beta"]}
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
    names, queue, args = adapters.resolve_submission(project, ["pair"])
    assert names == ["alpha", "beta"]
    assert queue == "main"
    assert args == ["alpha", "beta"]


def test_resolve_rejects_unknown(project):
    with pytest.raises(ValueError, match="unknown experiment 'nope'"):
        adapters.resolve_submission(project, ["nope"])


def test_resolve_rejects_cross_cluster(project):
    with pytest.raises(ValueError, match="multiple clusters"):
        adapters.resolve_submission(project, ["alpha", "gamma"])


def test_preset_must_be_alone(project):
    names, queue, args = adapters.resolve_submission(project, ["preset"])
    assert args == ["--config", "x.yaml", "--n", "3"]
    with pytest.raises(ValueError, match="own arguments"):
        adapters.resolve_submission(project, ["alpha", "preset"])


def test_missing_adapter_passthrough(tmp_path):
    p = ProjectConfig(name="raw", repo_path=tmp_path)
    names, queue, args = adapters.resolve_submission(p, ["anything"])
    assert (names, queue, args) == (["anything"], None, ["anything"])


def test_broken_adapter_reports_error(tmp_path):
    adapters.clear_cache()
    ap = tmp_path / "kitchen_adapter.py"
    ap.write_text("raise RuntimeError('boom')")
    p = ProjectConfig(name="broken", repo_path=tmp_path, adapter_path=ap)
    cat = adapters.catalog(p)
    assert "boom" in cat["error"]
