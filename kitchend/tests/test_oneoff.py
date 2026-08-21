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
