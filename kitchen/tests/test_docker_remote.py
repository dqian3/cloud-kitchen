"""DockerRemote against a real two-container compose fleet (skipped without
docker)."""

import os
import shutil
import subprocess

import pytest

from kitchen.cluster import start_vms, stop_vms
from kitchen.remote import DockerRemote, load_remote

COMPOSE = os.path.join(os.path.dirname(__file__), "fixtures", "docker-compose.yml")
PROJECT = "kitchen-test-fleet"


def _docker_ok():
    if not shutil.which("docker"):
        return False
    return subprocess.run(["docker", "compose", "version"],
                          capture_output=True).returncode == 0


pytestmark = pytest.mark.skipif(not _docker_ok(), reason="docker compose unavailable")


@pytest.fixture
def fleet():
    r = DockerRemote(COMPOSE, project=PROJECT)
    yield r
    subprocess.run(["docker", "compose", "-f", COMPOSE, "-p", PROJECT,
                    "down", "--remove-orphans"], capture_output=True)


def test_fleet_lifecycle_and_io(fleet, tmp_path):
    r = fleet
    assert r.vm_status(["a", "b"]) == {}          # nothing created yet
    # start_vms drives vm_start and skips the dead-man (no shutdown here).
    started = start_vms(r, ["a", "b"], drain_first=False)
    assert started == ["a", "b"]
    assert r.vm_status(["a", "b", "ghost"]) == {"a": "RUNNING", "b": "RUNNING"}
    r.check_vms_running(["a", "b"])

    # Distinct addresses per node, unlike processes on 127.0.0.1.
    ips = r.get_all_ips(["a", "b"])
    assert ips["a"] != ips["b"] and ips["a"].count(".") == 3

    # ssh: ~ resolves inside the container; blocking and background forms.
    out = r.ssh("a", "echo $HOME; hostname").stdout.split()
    assert out[0] == "/root"
    p = r.ssh("b", "sleep 30", bg=True)
    assert p.poll() is None
    p.terminate()

    # File transfer both ways through ~.
    src = tmp_path / "up.txt"; src.write_text("hello")
    r.scp_upload(str(src), "a", "~/in/up.txt")
    assert r.ssh("a", "cat ~/in/up.txt").stdout == "hello"
    r.ssh("a", "echo back > ~/out.txt")
    r.scp_download("a", "~/out.txt", str(tmp_path / "down.txt"))
    assert (tmp_path / "down.txt").read_text().strip() == "back"
    with pytest.raises(RuntimeError, match="failed"):
        r.scp_download("a", "~/nope.txt", str(tmp_path / "x"))

    # kill_process (base class, via exec + pkill) and stop.
    r.ssh("b", "sleep 1000 >/dev/null 2>&1 &")
    r.kill_process(["b"], "sleep 1000")
    stop_vms(r, ["a", "b"])
    assert r.vm_status(["a", "b"]) == {"a": "TERMINATED", "b": "TERMINATED"}


def test_load_remote_docker():
    r = load_remote({"platform": "docker", "compose_file": COMPOSE,
                     "compose_project": PROJECT})
    assert isinstance(r, DockerRemote) and r.supports_deadman is False
    with pytest.raises(ValueError, match="compose_file"):
        load_remote({"platform": "docker"})
