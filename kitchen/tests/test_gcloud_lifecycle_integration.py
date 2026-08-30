import subprocess

import pytest

from kitchen.cluster.lifecycle import start_vms, stop_vms
from kitchen.remote.gcloud import GCloudRemote
from kitchen.remote.settings import RemoteSettings


class FakeGcp:
    """Small stateful stand-in for the gcloud CLI used by GCloudRemote."""

    def __init__(self, states, *, fail_deadman=(), fail_stop=()):
        self.states = dict(states)
        self.fail_deadman = set(fail_deadman)
        self.fail_stop = set(fail_stop)
        self.calls = []

    def __call__(self, cmd, **kwargs):
        self.calls.append(list(cmd))
        stdout = stderr = ""
        rc = 0

        if cmd[1:4] == ["compute", "instances", "list"]:
            fmt = next(x for x in cmd if x.startswith("--format="))
            if "name,zone" in fmt:
                stdout = "".join(f"{name} us-test1-a\n" for name in self.states)
            elif "name,status" in fmt:
                stdout = "".join(
                    f"{name} {state}\n" for name, state in self.states.items())
        elif cmd[1:4] == ["compute", "instances", "start"]:
            self.states[cmd[4]] = "RUNNING"
        elif cmd[1:4] == ["compute", "instances", "stop"]:
            vm = cmd[4]
            if vm in self.fail_stop:
                rc, stderr = 1, "mock stop failure"
            else:
                self.states[vm] = "TERMINATED"
        elif cmd[1:3] == ["compute", "ssh"]:
            vm = cmd[3]
            if vm in self.fail_deadman and "shutdown -h" in cmd[-1]:
                rc, stderr = 1, "mock sudo failure"

        result = subprocess.CompletedProcess(cmd, rc, stdout, stderr)
        if kwargs.get("check") and rc:
            raise subprocess.CalledProcessError(
                rc, cmd, output=stdout, stderr=stderr)
        return result


def remote(fake, monkeypatch):
    monkeypatch.setattr("kitchen.remote.gcloud.subprocess.run", fake)
    settings = RemoteSettings(vm_start_attempts=1, ssh_attempts=1)
    return GCloudRemote(project="mock-project", settings=settings)


def test_real_gcloud_backend_starts_and_arms_every_vm(monkeypatch):
    fake = FakeGcp({"vm-a": "TERMINATED", "vm-b": "TERMINATED"})
    gcp = remote(fake, monkeypatch)

    assert sorted(start_vms(gcp, ["vm-a", "vm-b"])) == ["vm-a", "vm-b"]
    assert fake.states == {"vm-a": "RUNNING", "vm-b": "RUNNING"}
    armed = [c for c in fake.calls if c[1:3] == ["compute", "ssh"]]
    assert {c[3] for c in armed} == {"vm-a", "vm-b"}


def test_real_gcloud_backend_reports_partial_stop(monkeypatch):
    fake = FakeGcp({"vm-a": "RUNNING", "vm-b": "RUNNING"},
                   fail_stop={"vm-b"})
    gcp = remote(fake, monkeypatch)

    assert stop_vms(gcp, ["vm-a", "vm-b"]) == ["vm-b"]
    assert fake.states["vm-a"] == "TERMINATED"
    assert fake.states["vm-b"] == "RUNNING"


def test_real_gcloud_backend_cleans_up_unprotected_start(monkeypatch):
    fake = FakeGcp({"vm-a": "TERMINATED"}, fail_deadman={"vm-a"})
    gcp = remote(fake, monkeypatch)

    with pytest.raises(RuntimeError, match="shutdown timer"):
        start_vms(gcp, ["vm-a"])

    assert fake.states["vm-a"] == "TERMINATED"
