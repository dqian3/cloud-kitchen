import subprocess

import pytest

from kitchen.cluster.lifecycle import start_vms, stop_vms
from kitchen.remote.gcloud import GCloudRemote
from kitchen.remote.settings import RemoteSettings


class FakeGcp:
    """Small stateful stand-in for the gcloud CLI used by GCloudRemote."""

    def __init__(self, states, *, fail_deadman=(), fail_stop=(), zones=None):
        self.states = dict(states)
        self.fail_deadman = set(fail_deadman)
        self.fail_stop = set(fail_stop)
        # Where each VM lives. A start or stop aimed at any other zone is a
        # 404, exactly as gcloud answers one.
        self.zones = dict(zones or {v: "us-test1-a" for v in self.states})
        self.calls = []

    def recreate(self, vm, zone):
        """The stockout move: delete a VM and build it again elsewhere."""
        self.zones[vm] = zone
        self.states[vm] = "RUNNING"

    def _zone_of(self, cmd):
        arg = next((x for x in cmd if x.startswith("--zone=")), None)
        return arg.split("=", 1)[1] if arg else None

    def _filtered(self, cmd):
        """The names the --filter asks about that actually exist."""
        arg = next(x for x in cmd if x.startswith("--filter="))
        asked = [t.split("=", 1)[1]
                 for t in arg.split("=", 1)[1].split(" OR ")]
        return [n for n in asked if n in self.states]

    def __call__(self, cmd, **kwargs):
        self.calls.append(list(cmd))
        stdout = stderr = ""
        rc = 0

        if cmd[1:4] == ["compute", "instances", "list"]:
            fmt = next(x for x in cmd if x.startswith("--format="))
            wanted = self._filtered(cmd)
            if fmt == "--format=value(name,zone)":
                stdout = "".join(
                    f"{n} {self.zones[n]}\n" for n in wanted)
            elif fmt == "--format=value(name,zone,status)":
                stdout = "".join(
                    f"{n} {self.zones[n]} {self.states[n]}\n" for n in wanted)
            elif fmt.startswith("--format=value(name,zone,network"):
                stdout = "".join(
                    f"{n} {self.zones[n]} 10.0.0.{i}\n"
                    for i, n in enumerate(wanted))
            else:
                raise AssertionError(f"unhandled listing format: {fmt}")
        elif cmd[1:4] == ["compute", "instances", "start"]:
            vm = cmd[4]
            if self._zone_of(cmd) != self.zones.get(vm):
                rc, stderr = 1, f"HTTPError 404: {vm} not found"
            else:
                self.states[vm] = "RUNNING"
        elif cmd[1:4] == ["compute", "instances", "stop"]:
            vm = cmd[4]
            if self._zone_of(cmd) != self.zones.get(vm):
                rc, stderr = 1, f"HTTPError 404: {vm} not found"
            elif vm in self.fail_stop:
                rc, stderr = 1, "mock stop failure"
            else:
                self.states[vm] = "TERMINATED"
        elif cmd[1:3] == ["compute", "ssh"]:
            vm = cmd[3]
            if vm in self.fail_deadman and "shutdown -h" in cmd[-1]:
                rc, stderr = 1, "mock sudo failure"

        result = subprocess.CompletedProcess(cmd, rc, stdout, stderr)  # noqa: E501
        if kwargs.get("check") and rc:
            raise subprocess.CalledProcessError(
                rc, cmd, output=stdout, stderr=stderr)
        return result


def remote(fake, monkeypatch):
    monkeypatch.setattr("kitchen.remote.gcloud.subprocess.run", fake)
    settings = RemoteSettings(ssh_attempts=1)
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


def test_start_addresses_a_vm_recreated_in_another_zone(monkeypatch):
    """A zone read once must not be reused after the VM is rebuilt.

    us-central1 ran out of n4-standard-16, the provisioner rebuilt two VMs in
    a sibling zone, and the daemon -- holding the zone it had read at
    startup -- sent every start, stop and arm to where they used to be and
    collected 404s for half an hour.
    """
    fake = FakeGcp({"vm-a": "TERMINATED"})
    gcp = remote(fake, monkeypatch)

    gcp.vm_status(["vm-a"])                     # learns us-test1-a
    fake.recreate("vm-a", "us-test1-f")
    fake.states["vm-a"] = "TERMINATED"

    start_vms(gcp, ["vm-a"])

    assert fake.states["vm-a"] == "RUNNING"
    started = [c for c in fake.calls
               if c[1:4] == ["compute", "instances", "start"]]
    assert started and all("--zone=us-test1-f" in c for c in started)


def test_stop_rereads_the_zone_it_was_given_earlier(monkeypatch):
    """vm_stop is reached without a status poll in front of it (a teardown
    calls it straight), so its own lookup has to be the fresh one."""
    fake = FakeGcp({"vm-a": "RUNNING"}, zones={"vm-a": "us-test1-f"})
    gcp = remote(fake, monkeypatch)
    gcp._zone_cache["vm-a"] = "us-test1-a"      # what an earlier read left

    assert stop_vms(gcp, ["vm-a"]) == []
    assert fake.states["vm-a"] == "TERMINATED"
    stopped = [c for c in fake.calls
               if c[1:4] == ["compute", "instances", "stop"]]
    assert stopped and all("--zone=us-test1-f" in c for c in stopped)


def test_arming_rereads_the_zone_it_was_given_earlier(monkeypatch):
    """The dead-man switch is an ssh per VM, and ssh is zone-addressed too."""
    fake = FakeGcp({"vm-a": "RUNNING"}, zones={"vm-a": "us-test1-f"})
    gcp = remote(fake, monkeypatch)
    gcp._zone_cache["vm-a"] = "us-test1-a"

    gcp.run_on_all(["vm-a"], "true", quiet=True)

    sshed = [c for c in fake.calls if c[1:3] == ["compute", "ssh"]]
    assert sshed and all("--zone=us-test1-f" in c for c in sshed)


def test_a_deleted_vm_does_not_keep_its_zone(monkeypatch):
    """Otherwise the next VM built under that name is addressed where the
    last one stood."""
    fake = FakeGcp({"vm-a": "RUNNING", "vm-b": "RUNNING"})
    gcp = remote(fake, monkeypatch)

    gcp.vm_status(["vm-a", "vm-b"])
    del fake.states["vm-a"], fake.zones["vm-a"]

    assert gcp.vm_status(["vm-a", "vm-b"]) == {"vm-b": "RUNNING"}
    assert "vm-a" not in gcp._zone_cache


def test_a_moved_vm_does_not_keep_its_address(monkeypatch):
    """A rebuilt VM gets a new internal IP as well as a new zone, so noticing
    the move has to drop the address cached for the VM that used to be
    there."""
    fake = FakeGcp({"vm-a": "RUNNING"})
    gcp = remote(fake, monkeypatch)
    gcp.get_all_ips(["vm-a"])
    assert gcp._ip_cache["vm-a"]

    fake.recreate("vm-a", "us-test1-f")
    gcp.vm_status(["vm-a"])

    assert "vm-a" not in gcp._ip_cache


def test_a_deleted_vm_does_not_keep_its_address(monkeypatch):
    """The other way a rebuild is caught: any listing taken while the VM is
    gone drops it, so a rebuild in the same zone is covered too."""
    fake = FakeGcp({"vm-a": "RUNNING"})
    gcp = remote(fake, monkeypatch)
    gcp.get_all_ips(["vm-a"])

    del fake.states["vm-a"], fake.zones["vm-a"]
    gcp.vm_status(["vm-a"])

    assert "vm-a" not in gcp._ip_cache
    assert "vm-a" not in gcp._zone_cache


STOCKOUT = """Starting instance(s) wan-replica00...
......failed.
ERROR: (gcloud.compute.instances.start) ---
code: ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS
errorDetails:
- help:
    links:
    - description: Troubleshooting documentation
      url: https://cloud.google.com/compute/docs/resource-error
- localizedMessage:
    locale: en-US
    message: A n4-standard-16 VM instance is currently unavailable in the us-central1-f
      zone. Try requesting the VM in another zone.
- errorInfo:
    domain: compute.googleapis.com
    reason: stockout
message: The zone 'projects/p/zones/us-central1-f' does not have enough resources
  available to fulfill the request.
"""


def test_a_stockout_reason_survives_into_the_error(monkeypatch):
    """The one cause an operator can act on -- by moving the VM -- used to be
    printed to the daemon's stdout and dropped from the raised error, so the
    event log said only which VM would not start."""
    fake = FakeGcp({"vm-a": "TERMINATED"})
    gcp = remote(fake, monkeypatch)

    why = gcp._why_start_failed(STOCKOUT)

    assert "ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS" in why
    assert "us-central1-f" in why
    assert "n4-standard-16" in why
    assert "\n" not in why and "url:" not in why


def test_start_failure_names_the_reason_not_just_the_vm(monkeypatch):
    fake = FakeGcp({"vm-a": "TERMINATED"})
    gcp = remote(fake, monkeypatch)

    def stockout(cmd, **kw):
        if cmd[1:4] == ["compute", "instances", "start"]:
            return subprocess.CompletedProcess(cmd, 1, "", STOCKOUT)
        return fake(cmd, **kw)

    monkeypatch.setattr("kitchen.remote.gcloud.subprocess.run", stockout)
    with pytest.raises(RuntimeError, match="ZONE_RESOURCE_POOL_EXHAUSTED"):
        gcp.vm_start(["vm-a"])
