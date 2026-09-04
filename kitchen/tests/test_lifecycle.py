import pytest

from kitchen.cluster.keepalive import KeepAlive
from kitchen.cluster.lifecycle import arm_shutdown, start_vms, stop_vms
from kitchen.remote.mock import MockRemote


def test_arm_shutdown_reports_unprotected_vm():
    remote = MockRemote()
    remote.script("shutdown -h", host="vm-a", raises=RuntimeError("denied"))

    with pytest.raises(RuntimeError, match="vm-a"):
        arm_shutdown(remote, ["vm-a"])


def test_start_stops_vm_when_deadman_cannot_be_armed():
    remote = MockRemote()
    remote.vm_states["vm-a"] = "TERMINATED"
    remote.script("shutdown -h", host="vm-a", raises=RuntimeError("denied"))

    with pytest.raises(RuntimeError, match="shutdown timer"):
        start_vms(remote, ["vm-a"])

    assert remote.vm_states["vm-a"] == "TERMINATED"


def test_arm_error_says_why_not_just_which_vms():
    """A VM still booting and one with a broken key used to read identically:
    the failure named the hosts and dropped the exception."""
    remote = MockRemote()
    remote.script("shutdown -h", host="vm-a",
                  raises=RuntimeError("Permission denied (publickey)"))

    with pytest.raises(RuntimeError, match="publickey"):
        arm_shutdown(remote, ["vm-a"])


def test_stop_vms_returns_survivors():
    class FailedStop(MockRemote):
        def vm_stop(self, vm_names):
            return list(vm_names)

    assert stop_vms(FailedStop(), ["vm-a"]) == ["vm-a"]


def test_rearm_skips_vms_the_driver_released():
    """A sweep that stops VMs it no longer needs must not fail the heartbeat.

    Arming the whole leased set raised on every released VM, and the daemon
    stops a cluster whose heartbeat failed -- so releasing VMs mid-run killed
    the run that released them.
    """
    remote = MockRemote()
    remote.vm_states["vm-a"] = "RUNNING"
    remote.vm_states["vm-b"] = "TERMINATED"
    remote.script("shutdown -h", host="vm-b", raises=RuntimeError("no route"))

    ka = KeepAlive(remote, ["vm-a", "vm-b"], state=None)

    assert ka.rearm() == ["vm-a"]


def test_rearm_still_raises_when_a_running_vm_cannot_be_armed():
    """Skipping stopped VMs must not swallow a genuine arming failure."""
    remote = MockRemote()
    remote.vm_states["vm-a"] = "RUNNING"
    remote.script("shutdown -h", host="vm-a", raises=RuntimeError("denied"))

    with pytest.raises(RuntimeError, match="vm-a"):
        KeepAlive(remote, ["vm-a"], state=None).rearm()


def test_a_proxycommand_hop_failure_is_retried():
    """`stdio forwarding failed` is the jump hop giving up, not the VM.

    A jumped fleet reaches every VM through one forward, so this is what it
    fails with under contention. It was the only connection-layer error not
    in the retry list, and one occurrence on one VM failed a 102-VM bring-up.
    """
    from kitchen.remote.settings import DEFAULT_SSH_TRANSIENT_MARKERS
    assert any(m in "n51client30: stdio forwarding failed"
               for m in DEFAULT_SSH_TRANSIENT_MARKERS)
