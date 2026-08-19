import pytest

import kitchen.cluster.lifecycle as lifecycle
from kitchen.cluster import ClusterState, start_vms, stop_vms, wait_drained
from kitchen.remote import FakeRemote


@pytest.fixture(autouse=True)
def fast_sleep(monkeypatch):
    monkeypatch.setattr(lifecycle.time, "sleep", lambda s: None)


def test_start_arms_deadman():
    r = FakeRemote()
    r.vm_states = {"a": "TERMINATED", "b": "TERMINATED"}
    start_vms(r, ["a", "b"], deadman_minutes=60)
    assert r.vm_states == {"a": "RUNNING", "b": "RUNNING"}
    arms = r.ssh_calls(r"shutdown -h \+60")
    assert len(arms) == 2


def test_start_skips_already_running():
    r = FakeRemote()
    r.vm_states = {"a": "RUNNING", "b": "TERMINATED"}
    start_vms(r, ["a", "b"])
    starts = [c for c in r.calls if c.kind == "vm_start"]
    assert len(starts) == 1 and starts[0].detail == "b"
    # Dead-man still re-armed everywhere.
    assert len(r.ssh_calls(r"shutdown -h \+60")) == 2


def test_start_drains_stopping_vms_first(capsys):
    r = FakeRemote()
    # Two polls see a STOPPING vm; the third sees TERMINATED.
    r.status_sequence = [
        {"a": "STOPPING", "b": "TERMINATED"},
        {"a": "STOPPING", "b": "TERMINATED"},
        {"a": "TERMINATED", "b": "TERMINATED"},
    ]
    start_vms(r, ["a", "b"])
    out = capsys.readouterr().out
    assert "waiting for 1 VM(s) to reach TERMINATED" in out
    assert r.vm_states["a"] == "RUNNING"


def test_wait_drained_times_out(monkeypatch):
    r = FakeRemote()
    r.vm_states = {"a": "STOPPING"}
    clock = iter(range(0, 10000, 100))
    monkeypatch.setattr(lifecycle.time, "monotonic", lambda: next(clock))
    with pytest.raises(RuntimeError, match="timed out.*STOPPING|timed out"):
        wait_drained(r, ["a"], timeout_s=300)


def test_stop_refuses_with_live_lease(tmp_path):
    r = FakeRemote()
    r.vm_states = {"a": "RUNNING"}
    state = ClusterState("c1", root=tmp_path)
    with state.acquire_lease("test-sweep"):
        with pytest.raises(RuntimeError, match="live leases.*test-sweep"):
            stop_vms(r, ["a"], state=state)
        # force overrides
        stop_vms(r, ["a"], state=state, force=True)
        assert r.vm_states["a"] == "TERMINATED"


def test_stop_proceeds_after_release(tmp_path):
    r = FakeRemote()
    r.vm_states = {"a": "RUNNING"}
    state = ClusterState("c1", root=tmp_path)
    lease = state.acquire_lease("test-sweep")
    lease.release()
    stop_vms(r, ["a"], state=state)
    assert r.vm_states["a"] == "TERMINATED"
