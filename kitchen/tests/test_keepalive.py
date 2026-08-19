import pytest

import kitchen.cluster.keepalive as keepalive_mod
from kitchen.cluster import AlreadyRunning, ClusterState, KeepAlive
from kitchen.remote import MockRemote


def _run_ticks(monkeypatch, ka, ticks):
    """Run the keep-alive loop for `ticks` iterations, then interrupt it."""
    remaining = [ticks]

    def fake_sleep(s):
        if remaining[0] == 0:
            raise KeyboardInterrupt
        remaining[0] -= 1

    monkeypatch.setattr(keepalive_mod.time, "sleep", fake_sleep)
    ka.run()


def test_rearm_then_stop_on_interrupt(monkeypatch, capsys):
    r = MockRemote()
    r.vm_states = {"a": "RUNNING", "b": "RUNNING"}
    ka = KeepAlive(r, ["a", "b"], interval_s=1800)
    _run_ticks(monkeypatch, ka, ticks=2)
    # Two ticks * two VMs of cancel+rearm.
    rearms = r.ssh_calls(r"shutdown -c 2>/dev/null; nohup sudo shutdown -h \+60")
    assert len(rearms) == 4
    assert r.vm_states == {"a": "TERMINATED", "b": "TERMINATED"}
    out = capsys.readouterr().out
    assert "Heartbeat every 30 min" in out
    assert "All VMs stopped." in out


def test_no_stop_on_exit_leaves_vms(monkeypatch, capsys):
    r = MockRemote()
    r.vm_states = {"a": "RUNNING"}
    ka = KeepAlive(r, ["a"], stop_on_exit=False)
    _run_ticks(monkeypatch, ka, ticks=1)
    assert r.vm_states == {"a": "RUNNING"}
    assert "dead-man switch" in capsys.readouterr().out


def test_restart_fallen(monkeypatch):
    r = MockRemote()
    r.vm_states = {"a": "RUNNING", "b": "TERMINATED"}
    ka = KeepAlive(r, ["a", "b"], restart_fallen=True, stop_on_exit=False)
    _run_ticks(monkeypatch, ka, ticks=1)
    starts = [c for c in r.calls if c.kind == "vm_start"]
    assert starts and starts[0].detail == "b"


def test_single_actor_lock(monkeypatch, tmp_path):
    r = MockRemote()
    state = ClusterState("c1", root=tmp_path)
    ka1 = KeepAlive(r, ["a"], state=state, stop_on_exit=False)
    ka1.acquire_lock()
    try:
        ka2 = KeepAlive(r, ["a"], state=state, stop_on_exit=False)
        with pytest.raises(AlreadyRunning, match="c1"):
            _run_ticks(monkeypatch, ka2, ticks=0)
    finally:
        ka1.release_lock()
    # Lock released: a new keep-alive can run.
    ka3 = KeepAlive(r, ["a"], state=state, stop_on_exit=False)
    _run_ticks(monkeypatch, ka3, ticks=0)


def test_sigterm_restored_after_run(monkeypatch):
    import signal
    prev = signal.getsignal(signal.SIGTERM)
    r = MockRemote()
    _run_ticks(monkeypatch, KeepAlive(r, ["a"], stop_on_exit=False), ticks=0)
    assert signal.getsignal(signal.SIGTERM) is prev
