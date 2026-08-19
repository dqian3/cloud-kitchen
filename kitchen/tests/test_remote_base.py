import subprocess

from kitchen.remote import FakeRemote


def test_run_on_all_empty_hosts_is_noop():
    r = FakeRemote()
    assert r.run_on_all([], "echo hi") == {}
    assert r.calls == []


def test_run_on_all_passes_timeout_and_collects_errors():
    r = FakeRemote()
    r.script("boom", host="h2", returncode=1)
    results = r.run_on_all(["h1", "h2"], "boom", quiet=True, timeout=42)
    assert results["h1"].returncode == 0
    assert isinstance(results["h2"], subprocess.CalledProcessError)
    assert all(c.timeout == 42 for c in r.ssh_calls("boom"))


def test_kill_process_is_quiet_and_bounded():
    r = FakeRemote()
    r.script("pkill", returncode=1)  # pkill failure must not raise
    r.kill_process(["a", "b"], "myproc")
    calls = r.ssh_calls("pkill -9 -f myproc")
    assert len(calls) == 2
    assert all(c.timeout == 60 for c in calls)


def test_seed_broadcast_delivers_to_all(tmp_path):
    payload = tmp_path / "bin"
    payload.write_text("x")
    r = FakeRemote()
    hosts = [f"h{i}" for i in range(5)]
    r.setup_inter_vm_ssh(hosts)
    r.seed_broadcast(str(payload), hosts, "~/bin")
    # Seed got a real upload; everyone else got a vm-to-vm copy.
    assert (hosts[0], "~/bin") in r.files
    for h in hosts[1:]:
        assert (h, "~/bin") in r.files
    # All copies originate from the seed.
    assert len(r.ssh_calls(r"scp .* -i ", host=hosts[0])) == 4


def test_tree_broadcast_delivers_to_all_in_rounds(tmp_path):
    payload = tmp_path / "bin"
    payload.write_text("x")
    r = FakeRemote()
    hosts = [f"h{i}" for i in range(7)]
    r.setup_inter_vm_ssh(hosts)
    r.tree_broadcast(str(payload), hosts, "~/bin", fanout=2)
    for h in hosts:
        assert (h, "~/bin") in r.files
    # With fanout=2 not every copy comes from the seed: later rounds fan out
    # from hosts that already have the file.
    non_seed_copies = [
        c for c in r.ssh_calls(r"scp .* -i ") if c.host != hosts[0]
    ]
    assert non_seed_copies, "tree rounds should copy from non-seed hosts"


def test_broadcast_empty_hosts_is_noop(tmp_path):
    payload = tmp_path / "bin"
    payload.write_text("x")
    r = FakeRemote()
    r.seed_broadcast(str(payload), [], "~/bin")
    r.tree_broadcast(str(payload), [], "~/bin")
    assert r.calls == []
