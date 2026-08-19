import subprocess

import pytest

import kitchen.remote.gcloud as gcloud_mod
from kitchen.remote import GCloudRemote, RemoteSettings


class ScriptedRun:
    """Replaces subprocess.run in the gcloud module with scripted results."""

    def __init__(self):
        self.rules = []  # (predicate, [results consumed in order])
        self.calls = []

    def add(self, substr, *results):
        self.rules.append((substr, list(results)))

    def __call__(self, cmd, **kwargs):
        self.calls.append(cmd)
        joined = " ".join(cmd)
        for substr, results in self.rules:
            if substr in joined and results:
                rc, out, err = results.pop(0)
                if kwargs.get("check") and rc != 0:
                    raise subprocess.CalledProcessError(rc, cmd, output=out, stderr=err)
                return subprocess.CompletedProcess(cmd, rc, stdout=out, stderr=err)
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")


@pytest.fixture
def runner(monkeypatch):
    r = ScriptedRun()
    monkeypatch.setattr(gcloud_mod.subprocess, "run", r)
    monkeypatch.setattr(gcloud_mod.time, "sleep", lambda s: None)
    return r


def _remote(**kw):
    settings = RemoteSettings(vm_start_attempts=3, vm_start_retry_delay_s=0)
    return GCloudRemote(zone="us-central1-a", project="proj", settings=settings, **kw)


def test_vm_start_retries_stockout_then_succeeds(runner, capsys):
    r = _remote()
    r._zone_cache["vm1"] = "us-central1-a"
    runner.add("instances start",
               (1, "", "ZONE_RESOURCE_POOL_EXHAUSTED"),
               (0, "", ""))
    r.vm_start(["vm1"])
    out = capsys.readouterr().out
    assert "retrying" in out and "attempt 2" in out


def test_vm_start_does_not_retry_permanent_errors(runner):
    r = _remote()
    r._zone_cache["vm1"] = "us-central1-a"
    runner.add("instances start", (1, "", "Permission denied"), (0, "", ""))
    with pytest.raises(RuntimeError, match="vm_start failed for 1/1"):
        r.vm_start(["vm1"])
    starts = [c for c in runner.calls if "start" in c]
    assert len(starts) == 1  # no second attempt


def test_vm_start_gives_up_after_attempts(runner):
    r = _remote()
    r._zone_cache["vm1"] = "us-central1-a"
    runner.add("instances start", *[(1, "", "resource pool exhausted")] * 3)
    with pytest.raises(RuntimeError):
        r.vm_start(["vm1"])
    assert len([c for c in runner.calls if "start" in c]) == 3


def test_vm_status_empty_list_never_calls_gcloud(runner):
    assert _remote().vm_status([]) == {}
    assert runner.calls == []


def test_resolve_zone_rejects_empty_name(runner):
    with pytest.raises(RuntimeError, match="empty VM name"):
        _remote()._resolve_zone("")


def test_iap_flag_only_on_ssh_args(runner):
    r = _remote(tunnel_through_iap=True)
    r._zone_cache["vm1"] = "us-central1-a"
    assert "--tunnel-through-iap" in r._ssh_args("vm1")
    assert "--tunnel-through-iap" not in r._base_args("vm1")


def test_get_all_ips_empty_and_cached(runner):
    r = _remote()
    assert r.get_all_ips([]) == {}
    assert runner.calls == []
    r._ip_cache = {"vm1": "10.0.0.1"}
    assert r.get_all_ips(["vm1"]) == {"vm1": "10.0.0.1"}
    assert runner.calls == []


def test_ssh_retries_transient_connection_errors(runner, capsys):
    r = _remote()
    r._zone_cache["vm1"] = "us-central1-a"
    runner.add("compute ssh",
               (255, "", "kex_exchange_identification: Connection closed by remote host"),
               (0, "ok", ""))
    result = r.ssh("vm1", "true")
    assert result.stdout == "ok"
    assert "retrying" in capsys.readouterr().out


def test_ssh_does_not_retry_command_failures(runner):
    r = _remote()
    r._zone_cache["vm1"] = "us-central1-a"
    runner.add("compute ssh", (1, "", "some command error"), (0, "", ""))
    with pytest.raises(subprocess.CalledProcessError):
        r.ssh("vm1", "false")
    assert len([c for c in runner.calls if "compute" in c and "ssh" in c]) == 1
