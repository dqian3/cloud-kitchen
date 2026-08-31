from types import SimpleNamespace

import pytest

from kitchend import main


def test_restart_refuses_running_job_without_force(monkeypatch):
    calls = []

    def api(_config, path):
        if "state=running" in path:
            return [{"id": 51, "state": "running", "display_state": "running"}]
        return []

    monkeypatch.setattr(main, "_api", api)
    monkeypatch.setattr(main.subprocess, "run", lambda *a, **k: calls.append(a))

    with pytest.raises(SystemExit, match=r"active job\(s\) #51.*--force"):
        main.cmd_restart(object(), SimpleNamespace(force=False))
    assert calls == []


def test_restart_refuses_cluster_bringup_without_force(monkeypatch):
    def api(_config, path):
        if "state=waiting" in path:
            return [{"id": 51, "state": "waiting", "display_state": "starting",
                     "bringing_up": "aspen-bft/main"}]
        return []

    monkeypatch.setattr(main, "_api", api)
    monkeypatch.setattr(
        main.subprocess, "run",
        lambda *a, **k: pytest.fail("restart must not be invoked"))

    with pytest.raises(SystemExit, match=r"#51 \(starting\)"):
        main.cmd_restart(object(), SimpleNamespace(force=False))


def test_forced_restart_bypasses_check(monkeypatch):
    calls = []
    monkeypatch.setattr(
        main, "_api", lambda *_a, **_k: pytest.fail("force skips the check"))
    monkeypatch.setattr(
        main.subprocess, "run", lambda argv, **kw: calls.append((argv, kw)))

    main.cmd_restart(object(), SimpleNamespace(force=True))

    assert calls == [
        (["systemctl", "--user", "restart", "kitchend.service"],
         {"check": True})
    ]


def test_restart_runs_when_idle(monkeypatch):
    calls = []
    monkeypatch.setattr(main, "_api", lambda *_a, **_k: [])
    monkeypatch.setattr(
        main.subprocess, "run", lambda argv, **kw: calls.append((argv, kw)))

    main.cmd_restart(object(), SimpleNamespace(force=False))

    assert len(calls) == 1
