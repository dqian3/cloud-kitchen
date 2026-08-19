import pytest

from kitchen.remote import (
    GCloudRemote,
    LocalRemote,
    RemoteSettings,
    SSHRemote,
    load_remote,
)


def test_env_prefix_precedence(monkeypatch):
    monkeypatch.setenv("ASPEN_GCP_PROJECT", "aspen-proj")
    monkeypatch.setenv("KITCHEN_GCP_PROJECT", "kitchen-proj")
    s = RemoteSettings.from_env(prefixes=("ASPEN_", "KITCHEN_"))
    assert s.gcp_project == "aspen-proj"
    s = RemoteSettings.from_env(prefixes=("KITCHEN_",))
    assert s.gcp_project == "kitchen-proj"


def test_env_wins_over_fallback(monkeypatch):
    monkeypatch.setenv("ASPEN_GCP_PROJECT", "from-env")
    s = RemoteSettings.from_env(prefixes=("ASPEN_",), gcp_project="fallback")
    assert s.gcp_project == "from-env"
    monkeypatch.delenv("ASPEN_GCP_PROJECT")
    s = RemoteSettings.from_env(prefixes=("ASPEN_",), gcp_project="fallback")
    assert s.gcp_project == "fallback"


def test_iap_parsing_matches_historical_semantics(monkeypatch):
    # Unset -> fallback; set to a falsy string (including empty) -> False.
    s = RemoteSettings.from_env(prefixes=("X_",), tunnel_through_iap=True)
    assert s.tunnel_through_iap is True
    for falsy in ("0", "false", "no", ""):
        monkeypatch.setenv("X_GCP_IAP", falsy)
        assert RemoteSettings.from_env(prefixes=("X_",), tunnel_through_iap=True).tunnel_through_iap is False
    monkeypatch.setenv("X_GCP_IAP", "1")
    assert RemoteSettings.from_env(prefixes=("X_",)).tunnel_through_iap is True


def test_numeric_empty_env_falls_through(monkeypatch):
    monkeypatch.setenv("X_VM_START_ATTEMPTS", "")
    s = RemoteSettings.from_env(prefixes=("X_",))
    assert s.vm_start_attempts == 4
    monkeypatch.setenv("X_VM_START_ATTEMPTS", "7")
    assert RemoteSettings.from_env(prefixes=("X_",)).vm_start_attempts == 7


def test_load_remote_dispatch(tmp_path):
    settings = RemoteSettings(gcp_project="site-proj", tunnel_through_iap=True)
    r = load_remote({"platform": "gcloud", "zone": "z"}, settings=settings)
    assert isinstance(r, GCloudRemote)
    assert r.project == "site-proj"          # settings default applied
    assert r.tunnel_through_iap is True

    r = load_remote({"platform": "gcloud", "project": "explicit",
                     "tunnel_through_iap": False}, settings=settings)
    assert r.project == "explicit"           # config wins over settings
    assert r.tunnel_through_iap is False

    assert isinstance(load_remote({"platform": "ssh", "user": "u"}), SSHRemote)
    assert isinstance(
        load_remote({"platform": "local", "local_root": str(tmp_path)}), LocalRemote)

    with pytest.raises(ValueError, match="Unknown platform"):
        load_remote({"platform": "carrier-pigeon"})


def test_load_remote_accepts_objects():
    class Cfg:
        platform = "gcloud"
        zone = "z"
        project = "p"
        tunnel_through_iap = False

    r = load_remote(Cfg())
    assert isinstance(r, GCloudRemote) and r.project == "p"
