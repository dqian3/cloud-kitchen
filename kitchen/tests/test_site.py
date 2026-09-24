from kitchen import site
from kitchen.remote import RemoteSettings

CONFIG = """
[[projects]]
name = "aspen-bft"
gcp_project = "site-project"
tunnel_through_iap = true

[[projects]]
name = "other"
"""


def _use_config(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    path.write_text(CONFIG)
    monkeypatch.setattr(site, "CONFIG_PATH", path)
    for var in ("KITCHEN_GCP_PROJECT", "ASPEN_GCP_PROJECT", "KITCHEN_GCP_IAP"):
        monkeypatch.delenv(var, raising=False)


def test_for_project_reads_the_site_entry(tmp_path, monkeypatch):
    _use_config(tmp_path, monkeypatch)
    s = RemoteSettings.for_project("aspen-bft")
    assert s.gcp_project == "site-project"
    assert s.tunnel_through_iap is True


def test_environment_overrides_the_site_entry(tmp_path, monkeypatch):
    _use_config(tmp_path, monkeypatch)
    monkeypatch.setenv("KITCHEN_GCP_PROJECT", "env-project")
    assert RemoteSettings.for_project("aspen-bft").gcp_project == "env-project"


def test_unknown_project_and_missing_file_leave_it_unset(tmp_path, monkeypatch):
    _use_config(tmp_path, monkeypatch)
    assert RemoteSettings.for_project("other").gcp_project is None
    monkeypatch.setattr(site, "CONFIG_PATH", tmp_path / "absent.toml")
    assert RemoteSettings.for_project("aspen-bft").gcp_project is None
