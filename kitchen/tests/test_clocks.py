from kitchen.cluster import parse_chronyc_offset, sync_clocks
from kitchen.remote import MockRemote

SELECTED_US = "^* ntp.example.com  2   6   377   23  +1234us[ +1234us] +/-   12ms"
SELECTED_MS = "^* ntp.example.com  2   6   377   23  -3ms[   -3ms] +/-   12ms"
SELECTED_NS = "^* ntp.example.com  2   6   377   23  +500ns[ +500ns] +/-   12ms"
SELECTED_S = "^* ntp.example.com  2   6   377   23  +2s[ +2s] +/-   12ms"
CANDIDATE_ONLY = "^+ ntp.example.com  2   6   377   23  +1234us[ +1234us] +/-   12ms"


def test_parse_units():
    assert parse_chronyc_offset(SELECTED_US) == 1.234
    assert parse_chronyc_offset(SELECTED_MS) == -3.0
    assert parse_chronyc_offset(SELECTED_NS) == 0.0005
    assert parse_chronyc_offset(SELECTED_S) == 2000.0


def test_parse_requires_selected_source():
    assert parse_chronyc_offset(CANDIDATE_ONLY) is None
    assert parse_chronyc_offset("") is None
    assert parse_chronyc_offset(None) is None
    assert parse_chronyc_offset("=? garbage") is None


def test_sync_clocks_happy_path(capsys):
    r = MockRemote()
    r.script("chronyc", stdout=SELECTED_US)
    sync_clocks(r, ["a", "b"], threshold_ms=2.0, max_retries=3)
    out = capsys.readouterr().out
    assert "All 2 VMs synced" in out
    # One round only: happy path must not retry.
    assert len(r.ssh_calls("chronyc")) == 2


def test_sync_clocks_retries_on_divergence(capsys):
    r = MockRemote()
    diverged = "^* x  2 6 377 23  +9ms[ +9ms] +/- 12ms"
    r.script("chronyc", host="b", stdout=diverged, times=1)   # first round: b diverged
    r.script("chronyc", stdout=SELECTED_US)                   # everything else fine
    sync_clocks(r, ["a", "b"], threshold_ms=2.0, max_retries=3)
    out = capsys.readouterr().out
    assert "clock offset > 2.0ms" in out
    assert "Retrying..." in out
    assert "All 2 VMs synced" in out


def test_sync_clocks_gives_up(capsys):
    r = MockRemote()
    r.script("chronyc", stdout="no sources here")
    sync_clocks(r, ["a"], max_retries=2)
    out = capsys.readouterr().out
    assert "could not parse chronyc offset" in out
    assert "WARNING: giving up after 2 attempts" in out
