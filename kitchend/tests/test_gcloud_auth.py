"""A dead gcloud login must read as one, wherever it surfaces."""

import subprocess

import pytest

from kitchen.remote.gcloud import GCloudAuthError, raise_for_auth
from kitchend.core.clusters import ClusterManager

# gcloud's own words when the refresh token is dead and it cannot prompt,
# which it never can from a systemd unit.
REAUTH_STDERR = (
    "ERROR: (gcloud.compute.instances.list) There was a problem refreshing "
    "your current auth tokens: Reauthentication failed. cannot prompt during "
    "non-interactive execution.\nPlease run:\n\n  $ gcloud auth login\n"
)

BIG_FILTER = "--filter=" + " OR ".join(f"name=wan-replica{i:02d}" for i in range(51))


def _done(returncode, stderr=""):
    return subprocess.CompletedProcess(["gcloud"], returncode, "", stderr)


def test_success_is_not_an_auth_failure():
    raise_for_auth(_done(0))


def test_other_failures_are_left_alone():
    raise_for_auth(_done(1, "ERROR: quota exceeded"))


@pytest.mark.parametrize("stderr", [
    REAUTH_STDERR,
    "ERROR: Your default credentials were not found.",
    "ERROR: You do not have any valid credentials.",
])
def test_reauth_stderr_raises_named_error(stderr):
    with pytest.raises(GCloudAuthError) as excinfo:
        raise_for_auth(_done(1, stderr))
    assert "gcloud auth login" in str(excinfo.value)


def test_describe_keeps_the_reason_and_drops_the_argv():
    """The bug this guards: repr() of a 102-VM status poll was two kilobytes
    of `name=... OR ...` and never mentioned stderr, where the reason was."""
    err = subprocess.CalledProcessError(
        1, ["gcloud", "compute", "instances", "list", BIG_FILTER],
        "", "ERROR: (gcloud.compute.instances.list) some other failure")
    described = ClusterManager.describe_error(err)
    assert "some other failure" in described
    assert "wan-replica50" not in described
    assert len(described) < len(repr(err)) / 4


def test_auth_error_describes_as_the_action_to_take():
    err = GCloudAuthError("gcloud has no usable credentials; run `gcloud auth login`")
    assert ClusterManager.describe_error(err) == str(err)
