"""Site defaults for remote backends.

Which cloud account a testbed lives in and how to reach it describes the
testbed, not the experiment, so it lives in settings rather than in checked-in
per-cluster configs. Deliberately NOT read from gcloud's active configuration:
that is global mutable state shared with everything else on the machine, and
when it gets switched to another project mid-session every VM lookup resolves
against the wrong project and a live cluster reports itself as NOT_FOUND.

Consumer repos build their own defaults (typically once, in their shim module)
and either pass them explicitly or install them process-wide:

    SETTINGS = RemoteSettings.from_env(
        prefixes=("ASPEN_", "KITCHEN_"),
        gcp_project="my-project", tunnel_through_iap=True,
    )
    set_default_settings(SETTINGS)

Recognized environment variables, tried per prefix in order (first prefix that
has the variable set wins): {P}GCP_PROJECT, {P}GCP_IAP, {P}SSH_ATTEMPTS,
{P}SSH_RETRY_DELAY_S.
"""

import os
from dataclasses import dataclass, field

# A single ssh that dies on connection setup (VM still booting, IAP hiccup,
# sshd not yet up) is usually fine seconds later. Only connection-layer
# failures are retried — a command that ran and exited nonzero is not.
DEFAULT_SSH_TRANSIENT_MARKERS = (
    "Connection closed by",
    "Connection refused",
    "Connection reset by peer",
    "Connection timed out",
    "failed to connect to backend",
    "kex_exchange_identification",
    "Error while connecting [4003",
    "Could not establish connection",
    "ssh_exchange_identification",
)

_FALSY = ("0", "false", "False", "no", "")


@dataclass(frozen=True)
class RemoteSettings:
    gcp_project: str | None = None
    tunnel_through_iap: bool = False
    # Connection setup is retried; a command that ran and exited nonzero is
    # not (see GCloudRemote.ssh). This covers two things with one mechanism:
    # a jump host that refuses one connection in a large fan-out (without a
    # retry, three points of a 102-VM sweep died on a single dropped scp),
    # and a VM that was just started and whose sshd is not listening yet.
    # The second sets the budget: a cold boot plus key propagation runs to a
    # minute or two, where the fan-out case clears in seconds. Each retry is
    # printed, so this recovers from a burst without hiding a real fault.
    ssh_attempts: int = 12
    ssh_retry_delay_s: int = 10
    ssh_transient_markers: tuple[str, ...] = field(
        default=DEFAULT_SSH_TRANSIENT_MARKERS
    )

    @classmethod
    def from_env(cls, prefixes=("KITCHEN_",), **fallbacks):
        """Build settings from the environment, with keyword fallbacks.

        A variable that is set (even to the empty string) wins over the
        fallback; for numeric knobs an empty value falls through to the
        fallback, matching the historical `int(os.environ.get(...) or N)`
        idiom. An unset variable uses the fallback, then the field default.
        """

        def _lookup(name):
            for p in prefixes:
                val = os.environ.get(p + name)
                if val is not None:
                    return val
            return None

        kwargs = dict(fallbacks)

        project = _lookup("GCP_PROJECT")
        if project is not None:
            kwargs["gcp_project"] = project

        iap = _lookup("GCP_IAP")
        if iap is not None:
            kwargs["tunnel_through_iap"] = iap not in _FALSY

        ssh_attempts = _lookup("SSH_ATTEMPTS")
        if ssh_attempts:
            kwargs["ssh_attempts"] = int(ssh_attempts)

        ssh_delay = _lookup("SSH_RETRY_DELAY_S")
        if ssh_delay:
            kwargs["ssh_retry_delay_s"] = int(ssh_delay)

        return cls(**kwargs)


_default_settings: RemoteSettings | None = None


def set_default_settings(settings: RemoteSettings) -> None:
    """Install process-wide defaults, used when a backend is built without
    explicit settings. Consumer shims call this once at import time so that
    code constructing `GCloudRemote(...)` directly still honours the repo's
    environment overrides."""
    global _default_settings
    _default_settings = settings


def get_default_settings() -> RemoteSettings:
    global _default_settings
    if _default_settings is None:
        _default_settings = RemoteSettings.from_env()
    return _default_settings
