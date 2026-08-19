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
has the variable set wins): {P}GCP_PROJECT, {P}GCP_IAP, {P}VM_START_ATTEMPTS,
{P}VM_START_RETRY_DELAY_S.
"""

import os
from dataclasses import dataclass, field

# Starting a stopped cluster asks one zone for N machines of one type at once,
# and a zone that is short right now is usually fine seconds later. Without a
# retry the whole run ends on a condition that resolves itself. Only capacity
# is worth retrying: bad machine type, permissions, or a VM that does not
# exist fail the same way every time.
DEFAULT_VM_START_RETRY_MARKERS = (
    "ZONE_RESOURCE_POOL_EXHAUSTED",
    "does not have enough resources available",
    "resource pool exhausted",
    "Internal error",
)

_FALSY = ("0", "false", "False", "no", "")


@dataclass(frozen=True)
class RemoteSettings:
    gcp_project: str | None = None
    tunnel_through_iap: bool = False
    vm_start_attempts: int = 4
    vm_start_retry_delay_s: int = 20
    vm_start_retry_markers: tuple[str, ...] = field(
        default=DEFAULT_VM_START_RETRY_MARKERS
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

        attempts = _lookup("VM_START_ATTEMPTS")
        if attempts:
            kwargs["vm_start_attempts"] = int(attempts)

        delay = _lookup("VM_START_RETRY_DELAY_S")
        if delay:
            kwargs["vm_start_retry_delay_s"] = int(delay)

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
