"""Remote execution backends: gcloud, plain ssh, local processes, and a test fake.

Extracted from aspen-bft/scripts/benchmarks/remote.py (of which the VSAC
repos carried a drifted fork). Site defaults (GCP project, IAP, retry knobs)
live in RemoteSettings rather than module constants; consumer repos install
theirs with set_default_settings() or pass them to load_remote().
"""

from .base import Remote
from .mock import MockRemote
from .gcloud import GCloudRemote
from .docker import DockerRemote
from .local import LocalRemote
from .settings import (
    RemoteSettings,
    get_default_settings,
    set_default_settings,
)
from .sshremote import SSHRemote

__all__ = [
    "Remote", "GCloudRemote", "SSHRemote", "LocalRemote",
    "DockerRemote", "MockRemote",
    "RemoteSettings", "get_default_settings", "set_default_settings",
    "load_remote",
]


def _cfg_get(config, key, default=None):
    if isinstance(config, dict):
        return config.get(key, default)
    return getattr(config, key, default)


def load_remote(config, settings: RemoteSettings | None = None):
    """Factory: reads 'platform' field from config, returns appropriate Remote.

    `settings` supplies site defaults (project, IAP) for fields the config
    leaves unset; defaults to the process-wide settings.
    """
    settings = settings or get_default_settings()
    platform = _cfg_get(config, "platform")
    if platform == "gcloud":
        # A config may still name either explicitly -- useful for a cluster in
        # a different account, e.g. one whose VMs do have public addresses --
        # but it does not have to, and checked-in configs deliberately do not.
        project = _cfg_get(config, "project") or settings.gcp_project
        if not project:
            # Never fall through to gcloud's ambient project: the VMs named in
            # this config exist in one project, and picking up whatever the
            # CLI happens to be pointed at finds either nothing or, worse,
            # someone else's fleet with the same names.
            raise ValueError(
                "no GCP project for this cluster: set `project:` in the "
                "cluster config, or KITCHEN_GCP_PROJECT in the environment")
        iap = _cfg_get(config, "tunnel_through_iap")
        if iap is None:
            iap = settings.tunnel_through_iap
        return GCloudRemote(
            zone=_cfg_get(config, "zone"),
            project=project,
            tunnel_through_iap=bool(iap),
            settings=settings,
            # Set per cluster: only a fleet large enough for the per-VM IAP
            # tunnels to exhaust memory needs a jump host.
            proxy_jump=_cfg_get(config, "proxy_jump"),
            ssh_user=_cfg_get(config, "ssh_user"),
            ssh_key_file=_cfg_get(config, "ssh_key_file"),
        )
    elif platform == "ssh":
        return SSHRemote(user=_cfg_get(config, "user", "root"), key_file=_cfg_get(config, "key_file"))
    elif platform == "local":
        return LocalRemote(root=_cfg_get(config, "local_root"))
    elif platform == "docker":
        compose = _cfg_get(config, "compose_file")
        if not compose:
            raise ValueError("platform: docker needs compose_file")
        return DockerRemote(compose, project=_cfg_get(config, "compose_project"))
    else:
        raise ValueError(f"Unknown platform: {platform}")
