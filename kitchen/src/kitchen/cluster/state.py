"""Per-cluster directory for the keepalive singleton lock."""

import os
from pathlib import Path

DEFAULT_ROOT = Path(os.environ.get("KITCHEN_STATE_DIR", "~/.cloud-kitchen")).expanduser()

class ClusterState:
    def __init__(self, name: str, root: Path | None = None):
        if not name or "/" in name:
            raise ValueError(f"bad cluster name: {name!r}")
        self.name = name
        self.dir = (root or DEFAULT_ROOT) / "clusters" / name
        self.dir.mkdir(parents=True, exist_ok=True)

    @property
    def keepalive_lock_path(self) -> Path:
        return self.dir / "keepalive.lock"
