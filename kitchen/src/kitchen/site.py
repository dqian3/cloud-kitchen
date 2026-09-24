"""Site configuration shared by the daemon and direct runs.

`~/.cloud-kitchen/config.toml` (or `$KITCHEN_STATE_DIR/config.toml`) is the
daemon's configuration, and its `[[projects]]` entries also hold per-project
site settings such as the GCP project VMs are created in. A driver run without
the daemon reads the same entry, so both paths use one project id.
"""

import os
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    tomllib = None

STATE_DIR = Path(os.environ.get("KITCHEN_STATE_DIR", "~/.cloud-kitchen")).expanduser()
CONFIG_PATH = STATE_DIR / "config.toml"


def project_entry(name: str, path: Path | None = None) -> dict:
    """The `[[projects]]` table named `name`, or {} if there is none."""
    path = path or CONFIG_PATH
    if tomllib is None or not path.exists():
        return {}
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    for project in raw.get("projects", []):
        if project.get("name") == name:
            return project
    return {}
