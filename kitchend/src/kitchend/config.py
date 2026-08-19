"""Daemon configuration: ~/.cloud-kitchen/config.toml.

Example:

    bind_host = "127.0.0.1"       # keep loopback; expose via `tailscale serve`
    bind_port = 8321

    [[projects]]
    name = "aspen-bft"
    repo_path = "~/Projects/bft/aspen-bft"
    runs_roots = ["runs", "paper_data"]

    [[projects]]
    name = "lazylog"
    repo_path = "~/Projects/vsac/lazylog-rpc"
    runs_roots = ["vsac-scripts/sweeps"]
"""

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

STATE_DIR = Path(os.environ.get("KITCHEN_STATE_DIR", "~/.cloud-kitchen")).expanduser()
CONFIG_PATH = STATE_DIR / "config.toml"


@dataclass(frozen=True)
class ProjectConfig:
    name: str
    repo_path: Path
    runs_roots: tuple[str, ...] = ()
    adapter_path: Path | None = None


@dataclass(frozen=True)
class Config:
    bind_host: str = "127.0.0.1"
    bind_port: int = 8321
    db_path: Path = STATE_DIR / "kitchend.sqlite3"
    projects: tuple[ProjectConfig, ...] = field(default=())


def load_config(path: Path | None = None) -> Config:
    path = path or CONFIG_PATH
    if not path.exists():
        return Config()
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    projects = tuple(
        ProjectConfig(
            name=p["name"],
            repo_path=Path(p["repo_path"]).expanduser(),
            runs_roots=tuple(p.get("runs_roots", ())),
            adapter_path=(Path(p["adapter_path"]).expanduser()
                          if p.get("adapter_path") else None),
        )
        for p in raw.get("projects", [])
    )
    return Config(
        bind_host=raw.get("bind_host", "127.0.0.1"),
        bind_port=int(raw.get("bind_port", 8321)),
        db_path=Path(raw.get("db_path", STATE_DIR / "kitchend.sqlite3")).expanduser(),
        projects=projects,
    )
