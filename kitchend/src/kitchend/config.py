"""Daemon configuration: ~/.cloud-kitchen/config.toml.

Example:

    bind_host = "127.0.0.1"       # keep loopback; expose via `tailscale serve`
    bind_port = 8321

    [[projects]]
    name = "aspen-bft"
    repo_path = "~/Projects/bft/aspen-bft"
    runs_roots = ["runs", "paper_data"]
    driver = ["python3", "run_experiment.py"]
    driver_cwd = "scripts/benchmarks"          # relative to repo_path
    output_dir_flag = "--output-dir"
    resume_flag = "--resume"
    name_flag = "--name"                 # what the driver calls the run
    gcp_project = "nyu-rdg-fy26-as11800-2203"   # per-cluster override below
    tunnel_through_iap = true
    publish_root = "data/figures"        # served read-only at /pub/aspen-bft/

      [[projects.clusters]]
      name = "main"
      config = "scripts/benchmarks/configs/gcloud-aspen-16.yaml"  # rel. to repo
      hourly_usd = 0.7256      # per VM; cost meter multiplies by VM count

The daemon reads each cluster's YAML just to learn the VM list; it understands
the aspen shape (replica.vms + client.vms), the vsac role-keyed shape, and a
plain `vms: [...]` list.
"""

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

STATE_DIR = Path(os.environ.get("KITCHEN_STATE_DIR", "~/.cloud-kitchen")).expanduser()
CONFIG_PATH = STATE_DIR / "config.toml"


@dataclass(frozen=True)
class ClusterConfig:
    name: str
    config: str                       # path to the cluster YAML, relative to repo
    hourly_usd: float | None = None
    # Provisioning command (argv, run in the project's driver_cwd): CREATES
    # the VMs — gcloud instances create, not start — via the repo's own
    # tested setup script. Unset = the daemon can only start/stop existing
    # VMs for this cluster.
    create_cmd: tuple[str, ...] = ()
    # GCP project this cluster's VMs live in, when it is not the project's
    # own. A fleet can be rebuilt in a different GCP project (a zone with no
    # capacity is a property of the project's region, and creation there can
    # place VMs where starting them cannot) without moving the rest.
    gcp_project: str | None = None


@dataclass(frozen=True)
class ProjectConfig:
    name: str
    repo_path: Path
    runs_roots: tuple[str, ...] = ()
    adapter_path: Path | None = None
    driver: tuple[str, ...] = ()
    driver_cwd: str = "."
    output_dir_flag: str = "--output-dir"
    resume_flag: str = "--resume"
    # The flag a driver takes its run's name in. A job submitted as a raw
    # command gets its label from it, so the queue shows a name and not argv.
    name_flag: str = "--name"
    gcp_project: str | None = None
    tunnel_through_iap: bool = False
    # A directory (relative to repo_path) the daemon serves as static files
    # at /pub/<project>/. What goes there is the project's business — the
    # daemon knows nothing about its contents.
    publish_root: str | None = None
    clusters: tuple[ClusterConfig, ...] = ()


@dataclass(frozen=True)
class Config:
    bind_host: str = "127.0.0.1"
    bind_port: int = 8321
    db_path: Path = STATE_DIR / "kitchend.sqlite3"
    jobs_dir: Path = STATE_DIR / "jobs"
    max_concurrent_queues: int = 1     # one job at a time
    projects: tuple[ProjectConfig, ...] = field(default=())

    def project(self, name: str) -> ProjectConfig:
        for p in self.projects:
            if p.name == name:
                return p
        raise KeyError(f"unknown project: {name}")


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
            driver=tuple(p.get("driver", ())),
            driver_cwd=p.get("driver_cwd", "."),
            output_dir_flag=p.get("output_dir_flag", "--output-dir"),
            resume_flag=p.get("resume_flag", "--resume"),
            name_flag=p.get("name_flag", "--name"),
            gcp_project=p.get("gcp_project"),
            tunnel_through_iap=bool(p.get("tunnel_through_iap", False)),
            publish_root=p.get("publish_root"),
            clusters=tuple(
                ClusterConfig(
                    name=c["name"],
                    config=c["config"],
                    hourly_usd=c.get("hourly_usd"),
                    create_cmd=tuple(c.get("create_cmd", ())),
                    gcp_project=c.get("gcp_project"),
                )
                for c in p.get("clusters", [])
            ),
        )
        for p in raw.get("projects", [])
    )
    return Config(
        bind_host=raw.get("bind_host", "127.0.0.1"),
        bind_port=int(raw.get("bind_port", 8321)),
        db_path=Path(raw.get("db_path", STATE_DIR / "kitchend.sqlite3")).expanduser(),
        jobs_dir=Path(raw.get("jobs_dir", STATE_DIR / "jobs")).expanduser(),
        max_concurrent_queues=int(raw.get("max_concurrent_queues", 1)),
        projects=projects,
    )
