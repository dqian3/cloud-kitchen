"""Daemon configuration: ~/.cloud-kitchen/config.toml.

Example:

    bind_host = "127.0.0.1"       # keep loopback; expose via `tailscale serve`
    bind_port = 8321
    cluster_retry_delay_secs = 600   # cooldown after a failed bring-up

    [[projects]]
    name = "aspen-bft"
    repo_path = "~/Projects/bft/aspen-bft"
    runs_roots = ["data/runs", "data/paper_data"]
    adapter_path = "~/Projects/bft/aspen-bft/benchmarks/kitchen_adapter.py"
    driver_cwd = "benchmarks"            # where jobs run, relative to repo_path
    # driver = ["python3", "run.py"]     # only for catalog experiments that
    #                                    # carry no command of their own
    output_dir_flag = "--output-dir"
    resume_flag = "--resume"
    name_flag = "--name"                 # what the driver calls the run
    gcp_project = "my-gcp-project"       # default for every cluster below
    tunnel_through_iap = true
    publish_root = "data/figures"        # served read-only at /pub/aspen-bft/

      [[projects.clusters]]
      name = "main"
      config = "benchmarks/fleets/main.yaml"   # relative to repo_path
      hourly_usd = 0.7256      # per VM; cost meter multiplies by VM count
      # gcp_project = "..."    # only to override the project-wide one

The daemon reads each cluster's YAML just to learn the VM list; it understands
the aspen shape (replica.vms or replica.pool + client.vms), a fleet shape
(replica/client groups of `prefix` plus `count` or `regions`), the vsac
role-keyed shape, and a plain `vms: [...]` list.
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
    # GCP project this cluster's VMs live in, when it differs from the
    # project-wide `gcp_project`.
    gcp_project: str | None = None
    # Does this cluster need an ssh jump host, and what creates it?
    #
    # The daemon used to learn this only from the cluster YAML's
    # `proxy_jump_vm`, which says the jump *exists* -- so a lease opened the
    # tunnel, `start` failed on a VM that was never created, and the fleet
    # behind it was unreachable for a reason nothing named. Declaring it here
    # is what lets the daemon create one, the same way create_cmd does for
    # the fleet. Set jump_vm and the YAML's proxy_jump_vm must agree; the
    # daemon refuses a cluster where they disagree, as it does for the
    # project. jump_create_cmd unset = start/stop only, as before.
    jump_vm: str | None = None
    jump_create_cmd: tuple[str, ...] = ()


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
    # GCP project for this project's clusters. Direct runs read the same key
    # (kitchen.site), so this is the one place to change it.
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
    # How long a job waits after its cluster would not come up. A daemon
    # setting, not a per-job one: it exists to keep a regional stockout from
    # turning into enough create calls to hit API limits, and that ceiling
    # belongs to the fleet rather than to whoever submitted the job. It also
    # used to travel: submitted jobs stored the value, resubmit copied the
    # spec verbatim, and a delay from before the ten-minute change kept being
    # inherited by descendants long after the default moved.
    cluster_retry_delay_secs: int = 600
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
                    jump_vm=c.get("jump_vm"),
                    jump_create_cmd=tuple(c.get("jump_create_cmd", ())),
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
        cluster_retry_delay_secs=int(
            raw.get("cluster_retry_delay_secs", 600)),
        projects=projects,
    )
