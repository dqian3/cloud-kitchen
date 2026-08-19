"""Project adapter: how a repo exposes its experiments to the daemon.

A consumer repo ships a `kitchen_adapter.py` next to its scripts, registered
in ~/.cloud-kitchen/config.toml via `adapter_path`. The module must export
`get_adapter() -> ProjectAdapter`. The daemon imports it with the module's
own directory on sys.path, so it can import the repo's script modules.

v1 surface (catalog only): the daemon uses it to list real experiments, to
resolve a submitted experiment name to driver arguments, and to route the job
onto the right queue (one queue per cluster bucket, so experiments that share
a cluster serialize). Later versions add run-native execution.
"""

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class ExperimentInfo:
    name: str
    description: str = ""
    queue: str = ""                      # serialization key, e.g. cluster bucket
    args: tuple[str, ...] = ()           # driver args that run this experiment
    replicas: int | None = None          # VM-count hint, if static
    default_flags: tuple[str, ...] = ()  # extra flags always passed


@runtime_checkable
class ProjectAdapter(Protocol):
    name: str

    def experiments(self) -> list[ExperimentInfo]: ...

    def aggregates(self) -> dict[str, list[str]]:
        """Named groups that expand to experiment lists (may be empty)."""
        ...
