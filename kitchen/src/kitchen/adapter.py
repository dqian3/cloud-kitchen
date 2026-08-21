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


@dataclass(frozen=True)
class DimInfo:
    """Display metadata for one sweep dimension the project understands."""
    name: str                            # sweep param key, e.g. "payload_size"
    label: str = ""                      # short display label; "" -> name
    unit: str = ""                       # e.g. "bytes"
    description: str = ""
    choices: tuple = ()                  # suggested values for submit forms


@dataclass(frozen=True)
class MetricInfo:
    """Display metadata for one per-point result metric."""
    name: str                            # key in a point's metrics dict
    label: str = ""                      # short column header; "" -> name
    unit: str = ""                       # e.g. "ms", "msg/s"


@dataclass(frozen=True)
class DisplayInfo:
    """What the project's UI surfaces should look like: which dims the
    submit form offers and which metric columns result tables show."""
    dims: tuple[DimInfo, ...] = ()
    metrics: tuple[MetricInfo, ...] = ()


@runtime_checkable
class ProjectAdapter(Protocol):
    """Required surface. Adapters may additionally implement

        def oneoff(self, params: dict) -> list[str]

    returning a full argv for an ad-hoc sweep: params carries generic sweep
    fields (base experiment, dims {name: [values]}, rates, rate_search,
    trials, duration_secs, extra_flags) and the adapter translates them to
    its driver's flags. And

        def display(self) -> DisplayInfo

    advertising the project's dims and standard metric names with labels/
    units, so the dashboard builds its submit form and result columns per
    project instead of guessing. Neither is part of the Protocol so
    isinstance checks keep passing for catalog-only adapters; the daemon
    probes with getattr.
    """
    name: str

    def experiments(self) -> list[ExperimentInfo]: ...

    def aggregates(self) -> dict[str, list[str]]:
        """Named groups that expand to experiment lists (may be empty)."""
        ...
