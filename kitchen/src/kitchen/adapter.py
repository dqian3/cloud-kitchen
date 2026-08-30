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

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class ExperimentInfo:
    name: str
    description: str = ""
    queue: str = ""                      # serialization key, e.g. cluster bucket
    args: tuple[str, ...] = ()           # driver args that run this experiment
    replicas: int | None = None          # VM-count hint, if static
    default_flags: tuple[str, ...] = ()  # extra flags always passed
    # A native experiment: a full argv (no driver prefix) that runs this
    # experiment on the SweepEngine. It assumes its cluster is already up —
    # the daemon leases the queue's cluster around the job when the queue
    # names one it manages. Command experiments are one job each; submitting
    # several (or an aggregate containing them) fans out into sibling jobs
    # that the per-queue FIFO serializes and same-cluster leases hand over
    # between. Empty () = a classic driver-run experiment.
    command: tuple[str, ...] = ()
    # Display grouping: a variant (pbft_n4, aspen_no_crypto, ...) names its
    # base experiment here so catalogs can fold it behind that base instead
    # of crowding the top-level list. Purely presentational — grouped
    # experiments stay individually submittable, and submission aggregates
    # (`aggregates()`) are a separate, orthogonal construct.
    group: str = ""
    # The cluster VMs this experiment addresses, when fewer than the whole
    # cluster (a committee sweep whose largest point uses 22 of 102). The
    # daemon's lease starts only these. Empty = the whole cluster.
    hosts: tuple[str, ...] = ()


@dataclass(frozen=True)
class DimDisplay:
    """How a sweep dimension should be presented (form hints, table columns)."""
    name: str                            # the dim's sweep-param name
    label: str = ""                      # column/axis header; name if empty
    unit: str = ""
    description: str = ""
    example: str = ""                    # example value list, e.g. "16,1024"


@dataclass(frozen=True)
class MetricDisplay:
    """How a per-point metric should be presented in results tables."""
    name: str                            # key in the point's metrics dict
    label: str = ""                      # column header; name if empty
    unit: str = ""


@dataclass(frozen=True)
class DisplayInfo:
    dims: tuple[DimDisplay, ...] = ()
    metrics: tuple[MetricDisplay, ...] = ()


@runtime_checkable
class ProjectAdapter(Protocol):
    """Required surface. Adapters may additionally implement

        def oneoff(self, params: dict) -> list[str]

    returning a full argv for an ad-hoc sweep: params carries generic sweep
    fields (base experiment, dims {name: [values]}, rates, rate_search,
    trials, duration_secs, extra_flags) and the adapter translates them to
    its driver's flags. Not part of the Protocol so isinstance checks keep
    passing for catalog-only adapters; the daemon probes with getattr.

    A second optional hook,

        def display(self) -> DisplayInfo

    advertises how this project's dims and metrics should be shown: the
    catalog endpoint passes it through so UIs can build parameter forms and
    result columns per project instead of hardcoding names. Metric order is
    the column order; unknown dims stay submittable (dims here are hints,
    not a schema).
    """
    name: str

    def experiments(self) -> list[ExperimentInfo]: ...

    def aggregates(self) -> dict[str, list[str]]:
        """Named groups that expand to experiment lists (may be empty)."""
        ...
