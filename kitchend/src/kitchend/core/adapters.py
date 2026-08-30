"""Load project adapters (kitchen_adapter.py modules) and resolve experiments.

An adapter import runs repo code (e.g. aspen's run_experiment.py builds its
catalog at import), so imports happen lazily, are cached, and a failure is
reported rather than crashing the daemon.
"""

import importlib.util
import sys
from dataclasses import dataclass

from kitchen.adapter import ExperimentInfo


@dataclass
class AdapterHandle:
    adapter: object | None
    error: str | None

    @property
    def ok(self):
        return self.adapter is not None


_cache: dict[str, AdapterHandle] = {}


def load_adapter(project_cfg) -> AdapterHandle:
    path = project_cfg.adapter_path
    if path is None:
        return AdapterHandle(None, "no adapter_path configured")
    key = str(path)
    if key in _cache:
        return _cache[key]
    try:
        moddir = str(path.parent)
        if moddir not in sys.path:
            sys.path.insert(0, moddir)
        spec = importlib.util.spec_from_file_location(
            f"kitchen_adapter_{project_cfg.name.replace('-', '_')}", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        adapter = mod.get_adapter()
        handle = AdapterHandle(adapter, None)
    except Exception as e:
        handle = AdapterHandle(None, f"{type(e).__name__}: {e}")
    _cache[key] = handle
    return handle


def _display_info(adapter) -> dict | None:
    """Serialize the optional display() hook (dim/metric presentation)."""
    hook = getattr(adapter, "display", None)
    if hook is None:
        return None
    d = hook()
    return {
        "dims": [
            {"name": x.name, "label": x.label or x.name, "unit": x.unit,
             "description": x.description, "example": x.example}
            for x in d.dims
        ],
        "metrics": [
            {"name": x.name, "label": x.label or x.name, "unit": x.unit}
            for x in d.metrics
        ],
    }


def catalog(project_cfg) -> dict:
    handle = load_adapter(project_cfg)
    if not handle.ok:
        return {"error": handle.error, "experiments": [], "aggregates": {},
                "display": None}
    exps = handle.adapter.experiments()
    return {
        "error": None,
        "experiments": [
            {"name": e.name, "description": e.description, "queue": e.queue,
             "replicas": e.replicas, "native": bool(e.command),
             "group": e.group}
            for e in exps
        ],
        "aggregates": handle.adapter.aggregates(),
        "display": _display_info(handle.adapter),
    }


def resolve_jobs(project_cfg, names: list[str]) -> list[dict]:
    """Expand aggregates, validate names, and plan the jobs a submission is.

    Returns a list of job plans, each {"experiments": [...], "queue": str|None}
    plus either "driver_args" (classic experiments, run in one driver
    invocation) or "command" (a native experiment's full argv — one job per
    experiment, so its retries, lease, and progress are its own). Unknown
    names raise ValueError with the valid options; classic experiments from
    different queues raise too, since their shared driver invocation runs on
    one cluster. Native experiments each carry their own queue, so an
    aggregate spanning clusters fans out fine as long as its classic members
    agree.
    """
    handle = load_adapter(project_cfg)
    if not handle.ok:
        # No adapter: pass through unvalidated (raw driver args mode).
        return [{"experiments": names, "queue": None, "driver_args": list(names)}]
    by_name: dict[str, ExperimentInfo] = {e.name: e for e in handle.adapter.experiments()}
    aggregates = handle.adapter.aggregates()

    expanded: list[str] = []
    for n in names:
        if n in aggregates:
            expanded.extend(x for x in aggregates[n] if x not in expanded)
        elif n in by_name:
            if n not in expanded:
                expanded.append(n)
        else:
            raise ValueError(
                f"unknown experiment '{n}' for project {project_cfg.name}; "
                f"known: {sorted(by_name)} + aggregates {sorted(aggregates)}")

    plans: list[dict] = []
    classic = [n for n in expanded if not by_name[n].command]
    for n in expanded:
        if by_name[n].command:
            plans.append({"experiments": [n],
                          "queue": by_name[n].queue or None,
                          "command": [str(a) for a in by_name[n].command],
                          "hosts": [str(h) for h in by_name[n].hosts]})

    if classic:
        queues = {by_name[n].queue for n in classic if by_name[n].queue}
        if len(queues) > 1:
            raise ValueError(
                f"experiments span multiple clusters ({sorted(queues)}); "
                "submit one job per cluster")
        # Driver args: concatenation of each experiment's args. Experiments
        # that carry a full argument preset (rather than just their own name)
        # can't be combined in one driver invocation.
        presets = [n for n in classic if tuple(by_name[n].args) != (n,)]
        if presets and len(classic) > 1:
            raise ValueError(
                f"{presets[0]} is a preset with its own arguments; "
                "submit it as its own job")
        driver_args = [a for n in classic for a in by_name[n].args]
        for n in classic:
            driver_args += list(by_name[n].default_flags)
        plans.append({"experiments": classic,
                      "queue": (queues.pop() if queues else None),
                      "driver_args": driver_args})
    return plans


def oneoff_command(project_cfg, params: dict):
    """Resolve an ad-hoc sweep to (argv, queue) via the project's adapter.

    The adapter's optional `oneoff(params) -> argv` owns the translation from
    generic sweep parameters (base experiment, dims, rates/search, trials,
    duration) to that project's driver flags — it is the only place that
    knows the flag names. Adapters without the hook can't run one-offs.
    """
    handle = load_adapter(project_cfg)
    if not handle.ok:
        raise ValueError(f"one-off sweeps need a working adapter: {handle.error}")
    build = getattr(handle.adapter, "oneoff", None)
    if build is None:
        raise ValueError(f"project '{project_cfg.name}' adapter does not "
                         "support one-off sweeps (no oneoff() hook)")
    argv = build(dict(params))
    if not argv:
        raise ValueError("adapter returned an empty one-off command")
    # Queue precedence: explicit queue, then the cluster the sweep names
    # (one-offs on a costed cluster must land on that cluster's queue so
    # they serialize with its other jobs and get cost-gated), then the base
    # experiment's queue.
    queue = params.get("queue") or params.get("cluster")
    base = params.get("base")
    if not queue and base:
        by_name = {e.name: e for e in handle.adapter.experiments()}
        if base not in by_name:
            raise ValueError(f"unknown base experiment '{base}'")
        queue = by_name[base].queue or None
    return [str(a) for a in argv], queue
