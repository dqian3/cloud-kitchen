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


def clear_cache():
    _cache.clear()


def catalog(project_cfg) -> dict:
    handle = load_adapter(project_cfg)
    if not handle.ok:
        return {"error": handle.error, "experiments": [], "aggregates": {}}
    exps = handle.adapter.experiments()
    return {
        "error": None,
        "experiments": [
            {"name": e.name, "description": e.description, "queue": e.queue,
             "replicas": e.replicas}
            for e in exps
        ],
        "aggregates": handle.adapter.aggregates(),
    }


def resolve_submission(project_cfg, names: list[str]):
    """Expand aggregates, validate names, derive the queue and driver args.

    Returns (expanded_names, queue, driver_args). Unknown names raise
    ValueError with the valid options; mixing experiments from different
    queues raises too, since one job is one driver invocation on one cluster.
    """
    handle = load_adapter(project_cfg)
    if not handle.ok:
        # No adapter: pass through unvalidated (raw driver args mode).
        return names, None, list(names)
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

    queues = {by_name[n].queue for n in expanded if by_name[n].queue}
    if len(queues) > 1:
        raise ValueError(
            f"experiments span multiple clusters ({sorted(queues)}); "
            "submit one job per cluster")

    # Driver args: concatenation of each experiment's args. Experiments that
    # carry a full argument preset (rather than just their own name) can't be
    # combined in one driver invocation.
    presets = [n for n in expanded if tuple(by_name[n].args) != (n,)]
    if presets and len(expanded) > 1:
        raise ValueError(
            f"{presets[0]} is a preset with its own arguments; "
            "submit it as its own job")
    driver_args = [a for n in expanded for a in by_name[n].args]
    for n in expanded:
        driver_args += list(by_name[n].default_flags)
    return expanded, (queues.pop() if queues else None), driver_args
