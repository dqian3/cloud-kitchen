"""Load project adapters (kitchen_adapter.py modules) and resolve experiments.

An adapter import runs repo code (a project builds its catalog at import),
so imports happen lazily, are cached, and a failure is reported rather than
crashing the daemon.
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
             "replicas": e.replicas, "group": e.group}
            for e in exps
        ],
        "aggregates": handle.adapter.aggregates(),
        "display": _display_info(handle.adapter),
    }


def resolve_jobs(project_cfg, names: list[str]) -> list[dict]:
    """Expand aggregates, validate names, and plan the jobs a submission is.

    Returns one plan per experiment: {"experiments": [name], "queue",
    "command", "hosts"}. One job each, so an experiment's retries, lease and
    progress are its own, and an aggregate spanning clusters fans out onto
    each experiment's own queue. Unknown names raise ValueError with the
    valid options.
    """
    handle = load_adapter(project_cfg)
    if not handle.ok:
        raise ValueError(
            f"project {project_cfg.name} has no experiment catalog "
            f"({handle.error}); submit an explicit command")
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

    missing = [n for n in expanded if not by_name[n].command]
    if missing:
        raise ValueError(
            f"experiment '{missing[0]}' has no command in the catalog of "
            f"project {project_cfg.name}")
    return [{"experiments": [n],
             "queue": by_name[n].queue or None,
             "command": [str(a) for a in by_name[n].command],
             "hosts": [str(h) for h in by_name[n].hosts]}
            for n in expanded]
