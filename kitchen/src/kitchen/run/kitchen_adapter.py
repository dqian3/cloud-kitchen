"""Project adapter for the toy protocol.

Point a daemon project at this file (adapter_path) to get a native,
event-emitting executor with no cloud behind it — for demos, for exercising
ingest and the ledger end-to-end, and as the reference for what a repo's
kitchen_adapter.py looks like once it supports one-off sweeps.

The `oneoff(params)` hook is the pattern the real projects will copy: the
daemon hands over generic sweep parameters and the adapter — the only layer
that knows its driver's flag names — turns them into an argv.
"""

import sys

from kitchen.adapter import (DimDisplay, DisplayInfo, ExperimentInfo,
                             MetricDisplay)

_BASE_FLAGS = {
    "toy-static": ("--rates", "1000", "2000", "4000", "8000"),
    "toy-search": ("--rate-search",),
}


class ToyProjectAdapter:
    name = "toy"

    def experiments(self):
        return [
            ExperimentInfo(name="toy-static", queue="local",
                           description="fixed rate list against the toy protocol",
                           args=_BASE_FLAGS["toy-static"]),
            ExperimentInfo(name="toy-search", queue="local",
                           description="knee search against the toy protocol",
                           args=_BASE_FLAGS["toy-search"]),
        ]

    def aggregates(self):
        return {}

    def display(self):
        """Dim/metric presentation for UIs (catalog passes it through).

        Metric names match what ToyAdapter.analyze() emits; their order here
        is the results-table column order. Toy dims only label points, so the
        one advertised dim is just a form hint — any dim name still works.
        """
        return DisplayInfo(
            dims=(
                DimDisplay(name="payload_size", label="payload", unit="B",
                           example="16,1024",
                           description="labels points; the toy ignores values"),
            ),
            metrics=(
                MetricDisplay(name="throughput_msgs_per_sec",
                              label="delivered/s"),
                MetricDisplay(name="offered_rate", label="offered/s"),
                MetricDisplay(name="drop_pct", label="drop", unit="%"),
                MetricDisplay(name="total_completed", label="completed"),
            ),
        )

    def oneoff(self, params: dict) -> list:
        """Generic sweep params -> `python -m kitchen.run.toy` argv.

        params: base (experiment name), dims ({name: [values]}), rates
        ([...]), rate_search (bool or {start,min_rate,max_rate,refine_steps}),
        trials, duration_secs, extra_flags. All optional; base supplies its
        preset flags first so overrides can extend it.
        """
        argv = [sys.executable, "-m", "kitchen.run.toy"]
        base = params.get("base")
        if base:
            if base not in _BASE_FLAGS:
                raise ValueError(f"unknown base experiment '{base}'")
            argv += list(_BASE_FLAGS[base])
        for name, values in (params.get("dims") or {}).items():
            argv += ["--dim", f"{name}=" + ",".join(str(v) for v in values)]
        if params.get("rates"):
            argv += ["--rates"] + [str(r) for r in params["rates"]]
        search = params.get("rate_search")
        if search:
            if "--rate-search" not in argv:
                argv.append("--rate-search")
            if isinstance(search, dict):
                for key, flag in (("start", "--rate-search-start"),
                                  ("min_rate", "--rate-search-min"),
                                  ("max_rate", "--rate-search-max"),
                                  ("refine_steps", "--refine-steps")):
                    if search.get(key) is not None:
                        argv += [flag, str(search[key])]
        if params.get("trials"):
            argv += ["--trials", str(params["trials"])]
        if params.get("duration_secs"):
            argv += ["--duration-secs", str(params["duration_secs"])]
        argv += [str(f) for f in params.get("extra_flags", [])]
        return argv


def get_adapter():
    return ToyProjectAdapter()
