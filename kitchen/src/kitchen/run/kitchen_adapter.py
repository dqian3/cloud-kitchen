"""Project adapter for the toy protocol.

Point a daemon project at this file (adapter_path) to get a native,
event-emitting executor with no cloud behind it — for demos, for exercising
ingest and the ledger end-to-end, and as a small reference project adapter.
"""

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

def get_adapter():
    return ToyProjectAdapter()
