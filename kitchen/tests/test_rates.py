"""Rate search: the decision algorithm and searched-rates persistence."""

import json

from kitchen.run.rates import (Measurement, SearchedRates, dead, saturated,
                               search)


def _capacity_measure(capacity, dead_above=None, log=None):
    """A fake protocol: delivers min(offered, capacity); dead at dead_above."""
    def measure(rate):
        if log is not None:
            log.append(rate)
        if dead_above is not None and rate >= dead_above:
            return Measurement(offered=rate, delivered=0.0, requested=rate,
                               window_secs=1.0, duration_secs=1.0)
        return Measurement(offered=rate, delivered=min(rate, capacity),
                           requested=rate, window_secs=1.0, duration_secs=1.0)
    return measure


def _run(measure, **kw):
    decisions = []
    search(measure, on_decision=lambda a, r, n: decisions.append((a, r)), **kw)
    return decisions


def test_climb_brackets_and_refines():
    visited = []
    decisions = _run(_capacity_measure(8000, log=visited),
                     start=1000, refine_steps=3)
    # Climb 1000→16000: at 16000 delivered/offered = 0.5 → saturated. The
    # refinement samples one step below the bracket (6000) before the three
    # inside it.
    assert visited == [1000, 2000, 4000, 8000, 16000, 6000, 10000, 12000, 14000]
    actions = [a for a, _ in decisions]
    assert actions == ["start", "climb", "climb", "climb", "climb",
                       "refine", "refine", "refine", "refine"]


def test_dead_at_start_abandons():
    visited = []
    decisions = _run(_capacity_measure(8000, dead_above=500, log=visited),
                     start=1000)
    assert visited == [1000]
    assert decisions[-1][0] == "abandon"


def test_dead_during_halving_abandons():
    visited = []

    def measure(rate):
        visited.append(rate)
        if rate >= 1000:
            # Saturated but alive: ratio below threshold.
            return Measurement(offered=rate, delivered=rate * 0.5,
                               requested=rate, window_secs=1.0)
        return Measurement(offered=rate, delivered=0.0, requested=rate,
                           window_secs=1.0)   # dead below 1000

    decisions = _run(measure, start=1000, min_rate=100)
    assert visited == [1000, 500]
    assert decisions[-1][0] == "abandon"


def test_worse_ratio_stops_descent():
    # Delivered/offered gets *worse* as rate drops: not load saturation.
    def measure(rate):
        return Measurement(offered=rate, delivered=rate * min(0.9, rate / 4000),
                           requested=rate, window_secs=1.0)

    decisions = _run(measure, start=2000, min_rate=100)
    assert decisions[-1][0] == "abandon"


def test_dead_during_refine_stops():
    def measure(rate):
        if rate > 9000:
            return Measurement(offered=rate, delivered=0.0, requested=rate,
                               window_secs=1.0)
        return Measurement(offered=rate, delivered=min(rate, 8000),
                           requested=rate, window_secs=1.0)

    visited = []

    def logging_measure(rate):
        visited.append(rate)
        return measure(rate)

    decisions = _run(logging_measure, start=8000, refine_steps=3)
    # 8000 ok → climb to 16000 (dead ⇒ saturated) → refine 6000 (below the
    # bracket) → 10000 dead → stop.
    assert visited == [8000, 16000, 6000, 10000]
    assert decisions[-1][0] == "abandon"


def test_saturated_client_limits_stop_climb():
    m = Measurement(offered=100, delivered=100, cap_wait_frac=0.5)
    stop, note = saturated(m, None)
    assert stop and "client" in note
    m = Measurement(offered=610, delivered=600, requested=16000, window_secs=1.0)
    stop, note = saturated(m, None)
    assert stop and "load generator" in note


def test_dead_predicate():
    assert dead(Measurement(offered=100, delivered=0))
    assert not dead(Measurement(offered=100, delivered=1))
    assert not dead(None)


def test_searched_rates_roundtrip(tmp_path):
    sr = SearchedRates(tmp_path)
    fields = {"payload_size": 1024, "gamma": None}
    assert sr.get(fields) is None
    sr.start(fields)
    sr.record(fields, 1000.0)
    sr.record(fields, 2000)
    sr.record(fields, 1000)          # deduped, 1000.0 == 1000
    # In-progress points are not replayed.
    assert sr.get(fields) is None
    sr.finish(fields)
    assert sr.get(fields) == [1000, 2000]
    # Survives reload; None-valued dims don't change the key.
    again = SearchedRates(tmp_path)
    assert again.get({"payload_size": 1024}) == [1000, 2000]
    data = json.loads((tmp_path / "searched_rates.json").read_text())
    assert data["format"] == "searched-rates/v2"
