"""Which rates the knee search visits, and what the pass cap bounds.

Every refinement pass is a set of full benchmark runs, so the cap is a cost
bound: one pass samples the bracket once and stops, more passes narrow it.
"""

from kitchen.run.rates import Measurement, search


def _measure_to(knee):
    """A protocol that delivers everything up to `knee` and plateaus above."""
    visited = []

    def measure(rate):
        visited.append(rate)
        delivered = min(rate, knee)
        return Measurement(offered=min(rate, knee * 1.02), delivered=delivered,
                           requested=rate)

    return measure, visited


def test_one_pass_is_the_default():
    measure, visited = _measure_to(50000)
    search(measure, start=16000, max_rate=200000)
    one = list(visited)

    measure, visited = _measure_to(50000)
    search(measure, start=16000, max_rate=200000, max_refine_passes=1)
    assert visited == one


def test_more_passes_measure_more_and_bracket_tighter():
    def run(passes):
        measure, visited = _measure_to(50000)
        search(measure, start=16000, max_rate=200000,
               max_refine_passes=passes, min_resolution=1.0)
        below = [r for r in visited if r <= 50000]
        above = [r for r in visited if r > 50000]
        return len(visited), min(above) - max(below)

    one_n, one_gap = run(1)
    four_n, four_gap = run(4)
    assert four_n > one_n
    assert four_gap < one_gap


def test_uncapped_terminates_and_refines_past_the_capped_runs():
    """No cap means refine until the tolerance; it must still finish."""
    def points(passes):
        measure, visited = _measure_to(50000)
        search(measure, start=16000, max_rate=200000,
               max_refine_passes=passes, knee_tolerance=0.05,
               min_resolution=1.0)
        return len(visited)

    assert points(None) > points(1)


def test_a_rate_is_measured_at_most_once():
    for passes in (1, 3, None):
        measure, visited = _measure_to(50000)
        search(measure, start=16000, max_rate=200000, max_refine_passes=passes,
               min_resolution=1.0)
        assert len(visited) == len(set(visited)), passes


def test_no_pass_measures_closer_than_the_resolution():
    """The floor is on the spacing measured, not only on where refining stops."""
    measure, visited = _measure_to(69000)
    search(measure, start=8000, max_rate=200000,
           max_refine_passes=8, min_resolution=1000.0)

    rates = sorted(set(visited))
    gaps = [b - a for a, b in zip(rates, rates[1:])]
    assert min(gaps) >= 1000.0
