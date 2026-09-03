"""Which rates each search visits.

The fixed-pass search is the default; the relative-knee search is opt-in
because it buys precision with extra measured points, and on a large
committee every point is billed.
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


def test_default_search_is_the_fixed_pass_one():
    measure, visited = _measure_to(50000)
    search(measure, start=32000, max_rate=200000)
    fixed = list(visited)

    measure, visited = _measure_to(50000)
    search(measure, start=32000, max_rate=200000, relative_knee=False)
    assert visited == fixed


def test_relative_knee_costs_more_points_than_fixed_passes():
    measure, fixed = _measure_to(50000)
    search(measure, start=32000, max_rate=200000)

    measure, relative = _measure_to(50000)
    search(measure, start=32000, max_rate=200000, relative_knee=True,
           knee_tolerance=0.05)

    assert len(relative) > len(fixed)


def test_relative_knee_leaves_a_tighter_bracket():
    """The point of the extra passes: a narrower gap around the knee."""
    def bracket(**kw):
        measure, visited = _measure_to(50000)
        search(measure, start=32000, max_rate=200000, **kw)
        below = [r for r in visited if r <= 50000]
        above = [r for r in visited if r > 50000]
        return min(above) - max(below)

    assert bracket(relative_knee=True, knee_tolerance=0.05,
                   min_resolution=1.0) < bracket()


def test_a_rate_is_measured_at_most_once_either_way():
    for relative in (False, True):
        measure, visited = _measure_to(50000)
        search(measure, start=32000, max_rate=200000, relative_knee=relative)
        assert len(visited) == len(set(visited)), relative
