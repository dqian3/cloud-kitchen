"""Declarative sweep description.

The old drivers had no parameter record — an experiment's grid lived inside
its handler as a literal argv fragment, and anything that needed the grid
(the dry-run cost estimator, the resume logic) had to re-parse the argv it
had just built. A SweepSpec is that record: the engine iterates it, the
daemon can estimate from it, and the events it produces describe it.
"""

import itertools
from dataclasses import dataclass, field


@dataclass(frozen=True)
class RateSearchSpec:
    """Knee search bounds (see rates.search for the algorithm)."""
    start: float = 1000.0
    min_rate: float = 100.0
    max_rate: float = 200000.0
    refine_steps: int = 3


@dataclass(frozen=True)
class SweepSpec:
    """One experiment: trials x dims x rates.

    dims maps dimension name -> the values to sweep, in the order the
    directory segments should nest (insertion order is the layout contract,
    the same way PRODUCT_DIMS order was in the old sweeper). rates is a
    static list; search replaces it with a knee search. Neither means one
    rate-less point per combo.

    max_attempts > 1 enables per-point retry on `error` (launch or analysis
    raised — "we don't know"). `dead` (ran but committed nothing) is a data
    condition and is not retried unless retry_dead is set, for the runs where
    an infra flake can masquerade as a dead protocol.

    resume_required_keys: a summary.json only counts as complete if these
    keys are present and non-None. None picks a default: the search-critical
    keys when searching (replaying a degenerate point would feed the search
    a lie), nothing extra otherwise.

    coupled: groups of dim names whose value lists step together (zip)
    instead of crossing — a diagonal sweep like f=p walks (1,1), (2,2)
    rather than the full grid. Lists within a group must be equal length; a
    dim may belong to at most one group. Directory naming is unchanged:
    every dim keeps its own segment.
    """
    name: str
    dims: dict[str, tuple] = field(default_factory=dict)
    rates: tuple[float, ...] = ()
    search: RateSearchSpec | None = None
    trials: int = 1
    trial_offset: int = 0
    duration_secs: float | None = None
    resume: bool = False
    max_attempts: int = 1
    retry_dead: bool = False
    pause_secs: float = 0.0
    resume_required_keys: tuple[str, ...] | None = None
    coupled: tuple[tuple[str, ...], ...] = ()

    def __post_init__(self):
        if self.rates and self.search is not None:
            raise ValueError(f"{self.name}: give static rates or a search, not both")
        if self.trials < 1:
            raise ValueError(f"{self.name}: trials must be >= 1")
        if self.max_attempts < 1:
            raise ValueError(f"{self.name}: max_attempts must be >= 1")
        seen: set[str] = set()
        for group in self.coupled:
            if len(group) < 2:
                raise ValueError(f"{self.name}: coupled group {group} needs "
                                 "at least two dims")
            lengths = set()
            for dim in group:
                if dim not in self.dims:
                    raise ValueError(f"{self.name}: coupled dim '{dim}' is "
                                     "not a sweep dim")
                if dim in seen:
                    raise ValueError(f"{self.name}: dim '{dim}' is in more "
                                     "than one coupled group")
                seen.add(dim)
                lengths.add(len(self.dims[dim]))
            if len(lengths) > 1:
                raise ValueError(
                    f"{self.name}: coupled dims {group} need equal-length "
                    f"value lists, got {sorted(lengths)}")

    def _axes(self):
        """One axis per independent dim, one per coupled group (placed at
        its first member's position); each axis is a list of partial dicts."""
        group_of = {d: g for g in self.coupled for d in g}
        axes, consumed = [], set()
        for name in self.dims:
            group = group_of.get(name)
            if group is None:
                axes.append([{name: v} for v in self.dims[name]])
            elif group not in consumed:
                consumed.add(group)
                axes.append([dict(zip(group, vals))
                             for vals in zip(*(self.dims[d] for d in group))])
        return axes

    def combos(self):
        """Every dims combination as a dict, in declaration order."""
        if not self.dims:
            yield {}
            return
        names = list(self.dims)
        for parts in itertools.product(*self._axes()):
            combo = {}
            for part in parts:
                combo.update(part)
            yield {n: combo[n] for n in names}

    def n_combos(self) -> int:
        n = 1
        for axis in self._axes():
            n *= len(axis)
        return n

    def est_points(self) -> int | None:
        """Total points, or None when a rate search makes it unknowable."""
        if self.search is not None:
            return None
        return self.trials * self.n_combos() * (len(self.rates) or 1)
