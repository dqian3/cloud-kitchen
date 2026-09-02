"""Find a protocol's knee by search, rather than by guessing a rate list.

Ported from aspen-bft's rate_search.py — specifically from its `search()`
function, not the older inlined loop in sweep.py, because `search()` carries
two safeguards the inlined loop never got: a dead-check inside the halving
descent, and an abort when halving makes delivered/offered *worse* (i.e. the
saturation was never load-caused). Both were paid for in postmortems; the
comments that explain them moved with the code.

The shape is: raise the offered rate until a point saturates, which brackets
the knee, then sample that bracket at even arithmetic steps. The climb is
geometric because it only has to *find* the bracket; the knee can sit
anywhere inside it, so uniform resolution is what is wanted there. A start
rate that is already saturated halves instead.

The climb doubles only while the step stays under `max_step`, then continues
in steps of that size. Unbounded doubling brackets the knee in fewer runs but
leaves a bracket as wide as the last good rate: a protocol serving 64k and
saturating at 128k is bracketed across 64k of range, which three refinement
points resolve to 16k. Capping the step keeps the bracket a bounded width
wherever the knee turns out to be, at the cost of a few more runs high up.

Refinement then repeats on whichever sub-bracket still straddles the
transition, until the bracket is within `knee_tolerance` of the knee rate (or
`min_resolution`, whichever is wider). One pass of fixed width cannot do this:
the bracket it starts from is set by the climb, so the resolution it reaches
is a fraction of the *climb* rather than of the answer. A 32k-wide bracket
resolves a 96k knee to 8% and a 24k knee to 33%, for the same three runs. A
relative target spends the runs where the answer is imprecise, and the pass
that reaches the target costs nothing on a protocol the first pass already
resolved.

Each pass samples `refine_steps` points across its bracket rather than
bisecting it, so the bracket narrows by refine_steps + 1 per pass. Bisection
would reach the same target in fewer *decisions* but not fewer runs, and it
would spend all of them adjacent to the knee: here every probe is a benchmark
that stays in the curve, and the ones that land past the knee are what draws
the collapse. `refine_steps=1` is binary search, if that is what a protocol
wants.

Each pass also takes one point a step under the last healthy rate. Below the
knee the climb leaves only doublings, and the run-up is where a *latency* knee
shows up -- the rules below are all throughput rules, so a protocol whose
latency has left the floor while delivered still tracks offered reads as
healthy, and nothing else will find it. A grid under a settled knee costs more
runs than the search itself and steers nothing; one point per pass costs one
run and lands closer in each time, since the step shrinks with the bracket.
"""

from __future__ import annotations

import json
import os
from typing import Callable, Optional

# Below this, delivered has stopped tracking what the clients actually put on
# the wire.
DELIVERED_RATIO = 0.95
# Doubling the offered rate should roughly double delivered throughput while
# there is headroom. Less than this means the curve has flattened even if the
# ratio still looks acceptable.
PLATEAU_GAIN = 1.10
# Below this, the clients did not put anywhere near the requested load on the
# wire, so the point measures the load generator rather than the protocol.
OFFERED_RATIO = 0.80

DEAD_NOTE = "committed nothing"


class Measurement:
    """One measured point, in the fields the saturation rules need.

    `offered` is what the clients achieved, not what they were asked for --
    past saturation the two diverge, and the configured number describes our
    intent rather than the protocol's behaviour.
    """

    def __init__(self, offered: float, delivered: float, *,
                 requested: Optional[float] = None,
                 window_secs: Optional[float] = None,
                 duration_secs: Optional[float] = None,
                 cap_wait_frac: float = 0.0, drop_pct: float = 0.0,
                 empty_window: bool = False,
                 invalid: Optional[str] = None):
        self.offered = float(offered or 0.0)
        self.delivered = float(delivered or 0.0)
        # What the point asked for, as opposed to what the clients achieved.
        self.requested = float(requested) if requested else None
        self.window_secs = window_secs
        self.duration_secs = duration_secs
        self.cap_wait_frac = float(cap_wait_frac or 0.0)
        self.drop_pct = float(drop_pct or 0.0)
        self.empty_window = bool(empty_window)
        # Set when the run did not measure the protocol at all -- part of the
        # committee never reported, a node died mid-run. The numbers such a run
        # does produce describe the survivors, not the system, so they must not
        # be read as a throughput that happens to be low. Ends the climb (a
        # rate that kills nodes bounds the usable range) and says why.
        self.invalid = invalid

    @property
    def ratio(self) -> float:
        return self.delivered / self.offered if self.offered else 0.0


def measurement_from_metrics(metrics: dict, *, requested: Optional[float] = None,
                             duration_secs: Optional[float] = None) -> Measurement:
    """Build a Measurement from the engine's standard metric names.

    An adapter's analyze() that returns offered_rate / delivered_rate (or
    throughput_msgs_per_sec) / stats_window_secs / cap_wait_frac / drop_pct
    gets the full rule set; missing fields just disable their rules.
    """
    return Measurement(
        offered=metrics.get("offered_rate") or requested or 0.0,
        delivered=(metrics.get("throughput_msgs_per_sec")
                   or metrics.get("delivered_rate") or 0.0),
        requested=requested if requested is not None else metrics.get("rate"),
        window_secs=metrics.get("stats_window_secs",
                                metrics.get("measurement_secs")),
        duration_secs=duration_secs,
        cap_wait_frac=metrics.get("cap_wait_frac") or 0.0,
        drop_pct=metrics.get("drop_pct") or 0.0,
        empty_window=bool(metrics.get("warning")),
        invalid=metrics.get("invalid"),
    )


def dead(point: Optional[Measurement]) -> bool:
    """Did this point commit anything at all?

    A wholly empty point slips past every rule below it: offered and delivered
    are both 0, so the ratio tests short-circuit on a falsy numerator and the
    plateau test has no previous value to compare against. The search then
    reads "no evidence of saturation" as "headroom" and doubles.

    Worth its own predicate rather than another line in `saturated()`, because
    the two mean different things to the driver. Saturation brackets a knee and
    is worth searching around; committing nothing is not a rung on the way to
    anything, and a protocol silent at 1,000 will be just as silent at 500, so
    the search abandons rather than halving into the floor.
    """
    return point is not None and not point.delivered


def saturated(point: Optional[Measurement],
              prev_delivered: Optional[float]) -> tuple[bool, str]:
    """Should the climb stop here, and why?

    Returns (stop, note). Two questions were conflated in an earlier version:
    whether a point is *protocol* saturation, and whether it ends the climb.
    They are different. A client that has hit its own limit is not evidence
    about the protocol -- but it does end the climb, because a generator
    already falling short at rate R cannot offer more at 2R. So client-side
    limits stop, and say so in the note rather than being reported as the
    protocol's ceiling.
    """
    if point is None:
        return False, "no result"
    if point.invalid:
        return True, point.invalid
    if dead(point):
        return True, DEAD_NOTE
    if point.cap_wait_frac > 0.01:
        return True, (f"client spent {point.cap_wait_frac:.0%} of the run capped "
                      f"on max_in_flight -- the client is the limit, not the "
                      f"protocol")
    if point.drop_pct > 1.0:
        return True, (f"client dropped {point.drop_pct:.0f}% of the load it "
                      f"generated -- the client is the limit, not the protocol")
    # Too few commits to form a measurement window means a collapse severe
    # enough that almost nothing came back. Name it as a collapse: reporting it
    # through the ratio rules below would call it a plateau.
    if point.empty_window or (point.window_secs is not None
                              and point.window_secs <= 0):
        return True, "collapsed: too few commits to form a measurement window"
    if (point.duration_secs and point.window_secs
            and point.window_secs > 1.2 * point.duration_secs):
        return True, "window ran past the send period, measured during drain"
    # The clients failing to generate the requested load is not protocol
    # saturation -- but it does end the climb, because asking for twice as much
    # from a generator that already fell short cannot produce a larger offered
    # rate. Delivered still tracks offered in this regime, so d/o stays near 1
    # and every other rule below stays silent: without this the search climbs
    # to its ceiling forever.
    if (point.requested and point.offered
            and point.offered < OFFERED_RATIO * point.requested):
        return True, (f"clients offered {point.offered:,.0f} of "
                      f"{point.requested:,.0f} requested -- the load generator "
                      f"is the limit, not the protocol")
    if point.offered and point.ratio and point.ratio < DELIVERED_RATIO:
        return True, f"delivered/offered {point.ratio:.3f}"
    if prev_delivered and point.delivered < prev_delivered * PLATEAU_GAIN:
        return True, (f"delivered gained only "
                      f"{point.delivered / prev_delivered:.2f}x on a doubling")
    return False, ""


# The widest the climb will step. Doubling below this, additive above it.
MAX_CLIMB_STEP = 32000.0

# How close the bracket must get to the knee before refinement stops, as a
# fraction of the knee rate.
KNEE_TOLERANCE = 0.05
# ...but never finer than this many msgs/sec. Below a knee of about 20k the
# fraction asks for a resolution the measurement cannot support: trial spread
# and the client generator's own jitter are both wider than the step, so the
# extra passes resolve noise rather than the knee.
MIN_KNEE_RESOLUTION = 1000.0


def next_climb_rate(rate: float, max_step: float) -> float:
    """The next rate up: double, but never by more than `max_step`."""
    return rate + min(rate, max_step)


def search(measure: Callable[[float], Optional[Measurement]],
           *, start: float = 1000.0, max_rate: float = 200000.0,
           min_rate: float = 100.0, refine_steps: int = 3,
           knee_tolerance: float = KNEE_TOLERANCE,
           min_resolution: float = MIN_KNEE_RESOLUTION,
           max_step: float = MAX_CLIMB_STEP,
           on_decision: Optional[Callable[[str, float, str], None]] = None,
           saturated_fn: Callable[..., tuple[bool, str]] = None) -> None:
    """Drive `measure` over a searched rate sequence.

    `measure(rate)` runs one point and returns a Measurement, or None if the
    run produced nothing usable. Results are the caller's to record; this only
    decides which rates to visit. `on_decision(action, rate, note)` fires as
    each decision is made, with action one of start|climb|halve|refine|abandon.
    `saturated_fn` overrides the default rule set (same signature as
    `saturated`).

    A rate is measured at most once. Refinement re-brackets off its own
    results, so it lands on rates it has already run; each point here is a
    benchmark run, and the second one would overwrite the first's output
    directory with a duplicate of the same measurement.
    """
    sat_fn = saturated_fn or saturated
    measured: dict[float, Optional[Measurement]] = {}

    def decide(action, rate, note=""):
        if on_decision is not None:
            on_decision(action, rate, note)

    def visit(rate):
        """Measure `rate`, or return what it measured last time."""
        if rate not in measured:
            measured[rate] = measure(rate)
        return measured[rate]

    rate = float(start)
    verdicts: dict[float, bool] = {}     # measured rate -> saturated?
    prev_delivered: Optional[float] = None
    last_good: Optional[float] = None
    first_bad: Optional[float] = None

    decide("start", rate)
    point = visit(rate)
    sat, note = sat_fn(point, None)
    verdicts[rate] = sat
    if dead(point):
        decide("abandon", rate,
               "committed nothing at the start rate; a protocol silent here "
               "will be silent lower down")
        return
    while sat and rate > min_rate:
        first_bad = rate
        prev_ratio = point.ratio if point is not None else 0.0
        rate = max(min_rate, rate / 2)
        decide("halve", rate, note)
        point = visit(rate)
        # Halving encodes the assumption that the saturation was load-caused.
        # Check it. Two observed failures in one night from a repair-churn
        # protocol: its healthy ceiling sits below DELIVERED_RATIO, so the rule
        # fires at *every* rate and each halving lands in a strictly worse
        # regime; and at very low rates the points stop being measurements at
        # all, which `dead()` catches -- but a loop that reads `dead` as
        # "still saturated" keeps halving into the floor.
        if dead(point):
            decide("abandon", rate,
                   "committed nothing; halving lower cannot commit more")
            return
        sat, note = sat_fn(point, None)
        verdicts[rate] = sat
        if sat and point is not None and point.ratio <= prev_ratio:
            decide("abandon", rate,
                   f"halving made delivered/offered worse "
                   f"({prev_ratio:.3f} -> {point.ratio:.3f}); this is not "
                   f"load saturation")
            return

    if not sat and point is not None:
        last_good = rate
        prev_delivered = point.delivered
        if first_bad is None:
            rate = next_climb_rate(rate, max_step)
            while rate <= max_rate:
                decide("climb", rate)
                point = visit(rate)
                sat, note = sat_fn(point, prev_delivered)
                verdicts[rate] = sat
                if sat:
                    first_bad = rate
                    break
                if point is not None:
                    last_good = rate
                    prev_delivered = point.delivered
                rate = next_climb_rate(rate, max_step)

    if last_good is None or first_bad is None:
        where = ("never saturated" if first_bad is None
                 else "saturated at the first rate")
        decide("abandon", rate, f"{where}; no knee bracketed")
        return

    # Narrow the bracket until it is within knee_tolerance of the knee. Each
    # pass samples the whole of the bracket it starts from, past the knee as
    # well as short of it -- the collapse beyond the knee is part of the curve
    # these sweeps draw -- and then re-brackets on the transition its own
    # points found.
    reported_stray = False
    while first_bad - last_good > max(knee_tolerance * last_good,
                                     min_resolution):
        step = (first_bad - last_good) / (refine_steps + 1)
        pass_rates = [round(last_good + step * i)
                      for i in range(1, refine_steps + 1)]
        pass_rates = [r for r in pass_rates
                      if last_good < r < first_bad and r not in measured]
        if not pass_rates:
            # Rounding has collapsed the bracket onto rates already run; there
            # is no finer question left to ask at integer rates.
            break
        # One step under the last healthy rate as well. The climb leaves the
        # approach to the knee sampled in doublings, and a curve read only at
        # the knee and above cannot show where the run-up left the floor --
        # which the throughput rules here do not see, so nothing else will
        # find it. One point per pass keeps that cheap and puts each pass's
        # point closer in than the last, since the step shrinks with the
        # bracket.
        below = round(last_good - step)
        if below > min_rate and below not in measured:
            pass_rates.insert(0, below)
        for r in pass_rates:
            if r < last_good:
                note = f"one step under the last healthy {last_good:g}"
            elif r == min(x for x in pass_rates if x > last_good):
                note = f"knee between {last_good:g} and {first_bad:g}"
            else:
                note = ""
            decide("refine", r, note)
            point = visit(r)
            if dead(point):
                # Refining upward from the last good rate, so once a point
                # commits nothing every higher one will too. Downward that
                # reasoning does not hold: a rate under one that served and
                # committing nothing is a broken run, not a ceiling.
                if r < last_good:
                    decide("refine", r,
                           "committed nothing below a rate that served; not a "
                           "load limit, so the refinement continues")
                    continue
                decide("abandon", r,
                       "committed nothing; stopping the refinement here")
                return
            if point is None:
                continue    # no result: no evidence either way about the knee
            # prev_delivered is deliberately not passed: the plateau rule
            # compares a doubling's gain, and these steps are a fraction of one.
            verdicts[r], _ = sat_fn(point, None)

        # Re-bracket off every verdict taken so far rather than off this pass
        # in isolation. Saturation is not guaranteed monotone in the rate --
        # one point inside the healthy region can read saturated, and updating
        # the ends independently then puts last_good above first_bad, which
        # reads as a bracket of negative width and quietly ends the refinement
        # at a knee above a rate already seen to saturate. Taking the lowest
        # saturated rate and the highest healthy rate under it keeps the
        # bracket well formed, makes first_bad monotonically non-increasing so
        # the loop still terminates, and claims the earliest saturation, which
        # is the honest reading when the two disagree.
        first_bad = min(r for r, sat in verdicts.items() if sat)
        good_below = [r for r, sat in verdicts.items()
                      if not sat and r < first_bad]
        if not good_below:
            decide("abandon", first_bad,
                   f"every rate measured under {first_bad:g} also saturated; "
                   f"the knee is not bracketed here")
            return
        last_good = max(good_below)
        stray = [r for r, sat in verdicts.items() if not sat and r > first_bad]
        if stray and not reported_stray:
            reported_stray = True
            decide("refine", first_bad,
                   f"saturation is not monotone: {min(stray):g} measured "
                   f"healthy above a saturated {first_bad:g}")


# --- persistence: the rates a search visited, so later trials replay them ---

# Where a sweep dir records the rates its search visited, so a later invocation
# adding trials to that dir replays them instead of searching again. A second
# search would agree on the geometric climb but step its refinement off
# whichever bracket that run landed in, and every analyzer groups trials on the
# exact rate -- so re-searched trials silently fail to combine.
SEARCHED_RATES_FILE = "searched_rates.json"
SEARCHED_RATES_FORMAT = "searched-rates/v3"
SEARCHED_RATES_NOTE = (
    "Offered rates (msgs/sec) the knee search visited at each sweep point, in visit "
    "order: the geometric climb that brackets the knee, then the refinement passes that "
    "narrow it. Complete points are replayed by later invocations; incomplete points "
    "resume the search through their already-measured prefix. `params` is the search "
    "that produced the list -- a later invocation asking a different question (a tighter "
    "knee_tolerance, more refine_steps) continues the search instead of replaying it, "
    "which under --resume costs only the rates the old settings never visited."
)


def describe_point(fields: dict) -> str:
    """Readable one-line identity for a sweep point, for console output."""
    parts = [f"{k}={_fmt_value(v)}" for k, v in sorted(_clean(fields).items())]
    return " ".join(parts) if parts else "(sweep default)"


def _fmt_value(value):
    if isinstance(value, float) and value.is_integer():
        return f"{value:g}"
    return str(value)


def _clean(fields: dict) -> dict:
    """Drop the dimensions this sweep did not vary. `None` means "left at the
    config value", which is the same point everywhere, so carrying it would only
    make the key -- and the file -- noisier."""
    return {k: v for k, v in fields.items() if v is not None}


def _lookup_key(fields: dict) -> str:
    """Internal, order-independent lookup key. Never written to disk: the file
    stores the field dict itself, and this is rebuilt from it on load."""
    return json.dumps(sorted(_clean(fields).items()), sort_keys=True)


def _norm_rate(rate):
    """1000.0 and 1000 are the same rate and must not read as two of them.
    Every consumer formats rates with %g, so the integral ones are stored as
    ints and the file stops mixing `1000.0` with `80000`."""
    value = float(rate)
    return int(value) if value.is_integer() else value


class SearchedRates:
    """The rates a sweep's search visited, one record per sweep point.

    A point is addressed by a dict of named dimensions -- {"payload_size":
    1024, "gamma": 1.2} -- so the file can be read without the writing
    sweeper's dimension table at hand.

    Every mutation persists immediately, through a temp file: an interrupted
    sweep leaves the previous record intact rather than a half-written one.
    """

    def __init__(self, sweep_dir):
        self.path = os.path.join(sweep_dir, SEARCHED_RATES_FILE)
        # lookup key -> {"fields": {...}, "rates": [...], "complete": bool}
        self._records: dict[str, dict] = {}
        self._read()

    def __len__(self) -> int:
        return len(self._records)

    def get(self, fields: dict, params: Optional[dict] = None) -> Optional[list]:
        """The rates to replay at a completed point, or None if it needs search.

        An in-progress point deliberately returns None. The caller restarts the
        deterministic search, feeds its completed prefix back through the search
        algorithm, and continues beyond the interruption. ``start`` and
        ``record`` preserve and de-duplicate that prefix.

        So does a point recorded under different search settings. Replaying it
        would answer the old question with the new settings' name on it; the
        search instead re-runs and extends the list, and under --resume every
        rate the old settings already visited comes back off disk. A record
        from before these settings were written down (no `params`) counts as
        different, which costs one re-search per point and then matches.
        """
        record = self._records.get(_lookup_key(_clean(fields)))
        if record is None or not record["complete"]:
            return None
        if params is not None and record.get("params") != _clean(params):
            return None
        return list(record["rates"])

    def start(self, fields: dict, params: Optional[dict] = None) -> None:
        """Claim a point before searching it, so an interrupted search leaves a
        partial record behind, and a point whose search yields nothing is
        remembered as empty instead of re-searched by every later trial.

        Rates already recorded are kept: a continuation adds to them rather
        than starting the list over."""
        fields = _clean(fields)
        record = self._records.setdefault(
            _lookup_key(fields),
            {"fields": fields, "rates": [], "params": {}, "complete": False},
        )
        record["complete"] = False
        if params is not None:
            record["params"] = _clean(params)
        self._write()

    def record(self, fields: dict, rate) -> None:
        """Note that the search is about to run `rate` at this point."""
        fields = _clean(fields)
        record = self._records.setdefault(
            _lookup_key(fields),
            {"fields": fields, "rates": [], "params": {}, "complete": False},
        )
        value = _norm_rate(rate)
        if value in record["rates"]:
            return
        record["rates"].append(value)
        self._write()

    def finish(self, fields: dict) -> None:
        """Mark a search complete only after its iterator returns normally."""
        record = self._records[_lookup_key(_clean(fields))]
        record["complete"] = True
        self._write()

    def _read(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path) as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return
        for record in data.get("points", []):
            if "point" not in record:
                continue
            fields = _clean(record["point"])
            self._records[_lookup_key(fields)] = {
                "fields": fields,
                "rates": [_norm_rate(r) for r in record.get("rates", [])],
                "params": _clean(record.get("params") or {}),
                "complete": record.get("complete", True),
            }

    def _write(self) -> None:
        # Hand-rolled rather than json.dump(indent=2) so a point is three lines
        # rather than one line per rate. The file is meant to be skimmed, which
        # is the whole reason for the format.
        blocks = []
        for record in sorted(self._records.values(),
                             key=lambda r: describe_point(r["fields"])):
            blocks.append(
                '    {\n'
                f'      "point": {json.dumps(record["fields"], sort_keys=True)},\n'
                f'      "params": {json.dumps(record.get("params") or {}, sort_keys=True)},\n'
                f'      "rates": {json.dumps(record["rates"])},\n'
                f'      "complete": {json.dumps(record["complete"])}\n'
                '    }')
        text = ('{\n'
                f'  "format": {json.dumps(SEARCHED_RATES_FORMAT)},\n'
                f'  "note": {json.dumps(SEARCHED_RATES_NOTE)},\n'
                '  "points": [\n'
                + ',\n'.join(blocks)
                + ('\n' if blocks else '')
                + '  ]\n'
                '}\n')
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            f.write(text)
        os.replace(tmp, self.path)
