"""Find a protocol's knee by search, rather than by guessing a rate list.

Ported from aspen-bft's rate_search.py — specifically from its `search()`
function, not the older inlined loop in sweep.py, because `search()` carries
two safeguards the inlined loop never got: a dead-check inside the halving
descent, and an abort when halving makes delivered/offered *worse* (i.e. the
saturation was never load-caused). Both were paid for in postmortems; the
comments that explain them moved with the code.

The shape is: double the offered rate until a point saturates, which brackets
the knee in O(log) runs, then sample that bracket at even arithmetic steps.
The climb is geometric because it only has to *find* the bracket; the knee can
sit anywhere inside it, so uniform resolution is what is wanted there. A start
rate that is already saturated halves instead.
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


def search(measure: Callable[[float], Optional[Measurement]],
           *, start: float = 1000.0, max_rate: float = 200000.0,
           min_rate: float = 100.0, refine_steps: int = 3,
           on_decision: Optional[Callable[[str, float, str], None]] = None,
           saturated_fn: Callable[..., tuple[bool, str]] = None) -> None:
    """Drive `measure` over a searched rate sequence.

    `measure(rate)` runs one point and returns a Measurement, or None if the
    run produced nothing usable. Results are the caller's to record; this only
    decides which rates to visit. `on_decision(action, rate, note)` fires as
    each decision is made, with action one of start|climb|halve|refine|abandon.
    `saturated_fn` overrides the default rule set (same signature as
    `saturated`).
    """
    sat_fn = saturated_fn or saturated

    def decide(action, rate, note=""):
        if on_decision is not None:
            on_decision(action, rate, note)

    rate = float(start)
    prev_delivered: Optional[float] = None
    last_good: Optional[float] = None
    first_bad: Optional[float] = None

    decide("start", rate)
    point = measure(rate)
    sat, note = sat_fn(point, None)
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
        point = measure(rate)
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
            rate *= 2
            while rate <= max_rate:
                decide("climb", rate)
                point = measure(rate)
                sat, note = sat_fn(point, prev_delivered)
                if sat:
                    first_bad = rate
                    break
                if point is not None:
                    last_good = rate
                    prev_delivered = point.delivered
                rate *= 2

    if last_good is None or first_bad is None:
        where = ("never saturated" if first_bad is None
                 else "saturated at the first rate")
        decide("abandon", rate, f"{where}; no knee bracketed")
        return

    step = (first_bad - last_good) / (refine_steps + 1)
    for i in range(1, refine_steps + 1):
        refined = round(last_good + step * i)
        decide("refine", refined,
               f"knee between {last_good:g} and {first_bad:g}" if i == 1 else "")
        point = measure(refined)
        # Refining upward from the last good rate, so once a point commits
        # nothing every higher one will too.
        if dead(point):
            decide("abandon", refined,
                   "committed nothing; stopping the refinement here")
            return


# --- persistence: the rates a search visited, so later trials replay them ---

# Where a sweep dir records the rates its search visited, so a later invocation
# adding trials to that dir replays them instead of searching again. A second
# search would agree on the geometric climb but step its refinement off
# whichever bracket that run landed in, and every analyzer groups trials on the
# exact rate -- so re-searched trials silently fail to combine.
SEARCHED_RATES_FILE = "searched_rates.json"
SEARCHED_RATES_FORMAT = "searched-rates/v2"
SEARCHED_RATES_NOTE = (
    "Offered rates (msgs/sec) the knee search visited at each sweep point, in visit "
    "order: the geometric climb that brackets the knee, then the arithmetic refinement "
    "inside that bracket. Complete points are replayed by later invocations; incomplete "
    "points resume the search through their already-measured prefix."
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

    def get(self, fields: dict) -> Optional[list]:
        """The rates to replay at a completed point, or None if it needs search.

        An in-progress point deliberately returns None. The caller restarts the
        deterministic search, feeds its completed prefix back through the search
        algorithm, and continues beyond the interruption. ``start`` and
        ``record`` preserve and de-duplicate that prefix.
        """
        record = self._records.get(_lookup_key(_clean(fields)))
        if record is None:
            return None
        return list(record["rates"]) if record["complete"] else None

    def start(self, fields: dict) -> None:
        """Claim a point before searching it, so an interrupted search leaves a
        partial record behind, and a point whose search yields nothing is
        remembered as empty instead of re-searched by every later trial."""
        fields = _clean(fields)
        self._records.setdefault(
            _lookup_key(fields),
            {"fields": fields, "rates": [], "complete": False},
        )
        self._write()

    def record(self, fields: dict, rate) -> None:
        """Note that the search is about to run `rate` at this point."""
        fields = _clean(fields)
        record = self._records.setdefault(
            _lookup_key(fields),
            {"fields": fields, "rates": [], "complete": False},
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
