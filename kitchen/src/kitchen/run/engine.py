"""The native sweep engine: trials x dims x rates with resume, retry, events.

Replaces the old drivers' run_experiment.py/sweep.py pair. An adapter supplies
the three project-specific phases; the engine owns everything the sweepers
used to share by copy-paste: the loop order (trial outermost, so trial 1 only
starts after trial 0 has visited every point), directory naming, resume,
per-point retry, the rate search, the events.jsonl stream, and the exit-code
contract (0 all points produced data, 2 some committed nothing, 1 anything
raised — an error means "we don't know", which is not the same as "dead").
"""

import json
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

from kitchen.events import EventEmitter

from .rates import SearchedRates, measurement_from_metrics, search
from .spec import SweepSpec

# A summary must carry these, non-None, to be resumed under a rate search:
# they are exactly the fields the saturation rules read, and replaying a
# degenerate point (data but no measurement window) would feed the search a
# lie about that rate.
SEARCH_REQUIRED_KEYS = ("offered_rate", "delivered_rate", "stats_window_secs")


class PointFailed(Exception):
    """A point ended in error under spec.abort_on_error.

    Raised out of the point loop to end the experiment there. Carried as the
    result's `fatal`, so the run exits 1 and the points that did produce data
    keep it.
    """


@dataclass(frozen=True)
class SweepPoint:
    """One benchmark execution: dims + rate + trial -> one directory."""
    dims: dict
    rate: float | None
    trial: int
    rel_dir: str
    dir: Path


@dataclass(frozen=True)
class RunContext:
    out_root: Path
    sweep_dir: Path
    spec: SweepSpec


@runtime_checkable
class SweepAdapter(Protocol):
    """The project-specific phases of running one experiment.

    deploy() runs once per experiment (upload binaries, sync clocks).
    launch() runs the workload for one point, leaving raw artifacts in
    point.dir. analyze() turns those artifacts into a flat metrics dict —
    using the standard names (offered_rate, delivered_rate or
    throughput_msgs_per_sec, total_completed, stats_window_secs,
    cap_wait_frac, drop_pct) where they apply, so the engine's default
    dead/saturation rules work; anything extra is carried through to
    summary.json untouched.
    """
    name: str

    def deploy(self, ctx: RunContext) -> None: ...
    def launch(self, ctx: RunContext, point: SweepPoint) -> None: ...
    def analyze(self, ctx: RunContext, point: SweepPoint) -> dict: ...

    # Optional. vms_for(dims) names the hosts a point addresses (a committee
    # sweep uses fewer at smaller points); with it and a `remote` attribute
    # carrying check_vms_running / vm_stop, the engine checks that the
    # largest point's hosts are up before deploying, and under
    # release_unused_vms stops each host once no later point addresses it.
    # The adapter knows its config's hosts; the engine owns their lifecycle.
    # def vms_for(self, dims: dict) -> Iterable[str]: ...
    # remote: object


def default_point_dead(entry: dict) -> bool:
    """Did this point commit anything? total_completed is authoritative when
    the adapter reports it; otherwise fall back to delivered throughput."""
    if "total_completed" in entry:
        return not entry["total_completed"]
    return not (entry.get("throughput_msgs_per_sec") or entry.get("delivered_rate"))


def _fmt(value) -> str:
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def point_rel_dir(dims: dict, rate, trial=None) -> str:
    """Directory segments for a point: dims in declaration order, then rate,
    then -- only for a multi-trial sweep -- trial_N. A single-trial sweep
    (every point once; a repeat is a new run in a new directory) ends at
    rate_R. Rows still carry a `trial` tag, so readers that pool runs or
    aggregate trials work on either layout."""
    parts = [f"{name}_{_fmt(value)}" for name, value in dims.items()]
    if rate is not None:
        parts.append(f"rate_{_fmt(float(rate))}")
    if trial is not None:
        parts.append(f"trial_{trial}")
    return "/".join(parts)


def completed_entry(point_dir: Path, tags: dict,
                    required_keys: tuple[str, ...]) -> dict | None:
    """The point's prior summary if it is complete enough to reuse, else None.

    There is no marker file: the presence of a readable summary.json whose
    required keys are non-None *is* the marker. Dim tags are re-applied from
    the live sweep rather than trusted from disk — a resumed point is tagged
    by what this invocation is sweeping.
    """
    try:
        with open(point_dir / "summary.json") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if any(data.get(k) is None for k in required_keys):
        return None
    data.update(tags)
    return data


@dataclass
class ExperimentResult:
    name: str
    entries: list = field(default_factory=list)
    ok: int = 0
    dead: int = 0
    failed: int = 0
    skipped: int = 0        # subset of ok/dead that was resumed, not re-run
    fatal: str | None = None

    @property
    def total(self) -> int:
        return self.ok + self.dead + self.failed

    @property
    def status(self) -> str:
        # Errors outrank dead: "we don't know" must not be reported as the
        # expected shape of a saturating sweep.
        if self.fatal or self.failed:
            return "failed"
        if self.dead:
            return "degraded"
        return "ok"

    def detail(self) -> str:
        if self.fatal:
            return self.fatal
        return (f"{self.ok} ok, {self.dead} dead, {self.failed} failed"
                + (f" ({self.skipped} resumed)" if self.skipped else ""))


class SweepEngine:
    """Runs SweepSpecs through an adapter, emitting events along the way.

    saturated / point_dead override the default rule set for projects whose
    metrics don't fit the standard names; the defaults live in rates.py and
    engine.default_point_dead.
    """

    def __init__(self, adapter: SweepAdapter, out_root, emitter=None, *,
                 saturated=None, point_dead=None, sleep=time.sleep,
                 release_unused_vms=False):
        self.adapter = adapter
        self.out_root = Path(out_root)
        self.emitter = emitter
        self._saturated = saturated
        self._point_dead = point_dead or default_point_dead
        self._sleep = sleep
        self.release_unused_vms = release_unused_vms
        self.points_run = 0     # across every spec, for interrupt reporting
        self._released: set = set()

    # -- host lifecycle (only with an adapter that names a point's hosts) --

    def _vms_for(self, dims) -> set:
        hook = getattr(self.adapter, "vms_for", None)
        return set(hook(dims)) if hook else set()

    def _remote(self):
        return getattr(self.adapter, "remote", None)

    def _check_hosts(self, spec: SweepSpec) -> None:
        """Every host any point addresses must be up before deploy: for a
        committee sweep that is the largest point's set, not the config's."""
        remote = self._remote()
        if not hasattr(self.adapter, "vms_for") or remote is None:
            return
        needed = set()
        for dims in spec.combos():
            needed |= self._vms_for(dims)
        if needed and hasattr(remote, "check_vms_running"):
            remote.check_vms_running(sorted(needed))

    def _release_hosts(self, spec: SweepSpec, point: SweepPoint) -> None:
        """Stop every host neither this point nor any later one addresses.
        Only on the last trial (an earlier trial's points all run again).
        Points run in declaration order, so a sweep ordered largest-first
        releases hosts as it shrinks; smallest-first releases nothing until
        its last point."""
        remote = self._remote()
        if not self.release_unused_vms or remote is None \
                or not hasattr(self.adapter, "vms_for") \
                or not hasattr(remote, "vm_stop"):
            return
        if point.trial != spec.trial_offset + spec.trials - 1:
            return
        combos = list(spec.combos())
        if dict(point.dims) not in combos:
            return
        every, later = set(), set()
        for i, dims in enumerate(combos):
            hosts = self._vms_for(dims)
            every |= hosts
            if i >= combos.index(dict(point.dims)):
                later |= hosts
        idle = sorted(every - later - self._released)
        if not idle:
            return
        self._emit("hosts.released", experiment=spec.name, dims=point.dims,
                   hosts=idle)
        remote.vm_stop(idle)
        self._released.update(idle)

    def _emit(self, type: str, **data) -> None:
        if self.emitter is not None:
            self.emitter.emit(type, **data)

    def run(self, spec: SweepSpec) -> ExperimentResult:
        sweep_dir = self.out_root / spec.name
        sweep_dir.mkdir(parents=True, exist_ok=True)
        ctx = RunContext(self.out_root, sweep_dir, spec)
        result = ExperimentResult(name=spec.name)
        self._emit("experiment.started", name=spec.name,
                   est_points=spec.est_points())
        try:
            self._emit("run.phase", phase="deploy", experiment=spec.name)
            self._check_hosts(spec)
            self.adapter.deploy(ctx)
            self._emit("run.phase", phase="sweep", experiment=spec.name)
            searched = SearchedRates(sweep_dir) if spec.search else None
            for trial in range(spec.trial_offset,
                               spec.trial_offset + spec.trials):
                if spec.retry_point is not None \
                        and trial != spec.retry_point["trial"]:
                    continue
                for dims in spec.combos():
                    if spec.retry_point is not None \
                            and dims != spec.retry_point["dims"]:
                        continue
                    self._run_combo(ctx, searched, dims, trial, result)
        except KeyboardInterrupt:
            self._write_results(sweep_dir, result, spec)
            raise
        except Exception as e:
            # A failure outside any point (deploy, the search plumbing) fails
            # the experiment; the points that did run keep their data.
            result.fatal = f"{type(e).__name__}: {e}"
        self._write_results(sweep_dir, result, spec)
        self._emit("experiment.finished", name=spec.name,
                   status=result.status, detail=result.detail())
        return result

    def _write_results(self, sweep_dir: Path, result: ExperimentResult,
                       spec: SweepSpec) -> None:
        # Written before any reporting that could fail, so a multi-hour
        # sweep's measurements survive a formatting bug.
        path = sweep_dir / "sweep_results.json"
        try:
            with open(path) as f:
                existing = json.load(f)
            if not isinstance(existing, list):
                existing = []
        except (OSError, ValueError):
            existing = []

        # A sweep dir may be extended by later --trial-offset invocations.
        # Preserve their predecessors while replacing any point this
        # invocation resumed or retried. Dimensions come from the live spec,
        # so project-specific metric names cannot accidentally become part of
        # point identity.
        dim_names = tuple(spec.dims)

        def identity(entry):
            return (tuple((name, entry.get(name)) for name in dim_names),
                    entry.get("rate"), entry.get("trial"))

        written = result.entries
        if spec.retry_point is not None:
            # A failed replacement is diagnostic state for this invocation,
            # not a replacement for the valid measurement still on disk.
            written = [entry for entry in written if "error" not in entry]
        replacements = {identity(entry) for entry in written}
        merged = [entry for entry in existing
                  if not isinstance(entry, dict)
                  or identity(entry) not in replacements]
        merged.extend(written)
        _write_json(path, merged)

    def _run_combo(self, ctx, searched, dims, trial, result) -> None:
        spec = ctx.spec
        if spec.retry_point is not None:
            self._run_point(ctx, dims, spec.retry_point.get("rate"), trial,
                            result)
            return
        if spec.search is None:
            for rate in (spec.rates or (None,)):
                self._run_point(ctx, dims, rate, trial, result)
            return

        replay = searched.get(dims)
        if replay is not None:
            for rate in replay:
                self._emit("search.decision", dims=dims, rate=rate,
                           action="replay", note="")
                self._run_point(ctx, dims, rate, trial, result)
            return

        searched.start(dims)

        def measure(rate):
            searched.record(dims, rate)
            entry = self._run_point(ctx, dims, rate, trial, result)
            if "error" in entry:
                return None     # "no result": the search treats it as unknown
            return measurement_from_metrics(entry, requested=rate,
                                            duration_secs=spec.duration_secs)

        def on_decision(action, rate, note):
            self._emit("search.decision", dims=dims, rate=rate,
                       action=action, note=note)

        search(measure, start=spec.search.start,
               min_rate=spec.search.min_rate, max_rate=spec.search.max_rate,
               refine_steps=spec.search.refine_steps,
               on_decision=on_decision, saturated_fn=self._saturated)
        searched.finish(dims)

    def _run_point(self, ctx, dims, rate, trial, result) -> dict:
        spec = ctx.spec
        multi = spec.trials > 1 or spec.trial_offset > 0
        rel = point_rel_dir(dims, rate, trial if multi else None)
        point_dir = ctx.sweep_dir / rel
        tags = dict(dims)
        if rate is not None:
            tags["rate"] = rate
        tags["trial"] = trial

        if spec.resume and spec.retry_point is None:
            required = spec.resume_required_keys
            if required is None:
                required = SEARCH_REQUIRED_KEYS if spec.search else ()
            entry = completed_entry(point_dir, tags, required)
            if entry is not None:
                status = "dead" if self._point_dead(entry) else "ok"
                # metrics ride along so a consumer (the daemon's ledger) can
                # record the resumed point without re-reading summary.json.
                self._emit("point.skipped", experiment=spec.name, dims=dims,
                           rate=rate, trial=trial, rel_dir=rel, reason="resume",
                           metrics=entry)
                self._record(result, entry, status, skipped=True)
                return entry

        if self.points_run and spec.pause_secs:
            self._sleep(spec.pause_secs)
        transactional = spec.retry_point is not None
        if transactional:
            point_dir.parent.mkdir(parents=True, exist_ok=True)
            work_dir = Path(tempfile.mkdtemp(
                prefix=f".{point_dir.name}.retry-", dir=point_dir.parent))
        else:
            point_dir.mkdir(parents=True, exist_ok=True)
            work_dir = point_dir
        point = SweepPoint(dims=dict(dims), rate=rate, trial=trial,
                           rel_dir=rel, dir=work_dir)

        attempt = 0
        while True:
            attempt += 1
            self._emit("point.started", experiment=spec.name, dims=dims,
                       rate=rate, trial=trial, rel_dir=rel)
            t0 = time.monotonic()
            metrics = None
            try:
                self._release_hosts(spec, point)
                self.adapter.launch(ctx, point)
                metrics = self.adapter.analyze(ctx, point)
                if not isinstance(metrics, dict):
                    raise TypeError(f"analyze returned {type(metrics).__name__},"
                                    f" expected dict")
                entry = {**metrics, **tags}
                _write_json(work_dir / "summary.json", entry)
                status = "dead" if self._point_dead(entry) else "ok"
            except Exception as e:
                entry = {"error": f"{type(e).__name__}: {e}", **tags}
                status = "error"
            duration = round(time.monotonic() - t0, 3)
            self.points_run += 1
            retry = (status == "error"
                     or (status == "dead" and spec.retry_dead))
            if not retry or attempt >= spec.max_attempts:
                break
            self._emit("point.retrying", attempt=attempt,
                       max_attempts=spec.max_attempts,
                       reason=entry.get("error", "committed nothing"))

        if transactional and status != "error":
            backup = point_dir.with_name(
                f".{point_dir.name}.replaced-{time.time_ns()}")
            had_original = point_dir.exists()
            if had_original:
                point_dir.rename(backup)
            try:
                work_dir.rename(point_dir)
            except Exception:
                if had_original and backup.exists():
                    backup.rename(point_dir)
                raise
            if backup.exists():
                shutil.rmtree(backup, ignore_errors=True)
        elif transactional:
            shutil.rmtree(work_dir, ignore_errors=True)

        event_type = ("point.retry_failed"
                      if transactional and status == "error"
                      else "point.finished")
        self._emit(event_type, experiment=spec.name, dims=dims,
                   rate=rate, trial=trial, rel_dir=rel, status=status,
                   duration_s=duration, metrics=metrics or {})
        self._record(result, entry, status)
        # `error` covers both halves of a failed point: the engine sets it when
        # launch or analysis raised, and an adapter sets it on a run whose
        # numbers describe less than the system under test (a partial
        # committee). Either way the point is not a measurement.
        if spec.abort_on_error and "error" in entry:
            reason = entry.get("error") or entry.get("error_reason") or "failed"
            raise PointFailed(f"{rel}: {reason}")
        return entry

    def _record(self, result, entry, status, skipped=False) -> None:
        result.entries.append(entry)
        if status == "error":
            result.failed += 1
        elif status == "dead":
            result.dead += 1
        else:
            result.ok += 1
        if skipped:
            result.skipped += 1


def run_experiments(adapter: SweepAdapter, specs, out_root, *,
                    run_id=None, argv=None, emitter=None,
                    saturated=None, point_dead=None,
                    release_unused_vms=False) -> int:
    """Run a list of SweepSpecs; returns the process exit code (0/1/2/130).

    Owns the run-level bookkeeping one process is responsible for: the
    events.jsonl emitter, the run.started/finished/interrupted envelope, and
    a provenance line — sweep dirs are filled over several invocations
    (--trial-offset), and trials run under different code aren't repeats.
    """
    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    argv = list(argv if argv is not None else sys.argv)
    run_id = run_id or f"{adapter.name}-{time.strftime('%Y%m%d-%H%M%S')}"
    own_emitter = emitter is None
    if own_emitter:
        emitter = EventEmitter(out_root / "events.jsonl", run_id)
    engine = SweepEngine(adapter, out_root, emitter,
                         saturated=saturated, point_dead=point_dead,
                         release_unused_vms=release_unused_vms)
    emitter.emit("run.started", argv=argv, adapter=adapter.name,
                 experiments=[s.name for s in specs], out_root=str(out_root))
    _append_provenance(out_root, argv, specs)
    results = []
    try:
        for spec in specs:
            results.append(engine.run(spec))
    except KeyboardInterrupt:
        emitter.emit("run.interrupted", signal="SIGINT",
                     points_completed=engine.points_run)
        if own_emitter:
            emitter.close()
        return 130
    if any(r.status == "failed" for r in results):
        exit_code = 1
    elif any(r.status == "degraded" for r in results):
        exit_code = 2
    else:
        exit_code = 0
    emitter.emit("run.finished", exit_code=exit_code,
                 points_total=sum(r.total for r in results),
                 points_ok=sum(r.ok for r in results),
                 points_dead=sum(r.dead for r in results),
                 points_failed=sum(r.failed for r in results))
    if own_emitter:
        emitter.close()
    return exit_code


def _append_provenance(out_root: Path, argv, specs) -> None:
    line = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "argv": argv,
        "experiments": [{"name": s.name, "trials": s.trials,
                         "trial_offset": s.trial_offset} for s in specs],
    }
    with open(out_root / "provenance.jsonl", "a") as f:
        f.write(json.dumps(line) + "\n")


def _write_json(path: Path, data) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, default=str)
        f.write("\n")
    tmp.replace(path)
