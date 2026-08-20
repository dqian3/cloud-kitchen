"""Toy protocol: the SweepEngine's reference adapter and test harness.

A deterministic fake pipeline over the Remote interface (normally
LocalRemote): deploy() uploads a small Python "client" to every host,
launch() runs it everywhere in parallel with the point's per-client offered
rate, analyze() downloads each host's stats file and sums them into the
standard metric names. Each client delivers min(offered, capacity), so the
whole system genuinely saturates at `capacity` msgs/sec, and rates at or
above `dead_above` commit nothing — enough behaviour to exercise every
branch of the rate search, resume, and retry without a cloud bill.

Also runnable as a process (`python -m kitchen.run.toy --output-dir ...`),
which is how the daemon gets a native, event-emitting executor to point its
ingest at before the real projects migrate.
"""

import argparse
import json
import signal
import sys
import tempfile
from pathlib import Path

from kitchen.remote.local import LocalRemote

from .engine import run_experiments
from .spec import RateSearchSpec, SweepSpec

CLIENT_SCRIPT = """\
import json, sys, time
offered, capacity, duration, dead = (float(a) for a in sys.argv[1:5])
time.sleep(duration)
delivered = 0.0 if dead else min(offered, capacity)
with open("stats.json", "w") as f:
    json.dump({"offered": offered, "delivered": delivered,
               "completed": int(delivered * duration),
               "window": duration}, f)
"""


class ToyAdapter:
    name = "toy"

    def __init__(self, remote, hosts, *, capacity=8000.0, dead_above=None,
                 duration_secs=0.05, default_rate=1000.0):
        self.remote = remote
        self.hosts = list(hosts)
        self.capacity = float(capacity)
        self.dead_above = dead_above
        self.duration_secs = float(duration_secs)
        self.default_rate = float(default_rate)

    def deploy(self, ctx):
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
            f.write(CLIENT_SCRIPT)
            script = f.name
        try:
            for host in self.hosts:
                self.remote.scp_upload(script, host, "toy_client.py")
        finally:
            Path(script).unlink(missing_ok=True)

    def launch(self, ctx, point):
        rate = point.rate if point.rate is not None else self.default_rate
        per_client = rate / len(self.hosts)
        cap_per_client = self.capacity / len(self.hosts)
        dead = int(self.dead_above is not None and rate >= self.dead_above)
        cmd = (f"python3 toy_client.py {per_client} {cap_per_client} "
               f"{self.duration_secs} {dead}")
        results = self.remote.run_on_all(self.hosts, cmd, quiet=True)
        failures = {h: r for h, r in results.items() if isinstance(r, Exception)}
        if failures:
            host, err = next(iter(failures.items()))
            raise RuntimeError(f"toy client failed on {len(failures)} host(s), "
                               f"first: {host}: {err}")

    def analyze(self, ctx, point):
        offered = delivered = 0.0
        completed = 0
        window = None
        for host in self.hosts:
            local = point.dir / f"stats_{host}.json"
            self.remote.scp_download(host, "stats.json", str(local))
            with open(local) as f:
                stats = json.load(f)
            offered += stats["offered"]
            delivered += stats["delivered"]
            completed += stats["completed"]
            window = stats["window"]
        return {
            "num_clients": len(self.hosts),
            "total_completed": completed,
            "throughput_msgs_per_sec": delivered,
            "offered_rate": offered,
            "delivered_rate": delivered,
            "stats_window_secs": window,
            "measurement_secs": window,
            "cap_wait_frac": 0.0,
            "drop_pct": 0.0,
        }


def _parse_dim(text: str):
    """NAME=v1,v2,... with values coerced to int/float when they parse."""
    name, _, raw = text.partition("=")
    if not raw:
        raise argparse.ArgumentTypeError(f"expected NAME=v1,v2 got {text!r}")

    def coerce(v):
        for cast in (int, float):
            try:
                return cast(v)
            except ValueError:
                continue
        return v

    return name, tuple(coerce(v) for v in raw.split(","))


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="toy-protocol sweep")
    p.add_argument("--output-dir", required=True)
    p.add_argument("--name", default="toy")
    p.add_argument("--hosts", type=int, default=2)
    p.add_argument("--capacity", type=float, default=8000.0)
    p.add_argument("--dead-above", type=float, default=None)
    p.add_argument("--duration-secs", type=float, default=0.05)
    p.add_argument("--trials", type=int, default=1)
    p.add_argument("--trial-offset", type=int, default=0)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--max-attempts", type=int, default=1)
    p.add_argument("--pause-secs", type=float, default=0.0)
    p.add_argument("--dim", action="append", type=_parse_dim, default=[],
                   metavar="NAME=V1,V2")
    p.add_argument("--rates", type=float, nargs="+", default=None)
    p.add_argument("--rate-search", action="store_true")
    p.add_argument("--rate-search-start", type=float, default=1000.0)
    p.add_argument("--rate-search-min", type=float, default=100.0)
    p.add_argument("--rate-search-max", type=float, default=200000.0)
    p.add_argument("--refine-steps", type=int, default=3)
    args = p.parse_args(argv)

    out_root = Path(args.output_dir)
    spec = SweepSpec(
        name=args.name,
        dims=dict(args.dim),
        rates=tuple(args.rates or ()),
        search=(RateSearchSpec(start=args.rate_search_start,
                               min_rate=args.rate_search_min,
                               max_rate=args.rate_search_max,
                               refine_steps=args.refine_steps)
                if args.rate_search else None),
        trials=args.trials,
        trial_offset=args.trial_offset,
        duration_secs=args.duration_secs,
        resume=args.resume,
        max_attempts=args.max_attempts,
        pause_secs=args.pause_secs,
    )
    hosts = [f"host{i:02d}" for i in range(args.hosts)]
    remote = LocalRemote(root=out_root / ".hosts")
    adapter = ToyAdapter(remote, hosts, capacity=args.capacity,
                         dead_above=args.dead_above,
                         duration_secs=args.duration_secs)
    # The daemon cancels with SIGINT then SIGTERM; both should leave the same
    # clean interrupt trail in events.jsonl.
    def _sigterm(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, _sigterm)
    return run_experiments(adapter, [spec], out_root)


if __name__ == "__main__":
    sys.exit(main())
