# cloud-kitchen

A job queue and hosted UI for running distributed-systems experiments on
gcloud, plus the shared orchestration library it is built on.

- **`kitchen/`** — the library: remote execution backends (gcloud / ssh /
  docker / local / mock), cluster start and stop with a self-shutdown timer
  on every VM, structured JSONL events, the sweep engine, and the
  project-adapter interface.
- **`kitchend/`** — the daemon: FastAPI + SQLite job queue, cluster manager,
  live progress, a ledger of finished runs, MCP server. Runs as a systemd
  user unit on the workstation.
- **`ui/`** — Vite + React dashboard for the queue, clusters and runs.

The daemon knows nothing about any one project's protocol. It knows
clusters, command lines, and the event format the sweep engine writes.

## Install

Needs [uv](https://docs.astral.sh/uv/), Node (to build the dashboard), and an
authenticated `gcloud` for any cluster on GCP.

```bash
git clone git@github.com:dqian3/cloud-kitchen.git ~/Projects/cloud-kitchen
cd ~/Projects/cloud-kitchen
(cd ui && npm install && npm run build)        # dashboard, served from ui/dist
```

Write `~/.cloud-kitchen/config.toml` (next section), then start the daemon:

```bash
uv run --project kitchend kitchend serve       # foreground, on 127.0.0.1:8321
```

To keep it running, install the user service; the commands are in the header
of `systemd/kitchend.service`. The daemon binds to loopback only; reach the
dashboard from other machines with `tailscale serve --bg 8321`.

## Add a project

One `[[projects]]` entry in `~/.cloud-kitchen/config.toml` per repo:

```toml
[[projects]]
name = "myproj"
repo_path = "~/Projects/myproj"
driver_cwd = "bench"                     # where job commands run, under repo_path
runs_roots = ["data/runs"]               # where run directories land
adapter_path = "~/Projects/myproj/bench/kitchen_adapter.py"   # optional
gcp_project = "my-gcp-project"
tunnel_through_iap = true                # VMs without external addresses
publish_root = "data/figures"            # optional; served at /pub/myproj/

  [[projects.clusters]]
  name = "main"
  config = "bench/clusters/main.yaml"    # under repo_path
  hourly_usd = 0.73                      # per VM, for the cost meter
  # create_cmd = ["python3", "create_cluster.py"]   # optional, see below
```

A cluster YAML names its backend with `platform:` (`gcloud` by default, or
`docker`, `ssh`, `local`) and lists its VMs in one of these forms:

```yaml
vms: [vm-a, vm-b]                        # a plain list

replica: {vms: [r0, r1, r2, r3]}         # or role groups
client:  {prefix: client, count: 4}      # client00..client03
```

A role group may also give `prefix` with `regions:` (one VM per region). With
no `create_cmd` the daemon only starts and stops VMs that already exist; with
one, it runs that command in `driver_cwd` to create missing VMs first.

Run `kitchend restart` after editing the config.

## What a job is

A job is one command line, run in `driver_cwd`. The daemon:

1. brings the job's cluster up, and keeps re-arming the shutdown timer on its
   VMs while the job runs;
2. runs the command with `--output-dir <dir>` appended (and `--resume` on a
   later attempt);
3. reads the exit code: **0** done, **2** finished but some points produced
   no data (kept, not retried), **anything else** retried into the same
   directory, up to 20 attempts;
4. hands the cluster to the next job queued on it, or stops it.

Jobs on the same cluster run one at a time, in queue order. If the daemon
dies, nothing re-arms the timers and the VMs power themselves off within the
hour.

So the minimum a script needs is: accept `--output-dir` and `--resume`, and
follow those exit codes. Queue it with:

```bash
kitchend submit-argv myproj '["python3","run.py","--rates","1000"]' --cluster main
```

The flag names can be changed per project (`output_dir_flag`, `resume_flag`).

## Named experiments and live progress

Two optional steps give a project a catalog in the dashboard, per-point
progress, and its results in the run ledger.

**An adapter.** `kitchen_adapter.py` exports `get_adapter()`, returning an
object with a `name`, `experiments()` and `aggregates()`
(`kitchen/src/kitchen/adapter.py` has the interface). Each experiment is a
name, the cluster it runs on (`queue`), and its command line (`command`);
an aggregate is a named list of experiments. Then:

```bash
kitchend catalog myproj
kitchend submit myproj baseline
```

**The sweep engine.** Install the library into the interpreter your scripts
use (`pip install -e ~/Projects/cloud-kitchen/kitchen`) and implement three
steps for your system:

```python
from kitchen.run import SweepSpec, run_experiments

class MyAdapter:
    name = "myproj"
    def deploy(self, ctx): ...            # once: upload binaries, sync clocks
    def launch(self, ctx, point): ...     # run one point, leave logs in point.dir
    def analyze(self, ctx, point): ...    # return a metrics dict for the point

spec = SweepSpec(name="baseline", dims={"payload": (16, 1024)},
                 rates=(1000, 2000, 4000))
sys.exit(run_experiments(MyAdapter(), [spec], args.output_dir))
```

The engine owns the loop over trials, dimensions and rates, the directory
layout, resume, per-point retry, the search for the saturation rate
(`SweepSpec.search`), the exit code, and an `events.jsonl` stream in the
output directory. The daemon reads that stream to show progress and to
record each point's metrics. Use the standard metric names listed on
`SweepAdapter` in `kitchen/src/kitchen/run/engine.py` so the engine can tell
a saturated or dead point. `kitchen.remote.load_remote(config)`, given the
parsed cluster YAML, returns the ssh/scp handle for the cluster.

The toy project is a complete example that needs no cloud:
`kitchen/src/kitchen/run/toy.py` is a sweep on the engine and
`kitchen/src/kitchen/run/kitchen_adapter.py` its adapter.

```bash
uv run --project kitchen python -m kitchen.run.toy --output-dir /tmp/toy --rate-search
```

## Day to day

```bash
kitchend jobs                         # recent jobs
kitchend watch JOB_ID                 # follow one to completion
kitchend log JOB_ID                   # tail its output
kitchend cancel JOB_ID
kitchend resubmit JOB_ID [--fresh]    # same directory, or a new one
kitchend hold myproj main 2h          # keep a cluster up after its queue
kitchend clusters                     # cluster states and cost rate
kitchend runs --project myproj        # finished runs in the ledger
kitchend spend --days 7
kitchend restart                      # refuses while a job is running
```

`kitchend restart` is the way to restart the installed service: it refuses
while a job is running or bringing up a cluster unless given `--force`.
`systemctl --user restart kitchend.service` bypasses that check.

In the dashboard, a finished run shows its per-point metrics, tags and
notes, and can queue more trials or a retry of one point into the same
directory. Analysis and plotting stay in the project's own scripts over the
run directories; `publish_root` is for serving what they produce.

## Development

```bash
uv run --project kitchen --with pytest pytest kitchen/tests
uv run --project kitchend --extra dev pytest kitchend/tests
(cd ui && npm run dev)                # dashboard with hot reload
```

State (SQLite database, job logs, cluster locks) lives under
`~/.cloud-kitchen/`; `KITCHEN_STATE_DIR` moves it.
