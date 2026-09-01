"""kitchend CLI: the daemon and a terminal workflow against it.

    kitchend serve                      run the daemon
    kitchend status                     ping a running daemon
    kitchend catalog PROJECT            list experiments and aggregates
    kitchend submit PROJECT NAME...     queue experiments (native ones use
                                        their cluster; fan-out prints one
                                        line per job)
    kitchend submit-argv PROJECT JSON   queue one exact argv JSON array
    kitchend jobs [--state S] [-n N]    recent jobs
    kitchend watch JOB_ID               follow a job to completion
    kitchend log JOB_ID [-n N]          tail a job's driver output
    kitchend cancel JOB_ID              cancel a waiting/running job
    kitchend resubmit JOB_ID            resubmit into the same run dir
    kitchend retry JOB_ID               stop waiting, try it now
    kitchend restart [--force]          safely restart the user daemon
    kitchend clusters                   cluster states, VMs, burn
    kitchend spend [--days N]           what the clusters cost: probes vs runs
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

from kitchend.config import CONFIG_PATH, load_config


def _api(config, path, body=None, method=None):
    url = f"http://{config.bind_host}:{config.bind_port}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method or ("POST" if body is not None else "GET"),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        try:
            detail = json.load(e).get("detail", str(e))
        except Exception:
            detail = str(e)
        raise SystemExit(f"error: {detail}")
    except (urllib.error.URLError, OSError) as e:
        raise SystemExit(f"daemon not reachable at {url}: {e}")


def _fmt_job_line(j):
    what = j["spec"].get("name") \
        or " ".join(j["spec"].get("experiments") or []) or "job"
    queue = j["spec"].get("queue") or j["project"]
    managed = " ⚙" if j["spec"].get("cluster") else ""
    state = j["outcome"] if j["state"] == "done" else j["state"]
    if j.get("attempts", 0) > 1 and j["state"] != "done":
        state += f" #{j['attempts']}"
    return (f"#{j['id']:<5} {j['project']:<10} {state:<11} "
            f"{queue}{managed}  {what}")


def cmd_catalog(config, args):
    cat = _api(config, f"/api/experiments?project={args.project}")
    if cat.get("error"):
        raise SystemExit(f"no catalog: {cat['error']}")
    names = {e["name"] for e in cat["experiments"]}
    by_queue: dict[str, list] = {}
    variants: dict[str, list] = {}
    orphan_groups: dict[str, dict[str, list]] = {}   # queue -> group -> exps
    for e in cat["experiments"]:
        queue = e.get("queue") or "other"
        group = e.get("group")
        if group and group in names:
            variants.setdefault(group, []).append(e)
        elif group:
            # A family label (e.g. intraregion_latency) that is no
            # experiment itself: fold its members under a plain header.
            orphan_groups.setdefault(queue, {}).setdefault(group, []).append(e)
        else:
            by_queue.setdefault(queue, []).append(e)
    for queue in {**by_queue, **orphan_groups}:
        print(f"[{queue}]")
        for e in by_queue.get(queue, []):
            print(f"  {e['name']}  — {e['description']}")
            for v in variants.get(e["name"], []):
                print(f"      {v['name']}  — {v['description']}")
        for group, members in orphan_groups.get(queue, {}).items():
            print(f"  {group}/")
            for v in members:
                print(f"      {v['name']}  — {v['description']}")
    if cat.get("aggregates"):
        print("[aggregates]")
        for name, members in cat["aggregates"].items():
            print(f"  {name}: {' '.join(members)}")


def cmd_submit(config, args):
    spec = {"project": args.project, "experiments": args.experiments,
            "priority": args.priority, "max_attempts": args.retries}
    if args.cluster:
        spec["cluster"] = args.cluster
    if args.after is not None:
        spec["after"] = args.after
    out = _api(config, "/api/jobs", body=spec)
    ids = out.get("ids", [out["id"]])
    jobs = {j["id"]: j for j in _api(config, "/api/jobs?limit=100")}
    clusters = {c["key"]: c for c in _api(config, "/api/clusters")}
    for jid in ids:
        j = jobs.get(jid)
        if j is None:
            print(f"#{jid} queued")
            continue
        line = _fmt_job_line(j)
        cluster = j["spec"].get("cluster")
        if cluster:
            key = f"{args.project}/{cluster}"
            rate = (clusters.get(key) or {}).get("est_usd_per_hr")
            if rate:
                line += f"  (uses {cluster}: ~${rate:.2f}/hr while running)"
        print(line)
    print(f"{len(ids)} job(s) queued; follow with: kitchend watch {ids[0]}")


def cmd_submit_argv(config, args):
    try:
        command = json.loads(args.argv)
    except ValueError as e:
        raise SystemExit(f"argv must be a JSON array of strings: {e}")
    if not isinstance(command, list) or not command \
            or any(not isinstance(a, str) for a in command):
        raise SystemExit("argv must be a non-empty JSON array of strings")
    spec = {"project": args.project, "command": command,
            "priority": args.priority, "max_attempts": args.retries}
    if args.name:
        spec["name"] = args.name
    if args.cluster:
        spec["cluster"] = args.cluster
    if args.after is not None:
        spec["after"] = args.after
    out = _api(config, "/api/jobs", body=spec)
    print(f"#{out['id']} queued; follow with: kitchend watch {out['id']}")


def cmd_jobs(config, args):
    q = f"/api/jobs?limit={args.n}"
    if args.state:
        q += f"&state={args.state}"
    for j in _api(config, q):
        print(_fmt_job_line(j))


def _progress_line(j):
    p = j.get("progress") or {}
    pts = p.get("points") or {}
    total = (p.get("totals_final") or {}).get("points_total") or p.get("est_points")
    line = f"{j['state']}"
    if pts:
        line += (f"  {pts.get('done', 0)}{f'/{total}' if total else ''} pts"
                 f"  ok {pts.get('ok', 0)} dead {pts.get('dead', 0)}"
                 f" failed {pts.get('failed', 0)}")
    if p.get("eta_secs") is not None:
        line += f"  ~{int(p['eta_secs'] // 60)}m left"
    cur = p.get("current")
    if cur:
        dims = " ".join(f"{k}={v}" for k, v in (cur.get("dims") or {}).items())
        rate = f" rate={cur['rate']:g}" if cur.get("rate") is not None else ""
        line += f"  [{dims}{rate} trial {cur.get('trial')}]"
    return line


def cmd_watch(config, args):
    last = ""
    while True:
        j = _api(config, f"/api/jobs/{args.job_id}")
        line = _progress_line(j)
        if line != last:
            print(f"\r{time.strftime('%H:%M:%S')}  {line}")
            last = line
        if j["state"] == "done":
            tail = _api(config, f"/api/jobs/{args.job_id}/log?tail=15")["log"]
            if tail:
                print("--- last output ---")
                print(tail)
            print(f"finished: {j['outcome']} after "
                  f"{j['attempts']} attempt(s)"
                  + (f" — {j['last_error']}" if j.get("last_error") else ""))
            return {"ok": 0, "degraded": 2}.get(j["outcome"], 1)
        time.sleep(args.interval)


def cmd_log(config, args):
    print(_api(config, f"/api/jobs/{args.job_id}/log?tail={args.n}")["log"])


def cmd_cancel(config, args):
    _api(config, f"/api/jobs/{args.job_id}/cancel", body={})
    print(f"#{args.job_id} canceled")


def cmd_retry(config, args):
    _api(config, f"/api/jobs/{args.job_id}/retry", body={})
    print(f"#{args.job_id} due now")


def _restart_blockers(config):
    """Jobs whose work or cluster lease a restart would interrupt."""
    running = _api(config, "/api/jobs?state=running&limit=1000")
    waiting = _api(config, "/api/jobs?state=waiting&limit=1000")
    return running + [j for j in waiting if j.get("bringing_up")]


def cmd_restart(config, args):
    """Restart through the guarded operator path.

    A job waiting for cluster bring-up already owns the scheduler slot and
    capacity attempt, so it is just as active as a spawned driver here.
    """
    if not args.force:
        blockers = _restart_blockers(config)
        if blockers:
            details = ", ".join(
                f"#{j['id']} ({j.get('display_state') or j['state']})"
                for j in blockers)
            raise SystemExit(
                f"refusing restart: active job(s) {details}; "
                "wait for completion or pass --force")
    subprocess.run(
        ["systemctl", "--user", "restart", "kitchend.service"], check=True)
    print("kitchend.service restarted")


def cmd_resubmit(config, args):
    out = _api(config, f"/api/jobs/{args.job_id}/resubmit",
               body={"resume": not args.fresh})
    print(f"resubmitted as #{out.get('id', out)}")


def cmd_purge(config, args):
    body = {"outcomes": args.outcomes}
    if args.project:
        body["project"] = args.project
    out = _api(config, "/api/jobs/purge", body=body)
    print(f"purged {len(out['purged'])} job(s): {out['purged']}")


def cmd_spend(config, args):
    out = _api(config, f"/api/spend?days={args.days}")
    print(f"{'':<26}{'spans':>7}{'VM-min':>10}{'cost':>10}")
    for label, key in (("probes (nothing ran)", "probes"), ("runs", "runs")):
        b = out[key]
        print(f"{label:<26}{b['n']:>7}{b['vm_minutes']:>10.0f}"
              f"{'$' + format(b['usd'], '.2f'):>10}")
    for title, section in (("by cluster", "by_cluster"), ("by day", "by_day"),
                           ("by job", "by_job")):
        rows = sorted(out[section].items(), key=lambda kv: -kv[1]["usd"])[:12]
        if not rows:
            continue
        print(f"\n{title}")
        for name, b in rows:
            print(f"  {name:<24}{b['n']:>7}{b['vm_minutes']:>10.0f}"
                  f"{'$' + format(b['usd'], '.2f'):>10}")
    print(f"\ntotal ${out['total_usd']:.2f}")


def cmd_clusters(config, args):
    for c in _api(config, "/api/clusters"):
        up = c.get("vms_running")
        vms = f"{up}/{c['vm_count']}" if up is not None and c.get("vm_count") \
            else (c.get("vm_count") or "?")
        line = f"{c['key']:<24} {c['state']:<11} vms {vms}"
        if c.get("burn_usd_per_hr"):
            line += f"  ${c['burn_usd_per_hr']:.2f}/hr"
        elif c.get("est_usd_per_hr"):
            line += f"  (${c['est_usd_per_hr']:.2f}/hr when up)"
        if c.get("used_by"):
            line += f"  in use: {c['used_by']}"
        elif c.get("kept_for_next"):
            line += "  kept for the next job"
        print(line)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="kitchend", description=__doc__)
    parser.add_argument("--config", default=None,
                        help=f"config path (default {CONFIG_PATH})")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve", help="run the daemon")
    sub.add_parser("status", help="ping a running daemon")

    p = sub.add_parser("catalog", help="list a project's experiments")
    p.add_argument("project")

    p = sub.add_parser("submit", help="queue catalog experiments")
    p.add_argument("project")
    p.add_argument("experiments", nargs="+")
    p.add_argument("--cluster", help="daemon-managed lease on this cluster")
    p.add_argument("--priority", type=int, default=0)
    p.add_argument("--attempts", type=int, default=20, dest="retries",
                   help="driver invocations before giving up")
    p.add_argument("--after", type=int, default=None,
                   help="queue directly behind this job "
                        "(default: behind the last job on the same cluster)")

    p = sub.add_parser("submit-argv", help="queue one exact argv JSON array")
    p.add_argument("project")
    p.add_argument("argv", help='JSON array, e.g. ["python3","run.py"]')
    p.add_argument("--name")
    p.add_argument("--cluster", help="daemon-managed lease on this cluster")
    p.add_argument("--priority", type=int, default=0)
    p.add_argument("--attempts", type=int, default=20, dest="retries")
    p.add_argument("--after", type=int, default=None)

    p = sub.add_parser("jobs", help="recent jobs")
    p.add_argument("--state")
    p.add_argument("-n", type=int, default=20)

    p = sub.add_parser("watch", help="follow a job to completion")
    p.add_argument("job_id", type=int)
    p.add_argument("--interval", type=float, default=3.0)

    p = sub.add_parser("log", help="tail a job's driver output")
    p.add_argument("job_id", type=int)
    p.add_argument("-n", type=int, default=100)

    p = sub.add_parser("cancel", help="cancel a queued/running job")
    p.add_argument("job_id", type=int)

    p = sub.add_parser("retry", help="try a waiting job now: drop its delay "
                       "and any cluster cooldown holding it")
    p.add_argument("job_id", type=int)

    p = sub.add_parser("restart", help="restart the user daemon; refuses while "
                       "a job is running or bringing up a cluster")
    p.add_argument("--force", action="store_true",
                   help="restart even if active work will be interrupted")

    p = sub.add_parser("resubmit", help="resubmit a finished job")
    p.add_argument("job_id", type=int)
    p.add_argument("--fresh", action="store_true",
                   help="new run dir instead of resuming")

    sub.add_parser("clusters", help="cluster states, VMs, burn")

    p = sub.add_parser("spend", help="cluster time and cost, probes apart "
                       "from runs (from the event log; GCP billing is not "
                       "readable from here)")
    p.add_argument("--days", type=int, default=7)

    p = sub.add_parser("purge", help="delete job rows that produced no data "
                       "(files are never touched; delete runs for that)")
    p.add_argument("--project")
    p.add_argument("--outcomes", nargs="+", default=["failed", "canceled"])

    args = parser.parse_args(argv)

    from pathlib import Path
    config = load_config(Path(args.config) if args.config else None)

    if args.cmd == "serve":
        import uvicorn

        from kitchend.api.app import create_app
        # Open SSE streams and MCP sessions never close on their own, and
        # uvicorn's default graceful shutdown waits for them indefinitely —
        # a `systemctl restart` then hangs 90s until systemd SIGKILLs the
        # daemon (and every proxied browser stream sees a non-200 meanwhile).
        # Five seconds is grace enough for real in-flight requests.
        uvicorn.run(create_app(config), host=config.bind_host,
                    port=config.bind_port, timeout_graceful_shutdown=5)
        return 0
    if args.cmd == "status":
        print(json.dumps(_api(config, "/api/health"), indent=2))
        return 0
    handler = {
        "catalog": cmd_catalog, "submit": cmd_submit,
        "submit-argv": cmd_submit_argv, "jobs": cmd_jobs,
        "watch": cmd_watch, "log": cmd_log, "cancel": cmd_cancel,
        "resubmit": cmd_resubmit, "retry": cmd_retry,
        "restart": cmd_restart,
        "clusters": cmd_clusters,
        "spend": cmd_spend,
        "purge": cmd_purge,
    }[args.cmd]
    try:
        return handler(config, args) or 0
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
