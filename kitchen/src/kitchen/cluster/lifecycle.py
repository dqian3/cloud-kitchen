"""VM lifecycle: start with the dead-man switch armed, drain-before-start, stop.

The dead-man switch: every started VM schedules `sudo shutdown -h +N` so that
if every local process dies, the VMs power off within N minutes. Re-arming is
the keep-alive's job (see keepalive.py).
"""

import time


def arm_shutdown_cmd(minutes=60, cancel_first=True):
    """The canonical dead-man command: (optionally) cancel any pending timer,
    then schedule a fresh one, detached so the ssh session can return."""
    cancel = "sudo shutdown -c 2>/dev/null; " if cancel_first else ""
    return f"{cancel}nohup sudo shutdown -h +{minutes} >/dev/null 2>&1 &"


def arm_shutdown(remote, vms, minutes=60, cancel_first=True, timeout=120):
    """Arm (or re-arm) the auto-shutdown timer on all VMs. A no-op for
    backends whose hosts can't shut themselves down (containers)."""
    if not getattr(remote, "supports_deadman", True):
        return {}
    return remote.run_on_all(
        vms, arm_shutdown_cmd(minutes, cancel_first), quiet=True, timeout=timeout,
    )


def wait_drained(remote, vms, timeout_s=600, poll_s=10):
    """Block until no VM is mid-shutdown (STOPPING/SUSPENDING).

    A `vm-start` issued while VMs are STOPPING fails with gcloud's "resource
    fingerprint changed"; the only safe move is to wait for TERMINATED and
    then start. Raises RuntimeError on timeout.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        statuses = remote.vm_status(vms)
        draining = [v for v in vms if statuses.get(v) in ("STOPPING", "SUSPENDING")]
        if not draining:
            return statuses
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"timed out after {timeout_s}s waiting for VMs to finish stopping: {draining}"
            )
        print(f"  waiting for {len(draining)} VM(s) to reach TERMINATED "
              f"before starting: {draining}")
        time.sleep(poll_s)


def start_vms(remote, vms, deadman_minutes=60, drain_first=True,
              drain_timeout_s=600, stop_on_partial=False):
    """Start VMs and arm the dead-man switch. Returns the VMs it started.

    With drain_first (default), a start that races a concurrent stop waits for
    the VMs to reach TERMINATED instead of failing on gcloud's fingerprint
    error. Statuses are fetched anyway for the drain check, so already-RUNNING
    VMs are skipped rather than re-started.

    On a partial start (a zone stockout fails one VM after its siblings came
    up), the started subset must not keep running unattended at cost: it is
    stopped again when stop_on_partial, and armed with the dead-man switch
    otherwise, before the error propagates. VMs that were already RUNNING
    before this call are never touched — they may belong to a driver-managed
    run.
    """
    if not vms:
        return []
    if drain_first:
        statuses = wait_drained(remote, vms, timeout_s=drain_timeout_s)
    else:
        statuses = remote.vm_status(vms)
    to_start = [v for v in vms if statuses.get(v) != "RUNNING"]
    try:
        if to_start:
            remote.vm_start(to_start)
    except Exception:
        post = remote.vm_status(vms)
        started = [v for v in to_start if post.get(v) == "RUNNING"]
        if started and stop_on_partial:
            remote.vm_stop(started)
        elif started:
            arm_shutdown(remote, started, minutes=deadman_minutes)
        raise
    # Fresh boots have no pending timer, but re-arming unconditionally is
    # harmless and covers the VMs that were already running.
    arm_shutdown(remote, vms, minutes=deadman_minutes)
    return to_start


def stop_vms(remote, vms, state=None, force=False):
    """Stop VMs. With a ClusterState, refuse while live leases exist unless forced."""
    if not vms:
        return
    if state is not None and not force:
        live = state.live_leases()
        if live:
            holders = ", ".join(f"{l.purpose} (pid {l.pid})" for l in live)
            raise RuntimeError(
                f"cluster '{state.name}' has live leases: {holders}. "
                "Release them or stop with force=True."
            )
    remote.vm_stop(vms)
