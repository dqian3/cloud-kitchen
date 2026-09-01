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


def _why_unarmed(results, vms) -> str:
    """Why the arm did not land. run_on_all keeps the exception it caught per
    host; reporting only the host names left the reason unread, so a VM that
    was still booting and one with a broken key read identically."""
    for vm in vms:
        out = results.get(vm)
        if isinstance(out, Exception):
            lines = [ln.strip() for ln in str(out).splitlines() if ln.strip()]
            if lines:
                return f"{vm}: {lines[-1][:200]}"
    return "no output from any of them"


def arm_shutdown(remote, vms, minutes=60, cancel_first=True, timeout=120):
    """Arm (or re-arm) the auto-shutdown timer on all VMs. A no-op for
    backends whose hosts can't shut themselves down (containers).

    Waiting for a just-started VM's sshd is `remote.ssh`'s job, not this
    one's: a refused connection is retried there long enough to cover a cold
    boot. If it still cannot connect, the VM is not coming up and the caller
    stops it.
    """
    if not getattr(remote, "supports_deadman", True):
        return {}
    results = remote.run_on_all(
        vms, arm_shutdown_cmd(minutes, cancel_first), quiet=True, timeout=timeout,
    )
    failed = unarmed(results)
    if failed:
        error = RuntimeError(
            f"could not arm shutdown timer on {len(failed)} VM(s): "
            f"{', '.join(failed)} ({_why_unarmed(results, failed)})")
        error.failed_vms = failed
        raise error
    return results


def unarmed(results) -> list:
    """The VMs whose dead-man command did not land, from an arm_shutdown
    result. run_on_all reports a failure as the exception it caught, so a
    caller that ignores the return value cannot tell armed from unarmed."""
    return sorted(vm for vm, out in (results or {}).items()
                  if isinstance(out, Exception))


# Already up, or on its way up: a bring-up adopts these rather than starting
# them again. STAGING/PROVISIONING count because a VM asked to start is in
# one of them for a while before it is RUNNING, and a second start there is
# a fingerprint error, not a no-op.
ADOPTABLE_STATES = ("RUNNING", "STAGING", "PROVISIONING")

# States in which a VM is not consuming anything and needs no cleanup.
# Everything else -- RUNNING, STAGING, PROVISIONING, REPAIRING -- is either
# billing already or about to, so it must be stopped or armed.
DOWN_STATES = ("TERMINATED", "STOPPING", "SUSPENDED", "SUSPENDING")


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
    # Take over whatever is already up or on its way up, rather than
    # restarting it or treating it as foreign. A VM someone started by hand
    # is usable as-is: re-issuing start on a RUNNING or STAGING VM is at best
    # wasted and at worst an error, and stopping it would fight the person
    # who started it.
    to_start = [v for v in vms if statuses.get(v) not in ADOPTABLE_STATES]
    try:
        if to_start:
            remote.vm_start(to_start)
    except Exception:
        post = remote.vm_status(vms)
        # Anything not plainly down is treated as up. A VM asked to start is
        # STAGING for a while before it is RUNNING, and testing for RUNNING
        # here missed exactly that window: the VM was neither stopped nor
        # armed, finished booting a moment later, and ran unmanaged with no
        # dead-man timer -- twelve did, for four hours, while the daemon
        # reported them as failed to start.
        # When this brings VMs down it brings all of them down. Stopping only
        # what this call started kept leaving strays: VMs a probe started, and
        # VMs already up before the call. A cluster that failed to start is
        # not usable half-up, so there is nothing worth preserving.
        started = [v for v in vms
                   if post.get(v, "TERMINATED") not in DOWN_STATES]
        if started and stop_on_partial:
            # A stop that fails leaves a VM running with nobody responsible
            # for it, so anything that survives cleanup gets the dead-man
            # switch instead: it powers itself off within the hour rather
            # than running until someone notices.
            left = remote.vm_stop(started) or []
            if left:
                print(f"  WARNING: could not stop {len(left)}: "
                      f"{', '.join(left)} -- arming their shutdown timer")
                arm_shutdown(remote, left, minutes=deadman_minutes)
        elif started:
            arm_shutdown(remote, started, minutes=deadman_minutes)
        raise
    # Fresh boots have no pending timer, but re-arming unconditionally is
    # harmless and covers the VMs that were already running. This is the arm
    # that races the boot: the VMs were asked to start a moment ago, so give
    # sshd time to come up rather than reading a closed connection as a fault.
    try:
        arm_shutdown(remote, vms, minutes=deadman_minutes)
    except Exception:
        # A running VM without a shutdown deadline is an unbounded bill.
        remote.vm_stop(vms)
        raise
    return to_start


def stop_vms(remote, vms):
    """Stop VMs and return any that the backend could not stop."""
    if not vms:
        return
    return remote.vm_stop(vms) or []
