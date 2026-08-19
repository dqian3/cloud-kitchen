"""The keep-alive heartbeat: the single actor that re-arms the dead-man switch.

Canonical semantics (formerly aspen's `bench.py vm-keep-alive`): every VM
carries a `shutdown -h +60` timer; this loop cancels and re-arms it every
30 minutes. SIGINT and SIGTERM both stop the loop and (by default) stop the
VMs; any harder death simply stops the re-arming, and the VMs power off
within the dead-man window.

Exactly one keep-alive may run per cluster, enforced with a flock on the
cluster's state dir — a second one exits with AlreadyRunning instead of
silently double-arming.
"""

import fcntl
import os
import signal
import time

from .lifecycle import arm_shutdown


class AlreadyRunning(RuntimeError):
    """Another keep-alive holds this cluster's lock."""


def _interval_desc(interval_s):
    if interval_s % 3600 == 0:
        n = interval_s // 3600
        return f"{n} h"
    if interval_s % 60 == 0:
        return f"{interval_s // 60} min"
    return f"{interval_s} s"


class KeepAlive:
    def __init__(self, remote, vms, state=None, deadman_minutes=60,
                 interval_s=30 * 60, restart_fallen=False, stop_on_exit=True):
        """
        state: ClusterState for the single-actor lock (None = no lock, e.g. tests).
        restart_fallen: also restart VMs that dropped out of RUNNING each tick.
            Off by default — a VM that died mid-experiment usually means the
            experiment is already broken, and silently reviving it masks that.
        stop_on_exit: stop the VMs when the loop is interrupted (SIGINT/SIGTERM).
        """
        self.remote = remote
        self.vms = list(vms)
        self.state = state
        self.deadman_minutes = deadman_minutes
        self.interval_s = interval_s
        self.restart_fallen = restart_fallen
        self.stop_on_exit = stop_on_exit
        self._lock_fd = None

    # --- single-actor lock ---

    def acquire_lock(self):
        if self.state is None:
            return
        path = self.state.keepalive_lock_path
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            holder = ""
            try:
                with open(path) as f:
                    holder = f.read().strip()
            except OSError:
                pass
            raise AlreadyRunning(
                f"another keep-alive is already running for cluster "
                f"'{self.state.name}'" + (f" ({holder})" if holder else "")
            )
        os.ftruncate(fd, 0)
        os.write(fd, f"pid {os.getpid()}\n".encode())
        self._lock_fd = fd

    def release_lock(self):
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                os.close(self._lock_fd)
            except OSError:
                pass
            self._lock_fd = None

    # --- the loop ---

    def rearm(self):
        arm_shutdown(self.remote, self.vms, minutes=self.deadman_minutes,
                     cancel_first=True)

    def run(self):
        """Re-arm forever; on SIGINT/SIGTERM stop (and stop VMs if configured).

        The caller is responsible for the VMs being up and armed once already
        (start_vms does both); the first re-arm happens after the first
        interval.
        """
        self.acquire_lock()

        # SIGTERM behaves like Ctrl-C rather than killing without cleanup:
        # a supervisor's default kill used to leave the VMs to the dead-man
        # switch (up to an hour of paid idle) instead of stopping them.
        def _on_term(signum, frame):
            raise KeyboardInterrupt

        prev_term = signal.signal(signal.SIGTERM, _on_term)
        desc = _interval_desc(self.interval_s)
        try:
            print(f"Keeping alive {len(self.vms)} VMs. Heartbeat every {desc}. "
                  f"Ctrl-C to stop.")
            while True:
                time.sleep(self.interval_s)
                print(f"[{time.strftime('%H:%M:%S')}] Resetting shutdown timer "
                      f"on {len(self.vms)} VMs...")
                if self.restart_fallen:
                    statuses = self.remote.vm_status(self.vms)
                    fallen = [v for v in self.vms if statuses.get(v) != "RUNNING"]
                    if fallen:
                        print(f"  Restarting {len(fallen)} stopped VM(s): {fallen}")
                        self.remote.vm_start(fallen)
                self.rearm()
                print(f"[{time.strftime('%H:%M:%S')}] Done. Next heartbeat in {desc}.")
        except KeyboardInterrupt:
            if self.stop_on_exit:
                print("\nStopping VMs...")
                self.remote.vm_stop(self.vms)
                print("All VMs stopped.")
            else:
                print("\nKeep-alive stopped; VMs left to the dead-man switch "
                      f"(off within {self.deadman_minutes} min).")
        finally:
            signal.signal(signal.SIGTERM, prev_term)
            self.release_lock()
