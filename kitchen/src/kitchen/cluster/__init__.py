"""Cluster lifecycle: start/stop with the dead-man switch, clock sync,
keep-alive heartbeat, and file-based leases.

The safety model, unchanged from the scripts this was extracted from: every
running VM carries a GCE-side `shutdown -h +N` timer, re-armed periodically by
exactly one keep-alive actor. If nothing re-arms — daemon dead, laptop asleep,
process SIGKILLed — the VMs power themselves off within N minutes. The
fail-safe lives on the VMs, not in any local process.
"""

from .clocks import parse_chronyc_offset, sync_clocks
from .keepalive import AlreadyRunning, KeepAlive
from .lifecycle import (
    arm_shutdown,
    arm_shutdown_cmd,
    start_vms,
    stop_vms,
    wait_drained,
)
from .state import ClusterState, Lease, LeaseInfo

__all__ = [
    "parse_chronyc_offset", "sync_clocks",
    "KeepAlive", "AlreadyRunning",
    "arm_shutdown", "arm_shutdown_cmd", "start_vms", "stop_vms", "wait_drained",
    "ClusterState", "Lease", "LeaseInfo",
]
