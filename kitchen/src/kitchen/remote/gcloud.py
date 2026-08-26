"""GCloud backend: hosts are VM instance names, transport is gcloud compute ssh/scp."""

import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from .base import Remote
from .settings import RemoteSettings, get_default_settings


class GCloudRemote(Remote):
    """Uses gcloud compute ssh/scp. Hosts are VM instance names.

    Automatically discovers the zone for each VM via `gcloud compute instances list`.
    A default zone can be provided as fallback.
    """

    def __init__(self, zone=None, project=None, tunnel_through_iap=False,
                 settings: RemoteSettings | None = None):
        self.default_zone = zone
        self.project = project
        # Route ssh/scp through IAP instead of a public address. Needed in any
        # project whose org policy sets constraints/compute.vmExternalIpAccess,
        # where VMs have no external IP at all and a direct connection has
        # nothing to connect to. Off by default: where public addresses exist,
        # IAP only adds a tunnel handshake and lower throughput.
        #
        # Only ssh/scp need it. The protocol itself already runs over the
        # internal addresses from get_all_ips(), which are reachable either way.
        self.tunnel_through_iap = tunnel_through_iap
        # Retry knobs and other site defaults. Falls back to the process-wide
        # defaults so consumer shims that call set_default_settings() cover
        # direct constructions too.
        self.settings = settings or get_default_settings()
        self._zone_cache = {}  # vm_name -> zone

    def _resolve_zone(self, vm_name):
        """Look up the zone for a VM, using cache or gcloud discovery."""
        # Same empty-filter trap as vm_status: a blank name builds
        # `--filter=name=`, which matches every instance, so we would cache
        # some unrelated VM's zone and then ssh into the wrong region.
        if not vm_name:
            raise RuntimeError("_resolve_zone called with an empty VM name")
        if vm_name in self._zone_cache:
            return self._zone_cache[vm_name]

        # Try to discover via gcloud
        cmd = [
            "gcloud", "compute", "instances", "list",
            f"--filter=name={vm_name}",
            "--format=value(zone)",
        ]
        if self.project:
            cmd.append(f"--project={self.project}")

        result = subprocess.run(cmd, capture_output=True, text=True)
        zone = result.stdout.strip()

        if zone:
            self._zone_cache[vm_name] = zone
            return zone

        if self.default_zone:
            self._zone_cache[vm_name] = self.default_zone
            return self.default_zone

        raise RuntimeError(
            f"Could not determine zone for VM '{vm_name}'. "
            "Set 'zone' in config as a fallback."
        )

    def _base_args(self, vm_name):
        zone = self._resolve_zone(vm_name)
        args = [f"--zone={zone}"]
        if self.project:
            args.append(f"--project={self.project}")
        return args

    def _ssh_args(self, vm_name):
        """_base_args plus the IAP flag, for the ssh/scp commands only.

        Kept separate rather than folded into _base_args: that is also used by
        `instances start` / `stop` / `describe`, which reject
        --tunnel-through-iap outright ("unrecognized arguments"), so putting it
        there took down the whole cluster lifecycle rather than just tunnelling.
        """
        args = self._base_args(vm_name)
        if self.tunnel_through_iap:
            args.append("--tunnel-through-iap")
        return args

    def _discover_all(self, vm_names):
        """Pre-fetch zones for all VMs in a single gcloud call."""
        unknown = [v for v in vm_names if v not in self._zone_cache]
        if not unknown:
            return

        filter_expr = " OR ".join(f"name={v}" for v in unknown)
        cmd = [
            "gcloud", "compute", "instances", "list",
            f"--filter={filter_expr}",
            "--format=value(name,zone)",
        ]
        if self.project:
            cmd.append(f"--project={self.project}")

        result = subprocess.run(cmd, capture_output=True, text=True)
        for line in result.stdout.strip().splitlines():
            parts = line.split()
            if len(parts) == 2:
                self._zone_cache[parts[0]] = parts[1]

    def zones_of(self, vm_names):
        """{vm_name: zone} for these VMs, discovering unknown ones in one call."""
        vm_names = list(vm_names)
        self._discover_all(vm_names)
        return {v: self._zone_cache.get(v) for v in vm_names}

    def get_instance_ids(self, vm_names):
        """{vm_name: GCE instance id}. Empty entries for VMs that don't exist.

        The id is unique per instance and changes when a VM is deleted and
        recreated, which is what the upload cache needs to tell a rebuilt
        cluster apart from the one whose name it inherited.
        """
        if not vm_names:
            return {}
        filter_expr = " OR ".join(f"name={v}" for v in vm_names)
        cmd = [
            "gcloud", "compute", "instances", "list",
            f"--filter={filter_expr}",
            "--format=value(name,id)",
        ]
        if self.project:
            cmd.append(f"--project={self.project}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            detail = result.stderr.strip() or "(no output)"
            raise RuntimeError(f"instance id lookup failed: {detail}")
        ids = {}
        for line in result.stdout.strip().splitlines():
            parts = line.split()
            if len(parts) == 2:
                ids[parts[0]] = parts[1]
        return ids

    def check_vms_running(self, vm_names):
        """Check that all VMs are RUNNING. Exit with an error if any are not."""
        self._discover_all(vm_names)
        statuses = self.vm_status(vm_names)
        not_running = []
        for vm in vm_names:
            status = statuses.get(vm, "NOT_FOUND")
            if status != "RUNNING":
                not_running.append((vm, status))
        if not_running:
            print("ERROR: The following VMs are not running:", file=sys.stderr)
            for vm, status in not_running:
                print(f"  {vm}: {status}", file=sys.stderr)
            print("\nStart them with: python bench.py vm-start --config <config>", file=sys.stderr)
            sys.exit(1)

    def ssh(self, vm_name, command, bg=False, timeout=None):
        cmd = [
            "gcloud", "compute", "ssh", vm_name,
            *self._ssh_args(vm_name),
            "--command", command,
        ]
        if bg:
            return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # `timeout` is a hard wall-clock cap. Without it a single hung VM
        # (network drop, IAP token glitch) makes run_on_all() block forever
        # — that's how the autobahn pkill stuck for 20+ min.
        #
        # Connection-layer failures (VM still booting, IAP hiccup) are
        # retried a few times; a command that ran and exited nonzero is not.
        attempts = self.settings.ssh_attempts
        result = None
        for attempt in range(1, attempts + 1):
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if result.returncode == 0:
                return result
            stderr = (result.stderr or "").strip()
            if attempt < attempts and any(
                    m in stderr for m in self.settings.ssh_transient_markers):
                print(f"  [{vm_name}] ssh connection failed "
                      f"(attempt {attempt}/{attempts}), retrying in "
                      f"{self.settings.ssh_retry_delay_s}s: {stderr[:120]}")
                time.sleep(self.settings.ssh_retry_delay_s)
                continue
            break
        raise subprocess.CalledProcessError(
            result.returncode, cmd,
            output=result.stdout, stderr=result.stderr,
        )

    def scp_upload(self, local_path, vm_name, remote_path):
        cmd = [
            "gcloud", "compute", "scp",
            local_path, f"{vm_name}:{remote_path}",
            *self._ssh_args(vm_name),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "(no output)"
            raise RuntimeError(f"scp upload to {vm_name}:{remote_path} failed (exit {result.returncode}): {detail}")

    def scp_upload_many(self, local_paths, vm_name, remote_dir):
        """Upload multiple local files to `vm_name:remote_dir` in a single
        scp invocation. Saves the per-tunnel handshake overhead (~5-10s
        each) when shipping a batch of small config files. `remote_dir`
        must end in `/` or be a directory that exists on the remote."""
        if not local_paths:
            return
        cmd = [
            "gcloud", "compute", "scp",
            *local_paths,
            f"{vm_name}:{remote_dir}",
            *self._ssh_args(vm_name),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "(no output)"
            raise RuntimeError(
                f"scp upload of {len(local_paths)} files to {vm_name}:{remote_dir} "
                f"failed (exit {result.returncode}): {detail}"
            )

    def scp_download(self, vm_name, remote_path, local_path):
        cmd = [
            "gcloud", "compute", "scp",
            f"{vm_name}:{remote_path}", local_path,
            *self._ssh_args(vm_name),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "(no output)"
            raise RuntimeError(f"scp download {vm_name}:{remote_path} failed (exit {result.returncode}): {detail}")

    def get_ip(self, vm_name):
        if hasattr(self, '_ip_cache') and vm_name in self._ip_cache:
            return self._ip_cache[vm_name]
        cmd = [
            "gcloud", "compute", "instances", "describe", vm_name,
            *self._base_args(vm_name),
            "--format=get(networkInterfaces[0].networkIP)",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "(no output)"
            raise RuntimeError(f"Failed to get IP for '{vm_name}' (exit {result.returncode}): {detail}")
        return result.stdout.strip()

    def get_all_ips(self, vm_names):
        """Fetch IPs for all VMs in a single gcloud call instead of N separate ones.

        Cached: if every requested VM is already in `_ip_cache`, skips the
        gcloud round-trip entirely. Across a sweep this saves ~1-2s per trial.
        """
        if not hasattr(self, '_ip_cache'):
            self._ip_cache = {}
        # No VMs, no call. The cache fast path below already happens to cover
        # this (all([]) is True), but only by accident of that idiom; stated
        # explicitly so reordering it cannot reintroduce the empty `--filter=`
        # that vm_status was bitten by.
        if not vm_names:
            return {}
        # Fast path: every requested VM already cached.
        if all(v in self._ip_cache for v in vm_names):
            return {v: self._ip_cache[v] for v in vm_names}

        self._discover_all(vm_names)
        filter_expr = " OR ".join(f"name={v}" for v in vm_names)
        cmd = [
            "gcloud", "compute", "instances", "list",
            f"--filter={filter_expr}",
            "--format=value(name,networkInterfaces[0].networkIP)",
        ]
        if self.project:
            cmd.append(f"--project={self.project}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or "(no output)"
            raise RuntimeError(f"Failed to get IPs (exit {result.returncode}): {detail}")
        ips = {}
        for line in result.stdout.strip().splitlines():
            parts = line.split()
            if len(parts) == 2:
                ips[parts[0]] = parts[1]
        self._ip_cache.update(ips)
        missing = [v for v in vm_names if v not in ips]
        if missing:
            raise RuntimeError(f"Could not resolve IPs for VMs: {missing}")
        return ips

    def vm_start(self, vm_names):
        if not vm_names:
            return
        self._discover_all(vm_names)
        attempts = self.settings.vm_start_attempts
        retry_delay = self.settings.vm_start_retry_delay_s
        markers = self.settings.vm_start_retry_markers

        def _start_one(vm):
            cmd = [
                "gcloud", "compute", "instances", "start",
                vm, *self._base_args(vm),
            ]
            detail = "(no output)"
            for attempt in range(attempts):
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    if attempt:
                        print(f"[{vm}] started on attempt {attempt + 1}")
                    return
                detail = result.stderr.strip() or result.stdout.strip() or "(no output)"
                # Zone capacity is a transient condition, not a broken config: a
                # cluster of stopped VMs is asking the zone for its machine type
                # back all at once, and a zone that cannot supply all of them
                # this second usually can a few seconds later. Retrying turned a
                # run-ending "vm_start failed for 5/8" into a clean start on the
                # second attempt. Anything else -- bad machine type, permissions,
                # a VM that does not exist -- fails the same way every time, so
                # only capacity is worth retrying.
                if not any(m in detail for m in markers):
                    break
                if attempt + 1 < attempts:
                    print(f"[{vm}] zone out of capacity, retrying "
                          f"({attempt + 2}/{attempts})...")
                    time.sleep(retry_delay)
            raise RuntimeError(f"Failed to start VM '{vm}': {detail}")

        failures: list[tuple[str, Exception]] = []
        with ThreadPoolExecutor(max_workers=len(vm_names)) as pool:
            futures = {pool.submit(_start_one, vm): vm for vm in vm_names}
            for f in as_completed(futures):
                vm = futures[f]
                try:
                    f.result()
                except Exception as e:
                    print(f"[{vm}] {e}")
                    failures.append((vm, e))
        if failures:
            raise RuntimeError(
                f"vm_start failed for {len(failures)}/{len(vm_names)} VM(s): "
                + ", ".join(vm for vm, _ in failures)
            )

    def vm_status(self, vm_names):
        """Return {vm_name: status} for each VM (e.g. 'RUNNING', 'TERMINATED')."""
        # An empty list would build `--filter=` (empty), and gcloud reads an
        # empty filter as "match everything" rather than "match nothing" — so
        # asking about no VMs would return the status of every instance in the
        # project. check_vms_running() would then report on, and refuse to
        # start, VMs that have nothing to do with this run.
        if not vm_names:
            return {}
        self._discover_all(vm_names)
        filter_expr = " OR ".join(f"name={v}" for v in vm_names)
        cmd = [
            "gcloud", "compute", "instances", "list",
            f"--filter={filter_expr}",
            "--format=value(name,status)",
        ]
        if self.project:
            cmd.append(f"--project={self.project}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        statuses = {}
        for line in result.stdout.strip().splitlines():
            parts = line.split()
            if len(parts) == 2:
                statuses[parts[0]] = parts[1]
        return statuses

    def vm_stop(self, vm_names):
        if not vm_names:
            return
        self._discover_all(vm_names)
        def _stop_one(vm):
            cmd = [
                "gcloud", "compute", "instances", "stop",
                vm, *self._base_args(vm),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                detail = result.stderr.strip() or result.stdout.strip() or "(no output)"
                raise RuntimeError(f"Failed to stop VM '{vm}' (exit {result.returncode}): {detail}")
        with ThreadPoolExecutor(max_workers=len(vm_names)) as pool:
            futures = {pool.submit(_stop_one, vm): vm for vm in vm_names}
            for f in as_completed(futures):
                vm = futures[f]
                try:
                    f.result()
                except Exception as e:
                    print(f"[{vm}] {e}")
