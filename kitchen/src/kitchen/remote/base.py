"""Abstract Remote interface: run commands, move files, broadcast artifacts.

Extracted from aspen-bft/scripts/benchmarks/remote.py; behaviour is preserved
(the docstrings that explain hard-won behaviour moved with the code).
"""

import contextlib
import os
import subprocess
import tempfile
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed


class Remote(ABC):
    """Abstract interface for remote machine operations."""

    @abstractmethod
    def ssh(self, host, command, bg=False, timeout=None):
        """Run a command on a remote host.

        Args:
            host: Host identifier (VM name for GCloud, IP/hostname for SSH).
            command: Shell command string to execute.
            bg: If True, return Popen handle without waiting. Otherwise block.
            timeout: Wall-clock cap in seconds on a blocking call (None = wait
                forever). Every backend must accept it, because `run_on_all`
                passes it unconditionally: a stuck SSH connection has
                previously hung a cluster-wide step for hours, and a backend
                that merely *ignores* the argument still reverts to that
                behaviour quietly, whereas one that does not accept it at all
                raises TypeError inside the thread pool and gets swallowed as
                a per-host "error" — which is how `platform: ssh` silently
                skipped every pkill, log wipe and clock sync it ever ran.
                Ignored when bg=True: a Popen this call does not wait on has
                nothing to time out.

        Returns:
            Popen if bg=True, CompletedProcess otherwise.
        """

    @abstractmethod
    def scp_upload(self, local_path, host, remote_path):
        """Upload a local file to a remote host."""

    @abstractmethod
    def scp_download(self, host, remote_path, local_path):
        """Download a file from a remote host."""

    @abstractmethod
    def get_ip(self, host):
        """Get the internal/reachable IP for a host."""

    def run_on_all(self, hosts, command, quiet=False, timeout=None):
        """Run a command on all hosts in parallel.

        `timeout`: per-host wall-clock cap. Pass it for short rpc's like
        pkill where waiting forever on a single hung VM would block the
        whole sweep.
        """
        # An empty host list is a legitimate no-op (a config with no client
        # VMs, a teardown after nothing came up), but ThreadPoolExecutor
        # rejects max_workers=0 with ValueError, so the no-op used to crash
        # the caller instead. Return the empty result it asked for.
        if not hosts:
            return {}
        results = {}
        with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
            futures = {pool.submit(self.ssh, h, command, timeout=timeout): h for h in hosts}
            for f in as_completed(futures):
                host = futures[f]
                try:
                    results[host] = f.result()
                except subprocess.CalledProcessError as e:
                    if not quiet:
                        stderr = (e.stderr or "").strip()
                        stdout = (e.output or "").strip()
                        detail = stderr or stdout or "(no output)"
                        print(f"[{host}] Command failed (exit {e.returncode}): {detail}")
                    results[host] = e
                except Exception as e:
                    if not quiet:
                        print(f"[{host}] Error: {e}")
                    results[host] = e
        return results

    def kill_process(self, hosts, process_name, timeout=60):
        """Kill a process by name on all hosts (SIGKILL).

        `timeout`: per-host wall-clock cap on the underlying ssh call. A
        `pkill -9` finishes in well under a second normally; a stuck SSH
        connection has previously hung this for hours. 60s gives plenty of
        slack for handshake jitter while bounding the worst case.
        """
        self.run_on_all(
            hosts, f"pkill -9 -f {process_name} || true",
            quiet=True, timeout=timeout,
        )

    # --- Inter-VM SSH key setup for tree/seed broadcasts ---

    _EPHEMERAL_KEY = "~/.ssh/kitchen_ephemeral"
    _EPHEMERAL_TAG = "# kitchen-ephemeral"

    def setup_inter_vm_ssh(self, hosts):
        """Generate an ephemeral keypair and distribute it so VMs can scp to each other.

        Uploads a private key and appends the public key to authorized_keys on
        every host. Uses existing scp_upload (gcloud/ssh) which already works.
        Also detects the remote username for VM-to-VM scp.
        """
        # No hosts means no keys to distribute. Guarded for the same reason as
        # run_on_all -- and because the username probe below indexes hosts[0].
        if not hosts:
            return
        with tempfile.TemporaryDirectory() as tmpdir:
            key_path = os.path.join(tmpdir, "kitchen_ephemeral")
            subprocess.run(
                ["ssh-keygen", "-t", "ed25519", "-f", key_path, "-N", "", "-q"],
                check=True,
            )
            pub_path = key_path + ".pub"
            with open(pub_path) as f:
                pubkey = f.read().strip()

            # Upload private key + public key to all hosts in parallel
            def _setup_one(host):
                self.scp_upload(key_path, host, self._EPHEMERAL_KEY)
                self.ssh(host, f"chmod 600 {self._EPHEMERAL_KEY}")
                # Append pubkey to authorized_keys (idempotent: remove old one first)
                self.ssh(host, (
                    f"mkdir -p ~/.ssh && "
                    f"sed -i '/{self._EPHEMERAL_TAG}/d' ~/.ssh/authorized_keys 2>/dev/null; "
                    f"echo '{pubkey} {self._EPHEMERAL_TAG}' >> ~/.ssh/authorized_keys"
                ))

            with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
                futures = {pool.submit(_setup_one, h): h for h in hosts}
                for f in as_completed(futures):
                    host = futures[f]
                    try:
                        f.result()
                    except Exception as e:
                        print(f"  Warning: SSH key setup failed on {host}: {e}")

        # Detect the remote username (needed for VM-to-VM scp)
        result = self.ssh(hosts[0], "whoami")
        self._remote_user = result.stdout.strip() if hasattr(result, 'stdout') else None
        if self._remote_user:
            print(f"  Distributed ephemeral SSH keys to {len(hosts)} VMs (user={self._remote_user})")
        else:
            print(f"  Distributed ephemeral SSH keys to {len(hosts)} VMs")

    def cleanup_inter_vm_ssh(self, hosts):
        """Remove ephemeral keys from all hosts."""
        cleanup_cmd = (
            f"rm -f {self._EPHEMERAL_KEY} {self._EPHEMERAL_KEY}.pub; "
            f"sed -i '/{self._EPHEMERAL_TAG}/d' ~/.ssh/authorized_keys 2>/dev/null; "
            "true"
        )
        self.run_on_all(hosts, cleanup_cmd, quiet=True)

    @contextlib.contextmanager
    def inter_vm_ssh(self, hosts):
        """Context manager: sets up ephemeral SSH keys, cleans up on exit.

        Usage:
            with remote.inter_vm_ssh(vms):
                remote.seed_broadcast(...)
                remote.tree_broadcast(...)
        """
        self.setup_inter_vm_ssh(hosts)
        try:
            yield
        finally:
            self.cleanup_inter_vm_ssh(hosts)

    # --- VM-to-VM copy (requires setup_inter_vm_ssh / inter_vm_ssh first) ---

    def vm_to_vm_scp(self, src_host, remote_path, dst_host):
        """Copy a file from src_host to dst_host via internal network.

        Uses the ephemeral key distributed by setup_inter_vm_ssh.
        """
        dst_ip = self.get_ip(dst_host)
        user = getattr(self, "_remote_user", None)
        dst_target = f"{user}@{dst_ip}" if user else dst_ip
        self.ssh(
            src_host,
            f"scp -o StrictHostKeyChecking=no -i {self._EPHEMERAL_KEY} "
            f"{remote_path} {dst_target}:{remote_path}",
        )

    # --- Broadcast strategies ---

    _MAX_CONCURRENT_SCP = 8  # avoid overwhelming sshd MaxStartups on seed

    def seed_broadcast(self, local_path, hosts, remote_path):
        """Upload to one seed VM, then seed fans out to all others in parallel.

        Requires setup_inter_vm_ssh to have been called first.
        One slow local upload + one round of fast internal copies.
        """
        if not hosts:
            return

        seed = hosts[0]
        self.scp_upload(local_path, seed, remote_path)

        others = hosts[1:]
        if not others:
            return

        with ThreadPoolExecutor(max_workers=min(self._MAX_CONCURRENT_SCP, len(others))) as pool:
            futures = {}
            for dst in others:
                f = pool.submit(self.vm_to_vm_scp, seed, remote_path, dst)
                futures[f] = dst
            for f in as_completed(futures):
                dst = futures[f]
                try:
                    f.result()
                except Exception as e:
                    print(f"  Warning: seed copy {seed} -> {dst} failed: {e}")

    def tree_broadcast(self, local_path, hosts, remote_path, fanout=2):
        """Upload to seed VM, then fan out in tree rounds via internal network.

        Requires setup_inter_vm_ssh to have been called first.
        With fanout=2, distributing to N hosts takes ceil(log2(N)) rounds
        of VM-to-VM copies instead of N serial uploads from the local machine.
        """
        if not hosts:
            return

        seed = hosts[0]
        self.scp_upload(local_path, seed, remote_path)

        remaining = list(hosts[1:])
        have_file = [seed]

        while remaining:
            pairs = []
            for src in have_file:
                for _ in range(fanout):
                    if not remaining:
                        break
                    dst = remaining.pop(0)
                    pairs.append((src, dst))

            # Only reachable with fanout <= 0, where the loop would otherwise
            # spin forever without copying anything. Bail rather than hand
            # ThreadPoolExecutor a max_workers of 0.
            if not pairs:
                break

            with ThreadPoolExecutor(max_workers=len(pairs)) as pool:
                futures = {}
                for src, dst in pairs:
                    f = pool.submit(self.vm_to_vm_scp, src, remote_path, dst)
                    futures[f] = (src, dst)
                for f in as_completed(futures):
                    src, dst = futures[f]
                    try:
                        f.result()
                    except Exception as e:
                        print(f"  Warning: tree copy {src} -> {dst} failed: {e}")

            have_file.extend(dst for _, dst in pairs)
