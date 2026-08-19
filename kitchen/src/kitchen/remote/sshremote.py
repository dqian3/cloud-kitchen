"""Plain ssh/scp backend: hosts are IPs or hostnames."""

import subprocess

from .base import Remote


class SSHRemote(Remote):
    """Uses plain ssh/scp. Hosts are IPs or hostnames."""

    def __init__(self, user="root", key_file=None):
        self.user = user
        self.key_file = key_file

    def _ssh_opts(self):
        opts = ["-o", "StrictHostKeyChecking=no"]
        if self.key_file:
            opts += ["-i", self.key_file]
        return opts

    def _target(self, host):
        return f"{self.user}@{host}"

    def ssh(self, host, command, bg=False, timeout=None):
        cmd = ["ssh", *self._ssh_opts(), self._target(host), command]
        if bg:
            # Nothing to time out: the caller gets the Popen and decides how
            # long to wait. Dropping `timeout` here is deliberate, not an
            # oversight — see the note on Remote.ssh.
            return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # `timeout` must be honoured, not just accepted. Omitting it from this
        # signature is what made every run_on_all() on `platform: ssh` raise
        # TypeError into the thread pool, where run_on_all's generic
        # `except Exception` recorded it as a per-host error: the run did not
        # crash, it just quietly performed no pkill, no stale-log wipe, no log
        # bundling, no clock sync, and then failed later for reasons that
        # pointed nowhere near here.
        return subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=timeout)

    def scp_upload(self, local_path, host, remote_path):
        cmd = [
            "scp", *self._ssh_opts(),
            local_path, f"{self._target(host)}:{remote_path}",
        ]
        subprocess.run(cmd, check=True)

    def scp_download(self, host, remote_path, local_path):
        cmd = [
            "scp", *self._ssh_opts(),
            f"{self._target(host)}:{remote_path}", local_path,
        ]
        subprocess.run(cmd, check=True)

    def get_ip(self, host):
        return host
