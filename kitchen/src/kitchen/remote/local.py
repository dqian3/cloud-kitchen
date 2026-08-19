"""Local backend: every 'host' is a local process tree under its own fake $HOME."""

import os
import shutil
import subprocess
import tempfile

from .base import Remote


class LocalRemote(Remote):
    """Runs every 'host' as a local process tree under its own fake $HOME.

    Lets a harness written against the ssh/scp interface drive an all-on-one-box
    run with no other changes: each host gets `<root>/<host>` as its home, so
    `~/foo` in a command resolves to a per-host directory the same way it would
    on a real VM, and uploads/downloads are file copies. Everything resolves to
    127.0.0.1, so the protocol under test must give each node distinct ports.

    Commands run through `bash -c` rather than `bash -lc`: no login profile to
    source (faster, and no surprise PATH/alias differences between a developer
    box and CI), while `~` still expands from the HOME we set.
    """

    def __init__(self, root=None):
        self.root = os.path.abspath(root or os.path.join(tempfile.gettempdir(), "kitchen-local"))

    def home(self, host):
        """Per-host home directory, created on demand."""
        path = os.path.join(self.root, str(host))
        os.makedirs(path, exist_ok=True)
        return path

    def _expand(self, host, path):
        """Resolve a remote-style path ('~/x', 'x', '/abs/x') against the host's home."""
        home = self.home(host)
        path = str(path)
        if path.startswith("~"):
            path = home + path[1:]
        elif not os.path.isabs(path):
            path = os.path.join(home, path)
        return path

    def _env(self, host):
        env = dict(os.environ)
        env["HOME"] = self.home(host)
        return env

    def ssh(self, host, command, bg=False, timeout=None):
        home = self.home(host)
        cmd = ["bash", "-c", command]
        if bg:
            return subprocess.Popen(
                cmd, cwd=home, env=self._env(host),
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        # start_new_session detaches from this process group, so a signal sent
        # to the driver's group (Ctrl-C, a supervisor killing the run) does not
        # also hit the long-lived servers a previous call left running — they
        # are torn down explicitly by pkill instead, the same way a real ssh
        # session's processes are.
        result = subprocess.run(
            cmd, cwd=home, env=self._env(host),
            capture_output=True, text=True, timeout=timeout,
            start_new_session=True,
        )
        if result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode, cmd,
                output=result.stdout, stderr=result.stderr,
            )
        return result

    def scp_upload(self, local_path, host, remote_path):
        dest = self._expand(host, remote_path)
        # A trailing slash (or an existing dir) means "into this directory",
        # matching scp semantics.
        if remote_path.endswith("/") or os.path.isdir(dest):
            os.makedirs(dest, exist_ok=True)
            dest = os.path.join(dest, os.path.basename(local_path))
        else:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.copy2(local_path, dest)

    def scp_upload_many(self, local_paths, host, remote_dir):
        for path in local_paths:
            self.scp_upload(path, host, remote_dir)

    def scp_download(self, host, remote_path, local_path):
        src = self._expand(host, remote_path)
        if not os.path.exists(src):
            raise RuntimeError(f"local download {host}:{remote_path} failed: no such file {src}")
        shutil.copy2(src, local_path)

    def get_ip(self, host):
        return "127.0.0.1"

    def get_all_ips(self, hosts):
        return {h: "127.0.0.1" for h in hosts}

    def check_vms_running(self, hosts):
        """No VMs to check; just make sure each host has a home to run in."""
        for host in hosts:
            self.home(host)
