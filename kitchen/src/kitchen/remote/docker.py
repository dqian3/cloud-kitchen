"""Docker backend: every 'host' is a compose service on this machine.

The closest local stand-in for a VM fleet: each node is its own container
with its own network address, so protocols keep the port layout they use on
real VMs instead of sharing 127.0.0.1, and CPU/memory limits in the compose
file make rehearsals less noisy. Commands run through `docker exec`, file
transfer is `docker cp`, and start/stop are compose operations — so the
daemon leases a compose fleet exactly like a gcloud one, at no cost.

The compose file is the cluster: service names are the host names the
cluster YAML lists. Images need bash and procps (pkill); the protocol
binaries are bind-mounted from the checkout by the compose file.
"""

import json
import os
import subprocess

from .base import Remote


class DockerRemote(Remote):
    # No dead-man switch: nothing in a container can `shutdown` it, and a
    # stray container costs nothing.
    supports_deadman = False

    def __init__(self, compose_file, project=None, home="/root"):
        self.compose_file = os.path.abspath(compose_file)
        self.project = project
        self.home = home
        self._ids: dict[str, str] = {}

    # --- compose plumbing ---

    def _compose_cmd(self, *args):
        cmd = ["docker", "compose", "-f", self.compose_file]
        if self.project:
            cmd += ["-p", self.project]
        return cmd + list(args)

    def _compose(self, *args, timeout=600):
        return subprocess.run(self._compose_cmd(*args), check=True,
                              capture_output=True, text=True, timeout=timeout)

    def _cid(self, host):
        cid = self._ids.get(host)
        if not cid:
            out = self._compose("ps", "-a", "-q", host).stdout.strip()
            if not out:
                raise RuntimeError(f"no container for service '{host}' "
                                   f"(is the fleet up? compose file "
                                   f"{self.compose_file})")
            cid = self._ids[host] = out.splitlines()[0]
        return cid

    def _expand(self, path):
        path = str(path)
        if path.startswith("~"):
            return self.home + path[1:]
        if not os.path.isabs(path):
            return os.path.join(self.home, path)
        return path

    # --- Remote interface ---

    def ssh(self, host, command, bg=False, timeout=None):
        cmd = ["docker", "exec", self._cid(host), "bash", "-c", command]
        if bg:
            return subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL,
                                    start_new_session=True)
        result = subprocess.run(cmd, capture_output=True, text=True,
                                timeout=timeout)
        if result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode, cmd, output=result.stdout,
                stderr=result.stderr)
        return result

    def scp_upload(self, local_path, host, remote_path):
        dest = self._expand(remote_path)
        cid = self._cid(host)
        if remote_path.endswith("/"):
            dest = os.path.join(dest, os.path.basename(local_path))
        subprocess.run(["docker", "exec", cid, "mkdir", "-p",
                        os.path.dirname(dest)], check=True,
                       capture_output=True)
        subprocess.run(["docker", "cp", local_path, f"{cid}:{dest}"],
                       check=True, capture_output=True, text=True)

    def scp_download(self, host, remote_path, local_path):
        src = self._expand(remote_path)
        result = subprocess.run(
            ["docker", "cp", f"{self._cid(host)}:{src}", local_path],
            capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"docker cp {host}:{remote_path} failed: "
                               f"{result.stderr.strip()}")

    def get_ip(self, host):
        out = subprocess.run(
            ["docker", "inspect", "-f",
             "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
             self._cid(host)],
            check=True, capture_output=True, text=True).stdout.strip()
        if not out:
            raise RuntimeError(f"container for '{host}' has no network address")
        return out

    def get_all_ips(self, hosts):
        return {h: self.get_ip(h) for h in hosts}

    # --- fleet lifecycle (the daemon's up/down and status poll) ---

    def vm_status(self, vm_names):
        """{service: 'RUNNING' | 'TERMINATED'}; services with no container
        are absent, like VMs that don't exist."""
        if not vm_names:
            return {}
        out = self._compose("ps", "-a", "--format", "json").stdout
        states = {}
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            entries = entry if isinstance(entry, list) else [entry]
            for e in entries:
                states[e["Service"]] = e.get("State", "")
        return {v: ("RUNNING" if states[v] == "running" else "TERMINATED")
                for v in vm_names if v in states}

    def vm_start(self, vm_names):
        if not vm_names:
            return
        self._compose("up", "-d", "--no-recreate", *vm_names)
        self._ids.clear()

    def vm_stop(self, vm_names):
        if not vm_names:
            return
        self._compose("stop", *vm_names)

    def check_vms_running(self, vm_names):
        statuses = self.vm_status(vm_names)
        down = [v for v in vm_names if statuses.get(v) != "RUNNING"]
        if down:
            raise RuntimeError(f"containers not running: {down}")

    def _discover_all(self, vm_names):
        self._ids.clear()
