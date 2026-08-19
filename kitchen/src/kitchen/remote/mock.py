"""In-memory Remote for tests: scripted responses, injected failures, a call log.

No subprocesses, no network. Hosts are arbitrary strings; files live in a dict.
"""

import re
import subprocess
from dataclasses import dataclass, field

from .base import Remote


@dataclass
class _Rule:
    cmd_pattern: str
    host: str | None
    stdout: str
    returncode: int
    raises: BaseException | None
    times: int | None  # None = unlimited
    used: int = 0

    def matches(self, host, command):
        if self.times is not None and self.used >= self.times:
            return False
        if self.host is not None and self.host != host:
            return False
        return re.search(self.cmd_pattern, command) is not None


@dataclass
class Call:
    kind: str  # "ssh" | "upload" | "download"
    host: str
    detail: str
    bg: bool = False
    timeout: float | None = None


class MockRemote(Remote):
    def __init__(self):
        self.calls: list[Call] = []
        self.rules: list[_Rule] = []
        self.files: dict[tuple[str, str], str] = {}  # (host, remote_path) -> local source
        self._hosts_seen: list[str] = []
        # Simulated VM lifecycle for cluster tests. Unknown VMs report absent
        # (no entry), matching gcloud's behaviour for nonexistent names.
        self.vm_states: dict[str, str] = {}
        # Optional scripted sequence of vm_status() results; once exhausted,
        # falls back to vm_states. Lets tests model external transitions
        # (e.g. STOPPING -> TERMINATED while wait_drained polls).
        self.status_sequence: list[dict[str, str]] = []

    def script(self, cmd_pattern, *, host=None, stdout="", returncode=0,
               raises=None, times=None):
        """Queue a response for ssh commands matching `cmd_pattern` (regex).

        First matching, non-exhausted rule wins; unmatched commands succeed
        with empty output. `times` limits how often the rule fires, so a
        fail-then-succeed sequence is two rules.
        """
        self.rules.append(_Rule(cmd_pattern, host, stdout, returncode, raises, times))

    # --- Remote interface ---

    def ssh(self, host, command, bg=False, timeout=None):
        self.calls.append(Call("ssh", host, command, bg=bg, timeout=timeout))
        for rule in self.rules:
            if rule.matches(host, command):
                rule.used += 1
                if rule.raises is not None:
                    raise rule.raises
                if rule.returncode != 0:
                    raise subprocess.CalledProcessError(
                        rule.returncode, ["ssh", host, command],
                        output=rule.stdout, stderr="scripted failure",
                    )
                return subprocess.CompletedProcess(
                    ["ssh", host, command], 0, stdout=rule.stdout, stderr="")
        if bg:
            # A no-op background process handle.
            return subprocess.Popen(["true"])
        return subprocess.CompletedProcess(["ssh", host, command], 0, stdout="", stderr="")

    def scp_upload(self, local_path, host, remote_path):
        self.calls.append(Call("upload", host, f"{local_path} -> {remote_path}"))
        self.files[(host, remote_path)] = local_path

    def scp_download(self, host, remote_path, local_path):
        self.calls.append(Call("download", host, f"{remote_path} -> {local_path}"))
        if (host, remote_path) not in self.files:
            raise RuntimeError(f"fake download {host}:{remote_path}: no such file")

    def get_ip(self, host):
        if host not in self._hosts_seen:
            self._hosts_seen.append(host)
        return f"10.0.0.{self._hosts_seen.index(host) + 1}"

    # vm_to_vm_scp goes through ssh(); mark the destination as having the file
    # so broadcast tests can assert delivery.
    def vm_to_vm_scp(self, src_host, remote_path, dst_host):
        super().vm_to_vm_scp(src_host, remote_path, dst_host)
        self.files[(dst_host, remote_path)] = f"(copied from {src_host})"

    # --- simulated VM lifecycle ---

    def vm_status(self, vm_names):
        self.calls.append(Call("vm_status", "*", ",".join(vm_names)))
        if self.status_sequence:
            snapshot = self.status_sequence.pop(0)
        else:
            snapshot = self.vm_states
        return {v: snapshot[v] for v in vm_names if v in snapshot}

    def vm_start(self, vm_names):
        self.calls.append(Call("vm_start", "*", ",".join(vm_names)))
        for v in vm_names:
            self.vm_states[v] = "RUNNING"

    def vm_stop(self, vm_names):
        self.calls.append(Call("vm_stop", "*", ",".join(vm_names)))
        for v in vm_names:
            self.vm_states[v] = "TERMINATED"

    # --- assertions ---

    def ssh_calls(self, pattern=None, host=None):
        out = [c for c in self.calls if c.kind == "ssh"]
        if host is not None:
            out = [c for c in out if c.host == host]
        if pattern is not None:
            out = [c for c in out if re.search(pattern, c.detail)]
        return out
