"""Per-cluster state directory: ~/.cloud-kitchen/clusters/<name>/.

Holds the keep-alive lock and file-based leases. Files, not a daemon, are the
canonical store so everything works with no daemon running; the daemon mirrors
them into its DB for queries and history.

A lease says "something is using this cluster": liveness requires the holder
pid to be alive on this host AND the TTL to be unexpired. Stale leases are
garbage-collected whenever leases are listed.
"""

import json
import os
import socket
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ROOT = Path(os.environ.get("KITCHEN_STATE_DIR", "~/.cloud-kitchen")).expanduser()


def _pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@dataclass(frozen=True)
class LeaseInfo:
    id: str
    pid: int
    host: str
    purpose: str
    acquired_at: float
    ttl_s: float
    path: Path

    @property
    def expires_at(self):
        return self.acquired_at + self.ttl_s

    def is_live(self):
        if time.time() >= self.expires_at:
            return False
        # A lease taken on another host can't be liveness-checked by pid;
        # trust its TTL alone.
        if self.host == socket.gethostname() and not _pid_alive(self.pid):
            return False
        return True


class Lease:
    """Context manager handle for one acquired lease."""

    def __init__(self, state, info: LeaseInfo):
        self._state = state
        self.info = info

    def renew(self, ttl_s=None):
        data = json.loads(self.info.path.read_text())
        data["acquired_at"] = time.time()
        if ttl_s is not None:
            data["ttl_s"] = ttl_s
        _atomic_write(self.info.path, data)

    def release(self):
        self.info.path.unlink(missing_ok=True)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.release()
        return False


def _atomic_write(path: Path, data: dict):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, path)


class ClusterState:
    def __init__(self, name: str, root: Path | None = None):
        if not name or "/" in name:
            raise ValueError(f"bad cluster name: {name!r}")
        self.name = name
        self.dir = (root or DEFAULT_ROOT) / "clusters" / name
        self.leases_dir = self.dir / "leases"
        self.leases_dir.mkdir(parents=True, exist_ok=True)

    @property
    def keepalive_lock_path(self) -> Path:
        return self.dir / "keepalive.lock"

    # --- leases ---

    def acquire_lease(self, purpose: str, ttl_s: float = 3600.0) -> Lease:
        info = LeaseInfo(
            id=uuid.uuid4().hex[:12],
            pid=os.getpid(),
            host=socket.gethostname(),
            purpose=purpose,
            acquired_at=time.time(),
            ttl_s=ttl_s,
            path=self.leases_dir / f"{uuid.uuid4().hex[:12]}.json",
        )
        _atomic_write(info.path, {
            "id": info.id, "pid": info.pid, "host": info.host,
            "purpose": info.purpose, "acquired_at": info.acquired_at,
            "ttl_s": info.ttl_s,
        })
        return Lease(self, info)

    def live_leases(self) -> list[LeaseInfo]:
        """List live leases; garbage-collect stale ones as a side effect."""
        live = []
        for path in sorted(self.leases_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text())
                info = LeaseInfo(
                    id=data["id"], pid=int(data["pid"]), host=data["host"],
                    purpose=data.get("purpose", "?"),
                    acquired_at=float(data["acquired_at"]),
                    ttl_s=float(data["ttl_s"]), path=path,
                )
            except (json.JSONDecodeError, KeyError, ValueError, OSError):
                path.unlink(missing_ok=True)
                continue
            if info.is_live():
                live.append(info)
            else:
                path.unlink(missing_ok=True)
        return live
