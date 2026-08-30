"""Spawn and supervise one job's driver process.

The driver runs in its own session (process group) with stdout+stderr
appended to ~/.cloud-kitchen/jobs/<id>.log. Cancellation escalates
SIGINT → SIGTERM → SIGKILL against the whole group; the drivers clean up on
SIGINT (and, since the extraction, on SIGTERM too).
"""

import asyncio
import time
import os
import signal


class JobRunner:
    def __init__(self, jobs_dir):
        self.jobs_dir = jobs_dir
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self._procs: dict[int, asyncio.subprocess.Process] = {}

    def log_path(self, job_id):
        return self.jobs_dir / f"{job_id}.log"

    async def run(self, job_id, argv, cwd, on_start=None) -> int:
        """Start the driver; returns its exit code. `on_start(pid)` fires as
        soon as the process exists, so the pid is on record for crash
        recovery even if the daemon dies while the job runs."""
        log = open(self.log_path(job_id), "ab", buffering=0)
        # Every attempt appends to one file, so without a marker the tail
        # shows the previous attempt's output until this one prints something
        # -- a UI reading a stale build log while the new driver is still
        # connecting, with nothing to say which attempt it belongs to.
        log.write(f"\n=== attempt started {time.strftime('%Y-%m-%d %H:%M:%S')} "
                  f": {' '.join(argv)}\n".encode())
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv, cwd=str(cwd),
                stdout=log, stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        finally:
            log.close()
        self._procs[job_id] = proc
        if on_start is not None:
            on_start(proc.pid)
        try:
            return await proc.wait()
        finally:
            self._procs.pop(job_id, None)

    async def cancel(self, job_id, grace_s=60):
        """SIGINT the process group; escalate if it won't die."""
        proc = self._procs.get(job_id)
        if proc is None:
            return False
        pgid = proc.pid  # start_new_session makes pid == pgid
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(pgid, sig)
            except ProcessLookupError:
                return True
            try:
                await asyncio.wait_for(proc.wait(), timeout=grace_s)
                return True
            except asyncio.TimeoutError:
                continue
        return False

    def tail_log(self, job_id, lines=200):
        path = self.log_path(job_id)
        if not path.exists():
            return ""
        # Read at most ~1MB from the end; enough for any sane tail request.
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 1_000_000))
            data = f.read().decode(errors="replace")
        return "\n".join(data.splitlines()[-lines:])
