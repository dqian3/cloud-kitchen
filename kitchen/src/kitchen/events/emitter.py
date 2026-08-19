"""Append-only JSONL event writer.

Single-line O_APPEND writes are atomic for lines under the pipe/page size, so
concurrent writers can share a file safely; each event is flushed immediately
so a tail sees it the moment it happens. Emitting never raises into the run:
an unwritable events file must not fail an experiment.
"""

import json
import os
import sys

from .schema import make_event


class EventEmitter:
    def __init__(self, path, run_id: str):
        self.path = os.fspath(path)
        self.run_id = run_id
        self.seq = 0
        self._fd = None
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            self._fd = os.open(self.path,
                               os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        except OSError as e:
            print(f"warning: events disabled ({e})", file=sys.stderr)

    def emit(self, type: str, **data) -> None:
        self.seq += 1
        event = make_event(type, self.run_id, self.seq, data)
        if self._fd is None:
            return
        line = json.dumps(event, separators=(",", ":")) + "\n"
        try:
            os.write(self._fd, line.encode())
        except OSError:
            pass

    def close(self):
        if self._fd is not None:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False
