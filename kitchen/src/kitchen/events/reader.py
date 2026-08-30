"""Read and follow events.jsonl files."""

import json
import time


def read_all(path, offset=0):
    """(events, new_offset): parse complete lines from `offset` on. A torn
    final line (mid-write) is left for the next call."""
    events = []
    try:
        with open(path, "rb") as f:
            f.seek(offset)
            data = f.read()
    except FileNotFoundError:
        return [], offset
    end = data.rfind(b"\n")
    if end < 0:
        return [], offset
    for line in data[:end].split(b"\n"):
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # torn or corrupt line; skip rather than wedge the tail
    return events, offset + end + 1


def follow(path, poll_s=1.0, stop=None):
    """Generator: yield events as they are appended. `stop` is an optional
    zero-arg callable checked between polls (return True to end). Polling
    rather than inotify: it costs nothing at this scale and inotify is
    unreliable on some filesystems (WSL2 included)."""
    offset = 0
    while True:
        events, offset = read_all(path, offset)
        yield from events
        if stop is not None and stop():
            return
        time.sleep(poll_s)
