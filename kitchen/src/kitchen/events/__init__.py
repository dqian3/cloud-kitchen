"""Structured run events: the machine-readable progress contract.

A run appends one JSON line per event to `<run_dir>/events.jsonl`; the daemon
tails the file. File-append survives daemon restarts, works with no daemon at
all, and is replayable for postmortems. Consumers must ignore unknown fields;
`v` bumps only on breaking changes.
"""

from .emitter import EventEmitter
from .reader import follow, read_all
from .schema import EVENT_TYPES, make_event

__all__ = ["EventEmitter", "follow", "read_all", "EVENT_TYPES", "make_event"]
