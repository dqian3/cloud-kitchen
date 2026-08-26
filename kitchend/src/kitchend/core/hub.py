"""Event hub: append to the events table, fan out to SSE subscribers.

Every state change in the daemon goes through emit(), so the events table is
the audit log and the SSE stream is just its live tail. SSE ids are the table
row ids, which lets a reconnecting client replay from Last-Event-ID.
"""

import asyncio
import json
import sqlite3


class EventHub:
    def __init__(self, db):
        self.db = db
        self._subscribers: set[asyncio.Queue] = set()
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop):
        self._loop = loop

    def emit(self, type: str, job_id=None, cluster_id=None, **payload) -> int:
        """Record an event and broadcast it. Safe to call from any thread."""
        try:
            event_id = self._insert(type, job_id, cluster_id, payload)
        except sqlite3.IntegrityError:
            # The job or cluster went away mid-flight (deleted while its
            # dispatch was still running). The event still happened, so keep
            # it with the id in the payload instead of losing it — and, more
            # to the point, never raise inside a caller's failure path.
            payload = {**payload, "job_id": job_id, "cluster_id": cluster_id}
            event_id = self._insert(type, None, None, payload)
        row = self.db.query_one("SELECT ts FROM events WHERE id = ?", (event_id,))
        event = {
            "id": event_id, "ts": row["ts"], "type": type,
            "job_id": job_id, "cluster_id": cluster_id, **payload,
        }
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._broadcast, event)
        return event_id

    def _insert(self, type, job_id, cluster_id, payload) -> int:
        return self.db.insert(
            "INSERT INTO events (job_id, cluster_id, type, payload_json) "
            "VALUES (?, ?, ?, ?)",
            (job_id, cluster_id, type, json.dumps(payload)),
        )

    def _broadcast(self, event):
        for q in list(self._subscribers):
            if q.full():
                # A stalled subscriber drops events rather than blocking the
                # daemon; it can replay from the table via Last-Event-ID.
                continue
            q.put_nowait(event)

    def subscribe(self, maxsize=1000) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=maxsize)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q):
        self._subscribers.discard(q)

    def replay_since(self, last_id: int, limit=500):
        rows = self.db.query(
            "SELECT id, ts, job_id, cluster_id, type, payload_json FROM events "
            "WHERE id > ? ORDER BY id LIMIT ?", (last_id, limit),
        )
        out = []
        for r in rows:
            out.append({
                "id": r["id"], "ts": r["ts"], "type": r["type"],
                "job_id": r["job_id"], "cluster_id": r["cluster_id"],
                **json.loads(r["payload_json"]),
            })
        return out
