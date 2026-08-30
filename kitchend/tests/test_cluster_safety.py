import asyncio
import sqlite3
import time
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from kitchen.remote.mock import MockRemote
from kitchend.core.clusters import ClusterManager, ManagedCluster
from kitchend.core.db import open_db


class FakeDb:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))


class FakeHub:
    def __init__(self):
        self.events = []

    def emit(self, kind, **payload):
        self.events.append((kind, payload))


def managed(remote=None):
    return ManagedCluster(
        key="p/c", project="p", name="c", config_path=Path("unused"),
        hourly_usd=1.0, remote=remote or MockRemote(),
        state=None, db_id=1, vms=["vm-a"],
    )


def manager():
    value = ClusterManager.__new__(ClusterManager)
    value.db = FakeDb()
    value.hub = FakeHub()
    return value


def test_failed_stop_stays_unmanaged_and_keeps_session_open():
    class FailedStop(MockRemote):
        supports_deadman = False

        def vm_stop(self, vm_names):
            return list(vm_names)

    cm = manager()
    mc = managed(FailedStop())
    mc.session_id = 9

    async def inline(func, *args, **kwargs):
        return func(*args, **kwargs)

    with patch("kitchend.core.clusters.asyncio.to_thread", inline):
        with pytest.raises(RuntimeError, match="could not stop"):
            asyncio.run(cm._shutdown(mc, mc.vms, reason="test"))

    assert mc.session_id == 9
    assert any(params[0] == "unmanaged" for sql, params in cm.db.calls
               if sql.startswith("UPDATE clusters SET state"))


def test_expired_deadline_stops_cluster():
    cm = manager()
    cm.TICK_S = 0
    cm._vms = lambda mc: mc.vms
    cm._shutdown = AsyncMock()
    mc = managed()
    mc.stop_at = time.time() - 1

    asyncio.run(cm._tick_loop(mc))

    cm._shutdown.assert_awaited_once_with(mc, ["vm-a"], reason="deadline")


def test_v3_schema_cleanup_preserves_operational_rows(tmp_path):
    path = tmp_path / "state.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE clusters (
            id INTEGER PRIMARY KEY, machine_type TEXT, orphan INTEGER,
            last_error TEXT, state TEXT, state_updated_at TEXT
        );
        CREATE TABLE cluster_leases (id INTEGER PRIMARY KEY);
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY, estimate_json TEXT, state TEXT
        );
        CREATE TABLE runs (
            id INTEGER PRIMARY KEY, archived INTEGER, status TEXT
        );
        INSERT INTO clusters VALUES (1, 'n2', 0, NULL, 'running', 'now');
        INSERT INTO cluster_leases VALUES (1);
        INSERT INTO jobs VALUES (2, '{}', 'running');
        INSERT INTO runs VALUES (3, 0, 'ok');
        PRAGMA user_version = 3;
    """)
    conn.close()

    migrated = open_db(path)
    try:
        assert migrated.execute("PRAGMA user_version").fetchone()[0] == 4
        assert migrated.execute(
            "SELECT state FROM clusters WHERE id = 1").fetchone()[0] == "running"
        assert migrated.execute(
            "SELECT state FROM jobs WHERE id = 2").fetchone()[0] == "running"
        assert migrated.execute(
            "SELECT status FROM runs WHERE id = 3").fetchone()[0] == "ok"
        assert migrated.execute(
            "SELECT name FROM sqlite_master WHERE name = 'cluster_leases'"
        ).fetchone() is None
        for table, removed in {
            "clusters": {"machine_type", "orphan", "last_error"},
            "jobs": {"estimate_json"},
            "runs": {"archived"},
        }.items():
            columns = {r[1] for r in migrated.execute(f"PRAGMA table_info({table})")}
            assert columns.isdisjoint(removed)
    finally:
        migrated.close()


def test_unknown_old_schema_is_not_destroyed(tmp_path):
    path = tmp_path / "old.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE important (value TEXT);
        INSERT INTO important VALUES ('keep me');
        PRAGMA user_version = 2;
    """)
    conn.close()

    with pytest.raises(RuntimeError, match="unsupported database schema version 2"):
        open_db(path)

    conn = sqlite3.connect(path)
    try:
        assert conn.execute("SELECT value FROM important").fetchone()[0] == "keep me"
    finally:
        conn.close()
