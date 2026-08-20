"""SQLite schema and migrations.

One writer: the daemon process (the MCP server runs in-process; the CLI talks
HTTP). WAL mode so readers never block on the writer.
"""

import sqlite3
from pathlib import Path

MIGRATIONS = [
    # v1 — jobs / clusters / events / results catalog
    """
    CREATE TABLE projects (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        repo_path TEXT NOT NULL,
        adapter_path TEXT,
        runs_roots TEXT NOT NULL DEFAULT '[]',   -- JSON list
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE clusters (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL REFERENCES projects(id),
        name TEXT NOT NULL,
        config_path TEXT NOT NULL,
        machine_type TEXT,
        hourly_usd REAL,
        vm_count INTEGER,
        state TEXT NOT NULL DEFAULT 'terminated',
        state_updated_at TEXT,
        orphan INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        UNIQUE (project_id, name)
    );

    CREATE TABLE cluster_sessions (
        id INTEGER PRIMARY KEY,
        cluster_id INTEGER NOT NULL REFERENCES clusters(id),
        started_at TEXT NOT NULL,
        stopped_at TEXT,
        vm_count INTEGER NOT NULL,
        hourly_usd REAL
    );

    CREATE TABLE cluster_leases (
        id INTEGER PRIMARY KEY,
        cluster_id INTEGER NOT NULL REFERENCES clusters(id),
        holder_type TEXT NOT NULL,              -- job | user | agent
        holder_id TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        expires_at TEXT,
        released_at TEXT
    );

    CREATE TABLE jobs (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL REFERENCES projects(id),
        spec_json TEXT NOT NULL,
        cluster_id INTEGER REFERENCES clusters(id),
        run_dir TEXT,
        state TEXT NOT NULL DEFAULT 'queued',
        priority INTEGER NOT NULL DEFAULT 0,
        retries_left INTEGER NOT NULL DEFAULT 0,
        exit_code INTEGER,
        pid INTEGER,
        events_offset INTEGER NOT NULL DEFAULT 0,
        estimate_json TEXT,
        parent_job_id INTEGER REFERENCES jobs(id),
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        started_at TEXT,
        finished_at TEXT
    );
    CREATE INDEX idx_jobs_state ON jobs(state, cluster_id);

    CREATE TABLE events (
        id INTEGER PRIMARY KEY,
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        job_id INTEGER REFERENCES jobs(id),
        cluster_id INTEGER REFERENCES clusters(id),
        type TEXT NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}'
    );
    CREATE INDEX idx_events_job ON events(job_id, id);
    CREATE INDEX idx_events_type ON events(type, ts);

    CREATE TABLE runs (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL REFERENCES projects(id),
        run_dir TEXT NOT NULL UNIQUE,
        experiment TEXT,
        started_at TEXT,
        git_commit TEXT,
        argv TEXT,
        n_points INTEGER,
        key_metrics_json TEXT,
        status TEXT,
        dir_exists INTEGER NOT NULL DEFAULT 1,
        archived INTEGER NOT NULL DEFAULT 0,
        job_id INTEGER REFERENCES jobs(id),
        indexed_at TEXT
    );

    CREATE TABLE run_points (
        id INTEGER PRIMARY KEY,
        run_id INTEGER NOT NULL REFERENCES runs(id),
        dims_json TEXT NOT NULL DEFAULT '{}',
        rate REAL,
        trial INTEGER,
        metrics_json TEXT NOT NULL DEFAULT '{}',
        summary_path TEXT
    );
    CREATE INDEX idx_run_points_run ON run_points(run_id);

    CREATE TABLE figures (
        id INTEGER PRIMARY KEY,
        run_id INTEGER NOT NULL REFERENCES runs(id),
        relpath TEXT NOT NULL,
        kind TEXT,
        mtime REAL
    );

    CREATE TABLE tags (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE
    );
    CREATE TABLE run_tags (
        run_id INTEGER NOT NULL REFERENCES runs(id),
        tag_id INTEGER NOT NULL REFERENCES tags(id),
        PRIMARY KEY (run_id, tag_id)
    );
    CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        run_id INTEGER NOT NULL REFERENCES runs(id),
        ts TEXT NOT NULL DEFAULT (datetime('now')),
        text TEXT NOT NULL
    );
    """,
    # v2 — ingest: progress summary folded from <run_dir>/events.jsonl
    """
    ALTER TABLE jobs ADD COLUMN progress_json TEXT;
    """,
    # v3 — saved sweeps: one-off experiments promoted to reusable presets
    """
    CREATE TABLE saved_sweeps (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL REFERENCES projects(id),
        name TEXT NOT NULL,
        params_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE (project_id, name)
    );
    """,
]


class Db:
    """Thread-safe wrapper around the single daemon SQLite connection.

    FastAPI runs sync endpoints in a thread pool, so every access goes through
    one lock; queries are short and the DB is WAL, so contention is trivial.
    """

    def __init__(self, path: Path):
        import threading
        self._conn = open_db(path)
        self._lock = threading.Lock()

    def execute(self, sql, params=()):
        with self._lock, self._conn:
            return self._conn.execute(sql, params)

    def executemany(self, sql, rows):
        with self._lock, self._conn:
            return self._conn.executemany(sql, rows)

    def query(self, sql, params=()):
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql, params=()):
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def insert(self, sql, params=()):
        with self._lock, self._conn:
            return self._conn.execute(sql, params).lastrowid

    def close(self):
        with self._lock:
            self._conn.close()


def open_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    # check_same_thread=False: the Db wrapper serializes all access with its
    # own lock, and FastAPI's threadpool means callers arrive on many threads.
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    migrate(conn)
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    for i, script in enumerate(MIGRATIONS[version:], start=version + 1):
        with conn:
            conn.executescript(script)
            conn.execute(f"PRAGMA user_version = {i}")
