"""SQLite schema.

One writer: the daemon process (the MCP server runs in-process; the CLI talks
HTTP). WAL mode so readers never block on the writer.
"""

import sqlite3
from pathlib import Path

SCHEMA = """
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

    -- One row per job, for its whole life: waiting -> running -> done.
    -- A retry raises attempts and goes back to waiting; it never adds a row.
    CREATE TABLE jobs (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL REFERENCES projects(id),
        spec_json TEXT NOT NULL,
        cluster_id INTEGER REFERENCES clusters(id),
        run_dir TEXT,
        state TEXT NOT NULL DEFAULT 'waiting',   -- waiting | running | done
        outcome TEXT,                            -- done only: ok|degraded|failed|canceled
        priority INTEGER NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 20,
        next_attempt_at TEXT,                    -- waiting only: not before this
        last_error TEXT,
        pid INTEGER,
        events_offset INTEGER NOT NULL DEFAULT 0,
        progress_json TEXT,
        estimate_json TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        started_at TEXT,
        finished_at TEXT
    );
    CREATE INDEX idx_jobs_state ON jobs(state, cluster_id);

    -- One row per driver invocation: the retry history, without the rows.
    CREATE TABLE job_attempts (
        id INTEGER PRIMARY KEY,
        job_id INTEGER NOT NULL REFERENCES jobs(id),
        n INTEGER NOT NULL,
        started_at TEXT NOT NULL DEFAULT (datetime('now')),
        finished_at TEXT,
        exit_code INTEGER,
        points INTEGER,                          -- points this attempt finished
        error TEXT
    );
    CREATE INDEX idx_job_attempts_job ON job_attempts(job_id, n);

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

    -- The ledger: one row per sweep dir, and only once it holds a point.
    -- status describes the data (ok|degraded|failed), never the job.
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
        job_id INTEGER REFERENCES jobs(id),      -- provenance: last writer
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

    CREATE TABLE saved_sweeps (
        id INTEGER PRIMARY KEY,
        project_id INTEGER NOT NULL REFERENCES projects(id),
        name TEXT NOT NULL,
        params_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE (project_id, name)
    );
"""

SCHEMA_VERSION = 1


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
    """Create the schema, rebuilding the file if it holds an older one.

    The daemon's tables are a working record of what it is doing now; the
    part worth keeping is the ledger, and that is rebuilt from the run dirs
    on disk with `runs/scan`. So a schema change drops and recreates rather
    than migrating.
    """
    if conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION:
        return
    with conn:
        for (name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%'").fetchall():
            conn.execute(f"DROP TABLE IF EXISTS {name}")
        conn.executescript(SCHEMA)
        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
