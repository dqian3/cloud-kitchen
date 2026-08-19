# cloud-kitchen

A job queue and hosted UI for running distributed-systems experiments on
gcloud, plus the shared orchestration library it is built on. Extracted from
`aspen-bft/scripts/benchmarks` (of which the VSAC repos carried a drifted
fork). The full design lives in the plan this repo was built from; the short
version:

- **`kitchen/`** — the library: remote execution backends (gcloud / ssh /
  local / fake), and later cluster lifecycle with leases and the dead-man
  switch, structured JSONL events, the sweep engine, and the project-adapter
  interface.
- **`kitchend/`** — the daemon: FastAPI + SQLite job queue, cluster manager
  with daemon-owned keep-alive, SSE progress, results catalog, MCP server.
  Runs as a systemd user unit on the workstation; expose the UI over your
  tailnet with `tailscale serve --bg 8321`.
- **`ui/`** — Vite + React frontend (placeholder page until built).

## Consumers

Consumer repos import the library through a small shim named `remote.py`
(path-inserted; override the checkout location with `KITCHEN_PATH`):

- `aspen-bft/scripts/benchmarks/remote.py` — site defaults from `ASPEN_*` env
- `vsac/{lazylog-rpc,corfu-cplusplus}/vsac-scripts/remote.py` — `VSAC_*` env

## Development

```bash
uv run --project kitchen --with pytest pytest kitchen/tests   # library tests
uv run --project kitchend kitchend serve                      # daemon on :8321
uv run --project kitchend kitchend status
```

Daemon config: `~/.cloud-kitchen/config.toml` (see `kitchend/src/kitchend/config.py`
for the format). State (SQLite DB, cluster state, archives) lives under
`~/.cloud-kitchen/`.
