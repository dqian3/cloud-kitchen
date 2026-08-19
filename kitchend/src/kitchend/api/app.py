"""FastAPI application: core services on app.state, API routes, static UI."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from kitchend import __version__
from kitchend.config import Config
from kitchend.core import jobs
from kitchend.core.clusters import ClusterManager
from kitchend.core.db import Db
from kitchend.core.hub import EventHub
from kitchend.core.runner import JobRunner
from kitchend.core.scheduler import Scheduler

from .routes import router

UI_DIST = Path(__file__).resolve().parents[4] / "ui" / "dist"
UI_PLACEHOLDER = Path(__file__).resolve().parents[4] / "ui" / "index.html"


def create_app(config: Config) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.hub.bind_loop(asyncio.get_running_loop())
        # Jobs that were running under a previous daemon and whose pid is
        # gone become 'interrupted' (resumable); live ones keep running and
        # will be re-noticed only as log files — M1 does not re-adopt them.
        jobs.recover_orphans(app.state.db, app.state.hub)
        scheduler_task = asyncio.get_running_loop().create_task(
            app.state.scheduler.loop())
        app.state.hub.emit("daemon.started", version=__version__)
        try:
            yield
        finally:
            app.state.scheduler.stop()
            scheduler_task.cancel()
            await app.state.clusters.shutdown_all()
            app.state.hub.emit("daemon.stopped")

    app = FastAPI(title="cloud-kitchen", version=__version__, lifespan=lifespan)
    app.state.config = config
    app.state.db = Db(config.db_path)
    app.state.hub = EventHub(app.state.db)
    app.state.runner = JobRunner(config.jobs_dir)
    app.state.scheduler = Scheduler(config, app.state.db, app.state.hub,
                                    app.state.runner)
    app.state.clusters = ClusterManager(config, app.state.db, app.state.hub)

    @app.get("/api/health")
    def health():
        return {"status": "ok", "version": __version__}

    @app.get("/api/projects")
    def projects():
        return [
            {
                "name": p.name,
                "repo_path": str(p.repo_path),
                "runs_roots": list(p.runs_roots),
                "clusters": [c.name for c in p.clusters],
                "has_driver": bool(p.driver),
            }
            for p in config.projects
        ]

    app.include_router(router)

    if UI_DIST.is_dir():
        app.mount("/", StaticFiles(directory=UI_DIST, html=True), name="ui")
    else:
        @app.get("/", response_class=HTMLResponse)
        def placeholder():
            if UI_PLACEHOLDER.exists():
                return UI_PLACEHOLDER.read_text()
            return "<h1>cloud-kitchen</h1><p>UI not built yet.</p>"

    return app
