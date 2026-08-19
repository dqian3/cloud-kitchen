"""FastAPI application: API routes + static UI."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from kitchend import __version__
from kitchend.config import Config
from kitchend.core import db

UI_DIST = Path(__file__).resolve().parents[4] / "ui" / "dist"
UI_PLACEHOLDER = Path(__file__).resolve().parents[4] / "ui" / "index.html"


def create_app(config: Config) -> FastAPI:
    app = FastAPI(title="cloud-kitchen", version=__version__)
    app.state.config = config
    app.state.db = db.open_db(config.db_path)

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
            }
            for p in config.projects
        ]

    if UI_DIST.is_dir():
        app.mount("/", StaticFiles(directory=UI_DIST, html=True), name="ui")
    else:
        @app.get("/", response_class=HTMLResponse)
        def placeholder():
            if UI_PLACEHOLDER.exists():
                return UI_PLACEHOLDER.read_text()
            return "<h1>cloud-kitchen</h1><p>UI not built yet.</p>"

    return app
