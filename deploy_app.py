from __future__ import annotations

import logging
import os
from pathlib import Path

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.routing import Mount
from starlette.staticfiles import StaticFiles
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from shmarql.main import app_factory

log = logging.getLogger("matwerk.deploy")

# The public path prefix. Leading slash, no trailing slash.
PREFIX = "/" + os.environ.get("APP_PREFIX", "/matwerk").strip("/")
# Where the Dockerfile's `mkdocs build -d ...` wrote the site.
SITE_DIR = Path(os.environ.get("STATIC_SITE_PATH", "/app/src/site/matwerk"))


class StaticThenApp:
    """Serve `static`; when it has no such file, serve `app` for that request.

    StaticFiles signals a miss in two ways depending on how it is invoked: a 404
    response message, or a raised HTTPException(404). Both are handled, because
    mounting it as a bare sub-app bypasses the exception middleware that would
    normally turn the raised form into a response.
    """

    def __init__(self, static: ASGIApp, app: ASGIApp) -> None:
        self.static = static
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # StaticFiles answers anything that is not GET/HEAD with 405, not 404,
        # so a POST would never reach the app. SPARQL-over-POST is how clients
        # send queries too long for a URL, so those methods skip static
        # entirely -- a static file could not have served them in any case.
        if scope["type"] != "http" or scope.get("method") not in ("GET", "HEAD"):
            await self.app(scope, receive, send)
            return

        status: int | None = None
        forwarded = False

        async def capture(message: Message) -> None:
            nonlocal status, forwarded
            if message["type"] == "http.response.start":
                status = message["status"]
                if status == 404:
                    return  # hold back; the app may answer instead
                forwarded = True
            elif message["type"] == "http.response.body" and status == 404 and not forwarded:
                return
            await send(message)

        try:
            await self.static(scope, receive, capture)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
            status = 404

        if status == 404 and not forwarded:
            await self.app(scope, receive, send)


def build() -> Starlette:
    inner: ASGIApp = app_factory()
    if SITE_DIR.is_dir():
        inner = StaticThenApp(StaticFiles(directory=str(SITE_DIR), html=True), inner)
        log.info("serving %s first; the app answers what it does not contain", SITE_DIR)
    else:
        # Not fatal: the app still serves /sparql and renders docs/*.md itself.
        log.warning("static site %s not found; serving the app alone", SITE_DIR)
    log.info("mounting SHMARQL under %s/", PREFIX)
    return Starlette(routes=[Mount(PREFIX, app=inner)])


app = build()
