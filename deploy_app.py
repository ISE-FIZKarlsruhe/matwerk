from __future__ import annotations

import logging
import os
import re
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


# Root-absolute paths the app hardcodes in redirects, templates and its own JS
# (e.g. RedirectResponse("/query?..."), src="/static/js/query.js", fetch("/sparql")).
# Under a mount they point at the domain root, so the page renders but its assets
# 404 and its forms post nowhere.
APP_ROOTS = ("static", "query", "sparql", "spo", "lodeon", "entity", "api", "shmarql")
_ABSOLUTE = re.compile(
    r"""(["'(])/(""" + "|".join(APP_ROOTS) + r""")(?=["'/?#)]|$)"""
)
# Only these are rewritten. SPARQL results must stream through untouched: a
# literal in someone's data could look like a path and would be corrupted.
REWRITABLE = ("text/html", "javascript", "text/css")


class PrefixAbsoluteUrls:
    """Rewrite the app's root-absolute URLs so they sit under the mount."""

    def __init__(self, app: ASGIApp, prefix: str) -> None:
        self.app = app
        self.prefix = prefix

    def _body(self, raw: bytes) -> bytes:
        text = raw.decode("utf-8", "replace")
        return _ABSOLUTE.sub(rf"\1{self.prefix}/\2", text).encode("utf-8")

    def _location(self, value: str) -> str:
        if value.startswith("/") and not value.startswith(self.prefix + "/"):
            return self.prefix + value
        return value

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start: Message | None = None
        chunks: list[bytes] = []
        rewriting = False

        async def capture(message: Message) -> None:
            nonlocal start, rewriting
            if message["type"] == "http.response.start":
                headers = []
                content_type = ""
                for key, value in message["headers"]:
                    name = key.decode("latin-1").lower()
                    if name == "location":
                        value = self._location(value.decode("latin-1")).encode("latin-1")
                    elif name == "content-type":
                        content_type = value.decode("latin-1").lower()
                    headers.append((key, value))
                rewriting = any(kind in content_type for kind in REWRITABLE)
                message = {**message, "headers": headers}
                if not rewriting:
                    await send(message)
                    return
                start = message  # held until the body is complete
                return

            if message["type"] == "http.response.body" and rewriting:
                chunks.append(message.get("body", b""))
                if message.get("more_body"):
                    return
                body = self._body(b"".join(chunks))
                assert start is not None
                headers = [(k, v) for k, v in start["headers"]
                           if k.decode("latin-1").lower() != "content-length"]
                headers.append((b"content-length", str(len(body)).encode("latin-1")))
                await send({**start, "headers": headers})
                await send({"type": "http.response.body", "body": body, "more_body": False})
                return

            await send(message)

        await self.app(scope, receive, capture)


def build() -> Starlette:
    inner: ASGIApp = PrefixAbsoluteUrls(app_factory(), PREFIX)
    if SITE_DIR.is_dir():
        inner = StaticThenApp(StaticFiles(directory=str(SITE_DIR), html=True), inner)
        log.info("serving %s first; the app answers what it does not contain", SITE_DIR)
    else:
        # Not fatal: the app still serves /sparql and renders docs/*.md itself.
        log.warning("static site %s not found; serving the app alone", SITE_DIR)
    log.info("mounting SHMARQL under %s/", PREFIX)
    return Starlette(routes=[Mount(PREFIX, app=inner)])


app = build()
