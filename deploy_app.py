from __future__ import annotations

import hashlib
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
APP_ROOTS = ("static", "query", "sparql", "spo", "lodeon", "entity", "api",
             "shmarql", "story")
# The opening delimiter must include the backtick: the LODEON editor builds its
# API URLs as template literals, e.g. `/api/sparql/${encodeURIComponent(ref)}`.
# Missing those sent every story query to the domain root, which is what made
# the editor report "The SPARQL request failed".
_ABSOLUTE = re.compile(
    r"""(["'(`])/(""" + "|".join(APP_ROOTS) + r""")(?=["'`/?#)]|$)"""
)
# Only these are rewritten. application/json is included for one specific reason:
# the LODEON editor fetches /api/config and takes URLs straight out of the JSON --
# welcomeStory.url, previewURL, publishURL, the per-profile sparqlUrl, configURL
# and expandURL. Those never pass through JavaScript, so leaving JSON alone sent
# every one of them to the domain root, which is what produced
# "Welcome Story loading failed ... 404".
REWRITABLE = ("text/html", "javascript", "text/css", "application/json")
# ...but never query results or RDF payloads: a literal in someone's data can look
# like a path, and rewriting it would corrupt the response. Checked first.
NEVER_REWRITE = ("sparql-results", "turtle", "n-triples", "n-quads", "trig",
                 "rdf+xml", "ld+json")


# WORKAROUND -- delete this block once the Caddy rule is scoped.
#
# The site's reverse proxy answers ANY request whose Accept header mentions
# text/turtle with "302 Found -> https://ise-fizkarlsruhe.github.io/nfdicore/
# 3.0.4/ontology.ttl", whatever the path, including POSTs to /matwerk/sparql.
# The query editor asks for
#   application/sparql-results+json, text/turtle;q=0.9, */*;q=0.1
# so every query came back as the NFDIcore ontology instead of its results.
#
# Dropping turtle from the Accept the browser sends keeps the request on this
# host; "*/*;q=0.1" survives and is not matched by the rule. This only helps our
# own pages -- any other SPARQL client asking for Turtle, and every CONSTRUCT or
# DESCRIBE query, stays broken until the Caddyfile narrows that rule to the
# ontology namespace.
DROP_TURTLE_ACCEPT = os.environ.get("DROP_TURTLE_ACCEPT", "1") != "0"
_TURTLE_ACCEPT = re.compile(r"text/turtle;q=0\.9,\s*")

# The site header's two leftmost links are a bare href="/", which the rewrite
# above cannot help with: there is no path segment to recognise. At the domain
# root they lead nowhere. The brand sends people to the upstream project, and
# the first nav item becomes the way back to this site's own home page.
SHMARQL_HOME = "https://shmarql.com/"
_NAV_LINKS = (
    ('<a href="/" class="site-title">', f'<a href="{SHMARQL_HOME}" class="site-title">'),
    ('<a href="/" class="nav-item">Docs</a>', '<a href="{prefix}/" class="nav-item">MatWerk KG</a>'),
)


class PrefixAbsoluteUrls:
    """Rewrite the app's root-absolute URLs so they sit under the mount."""

    def __init__(self, app: ASGIApp, prefix: str) -> None:
        self.app = app
        self.prefix = prefix

    def _body(self, raw: bytes) -> bytes:
        text = raw.decode("utf-8", "replace")
        text = _ABSOLUTE.sub(rf"\1{self.prefix}/\2", text)
        for old, new in _NAV_LINKS:
            text = text.replace(old, new.replace("{prefix}", self.prefix))
        if DROP_TURTLE_ACCEPT:
            text = _TURTLE_ACCEPT.sub("", text)
        return text.encode("utf-8")

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
                rewriting = (
                    any(kind in content_type for kind in REWRITABLE)
                    and not any(kind in content_type for kind in NEVER_REWRITE)
                )
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
                # The validators belong to the file on disk, which this rewrite has
                # just changed. Reusing them makes a browser revalidate with
                # If-None-Match, get 304, and keep a stale body forever -- which is
                # how an already-deployed fix kept appearing broken. Replace the
                # ETag with one derived from what we actually send, and drop
                # Last-Modified, whose timestamp is now meaningless.
                drop = ("content-length", "etag", "last-modified")
                headers = [(k, v) for k, v in start["headers"]
                           if k.decode("latin-1").lower() not in drop]
                digest = hashlib.sha256(body).hexdigest()[:32]
                headers.append((b"etag", f'"{digest}"'.encode("latin-1")))
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
