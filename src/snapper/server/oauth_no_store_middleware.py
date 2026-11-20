"""Pure-ASGI middleware enforcing no-store and no-referrer headers on OAuth surfaces.

Ensures every HTTP response on an OAuth surface path — including dependency
rejections (401/403/404/422/503), rate-limit responses (429), CORS errors, and
unhandled 500s emitted by Starlette's ``ServerErrorMiddleware`` — carries
``Cache-Control: no-store``, ``Pragma: no-cache``, and
``Referrer-Policy: no-referrer`` without overriding headers already set by a
downstream handler.
"""

from typing import Final

from fastapi import FastAPI
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp
from starlette.types import Message
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from snapper.server.oauth_paths import is_oauth_surface_path

_OAUTH_RESPONSE_HEADERS: Final[tuple[tuple[str, str], ...]] = (
    ("cache-control", "no-store"),
    ("pragma", "no-cache"),
    ("referrer-policy", "no-referrer"),
)


class OAuthNoStoreMiddleware:
    """Pure-ASGI middleware that injects missing cache and referrer headers on OAuth paths."""

    def __init__(self, app: ASGIApp) -> None:
        """Store the wrapped ASGI application.

        Args:
            app: Downstream ASGI application.
        """
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Inject missing no-store and no-referrer headers on OAuth HTTP responses.

        Args:
            scope: ASGI connection scope.
            receive: ASGI receive callable.
            send: ASGI send callable.
        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path", "")).split("?", 1)[0]
        if not is_oauth_surface_path(path):
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            """Add missing security headers to ``http.response.start`` messages.

            Args:
                message: Outgoing ASGI event message.
            """
            if message.get("type") == "http.response.start":
                message.setdefault("headers", [])
                headers = MutableHeaders(scope=message)
                for header_name, header_value in _OAUTH_RESPONSE_HEADERS:
                    headers.setdefault(header_name, header_value)
            await send(message)

        await self.app(scope, receive, send_with_headers)


class OAuthNoStoreFastAPI(FastAPI):
    """FastAPI application whose outermost middleware stack enforces OAuth headers.

    Starlette's ``ServerErrorMiddleware`` wraps all entries registered via
    ``app.add_middleware(...)``, so an unhandled exception producing a 500
    response would otherwise bypass user middlewares. Wrapping the built
    middleware stack here places :class:`OAuthNoStoreMiddleware` outside
    ``ServerErrorMiddleware`` so even unhandled 500s on OAuth surface paths
    receive ``Cache-Control: no-store``, ``Pragma: no-cache``, and
    ``Referrer-Policy: no-referrer`` while preserving the ``FastAPI`` type.
    """

    def build_middleware_stack(self) -> ASGIApp:
        """Wrap the complete Starlette/FastAPI middleware stack outermost.

        Returns:
            ASGI application wrapped by :class:`OAuthNoStoreMiddleware`.
        """
        return OAuthNoStoreMiddleware(super().build_middleware_stack())
