"""Pure-ASGI middleware enforcing OAuth response headers and credential error containment.

Ensures every HTTP response on an OAuth surface path — including dependency
rejections (401/403/404/422/503), rate-limit responses (429), CORS errors, and
unhandled 500s emitted by Starlette's ``ServerErrorMiddleware`` — carries
``Cache-Control: no-store``, ``Pragma: no-cache``, and
``Referrer-Policy: no-referrer`` without overriding headers already set by a
downstream handler.

On an OAuth credential path an unhandled ``Exception`` never leaves this
middleware. ``ServerErrorMiddleware`` always re-raises after sending its 500,
and uvicorn would then log ``str(exc)`` with the full traceback, which on a
credential endpoint can echo submitted secrets. Instead a single ``error``
record is written that names only the exception class, the request method, the
``ascii()``-escaped path, and a random ``error_id``; a plain 500 is sent when no
response has started yet; and the call returns normally. ``BaseException``
subclasses that are not ``Exception`` subclasses, such as
``asyncio.CancelledError``, still propagate. Surface-only and non-OAuth paths
re-raise exactly as before.
"""

from typing import Final
from uuid import uuid4

from fastapi import FastAPI
from loguru import logger
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp
from starlette.types import Message
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from snapper.server.oauth_paths import is_oauth_credential_path
from snapper.server.oauth_paths import is_oauth_surface_path

_OAUTH_RESPONSE_HEADERS: Final[tuple[tuple[str, str], ...]] = (
    ("cache-control", "no-store"),
    ("pragma", "no-cache"),
    ("referrer-policy", "no-referrer"),
)

_INTERNAL_ERROR_BODY: Final[bytes] = b"Internal Server Error"

_INTERNAL_ERROR_HEADERS: Final[tuple[tuple[bytes, bytes], ...]] = (
    (b"content-type", b"text/plain; charset=utf-8"),
    (b"content-length", str(len(_INTERNAL_ERROR_BODY)).encode("latin-1")),
)


def _inject_missing_headers(message: Message) -> None:
    """Add every missing OAuth cache and referrer header to a response start message.

    Args:
        message: Outgoing ``http.response.start`` ASGI message, mutated in place.
    """
    message.setdefault("headers", [])
    headers = MutableHeaders(scope=message)
    for header_name, header_value in _OAUTH_RESPONSE_HEADERS:
        headers.setdefault(header_name, header_value)


class _OAuthResponseSend:
    """ASGI send wrapper that injects OAuth headers and records the response start.

    Attributes:
        response_started: True once ``http.response.start`` has been sent.
    """

    def __init__(self, send: Send) -> None:
        """Store the downstream send callable and reset the start flag.

        Args:
            send: ASGI send callable of the enclosing server.
        """
        self._send = send
        self.response_started = False

    async def __call__(self, message: Message) -> None:
        """Forward ``message`` after injecting headers into a response start.

        Args:
            message: Outgoing ASGI event message.
        """
        if message.get("type") == "http.response.start":
            _inject_missing_headers(message)
            self.response_started = True
        await self._send(message)


def _log_credential_failure(exc: Exception, scope: Scope, path: str) -> None:
    """Log an unhandled credential-path exception without any of its content.

    The exception message, arguments, and traceback are deliberately omitted
    because on a credential endpoint they can echo submitted secrets. The path
    is ``ascii()``-escaped so a control character cannot forge a log line.

    Args:
        exc: Exception raised by the downstream application.
        scope: ASGI connection scope of the failed request.
        path: Request path without query string.
    """
    logger.error(
        "Unhandled {exc_type} on OAuth credential request {method} {path} error_id={error_id}",
        exc_type=type(exc).__name__,
        method=str(scope.get("method", "")),
        path=ascii(path),
        error_id=uuid4().hex,
    )


async def _send_internal_server_error(send: Send) -> None:
    """Send a complete plain-text 500 response.

    Args:
        send: Header-injecting ASGI send callable.
    """
    await send(
        {
            "type": "http.response.start",
            "status": 500,
            "headers": list(_INTERNAL_ERROR_HEADERS),
        }
    )
    await send({"type": "http.response.body", "body": _INTERNAL_ERROR_BODY})


class OAuthNoStoreMiddleware:
    """Pure-ASGI middleware enforcing OAuth response headers and credential error containment.

    Every HTTP response on an OAuth surface path receives any missing
    ``Cache-Control: no-store``, ``Pragma: no-cache``, and
    ``Referrer-Policy: no-referrer`` header. On an OAuth credential path an
    unhandled ``Exception`` is contained: only its class, the method, the
    escaped path, and a random ``error_id`` are logged; a plain 500 carrying the
    same headers is sent when no response has started; and the call returns
    normally, so uvicorn never logs the exception message or traceback. A
    response that started but did not complete is left as is, and uvicorn then
    logs only that the ASGI callable returned without completing the response.
    """

    def __init__(self, app: ASGIApp) -> None:
        """Store the wrapped ASGI application.

        Args:
            app: Downstream ASGI application.
        """
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Inject OAuth headers and contain unhandled exceptions on credential paths.

        Exceptions on non-credential paths, and any ``BaseException`` that is
        not an ``Exception`` on credential paths, propagate unchanged.

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

        oauth_send = _OAuthResponseSend(send)
        if not is_oauth_credential_path(path):
            await self.app(scope, receive, oauth_send)
            return

        failed = await self._downstream_failed(scope, receive, oauth_send, path)
        if failed and not oauth_send.response_started:
            await _send_internal_server_error(oauth_send)

    async def _downstream_failed(
        self,
        scope: Scope,
        receive: Receive,
        oauth_send: _OAuthResponseSend,
        path: str,
    ) -> bool:
        """Call the downstream application and log any ``Exception`` it raises.

        Only ``Exception`` is caught so cancellation and other ``BaseException``
        subclasses keep propagating. The caller sends any fallback response
        after this ``except`` block has exited, so a failing fallback send
        carries no implicit ``__context__`` link to the original exception.

        Args:
            scope: ASGI connection scope.
            receive: ASGI receive callable.
            oauth_send: Header-injecting send wrapper that records the response start.
            path: Request path without query string.

        Returns:
            True when the downstream application raised an ``Exception``.
        """
        try:
            await self.app(scope, receive, oauth_send)
        except Exception as exc:
            _log_credential_failure(exc, scope, path)
            return True
        return False


class OAuthNoStoreFastAPI(FastAPI):
    """FastAPI application whose outermost middleware stack enforces OAuth headers.

    Starlette's ``ServerErrorMiddleware`` wraps all entries registered via
    ``app.add_middleware(...)``, so an unhandled exception producing a 500
    response would otherwise bypass user middlewares. Wrapping the built
    middleware stack here places :class:`OAuthNoStoreMiddleware` outside
    ``ServerErrorMiddleware`` so even unhandled 500s on OAuth surface paths
    receive ``Cache-Control: no-store``, ``Pragma: no-cache``, and
    ``Referrer-Policy: no-referrer`` while preserving the ``FastAPI`` type.
    This placement also lets the middleware absorb the exception that
    ``ServerErrorMiddleware`` re-raises on an OAuth credential path, so it
    never reaches uvicorn's error log.
    """

    def build_middleware_stack(self) -> ASGIApp:
        """Wrap the complete Starlette/FastAPI middleware stack outermost.

        Returns:
            ASGI application wrapped by :class:`OAuthNoStoreMiddleware`.
        """
        return OAuthNoStoreMiddleware(super().build_middleware_stack())
