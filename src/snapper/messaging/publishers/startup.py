"""Classify upstream outages at the market-data publisher's connect boundary."""

import asyncio
import errno
import socket
import ssl

import aiohttp
import ccxt
import httpx
import requests

from snapper.core.process_exit import UpstreamUnavailableError
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.network.egress_context import resolve_egress_traffic
from snapper.infrastructure.network.egress_exceptions import AllRoutesQuarantinedError
from snapper.infrastructure.network.egress_pool import get_egress_pool
from snapper.infrastructure.network.egress_pool import public_egress_pool

_UPSTREAM_HTTP_STATUSES = frozenset({502, 503, 504})
_NETWORK_ERRNOS = frozenset(
    {errno.EHOSTUNREACH, errno.ENETDOWN, errno.ENETUNREACH, errno.ETIMEDOUT}
)
_SOCKS_OUTAGE_REPLIES = frozenset(
    {
        "Proxy Server could not connect: General SOCKS server failure.",
        "Proxy Server could not connect: Network unreachable.",
        "Proxy Server could not connect: Host unreachable.",
        "Proxy Server could not connect: Connection refused.",
        "Proxy Server could not connect: TTL expired.",
    }
)
_NETWORK_ERRORS = (
    aiohttp.ClientConnectionError,
    ccxt.NetworkError,
    httpx.NetworkError,
    httpx.TimeoutException,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    socket.gaierror,
    TimeoutError,
    BrokenPipeError,
    ConnectionAbortedError,
    ConnectionRefusedError,
    ConnectionResetError,
)
_LOCAL_OR_REJECTED_ERRORS = (
    aiohttp.ClientSSLError,
    ccxt.ArgumentsRequired,
    ccxt.AuthenticationError,
    ccxt.BadRequest,
    ccxt.InvalidNonce,
    ccxt.InvalidProxySettings,
    ccxt.RateLimitExceeded,
    httpx.LocalProtocolError,
    httpx.PoolTimeout,
    requests.exceptions.SSLError,
    ssl.SSLError,
    AssertionError,
    AttributeError,
    LookupError,
    TypeError,
    ValueError,
)


def _http_failure_is_unavailable(error: BaseException) -> bool | None:
    """Check HTTP rejections and SOCKS replies before generic network errors.

    HTTPX proxy errors flatten both CONNECT status and SOCKS replies into
    text. Only accept gateway status codes and known SOCKS reachability
    replies so proxy authentication and policy rejections remain failures.

    Args:
        error: Error from the connection exception chain.

    Returns:
        Whether the HTTP or proxy error signals an outage, or None for
        other error types.
    """
    match error:
        case (
            aiohttp.ClientResponseError(status=status)
            | httpx.HTTPStatusError(response=httpx.Response(status_code=status))
            | requests.exceptions.HTTPError(response=requests.Response(status_code=status))
        ):
            return status in _UPSTREAM_HTTP_STATUSES
        case httpx.ProxyError():
            detail = str(error)
            status_text = detail.partition(" ")[0]
            return detail in _SOCKS_OUTAGE_REPLIES or status_text in {"502", "503", "504"}
        case _:
            return None


def _is_upstream_unavailable(error: BaseException) -> bool:
    """Recognize venue or proxy outages through the active exception chain.

    A typed status takes precedence over broad wrappers such as CCXT's
    ExchangeNotAvailable, which can wrap a 4xx. Walutomat wraps any startup
    failure in a generic ConnectionError, so its cause supplies the evidence.

    Args:
        error: Exception raised by the exchange client during startup.

    Returns:
        Whether the connection failed because the upstream was unavailable.
    """
    current: BaseException | None = error
    seen: set[int] = set()
    unavailable = False
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, _LOCAL_OR_REJECTED_ERRORS):
            return False
        http_unavailable = _http_failure_is_unavailable(current)
        if http_unavailable is not None:
            return http_unavailable
        next_error = current.__cause__
        if next_error is None and not current.__suppress_context__:
            next_error = current.__context__
        if (
            isinstance(current, _NETWORK_ERRORS)
            or (isinstance(current, OSError) and current.errno in _NETWORK_ERRNOS)
            or (type(current) is ConnectionError and next_error is None)
        ):
            unavailable = True
        current = next_error
    return unavailable


async def connect_publisher_exchange(client: ExchangeClientBase) -> None:
    """Connect a market-data publisher, waiting in-process for public VPN egress.

    Route exhaustion is retried without leaving this process. Upstream
    network failures retain their existing supervisor backoff.

    Args:
        client: The publisher's exchange client.

    Raises:
        UpstreamUnavailableError: The venue or proxy could not be reached.
        Exception: Other startup connection errors propagate unchanged.
    """
    while True:
        try:
            await client.connect()
            return
        except AllRoutesQuarantinedError:
            exchange, traffic_class = resolve_egress_traffic()
            if traffic_class == "private":
                raise
            earliest = public_egress_pool(get_egress_pool()).earliest_release_in_seconds(
                exchange=exchange, traffic_class="public"
            )
            await asyncio.sleep(max(earliest or 0.0, 1.0))
        except Exception as error:
            if _is_upstream_unavailable(error):
                raise UpstreamUnavailableError(str(error)) from error
            raise
