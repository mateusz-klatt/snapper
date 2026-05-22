"""Egress-pool-aware ``httpx`` transport for HTTP-polling publishers.

Phase B'.7 F3 of plan_2026_05_22_kraken_equities_us_tunnel + the
``proprietary/memory/project_2026_05_22_b_prime_5_shipped.md``
follow-on. The Kraken SDK shim in ``kraken_sdk_patches.py`` routes
every WebSocket handshake through the egress pool; Walutomat
publishes via ``httpx.AsyncClient`` so needs an analogous transport
hook.

The transport:

1. Reads ``_CURRENT_PUBLISHER`` (shared with the Kraken shim) to get
   the calling publisher's exchange name.
2. Reserves a pool route with ``purpose="http"``.
3. Picks a cached ``httpx.AsyncHTTPTransport`` keyed on the route's
   ``proxy_url`` (None for direct, ``socks5h://...`` for SOCKS5) —
   so successive requests on the same route reuse the connection
   pool.
4. Forwards the request.
5. On ``httpx.ConnectError`` / ``ConnectTimeout``: quarantines the
   borrowed route then re-raises, so the next request reserves a
   fresh route (failover to ``default`` direct when every SOCKS5
   route is quarantined).
6. Releases the reservation in ``finally``.

Pool-disabled path (``get_egress_pool()`` returns ``None`` or empty):
forward to the cached direct transport — no reservation, no
failover, identical to a bare ``httpx.AsyncClient`` on the host's
default route. Preserves byte-for-byte behaviour when the operator
runs without the sidecar.
"""

from typing import Any

import httpx
from loguru import logger

from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
from snapper.infrastructure.network.egress_pool import get_egress_pool

_HTTP_CONNECT_ERROR_QUARANTINE_S: float = 60.0
"""Quarantine duration (seconds) for an egress route that failed a connect attempt.

Mirrors ``_CLOSE_1015_QUARANTINE_S`` in the Spot shim — long enough
to take a single-PoP outage out of the rotation, short enough to let
a transient DNS/connect blip recover before the next request cycle.
"""


class PooledAsyncTransport(httpx.AsyncBaseTransport):
    """``httpx.AsyncBaseTransport`` that reserves an egress-pool route per request.

    Unlike a per-connection reservation (one route held for the
    entire ``AsyncClient`` lifetime), per-request reservation lets a
    Walutomat polling cycle hop between routes the moment one
    quarantines — failover is sub-second instead of waiting for
    ``walutomat:_handle_http_error`` to count 5 consecutive errors
    before tearing down + reconnecting the client.

    The trade-off is one ``pool.reserve()`` per HTTP request. Reserve
    is a lock-acquire + dict-lookup so the cost is sub-microsecond,
    negligible against any real HTTP round-trip.
    """

    def __init__(
        self,
        *,
        default_exchange_tag: str = "walutomat",
        transport_factory: Any | None = None,
    ) -> None:
        """Initialise the transport.

        Args:
            default_exchange_tag: Exchange name used when
                ``_CURRENT_PUBLISHER.get()`` is ``None``. Lets the
                transport route Walutomat requests through the pool
                even when invoked outside a publisher's ContextVar
                span (e.g. one-shot CLI tooling that hits Walutomat
                without spawning the full publisher).
            transport_factory: Override for the underlying
                ``httpx.AsyncHTTPTransport`` constructor. Tests
                inject a fake factory; production keeps the default
                (``httpx.AsyncHTTPTransport``).
        """
        self._default_exchange_tag = default_exchange_tag
        self._transport_factory: Any = transport_factory or httpx.AsyncHTTPTransport
        self._transports: dict[str | None, httpx.AsyncBaseTransport] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Reserve a route, forward via the matched transport, quarantine on connect errors.

        Args:
            request: The ``httpx.Request`` to dispatch.

        Returns:
            The response from the underlying transport.

        Raises:
            httpx.ConnectError: Re-raised after the borrowed route
                is quarantined so the caller's retry hits a fresh
                route (or the direct fallback).
            httpx.ConnectTimeout: Same handling as ``ConnectError``.
            Exception: Any other exception from the underlying
                transport propagates unchanged. The reservation is
                still released in the ``finally`` block.
        """
        pool = get_egress_pool()
        if pool is None or pool.size() == 0:
            direct_transport = self._get_or_create_transport(None)
            return await direct_transport.handle_async_request(request)

        publisher = _CURRENT_PUBLISHER.get()
        exchange_name = (
            publisher._get_exchange_name() if publisher is not None else self._default_exchange_tag
        )
        reservation = pool.reserve(exchange=exchange_name, purpose="http")
        transport = self._get_or_create_transport(reservation.proxy_url)
        try:
            return await transport.handle_async_request(request)
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            reservation.quarantine(
                _HTTP_CONNECT_ERROR_QUARANTINE_S,
                reason="http-connect-error",
            )
            logger.warning(
                f"PooledAsyncTransport: connect error on route={reservation.route_id} "
                f"exchange={exchange_name} — quarantined for "
                f"{_HTTP_CONNECT_ERROR_QUARANTINE_S}s. Cause: {exc!r}"
            )
            raise
        finally:
            reservation.release()

    def _get_or_create_transport(self, proxy_url: str | None) -> httpx.AsyncBaseTransport:
        """Return the cached transport for ``proxy_url``, building it lazily.

        Args:
            proxy_url: ``None`` for direct, ``socks5h://...`` for
                SOCKS5 (matches the keys returned by
                ``EgressReservation.proxy_url``).

        Returns:
            A cached or freshly-built ``httpx.AsyncHTTPTransport``
            (or fake-factory-produced transport in tests).
        """
        if proxy_url not in self._transports:
            self._transports[proxy_url] = self._transport_factory(proxy=proxy_url)
        return self._transports[proxy_url]

    async def aclose(self) -> None:
        """Close every cached underlying transport.

        Called from ``httpx.AsyncClient.aclose``. Idempotent — the
        cache is cleared after close so a second call is a no-op.
        """
        for transport in self._transports.values():
            await transport.aclose()
        self._transports.clear()
