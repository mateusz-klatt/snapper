"""Strict egress routing helpers for Kraken REST calls."""

import contextlib
import errno
import socket
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import MutableMapping
from contextlib import AbstractContextManager
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Literal
from typing import cast

import requests
import urllib3.exceptions

from snapper.infrastructure.network.egress_context import TrafficClass
from snapper.infrastructure.network.egress_context import current_egress_identity
from snapper.infrastructure.network.egress_context import egress_identity
from snapper.infrastructure.network.egress_pool import get_egress_pool
from snapper.infrastructure.network.egress_reservation import EgressReservation

KrakenRestOperationKind = Literal["public_read", "private_idempotent_read", "private_mutation"]
KrakenRestIdempotency = Literal["idempotent_read", "mutation"]

_KRAKEN_REST_CONNECT_ERROR_QUARANTINE_S = 60.0
_PRESEND_ERRNOS = frozenset(
    {
        errno.EADDRNOTAVAIL,
        errno.ECONNREFUSED,
        errno.EHOSTUNREACH,
        errno.ENETUNREACH,
        errno.ETIMEDOUT,
    }
)
_MISSING = object()


@dataclass(frozen=True)
class KrakenRestOperationClassification:
    """Routing classification for one Kraken REST operation.

    Attributes:
        operation: Stable operation name used by call sites.
        traffic_class: Public market-data or private authenticated traffic.
        idempotency: Whether the call is an idempotent read or a mutation.
        private_fallback_allowed: True only for private idempotent reads.
    """

    operation: str
    traffic_class: TrafficClass
    idempotency: KrakenRestIdempotency
    private_fallback_allowed: bool


@dataclass(frozen=True)
class RestProxyTarget:
    """Mutable proxy surface for a blocking REST SDK client.

    Attributes:
        client: External SDK client that owns the HTTP session.
        session_attr: Attribute containing the ``requests.Session``.
        proxy_attr: Optional SDK-level proxy attribute to keep in sync.
        proxy_attr_shape: ``"mapping"`` for CCXT, ``"url"`` for Kraken SDK.
    """

    client: object
    session_attr: str
    proxy_attr: str | None
    proxy_attr_shape: Literal["mapping", "url"]


@dataclass(frozen=True)
class _ClientProxyState:
    """Snapshot of an SDK client's proxy attribute."""

    value: object
    existed: bool


@dataclass(frozen=True)
class _SessionProxyState:
    """Snapshot of a ``requests.Session`` proxy state."""

    session: requests.Session
    proxies: MutableMapping[str, str]
    previous_proxies: dict[str, str]
    previous_trust_env: bool | None


_CLASSIFICATION_BY_KIND: dict[KrakenRestOperationKind, KrakenRestOperationClassification] = {
    "public_read": KrakenRestOperationClassification(
        operation="public_read",
        traffic_class="public",
        idempotency="idempotent_read",
        private_fallback_allowed=False,
    ),
    "private_idempotent_read": KrakenRestOperationClassification(
        operation="private_idempotent_read",
        traffic_class="private",
        idempotency="idempotent_read",
        private_fallback_allowed=True,
    ),
    "private_mutation": KrakenRestOperationClassification(
        operation="private_mutation",
        traffic_class="private",
        idempotency="mutation",
        private_fallback_allowed=False,
    ),
}


def classify_kraken_rest_operation(
    kind: KrakenRestOperationKind,
    *,
    operation: str,
) -> KrakenRestOperationClassification:
    """Return the routing classification for a Kraken REST call site.

    Args:
        kind: Coarse operation kind assigned by the call site.
        operation: Stable call-site operation name to stamp into the
            returned classification.

    Returns:
        Classification with traffic class, idempotency, and fallback policy.
    """
    base = _CLASSIFICATION_BY_KIND[kind]
    return KrakenRestOperationClassification(
        operation=operation,
        traffic_class=base.traffic_class,
        idempotency=base.idempotency,
        private_fallback_allowed=base.private_fallback_allowed,
    )


def ccxt_proxy_target(client: object) -> RestProxyTarget:
    """Build a proxy target for a CCXT REST client.

    Args:
        client: CCXT exchange instance.

    Returns:
        Proxy target using ``client.session`` and ``client.proxies``.
    """
    return RestProxyTarget(
        client=client,
        session_attr="session",
        proxy_attr="proxies",
        proxy_attr_shape="mapping",
    )


def spot_sdk_proxy_target(client: object) -> RestProxyTarget:
    """Build a proxy target for a python-kraken-sdk Spot REST client.

    Args:
        client: Spot SDK ``Trade`` or ``User`` client.

    Returns:
        Proxy target for the Spot client's private proxy/session fields.
    """
    return RestProxyTarget(
        client=client,
        session_attr="_SpotClient__session",
        proxy_attr="_SpotClient__proxy",
        proxy_attr_shape="url",
    )


def futures_sdk_proxy_target(client: object) -> RestProxyTarget:
    """Build a proxy target for a python-kraken-sdk Futures REST client.

    Args:
        client: Futures SDK ``Market``, ``Trade``, or ``User`` client.

    Returns:
        Proxy target for the Futures client's private proxy/session fields.
    """
    return RestProxyTarget(
        client=client,
        session_attr="_FuturesClient__session",
        proxy_attr="_FuturesClient__proxy",
        proxy_attr_shape="url",
    )


def route_kraken_rest_sync_call[ResultT](
    *,
    exchange: str,
    operation: str,
    kind: KrakenRestOperationKind,
    target: RestProxyTarget,
    proxy_lock: AbstractContextManager[object],
    sync_call: Callable[[], ResultT],
) -> ResultT:
    """Route one blocking Kraken REST call according to its safety policy.

    Args:
        exchange: Venue tag used by the egress pool.
        operation: Stable operation name for egress identity and tests.
        kind: Call-site classification.
        target: External REST client whose proxy state should be scoped.
        proxy_lock: Per-client lock guarding mutable SDK/session proxy state.
        sync_call: Blocking SDK call to execute.

    Returns:
        The SDK call result.

    Raises:
        Exception: Any SDK exception not eligible for private read fallback,
            or the fallback attempt's exception.
    """
    active_identity = current_egress_identity()
    if active_identity is not None and active_identity.traffic_class == "private":
        return _route_under_active_private_identity(
            exchange=exchange,
            target=target,
            proxy_lock=proxy_lock,
            sync_call=sync_call,
            kind=kind,
        )
    classification = classify_kraken_rest_operation(kind, operation=operation)
    with egress_identity(
        exchange=exchange,
        traffic_class=classification.traffic_class,
        owner="kraken_rest",
        operation=operation,
    ):
        if kind == "public_read":
            return _run_public_read(
                exchange=exchange,
                target=target,
                proxy_lock=proxy_lock,
                sync_call=sync_call,
            )
        if kind == "private_idempotent_read":
            return _run_private_idempotent_read(
                exchange=exchange,
                target=target,
                proxy_lock=proxy_lock,
                sync_call=sync_call,
            )
        return _run_private_mutation(
            exchange=exchange,
            target=target,
            proxy_lock=proxy_lock,
            sync_call=sync_call,
        )


def _route_under_active_private_identity[ResultT](
    *,
    exchange: str,
    target: RestProxyTarget,
    proxy_lock: AbstractContextManager[object],
    sync_call: Callable[[], ResultT],
    kind: KrakenRestOperationKind,
) -> ResultT:
    """Route a REST call while preserving an outer private identity.

    Args:
        exchange: Venue tag used by the egress pool.
        target: External REST client whose proxy state should be scoped.
        proxy_lock: Per-client lock guarding mutable SDK/session proxy state.
        sync_call: Blocking SDK call to execute.
        kind: Call-site classification.

    Returns:
        Result returned by the SDK call.
    """
    if kind == "public_read":
        return _run_private_direct_without_reservation(
            target=target,
            proxy_lock=proxy_lock,
            sync_call=sync_call,
        )
    if kind == "private_idempotent_read":
        return _run_private_idempotent_read(
            exchange=exchange,
            target=target,
            proxy_lock=proxy_lock,
            sync_call=sync_call,
        )
    return _run_private_mutation(
        exchange=exchange,
        target=target,
        proxy_lock=proxy_lock,
        sync_call=sync_call,
    )


def is_provable_presend_connect_error(error: BaseException) -> bool:
    """Return whether an exception proves failure before request bytes were sent.

    Args:
        error: Exception raised by CCXT, requests, urllib3, or the OS layer.

    Returns:
        True only for connect-timeout, DNS, connection-refused, and related
        setup failures found in the exception cause chain. Generic connection
        errors, read timeouts, and SDK network wrappers without a specific
        pre-send cause return False.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(
            current,
            requests.exceptions.ConnectTimeout
            | urllib3.exceptions.ConnectTimeoutError
            | urllib3.exceptions.NewConnectionError
            | socket.gaierror
            | ConnectionRefusedError,
        ):
            return True
        if isinstance(current, OSError) and current.errno in _PRESEND_ERRNOS:
            return True
        current = current.__cause__ or current.__context__
    return False


def _run_public_read[ResultT](
    *,
    exchange: str,
    target: RestProxyTarget,
    proxy_lock: AbstractContextManager[object],
    sync_call: Callable[[], ResultT],
) -> ResultT:
    """Execute a public read through the public egress selector."""
    pool = get_egress_pool()
    if pool is None or pool.size() == 0:
        return _execute_with_proxy(
            target=target,
            proxy_lock=proxy_lock,
            proxy_url=None,
            sync_call=sync_call,
        )
    reservation = pool.reserve(exchange=exchange, purpose="http", traffic_class="public")
    try:
        try:
            return _execute_with_proxy(
                target=target,
                proxy_lock=proxy_lock,
                proxy_url=reservation.proxy_url,
                sync_call=sync_call,
            )
        except Exception as exc:
            if is_provable_presend_connect_error(exc):
                reservation.quarantine(
                    _KRAKEN_REST_CONNECT_ERROR_QUARANTINE_S,
                    reason="http-connect-error",
                )
            raise
    finally:
        reservation.release()


def _run_private_idempotent_read[ResultT](
    *,
    exchange: str,
    target: RestProxyTarget,
    proxy_lock: AbstractContextManager[object],
    sync_call: Callable[[], ResultT],
) -> ResultT:
    """Execute a private read direct first, with one safe fallback attempt."""
    try:
        return _run_private_direct(
            exchange=exchange,
            target=target,
            proxy_lock=proxy_lock,
            sync_call=sync_call,
        )
    except Exception as exc:
        if not is_provable_presend_connect_error(exc):
            raise
        fallback = _reserve_private_fallback(exchange)
        if fallback is None:
            raise
        try:
            return _execute_with_proxy(
                target=target,
                proxy_lock=proxy_lock,
                proxy_url=fallback.proxy_url,
                sync_call=sync_call,
            )
        finally:
            fallback.release()


def _run_private_mutation[ResultT](
    *,
    exchange: str,
    target: RestProxyTarget,
    proxy_lock: AbstractContextManager[object],
    sync_call: Callable[[], ResultT],
) -> ResultT:
    """Execute a private mutation direct only, without fallback."""
    return _run_private_direct(
        exchange=exchange,
        target=target,
        proxy_lock=proxy_lock,
        sync_call=sync_call,
    )


def _run_private_direct[ResultT](
    *,
    exchange: str,
    target: RestProxyTarget,
    proxy_lock: AbstractContextManager[object],
    sync_call: Callable[[], ResultT],
) -> ResultT:
    """Execute private traffic on the direct route only."""
    pool = get_egress_pool()
    reservation: EgressReservation | None = None
    if pool is not None and pool.size() > 0:
        reservation = pool.reserve_private_direct(exchange=exchange, purpose="http")
    try:
        return _execute_with_proxy(
            target=target,
            proxy_lock=proxy_lock,
            proxy_url=reservation.proxy_url if reservation is not None else None,
            sync_call=sync_call,
        )
    finally:
        if reservation is not None:
            reservation.release()


def _run_private_direct_without_reservation[ResultT](
    *,
    target: RestProxyTarget,
    proxy_lock: AbstractContextManager[object],
    sync_call: Callable[[], ResultT],
) -> ResultT:
    """Execute a private-scoped public read directly without pool reservation.

    Args:
        target: External REST client whose proxy state should be scoped.
        proxy_lock: Per-client lock guarding mutable SDK/session proxy state.
        sync_call: Blocking SDK call to execute.

    Returns:
        Result returned by the SDK call.
    """
    return _execute_with_proxy(
        target=target,
        proxy_lock=proxy_lock,
        proxy_url=None,
        sync_call=sync_call,
    )


def _reserve_private_fallback(exchange: str) -> EgressReservation | None:
    """Reserve the configured private fallback route when it is available."""
    pool = get_egress_pool()
    if pool is None or pool.size() == 0:
        return None
    return pool.reserve_private_fallback(exchange=exchange, purpose="http")


def _execute_with_proxy[ResultT](
    *,
    target: RestProxyTarget,
    proxy_lock: AbstractContextManager[object],
    proxy_url: str | None,
    sync_call: Callable[[], ResultT],
) -> ResultT:
    """Execute ``sync_call`` with the target client's proxy state scoped."""
    with proxy_lock, _scoped_proxy(target, proxy_url):
        return sync_call()


@contextmanager
def _scoped_proxy(target: RestProxyTarget, proxy_url: str | None) -> Iterator[None]:
    """Temporarily apply a direct or SOCKS proxy to a REST client."""
    client_state = _capture_client_proxy_state(target)
    session_state = _capture_session_proxy_state(target)
    _apply_proxy(target, session_state, proxy_url)
    try:
        yield
    finally:
        _restore_proxy(target, client_state, session_state)


def _capture_client_proxy_state(target: RestProxyTarget) -> _ClientProxyState:
    """Capture the SDK-level proxy attribute before mutation."""
    if target.proxy_attr is None:
        return _ClientProxyState(value=_MISSING, existed=False)
    value = getattr(target.client, target.proxy_attr, _MISSING)
    return _ClientProxyState(value=value, existed=value is not _MISSING)


def _capture_session_proxy_state(target: RestProxyTarget) -> _SessionProxyState | None:
    """Capture mutable session proxy state when the SDK exposes it."""
    session = getattr(target.client, target.session_attr, None)
    if not isinstance(session, requests.Session):
        return None
    proxies = getattr(session, "proxies", None)
    if not isinstance(proxies, MutableMapping):
        return None
    trust_env = getattr(session, "trust_env", None)
    return _SessionProxyState(
        session=session,
        proxies=cast(MutableMapping[str, str], proxies),
        previous_proxies={str(key): str(value) for key, value in proxies.items()},
        previous_trust_env=trust_env if isinstance(trust_env, bool) else None,
    )


def _apply_proxy(
    target: RestProxyTarget,
    session_state: _SessionProxyState | None,
    proxy_url: str | None,
) -> None:
    """Apply proxy settings to the SDK client and requests session."""
    if target.proxy_attr is not None:
        setattr(target.client, target.proxy_attr, _client_proxy_value(target, proxy_url))
    if session_state is None:
        return
    session_state.proxies.clear()
    session_state.proxies.update(_session_proxy_mapping(proxy_url))
    if session_state.previous_trust_env is not None:
        session_state.session.trust_env = False


def _restore_proxy(
    target: RestProxyTarget,
    client_state: _ClientProxyState,
    session_state: _SessionProxyState | None,
) -> None:
    """Restore proxy state captured before a scoped call."""
    if target.proxy_attr is not None:
        if client_state.existed:
            setattr(target.client, target.proxy_attr, client_state.value)
        else:
            with contextlib.suppress(AttributeError):
                delattr(target.client, target.proxy_attr)
    if session_state is None:
        return
    session_state.proxies.clear()
    session_state.proxies.update(session_state.previous_proxies)
    if session_state.previous_trust_env is not None:
        session_state.session.trust_env = session_state.previous_trust_env


def _client_proxy_value(target: RestProxyTarget, proxy_url: str | None) -> object:
    """Return the SDK-level proxy value for the configured target shape."""
    if target.proxy_attr_shape == "url":
        return proxy_url
    return _session_proxy_mapping(proxy_url)


def _session_proxy_mapping(proxy_url: str | None) -> dict[str, str]:
    """Return the requests proxy mapping for a direct or proxied route."""
    if proxy_url is None:
        return {}
    return {"http": proxy_url, "https": proxy_url}
