"""Prove actual Kraken REST and WebSocket routing from the selected SDK objects."""

from collections.abc import Mapping
from urllib.parse import urlsplit

from snapper.infrastructure.exchanges.contracts import ExecutionApiRoute

_SPOT_ORIGIN = "https://api.kraken.com"
_FUTURES_ORIGIN = "https://futures.kraken.com"
_FUTURES_DEMO_ORIGIN = "https://demo-futures.kraken.com"


def _api_origin(value: object) -> str | None:
    """Read a public HTTPS origin without retaining authentication or request parameters."""
    if not isinstance(value, str) or value.strip() != value:
        return None
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        return None
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or port not in (None, 443)
    ):
        return None
    return f"https://{parsed.hostname}"


def _ccxt_origin(client: object) -> str | None:
    """Use the private URL consumed by the actual CCXT instance rather than its sandbox setting."""
    urls = getattr(client, "urls", None)
    if not isinstance(urls, Mapping):
        return None
    api = urls.get("api")
    return _api_origin(api.get("private")) if isinstance(api, Mapping) else None


def _agreed_rest_origin(
    ccxt_client: object,
    sdk_clients: tuple[object | None, ...],
    attribute: str,
    supported: tuple[str, ...],
) -> str | None:
    """Require every actually constructed read/write SDK object to share the supported origin."""
    origin = _ccxt_origin(ccxt_client)
    if origin not in supported:
        return None
    for client in sdk_clients:
        if client is not None and _api_origin(getattr(client, attribute, None)) != origin:
            return None
    return origin


def _websocket_matches(
    client: object, connection_field: str, endpoint_field: str, expected: str
) -> bool:
    """Verify the exact connector address and an active socket's original handshake when present."""
    connector = getattr(client, connection_field, None)
    if connector is None or getattr(connector, endpoint_field, None) != expected:
        return False
    socket = getattr(connector, "socket", None)
    if socket is None:
        return True
    request = getattr(socket, "request", None)
    headers = getattr(request, "headers", None)
    if not isinstance(headers, Mapping):
        return False
    parsed = urlsplit(expected if expected.startswith("wss://") else f"wss://{expected}")
    try:
        return (
            headers.get("Host") == parsed.netloc and getattr(request, "path", None) == parsed.path
        )
    except (LookupError, ValueError, TypeError):
        return False


def spot_execution_route(
    ccxt_client: object,
    sdk_clients: tuple[object | None, ...],
    websocket_client: object | None,
) -> ExecutionApiRoute | None:
    """Prove the supported Spot production endpoints without inventing a demo route.

    Args:
        ccxt_client: The actual fallback/order HTTP client.
        sdk_clients: Actual account/order SDK instances already constructed by this client.
        websocket_client: Actual shared Spot SDK object, absent before construction.

    Returns:
        Production routing, or no proof on missing, unsupported or contradictory endpoint evidence.
    """
    origin = _agreed_rest_origin(ccxt_client, sdk_clients, "URL", (_SPOT_ORIGIN,))
    if origin is None:
        return None
    if websocket_client is not None:
        if _api_origin(getattr(websocket_client, "URL", None)) != origin:
            return None
        for connection, expected in (
            ("_pub_conn", "wss://ws.kraken.com/v2"),
            ("_priv_conn", "wss://ws-auth.kraken.com/v2"),
        ):
            if getattr(websocket_client, connection, None) is not None and not _websocket_matches(
                websocket_client, connection, "_ConnectSpotWebsocketBase__ws_endpoint", expected
            ):
                return None
    return ExecutionApiRoute(environment="production", public_api_origin=origin)


def futures_execution_route(
    ccxt_client: object,
    sdk_clients: tuple[object | None, ...],
    websocket_clients: tuple[object | None, ...],
) -> ExecutionApiRoute | None:
    """Prove actual Futures production/demo agreement across read, write and active WS objects.

    Args:
        ccxt_client: The selected CCXT client after actual sandbox-mode configuration.
        sdk_clients: Actual account/order/market SDK objects that may issue HTTP requests.
        websocket_clients: Actual public and private WebSocket SDK objects when constructed.

    Returns:
        The observed supported environment, or no proof when any actual endpoint disagrees.
    """
    origin = _agreed_rest_origin(
        ccxt_client, sdk_clients, "url", (_FUTURES_ORIGIN, _FUTURES_DEMO_ORIGIN)
    )
    if origin is None:
        return None
    expected = origin.removeprefix("https://") + "/ws/v1"
    for client in websocket_clients:
        if client is not None and (
            _api_origin(getattr(client, "url", None)) != origin
            or not _websocket_matches(
                client, "_conn", "_ConnectFuturesWebsocket__ws_endpoint", expected
            )
        ):
            return None
    return ExecutionApiRoute(
        environment="demo" if origin == _FUTURES_DEMO_ORIGIN else "production",
        public_api_origin=origin,
    )
