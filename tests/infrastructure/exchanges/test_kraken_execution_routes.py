"""Actual private REST and constructed/connected WebSocket environment agreement."""

from types import SimpleNamespace

import pytest
from websockets.datastructures import Headers

from snapper.infrastructure.exchanges.kraken_execution_routes import futures_execution_route
from snapper.infrastructure.exchanges.kraken_execution_routes import spot_execution_route

_SPOT = "https://api.kraken.com"
_PRODUCTION = "https://futures.kraken.com"
_DEMO = "https://demo-futures.kraken.com"


def _http(origin: object) -> SimpleNamespace:
    """Expose the actual private URL used by a selected CCXT instance."""
    return SimpleNamespace(urls={"api": {"private": origin}})


def _connection(endpoint: str, *, active: bool = True) -> SimpleNamespace:
    """Carry pinned connector fields and the socket's actual handshake evidence."""
    futures = not endpoint.startswith("wss://")
    address = endpoint if futures else endpoint.removeprefix("wss://")
    host, path = address.split("/", 1)
    request = SimpleNamespace(headers=Headers({"Host": host}), path="/" + path)
    field = (
        "_ConnectFuturesWebsocket__ws_endpoint"
        if futures
        else "_ConnectSpotWebsocketBase__ws_endpoint"
    )
    return SimpleNamespace(
        **{field: endpoint, "socket": SimpleNamespace(request=request) if active else None}
    )


def _spot_ws(*, active: bool = True) -> SimpleNamespace:
    """Model the selected shared SDK's real production REST and private connector."""
    return SimpleNamespace(
        URL=_SPOT,
        _pub_conn=None,
        _priv_conn=_connection("wss://ws-auth.kraken.com/v2", active=active),
    )


def _futures_ws(origin: str, *, active: bool = True) -> SimpleNamespace:
    """Expose the exact endpoint field consumed by the pinned Futures connector."""
    return SimpleNamespace(
        url=origin,
        _conn=_connection(origin.removeprefix("https://") + "/ws/v1", active=active),
    )


@pytest.mark.parametrize("active", (True, False))
def test_spot_route_requires_matching_rest_and_real_private_connector(active: bool) -> None:
    """Connected and prepared Spot transports both prove their actual supported route.

    Given: A production CCXT URL, actual account/order SDK URL and matching private connector,
    When: Their public route is requested,
    Then: The result explicitly proves production without using a sandbox preference.
    """
    result = spot_execution_route(
        _http(_SPOT), (SimpleNamespace(URL=_SPOT), None), _spot_ws(active=active)
    )
    assert result is not None
    assert result.environment == "production"
    assert result.public_api_origin == _SPOT
    assert spot_execution_route(_http(_SPOT), (), None) == result


@pytest.mark.parametrize("origin", (_PRODUCTION, _DEMO))
@pytest.mark.parametrize("active", (True, False))
def test_futures_environment_comes_from_agreed_actual_http_and_websocket_routes(
    origin: str, active: bool
) -> None:
    """Both real Futures environments retain separate route identities.

    Given: Matching selected private REST, account/order SDK and public/private WS endpoints,
    When: The actual route is projected,
    Then: Its environment agrees with those endpoints and never an independent flag.
    """
    result = futures_execution_route(
        _http(origin + "/derivatives/api/"),
        (SimpleNamespace(url=origin), None),
        (_futures_ws(origin, active=active), None),
    )
    assert result is not None
    assert result.environment == ("demo" if origin == _DEMO else "production")
    assert result.public_api_origin == origin


@pytest.mark.parametrize(
    "value",
    (
        None,
        " https://api.kraken.com",
        "https://[invalid",
        "https://api.kraken.com:invalid",
        "http://api.kraken.com",
        "https://api.kraken.com:444",
        "https://user:synthetic@api.kraken.com",
        "https://api.kraken.com?token=synthetic",
        "https://api.kraken.com#fragment",
        "https:///missing-host",
        "https://different.invalid",
    ),
)
def test_unproven_or_authentication_bearing_http_endpoints_cannot_become_public_route(
    value: object,
) -> None:
    """Untrusted SDK URL shapes cannot be sanitized into apparent production proof.

    Given: A missing, malformed, authenticated, parameterized or unsupported private URL,
    When: Execution routing is requested,
    Then: No route is returned and no original URL is logged or included in an exception.
    """
    assert spot_execution_route(_http(value), (), None) is None


@pytest.mark.parametrize("urls", (None, {}, {"api": "https://api.kraken.com"}))
def test_missing_actual_ccxt_private_configuration_is_not_filled_from_sdk_defaults(
    urls: object,
) -> None:
    """A familiar SDK class name cannot substitute the actual selected CCXT endpoint.

    Given: Missing or unexpected private-URL configuration,
    When: A matching standalone account SDK is present,
    Then: Execution route proof remains unavailable.
    """
    assert (
        spot_execution_route(SimpleNamespace(urls=urls), (SimpleNamespace(URL=_SPOT),), None)
        is None
    )


def test_read_write_and_websocket_environment_disagreement_refuses_authority() -> None:
    """A demo account response cannot authorize orders or recovery against production.

    Given: Individually valid SDK endpoints from different environments,
    When: They are combined as one executor route,
    Then: Both HTTP disagreement and active WS disagreement refuse proof.
    """
    assert futures_execution_route(_http(_PRODUCTION), (SimpleNamespace(url=_DEMO),), ()) is None
    assert futures_execution_route(_http(_PRODUCTION), (), (_futures_ws(_DEMO),)) is None
    assert (
        futures_execution_route(_http(_PRODUCTION), (), (SimpleNamespace(url=_PRODUCTION),)) is None
    )
    assert futures_execution_route(_http("https://other.invalid"), (), ()) is None
    assert spot_execution_route(_http(_SPOT), (), SimpleNamespace(URL=_DEMO)) is None


@pytest.mark.parametrize(
    "defect", ("connector", "endpoint", "request", "headers", "host", "path", "duplicate_host")
)
def test_actual_websocket_handshake_and_connector_must_agree(defect: str) -> None:
    """Contradictory or absent active-connection evidence fails independently of REST agreement.

    Given: An otherwise supported Spot route,
    When: Its real connector or active handshake cannot prove the same endpoint,
    Then: No account route is manufactured from class defaults.
    """
    client = _spot_ws()
    connection = client._priv_conn
    if defect == "connector":
        client._priv_conn = SimpleNamespace()
    elif defect == "endpoint":
        connection._ConnectSpotWebsocketBase__ws_endpoint = "wss://other.invalid/v2"
    elif defect == "request":
        connection.socket.request = None
    elif defect == "headers":
        connection.socket.request.headers = None
    elif defect == "host":
        connection.socket.request.headers = Headers({"Host": "other.invalid"})
    elif defect == "path":
        connection.socket.request.path = "/unexpected"
    else:
        connection.socket.request.headers = Headers(
            [("Host", "ws-auth.kraken.com"), ("Host", "other.invalid")]
        )
    assert spot_execution_route(_http(_SPOT), (), client) is None


def test_supported_https_port_and_public_spot_connector_remain_explicit() -> None:
    """Equivalent HTTPS routing still checks the actual public and private connector paths.

    Given: The standard HTTPS port and matching actual Spot connectors,
    When: Their route is projected,
    Then: Production origin is retained and a changed public connector refuses it.
    """
    client = _spot_ws()
    client._pub_conn = _connection("wss://ws.kraken.com/v2")
    assert spot_execution_route(_http(_SPOT + ":443"), (), client) is not None
    client._pub_conn._ConnectSpotWebsocketBase__ws_endpoint = "wss://other.invalid/v2"
    assert spot_execution_route(_http(_SPOT), (), client) is None
