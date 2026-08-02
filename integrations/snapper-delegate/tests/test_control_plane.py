"""Tests for the typed delegate control-plane client."""

from datetime import UTC
from datetime import datetime
from pathlib import Path

import httpx
import pytest

from snapper_delegate.control_plane import ControlPlaneError
from snapper_delegate.control_plane import ControlPlaneErrorKind
from snapper_delegate.control_plane import SnapperControlClient

_TOKEN = "header.payload.signature"
_EXPIRY = "2026-08-02T12:00:00Z"


def _token_file(tmp_path: Path, value: str = _TOKEN) -> Path:
    """Create one rotating delegate token file."""
    path = tmp_path / "delegate-token"
    path.write_text(value, encoding="utf-8")
    return path


@pytest.mark.asyncio
async def test_control_plane_happy_path_uses_fresh_bearer_and_typed_payloads(
    tmp_path: Path,
) -> None:
    """All supported endpoints retain their typed public response fields.

    Given a rotating bearer file and valid typed responses from every endpoint,
    When the client mints a socket token, resolves identity, and lists pending work,
    Then it authenticates each request freshly and preserves validated public fields.
    """
    requests: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/auth/ws_token":
            return httpx.Response(
                200,
                json={
                    "payload": {
                        "ws_token": "one-shot-secret",
                        "ws_token_exp": _EXPIRY,
                        "extension": True,
                    },
                    "extension": None,
                },
            )
        if request.url.path == "/api/auth/me":
            return httpx.Response(
                200,
                json={"payload": {"delegate_public_id": " delegate-1 "}},
            )
        return httpx.Response(
            200,
            json={
                "items": [
                    {
                        "review_public_id": "review-1",
                        "selected_delegate_public_id": "delegate-1",
                        "wallet_public_id": "wallet-1",
                        "dispatch_version": 2,
                        "status": "pending",
                        "deadline": "2026-08-02T12:01:00Z",
                        "fanout_after": "2026-08-02T12:00:30Z",
                        "instrument": "BTC/USD",
                        "signal_envelope": {"side": "buy"},
                        "extension": "preserved",
                    }
                ],
                "count": 1,
            },
        )

    token_file = _token_file(tmp_path, f"  {_TOKEN}\n")
    client = SnapperControlClient(
        "https://snapper.invalid/",
        token_file,
        transport=httpx.MockTransport(_handler),
    )
    async with client as entered:
        assert entered is client
        token = await client.mint_ws_token()
        identity = await client.fetch_delegate_identity()
        pending = await client.list_pending_reviews()

    assert token.value.get_secret_value() == "one-shot-secret"
    assert "one-shot-secret" not in repr(token)
    assert token.expires_at == datetime(2026, 8, 2, 12, tzinfo=UTC)
    assert identity == "delegate-1"
    assert pending[0].signal_envelope == {"side": "buy"}
    assert pending[0].__pydantic_extra__ == {"extension": "preserved"}
    assert client._http_client.is_closed is True
    assert [request.method for request in requests] == ["POST", "GET", "GET"]
    assert requests[0].content == b""
    assert requests[2].url.params["limit"] == "100"
    assert all(request.headers["Authorization"] == f"Bearer {_TOKEN}" for request in requests)


@pytest.mark.asyncio
async def test_unauthorized_response_rereads_rotated_token_before_success(
    tmp_path: Path,
) -> None:
    """A 401 causes one immediate file refresh before normal backoff is needed.

    Given the credential file rotates after the first request is refused,
    When the client retries the identity request once,
    Then it rereads the file and succeeds with the new bearer.
    """
    token_file = _token_file(tmp_path, "old-token")
    authorizations: list[str] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        authorizations.append(request.headers["Authorization"])
        if len(authorizations) == 1:
            token_file.write_text("new-token", encoding="utf-8")
            return httpx.Response(401)
        return httpx.Response(200, json={"payload": {"delegate_public_id": "delegate-1"}})

    client = SnapperControlClient(
        "http://snapper.invalid",
        token_file,
        transport=httpx.MockTransport(_handler),
    )
    try:
        assert await client.fetch_delegate_identity() == "delegate-1"
    finally:
        await client.aclose()
    assert authorizations == ["Bearer old-token", "Bearer new-token"]


@pytest.mark.parametrize(
    ("body", "expected_error_code"),
    [
        (
            {
                "detail": {
                    "success": False,
                    "error_code": "not_a_delegate",
                    "message": "refused",
                    "details": {},
                }
            },
            "not_a_delegate",
        ),
        ("not-json", None),
    ],
)
@pytest.mark.asyncio
async def test_http_failures_expose_only_status_and_public_error_code(
    tmp_path: Path,
    body: object,
    expected_error_code: str | None,
) -> None:
    """REST refusals become credential-free typed failures.

    Given a public error envelope or an unparseable HTTP failure body,
    When a pending-review request fails,
    Then the typed error exposes only status and any stable public error code.
    """

    def _handler(request: httpx.Request) -> httpx.Response:
        if isinstance(body, dict):
            return httpx.Response(422, json=body, request=request)
        return httpx.Response(503, content=b"not-json", request=request)

    client = SnapperControlClient(
        "https://snapper.invalid",
        _token_file(tmp_path),
        transport=httpx.MockTransport(_handler),
    )
    try:
        with pytest.raises(ControlPlaneError) as caught:
            await client.list_pending_reviews(7)
    finally:
        await client.aclose()
    assert caught.value.kind is ControlPlaneErrorKind.HTTP_STATUS
    assert caught.value.status_code == (422 if isinstance(body, dict) else 503)
    assert caught.value.error_code == expected_error_code
    assert _TOKEN not in str(caught.value)


@pytest.mark.asyncio
async def test_transport_failure_is_recoverable_and_value_safe(tmp_path: Path) -> None:
    """Network exceptions remain inside the typed control boundary.

    Given the HTTP transport cannot connect,
    When the client requests a one-shot WebSocket credential,
    Then it raises a recoverable transport error without leaking request values.
    """

    def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    client = SnapperControlClient(
        "https://snapper.invalid",
        _token_file(tmp_path),
        transport=httpx.MockTransport(_handler),
    )
    try:
        with pytest.raises(ControlPlaneError) as caught:
            await client.mint_ws_token()
    finally:
        await client.aclose()
    assert caught.value.kind is ControlPlaneErrorKind.TRANSPORT
    assert caught.value.status_code is None


@pytest.mark.parametrize("file_state", ["missing", "empty", "invalid_utf8"])
def test_access_token_file_failures_are_generic(tmp_path: Path, file_state: str) -> None:
    """Missing, empty, and undecodable credentials share one safe outcome.

    Given an absent, blank, or invalid UTF-8 token file,
    When the client reads the current access token,
    Then it reports one generic token-file failure without revealing its path.
    """
    token_file = tmp_path / "delegate-token"
    if file_state == "empty":
        token_file.write_text(" \n", encoding="utf-8")
    if file_state == "invalid_utf8":
        token_file.write_bytes(b"\xff")
    client = SnapperControlClient("https://snapper.invalid", token_file)
    with pytest.raises(ControlPlaneError) as caught:
        client.read_access_token()
    assert caught.value.kind is ControlPlaneErrorKind.TOKEN_FILE
    assert str(token_file) not in str(caught.value)


@pytest.mark.parametrize(
    "endpoint",
    ["token", "identity_schema", "identity_blank", "pending_schema", "pending_count"],
)
@pytest.mark.asyncio
async def test_malformed_success_payloads_fail_closed(
    tmp_path: Path,
    endpoint: str,
) -> None:
    """Malformed success bodies never escape their local parse boundary.

    Given a successful status carrying an invalid endpoint-specific payload,
    When the client validates the response body,
    Then it fails closed with the common malformed-response category.
    """

    def _handler(request: httpx.Request) -> httpx.Response:
        if endpoint == "identity_schema":
            return httpx.Response(200, json={})
        if endpoint == "identity_blank":
            return httpx.Response(200, json={"payload": {"delegate_public_id": " "}})
        if endpoint == "pending_schema":
            return httpx.Response(200, json={"count": 0})
        if endpoint == "pending_count":
            return httpx.Response(200, json={"items": [], "count": 1})
        return httpx.Response(200, json={"payload": {"ws_token": "missing-expiry"}})

    client = SnapperControlClient(
        "https://snapper.invalid",
        _token_file(tmp_path),
        transport=httpx.MockTransport(_handler),
    )
    try:
        with pytest.raises(ControlPlaneError) as caught:
            if endpoint.startswith("identity"):
                await client.fetch_delegate_identity()
            elif endpoint.startswith("pending"):
                await client.list_pending_reviews()
            else:
                await client.mint_ws_token()
    finally:
        await client.aclose()
    assert caught.value.kind is ControlPlaneErrorKind.MALFORMED_RESPONSE
