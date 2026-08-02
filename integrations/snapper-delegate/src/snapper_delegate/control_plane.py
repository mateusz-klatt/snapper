"""Typed Snapper control-plane client for delegate identity and wake catch-up."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Self

import httpx
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import SecretStr
from pydantic import ValidationError

from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue

_WIRE_MODEL_CONFIG = ConfigDict(
    extra="allow",
    frozen=True,
    strict=True,
    validate_default=True,
)


class ControlPlaneErrorKind(StrEnum):
    """Classify recoverable control-plane failures without sensitive details."""

    TOKEN_FILE = "token_file"
    TRANSPORT = "transport"
    HTTP_STATUS = "http_status"
    MALFORMED_RESPONSE = "malformed_response"


class ControlPlaneError(RuntimeError):
    """Report a recoverable control-plane failure without retaining credentials."""

    def __init__(
        self,
        kind: ControlPlaneErrorKind,
        status_code: int | None = None,
        error_code: str | None = None,
    ) -> None:
        """Initialize a value-safe failure description."""
        super().__init__(kind.value)
        self.kind = kind
        self.status_code = status_code
        self.error_code = error_code


class _WireModel(BaseModel):
    """Preserve forward-compatible fields received from the control plane."""

    model_config = _WIRE_MODEL_CONFIG

    __pydantic_extra__: dict[str, JsonValue] = Field(init=False)


class _WsTokenPayload(_WireModel):
    """Represent the one-shot WebSocket credential payload."""

    ws_token: str = Field(min_length=1)
    ws_token_exp: datetime


class _WsTokenEnvelope(_WireModel):
    """Wrap the WebSocket credential payload."""

    payload: _WsTokenPayload


class _IdentityPayload(_WireModel):
    """Represent the delegate identity returned for the access principal."""

    delegate_public_id: str = Field(min_length=1)


class _IdentityEnvelope(_WireModel):
    """Wrap the authenticated principal payload."""

    payload: _IdentityPayload


class PendingReview(_WireModel):
    """Represent one pending review exposed to its selected delegate."""

    review_public_id: str = Field(min_length=1)
    selected_delegate_public_id: str = Field(min_length=1)
    wallet_public_id: str = Field(min_length=1)
    dispatch_version: int = Field(ge=0)
    status: str = Field(min_length=1)
    deadline: datetime
    fanout_after: datetime
    instrument: str | None = None
    signal_envelope: JsonObject | None = None


class _PendingReviewList(_WireModel):
    """Represent the bounded pending-review response."""

    items: list[PendingReview]
    count: int = Field(ge=0)


class _ErrorDetail(_WireModel):
    """Represent the stable public error code in an HTTP error body."""

    error_code: str | None = None


class _ErrorEnvelope(_WireModel):
    """Wrap a FastAPI detail response."""

    detail: _ErrorDetail


@dataclass(frozen=True, slots=True)
class WsToken:
    """Carry a secret one-shot credential and its expiry."""

    value: SecretStr
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class _RequestSpec:
    """Describe one authenticated control-plane request."""

    method: str
    path: str
    params: dict[str, int] | None = None


class SnapperControlClient:
    """Call the authenticated Snapper REST surfaces used by the delegate."""

    def __init__(
        self,
        snapper_base_url: str,
        delegate_token_file: str | Path,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        """Initialize an inert client with injectable HTTP transport."""
        self._delegate_token_file = Path(delegate_token_file)
        self._http_client = httpx.AsyncClient(
            base_url=f"{snapper_base_url.rstrip('/')}/",
            timeout=timeout_seconds,
            transport=transport,
        )

    async def __aenter__(self) -> Self:
        """Enter the client lifetime context."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the owned HTTP client when leaving its context."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close the owned asynchronous HTTP client."""
        await self._http_client.aclose()

    def read_access_token(self) -> SecretStr:
        """Read the current delegate JWT for one authenticated use.

        Returns:
            The nonempty token with secret-safe representation.
        """
        try:
            value = self._delegate_token_file.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError) as error:
            raise ControlPlaneError(ControlPlaneErrorKind.TOKEN_FILE) from error
        if not value:
            raise ControlPlaneError(ControlPlaneErrorKind.TOKEN_FILE)
        return SecretStr(value)

    async def mint_ws_token(self) -> WsToken:
        """Mint a fresh one-shot credential for a WebSocket authentication.

        Returns:
            The validated one-shot token and its expiry timestamp.
        """
        response = await self._request(_RequestSpec("POST", "api/auth/ws_token"))
        try:
            envelope = _WsTokenEnvelope.model_validate_json(response.content)
        except ValidationError as error:
            raise ControlPlaneError(ControlPlaneErrorKind.MALFORMED_RESPONSE) from error
        return WsToken(
            value=SecretStr(envelope.payload.ws_token),
            expires_at=envelope.payload.ws_token_exp,
        )

    async def fetch_delegate_identity(self) -> str:
        """Return the authenticated principal's delegate public identifier.

        Returns:
            The validated nonempty delegate public identifier.
        """
        response = await self._request(_RequestSpec("GET", "api/auth/me"))
        try:
            envelope = _IdentityEnvelope.model_validate_json(response.content)
        except ValidationError as error:
            raise ControlPlaneError(ControlPlaneErrorKind.MALFORMED_RESPONSE) from error
        delegate_public_id = envelope.payload.delegate_public_id.strip()
        if not delegate_public_id:
            raise ControlPlaneError(ControlPlaneErrorKind.MALFORMED_RESPONSE)
        return delegate_public_id

    async def list_pending_reviews(self, limit: int = 100) -> list[PendingReview]:
        """Return the selected delegate's bounded fanout-eligible snapshot.

        Args:
            limit: Maximum number of pending reviews requested from the server.

        Returns:
            The validated pending-review snapshot.
        """
        response = await self._request(
            _RequestSpec("GET", "api/ai-reviews/pending", {"limit": limit})
        )
        try:
            result = _PendingReviewList.model_validate_json(response.content)
        except ValidationError as error:
            raise ControlPlaneError(ControlPlaneErrorKind.MALFORMED_RESPONSE) from error
        if result.count != len(result.items):
            raise ControlPlaneError(ControlPlaneErrorKind.MALFORMED_RESPONSE)
        return result.items

    async def _request(self, spec: _RequestSpec) -> httpx.Response:
        """Send an authenticated request and refresh the file once after a 401."""
        response = await self._send(spec, self.read_access_token())
        if response.status_code == httpx.codes.UNAUTHORIZED:
            response = await self._send(spec, self.read_access_token())
        if not response.is_success:
            raise ControlPlaneError(
                ControlPlaneErrorKind.HTTP_STATUS,
                response.status_code,
                _read_error_code(response),
            )
        return response

    async def _send(self, spec: _RequestSpec, token: SecretStr) -> httpx.Response:
        """Send one request while converting transport failures to typed state."""
        try:
            return await self._http_client.request(
                spec.method,
                spec.path,
                params=spec.params,
                headers={"Authorization": f"Bearer {token.get_secret_value()}"},
            )
        except httpx.RequestError as error:
            raise ControlPlaneError(ControlPlaneErrorKind.TRANSPORT) from error


def _read_error_code(response: httpx.Response) -> str | None:
    """Extract a stable public error code from a failed response when present."""
    try:
        return _ErrorEnvelope.model_validate_json(response.content).detail.error_code
    except ValidationError:
        return None
