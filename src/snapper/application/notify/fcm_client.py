"""FCM HTTP v1 FID transport with data-only payloads and bounded typed results."""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from email.utils import parsedate_to_datetime
from typing import Literal

import httpx
import jwt
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import SecretStr
from pydantic import ValidationError

from snapper.api.schemas.android_devices import AndroidPushConfiguration

_TOKEN_URL = "https://oauth2.googleapis.com/token"
_SCOPE = "https://www.googleapis.com/auth/firebase.messaging"


class FcmServiceAccount(BaseModel):
    """Runtime-only service-account input; secrets never appear in representations."""

    model_config = ConfigDict(extra="ignore", frozen=True, hide_input_in_errors=True)
    type: Literal["service_account"]
    project_id: str
    client_email: str
    private_key: SecretStr
    private_key_id: str
    token_uri: Literal["https://oauth2.googleapis.com/token"]


class _TokenResponse(BaseModel):
    """Strict authorization response boundary."""

    access_token: SecretStr
    token_type: Literal["Bearer"]
    expires_in: int = Field(gt=0, le=3600)


class _FcmDetail(BaseModel):
    """Only documented FCM error details can invalidate a registration."""

    type_url: str = Field(alias="@type", default="")
    error_code: str = Field(alias="errorCode", default="")


class _FcmError(BaseModel):
    """Provider text is ignored rather than persisted or logged."""

    status: str = ""
    details: list[_FcmDetail] = Field(default_factory=list)


class _FcmResponse(BaseModel):
    """Bounded receipt/error envelope from the provider trust boundary."""

    name: str | None = Field(default=None, max_length=512)
    error: _FcmError | None = None


@dataclass(frozen=True, slots=True)
class FcmSendResult:
    """Provider acceptance is distinct from device delivery."""

    outcome: Literal["accepted", "unregistered", "permanent", "retry", "expired", "cancelled"]
    reason: str
    receipt: str | None = None
    retry_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class FcmHint:
    """Immutable opaque references captured in the durable outbox."""

    registration_public_id: str
    binding_id: str
    alert_public_id: str
    expires_at: datetime


class FcmClient:
    """One project, injected HTTP client, no implicit send retries or redirects."""

    def __init__(
        self,
        configuration: AndroidPushConfiguration,
        credentials: FcmServiceAccount,
        client: httpx.AsyncClient,
        clock: Callable[[], datetime],
    ) -> None:
        """Validate project identity before accepting any private credential."""
        if credentials.project_id != configuration.project_id:
            raise ValueError("FCM credential project does not match public configuration")
        try:
            key = load_pem_private_key(
                credentials.private_key.get_secret_value().encode(), password=None
            )
            if not isinstance(key, RSAPrivateKey):
                raise ValueError("FCM requires an RSA signing key")
        except (ValueError, TypeError):
            raise ValueError("Invalid FCM signing key") from None
        self.configuration = configuration
        self._credentials = credentials
        self._client = client
        self._clock = clock
        self._token: SecretStr | None = None
        self._token_expiry = datetime.min.replace(tzinfo=UTC)
        self._token_lock = asyncio.Lock()

    async def aclose(self) -> None:
        """Release the owned transport and cached access token at shutdown."""
        self._token = None
        await self._client.aclose()

    async def _access_token(self) -> str:
        """Cache authorization only; never automatically replay a message after 401."""
        async with self._token_lock:
            now = self._clock()
            if self._token is not None and now + timedelta(seconds=60) < self._token_expiry:
                return self._token.get_secret_value()
            assertion = jwt.encode(
                {
                    "iss": self._credentials.client_email,
                    "scope": _SCOPE,
                    "aud": _TOKEN_URL,
                    "iat": int(now.timestamp()),
                    "exp": int((now + timedelta(hours=1)).timestamp()),
                },
                self._credentials.private_key.get_secret_value(),
                algorithm="RS256",
                headers={"kid": self._credentials.private_key_id},
            )
            response = await self._client.post(
                _TOKEN_URL,
                follow_redirects=False,
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": assertion,
                },
            )
            response.raise_for_status()
            token = _TokenResponse.model_validate_json(response.content)
            self._token = token.access_token
            self._token_expiry = now + timedelta(seconds=token.expires_in)
            return self._token.get_secret_value()

    async def send(
        self, fid: str, hint: FcmHint, admitted: Callable[[], Awaitable[bool]]
    ) -> FcmSendResult:
        """Send one opaque hint, never a provider-rendered notification payload.

        Args:
            fid: Opaque Firebase installation ID from the bound registration.
            hint: Immutable registration references and original delivery expiry.
            admitted: Fresh asynchronous binding and authority check after OAuth acquisition.

        Returns:
            A typed provider outcome without automatic message replay.
        """
        now = self._clock()
        if hint.expires_at <= now:
            return FcmSendResult("expired", "expired")
        try:
            token = await self._access_token()
            if not await admitted():
                return FcmSendResult("cancelled", "binding_or_scope_changed")
            now = self._clock()
            if hint.expires_at <= now:
                return FcmSendResult("expired", "expired")
            response = await self._client.post(
                f"https://fcm.googleapis.com/v1/projects/{self.configuration.project_id}/messages:send",
                follow_redirects=False,
                headers={"Authorization": f"Bearer {token}"},
                json=self._payload(fid, hint, now),
            )
        except httpx.HTTPStatusError as error:
            return _authorization_failure(error.response, self._clock())
        except (httpx.HTTPError, ValidationError, jwt.PyJWTError):
            return FcmSendResult("retry", "transport_or_authorization_unavailable")
        if response.status_code == 401 and self._token is not None:
            if self._token.get_secret_value() == token:
                self._token = None
        return classify_fcm_response(response, now)

    def _payload(self, fid: str, hint: FcmHint, now: datetime) -> dict[str, object]:
        """Keep private account and alert content out of Google's message payload."""
        seconds = min(86400, max(0, int((hint.expires_at - now).total_seconds())))
        return {
            "message": {
                "fid": fid,
                "data": {
                    "type": "snapper_alert_hint",
                    "schema_version": "1",
                    "configuration_id": self.configuration.configuration_id,
                    "binding_id": hint.binding_id,
                    "registration_public_id": hint.registration_public_id,
                    "alert_public_id": hint.alert_public_id,
                    "expires_at": hint.expires_at.astimezone(UTC)
                    .isoformat()
                    .replace("+00:00", "Z"),
                },
                "android": {
                    "ttl": f"{seconds}s",
                    "priority": "normal",
                    "restricted_package_name": self.configuration.package_name,
                },
            }
        }


def classify_fcm_response(response: httpx.Response, now: datetime) -> FcmSendResult:
    """Invalidate only an explicit FCM UNREGISTERED result, never a generic 404.

    Args:
        response: Provider HTTP response, including status and Retry-After.
        now: Current UTC boundary for retry scheduling.

    Returns:
        Acceptance, permanent refusal, explicit unregistration or a bounded retry outcome.
    """
    try:
        payload = _FcmResponse.model_validate_json(response.content)
    except ValidationError:
        if 400 <= response.status_code < 500 and response.status_code != 429:
            return FcmSendResult("permanent", "provider_rejected")
        return _provider_retry(response, now, "invalid_provider_response")
    if response.is_success and payload.name:
        return FcmSendResult("accepted", "accepted", receipt=payload.name)
    if (
        payload.error is not None
        and response.status_code == 404
        and _is_unregistered(payload.error)
    ):
        return FcmSendResult("unregistered", "unregistered")
    if response.status_code == 429 or response.status_code >= 500:
        return _provider_retry(response, now, "provider_unavailable")
    if response.is_success:
        return FcmSendResult("retry", "missing_provider_receipt")
    return FcmSendResult("permanent", "provider_rejected")


def _is_unregistered(error: _FcmError) -> bool:
    """Recognize only the documented FCM registration-retirement detail."""
    for detail in error.details:
        if (
            detail.type_url == "type.googleapis.com/google.firebase.fcm.v1.FcmError"
            and detail.error_code == "UNREGISTERED"
        ):
            return True
    return False


def _provider_retry(response: httpx.Response, now: datetime, reason: str) -> FcmSendResult:
    """Honor throttling minima even when an intermediary replaces the JSON body."""
    retry_at = _retry_after(response.headers.get("Retry-After"), now)
    if response.status_code == 429:
        retry_at = max(retry_at or now, now + timedelta(seconds=60))
    return FcmSendResult("retry", reason, retry_at=retry_at)


def _retry_after(value: str | None, now: datetime) -> datetime | None:
    """Honor integer or HTTP-date Retry-After without echoing provider strings."""
    if value is None:
        return None
    try:
        if value.isdigit():
            return now + timedelta(seconds=min(int(value), 604800))
        parsed = parsedate_to_datetime(value)
        return max(now, parsed.astimezone(UTC))
    except (ValueError, TypeError, OverflowError):
        return None


def _authorization_failure(response: httpx.Response, now: datetime) -> FcmSendResult:
    """Distinguish permanent OAuth rejection from throttling and provider outages."""
    if response.status_code == 429 or response.status_code >= 500:
        return FcmSendResult(
            "retry",
            "authorization_unavailable",
            retry_at=_retry_after(response.headers.get("Retry-After"), now),
        )
    return FcmSendResult("permanent", "authorization_failed")
