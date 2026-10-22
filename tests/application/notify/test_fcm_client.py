"""Synthetic OAuth and FID transport tests; no credential file or network is used."""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from urllib.parse import parse_qs
from uuid import uuid4

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import SecretStr

from snapper.api.schemas.android_devices import AndroidPushConfiguration
from snapper.application.notify.fcm_client import FcmClient
from snapper.application.notify.fcm_client import FcmHint
from snapper.application.notify.fcm_client import FcmServiceAccount
from snapper.application.notify.fcm_client import _retry_after
from snapper.application.notify.fcm_client import classify_fcm_response

_NOW = datetime(2026, 10, 16, tzinfo=UTC)


@pytest.fixture
def configuration() -> AndroidPushConfiguration:
    """Provide only public synthetic project options."""
    return AndroidPushConfiguration(
        configuration_id=str(uuid4()),
        package_name="ch.snapper.android",
        project_id="synthetic",
        application_id="synthetic-app",
        api_key="public-key",
        messaging_sender_id="123",
    )


@pytest.fixture
def credentials() -> FcmServiceAccount:
    """Generate a disposable signing key unrelated to any provider account."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    return FcmServiceAccount(
        type="service_account",
        project_id="synthetic",
        client_email="synthetic@example.invalid",
        private_key=SecretStr(pem),
        private_key_id="synthetic-key",
        token_uri="https://oauth2.googleapis.com/token",
    )


def _hint() -> FcmHint:
    """Capture immutable opaque IDs and expiry once before sending."""
    return FcmHint(str(uuid4()), str(uuid4()), str(uuid4()), _NOW + timedelta(hours=24))


async def _admitted() -> bool:
    """Represent a successfully revalidated registration."""
    return True


@pytest.mark.asyncio
async def test_actual_oauth_assertion_and_data_only_fid_wire(
    configuration: AndroidPushConfiguration,
    credentials: FcmServiceAccount,
) -> None:
    """Verify RSA claims, fixed endpoint, exact wire keys and reusable OAuth only.

    Given: A generated RSA key and an in-process OAuth and FCM transport.
    When: Two distinct send calls use the configured project.
    Then: The assertion is valid, messages contain only the expected FID hint, and OAuth is cached.
    """
    requests: list[httpx.Request] = []
    hint = _hint()

    def respond(request: httpx.Request) -> httpx.Response:
        """Keep the synthetic transport entirely in process."""
        requests.append(request)
        if request.url.host == "oauth2.googleapis.com":
            assertion = parse_qs(request.content.decode())["assertion"][0]
            key = serialization.load_pem_private_key(
                credentials.private_key.get_secret_value().encode(), password=None
            )
            assert isinstance(key, rsa.RSAPrivateKey)
            claims = jwt.decode(
                assertion,
                key.public_key(),
                algorithms=["RS256"],
                audience="https://oauth2.googleapis.com/token",
                options={"verify_exp": False},
            )
            assert claims["scope"] == "https://www.googleapis.com/auth/firebase.messaging"
            assert claims["iss"] == credentials.client_email
            return httpx.Response(
                200,
                json={
                    "access_token": "synthetic-token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                },
            )
        assert request.url.path == "/v1/projects/synthetic/messages:send"
        assert request.headers["Authorization"] == "Bearer synthetic-token"
        message = json.loads(request.content)["message"]
        assert set(message) == {"fid", "data", "android"}
        assert message["fid"] == "opaque-fid"
        assert message["data"] == {
            "type": "snapper_alert_hint",
            "schema_version": "1",
            "configuration_id": configuration.configuration_id,
            "binding_id": hint.binding_id,
            "registration_public_id": hint.registration_public_id,
            "alert_public_id": hint.alert_public_id,
            "expires_at": "2026-10-17T00:00:00Z",
        }
        assert message["android"] == {
            "ttl": "86400s",
            "priority": "normal",
            "restricted_package_name": "ch.snapper.android",
        }
        return httpx.Response(200, json={"name": "projects/synthetic/messages/receipt"})

    client = FcmClient(
        configuration,
        credentials,
        httpx.AsyncClient(transport=httpx.MockTransport(respond)),
        lambda: _NOW,
    )
    for _ in range(2):
        result = await client.send("opaque-fid", hint, _admitted)
        assert result.outcome == "accepted"
        assert result.receipt == "projects/synthetic/messages/receipt"
    assert len(requests) == 3
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status,outcome", [(401, "permanent"), (429, "retry"), (503, "retry")])
async def test_oauth_failures_never_dispatch_fid_or_follow_redirects(
    configuration: AndroidPushConfiguration,
    credentials: FcmServiceAccount,
    status: int,
    outcome: str,
) -> None:
    """Authorization errors are classified without a second provider request.

    Given: A synthetic OAuth endpoint returning a refusal or outage.
    When: One hint attempts authorization.
    Then: The failure is classified and no FCM message or follow-up request is dispatched.
    """
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Return a bounded synthetic provider failure."""
        requests.append(request)
        return httpx.Response(status, headers={"Retry-After": "90"})

    client = FcmClient(
        configuration,
        credentials,
        httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=True),
        lambda: _NOW,
    )
    result = await client.send("opaque-fid", _hint(), _admitted)
    assert result.outcome == outcome
    assert len(requests) == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_authority_revoked_during_oauth_prevents_send(
    configuration: AndroidPushConfiguration,
    credentials: FcmServiceAccount,
) -> None:
    """The last admission runs after the asynchronous credential acquisition.

    Given: A binding whose authority changes while OAuth is acquired.
    When: Post-OAuth admission rejects the binding.
    Then: The result is cancelled and the only HTTP request was OAuth.
    """
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Return an OAuth token without permitting any FCM call."""
        requests.append(request)
        return httpx.Response(
            200, json={"access_token": "synthetic", "token_type": "Bearer", "expires_in": 3600}
        )

    async def revoked() -> bool:
        """Model account/configuration retirement committed during OAuth."""
        return False

    client = FcmClient(
        configuration,
        credentials,
        httpx.AsyncClient(transport=httpx.MockTransport(respond)),
        lambda: _NOW,
    )
    assert (await client.send("opaque-fid", _hint(), revoked)).outcome == "cancelled"
    assert len(requests) == 1
    await client.aclose()


@pytest.mark.parametrize(
    "status,payload,outcome",
    [
        (404, '{"error":{"status":"NOT_FOUND"}}', "permanent"),
        (
            404,
            '{"error":{"details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}',
            "unregistered",
        ),
        (404, '{"error":{"details":[{"@type":"other","errorCode":"UNREGISTERED"}]}}', "permanent"),
        (403, '{"error":{"status":"PERMISSION_DENIED"}}', "permanent"),
        (429, "{}", "retry"),
        (503, "{}", "retry"),
        (200, "{}", "retry"),
        (200, "invalid", "retry"),
        (200, '{"name":"receipt"}', "accepted"),
    ],
)
def test_exact_provider_error_classification(status: int, payload: str, outcome: str) -> None:
    """Only explicit FCM UNREGISTERED retires the bound registration.

    Given: Representative FCM status and error-detail combinations.
    When: Each response is classified.
    Then: Only explicit UNREGISTERED retires a registration and throttling respects its minimum delay.
    """
    result = classify_fcm_response(httpx.Response(status, content=payload), _NOW)
    assert result.outcome == outcome
    if status == 429:
        assert result.retry_at == _NOW + timedelta(seconds=60)


@pytest.mark.parametrize(
    "raw,seconds",
    [
        (None, None),
        ("90", 90),
        ("garbage", None),
        ("Fri, 16 Oct 2026 00:02:00 GMT", 120),
        ("Thu, 15 Oct 2026 00:00:00 GMT", 0),
        ("9999999999", 604800),
    ],
)
def test_retry_after_is_bounded_and_never_past(raw: str | None, seconds: int | None) -> None:
    """Provider retry directives cannot overflow timestamps or cause early delivery.

    Given: Missing, malformed, dated or oversized Retry-After values.
    When: The retry boundary is parsed against a fixed UTC clock.
    Then: The result is absent or bounded without scheduling in the past.
    """
    assert _retry_after(raw, _NOW) == (
        None if seconds is None else _NOW + timedelta(seconds=seconds)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("expired_stage", ["before_oauth", "after_oauth"])
async def test_expiry_fences_provider_send_on_both_sides_of_oauth(
    configuration: AndroidPushConfiguration,
    credentials: FcmServiceAccount,
    expired_stage: str,
) -> None:
    """Credential acquisition cannot renew a hint that expires during the wait.

    Given: A hint that expires before or during authorization.
    When: A send crosses the configured clock boundary.
    Then: No FCM request occurs and the outcome is expired.
    """
    now = _NOW + timedelta(days=2) if expired_stage == "before_oauth" else _NOW
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Advance the clock while returning the only allowed OAuth response."""
        nonlocal now
        requests.append(request)
        now += timedelta(days=2)
        return httpx.Response(
            200, json={"access_token": "synthetic", "token_type": "Bearer", "expires_in": 3600}
        )

    client = FcmClient(
        configuration,
        credentials,
        httpx.AsyncClient(transport=httpx.MockTransport(respond)),
        lambda: now,
    )
    assert (await client.send("fid", _hint(), _admitted)).outcome == "expired"
    assert len(requests) == (0 if expired_stage == "before_oauth" else 1)
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["invalid_token", "disconnect", "redirect", "message_401", "message_503"]
)
async def test_no_message_replay_or_redirect_after_transport_failures(
    configuration: AndroidPushConfiguration,
    credentials: FcmServiceAccount,
    failure: str,
) -> None:
    """Even Retry-After zero never causes this transport to issue a second message.

    Given: Synthetic disconnects, invalid responses, redirects or message failures.
    When: One send is attempted with redirects enabled on the injected client.
    Then: The adapter does not follow redirects or replay the message.
    """
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Supply synthetic failures at the actual HTTP adapter boundary."""
        requests.append(request)
        if failure == "disconnect":
            raise httpx.ConnectError("synthetic", request=request)
        if failure == "invalid_token":
            return httpx.Response(200, content="invalid")
        if failure == "redirect":
            return httpx.Response(307, headers={"Location": "https://example.invalid/"})
        if request.url.host == "oauth2.googleapis.com":
            return httpx.Response(
                200, json={"access_token": "synthetic", "token_type": "Bearer", "expires_in": 3600}
            )
        return httpx.Response(
            401 if failure == "message_401" else 503, headers={"Retry-After": "0"}, json={}
        )

    client = FcmClient(
        configuration,
        credentials,
        httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=True),
        lambda: _NOW,
    )
    result = await client.send("fid", _hint(), _admitted)
    assert result.outcome == ("permanent" if failure in {"redirect", "message_401"} else "retry")
    assert len(requests) == (2 if failure.startswith("message_") else 1)
    await client.aclose()


def test_credentials_reject_non_rsa_key_without_echoing_it(
    configuration: AndroidPushConfiguration,
    credentials: FcmServiceAccount,
) -> None:
    """Valid PEM for a different algorithm is still refused before any HTTP request.

    Given: A valid PEM key for an unsupported signing algorithm.
    When: The FCM client validates its credentials.
    Then: Construction refuses the key without echoing its private bytes.
    """
    key = ec.generate_private_key(ec.SECP256R1())
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    changed = credentials.model_copy(update={"private_key": SecretStr(pem)})
    with pytest.raises(ValueError, match="Invalid FCM signing key") as caught:
        FcmClient(configuration, changed, httpx.AsyncClient(), lambda: _NOW)
    assert pem not in str(caught.value)


@pytest.mark.parametrize(
    "status,outcome",
    [(400, "permanent"), (403, "permanent"), (404, "permanent"), (429, "retry"), (503, "retry")],
)
def test_non_json_status_retains_retry_and_invalidation_semantics(
    status: int, outcome: str
) -> None:
    """An HTML proxy body never turns a permanent refusal into a retry or token retirement.

    Given: A proxy HTML body with a permanent or retryable HTTP status.
    When: The provider response is classified.
    Then: Permanent refusals remain permanent and throttling keeps its minimum delay.
    """
    result = classify_fcm_response(
        httpx.Response(status, content="<html>proxy</html>", headers={"Retry-After": "0"}), _NOW
    )
    assert result.outcome == outcome
    if status == 429:
        assert result.retry_at == _NOW + timedelta(seconds=60)


@pytest.mark.asyncio
async def test_message_401_invalidates_cached_oauth_only_for_next_hint(
    configuration: AndroidPushConfiguration,
    credentials: FcmServiceAccount,
) -> None:
    """No replay occurs, but a different hint obtains fresh credentials after a known invalid token.

    Given: A cached authorization rejected by the first message request.
    When: A separate second hint is submitted.
    Then: The first message is not replayed and the second acquires fresh authorization.
    """
    oauth = 0
    messages = 0

    def respond(request: httpx.Request) -> httpx.Response:
        """Reject the first message and accept a separately requested second hint."""
        nonlocal oauth, messages
        if request.url.host == "oauth2.googleapis.com":
            oauth += 1
            return httpx.Response(
                200,
                json={
                    "access_token": f"synthetic-{oauth}",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                },
            )
        messages += 1
        return httpx.Response(401 if messages == 1 else 200, json={"name": "receipt"})

    client = FcmClient(
        configuration,
        credentials,
        httpx.AsyncClient(transport=httpx.MockTransport(respond)),
        lambda: _NOW,
    )
    assert (await client.send("fid", _hint(), _admitted)).outcome == "permanent"
    assert (oauth, messages) == (1, 1)
    assert (await client.send("fid", _hint(), _admitted)).outcome == "accepted"
    assert (oauth, messages) == (2, 2)
    await client.aclose()


@pytest.mark.asyncio
async def test_late_401_does_not_discard_a_newer_cached_authorization(
    configuration: AndroidPushConfiguration,
    credentials: FcmServiceAccount,
) -> None:
    """An old request may invalidate only the OAuth token it actually used.

    Given: An old request and a newer cached OAuth token.
    When: The old request receives HTTP 401.
    Then: The newer token remains available for the next hint.
    """

    def respond(request: httpx.Request) -> httpx.Response:
        """Reject the message after the cache has independently advanced."""
        if request.url.host == "oauth2.googleapis.com":
            return httpx.Response(
                200, json={"access_token": "old", "token_type": "Bearer", "expires_in": 3600}
            )
        return httpx.Response(401, json={})

    client = FcmClient(
        configuration,
        credentials,
        httpx.AsyncClient(transport=httpx.MockTransport(respond)),
        lambda: _NOW,
    )

    async def admission() -> bool:
        """Represent a newer concurrent authorization response already committed in memory."""
        client._token = SecretStr("new")
        return True

    assert (await client.send("fid", _hint(), admission)).outcome == "permanent"
    assert await client._access_token() == "new"
    await client.aclose()
