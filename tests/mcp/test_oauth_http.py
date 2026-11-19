"""OAuth Slice 2 HTTP adapters through the real ``create_app()``.

The OAuth service, repository and token manager are real (isolated SQLite with
two registered confidential clients). Only authentication of the consent
owner, the repository dependency and the bootstrap settings are overridden.
"""

import base64
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs
from urllib.parse import urlsplit
from uuid import uuid7

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response
from sqlalchemy import select

from snapper.application.ai_delegates.service import DelegateOperatorBindingError
from snapper.application.ai_delegates.service import DelegateProliferationError
from snapper.auth.dependencies import get_csrf_manager
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.json_types import JsonObject
from snapper.data.models import OAuthAuthorizationRequest
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.mcp.oauth.service import MCPOAuthService
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server import mcp_oauth_routes
from snapper.server.app import create_app
from snapper.server.dependencies import get_repository_dependency
from snapper.server.mcp_oauth_routes import oauth_settings
from tests.mcp.test_oauth_service import _CLIENT_ID
from tests.mcp.test_oauth_service import _CLIENT_SECRET
from tests.mcp.test_oauth_service import _OPERATOR_A
from tests.mcp.test_oauth_service import _OTHER_CLIENT_ID
from tests.mcp.test_oauth_service import _OTHER_CLIENT_SECRET
from tests.mcp.test_oauth_service import _REDIRECT_URI
from tests.mcp.test_oauth_service import _RESOURCE
from tests.mcp.test_oauth_service import _VERIFIER
from tests.mcp.test_oauth_service import _build_harness
from tests.mcp.test_oauth_service import _OAuthHarness
from tests.mcp.test_oauth_service import _pkce_s256
from tests.mcp.test_oauth_service import _principal
from tests.mcp.test_oauth_service import _seed_user_with_membership

_ISSUER = _RESOURCE
_FORM = {"content-type": "application/x-www-form-urlencoded"}
_ROUTES = [
    ("GET", "/.well-known/oauth-protected-resource/api/mcp"),
    ("GET", "/api/mcp/.well-known/oauth-protected-resource"),
    ("GET", "/.well-known/oauth-authorization-server/api/mcp"),
    ("GET", "/api/mcp/.well-known/oauth-authorization-server"),
    ("GET", "/api/mcp/authorize"),
    ("POST", "/api/mcp/token"),
    ("POST", "/api/mcp/revoke"),
    ("GET", "/api/mcp-oauth/consent"),
    ("POST", "/api/mcp-oauth/consent/approve"),
    ("POST", "/api/mcp-oauth/consent/deny"),
]


class _Flags:
    """Live database settings stand-in for ``ai_integration_enabled``."""

    def __init__(self) -> None:
        """Start with the integration enabled, as the real default is."""
        self.values: dict[str, bool] = {}

    def get_setting(self, key: str, default: bool) -> bool:
        """Return one live flag."""
        return self.values.get(key, default)


@dataclass
class HttpCase:
    """One real app, client and seeded OAuth harness."""

    app: FastAPI
    client: TestClient
    harness: _OAuthHarness
    owner: AuthPrincipal
    flags: _Flags


@asynccontextmanager
async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable the production lifespan."""
    yield


def _settings(*, enabled: bool) -> AppSettings:
    """Exact canonical bootstrap identities, as production will configure them."""
    return AppSettings(
        BootstrapSettingsLoader(
            _env_file=None,
            MCP_OAUTH_ENABLED=enabled,
            MCP_PUBLIC_RESOURCE_URL=_RESOURCE,
            MCP_OAUTH_ISSUER=_ISSUER,
        )
    )


def _build_case(harness: _OAuthHarness, owner: AuthPrincipal, *, enabled: bool) -> HttpCase:
    """Assemble the real app around the harness repository."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    app.state.rest_tracker = SequenceTracker()
    flags = _Flags()
    app.state.settings_service = flags
    app.dependency_overrides[oauth_settings] = lambda: _settings(enabled=enabled)
    app.dependency_overrides[get_repository_dependency] = lambda: harness.repository
    app.dependency_overrides[require_authentication] = lambda: owner
    app.dependency_overrides[validate_csrf_token] = lambda: None
    return HttpCase(app, TestClient(app, follow_redirects=False), harness, owner, flags)


@pytest.fixture
async def case(tmp_path: Path) -> AsyncGenerator[HttpCase]:
    """An enabled OAuth deployment with one authorized owner."""
    harness = await _build_harness(tmp_path)
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    built = _build_case(
        harness, _principal(owner_id, membership_public_id=membership), enabled=True
    )
    try:
        yield built
    finally:
        await harness.repository.engine.dispose()


@pytest.fixture
async def disabled(tmp_path: Path) -> AsyncGenerator[HttpCase]:
    """The source default: MCP_OAUTH_ENABLED=false."""
    harness = await _build_harness(tmp_path)
    built = _build_case(harness, _principal(str(uuid7())), enabled=False)
    try:
        yield built
    finally:
        await harness.repository.engine.dispose()


def _authorize_params(**overrides: str | None) -> dict[str, str]:
    """Exact registered authorize parameters; ``None`` removes a parameter."""
    params: dict[str, str | None] = {
        "response_type": "code",
        "client_id": _CLIENT_ID,
        "redirect_uri": _REDIRECT_URI,
        "scope": "snapper.read offline_access",
        "state": "opaque-state",
        "code_challenge": _pkce_s256(_VERIFIER),
        "code_challenge_method": "S256",
        "resource": _RESOURCE,
    }
    params.update(overrides)
    return {key: value for key, value in params.items() if value is not None}


def _basic(client_id: str = _CLIENT_ID, secret: str = _CLIENT_SECRET) -> dict[str, str]:
    """RFC 6749 HTTP Basic client authentication header."""
    raw = base64.b64encode(f"{client_id}:{secret}".encode()).decode()
    return {**_FORM, "authorization": f"Basic {raw}"}


def _envelope(kind: str, payload: JsonObject) -> JsonObject:
    """The frontend's standard provenance request envelope."""
    return {
        "type": kind,
        "public_id": str(uuid7()),
        "session_id": "consent-test",
        "sequence_id": 1,
        "timestamp": "2026-11-18T12:00:00Z",
        "payload": payload,
    }


def _pending(case: HttpCase, **overrides: str | None) -> str:
    """Run a real authorize request and return the opaque consent request id."""
    response = case.client.get("/api/mcp/authorize", params=_authorize_params(**overrides))
    assert response.status_code == 302
    location = urlsplit(response.headers["location"])
    assert location.path == "/oauth/authorize" and location.query == ""
    return parse_qs(location.fragment)["request"][0]


def _describe(case: HttpCase, request_id: str) -> Response:
    """Read the consent view with the capability in its dedicated header."""
    return case.client.get("/api/mcp-oauth/consent", headers={"X-MCP-OAuth-Request": request_id})


def _approve(case: HttpCase, request_id: str, *, replace: bool = False) -> dict[str, list[str]]:
    """Approve through the consent API and return the callback query."""
    response = case.client.post(
        "/api/mcp-oauth/consent/approve",
        json=_envelope(
            "mcp_oauth_consent_approve_request",
            {
                "oauth_request": request_id,
                "operator_public_id": _OPERATOR_A,
                "replace_existing_grant": replace,
            },
        ),
    )
    assert response.status_code == 200, response.text
    target = urlsplit(response.json()["payload"]["redirect_to"])
    assert f"{target.scheme}://{target.netloc}{target.path}" == _REDIRECT_URI
    return parse_qs(target.query)


def _exchange(case: HttpCase, code: str) -> JsonObject:
    """Exchange one code with Basic client authentication."""
    response = case.client.post(
        "/api/mcp/token",
        headers=_basic(),
        data={
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": _VERIFIER,
            "redirect_uri": _REDIRECT_URI,
            "resource": _RESOURCE,
        },
    )
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    body: JsonObject = response.json()
    return body


def _tokens(case: HttpCase) -> JsonObject:
    """Complete authorize, consent and code exchange."""
    return _exchange(case, _approve(case, _pending(case))["code"][0])


def test_every_oauth_surface_is_404_by_default(disabled: HttpCase) -> None:
    """MCP_OAUTH_ENABLED=false publishes nothing.

    Given the source-default bootstrap.
    When any OAuth metadata, protocol or consent route is called.
    Then each answers 404 and no metadata is disclosed.
    """
    for method, path in _ROUTES:
        response = disabled.client.request(method, path)
        assert response.status_code == 404, (method, path)


def test_registration_and_openid_stay_absent(case: HttpCase) -> None:
    """DCR and OpenID discovery are never published.

    Given an enabled deployment.
    When the registration endpoint or OpenID configuration is requested.
    Then both are 404 and the metadata names no registration endpoint.
    """
    assert case.client.post("/api/mcp/register", json={}).status_code == 404
    assert case.client.get("/.well-known/openid-configuration").status_code == 404
    metadata = case.client.get("/.well-known/oauth-authorization-server/api/mcp").json()
    assert "registration_endpoint" not in metadata


def test_metadata_spellings_are_equivalent_and_exact(case: HttpCase) -> None:
    """Canonical and endpoint-relative documents agree on exact identities.

    Given an enabled deployment with issuer and resource without trailing slash.
    When both spellings of each metadata document are fetched.
    Then they are identical, exact and advertise S256 and both client auth methods only.
    """
    resource = case.client.get("/.well-known/oauth-protected-resource/api/mcp").json()
    assert resource == case.client.get("/api/mcp/.well-known/oauth-protected-resource").json()
    assert resource["resource"] == _RESOURCE
    assert resource["authorization_servers"] == [_ISSUER]
    server = case.client.get("/.well-known/oauth-authorization-server/api/mcp").json()
    assert server == case.client.get("/api/mcp/.well-known/oauth-authorization-server").json()
    assert server["issuer"] == _ISSUER
    assert server["token_endpoint"] == f"{_ISSUER}/token"
    assert server["code_challenge_methods_supported"] == ["S256"]
    assert server["token_endpoint_auth_methods_supported"] == [
        "client_secret_basic",
        "client_secret_post",
    ]


def test_metadata_stays_stable_while_live_integration_is_disabled(case: HttpCase) -> None:
    """D5: the database flag never changes discovery.

    Given ai_integration_enabled=false.
    When metadata is fetched.
    Then it is still served unchanged.
    """
    before = case.client.get("/.well-known/oauth-authorization-server/api/mcp").json()
    case.flags.values["ai_integration_enabled"] = False
    assert case.client.get("/.well-known/oauth-authorization-server/api/mcp").json() == before


@pytest.mark.parametrize(
    "overrides",
    [
        {"client_id": "unknown-client"},
        {"redirect_uri": "https://attacker.example/callback"},
        {"redirect_uri": f"{_REDIRECT_URI}/extra"},
    ],
    ids=["unknown-client", "foreign-redirect", "prefix-redirect"],
)
def test_untrusted_client_or_redirect_never_redirects(
    case: HttpCase, overrides: dict[str, str]
) -> None:
    """Trusted redirect validation precedes every error redirect.

    Given an unknown client or an unregistered callback.
    When authorize is called, even with other invalid parameters.
    Then the answer is a direct 400 with no Location header.
    """
    response = case.client.get(
        "/api/mcp/authorize", params=_authorize_params(response_type="token", **overrides)
    )
    assert response.status_code == 400
    assert "location" not in response.headers


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"scope": None}, "invalid_scope"),
        ({"scope": ""}, "invalid_scope"),
        ({"scope": "offline_access"}, "invalid_scope"),
        ({"scope": "snapper.read snapper.trade"}, "invalid_scope"),
        ({"code_challenge_method": "plain"}, "invalid_request"),
        ({"resource": f"{_RESOURCE}/"}, "invalid_target"),
        ({"response_type": "token"}, "unsupported_response_type"),
    ],
    ids=["omitted", "empty", "offline-only", "widened", "pkce-plain", "resource", "implicit"],
)
def test_invalid_authorize_parameters_redirect_back_with_an_error(
    case: HttpCase, overrides: dict[str, str | None], error: str
) -> None:
    """An omitted scope is refused, never defaulted (D1); other faults go back to the client.

    Given a trusted client and callback with one invalid parameter.
    When authorize is called.
    Then it redirects to the registered callback with the error, state and issuer.
    """
    response = case.client.get("/api/mcp/authorize", params=_authorize_params(**overrides))
    assert response.status_code == 302
    target = urlsplit(response.headers["location"])
    assert f"{target.scheme}://{target.netloc}{target.path}" == _REDIRECT_URI
    query = parse_qs(target.query)
    assert query == {"error": [error], "state": ["opaque-state"], "iss": [_ISSUER]}


def test_authorize_is_refused_while_live_integration_is_disabled(case: HttpCase) -> None:
    """D5: the live flag refuses new authorizations without a pending row.

    Given ai_integration_enabled=false.
    When a valid authorize request arrives.
    Then the client is sent back with temporarily_unavailable.
    """
    case.flags.values["ai_integration_enabled"] = False
    response = case.client.get("/api/mcp/authorize", params=_authorize_params())
    assert parse_qs(urlsplit(response.headers["location"]).query)["error"] == [
        "temporarily_unavailable"
    ]


def test_full_flow_consent_code_refresh_narrowing_and_revocation(case: HttpCase) -> None:
    """The complete dark flow works end to end through the real app.

    Given a registered client and an authorized owner.
    When authorize, consent, code exchange, rotation, narrowing and revocation run.
    Then each step answers per RFC and the final revocation retires the grant.
    """
    request_id = _pending(case)
    detail = _describe(case, request_id)
    assert detail.status_code == 200
    assert detail.headers["cache-control"] == "no-store"
    payload = detail.json()["payload"]
    assert payload["client_name"] == "ChatGPT Connector"
    assert payload["scopes"] == ["snapper.read", "offline_access"]
    assert payload["resource"] == _RESOURCE
    assert payload["read_only"] is True
    assert "opaque-state" not in detail.text and _REDIRECT_URI not in detail.text
    callback = _approve(case, request_id)
    assert callback["state"] == ["opaque-state"] and callback["iss"] == [_ISSUER]
    tokens = _exchange(case, callback["code"][0])
    assert tokens["token_type"] == "Bearer" and tokens["scope"] == "snapper.read offline_access"
    replay = case.client.post(
        "/api/mcp/token",
        headers=_basic(),
        data={
            "grant_type": "authorization_code",
            "code": callback["code"][0],
            "code_verifier": _VERIFIER,
            "redirect_uri": _REDIRECT_URI,
            "resource": _RESOURCE,
        },
    )
    assert replay.status_code == 400 and replay.json() == {"error": "invalid_grant"}
    rotated = case.client.post(
        "/api/mcp/token",
        headers=_FORM,
        data={
            "grant_type": "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "resource": _RESOURCE,
            "client_id": _CLIENT_ID,
            "client_secret": _CLIENT_SECRET,
        },
    )
    assert rotated.status_code == 401
    rotated = case.client.post(
        "/api/mcp/token",
        headers=_basic(),
        data={
            "grant_type": "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "resource": _RESOURCE,
        },
    ).json()
    assert rotated["refresh_token"] != tokens["refresh_token"]
    narrowed = case.client.post(
        "/api/mcp/token",
        headers=_basic(),
        data={
            "grant_type": "refresh_token",
            "refresh_token": rotated["refresh_token"],
            "resource": _RESOURCE,
            "scope": "snapper.read",
        },
    ).json()
    assert "refresh_token" not in narrowed and narrowed["scope"] == "snapper.read"
    revoked = case.client.post(
        "/api/mcp/revoke", headers=_basic(), data={"token": narrowed["access_token"]}
    )
    assert revoked.status_code == 200 and revoked.json() == {}


async def _grants_revoked(case: HttpCase) -> list[bool]:
    """Return the revoked state of every grant."""
    async with case.harness.repository.session() as session:
        grants = (await session.execute(select(OAuthGrant))).scalars().all()
        return [grant.revoked_at is not None for grant in grants]


async def test_revocation_rules_and_its_availability(case: HttpCase) -> None:
    """Revocation is RFC 7009 compliant and survives the live flag.

    Given an issued token family.
    When unknown, missing, other-client and owned tokens are revoked, with the flag off.
    Then only the owned revocation mutates, and every call shape answers per RFC.
    """
    tokens = _tokens(case)
    case.flags.values["ai_integration_enabled"] = False
    assert (
        case.client.post("/api/mcp/revoke", headers=_basic(), data={"token": "x"}).status_code
        == 200
    )
    assert case.client.post("/api/mcp/revoke", headers=_basic(), data={}).json() == {
        "error": "invalid_request"
    }
    other = case.client.post(
        "/api/mcp/revoke",
        headers=_FORM,
        data={
            "token": tokens["refresh_token"],
            "client_id": _OTHER_CLIENT_ID,
            "client_secret": _OTHER_CLIENT_SECRET,
        },
    )
    assert other.status_code == 200
    assert await _grants_revoked(case) == [False]
    owned = case.client.post(
        "/api/mcp/revoke", headers=_basic(), data={"token": tokens["refresh_token"]}
    )
    assert owned.status_code == 200
    assert await _grants_revoked(case) == [True]
    async with case.harness.repository.session() as session:
        rows = (await session.execute(select(OAuthRefreshToken))).scalars().all()
    assert all(row.revoked_at is not None for row in rows)


@pytest.mark.parametrize(
    ("headers", "data", "status_code", "error"),
    [
        (_basic(secret="wrong"), {"grant_type": "refresh_token"}, 401, "invalid_client"),
        (_FORM, {"grant_type": "refresh_token"}, 401, "invalid_client"),
        (
            {**_FORM, "authorization": "Basic !!!"},
            {"grant_type": "refresh_token"},
            401,
            "invalid_client",
        ),
        (
            {**_FORM, "authorization": "Basic " + base64.b64encode(b"no-colon").decode()},
            {"grant_type": "refresh_token"},
            401,
            "invalid_client",
        ),
        (
            _basic(),
            {
                "grant_type": "refresh_token",
                "client_id": _CLIENT_ID,
                "client_secret": _CLIENT_SECRET,
            },
            401,
            "invalid_client",
        ),
        (_basic(), {"grant_type": "password"}, 400, "unsupported_grant_type"),
        (
            _basic(),
            {"grant_type": "refresh_token", "refresh_token": "x", "resource": _RESOURCE},
            400,
            "invalid_grant",
        ),
    ],
    ids=[
        "wrong-secret",
        "no-credentials",
        "malformed-basic",
        "basic-without-colon",
        "two-methods",
        "unsupported-grant",
        "unknown-refresh",
    ],
)
def test_token_endpoint_errors_follow_rfc_6749(
    case: HttpCase, headers: dict[str, str], data: dict[str, str], status_code: int, error: str
) -> None:
    """Every token refusal is an RFC error with no internal detail.

    Given a malformed or unauthenticated token request.
    When it reaches the token endpoint.
    Then the RFC status and error code are returned, uncacheable.
    """
    response = case.client.post("/api/mcp/token", headers=headers, data=data)
    assert response.status_code == status_code
    assert response.json() == {"error": error}
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.parametrize("grant", ["authorization_code", "refresh_token"])
def test_both_exchanges_require_the_exact_resource(case: HttpCase, grant: str) -> None:
    """The exact resource is enforced on code exchange and on refresh.

    Given a valid code or refresh token.
    When it is exchanged without the resource parameter.
    Then invalid_target is returned and the credential is not consumed.
    """
    request_id = _pending(case)
    code = _approve(case, request_id)["code"][0]
    if grant == "authorization_code":
        data = {
            "grant_type": grant,
            "code": code,
            "code_verifier": _VERIFIER,
            "redirect_uri": _REDIRECT_URI,
        }
    else:
        data = {"grant_type": grant, "refresh_token": str(_exchange(case, code)["refresh_token"])}
    refused = case.client.post("/api/mcp/token", headers=_basic(), data=data)
    assert refused.status_code == 400 and refused.json() == {"error": "invalid_target"}
    accepted = case.client.post(
        "/api/mcp/token", headers=_basic(), data={**data, "resource": _RESOURCE}
    )
    assert accepted.status_code == 200


def test_token_basic_failure_carries_a_basic_challenge(case: HttpCase) -> None:
    """RFC 6749 section 5.2: a failed Basic authentication names the scheme.

    Given a wrong secret sent with HTTP Basic.
    When the token endpoint refuses it.
    Then the 401 carries a Basic WWW-Authenticate challenge.
    """
    response = case.client.post(
        "/api/mcp/token", headers=_basic(secret="wrong"), data={"grant_type": "refresh_token"}
    )
    assert response.headers["www-authenticate"].startswith("Basic ")


@pytest.mark.parametrize(
    ("headers", "content"),
    [
        ({"content-type": "application/json"}, b'{"grant_type": "refresh_token"}'),
        (_FORM, b"grant_type=refresh_token&grant_type=authorization_code"),
    ],
    ids=["json-body", "repeated-parameter"],
)
def test_token_endpoint_requires_one_valued_form_bodies(
    case: HttpCase, headers: dict[str, str], content: bytes
) -> None:
    """A non-form or repeated-parameter body is an invalid request.

    Given a JSON body or a form with a repeated parameter.
    When it reaches the token endpoint.
    Then invalid_request is returned before any client authentication.
    """
    response = case.client.post("/api/mcp/token", headers=headers, content=content)
    assert response.status_code == 400
    assert response.json() == {"error": "invalid_request"}


def test_token_issuance_is_refused_while_live_integration_is_disabled(case: HttpCase) -> None:
    """D5: no new token while the live flag is off.

    Given an issued refresh token and ai_integration_enabled=false.
    When the client refreshes.
    Then 503 temporarily_unavailable is returned and the family is untouched.
    """
    tokens = _tokens(case)
    case.flags.values["ai_integration_enabled"] = False
    response = case.client.post(
        "/api/mcp/token",
        headers=_basic(),
        data={
            "grant_type": "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "resource": _RESOURCE,
        },
    )
    assert response.status_code == 503
    assert response.json() == {"error": "temporarily_unavailable"}


def test_consent_read_ignores_a_query_credential(case: HttpCase) -> None:
    """v1.1: the capability is accepted only in its header, never in the query.

    Given a valid pending request.
    When the consent view is read with the id only as a query parameter.
    Then it answers consent_request_invalid, and the header form still works.
    """
    request_id = _pending(case)
    query = case.client.get("/api/mcp-oauth/consent", params={"oauth_request": request_id})
    assert query.status_code == 400
    assert query.json() == {"detail": {"error_code": "consent_request_invalid"}}
    assert _describe(case, request_id).status_code == 200


def test_unavailable_consent_requests_are_indistinguishable(case: HttpCase) -> None:
    """Unknown and resolved requests give the same structured error.

    Given one never-issued and one already approved request.
    When the consent page reads or approves them.
    Then both answer 400 with consent_request_invalid.
    """
    resolved = _pending(case)
    _approve(case, resolved)
    for request_id in ["never-issued-request-id", resolved]:
        read = _describe(case, request_id)
        assert read.status_code == 400
        assert read.json() == {"detail": {"error_code": "consent_request_invalid"}}
        approve = case.client.post(
            "/api/mcp-oauth/consent/approve",
            json=_envelope(
                "mcp_oauth_consent_approve_request",
                {"oauth_request": request_id, "replace_existing_grant": False},
            ),
        )
        assert approve.json() == {"detail": {"error_code": "consent_request_invalid"}}


def test_incompatible_reconsent_requires_explicit_replacement(case: HttpCase) -> None:
    """D2: a different scope set needs replace_existing_grant=true.

    Given an existing read/offline grant.
    When the owner approves a read-only request for the same client and operator.
    Then 409 grant_replacement_required is returned until replacement is confirmed.
    """
    _approve(case, _pending(case))
    narrower = _pending(case, scope="snapper.read")
    conflict = case.client.post(
        "/api/mcp-oauth/consent/approve",
        json=_envelope(
            "mcp_oauth_consent_approve_request",
            {
                "oauth_request": narrower,
                "operator_public_id": _OPERATOR_A,
                "replace_existing_grant": False,
            },
        ),
    )
    assert conflict.status_code == 409
    assert conflict.json() == {"detail": {"error_code": "grant_replacement_required"}}
    assert "code" in _approve(case, narrower, replace=True)


def test_deny_redirects_back_with_access_denied(case: HttpCase) -> None:
    """Denial resolves the request and hands the client its error and state.

    Given a pending request.
    When the owner denies it.
    Then the redirect target is the exact callback with access_denied, state and issuer.
    """
    response = case.client.post(
        "/api/mcp-oauth/consent/deny",
        json=_envelope(
            "mcp_oauth_consent_deny_request",
            {"oauth_request": _pending(case), "operator_public_id": _OPERATOR_A},
        ),
    )
    assert response.status_code == 200
    target = urlsplit(response.json()["payload"]["redirect_to"])
    assert parse_qs(target.query) == {
        "error": ["access_denied"],
        "state": ["opaque-state"],
        "iss": [_ISSUER],
    }


def test_deny_with_a_foreign_operator_is_refused(case: HttpCase) -> None:
    """A denial still needs current authority over the selected operator.

    Given a pending request.
    When the owner denies it for an operator they do not belong to.
    Then 403 consent_access_denied is returned.
    """
    response = case.client.post(
        "/api/mcp-oauth/consent/deny",
        json=_envelope(
            "mcp_oauth_consent_deny_request",
            {"oauth_request": _pending(case), "operator_public_id": str(uuid7())},
        ),
    )
    assert response.status_code == 403
    assert response.json() == {"detail": {"error_code": "consent_access_denied"}}


def test_consent_api_is_disabled_with_the_live_flag(case: HttpCase) -> None:
    """The consent API keeps the existing feature_disabled envelope.

    Given a pending request and ai_integration_enabled=false.
    When the consent page reads it.
    Then the existing 503 feature_disabled body is returned.
    """
    request_id = _pending(case)
    case.flags.values["ai_integration_enabled"] = False
    response = _describe(case, request_id)
    assert response.status_code == 503
    assert response.json()["error_code"] == "feature_disabled"


def test_consent_requires_ai_integration_management(case: HttpCase) -> None:
    """Only owners who may manage AI integration can see or decide consent.

    Given an authenticated viewer without manage:ai_integration.
    When they read a pending request.
    Then the normal 403 permission response is returned.
    """
    request_id = _pending(case)
    case.app.dependency_overrides[require_authentication] = lambda: _principal(
        case.owner.user_public_id, role=UserRole.VIEWER
    )
    response = _describe(case, request_id)
    assert response.status_code == 403


def test_consent_mutations_enforce_csrf(case: HttpCase) -> None:
    """A cookie-session decision without a CSRF token is refused.

    Given the real CSRF dependency.
    When approve is posted without CSRF cookie or header.
    Then 403 is returned and the request stays pending.
    """
    request_id = _pending(case)
    del case.app.dependency_overrides[validate_csrf_token]
    response = case.client.post(
        "/api/mcp-oauth/consent/approve",
        json=_envelope(
            "mcp_oauth_consent_approve_request",
            {"oauth_request": request_id, "replace_existing_grant": False},
        ),
    )
    assert response.status_code == 403
    case.app.dependency_overrides[validate_csrf_token] = lambda: None
    assert _describe(case, request_id).status_code == 200


@pytest.mark.parametrize("decision", ["approve", "deny"])
async def test_consent_requires_browser_session_and_csrf(case: HttpCase, decision: str) -> None:
    """Consent accepts the owner's browser cookie and its paired CSRF proof.

    Given a real persisted session and a pending authorization,
    When read and decision requests use the owner's bearer or cookie credentials,
    Then only the cookie session may read, and only its CSRF-protected decision succeeds.
    """
    del case.app.dependency_overrides[require_authentication]
    del case.app.dependency_overrides[validate_csrf_token]
    manager = case.harness.token_manager
    pair = manager.create_tokens(case.owner)
    await manager.persist_tokens(pair, case.owner.user_public_id, case.harness.repository)
    request_id = _pending(case)
    payload = _envelope(
        f"mcp_oauth_consent_{decision}_request",
        {"oauth_request": request_id, "operator_public_id": _OPERATOR_A},
    )
    url = f"/api/mcp-oauth/consent/{decision}"
    case.client.headers["Authorization"] = f"Bearer {pair.access_token}"
    assert _describe(case, request_id).status_code == 401
    assert case.client.post(url, json=payload).status_code == 401
    case.client.cookies.set("access_token", pair.access_token)
    assert case.client.post(url, json=payload).status_code == 401
    del case.client.headers["Authorization"]
    assert _describe(case, request_id).status_code == 200
    assert case.client.post(url, json=payload).status_code == 403
    csrf = get_csrf_manager().generate_token()
    case.client.cookies.set("csrf_token", csrf)
    assert case.client.post(url, json=payload, headers={"X-CSRF-Token": csrf}).status_code == 200


def test_oauth_settings_default_to_the_cached_bootstrap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without an override, routes read the process-wide bootstrap settings.

    Given no dependency override and a known bootstrap settings object.
    When the settings dependency is called.
    Then it returns exactly what the bootstrap settings accessor returns.
    """
    expected = _settings(enabled=False)
    monkeypatch.setattr(mcp_oauth_routes, "get_settings", lambda: expected)
    assert oauth_settings() is expected


def test_live_integration_defaults_to_enabled_without_a_settings_service(case: HttpCase) -> None:
    """Before the settings service starts, the integration flag defaults to on.

    Given an app whose settings service has not been initialised.
    When a valid authorize request arrives.
    Then it proceeds to the consent page rather than reporting unavailability.
    """
    case.app.state.settings_service = None
    assert _pending(case)


@pytest.mark.parametrize(
    ("error", "status_code", "code"),
    [
        (DelegateProliferationError("cap"), 403, "consent_access_denied"),
        (DelegateOperatorBindingError("operator"), 403, "consent_access_denied"),
    ],
    ids=["delegate-cap", "operator-binding"],
)
def test_delegate_refusals_map_to_stable_consent_codes(
    case: HttpCase,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    status_code: int,
    code: str,
) -> None:
    """Delegate-service refusals during approval become structured consent errors.

    Given a pending request and a delegate service that refuses the binding.
    When the owner approves.
    Then the stable consent error code is returned with no internal message.
    """
    request_id = _pending(case)

    async def refuse(*_args: object, **_kwargs: object) -> None:
        """Raise the delegate refusal under test."""
        raise error

    monkeypatch.setattr(MCPOAuthService, "approve_authorization_request", refuse)
    response = case.client.post(
        "/api/mcp-oauth/consent/approve",
        json=_envelope(
            "mcp_oauth_consent_approve_request",
            {"oauth_request": request_id, "replace_existing_grant": False},
        ),
    )
    assert response.status_code == status_code
    assert response.json() == {"detail": {"error_code": code}}


@pytest.mark.parametrize("caller", ["anonymous", "viewer"])
def test_consent_read_checks_session_and_permission_before_any_lookup(
    case: HttpCase, monkeypatch: pytest.MonkeyPatch, caller: str
) -> None:
    """Authentication (401) and permission (403) are refused before describe runs.

    The pending request is a pre-login capability without an owner, so the route
    must never consult it for an unauthenticated or unauthorised caller.

    Given a valid pending request and a describe method that must not be called.
    When an anonymous caller or a viewer reads the consent view.
    Then 401 or 403 is returned and the pending request is never looked up.
    """
    request_id = _pending(case)

    async def forbidden(*_args: object, **_kwargs: object) -> None:
        """Fail the test if the capability is ever consulted."""
        raise AssertionError("describe_authorization_request must not run")

    monkeypatch.setattr(MCPOAuthService, "describe_authorization_request", forbidden)
    if caller == "anonymous":
        del case.app.dependency_overrides[require_authentication]
        expected = 401
    else:
        case.app.dependency_overrides[require_authentication] = lambda: _principal(
            case.owner.user_public_id, role=UserRole.VIEWER
        )
        expected = 403
    assert _describe(case, request_id).status_code == expected


async def _pending_rows(case: HttpCase) -> int:
    """Count persisted pending authorization requests."""
    async with case.harness.repository.session() as session:
        return len((await session.execute(select(OAuthAuthorizationRequest))).scalars().all())


def _refresh_form(refresh_token: str, **extra: str) -> dict[str, str]:
    """Build an exact-resource refresh form."""
    return {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "resource": _RESOURCE,
        **extra,
    }


def test_explicit_empty_refresh_scope_is_refused_without_consuming_the_token(
    case: HttpCase,
) -> None:
    """F1: a blank scope is not an omitted scope.

    Given an issued read/offline refresh token.
    When the client refreshes with an explicit empty scope.
    Then invalid_scope is returned, and the same token still refreshes afterwards.
    """
    tokens = _tokens(case)
    refused = case.client.post(
        "/api/mcp/token",
        headers=_basic(),
        data=_refresh_form(str(tokens["refresh_token"]), scope=""),
    )
    assert refused.status_code == 400 and refused.json() == {"error": "invalid_scope"}
    rotated = case.client.post(
        "/api/mcp/token", headers=_basic(), data=_refresh_form(str(tokens["refresh_token"]))
    )
    assert rotated.status_code == 200 and "refresh_token" in rotated.json()


@pytest.mark.parametrize(
    "content",
    [
        b"grant_type=refresh_token&scope=snapper.read&scope=",
        b"grant_type=refresh_token&grant_type=",
        b"grant_type",
        b"grant_type=refresh_token&\xff=1",
    ],
    ids=["blank-duplicate", "blank-duplicate-grant", "malformed-field", "non-utf8"],
)
def test_ambiguous_or_malformed_token_forms_are_invalid_requests(
    case: HttpCase, content: bytes
) -> None:
    """Blank duplicates, fields without '=' and undecodable bodies are refused.

    Given a token request body that is ambiguous or malformed.
    When it reaches the token endpoint.
    Then invalid_request is returned before any client authentication.
    """
    response = case.client.post("/api/mcp/token", headers=_FORM, content=content)
    assert response.status_code == 400 and response.json() == {"error": "invalid_request"}


def test_blank_redirect_uri_is_not_an_omitted_redirect_uri(case: HttpCase) -> None:
    """A code bound to an explicit redirect cannot be exchanged with a blank one.

    Given a code issued for an explicitly supplied redirect URI.
    When it is exchanged with redirect_uri set to an empty value.
    Then invalid_grant is returned and the code remains exchangeable with the exact URI.
    """
    code = _approve(case, _pending(case))["code"][0]
    form = {
        "grant_type": "authorization_code",
        "code": code,
        "code_verifier": _VERIFIER,
        "resource": _RESOURCE,
    }
    blank = case.client.post("/api/mcp/token", headers=_basic(), data={**form, "redirect_uri": ""})
    assert blank.status_code == 400 and blank.json() == {"error": "invalid_grant"}
    exact = case.client.post(
        "/api/mcp/token", headers=_basic(), data={**form, "redirect_uri": _REDIRECT_URI}
    )
    assert exact.status_code == 200


def test_basic_credentials_are_form_url_decoded(case: HttpCase) -> None:
    """N1: RFC 6749 section 2.3.1 form-urlencodes Basic credentials before base64.

    Given a client id and secret sent percent-encoded inside HTTP Basic.
    When the client refreshes.
    Then the decoded credentials authenticate.
    """
    tokens = _tokens(case)
    encoded_id = "".join(f"%{ord(char):02X}" for char in _CLIENT_ID)
    encoded_secret = f"%{ord(_CLIENT_SECRET[0]):02X}{_CLIENT_SECRET[1:]}"
    raw = base64.b64encode(f"{encoded_id}:{encoded_secret}".encode()).decode()
    headers = {**_FORM, "authorization": f"Basic {raw}"}
    response = case.client.post(
        "/api/mcp/token", headers=headers, data=_refresh_form(str(tokens["refresh_token"]))
    )
    assert response.status_code == 200


@pytest.mark.parametrize(
    ("body_client_id", "status_code"),
    [(_CLIENT_ID, 200), (_OTHER_CLIENT_ID, 401), ("", 401)],
    ids=["matching", "mismatching", "blank"],
)
def test_body_client_id_must_match_basic_authentication(
    case: HttpCase, body_client_id: str, status_code: int
) -> None:
    """A body client_id alongside Basic is allowed only when it names the same client.

    Given a Basic-authenticated refresh that also carries client_id in the body.
    When the body id matches, differs, or is blank.
    Then only the matching id is accepted; the others are invalid_client.
    """
    tokens = _tokens(case)
    response = case.client.post(
        "/api/mcp/token",
        headers=_basic(),
        data=_refresh_form(str(tokens["refresh_token"]), client_id=body_client_id),
    )
    assert response.status_code == status_code
    if status_code == 401:
        assert response.json() == {"error": "invalid_client"}


@pytest.mark.parametrize("duplicate", ["client_id", "redirect_uri", "state", "scope"])
async def test_repeated_authorize_parameters_are_refused_directly(
    case: HttpCase, duplicate: str
) -> None:
    """N2: any repeated authorize parameter is ambiguous and is answered directly.

    Given an otherwise valid authorization request with one parameter repeated.
    When authorize is called.
    Then 400 invalid_request is returned with no Location and no pending request row.
    """
    params = list(_authorize_params().items())
    params.append((duplicate, dict(params)[duplicate]))
    response = case.client.get("/api/mcp/authorize", params=params)
    assert response.status_code == 400
    assert response.json() == {"error": "invalid_request"}
    assert "location" not in response.headers
    assert await _pending_rows(case) == 0
