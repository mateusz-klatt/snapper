"""Exercise OAuth authority at the real MCP resource server over JSON-RPC."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid7

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response
from mcp.server.auth.provider import AccessToken

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.tokens import MCPOAuthAccessTokenRequest
from snapper.auth.tokens import hash_token
from snapper.core.json_types import JsonObject
from snapper.data.repository_types import UserActiveTokenInsertRow
from snapper.mcp import tools
from snapper.mcp.oauth.scopes import SNAPPER_READ
from snapper.mcp.oauth.scopes import TOOL_OAUTH_SCOPES
from snapper.mcp.oauth.transport import oauth_tool_refusal
from snapper.mcp.tool_catalog import MCP_TOOL_VISIBILITY_POLICY
from snapper.mcp.tool_catalog import is_mcp_tool_visible
from snapper.server import app as server_app
from snapper.server import provenance_middleware
from tests.mcp.test_oauth_http import HttpCase
from tests.mcp.test_oauth_http import _basic
from tests.mcp.test_oauth_http import _build_case
from tests.mcp.test_oauth_http import _settings
from tests.mcp.test_oauth_http import _tokens
from tests.mcp.test_oauth_service import _ISSUER
from tests.mcp.test_oauth_service import _RESOURCE
from tests.mcp.test_oauth_service import _build_harness
from tests.mcp.test_oauth_service import _principal
from tests.mcp.test_oauth_service import _seed_user_with_membership
from tests.mcp.test_security_schemes import ALL_ERAS
from tests.mcp.test_security_schemes import MODERN_ERA
from tests.mcp.test_security_schemes import _request_params

_METADATA_URL = "https://snapper.ch/.well-known/oauth-protected-resource/api/mcp"
_ACCEPT = "application/json, text/event-stream"
_PROTOCOL = "2025-11-25"


@asynccontextmanager
async def _mcp_lifespan(app: FastAPI) -> AsyncGenerator[None]:
    """Start only the mounted MCP SDK lifespan for the SQLite test app."""
    mcp_app = app.state.mcp_sub_app
    async with mcp_app.router.lifespan_context(mcp_app):
        yield


@pytest.fixture
async def wire_case(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[HttpCase]:
    """Use one real repository and synthetic OAuth client, owner and delegate."""
    harness = await _build_harness(tmp_path)
    owner_id = str(uuid7())
    membership_id = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    owner = _principal(owner_id, membership_public_id=membership_id)
    try:
        with monkeypatch.context() as scoped:
            scoped.setattr(server_app, "get_settings", lambda: _settings(enabled=True))
            scoped.setattr(server_app, "get_repository_dependency", lambda: harness.repository)
            scoped.setattr(provenance_middleware, "get_repository", lambda _url: harness.repository)
            case = _build_case(harness, owner, enabled=True)
            case.app.router.lifespan_context = _mcp_lifespan
            with case.client:
                yield case
    finally:
        await harness.repository.engine.dispose()


def _rpc(
    client: TestClient,
    method: str,
    params: JsonObject,
    *,
    token: str,
    version: str = _PROTOCOL,
) -> Response:
    """Send one authenticated MCP JSON-RPC request through the FastAPI mount."""
    headers = {
        "Accept": _ACCEPT,
        "Authorization": f"Bearer {token}",
        "MCP-Protocol-Version": version,
        "Mcp-Method": method,
    }
    if isinstance(params.get("name"), str):
        headers["Mcp-Name"] = str(params["name"])
    return client.post(
        "/api/mcp",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": {**params, **_request_params(version)},
        },
    )


def _result(response: Response) -> JsonObject:
    """Read one successful JSON-RPC envelope from JSON or event-stream transport."""
    assert response.status_code == 200
    body = response.text
    if response.headers["content-type"].startswith("text/event-stream"):
        body = next(
            line.removeprefix("data: ") for line in body.splitlines() if line.startswith("data: ")
        )
    envelope: JsonObject = json.loads(body)
    result = envelope.get("result")
    assert isinstance(result, dict)
    return result


def _catalog(response: Response) -> dict[str, JsonObject]:
    """Index the raw wire catalog by tool name and reject duplicate entries."""
    entries = _result(response)["tools"]
    assert isinstance(entries, list)
    catalog: dict[str, JsonObject] = {}
    for tool in entries:
        assert isinstance(tool, dict)
        name = tool["name"]
        assert isinstance(name, str) and name not in catalog
        catalog[name] = tool
    return catalog


def _access_token(case: HttpCase) -> str:
    """Mint and return one genuine OAuth token through authorize and consent."""
    token = _tokens(case)["access_token"]
    assert isinstance(token, str)
    return token


def _challenge(result: JsonObject) -> str:
    """Return the single reauthorization challenge of an MCP tool refusal."""
    meta = result["_meta"]
    assert isinstance(meta, dict)
    challenges = meta["mcp/www_authenticate"]
    assert isinstance(challenges, list) and len(challenges) == 1
    challenge = challenges[0]
    assert isinstance(challenge, str)
    return challenge


@pytest.mark.parametrize("path", ["/api/mcp", "/api/mcp/"])
@pytest.mark.parametrize("token", [None, "invalid.test.token"])
def test_enabled_resource_auth_challenge_advertises_discovery(
    wire_case: HttpCase, path: str, token: str | None
) -> None:
    """Unauthenticated clients can discover the canonical protected resource.

    Given: The live OAuth configuration and either a missing or invalid bearer.
    When: Either spelling of the MCP mount receives a POST.
    Then: The endpoint returns 401 and the same discoverable metadata URL.
    """
    headers = {"Accept": _ACCEPT}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    response = wire_case.client.post(path, headers=headers, json={})
    assert response.status_code == 401
    assert "location" not in response.headers
    challenge = response.headers["www-authenticate"]
    assert challenge.startswith("Bearer ")
    assert f'resource_metadata="{_METADATA_URL}"' in challenge
    assert ('error="invalid_token"' in challenge) is (token is not None)


@pytest.mark.parametrize("version", ALL_ERAS)
def test_oauth_catalog_projects_read_scope_over_the_wire(wire_case: HttpCase, version: str) -> None:
    """A real OAuth grant yields the read-only catalog advertised to clients.

    Given: A signed token from the real PKCE and consent endpoints.
    When: A client requests the catalog on a supported wire version.
    Then: Returned tools intersect registrations, scope and permissions, with both scheme forms.
    """
    token = _access_token(wire_case)
    if version != MODERN_ERA:
        handshake = _rpc(
            wire_case.client,
            "initialize",
            {
                "protocolVersion": version,
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1"},
            },
            token=token,
            version=version,
        )
        assert _result(handshake)["protocolVersion"] == version
    catalog = _catalog(_rpc(wire_case.client, "tools/list", {}, token=token, version=version))
    claims = wire_case.harness.token_manager.verify_mcp_oauth_access_token(
        token, issuer=_ISSUER, audience=_RESOURCE
    )
    assert claims is not None
    expected = {
        name
        for name in MCP_TOOL_VISIBILITY_POLICY
        if TOOL_OAUTH_SCOPES[name] <= {SNAPPER_READ} and is_mcp_tool_visible(name, claims)
    }
    assert expected and catalog.keys() == expected
    for item in catalog.values():
        schemes = [{"type": "oauth2", "scopes": [SNAPPER_READ]}]
        meta = item["_meta"]
        assert isinstance(meta, dict)
        assert item["securitySchemes"] == meta["securitySchemes"] == schemes
        annotations = item["annotations"]
        assert isinstance(annotations, dict)
        assert annotations["readOnlyHint"] is True
        assert annotations["destructiveHint"] is False
        assert annotations["idempotentHint"] is True
        assert annotations["openWorldHint"] is False


async def test_oauth_never_widens_narrow_signed_permissions(
    wire_case: HttpCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The catalog stays narrow even when a grant has a broad read scope.

    Given: A genuine active grant and a same-grant token with market-data permission only.
    When: Both tokens list tools and the narrow token directly names another read tool.
    Then: Its catalog and direct calls both respect the narrower permissions.
    """
    read_handler = AsyncMock()
    monkeypatch.setattr(tools, "_list_recent_signals_tool", read_handler)
    token = _access_token(wire_case)
    verified = await wire_case.harness.service.verify_access_token(token)
    narrower = wire_case.harness.token_manager.create_mcp_oauth_access_token(
        MCPOAuthAccessTokenRequest(
            user=verified.principal,
            issued_at=datetime.now(UTC),
            ttl_seconds=60,
            grant_id=verified.grant_public_id,
            client_id=verified.client_id,
            issuer=_ISSUER,
            audience=_RESOURCE,
            scopes=(SNAPPER_READ,),
            permissions=(Permission.READ_MARKET_DATA,),
        )
    )
    wider_catalog = _catalog(_rpc(wire_case.client, "tools/list", {}, token=token))
    narrowed = _catalog(_rpc(wire_case.client, "tools/list", {}, token=narrower.access_token))
    expected = {name for name in wider_catalog if is_mcp_tool_visible(name, narrower.claims)}
    assert narrowed.keys() == expected
    assert "list_instruments" in narrowed
    assert narrowed.keys() < wider_catalog.keys()
    assert "list_recent_signals" in wider_catalog
    refused = _result(
        _rpc(
            wire_case.client,
            "tools/call",
            {"name": "list_recent_signals", "arguments": {"since": "2026-11-18T00:00:00Z"}},
            token=narrower.access_token,
        )
    )
    assert refused["isError"] is True
    read_handler.assert_not_awaited()


def test_oauth_resource_calls_read_and_refuses_direct_write(
    wire_case: HttpCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A scoped client cannot use a known write name to bypass the catalog.

    Given: A valid OAuth token with the read scope and a valid known write route.
    When: A client invokes one read and invokes the write by name.
    Then: The read returns real repository data and the write handler is never entered.
    """
    write_handler = AsyncMock()
    monkeypatch.setattr(tools, "_cancel_order_tool", write_handler)
    token = _access_token(wire_case)
    read = _result(
        _rpc(
            wire_case.client,
            "tools/call",
            {"name": "list_instruments", "arguments": {"exchange": "kraken"}},
            token=token,
        )
    )
    assert read.get("isError") is not True
    content = read["content"]
    assert isinstance(content, list) and len(content) == 1 and isinstance(content[0], dict)
    payload = content[0]["text"]
    assert isinstance(payload, str)
    assert json.loads(payload) == {"exchange": "kraken", "instruments": []}
    rejected = _result(
        _rpc(
            wire_case.client,
            "tools/call",
            {
                "name": "cancel_order",
                "arguments": {"plan_public_id": str(uuid7()), "idempotency_key": "wire"},
            },
            token=token,
        )
    )
    assert rejected["isError"] is True
    challenge = _challenge(rejected)
    assert 'error="insufficient_scope"' in challenge
    assert f'resource_metadata="{_METADATA_URL}"' in challenge
    write_handler.assert_not_awaited()


def test_oauth_unknown_tool_has_no_scope_to_advertise(wire_case: HttpCase) -> None:
    """Unmapped names remain inaccessible to a legitimate read grant.

    Given: A verified OAuth access token.
    When: The caller names an unmapped tool.
    Then: The response refuses it and advertises only the real discovery URL.
    """
    token = _access_token(wire_case)
    result = _result(
        _rpc(
            wire_case.client,
            "tools/call",
            {"name": "unmapped_tool", "arguments": {}},
            token=token,
        )
    )
    assert result["isError"] is True
    assert (
        _challenge(result)
        == f'Bearer error="insufficient_scope", resource_metadata="{_METADATA_URL}"'
    )


def test_oauth_refusal_with_sdk_token_without_resource_omits_metadata_url() -> None:
    """An unset optional SDK resource never produces an invented discovery URL.

    Given: An SDK token that carries a client and read scope but no resource field.
    When: The direct write check refuses access.
    Then: The challenge advertises the required scopes without a metadata URL.
    """
    access = AccessToken(token="synthetic-test-token", client_id="synthetic", scopes=[SNAPPER_READ])
    result = oauth_tool_refusal("cancel_order", access)
    assert result is not None and result.is_error
    assert result.meta == {
        "mcp/www_authenticate": [
            'Bearer error="insufficient_scope", scope="snapper.read snapper.trade"'
        ]
    }


def test_wire_rejects_grant_immediately_after_public_revocation(wire_case: HttpCase) -> None:
    """Revoking a grant takes effect on its next MCP request.

    Given: A valid OAuth credential whose catalog has just been observed.
    When: The registered client revokes it through the real RFC 7009 route.
    Then: The same unexpired credential no longer lists tools.
    """
    token = _access_token(wire_case)
    assert _catalog(_rpc(wire_case.client, "tools/list", {}, token=token))
    response = wire_case.client.post("/api/mcp/revoke", headers=_basic(), data={"token": token})
    assert response.status_code == 200
    refused = _rpc(wire_case.client, "tools/list", {}, token=token)
    assert refused.status_code == 401
    assert f'resource_metadata="{_METADATA_URL}"' in refused.headers["www-authenticate"]


async def test_legacy_delegate_pat_keeps_its_catalog_with_oauth_enabled(
    wire_case: HttpCase,
) -> None:
    """OAuth activation does not apply OAuth scope to a valid inventoried PAT.

    Given: An active legacy delegate with its own signed and inventoried bearer.
    When: It lists tools against the OAuth-enabled app.
    Then: The legacy permission policy remains authoritative, including granted writes.
    """
    repository = wire_case.harness.repository
    user_id = str(uuid7())
    role = UserRole.AI_DELEGATE
    membership = await _seed_user_with_membership(repository, user_public_id=user_id, role=role)
    principal = _principal(user_id, role=role, membership_public_id=membership)
    manager = wire_case.harness.token_manager
    now = datetime.now(UTC)
    pat = manager.create_delegate_access_token(principal, now)
    await repository.insert_user_active_tokens(
        [
            UserActiveTokenInsertRow(
                public_id=str(uuid7()),
                user_public_id=user_id,
                jti=pat.jti,
                token_hash=hash_token(pat.access_token),
                token_type=TOKEN_TYPE_ACCESS,
                issued_at=now,
                expires_at=pat.expires_at,
            )
        ]
    )
    claims = manager.decode_fresh_token(pat.access_token)
    expected = {name for name in MCP_TOOL_VISIBILITY_POLICY if is_mcp_tool_visible(name, claims)}
    catalog = _catalog(_rpc(wire_case.client, "tools/list", {}, token=pat.access_token))
    assert catalog.keys() == expected
    assert "submit_manual_order" in catalog
    assert "securitySchemes" not in catalog["list_instruments"]
