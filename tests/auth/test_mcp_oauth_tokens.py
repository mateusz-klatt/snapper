"""Security contract tests for MCP OAuth token primitives."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import jwt
import pytest

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import MCPOAuthAccessTokenRequest
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token

_ISSUER = "https://snapper.ch/api/mcp"
_AUDIENCE = "https://snapper.ch/api/mcp"
_CLIENT_ID = "chatgpt-client"


def _principal() -> AuthPrincipal:
    """Build one read-only OAuth delegate principal."""
    operator_public_id = "00000000-0000-7000-8000-000000000011"
    return AuthPrincipal(
        username="chatgpt-reader",
        role=UserRole.AI_DELEGATE,
        is_active=True,
        user_public_id="00000000-0000-7000-8000-000000000010",
        operator_public_ids=[operator_public_id],
        operator_membership_public_ids={operator_public_id: "00000000-0000-7000-8000-000000000012"},
        primary_operator_public_id=operator_public_id,
    )


def _mint(manager: TokenManager, *, issued_at: datetime | None = None) -> str:
    """Mint one valid OAuth access token and return its encoded form."""
    result = manager.create_mcp_oauth_access_token(
        MCPOAuthAccessTokenRequest(
            user=_principal(),
            client_id=_CLIENT_ID,
            grant_id="00000000-0000-7000-8000-000000000013",
            issuer=_ISSUER,
            audience=_AUDIENCE,
            scopes=("snapper.read", "offline_access", "snapper.read"),
            issued_at=issued_at or datetime.now(UTC) - timedelta(seconds=1),
            ttl_seconds=900,
            permissions=(Permission.READ_MARKET_DATA,),
        )
    )
    return result.access_token


def test_mcp_oauth_access_token_is_strictly_bound_and_not_legacy() -> None:
    """Verify issuer, audience, client, purpose, and transport separation.

    Given a freshly minted MCP OAuth access token,
    When it is verified under matching and mismatched bindings,
    Then only the exact OAuth binding succeeds and legacy verification fails.
    """
    manager = TokenManager()
    token = _mint(manager)

    claims = manager.verify_mcp_oauth_access_token(
        token,
        issuer=_ISSUER,
        audience=_AUDIENCE,
        client_id=_CLIENT_ID,
    )
    assert claims is not None
    assert claims.scope == "snapper.read offline_access"
    assert claims.sub == _principal().user_public_id
    assert claims.token_use == "mcp_oauth_access"
    assert manager.verify_token(token) is None
    assert (
        manager.verify_mcp_oauth_access_token(
            token,
            issuer="https://wrong.example",
            audience=_AUDIENCE,
            client_id=_CLIENT_ID,
        )
        is None
    )
    assert (
        manager.verify_mcp_oauth_access_token(
            token,
            issuer=_ISSUER,
            audience="https://wrong.example",
            client_id=_CLIENT_ID,
        )
        is None
    )
    assert (
        manager.verify_mcp_oauth_access_token(
            token,
            issuer=_ISSUER,
            audience=_AUDIENCE,
            client_id="wrong-client",
        )
        is None
    )


def test_mcp_oauth_verifier_accepts_only_exact_single_audience_list() -> None:
    """Verify a singleton audience list is accepted but extras are refused.

    Given a valid OAuth JWT whose audience is rewritten as a list,
    When the list contains exactly the resource or an additional resource,
    Then only the exact singleton audience passes semantic verification.
    """
    manager = TokenManager()
    token = _mint(manager)
    payload = jwt.decode(
        token,
        manager.settings.auth_secret_key,
        algorithms=[manager.settings.auth_algorithm],
        options={"verify_signature": True, "verify_aud": False},
    )
    payload["aud"] = [_AUDIENCE]
    singleton = jwt.encode(
        payload,
        manager.settings.auth_secret_key,
        algorithm=manager.settings.auth_algorithm,
    )
    assert (
        manager.verify_mcp_oauth_access_token(
            singleton,
            issuer=_ISSUER,
            audience=_AUDIENCE,
            client_id=_CLIENT_ID,
        )
        is not None
    )

    payload["aud"] = [_AUDIENCE, "https://extra.example"]
    multiple = jwt.encode(
        payload,
        manager.settings.auth_secret_key,
        algorithm=manager.settings.auth_algorithm,
    )
    assert (
        manager.verify_mcp_oauth_access_token(
            multiple,
            issuer=_ISSUER,
            audience=_AUDIENCE,
            client_id=_CLIENT_ID,
        )
        is None
    )


@pytest.mark.parametrize(
    "token_request",
    [
        MCPOAuthAccessTokenRequest(
            _principal(),
            "",
            "grant",
            _ISSUER,
            _AUDIENCE,
            ("snapper.read",),
            datetime.now(UTC),
            900,
            (Permission.READ_MARKET_DATA,),
        ),
        MCPOAuthAccessTokenRequest(
            _principal(),
            _CLIENT_ID,
            "",
            _ISSUER,
            _AUDIENCE,
            ("snapper.read",),
            datetime.now(UTC),
            900,
            (Permission.READ_MARKET_DATA,),
        ),
        MCPOAuthAccessTokenRequest(
            _principal(),
            _CLIENT_ID,
            "grant",
            "",
            _AUDIENCE,
            ("snapper.read",),
            datetime.now(UTC),
            900,
            (Permission.READ_MARKET_DATA,),
        ),
        MCPOAuthAccessTokenRequest(
            _principal(),
            _CLIENT_ID,
            "grant",
            _ISSUER,
            "",
            ("snapper.read",),
            datetime.now(UTC),
            900,
            (Permission.READ_MARKET_DATA,),
        ),
        MCPOAuthAccessTokenRequest(
            _principal(),
            _CLIENT_ID,
            "grant",
            _ISSUER,
            _AUDIENCE,
            (),
            datetime.now(UTC),
            900,
            (Permission.READ_MARKET_DATA,),
        ),
        MCPOAuthAccessTokenRequest(
            _principal(),
            _CLIENT_ID,
            "grant",
            _ISSUER,
            _AUDIENCE,
            ("",),
            datetime.now(UTC),
            900,
            (Permission.READ_MARKET_DATA,),
        ),
        MCPOAuthAccessTokenRequest(
            _principal(),
            _CLIENT_ID,
            "grant",
            _ISSUER,
            _AUDIENCE,
            ("snapper.read",),
            datetime.now(UTC),
            0,
            (Permission.READ_MARKET_DATA,),
        ),
    ],
)
def test_mcp_oauth_mint_rejects_incomplete_contract(
    token_request: MCPOAuthAccessTokenRequest,
) -> None:
    """Verify minting fails closed for incomplete OAuth token inputs.

    Given a mint request missing one required binding, scope, or lifetime,
    When the token manager attempts to create an OAuth access JWT,
    Then it raises before signing any credential.
    """
    manager = TokenManager()
    with pytest.raises(ValueError):
        manager.create_mcp_oauth_access_token(token_request)


def test_oauth_refresh_token_is_opaque_and_hash_only_at_rest() -> None:
    """Verify refresh token output separates one-time raw value from storage.

    Given two independent opaque refresh-token mints,
    When their raw values and persistence digests are compared,
    Then the values are unique and only deterministic SHA-256 hashes persist.
    """
    first = TokenManager.mint_oauth_refresh_token()
    second = TokenManager.mint_oauth_refresh_token()

    assert first.token != second.token
    assert first.token_hash == hash_token(first.token)
    assert len(first.token_hash) == 64
    assert first.token not in first.token_hash
