"""Tests for the authorization context carried by a ws_token.

The context exists because the database cannot reconstruct it:
``build_auth_principal`` deliberately refuses to populate
``active_wallet_public_id`` and omits the permission fields, which then default
to ``None`` — and ``None`` means the FULL current-role grant. Every assertion
here defends one half of that: that a carried scope survives the round trip
exactly, and that the two "empty-looking" values never collapse into each other.

The existing ws_token tests assert subject and session binding only, so none of
them would fail if the context were dropped, mangled, or sourced from the wrong
object. These are written to fail on precisely those mistakes.
"""

import jwt
import pytest

from snapper.api.auth.schemas.ws_token import WS_AUTHORIZATION_CONTEXT_VERSION
from snapper.api.auth.schemas.ws_token import WsAuthorizationContext
from snapper.api.auth.services.ws_token_service import WsTokenService


def _decode(service: WsTokenService, token: str) -> dict[str, object]:
    """Decode a minted ticket back into raw claims.

    Deliberately decodes the WIRE format rather than reading the returned
    payload object: a context that never reached the JWT would still look
    correct on the in-memory result.

    Uses the service's own settings rather than a fresh ``get_settings()`` —
    the latter builds an instance with no database access and cannot resolve
    the signing algorithm, and it would also risk verifying against different
    settings than the mint used.
    """
    decoded: dict[str, object] = jwt.decode(
        token,
        service.settings.auth_secret_key,
        algorithms=[service.settings.auth_algorithm],
    )
    return decoded


@pytest.fixture(name="service")
def _service() -> WsTokenService:
    """Provide the ws_token service singleton."""
    return WsTokenService()


def test_context_is_carried_verbatim_into_the_ticket(service: WsTokenService) -> None:
    """Verify wallet, permissions and scope version all reach the wire.

    Given: A context with a wallet, a narrow permission list and a version,
    When: A ticket is minted with it,
    Then: All three appear in the decoded claims, alongside version 1.
    """
    context = WsAuthorizationContext(
        active_wallet_public_id="019e873c-d062-720f-85df-fd4d7fce5bdf",
        permissions=["read:positions"],
        permission_scope_version=1,
    )
    result = service.generate(user_id="u", session_id="s", context=context)
    claims = _decode(service, result.token)
    assert claims["authorization_context_version"] == WS_AUTHORIZATION_CONTEXT_VERSION
    assert claims["active_wallet_public_id"] == "019e873c-d062-720f-85df-fd4d7fce5bdf"
    assert claims["permissions"] == ["read:positions"]
    assert claims["permission_scope_version"] == 1


def test_empty_permissions_survive_as_empty_not_none(service: WsTokenService) -> None:
    """Verify a deliberate zero scope is not degraded into the full role grant.

    Given: A context whose permissions are an EMPTY list,
    When: A ticket is minted,
    Then: The claim is `[]`, not None.

    This is the assertion that matters most in the whole module. `[]` means
    "this token was deliberately granted nothing"; `None` means "fall back to
    everything the role allows". Any `or []`/`or None` introduced anywhere along
    the minting path collapses one into the other, and the failure is silent and
    in the widening direction.
    """
    context = WsAuthorizationContext(
        active_wallet_public_id=None,
        permissions=[],
        permission_scope_version=1,
    )
    claims = _decode(service, service.generate(user_id="u", session_id="s", context=context).token)
    assert claims["permissions"] == []
    assert claims["permissions"] is not None


def test_context_version_is_stamped_even_when_every_field_is_none(
    service: WsTokenService,
) -> None:
    """Verify a present-but-empty context is still a CONTEXTFUL ticket.

    Given: A context whose three fields are all None,
    When: A ticket is minted,
    Then: The version claim is still 1.

    The version is the sole legacy discriminator. A consumer must be able to
    tell "this ticket carries a context that happens to be empty" from "this
    ticket predates contexts", because only the second may rebuild scope from
    the bearer.
    """
    context = WsAuthorizationContext(
        active_wallet_public_id=None,
        permissions=None,
        permission_scope_version=None,
    )
    claims = _decode(service, service.generate(user_id="u", session_id="s", context=context).token)
    assert claims["authorization_context_version"] == WS_AUTHORIZATION_CONTEXT_VERSION


def test_omitted_context_mints_a_legacy_ticket(service: WsTokenService) -> None:
    """Verify callers that pass no context produce an unversioned ticket.

    Given: No context argument,
    When: A ticket is minted,
    Then: The version claim is None and no context field is populated.

    Legacy tickets stay mintable so the rollout can proceed in stages, and a
    consumer seeing no version rebuilds scope from the separately verified
    bearer rather than assuming an empty scope.
    """
    claims = _decode(service, service.generate(user_id="u", session_id="s").token)
    assert claims["authorization_context_version"] is None
    assert claims["active_wallet_public_id"] is None
    assert claims["permissions"] is None
    assert claims["permission_scope_version"] is None
