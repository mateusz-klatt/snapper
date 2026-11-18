"""Fail-closed protocol boundaries, rollback recovery, and live OAuth authority."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid7

import jwt
import pytest
import pytest_asyncio
from freezegun import freeze_time
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from snapper.application.ai_delegates import oauth_authority as authority_module
from snapper.application.ai_delegates import service as delegate_module
from snapper.application.ai_delegates.service import DelegateService
from snapper.application.ai_delegates.service import OAuthDelegateIdentity
from snapper.application.ai_delegates.service import OAuthDelegateRequest
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import hash_token
from snapper.core.json_types import JsonObject
from snapper.data.models import AiDelegate
from snapper.data.models import OAuthAuthorizationCode
from snapper.data.models import OAuthAuthorizationRequest
from snapper.data.models import OAuthClient
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.models import UserTradingCaps
from snapper.mcp.oauth import transaction as transaction_module
from snapper.mcp.oauth.contracts import MCPOAuthError
from snapper.mcp.oauth.contracts import MCPOAuthServiceConfig
from snapper.mcp.oauth.contracts import OAuthApprovedAuthorization
from snapper.mcp.oauth.contracts import OAuthAuthorizationRequestCreate
from snapper.mcp.oauth.contracts import OAuthCodeExchangeCommand
from snapper.mcp.oauth.contracts import OAuthConsentApprovalCommand
from snapper.mcp.oauth.contracts import OAuthConsentDenialCommand
from snapper.mcp.oauth.contracts import OAuthRefreshExchangeCommand
from snapper.mcp.oauth.contracts import OAuthRefreshReuseDetectedError
from snapper.mcp.oauth.contracts import OAuthTokenExchangeResult
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import SNAPPER_READ
from snapper.mcp.oauth.store import OAuthRefreshRotationOutcome
from snapper.mcp.oauth.transaction import OAuthTransaction
from snapper.mcp.oauth.transaction import OAuthTransactionConflictError
from tests.mcp.test_oauth_service import _CLIENT_ID
from tests.mcp.test_oauth_service import _CLIENT_SECRET
from tests.mcp.test_oauth_service import _ISSUER
from tests.mcp.test_oauth_service import _REDIRECT_URI
from tests.mcp.test_oauth_service import _RESOURCE
from tests.mcp.test_oauth_service import _VERIFIER
from tests.mcp.test_oauth_service import _build_harness
from tests.mcp.test_oauth_service import _OAuthHarness
from tests.mcp.test_oauth_service import _pkce_s256
from tests.mcp.test_oauth_service import _principal
from tests.mcp.test_oauth_service import _seed_user_with_membership
from tests.mcp.test_oauth_store_legacy import _lose_one_claim


@dataclass(frozen=True)
class OAuthCase:
    """One isolated real SQLite service and its currently authorized owner."""

    harness: _OAuthHarness
    owner: AuthPrincipal


@pytest_asyncio.fixture
async def oauth_case(tmp_path: Path) -> AsyncIterator[OAuthCase]:
    """Seed current membership while keeping all operations off external services."""
    harness = await _build_harness(tmp_path)
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    try:
        yield OAuthCase(harness, _principal(owner_id, membership_public_id=membership))
    finally:
        await harness.repository.engine.dispose()


def _request() -> OAuthAuthorizationRequestCreate:
    """Return exact registered parameters, with no credential logging."""
    return OAuthAuthorizationRequestCreate(
        client_id=_CLIENT_ID,
        redirect_uri=_REDIRECT_URI,
        scopes=(SNAPPER_READ, OFFLINE_ACCESS),
        code_challenge=_pkce_s256(_VERIFIER),
        code_challenge_method="S256",
        resource=_RESOURCE,
    )


async def _approve(case: OAuthCase) -> OAuthApprovedAuthorization:
    """Create and approve a fresh exact request under current owner authority."""
    pending = await case.harness.service.create_authorization_request(_request())
    return await case.harness.service.approve_authorization_request(
        OAuthConsentApprovalCommand(pending.raw_request_id, case.owner)
    )


def _code(raw: str) -> OAuthCodeExchangeCommand:
    """Build an exact S256 confidential-client code exchange."""
    return OAuthCodeExchangeCommand(
        _CLIENT_ID, _CLIENT_SECRET, "client_secret_basic", raw, _VERIFIER, _REDIRECT_URI, _RESOURCE
    )


def _refresh(raw: str) -> OAuthRefreshExchangeCommand:
    """Build a non-widening refresh request for this exact resource."""
    return OAuthRefreshExchangeCommand(
        _CLIENT_ID, _CLIENT_SECRET, "client_secret_basic", raw, _RESOURCE
    )


async def _tokens(case: OAuthCase) -> OAuthTokenExchangeResult:
    """Obtain credentials from the actual committed code exchange under test."""
    return await case.harness.service.exchange_authorization_code(
        _code((await _approve(case)).raw_code)
    )


async def _no_grants(case: OAuthCase) -> None:
    """Check that a failed boundary left no delegate, grant, or code behind."""
    async with case.harness.repository.session() as session:
        assert list((await session.execute(select(OAuthGrant))).scalars()) == []
        assert list((await session.execute(select(OAuthAuthorizationCode))).scalars()) == []
        assert (
            list(
                (
                    await session.execute(
                        select(User).where(
                            User.created_by_user_public_id == case.owner.user_public_id
                        )
                    )
                ).scalars()
            )
            == []
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"client_id": "unknown"},
        {"resource": "https://other.invalid/mcp"},
        {"code_challenge_method": "plain"},
        {"code_challenge": "not-s256"},
        {"scopes": ()},
        {"scopes": (OFFLINE_ACCESS,)},
        {"scopes": ("snapper.trade",)},
        {"redirect_uri": "https://other.invalid/callback"},
        {"redirect_uri_provided_explicitly": False},
    ],
)
@pytest.mark.asyncio
async def test_authorize_rejects_untrusted_parameters_without_rows(
    oauth_case: OAuthCase, changes: dict[str, object]
) -> None:
    """Invalid client/resource/PKCE/scope/redirect input never leaves a pending credential.

    Given a client and invalid resource, PKCE, redirect or scope input.
    When authorization is requested.
    Then the typed refusal leaves no pending request or grant.
    """
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.create_authorization_request(
            replace(_request(), **changes)
        )
    async with oauth_case.harness.repository.session() as session:
        assert list((await session.execute(select(OAuthAuthorizationRequest))).scalars()) == []
    await _no_grants(oauth_case)


@pytest.mark.asyncio
async def test_implicit_redirect_requires_one_registered_uri(oauth_case: OAuthCase) -> None:
    """A single omitted callback resolves exactly; multiple registered choices remain ambiguous.

    Given a client with one or multiple registered redirects.
    When authorization omits an explicit redirect.
    Then only the unambiguous single redirect is accepted.
    """
    command = replace(_request(), redirect_uri=None, redirect_uri_provided_explicitly=False)
    pending = await oauth_case.harness.service.create_authorization_request(command)
    approved = await oauth_case.harness.service.approve_authorization_request(
        OAuthConsentApprovalCommand(pending.raw_request_id, oauth_case.owner)
    )
    exchanged = await oauth_case.harness.service.exchange_authorization_code(
        replace(_code(approved.raw_code), redirect_uri=None)
    )
    assert exchanged.access_token
    async with oauth_case.harness.repository.session() as session:
        await session.execute(
            update(OAuthClient)
            .where(OAuthClient.client_id == _CLIENT_ID)
            .values(redirect_uris=[_REDIRECT_URI, "https://second.invalid/callback"])
        )
        await session.commit()
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.create_authorization_request(command)


@pytest.mark.parametrize(
    "kind", ["unknown", "inactive", "redirect", "challenge", "scope", "resource"]
)
@pytest.mark.asyncio
async def test_pending_request_rechecks_current_client_and_exact_metadata(
    oauth_case: OAuthCase, kind: str
) -> None:
    """A changed or unknown pending request is refused before any consuming mutation.

    Given an already persisted pending request.
    When client activity or exact resource, redirect or PKCE metadata changes.
    Then approval refuses the stale request without creating a binding.
    """
    pending = await oauth_case.harness.service.create_authorization_request(_request())
    raw = pending.raw_request_id
    async with oauth_case.harness.repository.session() as session:
        if kind == "inactive":
            await session.execute(
                update(OAuthClient)
                .where(OAuthClient.client_id == _CLIENT_ID)
                .values(is_active=False)
            )
        elif kind != "unknown":
            updates = {
                "redirect": {"redirect_uri": "https://unregistered.invalid"},
                "challenge": {"code_challenge": "bad"},
                "scope": {"scopes": ["snapper.trade"]},
                "resource": {"resource": "https://unapproved.invalid"},
            }
            await session.execute(update(OAuthAuthorizationRequest).values(**updates[kind]))
        await session.commit()
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.approve_authorization_request(
            OAuthConsentApprovalCommand("unknown" if kind == "unknown" else raw, oauth_case.owner)
        )
    await _no_grants(oauth_case)


@pytest.mark.parametrize("stage", ["request_lock", "after_owner"])
@pytest.mark.asyncio
async def test_expiry_during_a_wait_rolls_back_the_whole_consent(
    oauth_case: OAuthCase, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    """Expiry crossed after first lookup or after identity construction never publishes a code.

    Given a valid pending request before a lock or identity creation wait.
    When time advances past expiry during approval.
    Then the complete consent transaction rolls back.
    """
    with freeze_time(datetime.now(UTC)) as clock:
        pending = await oauth_case.harness.service.create_authorization_request(_request())
        if stage == "request_lock":
            original = OAuthTransaction.request

            async def delayed_request(
                tx: OAuthTransaction, raw: str, now: datetime, *, lock: bool = True
            ) -> OAuthAuthorizationRequest | None:
                """Advance only after the identity-only read preceding the client lock."""
                result = await original(tx, raw, now, lock=lock)
                if not lock:
                    clock.tick(delta=timedelta(seconds=301))
                return result

            monkeypatch.setattr(OAuthTransaction, "request", delayed_request)
        else:
            original_resolve = oauth_case.harness.delegate_service.resolve_oauth_delegate_in_tx

            async def delayed_owner(
                session: AsyncSession, owner: AuthPrincipal, request: OAuthDelegateRequest
            ) -> OAuthDelegateIdentity:
                """Expire the pending credential after real delegate construction but before commit."""
                result = await original_resolve(session, owner, request)
                clock.tick(delta=timedelta(seconds=301))
                return result

            monkeypatch.setattr(
                oauth_case.harness.delegate_service, "resolve_oauth_delegate_in_tx", delayed_owner
            )
        with pytest.raises(MCPOAuthError):
            await oauth_case.harness.service.approve_authorization_request(
                OAuthConsentApprovalCommand(pending.raw_request_id, oauth_case.owner)
            )
    await _no_grants(oauth_case)


@pytest.mark.asyncio
async def test_denial_resolves_once_without_grant_or_code(oauth_case: OAuthCase) -> None:
    """An authorized denial commits once and cannot subsequently be approved.

    Given an unresolved request and authorized owner.
    When denial is submitted twice.
    Then one terminal denial persists without a grant or code.
    """
    pending = await oauth_case.harness.service.create_authorization_request(_request())
    await oauth_case.harness.service.deny_authorization_request(
        OAuthConsentDenialCommand(pending.raw_request_id, oauth_case.owner)
    )
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.approve_authorization_request(
            OAuthConsentApprovalCommand(pending.raw_request_id, oauth_case.owner)
        )
    await _no_grants(oauth_case)


@pytest.mark.parametrize("kind", ["malformed_hash", "long_secret"])
@pytest.mark.asyncio
async def test_invalid_client_credential_never_consumes_code(
    oauth_case: OAuthCase, kind: str
) -> None:
    """Malformed stored bcrypt or oversized presented credentials fail without a claim.

    Given a valid one-time code.
    When client method, secret or stored hash authentication fails.
    Then the code remains unconsumed.
    """
    approved = await _approve(oauth_case)
    command = _code(approved.raw_code)
    if kind == "malformed_hash":
        async with oauth_case.harness.repository.session() as session:
            await session.execute(
                update(OAuthClient)
                .where(OAuthClient.client_id == _CLIENT_ID)
                .values(client_secret_hash="not-bcrypt")
            )
            await session.commit()
    else:
        command = replace(command, client_secret="x" * 73)
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.exchange_authorization_code(command)
    assert await oauth_case.harness.store.load_authorization_code(
        approved.raw_code, client_id=_CLIENT_ID
    )


@pytest.mark.parametrize("operation", ["code", "refresh", "verify"])
@pytest.mark.asyncio
async def test_dead_delegate_retirement_commits_without_credential_or_legacy_pat(
    oauth_case: OAuthCase, operation: str
) -> None:
    """Dead-role projection retires its grant and every family even when the public call refuses.

    Given a grant whose dedicated delegate is inactive.
    When code exchange, refresh or live verification projects the binding.
    Then grant and family retirement commits without issuing credentials or PATs.
    """
    approved = await _approve(oauth_case)
    tokens = None
    if operation != "code":
        tokens = await oauth_case.harness.service.exchange_authorization_code(
            _code(approved.raw_code)
        )
    async with oauth_case.harness.repository.session() as session:
        await session.execute(
            update(User)
            .where(User.created_by_user_public_id == oauth_case.owner.user_public_id)
            .values(role=UserRole.AI_REVIEWER.value)
        )
        await session.commit()
    with pytest.raises(MCPOAuthError):
        if operation == "code":
            await oauth_case.harness.service.exchange_authorization_code(_code(approved.raw_code))
        elif operation == "refresh":
            assert tokens is not None and tokens.refresh_token is not None
            await oauth_case.harness.service.exchange_refresh_token(_refresh(tokens.refresh_token))
        else:
            assert tokens is not None
            await oauth_case.harness.service.verify_access_token(tokens.access_token)
    assert await oauth_case.harness.store.get_active_grant(approved.grant_public_id) is None


@pytest.mark.parametrize(
    "change",
    [
        {"sub": "00000000-0000-7000-8000-000000009999"},
        {"role": "operator"},
        {"role": "ai_reviewer"},
        {"sid": "different"},
        {"operator_public_ids": []},
        {"primary_operator_public_id": "different"},
        {"operator_membership_public_ids": {}},
        {"permissions": []},
        {"scope": "snapper.unknown"},
        {"scope": "offline_access"},
    ],
)
@pytest.mark.asyncio
async def test_signed_but_invalid_live_linkage_is_refused(
    oauth_case: OAuthCase, change: JsonObject
) -> None:
    """An otherwise valid signature cannot substitute grant, delegate, operator, or scope authority.

    Given a correctly signed JWT with forged identity, permission or scope claims.
    When live verification runs.
    Then the invalid binding is denied despite the signature.
    """
    tokens = await _tokens(oauth_case)
    manager = oauth_case.harness.token_manager
    claims = manager.verify_mcp_oauth_access_token(
        tokens.access_token, issuer=_ISSUER, audience=_RESOURCE
    )
    assert claims is not None
    payload: JsonObject = claims.model_dump(mode="json")
    payload.update(change)
    altered = jwt.encode(
        payload, manager.settings.auth_secret_key, algorithm=manager.settings.auth_algorithm
    )
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.verify_access_token(altered)


@pytest.mark.parametrize("state", ["expired", "revoked", "unknown", "widened"])
@pytest.mark.asyncio
async def test_refresh_invalid_state_is_non_consuming(oauth_case: OAuthCase, state: str) -> None:
    """Expiry, family-only revocation, unknown credentials and widening cannot rotate a token.

    Given a refresh credential with invalid state or requested scope.
    When refresh exchange is attempted.
    Then the predecessor remains unconsumed and unrelated authority is unchanged.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    command = _refresh(tokens.refresh_token)
    async with oauth_case.harness.repository.session() as session:
        if state == "expired":
            await session.execute(
                update(OAuthRefreshToken).values(
                    expires_at=datetime.now(UTC) - timedelta(seconds=1)
                )
            )
        if state == "revoked":
            await session.execute(update(OAuthRefreshToken).values(revoked_at=datetime.now(UTC)))
        await session.commit()
    if state == "unknown":
        command = replace(command, raw_refresh_token="unknown")
    if state == "widened":
        command = replace(command, scopes=(SNAPPER_READ, "snapper.trade"))
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.exchange_refresh_token(command)
    async with oauth_case.harness.repository.session() as session:
        rows = (await session.execute(select(OAuthRefreshToken))).scalars().all()
        assert len(rows) == 1 and rows[0].used_at is None
        assert (await session.execute(select(OAuthGrant))).scalar_one().revoked_at is None


@pytest.mark.parametrize("outcome", ["unused", "used", "dead", "revoked"])
@pytest.mark.asyncio
async def test_lost_service_refresh_restarts_only_after_full_rollback(
    oauth_case: OAuthCase, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    """Recovery sees only independently committed state, never the losing transaction's own claim.

    Given a refresh compare-and-swap loser and an independently observed winner state.
    When the service reclassifies in a fresh transaction.
    Then only confirmed reuse commits revocation and partial loser changes never persist.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    repository = oauth_case.harness.repository
    original_session = repository.session
    original_rotate = OAuthTransaction.rotate_refresh

    async def losing_rotate(
        tx: OAuthTransaction,
        predecessor: OAuthRefreshToken,
        successor: OAuthRefreshToken,
        now: datetime,
    ) -> None:
        """Claim in the real losing transaction, then require its entire rollback."""
        await original_rotate(tx, predecessor, successor, now)
        raise OAuthTransactionConflictError("injected zero-row claim recovery")

    @asynccontextmanager
    async def racing_session() -> AsyncIterator[AsyncSession]:
        """Publish a separate winner only after the original repository has rolled back."""
        try:
            async with original_session() as session:
                yield session
        except OAuthTransactionConflictError:
            async with original_session() as winner:
                if outcome == "used":
                    await winner.execute(
                        update(OAuthRefreshToken).values(used_at=datetime.now(UTC))
                    )
                if outcome == "revoked":
                    await winner.execute(
                        update(OAuthRefreshToken).values(revoked_at=datetime.now(UTC))
                    )
                if outcome == "dead":
                    await winner.execute(
                        update(User)
                        .where(User.created_by_user_public_id == oauth_case.owner.user_public_id)
                        .values(role=UserRole.AI_REVIEWER.value)
                    )
                await winner.commit()
            raise

    monkeypatch.setattr(OAuthTransaction, "rotate_refresh", losing_rotate)
    monkeypatch.setattr(repository, "session", racing_session)
    expected = OAuthRefreshReuseDetectedError if outcome == "used" else MCPOAuthError
    with pytest.raises(expected):
        await oauth_case.harness.service.exchange_refresh_token(_refresh(tokens.refresh_token))
    async with original_session() as session:
        grant = (await session.execute(select(OAuthGrant))).scalar_one()
        rows = (await session.execute(select(OAuthRefreshToken))).scalars().all()
        assert len(rows) == 1
        assert (grant.revoked_at is not None) == (outcome in ["used", "dead"])
        assert (rows[0].used_at is not None) == (outcome == "used")


@pytest.mark.parametrize(
    "changes",
    [
        {"issuer": ""},
        {"resource": ""},
        {"access_token_ttl_seconds": 0},
        {"code_ttl_seconds": 0},
        {"request_ttl_seconds": 0},
        {"refresh_token_ttl_days": 0},
    ],
)
def test_configuration_rejects_empty_identities_and_nonpositive_lifetimes(
    changes: dict[str, object],
) -> None:
    """Invalid configuration fails before any repository or raw credential is used.

    Given an empty issuer/resource or nonpositive configured lifetime.
    When the service configuration is constructed.
    Then invalid configuration is refused before persistence.
    """
    with pytest.raises(ValueError):
        replace(MCPOAuthServiceConfig(_ISSUER, _RESOURCE), **changes)


@pytest.mark.parametrize("kind", ["unknown_role", "empty_operator"])
@pytest.mark.asyncio
async def test_owner_refusals_are_typed_and_non_consuming(oauth_case: OAuthCase, kind: str) -> None:
    """Malformed live authority or an unbound token is refused without consuming consent.

    Given an owner with an unknown role or absent operator binding.
    When consent approval reloads current authority.
    Then a typed protocol refusal leaves no delegate or grant.
    """
    pending = await oauth_case.harness.service.create_authorization_request(_request())
    owner = oauth_case.owner
    if kind == "unknown_role":
        async with oauth_case.harness.repository.session() as session:
            await session.execute(
                update(User)
                .where(User.public_id == owner.user_public_id)
                .values(role="unknown-role")
            )
            await session.commit()
    else:
        owner = owner.model_copy(
            update={"primary_operator_public_id": "", "operator_public_ids": []}
        )
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.approve_authorization_request(
            OAuthConsentApprovalCommand(pending.raw_request_id, owner)
        )
    await _no_grants(oauth_case)


@pytest.mark.asyncio
async def test_isolation_guard_is_transaction_local_and_helpers_do_not_commit(
    oauth_case: OAuthCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The PostgreSQL boundary requests READ COMMITTED before reads without owning commit.

    Given caller-owned sessions representing each supported dialect.
    When the OAuth write boundary selects isolation.
    Then the exact transaction-local statement executes without a helper commit.
    """
    fake = MagicMock(spec=AsyncSession)
    fake.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

    @asynccontextmanager
    async def session() -> AsyncIterator[AsyncSession]:
        """Expose only the typed session boundary under observation."""
        yield fake

    monkeypatch.setattr(oauth_case.harness.repository, "session", session)
    async with oauth_case.harness.store.begin_write_transaction() as actual:
        assert actual is fake
    await DelegateService._begin_oauth_write(fake)
    assert fake.execute.await_count == 2
    assert all(
        str(call.args[0]) == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
        for call in fake.execute.await_args_list
    )
    fake.commit.assert_not_called()
    fake.rollback.assert_not_called()


@pytest.mark.asyncio
async def test_failed_consent_claim_rolls_back_new_identity(
    oauth_case: OAuthCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A downstream claim refusal must not leave an orphaned delegate or grant.

    Given consent that has created an uncommitted identity.
    When the request claim is refused.
    Then the outer rollback removes the partial identity and binding.
    """
    pending = await oauth_case.harness.service.create_authorization_request(_request())
    monkeypatch.setattr(
        oauth_case.harness.service.store,
        "approve_authorization_request_in_tx",
        AsyncMock(return_value=None),
    )
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.approve_authorization_request(
            OAuthConsentApprovalCommand(pending.raw_request_id, oauth_case.owner)
        )
    await _no_grants(oauth_case)


@pytest.mark.asyncio
async def test_code_scope_widening_and_consume_refusal_are_atomic(
    oauth_case: OAuthCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A narrowed live grant or downstream consuming refusal cannot publish credentials.

    Given a valid grant and code exchange.
    When code scopes widen or the downstream consume operation refuses.
    Then no credential is published and the code remains usable.
    """
    approved = await _approve(oauth_case)
    async with oauth_case.harness.repository.session() as session:
        await session.execute(update(OAuthGrant).values(scopes=[SNAPPER_READ]))
        await session.commit()
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.exchange_authorization_code(_code(approved.raw_code))
    async with oauth_case.harness.repository.session() as session:
        await session.execute(update(OAuthGrant).values(scopes=[SNAPPER_READ, OFFLINE_ACCESS]))
        await session.commit()
    monkeypatch.setattr(
        oauth_case.harness.service.store,
        "consume_authorization_code_in_tx",
        AsyncMock(return_value=None),
    )
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.exchange_authorization_code(_code(approved.raw_code))
    assert await oauth_case.harness.store.load_authorization_code(
        approved.raw_code, client_id=_CLIENT_ID
    )
    async with oauth_case.harness.repository.session() as session:
        assert list((await session.execute(select(OAuthRefreshToken))).scalars()) == []


@pytest.mark.asyncio
async def test_unknown_code_and_missing_locked_code_fail_closed(
    oauth_case: OAuthCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Neither unknown credentials nor a disappeared locked lookup can yield tokens.

    Given an unknown code or a code that disappears before locking.
    When code exchange runs.
    Then a typed invalid-grant refusal is returned.
    """
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.exchange_authorization_code(_code("unknown"))
    approved = await _approve(oauth_case)
    original = OAuthTransaction.code

    async def missing_after_lock(
        tx: OAuthTransaction, raw: str, *, lock: bool = True
    ) -> OAuthAuthorizationCode | None:
        """Model a disappearing dependency only at its locked lookup boundary."""
        return None if lock else await original(tx, raw, lock=False)

    monkeypatch.setattr(OAuthTransaction, "code", missing_after_lock)
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.exchange_authorization_code(_code(approved.raw_code))
    assert await oauth_case.harness.store.load_authorization_code(
        approved.raw_code, client_id=_CLIENT_ID
    )


@pytest.mark.asyncio
async def test_unknown_grant_revocation_and_inactive_client_lock(oauth_case: OAuthCase) -> None:
    """Revocation is idempotent for unknown identities and can lock an inactive client.

    Given an absent grant and an inactive client.
    When revocation or inactive-inclusive locking runs.
    Then revocation is idempotent and the explicit inactive lookup remains available.
    """
    assert not await oauth_case.harness.store.revoke_grant(str(uuid7()), datetime.now(UTC))
    async with oauth_case.harness.store.begin_write_transaction() as session:
        client = await OAuthTransaction(session).client(_CLIENT_ID, lock=False, active_only=False)
        assert client is not None


@pytest.mark.asyncio
async def test_failed_deny_claim_keeps_request_pending(
    oauth_case: OAuthCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A losing denial CAS is rolled back by the wrapper without resolving consent.

    Given a valid unresolved request.
    When its denial compare-and-swap claim loses.
    Then the request remains pending after rollback.
    """
    pending = await oauth_case.harness.service.create_authorization_request(_request())
    _lose_one_claim(oauth_case.harness.repository, monkeypatch, "oauth_authorization_requests")
    assert (
        await oauth_case.harness.store.deny_authorization_request(
            pending.raw_request_id, resolved_at=datetime.now(UTC)
        )
        is None
    )
    assert await oauth_case.harness.store.get_pending_authorization_request(
        pending.raw_request_id, now=datetime.now(UTC)
    )


@pytest.mark.asyncio
async def test_code_binding_validation_preserves_request_and_code(oauth_case: OAuthCase) -> None:
    """A forged consent code is not inserted; wrong-client/refresh binding cannot consume one.

    Given pending consent with mismatched code or refresh linkage.
    When the store attempts approval or consumption.
    Then invalid linkage cannot resolve the request or consume its code.
    """
    pending = await oauth_case.harness.service.create_authorization_request(_request())
    approved = await oauth_case.harness.service.approve_authorization_request(
        OAuthConsentApprovalCommand(pending.raw_request_id, oauth_case.owner)
    )
    another = await oauth_case.harness.service.create_authorization_request(_request())
    now = datetime.now(UTC)
    invalid = OAuthAuthorizationCode(
        public_id=str(uuid7()),
        code_hash=hash_token("forged"),
        grant_public_id=approved.grant_public_id,
        redirect_uri="https://forged.invalid",
        redirect_uri_provided_explicitly=True,
        code_challenge=_pkce_s256(_VERIFIER),
        code_challenge_method="S256",
        scopes=[SNAPPER_READ, OFFLINE_ACCESS],
        created_at=now,
        expires_at=now + timedelta(minutes=1),
    )
    assert (
        await oauth_case.harness.store.approve_authorization_request(
            another.raw_request_id, code=invalid, resolved_at=now
        )
        is None
    )
    assert await oauth_case.harness.store.get_pending_authorization_request(
        another.raw_request_id, now=now
    )
    assert (
        await oauth_case.harness.store.consume_authorization_code(
            approved.raw_code, client_id="unknown", consumed_at=now
        )
        is None
    )
    refresh = OAuthRefreshToken(
        public_id=str(uuid7()),
        token_hash=hash_token("wrong-grant"),
        family_public_id=str(uuid7()),
        grant_public_id=str(uuid7()),
        scopes=[SNAPPER_READ],
        created_at=now,
        expires_at=now + timedelta(days=1),
    )
    with pytest.raises(ValueError):
        await oauth_case.harness.store.consume_authorization_code(
            approved.raw_code, client_id=_CLIENT_ID, consumed_at=now, refresh_token=refresh
        )
    assert await oauth_case.harness.store.load_authorization_code(
        approved.raw_code, client_id=_CLIENT_ID
    )


@pytest.mark.asyncio
async def test_unknown_store_request_and_family_only_recovery_are_idempotent(
    oauth_case: OAuthCase,
) -> None:
    """Unknown request/refresh identities produce no claims and no unrelated revocations.

    Given an absent request or token and a family-only revocation operation.
    When the low-level store recovery primitives run.
    Then missing identities are harmless and family-only behavior remains scoped.
    """
    now = datetime.now(UTC)
    code = OAuthAuthorizationCode(
        public_id=str(uuid7()),
        code_hash=hash_token("unknown-code"),
        grant_public_id=str(uuid7()),
        redirect_uri=_REDIRECT_URI,
        redirect_uri_provided_explicitly=True,
        code_challenge=_pkce_s256(_VERIFIER),
        code_challenge_method="S256",
        scopes=[SNAPPER_READ],
        created_at=now,
        expires_at=now + timedelta(minutes=1),
    )
    assert (
        await oauth_case.harness.store.approve_authorization_request(
            "unknown", code=code, resolved_at=now
        )
        is None
    )
    assert (
        await oauth_case.harness.store.consume_authorization_code(
            "unknown", client_id=_CLIENT_ID, consumed_at=now
        )
        is None
    )
    assert (
        await oauth_case.harness.store._revoke_after_lost_rotation(
            "unknown", client_id=_CLIENT_ID, revoked_at=now
        )
        is OAuthRefreshRotationOutcome.INVALID
    )


@pytest.mark.asyncio
async def test_unknown_integrity_conflict_does_not_retry_or_leave_partial_consent(
    oauth_case: OAuthCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the exact active-binding uniqueness conflict is eligible for one complete retry.

    Given valid consent and an unrelated database constraint failure.
    When binding resolution raises that failure.
    Then the original error propagates without retry or partial rows.
    """
    pending = await oauth_case.harness.service.create_authorization_request(_request())
    failure = IntegrityError("INSERT", {}, RuntimeError("different constraint"))
    resolve = AsyncMock(side_effect=failure)
    monkeypatch.setattr(
        oauth_case.harness.delegate_service, "resolve_oauth_delegate_in_tx", resolve
    )
    with pytest.raises(IntegrityError):
        await oauth_case.harness.service.approve_authorization_request(
            OAuthConsentApprovalCommand(pending.raw_request_id, oauth_case.owner)
        )
    assert resolve.await_count == 1
    await _no_grants(oauth_case)


@pytest.mark.asyncio
async def test_refresh_cannot_reexpand_a_predecessor_after_scope_narrowing(
    oauth_case: OAuthCase,
) -> None:
    """Dropping offline_access ends the family; its consumed credential cannot re-expand.

    Given a refresh family narrowed from read/offline to read, which is terminal.
    When the consumed predecessor asks for offline access again.
    Then no successor exists, the attempt is treated as replay, and the grant is revoked.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    narrowed = await oauth_case.harness.service.exchange_refresh_token(
        replace(_refresh(tokens.refresh_token), scopes=(SNAPPER_READ,))
    )
    assert narrowed.refresh_token is None
    assert await oauth_case.harness.service.verify_access_token(narrowed.access_token)
    with pytest.raises(OAuthRefreshReuseDetectedError):
        await oauth_case.harness.service.exchange_refresh_token(
            replace(_refresh(tokens.refresh_token), scopes=(SNAPPER_READ, OFFLINE_ACCESS))
        )
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.verify_access_token(narrowed.access_token)


@pytest.mark.parametrize(
    "change",
    [
        "owner_missing",
        "owner_role",
        "owner_inactive",
        "owner_membership",
        "delegate_creator",
        "delegate_inactive",
        "delegate_caps",
        "delegate_operational",
        "delegate_membership",
    ],
)
@pytest.mark.asyncio
async def test_live_projection_reloads_each_authority_link(
    oauth_case: OAuthCase, change: str
) -> None:
    """Every changed live owner or delegate link is observed without trusting the prior valid JWT.

    Given a previously valid JWT and a changed owner or delegate authority link.
    When the binding and token are projected again.
    Then the current invalid authority is observed without trusting the old JWT.
    """
    tokens = await _tokens(oauth_case)
    before = await oauth_case.harness.service.verify_access_token(tokens.access_token)
    owner_id = oauth_case.owner.user_public_id
    delegate_id = before.principal.user_public_id
    async with oauth_case.harness.repository.session() as session:
        statements = {
            "owner_missing": delete(User).where(User.public_id == owner_id),
            "owner_role": update(User)
            .where(User.public_id == owner_id)
            .values(role="unknown-role"),
            "owner_inactive": update(User)
            .where(User.public_id == owner_id)
            .values(is_active=False),
            "owner_membership": update(UserOperatorMembership)
            .where(UserOperatorMembership.user_public_id == owner_id)
            .values(known_to=datetime.now(UTC)),
            "delegate_creator": update(User)
            .where(User.public_id == delegate_id)
            .values(created_by_user_public_id=str(uuid7())),
            "delegate_inactive": update(User)
            .where(User.public_id == delegate_id)
            .values(is_active=False),
            "delegate_caps": delete(UserTradingCaps).where(
                UserTradingCaps.user_public_id == delegate_id
            ),
            "delegate_operational": delete(AiDelegate).where(
                AiDelegate.user_public_id == delegate_id
            ),
            "delegate_membership": update(UserOperatorMembership)
            .where(UserOperatorMembership.user_public_id == delegate_id)
            .values(known_to=datetime.now(UTC)),
        }
        await session.execute(statements[change])
        await session.commit()
    assert (
        await oauth_case.harness.delegate_service.get_oauth_delegate_by_grant(
            before.grant_public_id, _CLIENT_ID
        )
        is None
    )
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.verify_access_token(tokens.access_token)


@pytest.mark.asyncio
async def test_disappeared_grant_after_owner_validation_cannot_authenticate(
    oauth_case: OAuthCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing locked grant stays unavailable even when its initial identity lookup succeeded.

    Given a grant found by its initial identity lookup.
    When the subsequent locked lookup no longer finds it.
    Then verification fails without authenticating the stale identity.
    """
    tokens = await _tokens(oauth_case)
    original = OAuthTransaction.grant

    async def missing_locked_grant(
        tx: OAuthTransaction, grant_public_id: str, client_id: str, *, lock: bool = True
    ) -> OAuthGrant | None:
        """Keep the real initial lookup and fail only the locking dependency boundary."""
        return None if lock else await original(tx, grant_public_id, client_id, lock=False)

    monkeypatch.setattr(OAuthTransaction, "grant", missing_locked_grant)
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.verify_access_token(tokens.access_token)


@pytest.mark.parametrize("link", ["owner", "membership", "delegate", "persistence"])
@pytest.mark.asyncio
async def test_authority_interval_is_rechecked_after_query_time(
    oauth_case: OAuthCase, monkeypatch: pytest.MonkeyPatch, link: str
) -> None:
    """Reject a row that was active at query construction but expired before lock completion.

    Given a real persisted authority row with a bounded future end time.
    When time crosses that end after the query captures its temporal range.
    Then post-lock validation refuses the stale authority without publishing access.
    """
    tokens = await _tokens(oauth_case)
    verified = await oauth_case.harness.service.verify_access_token(tokens.access_token)
    start = datetime.now(UTC)
    model: type[User] | type[UserOperatorMembership] | type[UserTradingCaps] = (
        UserOperatorMembership if link == "membership" else User
    )
    if link == "delegate":
        model = UserTradingCaps
    identity = (
        verified.principal.user_public_id if link == "delegate" else oauth_case.owner.user_public_id
    )
    column = {
        "owner": User.public_id,
        "persistence": User.public_id,
        "membership": UserOperatorMembership.user_public_id,
        "delegate": UserTradingCaps.user_public_id,
    }[link]
    async with oauth_case.harness.repository.session() as session:
        await session.execute(
            update(model).where(column == identity).values(known_to=start + timedelta(seconds=1))
        )
        await session.commit()
    with freeze_time(start) as clock:
        original_now = authority_module.where_active_now
        original_at = delegate_module.where_active

        def crossing_now(
            target: type[User] | type[UserOperatorMembership],
        ) -> tuple[ColumnElement[bool], ColumnElement[bool]]:
            """Capture the real range before the modeled database wait crosses expiry."""
            result = original_now(target)
            if target is model:
                clock.tick(timedelta(seconds=2))
            return result

        def crossing_at(
            target: type[User] | type[UserTradingCaps] | type[UserOperatorMembership], at: datetime
        ) -> tuple[ColumnElement[bool], ColumnElement[bool]]:
            """Keep the query's captured instant while time advances before its result."""
            result = original_at(target, at)
            if target is model:
                clock.tick(timedelta(seconds=2))
            return result

        if link == "persistence":
            monkeypatch.setattr(transaction_module, "where_active_now", crossing_now)
            async with oauth_case.harness.store.begin_write_transaction() as session:
                with pytest.raises(OAuthTransactionConflictError):
                    await OAuthTransaction(session).lock_owner_identity(identity)
        else:
            if link == "delegate":
                monkeypatch.setattr(delegate_module, "where_active", crossing_at)
            else:
                monkeypatch.setattr(authority_module, "where_active_now", crossing_now)
            with pytest.raises(MCPOAuthError):
                await oauth_case.harness.service.verify_access_token(tokens.access_token)


@pytest.mark.parametrize("authority", ["owner", "operator"])
@pytest.mark.asyncio
async def test_denial_authority_refusals_are_typed_and_leave_request_pending(
    oauth_case: OAuthCase, authority: str
) -> None:
    """Report a typed refusal after rolling back a denial with stale authority.

    Given a pending request whose owner role or operator membership loses authority.
    When that owner attempts to deny the request.
    Then access_denied is reported with the request pending and no grant, code or delegate written.
    """
    pending = await oauth_case.harness.service.create_authorization_request(_request())
    owner_id = oauth_case.owner.user_public_id
    statement = (
        update(User).where(User.public_id == owner_id).values(role=UserRole.VIEWER.value)
        if authority == "owner"
        else update(UserOperatorMembership)
        .where(UserOperatorMembership.user_public_id == owner_id)
        .values(known_to=datetime.now(UTC))
    )
    async with oauth_case.harness.repository.session() as session:
        await session.execute(statement)
        await session.commit()
    try:
        with pytest.raises(MCPOAuthError, match="access_denied"):
            await oauth_case.harness.service.deny_authorization_request(
                OAuthConsentDenialCommand(pending.raw_request_id, oauth_case.owner)
            )
    finally:
        request = await oauth_case.harness.store.get_pending_authorization_request(
            pending.raw_request_id, now=datetime.now(UTC)
        )
        assert request is not None and request.decision is None and request.resolved_at is None
        async with oauth_case.harness.repository.session() as session:
            for model in (OAuthGrant, OAuthAuthorizationCode, OAuthRefreshToken, AiDelegate):
                assert list((await session.execute(select(model))).scalars()) == []
