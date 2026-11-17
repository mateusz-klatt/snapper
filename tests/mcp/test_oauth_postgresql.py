"""Actual PostgreSQL races for the dormant OAuth transaction/authority boundary."""

import asyncio
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from uuid import uuid7

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.engine import Connection
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.ai_delegates.service import DelegateProliferationError
from snapper.application.ai_delegates.service import DelegateService
from snapper.application.ai_delegates.service import OAuthDelegateIdentity
from snapper.application.ai_delegates.service import OAuthDelegateRequest
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import TokenManager
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AiDelegate
from snapper.data.models import Base
from snapper.data.models import OAuthAuthorizationCode
from snapper.data.models import OAuthAuthorizationRequest
from snapper.data.models import OAuthClient
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.data.models import User
from snapper.data.models import UserActiveToken
from snapper.data.models import UserOperatorMembership
from snapper.data.models import UserTradingCaps
from snapper.data.repository import SQLAlchemyRepository
from snapper.mcp.oauth.contracts import MCPOAuthError
from snapper.mcp.oauth.contracts import MCPOAuthServiceConfig
from snapper.mcp.oauth.contracts import OAuthAuthorizationRequestCreate
from snapper.mcp.oauth.contracts import OAuthCodeExchangeCommand
from snapper.mcp.oauth.contracts import OAuthConsentApprovalCommand
from snapper.mcp.oauth.contracts import OAuthRefreshExchangeCommand
from snapper.mcp.oauth.contracts import OAuthRefreshReuseDetectedError
from snapper.mcp.oauth.contracts import OAuthTokenExchangeResult
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import SNAPPER_READ
from snapper.mcp.oauth.service import MCPOAuthService
from snapper.mcp.oauth.transaction import OAuthTransaction
from tests.mcp.test_oauth_service import _CLIENT_ID
from tests.mcp.test_oauth_service import _CLIENT_SECRET
from tests.mcp.test_oauth_service import _CLIENT_SECRET_HASH
from tests.mcp.test_oauth_service import _ISSUER
from tests.mcp.test_oauth_service import _OPERATOR_A
from tests.mcp.test_oauth_service import _OTHER_CLIENT_ID
from tests.mcp.test_oauth_service import _OTHER_SECRET_HASH
from tests.mcp.test_oauth_service import _PASSWORD_HASH
from tests.mcp.test_oauth_service import _REDIRECT_URI
from tests.mcp.test_oauth_service import _RESOURCE
from tests.mcp.test_oauth_service import _VERIFIER
from tests.mcp.test_oauth_service import _fresh_token_manager
from tests.mcp.test_oauth_service import _pkce_s256
from tests.mcp.test_oauth_service import _principal
from tests.mcp.test_oauth_service import _seed_user_with_membership

_PG_URL = os.getenv("SNAPPER_OAUTH_TEST_PG_URL", "")
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(not _PG_URL, reason="Owned local PostgreSQL fixture not requested"),
]


@dataclass(frozen=True)
class PGFrame:
    """One uniquely created test database, two independent pools, and native lock observer."""

    first: MCPOAuthService
    second: MCPOAuthService
    repository: SQLAlchemyRepository
    other_repository: SQLAlchemyRepository
    admin: asyncpg.Connection
    second_application: str


def _create_oauth_tables(connection: Connection) -> None:
    """Create the ten unchanged real ORM tables needed by these protocol races."""
    models = [
        User,
        UserTradingCaps,
        UserOperatorMembership,
        AiDelegate,
        OAuthClient,
        OAuthGrant,
        OAuthAuthorizationRequest,
        OAuthAuthorizationCode,
        OAuthRefreshToken,
        UserActiveToken,
    ]
    Base.metadata.create_all(connection, tables=[model.__table__ for model in models])


def _service(repository: SQLAlchemyRepository, manager: TokenManager) -> MCPOAuthService:
    """Build another actual service instance with no shared owner mutex requirement."""
    return MCPOAuthService(
        repository=repository,
        token_manager=manager,
        config=MCPOAuthServiceConfig(_ISSUER, _RESOURCE),
        delegate_service=DelegateService(repository=repository, token_manager=manager),
    )


@pytest_asyncio.fixture
async def pg_frame() -> AsyncIterator[PGFrame]:
    """Create and remove a dedicated database on the explicitly selected owned loopback server."""
    url = make_url(_PG_URL)
    if url.host != "127.0.0.1" or url.database != "snapper_oauth_tests" or not url.port:
        raise ValueError("OAuth PG tests require the dedicated loopback snapper_oauth_tests server")
    admin = await asyncpg.connect(
        host=url.host, port=url.port, user=url.username, database=url.database
    )
    database = "snapper_oauth_case_" + uuid7().hex
    await admin.execute(f'CREATE DATABASE "{database}"')
    child = url.set(database=database).render_as_string(hide_password=False)
    repository = SQLAlchemyRepository(child)
    other_repository = SQLAlchemyRepository(child)
    application = "oauth_second_" + uuid7().hex
    try:
        async with repository.engine.begin() as connection:
            await connection.run_sync(_create_oauth_tables)
        async with other_repository.session() as session:
            await session.execute(
                text("SELECT set_config('application_name', :name, false)"), {"name": application}
            )
            await session.execute(
                text("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            )
            await session.commit()
        manager = _fresh_token_manager()
        first, second = _service(repository, manager), _service(other_repository, manager)
        now = datetime.now(UTC)
        for client_id, secret_hash in [
            (_CLIENT_ID, _CLIENT_SECRET_HASH),
            (_OTHER_CLIENT_ID, _OTHER_SECRET_HASH),
        ]:
            await first.store.add_client(
                OAuthClient(
                    client_id=client_id,
                    client_secret_hash=secret_hash,
                    client_name="isolated race fixture",
                    redirect_uris=[_REDIRECT_URI],
                    token_endpoint_auth_method="client_secret_basic",
                    allowed_scopes=[SNAPPER_READ, OFFLINE_ACCESS],
                    is_active=True,
                    created_at=now,
                )
            )
        yield PGFrame(first, second, repository, other_repository, admin, application)
    finally:
        await repository.engine.dispose()
        await other_repository.engine.dispose()
        await admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", database
        )
        await admin.close()


async def _wait_for_native_lock(frame: PGFrame) -> None:
    """Require an actual PostgreSQL lock wait from the second independent service pool."""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if await frame.admin.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')",
            frame.second_application,
        ):
            return
        await asyncio.sleep(0.01)
    raise AssertionError("second OAuth transaction never entered an actual PostgreSQL lock wait")


async def _pending(service: MCPOAuthService, state: str = "race") -> str:
    """Create one real, resource-bound request without an external HTTP route."""
    result = await service.create_authorization_request(
        OAuthAuthorizationRequestCreate(
            client_id=_CLIENT_ID,
            redirect_uri=_REDIRECT_URI,
            scopes=(SNAPPER_READ, OFFLINE_ACCESS),
            code_challenge=_pkce_s256(_VERIFIER),
            code_challenge_method="S256",
            resource=_RESOURCE,
            state=state,
        )
    )
    return result.raw_request_id


def _code(raw_code: str) -> OAuthCodeExchangeCommand:
    """Return one exact-client, redirect-bound S256 exchange command."""
    return OAuthCodeExchangeCommand(
        client_id=_CLIENT_ID,
        client_secret=_CLIENT_SECRET,
        auth_method="client_secret_basic",
        raw_code=raw_code,
        code_verifier=_VERIFIER,
        redirect_uri=_REDIRECT_URI,
        resource=_RESOURCE,
    )


def _refresh(raw: str) -> OAuthRefreshExchangeCommand:
    """Return one authentic client refresh command for the dedicated test grant."""
    return OAuthRefreshExchangeCommand(
        client_id=_CLIENT_ID,
        client_secret=_CLIENT_SECRET,
        auth_method="client_secret_basic",
        raw_refresh_token=raw,
        resource=_RESOURCE,
    )


async def _tokens(frame: PGFrame) -> tuple[str, OAuthTokenExchangeResult]:
    """Seed one actual owner and complete one consent/code exchange for a race."""
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(frame.repository, user_public_id=owner_id)
    owner = _principal(owner_id, membership_public_id=membership)
    approved = await frame.first.approve_authorization_request(
        OAuthConsentApprovalCommand(await _pending(frame.first), owner)
    )
    return owner_id, await frame.first.exchange_authorization_code(_code(approved.raw_code))


async def test_two_services_absent_binding_serialize_on_existing_owner(
    pg_frame: PGFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two independent pools reuse one absent binding after a witnessed native lock wait.

    Given two independent PostgreSQL service pools and no existing binding.
    When both approve consent while one owner lock is held.
    Then the native lock wait resolves to one delegate and one shared grant.
    """
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(pg_frame.repository, user_public_id=owner_id)
    owner = _principal(owner_id, membership_public_id=membership)
    requests = [await _pending(pg_frame.first, str(n)) for n in range(2)]
    entered, release = asyncio.Event(), asyncio.Event()
    original = DelegateService._load_oauth_delegate

    async def hold_empty(
        session: AsyncSession, *, owner_public_id: str, operator_public_id: str, client_id: str
    ) -> OAuthGrant | None:
        """Pause only the first absent-binding lookup after its owner lock exists."""
        row = await original(
            session,
            owner_public_id=owner_public_id,
            operator_public_id=operator_public_id,
            client_id=client_id,
        )
        if row is None and not entered.is_set():
            entered.set()
            await release.wait()
        return row

    monkeypatch.setattr(DelegateService, "_load_oauth_delegate", staticmethod(hold_empty))
    first = asyncio.create_task(
        pg_frame.first.approve_authorization_request(
            OAuthConsentApprovalCommand(requests[0], owner)
        )
    )
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 10)
        second = asyncio.create_task(
            pg_frame.second.approve_authorization_request(
                OAuthConsentApprovalCommand(requests[1], owner)
            )
        )
        await _wait_for_native_lock(pg_frame)
        release.set()
        one, two = await asyncio.wait_for(asyncio.gather(first, second), 10)
        assert one.grant_public_id == two.grant_public_id
        async with pg_frame.repository.session() as session:
            assert (
                len(
                    (
                        await session.execute(
                            select(User).where(User.created_by_user_public_id == owner_id)
                        )
                    )
                    .scalars()
                    .all()
                )
                == 1
            )
            assert len((await session.execute(select(OAuthGrant))).scalars().all()) == 1
            assert len((await session.execute(select(OAuthAuthorizationCode))).scalars().all()) == 2
    finally:
        release.set()
        for task in [first, second]:
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *[task for task in [first, second] if task is not None], return_exceptions=True
        )


async def test_code_one_use_race_leaves_one_successor_family(
    pg_frame: PGFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One real code exchange commits; the waiting exchange cannot consume it twice.

    Given one approved code and two independent PostgreSQL pools.
    When both exchange that code through an actual database lock wait.
    Then one exchange succeeds and exactly one refresh family is stored.
    """
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(pg_frame.repository, user_public_id=owner_id)
    approved = await pg_frame.first.approve_authorization_request(
        OAuthConsentApprovalCommand(
            await _pending(pg_frame.first), _principal(owner_id, membership_public_id=membership)
        )
    )
    entered, release = asyncio.Event(), asyncio.Event()
    original = OAuthTransaction.consume_code

    async def hold_claim(tx: OAuthTransaction, code: OAuthAuthorizationCode, now: datetime) -> None:
        """Hold the first successful real claim before its enclosing commit."""
        await original(tx, code, now)
        entered.set()
        await release.wait()

    monkeypatch.setattr(OAuthTransaction, "consume_code", hold_claim)
    first = asyncio.create_task(
        pg_frame.first.exchange_authorization_code(_code(approved.raw_code))
    )
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 10)
        second = asyncio.create_task(
            pg_frame.second.exchange_authorization_code(_code(approved.raw_code))
        )
        await _wait_for_native_lock(pg_frame)
        release.set()
        token = await asyncio.wait_for(first, 10)
        assert token.refresh_token is not None
        with pytest.raises(MCPOAuthError):
            await asyncio.wait_for(second, 10)
        async with pg_frame.repository.session() as session:
            assert len((await session.execute(select(OAuthRefreshToken))).scalars().all()) == 1
            assert list((await session.execute(select(UserActiveToken))).scalars()) == []
    finally:
        release.set()
        for task in [first, second]:
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *[task for task in [first, second] if task is not None], return_exceptions=True
        )


async def test_refresh_race_commits_strict_grant_wide_revocation(
    pg_frame: PGFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A waiting reuse revokes the first rotation and every sibling family after commit.

    Given two refresh families on one grant and an unrelated live grant.
    When two PostgreSQL pools race to use one predecessor.
    Then confirmed reuse retires the affected grant and every attached family only.
    """
    owner_id, tokens = await _tokens(pg_frame)
    assert tokens.refresh_token is not None
    same_owner = _principal(owner_id)
    sibling_consent = await pg_frame.first.approve_authorization_request(
        OAuthConsentApprovalCommand(await _pending(pg_frame.first, "sibling"), same_owner)
    )
    sibling = await pg_frame.first.exchange_authorization_code(_code(sibling_consent.raw_code))
    _unrelated_owner, unrelated = await _tokens(pg_frame)
    entered, release = asyncio.Event(), asyncio.Event()
    original = OAuthTransaction.rotate_refresh

    async def hold_rotation(
        tx: OAuthTransaction,
        predecessor: OAuthRefreshToken,
        successor: OAuthRefreshToken,
        now: datetime,
    ) -> None:
        """Pause only after the first real atomic predecessor claim."""
        await original(tx, predecessor, successor, now)
        entered.set()
        await release.wait()

    monkeypatch.setattr(OAuthTransaction, "rotate_refresh", hold_rotation)
    first = asyncio.create_task(
        pg_frame.first.exchange_refresh_token(_refresh(tokens.refresh_token))
    )
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 10)
        second = asyncio.create_task(
            pg_frame.second.exchange_refresh_token(_refresh(tokens.refresh_token))
        )
        await _wait_for_native_lock(pg_frame)
        release.set()
        rotated = await asyncio.wait_for(first, 10)
        with pytest.raises(OAuthRefreshReuseDetectedError):
            await asyncio.wait_for(second, 10)
        with pytest.raises(MCPOAuthError):
            await pg_frame.first.verify_access_token(rotated.access_token)
        with pytest.raises(MCPOAuthError):
            await pg_frame.first.verify_access_token(sibling.access_token)
        assert await pg_frame.first.verify_access_token(unrelated.access_token)
        async with pg_frame.repository.session() as session:
            grants = (
                (
                    await session.execute(
                        select(OAuthGrant).where(OAuthGrant.owner_user_public_id == owner_id)
                    )
                )
                .scalars()
                .all()
            )
            assert len(grants) == 1 and grants[0].revoked_at is not None
            rows = (
                (
                    await session.execute(
                        select(OAuthRefreshToken).where(
                            OAuthRefreshToken.grant_public_id == grants[0].public_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(rows) == 3 and all(row.revoked_at is not None for row in rows)
    finally:
        release.set()
        for task in [first, second]:
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *[task for task in [first, second] if task is not None], return_exceptions=True
        )


@pytest.mark.parametrize("change", ["role", "membership", "scd2_role"])
async def test_waiting_owner_authority_refuses_committed_demotion(
    pg_frame: PGFrame, change: str
) -> None:
    """Read-committed live authority rejects a role/membership change committed during its wait.

    Given a waiting PostgreSQL exchange and an owner authority writer.
    When the writer commits role, membership or SCD2 demotion before releasing its lock.
    Then fresh authority denies exchange without an orphan or consumed code.
    """
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(pg_frame.repository, user_public_id=owner_id)
    raw = await _pending(pg_frame.first)
    owner = _principal(owner_id, membership_public_id=membership)
    waiter = None
    async with pg_frame.repository.session() as writer:
        now = datetime.now(UTC)
        if change == "membership":
            await writer.execute(
                update(UserOperatorMembership)
                .where(UserOperatorMembership.user_public_id == owner_id)
                .values(known_to=now)
            )
        elif change == "role":
            await writer.execute(
                update(User).where(User.public_id == owner_id).values(role=UserRole.VIEWER.value)
            )
        else:
            row = (
                await writer.execute(select(User).where(User.public_id == owner_id))
            ).scalar_one()
            await writer.execute(update(User).where(User.id == row.id).values(known_to=now))
            writer.add(
                User(
                    public_id=owner_id,
                    username=row.username,
                    email=row.email,
                    password_hash=row.password_hash,
                    role=UserRole.VIEWER.value,
                    is_active=True,
                    created_at=row.created_at,
                    timestamp=now,
                    known_to=KNOWN_TO_MAX,
                    session_id=str(uuid7()),
                    sequence_id=3,
                )
            )
            await writer.flush()
        try:
            waiter = asyncio.create_task(
                pg_frame.second.approve_authorization_request(
                    OAuthConsentApprovalCommand(raw, owner)
                )
            )
            await _wait_for_native_lock(pg_frame)
            await writer.commit()
            with pytest.raises((PermissionError, MCPOAuthError)):
                await asyncio.wait_for(waiter, 10)
        finally:
            if waiter is not None and not waiter.done():
                waiter.cancel()
            if waiter is not None:
                await asyncio.gather(waiter, return_exceptions=True)
    assert (
        await pg_frame.first.store.get_pending_authorization_request(raw, now=datetime.now(UTC))
        is not None
    )
    async with pg_frame.repository.session() as session:
        assert list((await session.execute(select(OAuthGrant))).scalars()) == []
        assert list((await session.execute(select(OAuthAuthorizationCode))).scalars()) == []
        assert (
            list(
                (
                    await session.execute(
                        select(User).where(User.created_by_user_public_id == owner_id)
                    )
                ).scalars()
            )
            == []
        )


async def test_owner_lock_prevents_cross_client_delegate_cap_race(
    pg_frame: PGFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Different clients serialize on the owner and cannot turn four delegates into six.

    Given an owner with four delegates and two different OAuth clients.
    When independent PostgreSQL pools race to create the fifth delegate.
    Then the owner lock prevents a sixth delegate across client locks.
    """
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(pg_frame.repository, user_public_id=owner_id)
    owner = _principal(owner_id, membership_public_id=membership)
    async with pg_frame.repository.session() as session:
        for _ in range(4):
            uid = str(uuid7())
            session.add(
                User(
                    public_id=uid,
                    username="existing-" + uid,
                    password_hash=_PASSWORD_HASH,
                    role=UserRole.AI_DELEGATE.value,
                    created_by_user_public_id=owner_id,
                    is_active=True,
                    created_at=datetime.now(UTC),
                    timestamp=datetime.now(UTC),
                    known_to=KNOWN_TO_MAX,
                    session_id=str(uuid7()),
                    sequence_id=1,
                )
            )
        await session.commit()
    entered, release = asyncio.Event(), asyncio.Event()
    original = DelegateService._load_oauth_delegate

    async def hold_first(
        session: AsyncSession, *, owner_public_id: str, operator_public_id: str, client_id: str
    ) -> OAuthGrant | None:
        """Hold the existing owner lock before the first absent-binding create."""
        row = await original(
            session,
            owner_public_id=owner_public_id,
            operator_public_id=operator_public_id,
            client_id=client_id,
        )
        if client_id == _CLIENT_ID and row is None:
            entered.set()
            await release.wait()
        return row

    monkeypatch.setattr(DelegateService, "_load_oauth_delegate", staticmethod(hold_first))
    first = asyncio.create_task(
        pg_frame.first.delegate_service.get_or_create_oauth_delegate(
            owner, OAuthDelegateRequest(_CLIENT_ID, _RESOURCE, (SNAPPER_READ,))
        )
    )
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 10)
        second = asyncio.create_task(
            pg_frame.second.delegate_service.get_or_create_oauth_delegate(
                owner, OAuthDelegateRequest(_OTHER_CLIENT_ID, _RESOURCE, (SNAPPER_READ,))
            )
        )
        await _wait_for_native_lock(pg_frame)
        release.set()
        await asyncio.wait_for(first, 10)
        with pytest.raises(DelegateProliferationError):
            await asyncio.wait_for(second, 10)
        async with pg_frame.repository.session() as session:
            assert (
                len(
                    (
                        await session.execute(
                            select(User).where(User.created_by_user_public_id == owner_id)
                        )
                    )
                    .scalars()
                    .all()
                )
                == 5
            )
            assert len((await session.execute(select(OAuthGrant))).scalars().all()) == 1
    finally:
        release.set()
        for task in [first, second]:
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *[task for task in [first, second] if task is not None], return_exceptions=True
        )


async def test_commit_failure_rolls_back_whole_postgresql_consent(
    pg_frame: PGFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed commit publishes no credential and leaves no delegate, grant, code, or resolved request.

    Given valid consent on an actual PostgreSQL database.
    When the outer commit fails before publication.
    Then request, delegate and grant changes all roll back.
    """
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(pg_frame.repository, user_public_id=owner_id)
    raw = await _pending(pg_frame.first)
    original = pg_frame.other_repository.session
    attempts = 0

    @asynccontextmanager
    async def fail_commit() -> AsyncIterator[AsyncSession]:
        """Fail only the actual outer commit; the repository still performs real rollback."""
        async with original() as session:

            async def refuse_commit() -> None:
                """Count the single attempted commit without dispatching it."""
                nonlocal attempts
                attempts += 1
                raise RuntimeError("injected commit failure")

            monkeypatch.setattr(session, "commit", refuse_commit)
            yield session

    monkeypatch.setattr(pg_frame.other_repository, "session", fail_commit)
    with pytest.raises(RuntimeError, match="injected commit failure"):
        await pg_frame.second.approve_authorization_request(
            OAuthConsentApprovalCommand(raw, _principal(owner_id, membership_public_id=membership))
        )
    assert attempts == 1
    assert await pg_frame.first.store.get_pending_authorization_request(raw, now=datetime.now(UTC))
    async with pg_frame.repository.session() as session:
        assert list((await session.execute(select(OAuthGrant))).scalars()) == []
        assert list((await session.execute(select(OAuthAuthorizationCode))).scalars()) == []
        assert (
            list(
                (
                    await session.execute(
                        select(User).where(User.created_by_user_public_id == owner_id)
                    )
                ).scalars()
            )
            == []
        )


async def test_actual_asyncpg_binding_conflict_rolls_back_and_reuses_winner(
    pg_frame: PGFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retry only the actual asyncpg active-binding constraint after complete rollback.

    Given an independently committed binding and pending consent on PostgreSQL.
    When a duplicate active binding raises the real SQLAlchemy asyncpg error.
    Then the outer transaction rolls back before one retry reuses the winner.
    """
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(pg_frame.repository, user_public_id=owner_id)
    owner = _principal(owner_id, membership_public_id=membership)
    request = OAuthDelegateRequest(
        _CLIENT_ID, _RESOURCE, (SNAPPER_READ, OFFLINE_ACCESS), _OPERATOR_A
    )
    winner = await pg_frame.first.delegate_service.get_or_create_oauth_delegate(owner, request)
    raw = await _pending(pg_frame.first)
    original = pg_frame.first.delegate_service.resolve_oauth_delegate_in_tx
    attempts = 0

    async def duplicate_once(
        session: AsyncSession, caller: AuthPrincipal, binding: OAuthDelegateRequest
    ) -> OAuthDelegateIdentity:
        """Trigger the actual named unique index through the service-owned transaction."""
        nonlocal attempts
        attempts += 1
        identity = await original(session, caller, binding)
        if attempts == 1:
            session.add(
                OAuthGrant(
                    public_id=str(uuid7()),
                    owner_user_public_id=owner_id,
                    delegate_user_public_id=identity.principal.user_public_id,
                    client_id=_CLIENT_ID,
                    resource=_RESOURCE,
                    scopes=list(binding.scopes),
                    operator_public_id=_OPERATOR_A,
                    created_at=datetime.now(UTC),
                )
            )
            await session.flush()
        return identity

    monkeypatch.setattr(
        pg_frame.first.delegate_service, "resolve_oauth_delegate_in_tx", duplicate_once
    )
    approved = await pg_frame.first.approve_authorization_request(
        OAuthConsentApprovalCommand(raw, owner)
    )
    assert approved.grant_public_id == winner.grant_public_id
    assert attempts == 2
    async with pg_frame.repository.session() as session:
        assert len((await session.execute(select(OAuthGrant))).scalars().all()) == 1
        assert len((await session.execute(select(User))).scalars().all()) == 2
        assert len((await session.execute(select(OAuthAuthorizationCode))).scalars().all()) == 1
