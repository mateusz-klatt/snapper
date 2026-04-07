"""Repository-level tests for wallet_operator_scope_grants methods.

Covers Phase 0a step 3 of Plan 0 (multi-tenant foundation): the
``create_scope_grant`` / ``handover_grant`` methods on
``SQLAlchemyRepository`` and the cross-scope overlap detection that
enforces the instrument-exclusive rule (D2).

These tests run against a real on-disk SQLite database (no mocks) so
the partial unique indexes, advisory-lock no-op, and SCD2 close+insert
flow are exercised end-to-end.

Plan reference: ``proprietary/plans/plan_multi_tenant_foundation.md``
Sections 3.6, 3.7, 14.6 D2/D3, 14.7.1, 14.7.2, 14.7.4, 14.7.8.
"""

from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from sqlalchemy.exc import IntegrityError

from snapper.data.models import InstrumentUnderlyingMapping
from snapper.data.models import Operator
from snapper.data.models import UnderlyingAsset
from snapper.data.models import Wallet
from snapper.data.repository import ScopeGrantConflictError
from snapper.data.repository import ScopeGrantNotFoundError
from snapper.data.repository import ScopeGrantValidationError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CreateScopeGrantRequest


async def _seed_world(repo: SQLAlchemyRepository) -> dict[str, str]:
    """Insert a wallet, two operators, an underlying, and two instruments.

    Returns a dict of public_ids the test can pull from.
    """
    base_ts = datetime.now(UTC) - timedelta(minutes=5)
    async with repo.session() as s:
        wallet = Wallet(
            label="alice-personal-kraken",
            description=None,
            is_paper=False,
            session_id="test-session",
            sequence_id=1,
            timestamp=base_ts,
        )
        op_alice = Operator(
            label="alice",
            description="alice the trader",
            session_id="test-session",
            sequence_id=2,
            timestamp=base_ts,
        )
        op_bob = Operator(
            label="bob",
            description="bob the trader",
            session_id="test-session",
            sequence_id=3,
            timestamp=base_ts,
        )
        underlying = UnderlyingAsset(
            ticker="BTC",
            name="Bitcoin",
            asset_class="crypto",
            session_id="test-session",
            sequence_id=4,
            timestamp=base_ts,
        )
        s.add_all([wallet, op_alice, op_bob, underlying])
        await s.commit()
        await s.refresh(wallet)
        await s.refresh(op_alice)
        await s.refresh(op_bob)
        await s.refresh(underlying)

        btc_perp = "00000000-0000-7000-8000-0000000000aa"
        btc_spot = "00000000-0000-7000-8000-0000000000bb"
        s.add_all(
            [
                InstrumentUnderlyingMapping(
                    instrument_public_id=btc_perp,
                    underlying_public_id=underlying.public_id,
                    relationship_type="derivative",
                    contract_family=None,
                    session_id="test-session",
                    sequence_id=5,
                    timestamp=base_ts,
                ),
                InstrumentUnderlyingMapping(
                    instrument_public_id=btc_spot,
                    underlying_public_id=underlying.public_id,
                    relationship_type="exact",
                    contract_family=None,
                    session_id="test-session",
                    sequence_id=6,
                    timestamp=base_ts,
                ),
            ]
        )
        await s.commit()

        return {
            "wallet": wallet.public_id,
            "alice": op_alice.public_id,
            "bob": op_bob.public_id,
            "user_admin": "00000000-0000-7000-8000-000000000099",
            "underlying_btc": underlying.public_id,
            "btc_perp": btc_perp,
            "btc_spot": btc_spot,
        }


def _make_request(
    *,
    operator_public_id: str,
    wallet_public_id: str,
    granted_by: str,
    scope_kind: str,
    underlying_public_id: str | None = None,
    instrument_public_id: str | None = None,
    sequence_id: int = 100,
    note: str | None = None,
) -> CreateScopeGrantRequest:
    return CreateScopeGrantRequest(
        operator_public_id=operator_public_id,
        wallet_public_id=wallet_public_id,
        granted_by_user_public_id=granted_by,
        scope_kind=scope_kind,
        underlying_public_id=underlying_public_id,
        instrument_public_id=instrument_public_id,
        note=note,
        session_id="test-session",
        sequence_id=sequence_id,
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
async def repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Disposable on-disk SQLite repository with the full schema."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/scope_grants.db")
    await r.create_all()
    return r


class TestCreateScopeGrant:
    """Tests for ``SQLAlchemyRepository.create_scope_grant``."""

    @pytest.mark.asyncio
    async def test_underlying_scoped_grant_succeeds(self, repo: SQLAlchemyRepository) -> None:
        """A first underlying-scoped grant inserts cleanly.

        Given: An empty wallet with no active grants,
        When: An underlying-scoped grant is created for alice on BTC,
        Then: The repository returns the new grant row populated with
            the requested fields and the SCD2 active sentinel.
        """
        ids = await _seed_world(repo)
        row = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=ids["underlying_btc"],
                note="alice trades all BTC instruments",
            )
        )
        assert row["operator_public_id"] == ids["alice"]
        assert row["scope_kind"] == "underlying"
        assert row["underlying_public_id"] == ids["underlying_btc"]
        assert row["instrument_public_id"] is None
        assert row["note"] == "alice trades all BTC instruments"

    @pytest.mark.asyncio
    async def test_instrument_scoped_grant_succeeds(self, repo: SQLAlchemyRepository) -> None:
        """A first instrument-scoped carve-out grant inserts cleanly.

        Given: An empty wallet,
        When: An instrument-scoped grant for bob on BTC-PERP is created,
        Then: The new row carries instrument_public_id and a NULL
            underlying_public_id (XOR honored).
        """
        ids = await _seed_world(repo)
        row = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["bob"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="instrument",
                instrument_public_id=ids["btc_perp"],
            )
        )
        assert row["scope_kind"] == "instrument"
        assert row["instrument_public_id"] == ids["btc_perp"]
        assert row["underlying_public_id"] is None

    @pytest.mark.asyncio
    async def test_same_scope_underlying_duplicate_409(self, repo: SQLAlchemyRepository) -> None:
        """Two underlying grants on the same wallet/underlying conflict.

        Given: An existing alice grant on BTC,
        When: A second grant attempts the same underlying for bob,
        Then: ScopeGrantConflictError is raised pointing at alice's grant.
        """
        ids = await _seed_world(repo)
        first = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=ids["underlying_btc"],
            )
        )
        with pytest.raises(ScopeGrantConflictError) as excinfo:
            await repo.create_scope_grant(
                _make_request(
                    operator_public_id=ids["bob"],
                    wallet_public_id=ids["wallet"],
                    granted_by=ids["user_admin"],
                    scope_kind="underlying",
                    underlying_public_id=ids["underlying_btc"],
                    sequence_id=200,
                )
            )
        assert excinfo.value.conflicting_grant_public_id == first["public_id"]
        assert excinfo.value.conflicting_operator_public_id == ids["alice"]

    @pytest.mark.asyncio
    async def test_cross_scope_underlying_then_instrument_409(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An underlying grant blocks a later instrument grant on a child instrument.

        Given: alice holds an active underlying grant on BTC,
        When: bob attempts an instrument-scoped grant on BTC-PERP,
        Then: ScopeGrantConflictError is raised — the cross-scope overlap
            check expands alice's underlying scope to its instrument set
            and detects the BTC-PERP intersection.
        """
        ids = await _seed_world(repo)
        await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=ids["underlying_btc"],
            )
        )
        with pytest.raises(ScopeGrantConflictError):
            await repo.create_scope_grant(
                _make_request(
                    operator_public_id=ids["bob"],
                    wallet_public_id=ids["wallet"],
                    granted_by=ids["user_admin"],
                    scope_kind="instrument",
                    instrument_public_id=ids["btc_perp"],
                    sequence_id=200,
                )
            )

    @pytest.mark.asyncio
    async def test_cross_scope_instrument_then_underlying_409(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An instrument grant blocks a later parent-underlying grant.

        Given: bob holds an instrument-scoped grant on BTC-PERP,
        When: alice attempts an underlying grant on BTC,
        Then: ScopeGrantConflictError is raised — alice's expanded set
            includes BTC-PERP and intersects bob's existing scope.
        """
        ids = await _seed_world(repo)
        await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["bob"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="instrument",
                instrument_public_id=ids["btc_perp"],
            )
        )
        with pytest.raises(ScopeGrantConflictError):
            await repo.create_scope_grant(
                _make_request(
                    operator_public_id=ids["alice"],
                    wallet_public_id=ids["wallet"],
                    granted_by=ids["user_admin"],
                    scope_kind="underlying",
                    underlying_public_id=ids["underlying_btc"],
                    sequence_id=200,
                )
            )

    @pytest.mark.asyncio
    async def test_disjoint_instrument_grants_coexist(self, repo: SQLAlchemyRepository) -> None:
        """Two instrument grants on different instruments do not conflict.

        Given: alice holds an instrument grant on BTC-PERP,
        When: bob attempts an instrument grant on BTC-SPOT (sibling under
            the same underlying but a different instrument),
        Then: Both grants coexist — instrument-exclusive holds at the
            instrument level, not at the underlying level when neither
            grant is underlying-scoped.
        """
        ids = await _seed_world(repo)
        await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="instrument",
                instrument_public_id=ids["btc_perp"],
            )
        )
        await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["bob"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="instrument",
                instrument_public_id=ids["btc_spot"],
                sequence_id=200,
            )
        )
        active = await repo.list_active_scope_grants_for_wallet(ids["wallet"], datetime.now(UTC))
        assert len(active) == 2

    @pytest.mark.asyncio
    async def test_xor_violation_raises_validation_error(self, repo: SQLAlchemyRepository) -> None:
        """Setting both underlying_public_id and instrument_public_id is rejected.

        Given: A request with scope_kind='underlying' and both pid fields set,
        When: create_scope_grant is invoked,
        Then: ScopeGrantValidationError is raised before any DB write.
        """
        ids = await _seed_world(repo)
        with pytest.raises(ScopeGrantValidationError):
            await repo.create_scope_grant(
                _make_request(
                    operator_public_id=ids["alice"],
                    wallet_public_id=ids["wallet"],
                    granted_by=ids["user_admin"],
                    scope_kind="underlying",
                    underlying_public_id=ids["underlying_btc"],
                    instrument_public_id=ids["btc_perp"],
                )
            )

    @pytest.mark.asyncio
    async def test_unknown_scope_kind_raises_validation_error(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An unknown scope_kind is rejected at validation time.

        Given: A request with scope_kind='wallet' (not in the enum),
        When: create_scope_grant is invoked,
        Then: ScopeGrantValidationError is raised.
        """
        ids = await _seed_world(repo)
        with pytest.raises(ScopeGrantValidationError):
            await repo.create_scope_grant(
                _make_request(
                    operator_public_id=ids["alice"],
                    wallet_public_id=ids["wallet"],
                    granted_by=ids["user_admin"],
                    scope_kind="wallet",
                )
            )


class TestHandoverGrant:
    """Tests for ``SQLAlchemyRepository.handover_grant``."""

    @pytest.mark.asyncio
    async def test_handover_atomic_close_and_insert(self, repo: SQLAlchemyRepository) -> None:
        """Handover closes the source and inserts a new grant in one transaction.

        Given: alice holds an active underlying grant on BTC,
        When: handover_grant transfers ownership to bob,
        Then: The returned tuple contains the closed alice row (known_to set
            to the handover timestamp) and the new bob row (known_to=MAX),
            both share the same scope_kind/underlying_public_id, and
            list_active_scope_grants_for_wallet returns exactly one row
            for bob.
        """
        ids = await _seed_world(repo)
        original = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=ids["underlying_btc"],
            )
        )
        closed, new = await repo.handover_grant(
            from_grant_public_id=original["public_id"],
            to_operator_public_id=ids["bob"],
            granted_by_user_public_id=ids["user_admin"],
            reason="vacation cover",
            session_id="test-session",
            sequence_id=300,
            timestamp=datetime.now(UTC),
        )
        assert closed["operator_public_id"] == ids["alice"]
        assert closed["known_to"] != new["known_to"]
        assert new["operator_public_id"] == ids["bob"]
        assert new["scope_kind"] == "underlying"
        assert new["underlying_public_id"] == ids["underlying_btc"]
        assert new["note"] == "vacation cover"

        active = await repo.list_active_scope_grants_for_wallet(ids["wallet"], datetime.now(UTC))
        assert len(active) == 1
        assert active[0]["operator_public_id"] == ids["bob"]

    @pytest.mark.asyncio
    async def test_handover_self_handover_400(self, repo: SQLAlchemyRepository) -> None:
        """Self-handover (same operator) is rejected as a no-op.

        Given: An active grant held by alice,
        When: handover_grant targets alice as the destination,
        Then: ScopeGrantValidationError is raised.
        """
        ids = await _seed_world(repo)
        original = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=ids["underlying_btc"],
            )
        )
        with pytest.raises(ScopeGrantValidationError):
            await repo.handover_grant(
                from_grant_public_id=original["public_id"],
                to_operator_public_id=ids["alice"],
                granted_by_user_public_id=ids["user_admin"],
                reason=None,
                session_id="test-session",
                sequence_id=301,
                timestamp=datetime.now(UTC),
            )

    @pytest.mark.asyncio
    async def test_handover_missing_source_404(self, repo: SQLAlchemyRepository) -> None:
        """Handover with a non-existent source grant raises NotFound.

        Given: An empty wallet (no grants),
        When: handover_grant references a fabricated source public_id,
        Then: ScopeGrantNotFoundError is raised.
        """
        ids = await _seed_world(repo)
        with pytest.raises(ScopeGrantNotFoundError):
            await repo.handover_grant(
                from_grant_public_id="00000000-0000-7000-8000-0000000000ff",
                to_operator_public_id=ids["bob"],
                granted_by_user_public_id=ids["user_admin"],
                reason=None,
                session_id="test-session",
                sequence_id=302,
                timestamp=datetime.now(UTC),
            )

    @pytest.mark.asyncio
    async def test_handover_missing_destination_404(self, repo: SQLAlchemyRepository) -> None:
        """Handover to a non-existent operator raises NotFound.

        Given: An active grant held by alice,
        When: handover_grant targets a fabricated operator public_id,
        Then: ScopeGrantNotFoundError is raised after the source check
            but before the SCD2 close.
        """
        ids = await _seed_world(repo)
        original = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=ids["underlying_btc"],
            )
        )
        with pytest.raises(ScopeGrantNotFoundError):
            await repo.handover_grant(
                from_grant_public_id=original["public_id"],
                to_operator_public_id="00000000-0000-7000-8000-0000000000fe",
                granted_by_user_public_id=ids["user_admin"],
                reason=None,
                session_id="test-session",
                sequence_id=303,
                timestamp=datetime.now(UTC),
            )


class TestAdvisoryLockDialects:
    """Tests for ``_acquire_wallet_advisory_lock`` dialect dispatch."""

    @pytest.mark.asyncio
    async def test_postgresql_dialect_calls_pg_advisory_xact_lock(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """On PostgreSQL the advisory lock executes a transaction-scoped SQL.

        Given: A repository whose dialect_name is patched to ``"postgresql"``,
        When: ``_acquire_wallet_advisory_lock`` is invoked with a wallet id,
        Then: The session's ``execute`` is called exactly once with a text
            statement containing ``pg_advisory_xact_lock`` and the wallet id
            as the bound parameter.
        """
        mock_session = AsyncMock()
        with patch.object(type(repo), "dialect_name", new_callable=lambda: "postgresql"):
            await repo._acquire_wallet_advisory_lock(
                mock_session, "00000000-0000-7000-8000-0000000000aa"
            )
        assert mock_session.execute.await_count == 1
        call_args = mock_session.execute.await_args
        assert "pg_advisory_xact_lock" in str(call_args[0][0])
        assert call_args[0][1] == {"wid": "00000000-0000-7000-8000-0000000000aa"}

    @pytest.mark.asyncio
    async def test_unsupported_dialect_raises_not_implemented(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Unknown dialects raise NotImplementedError eagerly.

        Given: A repository whose dialect_name is patched to ``"mysql"``,
        When: ``_acquire_wallet_advisory_lock`` is invoked,
        Then: ``NotImplementedError`` is raised and no SQL is executed.
        """
        mock_session = AsyncMock()
        with (
            patch.object(type(repo), "dialect_name", new_callable=lambda: "mysql"),
            pytest.raises(NotImplementedError, match="mysql"),
        ):
            await repo._acquire_wallet_advisory_lock(mock_session, "wallet-id")
        mock_session.execute.assert_not_called()


class TestValidateScopeXor:
    """Edge cases for the XOR invariant checker."""

    @pytest.mark.asyncio
    async def test_instrument_kind_with_underlying_raises(self, repo: SQLAlchemyRepository) -> None:
        """scope_kind='instrument' with underlying_public_id set is rejected.

        Given: A request declaring instrument scope but also carrying a
            non-NULL underlying_public_id,
        When: create_scope_grant is invoked,
        Then: ScopeGrantValidationError is raised from the XOR checker.
        """
        ids = await _seed_world(repo)
        with pytest.raises(ScopeGrantValidationError):
            await repo.create_scope_grant(
                _make_request(
                    operator_public_id=ids["alice"],
                    wallet_public_id=ids["wallet"],
                    granted_by=ids["user_admin"],
                    scope_kind="instrument",
                    underlying_public_id=ids["underlying_btc"],
                    instrument_public_id=ids["btc_perp"],
                )
            )

    @pytest.mark.asyncio
    async def test_instrument_kind_missing_instrument_raises(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """scope_kind='instrument' without an instrument_public_id is rejected.

        Given: A request declaring instrument scope but leaving both scope
            pointers NULL,
        When: create_scope_grant is invoked,
        Then: ScopeGrantValidationError is raised from the XOR checker.
        """
        ids = await _seed_world(repo)
        with pytest.raises(ScopeGrantValidationError):
            await repo.create_scope_grant(
                _make_request(
                    operator_public_id=ids["alice"],
                    wallet_public_id=ids["wallet"],
                    granted_by=ids["user_admin"],
                    scope_kind="instrument",
                )
            )


class TestHandoverIntegrityError:
    """Cover the concurrent-handover race path inside ``handover_grant``."""

    @pytest.mark.asyncio
    async def test_integrity_error_on_commit_maps_to_conflict(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A partial-unique-index fire during commit is surfaced as a conflict.

        Given: alice holds an active grant and handover begins toward bob,
        When: ``s.commit()`` inside ``handover_grant`` raises IntegrityError
            (simulating a concurrent racing handover that inserted first),
        Then: ``handover_grant`` re-raises ``ScopeGrantConflictError`` with
            the target operator populated as the conflicting identity.
        """
        ids = await _seed_world(repo)
        original = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=ids["underlying_btc"],
            )
        )

        real_session = repo.session

        @asynccontextmanager
        async def failing_session() -> Any:
            async with real_session() as s:

                async def boom() -> None:
                    raise IntegrityError("stmt", {}, RuntimeError("unique violation"))

                object.__setattr__(s, "commit", boom)
                yield s

        with (
            patch.object(repo, "session", failing_session),
            pytest.raises(ScopeGrantConflictError) as excinfo,
        ):
            await repo.handover_grant(
                from_grant_public_id=original["public_id"],
                to_operator_public_id=ids["bob"],
                granted_by_user_public_id=ids["user_admin"],
                reason="concurrent race",
                session_id="test-session",
                sequence_id=900,
                timestamp=datetime.now(UTC),
            )
        assert excinfo.value.conflicting_operator_public_id == ids["bob"]


class TestListActiveScopeGrants:
    """Tests for ``SQLAlchemyRepository.list_active_scope_grants_for_wallet``."""

    @pytest.mark.asyncio
    async def test_empty_wallet_returns_empty_list(self, repo: SQLAlchemyRepository) -> None:
        """An untouched wallet has no active grants.

        Given: A seeded wallet with no grants ever created,
        When: list_active_scope_grants_for_wallet is invoked,
        Then: An empty list is returned.
        """
        ids = await _seed_world(repo)
        active = await repo.list_active_scope_grants_for_wallet(ids["wallet"], datetime.now(UTC))
        assert active == []
