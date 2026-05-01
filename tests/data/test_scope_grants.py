"""Repository-level tests for wallet_operator_scope_grants methods.

Covers the multi-tenant foundation: the
``create_scope_grant`` / ``handover_grant`` methods on
``SQLAlchemyRepository`` and the cross-scope overlap detection that
enforces the instrument-exclusive rule (D2).

These tests run against a real on-disk SQLite database (no mocks) so
the partial unique indexes, advisory-lock no-op, and SCD2 close+insert
flow are exercised end-to-end.
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
from sqlalchemy import update as _update
from sqlalchemy.exc import IntegrityError

from snapper.data.models import Instrument
from snapper.data.models import InstrumentUnderlyingMapping
from snapper.data.models import Operator
from snapper.data.models import Symbol
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import UnderlyingAsset
from snapper.data.models import UserOperatorMembership
from snapper.data.models import Wallet
from snapper.data.models import WalletOperatorScopeGrant
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
    async def test_same_underlying_duplicate_with_no_mappings_409(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Duplicate underlying grants conflict even when mappings are empty.

        Regression for a Codex phase-close finding: when an underlying has
        zero active ``instrument_underlying_mappings`` rows, ``_expand_to_instruments``
        returns an empty set on both sides and the set-intersection overlap
        check would fall through to the DB partial unique index,
        surfacing a raw IntegrityError to the caller instead of a clean
        ScopeGrantConflictError. The same-scope identity fast-path in
        ``_find_overlap`` now catches this.

        Given: A freshly-created underlying with no instrument mappings,
            and an existing alice grant on that underlying,
        When: bob attempts a duplicate underlying-scoped grant on the
            same underlying,
        Then: ScopeGrantConflictError is raised pointing at alice's grant
            rather than a raw IntegrityError.
        """
        ids = await _seed_world(repo)
        lonely_underlying_public_id: str = ""
        async with repo.session() as s:
            lonely = UnderlyingAsset(
                ticker="EMPTY",
                name="Empty Underlying",
                asset_class="crypto",
                session_id="test-session",
                sequence_id=99,
                timestamp=datetime.now(UTC) - timedelta(minutes=1),
            )
            s.add(lonely)
            await s.commit()
            await s.refresh(lonely)
            lonely_underlying_public_id = lonely.public_id

        first = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=lonely_underlying_public_id,
            )
        )
        with pytest.raises(ScopeGrantConflictError) as excinfo:
            await repo.create_scope_grant(
                _make_request(
                    operator_public_id=ids["bob"],
                    wallet_public_id=ids["wallet"],
                    granted_by=ids["user_admin"],
                    scope_kind="underlying",
                    underlying_public_id=lonely_underlying_public_id,
                    sequence_id=250,
                )
            )
        assert excinfo.value.conflicting_grant_public_id == first["public_id"]

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
        with pytest.raises(ScopeGrantNotFoundError, match="destination operator"):
            await repo.handover_grant(
                from_grant_public_id=original["public_id"],
                to_operator_public_id="00000000-0000-7000-8000-0000000000fe",
                granted_by_user_public_id=ids["user_admin"],
                reason=None,
                session_id="test-session",
                sequence_id=303,
                timestamp=datetime.now(UTC) + timedelta(minutes=1),
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
                    raise IntegrityError("stmt", {}, RuntimeError("UNIQUE constraint violated"))

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


class TestCreateScopeGrantIntegrityError:
    """Cover the concurrent-race safety net in ``create_scope_grant``.

    The repository-layer overlap check is the primary line of defense, but
    a racing concurrent insert could in theory slip past it (e.g., two
    coroutines on SQLite where the advisory lock is a no-op). The
    ``IntegrityError`` translation around ``s.commit()`` maps a fired
    partial unique index to ``ScopeGrantConflictError`` so the API layer
    still returns 409 rather than leaking the DBAPI error.
    """

    @pytest.mark.asyncio
    async def test_unique_integrity_error_maps_to_conflict(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A unique IntegrityError on commit maps to ScopeGrantConflictError.

        Given: A first grant succeeds via the repository,
        When: A second ``create_scope_grant`` invocation has its
            ``s.commit()`` patched to raise ``IntegrityError`` with a
            ``UNIQUE`` message,
        Then: ``create_scope_grant`` re-raises ``ScopeGrantConflictError``
            with the same wallet_public_id.
        """
        ids = await _seed_world(repo)
        real_session = repo.session

        @asynccontextmanager
        async def failing_session() -> Any:
            async with real_session() as s:

                async def boom() -> None:
                    raise IntegrityError(
                        "stmt", {}, RuntimeError("UNIQUE constraint failed: grants")
                    )

                object.__setattr__(s, "commit", boom)
                yield s

        with (
            patch.object(repo, "session", failing_session),
            pytest.raises(ScopeGrantConflictError) as excinfo,
        ):
            await repo.create_scope_grant(
                _make_request(
                    operator_public_id=ids["alice"],
                    wallet_public_id=ids["wallet"],
                    granted_by=ids["user_admin"],
                    scope_kind="instrument",
                    instrument_public_id=ids["btc_perp"],
                )
            )
        assert excinfo.value.wallet_public_id == ids["wallet"]

    @pytest.mark.asyncio
    async def test_non_unique_integrity_error_reraises(self, repo: SQLAlchemyRepository) -> None:
        """A non-unique IntegrityError on commit is re-raised untouched.

        Given: ``create_scope_grant`` encounters a NOT NULL violation at
            commit time (simulated),
        When: The integrity error's orig message does not contain
            ``unique`` or ``duplicate``,
        Then: ``create_scope_grant`` re-raises the original IntegrityError
            so the real bug surfaces with its traceback.
        """
        ids = await _seed_world(repo)
        real_session = repo.session

        @asynccontextmanager
        async def failing_session() -> Any:
            async with real_session() as s:

                async def boom() -> None:
                    raise IntegrityError("stmt", {}, RuntimeError("NOT NULL constraint failed"))

                object.__setattr__(s, "commit", boom)
                yield s

        with (
            patch.object(repo, "session", failing_session),
            pytest.raises(IntegrityError, match="NOT NULL"),
        ):
            await repo.create_scope_grant(
                _make_request(
                    operator_public_id=ids["alice"],
                    wallet_public_id=ids["wallet"],
                    granted_by=ids["user_admin"],
                    scope_kind="instrument",
                    instrument_public_id=ids["btc_perp"],
                )
            )


class TestHandoverIntegrityReraise:
    """Non-unique IntegrityError from commit is not masked as a conflict."""

    @pytest.mark.asyncio
    async def test_non_unique_integrity_error_reraises(self, repo: SQLAlchemyRepository) -> None:
        """A non-unique IntegrityError (e.g., NOT NULL/FK violation) re-raises.

        Given: alice holds an active grant and handover toward bob begins,
        When: ``s.commit()`` raises IntegrityError whose orig message does
            NOT match ``unique`` or ``duplicate`` (simulating a future
            NOT NULL or FK violation),
        Then: ``handover_grant`` re-raises the original IntegrityError so
            the real bug surfaces with its traceback intact — it is NOT
            silently translated to ``ScopeGrantConflictError``.
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
                    raise IntegrityError("stmt", {}, RuntimeError("NOT NULL constraint failed"))

                object.__setattr__(s, "commit", boom)
                yield s

        with (
            patch.object(repo, "session", failing_session),
            pytest.raises(IntegrityError, match="NOT NULL"),
        ):
            await repo.handover_grant(
                from_grant_public_id=original["public_id"],
                to_operator_public_id=ids["bob"],
                granted_by_user_public_id=ids["user_admin"],
                reason="non-unique integrity error test",
                session_id="test-session",
                sequence_id=901,
                timestamp=datetime.now(UTC),
            )


class TestListActiveOperators:
    """Tests for ``SQLAlchemyRepository.list_active_operators``."""

    @pytest.mark.asyncio
    async def test_returns_active_operators_ordered_by_label(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Active operators are returned ordered by label ascending.

        Given: Three operators inserted in label-out-of-order sequence,
        When: ``list_active_operators`` is called,
        Then: The result is ordered by label and includes all three.
        """
        ids = await _seed_world(repo)
        assert "alice" in ids and "bob" in ids
        operators = await repo.list_active_operators(datetime.now(UTC))
        labels = [op["label"] for op in operators]
        assert labels == sorted(labels)
        assert "alice" in labels
        assert "bob" in labels

    @pytest.mark.asyncio
    async def test_empty_when_no_operators(self, repo: SQLAlchemyRepository) -> None:
        """An empty operators table returns an empty list.

        Given: A fresh repository with no operator inserts,
        When: ``list_active_operators`` is called,
        Then: An empty list is returned.
        """
        operators = await repo.list_active_operators(datetime.now(UTC))
        assert operators == []


class TestGetUserOperatorMemberships:
    """Tests for ``SQLAlchemyRepository.get_user_operator_memberships``."""

    @pytest.mark.asyncio
    async def test_returns_membership_with_primary_flag(self, repo: SQLAlchemyRepository) -> None:
        """A primary membership flag round-trips through the repository.

        Given: A user_operator_memberships row with is_primary=True,
        When: ``get_user_operator_memberships`` is called for that user,
        Then: The returned row carries ``is_primary=True``.
        """
        ids = await _seed_world(repo)
        user_pid = "00000000-0000-7000-8000-000000000010"
        async with repo.session() as s:

            s.add(
                UserOperatorMembership(
                    user_public_id=user_pid,
                    operator_public_id=ids["alice"],
                    is_primary=True,
                    session_id="test-session",
                    sequence_id=500,
                    timestamp=datetime.now(UTC),
                )
            )
            await s.commit()

        memberships = await repo.get_user_operator_memberships(user_pid, datetime.now(UTC))
        assert len(memberships) == 1
        assert memberships[0]["operator_public_id"] == ids["alice"]
        assert memberships[0]["is_primary"] is True

    @pytest.mark.asyncio
    async def test_unknown_user_returns_empty(self, repo: SQLAlchemyRepository) -> None:
        """An unknown user has no memberships.

        Given: A repository with no membership rows for the queried user,
        When: ``get_user_operator_memberships`` is called,
        Then: An empty list is returned.
        """
        memberships = await repo.get_user_operator_memberships(
            "00000000-0000-7000-8000-00000000ffff", datetime.now(UTC)
        )
        assert memberships == []


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


class TestListGrantCoveredInstrumentPublicIds:
    """Tests for the new coverage helper."""

    @pytest.mark.asyncio
    async def test_underlying_grant_expands_to_all_mapped_instruments(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An underlying-scoped grant covers every mapped instrument."""
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
        covered = await repo.list_grant_covered_instrument_public_ids(
            operator_public_id=ids["alice"],
            wallet_public_id=ids["wallet"],
            as_of=datetime.now(UTC),
        )
        assert covered == {ids["btc_perp"], ids["btc_spot"]}

    @pytest.mark.asyncio
    async def test_instrument_grant_returns_singleton(self, repo: SQLAlchemyRepository) -> None:
        """An instrument-scoped grant covers only that instrument."""
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
        covered = await repo.list_grant_covered_instrument_public_ids(
            operator_public_id=ids["alice"],
            wallet_public_id=ids["wallet"],
            as_of=datetime.now(UTC),
        )
        assert covered == {ids["btc_perp"]}

    @pytest.mark.asyncio
    async def test_other_operator_grants_are_filtered_out(self, repo: SQLAlchemyRepository) -> None:
        """Only the requested operator's grants contribute to the covered set."""
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
        covered = await repo.list_grant_covered_instrument_public_ids(
            operator_public_id=ids["alice"],
            wallet_public_id=ids["wallet"],
            as_of=datetime.now(UTC),
        )
        assert covered == set()


class TestGetInstrumentPublicIdBySymbol:
    """Tests for the new symbol-to-instrument resolver."""

    @pytest.mark.asyncio
    async def test_returns_none_when_symbol_unknown(self, repo: SQLAlchemyRepository) -> None:
        """Unknown symbol on a known exchange returns None."""
        result = await repo.get_instrument_public_id_by_symbol(
            native_symbol="ZZZ-USD",
            exchange="kraken",
            as_of=datetime.now(UTC),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_public_id_for_known_pair(self, repo: SQLAlchemyRepository) -> None:
        """A symbol present on the requested exchange resolves to its instrument."""
        async with repo.session() as s:
            s.add(
                Symbol(
                    public_id="00000000-0000-7000-8000-0000000000c1",
                    native_symbol="BTC-USD",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=datetime.now(UTC),
                    session_id="t",
                    sequence_id=1,
                    timestamp=datetime.now(UTC),
                )
            )
            await s.commit()
        await repo.ensure_instrument(
            symbol_public_id="00000000-0000-7000-8000-0000000000c1",
            exchange="kraken",
            session_id="t",
            sequence_id=2,
            timestamp=datetime.now(UTC),
        )
        result = await repo.get_instrument_public_id_by_symbol(
            native_symbol="BTC-USD",
            exchange="kraken",
            as_of=datetime.now(UTC),
        )
        assert result is not None


class TestGetSymbolForInstrument:
    """Tests for the new instrument-to-symbol reverse resolver."""

    @pytest.mark.asyncio
    async def test_returns_none_when_instrument_unknown(self, repo: SQLAlchemyRepository) -> None:
        """Unknown instrument public_id returns None.

        Given: a UUID that has no active Instrument row,
        When: ``get_symbol_for_instrument`` is called,
        Then: None is returned so the capability guard can raise
            ``unknown_instrument`` rather than skipping the check.
        """
        result = await repo.get_symbol_for_instrument(
            instrument_public_id="00000000-0000-7000-8000-00000000dead",
            as_of=datetime.now(UTC),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_native_symbol_for_active_pair(self, repo: SQLAlchemyRepository) -> None:
        """Active Instrument + active Symbol resolves to native symbol.

        Given: a seeded Symbol and an Instrument referencing it,
        When: ``get_symbol_for_instrument`` is called with the
            instrument public_id at the current snapshot,
        Then: the Symbol's ``native_symbol`` string is returned.
        """
        async with repo.session() as s:
            s.add(
                Symbol(
                    public_id="00000000-0000-7000-8000-0000000000c2",
                    native_symbol="MNQM6-CME",
                    base="MNQ",
                    quote="USD",
                    asset_type="index",
                    created_at=datetime.now(UTC),
                    session_id="t",
                    sequence_id=1,
                    timestamp=datetime.now(UTC),
                )
            )
            await s.commit()
        _id, instrument_public_id = await repo.ensure_instrument(
            symbol_public_id="00000000-0000-7000-8000-0000000000c2",
            exchange="kraken_equities",
            session_id="t",
            sequence_id=2,
            timestamp=datetime.now(UTC),
        )
        result = await repo.get_symbol_for_instrument(
            instrument_public_id=instrument_public_id,
            as_of=datetime.now(UTC),
        )
        assert result == "MNQM6-CME"


class TestGetExchangeInstrumentsDetail:
    """Tests for the capability-aware instrument-detail repo method."""

    @pytest.mark.asyncio
    async def test_returns_empty_list_for_exchange_without_capabilities(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Exchange with no capability rows returns an empty list.

        Given: no ``SymbolExchangeCapability`` rows for the exchange,
        When: ``get_exchange_instruments_detail`` is called,
        Then: an empty list is returned (the front end renders no badges).
        """
        rows = await repo.get_exchange_instruments_detail(
            exchange="no_such_exchange",
            as_of=datetime.now(UTC),
        )
        assert rows == []

    @pytest.mark.asyncio
    async def test_returns_capability_rows_joined_with_symbol(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Active Symbol+Capability rows round-trip through the repo method.

        Given: a seeded Symbol and a ``SymbolExchangeCapability`` row
            (``can_trade=False``, ``can_market_data=True``) for the
            ``kraken_equities`` exchange,
        When: ``get_exchange_instruments_detail`` is called,
        Then: exactly one row is returned with ``symbol='MNQM6-CME'``,
            ``can_trade=False``, and the Instrument+Spec joins degrade
            gracefully to the symbol UUID when no Instrument row exists.
        """
        as_of = datetime.now(UTC)
        async with repo.session() as s:
            s.add(
                Symbol(
                    public_id="00000000-0000-7000-8000-0000000000d1",
                    native_symbol="MNQM6-CME",
                    base="MNQ",
                    quote="USD",
                    asset_type="index",
                    created_at=as_of,
                    session_id="t",
                    sequence_id=1,
                    timestamp=as_of,
                )
            )
            s.add(
                SymbolExchangeCapability(
                    public_id="00000000-0000-7000-8000-0000000000d2",
                    symbol_public_id="00000000-0000-7000-8000-0000000000d1",
                    exchange="kraken_equities",
                    can_market_data=True,
                    can_trade=False,
                    source="test",
                    reason=None,
                    created_at=as_of,
                    session_id="t",
                    sequence_id=2,
                    timestamp=as_of,
                )
            )
            await s.commit()
        rows = await repo.get_exchange_instruments_detail(
            exchange="kraken_equities",
            as_of=as_of,
        )
        assert len(rows) == 1
        row = rows[0]
        assert row["symbol"] == "MNQM6-CME"
        assert row["can_trade"] is False
        assert row["can_market_data"] is True
        assert row["instrument_public_id"] == "00000000-0000-7000-8000-0000000000d1"
        assert row["instrument_resolved"] is False
        assert row["instrument_kind"] is None

    @pytest.mark.asyncio
    async def test_joins_instrument_when_present(self, repo: SQLAlchemyRepository) -> None:
        """When an Instrument row exists, its public_id replaces the symbol_public_id.

        Given: a seeded Symbol + Capability + Instrument triple,
        When: ``get_exchange_instruments_detail`` is called,
        Then: the returned row's ``instrument_public_id`` matches the
            Instrument row's ``public_id`` (not the Symbol's).
        """
        as_of = datetime.now(UTC)
        async with repo.session() as s:
            s.add(
                Symbol(
                    public_id="00000000-0000-7000-8000-0000000000e1",
                    native_symbol="MESM6-CME",
                    base="MES",
                    quote="USD",
                    asset_type="index",
                    created_at=as_of,
                    session_id="t",
                    sequence_id=1,
                    timestamp=as_of,
                )
            )
            s.add(
                SymbolExchangeCapability(
                    public_id="00000000-0000-7000-8000-0000000000e2",
                    symbol_public_id="00000000-0000-7000-8000-0000000000e1",
                    exchange="kraken_equities",
                    can_market_data=True,
                    can_trade=False,
                    source="test",
                    reason=None,
                    created_at=as_of,
                    session_id="t",
                    sequence_id=2,
                    timestamp=as_of,
                )
            )
            await s.commit()
        _id, instrument_public_id = await repo.ensure_instrument(
            symbol_public_id="00000000-0000-7000-8000-0000000000e1",
            exchange="kraken_equities",
            session_id="t",
            sequence_id=3,
            timestamp=as_of,
        )
        rows = await repo.get_exchange_instruments_detail(
            exchange="kraken_equities",
            as_of=as_of,
        )
        assert len(rows) == 1
        assert rows[0]["instrument_public_id"] == instrument_public_id
        assert rows[0]["symbol"] == "MESM6-CME"
        assert rows[0]["instrument_resolved"] is True


class TestRevokeScopeGrant:
    """Tests for ``SQLAlchemyRepository.revoke_scope_grant``."""

    @pytest.mark.asyncio
    async def test_revoke_active_grant_closes_row(self, repo: SQLAlchemyRepository) -> None:
        """Revoke closes an active grant in place (no new row inserted).

        Given: alice holds an active instrument grant on BTC-USD,
        When: revoke_scope_grant is called at t_revoke,
        Then: returned projection has known_to == t_revoke and
            list_active_scope_grants_for_wallet drops alice's row.
        """
        ids = await _seed_world(repo)
        original = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="instrument",
                instrument_public_id=ids["btc_perp"],
            )
        )
        t_revoke = datetime.now(UTC)
        closed = await repo.revoke_scope_grant(
            grant_public_id=original["public_id"],
            revoked_by_user_public_id=ids["user_admin"],
            revoked_at=t_revoke,
            reason="alice left the team",
        )
        assert closed["public_id"] == original["public_id"]
        assert closed["operator_public_id"] == ids["alice"]
        assert closed["known_to"] == t_revoke
        assert closed["scope_kind"] == "instrument"
        assert closed["instrument_public_id"] == ids["btc_perp"]

        active = await repo.list_active_scope_grants_for_wallet(ids["wallet"], datetime.now(UTC))
        assert active == []

    @pytest.mark.asyncio
    async def test_revoke_already_closed_grant_raises_not_found(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Double-revoke on the same grant raises ScopeGrantNotFoundError.

        The active-row predicate in the SELECT excludes the already-closed
        row, so the second revoke sees nothing to close.
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
        await repo.revoke_scope_grant(
            grant_public_id=original["public_id"],
            revoked_by_user_public_id=ids["user_admin"],
            revoked_at=datetime.now(UTC),
            reason=None,
        )
        with pytest.raises(ScopeGrantNotFoundError):
            await repo.revoke_scope_grant(
                grant_public_id=original["public_id"],
                revoked_by_user_public_id=ids["user_admin"],
                revoked_at=datetime.now(UTC) + timedelta(seconds=1),
                reason=None,
            )

    @pytest.mark.asyncio
    async def test_revoke_unknown_public_id_raises_not_found(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Revoke on a fabricated grant public_id raises NotFound."""
        await _seed_world(repo)
        with pytest.raises(ScopeGrantNotFoundError):
            await repo.revoke_scope_grant(
                grant_public_id="00000000-0000-7000-8000-0000000000ff",
                revoked_by_user_public_id="00000000-0000-7000-8000-00000000aaaa",
                revoked_at=datetime.now(UTC),
                reason=None,
            )

    @pytest.mark.asyncio
    async def test_revoke_does_not_persist_reason_or_revoker(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Reason + revoked_by flow to the event payload, NOT to the row.

        The closed row keeps the original granted_by_user_public_id and
        note — audit reconstruction is via the event log, not the row.
        """
        ids = await _seed_world(repo)
        original = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=ids["underlying_btc"],
                note="original note",
            )
        )
        other_user = "00000000-0000-7000-8000-0000000000ee"
        closed = await repo.revoke_scope_grant(
            grant_public_id=original["public_id"],
            revoked_by_user_public_id=other_user,
            revoked_at=datetime.now(UTC),
            reason="REVOKED: alice left",
        )
        assert closed["granted_by_user_public_id"] == ids["user_admin"]
        assert closed["note"] == "original note"

    @pytest.mark.asyncio
    async def test_revoke_acquires_advisory_lock(self, repo: SQLAlchemyRepository) -> None:
        """revoke_scope_grant calls ``_acquire_wallet_advisory_lock`` once per call.

        SQLite path is a no-op; the test patches the helper to count
        invocations so we know the serialization primitive is wired.
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
        with patch.object(repo, "_acquire_wallet_advisory_lock", new=AsyncMock()) as lock_mock:
            await repo.revoke_scope_grant(
                grant_public_id=original["public_id"],
                revoked_by_user_public_id=ids["user_admin"],
                revoked_at=datetime.now(UTC),
                reason=None,
            )
        assert lock_mock.await_count == 1
        assert lock_mock.await_args is not None
        _session, wallet_id = lock_mock.await_args.args
        assert wallet_id == ids["wallet"]

    @pytest.mark.asyncio
    async def test_revoke_loses_race_to_concurrent_close_raises_not_found(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A concurrent close between our SELECT and our UPDATE is detected.

        Simulates the read-before-lock race: a parallel writer closes the
        grant in the window between our initial active-SELECT and the
        post-lock UPDATE. The UPDATE's ``where_active`` predicate sees
        ``known_to`` already in the past, so ``rowcount == 0`` and we
        raise ``ScopeGrantNotFoundError`` instead of silently closing a
        stale row (which would have produced a duplicate
        ``admin.scope_revoked`` event for a grant we did not revoke).
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
        pre_close = datetime.now(UTC)
        real_lock = repo._acquire_wallet_advisory_lock

        async def _close_under_lock(session: Any, wallet_public_id: str) -> None:
            """Acquire the lock then close the target row in the same session.

            Mirrors what a concurrent writer's commit looks like from
            this transaction: the row's ``known_to`` flips to
            ``pre_close`` before the outer revoke runs its UPDATE.
            """
            await real_lock(session, wallet_public_id)
            await session.execute(
                _update(WalletOperatorScopeGrant)
                .where(WalletOperatorScopeGrant.public_id == original["public_id"])
                .values(known_to=pre_close)
            )

        with (
            patch.object(repo, "_acquire_wallet_advisory_lock", side_effect=_close_under_lock),
            pytest.raises(ScopeGrantNotFoundError, match="concurrent mutation"),
        ):
            await repo.revoke_scope_grant(
                grant_public_id=original["public_id"],
                revoked_by_user_public_id=ids["user_admin"],
                revoked_at=pre_close + timedelta(seconds=1),
                reason=None,
            )


async def _seed_instrument_chain(repo: SQLAlchemyRepository, ids: dict[str, str]) -> None:
    """Seed active ``Symbol`` + ``Instrument`` rows for btc_perp and btc_spot.

    ``_seed_world`` only seeds ``InstrumentUnderlyingMapping`` (mapping
    instrument public_ids to an underlying) and does not create the
    actual ``Instrument`` or ``Symbol`` rows those ids point at. The
     wallet-pair projection JOINs against those tables, so the
    tests here extend the seed with the concrete rows.

    Maps:

    - ``btc_perp`` → Symbol "BTC-PERP" / Instrument on "kraken_futures"
    - ``btc_spot`` → Symbol "BTC-USD" / Instrument on "kraken"
    """
    base_ts = datetime.now(UTC) - timedelta(minutes=4)
    symbol_perp = "00000000-0000-7000-8000-0000000000s1"
    symbol_spot = "00000000-0000-7000-8000-0000000000s2"
    async with repo.session() as s:
        s.add_all(
            [
                Symbol(
                    public_id=symbol_perp,
                    native_symbol="BTC-PERP",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=base_ts,
                    session_id="test-session",
                    sequence_id=20,
                    timestamp=base_ts,
                ),
                Symbol(
                    public_id=symbol_spot,
                    native_symbol="BTC-USD",
                    base="BTC",
                    quote="USD",
                    asset_type="crypto",
                    created_at=base_ts,
                    session_id="test-session",
                    sequence_id=21,
                    timestamp=base_ts,
                ),
                Instrument(
                    public_id=ids["btc_perp"],
                    symbol_public_id=symbol_perp,
                    exchange="kraken_futures",
                    session_id="test-session",
                    sequence_id=22,
                    timestamp=base_ts,
                ),
                Instrument(
                    public_id=ids["btc_spot"],
                    symbol_public_id=symbol_spot,
                    exchange="kraken",
                    session_id="test-session",
                    sequence_id=23,
                    timestamp=base_ts,
                ),
            ]
        )
        await s.commit()


class TestListScopeGrantInstrumentPairs:
    """Tests for the projection repository method."""

    @pytest.mark.asyncio
    async def test_empty_operator_list_returns_empty(self, repo: SQLAlchemyRepository) -> None:
        """Fast path: no operators → empty set, no DB round-trip needed."""
        pairs = await repo.list_scope_grant_instrument_pairs([], datetime.now(UTC))
        assert pairs == set()

    @pytest.mark.asyncio
    async def test_operator_without_grants_returns_empty(self, repo: SQLAlchemyRepository) -> None:
        """Existing operator with no grants yields an empty set."""
        ids = await _seed_world(repo)
        await _seed_instrument_chain(repo, ids)
        pairs = await repo.list_scope_grant_instrument_pairs([ids["alice"]], datetime.now(UTC))
        assert pairs == set()

    @pytest.mark.asyncio
    async def test_underlying_scoped_grant_expands_to_all_instrument_pairs(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An underlying-scoped grant on BTC surfaces every mapped instrument.

        Given: alice holds an underlying grant on BTC and the mapping
            table links BTC → {btc_perp, btc_spot} at ``as_of``,
        When: ``list_scope_grant_instrument_pairs([alice], now)`` runs,
        Then: both the ``(kraken_futures, BTC-PERP)`` and
            ``(kraken, BTC-USD)`` pairs are returned.
        """
        ids = await _seed_world(repo)
        await _seed_instrument_chain(repo, ids)
        await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=ids["underlying_btc"],
            )
        )
        pairs = await repo.list_scope_grant_instrument_pairs([ids["alice"]], datetime.now(UTC))
        assert pairs == {("kraken_futures", "BTC-PERP"), ("kraken", "BTC-USD")}

    @pytest.mark.asyncio
    async def test_instrument_scoped_grant_returns_singleton_pair(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An instrument grant projects to exactly one pair."""
        ids = await _seed_world(repo)
        await _seed_instrument_chain(repo, ids)
        await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="instrument",
                instrument_public_id=ids["btc_perp"],
            )
        )
        pairs = await repo.list_scope_grant_instrument_pairs([ids["alice"]], datetime.now(UTC))
        assert pairs == {("kraken_futures", "BTC-PERP")}

    @pytest.mark.asyncio
    async def test_multi_operator_unions_pairs(self, repo: SQLAlchemyRepository) -> None:
        """Two operators holding different grants union to the combined set."""
        ids = await _seed_world(repo)
        await _seed_instrument_chain(repo, ids)
        await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="instrument",
                instrument_public_id=ids["btc_perp"],
                sequence_id=101,
            )
        )
        await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["bob"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="instrument",
                instrument_public_id=ids["btc_spot"],
                sequence_id=102,
            )
        )
        pairs = await repo.list_scope_grant_instrument_pairs(
            [ids["alice"], ids["bob"]], datetime.now(UTC)
        )
        assert pairs == {("kraken_futures", "BTC-PERP"), ("kraken", "BTC-USD")}

    @pytest.mark.asyncio
    async def test_revoked_grant_excluded(self, repo: SQLAlchemyRepository) -> None:
        """A closed grant no longer contributes to the pair set."""
        ids = await _seed_world(repo)
        await _seed_instrument_chain(repo, ids)
        original = await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="instrument",
                instrument_public_id=ids["btc_perp"],
            )
        )
        await repo.revoke_scope_grant(
            grant_public_id=original["public_id"],
            revoked_by_user_public_id=ids["user_admin"],
            revoked_at=datetime.now(UTC),
            reason=None,
        )
        pairs = await repo.list_scope_grant_instrument_pairs(
            [ids["alice"]], datetime.now(UTC) + timedelta(seconds=1)
        )
        assert pairs == set()

    @pytest.mark.asyncio
    async def test_orphan_instrument_without_active_rows_is_skipped(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Grants pointing to instruments with no active Instrument/Symbol are dropped.

        Covers the defensive case where ``instrument_underlying_mappings``
        references an instrument_public_id that has no active
        ``Instrument`` row (e.g. data-quality gap). The projection query
        silently omits orphans instead of raising — the subscribe filter
        will simply not extend allowed pairs to cover them, which is the
        safe fail-closed default.
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
        pairs = await repo.list_scope_grant_instrument_pairs([ids["alice"]], datetime.now(UTC))
        assert pairs == set()

    @pytest.mark.asyncio
    async def test_underlying_with_no_active_mappings_yields_empty_set(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Underlying-scoped grant on an underlying with zero mappings → empty.

        Covers the early-return branch where grants exist (so we don't
        hit the outer ``not grants`` bail-out) but the expansion step
        produces zero instrument ids. Without this guard the code
        would issue a pointless empty-IN() JOIN query.
        """
        ids = await _seed_world(repo)
        base_ts = datetime.now(UTC) - timedelta(minutes=3)
        async with repo.session() as s:
            empty_underlying = UnderlyingAsset(
                ticker="EMPTY",
                name="No Mappings",
                asset_class="crypto",
                session_id="test-session",
                sequence_id=30,
                timestamp=base_ts,
            )
            s.add(empty_underlying)
            await s.commit()
            await s.refresh(empty_underlying)
        await repo.create_scope_grant(
            _make_request(
                operator_public_id=ids["alice"],
                wallet_public_id=ids["wallet"],
                granted_by=ids["user_admin"],
                scope_kind="underlying",
                underlying_public_id=empty_underlying.public_id,
            )
        )
        pairs = await repo.list_scope_grant_instrument_pairs([ids["alice"]], datetime.now(UTC))
        assert pairs == set()
