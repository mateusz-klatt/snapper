"""Repository-level tests for the wallet catalogue helpers.

Covers ``SQLAlchemyRepository.list_active_wallets`` and
``list_accessible_wallets_for_operators``. These back the frontend
wallet picker: ADMIN sees every active wallet, while VIEWER /
OPERATOR see only the wallets covered by an active scope grant from
one of their operators.

The tests run against an on-disk SQLite database with the full
schema materialised via ``create_all``, so the SCD2 active index +
advisory-lock helper + WalletOperatorScopeGrant join are all
exercised end-to-end.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy.exc import IntegrityError

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Operator
from snapper.data.models import Wallet
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository import OperatorConflictError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import WalletConflictError


async def _seed_wallets_and_grants(repo: SQLAlchemyRepository) -> dict[str, str]:
    """Insert two wallets, two operators, and selective scope grants.

    Layout:
    - wallet_paper / wallet_live — two active wallets.
    - op_alice holds an instrument-scoped grant on wallet_paper.
    - op_bob holds an underlying-scoped grant on wallet_live.
    - op_carol has NO active grants (isolation check).
    """
    base_ts = datetime.now(UTC) - timedelta(minutes=5)
    async with repo.session() as s:
        wallet_paper = Wallet(
            label="default",
            description=None,
            is_paper=True,
            session_id="test-session",
            sequence_id=1,
            timestamp=base_ts,
            known_to=KNOWN_TO_MAX,
        )
        wallet_live = Wallet(
            label="default",
            description=None,
            is_paper=False,
            session_id="test-session",
            sequence_id=2,
            timestamp=base_ts,
            known_to=KNOWN_TO_MAX,
        )
        op_alice = Operator(
            label="alice",
            description=None,
            session_id="test-session",
            sequence_id=3,
            timestamp=base_ts,
            known_to=KNOWN_TO_MAX,
        )
        op_bob = Operator(
            label="bob",
            description=None,
            session_id="test-session",
            sequence_id=4,
            timestamp=base_ts,
            known_to=KNOWN_TO_MAX,
        )
        op_carol = Operator(
            label="carol",
            description=None,
            session_id="test-session",
            sequence_id=5,
            timestamp=base_ts,
            known_to=KNOWN_TO_MAX,
        )
        s.add_all([wallet_paper, wallet_live, op_alice, op_bob, op_carol])
        await s.commit()
        await s.refresh(wallet_paper)
        await s.refresh(wallet_live)
        await s.refresh(op_alice)
        await s.refresh(op_bob)
        await s.refresh(op_carol)

        grant_alice = WalletOperatorScopeGrant(
            operator_public_id=op_alice.public_id,
            wallet_public_id=wallet_paper.public_id,
            granted_by_user_public_id="00000000-0000-7000-8000-000000000099",
            scope_kind="instrument",
            underlying_public_id=None,
            instrument_public_id="00000000-0000-7000-8000-0000000000aa",
            note=None,
            session_id="test-session",
            sequence_id=10,
            timestamp=base_ts,
            known_to=KNOWN_TO_MAX,
        )
        grant_bob = WalletOperatorScopeGrant(
            operator_public_id=op_bob.public_id,
            wallet_public_id=wallet_live.public_id,
            granted_by_user_public_id="00000000-0000-7000-8000-000000000099",
            scope_kind="underlying",
            underlying_public_id="00000000-0000-7000-8000-0000000000bb",
            instrument_public_id=None,
            note=None,
            session_id="test-session",
            sequence_id=11,
            timestamp=base_ts,
            known_to=KNOWN_TO_MAX,
        )
        s.add_all([grant_alice, grant_bob])
        await s.commit()

        return {
            "wallet_paper": wallet_paper.public_id,
            "wallet_live": wallet_live.public_id,
            "alice": op_alice.public_id,
            "bob": op_bob.public_id,
            "carol": op_carol.public_id,
        }


@pytest.fixture
async def repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Disposable on-disk SQLite repository with the full schema."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/wallets.db")
    await r.create_all()
    return r


class TestListActiveWallets:
    """Behaviour of ``SQLAlchemyRepository.list_active_wallets``."""

    @pytest.mark.asyncio
    async def test_returns_every_active_wallet_ordered(self, repo: SQLAlchemyRepository) -> None:
        """Every active wallet is returned ordered by ``(is_paper, label)``.

        Given: Two active wallets sharing a label but differing on
            ``is_paper``,
        When: ``list_active_wallets`` is called at ``now``,
        Then: Both rows are returned with the live wallet first
            (because ``is_paper=False`` sorts ASC before
            ``is_paper=True``), matching the deterministic ordering
            contract used by the wallet picker default.
        """
        ids = await _seed_wallets_and_grants(repo)
        rows = await repo.list_active_wallets(datetime.now(UTC))

        assert len(rows) == 2
        assert rows[0]["public_id"] == ids["wallet_live"]
        assert rows[0]["is_paper"] is False
        assert rows[1]["public_id"] == ids["wallet_paper"]
        assert rows[1]["is_paper"] is True

    @pytest.mark.asyncio
    async def test_empty_catalogue_returns_empty_list(self, repo: SQLAlchemyRepository) -> None:
        """A freshly-created DB with no wallets returns an empty list.

        Given: An empty wallets table,
        When: ``list_active_wallets`` is called,
        Then: The method returns ``[]`` without error.
        """
        rows = await repo.list_active_wallets(datetime.now(UTC))

        assert rows == []


class TestListAccessibleWalletsForOperators:
    """Behaviour of ``list_accessible_wallets_for_operators``."""

    @pytest.mark.asyncio
    async def test_empty_operator_set_returns_empty_without_query(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An empty operator list short-circuits to an empty result.

        Given: An empty operator public_id list,
        When: ``list_accessible_wallets_for_operators`` is called,
        Then: The method returns ``[]`` immediately without running a
            SQL query — guarding against accidental SQL ``IN ()``.
        """
        await _seed_wallets_and_grants(repo)

        rows = await repo.list_accessible_wallets_for_operators([], datetime.now(UTC))

        assert rows == []

    @pytest.mark.asyncio
    async def test_single_operator_returns_only_granted_wallets(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An operator holding one grant sees exactly one wallet.

        Given: ``alice`` with a grant on ``wallet_paper``,
        When: Her operator ID is passed to the scoped lookup,
        Then: Only ``wallet_paper`` is returned.
        """
        ids = await _seed_wallets_and_grants(repo)

        rows = await repo.list_accessible_wallets_for_operators([ids["alice"]], datetime.now(UTC))

        assert len(rows) == 1
        assert rows[0]["public_id"] == ids["wallet_paper"]

    @pytest.mark.asyncio
    async def test_multi_operator_union_deduplicates(self, repo: SQLAlchemyRepository) -> None:
        """Passing two operators returns the deduplicated union of their wallets.

        Given: ``alice`` → ``wallet_paper`` and ``bob`` → ``wallet_live``,
        When: Both operator IDs are passed to the scoped lookup,
        Then: Both wallets are returned, ordered by ``(is_paper, label)``
            so the live wallet appears first.
        """
        ids = await _seed_wallets_and_grants(repo)

        rows = await repo.list_accessible_wallets_for_operators(
            [ids["alice"], ids["bob"]], datetime.now(UTC)
        )

        assert len(rows) == 2
        assert rows[0]["public_id"] == ids["wallet_live"]
        assert rows[1]["public_id"] == ids["wallet_paper"]

    @pytest.mark.asyncio
    async def test_operator_without_grants_returns_empty(self, repo: SQLAlchemyRepository) -> None:
        """Operators with zero active grants see zero wallets.

        Given: ``carol`` with no active scope grants,
        When: Her operator ID is passed to the scoped lookup,
        Then: The method returns ``[]``.
        """
        ids = await _seed_wallets_and_grants(repo)

        rows = await repo.list_accessible_wallets_for_operators([ids["carol"]], datetime.now(UTC))

        assert rows == []


class TestCreateWallet:
    """Behaviour of ``SQLAlchemyRepository.create_wallet``."""

    @pytest.mark.asyncio
    async def test_insert_new_wallet_returns_row(self, repo: SQLAlchemyRepository) -> None:
        """A first wallet insert returns a populated ``WalletRow``.

        Given: An empty wallets table,
        When: ``create_wallet`` is called,
        Then: The returned row carries the inserted label / flag and
            a freshly-generated ``public_id``.
        """
        now = datetime.now(UTC)

        row = await repo.create_wallet(
            label="default",
            description="firm wallet",
            is_paper=False,
            session_id="test-session",
            sequence_id=1,
            timestamp=now,
        )

        assert row["label"] == "default"
        assert row["is_paper"] is False
        assert row["description"] == "firm wallet"
        assert row["public_id"]

    @pytest.mark.asyncio
    async def test_duplicate_label_is_paper_raises_conflict(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A second active wallet with the same ``(label, is_paper)`` fails.

        Given: An existing ``(default, False)`` wallet,
        When: A second insert reuses both fields,
        Then: ``WalletConflictError`` is raised.
        """
        base_ts = datetime.now(UTC)
        await repo.create_wallet(
            label="default",
            description=None,
            is_paper=False,
            session_id="test-session",
            sequence_id=1,
            timestamp=base_ts,
        )

        s5778_value_1 = timedelta(seconds=1)
        with pytest.raises(WalletConflictError) as excinfo:
            await repo.create_wallet(
                label="default",
                description=None,
                is_paper=False,
                session_id="test-session",
                sequence_id=2,
                timestamp=base_ts + s5778_value_1,
            )

        assert excinfo.value.label == "default"
        assert excinfo.value.is_paper is False

    @pytest.mark.asyncio
    async def test_paper_and_live_same_label_both_insert(self, repo: SQLAlchemyRepository) -> None:
        """Paper and live wallets sharing a label are independent rows.

        Given: No existing wallets,
        When: Two inserts share ``label='default'`` but differ on
            ``is_paper``,
        Then: Both succeed and ``list_active_wallets`` returns both
            rows ordered live-first.
        """
        base_ts = datetime.now(UTC) - timedelta(minutes=1)
        await repo.create_wallet(
            label="default",
            description=None,
            is_paper=False,
            session_id="test-session",
            sequence_id=1,
            timestamp=base_ts,
        )
        await repo.create_wallet(
            label="default",
            description=None,
            is_paper=True,
            session_id="test-session",
            sequence_id=2,
            timestamp=base_ts + timedelta(microseconds=1),
        )

        rows = await repo.list_active_wallets(datetime.now(UTC))

        assert len(rows) == 2
        assert rows[0]["is_paper"] is False
        assert rows[1]["is_paper"] is True

    @pytest.mark.asyncio
    async def test_non_unique_integrity_error_reraises_unhandled(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An IntegrityError whose message does not mention 'unique' re-raises.

        Given: A mocked session whose ``commit`` raises an
            ``IntegrityError`` with a non-unique cause (e.g. NOT NULL
            violation),
        When: ``create_wallet`` is called,
        Then: The error propagates as-is (not wrapped in
            ``WalletConflictError``) so callers can distinguish true
            conflicts from other DB-level invariant breaches.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        orig = Exception("NOT NULL constraint failed: wallets.label")
        mock_session.commit = AsyncMock(
            side_effect=IntegrityError(statement="INSERT", params={}, orig=orig)
        )
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        created_at = datetime.now(UTC)
        with (
            patch.object(repo, "session", return_value=mock_ctx),
            pytest.raises(IntegrityError),
        ):
            await repo.create_wallet(
                label="",
                description=None,
                is_paper=False,
                session_id="test-session",
                sequence_id=99,
                timestamp=created_at,
            )


class TestCreateOperator:
    """Behaviour of ``SQLAlchemyRepository.create_operator``."""

    @pytest.mark.asyncio
    async def test_insert_new_operator_returns_row(self, repo: SQLAlchemyRepository) -> None:
        """A first operator insert returns a populated ``OperatorRow``.

        Given: An empty operators table,
        When: ``create_operator`` is called,
        Then: The returned row carries the inserted label / description and a
            freshly-generated ``public_id``.
        """
        row = await repo.create_operator(
            label="firm-desk",
            description="scoped operator",
            session_id="test-session",
            sequence_id=1,
            timestamp=datetime.now(UTC),
        )

        assert row["label"] == "firm-desk"
        assert row["description"] == "scoped operator"
        assert row["public_id"]

    @pytest.mark.asyncio
    async def test_duplicate_label_raises_conflict(self, repo: SQLAlchemyRepository) -> None:
        """A second active operator with the same label fails.

        Given: An existing ``firm-desk`` operator,
        When: A second insert reuses the label,
        Then: ``OperatorConflictError`` is raised.
        """
        base_ts = datetime.now(UTC)
        await repo.create_operator(
            label="firm-desk",
            description=None,
            session_id="test-session",
            sequence_id=1,
            timestamp=base_ts,
        )

        s5778_value_1 = timedelta(seconds=1)
        with pytest.raises(OperatorConflictError) as excinfo:
            await repo.create_operator(
                label="firm-desk",
                description=None,
                session_id="test-session",
                sequence_id=2,
                timestamp=base_ts + s5778_value_1,
            )

        assert excinfo.value.label == "firm-desk"

    @pytest.mark.asyncio
    async def test_non_unique_integrity_error_reraises_unhandled(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An IntegrityError whose message does not mention 'unique' re-raises.

        Given: A mocked session whose ``commit`` raises an ``IntegrityError``
            with a non-unique cause (e.g. NOT NULL violation),
        When: ``create_operator`` is called,
        Then: The error propagates as-is (not wrapped in ``OperatorConflictError``).
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        orig = Exception("NOT NULL constraint failed: operators.label")
        mock_session.commit = AsyncMock(
            side_effect=IntegrityError(statement="INSERT", params={}, orig=orig)
        )
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        created_at = datetime.now(UTC)
        with (
            patch.object(repo, "session", return_value=mock_ctx),
            pytest.raises(IntegrityError),
        ):
            await repo.create_operator(
                label="",
                description=None,
                session_id="test-session",
                sequence_id=1,
                timestamp=created_at,
            )
