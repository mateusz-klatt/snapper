"""Repository-level tests for the Phase 0d wallet catalogue helpers.

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

import pytest

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Operator
from snapper.data.models import Wallet
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository import SQLAlchemyRepository


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
