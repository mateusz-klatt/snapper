"""Repository-level tests for the wallet user read-grant plane.

Covers ``SQLAlchemyRepository.list_readable_wallets_for_user``,
``grant_wallet_user_read_access``, and ``revoke_wallet_user_read_grant``.

The read plane exists because the trade plane cannot express per-user read
visibility: every ``wallet_operator_scope_grants`` row is instrument-exclusive
on its wallet, so two people who must both see one wallet would have to share
one operator. These tests pin the property that makes the read plane different
— the USER is in the active-unique key — and the union semantics that let a
principal see the wallets from either plane exactly once. They also pin the
containment direction: the trade-plane surfaces are re-queried while read
grants exist to prove a read grant never widens what may be traded.

The suite runs against an on-disk SQLite database with the full schema
materialised via ``create_all``, so the SCD2 active partial indexes and the
temporal predicates are exercised physically rather than simulated.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Operator
from snapper.data.models import Wallet
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.models import WalletUserReadGrant
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import WalletUserReadGrantConflictError
from snapper.data.repository import WalletUserReadGrantNotFoundError
from snapper.data.repository_types import CreateWalletUserReadGrantRequest

_ADMIN_USER = "00000000-0000-7000-8000-0000000000a1"
_VIEWER_USER = "00000000-0000-7000-8000-0000000000a2"
_OTHER_USER = "00000000-0000-7000-8000-0000000000a3"
_GRANTED_INSTRUMENT = "00000000-0000-7000-8000-0000000000aa"


def _read_grant_request(
    user_public_id: str,
    wallet_public_id: str,
    timestamp: datetime,
    granted_by: str | None = _ADMIN_USER,
    note: str | None = "uat observer",
) -> CreateWalletUserReadGrantRequest:
    """Build a read-grant insert payload with test-stable provenance.

    Args:
        user_public_id: Public ID of the user gaining read access.
        wallet_public_id: Public ID of the wallet being exposed.
        timestamp: Bus time for the insert.
        granted_by: Granting user, or None for a seed-provisioned grant.
        note: Free-form audit note stored on the row.

    Returns:
        A populated ``CreateWalletUserReadGrantRequest``.
    """
    return CreateWalletUserReadGrantRequest(
        user_public_id=user_public_id,
        wallet_public_id=wallet_public_id,
        granted_by_user_public_id=granted_by,
        note=note,
        session_id="test-session",
        sequence_id=1,
        timestamp=timestamp,
    )


async def _seed_world(repo: SQLAlchemyRepository) -> dict[str, str]:
    """Insert two wallets, one operator, and one operator scope grant.

    Layout:
    - wallet_paper / wallet_live — two active wallets sharing the label
      ``default`` and differing only on ``is_paper``.
    - op_desk — one operator holding an instrument-scoped grant on
      wallet_live, so the trade plane contributes exactly wallet_live.
    - No read grants: every test adds the ones it needs.

    Args:
        repo: Repository whose schema is already materialised.

    Returns:
        Mapping of fixture names to the generated public IDs.
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
        op_desk = Operator(
            label="desk",
            description=None,
            session_id="test-session",
            sequence_id=3,
            timestamp=base_ts,
            known_to=KNOWN_TO_MAX,
        )
        s.add_all([wallet_paper, wallet_live, op_desk])
        await s.commit()
        await s.refresh(wallet_paper)
        await s.refresh(wallet_live)
        await s.refresh(op_desk)

        s.add(
            WalletOperatorScopeGrant(
                operator_public_id=op_desk.public_id,
                wallet_public_id=wallet_live.public_id,
                granted_by_user_public_id=_ADMIN_USER,
                scope_kind="instrument",
                underlying_public_id=None,
                instrument_public_id=_GRANTED_INSTRUMENT,
                note=None,
                session_id="test-session",
                sequence_id=10,
                timestamp=base_ts,
                known_to=KNOWN_TO_MAX,
            )
        )
        await s.commit()

        return {
            "wallet_paper": wallet_paper.public_id,
            "wallet_live": wallet_live.public_id,
            "desk": op_desk.public_id,
        }


@pytest.fixture
async def repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Disposable on-disk SQLite repository with the full schema."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/read_grants.db")
    await r.create_all()
    return r


class TestGrantWalletUserReadAccess:
    """Behaviour of ``SQLAlchemyRepository.grant_wallet_user_read_access``."""

    async def test_insert_returns_populated_row(self, repo: SQLAlchemyRepository) -> None:
        """A first read grant returns a fully populated active row.

        Given: An empty ``wallet_user_read_grants`` table and one wallet,
        When: ``grant_wallet_user_read_access`` is called for a viewer,
        Then: The returned row carries the pair, the granter, the note, a
            freshly-generated ``public_id``, and an open ``known_to``, so the
            caller can echo the grant back without re-reading it.
        """
        ids = await _seed_world(repo)
        now = datetime.now(UTC)

        row = await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], now)
        )

        assert row["user_public_id"] == _VIEWER_USER
        assert row["wallet_public_id"] == ids["wallet_paper"]
        assert row["granted_by_user_public_id"] == _ADMIN_USER
        assert row["note"] == "uat observer"
        assert row["timestamp"] == now
        assert row["known_to"] == KNOWN_TO_MAX
        assert row["public_id"]

    async def test_seed_provisioned_grant_has_no_granter(self, repo: SQLAlchemyRepository) -> None:
        """A grant with no human granter stores NULL rather than a stand-in.

        Given: A seed-provisioned grant whose ``granted_by_user_public_id``
            is None,
        When: The grant is inserted,
        Then: The persisted row keeps the granter NULL, so provenance stays
            honest instead of naming an arbitrary bootstrap admin.
        """
        ids = await _seed_world(repo)

        row = await repo.grant_wallet_user_read_access(
            _read_grant_request(
                _VIEWER_USER,
                ids["wallet_paper"],
                datetime.now(UTC),
                granted_by=None,
                note=None,
            )
        )

        assert row["granted_by_user_public_id"] is None
        assert row["note"] is None

    async def test_two_users_may_read_the_same_wallet(self, repo: SQLAlchemyRepository) -> None:
        """The user is in the key, so one wallet admits many readers.

        Given: One wallet already readable by one viewer,
        When: A second, different user is granted read access to the SAME
            wallet,
        Then: Both inserts succeed — precisely what the instrument-exclusive
            operator scope-grant plane can never express, since there a single
            wallet scope belongs to exactly one operator globally.
        """
        ids = await _seed_world(repo)
        base_ts = datetime.now(UTC)
        first = await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts)
        )

        second = await repo.grant_wallet_user_read_access(
            _read_grant_request(_OTHER_USER, ids["wallet_paper"], base_ts + timedelta(seconds=1))
        )

        assert first["public_id"] != second["public_id"]
        assert first["wallet_public_id"] == second["wallet_public_id"]

    async def test_duplicate_active_pair_raises_typed_conflict(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A second active grant on the same pair is refused by name.

        Given: An active read grant for (viewer, wallet_paper),
        When: The same pair is granted again,
        Then: ``WalletUserReadGrantConflictError`` names the offending pair —
            the pre-insert check answers before the raw index does, so callers
            never have to parse a dialect-specific IntegrityError message.
        """
        ids = await _seed_world(repo)
        base_ts = datetime.now(UTC)
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts)
        )

        s5778_value_1 = _read_grant_request(
            _VIEWER_USER, ids["wallet_paper"], base_ts + timedelta(seconds=1)
        )
        with pytest.raises(WalletUserReadGrantConflictError) as excinfo:
            await repo.grant_wallet_user_read_access(s5778_value_1)

        assert excinfo.value.user_public_id == _VIEWER_USER
        assert excinfo.value.wallet_public_id == ids["wallet_paper"]
        assert "already covers this pair" in excinfo.value.reason

    async def test_unique_integrity_error_maps_to_conflict(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A racing writer that beats the pre-check still yields a conflict.

        Given: A mocked session whose ``commit`` raises a UNIQUE
            ``IntegrityError`` (what a concurrent insert on the same pair
            looks like once the partial index fires),
        When: ``grant_wallet_user_read_access`` is called,
        Then: The raw error is translated into
            ``WalletUserReadGrantConflictError``, so the index remains a
            backstop rather than a second error contract.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_session.execute = AsyncMock(
            return_value=MagicMock(scalars=MagicMock(return_value=MagicMock(first=lambda: None)))
        )
        orig = Exception("UNIQUE constraint failed: ix_wallet_user_read_grants_unique_active")
        mock_session.commit = AsyncMock(
            side_effect=IntegrityError(statement="INSERT", params={}, orig=orig)
        )
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch.object(repo, "session", return_value=mock_ctx),
            pytest.raises(WalletUserReadGrantConflictError) as excinfo,
        ):
            await repo.grant_wallet_user_read_access(
                _read_grant_request(
                    _VIEWER_USER,
                    "00000000-0000-7000-8000-0000000000f1",
                    datetime.now(UTC),
                )
            )

        assert "concurrent writer" in excinfo.value.reason

    async def test_non_unique_integrity_error_reraises_unhandled(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An IntegrityError unrelated to uniqueness propagates untouched.

        Given: A mocked session whose ``commit`` raises an ``IntegrityError``
            with a NOT NULL cause,
        When: ``grant_wallet_user_read_access`` is called,
        Then: The error propagates as-is rather than being mislabelled a
            conflict, so a genuine schema breach stays visible.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_session.execute = AsyncMock(
            return_value=MagicMock(scalars=MagicMock(return_value=MagicMock(first=lambda: None)))
        )
        orig = Exception("NOT NULL constraint failed: wallet_user_read_grants.user_public_id")
        mock_session.commit = AsyncMock(
            side_effect=IntegrityError(statement="INSERT", params={}, orig=orig)
        )
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch.object(repo, "session", return_value=mock_ctx),
            pytest.raises(IntegrityError),
        ):
            await repo.grant_wallet_user_read_access(
                _read_grant_request(
                    _VIEWER_USER,
                    "00000000-0000-7000-8000-0000000000f2",
                    datetime.now(UTC),
                )
            )


class TestRevokeWalletUserReadGrant:
    """Behaviour of ``SQLAlchemyRepository.revoke_wallet_user_read_grant``."""

    async def test_revoke_closes_the_row_without_deleting_it(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Revoking is an SCD2 close, so the grant's lifetime stays queryable.

        Given: One active read grant,
        When: It is revoked at a later bus time,
        Then: The returned row reports ``known_to == revoked_at``, the
            physical row still exists with its original ``timestamp`` and
            business columns intact, and nothing was deleted.
        """
        ids = await _seed_world(repo)
        base_ts = datetime.now(UTC)
        granted = await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts)
        )
        revoked_at = base_ts + timedelta(minutes=1)

        closed = await repo.revoke_wallet_user_read_grant(
            user_public_id=_VIEWER_USER,
            wallet_public_id=ids["wallet_paper"],
            revoked_at=revoked_at,
        )

        assert closed["public_id"] == granted["public_id"]
        assert closed["known_to"] == revoked_at
        assert closed["timestamp"] == base_ts
        assert closed["note"] == "uat observer"
        async with repo.session() as s:
            stored = (await s.execute(select(WalletUserReadGrant))).scalars().all()
        assert len(stored) == 1
        assert stored[0].known_to == revoked_at

    async def test_revoke_without_active_grant_raises_not_found(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Revoking a pair that holds no active grant is a typed 404.

        Given: A wallet the viewer was never granted read access to,
        When: A revoke is attempted for that pair,
        Then: ``WalletUserReadGrantNotFoundError`` is raised rather than the
            revoke silently succeeding as a no-op.
        """
        ids = await _seed_world(repo)

        s5778_value_1 = datetime.now(UTC)
        with pytest.raises(WalletUserReadGrantNotFoundError, match="no active wallet read grant"):
            await repo.revoke_wallet_user_read_grant(
                user_public_id=_VIEWER_USER,
                wallet_public_id=ids["wallet_paper"],
                revoked_at=s5778_value_1,
            )

    async def test_double_revoke_raises_not_found(self, repo: SQLAlchemyRepository) -> None:
        """A second revoke of the same pair is rejected, not repeated.

        Given: A read grant that has already been revoked,
        When: The same pair is revoked again at a later bus time,
        Then: ``WalletUserReadGrantNotFoundError`` is raised, because the
            active-row predicate no longer matches the closed version.
        """
        ids = await _seed_world(repo)
        base_ts = datetime.now(UTC)
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts)
        )
        await repo.revoke_wallet_user_read_grant(
            user_public_id=_VIEWER_USER,
            wallet_public_id=ids["wallet_paper"],
            revoked_at=base_ts + timedelta(minutes=1),
        )

        s5778_value_1 = timedelta(minutes=2)
        with pytest.raises(WalletUserReadGrantNotFoundError):
            await repo.revoke_wallet_user_read_grant(
                user_public_id=_VIEWER_USER,
                wallet_public_id=ids["wallet_paper"],
                revoked_at=base_ts + s5778_value_1,
            )

    async def test_regrant_after_revoke_succeeds(self, repo: SQLAlchemyRepository) -> None:
        """Closing the pair frees the active index slot for a new grant.

        Given: A read grant that was granted and then revoked,
        When: The same pair is granted again after the close,
        Then: A NEW row is inserted with its own ``public_id``, proving the
            partial unique index constrains only ACTIVE rows and that read
            access is genuinely re-grantable rather than one-shot.
        """
        ids = await _seed_world(repo)
        base_ts = datetime.now(UTC)
        first = await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts)
        )
        await repo.revoke_wallet_user_read_grant(
            user_public_id=_VIEWER_USER,
            wallet_public_id=ids["wallet_paper"],
            revoked_at=base_ts + timedelta(minutes=1),
        )

        second = await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts + timedelta(minutes=2))
        )

        assert second["public_id"] != first["public_id"]
        assert second["known_to"] == KNOWN_TO_MAX

    async def test_revoke_losing_a_concurrent_close_race_raises_not_found(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A close that lands between our SELECT and our UPDATE is detected.

        Given: A mocked session whose SELECT returns a live grant but whose
            UPDATE reports ``rowcount == 0`` — exactly what a parallel writer
            closing the row in that window looks like,
        When: ``revoke_wallet_user_read_grant`` runs,
        Then: ``WalletUserReadGrantNotFoundError`` mentioning the concurrent
            mutation is raised instead of the caller being told it revoked a
            row it did not actually close.
        """
        now = datetime.now(UTC)
        grant = WalletUserReadGrant(
            user_public_id=_VIEWER_USER,
            wallet_public_id="00000000-0000-7000-8000-0000000000f3",
            granted_by_user_public_id=_ADMIN_USER,
            note=None,
            session_id="test-session",
            sequence_id=1,
            timestamp=now,
            known_to=KNOWN_TO_MAX,
        )
        grant.public_id = "00000000-0000-7000-8000-0000000000f4"
        select_result = MagicMock()
        select_result.scalars.return_value.first.return_value = grant
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock(side_effect=[select_result, MagicMock(rowcount=0)])
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        with (
            patch.object(repo, "session", return_value=mock_ctx),
            pytest.raises(WalletUserReadGrantNotFoundError, match="concurrent mutation"),
        ):
            await repo.revoke_wallet_user_read_grant(
                user_public_id=_VIEWER_USER,
                wallet_public_id="00000000-0000-7000-8000-0000000000f3",
                revoked_at=now + timedelta(seconds=1),
            )


class TestListReadableWalletsForUser:
    """Behaviour of ``SQLAlchemyRepository.list_readable_wallets_for_user``."""

    async def test_zero_memberships_plus_one_read_grant_still_sees_the_wallet(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An empty operator list must NOT short-circuit the read plane.

        Given: A user with NO operator memberships holding exactly one read
            grant on ``wallet_paper``,
        When: ``list_readable_wallets_for_user`` is called with an empty
            operator list,
        Then: The wallet is returned. The operator-only method short-circuits
            on an empty list and would answer ``[]`` here; the read plane
            exists precisely for this principal shape, so the short-circuit
            must not be inherited.
        """
        ids = await _seed_world(repo)
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], datetime.now(UTC))
        )

        rows = await repo.list_readable_wallets_for_user(
            user_public_id=_VIEWER_USER,
            operator_public_ids=[],
            as_of=datetime.now(UTC),
        )

        assert [row["public_id"] for row in rows] == [ids["wallet_paper"]]
        assert await repo.list_accessible_wallets_for_operators([], datetime.now(UTC)) == []

    async def test_membership_only_user_sees_the_trade_plane_wallet(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """With no read grants the union degrades to the operator plane.

        Given: A user holding a membership in ``desk`` (granted on
            ``wallet_live``) and no read grants at all,
        When: The readable list is requested,
        Then: Only ``wallet_live`` comes back — the read disjunct contributes
            nothing and never removes anything.
        """
        ids = await _seed_world(repo)

        rows = await repo.list_readable_wallets_for_user(
            user_public_id=_VIEWER_USER,
            operator_public_ids=[ids["desk"]],
            as_of=datetime.now(UTC),
        )

        assert [row["public_id"] for row in rows] == [ids["wallet_live"]]

    async def test_union_of_both_planes_is_ordered_by_is_paper_then_label(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Both planes contribute and the result keeps the picker's order.

        Given: A user whose membership reaches ``wallet_live`` and whose read
            grant reaches ``wallet_paper``,
        When: The readable list is requested,
        Then: Both wallets are returned with the live wallet first, matching
            the deterministic ``(is_paper, label)`` ordering the operator-only
            method already guarantees.
        """
        ids = await _seed_world(repo)
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], datetime.now(UTC))
        )

        rows = await repo.list_readable_wallets_for_user(
            user_public_id=_VIEWER_USER,
            operator_public_ids=[ids["desk"]],
            as_of=datetime.now(UTC),
        )

        assert [row["public_id"] for row in rows] == [ids["wallet_live"], ids["wallet_paper"]]
        assert rows[0]["is_paper"] is False
        assert rows[1]["is_paper"] is True

    async def test_wallet_reachable_through_both_planes_appears_once(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Overlapping visibility de-duplicates instead of double-listing.

        Given: A user whose operator membership already reaches
            ``wallet_live`` AND who separately holds a read grant on that
            same wallet,
        When: The readable list is requested,
        Then: ``wallet_live`` appears exactly once, because the query filters
            the wallets table by an OR of two IN predicates rather than
            joining the two grant tables.
        """
        ids = await _seed_world(repo)
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_live"], datetime.now(UTC))
        )

        rows = await repo.list_readable_wallets_for_user(
            user_public_id=_VIEWER_USER,
            operator_public_ids=[ids["desk"]],
            as_of=datetime.now(UTC),
        )

        assert [row["public_id"] for row in rows] == [ids["wallet_live"]]

    async def test_another_users_read_grant_is_not_visible(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Read grants are per-user, so one viewer's grant leaks to nobody.

        Given: ``_OTHER_USER`` holding a read grant on ``wallet_paper``,
        When: ``_VIEWER_USER`` — with no memberships and no grants — asks for
            their readable wallets,
        Then: The list is empty, which is the isolation property the trade
            plane cannot provide when two people share one operator.
        """
        ids = await _seed_world(repo)
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_OTHER_USER, ids["wallet_paper"], datetime.now(UTC))
        )

        rows = await repo.list_readable_wallets_for_user(
            user_public_id=_VIEWER_USER,
            operator_public_ids=[],
            as_of=datetime.now(UTC),
        )

        assert rows == []

    async def test_revoke_removes_the_wallet_from_the_list(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A revoked grant stops contributing from its close time onwards.

        Given: A membership-less user who could see ``wallet_paper`` through
            a read grant,
        When: The grant is revoked and the list is requested after the close,
        Then: The wallet is gone — and a query ``as_of`` a bus time BEFORE the
            close still shows it, because the close is temporal rather than
            destructive.
        """
        ids = await _seed_world(repo)
        base_ts = datetime.now(UTC)
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts)
        )
        revoked_at = base_ts + timedelta(minutes=1)
        await repo.revoke_wallet_user_read_grant(
            user_public_id=_VIEWER_USER,
            wallet_public_id=ids["wallet_paper"],
            revoked_at=revoked_at,
        )

        after = await repo.list_readable_wallets_for_user(
            user_public_id=_VIEWER_USER,
            operator_public_ids=[],
            as_of=revoked_at + timedelta(seconds=1),
        )
        before = await repo.list_readable_wallets_for_user(
            user_public_id=_VIEWER_USER,
            operator_public_ids=[],
            as_of=revoked_at - timedelta(seconds=1),
        )

        assert after == []
        assert [row["public_id"] for row in before] == [ids["wallet_paper"]]

    async def test_active_partial_index_rejects_a_second_live_row_for_one_pair(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """The index — not just the repository — enforces one live row per pair.

        Given: An active read grant inserted through the repository,
        When: A second ACTIVE row for the same pair is inserted directly via
            the ORM, bypassing the repository's pre-check,
        Then: ``ix_wallet_user_read_grants_unique_active`` rejects it, while
            inserting the same pair again AFTER closing the first row is
            accepted — proving the constraint is scoped to active rows.
        """
        ids = await _seed_world(repo)
        base_ts = datetime.now(UTC)
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts)
        )

        with pytest.raises(IntegrityError):
            async with repo.session() as s:
                s.add(
                    WalletUserReadGrant(
                        user_public_id=_VIEWER_USER,
                        wallet_public_id=ids["wallet_paper"],
                        granted_by_user_public_id=_ADMIN_USER,
                        note=None,
                        session_id="test-session",
                        sequence_id=2,
                        timestamp=base_ts + timedelta(seconds=1),
                        known_to=KNOWN_TO_MAX,
                    )
                )
                await s.commit()

        await repo.revoke_wallet_user_read_grant(
            user_public_id=_VIEWER_USER,
            wallet_public_id=ids["wallet_paper"],
            revoked_at=base_ts + timedelta(minutes=1),
        )
        successor = await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts + timedelta(minutes=2))
        )

        assert successor["known_to"] == KNOWN_TO_MAX


class TestReadGrantsDoNotWidenTheTradePlane:
    """The read plane must stay invisible to every trade-authority query.

    The union is deliberately one-directional: the trade-plane predicate feeds
    the read-plane list, never the reverse. These tests fail if a later
    maintainer folds the read predicate into an operator-facing method and so
    turns read visibility into order authority.
    """

    async def test_read_grant_does_not_widen_the_operator_wallet_list(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A read grant is invisible to the trade plane's wallet query.

        Given: A viewer holding read grants on BOTH wallets, including
            ``wallet_paper`` on which ``desk`` holds no scope grant at all,
        When: The operator-only ``list_accessible_wallets_for_operators`` is
            asked what ``desk`` may reach,
        Then: Only ``wallet_live`` comes back. The union is one-directional by
            design — the trade predicate feeds the read plane, never the
            reverse — and this is the regression that fires if a later
            maintainer folds the read predicate into the operator method and
            silently turns read access into trade authority.
        """
        ids = await _seed_world(repo)
        base_ts = datetime.now(UTC)
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts)
        )
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_live"], base_ts)
        )

        accessible = await repo.list_accessible_wallets_for_operators(
            [ids["desk"]], datetime.now(UTC)
        )

        assert [row["public_id"] for row in accessible] == [ids["wallet_live"]]

    async def test_read_grant_does_not_widen_covered_instruments(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A read grant adds no instrument to what an operator may trade.

        Given: A viewer holding read grants on both wallets,
        When: ``list_grant_covered_instrument_public_ids`` — the set the
            strategy permission check authorizes orders against — is computed
            for ``desk`` on each wallet,
        Then: ``wallet_live`` still yields exactly the one instrument its
            scope grant names and ``wallet_paper`` still yields the empty set,
            so read visibility cannot be laundered into order authority.
        """
        ids = await _seed_world(repo)
        base_ts = datetime.now(UTC)
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_paper"], base_ts)
        )
        await repo.grant_wallet_user_read_access(
            _read_grant_request(_VIEWER_USER, ids["wallet_live"], base_ts)
        )
        now = datetime.now(UTC)

        live = await repo.list_grant_covered_instrument_public_ids(
            operator_public_id=ids["desk"], wallet_public_id=ids["wallet_live"], as_of=now
        )
        paper = await repo.list_grant_covered_instrument_public_ids(
            operator_public_id=ids["desk"], wallet_public_id=ids["wallet_paper"], as_of=now
        )

        assert live == {_GRANTED_INSTRUMENT}
        assert paper == set()
