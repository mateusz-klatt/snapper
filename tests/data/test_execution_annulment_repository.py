"""Tests for the single guarded execution-annulment maintenance writer.

The lineage fixture mirrors the MEASURED production shape that motivated the
manifest (2026-07-25): five live executions against exactly two ``fill_observed``
witnesses. The ordering is load-bearing and is reproduced exactly — the Kraken
phantom (size 0, price 0, ``exec_id`` NULL, 21:54) POSTDATES the only genuine
money trade (Walutomat, 20.04 @ 4.3836, 21:22). That ordering is what killed the
forward-activation-cut option, so a fixture that puts the phantom first would
not exercise the real case at all.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import TypedDict
from typing import Unpack
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.execution_chain import ExecutionChainRecord
from snapper.application.portfolio.execution_chain import execution_row_digest
from snapper.core.json_types import JsonObject
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import ExecutionAnnulmentVisibility
from snapper.data.models import Order
from snapper.data.models import User
from snapper.data.models import VenueEvent
from snapper.data.repository import ExecutionAnnulmentActorError
from snapper.data.repository import ExecutionAnnulmentConflictError
from snapper.data.repository import ExecutionAnnulmentTargetError
from snapper.data.repository import ExecutionAnnulmentWitnessedError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _ExecutionAnnulmentCommand
from snapper.data.repository_types import ExecutionAnnulmentRequest
from snapper.data.repository_types import ExecutionAnnulmentRow
from snapper.data.repository_types import ExecutionAnnulmentVisibilityRow

_SESSION = "00000000-0000-7000-8000-000000000901"
_USER = "0000face-0000-7000-8000-0000000000d1"
_DEACTIVATED_USER = "0000face-0000-7000-8000-0000000000d2"
_ABSENT_USER = "0000face-0000-7000-8000-0000000000d3"
_MAIN_WALLET = "0000face-0000-7000-8000-0000000000a1"
_PAPER_WALLET = "0000face-0000-7000-8000-0000000000a2"

_KRAKEN_PHANTOM_AT = datetime(2026, 7, 19, 21, 54, tzinfo=UTC)
_WALUTOMAT_TRADE_AT = datetime(2026, 7, 19, 21, 22, tzinfo=UTC)
_PAPER_AT = datetime(2026, 7, 10, 9, 0, tzinfo=UTC)
_CORRECTION_AT = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)

_KRAKEN_PHANTOM = "00000000-0000-7000-8000-000000000e01"
_WALUTOMAT_TRADE = "00000000-0000-7000-8000-000000000e02"
_PAPER_PHANTOM = "00000000-0000-7000-8000-000000000e03"
_PAPER_LEGACY = "00000000-0000-7000-8000-000000000e04"
_PAPER_WITNESSED = "00000000-0000-7000-8000-000000000e05"

_KRAKEN_ORDER = "00000000-0000-7000-8000-000000000c01"
_WALUTOMAT_ORDER = "00000000-0000-7000-8000-000000000c02"
_PAPER_PHANTOM_ORDER = "00000000-0000-7000-8000-000000000c03"
_PAPER_LEGACY_ORDER = "00000000-0000-7000-8000-000000000c04"
_PAPER_WITNESSED_ORDER = "00000000-0000-7000-8000-000000000c05"

_EVIDENCE: JsonObject = {
    "diagnosis": "size 0 / price 0 residue of the resting-order open-closed mismap",
    "fixed_in_commit": "13a6a397",
    "fill_observed_witnesses": 0,
}


def _order(public_id: str, client_order_id: str | None, wallet_public_id: str) -> Order:
    """Build one sentinel-current Order lineage row for an execution."""
    return Order(
        public_id=public_id,
        instrument_public_id="00000000-0000-7000-8000-000000000b01",
        wallet_public_id=wallet_public_id,
        mode="live",
        client_order_id=client_order_id,
        created_at=_WALUTOMAT_TRADE_AT,
        timestamp=_WALUTOMAT_TRADE_AT,
        side="buy",
        order_type="limit",
        price=4.3836,
        size=20.04,
        status="filled",
        session_id=_SESSION,
        sequence_id=1,
        known_to=KNOWN_TO_MAX,
    )


class _ExecutionOptions(TypedDict, total=False):
    """Optional fields accepted by the Execution test-row builder."""

    order_public_id: str
    wallet_public_id: str
    scope_sequence: int
    timestamp: datetime
    price: float
    size: float
    exec_id: str | None
    operator_public_id: str | None


def _execution(
    public_id: str,
    exchange: str,
    **options: Unpack[_ExecutionOptions],
) -> Execution:
    """Build one committed execution with explicit immutable scope coordinates."""
    scope_sequence = options.get("scope_sequence", 1)
    return Execution(
        public_id=public_id,
        order_public_id=options.get("order_public_id", _KRAKEN_ORDER),
        wallet_public_id=options.get("wallet_public_id", _MAIN_WALLET),
        operator_public_id=options.get("operator_public_id"),
        exchange=exchange,
        mode="live",
        scope_sequence=scope_sequence,
        exec_id=options.get("exec_id"),
        trade_id=None,
        side="buy",
        status="filled",
        price=options.get("price", 0.0),
        size=options.get("size", 0.0),
        fee=0.0,
        fee_asset="USD",
        executed_at=None,
        liquidity_role="unknown",
        timestamp=options.get("timestamp", _KRAKEN_PHANTOM_AT),
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=scope_sequence,
    )


def _fill_event(
    public_id: str,
    client_order_id: str,
    wallet_public_id: str,
    exchange: str,
    timestamp: datetime,
) -> VenueEvent:
    """Build one append-only ``fill_observed`` witness for an order."""
    return VenueEvent(
        public_id=public_id,
        event_type="fill_observed",
        shard_key=f"{exchange}.EUR-PLN.live",
        wallet_public_id=wallet_public_id,
        command_public_id=None,
        exchange=exchange,
        instrument="EUR-PLN",
        mode="live",
        exchange_order_id=None,
        client_order_id=client_order_id,
        venue_client_id=client_order_id,
        side="buy",
        status="filled",
        fill_price=4.3836,
        fill_size=20.04,
        cum_fill_size=20.04,
        fee=0.0,
        fee_asset="PLN",
        exec_id=f"recon-{client_order_id}",
        trade_id=None,
        error=None,
        venue_timestamp=timestamp,
        received_at=timestamp,
        payload_json=None,
        liquidity_role="unknown",
        paired_group_id=None,
        timestamp=timestamp,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _user(public_id: str, username: str, is_active: bool) -> User:
    """Build one acting-user row the guarded writer can resolve."""
    return User(
        public_id=public_id,
        username=username,
        email=None,
        password_hash="x",
        role="admin",
        is_active=is_active,
        default_language=None,
        created_at=_WALUTOMAT_TRADE_AT,
        created_by_user_public_id=None,
        timestamp=_WALUTOMAT_TRADE_AT,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _production_lineage() -> list[Order | Execution | User | VenueEvent]:
    """Build the measured production shape: five executions, two witnesses.

    ``main/kraken/live`` seq 1 is the phantom that blocks the only real trading
    wallet, and it is deliberately LATER than ``main/walutomat/live`` seq 1, the
    genuine Walutomat trade. ``paper/paper/live`` holds the pricing-bug phantom,
    the row predating durable fill lineage, and one properly witnessed fill.
    """
    return [
        _user(_USER, "operator", True),
        _user(_DEACTIVATED_USER, "retired-operator", False),
        _order(_KRAKEN_ORDER, "client-kraken-1", _MAIN_WALLET),
        _order(_WALUTOMAT_ORDER, "client-walutomat-1", _MAIN_WALLET),
        _order(_PAPER_PHANTOM_ORDER, "client-paper-1", _PAPER_WALLET),
        _order(_PAPER_LEGACY_ORDER, None, _PAPER_WALLET),
        _order(_PAPER_WITNESSED_ORDER, "client-paper-3", _PAPER_WALLET),
        _execution(
            _WALUTOMAT_TRADE,
            "walutomat",
            order_public_id=_WALUTOMAT_ORDER,
            timestamp=_WALUTOMAT_TRADE_AT,
            price=4.3836,
            size=20.04,
            exec_id="recon-client-walutomat-1",
        ),
        _execution(_KRAKEN_PHANTOM, "kraken"),
        _execution(
            _PAPER_PHANTOM,
            "paper",
            order_public_id=_PAPER_PHANTOM_ORDER,
            wallet_public_id=_PAPER_WALLET,
            timestamp=_PAPER_AT,
            size=1.0,
            exec_id="paper-1",
        ),
        _execution(
            _PAPER_LEGACY,
            "paper",
            order_public_id=_PAPER_LEGACY_ORDER,
            wallet_public_id=_PAPER_WALLET,
            scope_sequence=2,
            timestamp=_PAPER_AT,
            price=100.0,
            size=1.0,
            exec_id="paper-2",
        ),
        _execution(
            _PAPER_WITNESSED,
            "paper",
            order_public_id=_PAPER_WITNESSED_ORDER,
            wallet_public_id=_PAPER_WALLET,
            scope_sequence=3,
            timestamp=_PAPER_AT,
            price=100.0,
            size=1.0,
            exec_id="paper-3",
        ),
        _fill_event(
            "00000000-0000-7000-8000-000000000f01",
            "client-walutomat-1",
            _MAIN_WALLET,
            "walutomat",
            _WALUTOMAT_TRADE_AT,
        ),
        _fill_event(
            "00000000-0000-7000-8000-000000000f02",
            "client-paper-3",
            _PAPER_WALLET,
            "paper",
            _PAPER_AT,
        ),
    ]


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create a repository holding the measured production annulment lineage."""
    db_path = tmp_path / "execution-annulments.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Order.__table__.create(schema_engine)
    Execution.__table__.create(schema_engine)
    VenueEvent.__table__.create(schema_engine)
    ExecutionAnnulment.__table__.create(schema_engine)
    ExecutionAnnulmentVisibility.__table__.create(schema_engine)
    User.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        async with repo.session() as s:
            s.add_all(_production_lineage())
            await s.commit()
        yield repo
    finally:
        await repo.engine.dispose()


async def _expected_digest(repository: SQLAlchemyRepository, public_id: str) -> str:
    """Independently project one stored execution and digest its canonical bytes."""
    async with repository.session() as s:
        row = (
            (await s.execute(select(Execution).where(Execution.public_id == public_id)))
            .scalars()
            .one()
        )
        return execution_row_digest(
            ExecutionChainRecord(
                scope_sequence=row.scope_sequence,
                public_id=row.public_id,
                order_public_id=row.order_public_id,
                wallet_public_id=row.wallet_public_id,
                operator_public_id=row.operator_public_id,
                exchange=row.exchange,
                mode=row.mode,
                exec_id=row.exec_id,
                trade_id=row.trade_id,
                side=row.side,
                status=row.status,
                fee_asset=row.fee_asset,
                price_decimal=row.price_decimal,
                size_decimal=row.size_decimal,
                fee_decimal=row.fee_decimal,
                counter_amount_decimal=row.counter_amount_decimal,
                numeric_provenance=row.numeric_provenance,
                liquidity_role=row.liquidity_role,
                timestamp=row.timestamp,
                executed_at=row.executed_at,
            )
        )


def _request(digest: str, **overrides: object) -> ExecutionAnnulmentRequest:
    """Build one valid Kraken-phantom annulment request, overriding named fields."""
    base: dict[str, object] = {
        "target_execution_public_id": _KRAKEN_PHANTOM,
        "expected_execution_digest": digest,
        "wallet_public_id": _MAIN_WALLET,
        "exchange": "kraken",
        "mode": "live",
        "scope_sequence": 1,
        "annulled_by_user_public_id": _USER,
        "correction_time": _CORRECTION_AT,
        "reason": "unwitnessed_phantom",
        "evidence": dict(_EVIDENCE),
        "session_id": _SESSION,
        "sequence_id": 1,
    }
    base.update(overrides)
    return cast(ExecutionAnnulmentRequest, base)


async def _record_annulment(
    repository: SQLAlchemyRepository,
    request: ExecutionAnnulmentRequest,
) -> ExecutionAnnulmentRow:
    """Record one annulment through the guarded writer, keeping its manifest row.

    The writer's result also names the state of the correction's durability
    observation; a test whose subject is the manifest row itself projects that
    row out here so the assertion stays about the row.
    """
    return (await repository.record_execution_annulment(request))["annulment"]


async def _execution_snapshot(
    repository: SQLAlchemyRepository,
) -> list[tuple[str, str, int, float, float, str | None, datetime]]:
    """Capture every execution's economic identity for an untouched-ledger check."""
    async with repository.session() as s:
        rows = (
            (
                await s.execute(
                    select(Execution).order_by(
                        Execution.exchange.asc(), Execution.scope_sequence.asc()
                    )
                )
            )
            .scalars()
            .all()
        )
        return [
            (
                row.public_id,
                row.exchange,
                int(row.scope_sequence),
                float(row.price),
                float(row.size),
                row.exec_id,
                row.known_to,
            )
            for row in rows
        ]


async def test_the_knowledge_instant_is_stamped_by_the_server_not_the_operator(
    repository: SQLAlchemyRepository,
) -> None:
    """A correction cannot claim to have been known before it was written.

    Given: An operator whose declared ``correction_time`` is backdated far into
        the past — the shape of a request that would like a historical read to
        be re-answered.
    When: The correction is recorded.
    Then: The manifest row's ``timestamp`` — the value every knowledge-horizon
        fold filters on — is the SERVER's own instant, bracketed by this test's
        clock readings, while the backdated declaration is preserved verbatim as
        audit metadata. Only an instant no caller can choose can turn "was this
        already known?" from a claim into a fact.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    backdated = datetime(2020, 1, 1, tzinfo=UTC)
    before = datetime.now(UTC)

    row = await _record_annulment(repository, _request(digest, correction_time=backdated))

    after = datetime.now(UTC)
    assert row["correction_time"] == backdated
    assert before <= row["timestamp"] <= after


async def test_the_knowledge_stamp_postdates_every_lock_the_writer_waits_on(
    repository: SQLAlchemyRepository,
) -> None:
    """A correction cannot be dated from before the wait that delayed it.

    Given: A scope fence that takes measurable time to acquire — the shape of a
        real advisory-lock wait behind a concurrent writer.
    When: The correction is recorded.
    Then: The knowledge stamp postdates the fence's release. Stamping before the
        wait would date the row from an instant at which it did not yet durably
        exist, so a historical read landing in that gap would fold a correction
        the database could not have shown it. What remains after this is bounded
        by the writer's own insert-to-commit latency, with no lock awaited in
        between.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    fence_released: list[datetime] = []
    real_fence = SQLAlchemyRepository._acquire_execution_annulment_fence

    async def slow_fence(
        self: SQLAlchemyRepository,
        session: AsyncSession,
        command: _ExecutionAnnulmentCommand,
    ) -> None:
        """Hold the fence briefly, then record the instant it was acquired."""
        await real_fence(self, session, command)
        await asyncio.sleep(0.05)
        fence_released.append(datetime.now(UTC))

    with patch.object(SQLAlchemyRepository, "_acquire_execution_annulment_fence", slow_fence):
        row = await _record_annulment(repository, _request(digest))

    assert fence_released
    assert row["timestamp"] >= fence_released[0]


async def _refuse_observation(
    self: SQLAlchemyRepository,
    annulment_public_id: str,
) -> ExecutionAnnulmentVisibilityRow:
    """Fail the durability observation so a correction is left unobserved."""
    del self, annulment_public_id
    raise RuntimeError("visibility transaction unavailable")


async def test_the_writer_proves_durability_with_a_second_transaction(
    repository: SQLAlchemyRepository,
) -> None:
    """The knowledge instant is observed after the commit, never stamped before it.

    Given: The ordinary writer.
    When: A correction is recorded.
    Then: A visibility observation exists for it, bound to both spellings of its
        identity, and its ``observed_at`` is STRICTLY LATER than the correction's
        own pre-commit stamp. That ordering is the proof: the observation was
        taken by a second transaction that had already SEEN the committed row,
        so ``annulment_durable_at <= observed_at`` holds and a historical fold
        keyed on ``observed_at`` can never claim knowledge earlier than
        durability — for any stall, with no margin and nothing to monitor.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)

    row = await _record_annulment(repository, _request(digest))

    async with repository.session() as s:
        observation = (
            (
                await s.execute(
                    select(ExecutionAnnulmentVisibility).where(
                        ExecutionAnnulmentVisibility.annulment_public_id == row["public_id"]
                    )
                )
            )
            .scalars()
            .one()
        )
        annulment = (
            (
                await s.execute(
                    select(ExecutionAnnulment).where(
                        ExecutionAnnulment.public_id == row["public_id"]
                    )
                )
            )
            .scalars()
            .one()
        )
    assert observation.annulment_id == annulment.id
    assert observation.wallet_public_id == _MAIN_WALLET
    assert observation.exchange == "kraken"
    assert observation.mode == "live"
    assert observation.observed_at > row["timestamp"]


async def test_a_failed_observation_leaves_a_resumable_correction(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A correction that could not be observed is durable but not yet knowable.

    Given: A second transaction that fails after the correction has committed.
    When: The correction is recorded, and the observation is completed later
        through the public maintenance surface.
    Then: The correction is returned and durable regardless — the manifest is
        append-only, so refusing afterwards would be a lie — while carrying no
        observation, which withholds it from historical reads rather than
        letting them fold something unproven. The resumable call then completes
        it, and repeating that call is idempotent: it returns the SAME
        observation instead of minting a later one, because a second instant
        would move the correction's proven knowledge time forward.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    real_observe = SQLAlchemyRepository.observe_execution_annulment_visibility
    failed: list[str] = []

    async def failing_observe(
        self: SQLAlchemyRepository,
        annulment_public_id: str,
    ) -> ExecutionAnnulmentVisibilityRow:
        """Fail the durability observation exactly once, after the commit."""
        failed.append(annulment_public_id)
        raise RuntimeError("visibility transaction unavailable")

    monkeypatch.setattr(
        SQLAlchemyRepository,
        "observe_execution_annulment_visibility",
        failing_observe,
    )
    row = await _record_annulment(repository, _request(digest))
    monkeypatch.setattr(
        SQLAlchemyRepository,
        "observe_execution_annulment_visibility",
        real_observe,
    )

    assert failed == [row["public_id"]]
    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == [row]
    async with repository.session() as s:
        pending = (await s.execute(select(ExecutionAnnulmentVisibility))).scalars().all()
    assert list(pending) == []

    completed = await repository.observe_execution_annulment_visibility(row["public_id"])
    repeated = await repository.observe_execution_annulment_visibility(row["public_id"])

    assert completed["annulment_public_id"] == row["public_id"]
    assert repeated == completed


async def test_observing_an_unknown_correction_refuses(
    repository: SQLAlchemyRepository,
) -> None:
    """The observation is a proof about a real row, never a bare assertion.

    Given: An identity no correction in this database carries.
    When: The maintenance surface is asked to observe it.
    Then: It refuses. An observation minted without a re-read would prove
        nothing about durability, which is the one thing this ledger exists to
        establish.
    """
    with pytest.raises(ValueError, match="unknown execution annulment"):
        await repository.observe_execution_annulment_visibility(
            "00000000-0000-7000-8000-0000000009ff"
        )


async def test_annulling_the_kraken_phantom_leaves_the_real_trade_untouched(
    repository: SQLAlchemyRepository,
) -> None:
    """The one production correction unblocks the scope and changes nothing else.

    Given: The measured production lineage — five executions, two
        ``fill_observed`` witnesses, and the Kraken phantom recorded LATER than
        the genuine Walutomat trade it shares a wallet with.
    When: The Kraken phantom alone is annulled through the guarded writer.
    Then: One manifest row is appended carrying the target binding, the
        canonical scope coordinates, the acting user, the correction time, the
        typed reason and the canonicalized evidence; the scope manifest contains
        exactly that row; and every execution row — the real Walutomat trade
        first among them — is byte-identical to before, because the correction
        was APPENDED to a separate plane rather than applied to the ledger.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    before = await _execution_snapshot(repository)

    row = await _record_annulment(repository, _request(digest))

    assert row["target_execution_public_id"] == _KRAKEN_PHANTOM
    assert row["target_execution_digest"] == digest
    assert row["wallet_public_id"] == _MAIN_WALLET
    assert row["exchange"] == "kraken"
    assert row["mode"] == "live"
    assert row["scope_sequence"] == 1
    assert row["annulled_by_user_public_id"] == _USER
    assert row["correction_time"] == _CORRECTION_AT
    assert row["reason"] == "unwitnessed_phantom"
    assert row["evidence_json"] == (
        '{"diagnosis":"size 0 / price 0 residue of the resting-order open-closed mismap",'
        '"fill_observed_witnesses":0,"fixed_in_commit":"13a6a397"}'
    )
    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == [row]
    assert await _execution_snapshot(repository) == before


async def test_a_second_annulment_of_one_execution_conflicts_with_its_winner(
    repository: SQLAlchemyRepository,
) -> None:
    """One execution admits exactly one annulment, and the loser learns which.

    Given: A Kraken phantom that has already been annulled once.
    When: The identical annulment is requested again.
    Then: The writer refuses with a typed conflict carrying the COMMITTED
        winner, the manifest still holds exactly one row, and that row is the
        original — a repeated operator command cannot fork the correction
        history.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    winner = await _record_annulment(repository, _request(digest))

    s5778_value_1 = _request(digest, sequence_id=2, correction_time=_CORRECTION_AT)
    with pytest.raises(ExecutionAnnulmentConflictError) as conflict:
        await repository.record_execution_annulment(s5778_value_1)

    assert conflict.value.winner == winner
    assert _KRAKEN_PHANTOM in str(conflict.value)
    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == [winner]


async def test_a_witnessed_execution_can_never_be_annulled(
    repository: SQLAlchemyRepository,
) -> None:
    """Real money refuses repudiation, and the refusal writes nothing.

    Given: The genuine Walutomat trade, which carries a durable
        ``fill_observed`` witness on its order's client id.
    When: An annulment of it is requested with a correct digest and scope.
    Then: The writer fails closed with a witnessed-target refusal naming the
        witnessing fill event, and the manifest stays empty — the write-time
        check is what keeps a real fill out of the correction plane.
    """
    digest = await _expected_digest(repository, _WALUTOMAT_TRADE)

    s5778_value_1 = _request(
        digest,
        target_execution_public_id=_WALUTOMAT_TRADE,
        exchange="walutomat",
    )
    with pytest.raises(ExecutionAnnulmentWitnessedError, match="witnessed_execution_target"):
        await repository.record_execution_annulment(s5778_value_1)

    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == []


async def test_a_target_without_client_order_lineage_is_still_annullable(
    repository: SQLAlchemyRepository,
) -> None:
    """A row predating durable fill lineage has no witness to find, and annuls.

    Given: The paper execution whose order carries no ``client_order_id`` at
        all — the shape of a booking made before durable fill lineage existed.
    When: It is annulled with the legacy-lineage reason.
    Then: The witness probe finds no client id to match on, so it cannot
        manufacture evidence either way, and the correction is appended with
        the reason that states exactly that.
    """
    digest = await _expected_digest(repository, _PAPER_LEGACY)

    row = await _record_annulment(
        repository,
        _request(
            digest,
            target_execution_public_id=_PAPER_LEGACY,
            wallet_public_id=_PAPER_WALLET,
            exchange="paper",
            scope_sequence=2,
            reason="unwitnessed_legacy_lineage",
        ),
    )

    assert row["reason"] == "unwitnessed_legacy_lineage"
    assert await repository.get_execution_annulments(_PAPER_WALLET, "live") == [row]


async def test_an_unrelated_witness_does_not_block_an_unwitnessed_phantom(
    repository: SQLAlchemyRepository,
) -> None:
    """Witness evidence binds per order, not per scope.

    Given: A paper scope holding both an unwitnessed pricing-bug phantom and a
        properly witnessed fill on a different order.
    When: Only the phantom is annulled.
    Then: The neighbouring witness does not block it, and the witnessed sibling
        still refuses annulment — the binding is the order's client id, exactly
        as the prefix proof reads it.
    """
    phantom_digest = await _expected_digest(repository, _PAPER_PHANTOM)
    witnessed_digest = await _expected_digest(repository, _PAPER_WITNESSED)
    common = {
        "wallet_public_id": _PAPER_WALLET,
        "exchange": "paper",
    }

    row = await _record_annulment(
        repository,
        _request(
            phantom_digest,
            target_execution_public_id=_PAPER_PHANTOM,
            scope_sequence=1,
            **common,
        ),
    )

    s5778_value_1 = _request(
        witnessed_digest,
        target_execution_public_id=_PAPER_WITNESSED,
        scope_sequence=3,
        **common,
    )
    with pytest.raises(ExecutionAnnulmentWitnessedError):
        await repository.record_execution_annulment(s5778_value_1)

    assert await repository.get_execution_annulments(_PAPER_WALLET, "live") == [row]


async def test_an_unknown_target_is_refused(repository: SQLAlchemyRepository) -> None:
    """An annulment of an execution that does not exist writes nothing.

    Given: A well-formed request naming an execution id no ledger row carries.
    When: The annulment is attempted.
    Then: It is refused with ``unknown_execution_target`` and the manifest stays
        empty.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)

    s5778_value_1 = _request(
        digest,
        target_execution_public_id="00000000-0000-7000-8000-0000000009ff",
    )
    with pytest.raises(ExecutionAnnulmentTargetError, match="unknown_execution_target"):
        await repository.record_execution_annulment(s5778_value_1)

    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == []


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"wallet_public_id": _PAPER_WALLET}, id="wallet"),
        pytest.param({"exchange": "walutomat"}, id="exchange"),
        pytest.param({"mode": "paper"}, id="mode"),
        pytest.param({"scope_sequence": 2}, id="scope_sequence"),
    ],
)
async def test_a_target_in_a_different_scope_is_refused(
    repository: SQLAlchemyRepository,
    overrides: dict[str, object],
) -> None:
    """Every certification coordinate is proven, not trusted.

    Given: A request whose wallet, exchange, mode, or scope sequence disagrees
        with the stored target row.
    When: The annulment is attempted.
    Then: It is refused with ``crossed_execution_annulment_scope``, so a
        manifest row can never denormalize coordinates the ledger does not
        agree with.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)

    s5778_value_1 = _request(digest, **overrides)
    with pytest.raises(ExecutionAnnulmentTargetError, match="crossed_execution_annulment_scope"):
        await repository.record_execution_annulment(s5778_value_1)


async def test_a_target_with_an_unparseable_stored_wallet_fails_closed(
    repository: SQLAlchemyRepository,
) -> None:
    """A corrupt stored wallet identity refuses the annulment instead of crashing.

    Given: An execution whose stored wallet identity is not a UUID at all.
    When: An annulment naming a canonical wallet targets it.
    Then: The scope proof cannot establish equality, so it refuses with a
        crossed-scope error rather than raising an unhandled parse failure.
    """
    corrupt = "00000000-0000-7000-8000-0000000009c1"
    async with repository.session() as s:
        s.add(
            _execution(
                corrupt,
                "kraken",
                wallet_public_id="not-a-uuid",
                scope_sequence=2,
            )
        )
        await s.commit()

    s5778_value_1 = _request(
        "c" * 64,
        target_execution_public_id=corrupt,
        scope_sequence=2,
    )
    with pytest.raises(ExecutionAnnulmentTargetError, match="crossed_execution_annulment_scope"):
        await repository.record_execution_annulment(s5778_value_1)


async def test_a_target_that_cannot_be_canonicalized_fails_closed(
    repository: SQLAlchemyRepository,
) -> None:
    """A row whose canonical bytes cannot be produced is never annullable.

    Given: An execution whose stored order identity is not a UUID, so the shared
        canonical serialization refuses it.
    When: An annulment targets it with matching scope coordinates.
    Then: The writer converts the canonicalization failure into a typed
        ``uncanonicalizable_execution_target`` refusal — a row with no provable
        digest can never satisfy a binding proof, and must not crash the writer.
    """
    uncanonical = "00000000-0000-7000-8000-0000000009c2"
    async with repository.session() as s:
        s.add(
            _execution(
                uncanonical,
                "kraken",
                order_public_id="not-a-uuid",
                scope_sequence=3,
            )
        )
        await s.commit()

    s5778_value_1 = _request(
        "c" * 64,
        target_execution_public_id=uncanonical,
        scope_sequence=3,
    )
    with pytest.raises(ExecutionAnnulmentTargetError, match="uncanonicalizable_execution_target"):
        await repository.record_execution_annulment(s5778_value_1)


async def test_a_wrong_expected_digest_is_refused(repository: SQLAlchemyRepository) -> None:
    """The operator-supplied digest binds the request to one exact row content.

    Given: A request whose expected digest is the digest of a DIFFERENT
        execution — the shape of a command prepared against the wrong row or
        against content that has since been re-read.
    When: The annulment is attempted.
    Then: It is refused with ``execution_digest_mismatch`` and nothing is
        written, so a correction can never be recorded for content the operator
        did not actually authorize.
    """
    other_digest = await _expected_digest(repository, _WALUTOMAT_TRADE)

    s5778_value_1 = _request(other_digest)
    with pytest.raises(ExecutionAnnulmentTargetError, match="execution_digest_mismatch"):
        await repository.record_execution_annulment(s5778_value_1)

    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == []


async def test_an_active_acting_user_is_proven_and_stamped_on_the_correction(
    repository: SQLAlchemyRepository,
) -> None:
    """The acting identity is verified to EXIST, and that is all it claims.

    Given: An asserted acting user that resolves to a present, active ``users``
        row.
    When: The Kraken phantom is annulled.
    Then: The correction is appended carrying that identity. The proof is
        existence and not authentication — this writer has no principal to
        authenticate — so what the manifest records is an operator assertion the
        database could resolve at the instant it was recorded.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)

    appended = await _record_annulment(repository, _request(digest))

    assert appended["annulled_by_user_public_id"] == _USER


async def test_an_acting_user_no_row_resolves_is_refused(
    repository: SQLAlchemyRepository,
) -> None:
    """A fabricated or mistyped acting identity must not reach the manifest.

    Given: An asserted acting user that is a well-formed UUID no ``users`` row
        carries.
    When: The annulment is attempted.
    Then: It is refused with ``unknown_annulment_actor`` inside the fenced
        transaction and nothing is written, so an unwithdrawable correction can
        never be attributed to somebody who does not exist.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)

    s5778_value_1 = _request(digest, annulled_by_user_public_id=_ABSENT_USER)
    with pytest.raises(ExecutionAnnulmentActorError, match="unknown_annulment_actor"):
        await repository.record_execution_annulment(s5778_value_1)

    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == []


async def test_a_deactivated_acting_user_is_refused(
    repository: SQLAlchemyRepository,
) -> None:
    """A deactivated account may not author a money-truth correction.

    Given: An asserted acting user whose ``users`` row exists and is
        deactivated.
    When: The annulment is attempted.
    Then: It is refused with ``inactive_annulment_actor`` and nothing is
        written. The check runs in the same transaction that would append, so a
        deactivation committed a moment earlier is honoured rather than raced.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)

    s5778_value_1 = _request(digest, annulled_by_user_public_id=_DEACTIVATED_USER)
    with pytest.raises(ExecutionAnnulmentActorError, match="inactive_annulment_actor"):
        await repository.record_execution_annulment(s5778_value_1)

    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == []


async def test_a_superseded_acting_user_version_authorizes_nothing(
    repository: SQLAlchemyRepository,
) -> None:
    """Only the CURRENT version of a user can act.

    Given: An acting user whose only ``users`` version has been closed, which is
        how this schema records a superseded account version.
    When: The annulment is attempted.
    Then: It is refused with ``unknown_annulment_actor``: the writer asks about
        the open-sentinel version, so a historical row cannot quietly keep
        authorizing corrections after the account it described stopped being
        current.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    async with repository.session() as s:
        await s.execute(update(User).where(User.public_id == _USER).values(known_to=_CORRECTION_AT))
        await s.commit()

    s5778_value_1 = _request(digest)
    with pytest.raises(ExecutionAnnulmentActorError, match="unknown_annulment_actor"):
        await repository.record_execution_annulment(s5778_value_1)


async def test_the_unobserved_count_is_independent_of_the_discovery_page(
    repository: SQLAlchemyRepository,
) -> None:
    """The remaining-work number must not inherit the paging bound.

    Given: Two paper corrections, neither carrying a durability observation.
    When: The bounded discovery read is taken with a page of one, and the count
        is taken separately.
    Then: The page returns one row while the count returns two. A maintenance
        surface that derived "remaining" from the page would report a closed
        knowledge gap while a correction history still refuses sat beyond it.
    """
    corrections = await _two_unobserved_paper_corrections(repository)

    page = await repository.get_unobserved_execution_annulments(_PAPER_WALLET, "live", 1)
    total = await repository.count_unobserved_execution_annulments(_PAPER_WALLET, "live")

    assert len(corrections) == 2
    assert len(page) == 1
    assert total == 2


async def test_the_unobserved_count_refuses_a_malformed_wallet_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """A count is a scope answer, so an unresolvable scope has no answer.

    Given: A wallet spelling that is not a UUID.
    When: The unobserved count is taken.
    Then: It refuses rather than counting zero, because a zero would be read as
        "no corrections await observation".
    """
    with pytest.raises(ValueError, match="wallet_public_id is not a valid uuid"):
        await repository.count_unobserved_execution_annulments("not-a-uuid", "live")


async def test_a_scope_slot_collision_without_a_target_winner_reraises(
    repository: SQLAlchemyRepository,
) -> None:
    """An integrity failure with no readable winner is never masked as a conflict.

    Given: A manifest row inserted directly for the Kraken scope slot but
        pointing at a DIFFERENT target execution, so the scope unique index is
        already occupied while the target index is not.
    When: The genuine Kraken phantom annulment is attempted.
    Then: The scope index rejects it, the winner re-read by target id finds
        nothing, and the raw ``IntegrityError`` is re-raised rather than being
        reported as an already-annulled conflict that a caller could ignore.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    async with repository.session() as s:
        s.add(
            ExecutionAnnulment(
                target_execution_public_id="00000000-0000-7000-8000-0000000009fe",
                target_execution_digest="d" * 64,
                wallet_public_id=_MAIN_WALLET,
                exchange="kraken",
                mode="live",
                scope_sequence=1,
                annulled_by_user_public_id=_USER,
                correction_time=_CORRECTION_AT,
                reason="unwitnessed_phantom",
                evidence_json='{"diagnosis":"hand-inserted squatter"}',
                timestamp=_CORRECTION_AT,
                known_to=KNOWN_TO_MAX,
                session_id=_SESSION,
                sequence_id=1,
            )
        )
        await s.commit()

    s5778_value_1 = _request(digest)
    with pytest.raises(IntegrityError):
        await repository.record_execution_annulment(s5778_value_1)


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"target_execution_public_id": "not-a-uuid"}, id="target"),
        pytest.param({"wallet_public_id": "not-a-uuid"}, id="wallet"),
        pytest.param({"annulled_by_user_public_id": "not-a-uuid"}, id="user"),
        pytest.param({"session_id": "not-a-uuid"}, id="session"),
        pytest.param({"evidence": {}}, id="empty_evidence"),
        pytest.param({"evidence": {"ratio": float("nan")}}, id="non_finite_evidence"),
        pytest.param({"evidence": {"when": _CORRECTION_AT}}, id="unserializable_evidence"),
    ],
)
async def test_a_malformed_request_is_refused_before_any_database_work(
    repository: SQLAlchemyRepository,
    overrides: dict[str, object],
) -> None:
    """Identity and evidence are validated before the write transaction opens.

    Given: A request carrying a non-UUID identity, or an evidence envelope that
        is empty, non-finite, or not JSON-serializable.
    When: The annulment is attempted.
    Then: A ``ValueError`` is raised and the manifest is untouched, so a
        malformed operator command never reaches the fence, the ledger, or the
        witness probe.
    """
    s5778_value_1 = _request("e" * 64, **overrides)
    with pytest.raises(ValueError):
        await repository.record_execution_annulment(s5778_value_1)

    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == []


async def test_the_manifest_read_normalizes_wallets_orders_and_narrows_by_exchange(
    repository: SQLAlchemyRepository,
) -> None:
    """The manifest read is scope-ordered, alias-tolerant, and venue-narrowable.

    Given: Two committed annulments in one paper scope and one in the main
        wallet's Kraken scope.
    When: The manifest is read for the paper wallet with an upper-cased wallet
        spelling, then narrowed to a single exchange with a mixed-case venue
        spelling, then read for a wallet with no corrections.
    Then: Rows come back ordered by ``(exchange, scope_sequence)``, the alias
        wallet and venue spellings resolve to the canonical ones, the narrowed
        read returns the same scope, and an uncorrected wallet returns an empty
        manifest rather than an error.
    """
    phantom_digest = await _expected_digest(repository, _PAPER_PHANTOM)
    legacy_digest = await _expected_digest(repository, _PAPER_LEGACY)
    second = await _record_annulment(
        repository,
        _request(
            legacy_digest,
            target_execution_public_id=_PAPER_LEGACY,
            wallet_public_id=_PAPER_WALLET,
            exchange="paper",
            scope_sequence=2,
            reason="unwitnessed_legacy_lineage",
        ),
    )
    first = await _record_annulment(
        repository,
        _request(
            phantom_digest,
            target_execution_public_id=_PAPER_PHANTOM,
            wallet_public_id=_PAPER_WALLET,
            exchange="paper",
            scope_sequence=1,
        ),
    )

    assert await repository.get_execution_annulments(_PAPER_WALLET.upper(), "live") == [
        first,
        second,
    ]
    assert await repository.get_execution_annulments(_PAPER_WALLET, "live", " Paper ") == [
        first,
        second,
    ]
    assert await repository.get_execution_annulments(_PAPER_WALLET, "live", "kraken") == []
    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == []


async def test_the_manifest_read_refuses_a_malformed_wallet_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """A manifest read cannot be issued for an identity that is not a wallet.

    Given: A wallet spelling that is not a UUID.
    When: The manifest is read for it.
    Then: A ``ValueError`` is raised rather than a silently empty manifest,
        which a caller could otherwise mistake for "this scope has no
        corrections".
    """
    with pytest.raises(ValueError, match="wallet_public_id"):
        await repository.get_execution_annulments("not-a-uuid", "live")


async def test_the_postgresql_protocol_sets_isolation_then_takes_the_scope_fence(
    repository: SQLAlchemyRepository,
) -> None:
    """On PostgreSQL the writer pins isolation first, then fences its own keyspace.

    Given: A repository reporting the ``postgresql`` dialect and a recording
        session.
    When: The transaction opener and the scope fence are invoked in order.
    Then: ``SET TRANSACTION ISOLATION LEVEL READ COMMITTED`` is emitted first,
        then a two-argument ``pg_advisory_xact_lock`` keyed on the
        ``execution_annulment`` domain and the exact scope triple — a keyspace
        disjoint from the execution ingest fence, so annulling a historical row
        never contends with live fill ingest.
    """
    session = AsyncMock()
    command = SQLAlchemyRepository._normalized_execution_annulment_command(_request("f" * 64))
    with patch.object(
        SQLAlchemyRepository,
        "dialect_name",
        new_callable=PropertyMock,
        return_value="postgresql",
    ):
        await repository._begin_execution_annulment_transaction(session)
        await repository._acquire_execution_annulment_fence(session, command)

    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert "pg_advisory_xact_lock(hashtext('execution_annulment'), hashtext(:scope))" in " ".join(
        statements[1].split()
    )
    assert session.execute.await_args_list[1].args[1] == {"scope": f"{_MAIN_WALLET}|kraken|live"}


async def test_an_unknown_dialect_refuses_both_halves_of_the_protocol(
    repository: SQLAlchemyRepository,
) -> None:
    """Neither half of the durability protocol silently weakens on a new backend.

    Given: A repository reporting a dialect the writer has no explicit protocol
        for.
    When: The transaction opener and the scope fence are invoked.
    Then: Both raise ``NotImplementedError`` and no statement is issued — a
        money-truth correction is never appended under unknown transactional
        guarantees.
    """
    session = AsyncMock()
    command = SQLAlchemyRepository._normalized_execution_annulment_command(_request("f" * 64))
    with patch.object(
        SQLAlchemyRepository,
        "dialect_name",
        new_callable=PropertyMock,
        return_value="unknown",
    ):
        with pytest.raises(NotImplementedError, match="write transaction"):
            await repository._begin_execution_annulment_transaction(session)
        with pytest.raises(NotImplementedError, match="fence"):
            await repository._acquire_execution_annulment_fence(session, command)

    session.execute.assert_not_awaited()


async def test_an_observation_integrity_failure_without_a_winner_reraises(
    repository: SQLAlchemyRepository,
) -> None:
    """A collision that is not the idempotent case is never reported as success.

    Given: An observation row already occupying the surrogate-id index for a
        DIFFERENT correction, so the completion collides on that index while the
        public-id index — the one the idempotent re-read looks at — is free.
    When: The correction's observation is completed.
    Then: The raw ``IntegrityError`` is re-raised rather than being reported as
        an already-observed success. Returning quietly here would tell the
        caller a correction is historically knowable when nothing proves it is.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    real_observe = SQLAlchemyRepository.observe_execution_annulment_visibility
    with patch.object(
        SQLAlchemyRepository,
        "observe_execution_annulment_visibility",
        _refuse_observation,
    ):
        row = await _record_annulment(repository, _request(digest))
    async with repository.session() as s:
        annulment = (
            (
                await s.execute(
                    select(ExecutionAnnulment).where(
                        ExecutionAnnulment.public_id == row["public_id"]
                    )
                )
            )
            .scalars()
            .one()
        )
        s.add(
            ExecutionAnnulmentVisibility(
                annulment_public_id="00000000-0000-7000-8000-0000000009fd",
                annulment_id=int(annulment.id),
                observed_at=_CORRECTION_AT,
                wallet_public_id=_MAIN_WALLET,
                exchange="kraken",
                mode="live",
                session_id=_SESSION,
                sequence_id=1,
                timestamp=_CORRECTION_AT,
                known_to=KNOWN_TO_MAX,
            )
        )
        await s.commit()

    with pytest.raises(IntegrityError):
        await real_observe(repository, row["public_id"])


def _annulment_request_for(
    digest: str, target: str, **overrides: object
) -> ExecutionAnnulmentRequest:
    """Build one request for a named target with the paper-wallet defaults."""
    base: dict[str, object] = {
        "target_execution_public_id": target,
        "wallet_public_id": _PAPER_WALLET,
        "exchange": "paper",
    }
    base.update(overrides)
    return _request(digest, **base)


async def test_the_postgresql_writer_locks_venue_events_before_it_looks_for_a_witness(
    repository: SQLAlchemyRepository,
) -> None:
    """The no-witness proof is fenced against the writers that could refute it.

    Given: A repository reporting the ``postgresql`` dialect and a session that
        records every statement the guarded writer issues.
    When: One annulment is recorded end to end.
    Then: The statements are, in order, the isolation pin, the scope advisory
        lock, ``LOCK TABLE venue_events IN SHARE MODE``, and only afterwards the
        ``fill_observed`` witness query. That order is the whole invariant: SHARE
        conflicts with the ROW EXCLUSIVE an INSERT takes, so an in-flight witness
        either committed before the grant and is SEEN by the fresh READ COMMITTED
        statement snapshot, or waits behind this transaction and the correction
        it would contradict is never appended. Issuing the witness query first
        would let an uncommitted fill slip through and poison the manifest
        permanently, because both tables are append-only.
    """
    statements: list[str] = []
    session = AsyncMock()
    session.add = MagicMock()
    target = _execution(_KRAKEN_PHANTOM, "kraken")

    async def execute(statement: object, parameters: object = None) -> MagicMock:
        """Record one statement and answer it with the shape the writer expects."""
        del parameters
        rendered = " ".join(str(statement).split())
        statements.append(rendered)
        result = MagicMock()
        if rendered.startswith("SELECT users."):
            result.scalars.return_value.first.return_value = _user(_USER, "operator", True)
        elif rendered.startswith("SELECT executions."):
            result.scalars.return_value.first.return_value = target
        elif rendered.startswith("SELECT DISTINCT orders.client_order_id"):
            result.scalars.return_value.all.return_value = ["client-kraken-1"]
        else:
            result.scalars.return_value.first.return_value = None
        return result

    session.execute = AsyncMock(side_effect=execute)
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    with (
        patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ),
        patch.object(repository, "session") as session_context,
        patch.object(
            repository,
            "_observe_execution_annulment_durability",
            new=AsyncMock(return_value=None),
        ),
    ):
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        await repository.record_execution_annulment(_request(digest))

    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert "pg_advisory_xact_lock(hashtext('execution_annulment'), hashtext(:scope))" in (
        statements[1]
    )
    assert statements[2] == "LOCK TABLE venue_events IN SHARE MODE"
    witness_positions = [
        index for index, statement in enumerate(statements) if "FROM venue_events" in statement
    ]
    assert witness_positions
    assert min(witness_positions) > 2
    session.commit.assert_awaited_once_with()


async def test_sqlite_needs_no_table_lock_because_its_reservation_serializes_writers(
    repository: SQLAlchemyRepository,
) -> None:
    """The dialect split is in the mechanism, never in the guarantee.

    Given: A repository on SQLite, whose annulment transaction opens with
        ``BEGIN IMMEDIATE``.
    When: The fence is acquired.
    Then: No statement at all is issued. ``BEGIN IMMEDIATE`` already holds the
        database-wide write reservation, so a concurrent witness insert cannot
        even begin — a ``LOCK TABLE`` here would be a syntax error buying nothing.
    """
    session = AsyncMock()
    command = SQLAlchemyRepository._normalized_execution_annulment_command(_request("f" * 64))

    await repository._acquire_execution_annulment_fence(session, command)

    session.execute.assert_not_awaited()


async def test_a_recorded_correction_reports_the_observation_that_completed_it(
    repository: SQLAlchemyRepository,
) -> None:
    """A complete correction is a different VALUE from an incomplete one.

    Given: The ordinary writer, whose second transaction succeeds.
    When: A correction is recorded.
    Then: The result names the appended manifest row, reports
        ``visibility_state='observed'``, and carries the observation bound to
        the correction — so a caller can prove the correction is folded by
        historical horizons without a second query.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)

    result = await repository.record_execution_annulment(_request(digest))

    assert result["visibility_state"] == "observed"
    observation = result["visibility"]
    assert observation is not None
    assert observation["annulment_public_id"] == result["annulment"]["public_id"]
    assert observation["observed_at"] > result["annulment"]["timestamp"]


async def test_a_correction_whose_observation_failed_is_reported_as_pending(
    repository: SQLAlchemyRepository,
) -> None:
    """Success must never be the word for a historically invisible correction.

    Given: A second transaction that fails after the correction has committed.
    When: The correction is recorded.
    Then: The result still carries the durable manifest row — the manifest is
        append-only, so refusing afterwards would be a lie — but it reports
        ``visibility_state='pending'`` and no observation. Returning a bare row
        here was the defect: the caller was told the correction was complete
        while EVERY historical horizon still refused it.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)

    with patch.object(
        SQLAlchemyRepository,
        "observe_execution_annulment_visibility",
        _refuse_observation,
    ):
        result = await repository.record_execution_annulment(_request(digest))

    assert result["visibility_state"] == "pending"
    assert result["visibility"] is None
    assert result["annulment"]["target_execution_public_id"] == _KRAKEN_PHANTOM
    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == [result["annulment"]]


async def _two_unobserved_paper_corrections(
    repository: SQLAlchemyRepository,
) -> tuple[ExecutionAnnulmentRow, ExecutionAnnulmentRow]:
    """Append the legacy correction first, then the phantom, observing neither."""
    legacy_digest = await _expected_digest(repository, _PAPER_LEGACY)
    phantom_digest = await _expected_digest(repository, _PAPER_PHANTOM)
    with patch.object(
        SQLAlchemyRepository,
        "observe_execution_annulment_visibility",
        _refuse_observation,
    ):
        legacy = await repository.record_execution_annulment(
            _annulment_request_for(
                legacy_digest,
                _PAPER_LEGACY,
                scope_sequence=2,
                reason="unwitnessed_legacy_lineage",
            )
        )
        phantom = await repository.record_execution_annulment(
            _annulment_request_for(phantom_digest, _PAPER_PHANTOM, scope_sequence=1)
        )
    return legacy["annulment"], phantom["annulment"]


async def test_the_discovery_read_finds_exactly_the_corrections_no_observation_covers(
    repository: SQLAlchemyRepository,
) -> None:
    """A killed process leaves no log, so discovery cannot be based on logs.

    Given: Two paper corrections appended without observations and one Kraken
        correction appended with its observation.
    When: The unobserved manifest is discovered for the paper scope, then again
        after one of them is completed, then for the Kraken scope and for a mode
        that holds nothing.
    Then: Only the corrections lacking an observation are returned, ordered
        oldest-correction-first rather than by scope coordinate, the completed
        one disappears, and the scope filters hold — an observed correction, a
        different wallet, and a different mode all return nothing.
    """
    legacy, phantom = await _two_unobserved_paper_corrections(repository)
    kraken_digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    await repository.record_execution_annulment(_request(kraken_digest))

    assert await repository.get_unobserved_execution_annulments(_PAPER_WALLET, "live") == [
        legacy,
        phantom,
    ]
    assert await repository.get_execution_annulments(_PAPER_WALLET, "live") == [phantom, legacy]

    await repository.observe_execution_annulment_visibility(legacy["public_id"])

    assert await repository.get_unobserved_execution_annulments(_PAPER_WALLET, "live") == [phantom]
    assert await repository.get_unobserved_execution_annulments(_MAIN_WALLET, "live") == []
    assert await repository.get_unobserved_execution_annulments(_PAPER_WALLET, "paper") == []


async def test_the_discovery_read_is_bounded_and_accepts_an_alias_wallet_spelling(
    repository: SQLAlchemyRepository,
) -> None:
    """An operator surface must never be handed an unbounded result set.

    Given: Two unobserved paper corrections.
    When: The unobserved manifest is discovered with a limit of one, using an
        upper-cased wallet spelling.
    Then: Exactly the oldest correction is returned, and the alias spelling
        resolves to the canonical wallet identity.
    """
    legacy, _ = await _two_unobserved_paper_corrections(repository)

    assert await repository.get_unobserved_execution_annulments(
        _PAPER_WALLET.upper(), "live", 1
    ) == [legacy]


@pytest.mark.parametrize("limit", [0, -1, 1001])
async def test_every_discovery_read_refuses_an_unsupported_bound(
    repository: SQLAlchemyRepository,
    limit: int,
) -> None:
    """A bound outside the supported range is a refusal, never a silent clamp.

    Given: A discovery bound below one or above the hard ceiling.
    When: Either discovery read is issued with it.
    Then: Both raise, so a caller cannot silently receive a different scan than
        the one it asked for.
    """
    with pytest.raises(ValueError, match="discovery limit"):
        await repository.get_unobserved_execution_annulments(_MAIN_WALLET, "live", limit)
    with pytest.raises(ValueError, match="discovery limit"):
        await repository.get_unwitnessed_executions(_MAIN_WALLET, "live", None, limit)


async def test_every_discovery_read_refuses_a_malformed_wallet_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """An unparseable wallet is a refusal, never an empty all-clear.

    Given: A wallet spelling that is not a UUID.
    When: Either discovery read is issued for it.
    Then: Both raise, because an empty result would read as "this scope is
        clean" for a scope that was never actually looked at.
    """
    with pytest.raises(ValueError, match="wallet_public_id"):
        await repository.get_unobserved_execution_annulments("not-a-uuid", "live")
    with pytest.raises(ValueError, match="wallet_public_id"):
        await repository.get_unwitnessed_executions("not-a-uuid", "live")


async def test_the_unwitnessed_read_publishes_the_exact_digest_the_writer_demands(
    repository: SQLAlchemyRepository,
) -> None:
    """What the surface calls annullable is what the guarded writer accepts.

    Given: The measured production shape, in which the Kraken phantom is the one
        unwitnessed row in the main wallet and the genuine Walutomat trade is
        witnessed.
    When: The scope's unwitnessed executions are read, and the digest that read
        published is fed straight to the guarded writer.
    Then: Only the phantom is reported, with its blocking economics (size 0,
        price 0, ``exec_id`` NULL) and no standing correction; the writer accepts
        the published digest unchanged, so an operator copies a value the
        database produced instead of deriving one by hand; and a second read
        shows the row now bound to its correction rather than silently dropping
        it.
    """
    rows = await repository.get_unwitnessed_executions(_MAIN_WALLET, "live")

    assert [row["public_id"] for row in rows] == [_KRAKEN_PHANTOM]
    blocker = rows[0]
    assert blocker["exchange"] == "kraken"
    assert blocker["scope_sequence"] == 1
    assert blocker["exec_id"] is None
    assert blocker["size"] == 0.0
    assert blocker["price"] == 0.0
    assert blocker["timestamp"] == _KRAKEN_PHANTOM_AT
    assert blocker["annulment_public_id"] is None
    assert blocker["canonical_digest"] == await _expected_digest(repository, _KRAKEN_PHANTOM)

    published_digest = blocker["canonical_digest"]
    assert published_digest is not None
    result = await repository.record_execution_annulment(_request(published_digest))

    corrected = await repository.get_unwitnessed_executions(_MAIN_WALLET, "live")
    assert corrected[0]["annulment_public_id"] == result["annulment"]["public_id"]


async def test_the_unwitnessed_read_narrows_by_venue_and_is_bounded(
    repository: SQLAlchemyRepository,
) -> None:
    """The blocker list is scope-ordered, venue-narrowable, and bounded.

    Given: A paper scope holding two unwitnessed rows and one properly witnessed
        fill.
    When: The scope is read unnarrowed, narrowed with a mixed-case venue
        spelling, and read again with a limit of one.
    Then: The witnessed sibling never appears, rows come back ordered by
        ``(exchange, scope_sequence)``, the venue spelling normalizes, and the
        bound truncates rather than being ignored.
    """
    rows = await repository.get_unwitnessed_executions(_PAPER_WALLET, "live")

    assert [(row["exchange"], row["scope_sequence"]) for row in rows] == [
        ("paper", 1),
        ("paper", 2),
    ]
    assert _PAPER_WITNESSED not in {row["public_id"] for row in rows}
    assert await repository.get_unwitnessed_executions(_PAPER_WALLET, "live", " Paper ") == rows
    assert await repository.get_unwitnessed_executions(_PAPER_WALLET, "live", "kraken") == []
    assert await repository.get_unwitnessed_executions(_PAPER_WALLET, "live", None, 1) == rows[:1]


async def test_an_uncanonicalizable_blocker_is_listed_without_a_digest(
    repository: SQLAlchemyRepository,
) -> None:
    """One unserializable row must not hide every other blocker in the scope.

    Given: An unwitnessed execution carrying an operator identity that is not a
        UUID, so its canonical bytes cannot be produced at all.
    When: The scope's unwitnessed executions are read.
    Then: The row is listed with ``canonical_digest=None`` instead of raising.
        That is the honest answer — the guarded writer refuses such a target with
        ``uncanonicalizable_execution_target`` and no digest would change it —
        and the Kraken phantom beside it stays visible, which a crash would have
        hidden.
    """
    async with repository.session() as s:
        s.add(
            _execution(
                "00000000-0000-7000-8000-000000000e06",
                "kraken",
                scope_sequence=2,
                operator_public_id="not-a-uuid",
            )
        )
        await s.commit()

    rows = await repository.get_unwitnessed_executions(_MAIN_WALLET, "live", "kraken")

    assert [row["canonical_digest"] is None for row in rows] == [False, True]
    assert [row["scope_sequence"] for row in rows] == [1, 2]
