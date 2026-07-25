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
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.execution_chain import ExecutionChainRecord
from snapper.application.portfolio.execution_chain import execution_row_digest
from snapper.core.json_types import JsonObject
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import Order
from snapper.data.models import VenueEvent
from snapper.data.repository import ExecutionAnnulmentConflictError
from snapper.data.repository import ExecutionAnnulmentTargetError
from snapper.data.repository import ExecutionAnnulmentWitnessedError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _ExecutionAnnulmentCommand
from snapper.data.repository_types import ExecutionAnnulmentRequest

_SESSION = "00000000-0000-7000-8000-000000000901"
_USER = "0000face-0000-7000-8000-0000000000d1"
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
        operator_public_id=None,
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


def _production_lineage() -> list[Order | Execution | VenueEvent]:
    """Build the measured production shape: five executions, two witnesses.

    ``main/kraken/live`` seq 1 is the phantom that blocks the only real trading
    wallet, and it is deliberately LATER than ``main/walutomat/live`` seq 1, the
    genuine Walutomat trade. ``paper/paper/live`` holds the pricing-bug phantom,
    the row predating durable fill lineage, and one properly witnessed fill.
    """
    return [
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


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create a repository holding the measured production annulment lineage."""
    db_path = tmp_path / "execution-annulments.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Order.__table__.create(schema_engine)
    Execution.__table__.create(schema_engine)
    VenueEvent.__table__.create(schema_engine)
    ExecutionAnnulment.__table__.create(schema_engine)
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

    row = await repository.record_execution_annulment(_request(digest, correction_time=backdated))

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
        row = await repository.record_execution_annulment(_request(digest))

    assert fence_released
    assert row["timestamp"] >= fence_released[0]


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

    row = await repository.record_execution_annulment(_request(digest))

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
    winner = await repository.record_execution_annulment(_request(digest))

    with pytest.raises(ExecutionAnnulmentConflictError) as conflict:
        await repository.record_execution_annulment(
            _request(digest, sequence_id=2, correction_time=_CORRECTION_AT)
        )

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

    with pytest.raises(ExecutionAnnulmentWitnessedError, match="witnessed_execution_target"):
        await repository.record_execution_annulment(
            _request(
                digest,
                target_execution_public_id=_WALUTOMAT_TRADE,
                exchange="walutomat",
            )
        )

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

    row = await repository.record_execution_annulment(
        _request(
            digest,
            target_execution_public_id=_PAPER_LEGACY,
            wallet_public_id=_PAPER_WALLET,
            exchange="paper",
            scope_sequence=2,
            reason="unwitnessed_legacy_lineage",
        )
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

    row = await repository.record_execution_annulment(
        _request(
            phantom_digest,
            target_execution_public_id=_PAPER_PHANTOM,
            scope_sequence=1,
            **common,
        )
    )

    with pytest.raises(ExecutionAnnulmentWitnessedError):
        await repository.record_execution_annulment(
            _request(
                witnessed_digest,
                target_execution_public_id=_PAPER_WITNESSED,
                scope_sequence=3,
                **common,
            )
        )

    assert await repository.get_execution_annulments(_PAPER_WALLET, "live") == [row]


async def test_an_unknown_target_is_refused(repository: SQLAlchemyRepository) -> None:
    """An annulment of an execution that does not exist writes nothing.

    Given: A well-formed request naming an execution id no ledger row carries.
    When: The annulment is attempted.
    Then: It is refused with ``unknown_execution_target`` and the manifest stays
        empty.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)

    with pytest.raises(ExecutionAnnulmentTargetError, match="unknown_execution_target"):
        await repository.record_execution_annulment(
            _request(
                digest,
                target_execution_public_id="00000000-0000-7000-8000-0000000009ff",
            )
        )

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

    with pytest.raises(ExecutionAnnulmentTargetError, match="crossed_execution_annulment_scope"):
        await repository.record_execution_annulment(_request(digest, **overrides))


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

    with pytest.raises(ExecutionAnnulmentTargetError, match="crossed_execution_annulment_scope"):
        await repository.record_execution_annulment(
            _request(
                "c" * 64,
                target_execution_public_id=corrupt,
                scope_sequence=2,
            )
        )


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

    with pytest.raises(ExecutionAnnulmentTargetError, match="uncanonicalizable_execution_target"):
        await repository.record_execution_annulment(
            _request(
                "c" * 64,
                target_execution_public_id=uncanonical,
                scope_sequence=3,
            )
        )


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

    with pytest.raises(ExecutionAnnulmentTargetError, match="execution_digest_mismatch"):
        await repository.record_execution_annulment(_request(other_digest))

    assert await repository.get_execution_annulments(_MAIN_WALLET, "live") == []


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

    with pytest.raises(IntegrityError):
        await repository.record_execution_annulment(_request(digest))


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
    with pytest.raises(ValueError):
        await repository.record_execution_annulment(_request("e" * 64, **overrides))

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
    second = await repository.record_execution_annulment(
        _request(
            legacy_digest,
            target_execution_public_id=_PAPER_LEGACY,
            wallet_public_id=_PAPER_WALLET,
            exchange="paper",
            scope_sequence=2,
            reason="unwitnessed_legacy_lineage",
        )
    )
    first = await repository.record_execution_annulment(
        _request(
            phantom_digest,
            target_execution_public_id=_PAPER_PHANTOM,
            wallet_public_id=_PAPER_WALLET,
            exchange="paper",
            scope_sequence=1,
        )
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
