"""Tests for the effective-prefix certification that folds the annulment manifest.

*Effective accounting history is the deterministic fold of a physically
immutable contiguous execution prefix and a uniquely targeted, physically
immutable annulment manifest.* These tests exercise that theorem end to end
against the MEASURED production shape (2026-07-25): five live executions
against exactly two ``fill_observed`` witnesses, with the Kraken phantom
(size 0, price 0, ``exec_id`` NULL, 21:54) recorded LATER than the only genuine
money trade (Walutomat, 20.04 @ 4.3836, 21:22). That ordering is what killed
the forward-activation-cut option, so a fixture that puts the phantom first
would not exercise the real case at all.
"""

import inspect
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
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
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.execution_chain import ExecutionChainRecord
from snapper.application.portfolio.execution_chain import execution_chain_genesis
from snapper.application.portfolio.execution_chain import execution_row_digest
from snapper.core.json_types import JsonObject
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import ExecutionAnnulmentVisibility
from snapper.data.models import ExecutionPlanCheckpoint
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import Position
from snapper.data.models import Symbol
from snapper.data.models import TradeProjectionCheckpoint
from snapper.data.models import VenueEvent
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _PnlTimelineExecutionPrefixSource
from snapper.data.repository_types import ExecutionAnnulmentRequest
from snapper.data.repository_types import ExecutionAnnulmentRow
from snapper.data.repository_types import PnlTimelineExecutionPrefix

_SESSION = "00000000-0000-7000-8000-000000000901"
_USER = "0000face-0000-7000-8000-0000000000d1"
_MAIN_WALLET = "0000face-0000-7000-8000-0000000000a1"
_PAPER_WALLET = "0000face-0000-7000-8000-0000000000a2"

_WALUTOMAT_TRADE_AT = datetime(2026, 7, 19, 21, 22, tzinfo=UTC)
_KRAKEN_PHANTOM_AT = datetime(2026, 7, 19, 21, 54, tzinfo=UTC)
_PAPER_AT = datetime(2026, 7, 10, 9, 0, tzinfo=UTC)
_CORRECTION_AT = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
"""The instant the OPERATOR declares a correction from — audit metadata only."""

_AS_OF = datetime.now(UTC) + timedelta(hours=1)
"""A horizon later than any knowledge stamp the writer can assign in this run.

The fold keys on the manifest row's SERVER timestamp, which the writer takes
from the real clock, so a fixed literal horizon would silently stop covering
the corrections these tests record as soon as the wall clock passed it. Every
"before the correction" horizon is derived from the recorded row instead."""

_EURPLN_SYMBOL = "00000000-0000-7000-8000-000000000a01"
_BTCUSD_SYMBOL = "00000000-0000-7000-8000-000000000a02"
_WALUTOMAT_INSTRUMENT = "00000000-0000-7000-8000-000000000b01"
_KRAKEN_INSTRUMENT = "00000000-0000-7000-8000-000000000b02"
_PAPER_INSTRUMENT = "00000000-0000-7000-8000-000000000b03"

_KRAKEN_PHANTOM = "00000000-0000-7000-8000-000000000e01"
_WALUTOMAT_TRADE = "00000000-0000-7000-8000-000000000e02"
_PAPER_PHANTOM = "00000000-0000-7000-8000-000000000e03"
_PAPER_LEGACY = "00000000-0000-7000-8000-000000000e04"
_PAPER_WITNESSED = "00000000-0000-7000-8000-000000000e05"
_ABSENT_EXECUTION = "00000000-0000-7000-8000-000000000eff"

_KRAKEN_ORDER = "00000000-0000-7000-8000-000000000c01"
_WALUTOMAT_ORDER = "00000000-0000-7000-8000-000000000c02"
_PAPER_PHANTOM_ORDER = "00000000-0000-7000-8000-000000000c03"
_PAPER_LEGACY_ORDER = "00000000-0000-7000-8000-000000000c04"
_PAPER_WITNESSED_ORDER = "00000000-0000-7000-8000-000000000c05"

_KRAKEN_CID = "client-kraken-1"
_WALUTOMAT_CID = "client-walutomat-1"
_PAPER_PHANTOM_CID = "client-paper-1"
_PAPER_LEGACY_CID = "client-paper-2"
_PAPER_WITNESSED_CID = "client-paper-3"

_ANNULMENT_PUBLIC_ID = "00000000-0000-7000-8000-0000000000d9"

_EVIDENCE: JsonObject = {
    "diagnosis": "size 0 / price 0 residue of the resting-order open-closed mismap",
    "fixed_in_commit": "13a6a397",
    "fill_observed_witnesses": 0,
}


def _symbol(public_id: str, native_symbol: str, base: str, quote: str) -> Symbol:
    """Build one sentinel-current Symbol identity."""
    return Symbol(
        public_id=public_id,
        native_symbol=native_symbol,
        base=base,
        quote=quote,
        asset_type="crypto",
        created_at=_PAPER_AT,
        timestamp=_PAPER_AT,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _instrument(public_id: str, exchange: str, symbol_public_id: str) -> Instrument:
    """Build one sentinel-current Instrument identity for a venue."""
    return Instrument(
        public_id=public_id,
        symbol_public_id=symbol_public_id,
        exchange=exchange,
        timestamp=_PAPER_AT,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


def _order(
    public_id: str,
    client_order_id: str,
    wallet_public_id: str,
    instrument_public_id: str,
) -> Order:
    """Build one sentinel-current Order lineage row for an execution."""
    return Order(
        public_id=public_id,
        instrument_public_id=instrument_public_id,
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

    wallet_public_id: str
    scope_sequence: int
    timestamp: datetime
    price: float
    size: float
    exec_id: str | None


def _execution(
    public_id: str,
    exchange: str,
    order_public_id: str,
    **options: Unpack[_ExecutionOptions],
) -> Execution:
    """Build one committed execution with explicit immutable scope coordinates."""
    scope_sequence = options.get("scope_sequence", 1)
    return Execution(
        public_id=public_id,
        order_public_id=order_public_id,
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


class _FillOptions(TypedDict, total=False):
    """Optional fields accepted by the fill-witness test-row builder."""

    fill_size: float
    fill_price: float
    exec_id: str | None
    timestamp: datetime


def _fill_event(
    public_id: str,
    client_order_id: str,
    wallet_public_id: str,
    exchange: str,
    native_symbol: str,
    **options: Unpack[_FillOptions],
) -> VenueEvent:
    """Build one append-only ``fill_observed`` witness carrying durable shard lineage."""
    timestamp = options.get("timestamp", _WALUTOMAT_TRADE_AT)
    fill_size = options.get("fill_size", 20.04)
    return VenueEvent(
        public_id=public_id,
        event_type="fill_observed",
        shard_key=f"{exchange}.{native_symbol}.live",
        wallet_public_id=wallet_public_id,
        command_public_id=None,
        exchange=exchange,
        instrument=native_symbol,
        mode="live",
        exchange_order_id=None,
        client_order_id=client_order_id,
        venue_client_id=client_order_id,
        side="buy",
        status="filled",
        fill_price=options.get("fill_price", 4.3836),
        fill_size=fill_size,
        cum_fill_size=fill_size,
        fee=0.0,
        fee_asset="PLN",
        exec_id=options.get("exec_id", f"recon-{client_order_id}"),
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


def _identity_lineage() -> list[Symbol | Instrument | Order]:
    """Build the symbol, instrument, and order identities every scope resolves through."""
    return [
        _symbol(_EURPLN_SYMBOL, "EUR-PLN", "EUR", "PLN"),
        _symbol(_BTCUSD_SYMBOL, "BTC-USD", "BTC", "USD"),
        _instrument(_WALUTOMAT_INSTRUMENT, "walutomat", _EURPLN_SYMBOL),
        _instrument(_KRAKEN_INSTRUMENT, "kraken", _BTCUSD_SYMBOL),
        _instrument(_PAPER_INSTRUMENT, "paper", _BTCUSD_SYMBOL),
        _order(_KRAKEN_ORDER, _KRAKEN_CID, _MAIN_WALLET, _KRAKEN_INSTRUMENT),
        _order(_WALUTOMAT_ORDER, _WALUTOMAT_CID, _MAIN_WALLET, _WALUTOMAT_INSTRUMENT),
        _order(_PAPER_PHANTOM_ORDER, _PAPER_PHANTOM_CID, _PAPER_WALLET, _PAPER_INSTRUMENT),
        _order(_PAPER_LEGACY_ORDER, _PAPER_LEGACY_CID, _PAPER_WALLET, _PAPER_INSTRUMENT),
        _order(_PAPER_WITNESSED_ORDER, _PAPER_WITNESSED_CID, _PAPER_WALLET, _PAPER_INSTRUMENT),
    ]


def _walutomat_trade() -> Execution:
    """Build the only genuine money trade in the measured production ledger."""
    return _execution(
        _WALUTOMAT_TRADE,
        "walutomat",
        _WALUTOMAT_ORDER,
        timestamp=_WALUTOMAT_TRADE_AT,
        price=4.3836,
        size=20.04,
        exec_id=f"recon-{_WALUTOMAT_CID}",
    )


def _kraken_phantom() -> Execution:
    """Build the size-0 price-0 phantom that POSTDATES the real Walutomat trade."""
    return _execution(_KRAKEN_PHANTOM, "kraken", _KRAKEN_ORDER)


def _paper_executions() -> list[Execution]:
    """Build the paper scope: two unwitnessed phantoms and one witnessed fill."""
    return [
        _execution(
            _PAPER_PHANTOM,
            "paper",
            _PAPER_PHANTOM_ORDER,
            wallet_public_id=_PAPER_WALLET,
            timestamp=_PAPER_AT,
            size=1.0,
            exec_id="paper-1",
        ),
        _execution(
            _PAPER_LEGACY,
            "paper",
            _PAPER_LEGACY_ORDER,
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
            _PAPER_WITNESSED_ORDER,
            wallet_public_id=_PAPER_WALLET,
            scope_sequence=3,
            timestamp=_PAPER_AT,
            price=100.0,
            size=1.0,
            exec_id="paper-3",
        ),
    ]


def _witnesses() -> list[VenueEvent]:
    """Build the two durable witnesses the measured production ledger actually has."""
    return [
        _fill_event(
            "00000000-0000-7000-8000-000000000f01",
            _WALUTOMAT_CID,
            _MAIN_WALLET,
            "walutomat",
            "EUR-PLN",
        ),
        _fill_event(
            "00000000-0000-7000-8000-000000000f02",
            _PAPER_WITNESSED_CID,
            _PAPER_WALLET,
            "paper",
            "BTC-USD",
            fill_size=1.0,
            fill_price=100.0,
            exec_id="paper-3",
            timestamp=_PAPER_AT,
        ),
    ]


def _production_lineage() -> list[Symbol | Instrument | Order | Execution | VenueEvent]:
    """Build the measured production shape: five executions, two witnesses."""
    return [
        *_identity_lineage(),
        _walutomat_trade(),
        _kraken_phantom(),
        *_paper_executions(),
        *_witnesses(),
    ]


def _uncorrupted_lineage() -> list[Symbol | Instrument | Order | Execution | VenueEvent]:
    """Build the counterfactual ledger the phantom was never booked into."""
    return [
        *_identity_lineage(),
        _walutomat_trade(),
        *_paper_executions(),
        *_witnesses(),
    ]


async def _build_repository(
    db_path: Path,
    lineage: list[Symbol | Instrument | Order | Execution | VenueEvent],
) -> SQLAlchemyRepository:
    """Create an isolated repository holding one execution-lineage fixture."""
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Symbol.__table__.create(schema_engine)
    Instrument.__table__.create(schema_engine)
    Order.__table__.create(schema_engine)
    Execution.__table__.create(schema_engine)
    ExecutionAnnulment.__table__.create(schema_engine)
    ExecutionAnnulmentVisibility.__table__.create(schema_engine)
    ExecutionPlanCheckpoint.__table__.create(schema_engine)
    PortfolioPnlPoint.__table__.create(schema_engine)
    Position.__table__.create(schema_engine)
    TradeProjectionCheckpoint.__table__.create(schema_engine)
    VenueEvent.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as s:
        s.add_all(lineage)
        await s.commit()
    return repo


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create a repository holding the measured production certification lineage."""
    repo = await _build_repository(tmp_path / "annulment-certification.db", _production_lineage())
    try:
        yield repo
    finally:
        await repo.engine.dispose()


@pytest.fixture()
async def uncorrupted_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create the counterfactual repository whose ledger never held the phantom."""
    repo = await _build_repository(tmp_path / "annulment-counterfactual.db", _uncorrupted_lineage())
    try:
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


async def _annul_kraken_phantom(
    repository: SQLAlchemyRepository,
) -> ExecutionAnnulmentRow:
    """Record the one production correction and return its persisted row.

    The row carries the SERVER-stamped knowledge instant, which is the only
    honest way for a test to name a horizon before or after the correction.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    return await repository.record_execution_annulment(_request(digest))


class _ManifestOptions(TypedDict, total=False):
    """Optional fields accepted by the raw manifest-row builder."""

    public_id: str
    target_execution_public_id: str
    target_execution_digest: str
    exchange: str
    scope_sequence: int
    wallet_public_id: str
    correction_time: datetime
    timestamp: datetime


def _manifest_row(**options: Unpack[_ManifestOptions]) -> ExecutionAnnulment:
    """Build one manifest row DIRECTLY, bypassing the guarded writer's proofs.

    Read-time binding validation must hold against any stored row, including
    rows the writer would never have produced, so these tests construct the
    manifest by hand rather than through ``record_execution_annulment``.
    """
    return ExecutionAnnulment(
        public_id=options.get("public_id", _ANNULMENT_PUBLIC_ID),
        target_execution_public_id=options.get("target_execution_public_id", _KRAKEN_PHANTOM),
        target_execution_digest=options.get("target_execution_digest", "0" * 64),
        wallet_public_id=options.get("wallet_public_id", _MAIN_WALLET),
        exchange=options.get("exchange", "kraken"),
        mode="live",
        scope_sequence=options.get("scope_sequence", 1),
        annulled_by_user_public_id=_USER,
        correction_time=options.get("correction_time", _CORRECTION_AT),
        reason="unwitnessed_phantom",
        evidence_json='{"diagnosis":"hand-built"}',
        session_id=_SESSION,
        sequence_id=1,
        timestamp=options.get("timestamp", _CORRECTION_AT),
        known_to=KNOWN_TO_MAX,
    )


def _visibility_row(
    observed_at: datetime,
    annulment_public_id: str = _ANNULMENT_PUBLIC_ID,
) -> ExecutionAnnulmentVisibility:
    """Build one durability observation for a hand-built manifest row.

    Appended directly so a test can choose the proven instant; the real writer
    stamps it from the clock after re-reading the committed correction.
    """
    return ExecutionAnnulmentVisibility(
        annulment_public_id=annulment_public_id,
        annulment_id=1,
        observed_at=observed_at,
        wallet_public_id=_MAIN_WALLET,
        exchange="kraken",
        mode="live",
        session_id=_SESSION,
        sequence_id=1,
        timestamp=observed_at,
        known_to=KNOWN_TO_MAX,
    )


async def _observed_at(
    repository: SQLAlchemyRepository,
    annulment_public_id: str,
) -> datetime:
    """Return the instant one correction was PROVEN durable."""
    async with repository.session() as s:
        observation = (
            (
                await s.execute(
                    select(ExecutionAnnulmentVisibility).where(
                        ExecutionAnnulmentVisibility.annulment_public_id == annulment_public_id
                    )
                )
            )
            .scalars()
            .one()
        )
        return observation.observed_at


async def _certify_main_live_with(
    repository: SQLAlchemyRepository,
    annulment_rows: list[ExecutionAnnulment],
) -> None:
    """Certify ``main``/``live`` against a hand-built manifest and its observations.

    Each stored correction gets a durability observation well before the read
    horizon, because binding validation only runs on corrections the horizon can
    PROVE were durable — an unobserved row is withheld before its binding is
    ever examined, which would make these refusal tests pass vacuously.
    """
    async with repository.session() as s:
        s.add_all(annulment_rows)
        for annulment in annulment_rows:
            s.add(
                _visibility_row(
                    _CORRECTION_AT,
                    annulment_public_id=annulment.public_id,
                )
            )
        await s.commit()
    await repository.get_pnl_timeline_execution_prefix(_MAIN_WALLET, "live", _AS_OF)


async def test_main_live_still_refuses_the_unannulled_phantom(
    repository: SQLAlchemyRepository,
) -> None:
    """The production block is reproduced exactly, and the manifest changes nothing yet.

    Given: The measured production ledger with no correction recorded.
    When: The ``main``/``live`` opening prefix is certified.
    Then: It fails closed with ``missing_execution_shard_lineage`` — the phantom
        carries no durable fill witness and an un-annulled unwitnessed booking
        is still unprovable. This pins the behaviour the manifest must not
        change for anything it does not repudiate.
    """
    with pytest.raises(ExecutionChainError, match="missing_execution_shard_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_MAIN_WALLET, "live", _AS_OF)


async def test_kraken_annulment_certifies_the_real_trade_unchanged(
    repository: SQLAlchemyRepository,
    uncorrupted_repository: SQLAlchemyRepository,
) -> None:
    """One correction unblocks the scope and reproduces the counterfactual exactly.

    Given: The measured production ledger, blocked by the Kraken phantom.
    When: That single phantom is annulled through the guarded writer and the
        ``main``/``live`` prefix is certified.
    Then: The prefix proves; its only effective row is the genuine Walutomat
        trade; the applied correction is disclosed with its target id, canonical
        digest, scope coordinate, reason, and correction time; and the certified
        economics are BYTE-IDENTICAL to certifying a ledger the phantom was
        never booked into. The raw physical proof is untouched — the Kraken
        watermark still stands at 1 because the phantom is still there.
    """
    correction = await _annul_kraken_phantom(repository)
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)

    prefix = await repository.get_pnl_timeline_execution_prefix(_MAIN_WALLET, "live", _AS_OF)
    counterfactual = await uncorrupted_repository.get_pnl_timeline_execution_prefix(
        _MAIN_WALLET,
        "live",
        _AS_OF,
    )

    assert prefix["watermarks"] == {"kraken": 1, "walutomat": 1}
    assert counterfactual["watermarks"] == {"walutomat": 1}
    assert prefix["executions"] == counterfactual["executions"]
    assert [row["public_id"] for row in prefix["executions"]] == [_WALUTOMAT_TRADE]
    assert prefix["executions"][0]["size"] == 20.04
    assert prefix["executions"][0]["price"] == 4.3836
    assert prefix["annulments"] == [
        {
            "public_id": correction["public_id"],
            "target_execution_public_id": _KRAKEN_PHANTOM,
            "target_execution_digest": digest,
            "exchange": "kraken",
            "scope_sequence": 1,
            "reason": "unwitnessed_phantom",
            "correction_time": _CORRECTION_AT,
        }
    ]
    assert counterfactual["annulments"] == []


async def test_a_horizon_before_the_correction_keeps_failing(
    repository: SQLAlchemyRepository,
) -> None:
    """A correction the past did not know about cannot retroactively unblock it.

    Given: The Kraken phantom annulled, with the operator's declared
        ``correction_time`` BACKDATED years before the write — the shape of a
        request that would like an already-answered historical read redone.
    When: The scope is certified at a horizon before the SERVER knowledge stamp
        but long after the backdated declaration.
    Then: The certification fails exactly as it did before the operator acted.
        The fold keys on the instant the writer recorded, which no caller can
        set, so the past cannot silently acquire a correction; the same scope at
        a horizon past that stamp proves.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    correction = await repository.record_execution_annulment(
        _request(digest, correction_time=datetime(2020, 1, 1, tzinfo=UTC))
    )

    with pytest.raises(ExecutionChainError, match="missing_execution_shard_lineage"):
        await repository.get_pnl_timeline_execution_prefix(
            _MAIN_WALLET,
            "live",
            await _observed_at(repository, correction["public_id"]) - timedelta(seconds=1),
        )

    proven = await repository.get_pnl_timeline_execution_prefix(_MAIN_WALLET, "live", _AS_OF)
    assert [row["public_id"] for row in proven["executions"]] == [_WALUTOMAT_TRADE]


async def test_both_paper_phantoms_annulled_leave_the_witnessed_fill(
    repository: SQLAlchemyRepository,
) -> None:
    """The paper scope certifies on its one real fill once both phantoms are repudiated.

    Given: ``paper``/``live`` holding the pricing-bug phantom at sequence 1, the
        row predating durable fill lineage at sequence 2, and one properly
        witnessed fill at sequence 3.
    When: Both unwitnessed bookings are annulled and the scope is certified.
    Then: The contiguous raw range ``[1, 3]`` still proves physically, and the
        effective history is the witnessed fill alone with both corrections
        disclosed in scope-coordinate order.
    """
    with pytest.raises(ExecutionChainError, match="missing_execution_shard_lineage"):
        await repository.get_pnl_timeline_execution_prefix(_PAPER_WALLET, "live", _AS_OF)

    for target, sequence, reason in (
        (_PAPER_PHANTOM, 1, "unwitnessed_phantom"),
        (_PAPER_LEGACY, 2, "unwitnessed_legacy_lineage"),
    ):
        await repository.record_execution_annulment(
            _request(
                await _expected_digest(repository, target),
                target_execution_public_id=target,
                wallet_public_id=_PAPER_WALLET,
                exchange="paper",
                scope_sequence=sequence,
                reason=reason,
                sequence_id=sequence,
            )
        )

    prefix = await repository.get_pnl_timeline_execution_prefix(_PAPER_WALLET, "live", _AS_OF)

    assert prefix["watermarks"] == {"paper": 3}
    assert [row["public_id"] for row in prefix["executions"]] == [_PAPER_WITNESSED]
    assert [row["scope_sequence"] for row in prefix["annulments"]] == [1, 2]
    assert [row["reason"] for row in prefix["annulments"]] == [
        "unwitnessed_phantom",
        "unwitnessed_legacy_lineage",
    ]


async def test_a_witness_under_a_superseded_client_id_still_contradicts(
    repository: SQLAlchemyRepository,
) -> None:
    """The read side asks the writer's question with the writer's exact width.

    Given: The Kraken phantom annulled, then its order RE-VERSIONED under a new
        client id — the SCD2 shape a re-ACK or a client-id rotation leaves — and
        a durable fill delivered under the now-superseded spelling.
    When: The scope is certified.
    Then: The contradiction still fails closed. The writer counts every
        historical Order version's client id as witness-binding evidence, so a
        reader that only knew the sentinel-current spelling would let exactly
        this fill slip past the refusal the writer's symmetric check exists to
        guarantee.
    """
    await _annul_kraken_phantom(repository)
    async with repository.session() as s:
        superseded = (
            (await s.execute(select(Order).where(Order.public_id == _KRAKEN_ORDER))).scalars().one()
        )
        superseded.known_to = _AS_OF
        s.add(
            _order(
                _KRAKEN_ORDER,
                "client-kraken-rotated",
                _MAIN_WALLET,
                _KRAKEN_INSTRUMENT,
            )
        )
        s.add(
            _fill_event(
                "00000000-0000-7000-8000-000000000f04",
                _KRAKEN_CID,
                _MAIN_WALLET,
                "kraken",
                "BTC-USD",
                fill_size=1.0,
                fill_price=100.0,
                timestamp=_AS_OF,
            )
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="annulled_execution_witnessed"):
        await repository.get_pnl_timeline_execution_prefix(_MAIN_WALLET, "live", _AS_OF)


async def test_a_witness_beyond_the_read_horizon_still_contradicts(
    repository: SQLAlchemyRepository,
) -> None:
    """Contradiction evidence takes no horizon cut, exactly as the writer takes none.

    Given: The Kraken phantom annulled, and a durable fill for its order
        delivered LATER than the horizon being certified.
    When: The scope is certified at that earlier horizon.
    Then: It still fails closed. A fill from any instant proves the booking was
        real, and narrowing this probe to the read's horizon would let a
        certification keep suppressing a booking the venue has already
        confirmed — the economic assignment read is horizon-bounded, this one
        deliberately is not.
    """
    correction = await _annul_kraken_phantom(repository)
    async with repository.session() as s:
        s.add(
            _fill_event(
                "00000000-0000-7000-8000-000000000f05",
                _KRAKEN_CID,
                _MAIN_WALLET,
                "kraken",
                "BTC-USD",
                fill_size=1.0,
                fill_price=100.0,
                timestamp=_AS_OF + timedelta(days=1),
            )
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="annulled_execution_witnessed"):
        await repository.get_pnl_timeline_execution_prefix(
            _MAIN_WALLET,
            "live",
            await _observed_at(repository, correction["public_id"]) + timedelta(seconds=1),
        )


async def test_a_later_witness_contradicting_an_annulment_fails_closed(
    repository: SQLAlchemyRepository,
) -> None:
    """A fill that arrives after the correction refuses the whole certification.

    Given: The Kraken phantom annulled while no ``fill_observed`` row matched
        it, which is all the append-only writer could ever prove.
    When: A durable fill witness for that order arrives afterwards and the scope
        is certified.
    Then: The read side fails closed by name rather than continuing to suppress
        money the venue confirmed — the manifest cannot be withdrawn, so the
        contradiction has to surface at every horizon that sees the witness.
    """
    await _annul_kraken_phantom(repository)

    async with repository.session() as s:
        s.add(
            _fill_event(
                "00000000-0000-7000-8000-000000000f03",
                _KRAKEN_CID,
                _MAIN_WALLET,
                "kraken",
                "BTC-USD",
                fill_size=1.0,
                fill_price=100.0,
                timestamp=_AS_OF,
            )
        )
        await s.commit()

    with pytest.raises(ExecutionChainError, match="annulled_execution_witnessed"):
        await repository.get_pnl_timeline_execution_prefix(_MAIN_WALLET, "live", _AS_OF)


async def test_a_manifest_row_naming_a_row_outside_its_slot_is_dangling(
    repository: SQLAlchemyRepository,
) -> None:
    """A binding whose claimed slot holds a different row refuses certification.

    Given: A manifest row claiming ``kraken`` sequence 1 — a coordinate this
        proven prefix does occupy — while naming an execution the prefix does
        not contain.
    When: The scope is certified.
    Then: The fold refuses by name instead of ignoring the row, because a
        correction that binds to nothing must never be silently discarded.
    """
    with pytest.raises(ExecutionChainError, match="dangling_annulment_binding"):
        await _certify_main_live_with(
            repository,
            [_manifest_row(target_execution_public_id=_ABSENT_EXECUTION)],
        )


async def test_a_manifest_row_crossing_its_target_scope_refuses(
    repository: SQLAlchemyRepository,
) -> None:
    """A correction whose coordinate misses its own target refuses certification.

    Given: A manifest row naming the in-prefix Kraken phantom while claiming a
        scope coordinate beyond every proven watermark.
    When: The scope is certified.
    Then: The fold refuses rather than treating the row as out of this cut —
        otherwise a coordinate typo would silently fail to exclude a booking the
        operator did repudiate.
    """
    with pytest.raises(ExecutionChainError, match="crossed_annulment_scope"):
        await _certify_main_live_with(repository, [_manifest_row(scope_sequence=9)])


async def test_a_manifest_row_beyond_the_cut_is_simply_not_this_fold(
    repository: SQLAlchemyRepository,
) -> None:
    """A correction for a row outside this cut neither applies nor refuses.

    Given: A manifest row at a coordinate above every proven watermark whose
        target is not in this prefix at all.
    When: The scope is certified.
    Then: The row is ignored as belonging to a later cut, and the certification
        returns to its uncorrected verdict — the Kraken phantom still blocks it.
    """
    with pytest.raises(ExecutionChainError, match="missing_execution_shard_lineage"):
        await _certify_main_live_with(
            repository,
            [
                _manifest_row(
                    target_execution_public_id=_ABSENT_EXECUTION,
                    scope_sequence=9,
                )
            ],
        )


async def test_a_manifest_row_whose_digest_disagrees_refuses(
    repository: SQLAlchemyRepository,
) -> None:
    """A correction authorized for different row content is never honoured.

    Given: A manifest row correctly bound to the Kraken phantom by id and scope
        coordinate but carrying a digest that is not the stored row's canonical
        digest.
    When: The scope is certified.
    Then: The fold recomputes the digest from the row it actually loaded and
        refuses, so a repudiation can only ever suppress the exact content an
        operator inspected.
    """
    with pytest.raises(ExecutionChainError, match="annulment_digest_mismatch"):
        await _certify_main_live_with(repository, [_manifest_row()])


def test_two_manifest_rows_for_one_target_refuse_defensively() -> None:
    """A forked correction history refuses even though it cannot be written.

    Given: Two manifest rows repudiating the same execution — impossible through
        the TOTAL ``uq_execution_annulments_target`` index, so this is stated
        against the pure certification stage directly.
    When: The prefix is certified with both.
    Then: The fold refuses. A read that met a duplicate would be reading a
        ledger whose uniqueness guarantee no longer holds, and continuing would
        mean trusting one of two contradictory operator acts at random.
    """
    execution = _kraken_phantom()
    digest = execution_row_digest(SQLAlchemyRepository._execution_chain_record(execution))
    manifest = [
        _manifest_row(target_execution_digest=digest),
        _manifest_row(
            public_id="00000000-0000-7000-8000-0000000000da",
            target_execution_digest=digest,
        ),
    ]

    with pytest.raises(ExecutionChainError, match="duplicate_annulment_binding"):
        SQLAlchemyRepository._validate_pnl_timeline_execution_prefix(
            _PnlTimelineExecutionPrefixSource(
                wallet_public_id=_MAIN_WALLET,
                mode="live",
                watermarks={"kraken": 1},
                source_rows=[
                    (
                        execution,
                        _order(_KRAKEN_ORDER, _KRAKEN_CID, _MAIN_WALLET, _KRAKEN_INSTRUMENT),
                        _instrument(_KRAKEN_INSTRUMENT, "kraken", _BTCUSD_SYMBOL),
                    )
                ],
                fill_rows=[],
                native_symbols_by_symbol_public_id={_BTCUSD_SYMBOL: {"BTC-USD"}},
                order_instrument_ids_by_scope={_KRAKEN_CID: {_KRAKEN_INSTRUMENT}},
                annulment_rows=manifest,
                annulment_witnesses={},
            )
        )


def test_an_uncanonicalizable_target_refuses_instead_of_crashing() -> None:
    """A row whose canonical bytes cannot be produced cannot be proven annulled.

    Given: An in-prefix execution carrying a naive timestamp, so the canonical
        record the digest is taken over cannot be built at all.
    When: A manifest row bound to it is folded.
    Then: The fold refuses by name. A binding proof that cannot be evaluated is
        not a binding proof, and must never degrade into an exclusion granted on
        the manifest's own say-so.
    """
    execution = _kraken_phantom()
    execution.timestamp = datetime(2026, 7, 19, 21, 54)

    with pytest.raises(ExecutionChainError, match="uncanonicalizable_annulled_execution"):
        SQLAlchemyRepository._validate_pnl_timeline_execution_prefix(
            _PnlTimelineExecutionPrefixSource(
                wallet_public_id=_MAIN_WALLET,
                mode="live",
                watermarks={"kraken": 1},
                source_rows=[
                    (
                        execution,
                        _order(_KRAKEN_ORDER, _KRAKEN_CID, _MAIN_WALLET, _KRAKEN_INSTRUMENT),
                        _instrument(_KRAKEN_INSTRUMENT, "kraken", _BTCUSD_SYMBOL),
                    )
                ],
                fill_rows=[],
                native_symbols_by_symbol_public_id={_BTCUSD_SYMBOL: {"BTC-USD"}},
                order_instrument_ids_by_scope={_KRAKEN_CID: {_KRAKEN_INSTRUMENT}},
                annulment_rows=[_manifest_row()],
                annulment_witnesses={},
            )
        )


async def test_an_annulled_row_needs_no_lineage_to_be_excluded(
    repository: SQLAlchemyRepository,
) -> None:
    """The defect that produced a phantom cannot become a bar to correcting it.

    Given: The Kraken phantom annulled, and its Order version closed so the
        booking has no sentinel-current instrument lineage at all — the shape a
        booking defect can easily leave behind.
    When: The scope is certified.
    Then: The certification proves anyway. Lineage checks exist to bind a
        booking to durable fill evidence, and a correctly bound annulment has
        already established that this booking gets no evidence.
    """
    await _annul_kraken_phantom(repository)
    async with repository.session() as s:
        order = (
            (await s.execute(select(Order).where(Order.public_id == _KRAKEN_ORDER))).scalars().one()
        )
        order.known_to = _KRAKEN_PHANTOM_AT
        await s.commit()

    prefix = await repository.get_pnl_timeline_execution_prefix(_MAIN_WALLET, "live", _AS_OF)

    assert [row["public_id"] for row in prefix["executions"]] == [_WALUTOMAT_TRADE]


async def test_the_hash_chain_tip_is_untouched_by_the_correction(
    repository: SQLAlchemyRepository,
) -> None:
    """Tamper evidence folds the raw ledger, so a correction can never move a tip.

    Given: The Kraken scope's chain tip over its raw sealed prefix.
    When: The phantom that prefix contains is annulled.
    Then: The tip is bit-identical. The chain is a statement about what the
        ledger physically holds, not about what counts economically — if a
        manifest row could move it, appending corrections would become a way to
        make a tampered ledger agree with its checkpoint.
    """
    genesis = execution_chain_genesis(_MAIN_WALLET, "kraken", "live")
    before = await repository.get_spot_execution_chain_tip(
        _MAIN_WALLET,
        "kraken",
        "live",
        0,
        genesis,
        1,
    )

    await _annul_kraken_phantom(repository)

    assert (
        await repository.get_spot_execution_chain_tip(
            _MAIN_WALLET,
            "kraken",
            "live",
            0,
            genesis,
            1,
        )
        == before
    )


async def _store_bound_manifest_row(
    repository: SQLAlchemyRepository,
    observed_at: datetime | None = None,
) -> None:
    """Store one correctly bound Kraken-phantom correction, optionally observed.

    ``observed_at=None`` leaves the correction with NO visibility observation —
    durable, but never proven durable at any past instant — which is exactly the
    state a failed second transaction leaves behind.
    """
    digest = await _expected_digest(repository, _KRAKEN_PHANTOM)
    async with repository.session() as s:
        s.add(_manifest_row(target_execution_digest=digest))
        if observed_at is not None:
            s.add(_visibility_row(observed_at))
        await s.commit()


async def test_a_correction_is_not_folded_historically_before_it_was_observed(
    repository: SQLAlchemyRepository,
) -> None:
    """A horizon may fold a correction only once it PROVABLY existed.

    Given: A correctly bound correction whose durability was observed at a known
        instant, with its own pre-commit stamp an hour earlier — the shape a
        stalled writer leaves, and the exact case no settling margin could ever
        cover.
    When: The scope is certified at a horizon after the stamp but BEFORE the
        observation, and again after the observation.
    Then: Before the observation the correction is not folded and certification
        REFUSES, because the phantom stays unexcluded; after it, the same row
        folds and the scope proves. The stamp is never consulted: only the
        observation, which cannot precede durability because it was taken after
        a reader saw the committed row.
    """
    observed_at = datetime.now(UTC) - timedelta(hours=1)
    await _store_bound_manifest_row(repository, observed_at)

    with pytest.raises(ExecutionChainError, match="missing_execution_shard_lineage"):
        await repository.get_pnl_timeline_execution_prefix(
            _MAIN_WALLET,
            "live",
            observed_at - timedelta(seconds=1),
        )

    proven = await repository.get_pnl_timeline_execution_prefix(
        _MAIN_WALLET,
        "live",
        observed_at,
    )

    assert [row["public_id"] for row in proven["executions"]] == [_WALUTOMAT_TRADE]
    assert [row["public_id"] for row in proven["annulments"]] == [_ANNULMENT_PUBLIC_ID]


async def test_a_correction_with_no_observation_is_never_folded_historically(
    repository: SQLAlchemyRepository,
) -> None:
    """An unproven correction is withheld from history, however old its stamp.

    Given: A correction whose visibility observation is missing entirely — the
        state a failed second transaction leaves — carrying a stamp from long
        before the horizon.
    When: The scope is certified at that historical horizon.
    Then: It refuses. Nothing about the stamp can establish that the row was
        durable then, so history declines to assume it; the correction becomes
        historically knowable only once
        ``observe_execution_annulment_visibility`` completes.
    """
    await _store_bound_manifest_row(repository)

    with pytest.raises(ExecutionChainError, match="missing_execution_shard_lineage"):
        await repository.get_pnl_timeline_execution_prefix(
            _MAIN_WALLET,
            "live",
            datetime.now(UTC) - timedelta(hours=1),
        )


async def test_current_truth_folds_a_correction_with_no_observation(
    repository: SQLAlchemyRepository,
) -> None:
    """A read that can SEE the row has already performed the proof itself.

    Given: A correction with NO visibility observation at all.
    When: The scope is certified with NO horizon requested — what every route
        does when ``as_of`` is absent, and what the trader tick and the
        reconciliation capture do by construction.
    Then: It folds immediately and the scope proves. A row a reader can see is
        committed, which is the same fact the observation ledger records for
        historical reads; demanding a stored observation here would withhold an
        operator's just-recorded correction from the very read they recorded it
        for while proving nothing extra.
    """
    await _store_bound_manifest_row(repository)

    prefix = await repository.get_pnl_timeline_execution_prefix(
        _MAIN_WALLET,
        "live",
        None,
    )

    assert [row["public_id"] for row in prefix["executions"]] == [_WALUTOMAT_TRADE]
    assert [row["public_id"] for row in prefix["annulments"]] == [_ANNULMENT_PUBLIC_ID]


async def test_an_explicitly_requested_present_horizon_gets_no_exemption(
    repository: SQLAlchemyRepository,
) -> None:
    """The exemption keys on INTENT, never on how recent the horizon looks.

    Given: A correction with no observation, and a caller that names an instant
        of its own — one that happens to be the present.
    When: The scope is certified at that REQUESTED horizon.
    Then: The correction is withheld and certification refuses, exactly as for
        any other past instant. Current truth is not "a recent horizon", it is
        "no horizon": a caller that names an instant is making a claim about the
        past and must be answered from proof alone.
    """
    await _store_bound_manifest_row(repository)

    with pytest.raises(ExecutionChainError, match="missing_execution_shard_lineage"):
        await repository.get_pnl_timeline_execution_prefix(
            _MAIN_WALLET,
            "live",
            datetime.now(UTC),
        )


async def test_the_derived_planes_withhold_an_unobserved_correction_too(
    repository: SQLAlchemyRepository,
) -> None:
    """Recovery and the scope listing demand the same durability proof.

    Given: A correctly bound correction whose durability was observed AFTER the
        horizon being read.
    When: The recovery read and the timeline scope read run at that horizon.
    Then: Neither excludes the repudiated booking. A derived plane that folded
        on weaker evidence than certification would rebuild state from a history
        the certified path refuses to agree with.
    """
    historical = datetime.now(UTC) - timedelta(hours=1)
    await _store_bound_manifest_row(repository, historical + timedelta(seconds=1))

    recovered = await repository.get_executions_for_recovery(as_of=historical)
    timeline = await repository.get_pnl_timeline_executions(_MAIN_WALLET, "live", historical)

    assert _KRAKEN_PHANTOM in {row["public_id"] for row in recovered}
    assert _KRAKEN_PHANTOM in {row["public_id"] for row in timeline}


async def _add_kraken_witness(repository: SQLAlchemyRepository) -> None:
    """Deliver one durable fill for the phantom's order after its annulment."""
    async with repository.session() as s:
        s.add(
            _fill_event(
                "00000000-0000-7000-8000-000000000f06",
                _KRAKEN_CID,
                _MAIN_WALLET,
                "kraken",
                "BTC-USD",
                fill_size=1.0,
                fill_price=100.0,
                timestamp=_AS_OF,
            )
        )
        await s.commit()


async def test_recovery_refuses_a_contradicted_annulment_instead_of_omitting_it(
    repository: SQLAlchemyRepository,
) -> None:
    """A rebuild never silently drops money the venue has confirmed.

    Given: The Kraken phantom annulled, then a durable ``fill_observed`` witness
        for its order delivered afterwards — so the repudiation is now
        contradicted by real evidence.
    When: The startup recovery read runs.
    Then: It fails closed by name. A bare existence test on the target id would
        have quietly omitted a witnessed booking from the projection rebuild,
        which is a silent money loss with nothing said; refusing matches what
        recovery already does with any state it cannot certify — it quarantines
        rather than inventing.
    """
    await _annul_kraken_phantom(repository)
    await _add_kraken_witness(repository)

    with pytest.raises(ExecutionChainError, match="annulled_execution_witnessed"):
        await repository.get_executions_for_recovery(as_of=_AS_OF)


async def test_the_timeline_read_refuses_without_any_prior_certification(
    repository: SQLAlchemyRepository,
) -> None:
    """The scope listing proves the manifest itself, with no ordering precondition.

    Given: A contradicted repudiation, and NO certification performed first —
        this read is reached directly, which is exactly what a future consumer
        would do.
    When: The scope's timeline execution read runs.
    Then: It fails closed by name. The read carries the same bind, digest, and
        witness proof the certification prefix does, so its safety does not
        depend on an unenforced call ordering that a later caller could break
        without noticing.
    """
    await _annul_kraken_phantom(repository)
    await _add_kraken_witness(repository)

    with pytest.raises(ExecutionChainError, match="annulled_execution_witnessed"):
        await repository.get_pnl_timeline_executions(_MAIN_WALLET, "live", _AS_OF)


async def test_the_derived_planes_refuse_an_unprovable_manifest_binding(
    repository: SQLAlchemyRepository,
) -> None:
    """A repudiation the ledger cannot confirm suppresses nothing anywhere.

    Given: A manifest row bound to a real in-scope execution by id and
        coordinate but authorizing content the stored row does not have.
    When: The recovery read runs.
    Then: It fails closed by name rather than excluding the coordinate. The
        derived planes get the same theorem as certification, so no plane can
        drop a booking on weaker evidence than another would demand.
    """
    async with repository.session() as s:
        s.add(_manifest_row(timestamp=_AS_OF - timedelta(minutes=1)))
        s.add(_visibility_row(_CORRECTION_AT))
        await s.commit()

    with pytest.raises(ExecutionChainError, match="annulment_digest_mismatch"):
        await repository.get_executions_for_recovery(as_of=_AS_OF)


async def test_recovery_and_timeline_reads_drop_the_repudiated_booking(
    repository: SQLAlchemyRepository,
) -> None:
    """The derived planes apply the same predicate at the same knowledge horizon.

    Given: The Kraken phantom annulled at a known correction time.
    When: The startup recovery read and the scope's timeline execution read run
        at a horizon past the correction, and again at one before it.
    Then: Past the correction both planes omit the phantom, so it can neither
        seed a position projection nor reappear as timeline activity, while
        every un-annulled booking still replays. Before the correction both
        planes still return it, because the past does not silently acquire a
        correction it did not know about.
    """
    correction = await _annul_kraken_phantom(repository)

    recovered = await repository.get_executions_for_recovery(as_of=_AS_OF)
    timeline = await repository.get_pnl_timeline_executions(_MAIN_WALLET, "live", _AS_OF)
    historical_recovered = await repository.get_executions_for_recovery(
        as_of=await _observed_at(repository, correction["public_id"]) - timedelta(seconds=1)
    )
    historical_timeline = await repository.get_pnl_timeline_executions(
        _MAIN_WALLET,
        "live",
        await _observed_at(repository, correction["public_id"]) - timedelta(seconds=1),
    )

    assert [row["public_id"] for row in recovered] == [
        _PAPER_PHANTOM,
        _PAPER_LEGACY,
        _PAPER_WITNESSED,
        _WALUTOMAT_TRADE,
    ]
    assert [row["public_id"] for row in timeline] == [_WALUTOMAT_TRADE]
    assert {(row["exchange"], row["instrument"]) for row in recovered} == {
        ("walutomat", "EUR-PLN"),
        ("paper", "BTC-USD"),
    }
    assert ("kraken", "BTC-USD") in {
        (row["exchange"], row["instrument"]) for row in historical_recovered
    }
    assert _KRAKEN_PHANTOM in {row["public_id"] for row in historical_recovered}
    assert _KRAKEN_PHANTOM in {row["public_id"] for row in historical_timeline}


def _derived_plane_rows() -> (
    list[TradeProjectionCheckpoint | Position | ExecutionPlanCheckpoint | PortfolioPnlPoint]
):
    """Build the MEASURED production derived plane behind the same five bookings.

    Two ``trade_projection_checkpoints`` — one keyed on the genuine Walutomat
    ``recon-`` execution id, one on a paper fallback — one ``positions`` row,
    three ``execution_plan_checkpoints``, and no ``portfolio_pnl_points`` at all.
    The Kraken phantom has NO checkpoint of its own, which is exactly why the
    plan calls the production correction safe: nothing durable depends on it.
    """
    return [
        _projection_checkpoint("walutomat.EUR-PLN.live", _MAIN_WALLET, f"recon-{_WALUTOMAT_CID}"),
        _projection_checkpoint("paper.BTC-USD.live", _PAPER_WALLET, "paper-3"),
        Position(
            public_id="00000000-0000-7000-8000-000000000d01",
            instrument_public_id=_WALUTOMAT_INSTRUMENT,
            mode="live",
            wallet_public_id=_MAIN_WALLET,
            quantity=20.04,
            average_price=4.3836,
            unrealized_pnl=None,
            realized_pnl=0.0,
            mark_price=None,
            marked_at=None,
            source_venue_event_id=None,
            timestamp=_WALUTOMAT_TRADE_AT,
            known_to=KNOWN_TO_MAX,
            session_id=_SESSION,
            sequence_id=1,
        ),
        *(
            ExecutionPlanCheckpoint(
                public_id=f"00000000-0000-7000-8000-00000000d1{index:02d}",
                plan_public_id=f"00000000-0000-7000-8000-00000000d2{index:02d}",
                state={"phase": "terminal"},
                last_venue_event_id=index,
                last_tick_timestamp=_WALUTOMAT_TRADE_AT,
                checkpoint_at=_WALUTOMAT_TRADE_AT,
                timestamp=_WALUTOMAT_TRADE_AT,
                known_to=KNOWN_TO_MAX,
                session_id=_SESSION,
                sequence_id=index,
            )
            for index in (1, 2, 3)
        ),
    ]


def _projection_checkpoint(
    shard_key: str,
    wallet_public_id: str,
    seen_exec_id: str,
) -> TradeProjectionCheckpoint:
    """Build one durable shard projection keyed on a witnessed execution id."""
    return TradeProjectionCheckpoint(
        shard_key=shard_key,
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        position_qty=1.0,
        entry_price=100.0,
        position_opened_at=_WALUTOMAT_TRADE_AT,
        cash=0.0,
        peak_equity=0.0,
        realized_pnl=0.0,
        turnover=0.0,
        last_venue_event_id=1,
        last_venue_event_at=_WALUTOMAT_TRADE_AT,
        open_command_ids=None,
        seen_exec_ids=f'["{seen_exec_id}"]',
        checkpoint_at=_WALUTOMAT_TRADE_AT,
        timestamp=_WALUTOMAT_TRADE_AT,
        known_to=KNOWN_TO_MAX,
        session_id=_SESSION,
        sequence_id=1,
    )


async def test_the_derived_planes_rebuild_only_the_real_shards(
    repository: SQLAlchemyRepository,
) -> None:
    """The measured derived plane survives the correction with nothing invented.

    Given: The measured production derived plane behind the same five bookings —
        two trade projection checkpoints (the Walutomat ``recon-`` execution id
        and a paper fallback), one positions row, three execution-plan
        checkpoints, and an empty ``portfolio_pnl_points`` — with the Kraken
        phantom annulled.
    When: The annulment-aware recovery read runs.
    Then: It replays exactly the shards backed by real witnessed data, and the
        phantom-only ``kraken.BTC-USD`` shard does not reappear — a rebuild
        seeded from this read cannot recreate state that only ever existed
        because of the defect. The plan checkpoints are untouched: they are
        CONTROL state, not economic projection, so a money correction must not
        move them; the P&L point table stays empty because no anchor has been
        activated; and the raw ledger still holds all five bookings.
    """
    async with repository.session() as s:
        s.add_all(_derived_plane_rows())
        await s.commit()
    await _annul_kraken_phantom(repository)

    recovered = await repository.get_executions_for_recovery(as_of=_AS_OF)

    assert {(row["exchange"], row["instrument"]) for row in recovered} == {
        ("walutomat", "EUR-PLN"),
        ("paper", "BTC-USD"),
    }
    assert _KRAKEN_PHANTOM not in {row["public_id"] for row in recovered}
    async with repository.session() as s:
        plan_checkpoints = (await s.execute(select(ExecutionPlanCheckpoint))).scalars().all()
        projections = (await s.execute(select(TradeProjectionCheckpoint))).scalars().all()
        positions = (await s.execute(select(Position))).scalars().all()
        pnl_points = (await s.execute(select(PortfolioPnlPoint))).scalars().all()
        executions = (await s.execute(select(Execution))).scalars().all()
    assert len(plan_checkpoints) == 3
    assert all(row.known_to == KNOWN_TO_MAX for row in plan_checkpoints)
    assert {row.shard_key for row in projections} == {
        "walutomat.EUR-PLN.live",
        "paper.BTC-USD.live",
    }
    assert len(positions) == 1
    assert list(pnl_points) == []
    assert len(executions) == 5


async def test_certification_pins_one_snapshot_before_reading_evidence(
    repository: SQLAlchemyRepository,
) -> None:
    """Every read behind the proof answers for the same instant.

    Given: A PostgreSQL certification read.
    When: The effective prefix is loaded.
    Then: The transaction is pinned to REPEATABLE READ READ ONLY BEFORE any
        evidence statement runs. The proof reads the prefix, its fill lineage,
        the manifest, and the manifest's contradiction witnesses separately and
        concludes from how they AGREE; under READ COMMITTED each statement takes
        a fresh snapshot, so a fill committed mid-proof yields a torn view in
        which the manifest is read before the witness that already contradicts
        it. SQLite serializes one connection against any writer, which is why
        every other test in this file pins the BEHAVIOUR and this one pins the
        PostgreSQL-only property that produces it.
    """
    events: list[str] = []
    session = AsyncMock()

    async def execute(statement: object, parameters: object = None) -> MagicMock:
        """Record every statement the pinned transaction issues."""
        del parameters
        events.append(str(statement))
        return MagicMock()

    async def load(
        loaded_session: AsyncSession,
        wallet_public_id: str,
        mode: str,
        horizon: object,
    ) -> PnlTimelineExecutionPrefix:
        """Record that evidence loading begins only after the pin is set."""
        del loaded_session, wallet_public_id, mode, horizon
        events.append("load")
        return {"watermarks": {}, "executions": [], "annulments": []}

    session.execute = AsyncMock(side_effect=execute)
    session.begin = MagicMock(return_value=AsyncMock())
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
            "_load_pnl_timeline_execution_prefix_snapshot",
            side_effect=load,
        ),
    ):
        session_context.return_value.__aenter__.return_value = session
        session_context.return_value.__aexit__.return_value = None
        await repository.get_pnl_timeline_execution_prefix(_MAIN_WALLET, "live", _AS_OF)

    assert events == ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", "load"]


async def test_raw_audit_reads_keep_showing_the_corrected_booking(
    repository: SQLAlchemyRepository,
) -> None:
    """An appended correction is only honest if what it corrects stays visible.

    Given: The Kraken phantom annulled.
    When: The raw audit listing and the per-order audit read run at a horizon
        past the correction.
    Then: Both still return the phantom, and the manifest read returns its
        correction beside it — the exclusion belongs to the economic and
        certification planes alone.
    """
    correction = await _annul_kraken_phantom(repository)

    listed = await repository.get_executions(limit=10, as_of=_AS_OF)
    for_order = await repository.get_executions_for_order(_KRAKEN_ORDER, _AS_OF)
    manifest = await repository.get_execution_annulments(_MAIN_WALLET, "live")

    assert _KRAKEN_PHANTOM in {row["public_id"] for row in listed}
    assert [row["public_id"] for row in for_order] == [_KRAKEN_PHANTOM]
    assert [row["public_id"] for row in manifest] == [correction["public_id"]]


@pytest.mark.parametrize(
    "method_name",
    [
        "get_pnl_timeline_execution_prefix",
        "get_pnl_timeline_execution_prefix_bundle",
        "get_executions_for_recovery",
        "get_pnl_timeline_executions",
        "pnl_timeline_scope_has_fill_gap",
    ],
)
def test_the_repository_api_cannot_express_an_exempt_historical_horizon(
    method_name: str,
) -> None:
    """The forbidden pairing has no spelling at the money boundary.

    Given: Every abstract repository read whose fold consults the annulment
        manifest.
    When: Their signatures are inspected.
    Then: None accepts a ``current_truth`` argument, and each carries a nullable
        horizon instead. That is the whole point: while an instant and an
        independent flag were separate arguments, a caller could name a PAST
        horizon and still claim the current-truth exemption, bypassing the
        durability proof — and a money boundary must not depend on callers
        telling the truth about their own intent. With one nullable value,
        ``None`` means "no horizon requested, capture the present here" and any
        supplied instant is historical by construction; the illegal state is
        unrepresentable rather than merely undocumented.
    """
    signature = inspect.signature(getattr(Repository, method_name))

    assert "current_truth" not in signature.parameters
    horizon = signature.parameters["request_as_of" if method_name.endswith("bundle") else "as_of"]
    assert horizon.annotation == datetime | None


async def test_a_captured_present_horizon_folds_without_any_observation(
    repository: SQLAlchemyRepository,
) -> None:
    """``None`` always means the present the repository captures for itself.

    Given: A correction with NO durability observation, which no requested
        horizon could fold.
    When: The recovery and scope reads are called with ``as_of=None``.
    Then: Both fold it. The repository captured the present itself, so the
        horizon and its provenance cannot disagree — there is no way for a
        caller to obtain this answer for an instant it named.
    """
    await _store_bound_manifest_row(repository)

    recovered = await repository.get_executions_for_recovery(as_of=None)
    timeline = await repository.get_pnl_timeline_executions(_MAIN_WALLET, "live", None)

    assert _KRAKEN_PHANTOM not in {row["public_id"] for row in recovered}
    assert _KRAKEN_PHANTOM not in {row["public_id"] for row in timeline}
