"""Real-database evidence for borrowing classification and exact rollover recovery."""

from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Literal
from typing import cast

import pytest

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccrualLedger
from snapper.data.models import Base
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import Symbol
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository

_AT = datetime(2026, 11, 8, tzinfo=UTC)
_WALLET = "00000000-0000-7000-8000-000000000001"
_OTHER_WALLET = "00000000-0000-7000-8000-000000000002"
_INSTRUMENT = "00000000-0000-7000-8000-000000000003"
_SYMBOL = "00000000-0000-7000-8000-000000000004"
_SESSION = "00000000-0000-7000-8000-000000000005"
_COMMAND = "00000000-0000-7000-8000-000000000006"
_SHARD = "kraken.XRP-EUR.live"


async def _repository(tmp_path: Path) -> SQLAlchemyRepository:
    """Create isolated SQLite evidence with an exact current instrument identity."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'rollover.db'}")
    await repository.create_all()
    await _insert(
        repository,
        [
            Symbol(
                public_id=_SYMBOL,
                native_symbol="XRP-EUR",
                base="XRP",
                quote="EUR",
                asset_type="crypto",
                created_at=_AT,
                timestamp=_AT,
                session_id=_SESSION,
                sequence_id=1,
            ),
            Instrument(
                public_id=_INSTRUMENT,
                symbol_public_id=_SYMBOL,
                exchange="kraken",
                timestamp=_AT,
                session_id=_SESSION,
                sequence_id=1,
            ),
        ],
    )
    return repository


async def _insert(repository: SQLAlchemyRepository, rows: Sequence[Base]) -> None:
    """Commit witness rows using the real ORM and database constraints."""
    async with repository.session() as session:
        session.add_all(rows)
        await session.commit()


def _fill(index: int = 1) -> VenueEvent:
    """Build one wallet-attributed durable fill with a client order identifier."""
    return VenueEvent(
        shard_key=_SHARD,
        wallet_public_id=_WALLET,
        exchange="kraken",
        instrument="XRP-EUR",
        mode="live",
        event_type="fill_observed",
        client_order_id=f"client-{index}",
        exec_id=f"exec-{index}",
        fill_size=4.07,
        fill_price=1.015,
        side="buy",
        venue_timestamp=_AT + timedelta(minutes=index),
        received_at=_AT + timedelta(minutes=index),
        timestamp=_AT + timedelta(minutes=index),
        session_id=_SESSION,
        sequence_id=index,
    )


def _order(leverage: int | None = None, index: int = 1) -> Order:
    """Build a current order whose NULL leverage proves no borrowing."""
    return Order(
        instrument_public_id=_INSTRUMENT,
        wallet_public_id=_WALLET,
        mode="live",
        client_order_id=f"client-{index}",
        created_at=_AT,
        side="buy",
        order_type="market",
        price=1.015,
        size=4.07,
        status="filled",
        leverage=leverage,
        timestamp=_AT,
        session_id=_SESSION,
        sequence_id=index,
    )


def _command(leverage: int | None = None, index: int = 1) -> TradeCommand:
    """Build a durable create intent independently usable as financing evidence."""
    return TradeCommand(
        public_id=_COMMAND,
        shard_key=_SHARD,
        wallet_public_id=_WALLET,
        exchange="kraken",
        instrument="XRP-EUR",
        mode="live",
        command_type="create",
        strategy_id="manual",
        client_order_id=f"client-{index}",
        venue_client_id=f"venue-{index}",
        side="buy",
        order_type="market",
        quantity=4.07,
        leverage=leverage,
        status="filled",
        created_at=_AT,
        timestamp=_AT,
        session_id=_SESSION,
        sequence_id=index,
    )


@pytest.mark.parametrize(
    ("source", "leverage", "expected"),
    [
        ("order", None, "spot"),
        ("order", 1, "margin"),
        ("command", None, "spot"),
        ("command", 2, "margin"),
        ("both", None, "spot"),
        ("both", 3, "margin"),
        ("submit", 2, "margin"),
    ],
)
async def test_financing_requires_consistent_durable_intent(
    tmp_path: Path,
    source: str,
    leverage: int | None,
    expected: Literal["spot", "margin"],
) -> None:
    """Either order or dispatch intent establishes borrowing; agreement stays exact.

    Given: a fill and consistent current Order or create/submit intent.
    When: financing is resolved from either source or their agreement.
    Then: NULL leverage proves spot and positive leverage proves margin.
    """
    repository = await _repository(tmp_path)
    rows: list[Base] = [_fill()]
    if source in ("order", "both"):
        rows.append(_order(leverage))
    if source in ("command", "both", "submit"):
        command = _command(leverage)
        command.command_type = "submit" if source == "submit" else "create"
        rows.append(command)
    await _insert(repository, rows)
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) == expected


@pytest.mark.parametrize(("shard", "wallet"), [("invalid", _WALLET), (_SHARD, "")])
async def test_unattributed_financing_scope_is_unknown(
    tmp_path: Path, shard: str, wallet: str
) -> None:
    """Malformed shard identity or absent full wallet cannot authorize accrual.

    Given: a malformed shard key or an absent full wallet identifier.
    When: the repository is asked to establish position financing.
    Then: it reports unknown without authorizing an accrual.
    """
    repository = await _repository(tmp_path)
    assert await repository.get_shard_fill_financing(shard, wallet) is None


async def test_no_fills_or_missing_intent_is_unknown(tmp_path: Path) -> None:
    """An empty history and a fill with no durable order both fail closed.

    Given: an empty history followed by a fill without order intent.
    When: financing is queried before and after inserting that fill.
    Then: both histories remain unknown because borrowing is unproven.
    """
    repository = await _repository(tmp_path)
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None
    await _insert(repository, [_fill()])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None


@pytest.mark.parametrize("field", ["wallet_public_id", "exchange", "instrument", "mode"])
async def test_foreign_fill_scope_is_unknown(tmp_path: Path, field: str) -> None:
    """A foreign fill sharing the shard string cannot be hidden by wallet filtering.

    Given: a fill whose wallet, exchange, instrument or mode is foreign.
    When: its shared shard string is used to query financing.
    Then: the contradictory fill remains visible and classification refuses.
    """
    repository = await _repository(tmp_path)
    fill = _fill()
    values = {
        "wallet_public_id": _OTHER_WALLET,
        "exchange": "paper",
        "instrument": "BTC-EUR",
        "mode": "paper",
    }
    setattr(fill, field, values[field])
    await _insert(repository, [fill, _order()])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None


@pytest.mark.parametrize(
    "field",
    [
        "wallet_public_id",
        "exchange",
        "instrument",
        "mode",
        "shard_key",
        "client_order_id",
        "public_id",
    ],
)
async def test_conflicting_command_identity_is_unknown(tmp_path: Path, field: str) -> None:
    """Matching either identifier does not forgive contradiction in another field.

    Given: a referenced create command contradicting one identity field.
    When: financing resolves witnesses by command identifier or client ID.
    Then: a match on one identifier cannot excuse the conflicting evidence.
    """
    repository = await _repository(tmp_path)
    fill = _fill()
    fill.command_public_id = _COMMAND
    command = _command()
    values = {
        "wallet_public_id": _OTHER_WALLET,
        "exchange": "paper",
        "instrument": "BTC-EUR",
        "mode": "paper",
        "shard_key": "kraken.XRP-EUR.live.strategy",
        "client_order_id": "foreign-client",
        "public_id": "00000000-0000-7000-8000-000000000007",
    }
    setattr(command, field, values[field])
    await _insert(repository, [fill, command])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None


@pytest.mark.parametrize("leverage", [0, -1, 1.5])
async def test_invalid_leverage_is_unknown(tmp_path: Path, leverage: int | float) -> None:
    """Malformed SQLite leverage values never masquerade as unborrowed spot.

    Given: an order carrying zero, negative or fractional SQLite leverage.
    When: the fill financing classifier inspects that persisted value.
    Then: it reports unknown rather than treating malformed leverage as spot.
    """
    repository = await _repository(tmp_path)
    order = _order(cast(int, leverage))
    await _insert(repository, [_fill(), order])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None


async def test_disagreeing_intents_and_mixed_fill_financing_are_unknown(tmp_path: Path) -> None:
    """Neither contradictory witnesses nor a mixed spot/margin history prove borrowing.

    Given: contradictory witnesses and a separate mixed spot/margin history.
    When: each shard is classified using every witnessed fill.
    Then: neither inconsistency produces an authorization to charge rollover.
    """
    repository = await _repository(tmp_path)
    await _insert(repository, [_fill(), _order(), _command(2)])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None
    second_fill = _fill(2)
    second_fill.shard_key = "kraken.XRP-EUR.live.second"
    second_command = _command(2, 2)
    second_command.public_id = "00000000-0000-7000-8000-000000000008"
    second_command.shard_key = second_fill.shard_key
    third_fill = _fill(3)
    third_fill.shard_key = second_fill.shard_key
    third_command = _command(None, 3)
    third_command.public_id = "00000000-0000-7000-8000-000000000009"
    third_command.shard_key = third_fill.shard_key
    await _insert(repository, [second_fill, second_command, third_fill, third_command])
    assert await repository.get_shard_fill_financing(second_fill.shard_key, _WALLET) is None


async def test_ambiguous_or_missing_explicit_command_is_unknown(tmp_path: Path) -> None:
    """Two create intents for one CID and an absent referenced intent both refuse.

    Given: a missing referenced intent and then two intents for one client ID.
    When: each form of incomplete or ambiguous command lineage is queried.
    Then: the financing classifier refuses both histories.
    """
    repository = await _repository(tmp_path)
    fill = _fill()
    fill.command_public_id = _COMMAND
    await _insert(repository, [fill, _order()])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None
    first = _command()
    second = _command()
    second.public_id = "00000000-0000-7000-8000-000000000007"
    await _insert(repository, [first, second])
    fill.command_public_id = None
    async with repository.session() as session:
        persisted = await session.get(VenueEvent, fill.id)
        assert persisted is not None
        persisted.command_public_id = None
        await session.commit()
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None


async def test_command_identifier_without_client_identifier_proves_financing(
    tmp_path: Path,
) -> None:
    """An explicit durable command can establish borrowing when the fill omits CID.

    Given: a fill with an explicit command ID but no client order identifier.
    When: its matching current command records margin leverage two.
    Then: the complete command lineage establishes margin financing.
    """
    repository = await _repository(tmp_path)
    fill = _fill()
    fill.command_public_id = _COMMAND
    fill.client_order_id = None
    await _insert(repository, [fill, _command(2)])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) == "margin"


async def test_current_cycle_filter_does_not_reuse_previous_borrowing(tmp_path: Path) -> None:
    """A prior margin cycle cannot authorize charging a later spot position.

    Given: an earlier margin fill followed by a later unlevered spot fill.
    When: financing is queried globally and from the newer cycle boundary.
    Then: the mixed history is unknown while the new cycle proves spot.
    """
    repository = await _repository(tmp_path)
    await _insert(repository, [_fill(), _order(2), _fill(2), _order(None, 2)])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None
    assert (
        await repository.get_shard_fill_financing(_SHARD, _WALLET, _AT + timedelta(minutes=2))
        == "spot"
    )


async def test_missing_cycle_timestamp_is_unknown(tmp_path: Path) -> None:
    """Unknown venue instants are retained and refuse current-cycle classification.

    Given: a margin-intent fill whose venue execution timestamp is unknown.
    When: financing is queried for a bounded current position cycle.
    Then: the uncertain fill is retained and prevents classification.
    """
    repository = await _repository(tmp_path)
    fill = _fill()
    fill.venue_timestamp = None
    await _insert(repository, [fill, _order(2)])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET, _AT) is None


async def test_superseded_intent_does_not_authorize_borrowing(tmp_path: Path) -> None:
    """Only sentinel-current rows establish intent, independent of their bus clocks.

    Given: superseded margin intents and then a current spot order clocked ahead.
    When: the repository classifies current durable order evidence.
    Then: closed intents cannot authorize borrowing and clock skew hides no current row.
    """
    repository = await _repository(tmp_path)
    order = _order(2)
    order.known_to = _AT + timedelta(days=1)
    command = _command(2)
    command.known_to = _AT + timedelta(days=1)
    await _insert(repository, [_fill(), order, command])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None
    current = _order()
    current.timestamp = KNOWN_TO_MAX - timedelta(days=1)
    await _insert(repository, [current])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) == "spot"


@pytest.mark.parametrize("field", ["wallet_public_id", "mode", "instrument_public_id"])
async def test_foreign_or_unresolved_order_is_unknown(tmp_path: Path, field: str) -> None:
    """Order witnesses require full wallet, mode and a resolvable exact instrument.

    Given: an order with foreign wallet, mode or unresolved instrument identity.
    When: financing is resolved for its otherwise matching fill.
    Then: the repository refuses to infer borrowing from that order.
    """
    repository = await _repository(tmp_path)
    order = _order()
    values = {
        "wallet_public_id": _OTHER_WALLET,
        "mode": "paper",
        "instrument_public_id": "00000000-0000-7000-8000-000000000009",
    }
    setattr(order, field, values[field])
    await _insert(repository, [_fill(), order])
    assert await repository.get_shard_fill_financing(_SHARD, _WALLET) is None


def _accrual(index: int) -> AccrualLedger:
    """Build a current rollover row with fixed provenance and exact scope."""
    return AccrualLedger(
        public_id=f"00000000-0000-7000-8000-{index:012d}",
        instrument_public_id=_INSTRUMENT,
        wallet_public_id=_WALLET,
        mode="live",
        accrual_type="rollover",
        accrued_at=_AT,
        amount=0.001025324575,
        amount_asset="EUR",
        rate=0.00025,
        notional=4.1012983,
        position_quantity_at_accrual=4.07,
        exchange="kraken",
        timestamp=_AT,
        session_id=_SESSION,
        sequence_id=index,
    )


async def test_complete_accrual_evidence_is_clock_free_scoped_and_ordered(tmp_path: Path) -> None:
    """All current rows survive skewed clocks; foreign and closed rows never enter.

    Given: current, skewed, foreign, superseded and sequence-tied accrual rows.
    When: the complete ledger is read for one exact wallet/instrument/mode.
    Then: all current scope rows survive in deterministic provenance order.
    """
    repository = await _repository(tmp_path)
    late = _accrual(2)
    late.timestamp = KNOWN_TO_MAX - timedelta(days=1)
    late.accrued_at = late.timestamp
    early = _accrual(1)
    foreign_wallet = _accrual(3)
    foreign_wallet.wallet_public_id = _OTHER_WALLET
    foreign_instrument = _accrual(4)
    foreign_instrument.instrument_public_id = _SYMBOL
    foreign_mode = _accrual(5)
    foreign_mode.mode = "paper"
    closed = _accrual(6)
    closed.known_to = _AT + timedelta(days=1)
    tied = _accrual(7)
    tied.accrued_at = _AT + timedelta(hours=4)
    tied.sequence_id = 2
    await _insert(
        repository,
        [late, early, foreign_wallet, foreign_instrument, foreign_mode, closed, tied],
    )
    rows = await repository.get_all_shard_accruals(_WALLET, _INSTRUMENT, "live")
    assert [row["public_id"] for row in rows] == [early.public_id, late.public_id, tied.public_id]
    assert rows[1]["timestamp"] == late.timestamp
    assert all(row["amount"] == 0.001025324575 for row in rows)
    assert await repository.get_all_shard_accruals(_OTHER_WALLET, _SYMBOL, "paper") == []
