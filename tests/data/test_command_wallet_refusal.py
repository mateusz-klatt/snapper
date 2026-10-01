"""Require explicit command wallet scope before any persistence transaction."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from typing import cast
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from snapper.core.partitioning import ShardOwnership
from snapper.core.partitioning import ShardOwnershipError
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PairedExecutionLeg
from snapper.data.models import TradeCommand
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeCommandInsertRow

CommandEntry = Literal["ordinary", "paired", "flatten"]
_NOW = datetime(2026, 10, 1, 12, tzinfo=UTC)
_MISSING = object()
_WALLET = "01975a8b-3c7d-7000-8000-abcdef123456"
_ENTRIES: tuple[CommandEntry, ...] = ("ordinary", "paired", "flatten")
_INVALID = [pytest.param(_MISSING, id="omitted"), None, "", " ", "\t\n", 17, False]
_COMPATIBLE = [
    _WALLET,
    _WALLET.upper(),
    _WALLET.replace("-", ""),
    "12345678-1234-4234-8234-123456789abc",
    "legacy-wallet",
    " padded-wallet ",
]


def _command(wallet: object) -> TradeCommandInsertRow:
    """Build a complete command while preserving deliberately invalid boundary values."""
    row = TradeCommandInsertRow(
        command_type="submit",
        shard_key="kraken.BTC-USD.live",
        exchange="kraken",
        instrument="BTC-USD",
        mode="live",
        strategy_id="wallet-guard",
        client_order_id="flatten-client",
        venue_client_id="flatten-client",
        side="sell",
        order_type="market",
        quantity=1.0,
        reduce_only=True,
        status="created",
        created_at=_NOW,
        correlation_id="group-1",
        session_id=_WALLET,
        sequence_id=2,
        timestamp=_NOW,
        idempotency_key="group-1:leg-1:flatten:1",
        source_surface="strategy",
    )
    if wallet is not _MISSING:
        row["wallet_public_id"] = cast(str, wallet)
    return row


async def _insert(
    repo: SQLAlchemyRepository, entry: CommandEntry, row: TradeCommandInsertRow
) -> object:
    """Invoke one actual fresh-command write entrypoint."""
    if entry == "ordinary":
        return await repo.insert_trade_command(row)
    if entry == "paired":
        return await repo.insert_paired_compensation_command(row)
    return await repo.claim_leg_and_insert_flatten_command(
        leg_public_id="leg-1",
        expected_status="filled",
        new_compensation_seq=1,
        command_row=row,
        bus_time=_NOW,
        session_id=_WALLET,
        sequence_id=3,
    )


@pytest.fixture
async def repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a fully isolated real SQLite repository and dispose its engine."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _seed_leg(repo: SQLAlchemyRepository) -> None:
    """Persist the original filled leg whose compensation must remain atomic."""
    await repo.insert_paired_execution_leg(
        {
            "public_id": "leg-1",
            "group_public_id": "group-1",
            "leg_index": 0,
            "exchange": "kraken",
            "mode": "live",
            "instrument": "BTC-USD",
            "shard_key": "kraken.BTC-USD.live",
            "side": "buy",
            "target_qty": 1.0,
            "signal_public_id": "signal-1",
            "command_public_id": "original-command",
            "client_order_id": "original-client",
            "status": "filled",
            "filled_signed_qty": 1.0,
            "wallet_public_id": _WALLET,
            "operator_public_id": None,
            "created_at": _NOW - timedelta(seconds=1),
            "session_id": _WALLET,
            "sequence_id": 1,
            "timestamp": _NOW - timedelta(seconds=1),
        }
    )


@pytest.mark.parametrize("entry", _ENTRIES)
@pytest.mark.parametrize("wallet", _INVALID)
async def test_invalid_wallet_refuses_before_session(
    monkeypatch: pytest.MonkeyPatch, entry: CommandEntry, wallet: object
) -> None:
    """Refuse missing or blank command scope before opening a transaction.

    Given: An otherwise valid command with absent, nonstring or blank wallet identity,
    When: Any of the three actual fresh-command writers receives it,
    Then: Wallet validation raises ValueError without entering a session.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    poison = MagicMock(side_effect=AssertionError("invalid wallet reached session acquisition"))
    monkeypatch.setattr(repo, "session", poison)
    try:
        with pytest.raises(ValueError, match="wallet_public_id"):
            await _insert(repo, entry, _command(wallet))
        poison.assert_not_called()
    finally:
        await repo.engine.dispose()


@pytest.mark.parametrize("entry", _ENTRIES)
async def test_existing_guard_precedence_is_preserved(
    monkeypatch: pytest.MonkeyPatch, entry: CommandEntry
) -> None:
    """Retain existing ownership and idempotency refusal precedence.

    Given: A command lacks wallet identity and violates an older entrypoint guard,
    When: Its writer is invoked with poisoned session acquisition,
    Then: The existing ownership or idempotency error wins without persistence.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    poison = MagicMock(side_effect=AssertionError("guard reached database"))
    monkeypatch.setattr(repo, "session", poison)
    row = _command(_MISSING)
    try:
        if entry == "ordinary":
            owner = MagicMock(spec=ShardOwnership)
            owner.owns.return_value = False
            owner.instance_id = 0
            owner.instance_count = 2
            with pytest.raises(ShardOwnershipError):
                await repo.insert_trade_command(row, ownership=owner)
        else:
            row.pop("idempotency_key")
            with pytest.raises(ValueError, match="idempotency_key"):
                await _insert(repo, entry, row)
        poison.assert_not_called()
    finally:
        await repo.engine.dispose()


@pytest.mark.parametrize("wallet", _INVALID)
async def test_invalid_flatten_wallet_preserves_complete_leg_history(
    repository: SQLAlchemyRepository, wallet: object
) -> None:
    """Invalid compensation scope cannot close a leg or create its command.

    Given: A real persisted filled leg and an invalid flatten wallet,
    When: The atomic flatten writer refuses the command,
    Then: The original leg remains the only open version and no command exists.
    """
    await _seed_leg(repository)
    with pytest.raises(ValueError, match="wallet_public_id"):
        await _insert(repository, "flatten", _command(wallet))
    async with repository.session() as session:
        legs: list[PairedExecutionLeg] = list(
            (await session.scalars(select(PairedExecutionLeg))).all()
        )
        commands: list[TradeCommand] = list((await session.scalars(select(TradeCommand))).all())
    assert len(legs) == 1
    assert (legs[0].status, legs[0].compensation_seq, legs[0].filled_signed_qty) == (
        "filled",
        0,
        1.0,
    )
    assert legs[0].timestamp == _NOW - timedelta(seconds=1)
    assert legs[0].known_to == KNOWN_TO_MAX
    assert legs[0].client_order_id == "original-client"
    assert commands == []


@pytest.mark.parametrize("entry", _ENTRIES)
@pytest.mark.parametrize("wallet", _COMPATIBLE)
async def test_nonblank_wallet_spelling_and_atomic_behavior_are_preserved(
    repository: SQLAlchemyRepository, entry: CommandEntry, wallet: str
) -> None:
    """Preserve existing nonblank spellings without introducing UUID policy.

    Given: UUID4, UUID7, uppercase, undashed or legacy nonblank wallet identity,
    When: A real repository persists the command through each entrypoint,
    Then: The exact spelling survives and compensation remains idempotent and atomic.
    """
    if entry == "flatten":
        await _seed_leg(repository)
    row = _command(wallet)
    assert await _insert(repository, entry, row) is not None
    if entry != "ordinary":
        assert await _insert(repository, entry, row) is None
    async with repository.session() as session:
        commands: list[TradeCommand] = list((await session.scalars(select(TradeCommand))).all())
        legs: list[PairedExecutionLeg] = list(
            (
                await session.scalars(
                    select(PairedExecutionLeg).order_by(PairedExecutionLeg.timestamp)
                )
            ).all()
        )
    assert len(commands) == 1
    assert commands[0].wallet_public_id == wallet
    if entry == "flatten":
        assert len(legs) == 2
        assert legs[0].known_to == _NOW
        assert legs[0].status == "filled"
        assert legs[1].public_id == legs[0].public_id
        assert (legs[1].status, legs[1].compensation_seq) == ("compensating", 1)
        assert legs[1].timestamp == _NOW
        assert legs[1].known_to == KNOWN_TO_MAX
        assert legs[1].client_order_id == "original-client"
