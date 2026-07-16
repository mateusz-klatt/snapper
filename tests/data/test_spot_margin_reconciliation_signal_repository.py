"""Tests for durable spot-margin reconciliation tripwire evidence."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccrualLedger
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import PortfolioSpotReconciliationAnchor
from snapper.data.models import TradeCommand
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
_WALLET = "00000000-0000-7000-8000-000000000101"
_OTHER_WALLET = "00000000-0000-7000-8000-000000000102"
_ALPHA_WALLET = "abcdefab-cdef-7abc-8def-abcdefabcdef"
_SESSION = "00000000-0000-7000-8000-000000000201"
_INSTRUMENT = "00000000-0000-7000-8000-000000000301"
_SYMBOL = "00000000-0000-7000-8000-000000000401"


async def _make_repo(tmp_path: Path, name: str) -> SQLAlchemyRepository:
    """Create one isolated full-schema repository."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repository.create_all()
    return repository


def _trade_command(
    *,
    leverage: int | None,
    wallet_public_id: str = _WALLET,
    exchange: str = "kraken",
    mode: str = "live",
) -> TradeCommand:
    """Build one durable order command."""
    return TradeCommand(
        command_type="submit",
        shard_key=f"{exchange}.XBTUSD.{mode}",
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        user_public_id=None,
        exchange=exchange,
        instrument="XBTUSD",
        mode=mode,
        strategy_id="manual",
        client_order_id=f"command-{wallet_public_id}-{exchange}-{mode}-{leverage}",
        venue_client_id=f"venue-{wallet_public_id}-{exchange}-{mode}-{leverage}",
        idempotency_key=None,
        side="buy",
        order_type="market",
        quantity=1.0,
        price=None,
        stop_price=None,
        leverage=leverage,
        reduce_only=False,
        status="created",
        attempt_count=0,
        last_error=None,
        created_at=_NOW - timedelta(minutes=5),
        dispatched_at=None,
        acked_at=None,
        terminal_at=None,
        exchange_order_id=None,
        supersedes_command_id=None,
        correlation_id="00000000-0000-7000-8000-000000000501",
        session_id=_SESSION,
        sequence_id=1,
        timestamp=_NOW - timedelta(minutes=5),
        known_to=KNOWN_TO_MAX,
    )


def _instrument(
    *,
    exchange: str = "kraken",
    timestamp: datetime = _NOW - timedelta(hours=1),
    known_to: datetime = KNOWN_TO_MAX,
) -> Instrument:
    """Build one temporal instrument version."""
    return Instrument(
        symbol_public_id=_SYMBOL,
        exchange=exchange,
        source_exchange=None,
        public_id=_INSTRUMENT,
        session_id=_SESSION,
        sequence_id=2,
        timestamp=timestamp,
        known_to=known_to,
    )


def _order(
    *,
    leverage: int | None,
    wallet_public_id: str = _WALLET,
    mode: str = "live",
    timestamp: datetime = _NOW - timedelta(minutes=5),
) -> Order:
    """Build one durable order referencing a temporal instrument."""
    return Order(
        instrument_public_id=_INSTRUMENT,
        mode=mode,
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        client_order_id=f"order-{wallet_public_id}-{mode}-{leverage}",
        exchange_order_id=None,
        created_at=timestamp,
        updated_at=None,
        side="buy",
        order_type="market",
        price=None,
        size=1.0,
        status="filled",
        time_in_force=None,
        filled_size=1.0,
        average_price=100.0,
        error=None,
        leverage=leverage,
        reduce_only=False,
        plan_public_id=None,
        session_id=_SESSION,
        sequence_id=3,
        timestamp=timestamp,
        known_to=KNOWN_TO_MAX,
    )


def _accrual(
    accrual_type: str,
    *,
    wallet_public_id: str = _WALLET,
    exchange: str = "kraken",
    mode: str = "live",
) -> AccrualLedger:
    """Build one durable funding, borrow, or rollover entry."""
    return AccrualLedger(
        instrument_public_id=_INSTRUMENT,
        wallet_public_id=wallet_public_id,
        operator_public_id=None,
        mode=mode,
        accrual_type=accrual_type,
        accrued_at=_NOW - timedelta(minutes=5),
        amount=-1.0,
        amount_asset="USD",
        rate=0.001,
        notional=1000.0,
        position_quantity_at_accrual=1.0,
        exchange=exchange,
        session_id=_SESSION,
        sequence_id=4,
        timestamp=_NOW - timedelta(minutes=5),
        known_to=KNOWN_TO_MAX,
    )


def _anchor(
    margin_status: str,
    *,
    wallet_public_id: str = _WALLET,
    exchange: str = "kraken",
) -> PortfolioSpotReconciliationAnchor:
    """Build one active cash or non-cash spot anchor."""
    first_started = _NOW - timedelta(minutes=10)
    first_completed = first_started + timedelta(seconds=1)
    second_started = first_completed + timedelta(seconds=1)
    second_completed = second_started + timedelta(seconds=1)
    return PortfolioSpotReconciliationAnchor(
        wallet_public_id=wallet_public_id,
        exchange=exchange,
        mode="live",
        venue_account_state_public_id="00000000-0000-7000-8000-000000000601",
        balance_observation_id=41,
        source_watermark_kind="execution_id",
        source_watermark=7,
        balances_json='{"USD":"100"}',
        first_request_started_at=first_started,
        first_request_completed_at=first_completed,
        second_request_started_at=second_started,
        second_request_completed_at=second_completed,
        boundary_status="double_read_equal",
        inventory_status="certified_full",
        margin_status=margin_status,
        provenance="test",
        session_id=_SESSION,
        sequence_id=5,
        timestamp=second_completed,
        known_to=KNOWN_TO_MAX,
    )


async def _assert_target_only(repository: SQLAlchemyRepository) -> None:
    """Assert the signal is visible only for its exact account identity."""
    assert (
        await repository.has_spot_margin_reconciliation_signal(
            _WALLET,
            "kraken",
            "live",
            _NOW,
        )
        is True
    )
    assert (
        await repository.has_spot_margin_reconciliation_signal(
            _OTHER_WALLET,
            "kraken",
            "live",
            _NOW,
        )
        is False
    )
    assert (
        await repository.has_spot_margin_reconciliation_signal(
            _WALLET,
            "walutomat",
            "live",
            _NOW,
        )
        is False
    )
    assert (
        await repository.has_spot_margin_reconciliation_signal(
            _WALLET,
            "kraken",
            "paper",
            _NOW,
        )
        is False
    )


async def test_false_baseline_ignores_nonleveraged_cash_and_funding_rows(
    tmp_path: Path,
) -> None:
    """Null leverage, ordinary funding, and a cash anchor remain clean."""
    repository = await _make_repo(tmp_path, "spot-margin-baseline.db")
    async with repository.session() as session:
        session.add_all(
            [
                _trade_command(leverage=None),
                _instrument(),
                _order(leverage=None),
                _accrual("funding"),
                _anchor("cash"),
            ]
        )
        await session.commit()
    assert (
        await repository.has_spot_margin_reconciliation_signal(
            _WALLET,
            "kraken",
            "live",
            _NOW,
        )
        is False
    )


async def test_trade_command_leverage_is_independent_durable_signal(tmp_path: Path) -> None:
    """Any non-null scoped command leverage trips the cash-only guard."""
    repository = await _make_repo(tmp_path, "spot-margin-command.db")
    async with repository.session() as session:
        session.add(_trade_command(leverage=0))
        await session.commit()
    await _assert_target_only(repository)


@pytest.mark.parametrize(
    "wallet_alias",
    [_ALPHA_WALLET.upper(), _ALPHA_WALLET.replace("-", "")],
)
async def test_spot_margin_signal_read_canonicalizes_wallet_aliases(
    tmp_path: Path,
    wallet_alias: str,
) -> None:
    """Uppercase and hyphenless aliases find canonical durable signals.

    Args:
        tmp_path: Pytest temporary directory.
        wallet_alias: Alternate spelling of the canonical wallet UUID.
    """
    repository = await _make_repo(tmp_path, "canonical-signal-wallet.db")
    async with repository.session() as session:
        session.add(
            _trade_command(
                leverage=1,
                wallet_public_id=_ALPHA_WALLET,
            )
        )
        await session.commit()

    assert (
        await repository.has_spot_margin_reconciliation_signal(
            wallet_alias,
            "kraken",
            "live",
            _NOW,
        )
        is True
    )


async def test_spot_margin_signal_read_rejects_malformed_wallet_uuid(
    tmp_path: Path,
) -> None:
    """Malformed wallet text raises the shared stable ValueError."""
    repository = await _make_repo(tmp_path, "malformed-signal-wallet.db")

    with pytest.raises(ValueError, match="reconciliation wallet identity is invalid"):
        await repository.has_spot_margin_reconciliation_signal(
            "not-a-wallet-uuid",
            "kraken",
            "live",
            _NOW,
        )


async def test_order_leverage_uses_instrument_exchange_at_order_time(tmp_path: Path) -> None:
    """Order leverage resolves exchange through the historical instrument version."""
    repository = await _make_repo(tmp_path, "spot-margin-order.db")
    successor_at = _NOW - timedelta(minutes=1)
    order_at = _NOW - timedelta(minutes=5)
    async with repository.session() as session:
        session.add_all(
            [
                _instrument(
                    exchange="kraken",
                    timestamp=_NOW - timedelta(hours=1),
                    known_to=successor_at,
                ),
                _instrument(exchange="walutomat", timestamp=successor_at),
                _order(leverage=2, timestamp=order_at),
            ]
        )
        await session.commit()
    await _assert_target_only(repository)


@pytest.mark.parametrize("accrual_type", ["borrow", "rollover"])
async def test_borrow_and_rollover_are_independent_durable_signals(
    tmp_path: Path,
    accrual_type: str,
) -> None:
    """Each liability accrual type independently trips cash-only replay."""
    repository = await _make_repo(tmp_path, f"spot-margin-{accrual_type}.db")
    async with repository.session() as session:
        session.add(_accrual(accrual_type))
        await session.commit()
    await _assert_target_only(repository)


@pytest.mark.parametrize("margin_status", ["unsupported_margin", "unknown"])
async def test_active_non_cash_anchor_is_independent_durable_signal(
    tmp_path: Path,
    margin_status: str,
) -> None:
    """Every active non-cash anchor status trips cash-only replay."""
    repository = await _make_repo(tmp_path, f"spot-margin-anchor-{margin_status}.db")
    async with repository.session() as session:
        session.add(_anchor(margin_status))
        await session.commit()
    await _assert_target_only(repository)
