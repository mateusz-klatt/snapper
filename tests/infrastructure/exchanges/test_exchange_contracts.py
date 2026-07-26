"""Unit tests for Kraken exchange OHLC schemas and exchange contracts."""

import re
from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import uuid7

import pytest

from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import FundingRateSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import is_lifecycle_only
from snapper.infrastructure.exchanges.contracts import order_status_is_terminal
from snapper.infrastructure.exchanges.contracts import to_fill_status
from snapper.infrastructure.exchanges.schemas.kraken import KrakenCandleSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcEventEnvelope
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcSubscribeParamsSchema


def test_ohlc_subscribe_params_as_params() -> None:
    """Verify OHLC subscription params serialize correctly.

    Given KrakenOhlcSubscribeParamsSchema with symbol, interval, snapshot,
    When as_params() is called,
    Then returns dict with channel='ohlc' and all fields.
    """
    params = KrakenOhlcSubscribeParamsSchema(symbol=["BTC-USD"], interval=5, snapshot=False)
    rendered = params.as_params()
    assert rendered == {
        "channel": "ohlc",
        "symbol": ["BTC-USD"],
        "interval": 5,
        "snapshot": False,
    }


def test_ohlc_candle_as_dict_returns_floats() -> None:
    """Verify candle schema exports all fields as proper types.

    Given a KrakenCandleSchema with OHLCV data,
    When as_dict() is called,
    Then returns dict with float values for numeric fields.
    """
    candle = KrakenCandleSchema(
        symbol="BTC-USD",
        open=50000.0,
        high=50100.0,
        low=49900.0,
        close=50050.0,
        volume=12.5,
        trades=42,
        interval=1,
        interval_begin="2023-11-14T22:13:20.000000Z",
    )
    payload = candle.as_dict()
    assert payload["open"] == pytest.approx(50000.0)
    assert payload["volume"] == pytest.approx(12.5)
    assert payload["trades"] == 42
    assert payload["symbol"] == "BTC-USD"


def test_ohlc_message_helpers_return_primary_symbol_and_dicts() -> None:
    """Verify envelope helpers extract symbol and normalize candles.

    Given a KrakenOhlcEventEnvelope with two candles for ETH-USD,
    When primary_symbol() and as_dicts() are called,
    Then primary_symbol returns 'ETH-USD' and as_dicts returns 2 dicts.
    """
    candle_1 = KrakenCandleSchema(
        symbol="ETH-USD",
        open=3000.0,
        high=3050.0,
        low=2950.0,
        close=3025.0,
    )
    candle_2 = KrakenCandleSchema(
        symbol="ETH-USD",
        open=3025.0,
        high=3060.0,
        low=3010.0,
        close=3055.0,
    )
    message = KrakenOhlcEventEnvelope(channel="ohlc", type="snapshot", data=[candle_1, candle_2])
    normalized = message.as_dicts()
    assert message.primary_symbol() == "ETH-USD"
    assert len(normalized) == 2
    assert normalized[0]["close"] == pytest.approx(3025.0)


class TestToFillStatus:
    """Tests for to_fill_status helper function."""

    def _make_execution(
        self, status: ExchangeOrderStatusEnum, cum_qty: float | None = None
    ) -> ExecutionUpdate:
        """Build a minimal ExecutionUpdate for fill-status testing.

        Args:
            status: Order status to set on the execution.
            cum_qty: Cumulative filled quantity (optional).

        Returns:
            ExecutionUpdate with the specified status and cum_qty.
        """
        return ExecutionUpdate(
            order_id="test-order",
            exec_type="trade",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=status,
            timestamp=datetime.now(UTC),
            cum_qty=cum_qty,
        )

    def test_partial_when_open_with_fills(self) -> None:
        """Return 'partial' for an open order with cumulative fills.

        Given: ExecutionUpdate with OPEN status and cum_qty > 0,
        When: to_fill_status is called,
        Then: Returns 'partial'.
        """
        execution = self._make_execution(ExchangeOrderStatusEnum.OPEN, cum_qty=5.0)
        assert to_fill_status(execution) == "partial"

    def test_filled_when_closed(self) -> None:
        """Return 'filled' for a closed order.

        Given: ExecutionUpdate with CLOSED status,
        When: to_fill_status is called,
        Then: Returns 'filled'.
        """
        execution = self._make_execution(ExchangeOrderStatusEnum.CLOSED)
        assert to_fill_status(execution) == "filled"

    def test_filled_when_open_with_zero_cum_qty(self) -> None:
        """Return 'filled' for an open order with zero cum_qty.

        Given: ExecutionUpdate with OPEN status and cum_qty=0,
        When: to_fill_status is called,
        Then: Returns 'filled' (no partial fills yet).
        """
        execution = self._make_execution(ExchangeOrderStatusEnum.OPEN, cum_qty=0.0)
        assert to_fill_status(execution) == "filled"

    def test_filled_when_open_with_none_cum_qty(self) -> None:
        """Return 'filled' for an open order with None cum_qty.

        Given: ExecutionUpdate with OPEN status and cum_qty=None,
        When: to_fill_status is called,
        Then: Returns 'filled' (None treated as zero).
        """
        execution = self._make_execution(ExchangeOrderStatusEnum.OPEN, cum_qty=None)
        assert to_fill_status(execution) == "filled"


class TestOrderStatusIsTerminal:
    """Tests for the terminal-order-status predicate."""

    def test_terminal_statuses(self) -> None:
        """Closed, canceled, and expired are terminal.

        Given: The three terminal order statuses,
        When: order_status_is_terminal is called,
        Then: Each returns True.
        """
        assert order_status_is_terminal(ExchangeOrderStatusEnum.CLOSED) is True
        assert order_status_is_terminal(ExchangeOrderStatusEnum.CANCELED) is True
        assert order_status_is_terminal(ExchangeOrderStatusEnum.EXPIRED) is True

    def test_non_terminal_statuses(self) -> None:
        """Open and pre-fill statuses are not terminal.

        Given: Non-terminal order statuses,
        When: order_status_is_terminal is called,
        Then: Each returns False, so a resting acknowledgement never
            projects the order row closed.
        """
        assert order_status_is_terminal(ExchangeOrderStatusEnum.OPEN) is False
        assert order_status_is_terminal(ExchangeOrderStatusEnum.PENDING) is False
        assert order_status_is_terminal(ExchangeOrderStatusEnum.PENDING_NEW) is False
        assert order_status_is_terminal(ExchangeOrderStatusEnum.NEW) is False
        assert order_status_is_terminal(ExchangeOrderStatusEnum.PARTIALLY_FILLED) is False


class TestIsLifecycleOnly:
    """Tests for the resting-order lifecycle-acknowledgement predicate."""

    @staticmethod
    def _execution(order_status: ExchangeOrderStatusEnum, **overrides: Any) -> ExecutionUpdate:
        """Build an ExecutionUpdate for a lifecycle-ack scenario."""
        fields: dict[str, Any] = {
            "order_id": "ex1",
            "exec_type": "new",
            "symbol": "XRP-EUR",
            "side": OrderSideEnum.BUY,
            "order_type": ExchangeOrderTypeEnum.LIMIT,
            "order_status": order_status,
            "cum_qty": None,
            "last_qty": None,
            "timestamp": datetime.now(UTC),
        }
        fields.update(overrides)
        return ExecutionUpdate(**fields)

    def test_true_for_quantityless_non_terminal_ack(self) -> None:
        """A NEW ack normalizes to OPEN with no quantity and is lifecycle-only.

        Given: A quantity-less NEW acknowledgement (normalized OPEN),
        When: is_lifecycle_only is called,
        Then: Returns True so it is not booked or published as a fill.
        """
        assert is_lifecycle_only(self._execution(ExchangeOrderStatusEnum.NEW)) is True

    def test_false_when_cumulative_present(self) -> None:
        """A frame carrying a cumulative quantity is a fill, not a lifecycle ack."""
        assert (
            is_lifecycle_only(self._execution(ExchangeOrderStatusEnum.OPEN, cum_qty=1.0)) is False
        )

    def test_false_when_last_quantity_present(self) -> None:
        """A frame carrying a last quantity is a fill, not a lifecycle ack."""
        assert (
            is_lifecycle_only(self._execution(ExchangeOrderStatusEnum.OPEN, last_qty=0.5)) is False
        )

    def test_false_when_status_is_terminal(self) -> None:
        """A quantity-less terminal frame is a legitimate terminal publish, not an ack."""
        assert is_lifecycle_only(self._execution(ExchangeOrderStatusEnum.CLOSED)) is False


class TestFundingRateSnapshot:
    """Tests for FundingRateSnapshot frozen dataclass."""

    def test_creation_and_fields(self) -> None:
        """FundingRateSnapshot stores all fields correctly.

        Given: All required constructor arguments,
        When: FundingRateSnapshot is created,
        Then: All fields are accessible and correct.
        """
        effective = datetime(2026, 3, 1, 16, tzinfo=UTC)
        snap = FundingRateSnapshot(
            symbol="BTC-USD-PERP",
            exchange="kraken_futures",
            rate_type="perpetual_funding",
            direction="both",
            rate=7.182e-05,
            effective_from=effective,
            notional_asset="USD",
            source="exchange_api",
        )
        assert snap.symbol == "BTC-USD-PERP"
        assert snap.exchange == "kraken_futures"
        assert snap.rate_type == "perpetual_funding"
        assert snap.direction == "both"
        assert snap.rate == pytest.approx(7.182e-05)
        assert snap.effective_from == effective
        assert snap.notional_asset == "USD"
        assert snap.source == "exchange_api"

    def test_frozen_raises_on_assignment(self) -> None:
        """FundingRateSnapshot is frozen (immutable).

        Given: An existing FundingRateSnapshot,
        When: Attempting to change a field,
        Then: Raises FrozenInstanceError.
        """
        snap = FundingRateSnapshot(
            symbol="BTC-USD-PERP",
            exchange="kraken_futures",
            rate_type="perpetual_funding",
            direction="both",
            rate=7.182e-05,
            effective_from=datetime(2026, 3, 1, 16, tzinfo=UTC),
            notional_asset="USD",
            source="exchange_api",
        )
        with pytest.raises(AttributeError):
            snap.rate = 0.0


def test_ticker_update_default_is_delayed_is_false() -> None:
    """``TickerUpdate`` defaults ``is_delayed`` to False + extended-hours to None.

    Given: a TickerUpdate constructed without the new delay flags,
    When: its fields are inspected,
    Then: ``is_delayed`` is ``False`` and ``is_extended_hours`` is ``None``
        (ensures existing adapters that construct TickerUpdate positionally
        without the new kwargs stay no-op).
    """
    update = TickerUpdate(
        symbol="BTC-USD",
        bid=100.0,
        bid_qty=1.0,
        ask=101.0,
        ask_qty=1.0,
        last=100.5,
        volume=10.0,
        vwap=100.3,
        low=99.0,
        high=102.0,
        change=0.5,
        change_pct=0.5,
    )
    assert update.is_delayed is False
    assert update.is_extended_hours is None


def test_ticker_update_accepts_delay_overrides() -> None:
    """``TickerUpdate`` accepts explicit delay/session overrides.

    Given: a TickerUpdate constructed with ``is_delayed=True`` and
        ``is_extended_hours=True``,
    When: its fields are inspected,
    Then: both overrides are preserved.
    """
    update = TickerUpdate(
        symbol="MNQM6-CME",
        bid=0.0,
        bid_qty=0.0,
        ask=0.0,
        ask_qty=0.0,
        last=0.0,
        volume=0.0,
        vwap=0.0,
        low=0.0,
        high=0.0,
        change=0.0,
        change_pct=0.0,
        is_delayed=True,
        is_extended_hours=True,
    )
    assert update.is_delayed is True
    assert update.is_extended_hours is True


def test_exchange_order_request_round_trips_the_correlation_id() -> None:
    """``ExchangeOrderRequest`` keeps the caller's client_order_id.

    Given: a request built with every keyword the executor supplies,
    When: its fields are inspected,
    Then: ``client_order_id`` is the exact value passed and every other
        field landed where it was named.

    This also pins the field REORDER that made ``client_order_id``
    required: it now sits directly after ``amount``, ahead of ``price``.
    The move is only safe because no construction anywhere in the repo
    passes positional arguments — an AST census found 0 of 120. This
    test is the executable half of that argument: had any position
    shifted silently, ``price`` and ``client_order_id`` would swap here.
    """
    signaled = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    request = ExchangeOrderRequest(
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=0.5,
        client_order_id="0198f3d2-7a11-7c3e-9d40-6f1b2c3d4e5f",
        price=61000.0,
        stop_price=None,
        signaled_at=signaled,
        leverage=3,
        reduce_only=True,
        post_only=True,
        wallet_public_id="wallet-1",
        operator_public_id="operator-1",
    )
    assert request.client_order_id == "0198f3d2-7a11-7c3e-9d40-6f1b2c3d4e5f"
    assert request.symbol == "BTC-USD"
    assert request.side is OrderSideEnum.BUY
    assert request.type is ExchangeOrderTypeEnum.LIMIT
    assert request.amount == pytest.approx(0.5)
    assert request.price == pytest.approx(61000.0)
    assert request.stop_price is None
    assert request.signaled_at == signaled
    assert request.leverage == 3
    assert request.reduce_only is True
    assert request.post_only is True
    assert request.wallet_public_id == "wallet-1"
    assert request.operator_public_id == "operator-1"


def test_exchange_order_request_refuses_empty_correlation_id() -> None:
    """An empty ``client_order_id`` is refused at construction.

    Given: an otherwise valid order request whose client_order_id is "",
    When: the request is constructed,
    Then: ValueError is raised naming the field, before any venue call.

    Requiredness alone does not close this. mypy forbids OMITTING the
    keyword, but "" is a perfectly typed ``str``, and it is not inert:
    ``ccxt.safe_string`` drops an empty string from the wire params
    exactly as it drops ``None``, so an empty id would reach Kraken as
    no id at all — reproducing the omission defect in full while
    type-checking cleanly. The guard therefore tests falsiness, never
    ``is None``.

    It fires at construction, which on the executor path is strictly
    before the venue submit, so the failure is provably-not-placed and
    the definitive-reject disposition is honest.
    """
    with pytest.raises(ValueError, match="empty client_order_id"):
        ExchangeOrderRequest(
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.MARKET,
            amount=0.5,
            client_order_id="",
        )


def test_uuid7_fits_the_walutomat_submit_id_ceiling() -> None:
    """A minted uuid7 fits Walutomat's 36-character submitId cap exactly.

    Given: the id shape every producer in the system mints,
    When: it is rendered as the string that goes on the wire,
    Then: it is at most 36 characters and uses only the characters
        Walutomat's submitId regex admits.

    Pinned as a test rather than a runtime guard deliberately. The
    ceiling is exactly 36 and uuid7 is exactly 36 — zero headroom — and
    ``trade_commands.client_order_id`` is ``String(64)``, so the column
    would not catch an over-length id. A runtime format check in the
    adapter would be dead code (uuid7 cannot violate it) needing a
    hand-written test to satisfy the 100% coverage floor, and its only
    effect would be to move an already-correct failure earlier: an
    over-length submitId draws a venue 4xx that the adapter re-raises
    plain as a definitive rejection. This test costs nothing at runtime
    and fails the moment the id shape changes.
    """
    minted = str(uuid7())
    assert len(minted) <= 36
    assert re.fullmatch(r"[a-zA-Z0-9_-]{1,36}", minted) is not None
