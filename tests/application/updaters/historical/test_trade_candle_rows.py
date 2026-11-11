"""Verify the row builders shared by the trade candle backfill and the candle repair driver."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from snapper.application.updaters.historical.trade_candle_rows import as_utc
from snapper.application.updaters.historical.trade_candle_rows import calculated_row
from snapper.application.updaters.historical.trade_candle_rows import trade_update
from snapper.data.repository_types import TradeRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate

_PLUS_TWO = timezone(timedelta(hours=2))


def test_as_utc_reads_naive_values_as_utc_and_converts_aware_ones() -> None:
    """Naive times are UTC and aware times are converted.

    Given: A naive time and the same instant at UTC+2.
    When: Both are normalized.
    Then: Both are the same aware UTC instant.
    """
    naive = datetime(2026, 11, 10, 15, 0)
    shifted = datetime(2026, 11, 10, 17, 0, tzinfo=_PLUS_TWO)
    assert as_utc(naive) == datetime(2026, 11, 10, 15, 0, tzinfo=UTC)
    assert as_utc(shifted).tzinfo is UTC
    assert as_utc(shifted) == as_utc(naive)


def test_trade_update_keys_the_builder_by_instrument_and_execution_time() -> None:
    """A stored trade becomes a market trade of its instrument at its execution time.

    Given: A stored trade with numeric size and price.
    When: It is converted.
    Then: Every builder field carries the stored value.
    """
    executed = datetime(2026, 11, 10, 15, 0, 5, tzinfo=UTC)
    stored: TradeRow = {
        "timestamp": executed + timedelta(seconds=1),
        "executed_at": executed,
        "price": 1.25,
        "size": 4,
        "side": "sell",
        "trade_id": "t-1",
    }
    assert trade_update(stored, "instrument", executed) == TradeUpdate(
        symbol="instrument",
        side="sell",
        quantity=4.0,
        price=1.25,
        ord_type="market",
        timestamp=executed,
        trade_id="t-1",
    )


def test_calculated_row_carries_the_bar_and_the_given_provenance() -> None:
    """A builder candle becomes a complete calculated one-minute row.

    Given: A candle opening at 17:00 at UTC+2.
    When: It is turned into a row with a knowledge time, session and sequence.
    Then: The row opens at 15:00 UTC and carries the bar, the source and the provenance.
    """
    known = datetime(2026, 11, 11, 9, 0, tzinfo=UTC)
    candle = CandleUpdate(
        symbol="instrument",
        open=1.0,
        high=1.5,
        low=0.5,
        close=1.25,
        vwap=1.1,
        trades=3,
        volume=6.0,
        interval_begin=datetime(2026, 11, 10, 17, 0, tzinfo=_PLUS_TWO),
        interval=60,
    )
    row = calculated_row(candle, "instrument", timestamp=known, session_id="s", sequence_id=9)
    assert row == {
        "instrument_public_id": "instrument",
        "open_at": datetime(2026, 11, 10, 15, 0, tzinfo=UTC),
        "timestamp": known,
        "timeframe": "1m",
        "open": 1.0,
        "high": 1.5,
        "low": 0.5,
        "close": 1.25,
        "volume": 6.0,
        "vwap": 1.1,
        "trades": 3,
        "source": "calculated",
        "complete": True,
        "session_id": "s",
        "sequence_id": 9,
    }
