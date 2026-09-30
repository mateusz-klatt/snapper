"""Tests for ``OrderFillFullRule``."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.rules.order_fill_full import OrderFillFullRule
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import TickData


def _now() -> datetime:
    """Fixed timestamp for deterministic dedup checks."""
    return datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)


def _reference(quote: str | None, symbol: str = "BTC-USD") -> InstrumentSymbolRefRow:
    """Return a historical quote reference spanning the test execution."""
    return {
        "instrument_public_id": "instrument-1",
        "native_symbol": symbol,
        "exchange": "kraken",
        "instrument_exchange": "kraken",
        "base_currency": "BTC",
        "quote_currency": quote,
        "valid_from": _now() - timedelta(days=1),
        "valid_to": _now() + timedelta(days=1),
    }


def _execution(status: str = "filled", user: str | None = "user-1") -> bytes:
    """Build an ExecutionData JSON payload as it would arrive on the bus."""
    data = ExecutionData(
        session_id="s1",
        sequence_id=1,
        public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
        timestamp=_now(),
        client_order_id="coid-abc",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        size=0.1,
        price=50000.0,
        last_size=0.1,
        last_price=50000.0,
        fee=5.0,
        fee_asset="USD",
        status=status,
        executed_at=_now(),
        wallet_public_id="wallet-1",
        operator_public_id="op-1",
        user_public_id=user,
    )
    return data.to_json().encode("utf-8")


class TestOrderFillFullRule:
    """Fire-on-FILLED, drop partials, dedup-aware, no-user-id guard."""

    @pytest.mark.asyncio
    async def test_fires_on_full_fill(self) -> None:
        """Happy path — FILLED emits one row with ``order_fill_full`` alert_type."""
        rule = OrderFillFullRule()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="instrument-1")
        repo.get_instrument_symbol_refs = AsyncMock(return_value=[_reference("USD")])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.executed",
            _execution(status="filled"),
            repo,
            _now(),
        )

        assert len(rows) == 1
        assert rows[0]["alert_type"] == "order_fill_full"
        assert rows[0]["dedup_key"] == "order_fill_full.coid-abc"
        assert "BTC-USD" in rows[0]["body"]
        payload = rows[0]["payload"]
        assert payload is not None
        assert payload["title_loc_key"] == "alerts.title.order_fill_full"
        assert payload["body_loc_key"] == "alerts.body.order_fill_full_quoted"
        assert payload["body_loc_args"] == [
            "BUY",
            "0.1",
            "BTC-USD",
            "50000.0 USD",
            "kraken",
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("symbol", "price", "instrument_id", "quote", "expected"),
        [
            ("BTC/PLN", 250000.0, "instrument-1", "PLN", "250000.0 PLN"),
            ("ETH/BTC", 0.00234, "instrument-1", "BTC", "0.00234 BTC"),
            ("VENUE-CODE", 1e-08, "instrument-1", "BTC", "0.00000001 BTC"),
            ("UNKNOWN-USD", 0.00234, None, "USD", "0.00234"),
            ("BTC-USD", 123.456789, "instrument-1", None, "123.456789"),
            ("BTC-USD", 123.456789, "instrument-1", "", "123.456789"),
        ],
    )
    async def test_quote_metadata_and_execution_precision_are_preserved(
        self,
        symbol: str,
        price: float,
        instrument_id: str | None,
        quote: str | None,
        expected: str,
    ) -> None:
        """Given a fill, its price retains precision and only a verified quote unit.

        When the venue symbol resolves, the repository supplies the unit.
        Missing identity or metadata never causes a USD guess from the symbol.
        """
        data = ExecutionData.model_validate_json(_execution())
        data = data.model_copy(update={"instrument": symbol, "price": price})
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=instrument_id)
        repo.get_instrument_symbol_refs = AsyncMock(return_value=[_reference(quote, symbol)])

        rows = await OrderFillFullRule().evaluate(
            f"orders.events.kraken.{symbol}.executed", data.to_json().encode(), repo, _now()
        )

        assert len(rows) == 1
        assert rows[0]["body"] == f"BUY 0.1 {symbol} @ {expected} filled on kraken"
        payload = rows[0]["payload"]
        assert payload is not None
        assert payload["body_loc_args"] == ["BUY", "0.1", symbol, expected, "kraken"]
        repo.get_instrument_public_id_by_symbol.assert_awaited_once_with(
            symbol, "kraken", data.executed_at
        )
        if instrument_id is None:
            repo.get_instrument_symbol_refs.assert_not_awaited()
        else:
            repo.get_instrument_symbol_refs.assert_awaited_once_with([instrument_id], _now())

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "scenario",
        ["quote_revision", "base_revision", "missing", "expired", "future", "symbol", "venue"],
    )
    async def test_ambiguous_or_unmatched_history_never_labels_a_fill(self, scenario: str) -> None:
        """Given delayed fills and revised metadata, uncertain quote units are withheld.

        Currency-leg changes cannot be distinguished from retrospective
        corrections, so even an old covering interval cannot justify a unit.
        """
        original = _reference("USD")
        original["valid_to"] = _now() + timedelta(minutes=30)
        replacement = _reference("USD")
        replacement["valid_from"] = original["valid_to"]
        scenarios: dict[str, list[InstrumentSymbolRefRow]] = {
            "quote_revision": [original, {**replacement, "quote_currency": "PLN"}],
            "base_revision": [original, {**replacement, "base_currency": "ETH"}],
            "missing": [],
            "expired": [{**original, "valid_to": _now()}],
            "future": [{**original, "valid_from": _now() + timedelta(seconds=1)}],
            "symbol": [{**original, "native_symbol": "OTHER"}],
            "venue": [{**original, "instrument_exchange": "paper"}],
        }
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="instrument-1")
        repo.get_instrument_symbol_refs = AsyncMock(return_value=scenarios[scenario])
        now = _now() + timedelta(hours=1)

        rows = await OrderFillFullRule().evaluate(
            "orders.events.kraken.BTC-USD.executed", _execution(), repo, now
        )

        assert rows[0]["body"] == "BUY 0.1 BTC-USD @ 50000.0 filled on kraken"
        repo.get_instrument_symbol_refs.assert_awaited_once_with(["instrument-1"], now)

    @pytest.mark.asyncio
    async def test_compatible_history_selects_the_execution_reference(self) -> None:
        """Given a symbol rename with unchanged currency legs, the matching interval is used."""
        previous = _reference("USD", "OLDER-SYMBOL")
        previous["valid_to"] = _now()
        current = _reference("USD")
        current["valid_from"] = _now()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="instrument-1")
        repo.get_instrument_symbol_refs = AsyncMock(return_value=[previous, current])

        rows = await OrderFillFullRule().evaluate(
            "orders.events.kraken.BTC-USD.executed", _execution(), repo, _now()
        )

        assert rows[0]["body"] == "BUY 0.1 BTC-USD @ 50000.0 USD filled on kraken"

    @pytest.mark.asyncio
    async def test_missing_execution_timezone_keeps_precise_unlabelled_price(self) -> None:
        """Given a timezone-free event, no historical currency proof is invented."""
        data = ExecutionData.model_validate_json(_execution())
        data = data.model_copy(update={"executed_at": data.executed_at.replace(tzinfo=None)})
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await OrderFillFullRule().evaluate(
            "orders.events.kraken.BTC-USD.executed", data.to_json().encode(), repo, _now()
        )

        assert rows[0]["body"] == "BUY 0.1 BTC-USD @ 50000.0 filled on kraken"
        repo.get_instrument_public_id_by_symbol.assert_not_called()
        repo.get_instrument_symbol_refs.assert_not_called()

    @pytest.mark.asyncio
    async def test_ignored_on_partial(self) -> None:
        """Partial fills never fire (only final FILLED emits)."""
        rule = OrderFillFullRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.executed",
            _execution(status="partial"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_ignores_non_executed_topic(self) -> None:
        """Rule skips events whose topic does not end with ``.executed``."""
        rule = OrderFillFullRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _execution(status="filled"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_drops_alert_when_user_public_id_missing(self) -> None:
        """No user_public_id → silent drop (cannot page a no-op scope)."""
        rule = OrderFillFullRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.executed",
            _execution(user=None),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_malformed_payload_silently_dropped(self) -> None:
        """Invalid JSON / wrong schema yields [] without raising."""
        rule = OrderFillFullRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.executed",
            b"not-json",
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_non_execution_payload_dropped(self) -> None:
        """A well-formed non-ExecutionData message (e.g. tick) yields []."""
        tick = TickData(
            session_id="s",
            sequence_id=1,
            public_id="pid",
            timestamp=_now(),
            instrument="BTC-USD",
            volume=0.0,
            exchange="kraken",
        )
        rule = OrderFillFullRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.executed",
            tick.to_json().encode("utf-8"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_dedup_window_hit_suppresses_fire(self) -> None:
        """A pre-existing alert_event inside the dedup window suppresses the fire."""
        rule = OrderFillFullRule()
        rule.suppression_window_seconds = 300
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[{"public_id": "prior"}])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.executed",
            _execution(status="filled"),
            repo,
            _now(),
        )

        assert rows == []
