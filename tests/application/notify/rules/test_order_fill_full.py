"""Tests for ``OrderFillFullRule`` (§D6.1 Rule 1)."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.rules.order_fill_full import OrderFillFullRule
from snapper.messaging.schemas.data import ExecutionData


def _now() -> datetime:
    """Fixed timestamp for deterministic dedup checks."""
    return datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)


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
        from snapper.messaging.schemas.data import TickData

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
