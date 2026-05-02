"""Tests for ``OrderRejectedRule``."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.rules.order_rejected import OrderRejectedRule
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData


def _now() -> datetime:
    """Fixed timestamp for deterministic dedup checks."""
    return datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)


def _order(
    reason: str | None = "insufficient_funds",
    error: str | None = None,
    user: str | None = "user-1",
) -> bytes:
    """Build an OrderData JSON payload for the rejected.* topic family."""
    data = OrderData(
        session_id="s1",
        sequence_id=1,
        public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
        timestamp=_now(),
        client_order_id="coid-rej",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        status="rejected",
        order_type="market",
        size=0.2,
        filled_size=0.0,
        reason=reason,
        error=error,
        created_at=_now(),
        wallet_public_id="wallet-1",
        operator_public_id=None,
        user_public_id=user,
    )
    return data.to_json().encode("utf-8")


class TestOrderRejectedRule:
    """Fires every rejected event, safety-critical, body contains reason."""

    @pytest.mark.asyncio
    async def test_fires_on_rejection(self) -> None:
        """Rejection emits one safety-critical high-priority alert."""
        rule = OrderRejectedRule()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _order(reason="insufficient_funds"),
            repo,
            _now(),
        )

        assert len(rows) == 1
        assert rows[0]["is_safety_critical"] is True
        assert rows[0]["priority"] == "high"
        assert "insufficient_funds" in rows[0]["body"]

    @pytest.mark.asyncio
    async def test_emits_fallback_when_no_reason(self) -> None:
        """No reason + no error → ``unknown reason`` in body."""
        rule = OrderRejectedRule()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _order(reason=None, error=None),
            repo,
            _now(),
        )

        assert "unknown reason" in rows[0]["body"]

    @pytest.mark.asyncio
    async def test_ignores_non_rejected_topic(self) -> None:
        """Rule skips non-``rejected`` topics in the orders.events namespace."""
        rule = OrderRejectedRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.executed",
            _order(),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_drops_alert_when_user_public_id_missing(self) -> None:
        """No user → silent drop."""
        rule = OrderRejectedRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _order(user=None),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_malformed_payload_dropped(self) -> None:
        """Broken JSON yields [] without raising."""
        rule = OrderRejectedRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            b"not-json",
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_non_order_payload_dropped(self) -> None:
        """Well-formed non-OrderData (e.g. ExecutionData) yields []."""
        ex = ExecutionData(
            session_id="s",
            sequence_id=1,
            public_id="pid",
            timestamp=_now(),
            client_order_id="x",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            size=0.1,
            price=1.0,
            last_size=0.1,
            last_price=1.0,
            fee=0.0,
            fee_asset="USD",
            status="filled",
            executed_at=_now(),
        )
        rule = OrderRejectedRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            ex.to_json().encode("utf-8"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_dedup_window_hit_suppresses_fire(self) -> None:
        """A prior alert_event in the dedup window suppresses the rejected fire."""
        rule = OrderRejectedRule()
        rule.suppression_window_seconds = 300
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[{"public_id": "prior"}])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _order(),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_skips_margin_keyword_rejections(self) -> None:
        """Margin-keyword rejections are partitioned out to ``MarginWarningRule``.

        Both rules subscribe to ``orders.events.`` so the registry
        dispatches every ``.rejected`` event to both. The shared
        ``is_margin_related_rejection`` predicate keeps each rule's
        emission set disjoint so the user gets one alert per event,
        not two.
        """
        rule = OrderRejectedRule()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _order(reason="Margin requirement not met"),
            repo,
            _now(),
        )

        assert rows == []
