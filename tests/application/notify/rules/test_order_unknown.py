"""Tests for ``OrderUnknownRule`` (#145 P0-1 ambiguous-submit alert)."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.rules.order_rejected import OrderRejectedRule
from snapper.application.notify.rules.order_unknown import OrderUnknownRule
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData


def _now() -> datetime:
    """Fixed timestamp for deterministic dedup checks."""
    return datetime(2026, 6, 10, 1, 0, 0, tzinfo=UTC)


def _order(
    error: str | None = "timeout after send",
    user: str | None = "user-1",
) -> bytes:
    """Build an OrderData JSON payload for the unknown.* topic family."""
    data = OrderData(
        session_id="s1",
        sequence_id=1,
        public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
        timestamp=_now(),
        client_order_id="coid-unk",
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        status="unknown",
        order_type="market",
        size=0.2,
        filled_size=0.0,
        reason=None,
        error=error,
        created_at=_now(),
        wallet_public_id="wallet-1",
        operator_public_id=None,
        user_public_id=user,
    )
    return data.to_json().encode("utf-8")


class TestOrderUnknownRule:
    """Fires on every unknown event; safety-critical; warns against assuming flat."""

    @pytest.mark.asyncio
    async def test_fires_on_unknown(self) -> None:
        """An unknown event emits one safety-critical high-priority alert."""
        rule = OrderUnknownRule()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.unknown",
            _order(),
            repo,
            _now(),
        )

        assert len(rows) == 1
        assert rows[0]["is_safety_critical"] is True
        assert rows[0]["priority"] == "high"
        assert "do not assume flat" in rows[0]["body"]
        payload = rows[0]["payload"]
        assert payload is not None
        assert payload["title_loc_key"] == "alerts.title.order_unknown"
        assert payload["body_loc_key"] == "alerts.body.order_unknown"
        assert payload["body_loc_args"] == [
            "BUY",
            "0.2",
            "BTC-USD",
            "timeout after send",
        ]

    @pytest.mark.asyncio
    async def test_ignores_other_suffixes(self) -> None:
        """Non-unknown topics are ignored by this rule."""
        rule = OrderUnknownRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _order(),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_rejected_rule_does_not_fire_on_unknown(self) -> None:
        """OrderRejectedRule stays silent on the unknown suffix.

        The whole point of UNKNOWN is that the order was NOT rejected —
        a rejection alert here would tell the operator the opposite of
        the truth.
        """
        rule = OrderRejectedRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.unknown",
            _order(),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_strategy_order_fans_out_to_admins(self) -> None:
        """Events without user scope fan out to the admin set.

        Strategy orders have no user_public_id; silencing their alert
        would hide exactly the unknowns nobody is watching, so they go
        to every admin with read:system_status (one row each).
        """
        rule = OrderUnknownRule()
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=["admin-1", "admin-2"])
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.unknown",
            _order(user=None),
            repo,
            _now(),
        )

        assert [r["user_public_id"] for r in rows] == ["admin-1", "admin-2"]
        repo.list_users_with_permission.assert_awaited_once_with("read:system_status")

    @pytest.mark.asyncio
    async def test_strategy_order_with_no_admins_drops(self) -> None:
        """No user scope and no admins leaves nothing to deliver."""
        rule = OrderUnknownRule()
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.unknown",
            _order(user=None),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_malformed_payload_returns_empty(self) -> None:
        """Unparseable payload bytes produce no alert."""
        rule = OrderUnknownRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.unknown",
            b"not json",
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_non_order_payload_returns_empty(self) -> None:
        """A valid message of a different type produces no alert."""
        rule = OrderUnknownRule()
        repo = MagicMock()
        fill = ExecutionData(
            session_id="s1",
            sequence_id=1,
            public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
            timestamp=_now(),
            trade_id="t-1",
            exchange_order_id="ex-1",
            client_order_id="coid-unk",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            size=0.2,
            price=50000.0,
            last_size=0.2,
            last_price=50000.0,
            fee=0.1,
            fee_asset="USD",
            status="filled",
            executed_at=_now(),
        )

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.unknown",
            fill.to_json().encode("utf-8"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_dedup_window_suppresses_repeat(self) -> None:
        """A second unknown alert within the window is suppressed."""
        rule = OrderUnknownRule()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[{"created_at": _now()}])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.unknown",
            _order(),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_default_reason_when_error_missing(self) -> None:
        """A missing error falls back to the generic ambiguous text."""
        rule = OrderUnknownRule()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.unknown",
            _order(error=None),
            repo,
            _now(),
        )

        assert len(rows) == 1
        assert "ambiguous venue response" in rows[0]["body"]
