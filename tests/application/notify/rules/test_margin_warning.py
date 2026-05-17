"""Tests for ``MarginWarningRule``.

Covers the substring-match predicate, the fire path on margin-keyword
rejections, the skip path on non-margin rejections, dedup, and the
shape of the emitted ``AlertEventInsertRow`` (deep-link, thread-key,
safety-critical flag).
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.rules.margin_warning import MarginWarningRule
from snapper.application.notify.rules.margin_warning import is_margin_related_rejection
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData


def _now() -> datetime:
    """Deterministic UTC now for fixtures."""
    return datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)


def _order(
    reason: str | None = "Margin requirement not met",
    error: str | None = None,
    user: str | None = "user-1",
    wallet: str | None = "wallet-1",
    client_order_id: str = "coid-mw",
) -> bytes:
    """Build a rejected ``OrderData`` payload as JSON bytes."""
    data = OrderData(
        session_id="s1",
        sequence_id=1,
        public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
        timestamp=_now(),
        client_order_id=client_order_id,
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        status="rejected",
        order_type="market",
        size=0.5,
        filled_size=0.0,
        reason=reason,
        error=error,
        created_at=_now(),
        wallet_public_id=wallet or "",
        operator_public_id=None,
        user_public_id=user,
    )
    return data.to_json().encode("utf-8")


class TestIsMarginRelatedRejection:
    """Predicate behaviour for the margin-keyword classifier.

    Checked directly so both ``MarginWarningRule`` and
    ``OrderRejectedRule`` rely on a single trusted partition.
    """

    @pytest.mark.parametrize(
        "reason,error",
        [
            ("Margin requirement not met", None),
            ("insufficient funds", None),
            ("Insufficient Balance", None),
            ("leverage exceeds account limit", None),
            ("stop out triggered", None),
            ("position would liquidate", None),
            (None, "MARGIN_CALL"),
            ("submit failed", "collateral too low"),
        ],
    )
    def test_returns_true_for_margin_keywords(self, reason: str | None, error: str | None) -> None:
        """Every documented margin keyword variant fires the predicate."""
        assert is_margin_related_rejection(reason, error) is True

    @pytest.mark.parametrize(
        "reason,error",
        [
            (None, None),
            ("", None),
            ("rate limited", None),
            ("instrument suspended", None),
            ("invalid client_order_id", None),
            (None, "EOrder:Unknown order"),
        ],
    )
    def test_returns_false_for_non_margin_payloads(
        self, reason: str | None, error: str | None
    ) -> None:
        """Predicate stays narrow — only margin-keyword strings flag."""
        assert is_margin_related_rejection(reason, error) is False


class TestMarginWarningRule:
    """Behaviour of the rule's evaluate() — fire / skip / dedup paths."""

    @pytest.mark.asyncio
    async def test_fires_on_margin_keyword_rejection(self) -> None:
        """Margin-keyword rejection emits one safety-critical high-priority alert."""
        rule = MarginWarningRule()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _order(reason="Margin requirement not met"),
            repo,
            _now(),
        )

        assert len(rows) == 1
        row = rows[0]
        assert row["alert_type"] == "margin_warning"
        assert row["is_safety_critical"] is True
        assert row["priority"] == "high"
        assert row["title"] == "Margin warning"
        assert "BTC-USD" in row["body"]
        assert row["dedup_key"] == "margin_warning.coid-mw"
        assert row["thread_key"] == "snapper.margin.wallet-1"
        payload = row["payload"]
        assert payload is not None
        assert payload["deep_link_path"] == "/orders/coid-mw"
        assert payload["client_order_id"] == "coid-mw"
        assert payload["title_loc_key"] == "alerts.title.margin_warning"
        assert payload["body_loc_key"] == "alerts.body.margin_warning"
        assert payload["body_loc_args"] == [
            "BUY",
            "0.5",
            "BTC-USD",
            "Margin requirement not met",
        ]
        assert row["source_topic"] == "orders.events.kraken.BTC-USD.rejected"

    @pytest.mark.asyncio
    async def test_skips_non_margin_rejection(self) -> None:
        """Non-margin rejection passes through to ``OrderRejectedRule`` only."""
        rule = MarginWarningRule()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _order(reason="instrument suspended"),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_skips_non_rejected_topic(self) -> None:
        """``executed`` events fall through even with margin keyword in payload."""
        rule = MarginWarningRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.executed",
            _order(),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_skips_when_user_public_id_missing(self) -> None:
        """Margin event without a user scope drops silently — no fan-out path."""
        rule = MarginWarningRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _order(user=None),
            repo,
            _now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_thread_key_falls_back_when_wallet_missing(self) -> None:
        """No wallet -> ``snapper.margin.no-wallet`` thread key."""
        rule = MarginWarningRule()
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            _order(wallet=None),
            repo,
            _now(),
        )

        assert rows[0]["thread_key"] == "snapper.margin.no-wallet"

    @pytest.mark.asyncio
    async def test_dedup_window_hit_suppresses_fire(self) -> None:
        """Prior alert with the same dedup key in window -> skip."""
        rule = MarginWarningRule()
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
    async def test_malformed_payload_dropped(self) -> None:
        """Broken JSON returns [] without raising."""
        rule = MarginWarningRule()
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
        """Well-formed non-OrderData payload (ExecutionData) yields []."""
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
        rule = MarginWarningRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "orders.events.kraken.BTC-USD.rejected",
            ex.to_json().encode("utf-8"),
            repo,
            _now(),
        )

        assert rows == []
