"""Tests for ``snapper.application.notify.rules.dedup.check_dedup_window``."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.rules.dedup import check_dedup_window


def _ts(seconds: int = 0) -> datetime:
    """Deterministic UTC timestamp offset by ``seconds`` from a fixed base."""
    return datetime(2026, 4, 24, 12, 0, seconds, tzinfo=UTC)


class TestCheckDedupWindow:
    """Covers zero-window short-circuit, hit, and miss."""

    @pytest.mark.asyncio
    async def test_zero_window_never_matches(self) -> None:
        """``window_seconds = 0`` returns False without a repo call."""
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=["anything"])

        hit = await check_dedup_window(
            repo=repo,
            user_public_id="user-1",
            dedup_key="order_fill_full.coid-1",
            window_seconds=0,
            now=_ts(0),
        )

        assert hit is False
        repo.list_alert_events_with_dedup_key.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_within_window_returns_true(self) -> None:
        """Non-empty repo response suppresses the pending alert."""
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[{"public_id": "pid"}])

        hit = await check_dedup_window(
            repo=repo,
            user_public_id="user-1",
            dedup_key="order_fill_full.coid-1",
            window_seconds=60,
            now=_ts(30),
        )

        assert hit is True
        repo.list_alert_events_with_dedup_key.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_outside_window_returns_false(self) -> None:
        """Empty repo response lets the pending alert through."""
        repo = MagicMock()
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])

        hit = await check_dedup_window(
            repo=repo,
            user_public_id="user-1",
            dedup_key="order_fill_full.coid-1",
            window_seconds=60,
            now=_ts(0),
        )

        assert hit is False
