"""Shared test doubles for DB-backed deactivation fallback tests."""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest


class InactiveUserLookupRepo:
    """Repository stub that returns configured inactive user ids."""

    def __init__(self, inactive_user_public_ids: list[str]) -> None:
        """Capture inactive ids and query history."""
        self.inactive_user_public_ids = inactive_user_public_ids
        self.queries: list[list[str]] = []

    async def list_inactive_user_public_ids(self, user_public_ids: list[str]) -> list[str]:
        """Return inactive ids from the provided candidate list."""
        self.queries.append(user_public_ids)
        return [
            user_public_id
            for user_public_id in user_public_ids
            if user_public_id in self.inactive_user_public_ids
        ]


class FailingInactiveUserLookupRepo:
    """Repository stub whose inactive lookup raises."""

    async def list_inactive_user_public_ids(self, user_public_ids: list[str]) -> list[str]:
        """Raise to exercise fallback-scan resilience."""
        raise RuntimeError(f"db unavailable for {len(user_public_ids)} users")


async def assert_fallback_loop_propagates_scan_cancelled(
    loop: Callable[[], Awaitable[None]],
    target: object,
    scan_method_name: str,
) -> None:
    """Assert fallback loop cancellation propagates from the scan callback."""
    with (
        patch.object(
            target,
            scan_method_name,
            new=AsyncMock(side_effect=asyncio.CancelledError()),
        ),
        pytest.raises(asyncio.CancelledError),
    ):
        await loop()


async def assert_fallback_loop_propagates_sleep_cancelled(
    loop: Callable[[], Awaitable[None]],
) -> None:
    """Assert fallback loop cancellation propagates from interval sleep."""
    sleep_mock = AsyncMock(side_effect=asyncio.CancelledError())
    with (
        patch("snapper.auth.deactivation_fallback._deactivation_fallback_sleep", new=sleep_mock),
        pytest.raises(asyncio.CancelledError),
    ):
        await loop()
    sleep_mock.assert_awaited_once_with(5.0)
