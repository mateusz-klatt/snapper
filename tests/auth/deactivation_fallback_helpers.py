"""Shared test doubles for DB-backed deactivation fallback tests."""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from snapper.data.repository_types import UserOperatorMembershipRow


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


class MembershipLookupRepo(InactiveUserLookupRepo):
    """Repository stub exposing active memberships alongside user state."""

    def __init__(self, active_operator_public_ids: dict[str, set[str]]) -> None:
        """Capture active memberships and query history."""
        super().__init__([])
        self.active_operator_public_ids = active_operator_public_ids
        self.membership_queries: list[tuple[str, datetime]] = []

    async def get_user_operator_memberships(
        self,
        user_public_id: str,
        as_of: datetime,
    ) -> list[UserOperatorMembershipRow]:
        """Return active membership rows for the requested user."""
        self.membership_queries.append((user_public_id, as_of))
        return [
            UserOperatorMembershipRow(
                public_id=f"membership-{operator_public_id}",
                user_public_id=user_public_id,
                operator_public_id=operator_public_id,
                is_primary=index == 0,
                timestamp=as_of,
                session_id="membership-test",
                sequence_id=index + 1,
            )
            for index, operator_public_id in enumerate(
                sorted(self.active_operator_public_ids.get(user_public_id, set()))
            )
        ]


class FailingMembershipLookupRepo(InactiveUserLookupRepo):
    """Repository stub whose membership lookup raises."""

    def __init__(self) -> None:
        """Initialize the inactive-user half with no findings."""
        super().__init__([])

    async def get_user_operator_memberships(
        self,
        user_public_id: str,
        as_of: datetime,
    ) -> list[UserOperatorMembershipRow]:
        """Raise to exercise tolerant membership fallback handling."""
        raise RuntimeError(
            f"membership db unavailable for user={user_public_id} at={as_of.isoformat()}"
        )


class MembershipTokenLookupRepo(MembershipLookupRepo):
    """Repository stub exposing active memberships and token JTIs."""

    def __init__(
        self,
        active_operator_public_ids: dict[str, set[str]],
        active_token_jtis: dict[str, set[str]],
    ) -> None:
        """Capture active authority state and token-query history."""
        super().__init__(active_operator_public_ids)
        self.active_token_jtis = active_token_jtis
        self.token_queries: list[str] = []

    async def list_active_user_token_jtis(self, user_public_id: str) -> list[str]:
        """Return active token JTIs for one user."""
        self.token_queries.append(user_public_id)
        return sorted(self.active_token_jtis.get(user_public_id, set()))


class FailingTokenInventoryLookupRepo(InactiveUserLookupRepo):
    """Repository stub whose token-inventory lookup raises."""

    def __init__(self) -> None:
        """Initialize the inactive-user half with no findings."""
        super().__init__([])

    async def list_active_user_token_jtis(self, user_public_id: str) -> list[str]:
        """Raise to exercise tolerant token-inventory reconciliation."""
        raise RuntimeError(f"token inventory unavailable for user={user_public_id}")


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
