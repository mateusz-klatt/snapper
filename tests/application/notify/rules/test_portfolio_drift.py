"""Tests for ``PortfolioDriftRule``."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.rules.portfolio_drift import PortfolioDriftRule
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.repository_types import ScopeGrantRow
from snapper.messaging.schemas.data import PortfolioDriftEpisodeEventData
from snapper.messaging.schemas.data import TickData

_TOPIC = "bus.portfolio_drift_episode"
_EPISODE_PUBLIC_ID = "019dbb34-f439-77bd-afa8-ee5321d60307"
_EVENT_PUBLIC_ID = "019dbb34-f439-77bd-afa8-ee5321d60308"
_WALLET_PUBLIC_ID = "019dbb34-f439-77bd-afa8-ee5321d60309"


def _now() -> datetime:
    """Return a deterministic aware timestamp."""
    return datetime(2026, 7, 15, 12, 0, tzinfo=UTC)


def _event(
    lifecycle: Literal["opened", "resolved"] = "opened",
    *,
    mismatch_count: int = 3,
) -> bytes:
    """Build a serialized drift-episode lifecycle event."""
    resolved = lifecycle == "resolved"
    data = PortfolioDriftEpisodeEventData(
        session_id="session-drift",
        sequence_id=1,
        public_id=_EVENT_PUBLIC_ID,
        timestamp=_now(),
        wallet_public_id=_WALLET_PUBLIC_ID,
        exchange="kraken",
        mode="live",
        episode_public_id=_EPISODE_PUBLIC_ID,
        lifecycle=lifecycle,
        opened_at=_now(),
        closed_at=_now() + timedelta(minutes=10) if resolved else None,
        mismatch_count=mismatch_count,
        resolution_reason="matched" if resolved else None,
    )
    return data.to_json().encode("utf-8")


def _grant(operator_public_id: str) -> ScopeGrantRow:
    """Build the active grant projection used for owner discovery."""
    return ScopeGrantRow(
        public_id=f"grant-{operator_public_id}",
        operator_public_id=operator_public_id,
        wallet_public_id=_WALLET_PUBLIC_ID,
        granted_by_user_public_id="grantor-1",
        scope_kind="instrument",
        underlying_public_id=None,
        instrument_public_id="instrument-1",
        note=None,
        timestamp=_now(),
        known_to=KNOWN_TO_MAX,
        session_id="session-grant",
        sequence_id=1,
    )


def _repo(
    *,
    grants: list[ScopeGrantRow] | None = None,
    memberships: dict[str, list[str]] | None = None,
    dedup_hit: bool = False,
) -> MagicMock:
    """Build a repository mock with deterministic scope and dedup reads."""
    resolved_grants = [_grant("operator-1")] if grants is None else grants
    resolved_memberships = {"operator-1": ["user-1"]} if memberships is None else memberships
    repo = MagicMock()
    repo.list_active_scope_grants_for_wallet = AsyncMock(return_value=resolved_grants)

    async def list_members(operator_public_id: str, as_of: datetime) -> list[str]:
        """Return the configured active users for one operator."""
        return resolved_memberships.get(operator_public_id, [])

    repo.list_users_with_operator_membership = AsyncMock(side_effect=list_members)
    repo.list_alert_events_with_dedup_key = AsyncMock(
        return_value=[{"public_id": "prior-alert"}] if dedup_hit else []
    )
    return repo


class TestPortfolioDriftRule:
    """Covers open, resolution, dedup, owner scope, and ignore paths."""

    @pytest.mark.asyncio
    async def test_open_pages_one_owning_user(self) -> None:
        """An opened episode emits one high-priority safety-critical page."""
        rule = PortfolioDriftRule()
        repo = _repo()

        rows = await rule.evaluate(_TOPIC, _event(), repo, _now())

        assert rule.subscribe_topic_prefixes == (_TOPIC,)
        assert rule.suppression_window_seconds == 0
        assert len(rows) == 1
        row = rows[0]
        assert row["user_public_id"] == "user-1"
        assert row["operator_public_id"] == "operator-1"
        assert row["wallet_public_id"] == _WALLET_PUBLIC_ID
        assert row["alert_type"] == "drift"
        assert row["priority"] == "high"
        assert row["is_safety_critical"] is True
        assert row["title"] == "Portfolio drift detected"
        assert "3 consecutive full mismatches" in row["body"]
        assert row["dedup_key"] == f"drift.{_EPISODE_PUBLIC_ID}"
        assert row["thread_key"] == f"snapper.drift.{_EPISODE_PUBLIC_ID}"
        assert row["source_topic"] == _TOPIC
        payload = row["payload"]
        assert payload is not None
        assert payload == {
            "deep_link_path": "/portfolio/accounts",
            "episode_public_id": _EPISODE_PUBLIC_ID,
            "lifecycle": "opened",
            "exchange": "kraken",
            "mode": "live",
            "mismatch_count": 3,
            "opened_at": _now().isoformat(),
            "closed_at": None,
            "resolution_reason": None,
            "body_suppressed": False,
        }
        repo.list_active_scope_grants_for_wallet.assert_awaited_once_with(
            _WALLET_PUBLIC_ID,
            _now(),
        )
        repo.list_alert_events_with_dedup_key.assert_awaited_once_with(
            user_public_id="user-1",
            dedup_key=f"drift.{_EPISODE_PUBLIC_ID}",
            since=_now(),
        )

    @pytest.mark.asyncio
    async def test_resolution_uses_distinct_key_and_same_thread(self) -> None:
        """A resolved episode emits a resolution notice without re-paging open."""
        rule = PortfolioDriftRule()
        repo = _repo()

        rows = await rule.evaluate(
            _TOPIC,
            _event("resolved", mismatch_count=5),
            repo,
            _now() + timedelta(minutes=10),
        )

        assert len(rows) == 1
        row = rows[0]
        assert row["title"] == "Portfolio drift resolved"
        assert row["dedup_key"] == f"drift.resolved.{_EPISODE_PUBLIC_ID}"
        assert row["thread_key"] == f"snapper.drift.{_EPISODE_PUBLIC_ID}"
        assert row["priority"] == "high"
        assert row["is_safety_critical"] is True
        assert row["body"].endswith(": matched")
        payload = row["payload"]
        assert payload is not None
        assert payload["lifecycle"] == "resolved"
        assert payload["mismatch_count"] == 5
        assert payload["closed_at"] == (_now() + timedelta(minutes=10)).isoformat()
        assert payload["resolution_reason"] == "matched"

    @pytest.mark.parametrize(
        ("lifecycle", "expected_key"),
        [
            ("opened", f"drift.{_EPISODE_PUBLIC_ID}"),
            ("resolved", f"drift.resolved.{_EPISODE_PUBLIC_ID}"),
        ],
    )
    @pytest.mark.asyncio
    async def test_replayed_transition_is_suppressed_for_episode_lifetime(
        self,
        lifecycle: Literal["opened", "resolved"],
        expected_key: str,
    ) -> None:
        """A persisted same-lifecycle notice suppresses every later replay."""
        rule = PortfolioDriftRule()
        repo = _repo(dedup_hit=True)

        rows = await rule.evaluate(_TOPIC, _event(lifecycle), repo, _now())

        assert rows == []
        repo.list_alert_events_with_dedup_key.assert_awaited_once_with(
            user_public_id="user-1",
            dedup_key=expected_key,
            since=_now(),
        )

    @pytest.mark.asyncio
    async def test_shared_user_is_deduped_to_first_sorted_owner(self) -> None:
        """Grant and membership duplicates cannot produce duplicate user pages."""
        rule = PortfolioDriftRule()
        repo = _repo(
            grants=[_grant("operator-b"), _grant("operator-a"), _grant("operator-a")],
            memberships={
                "operator-a": ["user-shared", "user-a", "user-a"],
                "operator-b": ["user-b", "user-shared"],
            },
        )

        rows = await rule.evaluate(_TOPIC, _event(), repo, _now())

        assert [row["user_public_id"] for row in rows] == [
            "user-a",
            "user-b",
            "user-shared",
        ]
        assert [row["operator_public_id"] for row in rows] == [
            "operator-a",
            "operator-b",
            "operator-a",
        ]
        assert repo.list_users_with_operator_membership.await_args_list[0].args == (
            "operator-a",
            _now(),
        )
        assert repo.list_users_with_operator_membership.await_args_list[1].args == (
            "operator-b",
            _now(),
        )
        assert repo.list_alert_events_with_dedup_key.await_count == 3

    @pytest.mark.asyncio
    async def test_wallet_without_active_owner_grants_is_ignored(self) -> None:
        """No active grant means no owner recipient and no administrator fallback."""
        rule = PortfolioDriftRule()
        repo = _repo(grants=[])

        rows = await rule.evaluate(_TOPIC, _event(), repo, _now())

        assert rows == []
        repo.list_users_with_operator_membership.assert_not_awaited()
        repo.list_alert_events_with_dedup_key.assert_not_awaited()
        repo.list_users_with_permission.assert_not_called()

    @pytest.mark.asyncio
    async def test_owner_without_active_user_membership_is_ignored(self) -> None:
        """An owning operator with no active users cannot receive a notification."""
        rule = PortfolioDriftRule()
        repo = _repo(memberships={})

        rows = await rule.evaluate(_TOPIC, _event(), repo, _now())

        assert rows == []
        repo.list_users_with_operator_membership.assert_awaited_once()
        repo.list_alert_events_with_dedup_key.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_wrong_topic_is_ignored_before_parsing(self) -> None:
        """Only the exact committed lifecycle topic reaches owner discovery."""
        rule = PortfolioDriftRule()
        repo = _repo()

        rows = await rule.evaluate("bus.portfolio_drift_episode.extra", _event(), repo, _now())

        assert rows == []
        repo.list_active_scope_grants_for_wallet.assert_not_awaited()

    @pytest.mark.parametrize("payload", [b"not-json", b"\xff"])
    @pytest.mark.asyncio
    async def test_malformed_payload_is_ignored(self, payload: bytes) -> None:
        """Malformed JSON and invalid UTF-8 cannot escape rule evaluation."""
        rule = PortfolioDriftRule()
        repo = _repo()

        rows = await rule.evaluate(_TOPIC, payload, repo, _now())

        assert rows == []
        repo.list_active_scope_grants_for_wallet.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_drift_reconciliation_payload_is_ignored(self) -> None:
        """A normal non-drift bus payload never produces a page."""
        tick = TickData(
            session_id="session-tick",
            sequence_id=1,
            public_id="tick-1",
            timestamp=_now(),
            instrument="BTC-USD",
            volume=0.0,
            exchange="kraken",
        )
        rule = PortfolioDriftRule()
        repo = _repo()

        rows = await rule.evaluate(_TOPIC, tick.to_json().encode("utf-8"), repo, _now())

        assert rows == []
        repo.list_active_scope_grants_for_wallet.assert_not_awaited()
