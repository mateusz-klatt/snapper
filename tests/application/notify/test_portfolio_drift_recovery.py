"""Tests for durable open portfolio-drift page recovery."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.portfolio_drift_recovery import DEFAULT_INTERVAL_SECONDS
from snapper.application.notify.portfolio_drift_recovery import PortfolioDriftRecoveryScanner
from snapper.application.notify.portfolio_drift_recovery import _resolve_interval
from snapper.application.notify.rules.portfolio_drift import PortfolioDriftRule
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import PortfolioDriftEpisodeRow
from snapper.data.repository_types import ScopeGrantRow
from snapper.messaging.schemas.data import PortfolioDriftEpisodeEventData

_NOW = datetime(2026, 7, 15, 12, 0, tzinfo=UTC)
_EPISODE_PUBLIC_ID = "019dbb34-f439-77bd-afa8-ee5321d60307"
_EVENT_PUBLIC_ID = "019dbb34-f439-77bd-afa8-ee5321d60308"
_WALLET_PUBLIC_ID = "019dbb34-f439-77bd-afa8-ee5321d60309"


def _episode(
    *,
    public_id: str = _EPISODE_PUBLIC_ID,
    status: str = "open",
) -> PortfolioDriftEpisodeRow:
    """Build one scanner episode projection."""
    return PortfolioDriftEpisodeRow(
        wallet_public_id=_WALLET_PUBLIC_ID,
        exchange="kraken",
        mode="live",
        status=status,
        opened_at=_NOW,
        trigger_observation_id=3,
        last_observation_id=4,
        details_source_observation_id=4,
        latest_full_mismatch_count=4,
        public_id=public_id,
    )


def _grant() -> ScopeGrantRow:
    """Build the active owning scope grant used by page resolution."""
    return ScopeGrantRow(
        public_id="grant-1",
        operator_public_id="operator-1",
        wallet_public_id=_WALLET_PUBLIC_ID,
        granted_by_user_public_id="grantor-1",
        scope_kind="instrument",
        underlying_public_id=None,
        instrument_public_id="instrument-1",
        note=None,
        timestamp=_NOW,
        known_to=KNOWN_TO_MAX,
        session_id="scope-session",
        sequence_id=1,
    )


def _repo(
    episodes: list[PortfolioDriftEpisodeRow],
    *,
    dedup_hit: bool = False,
) -> MagicMock:
    """Build a repository mock for one recovery pass."""
    repo = MagicMock(spec=Repository)
    repo.list_open_portfolio_drift_episodes = AsyncMock(return_value=episodes)
    repo.list_active_scope_grants_for_wallet = AsyncMock(return_value=[_grant()])
    repo.list_users_with_operator_membership = AsyncMock(return_value=["user-1"])
    repo.list_alert_events_with_dedup_key = AsyncMock(
        return_value=[{"public_id": "bus-page"}] if dedup_hit else []
    )
    return repo


def _scanner(
    repo: MagicMock,
    emitted: list[tuple[AlertEventInsertRow, datetime]],
    *,
    interval_seconds: float = 30.0,
) -> PortfolioDriftRecoveryScanner:
    """Build a scanner whose sink captures emitted rows."""

    async def emit(row: AlertEventInsertRow, now: datetime) -> None:
        """Capture one row and its entry-boundary timestamp."""
        emitted.append((row, now))

    return PortfolioDriftRecoveryScanner(
        repo=cast(Repository, repo),
        emit_alert_row=emit,
        interval_seconds=interval_seconds,
    )


def _opened_event() -> bytes:
    """Serialize the Stage 1 event equivalent to :func:`_episode`."""
    event = PortfolioDriftEpisodeEventData(
        session_id="drift-session",
        sequence_id=1,
        public_id=_EVENT_PUBLIC_ID,
        timestamp=_NOW,
        wallet_public_id=_WALLET_PUBLIC_ID,
        exchange="kraken",
        mode="live",
        episode_public_id=_EPISODE_PUBLIC_ID,
        lifecycle="opened",
        opened_at=_NOW,
        closed_at=None,
        mismatch_count=3,
        resolution_reason=None,
    )
    return event.to_json().encode("utf-8")


async def _no_sleep(delay: float) -> None:
    """Replace loop sleeps with an immediate yield-free return."""


class _StopAfter:
    """Event stand-in that reports stopped after a call budget."""

    def __init__(self, false_calls: int) -> None:
        """Configure the number of leading false responses."""
        self._false_calls = false_calls
        self.calls = 0

    def is_set(self) -> bool:
        """Return false until the configured call budget is exhausted."""
        self.calls += 1
        return self.calls > self._false_calls

    def set(self) -> None:
        """Satisfy the event surface used by the scanner."""

    def clear(self) -> None:
        """Satisfy the event surface used by the scanner."""


class TestIntervalResolution:
    """Environment-driven cadence parsing."""

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, DEFAULT_INTERVAL_SECONDS),
            ("bad", DEFAULT_INTERVAL_SECONDS),
            ("0", DEFAULT_INTERVAL_SECONDS),
            ("-1", DEFAULT_INTERVAL_SECONDS),
            ("nan", DEFAULT_INTERVAL_SECONDS),
            ("inf", DEFAULT_INTERVAL_SECONDS),
            ("-inf", DEFAULT_INTERVAL_SECONDS),
            ("12.5", 12.5),
        ],
    )
    def test_resolve_interval(self, env_value: str | None, expected: float) -> None:
        """Unset and invalid values default while positive floats pass through."""
        assert _resolve_interval(env_value) == expected

    def test_constructor_reads_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An omitted constructor interval resolves from the owning env key."""
        monkeypatch.setenv("PORTFOLIO_DRIFT_RECOVERY_INTERVAL_SECONDS", "17")
        scanner = _scanner(_repo([]), [], interval_seconds=17.0)
        env_scanner = PortfolioDriftRecoveryScanner(
            repo=cast(Repository, _repo([])),
            emit_alert_row=scanner._emit_alert_row,
        )
        assert env_scanner.interval_seconds == 17.0

    @pytest.mark.parametrize("interval_seconds", [float("nan"), float("inf"), -1.0])
    def test_constructor_rejects_invalid_explicit_interval(
        self,
        interval_seconds: float,
    ) -> None:
        """Invalid direct overrides cannot escape into the sleep loop."""
        scanner = _scanner(_repo([]), [], interval_seconds=interval_seconds)

        assert scanner.interval_seconds == DEFAULT_INTERVAL_SECONDS


class TestRecoveryPass:
    """Open filtering, deduplication, parity, and per-episode isolation."""

    @pytest.mark.asyncio
    async def test_open_unpaged_episode_emits_one_page(self) -> None:
        """A current open episode without its bus page is recovered."""
        repo = _repo([_episode()])
        emitted: list[tuple[AlertEventInsertRow, datetime]] = []
        scanner = _scanner(repo, emitted)

        await scanner.run_once(_NOW + timedelta(minutes=1))

        assert len(emitted) == 1
        row, emitted_at = emitted[0]
        assert emitted_at == _NOW + timedelta(minutes=1)
        assert row["dedup_key"] == f"drift.{_EPISODE_PUBLIC_ID}"
        assert row["source_topic"] == "bus.portfolio_drift_episode"
        assert row["title"] == "Portfolio drift detected"
        assert row["is_safety_critical"] is True
        payload = row["payload"]
        assert payload is not None
        assert payload["mismatch_count"] == 3
        assert "after 3 consecutive full mismatches" in row["body"]

    @pytest.mark.asyncio
    async def test_bus_paged_episode_is_not_double_paged(self) -> None:
        """A persisted Stage 1 dedup key suppresses recovery fanout."""
        repo = _repo([_episode()], dedup_hit=True)
        emitted: list[tuple[AlertEventInsertRow, datetime]] = []
        scanner = _scanner(repo, emitted)

        await scanner.run_once(_NOW)

        assert emitted == []
        repo.list_alert_events_with_dedup_key.assert_awaited_once_with(
            user_public_id="user-1",
            dedup_key=f"drift.{_EPISODE_PUBLIC_ID}",
            since=_NOW,
        )

    @pytest.mark.asyncio
    async def test_defensive_closed_episode_is_skipped(self) -> None:
        """A closed row returned by a faulty repository cannot be paged."""
        repo = _repo([_episode(status="resolved")])
        emitted: list[tuple[AlertEventInsertRow, datetime]] = []
        scanner = _scanner(repo, emitted)

        await scanner.run_once(_NOW)

        assert emitted == []
        repo.list_active_scope_grants_for_wallet.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rule_and_scanner_emit_identical_open_rows(self) -> None:
        """Stage 1 and recovery remain exact adapters over one shared helper."""
        repo = _repo([_episode()])
        rule = PortfolioDriftRule()
        bus_rows = await rule.evaluate(
            "bus.portfolio_drift_episode",
            _opened_event(),
            cast(Repository, repo),
            _NOW,
        )
        emitted: list[tuple[AlertEventInsertRow, datetime]] = []
        scanner = _scanner(repo, emitted)

        await scanner.run_once(_NOW)

        assert [row for row, emitted_at in emitted if emitted_at == _NOW] == bus_rows

    @pytest.mark.asyncio
    async def test_episode_failure_does_not_block_later_episode(self) -> None:
        """One helper failure is isolated from the rest of the same pass."""
        second_id = "019dbb34-f439-77bd-afa8-ee5321d60310"
        repo = _repo([_episode(), _episode(public_id=second_id)])
        repo.list_active_scope_grants_for_wallet = AsyncMock(
            side_effect=[RuntimeError("owner lookup failed"), [_grant()]]
        )
        emitted: list[tuple[AlertEventInsertRow, datetime]] = []
        scanner = _scanner(repo, emitted)

        await scanner.run_once(_NOW)

        assert [row["dedup_key"] for row, emitted_at in emitted if emitted_at == _NOW] == [
            f"drift.{second_id}"
        ]

    @pytest.mark.asyncio
    async def test_recipient_failure_does_not_block_later_recipient(self) -> None:
        """One owner's fanout failure cannot postpone another owner page."""
        repo = _repo([_episode()])
        repo.list_users_with_operator_membership = AsyncMock(return_value=["user-1", "user-2"])
        attempted: list[str] = []

        async def emit(row: AlertEventInsertRow, now: datetime) -> None:
            """Fail the first user while capturing the second attempt."""
            attempted.append(row["user_public_id"])
            if row["user_public_id"] == "user-1":
                raise RuntimeError("sink failed")
            assert now == _NOW

        scanner = PortfolioDriftRecoveryScanner(
            repo=cast(Repository, repo),
            emit_alert_row=emit,
            interval_seconds=30.0,
        )

        await scanner.run_once(_NOW)

        assert attempted == ["user-1", "user-2"]


class TestLifecycle:
    """Eager execution, clean stop, and loop failure containment."""

    @pytest.mark.asyncio
    async def test_start_eager_scans_and_stop_cancels_loop(self) -> None:
        """Start scans immediately and stop clears the background task."""
        repo = _repo([])
        scanner = _scanner(repo, [])

        await scanner.start()
        await scanner.stop()

        repo.list_open_portfolio_drift_episodes.assert_awaited_once()
        assert scanner._loop_task is None

    @pytest.mark.asyncio
    async def test_stop_before_start_is_safe(self) -> None:
        """Stopping a never-started scanner tolerates the absent task."""
        scanner = _scanner(_repo([]), [])

        await scanner.stop()

        assert scanner._loop_task is None

    @pytest.mark.asyncio
    async def test_stop_during_eager_scan_prevents_late_loop_start(self) -> None:
        """A concurrent stop cannot be cleared after a blocked eager pass."""
        repo = _repo([])
        entered = asyncio.Event()
        release = asyncio.Event()

        async def blocked_query() -> list[PortfolioDriftEpisodeRow]:
            """Hold the eager query until stop has completed."""
            entered.set()
            await release.wait()
            return []

        repo.list_open_portfolio_drift_episodes = AsyncMock(side_effect=blocked_query)
        scanner = _scanner(repo, [])
        start_task = asyncio.create_task(scanner.start())
        await entered.wait()

        await scanner.stop()
        release.set()
        await start_task

        assert scanner._loop_task is None

    @pytest.mark.asyncio
    async def test_loop_survives_scan_pass_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A query failure is logged and the next scheduled pass still runs."""
        repo = _repo([])
        repo.list_open_portfolio_drift_episodes = AsyncMock(
            side_effect=[RuntimeError("database unavailable"), []]
        )
        scanner = _scanner(repo, [])
        monkeypatch.setattr(
            "snapper.application.notify.portfolio_drift_recovery.asyncio.sleep",
            _no_sleep,
        )
        scanner._stopping = cast(asyncio.Event, _StopAfter(4))

        await scanner._loop()

        assert repo.list_open_portfolio_drift_episodes.await_count == 2

    @pytest.mark.asyncio
    async def test_loop_skips_pass_when_stopped_during_sleep(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A stop landing during sleep prevents one final database scan."""
        repo = _repo([])
        scanner = _scanner(repo, [])
        monkeypatch.setattr(
            "snapper.application.notify.portfolio_drift_recovery.asyncio.sleep",
            _no_sleep,
        )
        scanner._stopping = cast(asyncio.Event, _StopAfter(1))

        await scanner._loop()

        repo.list_open_portfolio_drift_episodes.assert_not_awaited()
