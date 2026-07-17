"""Tests for the venue account-truth observer on the executor base (PnL Phase 3).

Covers the account-observer surface added to ``ExchangeExecutorService``:
serialization of native balances/positions, the paper/live mode derivation,
the fail-closed capability/outcome mapping of the balance and position reads,
the single-cycle snapshot assembly, and the supervised observer loop.
"""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import Literal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.core.types import ExchangeEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioDriftEpisodeTransitionRow
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import SpotAssetPrecisionEvidenceUpsertRow
from snapper.data.repository_types import VenueAccountAttemptRow
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry
from snapper.infrastructure.exchanges.contracts import OpenPositionSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.messaging.executors import base as base_module
from snapper.messaging.executors.base import _ACCOUNT_FRESHNESS_CEILING_S
from snapper.messaging.executors.base import _ACCOUNT_OBSERVE_INTERVAL_S
from snapper.messaging.executors.base import _ACCOUNT_UNEXPECTED_BALANCE_CAPABILITY_MSG
from snapper.messaging.executors.base import _ACCOUNT_UNEXPECTED_POSITION_CAPABILITY_MSG
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.schemas.data import AccountStateChangedEventData
from snapper.messaging.schemas.data import PortfolioDriftEpisodeEventData


class _DummyExecutor(ExchangeExecutorService[Any]):
    """Concrete executor stub exposing the observer methods under test."""

    def _create_exchange_client(self) -> Any:
        """Return an inert client; observer tests inject their own mock."""
        return MagicMock()

    def _get_exchange_name(self) -> str:
        """Return a non-paper venue name by default."""
        return "kraken"


def _make_executor() -> Any:
    """Build a minimal executor with mocked infrastructure for observer tests.

    Mirrors the reconciliation-suite construction: the real ``__init__``
    runs (so ``_tracker`` and ``_task_last_pass`` are genuine) under a
    patched ``get_settings``. The repository is a spec'd
    ``SQLAlchemyRepository`` mock so the ``isinstance`` durability gate in
    ``_observe_account_once`` passes, with the snapshot writer stubbed as an
    ``AsyncMock``.
    """
    with patch.object(
        base_module, "get_settings", return_value=MagicMock(db_url="sqlite:///:memory:")
    ):
        ex: Any = _DummyExecutor()
    ex.running = True
    ex.wallet_public_id = "wallet-1"
    ex.repository = MagicMock(spec=SQLAlchemyRepository)
    ex.repository.record_venue_account_snapshot = AsyncMock(return_value=1)
    ex.repository.upsert_spot_asset_precision_evidence = AsyncMock(return_value=1)
    ex.repository.get_portfolio_drift_episode_transition = AsyncMock(return_value=None)
    return ex


def _attach_account_event_publisher(executor: _DummyExecutor) -> MagicMock:
    """Attach an account-event publisher sharing the executor tracker."""
    publisher = MagicMock()
    publisher.tracker = executor._tracker
    publisher.send = AsyncMock()
    executor.msg_publisher = publisher
    return publisher


def _make_client(
    balance_capability: CapabilityStatus, position_capability: CapabilityStatus
) -> Any:
    """Build an async venue client with real capability enums set.

    ``AsyncMock`` makes every attribute an async child, so the two
    capability enums are assigned as concrete values; individual tests wire
    ``read_native_balances``/``read_native_positions`` as needed.
    """
    client: Any = AsyncMock()
    client.balance_capability = balance_capability
    client.position_capability = position_capability
    return client


def _portfolio_attempt(*, mode: str = "live", sequence_id: int = 7) -> VenueAccountAttemptRow:
    """Build one stable account attempt identity for orchestration tests."""
    now = datetime(2026, 7, 14, 10, 0, tzinfo=UTC)
    return {
        "wallet_public_id": "wallet-1",
        "exchange": "kraken_futures",
        "mode": mode,
        "balance_status": "observed",
        "position_status": "observed",
        "valuation_status": "native_only",
        "balances_json": "[]",
        "open_positions_json": "[]",
        "balance_observed_at": now,
        "position_observed_at": now,
        "authoritative_until": now + timedelta(minutes=5),
        "error": None,
        "session_id": "session-1",
        "sequence_id": sequence_id,
        "bus_time": now,
    }


def _portfolio_evaluation() -> PortfolioReconciliationEvaluationRow:
    """Build one stable committed evaluation for notification tests."""
    now = datetime(2026, 7, 15, 10, 0, tzinfo=UTC)
    return {
        "wallet_public_id": "wallet-1",
        "exchange": "kraken_futures",
        "mode": "live",
        "method": "futures_position",
        "evaluation_status": "mismatched",
        "venue_account_state_public_id": "account-state-1",
        "venue_account_observation_id": 10,
        "account_authoritative_until": now + timedelta(minutes=5),
        "source_watermark_kind": "execution_id",
        "source_watermark": 42,
        "anchor_public_id": None,
        "expected_json": "{}",
        "actual_json": "{}",
        "difference_json": "{}",
        "tolerance_json": "{}",
        "error": None,
        "session_id": "session-1",
        "sequence_id": 7,
        "bus_time": now,
    }


def _drift_transition(
    *,
    status: str = "open",
    mode: str = "live",
    closed_at: datetime | None = None,
    trigger_observation_id: int = 12,
    last_observation_id: int = 12,
    mismatch_count: int = 3,
    resolution_reason: str | None = None,
) -> PortfolioDriftEpisodeTransitionRow:
    """Build one post-commit drift lifecycle row."""
    return {
        "wallet_public_id": "wallet-1",
        "exchange": "kraken_futures",
        "mode": mode,
        "status": status,
        "opened_at": datetime(2026, 7, 15, 10, 0, tzinfo=UTC),
        "closed_at": closed_at,
        "trigger_observation_id": trigger_observation_id,
        "last_observation_id": last_observation_id,
        "latest_full_mismatch_count": mismatch_count,
        "resolution_reason": resolution_reason,
        "public_id": "episode-1",
        "session_id": "session-1",
        "sequence_id": 7,
    }


class TestSerializeNativeBalances:
    """Serialization of native balance entries to a stable JSON array."""

    def test_entries_serialize_with_nullable_free_used(self) -> None:
        """Entries render currency/total/free/used, preserving null free/used.

        Given: two entries — one with a full free/used split, one whose
            free/used are None (coin-margin venue),
        When: _serialize_native_balances runs,
        Then: the JSON array carries both entries with the null split kept
            as JSON null, never fabricated into a fake split.
        """
        entries = [
            NativeBalanceEntry(
                currency="USD",
                total=100.0,
                free=60.0,
                used=40.0,
                total_decimal="100.000000000000000005",
                free_decimal="60.0",
                used_decimal="40.0",
                numeric_provenance="venue_raw",
            ),
            NativeBalanceEntry(currency="XBT", total=1.5, free=None, used=None),
        ]
        payload = json.loads(ExchangeExecutorService._serialize_native_balances(entries))
        assert payload == [
            {
                "currency": "USD",
                "total": 100.0,
                "free": 60.0,
                "used": 40.0,
                "total_decimal": "100.000000000000000005",
                "free_decimal": "60.0",
                "used_decimal": "40.0",
                "numeric_provenance": "venue_raw",
            },
            {"currency": "XBT", "total": 1.5, "free": None, "used": None},
        ]

    def test_empty_entries_serialize_to_empty_array(self) -> None:
        """An empty balance list serializes to '[]', never null.

        Given: no balance entries,
        When: _serialize_native_balances runs,
        Then: the result is exactly '[]' so the repository stores an
            empty-but-present payload.
        """
        assert ExchangeExecutorService._serialize_native_balances([]) == "[]"


class TestSerializeOpenPositions:
    """Serialization of open positions to a stable JSON array."""

    def test_position_serializes_side_value_and_iso_timestamp(self) -> None:
        """A position renders side as its enum value and timestamp as ISO-8601.

        Given: one open position with side BUY and an aware timestamp,
        When: _serialize_open_positions runs,
        Then: the JSON object carries side='buy' (the enum .value) and the
            timestamp as its .isoformat() string, alongside every numeric
            field.
        """
        ts = datetime(2026, 7, 13, 12, 30, 15, tzinfo=UTC)
        positions = [
            OpenPositionSnapshot(
                symbol="BTC-USD",
                side=OrderSideEnum.BUY,
                size=1.0,
                entry_price=100.0,
                mark_price=110.0,
                unrealized_pnl=10.0,
                unrealized_funding=-0.5,
                timestamp=ts,
            )
        ]
        payload = json.loads(ExchangeExecutorService._serialize_open_positions(positions))
        assert payload == [
            {
                "symbol": "BTC-USD",
                "side": "buy",
                "size": 1.0,
                "entry_price": 100.0,
                "mark_price": 110.0,
                "unrealized_pnl": 10.0,
                "unrealized_funding": -0.5,
                "timestamp": ts.isoformat(),
            }
        ]

    def test_empty_positions_serialize_to_empty_array(self) -> None:
        """An empty position book serializes to '[]', never null.

        Given: no open positions,
        When: _serialize_open_positions runs,
        Then: the result is exactly '[]'.
        """
        assert ExchangeExecutorService._serialize_open_positions([]) == "[]"


class TestAccountMode:
    """The account-truth mode derived from the executor's venue."""

    def test_paper_exchange_maps_to_paper_mode(self) -> None:
        """A paper venue reports mode 'paper'.

        Given: an executor whose venue name is the paper exchange,
        When: _account_mode runs,
        Then: it returns 'paper'.
        """
        ex = _make_executor()
        ex._get_exchange_name = MagicMock(return_value=ExchangeEnum.PAPER)
        assert ex._account_mode() == "paper"

    def test_non_paper_exchange_maps_to_live_mode(self) -> None:
        """A non-paper venue reports mode 'live'.

        Given: an executor whose venue name is a live exchange,
        When: _account_mode runs,
        Then: it returns 'live'.
        """
        ex = _make_executor()
        ex._get_exchange_name = MagicMock(return_value="kraken")
        assert ex._account_mode() == "live"


class TestReadAccountBalances:
    """Capability/outcome mapping for the native balance read."""

    @pytest.mark.asyncio
    async def test_unsupported_capability_skips_reader(self) -> None:
        """An unsupported venue never calls the reader and reports 'unsupported'.

        Given: a client whose balance_capability is UNSUPPORTED,
        When: _read_account_balances runs,
        Then: it returns ('unsupported', None, None, None) without ever
            awaiting read_native_balances.
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.UNSUPPORTED, CapabilityStatus.UNSUPPORTED)
        client.read_native_balances = AsyncMock()
        result = await ex._read_account_balances(client, now)
        assert result == ("unsupported", None, None, None)
        client.read_native_balances.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_not_implemented_maps_to_unsupported(self) -> None:
        """A structural NotImplementedError degrades to 'unsupported'.

        Given: a supported-capability client whose reader raises
            NotImplementedError (structural, not a runtime fault),
        When: _read_account_balances runs,
        Then: it returns ('unsupported', None, None, None).
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.UNSUPPORTED)
        client.read_native_balances = AsyncMock(side_effect=NotImplementedError())
        result = await ex._read_account_balances(client, now)
        assert result == ("unsupported", None, None, None)

    @pytest.mark.asyncio
    async def test_generic_exception_maps_to_error_with_message(self) -> None:
        """A runtime read failure is 'error' with the message and null payload.

        Given: a supported-capability client whose reader raises a generic
            RuntimeError,
        When: _read_account_balances runs,
        Then: it returns status 'error', null payload/timestamp, and a
            non-empty error string carrying the failure text.
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.UNSUPPORTED)
        client.read_native_balances = AsyncMock(side_effect=RuntimeError("kaboom"))
        status, payload, observed_at, error = await ex._read_account_balances(client, now)
        assert status == "error"
        assert payload is None
        assert observed_at is None
        assert error is not None
        assert "kaboom" in error

    @pytest.mark.asyncio
    async def test_timeout_maps_to_error(self) -> None:
        """A timed-out read is recorded as 'error', last-good retained upstream.

        Given: a supported-capability client whose reader raises
            TimeoutError (the fetch bound firing),
        When: _read_account_balances runs,
        Then: it returns status 'error' with a non-null error string.
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.UNSUPPORTED)
        client.read_native_balances = AsyncMock(side_effect=TimeoutError("slow venue"))
        status, payload, observed_at, error = await ex._read_account_balances(client, now)
        assert status == "error"
        assert payload is None
        assert observed_at is None
        assert error is not None

    @pytest.mark.asyncio
    async def test_supported_success_is_observed(self) -> None:
        """A supported read yields 'observed' with serialized balances at ``now``.

        Given: a SUPPORTED client returning one balance entry,
        When: _read_account_balances runs,
        Then: it returns ('observed', <serialized json>, now, None).
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.UNSUPPORTED)
        client.read_native_balances = AsyncMock(
            return_value=[NativeBalanceEntry(currency="USD", total=100.0, free=60.0, used=40.0)]
        )
        status, payload, observed_at, error = await ex._read_account_balances(client, now)
        assert status == "observed"
        assert observed_at == now
        assert error is None
        assert payload is not None
        assert json.loads(payload) == [
            {"currency": "USD", "total": 100.0, "free": 60.0, "used": 40.0}
        ]

    @pytest.mark.asyncio
    async def test_simulated_success_is_simulated(self) -> None:
        """A simulated-capability read yields 'simulated', never 'observed'.

        Given: a SIMULATED (paper) client returning an empty balance list,
        When: _read_account_balances runs,
        Then: it returns status 'simulated' with the empty-array payload.
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.SIMULATED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(return_value=[])
        status, payload, observed_at, error = await ex._read_account_balances(client, now)
        assert status == "simulated"
        assert payload == "[]"
        assert observed_at == now
        assert error is None

    @pytest.mark.asyncio
    async def test_non_observable_capability_with_data_is_fail_closed_error(self) -> None:
        """A non-observable capability that still returned data is 'error'.

        Given: a client whose balance_capability is NOT_APPLICABLE (neither
            SUPPORTED nor SIMULATED) yet whose reader returns real entries,
        When: _read_account_balances runs,
        Then: it fail-closes to ('error', None, None, <fixed message>) rather
            than trusting the data as observed — a non-observable capability
            never yields an authoritative balance.
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.NOT_APPLICABLE, CapabilityStatus.UNSUPPORTED)
        client.read_native_balances = AsyncMock(
            return_value=[NativeBalanceEntry(currency="USD", total=1.0, free=1.0, used=0.0)]
        )
        result = await ex._read_account_balances(client, now)
        assert result == ("error", None, None, _ACCOUNT_UNEXPECTED_BALANCE_CAPABILITY_MSG)


class TestReadAccountPositions:
    """Capability/outcome mapping for the native position read."""

    @pytest.mark.asyncio
    async def test_not_applicable_skips_reader(self) -> None:
        """A venue without positions reports 'not_applicable' without a read.

        Given: a client whose position_capability is NOT_APPLICABLE,
        When: _read_account_positions runs,
        Then: it returns ('not_applicable', None, None, None) and never
            awaits read_native_positions.
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.UNSUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_positions = AsyncMock()
        result = await ex._read_account_positions(client, now)
        assert result == ("not_applicable", None, None, None)
        client.read_native_positions.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unsupported_capability_skips_reader(self) -> None:
        """An unsupported position component reports 'unsupported' without a read.

        Given: a client whose position_capability is UNSUPPORTED,
        When: _read_account_positions runs,
        Then: it returns ('unsupported', None, None, None) and never awaits
            the reader.
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.UNSUPPORTED, CapabilityStatus.UNSUPPORTED)
        client.read_native_positions = AsyncMock()
        result = await ex._read_account_positions(client, now)
        assert result == ("unsupported", None, None, None)
        client.read_native_positions.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_not_implemented_maps_to_unsupported(self) -> None:
        """A structural NotImplementedError degrades positions to 'unsupported'.

        Given: a SUPPORTED position client whose reader raises
            NotImplementedError,
        When: _read_account_positions runs,
        Then: it returns ('unsupported', None, None, None).
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.UNSUPPORTED, CapabilityStatus.SUPPORTED)
        client.read_native_positions = AsyncMock(side_effect=NotImplementedError())
        result = await ex._read_account_positions(client, now)
        assert result == ("unsupported", None, None, None)

    @pytest.mark.asyncio
    async def test_generic_exception_maps_to_error_with_message(self) -> None:
        """A runtime position-read failure is 'error' with the message.

        Given: a SUPPORTED position client whose reader raises RuntimeError,
        When: _read_account_positions runs,
        Then: it returns status 'error', null payload/timestamp, and a
            non-empty error string.
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        client = _make_client(CapabilityStatus.UNSUPPORTED, CapabilityStatus.SUPPORTED)
        client.read_native_positions = AsyncMock(side_effect=RuntimeError("pos boom"))
        status, payload, observed_at, error = await ex._read_account_positions(client, now)
        assert status == "error"
        assert payload is None
        assert observed_at is None
        assert error is not None
        assert "pos boom" in error

    @pytest.mark.asyncio
    async def test_supported_success_is_observed(self) -> None:
        """A supported read yields 'observed' with serialized positions at ``now``.

        Given: a SUPPORTED position client returning one open position,
        When: _read_account_positions runs,
        Then: it returns ('observed', <serialized json>, now, None) with the
            side rendered as its enum value.
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        ts = datetime(2026, 7, 13, 9, 0, 0, tzinfo=UTC)
        client = _make_client(CapabilityStatus.UNSUPPORTED, CapabilityStatus.SUPPORTED)
        client.read_native_positions = AsyncMock(
            return_value=[
                OpenPositionSnapshot(
                    symbol="BTC-USD",
                    side=OrderSideEnum.SELL,
                    size=2.0,
                    entry_price=100.0,
                    mark_price=95.0,
                    unrealized_pnl=10.0,
                    unrealized_funding=0.25,
                    timestamp=ts,
                )
            ]
        )
        status, payload, observed_at, error = await ex._read_account_positions(client, now)
        assert status == "observed"
        assert observed_at == now
        assert error is None
        assert payload is not None
        parsed = json.loads(payload)
        assert parsed[0]["side"] == "sell"
        assert parsed[0]["timestamp"] == ts.isoformat()

    @pytest.mark.asyncio
    async def test_non_observable_capability_with_data_is_fail_closed_error(self) -> None:
        """A non-SUPPORTED capability that still returned positions is 'error'.

        Given: a client whose position_capability is SIMULATED (it reaches
            the read but is not SUPPORTED) whose reader returns a position,
        When: _read_account_positions runs,
        Then: it fail-closes to ('error', None, None, <fixed message>) rather
            than trusting the data as observed.
        """
        ex = _make_executor()
        now = datetime.now(UTC)
        ts = datetime(2026, 7, 13, 9, 0, 0, tzinfo=UTC)
        client = _make_client(CapabilityStatus.UNSUPPORTED, CapabilityStatus.SIMULATED)
        client.read_native_positions = AsyncMock(
            return_value=[
                OpenPositionSnapshot(
                    symbol="BTC-USD",
                    side=OrderSideEnum.BUY,
                    size=1.0,
                    entry_price=100.0,
                    mark_price=110.0,
                    unrealized_pnl=10.0,
                    unrealized_funding=0.0,
                    timestamp=ts,
                )
            ]
        )
        result = await ex._read_account_positions(client, now)
        assert result == ("error", None, None, _ACCOUNT_UNEXPECTED_POSITION_CAPABILITY_MSG)


class TestObserveAccountOnce:
    """Single-cycle assembly and persistence of the account snapshot."""

    @pytest.mark.asyncio
    async def test_no_exchange_client_records_nothing(self) -> None:
        """A missing exchange client persists no snapshot.

        Given: an executor with exchange_client=None,
        When: _observe_account_once runs,
        Then: record_venue_account_snapshot is never awaited.
        """
        ex = _make_executor()
        ex.exchange_client = None
        await ex._observe_account_once()
        ex.repository.record_venue_account_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_durable_repository_records_nothing(self) -> None:
        """A non-SQL repository disables persistence.

        Given: an executor whose repository is a plain (non-SQLAlchemy)
            mock while a client is present,
        When: _observe_account_once runs,
        Then: the isinstance gate short-circuits and no snapshot is written.
        """
        ex = _make_executor()
        ex.exchange_client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        ex.repository = MagicMock()
        ex.repository.record_venue_account_snapshot = AsyncMock()
        await ex._observe_account_once()
        ex.repository.record_venue_account_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_happy_path_records_observed_snapshot(self) -> None:
        """A supported balance+position read records an authoritative snapshot.

        Given: a SUPPORTED balance and SUPPORTED position client,
        When: _observe_account_once runs,
        Then: exactly one snapshot is written with balance_status and
            position_status 'observed', valuation_status 'native_only',
            mode 'live', and an authoritative_until equal to
            balance_observed_at + the freshness ceiling; error is None.
        """
        ex = _make_executor()
        ts = datetime(2026, 7, 13, 8, 0, 0, tzinfo=UTC)
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.read_native_balances = AsyncMock(
            return_value=[NativeBalanceEntry(currency="USD", total=100.0, free=60.0, used=40.0)]
        )
        client.read_native_positions = AsyncMock(
            return_value=[
                OpenPositionSnapshot(
                    symbol="BTC-USD",
                    side=OrderSideEnum.BUY,
                    size=1.0,
                    entry_price=100.0,
                    mark_price=110.0,
                    unrealized_pnl=10.0,
                    unrealized_funding=-0.5,
                    timestamp=ts,
                )
            ]
        )
        ex.exchange_client = client
        await ex._observe_account_once()
        ex.repository.record_venue_account_snapshot.assert_awaited_once()
        attempt = ex.repository.record_venue_account_snapshot.await_args.args[0]
        assert attempt["balance_status"] == "observed"
        assert attempt["position_status"] == "observed"
        assert attempt["valuation_status"] == "native_only"
        assert attempt["mode"] == "live"
        assert attempt["wallet_public_id"] == "wallet-1"
        assert attempt["exchange"] == "kraken"
        assert attempt["error"] is None
        assert attempt["balance_observed_at"] is not None
        assert attempt["authoritative_until"] is not None
        assert attempt["authoritative_until"] == attempt["balance_observed_at"] + timedelta(
            seconds=_ACCOUNT_FRESHNESS_CEILING_S
        )

    @pytest.mark.asyncio
    async def test_snapshot_commit_schedules_reconciliation_before_invalidation(self) -> None:
        """A snapshot schedules required healing before detached invalidation.

        Given: a successful venue observation with an attached bus publisher,
        When: the observer persists the snapshot,
        Then: it schedules reconciliation from that committed state id before
            the owned wallet-scoped thin snapshot frame can run.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(return_value=[])
        ex.exchange_client = client
        timeline: list[str] = []

        async def record_snapshot(attempt: VenueAccountAttemptRow) -> int:
            assert attempt["wallet_public_id"] == "wallet-1"
            timeline.append("commit")
            return 41

        async def send_event(
            topic: str,
            event: AccountStateChangedEventData,
            *,
            flags: int,
        ) -> None:
            assert topic == "portfolio.accounts.wallet-1"
            assert event.kind == "snapshot"
            timeline.append("publish")

        def schedule_reconciliation(
            *,
            state_id: int,
            attempt: VenueAccountAttemptRow,
            position_capability: CapabilityStatus,
        ) -> None:
            assert state_id == 41
            assert attempt["wallet_public_id"] == "wallet-1"
            assert position_capability is CapabilityStatus.NOT_APPLICABLE
            timeline.append("schedule")

        ex.repository.record_venue_account_snapshot = AsyncMock(side_effect=record_snapshot)
        publisher = _attach_account_event_publisher(ex)
        publisher.send = AsyncMock(side_effect=send_event)
        ex._schedule_portfolio_reconciliation = MagicMock(side_effect=schedule_reconciliation)

        await ex._observe_account_once()

        assert timeline == ["commit", "schedule"]
        task = tuple(ex._account_state_invalidation_tasks)[0]
        await task
        await asyncio.sleep(0)
        assert timeline == ["commit", "schedule", "publish"]
        assert ex._account_state_invalidation_tasks == set()
        send_call = publisher.send.await_args
        assert send_call is not None
        event = send_call.args[1]
        assert isinstance(event, AccountStateChangedEventData)
        assert event.wallet_public_id == "wallet-1"
        assert event.exchange == "kraken"
        assert event.mode == "live"
        assert event.session_id == ex._tracker.session_id
        assert event.sequence_id == 1
        assert send_call.kwargs == {"flags": base_module.zmq.NOBLOCK}

    @pytest.mark.asyncio
    async def test_blocked_snapshot_invalidation_preserves_reconciliation_work(self) -> None:
        """A blocked snapshot send cannot delay required reconciliation scheduling.

        Given: a committed venue snapshot whose invalidation send never returns,
        When: the single observation completes and shutdown drains owned work,
        Then: reconciliation is already scheduled and the blocked send is cancelled.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(return_value=[])
        ex.exchange_client = client
        ex.repository.record_venue_account_snapshot = AsyncMock(return_value=41)
        ex._schedule_portfolio_reconciliation = MagicMock()
        publisher = _attach_account_event_publisher(ex)
        send_started = asyncio.Event()
        send_cancelled = asyncio.Event()

        async def blocked_send(
            topic: str,
            event: AccountStateChangedEventData,
            *,
            flags: int,
        ) -> None:
            assert topic == "portfolio.accounts.wallet-1"
            assert event.kind == "snapshot"
            assert flags == base_module.zmq.NOBLOCK
            send_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                send_cancelled.set()

        publisher.send = AsyncMock(side_effect=blocked_send)

        await asyncio.wait_for(ex._observe_account_once(), timeout=0.1)
        await asyncio.wait_for(send_started.wait(), timeout=0.1)

        ex.repository.record_venue_account_snapshot.assert_awaited_once()
        schedule_call = ex._schedule_portfolio_reconciliation.call_args
        assert schedule_call is not None
        assert schedule_call.kwargs["state_id"] == 41
        assert schedule_call.kwargs["position_capability"] is CapabilityStatus.NOT_APPLICABLE
        task = tuple(ex._account_state_invalidation_tasks)[0]
        assert task.done() is False
        assert ex._portfolio_reconciliation_failure_count == 0

        await ex._close_and_drain_portfolio_reconciliation()

        assert task.cancelled()
        assert send_cancelled.is_set()
        assert ex._account_state_invalidation_tasks == set()

    @pytest.mark.asyncio
    async def test_cancelled_snapshot_send_preserves_commit_and_reconciliation(self) -> None:
        """Publisher cancellation is isolated from a committed snapshot caller.

        Given: a committed venue snapshot whose owned invalidation send is cancelled,
        When: the publisher surfaces cancellation after required work is scheduled,
        Then: the caller returns and the reconciliation schedule remains intact.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(return_value=[])
        ex.exchange_client = client
        ex.repository.record_venue_account_snapshot = AsyncMock(return_value=41)
        ex._schedule_portfolio_reconciliation = MagicMock()
        publisher = _attach_account_event_publisher(ex)
        send_started = asyncio.Event()
        cancel_send = asyncio.Event()

        async def cancelled_send(
            topic: str,
            event: AccountStateChangedEventData,
            *,
            flags: int,
        ) -> None:
            assert topic == "portfolio.accounts.wallet-1"
            assert event.kind == "snapshot"
            assert flags == base_module.zmq.NOBLOCK
            send_started.set()
            await cancel_send.wait()
            raise asyncio.CancelledError

        publisher.send = AsyncMock(side_effect=cancelled_send)

        await ex._observe_account_once()
        task = tuple(ex._account_state_invalidation_tasks)[0]
        await asyncio.wait_for(send_started.wait(), timeout=0.1)

        ex.repository.record_venue_account_snapshot.assert_awaited_once()
        ex._schedule_portfolio_reconciliation.assert_called_once()
        cancel_send.set()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)

        assert task.cancelled()
        assert ex._account_state_invalidation_tasks == set()
        assert ex._portfolio_reconciliation_failure_count == 0

    @pytest.mark.asyncio
    async def test_balance_error_records_null_authority(self) -> None:
        """A failed balance read persists an error row with no authority window.

        Given: a SUPPORTED client whose balance read raises,
        When: _observe_account_once runs,
        Then: the snapshot carries balance_status 'error', a non-null
            error, and a null authoritative_until / balance_observed_at /
            balances_json so the last-good balance stays stale-visible.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.read_native_balances = AsyncMock(side_effect=RuntimeError("venue down"))
        client.read_native_positions = AsyncMock(return_value=[])
        ex.exchange_client = client
        await ex._observe_account_once()
        attempt = ex.repository.record_venue_account_snapshot.await_args.args[0]
        assert attempt["balance_status"] == "error"
        assert attempt["error"] is not None
        assert attempt["authoritative_until"] is None
        assert attempt["balance_observed_at"] is None
        assert attempt["balances_json"] is None

    @pytest.mark.asyncio
    async def test_paper_path_records_simulated_paper_snapshot(self) -> None:
        """A paper venue records a simulated balance under mode 'paper'.

        Given: a paper-named executor with a SIMULATED balance client,
        When: _observe_account_once runs,
        Then: the snapshot carries mode 'paper', balance_status 'simulated',
            and a non-null authoritative_until (simulated is authoritative).
        """
        ex = _make_executor()
        ex._get_exchange_name = MagicMock(return_value=ExchangeEnum.PAPER)
        client = _make_client(CapabilityStatus.SIMULATED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(return_value=[])
        ex.exchange_client = client
        await ex._observe_account_once()
        attempt = ex.repository.record_venue_account_snapshot.await_args.args[0]
        assert attempt["mode"] == "paper"
        assert attempt["balance_status"] == "simulated"
        assert attempt["authoritative_until"] is not None

    @pytest.mark.asyncio
    async def test_not_applicable_positions_clears_component(self) -> None:
        """A positionless venue records position_status 'not_applicable'.

        Given: a SUPPORTED balance client whose position_capability is
            NOT_APPLICABLE,
        When: _observe_account_once runs,
        Then: the snapshot carries position_status 'not_applicable' with a
            null open_positions_json and the reader is never awaited.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(return_value=[])
        client.read_native_positions = AsyncMock()
        ex.exchange_client = client
        await ex._observe_account_once()
        attempt = ex.repository.record_venue_account_snapshot.await_args.args[0]
        assert attempt["position_status"] == "not_applicable"
        assert attempt["open_positions_json"] is None
        client.read_native_positions.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_walutomat_raw_balance_precision_uses_retained_observation(self) -> None:
        """Only complete authenticated raw triplets become balance evidence.

        Given: A committed Walutomat balance payload containing one venue-raw
            triplet, one legacy-float triplet, and one incomplete raw triplet.
        When: The account observer completes its snapshot cycle.
        Then: The complete and incomplete raw assets are persisted with the
            balance observation time while the independent fee plane is untouched.
        """
        ex = _make_executor()
        ex._get_exchange_name = MagicMock(return_value=ExchangeEnum.WALUTOMAT)
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(
            return_value=[
                NativeBalanceEntry(
                    currency="EUR",
                    total=100.25,
                    free=80.0,
                    used=20.25,
                    total_decimal="100.25",
                    free_decimal="80.00",
                    used_decimal="20.25",
                    numeric_provenance="venue_raw",
                ),
                NativeBalanceEntry(
                    currency="PLN",
                    total=10.0,
                    free=8.0,
                    used=2.0,
                    total_decimal="10.00",
                    free_decimal="8.00",
                    used_decimal="2.00",
                ),
                NativeBalanceEntry(
                    currency="USD",
                    total=5.0,
                    free=4.0,
                    used=1.0,
                    total_decimal="5.00",
                    free_decimal=None,
                    used_decimal="1.00",
                    numeric_provenance="venue_raw",
                ),
            ]
        )
        ex.exchange_client = client
        persisted: list[SpotAssetPrecisionEvidenceUpsertRow] = []
        completed = asyncio.Event()

        async def upsert(row: SpotAssetPrecisionEvidenceUpsertRow) -> int:
            persisted.append(row)
            if len(persisted) == 2:
                completed.set()
            return 9

        ex.repository.upsert_spot_asset_precision_evidence = AsyncMock(side_effect=upsert)
        observation_time = datetime(2026, 7, 16, 10, 0, tzinfo=UTC)
        persistence_time = observation_time + timedelta(seconds=2)

        with patch.object(base_module, "datetime") as datetime_type:
            datetime_type.now.side_effect = [observation_time, persistence_time]
            await ex._observe_account_once()
            await asyncio.wait_for(completed.wait(), timeout=0.1)
            await asyncio.sleep(0)

        attempt = ex.repository.record_venue_account_snapshot.await_args.args[0]
        assert len(persisted) == 2
        rows = {row["asset"]: row for row in persisted}
        row = rows["EUR"]
        assert row["exchange"] == ExchangeEnum.WALUTOMAT.value
        assert row["asset"] == "EUR"
        assert row["balance_decimals"] == 2
        balance_source = row["balance_source"]
        balance_version = row["balance_version"]
        assert balance_source is not None
        assert balance_version is not None
        assert "account/balances" in balance_source
        assert balance_version.startswith("spot-asset-precision-v1:")
        assert attempt["balance_observed_at"] == observation_time
        assert row["balance_observed_at"] == observation_time
        assert row["timestamp"] == persistence_time
        assert row["session_id"] == attempt["session_id"]
        assert row["sequence_id"] == attempt["sequence_id"]
        assert row["fee_decimals"] is None
        assert row["fee_source"] is None
        assert row["fee_version"] is None
        assert row["fee_observed_at"] is None
        assert rows["USD"]["balance_decimals"] is None
        assert rows["USD"]["balance_observed_at"] == attempt["balance_observed_at"]
        assert ex._spot_precision_evidence_tasks == set()

    @pytest.mark.asyncio
    async def test_walutomat_incomplete_duplicate_prevents_balance_certification(self) -> None:
        """An incomplete duplicate cannot be filtered out before derivation.

        Given: Complete and incomplete venue-raw rows for the same Walutomat asset.
        When: The committed payload is converted into balance precision evidence.
        Then: The asset is persisted as uncertified because every duplicate must agree.
        """
        ex = _make_executor()
        ex._get_exchange_name = MagicMock(return_value=ExchangeEnum.WALUTOMAT)
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(
            return_value=[
                NativeBalanceEntry(
                    currency="EUR",
                    total=1.0,
                    free=1.0,
                    used=0.0,
                    total_decimal="1.00",
                    free_decimal="1.00",
                    used_decimal="0.00",
                    numeric_provenance="venue_raw",
                ),
                NativeBalanceEntry(
                    currency="EUR",
                    total=2.0,
                    free=2.0,
                    used=0.0,
                    total_decimal="2.00",
                    free_decimal=None,
                    used_decimal="0.00",
                    numeric_provenance="venue_raw",
                ),
            ]
        )
        ex.exchange_client = client
        completed = asyncio.Event()
        persisted: list[SpotAssetPrecisionEvidenceUpsertRow] = []

        async def upsert(row: SpotAssetPrecisionEvidenceUpsertRow) -> int:
            persisted.append(row)
            completed.set()
            return 10

        ex.repository.upsert_spot_asset_precision_evidence = AsyncMock(side_effect=upsert)

        await ex._observe_account_once()
        await asyncio.wait_for(completed.wait(), timeout=0.1)
        await asyncio.sleep(0)

        assert len(persisted) == 1
        assert persisted[0]["asset"] == "EUR"
        assert persisted[0]["balance_decimals"] is None
        assert persisted[0]["balance_source"] is not None
        assert persisted[0]["balance_observed_at"] is not None
        assert ex._spot_precision_evidence_tasks == set()

    @pytest.mark.asyncio
    async def test_walutomat_precision_shutdown_drains_latest_then_cancels_worker(self) -> None:
        """Shutdown attempts the retained latest observation before cancellation.

        Given: Two committed Walutomat snapshots whose first precision upsert blocks.
        When: Shutdown drains the owned evidence work.
        Then: Required followups are already scheduled, the pending latest
            observation is persisted, and only then is the worker cancelled.
        """
        ex = _make_executor()
        ex._get_exchange_name = MagicMock(return_value=ExchangeEnum.WALUTOMAT)
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(
            return_value=[
                NativeBalanceEntry(
                    currency="EUR",
                    total=1.0,
                    free=1.0,
                    used=0.0,
                    total_decimal="1.00",
                    free_decimal="1.00",
                    used_decimal="0.00",
                    numeric_provenance="venue_raw",
                )
            ]
        )
        ex.exchange_client = client
        ex._schedule_portfolio_reconciliation = MagicMock()
        ex._schedule_account_state_changed = MagicMock()
        started = asyncio.Event()
        cancelled = asyncio.Event()
        persistence_order: list[str] = []
        upsert_count = 0

        async def blocked_upsert(_row: SpotAssetPrecisionEvidenceUpsertRow) -> int:
            nonlocal upsert_count
            upsert_count += 1
            if upsert_count == 2:
                persistence_order.append("pending persisted")
                return 2
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                persistence_order.append("worker cancelled")
                cancelled.set()
            return 1

        ex.repository.upsert_spot_asset_precision_evidence = AsyncMock(side_effect=blocked_upsert)

        await asyncio.wait_for(ex._observe_account_once(), timeout=0.1)
        await asyncio.wait_for(started.wait(), timeout=0.1)
        await asyncio.wait_for(ex._observe_account_once(), timeout=0.1)

        assert ex._schedule_portfolio_reconciliation.call_count == 2
        assert ex._schedule_account_state_changed.call_count == 2
        assert len(ex._spot_precision_evidence_tasks) == 1
        assert ExchangeEnum.WALUTOMAT.value in ex._spot_precision_evidence_pending_latest

        with patch.object(base_module, "_SPOT_PRECISION_EVIDENCE_TIMEOUT_S", 0.01):
            await ex._close_and_drain_portfolio_reconciliation()

        assert cancelled.is_set()
        assert persistence_order == ["pending persisted", "worker cancelled"]
        assert ex.repository.upsert_spot_asset_precision_evidence.await_count == 2
        assert ex._spot_precision_evidence_tasks == set()
        assert ex._spot_precision_evidence_active_tasks == {}
        assert ex._spot_precision_evidence_in_flight == {}
        assert ex._spot_precision_evidence_pending_latest == {}

    @pytest.mark.asyncio
    async def test_walutomat_precision_shutdown_awaits_claimed_latest_in_flight(self) -> None:
        """Shutdown lets a claimed latest observation finish persistence.

        Given: A latest pending observation claimed by the exchange worker while
            its durable persistence is blocked.
        When: Shutdown begins after the pending-to-in-flight handoff.
        Then: Shutdown awaits that persistence within its grace bound instead of
            cancelling it after finding the pending slot empty.
        """
        ex = _make_executor()
        attempts = [_portfolio_attempt(sequence_id=sequence_id) for sequence_id in (1, 2)]
        for attempt in attempts:
            attempt["exchange"] = ExchangeEnum.WALUTOMAT.value
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        latest_started = asyncio.Event()
        release_latest = asyncio.Event()
        latest_cancelled = asyncio.Event()
        completed_sequences: list[int] = []

        async def persist(
            _repository: SQLAlchemyRepository,
            attempt: VenueAccountAttemptRow,
        ) -> None:
            if attempt["sequence_id"] == 1:
                first_started.set()
                await release_first.wait()
            else:
                latest_started.set()
                try:
                    await release_latest.wait()
                except asyncio.CancelledError:
                    latest_cancelled.set()
                    raise
            completed_sequences.append(attempt["sequence_id"])

        ex._persist_walutomat_balance_precision = AsyncMock(side_effect=persist)

        ex._schedule_walutomat_balance_precision(ex.repository, attempts[0])
        await asyncio.wait_for(first_started.wait(), timeout=0.1)
        ex._schedule_walutomat_balance_precision(ex.repository, attempts[1])
        release_first.set()
        await asyncio.wait_for(latest_started.wait(), timeout=0.1)

        exchange = ExchangeEnum.WALUTOMAT.value
        assert ex._spot_precision_evidence_pending_latest == {}
        assert ex._spot_precision_evidence_in_flight[exchange][1] is attempts[1]

        shutdown = asyncio.create_task(ex._close_and_drain_portfolio_reconciliation())
        await asyncio.sleep(0)

        assert not shutdown.done()
        assert not latest_cancelled.is_set()

        release_latest.set()
        await asyncio.wait_for(shutdown, timeout=0.1)

        assert completed_sequences == [1, 2]
        assert not latest_cancelled.is_set()
        assert ex._spot_precision_evidence_tasks == set()
        assert ex._spot_precision_evidence_active_tasks == {}
        assert ex._spot_precision_evidence_in_flight == {}
        assert ex._spot_precision_evidence_pending_latest == {}

    @pytest.mark.asyncio
    async def test_walutomat_precision_write_times_out_and_releases_ownership(self) -> None:
        """A stuck detached persistence attempt is cancelled at its hard deadline.

        Given: One precision worker blocked inside durable persistence.
        When: Its short evidence deadline expires.
        Then: The write is cancelled and every ownership slot is released.
        """
        ex = _make_executor()
        attempt = _portfolio_attempt(sequence_id=11)
        attempt["exchange"] = ExchangeEnum.WALUTOMAT.value
        attempt["balances_json"] = ex._serialize_native_balances(
            [
                NativeBalanceEntry(
                    currency="EUR",
                    total=1.0,
                    free=1.0,
                    used=0.0,
                    total_decimal="1.00",
                    free_decimal="1.00",
                    used_decimal="0.00",
                    numeric_provenance="venue_raw",
                )
            ]
        )
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def blocked_upsert(_row: SpotAssetPrecisionEvidenceUpsertRow) -> int:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
            return 1

        ex.repository.upsert_spot_asset_precision_evidence = AsyncMock(side_effect=blocked_upsert)

        with (
            patch.object(base_module, "_SPOT_PRECISION_EVIDENCE_TIMEOUT_S", 0.01),
            patch.object(base_module.logger, "warning") as warning,
        ):
            ex._schedule_walutomat_balance_precision(ex.repository, attempt)
            task = tuple(ex._spot_precision_evidence_tasks)[0]
            await asyncio.wait_for(started.wait(), timeout=0.1)
            await asyncio.wait_for(task, timeout=0.1)
            await asyncio.sleep(0)

        assert cancelled.is_set()
        assert ex._spot_precision_evidence_tasks == set()
        assert ex._spot_precision_evidence_active_tasks == {}
        assert ex._spot_precision_evidence_pending_latest == {}
        ex.repository.upsert_spot_asset_precision_evidence.assert_awaited_once()
        warning.assert_called_once()
        assert "timed out" in warning.call_args.args[0]

    @pytest.mark.asyncio
    async def test_walutomat_precision_single_flight_keeps_only_latest_pending(self) -> None:
        """Three rapid observations run the first and latest with one pending slot.

        Given: One running exchange worker followed by two newer observations.
        When: The running persistence fails after both observations arrive.
        Then: Only the latest pending content runs and the failure stays contained.
        """
        ex = _make_executor()
        attempts = [_portfolio_attempt(sequence_id=sequence_id) for sequence_id in (1, 2, 3)]
        for attempt in attempts:
            attempt["exchange"] = ExchangeEnum.WALUTOMAT.value
        started = asyncio.Event()
        release = asyncio.Event()
        persisted_sequences: list[int] = []

        async def persist(
            _repository: SQLAlchemyRepository,
            attempt: VenueAccountAttemptRow,
        ) -> None:
            persisted_sequences.append(attempt["sequence_id"])
            if len(persisted_sequences) == 1:
                started.set()
                await release.wait()
                raise RuntimeError("first persistence failed")

        ex._persist_walutomat_balance_precision = AsyncMock(side_effect=persist)

        with patch.object(base_module.logger, "warning") as warning:
            ex._schedule_walutomat_balance_precision(ex.repository, attempts[0])
            task = tuple(ex._spot_precision_evidence_tasks)[0]
            await asyncio.wait_for(started.wait(), timeout=0.1)
            ex._schedule_walutomat_balance_precision(ex.repository, attempts[1])
            ex._schedule_walutomat_balance_precision(ex.repository, attempts[2])

            assert len(ex._spot_precision_evidence_tasks) == 1
            pending = ex._spot_precision_evidence_pending_latest[ExchangeEnum.WALUTOMAT.value]
            assert pending[1] is attempts[2]

            release.set()
            await asyncio.wait_for(task, timeout=0.1)
            await asyncio.sleep(0)

        assert persisted_sequences == [1, 3]
        assert ex._persist_walutomat_balance_precision.await_count == 2
        assert ex._spot_precision_evidence_tasks == set()
        assert ex._spot_precision_evidence_active_tasks == {}
        assert ex._spot_precision_evidence_pending_latest == {}
        warning.assert_called_once()
        assert "worker failed" in warning.call_args.args[0]

    @pytest.mark.asyncio
    async def test_walutomat_precision_failure_is_contained_per_asset(self) -> None:
        """One failed evidence row does not suppress another observed asset.

        Given: Two valid Walutomat raw balance triplets whose first upsert fails.
        When: Detached precision persistence processes the committed attempt.
        Then: The second asset is still attempted and the failure stays outside
            the snapshot observer's required work.
        """
        ex = _make_executor()
        ex._get_exchange_name = MagicMock(return_value=ExchangeEnum.WALUTOMAT)
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(
            return_value=[
                NativeBalanceEntry(
                    currency=asset,
                    total=1.0,
                    free=1.0,
                    used=0.0,
                    total_decimal="1.00",
                    free_decimal="1.00",
                    used_decimal="0.00",
                    numeric_provenance="venue_raw",
                )
                for asset in ("EUR", "USD")
            ]
        )
        ex.exchange_client = client
        attempted_assets: list[str] = []
        completed = asyncio.Event()

        async def upsert(row: SpotAssetPrecisionEvidenceUpsertRow) -> int:
            attempted_assets.append(row["asset"])
            if row["asset"] == "EUR":
                raise RuntimeError("precision unavailable")
            completed.set()
            return 2

        ex.repository.upsert_spot_asset_precision_evidence = AsyncMock(side_effect=upsert)

        with patch.object(base_module.logger, "warning") as warning:
            await ex._observe_account_once()
            await asyncio.wait_for(completed.wait(), timeout=0.1)
            await asyncio.sleep(0)

        assert attempted_assets == ["EUR", "USD"]
        warning.assert_called_once()
        ex.repository.record_venue_account_snapshot.assert_awaited_once()
        assert ex._spot_precision_evidence_tasks == set()

    @pytest.mark.asyncio
    async def test_walutomat_precision_rejects_foreign_missing_or_malformed_payload(
        self,
    ) -> None:
        """Foreign, missing, and malformed retained payloads fail closed.

        Given: One foreign attempt, one without balances, and one with malformed JSON.
        When: Best-effort precision persistence is invoked directly.
        Then: None writes evidence and only the malformed payload is logged.
        """
        ex = _make_executor()
        foreign = _portfolio_attempt()
        await ex._persist_walutomat_balance_precision(ex.repository, foreign)

        missing = _portfolio_attempt()
        missing["exchange"] = ExchangeEnum.WALUTOMAT.value
        missing["balances_json"] = None
        await ex._persist_walutomat_balance_precision(ex.repository, missing)

        malformed = _portfolio_attempt()
        malformed["exchange"] = ExchangeEnum.WALUTOMAT.value
        malformed["balances_json"] = "{"
        with patch.object(base_module.logger, "warning") as warning:
            await ex._persist_walutomat_balance_precision(ex.repository, malformed)

        ex.repository.upsert_spot_asset_precision_evidence.assert_not_awaited()
        warning.assert_called_once()

    def test_walutomat_precision_scheduling_failure_is_contained(self) -> None:
        """Task creation failure closes the runner without leaking observation work.

        Given: A valid observed Walutomat attempt while task creation raises.
        When: Precision persistence is scheduled after snapshot commit.
        Then: The scheduler contains the failure and owns no orphaned task.
        """
        ex = _make_executor()
        attempt = _portfolio_attempt()
        attempt["exchange"] = ExchangeEnum.WALUTOMAT.value
        with (
            patch.object(base_module.asyncio, "create_task", side_effect=RuntimeError("closed")),
            patch.object(base_module.logger, "warning") as warning,
        ):
            ex._schedule_walutomat_balance_precision(ex.repository, attempt)

        warning.assert_called_once()
        assert ex._spot_precision_evidence_tasks == set()

    @pytest.mark.asyncio
    async def test_walutomat_precision_done_callback_contains_escaped_failure(self) -> None:
        """A stale failed task cannot remove replacement exchange ownership.

        Given: A failed owned task and a newer active task for the same exchange.
        When: The failed task's delayed completion callback runs.
        Then: Its exception is retrieved while the replacement remains active.
        """
        ex = _make_executor()

        async def fail() -> None:
            raise RuntimeError("escaped precision failure")

        failed = asyncio.create_task(fail())
        ex._spot_precision_evidence_tasks.add(failed)
        await asyncio.gather(failed, return_exceptions=True)
        replacement = asyncio.create_task(asyncio.Event().wait())
        exchange = ExchangeEnum.WALUTOMAT.value
        ex._spot_precision_evidence_active_tasks[exchange] = replacement

        with patch.object(base_module.logger, "warning") as warning:
            ex._spot_precision_evidence_task_done(exchange, failed)

        assert failed not in ex._spot_precision_evidence_tasks
        assert ex._spot_precision_evidence_active_tasks[exchange] is replacement
        warning.assert_called_once()
        assert "escaped containment" in warning.call_args.args[0]
        replacement.cancel()
        await asyncio.gather(replacement, return_exceptions=True)


class TestAccountObserverHandler:
    """The supervised observer loop's success and failure accounting."""

    @pytest.mark.asyncio
    async def test_handler_records_pass_then_sleeps(self) -> None:
        """A successful cycle stamps the last-pass clock and sleeps the cadence.

        Given: an observer whose single cycle succeeds and flips running
            to False,
        When: _account_observer_handler runs,
        Then: _observe_account_once is awaited once, 'account_observer' is
            stamped in _task_last_pass, and asyncio.sleep is awaited once
            with the observe interval.
        """
        ex = _make_executor()

        async def _observe_once() -> None:
            ex.running = False

        ex._observe_account_once = AsyncMock(side_effect=_observe_once)
        with patch(
            "snapper.messaging.executors.base.asyncio.sleep", new_callable=AsyncMock
        ) as sleep_mock:
            await ex._account_observer_handler()
        ex._observe_account_once.assert_awaited_once()
        assert "account_observer" in ex._task_last_pass
        sleep_mock.assert_awaited_once_with(_ACCOUNT_OBSERVE_INTERVAL_S)

    @pytest.mark.asyncio
    async def test_handler_counts_failure_and_survives(self) -> None:
        """A raising cycle increments the failure counter without crashing.

        Given: an observer whose single cycle raises after flipping running
            to False,
        When: _account_observer_handler runs,
        Then: _account_observer_failure_count is incremented by one, the
            last-pass clock is NOT stamped, and the loop still sleeps the
            cadence before exiting.
        """
        ex = _make_executor()
        start = ex._account_observer_failure_count

        async def _boom() -> None:
            ex.running = False
            raise RuntimeError("observe blew up")

        ex._observe_account_once = AsyncMock(side_effect=_boom)
        with patch(
            "snapper.messaging.executors.base.asyncio.sleep", new_callable=AsyncMock
        ) as sleep_mock:
            await ex._account_observer_handler()
        assert ex._account_observer_failure_count == start + 1
        assert "account_observer" not in ex._task_last_pass
        sleep_mock.assert_awaited_once_with(_ACCOUNT_OBSERVE_INTERVAL_S)


class TestPortfolioReconciliationOrchestration:
    """Owned observer-side reconciliation scheduling and lifecycle behavior."""

    @pytest.mark.asyncio
    async def test_snapshot_success_schedules_returned_state_without_blocking(self) -> None:
        """A committed live snapshot starts detached work with its exact state id."""
        ex = _make_executor()
        ex._get_exchange_name = MagicMock(return_value="kraken_futures")
        ex.repository.record_venue_account_snapshot = AsyncMock(return_value=41)
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.read_native_balances = AsyncMock(return_value=[])
        client.read_native_positions = AsyncMock(return_value=[])
        ex.exchange_client = client
        ex._portfolio_reconciliation_dispatch_open = True
        started = asyncio.Event()
        release = asyncio.Event()
        captured: list[base_module._PortfolioReconciliationWork] = []

        async def blocked(work: base_module._PortfolioReconciliationWork) -> None:
            captured.append(work)
            started.set()
            await release.wait()

        ex._run_portfolio_reconciliation = blocked
        await ex._observe_account_once()
        await asyncio.wait_for(started.wait(), timeout=1.0)
        assert captured[0].state_id == 41
        assert captured[0].position_capability is CapabilityStatus.SUPPORTED
        assert ex._background_tasks == set()
        client.read_native_balances.assert_awaited_once()
        client.read_native_positions.assert_awaited_once()
        release.set()
        await asyncio.gather(*tuple(ex._portfolio_reconciliation_tasks.values()))

    @pytest.mark.asyncio
    async def test_snapshot_failure_never_schedules(self) -> None:
        """A failed account write leaves no reconciliation work behind."""
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(return_value=[])
        ex.exchange_client = client
        ex.repository.record_venue_account_snapshot = AsyncMock(
            side_effect=RuntimeError("snapshot failed")
        )
        ex._schedule_portfolio_reconciliation = MagicMock()
        with pytest.raises(RuntimeError, match="snapshot failed"):
            await ex._observe_account_once()
        ex._schedule_portfolio_reconciliation.assert_not_called()

    @pytest.mark.asyncio
    async def test_paper_and_closed_gate_never_create_tasks(self) -> None:
        """Paper work and shutdown-gated live work are both rejected synchronously."""
        ex = _make_executor()
        ex._run_portfolio_reconciliation = MagicMock()
        ex._portfolio_reconciliation_dispatch_open = True
        ex._schedule_portfolio_reconciliation(
            state_id=1,
            attempt=_portfolio_attempt(mode="paper"),
            position_capability=CapabilityStatus.NOT_APPLICABLE,
        )
        ex._portfolio_reconciliation_dispatch_open = False
        ex._schedule_portfolio_reconciliation(
            state_id=2,
            attempt=_portfolio_attempt(),
            position_capability=CapabilityStatus.SUPPORTED,
        )
        ex._run_portfolio_reconciliation.assert_not_called()
        assert ex._portfolio_reconciliation_tasks == {}

    @pytest.mark.asyncio
    async def test_same_evaluation_key_is_single_flight(self) -> None:
        """A running exact-key task suppresses a duplicate scheduler call."""
        ex = _make_executor()
        ex._portfolio_reconciliation_dispatch_open = True
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocked(_work: object) -> None:
            started.set()
            await release.wait()

        ex._run_portfolio_reconciliation = AsyncMock(side_effect=blocked)
        attempt = _portfolio_attempt()
        ex._schedule_portfolio_reconciliation(
            state_id=1,
            attempt=attempt,
            position_capability=CapabilityStatus.SUPPORTED,
        )
        ex._schedule_portfolio_reconciliation(
            state_id=2,
            attempt=attempt,
            position_capability=CapabilityStatus.SUPPORTED,
        )
        await asyncio.wait_for(started.wait(), timeout=1.0)
        assert ex._run_portfolio_reconciliation.await_count == 1
        assert len(ex._portfolio_reconciliation_tasks) == 1
        release.set()
        await asyncio.gather(*tuple(ex._portfolio_reconciliation_tasks.values()))

    @pytest.mark.asyncio
    async def test_completed_same_key_can_be_replaced(self) -> None:
        """A completed entry does not suppress a later task with the same key."""
        ex = _make_executor()
        ex._portfolio_reconciliation_dispatch_open = True
        ex._run_portfolio_reconciliation = AsyncMock(return_value=None)
        attempt = _portfolio_attempt()
        ex._schedule_portfolio_reconciliation(
            state_id=1,
            attempt=attempt,
            position_capability=CapabilityStatus.SUPPORTED,
        )
        first = tuple(ex._portfolio_reconciliation_tasks.values())[0]
        await first
        ex._schedule_portfolio_reconciliation(
            state_id=2,
            attempt=attempt,
            position_capability=CapabilityStatus.SUPPORTED,
        )
        second = tuple(ex._portfolio_reconciliation_tasks.values())[0]
        await second
        assert ex._run_portfolio_reconciliation.await_count == 2

    def test_task_creation_failure_is_contained(self) -> None:
        """A scheduler runtime failure is counted and its coroutine is closed."""
        ex = _make_executor()
        ex._portfolio_reconciliation_dispatch_open = True
        with patch.object(
            base_module.asyncio,
            "create_task",
            side_effect=RuntimeError("no running loop"),
        ):
            ex._schedule_portfolio_reconciliation(
                state_id=1,
                attempt=_portfolio_attempt(),
                position_capability=CapabilityStatus.SUPPORTED,
            )
        assert ex._portfolio_reconciliation_failure_count == 1
        assert ex._last_portfolio_reconciliation_error == "no running loop"
        assert ex._portfolio_reconciliation_tasks == {}

    @pytest.mark.asyncio
    async def test_preflight_replay_skips_every_downstream_step(self) -> None:
        """An existing evaluation exits before state, config, dispatch, or S1 work."""
        ex = _make_executor()
        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(return_value=True)
        ex.repository.get_venue_account_state_version = AsyncMock()
        ex.repository.get_active_portfolio_reconciliation_method_config = AsyncMock()
        ex.repository.record_portfolio_reconciliation = AsyncMock()
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
        )
        with patch.object(
            base_module,
            "dispatch_portfolio_reconciliation",
            new_callable=AsyncMock,
        ) as dispatch:
            await ex._run_portfolio_reconciliation(work)
        ex.repository.get_venue_account_state_version.assert_not_awaited()
        ex.repository.get_active_portfolio_reconciliation_method_config.assert_not_awaited()
        dispatch.assert_not_awaited()
        ex.repository.record_portfolio_reconciliation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_preflight_race_reaches_s1_idempotency_backstop(self) -> None:
        """A post-preflight replay is delegated to the durable S1 writer.

        Given: The optimization misses a key that wins before the final S1 write.
        When: The runner builds and submits its single immutable evaluation object.
        Then: The S1 replay backstop receives that exact object without a retry rebuild.
        """
        ex = _make_executor()
        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(return_value=False)
        ex.repository.get_venue_account_state_version = AsyncMock(
            return_value={
                "wallet_public_id": "wallet-1",
                "exchange": "kraken_futures",
                "mode": "live",
                "session_id": "session-1",
                "sequence_id": 7,
            }
        )
        ex.repository.get_active_portfolio_reconciliation_method_config = AsyncMock(
            return_value=None
        )
        evaluation = _portfolio_evaluation()
        ex.repository.record_portfolio_reconciliation = AsyncMock(return_value=41)
        ex._schedule_portfolio_drift_notification = MagicMock()
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
        )
        with (
            patch.object(base_module, "build_portfolio_account_state", return_value=object()),
            patch.object(
                base_module,
                "dispatch_portfolio_reconciliation",
                new_callable=AsyncMock,
                return_value=evaluation,
            ) as dispatch,
        ):
            await ex._run_portfolio_reconciliation(work)
        dispatch.assert_awaited_once()
        ex.repository.record_portfolio_reconciliation.assert_awaited_once_with(evaluation)
        ex._schedule_portfolio_drift_notification.assert_called_once_with(evaluation)
        assert ex._portfolio_reconciliation_failure_count == 0

    @pytest.mark.asyncio
    async def test_reconciliation_commit_schedules_drift_before_invalidation(self) -> None:
        """A reconciliation schedules drift fanout before detached invalidation.

        Given: an exact account state that evaluates successfully,
        When: the durable reconciliation write returns,
        Then: the runner schedules the adjacent drift lifecycle lookup before
            the owned wallet-scoped thin reconciliation frame can run.
        """
        ex = _make_executor()
        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(return_value=False)
        ex.repository.get_venue_account_state_version = AsyncMock(
            return_value={
                "wallet_public_id": "wallet-1",
                "exchange": "kraken_futures",
                "mode": "live",
                "session_id": "session-1",
                "sequence_id": 7,
            }
        )
        ex.repository.get_active_portfolio_reconciliation_method_config = AsyncMock(
            return_value=None
        )
        evaluation = _portfolio_evaluation()
        timeline: list[str] = []

        async def record_reconciliation(
            committed: PortfolioReconciliationEvaluationRow,
        ) -> int:
            assert committed is evaluation
            timeline.append("commit")
            return 41

        async def send_event(
            topic: str,
            event: AccountStateChangedEventData,
            *,
            flags: int,
        ) -> None:
            assert topic == "portfolio.accounts.wallet-1"
            assert event.kind == "reconciliation"
            timeline.append("publish")

        def schedule_drift(committed: PortfolioReconciliationEvaluationRow) -> None:
            assert committed is evaluation
            timeline.append("drift")

        ex.repository.record_portfolio_reconciliation = AsyncMock(side_effect=record_reconciliation)
        publisher = _attach_account_event_publisher(ex)
        publisher.send = AsyncMock(side_effect=send_event)
        ex._schedule_portfolio_drift_notification = MagicMock(side_effect=schedule_drift)
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
        )

        with (
            patch.object(base_module, "build_portfolio_account_state", return_value=object()),
            patch.object(
                base_module,
                "dispatch_portfolio_reconciliation",
                new_callable=AsyncMock,
                return_value=evaluation,
            ),
        ):
            await ex._run_portfolio_reconciliation(work)

        assert timeline == ["commit", "drift"]
        task = tuple(ex._account_state_invalidation_tasks)[0]
        await task
        await asyncio.sleep(0)
        assert timeline == ["commit", "drift", "publish"]
        assert ex._account_state_invalidation_tasks == set()
        send_call = publisher.send.await_args
        assert send_call is not None
        event = send_call.args[1]
        assert isinstance(event, AccountStateChangedEventData)
        assert event.wallet_public_id == "wallet-1"
        assert event.exchange == "kraken_futures"
        assert event.mode == "live"
        assert event.session_id == ex._tracker.session_id
        assert event.sequence_id == 1
        assert send_call.kwargs == {"flags": base_module.zmq.NOBLOCK}
        assert ex._portfolio_reconciliation_failure_count == 0

    @pytest.mark.asyncio
    async def test_blocked_reconciliation_invalidation_stays_outside_timeout(self) -> None:
        """A blocked reconciliation send cannot become a recorded timeout.

        Given: a successfully committed reconciliation whose invalidation blocks,
        When: the owned send remains blocked beyond the reconciliation timeout,
        Then: drift work is scheduled and the committed runner remains successful.
        """
        ex = _make_executor()
        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(return_value=False)
        ex.repository.get_venue_account_state_version = AsyncMock(
            return_value={
                "wallet_public_id": "wallet-1",
                "exchange": "kraken_futures",
                "mode": "live",
                "session_id": "session-1",
                "sequence_id": 7,
            }
        )
        ex.repository.get_active_portfolio_reconciliation_method_config = AsyncMock(
            return_value=None
        )
        evaluation = _portfolio_evaluation()
        ex.repository.record_portfolio_reconciliation = AsyncMock(return_value=41)
        ex._schedule_portfolio_drift_notification = MagicMock()
        publisher = _attach_account_event_publisher(ex)
        send_started = asyncio.Event()
        send_cancelled = asyncio.Event()

        async def blocked_send(
            topic: str,
            event: AccountStateChangedEventData,
            *,
            flags: int,
        ) -> None:
            assert topic == "portfolio.accounts.wallet-1"
            assert event.kind == "reconciliation"
            assert flags == base_module.zmq.NOBLOCK
            send_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                send_cancelled.set()

        publisher.send = AsyncMock(side_effect=blocked_send)
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
        )

        with (
            patch.object(base_module, "_PORTFOLIO_RECONCILIATION_TIMEOUT_S", 0.01),
            patch.object(base_module, "build_portfolio_account_state", return_value=object()),
            patch.object(
                base_module,
                "dispatch_portfolio_reconciliation",
                new_callable=AsyncMock,
                return_value=evaluation,
            ),
        ):
            await asyncio.wait_for(ex._run_portfolio_reconciliation(work), timeout=0.1)
        await asyncio.wait_for(send_started.wait(), timeout=0.1)
        await asyncio.sleep(0.02)

        ex.repository.record_portfolio_reconciliation.assert_awaited_once_with(evaluation)
        ex._schedule_portfolio_drift_notification.assert_called_once_with(evaluation)
        task = tuple(ex._account_state_invalidation_tasks)[0]
        assert task.done() is False
        assert ex._portfolio_reconciliation_failure_count == 0

        await ex._close_and_drain_portfolio_reconciliation()

        assert task.cancelled()
        assert send_cancelled.is_set()
        assert ex._account_state_invalidation_tasks == set()

    @pytest.mark.asyncio
    async def test_cancelled_reconciliation_send_preserves_drift_notification(self) -> None:
        """Invalidation cancellation cannot suppress third-mismatch drift work.

        Given: a committed third-mismatch reconciliation and a cancelled account send,
        When: both owned notifications run after the durable transaction,
        Then: drift publication still runs and reconciliation stays successful.
        """
        ex = _make_executor()
        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(return_value=False)
        ex.repository.get_venue_account_state_version = AsyncMock(
            return_value={
                "wallet_public_id": "wallet-1",
                "exchange": "kraken_futures",
                "mode": "live",
                "session_id": "session-1",
                "sequence_id": 7,
            }
        )
        ex.repository.get_active_portfolio_reconciliation_method_config = AsyncMock(
            return_value=None
        )
        evaluation = _portfolio_evaluation()
        ex.repository.record_portfolio_reconciliation = AsyncMock(return_value=41)
        ex.repository.get_portfolio_drift_episode_transition = AsyncMock(
            return_value=_drift_transition()
        )
        publisher = _attach_account_event_publisher(ex)
        drift_sent = asyncio.Event()
        send_started = asyncio.Event()
        cancel_send = asyncio.Event()

        async def cancelled_send(
            topic: str,
            event: AccountStateChangedEventData | PortfolioDriftEpisodeEventData,
            *,
            flags: int,
        ) -> None:
            if topic == "bus.portfolio_drift_episode":
                assert isinstance(event, PortfolioDriftEpisodeEventData)
                assert event.lifecycle == "opened"
                assert event.mismatch_count == 3
                assert flags == base_module.zmq.NOBLOCK
                drift_sent.set()
                return
            assert topic == "portfolio.accounts.wallet-1"
            assert isinstance(event, AccountStateChangedEventData)
            assert event.kind == "reconciliation"
            assert flags == base_module.zmq.NOBLOCK
            send_started.set()
            await cancel_send.wait()
            raise asyncio.CancelledError

        publisher.send = AsyncMock(side_effect=cancelled_send)
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
        )

        with (
            patch.object(base_module, "build_portfolio_account_state", return_value=object()),
            patch.object(
                base_module,
                "dispatch_portfolio_reconciliation",
                new_callable=AsyncMock,
                return_value=evaluation,
            ),
        ):
            await ex._run_portfolio_reconciliation(work)
        invalidation_task = tuple(ex._account_state_invalidation_tasks)[0]
        await asyncio.wait_for(drift_sent.wait(), timeout=0.1)
        await asyncio.wait_for(send_started.wait(), timeout=0.1)

        ex.repository.record_portfolio_reconciliation.assert_awaited_once_with(evaluation)
        ex.repository.get_portfolio_drift_episode_transition.assert_awaited_once_with(
            "wallet-1",
            "kraken_futures",
            "live",
            "session-1",
            7,
        )
        cancel_send.set()
        await asyncio.gather(invalidation_task, return_exceptions=True)
        await asyncio.sleep(0)

        assert invalidation_task.cancelled()
        assert ex._account_state_invalidation_tasks == set()
        assert ex._portfolio_drift_notification_tasks == set()
        assert ex._portfolio_reconciliation_failure_count == 0

    @pytest.mark.asyncio
    async def test_scheduled_runner_failure_is_counted_exactly_once(self) -> None:
        """Runner containment and its done callback never double-count one failure.

        Given: A scheduled live reconciliation whose preflight read raises.
        When: The owned runner contains the error and its callback retrieves completion.
        Then: Portfolio failure accounting advances exactly once and the task is removed.
        """
        ex = _make_executor()
        ex._portfolio_reconciliation_dispatch_open = True
        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(
            side_effect=RuntimeError("preflight failed")
        )
        ex._schedule_portfolio_reconciliation(
            state_id=9,
            attempt=_portfolio_attempt(),
            position_capability=CapabilityStatus.SUPPORTED,
        )
        task = tuple(ex._portfolio_reconciliation_tasks.values())[0]
        await task
        await asyncio.sleep(0)
        assert ex._portfolio_reconciliation_failure_count == 1
        assert ex._last_portfolio_reconciliation_error == "preflight failed"
        assert ex._portfolio_reconciliation_tasks == {}

    @pytest.mark.asyncio
    async def test_runner_uses_exact_state_and_isolated_domain_dispatch(self) -> None:
        """The exact closed version flows to dispatch without taking the order lock."""
        ex = _make_executor()
        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(return_value=False)
        state_row = {
            "wallet_public_id": "wallet-1",
            "exchange": "kraken_futures",
            "mode": "live",
            "session_id": "session-1",
            "sequence_id": 7,
        }
        ex.repository.get_venue_account_state_version = AsyncMock(return_value=state_row)
        config = {"method": "futures_position"}
        ex.repository.get_active_portfolio_reconciliation_method_config = AsyncMock(
            return_value=config
        )
        ex.repository.record_portfolio_reconciliation = AsyncMock(return_value=12)
        ex._schedule_portfolio_drift_notification = MagicMock()
        account = object()
        evaluation = _portfolio_evaluation()
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
        )
        await ex._recon_lock.acquire()
        try:
            with (
                patch.object(
                    base_module,
                    "build_portfolio_account_state",
                    return_value=account,
                ) as build,
                patch.object(
                    base_module,
                    "dispatch_portfolio_reconciliation",
                    new_callable=AsyncMock,
                    return_value=evaluation,
                ) as dispatch,
            ):
                await asyncio.wait_for(ex._run_portfolio_reconciliation(work), timeout=1.0)
        finally:
            ex._recon_lock.release()
        ex.repository.get_venue_account_state_version.assert_awaited_once_with(9)
        evaluated_at = build.call_args.args[1]
        dispatch_call = dispatch.await_args
        assert dispatch_call is not None
        assert dispatch_call.kwargs == {
            "repository": ex.repository,
            "account": account,
            "method_config": config,
            "position_capability": CapabilityStatus.SUPPORTED,
            "evaluated_at": evaluated_at,
        }
        ex.repository.record_portfolio_reconciliation.assert_awaited_once_with(evaluation)
        ex._schedule_portfolio_drift_notification.assert_called_once_with(evaluation)
        assert ex._venue_recon_failure_count == 0
        assert ex._portfolio_reconciliation_failure_count == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("state_row", "expected_error"),
        [
            (None, "venue account state version is unavailable"),
            (
                {
                    "wallet_public_id": "other-wallet",
                    "exchange": "kraken_futures",
                    "mode": "live",
                    "session_id": "session-1",
                    "sequence_id": 7,
                },
                "venue account state version identity mismatch",
            ),
        ],
    )
    async def test_missing_or_mismatched_state_fails_before_mapping(
        self, state_row: object | None, expected_error: str
    ) -> None:
        """Unavailable or cross-identity state versions fail closed before dispatch."""
        ex = _make_executor()
        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(return_value=False)
        ex.repository.get_venue_account_state_version = AsyncMock(return_value=state_row)
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
        )
        with patch.object(base_module, "build_portfolio_account_state") as build:
            await ex._run_portfolio_reconciliation(work)
        build.assert_not_called()
        assert ex._portfolio_reconciliation_failure_count == 1
        assert ex._last_portfolio_reconciliation_error == expected_error
        assert ex._venue_recon_failure_count == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failure_step", ["config", "dispatch", "record"])
    async def test_branch_failures_are_contained_and_order_counters_unchanged(
        self, failure_step: str
    ) -> None:
        """Config, evaluation, and S1 failures stay in portfolio accounting."""
        ex = _make_executor()
        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(return_value=False)
        state_row = {
            "wallet_public_id": "wallet-1",
            "exchange": "kraken_futures",
            "mode": "live",
            "session_id": "session-1",
            "sequence_id": 7,
        }
        ex.repository.get_venue_account_state_version = AsyncMock(return_value=state_row)
        ex.repository.get_active_portfolio_reconciliation_method_config = AsyncMock(
            return_value=None
        )
        ex.repository.record_portfolio_reconciliation = AsyncMock(return_value=1)
        if failure_step == "config":
            ex.repository.get_active_portfolio_reconciliation_method_config.side_effect = (
                RuntimeError("config failed")
            )
        if failure_step == "record":
            ex.repository.record_portfolio_reconciliation.side_effect = RuntimeError(
                "record failed"
            )
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
        )
        dispatch_side_effect = (
            RuntimeError("dispatch failed") if failure_step == "dispatch" else None
        )
        with (
            patch.object(base_module, "build_portfolio_account_state", return_value=object()),
            patch.object(
                base_module,
                "dispatch_portfolio_reconciliation",
                new_callable=AsyncMock,
                return_value=object(),
                side_effect=dispatch_side_effect,
            ),
        ):
            await ex._run_portfolio_reconciliation(work)
        assert ex._portfolio_reconciliation_failure_count == 1
        assert ex._venue_recon_failure_count == 0
        assert ex._last_venue_recon_error == ""

    @pytest.mark.asyncio
    async def test_complete_runner_timeout_is_contained(self) -> None:
        """The hard bound cancels a wedged preflight and records one failure."""
        ex = _make_executor()

        async def blocked(*_args: object) -> bool:
            await asyncio.Event().wait()
            return False

        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(side_effect=blocked)
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
        )
        with patch.object(base_module, "_PORTFOLIO_RECONCILIATION_TIMEOUT_S", 0.01):
            await ex._run_portfolio_reconciliation(work)
        assert ex._portfolio_reconciliation_failure_count == 1
        assert ex._last_portfolio_reconciliation_error == "TimeoutError"

    @pytest.mark.asyncio
    async def test_runner_cancellation_propagates_without_failure_count(self) -> None:
        """Shutdown cancellation remains cancellation rather than an operational failure."""
        ex = _make_executor()
        started = asyncio.Event()

        async def blocked(*_args: object) -> bool:
            started.set()
            await asyncio.Event().wait()
            return False

        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(side_effect=blocked)
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
        )
        task = asyncio.create_task(ex._run_portfolio_reconciliation(work))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert ex._portfolio_reconciliation_failure_count == 0

    @pytest.mark.asyncio
    async def test_done_callback_removes_only_exact_task_and_retrieves_errors(self) -> None:
        """A stale callback preserves replacement work while consuming its own outcome."""
        ex = _make_executor()
        key = ("wallet-1", "kraken_futures", "live", "session-1", 7)

        async def fail() -> None:
            raise RuntimeError("escaped")

        failed = asyncio.create_task(fail())
        await asyncio.gather(failed, return_exceptions=True)
        replacement = asyncio.create_task(asyncio.Event().wait())
        ex._portfolio_reconciliation_tasks[key] = replacement
        ex._portfolio_reconciliation_task_done(key, failed)
        assert ex._portfolio_reconciliation_tasks[key] is replacement
        assert ex._portfolio_reconciliation_failure_count == 1
        replacement.cancel()
        await asyncio.gather(replacement, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_done_callback_accepts_cancelled_task(self) -> None:
        """A cancelled owned task is removed without being counted as a failure."""
        ex = _make_executor()
        key = ("wallet-1", "kraken_futures", "live", "session-1", 7)
        task = asyncio.create_task(asyncio.Event().wait())
        ex._portfolio_reconciliation_tasks[key] = task
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        ex._portfolio_reconciliation_task_done(key, task)
        assert ex._portfolio_reconciliation_tasks == {}
        assert ex._portfolio_reconciliation_failure_count == 0

    @pytest.mark.asyncio
    async def test_stop_drains_owned_tasks_even_when_already_not_running(self) -> None:
        """Stop closes, cancels, awaits, and removes reconciliation tasks unconditionally."""
        ex = _make_executor()
        ex.running = False
        ex._portfolio_reconciliation_dispatch_open = True
        started = asyncio.Event()

        async def blocked() -> None:
            started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(blocked())
        key = ("wallet-1", "kraken_futures", "live", "session-1", 7)
        ex._portfolio_reconciliation_tasks[key] = task
        await started.wait()
        await ex.stop()
        assert task.cancelled()
        assert ex._portfolio_reconciliation_tasks == {}
        assert ex._portfolio_reconciliation_dispatch_open is False

    @pytest.mark.asyncio
    async def test_stop_drains_observer_and_invalidation_tasks_spawned_during_drain(
        self,
    ) -> None:
        """Observer shutdown and repeated invalidation drainage leave no orphan.

        Given: A live observer whose cancellation tail starts an invalidation,
            and that invalidation's cancellation tail starts another one.
        When: Executor stop cancels the observer and drains owned publications.
        Then: The observer finishes before drainage, both publications are
            cancelled and awaited, and no task loses ownership while live.
        """
        ex = _make_executor()
        observer_started = asyncio.Event()
        observer_stopped = asyncio.Event()
        first_started = asyncio.Event()
        first_stopped = asyncio.Event()
        second_started = asyncio.Event()
        second_stopped = asyncio.Event()
        invalidation_tasks: list[asyncio.Task[None]] = []

        async def second_invalidation() -> None:
            second_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                second_stopped.set()

        async def first_invalidation() -> None:
            first_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                first_stopped.set()
                second_task = asyncio.create_task(second_invalidation())
                invalidation_tasks.append(second_task)
                ex._account_state_invalidation_tasks.add(second_task)
                second_task.add_done_callback(ex._account_state_invalidation_tasks.discard)
                await second_started.wait()

        async def observer() -> None:
            observer_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                observer_stopped.set()
                first_task = asyncio.create_task(first_invalidation())
                invalidation_tasks.append(first_task)
                ex._account_state_invalidation_tasks.add(first_task)
                first_task.add_done_callback(ex._account_state_invalidation_tasks.discard)
                await first_started.wait()

        observer_task = asyncio.create_task(observer())
        ex._account_observer_tasks.add(observer_task)
        observer_task.add_done_callback(ex._account_observer_tasks.discard)
        await observer_started.wait()

        await ex.stop()

        assert observer_task.cancelled()
        assert observer_stopped.is_set()
        assert len(invalidation_tasks) == 2
        assert all(task.cancelled() for task in invalidation_tasks)
        assert first_stopped.is_set()
        assert second_stopped.is_set()
        assert ex._account_observer_tasks == set()
        assert ex._account_state_invalidation_tasks == set()

    @pytest.mark.asyncio
    async def test_cancelled_stop_keeps_zmq_teardown_retryable(self) -> None:
        """Cancellation during owned-work drainage cannot suppress later teardown.

        Given: A running executor whose first stop is cancelled during drainage.
        When: Stop is retried after the cancellation.
        Then: Running state still authorizes the retry to close every ZMQ resource.
        """
        ex = _make_executor()
        subscriber = MagicMock()
        publisher = MagicMock()
        context = MagicMock()
        ex.subscriber = subscriber
        ex.publisher = publisher
        ex.context = context

        with (
            patch.object(
                ex,
                "_close_and_drain_portfolio_reconciliation",
                new=AsyncMock(side_effect=asyncio.CancelledError),
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await ex.stop()

        assert ex.running is True
        subscriber.close.assert_not_called()
        publisher.close.assert_not_called()
        context.term.assert_not_called()

        await ex.stop()

        assert ex.running is False
        subscriber.close.assert_called_once()
        publisher.close.assert_called_once()
        context.term.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_cancellation_outer_cleanup_drains_owned_work(self) -> None:
        """Outer start cleanup drains work even when cancellation bypasses stop."""
        ex = _make_executor()
        ex.running = False
        client = MagicMock(spec=base_module.ExchangeClientBase)
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=None)
        client.set_tracker = MagicMock()
        client.supports_websocket_executions = False
        client.balance_capability = CapabilityStatus.SUPPORTED
        client.position_capability = CapabilityStatus.NOT_APPLICABLE
        ex._create_exchange_client = MagicMock(return_value=client)
        ex._initialize_settings = AsyncMock()
        ex._resolve_credentials = AsyncMock()
        ex._setup_zmq_sockets = MagicMock()
        ex._recover_pending_orders = AsyncMock()
        portfolio_started = asyncio.Event()
        portfolio_cancelled = asyncio.Event()

        async def blocked(_work: object) -> None:
            portfolio_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                portfolio_cancelled.set()

        async def supervise(
            task_label: str,
            _attempt: object,
            **_kwargs: object,
        ) -> None:
            if task_label == "account_observer":
                assert ex._portfolio_reconciliation_dispatch_open is True
                ex._schedule_portfolio_reconciliation(
                    state_id=1,
                    attempt=_portfolio_attempt(),
                    position_capability=CapabilityStatus.NOT_APPLICABLE,
                )
            await asyncio.Event().wait()

        ex._run_portfolio_reconciliation = blocked
        ex._supervise_loop = supervise
        start_task = asyncio.create_task(ex.start())
        await asyncio.wait_for(portfolio_started.wait(), timeout=1.0)
        start_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await start_task
        assert portfolio_cancelled.is_set()
        assert ex._account_observer_tasks == set()
        assert ex._portfolio_reconciliation_tasks == {}
        assert ex._portfolio_reconciliation_dispatch_open is False
        assert ex._client_context_active is False


class TestAccountStateChangedEmission:
    """Best-effort account invalidation publication failure behavior."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["snapshot", "reconciliation"])
    async def test_publish_failure_is_swallowed_for_each_commit_kind(
        self,
        kind: Literal["snapshot", "reconciliation"],
    ) -> None:
        """A broker failure cannot replace either durable commit result.

        Given: an attached publisher whose non-blocking send fails,
        When: either account-state commit kind emits its invalidation,
        Then: the helper logs and returns without touching reconciliation or
            order-domain failure accounting.
        """
        ex = _make_executor()
        publisher = _attach_account_event_publisher(ex)
        publisher.send.side_effect = RuntimeError("broker full")

        with patch.object(base_module.logger, "warning") as warning:
            await ex._publish_account_state_changed(
                wallet_public_id="wallet-1",
                exchange=ExchangeEnum.KRAKEN_FUTURES,
                mode=base_module.ExecutionModeEnum.LIVE,
                kind=kind,
            )

        warning.assert_called_once()
        assert warning.call_args.args[0] == (
            "account state invalidation failed after {} commit: {}"
        )
        assert warning.call_args.args[1] == kind
        assert isinstance(warning.call_args.args[2], RuntimeError)
        assert ex._portfolio_reconciliation_failure_count == 0
        assert ex._venue_recon_failure_count == 0

    @pytest.mark.asyncio
    async def test_publish_cancellation_propagates(self) -> None:
        """Shutdown cancellation remains visible rather than becoming delivery loss."""
        ex = _make_executor()
        publisher = _attach_account_event_publisher(ex)
        publisher.send.side_effect = asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await ex._publish_account_state_changed(
                wallet_public_id="wallet-1",
                exchange=ExchangeEnum.KRAKEN_FUTURES,
                mode=base_module.ExecutionModeEnum.LIVE,
                kind="snapshot",
            )

    def test_scheduler_creation_failure_is_contained(self) -> None:
        """A task-factory failure cannot alter already committed account truth."""
        ex = _make_executor()

        with (
            patch.object(
                base_module.asyncio,
                "create_task",
                side_effect=RuntimeError("task factory unavailable"),
            ),
            patch.object(base_module.logger, "warning") as warning,
        ):
            ex._schedule_account_state_changed(
                wallet_public_id="wallet-1",
                exchange=ExchangeEnum.KRAKEN_FUTURES,
                mode=base_module.ExecutionModeEnum.LIVE,
                kind="reconciliation",
            )

        warning.assert_called_once()
        assert warning.call_args.args[0] == (
            "account state invalidation scheduling failed after {} commit: {}"
        )
        assert warning.call_args.args[1] == "reconciliation"
        assert isinstance(warning.call_args.args[2], RuntimeError)
        assert ex._account_state_invalidation_tasks == set()
        assert ex._portfolio_reconciliation_failure_count == 0


class TestPortfolioDriftNotificationEmission:
    """Post-commit lifecycle publication stays notify-only and isolated."""

    @staticmethod
    def _publisher(executor: _DummyExecutor) -> MagicMock:
        """Attach a publisher mock sharing the executor sequence tracker."""
        publisher = MagicMock()
        publisher.tracker = executor._tracker
        publisher.send = AsyncMock()
        executor.msg_publisher = publisher
        return publisher

    @pytest.mark.asyncio
    async def test_open_transition_publishes_nonblocking_typed_event(self) -> None:
        """The initial third mismatch emits one typed opened frame after commit."""
        ex = _make_executor()
        publisher = self._publisher(ex)
        transition = _drift_transition()
        ex.repository.get_portfolio_drift_episode_transition = AsyncMock(return_value=transition)

        await ex._publish_portfolio_drift_notification(_portfolio_evaluation())

        ex.repository.get_portfolio_drift_episode_transition.assert_awaited_once_with(
            "wallet-1",
            "kraken_futures",
            "live",
            "session-1",
            7,
        )
        send_call = publisher.send.await_args
        assert send_call is not None
        assert send_call.args[0] == "bus.portfolio_drift_episode"
        event = send_call.args[1]
        assert isinstance(event, PortfolioDriftEpisodeEventData)
        assert event.lifecycle == "opened"
        assert event.episode_public_id == "episode-1"
        assert event.closed_at is None
        assert event.resolution_reason is None
        assert send_call.kwargs == {"flags": base_module.zmq.NOBLOCK}

    @pytest.mark.asyncio
    async def test_resolved_transition_publishes_same_episode_resolution(self) -> None:
        """A matched close emits a resolved notice with durable close evidence."""
        ex = _make_executor()
        publisher = self._publisher(ex)
        closed_at = datetime(2026, 7, 15, 10, 8, tzinfo=UTC)
        ex.repository.get_portfolio_drift_episode_transition = AsyncMock(
            return_value=_drift_transition(
                status="resolved",
                closed_at=closed_at,
                mismatch_count=5,
                resolution_reason="matched",
            )
        )

        await ex._publish_portfolio_drift_notification(_portfolio_evaluation())

        send_call = publisher.send.await_args
        assert send_call is not None
        event = send_call.args[1]
        assert isinstance(event, PortfolioDriftEpisodeEventData)
        assert event.lifecycle == "resolved"
        assert event.episode_public_id == "episode-1"
        assert event.closed_at == closed_at
        assert event.mismatch_count == 5
        assert event.resolution_reason == "matched"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "transition",
        [
            None,
            _drift_transition(mode="paper"),
            _drift_transition(mismatch_count=4),
            _drift_transition(last_observation_id=13),
            _drift_transition(
                status="resolved",
                closed_at=None,
                resolution_reason="matched",
            ),
            _drift_transition(
                status="resolved",
                closed_at=datetime(2026, 7, 15, 10, 8, tzinfo=UTC),
                resolution_reason=None,
            ),
            _drift_transition(status="rebased"),
        ],
    )
    async def test_non_transition_and_invalid_lifecycle_rows_stay_silent(
        self,
        transition: PortfolioDriftEpisodeTransitionRow | None,
    ) -> None:
        """Normal, continued, malformed, and out-of-scope results never notify."""
        ex = _make_executor()
        publisher = self._publisher(ex)
        ex.repository.get_portfolio_drift_episode_transition = AsyncMock(return_value=transition)

        await ex._publish_portfolio_drift_notification(_portfolio_evaluation())

        publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_publisher_stays_silent_after_transition_read(self) -> None:
        """A not-yet-wired ZMQ publisher cannot affect committed truth."""
        ex = _make_executor()
        ex.msg_publisher = None
        ex.repository.get_portfolio_drift_episode_transition = AsyncMock(
            return_value=_drift_transition()
        )

        await ex._publish_portfolio_drift_notification(_portfolio_evaluation())

        ex.repository.get_portfolio_drift_episode_transition.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_lookup_or_publish_failure_is_fully_contained(self) -> None:
        """Notify failure never increments reconciliation or order counters."""
        ex = _make_executor()
        publisher = self._publisher(ex)
        ex.repository.get_portfolio_drift_episode_transition = AsyncMock(
            return_value=_drift_transition()
        )
        publisher.send.side_effect = RuntimeError("broker full")

        await ex._publish_portfolio_drift_notification(_portfolio_evaluation())

        assert ex._portfolio_reconciliation_failure_count == 0
        assert ex._venue_recon_failure_count == 0

    @pytest.mark.asyncio
    async def test_notification_cancellation_propagates(self) -> None:
        """Shutdown cancellation remains visible to the owning task drain."""
        ex = _make_executor()
        ex.repository.get_portfolio_drift_episode_transition = AsyncMock(
            side_effect=asyncio.CancelledError
        )

        with pytest.raises(asyncio.CancelledError):
            await ex._publish_portfolio_drift_notification(_portfolio_evaluation())

    @pytest.mark.asyncio
    async def test_scheduler_tracks_and_discards_completed_notify_task(self) -> None:
        """A successful scheduling call owns the task until completion."""
        ex = _make_executor()
        ex._publish_portfolio_drift_notification = AsyncMock(return_value=None)
        evaluation = _portfolio_evaluation()

        ex._schedule_portfolio_drift_notification(evaluation)

        task = tuple(ex._portfolio_drift_notification_tasks)[0]
        await task
        await asyncio.sleep(0)
        ex._publish_portfolio_drift_notification.assert_awaited_once_with(evaluation)
        assert ex._portfolio_drift_notification_tasks == set()

    def test_scheduler_creation_failure_is_contained(self) -> None:
        """A task-factory failure closes the coroutine without touching truth."""
        ex = _make_executor()
        evaluation = _portfolio_evaluation()
        with patch.object(
            base_module.asyncio,
            "create_task",
            side_effect=RuntimeError("task factory unavailable"),
        ):
            ex._schedule_portfolio_drift_notification(evaluation)

        assert ex._portfolio_drift_notification_tasks == set()
        assert ex._portfolio_reconciliation_failure_count == 0

    @pytest.mark.asyncio
    async def test_stop_cancels_and_drains_notification_task(self) -> None:
        """Executor shutdown owns and drains an in-flight notify-only task."""
        ex = _make_executor()
        ex.running = False
        started = asyncio.Event()

        async def blocked() -> None:
            started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(blocked())
        ex._portfolio_drift_notification_tasks.add(task)
        await started.wait()

        await ex.stop()

        assert task.cancelled()
        assert ex._portfolio_drift_notification_tasks == set()
