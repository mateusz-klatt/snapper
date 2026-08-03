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
from decimal import Decimal
from typing import Any
from typing import Literal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.portfolio.pnl_snapshot_planner import evaluate_basket
from snapper.application.portfolio.reconciliation_dispatch import SpotReplayBoundaryCapture
from snapper.application.portfolio.spot_anchor_witness import WitnessOutcome
from snapper.application.portfolio.walutomat_history_certificate import HistoryRangeEvidence
from snapper.application.portfolio.walutomat_history_certificate import SpotHistoryRangeCapture
from snapper.core.types import ExchangeEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PortfolioDriftEpisodeTransitionRow
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import SpotAssetPrecisionEvidenceUpsertRow
from snapper.data.repository_types import SpotReconciliationAnchorRow
from snapper.data.repository_types import VenueAccountAttemptRow
from snapper.data.repository_types import VenueAccountObservationAttemptRow
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry
from snapper.infrastructure.exchanges.contracts import OpenPositionSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryItem
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryTip
from snapper.infrastructure.exchanges.contracts import VenueOrderFillLegs
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.messaging.executors import base as base_module
from snapper.messaging.executors.base import _ACCOUNT_FRESHNESS_CEILING_S
from snapper.messaging.executors.base import _ACCOUNT_OBSERVE_INTERVAL_S
from snapper.messaging.executors.base import _ACCOUNT_UNEXPECTED_BALANCE_CAPABILITY_MSG
from snapper.messaging.executors.base import _ACCOUNT_UNEXPECTED_POSITION_CAPABILITY_MSG
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.executors.base import _PortfolioReconciliationWork
from snapper.messaging.executors.base import _SpotAnchorCursorCapture
from snapper.messaging.executors.base import _SpotHistoryTipCapture
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
    ex.repository.get_spot_execution_watermark = AsyncMock(
        return_value=(0, datetime(2026, 7, 14, 9, 59, tzinfo=UTC))
    )
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

    ``AsyncMock`` makes every attribute an async child, so all capability
    declarations are assigned as concrete values. Observation mirrors the
    policy argument unless an orthogonality test explicitly makes them
    disagree; individual tests wire the native readers as needed.
    """
    client: Any = AsyncMock()
    client.balance_capability = balance_capability
    client.position_capability = position_capability
    client.position_observation_capability = position_capability
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
        "source_watermark_kind": "scope_sequence",
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

        Given: a client whose position_observation_capability is NOT_APPLICABLE,
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

        Given: a client whose position_observation_capability is UNSUPPORTED,
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
    async def test_kraken_observation_support_ignores_not_applicable_policy(self) -> None:
        """Kraken observation support reads positions despite cash-only policy.

        Given: Kraken's reconciliation policy remains NOT_APPLICABLE while its
            observer declaration is SUPPORTED and its reader returns one
            position,
        When: _read_account_positions runs,
        Then: the position is faithfully serialized as observed and the reader
            is awaited exactly once. This catches a production mutation that
            consults ``position_capability`` at the observer boundary or
            requires both declarations to be SUPPORTED.
        """
        ex = _make_executor()
        now = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)
        position_time = datetime(2026, 8, 1, 9, 59, tzinfo=UTC)
        client = _make_client(
            CapabilityStatus.UNSUPPORTED,
            KrakenExchangeClient.position_capability,
        )
        client.position_observation_capability = (
            KrakenExchangeClient.position_observation_capability
        )
        client.read_native_positions = AsyncMock(
            return_value=[
                OpenPositionSnapshot(
                    symbol="BTC-EUR",
                    side=OrderSideEnum.SELL,
                    size=0.25,
                    entry_price=100000.0,
                    mark_price=96000.0,
                    unrealized_pnl=1000.0,
                    unrealized_funding=0.0,
                    timestamp=position_time,
                )
            ]
        )

        status, payload, observed_at, error = await ex._read_account_positions(client, now)

        assert status == "observed"
        assert observed_at == now
        assert error is None
        assert payload is not None
        assert json.loads(payload) == [
            {
                "symbol": "BTC-EUR",
                "side": "sell",
                "size": 0.25,
                "entry_price": 100000.0,
                "mark_price": 96000.0,
                "unrealized_pnl": 1000.0,
                "unrealized_funding": 0.0,
                "timestamp": position_time.isoformat(),
            }
        ]
        client.read_native_positions.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_not_applicable_observation_ignores_supported_policy(self) -> None:
        """An explicit observer absence wins over reconciliation support.

        Given: a client whose reconciliation policy is SUPPORTED but whose
            observer declaration is NOT_APPLICABLE,
        When: _read_account_positions runs,
        Then: it reports not_applicable without awaiting the reader. This
            catches a production mutation that routes observation from the
            legacy policy declaration instead of the observer declaration.
        """
        ex = _make_executor()
        now = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)
        client = _make_client(CapabilityStatus.UNSUPPORTED, CapabilityStatus.SUPPORTED)
        client.position_observation_capability = CapabilityStatus.NOT_APPLICABLE
        client.read_native_positions = AsyncMock()

        result = await ex._read_account_positions(client, now)

        assert result == ("not_applicable", None, None, None)
        client.read_native_positions.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_kraken_observed_empty_is_present_empty_array(self) -> None:
        """Kraken's genuine empty position book is observed as ``[]``.

        Given: Kraken's SUPPORTED observer declaration and an empty successful
            venue response while reconciliation remains NOT_APPLICABLE,
        When: _read_account_positions runs,
        Then: it reports observed with the literal serialized empty array and
            an observation timestamp. This catches production mutations that
            turn observed-empty into not_applicable, NULL, or a failed read.
        """
        ex = _make_executor()
        now = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)
        client = _make_client(
            CapabilityStatus.UNSUPPORTED,
            KrakenExchangeClient.position_capability,
        )
        client.position_observation_capability = (
            KrakenExchangeClient.position_observation_capability
        )
        client.read_native_positions = AsyncMock(return_value=[])

        result = await ex._read_account_positions(client, now)

        assert result == ("observed", "[]", now, None)
        client.read_native_positions.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_non_observable_capability_with_data_is_fail_closed_error(self) -> None:
        """A non-SUPPORTED capability that still returned positions is 'error'.

        Given: a client whose position_observation_capability is SIMULATED (it
            reaches the read but is not SUPPORTED) whose reader returns a
            position,
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
    async def test_position_failure_preserves_balance_authority_and_policy_gate(self) -> None:
        """A position-only failure leaves balance authority and policy intact.

        Given: Kraken observes a valid USD balance, its supported position
            reader fails, and its reconciliation policy remains NOT_APPLICABLE,
        When: one account-observer cycle persists and schedules the attempt,
        Then: balance status and authority remain observed, only position status
            is error, reconciliation receives the legacy policy declaration,
            and the Phase-5B basket gate still accepts the balance despite the
            shared error text. This catches production mutations that couple
            position failure to balance authority, gate Phase-5B on the shared
            error or position status, or pass observation capability into
            reconciliation.
        """
        ex = _make_executor()
        ex._schedule_portfolio_reconciliation = MagicMock()
        client = _make_client(
            CapabilityStatus.SUPPORTED,
            KrakenExchangeClient.position_capability,
        )
        client.position_observation_capability = (
            KrakenExchangeClient.position_observation_capability
        )
        client.read_native_balances = AsyncMock(
            return_value=[NativeBalanceEntry(currency="USD", total=125.0, free=100.0, used=25.0)]
        )
        client.read_native_positions = AsyncMock(
            side_effect=RuntimeError("Kraken position read failed")
        )
        ex.exchange_client = client

        await ex._observe_account_once()

        attempt: VenueAccountAttemptRow = (
            ex.repository.record_venue_account_snapshot.await_args.args[0]
        )
        assert attempt["balance_status"] == "observed"
        assert attempt["position_status"] == "error"
        assert attempt["balances_json"] is not None
        assert attempt["open_positions_json"] is None
        assert attempt["balance_observed_at"] is not None
        assert attempt["position_observed_at"] is None
        assert attempt["authoritative_until"] == attempt["balance_observed_at"] + timedelta(
            seconds=_ACCOUNT_FRESHNESS_CEILING_S
        )
        assert attempt["error"] == "Kraken position read failed"
        client.read_native_balances.assert_awaited_once_with()
        client.read_native_positions.assert_awaited_once_with()
        schedule_call = ex._schedule_portfolio_reconciliation.call_args
        assert schedule_call is not None
        assert schedule_call.kwargs["position_capability"] is CapabilityStatus.NOT_APPLICABLE

        basket_attempt: VenueAccountObservationAttemptRow = {
            "id": 1,
            "public_id": "obs-position-failure",
            "wallet_public_id": attempt["wallet_public_id"],
            "exchange": attempt["exchange"],
            "mode": attempt["mode"],
            "attempt_status": "error",
            "balance_status": attempt["balance_status"],
            "position_status": attempt["position_status"],
            "balances_json": attempt["balances_json"],
            "open_positions_json": attempt["open_positions_json"],
            "balance_observed_at": attempt["balance_observed_at"],
            "position_observed_at": attempt["position_observed_at"],
            "error": attempt["error"],
            "timestamp": attempt["bus_time"],
            "session_id": attempt["session_id"],
            "sequence_id": attempt["sequence_id"],
        }
        balance_observed_at = attempt["balance_observed_at"]
        assert balance_observed_at is not None
        basket = evaluate_basket(
            balance_observed_at,
            frozenset({"kraken"}),
            {"kraken": basket_attempt},
        )
        assert basket.reason_codes == frozenset({"position_book_unproven"})
        assert basket.observed_balances == {}

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
            boundary: SpotReplayBoundaryCapture | None,
            cursor_capture: _SpotAnchorCursorCapture | None,
            history_capture: SpotHistoryRangeCapture | None,
        ) -> None:
            assert cursor_capture is None
            assert history_capture is None
            assert state_id == 41
            assert attempt["wallet_public_id"] == "wallet-1"
            assert position_capability is CapabilityStatus.NOT_APPLICABLE
            assert isinstance(boundary, SpotReplayBoundaryCapture)
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

        Given: a SUPPORTED balance client whose position observation capability
            is NOT_APPLICABLE,
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
            datetime_type.now.side_effect = [observation_time] * 7 + [persistence_time]
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


class TestSpotBoundaryCapture:
    """Pre-balance execution-watermark capture and boundary assembly (S4c-2)."""

    @staticmethod
    def _live_executor(watermarks: AsyncMock) -> tuple[Any, list[str], list[datetime]]:
        """Wire one live executor whose scoped reads log a shared call order.

        The watermark repository read, the balance read, and the position
        read all append to one ordered log so tests can prove the physical
        capture ordering rather than infer it from timestamps alone. The
        exact ``as_of`` join instants the fenced repository read received
        are recorded too, mirroring the ``(watermark, as_of)`` return
        contract so the boundary's as_of threading is observable.
        """
        ex = _make_executor()
        calls: list[str] = []
        as_ofs: list[datetime] = []
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)

        async def read_balances() -> list[NativeBalanceEntry]:
            calls.append("balances")
            return []

        async def read_positions() -> list[OpenPositionSnapshot]:
            calls.append("positions")
            return []

        async def watermark_read(
            wallet: str, exchange: str, mode: str, as_of: datetime
        ) -> tuple[int, datetime]:
            assert wallet == "wallet-1"
            assert exchange == "kraken"
            assert mode == "live"
            assert as_of.utcoffset() is not None
            calls.append("watermark")
            as_ofs.append(as_of)
            watermark = await watermarks(wallet, exchange, mode, as_of)
            assert isinstance(watermark, int)
            return watermark, as_of

        client.read_native_balances = AsyncMock(side_effect=read_balances)
        client.read_native_positions = AsyncMock(side_effect=read_positions)
        ex.repository.get_spot_execution_watermark = AsyncMock(side_effect=watermark_read)
        ex.exchange_client = client
        ex._schedule_portfolio_reconciliation = MagicMock()
        return ex, calls, as_ofs

    @staticmethod
    def _scheduled_boundary(ex: Any) -> SpotReplayBoundaryCapture | None:
        """Return the boundary the observer handed to reconciliation scheduling."""
        schedule_call = ex._schedule_portfolio_reconciliation.call_args
        assert schedule_call is not None
        boundary = schedule_call.kwargs["boundary"]
        assert boundary is None or isinstance(boundary, SpotReplayBoundaryCapture)
        return boundary

    @pytest.mark.asyncio
    async def test_watermark_capture_strictly_precedes_the_balance_read(self) -> None:
        """The genuine pre-balance ordering is physically enforced, not labelled.

        Given: A live executor whose watermark, balance, and position reads
            log into one shared ordered call log.
        When: One observation cycle runs.
        Then: The watermark read happens strictly before the balance read and
            again strictly after the position read, and the scheduled boundary
            carries the pre-read watermark with an equal after-read and the
            quiescence flag True.
        """
        ex, calls, _ = self._live_executor(AsyncMock(return_value=7))
        await ex._observe_account_once()
        assert calls == ["watermark", "balances", "positions", "watermark"]
        boundary = self._scheduled_boundary(ex)
        assert boundary is not None
        assert boundary.source_watermark == 7
        assert boundary.watermark_after == 7
        assert boundary.watermark_unchanged is True

    @pytest.mark.asyncio
    async def test_boundary_binds_the_exact_account_state_identity(self) -> None:
        """The boundary carries the same identity the runner later verifies.

        A capture certifies exactly one observation cycle, so its binding
        must equal the persisted attempt's
        ``(wallet, exchange, mode, session, sequence)`` identity — dispatch
        refuses any boundary whose binding differs from the evaluated
        account, which is what makes a foreign or stale-cycle capture inert.

        Given: One live observation cycle.
        When: The snapshot is persisted and the boundary is scheduled.
        Then: Every boundary identity field equals the recorded attempt's.
        """
        ex, _, _ = self._live_executor(AsyncMock(return_value=7))
        await ex._observe_account_once()
        attempt = ex.repository.record_venue_account_snapshot.await_args.args[0]
        boundary = self._scheduled_boundary(ex)
        assert boundary is not None
        assert boundary.wallet_public_id == attempt["wallet_public_id"]
        assert boundary.exchange == attempt["exchange"]
        assert boundary.mode == attempt["mode"]
        assert boundary.session_id == attempt["session_id"]
        assert boundary.sequence_id == attempt["sequence_id"]

    @pytest.mark.asyncio
    async def test_boundary_threads_the_pre_read_join_as_of_verbatim(self) -> None:
        """The boundary carries the exact join instant of the pre-balance read.

        The S4c-4 replay bundle must re-read the execution range with the
        SAME temporal predicate the capture used, so the boundary's
        ``as_of`` must be the first repository read's join instant — never
        the after-read's and never a fresh clock sample.

        Given: One live observation cycle recording each read's as_of.
        When: The boundary is scheduled.
        Then: boundary.as_of is exactly the first read's join instant and
            precedes the recorded capture instant.
        """
        ex, _, as_ofs = self._live_executor(AsyncMock(return_value=7))
        await ex._observe_account_once()
        boundary = self._scheduled_boundary(ex)
        assert boundary is not None
        assert len(as_ofs) == 2
        assert boundary.as_of == as_ofs[0]
        assert boundary.as_of <= boundary.watermark_captured_at

    @pytest.mark.asyncio
    async def test_boundary_timestamps_bracket_the_balance_request(self) -> None:
        """Capture and request instants are ordered and bracket the venue read.

        Given: A live cycle whose balance read records its own wall-clock
            instant while executing.
        When: The observation completes.
        Then: as_of <= watermark_captured_at <= request_started_at <= the
            in-read instant <= request_completed_at <=
            watermark_after_captured_at.
        """
        ex, _, _ = self._live_executor(AsyncMock(return_value=3))
        during: list[datetime] = []
        original = ex.exchange_client.read_native_balances.side_effect

        async def timed_balances() -> list[NativeBalanceEntry]:
            during.append(datetime.now(UTC))
            result: list[NativeBalanceEntry] = await original()
            return result

        ex.exchange_client.read_native_balances = AsyncMock(side_effect=timed_balances)
        await ex._observe_account_once()
        boundary = self._scheduled_boundary(ex)
        assert boundary is not None
        assert boundary.watermark_after_captured_at is not None
        assert boundary.as_of <= boundary.watermark_captured_at
        assert boundary.watermark_captured_at <= boundary.request_started_at
        assert boundary.request_started_at <= during[0]
        assert during[0] <= boundary.request_completed_at
        assert boundary.request_completed_at <= boundary.watermark_after_captured_at

    @pytest.mark.asyncio
    async def test_failed_pre_read_leaves_observation_intact_and_boundary_absent(
        self,
    ) -> None:
        """A watermark failure degrades only the boundary, never observation.

        Given: A live cycle whose watermark repository read always raises.
        When: The observation cycle runs.
        Then: The snapshot is persisted and scheduled normally, the boundary
            is explicitly absent, and the after-read is never attempted (there
            is no genuine pre-read to compare against).
        """
        ex, calls, _ = self._live_executor(AsyncMock(side_effect=RuntimeError("db down")))
        await ex._observe_account_once()
        ex.repository.record_venue_account_snapshot.assert_awaited_once()
        attempt = ex.repository.record_venue_account_snapshot.await_args.args[0]
        assert attempt["balance_status"] == "observed"
        assert calls == ["watermark", "balances", "positions"]
        assert self._scheduled_boundary(ex) is None

    @pytest.mark.asyncio
    async def test_failed_after_read_keeps_the_genuine_pre_read_boundary(self) -> None:
        """A failed after-read never invalidates the genuine pre-read capture.

        Given: A live cycle whose second watermark read raises.
        When: The observation completes.
        Then: The boundary keeps the pre-read watermark while the after-read
            fields stay honestly absent and the quiescence flag stays False.
        """
        ex, _, _ = self._live_executor(AsyncMock(side_effect=[3, RuntimeError("db down")]))
        await ex._observe_account_once()
        boundary = self._scheduled_boundary(ex)
        assert boundary is not None
        assert boundary.source_watermark == 3
        assert boundary.watermark_after is None
        assert boundary.watermark_after_captured_at is None
        assert boundary.watermark_unchanged is False

    @pytest.mark.asyncio
    async def test_advanced_after_read_reports_inequality(self) -> None:
        """Watermark movement across the read window is reported truthfully.

        Given: A live cycle whose watermark advances between the two reads.
        When: The observation completes.
        Then: The boundary carries both watermarks with watermark_unchanged
            False for the future quiescence/no-advance check.
        """
        ex, _, _ = self._live_executor(AsyncMock(side_effect=[3, 9]))
        await ex._observe_account_once()
        boundary = self._scheduled_boundary(ex)
        assert boundary is not None
        assert boundary.source_watermark == 3
        assert boundary.watermark_after == 9
        assert boundary.watermark_unchanged is False

    @pytest.mark.asyncio
    async def test_regressed_after_read_degrades_to_an_absent_boundary(self) -> None:
        """A watermark moving backwards fails boundary validation, not the cycle.

        A regressed after-read contradicts the append-only sealed prefix
        (only a scope change or data defect can produce it), so the
        validating constructor rejects the capture and the observer threads
        an honestly absent boundary while the snapshot persists normally.

        Given: A live cycle whose after-read returns a LOWER watermark.
        When: The observation completes.
        Then: The snapshot is recorded, scheduling still runs, and the
            boundary is explicitly absent.
        """
        ex, calls, _ = self._live_executor(AsyncMock(side_effect=[9, 3]))
        await ex._observe_account_once()
        ex.repository.record_venue_account_snapshot.assert_awaited_once()
        assert calls == ["watermark", "balances", "positions", "watermark"]
        assert self._scheduled_boundary(ex) is None

    @pytest.mark.asyncio
    async def test_paper_mode_never_captures_and_threads_no_boundary(self) -> None:
        """Paper accounts observe without any watermark read or boundary.

        Given: A paper-named executor with a SIMULATED balance client.
        When: The observation cycle runs.
        Then: The watermark repository read is never awaited and scheduling
            receives an explicitly absent boundary.
        """
        ex = _make_executor()
        ex._get_exchange_name = MagicMock(return_value=ExchangeEnum.PAPER)
        client = _make_client(CapabilityStatus.SIMULATED, CapabilityStatus.NOT_APPLICABLE)
        client.read_native_balances = AsyncMock(return_value=[])
        ex.exchange_client = client
        ex._schedule_portfolio_reconciliation = MagicMock()
        await ex._observe_account_once()
        ex.repository.get_spot_execution_watermark.assert_not_awaited()
        assert self._scheduled_boundary(ex) is None

    @pytest.mark.asyncio
    async def test_work_item_boundary_reaches_dispatch_unchanged(self) -> None:
        """The captured boundary travels the work item into dispatch verbatim.

        Given: Reconciliation work carrying one captured boundary payload.
        When: The runner evaluates that exact account-state version.
        Then: Dispatch receives the identical boundary object by keyword.
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
        ex.repository.record_portfolio_reconciliation = AsyncMock(return_value=41)
        ex._schedule_portfolio_drift_notification = MagicMock()
        captured_at = datetime(2026, 7, 17, 9, 0, tzinfo=UTC)
        boundary = SpotReplayBoundaryCapture(
            wallet_public_id="wallet-1",
            exchange="kraken_futures",
            mode="live",
            session_id="session-1",
            sequence_id=7,
            source_watermark=7,
            as_of=captured_at - timedelta(milliseconds=1),
            watermark_captured_at=captured_at,
            request_started_at=captured_at + timedelta(milliseconds=1),
            request_completed_at=captured_at + timedelta(milliseconds=2),
            watermark_after=7,
            watermark_after_captured_at=captured_at + timedelta(milliseconds=3),
            watermark_unchanged=True,
        )
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken_futures", "live", "session-1", 7),
            position_capability=CapabilityStatus.SUPPORTED,
            boundary=boundary,
            cursor_capture=None,
            history_capture=None,
        )
        with (
            patch.object(base_module, "build_portfolio_account_state", return_value=object()),
            patch.object(
                base_module,
                "dispatch_portfolio_reconciliation",
                new_callable=AsyncMock,
                return_value=_portfolio_evaluation(),
            ) as dispatch,
        ):
            await ex._run_portfolio_reconciliation(work)
        dispatch_call = dispatch.await_args
        assert dispatch_call is not None
        assert dispatch_call.kwargs["boundary"] is boundary
        assert ex._portfolio_reconciliation_failure_count == 0


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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
        )
        ex._portfolio_reconciliation_dispatch_open = False
        ex._schedule_portfolio_reconciliation(
            state_id=2,
            attempt=_portfolio_attempt(),
            position_capability=CapabilityStatus.SUPPORTED,
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
        )
        ex._schedule_portfolio_reconciliation(
            state_id=2,
            attempt=attempt,
            position_capability=CapabilityStatus.SUPPORTED,
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
        )
        first = tuple(ex._portfolio_reconciliation_tasks.values())[0]
        await first
        ex._schedule_portfolio_reconciliation(
            state_id=2,
            attempt=attempt,
            position_capability=CapabilityStatus.SUPPORTED,
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
                boundary=None,
                cursor_capture=None,
                history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            "boundary": None,
            "history_capture": None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
            boundary=None,
            cursor_capture=None,
            history_capture=None,
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
                    boundary=None,
                    cursor_capture=None,
                    history_capture=None,
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
        evaluation = _portfolio_evaluation()

        with pytest.raises(asyncio.CancelledError):
            await ex._publish_portfolio_drift_notification(evaluation)

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


_ANCHOR_WALLET = "0198f0a1-2b3c-7d4e-8f90-a1b2c3d4e5f6"
_ANCHOR_EVALUATED_AT = datetime(2026, 7, 17, 10, 0, tzinfo=UTC)
_VALID_WITNESSES = WitnessOutcome(witnesses={1: frozenset({100})}, refusals=())


def _anchor_history_item() -> VenueAccountHistoryItem:
    """Return one API-attributed MARKET_FX row for an H0 account-history page."""
    return VenueAccountHistoryItem(
        item_id=490,
        operation_type="MARKET_FX",
        operation_amount=Decimal("100.25"),
        balance_after=Decimal("100.25"),
        currency="EUR",
        transaction_id="tx-1",
        ordered_by="API/key-1",
        order_id="O1",
    )


def _anchor_history_tip(item_id: int = 500) -> VenueAccountHistoryTip:
    """Return an account-history tip carrying a single MARKET_FX page item."""
    return VenueAccountHistoryTip(
        item_id=item_id, items=(_anchor_history_item(),), reached_genesis=True
    )


def _cursor_capture() -> _SpotAnchorCursorCapture:
    """Return one bracketed H0 cursor capture for a bootstrap work item."""
    requested_at = datetime(2026, 7, 17, 9, 58, tzinfo=UTC)
    return _SpotAnchorCursorCapture(
        tip=_anchor_history_tip(),
        requested_at=requested_at,
        observed_at=requested_at + timedelta(milliseconds=1),
    )


def _anchor_boundary() -> SpotReplayBoundaryCapture:
    """Return one valid pre-balance boundary bound to the bootstrap identity."""
    captured_at = datetime(2026, 7, 17, 9, 59, tzinfo=UTC)
    return SpotReplayBoundaryCapture(
        wallet_public_id=_ANCHOR_WALLET,
        exchange="kraken",
        mode="live",
        session_id="session-1",
        sequence_id=7,
        source_watermark=3,
        as_of=captured_at - timedelta(milliseconds=1),
        watermark_captured_at=captured_at,
        request_started_at=captured_at + timedelta(milliseconds=1),
        request_completed_at=captured_at + timedelta(milliseconds=2),
        watermark_after=3,
        watermark_after_captured_at=captured_at + timedelta(milliseconds=3),
        watermark_unchanged=True,
    )


def _anchor_work(
    *,
    boundary: SpotReplayBoundaryCapture | None,
    cursor_capture: _SpotAnchorCursorCapture | None,
) -> _PortfolioReconciliationWork:
    """Return reconciliation work carrying the given cursor capture and boundary."""
    return _PortfolioReconciliationWork(
        state_id=41,
        identity=(_ANCHOR_WALLET, "kraken", "live", "session-1", 7),
        position_capability=CapabilityStatus.SUPPORTED,
        boundary=boundary,
        cursor_capture=cursor_capture,
        history_capture=None,
    )


def _anchor_state_row(**overrides: str | int | None) -> dict[str, str | int | None]:
    """Return one venue account-state version row for the bootstrap read."""
    row: dict[str, str | int | None] = {
        "wallet_public_id": _ANCHOR_WALLET,
        "exchange": "kraken",
        "mode": "live",
        "balance_status": "observed",
        "balances_json": json.dumps(
            [
                {"currency": "EUR", "total_decimal": "100.25", "used_decimal": "0.00"},
                {"currency": "PLN", "total_decimal": "10.00", "used_decimal": "0.00"},
            ]
        ),
        "public_id": "vas-1",
        "balance_payload_source_observation_id": 55,
        "current_attempt_observation_id": 77,
    }
    row.update(overrides)
    return row


def _bootstrap_executor() -> tuple[Any, Any]:
    """Return an executor and client wired for a successful anchor bootstrap.

    Every venue read and repository read on the ten-instant gather path is
    stubbed to its happy-path value; individual tests override exactly one to
    drive a single early return. ``parse_execution_exec_id`` is a synchronous
    ``MagicMock`` (the client method is sync) so its truthy tuple result is not
    a coroutine.
    """
    ex = _make_executor()
    ex.wallet_public_id = _ANCHOR_WALLET
    client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
    client.account_history_capability = CapabilityStatus.SUPPORTED
    client.read_native_balances = AsyncMock(
        return_value=[
            NativeBalanceEntry(
                currency="EUR", total=100.25, free=None, used=None, total_decimal="100.25"
            ),
            NativeBalanceEntry(currency="PLN", total=10.0, free=None, used=None),
        ]
    )
    client.read_account_history_tip = AsyncMock(return_value=_anchor_history_tip(501))
    client.parse_execution_exec_id = MagicMock(return_value=("O1", 600000000, False))
    client.read_order_fill_legs = AsyncMock(
        return_value=VenueOrderFillLegs(
            order_id="O1",
            bought_amount=Decimal("1"),
            sold_amount=Decimal("2"),
            commission_amount=Decimal("0"),
            bought_currency="EUR",
            sold_currency="PLN",
            commission_currency="EUR",
            buy_sell="BUY",
        )
    )
    ex.exchange_client = client
    ex.repository.get_venue_account_state_version = AsyncMock(return_value=_anchor_state_row())
    ex.repository.get_spot_asset_precision_evidence = AsyncMock(return_value={"EUR": object()})
    ex.repository.has_spot_margin_reconciliation_signal = AsyncMock(return_value=False)
    ex.repository.get_spot_execution_chain_tip = AsyncMock(return_value="chain-tip")
    ex.repository.get_spot_execution_witness_rows = AsyncMock(
        return_value=[{"exec_id": "E1", "scope_sequence": 1}]
    )
    ex.repository.record_spot_reconciliation_anchor = AsyncMock(return_value=1)
    return ex, client


class TestCaptureSpotAnchorCursor:
    """The observer's pre-watermark H0 cursor read for unanchored accounts."""

    @pytest.mark.asyncio
    async def test_unsupported_history_capability_skips_capture(self) -> None:
        """A venue without the account-history contract captures no cursor.

        Given: A client whose account-history capability is not SUPPORTED.
        When: The cursor capture runs.
        Then: It returns None without ever reading the anchor state.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.UNSUPPORTED
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=None)
        result = await ex._capture_spot_anchor_cursor(ex.repository, client, "kraken", "live")
        assert result is None
        ex.repository.get_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_already_anchored_account_skips_capture(self) -> None:
        """An account that already holds an anchor captures no cursor.

        Given: A supported client but a persisted anchor row for the scope.
        When: The cursor capture runs.
        Then: It returns None without reading the venue history tip.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.SUPPORTED
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value={"public_id": "a"})
        client.read_account_history_tip = AsyncMock(return_value=_anchor_history_tip())
        result = await ex._capture_spot_anchor_cursor(ex.repository, client, "kraken", "live")
        assert result is None
        client.read_account_history_tip.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_history_read_failure_degrades_to_no_capture(self) -> None:
        """A failing venue tip read degrades to no capture rather than raising.

        Given: An unanchored scope whose account-history read raises.
        When: The cursor capture runs.
        Then: The failure is swallowed and None is returned.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.SUPPORTED
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=None)
        client.read_account_history_tip = AsyncMock(side_effect=RuntimeError("venue down"))
        result = await ex._capture_spot_anchor_cursor(ex.repository, client, "kraken", "live")
        assert result is None

    @pytest.mark.asyncio
    async def test_absent_history_tip_yields_no_capture(self) -> None:
        """A venue reporting no history tip yields no cursor capture.

        Given: An unanchored scope whose account-history tip read returns None.
        When: The cursor capture runs.
        Then: It returns None.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.SUPPORTED
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=None)
        client.read_account_history_tip = AsyncMock(return_value=None)
        result = await ex._capture_spot_anchor_cursor(ex.repository, client, "kraken", "live")
        assert result is None

    @pytest.mark.asyncio
    async def test_successful_capture_brackets_the_tip_read(self) -> None:
        """A successful read brackets the tip between request and observe instants.

        Given: An unanchored scope whose account-history tip read succeeds.
        When: The cursor capture runs.
        Then: It returns the tip with a request instant at or before the observe
            instant, and reads the tip at the anchor history limit.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.SUPPORTED
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=None)
        tip = _anchor_history_tip()
        client.read_account_history_tip = AsyncMock(return_value=tip)
        result = await ex._capture_spot_anchor_cursor(ex.repository, client, "kraken", "live")
        assert result is not None
        assert result.tip is tip
        assert result.requested_at <= result.observed_at
        client.read_account_history_tip.assert_awaited_once_with(
            base_module._SPOT_ANCHOR_HISTORY_LIMIT
        )


class TestMaybeBootstrapSpotAnchor:
    """The bounded, fully-degrading wrapper around one bootstrap attempt."""

    @pytest.mark.asyncio
    async def test_absent_cursor_capture_is_a_noop(self) -> None:
        """Work without a cursor capture attempts no bootstrap.

        Given: Reconciliation work whose cursor capture is None.
        When: The maybe-bootstrap wrapper runs.
        Then: The inner bootstrap is never invoked.
        """
        ex = _make_executor()
        ex._bootstrap_spot_anchor = AsyncMock()
        work = _anchor_work(boundary=_anchor_boundary(), cursor_capture=None)
        await ex._maybe_bootstrap_spot_anchor(work, _ANCHOR_EVALUATED_AT)
        ex._bootstrap_spot_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_absent_boundary_is_a_noop(self) -> None:
        """Work without a boundary attempts no bootstrap.

        Given: Reconciliation work whose boundary is None.
        When: The maybe-bootstrap wrapper runs.
        Then: The inner bootstrap is never invoked.
        """
        ex = _make_executor()
        ex._bootstrap_spot_anchor = AsyncMock()
        work = _anchor_work(boundary=None, cursor_capture=_cursor_capture())
        await ex._maybe_bootstrap_spot_anchor(work, _ANCHOR_EVALUATED_AT)
        ex._bootstrap_spot_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bootstrap_failure_is_caught_and_degraded(self) -> None:
        """A raising bootstrap is caught so the attempt degrades silently.

        Given: Work with both a cursor capture and a boundary and a bootstrap
            that raises.
        When: The maybe-bootstrap wrapper runs.
        Then: No exception propagates and the bootstrap was invoked once.
        """
        ex = _make_executor()
        ex._bootstrap_spot_anchor = AsyncMock(side_effect=RuntimeError("bootstrap blew up"))
        work = _anchor_work(boundary=_anchor_boundary(), cursor_capture=_cursor_capture())
        await ex._maybe_bootstrap_spot_anchor(work, _ANCHOR_EVALUATED_AT)
        ex._bootstrap_spot_anchor.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_bootstrap_success_completes_within_the_bound(self) -> None:
        """A succeeding bootstrap runs to completion inside the timeout bound.

        Given: Work with both a cursor capture and a boundary and a bootstrap
            that succeeds.
        When: The maybe-bootstrap wrapper runs.
        Then: The bootstrap is awaited once with the work and evaluation instant.
        """
        ex = _make_executor()
        ex._bootstrap_spot_anchor = AsyncMock(return_value=None)
        work = _anchor_work(boundary=_anchor_boundary(), cursor_capture=_cursor_capture())
        await ex._maybe_bootstrap_spot_anchor(work, _ANCHOR_EVALUATED_AT)
        ex._bootstrap_spot_anchor.assert_awaited_once_with(work, _ANCHOR_EVALUATED_AT)


class TestBootstrapSpotAnchor:
    """Evidence gathering, certification, and sealing of one bootstrap anchor."""

    @staticmethod
    def _work() -> _PortfolioReconciliationWork:
        """Return bootstrap work carrying a live boundary and cursor capture."""
        return _anchor_work(boundary=_anchor_boundary(), cursor_capture=_cursor_capture())

    @pytest.mark.asyncio
    async def test_absent_exchange_client_returns_without_reads(self) -> None:
        """A missing exchange client seals nothing and reads no state.

        Given: An executor whose exchange client is None.
        When: The bootstrap runs.
        Then: It returns before reading the account-state version.
        """
        ex, _ = _bootstrap_executor()
        ex.exchange_client = None
        await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        ex.repository.get_venue_account_state_version.assert_not_awaited()
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_absent_state_row_returns(self) -> None:
        """A vanished account-state version seals nothing.

        Given: A state-version read returning None.
        When: The bootstrap runs.
        Then: It returns without reading precision evidence.
        """
        ex, _ = _bootstrap_executor()
        ex.repository.get_venue_account_state_version = AsyncMock(return_value=None)
        await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        ex.repository.get_spot_asset_precision_evidence.assert_not_awaited()
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_manual_activity_on_the_window_skips_before_any_read(self) -> None:
        """Manual MARKET_FX activity on the captured page dooms the cycle cheaply.

        Given: A cursor capture whose page carries a non-API MARKET_FX leg.
        When: The bootstrap runs.
        Then: It returns before the state read and the order-legs fan-out — the
            pure refusal is inevitable, so no further egress is spent.
        """
        ex, _ = _bootstrap_executor()
        manual_item = VenueAccountHistoryItem(
            item_id=489,
            operation_type="MARKET_FX",
            operation_amount=Decimal("5"),
            balance_after=Decimal("5"),
            currency="EUR",
            transaction_id="tx-manual",
            ordered_by="",
            order_id=None,
        )
        tip = VenueAccountHistoryTip(
            item_id=500, items=(_anchor_history_item(), manual_item), reached_genesis=True
        )
        capture = _SpotAnchorCursorCapture(
            tip=tip,
            requested_at=datetime(2026, 7, 17, 9, 58, tzinfo=UTC),
            observed_at=datetime(2026, 7, 17, 9, 58, 0, 1000, tzinfo=UTC),
        )
        work = _anchor_work(boundary=_anchor_boundary(), cursor_capture=capture)
        await ex._bootstrap_spot_anchor(work, _ANCHOR_EVALUATED_AT)
        ex.repository.get_venue_account_state_version.assert_not_awaited()
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_absent_balances_json_returns(self) -> None:
        """A state row without a balances payload seals nothing.

        Given: A state row whose balances_json is None.
        When: The bootstrap runs.
        Then: It returns without sealing an anchor.
        """
        ex, _ = _bootstrap_executor()
        ex.repository.get_venue_account_state_version = AsyncMock(
            return_value=_anchor_state_row(balances_json=None)
        )
        await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_parsed_balances_returns(self) -> None:
        """A payload with no venue-raw balances seals nothing.

        Given: A state row whose balances payload parses to no exact balances.
        When: The bootstrap runs.
        Then: It returns before the second (re-read) native-balance call.
        """
        ex, client = _bootstrap_executor()
        ex.repository.get_venue_account_state_version = AsyncMock(
            return_value=_anchor_state_row(balances_json="[]")
        )
        await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        client.read_native_balances.assert_not_awaited()
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_absent_confirming_tip_returns(self) -> None:
        """A missing confirming H1 tip seals nothing.

        Given: A re-read whose account-history tip returns None.
        When: The bootstrap runs.
        Then: It returns before reading precision evidence.
        """
        ex, client = _bootstrap_executor()
        client.read_account_history_tip = AsyncMock(return_value=None)
        await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        ex.repository.get_spot_asset_precision_evidence.assert_not_awaited()
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_witness_row_missing_exec_id_returns(self) -> None:
        """A witness row without an exec id seals nothing.

        Given: A sealed-prefix witness row whose exec_id is None, plus an asset
            missing from the precision evidence to exercise the uncertified arm.
        When: The bootstrap runs.
        Then: It returns before reading order fill legs.
        """
        ex, client = _bootstrap_executor()
        ex.repository.get_spot_execution_witness_rows = AsyncMock(
            return_value=[{"exec_id": None, "scope_sequence": 1}]
        )
        with patch.object(base_module, "is_spot_precision_plane_certified", return_value=True):
            await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        client.read_order_fill_legs.assert_not_awaited()
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_undecodable_exec_id_returns(self) -> None:
        """An exec id the client cannot decode seals nothing.

        Given: A witness row whose exec id decodes to None.
        When: The bootstrap runs.
        Then: It returns before reading order fill legs.
        """
        ex, client = _bootstrap_executor()
        client.parse_execution_exec_id = MagicMock(return_value=None)
        with patch.object(base_module, "is_spot_precision_plane_certified", return_value=True):
            await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        client.read_order_fill_legs.assert_not_awaited()
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_absent_order_fill_legs_returns(self) -> None:
        """A missing per-order fill-legs read seals nothing.

        Given: A decodable prefix whose order fill-legs read returns None.
        When: The bootstrap runs.
        Then: It returns without composing witnesses.
        """
        ex, client = _bootstrap_executor()
        client.read_order_fill_legs = AsyncMock(return_value=None)
        with patch.object(base_module, "is_spot_precision_plane_certified", return_value=True):
            await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_witness_refusals_log_and_return(self) -> None:
        """Unresolved witnesses seal nothing and are logged by name.

        Given: A witness build that returns named refusals and no map.
        When: The bootstrap runs.
        Then: It returns without sealing an anchor.
        """
        ex, _ = _bootstrap_executor()
        with (
            patch.object(base_module, "is_spot_precision_plane_certified", return_value=True),
            patch.object(
                base_module,
                "build_witnesses_from_reads",
                return_value=WitnessOutcome(
                    witnesses={}, refusals=("execution_order_totals_missing",)
                ),
            ),
        ):
            await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_not_certifiable_observation_logs_and_returns(self) -> None:
        """A pure-module refusal seals nothing and is logged by name.

        Given: A composed observation (with a null payload-source observation id,
            so the current-attempt fallback is taken) that the pure builder
            refuses as not certifiable.
        When: The bootstrap runs.
        Then: It returns without sealing an anchor.
        """
        ex, _ = _bootstrap_executor()
        ex.repository.get_venue_account_state_version = AsyncMock(
            return_value=_anchor_state_row(balance_payload_source_observation_id=None)
        )
        with (
            patch.object(base_module, "is_spot_precision_plane_certified", return_value=True),
            patch.object(base_module, "build_witnesses_from_reads", return_value=_VALID_WITNESSES),
            patch.object(
                base_module,
                "build_spot_anchor",
                side_effect=base_module.SpotAnchorNotCertifiableError(
                    ("scope_without_committed_execution",)
                ),
            ),
        ):
            await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        ex.repository.record_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fully_proven_observation_seals_one_anchor(self) -> None:
        """A fully proven observation seals exactly one anchor row.

        Given: Every gather read yields usable data, the witness map composes,
            and the pure builder certifies the observation.
        When: The bootstrap runs.
        Then: The built anchor row is recorded exactly once.
        """
        ex, _ = _bootstrap_executor()
        anchor_row = {"public_id": "anchor-1"}
        with (
            patch.object(base_module, "is_spot_precision_plane_certified", return_value=True),
            patch.object(base_module, "build_witnesses_from_reads", return_value=_VALID_WITNESSES),
            patch.object(base_module, "build_spot_anchor", return_value=anchor_row),
        ):
            await ex._bootstrap_spot_anchor(self._work(), _ANCHOR_EVALUATED_AT)
        ex.repository.record_spot_reconciliation_anchor.assert_awaited_once()
        recorded = ex.repository.record_spot_reconciliation_anchor.await_args
        assert recorded.args[0] == anchor_row


def _history_anchor_row() -> SpotReconciliationAnchorRow:
    """Build one sealed cursor-certified anchor row for the anchored range path."""
    sealed_at = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)
    scheme = "walutomat:api-v2.0.0:account/history:v1"
    return {
        "public_id": "00000000-0000-7000-8000-000000000701",
        "wallet_public_id": _ANCHOR_WALLET,
        "exchange": "kraken",
        "mode": "live",
        "venue_account_state_public_id": "vas-1",
        "balance_observation_id": 55,
        "source_watermark_kind": "scope_sequence",
        "source_watermark": 1,
        "balances_json": '{"EUR":"100"}',
        "first_request_started_at": sealed_at,
        "first_request_completed_at": sealed_at,
        "second_request_started_at": sealed_at,
        "second_request_completed_at": sealed_at,
        "boundary_status": "cursor_certified",
        "inventory_status": "venue_reported_full",
        "margin_status": "cash",
        "provenance": f"spot_anchor_bootstrap:v1:{scheme}:windowed",
        "session_id": "session-1",
        "sequence_id": 1,
        "timestamp": sealed_at,
        "source_chain_tip": "a" * 64,
        "venue_cursor_kind": "account_history_item_id",
        "venue_cursor_scheme": scheme,
        "venue_cursor_value": "100",
        "venue_cursor_requested_at": sealed_at,
        "venue_cursor_observed_at": sealed_at,
        "venue_cursor_confirmed_at": sealed_at,
        "source_watermark_requested_at": sealed_at,
        "source_watermark_captured_at": sealed_at,
    }


def _history_tip_capture() -> _SpotHistoryTipCapture:
    """Return one pre-watermark anchored tip capture at H_anchor=100, H_E=103."""
    return _SpotHistoryTipCapture(anchor=_history_anchor_row(), anchor_item_id=100, tip_item_id=103)


def _history_range_capture() -> SpotHistoryRangeCapture:
    """Return one complete empty-range certificate evidence bundle."""
    return SpotHistoryRangeCapture(
        evidence=HistoryRangeEvidence(tip_item_id=100, confirming_tip_item_id=100, rows=()),
        parsed_executions=(),
        order_totals={},
        venue_balances={"EUR": Decimal("100")},
        anchor_watermark=1,
    )


def _range_history_item(item_id: int) -> VenueAccountHistoryItem:
    """Return one in-range MARKET_FX history row for the range walker result."""
    return VenueAccountHistoryItem(
        item_id=item_id,
        operation_type="MARKET_FX",
        operation_amount=Decimal("6"),
        balance_after=Decimal("106"),
        currency="EUR",
        transaction_id="tx-2",
        ordered_by="API/key-1",
        order_id="O1",
    )


def _range_fill_legs() -> VenueOrderFillLegs:
    """Return the lifetime cumulative fill legs of the one in-range order."""
    return VenueOrderFillLegs(
        order_id="O1",
        bought_amount=Decimal("6"),
        sold_amount=Decimal("30"),
        commission_amount=Decimal("0.02"),
        bought_currency="EUR",
        sold_currency="PLN",
        commission_currency="EUR",
        buy_sell="BUY",
    )


def _range_balances_json() -> str:
    """Return one venue-raw serialized balance payload for the range capture."""
    return json.dumps([{"currency": "EUR", "total_decimal": "106", "used_decimal": "0"}])


def _range_executor() -> tuple[Any, Any, list[str]]:
    """Wire one executor whose anchored range-capture reads log a call order.

    Every read on the evidence path appends a marker to one shared log so the
    tests can prove the physical sequencing (fill legs BEFORE the confirming
    tip, confirming tip BEFORE the range pages) rather than infer it.
    Individual tests override exactly one read to drive a single degrade.
    """
    ex = _make_executor()
    ex.wallet_public_id = _ANCHOR_WALLET
    client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
    client.account_history_capability = CapabilityStatus.SUPPORTED
    calls: list[str] = []

    async def witness_rows(*_args: object, **_kwargs: object) -> list[dict[str, object]]:
        calls.append("witness")
        return [
            {"exec_id": "E1", "scope_sequence": 2},
            {"exec_id": "E2", "scope_sequence": 3},
        ]

    def parse(exec_id: str) -> tuple[str, int, bool]:
        calls.append("parse")
        return ("O1", 600000000, exec_id == "E2")

    async def fill_legs(_order_id: str) -> VenueOrderFillLegs:
        calls.append("legs")
        return _range_fill_legs()

    async def confirming_tip(_limit: int) -> VenueAccountHistoryTip:
        calls.append("confirming")
        return VenueAccountHistoryTip(item_id=103, items=(), reached_genesis=False)

    async def range_read(
        _continue_from: int, _upto_item_id: int
    ) -> tuple[VenueAccountHistoryItem, ...]:
        calls.append("range")
        return (_range_history_item(101),)

    ex.repository.get_spot_execution_witness_rows = AsyncMock(side_effect=witness_rows)
    client.parse_execution_exec_id = MagicMock(side_effect=parse)
    client.read_order_fill_legs = AsyncMock(side_effect=fill_legs)
    client.read_account_history_tip = AsyncMock(side_effect=confirming_tip)
    client.read_account_history_range = AsyncMock(side_effect=range_read)
    ex.exchange_client = client
    return ex, client, calls


class TestCaptureSpotHistoryTip:
    """The observer's pre-watermark H_E tip read for anchored accounts."""

    @pytest.mark.asyncio
    async def test_unsupported_history_capability_skips_capture(self) -> None:
        """A venue without the account-history contract captures no range tip.

        Given: A client whose account-history capability is not SUPPORTED.
        When: The anchored tip capture runs.
        Then: It returns None without ever reading the anchor state.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.UNSUPPORTED
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=_history_anchor_row())
        result = await ex._capture_spot_history_tip(ex.repository, client, "kraken", "live")
        assert result is None
        ex.repository.get_spot_reconciliation_anchor.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unanchored_account_skips_capture(self) -> None:
        """An unanchored account captures no range tip (the bootstrap owns it).

        Given: A supported client but no persisted anchor row for the scope.
        When: The anchored tip capture runs.
        Then: It returns None without reading the venue history tip.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.SUPPORTED
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=None)
        client.read_account_history_tip = AsyncMock(return_value=_anchor_history_tip())
        result = await ex._capture_spot_history_tip(ex.repository, client, "kraken", "live")
        assert result is None
        client.read_account_history_tip.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", ["", "0", "-5", "1o1", "１００"])
    async def test_non_positive_anchor_cursor_value_skips_capture(self, value: str) -> None:
        """An anchor cursor that is not a positive item id yields no capture.

        Given: A persisted anchor whose venue cursor value is empty, zero,
            negative, alphabetic, or unicode digits ``int`` would accept.
        When: The anchored tip capture runs.
        Then: It returns None without reading the venue history tip.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.SUPPORTED
        anchor = _history_anchor_row()
        anchor["venue_cursor_value"] = value
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=anchor)
        client.read_account_history_tip = AsyncMock(return_value=_anchor_history_tip())
        result = await ex._capture_spot_history_tip(ex.repository, client, "kraken", "live")
        assert result is None
        client.read_account_history_tip.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_anchor_read_failure_degrades_to_no_capture(self) -> None:
        """A failing anchor read degrades to no capture rather than raising.

        Given: An anchor state read that raises.
        When: The anchored tip capture runs.
        Then: The failure is swallowed and None is returned.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.SUPPORTED
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        result = await ex._capture_spot_history_tip(ex.repository, client, "kraken", "live")
        assert result is None

    @pytest.mark.asyncio
    async def test_absent_history_tip_yields_no_capture(self) -> None:
        """A venue reporting no faithful tip read yields no capture.

        Given: An anchored scope whose account-history tip read returns None.
        When: The anchored tip capture runs.
        Then: It returns None.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.SUPPORTED
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=_history_anchor_row())
        client.read_account_history_tip = AsyncMock(return_value=None)
        result = await ex._capture_spot_history_tip(ex.repository, client, "kraken", "live")
        assert result is None

    @pytest.mark.asyncio
    async def test_successful_capture_carries_anchor_and_parsed_bounds(self) -> None:
        """A successful read carries the anchor row and both range bounds.

        Given: An anchored scope whose minimal tip read succeeds at item 103.
        When: The anchored tip capture runs.
        Then: It returns the anchor with its parsed H_anchor and the observed
            H_E, and the tip was read at the minimal tip limit.
        """
        ex = _make_executor()
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.SUPPORTED)
        client.account_history_capability = CapabilityStatus.SUPPORTED
        anchor = _history_anchor_row()
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=anchor)
        client.read_account_history_tip = AsyncMock(
            return_value=VenueAccountHistoryTip(item_id=103, items=(), reached_genesis=False)
        )
        result = await ex._capture_spot_history_tip(ex.repository, client, "kraken", "live")
        assert result == _SpotHistoryTipCapture(anchor=anchor, anchor_item_id=100, tip_item_id=103)
        client.read_account_history_tip.assert_awaited_once_with(
            base_module._SPOT_HISTORY_TIP_LIMIT
        )


class TestCaptureSpotHistoryRange:
    """The observer's post-boundary anchored-path evidence gather."""

    @staticmethod
    async def _capture(ex: Any, balances_json: str | None) -> SpotHistoryRangeCapture | None:
        """Run the range capture with the canonical tip capture and boundary."""
        result = await ex._capture_spot_history_range(
            ex.repository,
            ex.exchange_client,
            "kraken",
            "live",
            _history_tip_capture(),
            _anchor_boundary(),
            balances_json,
        )
        assert result is None or isinstance(result, SpotHistoryRangeCapture)
        return result

    @pytest.mark.asyncio
    async def test_happy_path_captures_complete_evidence_in_order(self) -> None:
        """The full evidence gather runs in the certified bracket order.

        Given: An anchored executor whose witness, decode, fill-leg,
            confirming-tip, and range reads all succeed and log a shared order.
        When: The range capture runs.
        Then: The fill legs are read BEFORE the confirming tip, the confirming
            tip before the range pages, the witness read covers exactly
            (W_anchor, W_E], the range read covers exactly (H_anchor, H_E],
            and the capture carries every field verbatim.
        """
        ex, client, calls = _range_executor()
        result = await self._capture(ex, _range_balances_json())
        assert calls == ["witness", "parse", "parse", "legs", "confirming", "range"]
        assert result == SpotHistoryRangeCapture(
            evidence=HistoryRangeEvidence(
                tip_item_id=103,
                confirming_tip_item_id=103,
                rows=(_range_history_item(101),),
            ),
            parsed_executions=((2, "O1", 600000000, False), (3, "O1", 600000000, True)),
            order_totals={"O1": _range_fill_legs()},
            venue_balances={"EUR": Decimal("106")},
            anchor_watermark=1,
        )
        ex.repository.get_spot_execution_witness_rows.assert_awaited_once_with(
            _ANCHOR_WALLET, "kraken", "live", 3, from_watermark=1
        )
        client.read_order_fill_legs.assert_awaited_once_with("O1")
        client.read_account_history_tip.assert_awaited_once_with(
            base_module._SPOT_HISTORY_TIP_LIMIT
        )
        client.read_account_history_range.assert_awaited_once_with(100, 103)

    @pytest.mark.asyncio
    async def test_empty_witness_range_still_captures(self) -> None:
        """A quiescent cycle with no new executions still gathers evidence.

        Given: An empty witness range (W_E reached no new executions).
        When: The range capture runs.
        Then: The capture carries no executions or order totals, no fill-leg
            read happens, and the confirming tip and range reads still run.
        """
        ex, client, calls = _range_executor()
        ex.repository.get_spot_execution_witness_rows = AsyncMock(return_value=[])
        result = await self._capture(ex, _range_balances_json())
        assert result is not None
        assert result.parsed_executions == ()
        assert result.order_totals == {}
        assert calls == ["confirming", "range"]
        client.read_order_fill_legs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_balance_payload_degrades(self) -> None:
        """A cycle without a balance payload gathers no evidence at all.

        Given: A None balances payload (a failed balance read).
        When: The range capture runs.
        Then: It returns None before any witness or venue read.
        """
        ex, _, calls = _range_executor()
        result = await self._capture(ex, None)
        assert result is None
        assert calls == []

    @pytest.mark.asyncio
    async def test_non_venue_raw_balances_degrade(self) -> None:
        """A balance payload without exact decimals gathers no evidence.

        Given: A balances payload whose entries lack the decimal mirrors.
        When: The range capture runs.
        Then: It returns None before any witness or venue read.
        """
        ex, _, calls = _range_executor()
        payload = json.dumps([{"currency": "EUR", "total": 106.0}])
        result = await self._capture(ex, payload)
        assert result is None
        assert calls == []

    @pytest.mark.asyncio
    async def test_witness_read_failure_degrades(self) -> None:
        """A witness-range gap or read failure degrades to no evidence.

        Given: A witness read raising (a purge, tamper, or plain DB failure).
        When: The range capture runs.
        Then: It returns None and no venue read happens.
        """
        ex, client, _ = _range_executor()
        ex.repository.get_spot_execution_witness_rows = AsyncMock(
            side_effect=RuntimeError("range is not contiguous")
        )
        result = await self._capture(ex, _range_balances_json())
        assert result is None
        client.read_order_fill_legs.assert_not_awaited()
        client.read_account_history_range.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_exec_id_degrades_without_decoding(self) -> None:
        """An execution without any exec id cannot be witnessed.

        Given: A witness row whose exec id is empty.
        When: The range capture runs.
        Then: It returns None without invoking the exec-id decoder.
        """
        ex, client, _ = _range_executor()
        ex.repository.get_spot_execution_witness_rows = AsyncMock(
            return_value=[{"exec_id": "", "scope_sequence": 2}]
        )
        result = await self._capture(ex, _range_balances_json())
        assert result is None
        client.parse_execution_exec_id.assert_not_called()
        client.read_order_fill_legs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_undecodable_exec_id_degrades(self) -> None:
        """An exec id outside the venue scheme cannot be witnessed.

        Given: An exec-id decoder returning None for the stored id.
        When: The range capture runs.
        Then: It returns None and no fill-leg read happens.
        """
        ex, client, _ = _range_executor()
        client.parse_execution_exec_id = MagicMock(return_value=None)
        result = await self._capture(ex, _range_balances_json())
        assert result is None
        client.read_order_fill_legs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_order_degrades_before_the_confirming_tip(self) -> None:
        """A venue-unknown order stops the gather before the tip bracket closes.

        Given: A fill-leg read returning None for the in-range order.
        When: The range capture runs.
        Then: It returns None and neither the confirming tip nor the range
            pages are read.
        """
        ex, client, calls = _range_executor()
        client.read_order_fill_legs = AsyncMock(return_value=None)
        result = await self._capture(ex, _range_balances_json())
        assert result is None
        assert "confirming" not in calls
        client.read_account_history_range.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failed_confirming_tip_still_delivers_the_capture(self) -> None:
        """A failed confirming re-read is evidence the certificate refuses by name.

        Given: A confirming tip read returning None.
        When: The range capture runs.
        Then: The capture is still delivered with an absent confirming tip so
            the pure certificate can refuse venue_cursor_unavailable.
        """
        ex, _, _ = _range_executor()
        ex.exchange_client.read_account_history_tip = AsyncMock(return_value=None)
        result = await self._capture(ex, _range_balances_json())
        assert result is not None
        assert result.evidence.confirming_tip_item_id is None

    @pytest.mark.asyncio
    async def test_unfaithful_range_read_degrades(self) -> None:
        """A range walk the venue could not deliver faithfully yields no evidence.

        Given: A range read returning None.
        When: The range capture runs.
        Then: It returns None.
        """
        ex, client, _ = _range_executor()
        client.read_account_history_range = AsyncMock(return_value=None)
        result = await self._capture(ex, _range_balances_json())
        assert result is None

    @pytest.mark.asyncio
    async def test_wedged_venue_read_degrades_within_the_bound(self) -> None:
        """A hung evidence read is cut by the envelope and degrades to None.

        Given: A witness read that never returns and a tiny reconciliation
            envelope.
        When: The range capture runs.
        Then: It returns None instead of wedging the observation.
        """
        ex, _, _ = _range_executor()

        async def blocked(*_args: object, **_kwargs: object) -> list[dict[str, object]]:
            await asyncio.Event().wait()
            return []

        ex.repository.get_spot_execution_witness_rows = AsyncMock(side_effect=blocked)
        with patch.object(base_module, "_PORTFOLIO_RECONCILIATION_TIMEOUT_S", 0.01):
            result = await self._capture(ex, _range_balances_json())
        assert result is None


class TestObserveAccountAnchoredCapture:
    """The observation cycle's anchored-path wiring around the range capture."""

    @staticmethod
    def _anchored_observer() -> tuple[Any, Any, list[str]]:
        """Wire one live anchored executor whose cycle reads log a call order."""
        ex = _make_executor()
        ex.wallet_public_id = _ANCHOR_WALLET
        client = _make_client(CapabilityStatus.SUPPORTED, CapabilityStatus.NOT_APPLICABLE)
        client.account_history_capability = CapabilityStatus.SUPPORTED
        calls: list[str] = []

        async def tip_read(_limit: int) -> VenueAccountHistoryTip:
            calls.append("tip")
            return VenueAccountHistoryTip(item_id=103, items=(), reached_genesis=False)

        async def watermark_read(
            _wallet: str, _exchange: str, _mode: str, as_of: datetime
        ) -> tuple[int, datetime]:
            calls.append("watermark")
            return 3, as_of

        async def read_balances() -> list[NativeBalanceEntry]:
            calls.append("balances")
            return []

        async def record_snapshot(_attempt: VenueAccountAttemptRow) -> int:
            calls.append("record")
            return 41

        client.read_account_history_tip = AsyncMock(side_effect=tip_read)
        client.read_native_balances = AsyncMock(side_effect=read_balances)
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=_history_anchor_row())
        ex.repository.get_spot_execution_watermark = AsyncMock(side_effect=watermark_read)
        ex.repository.record_venue_account_snapshot = AsyncMock(side_effect=record_snapshot)
        ex.exchange_client = client
        ex._schedule_portfolio_reconciliation = MagicMock()
        return ex, client, calls

    @pytest.mark.asyncio
    async def test_anchored_cycle_threads_the_history_capture(self) -> None:
        """An anchored cycle gathers the evidence and schedules it complete.

        Given: An anchored live executor whose pre-watermark tip, watermark,
            and balance reads log a shared order and whose range gather is
            stubbed to a complete capture.
        When: One observation cycle runs.
        Then: The H_E tip is read BEFORE the watermark capture, the range
            gather runs only AFTER the snapshot commit with the exact tip
            capture, boundary, and balance payload, and scheduling receives
            the capture with no bootstrap cursor.
        """
        ex, _, calls = self._anchored_observer()
        capture = _history_range_capture()

        async def range_capture(*_args: object) -> SpotHistoryRangeCapture:
            calls.append("range_capture")
            return capture

        ex._capture_spot_history_range = AsyncMock(side_effect=range_capture)

        await ex._observe_account_once()

        assert calls == ["tip", "watermark", "balances", "watermark", "record", "range_capture"]
        schedule_call = ex._schedule_portfolio_reconciliation.call_args
        assert schedule_call is not None
        assert schedule_call.kwargs["cursor_capture"] is None
        assert schedule_call.kwargs["history_capture"] is capture
        range_args = ex._capture_spot_history_range.await_args.args
        assert range_args[2] == "kraken"
        assert range_args[3] == "live"
        assert range_args[4] == _SpotHistoryTipCapture(
            anchor=_history_anchor_row(), anchor_item_id=100, tip_item_id=103
        )
        assert range_args[5] is schedule_call.kwargs["boundary"]
        assert range_args[6] == "[]"

    @pytest.mark.asyncio
    async def test_anchored_cycle_with_degraded_gather_still_observes(self) -> None:
        """A failing evidence gather costs only the capture, never the snapshot.

        Given: An anchored live executor whose real range gather fails on the
            range-page read.
        When: One observation cycle runs.
        Then: The snapshot is persisted and scheduling receives an explicitly
            absent history capture.
        """
        ex, client, _ = self._anchored_observer()
        client.read_native_balances = AsyncMock(
            return_value=[
                NativeBalanceEntry(
                    currency="EUR",
                    total=106.0,
                    free=None,
                    used=0.0,
                    total_decimal="106",
                    used_decimal="0",
                )
            ]
        )
        ex.repository.get_spot_execution_witness_rows = AsyncMock(return_value=[])
        client.read_account_history_range = AsyncMock(side_effect=RuntimeError("venue down"))

        await ex._observe_account_once()

        ex.repository.record_venue_account_snapshot.assert_awaited_once()
        schedule_call = ex._schedule_portfolio_reconciliation.call_args
        assert schedule_call is not None
        assert schedule_call.kwargs["history_capture"] is None

    @pytest.mark.asyncio
    async def test_anchored_cycle_without_a_boundary_skips_the_gather(self) -> None:
        """No boundary means no certificate evidence can be scoped at all.

        Given: An anchored live executor whose watermark capture fails.
        When: One observation cycle runs.
        Then: The range gather never runs and scheduling receives neither a
            boundary nor a history capture while the snapshot persists.
        """
        ex, _, _ = self._anchored_observer()
        ex.repository.get_spot_execution_watermark = AsyncMock(side_effect=RuntimeError("db down"))
        ex._capture_spot_history_range = AsyncMock()

        await ex._observe_account_once()

        ex._capture_spot_history_range.assert_not_awaited()
        ex.repository.record_venue_account_snapshot.assert_awaited_once()
        schedule_call = ex._schedule_portfolio_reconciliation.call_args
        assert schedule_call is not None
        assert schedule_call.kwargs["boundary"] is None
        assert schedule_call.kwargs["history_capture"] is None

    @pytest.mark.asyncio
    async def test_unanchored_cycle_still_captures_the_bootstrap_cursor(self) -> None:
        """The unanchored bootstrap path is untouched by the anchored gather.

        Given: A live executor with no persisted anchor whose H0 tip read
            succeeds.
        When: One observation cycle runs.
        Then: The bootstrap cursor capture is scheduled exactly as before, the
            history capture is absent, and the single tip read used the
            bootstrap's page limit.
        """
        ex, client, _ = self._anchored_observer()
        ex.repository.get_spot_reconciliation_anchor = AsyncMock(return_value=None)
        tip = _anchor_history_tip()
        client.read_account_history_tip = AsyncMock(return_value=tip)

        await ex._observe_account_once()

        schedule_call = ex._schedule_portfolio_reconciliation.call_args
        assert schedule_call is not None
        cursor_capture = schedule_call.kwargs["cursor_capture"]
        assert isinstance(cursor_capture, _SpotAnchorCursorCapture)
        assert cursor_capture.tip is tip
        assert schedule_call.kwargs["history_capture"] is None
        client.read_account_history_tip.assert_awaited_once_with(
            base_module._SPOT_ANCHOR_HISTORY_LIMIT
        )
        assert ex.repository.get_spot_reconciliation_anchor.await_count == 2

    @pytest.mark.asyncio
    async def test_work_item_history_capture_reaches_dispatch_unchanged(self) -> None:
        """The captured evidence travels the work item into dispatch verbatim.

        Given: Reconciliation work carrying one complete history capture.
        When: The runner evaluates that exact account-state version.
        Then: Dispatch receives the identical capture object by keyword.
        """
        ex = _make_executor()
        ex.repository.has_portfolio_reconciliation_evaluation = AsyncMock(return_value=False)
        ex.repository.get_venue_account_state_version = AsyncMock(
            return_value={
                "wallet_public_id": "wallet-1",
                "exchange": "kraken",
                "mode": "live",
                "session_id": "session-1",
                "sequence_id": 7,
            }
        )
        ex.repository.get_active_portfolio_reconciliation_method_config = AsyncMock(
            return_value=None
        )
        ex.repository.record_portfolio_reconciliation = AsyncMock(return_value=41)
        ex._schedule_portfolio_drift_notification = MagicMock()
        capture = _history_range_capture()
        work = base_module._PortfolioReconciliationWork(
            state_id=9,
            identity=("wallet-1", "kraken", "live", "session-1", 7),
            position_capability=CapabilityStatus.NOT_APPLICABLE,
            boundary=None,
            cursor_capture=None,
            history_capture=capture,
        )
        with (
            patch.object(base_module, "build_portfolio_account_state", return_value=object()),
            patch.object(
                base_module,
                "dispatch_portfolio_reconciliation",
                new_callable=AsyncMock,
                return_value=_portfolio_evaluation(),
            ) as dispatch,
        ):
            await ex._run_portfolio_reconciliation(work)
        dispatch_call = dispatch.await_args
        assert dispatch_call is not None
        assert dispatch_call.kwargs["history_capture"] is capture
        assert ex._portfolio_reconciliation_failure_count == 0
