"""Tests for the venue account-truth observer on the executor base (PnL Phase 3).

Covers the account-observer surface added to ``ExchangeExecutorService``:
serialization of native balances/positions, the paper/live mode derivation,
the fail-closed capability/outcome mapping of the balance and position reads,
the single-cycle snapshot assembly, and the supervised observer loop.
"""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.core.types import ExchangeEnum
from snapper.data.repository import SQLAlchemyRepository
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
    return ex


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
            NativeBalanceEntry(currency="USD", total=100.0, free=60.0, used=40.0),
            NativeBalanceEntry(currency="XBT", total=1.5, free=None, used=None),
        ]
        payload = json.loads(ExchangeExecutorService._serialize_native_balances(entries))
        assert payload == [
            {"currency": "USD", "total": 100.0, "free": 60.0, "used": 40.0},
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
