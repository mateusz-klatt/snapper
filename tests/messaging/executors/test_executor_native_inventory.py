"""Verify native inventory consumers preserve missing evidence and never invent recovery authority."""

import asyncio
import base64
import json
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.trade.futures_inventory_consumer import observe_futures_recovery_order
from snapper.application.trade.futures_inventory_consumer import (
    serialize_futures_inventory_positions,
)
from snapper.application.trade.futures_inventory_read import CurrentFuturesInventoryRead
from snapper.application.trade.futures_open_inventory import parse_futures_open_orders
from snapper.application.trade.futures_open_inventory import parse_futures_open_positions
from snapper.core.futures_inventory_read import FuturesInventoryReadOutcome
from snapper.core.futures_inventory_read import FuturesInventoryReadSource
from snapper.core.futures_inventory_read import futures_inventory_attempt_digest
from snapper.core.types import OrderExchange
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import OrderRow
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.executors import base as base_module
from snapper.messaging.executors.base import _ACCOUNT_UNEXPECTED_POSITION_CAPABILITY_MSG
from snapper.messaging.executors.base import ExchangeExecutorService
from tests.core.test_futures_inventory_read import _attempt

_NOW = datetime(2026, 10, 26, 10, tzinfo=UTC)
_VENUE_ID = "59302619-41d2-4f0b-941f-7e7914760ad3"
_EMPTY_ORDERS = b'{"result":"success","serverTime":"2026-10-26T10:00:00Z","openOrders":[]}'
_EMPTY_POSITIONS = b'{"result":"success","serverTime":"2026-10-26T10:00:00Z","openPositions":[]}'


@pytest.fixture(autouse=True)
def _position_symbol_mapping(
    monkeypatch: pytest.MonkeyPatch, mock_settings_for_tests: None
) -> None:
    """Provide an explicit mapped derivative without relying on unrelated seeded symbol rows."""
    mapper = SymbolMapperService.get_instance()
    mapper.load_cache_if_needed()
    monkeypatch.setitem(mapper.kraken_futures_ws_to_native, "PF_XBTUSD", "BTC-USD-PERP")


class _NativeInventoryExecutor(ExchangeExecutorService[KrakenFuturesExchangeClient]):
    """Expose real consumer methods while keeping transport at its explicit unit-test boundary."""

    def _create_exchange_client(self) -> KrakenFuturesExchangeClient:
        """Require test-owned explicit client injection instead of opening a network client."""
        raise RuntimeError("Explicit native inventory client injection required")

    def _get_exchange_name(self) -> OrderExchange:
        """Select the actual Futures branch without changing another venue's behavior."""
        return "kraken_futures"


def _executor() -> tuple[_NativeInventoryExecutor, MagicMock, MagicMock]:
    """Keep real consumer control flow while mocking only its transport/store collaborators."""
    executor = _NativeInventoryExecutor("5a828f85-af47-42df-a925-d5724a86555b")
    client = MagicMock(spec=KrakenFuturesExchangeClient)
    client.position_observation_capability = CapabilityStatus.SUPPORTED
    client.get_orders = AsyncMock()
    client.get_order = AsyncMock()
    client.read_native_positions = AsyncMock()
    repository = MagicMock(spec=SQLAlchemyRepository)
    repository.get_active_orders_for_recovery = AsyncMock(return_value=[_order_row()])
    executor.exchange_client = client
    executor.repository = repository
    return executor, client, repository


def _positions_outcome(wire: bytes) -> FuturesInventoryReadOutcome:
    """Build a coherent public source shape, without claiming SQL or issued transport evidence."""
    attempt = _attempt(purpose="kraken_futures.open_positions", started_at=_NOW)
    return FuturesInventoryReadOutcome(
        attempt_public_id="2c368b86-b0a2-41a2-9f75-7e0662f6d704",
        attempt_content_digest=futures_inventory_attempt_digest(attempt),
        attempt=attempt,
        completed_at=_NOW,
        result=FuturesInventoryReadSource(
            parser_policy="kraken_futures_open_positions_v1",
            body=base64.b64encode(wire).decode("ascii"),
            request_started_at=_NOW,
            observed_at=_NOW,
        ),
    )


def _order_row() -> OrderRow:
    """Supply one real-shaped retained order without inventing a submission admission."""
    return {
        "public_id": "08e046b8-78e1-4a32-b067-29444d974a49",
        "timestamp": _NOW,
        "session_id": "a5ab252c-d06a-443a-9227-2364ee3f56d2",
        "sequence_id": 1,
        "instrument": "BTC-USD-PERP",
        "exchange": "kraken_futures",
        "mode": "live",
        "client_order_id": "a2f5f19e-38a1-4b20-b5fb-4edc43cdd737",
        "exchange_order_id": _VENUE_ID,
        "created_at": _NOW,
        "updated_at": None,
        "side": "buy",
        "order_type": "limit",
        "price": 100.0,
        "size": 1.0,
        "filled_size": 0.0,
        "average_price": None,
        "status": "open",
        "time_in_force": None,
        "error": None,
        "leverage": None,
        "reduce_only": False,
        "wallet_public_id": "5a828f85-af47-42df-a925-d5724a86555b",
        "operator_public_id": None,
        "plan_public_id": None,
    }


def _position_wire(
    *,
    side: str = "long",
    symbol: str = "PF_XBTUSD",
    mark: bytes | None = b"110.5",
    funding: bytes = b"0.25",
) -> bytes:
    """Retain literal numeric JSON operands and optional display-only fields."""
    mark_field = b"" if mark is None else b',"markPrice":' + mark
    return (
        b'{"result":"success","serverTime":"2026-10-26T10:00:00Z","openPositions":[{'
        b'"symbol":'
        + json.dumps(symbol).encode()
        + b',"side":'
        + json.dumps(side).encode()
        + b',"size":1.12345678901234567890123456789,"price":100.25,'
        b'"unrealizedPnl":0.125,"unrealizedFunding":' + funding + mark_field + b"}]}"
    )


def _open_order_wire() -> bytes:
    """Retain one complete inventory row independently of the original command plane."""
    row = (
        b'{"order_id":"'
        + _VENUE_ID.encode()
        + b'","symbol":"PF_XBTUSD","side":"buy","orderType":"lmt",'
        b'"status":"untouched","filledSize":0,"reduceOnly":false,'
        b'"receivedTime":"2026-10-26T10:00:00Z",'
        b'"lastUpdateTime":"2026-10-26T10:00:00Z"}'
    )
    return _EMPTY_ORDERS.replace(b"[]", b"[" + row + b"]")


def test_unavailable_inventory_never_becomes_an_empty_book() -> None:
    """Given missing evidence, when recovery observes it, then the actual order remains unavailable.

    Given: One wallet-fenced durable order and no qualified complete inventory.
    When: The recovery adapter records its current knowledge.
    Then: Original identities remain and no absence or submission authority is inferred.
    """
    observation = observe_futures_recovery_order(_order_row(), None, "latest_attempt_pending")
    assert observation.presence == "unavailable"
    assert observation.reason == "latest_attempt_pending"
    assert observation.order_public_id == _order_row()["public_id"]
    assert observation.client_order_id == _order_row()["client_order_id"]
    assert observation.venue_order_id == _VENUE_ID


@pytest.mark.parametrize("present", (False, True))
def test_observed_order_presence_does_not_invent_original_submission_context(present: bool) -> None:
    """Given qualified inventory, when an order is checked, then its original context stays unproven.

    Given: A complete observed empty or matching open-order array and an actual durable order.
    When: Recovery correlates only the literal venue identifier.
    Then: Presence is honest and neither absence nor presence grants financial recovery.
    """
    inventory = parse_futures_open_orders(_open_order_wire() if present else _EMPTY_ORDERS)
    observation = observe_futures_recovery_order(_order_row(), inventory, "unused")
    assert observation.presence == ("present" if present else "absent")
    assert observation.reason == "original_submission_context_unavailable"
    assert not hasattr(observation, "request")
    assert not hasattr(observation, "admission")


@pytest.mark.parametrize("venue_id", (None, ""))
@pytest.mark.parametrize("present", (False, True))
def test_missing_original_venue_identity_cannot_prove_absence(
    venue_id: str | None, present: bool
) -> None:
    """Given no original venue identity, when inventory is compared, then absence stays unproven.

    Given: An actual durable order without an assigned venue order ID and a complete observed book.
    When: The recovery observer considers either an empty book or an unrelated observed order.
    Then: It reports unavailable rather than inventing an identity or declaring the order absent.
    """
    order = _order_row()
    order["exchange_order_id"] = venue_id
    inventory = parse_futures_open_orders(_open_order_wire() if present else _EMPTY_ORDERS)
    observation = observe_futures_recovery_order(order, inventory, "unused")
    assert observation.presence == "unavailable"
    assert observation.reason == "venue_order_identity_unavailable"
    assert observation.venue_order_id == venue_id


def test_genuinely_empty_positions_are_representable_without_a_mark() -> None:
    """Given an observed empty array, when projected, then the legacy presentation is honestly empty.

    Given: Actual original successful empty positions bytes and their complete parsed snapshot.
    When: The narrow observer presentation consumes those same retained bytes.
    Then: It returns an empty JSON array, without requiring invented position operands.
    """
    snapshot = parse_futures_open_positions(_EMPTY_POSITIONS)
    assert serialize_futures_inventory_positions(snapshot, _EMPTY_POSITIONS, _NOW) == "[]"


@pytest.mark.parametrize("side,expected", (("long", "buy"), ("short", "sell")))
def test_positions_project_actual_operands_at_the_display_boundary(
    side: str, expected: str
) -> None:
    """Given representable retained positions, when projected, then exact source stays independent.

    Given: Original high-precision size and actually supplied mark and funding operands.
    When: A qualified observation enters the legacy display adapter.
    Then: Only the presentation uses floats, source precision and original observation time remain.
    """
    wire = _position_wire(side=side)
    snapshot = parse_futures_open_positions(wire)
    payload = json.loads(serialize_futures_inventory_positions(snapshot, wire, _NOW))
    assert payload == [
        {
            "symbol": "BTC-USD-PERP",
            "side": expected,
            "size": 1.1234567890123457,
            "entry_price": 100.25,
            "mark_price": 110.5,
            "unrealized_pnl": 0.125,
            "unrealized_funding": 0.25,
            "timestamp": _NOW.isoformat(),
        }
    ]
    assert str(snapshot.positions[0].size) == "1.12345678901234567890123456789"


@pytest.mark.parametrize("mark", (None, b"null", b"true", b'"110.5"', b"1e999", b"1e-999"))
def test_missing_or_unrepresentable_mark_is_not_fabricated(mark: bytes | None) -> None:
    """Given an unrepresentable mark, when projecting, then unavailable is explicit.

    Given: Valid retained inventory whose narrower display mark is absent or invalid.
    When: The observer projection requests its required legacy fields.
    Then: It refuses instead of inventing a mark, a finite zero or an empty book.
    """
    wire = _position_wire(mark=mark)
    snapshot = parse_futures_open_positions(wire)
    with pytest.raises(ValueError, match="futures_inventory_display_numeric_"):
        serialize_futures_inventory_positions(snapshot, wire, _NOW)


def test_explicit_null_funding_does_not_become_zero() -> None:
    """Given documented null funding, when projecting, then the missing display operand is explicit.

    Given: A real-shaped position whose original funding is null.
    When: The narrower legacy account display is requested.
    Then: A missing operand refuses projection without changing retained inventory.
    """
    wire = _position_wire(funding=b"null")
    snapshot = parse_futures_open_positions(wire)
    with pytest.raises(ValueError, match="futures_inventory_display_numeric_unavailable"):
        serialize_futures_inventory_positions(snapshot, wire, _NOW)
    assert snapshot.positions[0].unrealized_funding is None


@pytest.mark.parametrize("side,symbol", (("unknown", "PF_XBTUSD"), ("long", "UNMAPPED")))
def test_unsupported_position_presentation_is_explicit(side: str, symbol: str) -> None:
    """Given literal unsupported identifiers, when projecting, then no guessed side or symbol appears.

    Given: Valid complete inventory containing unsupported display-side or symbol vocabulary.
    When: The legacy observer tries to represent it.
    Then: Projection refuses and does not filter an actual position out of the book.
    """
    wire = _position_wire(side=side, symbol=symbol)
    with pytest.raises(ValueError):
        serialize_futures_inventory_positions(parse_futures_open_positions(wire), wire, _NOW)


def test_other_original_position_bytes_cannot_supply_a_missing_mark() -> None:
    """Given different original inventory, when substituted, then the presentation refuses it.

    Given: Two complete parsed source responses with distinct original funding observations.
    When: A caller tries to pair one snapshot with the other's retained entity.
    Then: Source mismatch refuses projection before reading additional display operands.
    """
    wire = _position_wire()
    different_wire = _position_wire(funding=b"0.5")
    with pytest.raises(ValueError, match="futures_inventory_display_source_mismatch"):
        serialize_futures_inventory_positions(
            parse_futures_open_positions(wire), different_wire, _NOW
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("wire", (None, _EMPTY_ORDERS, _open_order_wire()))
async def test_startup_parks_actual_orders_without_legacy_lookup_or_adoption(
    monkeypatch: pytest.MonkeyPatch, wire: bytes | None
) -> None:
    """Given Futures startup, when qualified inventory is read, then missing context stays parked.

    Given: Actual executor recovery control flow and a durable active order without admission context.
    When: Qualified inventory is unavailable, honestly empty, or contains the literal venue order.
    Then: The original order stays parked, without SDK fallback or synthetic pending request.
    """
    executor, client, repository = _executor()
    snapshot = None if wire is None else parse_futures_open_orders(wire)
    current = CurrentFuturesInventoryRead(
        snapshot, None, "pending" if wire is None else "available"
    )
    refresh = AsyncMock(return_value=current)
    monkeypatch.setattr(base_module, "refresh_current_futures_inventory", refresh)
    await executor._recover_pending_orders("kraken_futures")
    parked = executor._native_inventory_parked[_order_row()["public_id"]]
    assert parked.presence == (
        "unavailable" if wire is None else "absent" if wire == _EMPTY_ORDERS else "present"
    )
    assert not executor.pending_orders
    client.get_orders.assert_not_called()
    client.get_order.assert_not_called()
    refresh.assert_awaited_once_with(
        repository, client, "kraken_futures.open_orders", executor._tracker
    )
    repository.get_active_orders_for_recovery.assert_awaited_once_with(
        exchange="kraken_futures",
        as_of=repository.get_active_orders_for_recovery.await_args.kwargs["as_of"],
        wallet_public_id=executor.wallet_public_id,
    )


@pytest.mark.asyncio
async def test_steady_reconciliation_uses_inventory_without_financial_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given Futures reconciliation, when inventory is available, then legacy recovery stays inactive.

    Given: An observed empty qualified book and a durable active order without its selected context.
    When: The actual unlocked reconciliation cycle runs.
    Then: It updates parked observation and skips legacy queries, adoption and corrective processing.
    """
    executor, client, _repository = _executor()
    current = CurrentFuturesInventoryRead(
        parse_futures_open_orders(_EMPTY_ORDERS), None, "available"
    )
    refresh = AsyncMock(return_value=current)
    financial_retry = AsyncMock()
    ghost_adoption = AsyncMock()
    ambiguous_recovery = AsyncMock()
    monkeypatch.setattr(base_module, "refresh_current_futures_inventory", refresh)
    monkeypatch.setattr(executor, "_retry_pending_reconciliation_work", financial_retry)
    monkeypatch.setattr(executor, "_adopt_ghost_orders", ghost_adoption)
    monkeypatch.setattr(executor, "_resolve_ambiguous_reconciliation_batch", ambiguous_recovery)
    await executor._reconcile_with_exchange_unlocked()
    assert executor._native_inventory_parked[_order_row()["public_id"]].presence == "absent"
    refresh.assert_awaited_once()
    client.get_orders.assert_not_called()
    client.get_order.assert_not_called()
    financial_retry.assert_not_called()
    ghost_adoption.assert_not_called()
    ambiguous_recovery.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("wire", (None, _EMPTY_ORDERS))
async def test_empty_durable_recovery_set_clears_old_parked_entries_without_inventing_orders(
    monkeypatch: pytest.MonkeyPatch, wire: bytes | None
) -> None:
    """Given an honestly empty durable set, when reconciled, then outdated parked identities clear.

    Given: An actual retained order was previously observed and parked without financial authority.
    When: The durable query later proves no active rows, with available or unavailable inventory.
    Then: Prior parked entries clear, no new orders appear and legacy financial recovery stays unused.
    """
    executor, client, repository = _executor()
    initial = CurrentFuturesInventoryRead(
        parse_futures_open_orders(_open_order_wire()), None, "available"
    )
    current = CurrentFuturesInventoryRead(
        None if wire is None else parse_futures_open_orders(wire),
        None,
        "pending" if wire is None else "available",
    )
    refresh = AsyncMock(side_effect=[initial, current])
    financial_retry, ghost_adoption, financial_execution = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(base_module, "refresh_current_futures_inventory", refresh)
    monkeypatch.setattr(executor, "_retry_pending_reconciliation_work", financial_retry)
    monkeypatch.setattr(executor, "_adopt_ghost_orders", ghost_adoption)
    monkeypatch.setattr(executor, "_process_execution", financial_execution)
    await executor._recover_pending_orders("kraken_futures")
    assert executor._native_inventory_parked[_order_row()["public_id"]].presence == "present"
    repository.get_active_orders_for_recovery.return_value = []
    await executor._reconcile_with_exchange_unlocked()
    assert not executor._native_inventory_parked
    assert not executor.pending_orders
    assert repository.get_active_orders_for_recovery.await_count == 2
    client.get_orders.assert_not_called()
    client.get_order.assert_not_called()
    financial_retry.assert_not_called()
    ghost_adoption.assert_not_called()
    financial_execution.assert_not_called()


@pytest.mark.asyncio
async def test_failed_inventory_read_preserves_explicit_unavailability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given a failed inventory read, when startup recovers, then the actual row is parked unavailable.

    Given: A current inventory collaborator fails before a qualified result is returned.
    When: Futures startup runs its real recovery method.
    Then: The failure stays unavailable, no legacy call verifies absence and startup does not fail.
    """
    executor, client, _repository = _executor()
    refresh = AsyncMock(side_effect=ValueError("Synthetic inventory read failure"))
    monkeypatch.setattr(base_module, "refresh_current_futures_inventory", refresh)
    await executor._recover_pending_orders("kraken_futures")
    parked = executor._native_inventory_parked[_order_row()["public_id"]]
    assert parked.presence == "unavailable"
    assert parked.reason == "inventory_read_unavailable"
    client.get_order.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("wire", (_EMPTY_ORDERS, _open_order_wire()))
async def test_failed_durable_order_query_does_not_erase_parked_knowledge(
    monkeypatch: pytest.MonkeyPatch, wire: bytes
) -> None:
    """Given an unreadable durable query, when refreshed, then prior statuses become unavailable.

    Given: A previous present or absent observation exists for an actual retained order.
    When: The following recovery query fails instead of proving an empty durable order set.
    Then: The actual identity remains parked with explicitly invalidated inventory knowledge.
    """
    executor, client, repository = _executor()
    current = CurrentFuturesInventoryRead(parse_futures_open_orders(wire), None, "available")
    monkeypatch.setattr(
        base_module, "refresh_current_futures_inventory", AsyncMock(return_value=current)
    )
    await executor._recover_pending_orders("kraken_futures")
    previous = dict(executor._native_inventory_parked)
    repository.get_active_orders_for_recovery.side_effect = RuntimeError("Synthetic store failure")
    await executor._recover_pending_orders("kraken_futures")
    assert executor._native_inventory_parked.keys() == previous.keys()
    updated = executor._native_inventory_parked[_order_row()["public_id"]]
    assert updated.presence == "unavailable"
    assert updated.reason == "durable_orders_unavailable"
    assert updated.venue_order_id == previous[_order_row()["public_id"]].venue_order_id
    client.get_order.assert_not_called()


@pytest.mark.asyncio
async def test_nonpersistent_repository_cannot_request_qualified_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given missing persistence, when inventory is requested, then no unretained SDK read runs.

    Given: The actual Futures consumer has no persistent repository prerequisite.
    When: Its bounded inventory helper and direct recovery path are called.
    Then: It reports unavailable without network I/O or fabricated durable observations.
    """
    executor, client, _repository = _executor()
    executor.repository = None
    refresh = AsyncMock()
    monkeypatch.setattr(base_module, "refresh_current_futures_inventory", refresh)
    current = await executor._current_native_inventory(client, "kraken_futures.open_orders")
    assert current.reason == "inventory_repository_unavailable"
    assert not current.available
    await executor._recover_native_inventory_orders(client, "kraken_futures")
    refresh.assert_not_called()
    assert not executor._native_inventory_parked


@pytest.mark.asyncio
@pytest.mark.parametrize("wire", (_EMPTY_POSITIONS, _position_wire()))
async def test_account_observer_consumes_retained_inventory_with_original_source_clock(
    monkeypatch: pytest.MonkeyPatch, wire: bytes
) -> None:
    """Given current positions, when the actual observer reads them, then source age is unchanged.

    Given: A qualified original source and the typed snapshot returned by the current runtime.
    When: Futures account observation runs at a later display clock.
    Then: It uses the retained inventory, returns genuine content and keeps the original observation.
    """
    executor, client, repository = _executor()
    snapshot = parse_futures_open_positions(wire)
    current = CurrentFuturesInventoryRead(snapshot, _positions_outcome(wire), "available")
    refresh = AsyncMock(return_value=current)
    monkeypatch.setattr(base_module, "refresh_current_futures_inventory", refresh)
    result = await executor._read_account_positions(client, _NOW.replace(hour=11))
    assert result == (
        "observed",
        serialize_futures_inventory_positions(snapshot, wire, _NOW),
        _NOW,
        None,
    )
    refresh.assert_awaited_once_with(
        repository, client, "kraken_futures.open_positions", executor._tracker
    )
    client.read_native_positions.assert_not_called()


@pytest.mark.asyncio
async def test_unavailable_positions_remain_an_explicit_component_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given failed or pending positions, when observed, then no empty book is emitted.

    Given: Explicit current inventory unavailability for the independent positions endpoint.
    When: The actual Futures account position component reads it.
    Then: Payload and source clock stay absent and the fixed availability reason remains.
    """
    executor, client, _repository = _executor()
    monkeypatch.setattr(
        base_module,
        "refresh_current_futures_inventory",
        AsyncMock(return_value=CurrentFuturesInventoryRead(None, None, "pending")),
    )
    assert await executor._read_account_positions(client, _NOW) == ("error", None, None, "pending")
    client.read_native_positions.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("wrong_endpoint", "missing_source", "missing_mark"))
async def test_unrepresentable_position_view_does_not_become_an_observed_empty_book(
    monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    """Given unusable display evidence, when observed, then the component explicitly refuses it.

    Given: A mismatched endpoint, absent original source or genuinely missing required display mark.
    When: The actual observer requests a legacy position presentation.
    Then: It reports a fixed unavailable presentation code without SDK fallback or empty success.
    """
    executor, client, _repository = _executor()
    wire = _position_wire(mark=None if case == "missing_mark" else b"110.5")
    snapshot = (
        parse_futures_open_orders(_EMPTY_ORDERS)
        if case == "wrong_endpoint"
        else parse_futures_open_positions(wire)
    )
    current = CurrentFuturesInventoryRead(
        snapshot, None if case == "missing_source" else _positions_outcome(wire), "available"
    )
    monkeypatch.setattr(
        base_module, "refresh_current_futures_inventory", AsyncMock(return_value=current)
    )
    assert await executor._read_account_positions(client, _NOW) == (
        "error",
        None,
        None,
        "inventory_positions_presentation_unavailable",
    )
    client.read_native_positions.assert_not_called()


@pytest.mark.asyncio
async def test_unexpected_futures_capability_never_invokes_legacy_positions() -> None:
    """Given an unsupported capability claim, when observed, then native lookup is refused.

    Given: A Futures client incorrectly claims a simulated position component.
    When: The actual account observer checks the explicit capability.
    Then: It returns the established component error without requesting unretained positions.
    """
    executor, client, _repository = _executor()
    client.position_observation_capability = CapabilityStatus.SIMULATED
    assert await executor._read_account_positions(client, _NOW) == (
        "error",
        None,
        None,
        _ACCOUNT_UNEXPECTED_POSITION_CAPABILITY_MSG,
    )
    client.read_native_positions.assert_not_called()


@pytest.mark.asyncio
async def test_native_inventory_consumer_preserves_caller_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given real caller cancellation, when inventory is awaited, then it is never availability data.

    Given: The root-owned source lifecycle propagates its original cancellation after cleanup.
    When: The actual account observer awaits the qualified inventory read.
    Then: Cancellation escapes instead of becoming an error row or an empty position book.
    """
    executor, client, _repository = _executor()
    monkeypatch.setattr(
        base_module,
        "refresh_current_futures_inventory",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    with pytest.raises(asyncio.CancelledError):
        await executor._read_account_positions(client, _NOW)
    client.read_native_positions.assert_not_called()
