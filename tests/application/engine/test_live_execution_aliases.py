"""Live checkpoints preserve the same uniquely witnessed aliases as durable replay."""

import json
from collections import OrderedDict
from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from snapper.application.trade.fill_aliases import aliases_cover_same_fills
from snapper.application.trade.fill_aliases import fill_aliases
from snapper.application.trade.fill_aliases import resolve_live_fill_event
from snapper.core.types import FillStatusEnum
from snapper.core.types import TradeSideEnum
from snapper.data.repository_types import VenueEventRow
from snapper.messaging.schemas.data import ExecutionData
from tests.application.engine.test_engine_config import _make_engine
from tests.application.engine.test_seen_execution_aliases import _SHARD
from tests.application.engine.test_seen_execution_aliases import _WALLET
from tests.application.engine.test_seen_execution_aliases import _coord_with_events
from tests.application.engine.test_seen_execution_aliases import _fill


def _live_event(durable: VenueEventRow) -> VenueEventRow:
    """Mirror the published native exec identifier under ExecutionData.trade_id."""
    event = durable.copy()
    event["exec_id"] = None
    event["trade_id"] = durable["exec_id"]
    return event


@pytest.mark.asyncio
@pytest.mark.parametrize("native_ids", [("E-1", "T-1"), ("E-1", None), (None, "T-1"), (None, None)])
async def test_live_alias_resolution_returns_preferred_native_alias(
    monkeypatch: pytest.MonkeyPatch,
    native_ids: tuple[str | None, str | None],
) -> None:
    """Return the alias adopted from durable evidence to the engine dedup caller.

    Given: a uniquely witnessed fill with both native aliases, one alias, or neither.
    When: live alias resolution enriches an event without a published exec identifier.
    Then: its explicit result prefers exec_id and preserves both enriched fields.
    """
    durable = _fill(1)
    durable["exec_id"], durable["trade_id"] = native_ids
    live = _live_event(durable)
    coord = _coord_with_events(monkeypatch, [durable])
    repository = cast(AsyncMock, coord.repository)
    repository.get_fill_venue_events_for_order_identity = AsyncMock(return_value=[durable])

    resolved = await coord._resolve_live_fill_aliases(live)

    assert resolved == (native_ids[0] or native_ids[1])
    assert live["exec_id"] == native_ids[0]
    assert live["trade_id"] == native_ids[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("native_ids", [("E-1", "T-1"), ("E-1", None), (None, "T-1"), (None, None)])
async def test_live_checkpoint_aliases_equal_durable_replay(
    monkeypatch: pytest.MonkeyPatch,
    native_ids: tuple[str | None, str | None],
) -> None:
    """Write every verified alias of the consumed fill without borrowing future fills.

    Given: a live native fill and its durable row precede another fill in the same order.
    When: the coordinator handles the fill, restores the engine and receives a redelivery.
    Then: aliases equal replay, exclude future fills and prevent duplicate engine booking.
    """
    durable = _fill(1)
    durable["exec_id"], durable["trade_id"] = native_ids
    future = _fill(2)
    future["client_order_id"] = durable["client_order_id"]
    future["exchange_order_id"] = durable["exchange_order_id"]
    coord = _coord_with_events(monkeypatch, [durable, future])
    repository = cast(AsyncMock, coord.repository)
    repository.get_fill_venue_events_for_order_identity = AsyncMock(return_value=[durable, future])
    repository.get_consumed_fill_venue_event_id = AsyncMock(return_value=1)
    monkeypatch.setattr(coord, "_project_paired_execution_leg_fill", AsyncMock())
    monkeypatch.setattr(coord, "_sync_position_cycle_on_fill", AsyncMock())
    monkeypatch.setattr(coord, "_persist_position_projection", AsyncMock())
    engine, _ = _make_engine(exchange="kraken")
    engine._shard_key = _SHARD
    engine.wallet_public_id = _WALLET
    coord.trade_service._get_or_create_shard(_SHARD)
    coord._restore_engine_from_shard(engine, _SHARD, "BTC-USD")
    monkeypatch.setattr(coord, "_find_engine_for_fill", Mock(return_value=engine))
    fill = ExecutionData(
        public_id="live-fill",
        timestamp=datetime.now(UTC),
        session_id="s-test",
        sequence_id=1,
        trade_id=durable["exec_id"],
        client_order_id=durable["client_order_id"] or "",
        exchange_order_id=durable["exchange_order_id"],
        exchange="kraken",
        instrument="BTC-USD",
        side=TradeSideEnum.BUY,
        size=0.1,
        price=100.0,
        last_size=0.1,
        last_price=100.0,
        fee=0.01,
        fee_asset="USD",
        status=FillStatusEnum.FILLED,
        executed_at=durable["venue_timestamp"] or durable["timestamp"],
        wallet_public_id=_WALLET,
    )

    await coord._handle_execution_fill("orders.events.kraken.BTC-USD.executed", fill)

    replay = coord.trade_service.project_fill_state_from_events([durable])
    shard = coord.trade_service._shards[_SHARD]
    assert shard.seen_exec_ids == replay["seen_exec_ids"]
    assert "E-2" not in shard.seen_exec_ids
    assert "T-2" not in shard.seen_exec_ids
    persisted = repository.upsert_checkpoint.await_args
    assert persisted is not None
    checkpoint_aliases = json.loads(persisted.args[0]["seen_exec_ids"])
    assert set(checkpoint_aliases) == set(replay["seen_exec_ids"])
    repository.get_fill_venue_events_for_order_identity.assert_awaited_with(
        client_order_id="client-1",
        wallet_public_id=_WALLET,
        mode=engine.mode,
        exchange="kraken",
        exchange_order_id=None,
    )
    repository.get_consumed_fill_venue_event_id.assert_awaited_once_with(
        shard_key=_SHARD,
        client_order_id=fill.client_order_id,
        exec_id=native_ids[0],
        cum_fill_size=fill.size,
    )
    assert engine.position_qty == replay["position_qty"]
    assert engine.portfolio.cash == replay["cash"]
    coord._restore_engine_from_shard(engine, _SHARD, "BTC-USD")

    await coord._handle_execution_fill("orders.events.kraken.BTC-USD.executed", fill)

    assert engine.position_qty == replay["position_qty"]
    assert engine.portfolio.cash == replay["cash"]
    assert repository.upsert_checkpoint.await_count == 1
    assert fill.trade_id == native_ids[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ["alias", "identity", "nullable_duplicate", "fee"])
async def test_live_alias_ambiguity_or_contradiction_refuses(
    monkeypatch: pytest.MonkeyPatch,
    conflict: str,
) -> None:
    """Refuse aliases whose complete durable evidence does not identify one live fill.

    Given: a shared native alias, different live identity, or a conflicting NULL-cum row.
    When: the coordinator attempts to enrich the live shadow event.
    Then: certification fails before any checkpoint or extra alias can be adopted.
    """
    first = _fill(1)
    live = _live_event(first)
    events = [first]
    if conflict == "identity":
        live["instrument"] = "ETH-USD"
    else:
        second = first.copy()
        second["id"] = 2
        second["exchange_order_id"] = "different-order"
        if conflict == "nullable_duplicate":
            second["cum_fill_size"] = None
        elif conflict == "fee":
            second["exchange_order_id"] = first["exchange_order_id"]
            second["fee"] = 0.02
        events.append(second)
    coord = _coord_with_events(monkeypatch, events)
    repository = cast(AsyncMock, coord.repository)
    repository.get_fill_venue_events_for_order_identity = AsyncMock(return_value=events)

    with pytest.raises(ValueError, match="ambiguous execution alias|contradicts"):
        await coord._resolve_live_fill_aliases(live)

    assert live["exec_id"] is None
    assert live["trade_id"] == "E-1"
    assert coord._recovery_certification_failed is True
    repository.upsert_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fill_size", [0.0, 0.1])
async def test_unrecorded_positive_live_fill_refuses(
    monkeypatch: pytest.MonkeyPatch,
    fill_size: float,
) -> None:
    """Refuse a positive fill whose durable identity cannot be proven.

    Given: durable lookup contains no row for a positive fill or zero-delta terminal frame.
    When: the coordinator resolves aliases before checkpointing.
    Then: a positive fill quarantines while a zero-delta terminal frame needs no witness.
    """
    coord = _coord_with_events(monkeypatch, [])
    repository = cast(AsyncMock, coord.repository)
    repository.get_fill_venue_events_for_order_identity = AsyncMock(return_value=[])
    live = _live_event(_fill(1))
    live["fill_size"] = fill_size

    if fill_size > 0.0:
        with pytest.raises(ValueError, match="no durable fill witness"):
            await coord._resolve_live_fill_aliases(live)
    else:
        await coord._resolve_live_fill_aliases(live)

    assert live["exec_id"] is None
    assert live["trade_id"] == "E-1"
    assert coord._recovery_certification_failed is (fill_size > 0.0)
    repository.upsert_checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_durable_lookup_failure_refuses_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Close order admission before a failed durable lookup yields to supervision.

    Given: the live durable lookup raises after the engine has consumed a fill.
    When: alias enrichment propagates the original database error.
    Then: certification already refuses new orders and no aliases or checkpoint change.
    """
    coord = _coord_with_events(monkeypatch, [])
    repository = cast(AsyncMock, coord.repository)
    error = RuntimeError("durable fill lookup failed")
    repository.get_fill_venue_events_for_order_identity = AsyncMock(side_effect=error)
    live = _live_event(_fill(1))
    original = live.copy()

    with pytest.raises(RuntimeError, match="durable fill lookup failed") as raised:
        await coord._resolve_live_fill_aliases(live)

    assert raised.value is error
    assert coord._recovery_certification_failed is True
    assert live == original
    repository.upsert_checkpoint.assert_not_awaited()


def test_fill_alias_proof_checks_complete_history_and_both_sets() -> None:
    """Require witnessed fill equality independently of replay's deduplicated output.

    Given: complete history includes a lifecycle row and a fill absent from both sets.
    When: the proof evaluates incomplete or unknown replay identifiers and id-less fills.
    Then: incomplete evidence refuses and the shared id-less fallback proves equivalence.
    """
    first, second = _fill(1), _fill(2)
    lifecycle = first.copy()
    lifecycle["event_type"] = "order_accepted"
    assert aliases_cover_same_fills(["E-1"], ["T-1"], [lifecycle, first]) is True
    assert aliases_cover_same_fills(["E-1"], ["T-1"], [first, second]) is False
    assert aliases_cover_same_fills(["E-1"], ["unknown"], [first]) is False
    assert aliases_cover_same_fills(OrderedDict(), OrderedDict(), []) is True
    idless = first.copy()
    idless["exec_id"], idless["trade_id"] = None, None
    aliases = fill_aliases(idless)
    assert aliases_cover_same_fills(aliases, aliases, [idless]) is True
    assert resolve_live_fill_event(_live_event(first), []) is None
    assert resolve_live_fill_event(first, []) is None
    assert resolve_live_fill_event(idless, []) is None


def test_live_alias_binding_preserves_cumulative_publish_absorption() -> None:
    """Resolve native aliases when a published delta includes an unpublished predecessor.

    Given: durable incremental quantity and fee differ from the absorbed published delta.
    When: its unique native alias is bound using the exact fill identity and order scope.
    Then: both current-fill aliases are proven without changing either plane's economics.
    """
    durable = _fill(2)
    live = _live_event(durable)
    live["fill_size"] = 0.2
    live["fee"] = 0.02
    original = live.copy()

    resolved = resolve_live_fill_event(live, [_fill(1), durable])

    assert resolved is durable
    assert fill_aliases(resolved) == ("E-2", "T-2")
    assert live == original


def test_idless_publication_requires_one_exact_durable_fill() -> None:
    """Refuse cumulative matching when distinct durable identities remain possible.

    Given: two different native fills share every available id-less publication field.
    When: resolving aliases by stable order, time, price and cumulative size.
    Then: multiple possible fills refuse rather than choosing one alias set.
    """
    first = _fill(1)
    second = first.copy()
    second["exec_id"], second["trade_id"] = "E-other", "T-other"
    live = first.copy()
    live["exec_id"], live["trade_id"] = None, None

    with pytest.raises(ValueError, match="ambiguous execution alias"):
        resolve_live_fill_event(live, [first, second])
