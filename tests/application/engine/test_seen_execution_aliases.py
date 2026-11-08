"""Startup certification reconciles only uniquely witnessed execution aliases."""

import json
from collections import OrderedDict
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.trader import TraderCoordinator
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import VenueEventRow
from tests.application.engine.test_checkpoint_recovery import _make_checkpoint
from tests.application.engine.test_checkpoint_recovery import _make_coord
from tests.application.engine.test_checkpoint_recovery import _make_venue_event
from tests.application.engine.test_checkpoint_recovery import _set_sqlalchemy_repo

_SHARD = "kraken.BTC-USD.live"
_WALLET = "w-1"


def _fill(index: int) -> VenueEventRow:
    """Build one uniquely identified, wallet-attributed durable fill."""
    event = _make_venue_event(
        event_id=index,
        fill_size=0.1,
        fill_price=100.0,
        exec_id=f"E-{index}",
        trade_id=f"T-{index}",
    )
    event["wallet_public_id"] = _WALLET
    event["client_order_id"] = f"client-{index}"
    event["exchange_order_id"] = f"order-{index}"
    event["venue_timestamp"] = datetime(2024, 6, 1, tzinfo=UTC) + timedelta(seconds=index)
    return event


def _coord_with_events(
    monkeypatch: pytest.MonkeyPatch,
    events: list[VenueEventRow],
) -> TraderCoordinator:
    """Provide exact-shard history and certain cash and accrual evidence."""
    coord = _make_coord(monkeypatch)
    repository = AsyncMock(spec=SQLAlchemyRepository)
    _set_sqlalchemy_repo(coord, repository)
    repository.get_venue_events_after = AsyncMock(return_value=events)
    repository.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
    repository.get_accruals = AsyncMock(return_value=[])
    repository.shard_has_any_accruals = AsyncMock(return_value=False)
    coord._projection_identities[_SHARD] = ("inst-pid", "live", _WALLET)
    return coord


def _checkpoint_for_events(
    coord: TraderCoordinator,
    events: list[VenueEventRow],
    aliases: list[str],
) -> TradeProjectionCheckpointRow:
    """Build a checkpoint with exact replay economics and selected aliases."""
    projection = coord.trade_service.project_fill_state_from_events(events)
    checkpoint = _make_checkpoint(
        position_qty=projection["position_qty"],
        entry_price=projection["entry_price"],
        position_opened_at=projection["position_opened_at"],
        cash=projection["cash"],
        realized_pnl=projection["realized_pnl"],
        turnover=projection["turnover"],
        last_venue_event_id=projection["last_venue_event_id"],
        seen_exec_ids=json.dumps(aliases),
    )
    checkpoint["wallet_public_id"] = _WALLET
    return checkpoint


@pytest.mark.asyncio
async def test_legacy_checkpoint_adopts_verified_aliases_before_engine_restore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normalize a six-fill checkpoint before initializing engine deduplication.

    Given: ten legacy aliases and twelve durable aliases describe the same six fills.
    When: startup recovers the exact checkpoint and certifies its fill evidence.
    Then: the shard and engine adopt all twelve aliases before engine restoration.
    """
    events = [_fill(index) for index in range(1, 7)]
    coord = _coord_with_events(monkeypatch, events)
    legacy_aliases = [f"E-{index}" for index in range(1, 7)] + [
        f"T-{index}" for index in range(1, 5)
    ]
    checkpoint = _checkpoint_for_events(coord, events, legacy_aliases)
    repository = cast(AsyncMock, coord.repository)
    repository.get_venue_events_after = AsyncMock(side_effect=[[], events])
    expected = {f"{prefix}-{index}" for prefix in ("E", "T") for index in range(1, 7)}
    observed: list[set[str]] = []
    restore_engine = coord._restore_engine_from_shard

    def restore_verified_engine(
        engine: TradingEngineService,
        shard_key: str,
        instrument: str,
    ) -> None:
        """Capture verified aliases at the engine dedup initialization boundary."""
        observed.append(set(coord.trade_service._shards[shard_key].seen_exec_ids))
        restore_engine(engine, shard_key, instrument)

    monkeypatch.setattr(coord, "_restore_engine_from_shard", restore_verified_engine)

    engine_key = await coord._recover_checkpoint_row(checkpoint, datetime.now(UTC))

    assert len(legacy_aliases) == 10
    assert len(expected) == 12
    assert observed == [expected]
    assert engine_key is not None
    assert set(coord.engines[engine_key].seen_exec_ids) == expected
    assert _SHARD in coord._trusted_recovery_shards
    assert coord._recovery_certification_failed is False


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_aliases", [["E-1"], ["E-1", "T-1"]])
async def test_shared_alias_between_same_economics_fills_refuses(
    monkeypatch: pytest.MonkeyPatch,
    legacy_aliases: list[str],
) -> None:
    """Require one durable fill per alias even when replay suppresses a collision.

    Given: two distinct native fills share one trade alias and have equal economics.
    When: checkpoint certification compares their durable evidence.
    Then: ambiguity refuses certification and preserves the restored alias set.
    """
    first = _fill(1)
    second = _fill(2)
    second["trade_id"] = first["trade_id"]
    events = [first, second]
    coord = _coord_with_events(monkeypatch, events)
    checkpoint = _checkpoint_for_events(coord, events, legacy_aliases)
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    aliases = coord.trade_service._shards[_SHARD].seen_exec_ids

    certain = await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)

    assert certain is False
    assert coord.trade_service._shards[_SHARD].seen_exec_ids is aliases
    assert list(aliases) == legacy_aliases


@pytest.mark.asyncio
async def test_alias_equivalence_does_not_adopt_economic_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep exact economic comparison after proving identifier equivalence.

    Given: a native-only alias covers the durable fill but position differs by 5e-10.
    When: startup checks the checkpoint against durable replay.
    Then: the economic mismatch refuses certification without adopting extra aliases.
    """
    events = [_fill(1)]
    coord = _coord_with_events(monkeypatch, events)
    checkpoint = _checkpoint_for_events(coord, events, ["E-1"])
    checkpoint["position_qty"] += 5e-10
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    aliases = coord.trade_service._shards[_SHARD].seen_exec_ids

    certain = await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)

    assert certain is False
    assert coord.trade_service._shards[_SHARD].seen_exec_ids is aliases
    assert list(aliases) == ["E-1"]


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_aliases", [["E-1", "E-2", "unproven"], ["E-1"]])
async def test_unknown_alias_or_missing_fill_never_certifies(
    monkeypatch: pytest.MonkeyPatch,
    legacy_aliases: list[str],
) -> None:
    """Require known identifiers and equality of the complete represented fill sets.

    Given: exact economics accompany an unknown alias or omit an entire durable fill.
    When: startup certifies the checkpoint identity set.
    Then: the incomplete proof refuses certification without modifying restored aliases.
    """
    events = [_fill(1), _fill(2)]
    coord = _coord_with_events(monkeypatch, events)
    checkpoint = _checkpoint_for_events(coord, events, legacy_aliases)
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    aliases = coord.trade_service._shards[_SHARD].seen_exec_ids

    certain = await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)

    assert certain is False
    assert coord.trade_service._shards[_SHARD].seen_exec_ids is aliases
    assert list(aliases) == legacy_aliases


@pytest.mark.asyncio
async def test_duplicate_witness_preserves_unique_fill_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow repeated observations of the same fully matching native fill.

    Given: the same fill is recorded twice with different observation metadata.
    When: startup proves a legacy alias belongs to exactly one fill.
    Then: the redelivery does not cause ambiguity and both native aliases are adopted.
    """
    first = _fill(1)
    duplicate = first.copy()
    duplicate["id"] = 2
    duplicate["public_id"] = "ve-redelivery"
    duplicate["sequence_id"] = 2
    duplicate["received_at"] += timedelta(seconds=1)
    duplicate["timestamp"] += timedelta(seconds=1)
    events = [first, duplicate]
    coord = _coord_with_events(monkeypatch, events)
    checkpoint = _checkpoint_for_events(coord, events, ["E-1"])
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])

    certain = await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)

    assert certain is True
    assert coord.trade_service._shards[_SHARD].seen_exec_ids == OrderedDict.fromkeys(["E-1", "T-1"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "conflict",
    ["client_order", "exchange_order", "fee", "fee_asset", "timestamp", "cumulative_size"],
)
async def test_same_aliases_with_conflicting_fill_witness_refuse(
    monkeypatch: pytest.MonkeyPatch,
    conflict: str,
) -> None:
    """Reject incompatible witnesses hidden by either-key replay deduplication.

    Given: repeated native aliases carry conflicting order anchors, fees, time, or size.
    When: startup checks aliases against all durable rows before replay deduplication.
    Then: the alias proof refuses even though replay alone would retain the first row.
    """
    first = _fill(1)
    duplicate = first.copy()
    duplicate["id"] = 2
    if conflict == "client_order":
        duplicate["client_order_id"] = "other-client"
    elif conflict == "exchange_order":
        duplicate["exchange_order_id"] = "other-order"
    elif conflict == "fee":
        duplicate["fee"] = 0.02
    elif conflict == "fee_asset":
        duplicate["fee_asset"] = "EUR"
    elif conflict == "timestamp":
        duplicate["venue_timestamp"] = datetime(2024, 6, 2, tzinfo=UTC)
    else:
        duplicate["cum_fill_size"] = 0.2
    events = [first, duplicate]
    coord = _coord_with_events(monkeypatch, events)
    checkpoint = _checkpoint_for_events(coord, events, ["E-1"])
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    aliases = coord.trade_service._shards[_SHARD].seen_exec_ids

    certain = await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)

    assert certain is False
    assert coord.trade_service._shards[_SHARD].seen_exec_ids is aliases


@pytest.mark.asyncio
@pytest.mark.parametrize("saturated_side", ["restored", "replayed"])
async def test_alias_normalization_never_certifies_saturated_identity_sets(
    monkeypatch: pytest.MonkeyPatch,
    saturated_side: str,
) -> None:
    """Keep the dedup completeness limit before reducing aliases to fill identities.

    Given: five thousand two-alias fills saturate replay or both raw alias sets.
    When: startup evaluates alias equivalence with matching economic state.
    Then: saturation refuses certification and the original aliases remain untouched.
    """
    events = [_fill(index) for index in range(1, 5001)]
    coord = _coord_with_events(monkeypatch, events)
    legacy_aliases = [f"E-{index}" for index in range(1, 5001)]
    if saturated_side == "restored":
        legacy_aliases += [f"T-{index}" for index in range(1, 5001)]
    checkpoint = _checkpoint_for_events(coord, events, legacy_aliases)
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    aliases = coord.trade_service._shards[_SHARD].seen_exec_ids

    certain = await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)

    assert certain is False
    assert coord.trade_service._shards[_SHARD].seen_exec_ids is aliases
