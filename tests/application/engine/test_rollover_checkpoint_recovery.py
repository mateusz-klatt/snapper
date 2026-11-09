"""Certify only exactly ledger-explained historical spot rollover costs."""

import json
import math
import struct
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.trader import CheckpointRecoveryContext
from snapper.application.engine.trader import TraderCoordinator
from snapper.application.trade.trade_service import PositionProjection
from snapper.application.trade.trade_service import ShardState
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AccrualLedgerRow
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import VenueEventRow
from tests.application.engine.test_checkpoint_recovery import _make_checkpoint
from tests.application.engine.test_checkpoint_recovery import _make_coord
from tests.application.engine.test_checkpoint_recovery import _make_venue_event
from tests.application.engine.test_checkpoint_recovery import _set_sqlalchemy_repo
from tests.application.engine.test_engine_trader import _make_spec

_SHARD = "kraken.XRP-EUR.live"
_WALLET = "w-1"
_OPENED = datetime(2026, 11, 8, 20, 10, tzinfo=UTC)
_CHARGE = 0.001025324575
_FIRST_LEDGER_SESSION = "01a1bda8-6be9-70ab-aab4-da4b9a7a8dbe"
_SECOND_LEDGER_SESSION = "01a1bdca-bd78-7344-80f2-9c0f30515a25"


def _rollover_case(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[TraderCoordinator, TradeProjectionCheckpointRow, list[AccrualLedgerRow]]:
    """Build the reported XRP economics with a complete internal ledger."""
    coord = _make_coord(monkeypatch)
    event = _make_venue_event(
        event_id=1,
        shard_key=_SHARD,
        fill_price=1.00769,
        fill_size=4.07,
        exec_id="E-1",
        trade_id="T-1",
    )
    event.update(
        {
            "instrument": "XRP-EUR",
            "wallet_public_id": _WALLET,
            "fee": 0.03281039,
            "fee_asset": "EUR",
            "venue_timestamp": _OPENED,
            "timestamp": _OPENED,
            "received_at": _OPENED,
        }
    )
    projection = coord.trade_service.project_fill_state_from_events([event])
    checkpoint = _make_checkpoint(
        shard_key=_SHARD,
        position_qty=projection["position_qty"],
        entry_price=projection["entry_price"],
        position_opened_at=projection["position_opened_at"],
        cash=projection["cash"] - _CHARGE,
        realized_pnl=projection["realized_pnl"] - _CHARGE,
        turnover=projection["turnover"],
        last_venue_event_id=projection["last_venue_event_id"],
        seen_exec_ids=json.dumps(list(projection["seen_exec_ids"])),
        checkpoint_at=_OPENED + timedelta(hours=5),
    )
    checkpoint.update({"wallet_public_id": _WALLET, "session_id": "accrual-session"})
    row: AccrualLedgerRow = {
        "public_id": "accrual-1",
        "instrument_public_id": "inst-pid",
        "mode": "live",
        "accrual_type": "rollover",
        "accrued_at": _OPENED + timedelta(hours=4),
        "amount": _CHARGE,
        "amount_asset": "EUR",
        "rate": 0.00025,
        "notional": 4.1012983,
        "position_quantity_at_accrual": 4.07,
        "exchange": "kraken",
        "timestamp": _OPENED + timedelta(hours=4, seconds=1),
        "session_id": "accrual-session",
        "sequence_id": 1,
    }
    repository = AsyncMock(spec=SQLAlchemyRepository)
    _set_sqlalchemy_repo(coord, repository)
    repository.get_venue_events_after = AsyncMock(return_value=[event])
    repository.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
    repository.get_shard_fill_financing = AsyncMock(return_value="spot")
    repository.get_all_shard_accruals = AsyncMock(return_value=[row])
    repository.get_accruals = AsyncMock(return_value=[])
    repository.shard_has_any_accruals = AsyncMock(return_value=True)
    coord._projection_identities[_SHARD] = ("inst-pid", "live", _WALLET)
    return coord, checkpoint, [row]


def _production_rollover_case(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[TraderCoordinator, TradeProjectionCheckpointRow, list[AccrualLedgerRow]]:
    """Reproduce checkpoint 229 by forward-subtracting its three recorded charges."""
    coord, checkpoint, rows = _rollover_case(monkeypatch)
    first = rows[0]
    boundary = datetime(2026, 11, 9, tzinfo=UTC)
    first.update(
        {
            "accrued_at": boundary,
            "timestamp": boundary + timedelta(seconds=1),
            "session_id": _FIRST_LEDGER_SESSION,
        }
    )
    for index, sequence in enumerate((1, 3), start=1):
        row = first.copy()
        row.update(
            {
                "public_id": f"accrual-{index + 1}",
                "accrued_at": boundary + timedelta(hours=4 * index),
                "timestamp": boundary + timedelta(hours=4 * index, seconds=1),
                "session_id": _SECOND_LEDGER_SESSION,
                "sequence_id": sequence,
            }
        )
        rows.append(row)
        checkpoint["cash"] -= row["amount"]
        checkpoint["realized_pnl"] -= row["amount"]
    checkpoint.update(
        {
            "session_id": _SECOND_LEDGER_SESSION,
            "checkpoint_at": boundary + timedelta(hours=8, minutes=30),
        }
    )
    repository = cast(AsyncMock, coord.repository)
    repository.get_all_shard_accruals = AsyncMock(return_value=rows)
    return coord, checkpoint, rows


@pytest.mark.asyncio
async def test_checkpoint_229_identical_charges_certify_at_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All three exact historical charges certify despite their session restart.

    Given: checkpoint 229 and three equal XRP rollover costs from two sessions,
    When: startup forward-folds the complete ledger and verifies both gates,
    Then: the exact production cash and realized bits certify without recharging.
    """
    coord, checkpoint, rows = _production_rollover_case(monkeypatch)
    repository = cast(AsyncMock, coord.repository)
    events = cast(list[VenueEventRow], await repository.get_venue_events_after(_SHARD, 0))
    projection = coord.trade_service.project_fill_state_from_events(events)
    cash, realized = projection["cash"], projection["realized_pnl"]
    for row in rows:
        cash -= row["amount"]
        realized -= row["amount"]
        assert struct.pack(">d", row["amount"]).hex() == "3f50cc85e11d3f40"
    assert checkpoint["cash"] == cash == 9995.862815336277
    assert checkpoint["realized_pnl"] == realized == -0.003075973725
    assert struct.pack(">d", cash).hex() == "40c385ee70bba1e6"
    assert struct.pack(">d", realized).hex() == "bf6932c8d1abdee0"
    assert checkpoint["position_qty"] == 4.07
    assert checkpoint["turnover"] == 4.1012983
    repository.get_venue_events_after = AsyncMock(side_effect=[[], events])

    engine_key = await coord._recover_checkpoint_row(checkpoint, datetime.now(UTC))

    assert engine_key is not None
    assert _SHARD in coord._trusted_recovery_shards
    assert not coord._recovery_certification_failed
    assert coord._verified_checkpoint_rollovers[_SHARD] == rows
    assert coord.trade_service._shards[_SHARD].cash == cash
    assert coord.engines[engine_key].portfolio.cash == cash
    assert repository.get_all_shard_accruals.await_count == 2
    repository.get_accruals.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["cash", "realized_pnl"])
async def test_multisession_rollover_plus_one_ulp_refuses(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    """Identical cross-session charges cannot explain an additional monetary bit.

    Given: checkpoint 229 with cash or realized PnL changed by one ULP,
    When: startup compares its full digest with the three-charge replay,
    Then: certification refuses and retains no verified rollover proof.
    """
    coord, checkpoint, _rows = _production_rollover_case(monkeypatch)
    value = checkpoint["cash"] if field == "cash" else checkpoint["realized_pnl"]
    checkpoint = cast(
        TradeProjectionCheckpointRow, dict(checkpoint) | {field: math.nextafter(value, math.inf)}
    )
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])

    assert not await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)
    assert coord._recovery_certification_failed
    assert _SHARD not in coord._verified_checkpoint_rollovers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "problem", ["amount-bit", "duplicate-boundary", "sequence", "boundary", "insertion", "currency"]
)
async def test_multisession_rollover_invalid_ledger_refuses(
    monkeypatch: pytest.MonkeyPatch,
    problem: str,
) -> None:
    """Cross-session equality preserves every ledger identity and ordering refusal.

    Given: the production ledger with one changed charge or ordering constraint,
    When: startup reconstructs the complete history,
    Then: the altered evidence cannot certify the checkpoint.
    """
    coord, checkpoint, rows = _production_rollover_case(monkeypatch)
    if problem == "amount-bit":
        rows[-1]["rate"] = math.nextafter(rows[-1]["rate"], math.inf)
        rows[-1]["amount"] = rows[-1]["notional"] * rows[-1]["rate"]
        assert struct.pack(">d", rows[-1]["amount"]).hex() == "3f50cc85e11d3f41"
        checkpoint["realized_pnl"] = 0.0
        for row in rows:
            checkpoint["realized_pnl"] -= row["amount"]
    elif problem == "duplicate-boundary":
        rows[1]["accrued_at"] = rows[0]["accrued_at"]
    elif problem == "sequence":
        rows[-1]["sequence_id"] = rows[1]["sequence_id"]
    elif problem == "boundary":
        rows[-1]["accrued_at"] = rows[1]["accrued_at"] - timedelta(seconds=1)
    elif problem == "insertion":
        rows[1]["timestamp"] = rows[-1]["timestamp"] + timedelta(seconds=1)
    else:
        rows[-1]["amount_asset"] = "USD"
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])

    assert not await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)
    assert _SHARD not in coord._verified_checkpoint_rollovers


@pytest.mark.asyncio
async def test_multisession_complete_ledger_change_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A certified three-charge digest still requires an unchanged complete ledger.

    Given: checkpoint 229 passes its exact digest and then a ledger row changes,
    When: startup rechecks accrual certainty,
    Then: the changed history refuses the second certification gate.
    """
    coord, checkpoint, rows = _production_rollover_case(monkeypatch)
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    assert await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)
    changed = [row.copy() for row in rows]
    changed[-1]["public_id"] = "changed-ledger-row"
    repository = cast(AsyncMock, coord.repository)
    repository.get_all_shard_accruals = AsyncMock(return_value=changed)
    context = CheckpointRecoveryContext(_SHARD, "kraken", "XRP-EUR", "live", None, _WALLET)

    assert not await coord._checkpoint_accruals_certain(context, True)


@pytest.mark.asyncio
async def test_exact_rollover_certifies_at_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    """The reported exact internal charge passes both startup certification gates.

    Given: the XRP spot buy and its complete exact internal rollover ledger,
    When: startup recovers the ledger-inclusive checkpoint,
    Then: both certainty gates pass and the recorded cost is applied once.
    """
    coord, checkpoint, rows = _rollover_case(monkeypatch)
    repository = cast(AsyncMock, coord.repository)
    events = cast(list[VenueEventRow], await repository.get_venue_events_after(_SHARD, 0))
    repository.get_venue_events_after = AsyncMock(side_effect=[[], events])
    assert checkpoint["cash"] == 9995.864865985426
    assert checkpoint["realized_pnl"] == -_CHARGE

    engine_key = await coord._recover_checkpoint_row(checkpoint, datetime.now(UTC))

    assert engine_key is not None
    assert _SHARD in coord._trusted_recovery_shards
    assert not coord._recovery_certification_failed
    assert coord._verified_checkpoint_rollovers[_SHARD] == rows
    assert coord.trade_service._shards[_SHARD].cash == checkpoint["cash"]
    assert coord.engines[engine_key].portfolio.cash == checkpoint["cash"]
    repository.get_accruals.assert_not_called()


@pytest.mark.asyncio
async def test_refreshed_checkpoint_can_certify_on_next_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refreshed checkpoint retains exactly proven historical costs.

    Given: historical rollover rows and a checkpoint from a later session,
    When: startup verifies its exact fill digest and complete ledger,
    Then: the session change does not prevent repeat certification.
    """
    coord, checkpoint, _rows = _rollover_case(monkeypatch)
    checkpoint["session_id"] = "new-coordinator-session"
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    assert await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)
    context = CheckpointRecoveryContext(_SHARD, "kraken", "XRP-EUR", "live", None, _WALLET)
    assert await coord._checkpoint_accruals_certain(context, True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cash", math.nextafter(9995.864865985426, math.inf)),
        ("realized_pnl", math.nextafter(-_CHARGE, math.inf)),
        ("position_qty", math.nextafter(4.07, math.inf)),
        ("entry_price", math.nextafter(1.00769, math.inf)),
        ("turnover", math.nextafter(4.1012983, math.inf)),
    ],
)
async def test_rollover_plus_any_economic_difference_refuses(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: float,
) -> None:
    """Even a one-ULP extra difference refuses alongside a valid ledger charge.

    Given: valid rollover evidence and one additionally corrupted digest field,
    When: startup compares the restored checkpoint with the exact replay,
    Then: certification refuses without retaining a verified rollover proof.
    """
    coord, checkpoint, _rows = _rollover_case(monkeypatch)
    checkpoint = cast(TradeProjectionCheckpointRow, dict(checkpoint) | {field: value})
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])

    assert not await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)
    assert coord._recovery_certification_failed
    assert _SHARD not in coord._verified_checkpoint_rollovers


@pytest.mark.asyncio
async def test_rollover_does_not_hide_fee_difference(monkeypatch: pytest.MonkeyPatch) -> None:
    """A fee difference still contradicts an accrual-only checkpoint.

    Given: a ledger-inclusive checkpoint and a changed durable fill fee,
    When: startup reconstructs the fill and rollover economics,
    Then: the fee difference fails certification.
    """
    coord, checkpoint, _rows = _rollover_case(monkeypatch)
    repository = cast(AsyncMock, coord.repository)
    events = cast(list[VenueEventRow], await repository.get_venue_events_after(_SHARD, 0))
    events[0]["fee"] = (events[0]["fee"] or 0.0) + 0.01
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    assert not await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)
    assert coord._recovery_certification_failed


@pytest.mark.asyncio
async def test_without_accrual_realized_difference_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Absent accrual evidence leaves realized PnL comparison exactly strict.

    Given: exact fill cash but divergent realized PnL without accrual evidence,
    When: startup verifies the checkpoint digest,
    Then: the unexplained realized PnL difference refuses certification.
    """
    coord, checkpoint, _rows = _rollover_case(monkeypatch)
    repository = cast(AsyncMock, coord.repository)
    repository.shard_has_any_accruals = AsyncMock(return_value=False)
    checkpoint["cash"] = 9995.86589131
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    assert not await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)
    assert coord._recovery_certification_failed


@pytest.mark.asyncio
@pytest.mark.parametrize("financing", [None, "margin"])
async def test_unproven_spot_cannot_use_legacy_allowance(
    monkeypatch: pytest.MonkeyPatch,
    financing: str | None,
) -> None:
    """Margin and unknown financing retain the existing refusal.

    Given: a rollover-bearing checkpoint whose spot financing is unproven,
    When: startup evaluates the legacy spot allowance,
    Then: it cannot explain the checkpoint through that allowance.
    """
    coord, checkpoint, _rows = _rollover_case(monkeypatch)
    repository = cast(AsyncMock, coord.repository)
    repository.get_shard_fill_financing = AsyncMock(return_value=financing)
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    assert not await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["extra-row", "empty", "currency", "funding"])
async def test_incomplete_or_foreign_ledger_cannot_explain_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    problem: str,
) -> None:
    """Only the complete validated rollover ledger explains historical spot costs.

    Given: a spot checkpoint and an incomplete, duplicated or foreign ledger,
    When: startup reconstructs its historical rollover costs,
    Then: the checkpoint refuses and receives no verified ledger proof.
    """
    coord, checkpoint, rows = _rollover_case(monkeypatch)
    repository = cast(AsyncMock, coord.repository)
    if problem == "extra-row":
        rows.append(rows[0].copy())
    elif problem == "empty":
        rows.clear()
    elif problem == "currency":
        rows[0]["amount_asset"] = "USD"
    else:
        rows[0]["accrual_type"] = "funding"
    repository.get_all_shard_accruals = AsyncMock(return_value=rows)
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    assert not await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)
    assert _SHARD not in coord._verified_checkpoint_rollovers


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["changed", "missing-instrument", "query-error"])
async def test_complete_ledger_recheck_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    """A complete ledger proof cannot survive a failed recheck.

    Given: a verified digest followed by changed or unavailable ledger evidence,
    When: startup rechecks the full ledger before granting accrual certainty,
    Then: accrual certainty fails closed, including after replay failure.
    """
    coord, checkpoint, rows = _rollover_case(monkeypatch)
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    assert await coord._checkpoint_fill_evidence_certain(_SHARD, _WALLET, checkpoint)
    repository = cast(AsyncMock, coord.repository)
    if failure == "changed":
        repository.get_all_shard_accruals = AsyncMock(return_value=rows + [rows[0]])
    elif failure == "missing-instrument":
        repository.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
    else:
        repository.get_all_shard_accruals = AsyncMock(side_effect=RuntimeError("unavailable"))
    context = CheckpointRecoveryContext(_SHARD, "kraken", "XRP-EUR", "live", None, _WALLET)
    assert not await coord._checkpoint_accruals_certain(context, True)
    assert not await coord._checkpoint_accruals_certain(context, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("financing", ["spot", "margin", None])
async def test_rollover_requires_actual_margin_position(
    monkeypatch: pytest.MonkeyPatch,
    financing: str | None,
) -> None:
    """A paper margin-capable instrument accrues only for proven margin fills.

    Given: an open position with spot, margin or unknown durable financing,
    When: the coordinator considers a due rollover boundary,
    Then: only margin produces a charge using a stable position snapshot.
    """
    coord = _make_coord(monkeypatch)
    shard_key = "paper.XRP-EUR.paper"
    engine = MagicMock(spec=TradingEngineService)
    engine.instrument, engine.exchange, engine.mode = "XRP-EUR", "paper", "paper"
    engine._shard_key, engine.wallet_public_id = shard_key, _WALLET
    repository = AsyncMock(spec=SQLAlchemyRepository)
    coord.repository = repository
    repository.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
    repository.get_instrument_spec = AsyncMock(return_value=_make_spec())
    repository.get_shard_fill_financing = AsyncMock(return_value=financing)
    repository.get_last_accrual = AsyncMock(return_value=None)
    event = _make_venue_event(event_id=1, shard_key=shard_key, fill_price=1.00769, fill_size=4.07)
    event.update(
        {
            "venue_timestamp": _OPENED,
            "instrument": "XRP-EUR",
            "mode": "paper",
            "exchange": "paper",
            "wallet_public_id": _WALLET,
        }
    )
    repository.get_venue_events_after = AsyncMock(return_value=[event])
    projection = coord.trade_service.project_fill_state_from_events([event])
    coord.trade_service._shards[shard_key] = ShardState(
        position=PositionProjection(
            position_qty=4.07, entry_price=1.00769, position_opened_at=_OPENED
        ),
        seen_exec_ids=projection["seen_exec_ids"],
    )
    accrue = AsyncMock()
    monkeypatch.setattr(coord, "_accrue_one_boundary", accrue)
    await coord._accrue_engine("engine", engine, _OPENED + timedelta(hours=5))
    assert accrue.call_count == (1 if financing == "margin" else 0)
    repository.get_shard_fill_financing.assert_awaited_once_with(shard_key, _WALLET, since=_OPENED)
    if financing == "margin":
        captured = cast(PositionProjection, accrue.call_args.kwargs["position"])
        assert captured is not coord.trade_service.get_position(shard_key)
        coord.trade_service.get_position(shard_key).position_qty += 1.0
        assert captured.position_qty == 4.07


@pytest.mark.asyncio
async def test_concurrent_spot_fill_cannot_use_prior_margin_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An arriving spot fill cannot extend an old margin proof.

    Given: margin evidence captured before a concurrent spot scale-up,
    When: the scale-up reaches the live position during financing lookup,
    Then: the frozen event replay disagrees and no margin charge is authorized.
    """
    coord, checkpoint, _rows = _rollover_case(monkeypatch)
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    repository = cast(AsyncMock, coord.repository)
    events = cast(list[VenueEventRow], await repository.get_venue_events_after(_SHARD, 0))
    extra = events[0].copy()
    extra.update({"id": 2, "exec_id": "E-2", "trade_id": "T-2", "fill_size": 1.0})
    mixed_events = [*events, extra]

    async def financing_with_arriving_fill(
        _shard_key: str,
        _wallet_public_id: str,
        *,
        since: datetime,
    ) -> str:
        """Simulate a new spot scale-up after the financing query's snapshot."""
        assert since == _OPENED
        coord.trade_service.overlay_fill_state(
            _SHARD, coord.trade_service.project_fill_state_from_events(mixed_events)
        )
        repository.get_venue_events_after = AsyncMock(return_value=mixed_events)
        return "margin"

    repository.get_shard_fill_financing = AsyncMock(side_effect=financing_with_arriving_fill)
    engine = MagicMock(spec=TradingEngineService)
    engine._shard_key, engine.wallet_public_id = _SHARD, _WALLET
    assert not await coord._position_uses_margin(engine, _OPENED)


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["quantity", "entry", "opening", "aliases", "query"])
async def test_incomplete_margin_fill_history_cannot_authorize_charge(
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    """Margin intent cannot charge a position with incomplete fill evidence.

    Given: margin intent and a position whose fill replay is inconsistent,
    When: the coordinator validates the position before rollover,
    Then: missing history or any position and alias difference blocks the charge.
    """
    coord, checkpoint, _rows = _rollover_case(monkeypatch)
    coord._restore_trade_service_from_checkpoint(checkpoint, _SHARD, [])
    repository = cast(AsyncMock, coord.repository)
    repository.get_shard_fill_financing = AsyncMock(return_value="margin")
    engine = MagicMock(spec=TradingEngineService)
    engine._shard_key, engine.wallet_public_id = _SHARD, _WALLET
    shard = coord.trade_service._shards[_SHARD]
    if corruption == "quantity":
        shard.position.position_qty += 0.01
    elif corruption == "entry":
        shard.position.entry_price = 2.0
    elif corruption == "opening":
        shard.position.position_opened_at = _OPENED + timedelta(seconds=1)
    elif corruption == "aliases":
        shard.seen_exec_ids["unobserved-fill"] = None
    else:
        repository.get_venue_events_after = AsyncMock(return_value=None)
    opened_at = shard.position.position_opened_at
    assert opened_at is not None
    assert not await coord._position_uses_margin(engine, opened_at)


@pytest.mark.asyncio
async def test_perpetual_funding_does_not_require_spot_margin_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Derivative funding remains independent of spot borrowing.

    Given: an open position with a perpetual funding schedule,
    When: the coordinator processes a due funding boundary,
    Then: it follows the existing funding path without spot margin evidence.
    """
    coord = _make_coord(monkeypatch)
    engine = MagicMock(spec=TradingEngineService)
    engine.instrument, engine.exchange, engine.mode = "XRP-EUR", "kraken", "live"
    engine._shard_key, engine.wallet_public_id = _SHARD, _WALLET
    repository = AsyncMock(spec=SQLAlchemyRepository)
    coord.repository = repository
    repository.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
    repository.get_instrument_spec = AsyncMock(return_value=_make_spec("perpetual_funding"))
    repository.get_last_accrual = AsyncMock(return_value=None)
    coord.trade_service._shards[_SHARD] = ShardState(
        position=PositionProjection(
            position_qty=4.07, entry_price=1.00769, position_opened_at=_OPENED
        )
    )
    accrue = AsyncMock()
    monkeypatch.setattr(coord, "_accrue_one_boundary", accrue)
    await coord._accrue_engine("engine", engine, _OPENED + timedelta(hours=5))
    accrue.assert_awaited_once()
    repository.get_shard_fill_financing.assert_not_called()
