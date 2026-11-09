"""Reconstruct narrowly identified legacy spot rollover costs for exact comparison.

These costs came from the internal accrual ledger, not an exchange charge. This
module preserves that recorded history when constructing an expected checkpoint;
it neither authorizes new spot accrual nor refunds a historical charge. Callers
must independently establish exact wallet/instrument scope and cash financing,
provide the complete ledger history, and compare every checkpoint digest field.
"""

import math
from datetime import datetime

from snapper.application.trade.trade_service import FillProjection
from snapper.application.trade.trade_service import TradeService
from snapper.data.repository_types import AccrualLedgerRow
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import VenueEventRow


def _single_buy_projection_valid(projection: FillProjection, fill: VenueEventRow) -> bool:
    """Require one positive, finite spot buy with an unambiguous quote asset."""
    values = (
        projection["position_qty"],
        projection["entry_price"] or 0.0,
        projection["cash"],
        projection["realized_pnl"],
    )
    parts = fill["instrument"].split("-")
    return (
        fill["side"] == "buy"
        and all(math.isfinite(value) for value in values)
        and values[0] > 0
        and values[1] > 0
        and len(parts) == 2
        and all(parts)
    )


def _rollover_matches_projection(
    row: AccrualLedgerRow, projection: FillProjection, fill: VenueEventRow
) -> bool:
    """Require exact charge arithmetic and the same position and quote asset."""
    positive_values = (row["amount"], row["rate"], row["notional"])
    return (
        row["accrual_type"] == "rollover"
        and row["exchange"] == fill["exchange"]
        and row["mode"] == fill["mode"]
        and bool(row["instrument_public_id"])
        and row["amount_asset"] == fill["instrument"].split("-")[1]
        and all(math.isfinite(value) and value > 0 for value in positive_values)
        and row["amount"] == row["notional"] * row["rate"]
        and row["position_quantity_at_accrual"] == projection["position_qty"]
        and row["notional"] == abs(projection["position_qty"]) * (projection["entry_price"] or 0.0)
    )


def _rollover_order_certain(
    rows: list[AccrualLedgerRow],
    index: int,
    checkpoint: TradeProjectionCheckpointRow,
    opened_at: datetime,
) -> bool:
    """Require one session's strictly ordered boundaries and insertion evidence."""
    row = rows[index]
    if not row["session_id"] or row["sequence_id"] <= 0:
        return False
    if not opened_at < row["accrued_at"] <= row["timestamp"] <= checkpoint["checkpoint_at"]:
        return False
    if index == 0:
        return True
    previous = rows[index - 1]
    return (
        row["instrument_public_id"] == previous["instrument_public_id"]
        and row["session_id"] == previous["session_id"]
        and row["sequence_id"] > previous["sequence_id"]
        and row["accrued_at"] > previous["accrued_at"]
        and row["timestamp"] >= previous["timestamp"]
    )


def project_legacy_spot_rollovers(
    projection: FillProjection,
    events: list[VenueEventRow],
    accruals: list[AccrualLedgerRow],
    checkpoint: TradeProjectionCheckpointRow,
) -> FillProjection | None:
    """Forward-fold proven internal rollover charges after one unique spot buy.

    Only a complete ledger for an independently proven unlevered, untagged spot
    shard may be supplied. Every charge must belong to one nonempty ledger
    session and exactly describe the single unchanged position. Later checkpoint
    refreshes may have a different session without changing the recorded costs.
    Multiple fills, unknown order, foreign identity, nonfinite values and inexact
    charges refuse. The caller must compare the returned expected state against
    the entire restored digest with zero tolerance; this helper does not grant
    certification.

    Args:
        projection: Full durable fill replay before any internal accrual costs.
        events: The same replay's complete exact-shard venue-event history.
        accruals: Complete exact-wallet/instrument ledger in original charge order.
        checkpoint: Checkpoint providing the insertion upper bound.

    Returns:
        Expected fill and rollover state, or null when the proof is incomplete.
    """
    fills = [
        event
        for event in TradeService.dedup_fill_events(events)
        if event["event_type"] == "fill_observed"
    ]
    if not accruals or len(fills) != 1:
        return None
    fill = fills[0]
    opened_at = projection["position_opened_at"]
    if opened_at is None or not _single_buy_projection_valid(projection, fill):
        return None
    expected = projection.copy()
    for index, row in enumerate(accruals):
        if not _rollover_matches_projection(row, projection, fill):
            return None
        if not _rollover_order_certain(accruals, index, checkpoint, opened_at):
            return None
        expected["cash"] -= row["amount"]
        expected["realized_pnl"] -= row["amount"]
    return expected
