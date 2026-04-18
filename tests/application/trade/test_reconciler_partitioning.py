"""Phase 4 Day 3 tests — :class:`ReconciliationLoop` shard-ownership filter (§3.4)."""

from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.trade.reconciler import ReconciliationLoop
from snapper.application.trade.trade_service import TradeService
from snapper.core.partitioning import ShardOwnership


def _cmd(shard_key: str, public_id: str = "cmd") -> dict[str, Any]:
    """Minimal active-command dict for reconcile filter tests."""
    return {
        "public_id": public_id,
        "status": "dispatched",
        "created_at": datetime.now(UTC),
        "shard_key": shard_key,
    }


@pytest.mark.asyncio
async def test_ownership_filters_foreign_shards_out_of_cycle() -> None:
    """Reconcile cycle records success ONLY for owned shards.

    With two shards in the active set, one owned by this instance and
    one foreign, only the owned shard_key appears in
    ``record_recon_success`` calls. Without the filter, both instances
    would record success for both shards → double-counted circuit
    breaker.
    """
    owned_shard = "kraken.MINE.live"
    foreign_shard = "kraken.FOREIGN.live"
    owner_id = ShardOwnership._hash(owned_shard) % 2
    ownership = ShardOwnership(instance_id=owner_id, instance_count=2)
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(
        return_value=[
            _cmd(owned_shard, "cmd-owned"),
            _cmd(foreign_shard, "cmd-foreign"),
        ]
    )
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken",
        repository=repo,
        trade_service=trade_svc,
        interval_seconds=60.0,
        ownership=ownership,
    )
    await recon._reconcile_cycle()
    recorded_shards = {call.args[0] for call in trade_svc.record_recon_success.call_args_list}
    assert recorded_shards == {owned_shard}


@pytest.mark.asyncio
async def test_no_ownership_preserves_pre_phase4_behavior() -> None:
    """With ``ownership=None`` every shard is reconciled (pre-Phase-4 path)."""
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(
        return_value=[
            _cmd("kraken.A.live", "cmd-a"),
            _cmd("kraken.B.live", "cmd-b"),
        ]
    )
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken",
        repository=repo,
        trade_service=trade_svc,
        interval_seconds=60.0,
    )
    await recon._reconcile_cycle()
    recorded = {call.args[0] for call in trade_svc.record_recon_success.call_args_list}
    assert recorded == {"kraken.A.live", "kraken.B.live"}


@pytest.mark.asyncio
async def test_all_foreign_no_success_recorded() -> None:
    """When the active set is entirely foreign, no recon_success is recorded."""
    foreign_shard = "kraken.FOREIGN.live"
    foreign_owner = ShardOwnership._hash(foreign_shard) % 2
    this_id = (foreign_owner + 1) % 2
    ownership = ShardOwnership(instance_id=this_id, instance_count=2)
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(return_value=[_cmd(foreign_shard)])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken",
        repository=repo,
        trade_service=trade_svc,
        interval_seconds=60.0,
        ownership=ownership,
    )
    await recon._reconcile_cycle()
    trade_svc.record_recon_success.assert_not_called()
