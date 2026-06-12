"""Tests for :class:`ReconciliationLoop` shard-ownership filter."""

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
        "command_type": "create",
        "client_order_id": f"cid-{public_id}",
        "session_id": "s1",
        "sequence_id": 1,
        "quantity": 1.0,
    }


def _repo(cmds: list[dict[str, Any]]) -> Any:
    """Repo mock returning the given active set with empty fold inputs."""
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(return_value=cmds)
    repo.get_order_lifecycle_events = AsyncMock(return_value=[])
    repo.advance_trade_command_lifecycle = AsyncMock(return_value=True)
    repo.get_rejected_commands_with_later_live_evidence = AsyncMock(return_value=[])
    return repo


@pytest.mark.asyncio
async def test_ownership_filters_foreign_shards_out_of_cycle() -> None:
    """Reconcile cycle records success ONLY for owned shards.

    Given: two shards in the active set — one owned by this
        instance and one foreign,
    When: ``_reconcile_cycle`` runs under N>1 with ownership set,
    Then: only the owned shard_key appears in
        ``record_recon_success`` calls (without the filter, both
        instances would double-count the circuit breaker).
    """
    owned_shard = "kraken.MINE.live"
    foreign_shard = "kraken.FOREIGN.live"
    owner_id = ShardOwnership._hash(owned_shard) % 2
    ownership = ShardOwnership(instance_id=owner_id, instance_count=2)
    repo = _repo([_cmd(owned_shard, "cmd-owned"), _cmd(foreign_shard, "cmd-foreign")])
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
async def test_no_ownership_preserves_unsharded_behavior() -> None:
    """With ``ownership=None`` every shard is reconciled (legacy path).

    Given: a ReconciliationLoop constructed without an ``ownership``
        kwarg (default ``None``),
    When: ``_reconcile_cycle`` runs against an active set of two
        shards on different hash buckets,
    Then: both shards appear in ``record_recon_success`` calls —
        the byte-identical legacy behavior.
    """
    repo = _repo([_cmd("kraken.A.live", "cmd-a"), _cmd("kraken.B.live", "cmd-b")])
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
    """When the active set is entirely foreign, no recon_success is recorded.

    Given: a ReconciliationLoop with ownership pointing at the
        opposite bucket of the only active-command shard,
    When: ``_reconcile_cycle`` runs,
    Then: ``record_recon_success`` is never called — the filter
        drops every row before the success-recording loop.
    """
    foreign_shard = "kraken.FOREIGN.live"
    foreign_owner = ShardOwnership._hash(foreign_shard) % 2
    this_id = (foreign_owner + 1) % 2
    ownership = ShardOwnership(instance_id=this_id, instance_count=2)
    repo = _repo([_cmd(foreign_shard)])
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


@pytest.mark.asyncio
async def test_failure_targets_only_owned_known_shards() -> None:
    """Failure recording respects shard ownership for real known shards.

    Given: known in-memory shards for this exchange include one owned
        shard and one sibling-owned shard,
    When: the reconciliation cycle fails before command scan,
    Then: only the owned shard receives a failure count.
    """
    owned_shard = "kraken.MINE.live"
    foreign_shard = "kraken.FOREIGN.live"
    owner_id = ShardOwnership._hash(owned_shard) % 2
    candidate_index = 0
    while ShardOwnership._hash(foreign_shard) % 2 == owner_id:
        candidate_index += 1
        foreign_shard = f"kraken.FOREIGN{candidate_index}.live"
    ownership = ShardOwnership(instance_id=owner_id, instance_count=2)
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(side_effect=RuntimeError("DB down"))
    trade_svc = MagicMock(spec=TradeService)
    trade_svc.known_shard_keys.return_value = {owned_shard, foreign_shard}
    trade_svc.record_recon_failure = MagicMock(return_value=False)
    recon = ReconciliationLoop(
        exchange_name="kraken",
        repository=repo,
        trade_service=trade_svc,
        interval_seconds=60.0,
        ownership=ownership,
    )
    await recon._reconcile_cycle()
    recorded_shards = {call.args[0] for call in trade_svc.record_recon_failure.call_args_list}
    assert recorded_shards == {owned_shard}
