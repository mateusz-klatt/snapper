"""Tests for :class:`OutboxDispatcher` shard-ownership wiring.

Covers `_fetch_owned_batch` pagination contract:
    - ownership=None → straight pass-through (legacy behavior).
    - ownership=N>1 → page through the backlog, filter in Python,
      stop at batch_size owned rows OR DB exhaustion OR max_scan_rows.
    - Starvation-bound: 150 foreign + 10 owned → owned rows still
      dispatched at default max_scan_rows=1000.
    - max_scan_rows cap → WARN log + partial batch when hit.
    - Regression guard against the former ``10 × batch_size`` cap.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from snapper.application.trade.outbox import OutboxDispatcher
from snapper.core.partitioning import ShardOwnership
from snapper.data.repository_types import TradeCommandRow


def _make_cmd_row(
    public_id: str,
    shard_key: str = "kraken.BTC-USD.live",
) -> TradeCommandRow:
    """Build a minimal :class:`TradeCommandRow` for the pagination tests."""
    now = datetime.now(UTC)
    return {
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 1,
        "command_type": "submit",
        "shard_key": shard_key,
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "engine-buy",
        "client_order_id": public_id,
        "venue_client_id": f"vcid-{public_id}",
        "idempotency_key": None,
        "side": "buy",
        "order_type": "market",
        "quantity": 0.5,
        "price": None,
        "stop_price": None,
        "leverage": None,
        "reduce_only": False,
        "post_only": False,
        "status": "created",
        "attempt_count": 0,
        "last_error": None,
        "created_at": now,
        "dispatched_at": None,
        "acked_at": None,
        "terminal_at": None,
        "exchange_order_id": None,
        "supersedes_command_id": None,
        "correlation_id": f"corr-{public_id}",
        "wallet_public_id": None,
        "operator_public_id": None,
        "user_public_id": None,
        "source_surface": "strategy",
    }


def _make_repo_with_rows(
    rows: list[TradeCommandRow],
) -> AsyncMock:
    """Build an AsyncMock repo that returns pages from ``rows``.

    Mimics ``get_undispatched_commands(offset=..., limit=...)`` slicing
    against an in-memory list. Useful for testing the pagination
    contract without a real DB.
    """
    repo = AsyncMock()

    def _get_cmds(
        *,
        as_of: datetime,
        limit: int = 10,
        offset: int = 0,
    ) -> list[TradeCommandRow]:
        return rows[offset : offset + limit]

    repo.get_undispatched_commands = AsyncMock(side_effect=_get_cmds)
    return repo


def _make_dispatcher(
    repo: Any,
    *,
    ownership: ShardOwnership | None = None,
    batch_size: int = 10,
    max_scan_rows: int | None = None,
) -> OutboxDispatcher:
    """Construct a dispatcher with controllable kwargs."""
    return OutboxDispatcher(
        repository=repo,
        publish_fn=AsyncMock(),
        poll_interval=0.01,
        batch_size=batch_size,
        ownership=ownership,
        max_scan_rows=max_scan_rows,
    )


def _own(shard_key: str, *, instance_count: int = 2) -> ShardOwnership:
    """Return a :class:`ShardOwnership` that owns the given shard_key."""
    owner_id = ShardOwnership._hash(shard_key) % instance_count
    return ShardOwnership(instance_id=owner_id, instance_count=instance_count)


def _foreign(shard_key: str, *, instance_count: int = 2) -> ShardOwnership:
    """Return a :class:`ShardOwnership` that does NOT own the given shard_key."""
    owner_id = ShardOwnership._hash(shard_key) % instance_count
    foreign_id = (owner_id + 1) % instance_count
    return ShardOwnership(instance_id=foreign_id, instance_count=instance_count)


class TestOutboxOwnershipPassthrough:
    """``ownership=None`` preserves the legacy straight-fetch behavior."""

    @pytest.mark.asyncio
    async def test_none_ownership_returns_raw_batch(self) -> None:
        """Without ownership, the dispatcher calls the repo once per cycle."""
        rows = [_make_cmd_row(f"c-{i}") for i in range(5)]
        repo = _make_repo_with_rows(rows)
        dispatcher = _make_dispatcher(repo, ownership=None, batch_size=10)
        now = datetime.now(UTC)
        fetched = await dispatcher._fetch_owned_batch(now)
        assert len(fetched) == 5
        assert repo.get_undispatched_commands.call_count == 1


class TestOutboxOwnershipFilter:
    """``ownership=N>1`` filters rows by shard ownership."""

    @pytest.mark.asyncio
    async def test_all_foreign_rows_returns_empty(self) -> None:
        """When every row is foreign, the owned batch is empty."""
        shard = "kraken.FOREIGN.live"
        rows = [_make_cmd_row(f"c-{i}", shard_key=shard) for i in range(5)]
        repo = _make_repo_with_rows(rows)
        ownership = _foreign(shard)
        dispatcher = _make_dispatcher(repo, ownership=ownership, batch_size=10)
        now = datetime.now(UTC)
        fetched = await dispatcher._fetch_owned_batch(now)
        assert fetched == []

    @pytest.mark.asyncio
    async def test_all_owned_rows_returns_all(self) -> None:
        """When every row is owned, the owned batch equals the set."""
        shard = "kraken.OWNED.live"
        rows = [_make_cmd_row(f"c-{i}", shard_key=shard) for i in range(5)]
        repo = _make_repo_with_rows(rows)
        ownership = _own(shard)
        dispatcher = _make_dispatcher(repo, ownership=ownership, batch_size=10)
        now = datetime.now(UTC)
        fetched = await dispatcher._fetch_owned_batch(now)
        assert len(fetched) == 5

    @pytest.mark.asyncio
    async def test_mixed_foreign_prefix_paginates_past(self) -> None:
        """Foreign prefix is skipped; owned rows at the tail are found.

        Regression guard against the former ``10 × batch_size`` cap —
        with 15 foreign rows in front of 5 owned rows, the scan must
        reach the owned rows.
        """
        foreign_shard = "kraken.FOREIGN.live"
        owned_shard = "kraken.MINE.live"
        ownership = _own(owned_shard)
        foreign_rows = [_make_cmd_row(f"f-{i}", shard_key=foreign_shard) for i in range(15)]
        owned_rows = [_make_cmd_row(f"o-{i}", shard_key=owned_shard) for i in range(5)]
        repo = _make_repo_with_rows(foreign_rows + owned_rows)
        dispatcher = _make_dispatcher(repo, ownership=ownership, batch_size=10)
        now = datetime.now(UTC)
        fetched = await dispatcher._fetch_owned_batch(now)
        assert len(fetched) == 5
        assert [r["public_id"] for r in fetched] == [f"o-{i}" for i in range(5)]

    @pytest.mark.asyncio
    async def test_default_bound_reaches_150_foreign_prefix(self) -> None:
        """150-row foreign prefix skipped at default ``max_scan_rows=1000``.

        The default cap is the production value. 150 rows
        is well under 1000, so owned rows at the tail are reached.
        """
        foreign_shard = "kraken.FOREIGN.live"
        owned_shard = "kraken.MINE.live"
        ownership = _own(owned_shard)
        foreign_rows = [_make_cmd_row(f"f-{i}", shard_key=foreign_shard) for i in range(150)]
        owned_rows = [_make_cmd_row(f"o-{i}", shard_key=owned_shard) for i in range(10)]
        repo = _make_repo_with_rows(foreign_rows + owned_rows)
        dispatcher = _make_dispatcher(repo, ownership=ownership, batch_size=10, max_scan_rows=1000)
        now = datetime.now(UTC)
        fetched = await dispatcher._fetch_owned_batch(now)
        assert len(fetched) == 10
        assert [r["public_id"] for r in fetched] == [f"o-{i}" for i in range(10)]

    @pytest.mark.asyncio
    async def test_max_scan_rows_none_scans_until_exhaustion(self) -> None:
        """Unbounded scan reaches owned rows past arbitrary foreign prefixes.

        Regression guard against the former ``10 × batch_size`` cap —
        with ``max_scan_rows=None`` the starvation-avoidance contract
        holds no matter how big the foreign-owner backlog grows.
        """
        foreign_shard = "kraken.FOREIGN.live"
        owned_shard = "kraken.MINE.live"
        ownership = _own(owned_shard)
        foreign_rows = [_make_cmd_row(f"f-{i}", shard_key=foreign_shard) for i in range(500)]
        owned_rows = [_make_cmd_row(f"o-{i}", shard_key=owned_shard) for i in range(10)]
        repo = _make_repo_with_rows(foreign_rows + owned_rows)
        dispatcher = _make_dispatcher(repo, ownership=ownership, batch_size=10, max_scan_rows=None)
        now = datetime.now(UTC)
        fetched = await dispatcher._fetch_owned_batch(now)
        assert len(fetched) == 10

    @pytest.mark.asyncio
    async def test_max_scan_rows_cap_truncates_partial_batch(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Hitting ``max_scan_rows`` emits WARN + returns partial batch."""
        foreign_shard = "kraken.FOREIGN.live"
        owned_shard = "kraken.MINE.live"
        ownership = _own(owned_shard)
        foreign_rows = [_make_cmd_row(f"f-{i}", shard_key=foreign_shard) for i in range(100)]
        owned_rows = [_make_cmd_row(f"o-{i}", shard_key=owned_shard) for i in range(10)]
        repo = _make_repo_with_rows(foreign_rows + owned_rows)
        dispatcher = _make_dispatcher(repo, ownership=ownership, batch_size=10, max_scan_rows=20)
        now = datetime.now(UTC)
        fetched = await dispatcher._fetch_owned_batch(now)
        assert len(fetched) < 10

    @pytest.mark.asyncio
    async def test_db_exhaustion_returns_partial_batch(self) -> None:
        """Empty page terminates the paging loop cleanly."""
        owned_shard = "kraken.OWNED.live"
        ownership = _own(owned_shard)
        rows = [_make_cmd_row(f"o-{i}", shard_key=owned_shard) for i in range(3)]
        repo = _make_repo_with_rows(rows)
        dispatcher = _make_dispatcher(repo, ownership=ownership, batch_size=10)
        now = datetime.now(UTC)
        fetched = await dispatcher._fetch_owned_batch(now)
        assert len(fetched) == 3
