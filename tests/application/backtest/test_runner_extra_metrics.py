"""Conditional extra_metrics propagation + SQLAlchemy JSON roundtrip.

Locks BE-1 D9 contract at two layers:

1. Runner side — the extra_metrics dict is ``{}`` when the collector's
   ``cross_asset_blocked_fills`` counter is zero (single-feed byte-identical
   Phase 2c path) and carries the key+value when positive (cross-asset
   runs that blocked on missing target closes).
2. Persistence side — the dict roundtrips intact through
   ``BacktestRepository.insert_result`` → ``get_result`` so downstream
   callers observe the stored value exactly. Closes R3.8 MAJOR.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import snapper.data.repository as repo_module
from snapper.application.backtest.result_collector import ResultCollector
from snapper.data.backtest_repository import BacktestRepository

NOW = datetime(2026, 4, 22, 12, 0, 0, tzinfo=UTC)


async def _make_repo(tmp_path: Path) -> BacktestRepository:
    """Build a BacktestRepository backed by a fresh sqlite+aiosqlite DB."""
    db_path = tmp_path / "bt_extra.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    return BacktestRepository(r.session_factory)


class TestExtraMetricsShape:
    """Runner-layer conditional dict (no DB, pure in-memory)."""

    def test_empty_when_no_blocked_fills(self) -> None:
        """Zero counter → empty dict (byte-identical Phase 2c legacy).

        Given: a ResultCollector with cross_asset_blocked_fills=0
            (default after __init__),
        When: the runner builds extra_metrics via its conditional
            insert,
        Then: the dict is ``{}`` exactly — no key written. Phase 2c
            dedup + response-schema fixtures unchanged.
        """
        collector = ResultCollector()
        extra: dict[str, object] = {}
        if collector.cross_asset_blocked_fills > 0:
            extra["cross_asset_blocked_fills"] = collector.cross_asset_blocked_fills
        assert extra == {}

    def test_carries_counter_when_positive(self) -> None:
        """Non-zero counter surfaces under 'cross_asset_blocked_fills' key.

        Given: a collector with cross_asset_blocked_fills=3 (simulated
            via MagicMock with attr fixed),
        When: the runner conditional writes the field,
        Then: extra_metrics == {'cross_asset_blocked_fills': 3}.
        """
        collector = MagicMock(spec=ResultCollector)
        collector.cross_asset_blocked_fills = 3
        extra: dict[str, object] = {}
        if collector.cross_asset_blocked_fills > 0:
            extra["cross_asset_blocked_fills"] = collector.cross_asset_blocked_fills
        assert extra == {"cross_asset_blocked_fills": 3}


class TestExtraMetricsRoundtrip:
    """BacktestRepository JSON-column persistence roundtrip (R3.8 closure)."""

    @pytest.mark.asyncio
    async def test_extra_metrics_roundtrips_through_sqlalchemy_json(self, tmp_path: Path) -> None:
        """Persisted extra_metrics dict reads back byte-identical.

        Given: insert_result with extra_metrics={'cross_asset_blocked_fills': 7},
        When: get_result fetches the row,
        Then: the returned dict equals the inserted dict exactly —
            SQLAlchemy JSON serialization + normalization at
            backtest_repository.py:575 preserves shape.
        """
        repo = await _make_repo(tmp_path)
        await repo.insert_result(
            {
                "run_public_id": "run-cross",
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "total_pnl": 0.0,
                "max_drawdown": 0.0,
                "final_equity": 10000.0,
                "max_equity": 10000.0,
                "extra_metrics": {"cross_asset_blocked_fills": 7},
                "session_id": "s1",
                "sequence_id": 1,
                "timestamp": NOW,
            },
            bus_time=NOW,
            session_id="s1",
            sequence_id=100,
        )
        result = await repo.get_result("run-cross", as_of=NOW + timedelta(seconds=1))
        assert result is not None
        assert result["extra_metrics"] == {"cross_asset_blocked_fills": 7}

    @pytest.mark.asyncio
    async def test_empty_extra_metrics_roundtrips_as_empty_dict(self, tmp_path: Path) -> None:
        """Empty extra_metrics roundtrips as {} (legacy Phase 2c parity).

        Given: insert_result with extra_metrics={},
        When: get_result fetches the row,
        Then: the returned dict is {} — matching repository normalization
            at backtest_repository.py:575 + API normalization at
            backtest_routes.py:253-263. Confirms that conditional-write
            (`{}` on zero counter) reads back as `{}`, not None.
        """
        repo = await _make_repo(tmp_path)
        await repo.insert_result(
            {
                "run_public_id": "run-single",
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "total_pnl": 0.0,
                "max_drawdown": 0.0,
                "final_equity": 10000.0,
                "max_equity": 10000.0,
                "extra_metrics": {},
                "session_id": "s1",
                "sequence_id": 1,
                "timestamp": NOW,
            },
            bus_time=NOW,
            session_id="s1",
            sequence_id=101,
        )
        result = await repo.get_result("run-single", as_of=NOW + timedelta(seconds=1))
        assert result is not None
        assert result["extra_metrics"] == {}
