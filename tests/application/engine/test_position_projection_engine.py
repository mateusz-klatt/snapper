"""Tests for the trader-side truthful position projection engine (Phase 2 S2).

Pins the D2/D3/D5/D6 consensus contracts: identities register
first-wins from engine-carried resolved IDs (never native-symbol or
wallet-short parsing), the projection triggers ONLY after a COMMITTED
checkpoint, paper aggregation refuses multi-instance ownership,
component state freezes before the awaited mark lookup, aggregation
uses fsum semantics with direction-aware NULL entry and mark-gated
unrealized PnL, marks degrade to honest NULLs, the watermark is the max
durable consumed id, and nothing ever raises into the trading path.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.engine.trader import TraderCoordinator
from snapper.application.trade.trade_service import TradeService
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import TradeProjectionCheckpointRow

_WALLET = "00000000-0000-7000-8000-aabbccddeeff"
_SHORT = "aabbccddeeff"
_INSTRUMENT = "00000000-0000-7000-8000-00000000000a"
_SOURCE = "00000000-0000-7000-8000-00000000000b"
_SHARD_A = f"paper.BTC-USD.paper.w{_SHORT}.strat_a"
_SHARD_B = f"paper.BTC-USD.paper.w{_SHORT}.strat_b"
_IDENTITY = (_INSTRUMENT, "paper", _WALLET)
_MARKED_AT = datetime(2026, 7, 11, 12, 0, 0, tzinfo=UTC)
_NOW = datetime(2026, 7, 11, 12, 30, 0, tzinfo=UTC)


def _make_repo() -> AsyncMock:
    """Build a repository mock with a resolvable mapped-paper mark chain.

    Returns:
        AsyncMock speccing SQLAlchemyRepository so isinstance gates pass.
    """
    repo = AsyncMock(spec=SQLAlchemyRepository)
    repo.resolve_source_instrument_public_id = AsyncMock(
        return_value={"valuation_public_id": _SOURCE, "is_paper": True, "mapped": True}
    )
    repo.get_active_market_snapshot_price = AsyncMock(return_value=(50100.0, _MARKED_AT))
    repo.upsert_position_projection = AsyncMock(return_value=1)
    repo.close_position_projection = AsyncMock(return_value=True)
    return repo


def _make_coord(repo: Any) -> TraderCoordinator:
    """Wire a bare TraderCoordinator with the projection surfaces.

    Args:
        repo: Repository double.

    Returns:
        Coordinator ready for projection-path calls.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord._tracker = MagicMock()
    coord._tracker.session_id = "s-test"
    coord._tracker.next_sequence = MagicMock(return_value=7)
    coord._wallet_short_to_id = {_SHORT: _WALLET}
    coord._consumed_venue_event_watermarks = {}
    coord._projection_identities = {}
    coord._projection_locks = {}
    coord._trusted_recovery_shards = set()
    coord._recovery_baseline_shards = set()
    coord._failed_recovery_shard_prefixes = set()
    coord._failed_recovery_identities = set()
    coord._recovery_certification_failed = False
    coord._ownership = None
    coord.repository = repo
    return coord


def _seed_shard(
    coord: TraderCoordinator,
    shard_key: str,
    qty: float,
    entry: float | None,
    realized: float,
    identity: tuple[str, str, str] = _IDENTITY,
) -> None:
    """Seed one in-memory shard position and register its identity.

    Args:
        coord: Coordinator under test.
        shard_key: Shard to seed.
        qty: Position quantity.
        entry: Entry price or None.
        realized: Cumulative realized PnL.
        identity: Registered projection identity for the shard.
    """
    pos = coord.trade_service.get_position(shard_key)
    pos.position_qty = qty
    pos.entry_price = entry
    pos.realized_pnl = realized
    coord._projection_identities[shard_key] = identity


def _make_engine(
    shard_key: str,
    *,
    wallet: str = _WALLET,
    public_id: str | None = _INSTRUMENT,
) -> MagicMock:
    """Build an engine stub carrying resolved projection identifiers.

    Args:
        shard_key: Engine shard key.
        wallet: Full wallet UUID carried by the engine.
        public_id: Resolved instrument public id in the spec, or None.

    Returns:
        Engine double for registration tests.
    """
    engine = MagicMock()
    engine._shard_key = shard_key
    engine.mode = "paper"
    engine.wallet_public_id = wallet
    engine.instrument = "BTC-USD"
    engine.instrument_specs = {"BTC-USD": {"public_id": public_id} if public_id else {}}
    return engine


async def test_projection_triggers_only_after_committed_checkpoint() -> None:
    """The projection rides the checkpoint SUCCESS path exclusively.

    Given: a coordinator whose checkpoint upsert fails once and then
        succeeds,
    When: _persist_checkpoint runs twice,
    Then: the projection trigger fires only for the committed write,
        with the checkpoint's own bus time.
    """
    repo = _make_repo()
    repo.upsert_checkpoint = AsyncMock(side_effect=[RuntimeError("db down"), 1])
    coord = _make_coord(repo)
    coord.engines = {}
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    with patch.object(coord, "_persist_position_projection", new_callable=AsyncMock) as trigger:
        await coord._persist_checkpoint(_SHARD_A)
        trigger.assert_not_awaited()
        await coord._persist_checkpoint(_SHARD_A)
        trigger.assert_awaited_once()
        assert trigger.await_args is not None
        assert trigger.await_args.args == (_SHARD_A,)
        assert isinstance(trigger.await_args.kwargs["now"], datetime)


def test_engine_registration_is_first_wins_from_resolved_ids() -> None:
    """Identity registration uses engine-carried IDs, first-wins.

    Given: an engine carrying full resolved identifiers, a conflicting
        re-registration, and an engine without a resolved instrument id,
    When: engines register,
    Then: the first identity is retained, the conflict is ignored,
        incomplete engines register nothing, a raw MagicMock stub's
        truthy non-string attributes never poison the registry, and
        instrument_specs=None cannot raise into engine registration.
    """
    coord = _make_coord(_make_repo())
    coord._engines_by_scope = {}
    coord._engines_by_scope_legacy = {}
    coord._engines_by_pending_coid = {}
    coord._register_projection_identity(_make_engine(_SHARD_A))
    assert coord._projection_identities[_SHARD_A] == _IDENTITY
    other = "00000000-0000-7000-8000-00000000000c"
    coord._register_projection_identity(_make_engine(_SHARD_A, public_id=other))
    assert coord._projection_identities[_SHARD_A] == _IDENTITY
    coord._register_projection_identity(_make_engine(_SHARD_B, public_id=None))
    assert _SHARD_B not in coord._projection_identities
    coord._register_projection_identity(_make_engine(_SHARD_B, wallet=""))
    assert _SHARD_B not in coord._projection_identities
    raw_stub = MagicMock()
    coord._register_projection_identity(raw_stub)
    assert list(coord._projection_identities) == [_SHARD_A]
    none_specs = _make_engine(_SHARD_B)
    none_specs.instrument_specs = None
    coord._register_projection_identity(none_specs)
    assert _SHARD_B not in coord._projection_identities


async def test_unregistered_shard_skips_projection() -> None:
    """A shard without a registered identity never writes.

    Given: a checkpointing shard whose engine identifiers were
        incomplete,
    When: the projection trigger runs,
    Then: nothing is upserted or closed.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.upsert_position_projection.assert_not_awaited()
    repo.close_position_projection.assert_not_awaited()


async def test_paper_projection_refuses_multi_instance_ownership() -> None:
    """Paper aggregation under N>1 coordinators is refused loudly.

    Given: a paper identity on a coordinator with instance_count > 1,
    When: the projection trigger runs,
    Then: nothing is written — competing partial aggregates would
        corrupt the shared identity row — while N == 1 proceeds.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    coord._ownership = MagicMock(instance_count=2)
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.upsert_position_projection.assert_not_awaited()
    coord._ownership = MagicMock(instance_count=1)
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.upsert_position_projection.assert_awaited_once()


async def test_single_shard_projection_row_carries_full_truth() -> None:
    """One registered shard projects its complete truthful snapshot.

    Given: a single non-flat shard with a durable watermark and a
        usable mapped-paper mark,
    When: the projection persists,
    Then: the upsert row carries the aggregate, the VWAP entry, the
        mark-based unrealized PnL, the mark trio, the watermark, and
        the checkpoint bus time.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 5.0)
    coord._consumed_venue_event_watermarks[_SHARD_A] = 42
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.upsert_position_projection.assert_awaited_once()
    row = repo.upsert_position_projection.await_args.args[0]
    assert row["instrument_public_id"] == _INSTRUMENT
    assert row["mode"] == "paper"
    assert row["wallet_public_id"] == _WALLET
    assert row["quantity"] == 1.0
    assert row["average_price"] == 50000.0
    assert row["unrealized_pnl"] == pytest.approx(100.0)
    assert row["realized_pnl"] == 5.0
    assert row["mark_price"] == 50100.0
    assert row["marked_at"] == _MARKED_AT
    assert row["source_venue_event_id"] == 42
    assert row["session_id"] == "s-test"
    assert row["sequence_id"] == 7
    assert row["bus_time"] == _NOW
    repo.get_active_market_snapshot_price.assert_awaited_once_with(_SOURCE)


async def test_multi_shard_same_direction_aggregates_with_vwap() -> None:
    """Same-direction strategy shards aggregate into one identity row.

    Given: two paper strategy shards long the same instrument with
        distinct entries, realized PnL, and watermarks,
    When: the projection persists,
    Then: quantity and realized PnL are fsum totals, the entry is the
        absolute-quantity-weighted VWAP, unrealized sums the per-shard
        terms, and the watermark is the max across components.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 5.0)
    _seed_shard(coord, _SHARD_B, 3.0, 51000.0, 7.0)
    coord._consumed_venue_event_watermarks[_SHARD_A] = 42
    coord._consumed_venue_event_watermarks[_SHARD_B] = 57
    await coord._persist_position_projection(_SHARD_B, now=_NOW)
    row = repo.upsert_position_projection.await_args.args[0]
    assert row["quantity"] == 4.0
    assert row["average_price"] == pytest.approx(50750.0)
    assert row["realized_pnl"] == 12.0
    assert row["unrealized_pnl"] == pytest.approx(100.0 - 2700.0)
    assert row["source_venue_event_id"] == 57


async def test_opposing_directions_stay_active_with_null_entry() -> None:
    """A net-zero aggregate of opposing shards keeps an honest row.

    Given: one long and one short component netting to zero,
    When: the projection persists,
    Then: the row is UPSERTED (never closed while a component is open)
        with NULL average_price and a summed unrealized PnL.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    _seed_shard(coord, _SHARD_B, -1.0, 52000.0, 0.0)
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.close_position_projection.assert_not_awaited()
    row = repo.upsert_position_projection.await_args.args[0]
    assert row["quantity"] == pytest.approx(0.0)
    assert row["average_price"] is None
    assert row["unrealized_pnl"] == pytest.approx(100.0 + 1900.0)


async def test_missing_entry_nulls_valuation_but_keeps_quantity() -> None:
    """A non-flat shard without an entry price degrades honestly.

    Given: a non-flat component whose entry price is unknown,
    When: the projection persists,
    Then: average_price and unrealized_pnl are NULL while quantity,
        realized PnL, and the mark trio stay truthful.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 2.0, None, 3.0)
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    row = repo.upsert_position_projection.await_args.args[0]
    assert row["quantity"] == 2.0
    assert row["average_price"] is None
    assert row["unrealized_pnl"] is None
    assert row["mark_price"] == 50100.0
    assert row["realized_pnl"] == 3.0


async def test_all_flat_components_close_without_successor() -> None:
    """A fully flat identity closes its active row.

    Given: every registered component shard flat,
    When: the projection persists,
    Then: close_position_projection is called with the identity and the
        checkpoint bus time, and no upsert happens.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 0.0, None, 4.0)
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.close_position_projection.assert_awaited_once_with(_INSTRUMENT, "paper", _WALLET, _NOW)
    repo.upsert_position_projection.assert_not_awaited()


async def test_epsilon_boundary_matches_trade_service_zero_snap() -> None:
    """Exactly 1e-12 quantity is NON-flat, mirroring the zero-snap.

    Given: a component holding exactly the TradeService zero-snap
        boundary quantity,
    When: the projection persists,
    Then: the identity is upserted as active, not closed.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1e-12, 50000.0, 0.0)
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.close_position_projection.assert_not_awaited()
    repo.upsert_position_projection.assert_awaited_once()


async def test_component_without_state_refuses_to_fabricate_flatness() -> None:
    """Registered components lacking materialized state block the write.

    Given: two registered components where only one has TradeService
        state,
    When: the projection persists,
    Then: nothing is written or closed — absence of state is not
        evidence of flatness.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    coord._projection_identities[_SHARD_B] = _IDENTITY
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.upsert_position_projection.assert_not_awaited()
    repo.close_position_projection.assert_not_awaited()


async def test_frozen_snapshot_survives_mid_await_mutation() -> None:
    """Component truth freezes before the awaited mark lookup.

    Given: a mark resolver that mutates the position and its watermark
        while the projection awaits it (a fill landing mid-write),
    When: the projection persists,
    Then: quantity, unrealized PnL, and the watermark all reflect the
        PRE-await snapshot — the fill's own trigger owns the newer
        truth.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 5.0)
    coord._consumed_venue_event_watermarks[_SHARD_A] = 42

    async def mutating_resolver(instrument_public_id: str) -> dict[str, Any]:
        pos = coord.trade_service.get_position(_SHARD_A)
        pos.position_qty = 99.0
        pos.entry_price = 1.0
        pos.realized_pnl = 999.0
        coord._consumed_venue_event_watermarks[_SHARD_A] = 777
        return {"valuation_public_id": _SOURCE, "is_paper": True, "mapped": True}

    repo.resolve_source_instrument_public_id.side_effect = mutating_resolver
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    row = repo.upsert_position_projection.await_args.args[0]
    assert row["quantity"] == 1.0
    assert row["realized_pnl"] == 5.0
    assert row["unrealized_pnl"] == pytest.approx(100.0)
    assert row["source_venue_event_id"] == 42


async def test_mark_degradation_paths_yield_null_trio() -> None:
    """Every mark failure mode degrades to honest NULLs, never zeros.

    Given: an unmapped paper identity, a resolver exception, a missing
        snapshot, and a non-positive price — in sequence,
    When: the mark resolves for each,
    Then: each yields (None, None) and the unmapped case never touches
        the snapshot accessor.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    repo.resolve_source_instrument_public_id.return_value = {
        "valuation_public_id": _INSTRUMENT,
        "is_paper": True,
        "mapped": False,
    }
    assert await coord._resolve_projection_mark(_INSTRUMENT) == (None, None)
    repo.get_active_market_snapshot_price.assert_not_awaited()
    repo.resolve_source_instrument_public_id.side_effect = RuntimeError("boom")
    assert await coord._resolve_projection_mark(_INSTRUMENT) == (None, None)
    repo.resolve_source_instrument_public_id.side_effect = None
    repo.resolve_source_instrument_public_id.return_value = {
        "valuation_public_id": _SOURCE,
        "is_paper": False,
        "mapped": False,
    }
    repo.get_active_market_snapshot_price.return_value = None
    assert await coord._resolve_projection_mark(_INSTRUMENT) == (None, None)
    repo.get_active_market_snapshot_price.return_value = (0.0, _MARKED_AT)
    assert await coord._resolve_projection_mark(_INSTRUMENT) == (None, None)
    repo.get_active_market_snapshot_price.return_value = (None, _MARKED_AT)
    assert await coord._resolve_projection_mark(_INSTRUMENT) == (None, None)
    repo.get_active_market_snapshot_price.return_value = (float("nan"), _MARKED_AT)
    assert await coord._resolve_projection_mark(_INSTRUMENT) == (None, None)
    plain_coord = _make_coord(MagicMock())
    assert await plain_coord._resolve_projection_mark(_INSTRUMENT) == (None, None)


async def test_mark_unavailable_projects_null_unrealized() -> None:
    """A markless identity persists truth with a NULL mark trio.

    Given: a non-flat shard whose mark lookup finds no snapshot,
    When: the projection persists,
    Then: the row keeps quantity/entry/realized while the mark trio and
        unrealized PnL are NULL.
    """
    repo = _make_repo()
    repo.get_active_market_snapshot_price.return_value = None
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 5.0)
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    row = repo.upsert_position_projection.await_args.args[0]
    assert row["mark_price"] is None
    assert row["marked_at"] is None
    assert row["unrealized_pnl"] is None
    assert row["average_price"] == 50000.0


async def test_watermarkless_identity_projects_null_watermark() -> None:
    """No durable consumed watermark projects as NULL, never zero.

    Given: a shard with no consumed venue-event watermark,
    When: the projection persists,
    Then: source_venue_event_id is None.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    row = repo.upsert_position_projection.await_args.args[0]
    assert row["source_venue_event_id"] is None


async def test_projection_failures_never_reach_the_trading_path() -> None:
    """Projection persistence is best-effort end to end.

    Given: an upsert that raises and separately a write helper guarded
        by a non-repository backend,
    When: _persist_position_projection runs,
    Then: no exception escapes and the degraded backend writes nothing.
    """
    repo = _make_repo()
    repo.upsert_position_projection.side_effect = RuntimeError("db down")
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    plain = MagicMock()
    coord_degraded = _make_coord(plain)
    coord_degraded._projection_identities[_SHARD_A] = _IDENTITY
    await coord_degraded._write_position_projection_locked(_IDENTITY, now=_NOW)
    plain.upsert_position_projection.assert_not_called()


async def test_checkpoint_wallet_resolves_beyond_boot_cache() -> None:
    """The full wallet UUID survives an empty wallet-short cache.

    Given: a wallet created after boot (absent from the wallet-short
        cache) whose engine-registered identity carries the full UUID,
    When: the checkpoint commits,
    Then: the checkpoint row and the projection row both carry the full
        wallet UUID — never an empty string PostgreSQL would reject.
    """
    repo = _make_repo()
    repo.upsert_checkpoint = AsyncMock(return_value=1)
    coord = _make_coord(repo)
    coord._wallet_short_to_id = {}
    engine = _make_engine(_SHARD_A)
    coord.engines = {"BTC-USD@paper": engine}
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    await coord._persist_checkpoint(_SHARD_A)
    checkpoint_row = repo.upsert_checkpoint.await_args.args[0]
    assert checkpoint_row["wallet_public_id"] == _WALLET
    projection_row = repo.upsert_position_projection.await_args.args[0]
    assert projection_row["wallet_public_id"] == _WALLET


def test_checkpoint_wallet_precedence_identity_engine_cache() -> None:
    """Wallet resolution is identity-first, then engine, then cache.

    Given: three DISTINCT wallet UUIDs planted in the registered
        identity, the matching engine, and the legacy wallet-short
        cache,
    When: layers are removed one by one,
    Then: resolution walks identity → engine → cache → empty string.
    """
    identity_wallet = "00000000-0000-7000-8000-000000000101"
    engine_wallet = "00000000-0000-7000-8000-000000000102"
    cache_wallet = "00000000-0000-7000-8000-000000000103"
    coord = _make_coord(_make_repo())
    coord._projection_identities[_SHARD_A] = (_INSTRUMENT, "paper", identity_wallet)
    coord.engines = {"BTC-USD@paper": _make_engine(_SHARD_A, wallet=engine_wallet)}
    coord._wallet_short_to_id = {_SHORT: cache_wallet}
    assert coord._resolve_checkpoint_wallet(_SHARD_A) == identity_wallet
    coord._projection_identities.clear()
    assert coord._resolve_checkpoint_wallet(_SHARD_A) == engine_wallet
    coord.engines = {}
    assert coord._resolve_checkpoint_wallet(_SHARD_A) == cache_wallet
    coord._wallet_short_to_id = {}
    assert coord._resolve_checkpoint_wallet(_SHARD_A) == ""


async def test_watermark_lookup_failure_never_kills_the_listener() -> None:
    """A transient watermark lookup error stays inside the checkpoint.

    Given: a durable venue-event id resolution that raises,
    When: _persist_checkpoint runs after a consumed fill,
    Then: no exception escapes, the checkpoint still commits with the
        prior conservative watermark, and the projection still runs.
    """
    repo = _make_repo()
    repo.upsert_checkpoint = AsyncMock(return_value=1)
    coord = _make_coord(repo)
    coord.engines = {}
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    coord._consumed_venue_event_watermarks[_SHARD_A] = 42
    repo.get_consumed_fill_venue_event_id = AsyncMock(side_effect=RuntimeError("db hiccup"))
    await coord._persist_checkpoint(_SHARD_A, consumed_fill=MagicMock())
    checkpoint_row = repo.upsert_checkpoint.await_args.args[0]
    assert checkpoint_row["last_venue_event_id"] == 42
    repo.upsert_position_projection.assert_awaited_once()


async def test_recovery_rebuild_refreshes_and_projects_once_per_identity() -> None:
    """The rebuild refreshes checkpoints then projects each identity once.

    Given: two recovered strategy shards sharing one identity,
    When: _rebuild_position_projections runs,
    Then: a checkpoint-only refresh commits per shard, the identity is
        projected exactly once with the full aggregate, and a
        pre-existing active row with no recovered local state is only
        scanned — left visibly stale, never written or closed.
    """
    repo = _make_repo()
    repo.upsert_checkpoint = AsyncMock(return_value=1)
    repo.get_active_position_identities = AsyncMock(
        return_value=[("pos-foreign", "inst-foreign", "paper", "wallet-foreign")]
    )
    coord = _make_coord(repo)
    coord.engines = {}
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    _seed_shard(coord, _SHARD_B, 2.0, 51000.0, 0.0)
    coord._trusted_recovery_shards.update({_SHARD_A, _SHARD_B})
    await coord._rebuild_position_projections()
    assert repo.upsert_checkpoint.await_count == 2
    repo.upsert_position_projection.assert_awaited_once()
    row = repo.upsert_position_projection.await_args.args[0]
    assert row["quantity"] == 3.0
    repo.close_position_projection.assert_not_awaited()
    repo.get_active_position_identities.assert_awaited_once()


async def test_recovery_rebuild_contains_failures_and_degraded_backends() -> None:
    """Rebuild failures never abort recovery.

    Given: an identity whose projection write raises, a stale-row scan
        that raises, and separately a non-repository backend,
    When: _rebuild_position_projections runs,
    Then: no exception escapes in either case and the degraded backend
        performs no work at all.
    """
    repo = _make_repo()
    repo.upsert_checkpoint = AsyncMock(return_value=1)
    repo.upsert_position_projection.side_effect = RuntimeError("db down")
    repo.get_active_position_identities = AsyncMock(side_effect=RuntimeError("scan down"))
    coord = _make_coord(repo)
    coord.engines = {}
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    coord._trusted_recovery_shards.add(_SHARD_A)
    await coord._rebuild_position_projections()
    repo.get_active_position_identities.assert_awaited_once()
    plain = MagicMock()
    coord_degraded = _make_coord(plain)
    await coord_degraded._rebuild_position_projections()
    plain.upsert_checkpoint.assert_not_called()


async def test_untrusted_component_blocks_refresh_and_projection() -> None:
    """Uncertain recovered state is never certified as truth.

    Given: two sibling shards of one identity where only one earned
        positive recovery certification (the other's gap correction or
        accrual replay failed),
    When: the rebuild runs,
    Then: only the trusted shard's checkpoint refreshes and the
        identity is neither projected nor closed — a partially
        recovered paper aggregate must not overwrite the shared row.
    """
    repo = _make_repo()
    repo.upsert_checkpoint = AsyncMock(return_value=1)
    repo.get_active_position_identities = AsyncMock(return_value=[])
    coord = _make_coord(repo)
    coord.engines = {}
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    _seed_shard(coord, _SHARD_B, 2.0, 51000.0, 0.0)
    coord._trusted_recovery_shards.add(_SHARD_A)
    await coord._rebuild_position_projections()
    assert repo.upsert_checkpoint.await_count == 1
    repo.upsert_position_projection.assert_not_awaited()
    repo.close_position_projection.assert_not_awaited()


async def test_gap_discovery_failure_skips_the_whole_rebuild() -> None:
    """A failed venue-event gap discovery uncertifies everything.

    Given: the recovery pass could not enumerate shards with fills,
    When: the rebuild runs,
    Then: no checkpoint refresh and no projection happen at all —
        recovered state cannot be certified.
    """
    repo = _make_repo()
    repo.upsert_checkpoint = AsyncMock(return_value=1)
    coord = _make_coord(repo)
    coord.engines = {}
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    coord._recovery_certification_failed = True
    await coord._rebuild_position_projections()
    repo.upsert_checkpoint.assert_not_awaited()
    repo.upsert_position_projection.assert_not_awaited()


def test_replay_venue_event_carries_both_venue_identifiers() -> None:
    """Synthetic replay events keep exec_id AND trade_id.

    Given: a recovery execution row that carries only a venue exec id
        (trade id absent, as Kraken Futures commonly reports),
    When: the synthetic replay venue event is built,
    Then: the event carries the exec id so fill dedup and watermark
        matching key exactly like the live path — a later checkpoint
        delta replay cannot re-apply the fill under a different key.
    """
    coord = _make_coord(_make_repo())
    engine = _make_engine(_SHARD_A)
    fill_row: dict[str, Any] = {
        "public_id": "exe-1",
        "timestamp": _NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "trade_id": None,
        "exec_id": "E-77",
        "exchange_order_id": "X-1",
        "client_order_id": "cid-1",
        "instrument": "BTC-USD",
        "exchange": "kraken",
        "side": "buy",
        "size": 0.5,
        "price": 50000.0,
        "fee": 0.1,
        "fee_asset": "USD",
        "status": "filled",
        "executed_at": _NOW,
        "wallet_public_id": _WALLET,
        "operator_public_id": None,
    }
    event = coord._build_replay_venue_event(engine, fill_row, synthetic_id=7)
    assert event["exec_id"] == "E-77"
    assert event["trade_id"] is None
    assert event["id"] == 7


async def test_checkpoint_discovery_failure_fails_certification() -> None:
    """A failed checkpoint enumeration blocks every certification.

    Given: the checkpoint discovery query raises,
    When: _recover_from_checkpoints runs,
    Then: the global certification flag trips so the projection rebuild
        will refuse to certify anything this boot.
    """
    repo = _make_repo()
    repo.get_all_checkpoints = AsyncMock(side_effect=RuntimeError("db down"))
    coord = _make_coord(repo)
    recovered = await coord._recover_from_checkpoints(_NOW)
    assert recovered == set()
    assert coord._recovery_certification_failed is True


async def test_failed_sibling_prefix_blocks_the_aggregate() -> None:
    """A sibling that failed before registration blocks its aggregate.

    Given: shard A registered and healthy while sibling B of the SAME
        (exchange, instrument, mode, wallet) group failed recovery
        before it could register an identity,
    When: A's projection trigger runs,
    Then: nothing is written — the aggregate cannot be certified from a
        partial component set.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    coord._failed_recovery_shard_prefixes = {("paper", "BTC-USD", "paper", _SHORT)}
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.upsert_position_projection.assert_not_awaited()
    repo.close_position_projection.assert_not_awaited()


async def test_accrual_replay_certification_holes_return_false() -> None:
    """Every uncertain accrual outcome refuses certification.

    Given: a checkpoint without an anchor, an unresolvable instrument,
        a failing accrual query, a clean nothing-to-replay pass, and a
        TAGGED paper shard with pending accruals (the accrual ledger
        carries no strategy-tag identity, so replaying into a sibling
        would double-count funding),
    When: _replay_checkpoint_accruals runs for each,
    Then: only the clean pass returns True; every uncertain or
        double-counting outcome returns False.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    kwargs: dict[str, Any] = {
        "now": _NOW,
        "instrument": "BTC-USD",
        "exchange_str": "paper",
        "mode_str": "paper",
        "wallet_public_id": _WALLET,
        "shard_key": _SHARD_A,
    }
    assert (
        await coord._replay_checkpoint_accruals(
            checkpoint=cast(TradeProjectionCheckpointRow, {}), **kwargs
        )
        is False
    )
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
    assert (
        await coord._replay_checkpoint_accruals(
            checkpoint=cast(TradeProjectionCheckpointRow, {"checkpoint_at": _NOW}), **kwargs
        )
        is False
    )
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=_INSTRUMENT)
    repo.get_accruals = AsyncMock(side_effect=RuntimeError("db down"))
    assert (
        await coord._replay_checkpoint_accruals(
            checkpoint=cast(TradeProjectionCheckpointRow, {"checkpoint_at": _NOW}), **kwargs
        )
        is False
    )
    repo.get_accruals = AsyncMock(return_value=[])
    assert (
        await coord._replay_checkpoint_accruals(
            checkpoint=cast(TradeProjectionCheckpointRow, {"checkpoint_at": _NOW}), **kwargs
        )
        is True
    )
    repo.get_accruals = AsyncMock(return_value=[{"amount": 1.0}])
    assert (
        await coord._replay_checkpoint_accruals(
            checkpoint=cast(TradeProjectionCheckpointRow, {"checkpoint_at": _NOW}), **kwargs
        )
        is False
    )


async def test_cycle_reconciliation_skips_uncertified_shards() -> None:
    """Cycle reconciliation never infers flatness from uncertified state.

    Given: two recovered engines where only one shard earned positive
        certification,
    When: _reconcile_position_cycles runs,
    Then: only the certified engine's cycle is reconciled — an
        active-order-only or failed-recovery engine cannot close its
        open cycle from a fabricated zero position.
    """
    repo = _make_repo()
    repo.get_open_position_cycles_for_shards = AsyncMock(return_value={})
    coord = _make_coord(repo)
    certified = _make_engine(_SHARD_A)
    uncertified = _make_engine(_SHARD_B)
    coord.engines = {"a": certified, "b": uncertified}
    coord._trusted_recovery_shards = {_SHARD_A}
    with patch.object(
        coord, "_reconcile_position_cycle_for_engine", new_callable=AsyncMock
    ) as reconcile:
        await coord._reconcile_position_cycles()
    reconcile.assert_awaited_once()
    assert reconcile.await_args is not None
    assert reconcile.await_args.args[1] is certified


async def test_execution_replay_certifies_live_but_never_paper_lineage() -> None:
    """Execution replay trust requires durable live lineage.

    Given: one LIVE execution group whose shard registered an identity
        and carries no accruals, and one PAPER group (execution rows
        hold no durable shard key, so paper lineage is reconstructed),
    When: _recover_execution_group replays each,
    Then: the live shard is certified while the paper shard stays
        uncertified.
    """
    live_shard = f"kraken.BTC-USD.live.w{_SHORT}"
    fill: dict[str, Any] = {
        "public_id": "exe-1",
        "timestamp": _NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "trade_id": "t1",
        "exec_id": None,
        "exchange_order_id": "x1",
        "client_order_id": "c1",
        "instrument": "BTC-USD",
        "exchange": "kraken",
        "side": "buy",
        "size": 0.5,
        "price": 50000.0,
        "fee": 0.1,
        "fee_asset": "USD",
        "status": "filled",
        "executed_at": _NOW,
        "wallet_public_id": _WALLET,
        "operator_public_id": None,
    }
    repo = _make_repo()
    repo.get_consumed_fill_venue_event_id = AsyncMock(return_value=7)
    repo.shard_has_any_accruals = AsyncMock(return_value=False)
    coord = _make_coord(repo)
    coord.engines = {}
    live_engine = _make_engine(live_shard)
    live_engine.exchange = "kraken"
    live_engine.mode = "live"
    live_engine.position_qty = 0.5
    live_engine.entry_price = 50000.0
    coord._projection_identities[live_shard] = (_INSTRUMENT, "live", _WALLET)
    with (
        patch.object(coord, "_create_engine_for_recovery", AsyncMock(return_value=live_engine)),
        patch.object(coord, "_restore_engine_from_shard", MagicMock()),
        patch.object(coord, "_register_recovered_engine", MagicMock()),
    ):
        await coord._recover_execution_group(
            engine_key="k1",
            fills=[cast(ExecutionRow, fill)],
            wallet_public_id=_WALLET,
            operator_public_id="",
        )
    assert live_shard in coord._trusted_recovery_shards
    paper_engine = _make_engine(_SHARD_A)
    paper_engine.exchange = "paper"
    paper_engine.position_qty = 0.5
    paper_engine.entry_price = 50000.0
    with (
        patch.object(coord, "_create_engine_for_recovery", AsyncMock(return_value=paper_engine)),
        patch.object(coord, "_restore_engine_from_shard", MagicMock()),
        patch.object(coord, "_register_recovered_engine", MagicMock()),
    ):
        await coord._recover_execution_group(
            engine_key="k2",
            fills=[cast(ExecutionRow, dict(fill, exchange="paper"))],
            wallet_public_id=_WALLET,
            operator_public_id="",
        )
    assert _SHARD_A not in coord._trusted_recovery_shards


async def test_global_certification_failure_quarantines_trusted_shards() -> None:
    """A global recovery failure revokes even previously granted trust.

    Given: a shard that earned trust before a discovery pass failed,
    When: the rebuild runs and a live fill later triggers the writer,
    Then: the rebuild clears recovery trust and the writer refuses the
        baseline shard for the process lifetime.
    """
    repo = _make_repo()
    repo.upsert_checkpoint = AsyncMock(return_value=1)
    coord = _make_coord(repo)
    coord.engines = {}
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    coord._trusted_recovery_shards.add(_SHARD_A)
    coord._recovery_certification_failed = True
    await coord._rebuild_position_projections()
    assert coord._trusted_recovery_shards == set()
    assert _SHARD_A in coord._recovery_baseline_shards
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.upsert_position_projection.assert_not_awaited()
    repo.close_position_projection.assert_not_awaited()


async def test_canonically_failed_identity_blocks_projection() -> None:
    """A canonical failure record blocks its exact identity.

    Given: the identity itself is recorded in the canonical failure
        set (a sibling failed recovery and was attributed by
        instrument/mode/wallet),
    When: the projection trigger runs,
    Then: nothing is written or closed.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    coord._failed_recovery_identities = {_IDENTITY}
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.upsert_position_projection.assert_not_awaited()
    repo.close_position_projection.assert_not_awaited()


async def test_non_matching_failed_prefix_does_not_block_projection() -> None:
    """Failure records for OTHER aggregates never block this one.

    Given: a recorded failure prefix for a different instrument,
    When: the projection trigger runs,
    Then: the identity still projects normally.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    coord._failed_recovery_shard_prefixes = {("kraken", "ETH-USD", "live", "")}
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.upsert_position_projection.assert_awaited_once()


async def test_recorder_without_repository_fails_certification() -> None:
    """A failure that cannot resolve its instrument blocks everything.

    Given: a recorder invocation on a coordinator whose backend is not
        a SQLAlchemy repository (no instrument lookup possible),
    When: the failure is recorded,
    Then: canonical attribution is unavailable and the global flag
        trips.
    """
    coord = _make_coord(MagicMock())
    await coord._record_recovery_shard_failure(f"kraken.BTC-USD.live.w{_SHORT}")
    assert coord._recovery_certification_failed is True


async def test_certification_failure_blocks_even_live_born_shards() -> None:
    """The global quarantine is unconditional in the shared writer.

    Given: a boot whose certification failed with an EMPTY baseline
        (the contradicted candidates never materialized state),
    When: a later live fill materializes a fresh shard and triggers the
        projection,
    Then: nothing is written — ambiguity or contradiction quarantines
        the whole projection surface for the process lifetime.
    """
    repo = _make_repo()
    coord = _make_coord(repo)
    _seed_shard(coord, _SHARD_A, 1.0, 50000.0, 0.0)
    coord._recovery_certification_failed = True
    await coord._persist_position_projection(_SHARD_A, now=_NOW)
    repo.upsert_position_projection.assert_not_awaited()
    repo.close_position_projection.assert_not_awaited()
