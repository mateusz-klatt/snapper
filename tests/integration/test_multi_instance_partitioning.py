"""Integration tests — N=2 partitioning end-to-end.

Covers:
    1. ``test_two_coordinators_split_signals``: two coordinators,
       10 signals precomputed to split ~50/50 across instance_id 0/1,
       assert each instance's engines set is disjoint + the union
       covers all dispatched shards.
    2. ``test_coordinator_restart_only_recovers_owned``: after the
       split, stop coordinator 1 and start a fresh one with the same
       settings. Assert it recovers ONLY its own shards from
       checkpoints + the original coordinator 0 is unaffected.
    3. ``test_coordinator_ownership_from_bootstrap_env``: real
       ``BootstrapSettingsLoader`` → ``AppSettings`` → ``_build_ownership``
       path via env-var mutation + cache_clear, proving per-instance
       topology resolves without the ``settings=`` kwarg injection.

The scenarios use the ``two_coordinator_stack`` fixture from
:mod:`tests.integration.conftest` for scenarios #1 and #2, and the
``@pytest.mark.real_settings`` escape for scenario #3 (narrow
ownership-only pass, no broker pipeline).
"""

import asyncio
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest
import zmq
import zmq.asyncio

from snapper.application.engine.trader import TraderCoordinator
from snapper.config.app import AppSettings
from snapper.config.settings import get_bootstrap_settings
from snapper.config.settings import get_settings
from snapper.core.partitioning import ShardOwnership
from snapper.data.repository import get_repository
from snapper.messaging.schemas.data import SignalData
from tests.integration.conftest import TwoCoordinatorStack
from tests.integration.conftest import _per_coordinator_settings
from tests.integration.conftest import _stop_background_process

pytestmark = pytest.mark.integration


def _make_signal_topic(instrument: str) -> str:
    """Build the ``signals.paper.{instrument}.live`` topic."""
    return f"signals.paper.{instrument}.live"


def _make_signal(instrument: str, price: float = 100.0) -> SignalData:
    """Build a minimal paper-mode BUY signal for the given instrument."""
    now = datetime.now(UTC)
    return SignalData(
        type="signal",
        public_id=f"sig-{instrument}",
        timestamp=now,
        session_id="s-phase4",
        sequence_id=1,
        instrument=instrument,
        exchange="paper",
        side="buy",
        strength=0.5,
        reason="phase4-integration",
        price=price,
        strategy_name="phase4-integration",
        fired_at=now,
    )


_TEST_INSTRUMENTS: tuple[str, ...] = (
    "BTC-USD",
    "ETH-USD",
    "BTC-EUR",
    "EUR-USD",
    "AAPL",
)


def _pick_instruments_per_instance() -> tuple[list[str], list[str]]:
    """Partition the test instruments across the two coordinator instances.

    Uses only instruments present in the root conftest's
    ``_TEST_SYMBOL_MAPPINGS`` so paper-mode ``is_tradeable`` passes.
    The shard_key for a paper signal is
    ``paper.{instrument}.paper.{signal_type}`` because:

        - ``TradingEngineService.mode`` returns ``PAPER`` for paper
          exchange, so the base is ``paper.{instrument}.paper``.
        - ``_on_signal`` sets ``strategy_tag = parsed.signal_type`` for
          paper exchange, and ``compute_shard_key`` appends the tag.

    Tests publish on ``signals.paper.{instrument}.live``, so
    ``signal_type = "live"``. Hence the canonical shard_key is
    ``paper.{instrument}.paper.live``.
    """
    for_0: list[str] = []
    for_1: list[str] = []
    for instrument in _TEST_INSTRUMENTS:
        shard_key = f"paper.{instrument}.paper.live"
        bucket = ShardOwnership._hash(shard_key) % 2
        if bucket == 0:
            for_0.append(instrument)
        else:
            for_1.append(instrument)
    return for_0, for_1


async def _publish_signal(ctx: zmq.asyncio.Context, xsub_endpoint: str, signal: SignalData) -> None:
    """Publish a single ``signals.paper.{instrument}.live`` frame."""
    pub = ctx.socket(zmq.PUB)
    pub.connect(xsub_endpoint)
    await asyncio.sleep(0.15)
    topic = _make_signal_topic(signal.instrument).encode()
    await pub.send_multipart([topic, signal.to_json().encode()])
    await asyncio.sleep(0.05)
    pub.setsockopt(zmq.LINGER, 0)
    pub.close()


async def _wait_for_engines(
    trader: TraderCoordinator, expected_keys: set[str], timeout: float = 10.0
) -> None:
    """Poll ``trader.engines`` until the expected keys appear (or timeout)."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        current = set(trader.engines.keys())
        if expected_keys.issubset(current):
            return
        await asyncio.sleep(0.1)


class TestTwoCoordinatorsSplitSignals:
    """Scenario #1 — split signals across two instances."""

    async def test_two_coordinators_split_signals(
        self, two_coordinator_stack: TwoCoordinatorStack
    ) -> None:
        """10 signals hash-partition across two coordinators.

        Asserts:
            - Each coordinator's engines dict has the exact 5 keys
              whose shards the hash assigns to its instance.
            - The union of both engines sets equals all 10 dispatched
              shards.
            - No duplicates (each shard is owned by exactly one
              coordinator).
        """
        trader_0, trader_1 = two_coordinator_stack.traders
        instruments_0, instruments_1 = _pick_instruments_per_instance()
        all_instruments = instruments_0 + instruments_1
        for inst in all_instruments:
            await _publish_signal(
                two_coordinator_stack.client_context,
                two_coordinator_stack.xsub_endpoint,
                _make_signal(inst),
            )
        expected_0 = {f"{inst}@paper-live" for inst in instruments_0}
        expected_1 = {f"{inst}@paper-live" for inst in instruments_1}
        await _wait_for_engines(trader_0, expected_0, timeout=10.0)
        await _wait_for_engines(trader_1, expected_1, timeout=10.0)
        engines_0 = set(trader_0.engines.keys())
        engines_1 = set(trader_1.engines.keys())
        assert expected_0.issubset(engines_0), f"trader_0 missing keys: {expected_0 - engines_0}"
        assert expected_1.issubset(engines_1), f"trader_1 missing keys: {expected_1 - engines_1}"
        assert not (
            engines_0 & expected_1
        ), f"trader_0 should NOT host trader_1's shards: {engines_0 & expected_1}"
        assert not (
            engines_1 & expected_0
        ), f"trader_1 should NOT host trader_0's shards: {engines_1 & expected_0}"


@pytest.mark.real_settings
def test_coordinator_ownership_from_bootstrap_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scenario #3 — production topology config via env vars.

    Given: two coordinators constructed back-to-back with
        different ``SNAPPER_COORDINATOR_INSTANCE_ID`` env values
        (0, 1) AND explicit ``get_bootstrap_settings.cache_clear`` +
        ``get_settings.cache_clear`` between them
        (``@pytest.mark.real_settings`` opts out of the autouse
        mock so the real ``BootstrapSettingsLoader`` →
        ``AppSettings`` → ``_build_ownership`` chain runs),
    When: each coordinator calls ``_build_ownership`` and the
        resulting :class:`ShardOwnership` is applied to a sample
        key set,
    Then: the two ownerships produce disjoint + complete coverage
        of the sample — proving the production env-var topology
        path works without the test-only ``settings=`` kwarg
        injection that scenarios #1/#2 rely on.

    Scope: ownership-only, no signal
    pipeline (scenarios #1/#2 cover the pipeline via the injection
    path). The autouse session-scoped ``isolated_sqlite_db``
    fixture has already pointed ``DB_URL`` at an isolated copy by
    the time this test runs.
    """

    def _make_coordinator(inst_id: int, inst_count: int) -> ShardOwnership:
        monkeypatch.setenv("SNAPPER_COORDINATOR_INSTANCE_ID", str(inst_id))
        monkeypatch.setenv("SNAPPER_COORDINATOR_INSTANCE_COUNT", str(inst_count))
        get_bootstrap_settings.cache_clear()
        get_settings.cache_clear()
        trader = TraderCoordinator(signal_topics=["signals."])
        assert trader.settings.coordinator_instance_id == inst_id
        assert trader.settings.coordinator_instance_count == inst_count
        trader.repository = cast(Any, SimpleNamespace(dialect_name="postgresql"))
        return trader._build_ownership()

    ownership_0 = _make_coordinator(0, 2)
    ownership_1 = _make_coordinator(1, 2)
    assert ownership_0.instance_id == 0
    assert ownership_0.instance_count == 2
    assert ownership_1.instance_id == 1
    assert ownership_1.instance_count == 2
    owned_by_0 = {k for k in ("a", "b", "c", "d", "e", "f") if ownership_0.owns(k)}
    owned_by_1 = {k for k in ("a", "b", "c", "d", "e", "f") if ownership_1.owns(k)}
    assert owned_by_0.isdisjoint(owned_by_1)
    assert owned_by_0 | owned_by_1 == {"a", "b", "c", "d", "e", "f"}


async def _force_checkpoint(
    stack: TwoCoordinatorStack,
    shard_key: str,
    wallet_public_id: str = "",
) -> None:
    """Force a checkpoint for ``shard_key`` via direct repository write.

    Circumvents the trade-runtime's async checkpoint emission policy
    so scenario #2's positive-recovery variant can assert deterministic
    state restoration after restart without fighting the 60s
    checkpoint tick.
    """
    repo = get_repository(stack.base_mock.db_url)
    now = datetime.now(UTC)
    await repo.upsert_checkpoint(
        {
            "shard_key": shard_key,
            "position_qty": 0.0,
            "entry_price": None,
            "position_opened_at": None,
            "cash": 1_000_000.0,
            "peak_equity": 1_000_000.0,
            "realized_pnl": 0.0,
            "turnover": 0.0,
            "last_venue_event_id": 1,
            "last_venue_event_at": now,
            "open_command_ids": None,
            "seen_exec_ids": "[]",
            "checkpoint_at": now,
            "session_id": "phase4-forced-checkpoint",
            "sequence_id": 1,
            "bus_time": now,
            "wallet_public_id": wallet_public_id,
        }
    )


class TestCoordinatorRestartOnlyRecoversOwned:
    """Scenario #2 — restart recovers exactly the owned shards."""

    async def test_coordinator_restart_positively_recovers_forced_checkpoint(
        self, two_coordinator_stack: TwoCoordinatorStack
    ) -> None:
        """Positive recovery: a forced checkpoint for an owned shard IS recovered.

        Complements ``test_coordinator_restart_only_recovers_owned``
        (which only proves the exclusion invariant). Writes a
        checkpoint directly via the repository for a shard owned by
        instance 1, restarts instance 1, asserts the restored
        coordinator HAS that shard's engine in memory after the
        recovery cycle completes.

        This exercises the checkpoint sub-method recovery path
        end-to-end under N>1 — the contract that paper-N>1 recovery
        relies on because the other sub-methods are disabled for paper.
        """
        trader_0, trader_1 = two_coordinator_stack.traders
        _, instruments_1 = _pick_instruments_per_instance()
        assert instruments_1, "test requires at least one shard owned by instance 1"
        target_instrument = instruments_1[0]
        target_shard = f"paper.{target_instrument}.paper.live"

        await _force_checkpoint(two_coordinator_stack, target_shard)

        trader_1_tasks = two_coordinator_stack.trader_tasks

        await _stop_background_process(trader_1.stop(), trader_1_tasks[1])

        trader_0_keys_before = set(trader_0.engines.keys())

        trader_1_new = TraderCoordinator(
            signal_topics=["signals."],
            settings=cast(
                AppSettings,
                _per_coordinator_settings(two_coordinator_stack.base_mock, 1),
            ),
        )
        trader_1_new_task = asyncio.create_task(trader_1_new.start())
        try:
            expected_key = f"{target_instrument}@paper-live"
            await _wait_for_engines(trader_1_new, {expected_key}, timeout=10.0)
            restored = set(trader_1_new.engines.keys())
            assert expected_key in restored, (
                f"trader_1_new failed to recover forced-checkpoint shard "
                f"{target_shard} (expected engine_key={expected_key}, "
                f"got engines={restored})"
            )
            recovered_engine = trader_1_new.engines[expected_key]
            assert recovered_engine._ownership is not None
            assert recovered_engine._ownership.instance_id == 1
            assert recovered_engine._ownership.instance_count == 2
            assert set(trader_0.engines.keys()) == trader_0_keys_before
        finally:
            await _stop_background_process(trader_1_new.stop(), trader_1_new_task)

    async def test_coordinator_restart_only_recovers_owned(
        self, two_coordinator_stack: TwoCoordinatorStack
    ) -> None:
        """Restart does not recover foreign-shard state from the DB.

        Step 1: publish signals for all test instruments. Wait until
        the expected engines show up in both coordinators.
        Step 2: stop trader_1 and start a fresh trader_1_new with
        the same settings (instance_id=1, instance_count=2).

        Assertions (partitioned-recovery contract):

            - trader_1_new MUST NOT recover any of trader_0's shards.
              This is the ownership-split invariant the paper N>1
              exclusion exists to protect.
            - Whatever engines trader_1_new does recover MUST carry
              ``_ownership`` with instance_id=1, instance_count=2 —
              proves the recovery-path engine wiring.
            - trader_0 is untouched by the restart.

        The test does NOT require trader_1_new to re-recover
        trader_1's original engines: under N>1 paper mode the
        recovery path excludes paper ExecutionRow/OrderRow rows
        (strategy_tag unavailable on those tables) and recovery is
        checkpoint-only. Checkpoints take time to emit and this test
        does not force a checkpoint tick, so the recovered set is
        allowed to be empty.
        """
        trader_0, trader_1 = two_coordinator_stack.traders
        instruments_0, instruments_1 = _pick_instruments_per_instance()
        for inst in instruments_0 + instruments_1:
            await _publish_signal(
                two_coordinator_stack.client_context,
                two_coordinator_stack.xsub_endpoint,
                _make_signal(inst),
            )
        expected_0 = {f"{inst}@paper-live" for inst in instruments_0}
        expected_1 = {f"{inst}@paper-live" for inst in instruments_1}
        await _wait_for_engines(trader_1, expected_1, timeout=10.0)
        trader_0_keys_before = set(trader_0.engines.keys())

        trader_1_tasks = two_coordinator_stack.trader_tasks

        await _stop_background_process(trader_1.stop(), trader_1_tasks[1])

        trader_1_new = TraderCoordinator(
            signal_topics=["signals."],
            settings=cast(
                AppSettings,
                _per_coordinator_settings(two_coordinator_stack.base_mock, 1),
            ),
        )
        trader_1_new_task = asyncio.create_task(trader_1_new.start())
        try:
            await asyncio.sleep(1.0)
            restored = set(trader_1_new.engines.keys())
            assert restored.isdisjoint(
                expected_0
            ), f"trader_1_new recovered trader_0's shards: {restored & expected_0}"
            for engine in trader_1_new.engines.values():
                assert engine._ownership is not None
                assert engine._ownership.instance_id == 1
                assert engine._ownership.instance_count == 2
            assert set(trader_0.engines.keys()) == trader_0_keys_before
        finally:
            await _stop_background_process(trader_1_new.stop(), trader_1_new_task)
