"""Unit tests for :class:`snapper.application.services.market_persist_policy.MarketPersistPolicy`.

Coverage targets: mode resolution under auto + explicit, overlays
(extra includes / exclude blocks), Branch A scope-grant refresh,
Branch B settings refresh, malformed-shape fallbacks, listener
start / stop idempotency, dispatcher routing, and the public
introspection surface used by the publisher safety rail.

DB-state mutations are integration-tested separately; these tests
focus on the policy contract: deterministic refresh given fixed
inputs, atomic swap under the lock, and graceful degradation when
inputs are absent or malformed.
"""

import asyncio
import contextlib
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.services.market_persist_policy import MarketPersistPolicy
from snapper.core.types import ExchangeEnum
from snapper.data.repository_types import OperatorRow


def _operator_row(public_id: str) -> OperatorRow:
    """Build a stub ``OperatorRow`` projection used by Branch A."""
    return OperatorRow(
        public_id=public_id,
        label="op-label",
        description=None,
        timestamp=MagicMock(),
        session_id="sess",
        sequence_id=1,
    )


class _StubSettingsService:
    """In-memory replacement for :class:`SettingsService` driving Branch B.

    Tests instantiate with a ``{key: value}`` dict; :meth:`get_setting`
    returns the configured value or the provided default. This avoids
    booting the real cache + ZMQ subscriber for unit tests.
    """

    def __init__(self, values: dict[str, Any]) -> None:
        """Capture the seed values mapping."""
        self._values = values

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Return the seeded value for ``key`` or ``default``."""
        return self._values.get(key, default)


def _build_policy(
    *,
    settings: dict[str, Any] | None = None,
    operators: list[OperatorRow] | None = None,
    pairs: set[tuple[str, str]] | None = None,
) -> tuple[MarketPersistPolicy, MagicMock, _StubSettingsService]:
    """Construct a policy wired against AsyncMock repo + stub settings."""
    repo = MagicMock()
    repo.list_active_operators = AsyncMock(return_value=operators or [])
    repo.list_scope_grant_instrument_pairs = AsyncMock(return_value=pairs or set())
    stub_settings = _StubSettingsService(settings or {})
    policy = MarketPersistPolicy(
        repository=cast(Any, repo),
        settings_service=cast(Any, stub_settings),
    )
    return policy, repo, stub_settings


class TestInitialRebuildBranchOrder:
    """Verify Branch B runs before Branch A on :meth:`initial_rebuild`."""

    @pytest.mark.asyncio
    async def test_initial_rebuild_applies_settings_before_scope_pairs(self) -> None:
        """Branch B populates modes; Branch A then builds maps using them.

        Given: settings set explicit mode for ticks + auto for trades/candles,
        And: one scope grant pair (kraken, BTC-USD),
        When: initial_rebuild runs,
        Then: ticks resolves via explicit allowlist; trades + candles via
            the scope pair (mode is auto).
        """
        settings = {
            "market_persist_ticks": {
                "mode": "explicit",
                "exchanges": {"kraken": ["ETH-USD"]},
            },
            "market_persist_trades": {"mode": "auto"},
            "market_persist_candles": {"mode": "auto"},
            "market_persist_extra": {"ticks": {}, "trades": {}, "candles": {}},
            "market_persist_exclude": {"ticks": {}, "trades": {}, "candles": {}},
        }
        policy, _, _ = _build_policy(
            settings=settings,
            operators=[_operator_row("op-1")],
            pairs={("kraken", "BTC-USD")},
        )

        await policy.initial_rebuild()

        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "ETH-USD") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is False
        assert policy.should_persist(ExchangeEnum.KRAKEN, "trades", "BTC-USD") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "candles", "BTC-USD") is True


class TestAutoModeResolution:
    """Auto mode unions scope-grant pairs into the per-type frozensets."""

    @pytest.mark.asyncio
    async def test_auto_mode_uses_scope_pair_union(self) -> None:
        """Three exchanges in the pair set produce three independent frozensets."""
        pairs: set[tuple[str, str]] = {
            ("kraken", "BTC-USD"),
            ("kraken_futures", "PI_XBTUSD"),
            ("walutomat", "EUR-PLN"),
        }
        policy, _, _ = _build_policy(
            operators=[_operator_row("op-1")],
            pairs=pairs,
        )
        await policy.initial_rebuild()

        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN_FUTURES, "trades", "PI_XBTUSD") is True
        assert policy.should_persist(ExchangeEnum.WALUTOMAT, "candles", "EUR-PLN") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "DOES-NOT-EXIST") is False

    @pytest.mark.asyncio
    async def test_auto_mode_empty_when_no_operators(self) -> None:
        """No active operators short-circuits Branch A; persist set is empty."""
        policy, _, _ = _build_policy(operators=[], pairs=set())
        await policy.initial_rebuild()
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is False
        assert policy.wallet_scope_pairs_for(ExchangeEnum.KRAKEN) == frozenset()


class TestExplicitModeResolution:
    """Explicit mode reads the configured ``exchanges`` allowlist verbatim."""

    @pytest.mark.asyncio
    async def test_explicit_mode_uses_configured_exchanges(self) -> None:
        """Explicit allowlist is the source of truth; scope pairs ignored for that type."""
        settings = {
            "market_persist_ticks": {
                "mode": "explicit",
                "exchanges": {"kraken": ["ETH-USD", "DOGE-USD"]},
            },
        }
        policy, _, _ = _build_policy(
            settings=settings,
            operators=[_operator_row("op-1")],
            pairs={("kraken", "BTC-USD")},
        )
        await policy.initial_rebuild()

        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "ETH-USD") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "DOGE-USD") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is False

    @pytest.mark.asyncio
    async def test_explicit_mode_wildcard_sentinel_persists_every_symbol(self) -> None:
        """``"*"`` in the explicit allowlist matches every native symbol.

        Given: operator configures ``exchanges = {"kraken": ["*"]}`` to
            persist every Kraken instrument under wildcard subscribe,
        When: ``should_persist`` is queried for any symbol,
        Then: the wildcard short-circuit returns True without enumerating
            the actual symbol — used for "persist everything from this
            exchange" without listing every native_symbol.
        """
        settings = {
            "market_persist_ticks": {
                "mode": "explicit",
                "exchanges": {"kraken": ["*"]},
            },
            "market_persist_trades": {"mode": "explicit", "exchanges": {}},
            "market_persist_candles": {"mode": "explicit", "exchanges": {}},
        }
        policy, _, _ = _build_policy(
            settings=settings,
            operators=[_operator_row("op-1")],
            pairs=set(),
        )
        await policy.initial_rebuild()

        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "ANY-NEW-LISTING") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN_FUTURES, "ticks", "PI_XBTUSD") is False


class TestOverlays:
    """``market_persist_extra`` adds; ``market_persist_exclude`` subtracts."""

    @pytest.mark.asyncio
    async def test_extra_overlay_adds_to_base_set(self) -> None:
        """Extra symbols land in the resolved set even when not scope-granted."""
        settings = {
            "market_persist_extra": {
                "ticks": {"kraken": ["MEME-USD"]},
                "trades": {},
                "candles": {},
            },
        }
        policy, _, _ = _build_policy(
            settings=settings,
            operators=[_operator_row("op-1")],
            pairs={("kraken", "BTC-USD")},
        )
        await policy.initial_rebuild()

        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "MEME-USD") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "trades", "MEME-USD") is False

    @pytest.mark.asyncio
    async def test_exclude_overlay_subtracts_from_base_set(self) -> None:
        """Exclude removes a symbol even when scope-granted."""
        settings = {
            "market_persist_exclude": {
                "ticks": {"kraken": ["BTC-USD"]},
                "trades": {},
                "candles": {},
            },
        }
        policy, _, _ = _build_policy(
            settings=settings,
            operators=[_operator_row("op-1")],
            pairs={("kraken", "BTC-USD"), ("kraken", "ETH-USD")},
        )
        await policy.initial_rebuild()

        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is False
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "ETH-USD") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "candles", "BTC-USD") is True


class TestIntrospection:
    """Public accessors for safety rail + prewarm."""

    @pytest.mark.asyncio
    async def test_iter_persisted_instruments_yields_resolved_pairs(self) -> None:
        """Iterator returns one tuple per (exchange, native_symbol) in the resolved map."""
        policy, _, _ = _build_policy(
            operators=[_operator_row("op-1")],
            pairs={("kraken", "BTC-USD"), ("walutomat", "EUR-PLN")},
        )
        await policy.initial_rebuild()

        pairs = set(policy.iter_persisted_instruments("ticks"))
        assert (ExchangeEnum.KRAKEN, "BTC-USD") in pairs
        assert (ExchangeEnum.WALUTOMAT, "EUR-PLN") in pairs

    @pytest.mark.asyncio
    async def test_mode_for_returns_configured_value(self) -> None:
        """mode_for returns the configured mode per data type."""
        settings = {
            "market_persist_ticks": {
                "mode": "explicit",
                "exchanges": {"kraken": []},
            },
        }
        policy, _, _ = _build_policy(settings=settings)
        await policy.initial_rebuild()
        assert policy.mode_for(ExchangeEnum.KRAKEN, "ticks") == "explicit"
        assert policy.mode_for(ExchangeEnum.KRAKEN, "trades") == "auto"

    @pytest.mark.asyncio
    async def test_extra_for_returns_overlay_set(self) -> None:
        """extra_for returns the per-(exchange, data_type) overlay-include set."""
        settings = {
            "market_persist_extra": {
                "ticks": {"kraken": ["X1", "X2"]},
                "trades": {},
                "candles": {},
            },
        }
        policy, _, _ = _build_policy(settings=settings)
        await policy.initial_rebuild()
        assert policy.extra_for(ExchangeEnum.KRAKEN, "ticks") == frozenset({"X1", "X2"})
        assert policy.extra_for(ExchangeEnum.KRAKEN, "trades") == frozenset()


class TestMalformedSettingsPreservation:
    """Strict shape validation: top-level malformed settings preserve prior state.

    Per plan v6 Branch B contract: a top-level shape mismatch on any of
    the five ``market_persist_*`` settings aborts the refresh and leaves
    the previously-applied policy state intact. Per-element drops
    (non-string symbols, unknown exchanges within an otherwise-valid
    list) stay as best-effort warnings.
    """

    @pytest.mark.asyncio
    async def test_non_dict_persist_setting_aborts_branch_b_in_initial_rebuild(
        self,
    ) -> None:
        """A malformed persist setting aborts Branch B; Branch A still runs.

        Constructor defaults (mode=auto for every type, empty overlays)
        remain in place after the aborted Branch B. Branch A then
        populates the scope-pair set, so the policy still serves a
        defensible default verdict instead of returning a stale or
        partially-applied state.
        """
        settings = {"market_persist_ticks": "not-a-dict"}
        policy, _, _ = _build_policy(
            settings=settings,
            operators=[_operator_row("op-1")],
            pairs={("kraken", "BTC-USD")},
        )
        await policy.initial_rebuild()
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is True
        assert policy.wallet_scope_pairs_for(ExchangeEnum.KRAKEN) == frozenset({"BTC-USD"})

    @pytest.mark.asyncio
    async def test_invalid_mode_string_preserves_prior_state(self) -> None:
        """``{"mode": "ignored"}`` aborts the refresh; prior state preserved."""
        settings = {"market_persist_ticks": {"mode": "ignored"}}
        policy, _, _ = _build_policy(settings=settings)
        await policy.initial_rebuild()
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is False

    @pytest.mark.asyncio
    async def test_runtime_settings_event_preserves_state_on_malformed(self) -> None:
        """Runtime ``system.settings`` with malformed payload leaves policy intact.

        Seeds the policy with valid settings + scope pair, dispatches a
        settings event after flipping ``market_persist_ticks`` to
        malformed, and verifies the original persist verdict still holds.
        """
        good_settings: dict[str, Any] = {
            "market_persist_ticks": {"mode": "auto"},
            "market_persist_trades": {"mode": "auto"},
            "market_persist_candles": {"mode": "auto"},
        }
        policy, _, stub_settings = _build_policy(
            settings=good_settings,
            operators=[_operator_row("op-1")],
            pairs={("kraken", "BTC-USD")},
        )
        await policy.initial_rebuild()
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is True

        stub_settings._values["market_persist_ticks"] = "not-a-dict"
        await policy._dispatch_topic("system.settings")

        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is True
        assert policy.mode_for(ExchangeEnum.KRAKEN, "ticks") == "auto"

    @pytest.mark.asyncio
    async def test_unknown_exchange_in_pair_set_is_dropped(self) -> None:
        """Repository returning a pair with an unknown exchange logs + drops."""
        policy, _, _ = _build_policy(
            operators=[_operator_row("op-1")],
            pairs={("bogus_exchange", "BTC-USD"), ("kraken", "BTC-USD")},
        )
        await policy.initial_rebuild()
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is True
        assert policy.wallet_scope_pairs_for(ExchangeEnum.KRAKEN) == frozenset({"BTC-USD"})

    @pytest.mark.asyncio
    async def test_non_dict_overlay_setting_preserves_state(self) -> None:
        """Non-dict ``market_persist_extra`` aborts the refresh; state preserved."""
        settings = {"market_persist_extra": "not-a-dict"}
        policy, _, _ = _build_policy(settings=settings)
        await policy.initial_rebuild()
        assert policy.extra_for(ExchangeEnum.KRAKEN, "ticks") == frozenset()

    @pytest.mark.asyncio
    async def test_non_dict_overlay_inner_preserves_state(self) -> None:
        """Non-dict per-data-type overlay entry aborts the refresh."""
        settings = {
            "market_persist_extra": {
                "ticks": "should-be-dict",
                "trades": {},
                "candles": {},
            },
        }
        policy, _, _ = _build_policy(settings=settings)
        await policy.initial_rebuild()
        assert policy.extra_for(ExchangeEnum.KRAKEN, "ticks") == frozenset()

    @pytest.mark.asyncio
    async def test_non_list_overlay_symbols_are_skipped(self) -> None:
        """Symbols not a list under (overlay, exchange) silently drop."""
        settings = {
            "market_persist_extra": {
                "ticks": {"kraken": "not-a-list"},
                "trades": {},
                "candles": {},
            },
        }
        policy, _, _ = _build_policy(settings=settings)
        await policy.initial_rebuild()
        assert policy.extra_for(ExchangeEnum.KRAKEN, "ticks") == frozenset()

    @pytest.mark.asyncio
    async def test_non_string_symbol_entries_are_dropped(self) -> None:
        """Non-string entries in an extra list are dropped with a warning."""
        settings = {
            "market_persist_extra": {
                "ticks": {"kraken": ["GOOD", 42, "ALSO-GOOD"]},
                "trades": {},
                "candles": {},
            },
        }
        policy, _, _ = _build_policy(settings=settings)
        await policy.initial_rebuild()
        assert policy.extra_for(ExchangeEnum.KRAKEN, "ticks") == frozenset({"GOOD", "ALSO-GOOD"})

    @pytest.mark.asyncio
    async def test_unknown_exchange_in_overlay_is_skipped(self) -> None:
        """Overlay entry under an unknown exchange logs + drops."""
        settings = {
            "market_persist_extra": {
                "ticks": {"bogus": ["X"]},
                "trades": {},
                "candles": {},
            },
        }
        policy, _, _ = _build_policy(settings=settings)
        await policy.initial_rebuild()
        assert policy.extra_for(ExchangeEnum.KRAKEN, "ticks") == frozenset()

    @pytest.mark.asyncio
    async def test_explicit_mode_non_dict_exchanges_preserves_state(self) -> None:
        """Explicit mode with non-dict exchanges aborts the refresh."""
        settings = {
            "market_persist_ticks": {"mode": "explicit", "exchanges": "not-a-dict"},
        }
        policy, _, _ = _build_policy(settings=settings)
        await policy.initial_rebuild()
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is False


class TestEmptyPolicyFallbacks:
    """An uninitialised policy returns conservative defaults."""

    def test_should_persist_returns_false_before_rebuild(self) -> None:
        """Reads on a policy that never rebuilt return False (don't write)."""
        policy, _, _ = _build_policy()
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is False

    def test_mode_for_returns_auto_before_rebuild(self) -> None:
        """Default mode is auto before any settings refresh."""
        policy, _, _ = _build_policy()
        assert policy.mode_for(ExchangeEnum.KRAKEN, "ticks") == "auto"

    def test_wallet_scope_pairs_for_returns_empty_before_rebuild(self) -> None:
        """Unknown exchange yields an empty frozenset."""
        policy, _, _ = _build_policy()
        assert policy.wallet_scope_pairs_for(ExchangeEnum.KRAKEN) == frozenset()


class TestRefreshFromSettings:
    """Branch B keeps the cached pair set when no per-type mode requests auto."""

    @pytest.mark.asyncio
    async def test_settings_dispatch_runs_branch_a_when_auto_present(self) -> None:
        """Branch B with any auto mode triggers a Branch A rebuild as well."""
        settings = {
            "market_persist_ticks": {"mode": "auto"},
        }
        policy, repo, _ = _build_policy(
            settings=settings,
            operators=[_operator_row("op-1")],
            pairs={("kraken", "BTC-USD")},
        )
        await policy.initial_rebuild()
        repo.list_scope_grant_instrument_pairs.reset_mock()

        await policy._dispatch_topic("system.settings")
        repo.list_scope_grant_instrument_pairs.assert_awaited()

    @pytest.mark.asyncio
    async def test_settings_dispatch_skips_branch_a_when_all_explicit(self) -> None:
        """All-explicit configuration leaves the cached pair set untouched."""
        settings = {
            "market_persist_ticks": {"mode": "explicit", "exchanges": {}},
            "market_persist_trades": {"mode": "explicit", "exchanges": {}},
            "market_persist_candles": {"mode": "explicit", "exchanges": {}},
        }
        policy, repo, _ = _build_policy(settings=settings)
        await policy.initial_rebuild()
        repo.list_scope_grant_instrument_pairs.reset_mock()

        await policy._dispatch_topic("system.settings")
        repo.list_scope_grant_instrument_pairs.assert_not_awaited()


class TestRefreshFromScopeGrants:
    """Admin scope events trigger Branch A refresh with operator list."""

    @pytest.mark.asyncio
    async def test_scope_revoked_dispatch_rebuilds_pair_set(self) -> None:
        """Admin event reloads the operator + scope pair query."""
        policy, repo, _ = _build_policy(
            operators=[_operator_row("op-1")],
            pairs={("kraken", "BTC-USD")},
        )
        await policy.initial_rebuild()
        repo.list_active_operators.reset_mock()
        repo.list_scope_grant_instrument_pairs.reset_mock()

        await policy._dispatch_topic("admin.scope_revoked")
        repo.list_active_operators.assert_awaited()
        repo.list_scope_grant_instrument_pairs.assert_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "topic",
        [
            "admin.scope_granted",
            "admin.scope_handed_over",
        ],
    )
    async def test_granted_and_handed_over_also_trigger_branch_a(self, topic: str) -> None:
        """Both new event topics trigger the same Branch A query."""
        policy, repo, _ = _build_policy(
            operators=[_operator_row("op-1")],
        )
        await policy.initial_rebuild()
        repo.list_active_operators.reset_mock()
        repo.list_scope_grant_instrument_pairs.reset_mock()

        await policy._dispatch_topic(topic)
        repo.list_active_operators.assert_awaited()
        repo.list_scope_grant_instrument_pairs.assert_awaited()

    @pytest.mark.asyncio
    async def test_unknown_topic_is_silently_ignored(self) -> None:
        """A topic outside the listened set does not refresh state."""
        policy, repo, _ = _build_policy(operators=[_operator_row("op-1")])
        await policy.initial_rebuild()
        repo.list_active_operators.reset_mock()
        repo.list_scope_grant_instrument_pairs.reset_mock()

        await policy._dispatch_topic("unrelated.topic")
        repo.list_active_operators.assert_not_awaited()
        repo.list_scope_grant_instrument_pairs.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dispatcher_swallows_handler_exception(self) -> None:
        """Handler failure logs but does not propagate to the listen loop."""
        policy, repo, _ = _build_policy(operators=[_operator_row("op-1")])
        await policy.initial_rebuild()
        repo.list_active_operators.side_effect = RuntimeError("DB down")

        await policy._dispatch_topic("admin.scope_revoked")


class TestListenerLifecycle:
    """``start_admin_listener`` is idempotent + ``stop`` cleans up resources."""

    @pytest.mark.asyncio
    async def test_empty_broker_skips_listener(self) -> None:
        """Empty XPUB endpoint short-circuits; no socket, no task."""
        policy, _, _ = _build_policy()
        await policy.start_admin_listener("")
        assert policy._listen_task is None
        assert policy._subscriber is None

    @pytest.mark.asyncio
    async def test_stop_without_start_is_idempotent(self) -> None:
        """Calling stop on a never-started policy is a no-op."""
        policy, _, _ = _build_policy()
        await policy.stop()
        assert policy._listen_task is None


class TestCandleAndTradeIndependence:
    """Each data type carries its own resolved frozenset map."""

    @pytest.mark.asyncio
    async def test_explicit_on_ticks_does_not_affect_trades(self) -> None:
        """Mode + overlay state for ticks is independent of trades + candles."""
        settings = {
            "market_persist_ticks": {
                "mode": "explicit",
                "exchanges": {"kraken": ["ONLY-TICKS"]},
            },
            "market_persist_trades": {"mode": "auto"},
            "market_persist_candles": {"mode": "auto"},
        }
        policy, _, _ = _build_policy(
            settings=settings,
            operators=[_operator_row("op-1")],
            pairs={("kraken", "FROM-GRANT")},
        )
        await policy.initial_rebuild()

        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "ONLY-TICKS") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "FROM-GRANT") is False
        assert policy.should_persist(ExchangeEnum.KRAKEN, "trades", "FROM-GRANT") is True
        assert policy.should_persist(ExchangeEnum.KRAKEN, "candles", "FROM-GRANT") is True


class TestListenLoopDispatch:
    """Direct tests for :meth:`_listen_loop` + :meth:`_recv_one_frame`."""

    @pytest.mark.asyncio
    async def test_listen_loop_returns_early_when_subscriber_none(self) -> None:
        """A loop entered without a subscriber returns immediately."""
        policy, _, _ = _build_policy()
        policy._subscriber = None
        policy._running = True
        await policy._listen_loop()

    @pytest.mark.asyncio
    async def test_listen_loop_dispatches_received_frames(self) -> None:
        """Each successful recv triggers a topic dispatch then exits on ``running=False``."""
        policy, repo, _ = _build_policy(operators=[_operator_row("op-1")])
        await policy.initial_rebuild()
        repo.list_active_operators.reset_mock()

        call_count = {"n": 0}

        async def _recv_then_stop() -> tuple[bytes, bytes]:
            call_count["n"] += 1
            policy._running = False
            return (b"admin.scope_revoked", b"{}")

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_recv_then_stop)
        policy._subscriber = subscriber
        policy._running = True

        await policy._listen_loop()
        assert call_count["n"] == 1
        repo.list_active_operators.assert_awaited()

    @pytest.mark.asyncio
    async def test_listen_loop_skips_when_recv_returns_none(self) -> None:
        """A transient recv failure (frame=None) is skipped without dispatch."""
        policy, repo, _ = _build_policy()
        await policy.initial_rebuild()
        repo.list_active_operators.reset_mock()

        async def _fail_then_stop() -> tuple[bytes, bytes]:
            policy._running = False
            raise RuntimeError("boom")

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_fail_then_stop)
        policy._subscriber = subscriber
        policy._running = True

        await policy._listen_loop()
        repo.list_active_operators.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_recv_one_frame_decodes_bytes_topic_and_payload(self) -> None:
        """Both bytes and str topics decode cleanly into the (topic, payload) tuple."""
        policy, _, _ = _build_policy()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(return_value=(b"admin.scope_revoked", b'{"x": 1}'))

        result = await policy._recv_one_frame(subscriber)
        assert result == ("admin.scope_revoked", '{"x": 1}')

    @pytest.mark.asyncio
    async def test_recv_one_frame_accepts_str_inputs(self) -> None:
        """Non-bytes topic/payload (e.g. test fakes) are coerced via ``str``."""
        policy, _, _ = _build_policy()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(return_value=("topic-as-str", 42))

        result = await policy._recv_one_frame(subscriber)
        assert result == ("topic-as-str", "42")

    @pytest.mark.asyncio
    async def test_recv_one_frame_returns_none_on_failure(self) -> None:
        """A non-cancellation recv error logs + returns None (no propagation)."""
        policy, _, _ = _build_policy()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=RuntimeError("recv fail"))

        result = await policy._recv_one_frame(subscriber)
        assert result is None

    @pytest.mark.asyncio
    async def test_recv_one_frame_propagates_cancellation(self) -> None:
        """``CancelledError`` from the socket recv must propagate to the loop."""
        policy, _, _ = _build_policy()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())

        with pytest.raises(asyncio.CancelledError):
            await policy._recv_one_frame(subscriber)

    @pytest.mark.asyncio
    async def test_dispatch_topic_propagates_cancellation(self) -> None:
        """``CancelledError`` raised by a handler propagates out of dispatch."""
        policy, repo, _ = _build_policy(operators=[_operator_row("op-1")])
        repo.list_active_operators.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await policy._dispatch_topic("admin.scope_revoked")

    @pytest.mark.asyncio
    async def test_listen_loop_propagates_cancellation_with_log(self) -> None:
        """``CancelledError`` from recv unwinds the loop after the info log."""
        policy, _, _ = _build_policy()

        async def _cancel() -> tuple[bytes, bytes]:
            raise asyncio.CancelledError()

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_cancel)
        policy._subscriber = subscriber
        policy._running = True

        with pytest.raises(asyncio.CancelledError):
            await policy._listen_loop()


class TestListenerStartIdempotency:
    """``start_admin_listener`` is idempotent across re-entries."""

    @pytest.mark.asyncio
    async def test_start_with_running_task_returns_early(self) -> None:
        """A second start while a task is still running short-circuits."""
        policy, _, _ = _build_policy()
        live_task = asyncio.create_task(asyncio.sleep(10))
        policy._listen_task = live_task
        try:
            await policy.start_admin_listener("tcp://nowhere:9999")
            assert policy._listen_task is live_task
        finally:
            live_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await live_task

    @pytest.mark.asyncio
    async def test_start_reaps_completed_prior_task(self) -> None:
        """A done-but-not-reaped prior task triggers cleanup then no-op (empty xpub)."""
        policy, _, _ = _build_policy()

        async def _done() -> None:
            return None

        finished = asyncio.create_task(_done())
        await finished
        policy._listen_task = finished

        await policy.start_admin_listener("")
        assert policy._listen_task is None

    @pytest.mark.asyncio
    async def test_start_with_real_endpoint_creates_subscriber(self) -> None:
        """A non-empty endpoint allocates a SUB socket and spawns the listen task.

        Uses an in-process ``inproc://`` endpoint so the test does not
        require an external broker; the listen task is cancelled
        before any frames are received.
        """
        policy, _, _ = _build_policy()
        endpoint = "inproc://market-persist-policy-test"

        await policy.start_admin_listener(endpoint)
        try:
            assert policy._listen_task is not None
            assert policy._subscriber is not None
        finally:
            await policy.stop()


class TestAtomicRebuildSwap:
    """Concurrent settings + scope refresh do not corrupt the resolved maps."""

    @pytest.mark.asyncio
    async def test_concurrent_refresh_serialises_under_lock(self) -> None:
        """Two refresh tasks complete without corrupting state."""
        policy, _, _ = _build_policy(
            operators=[_operator_row("op-1")],
            pairs={("kraken", "BTC-USD")},
        )
        await asyncio.gather(
            policy._refresh_from_scope_grants(),
            policy._refresh_from_scope_grants(),
        )
        assert policy.should_persist(ExchangeEnum.KRAKEN, "ticks", "BTC-USD") is True
