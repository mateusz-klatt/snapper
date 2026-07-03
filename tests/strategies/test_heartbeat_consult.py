"""Tests for the HeartbeatConsult wake-path strategy.

Fast-smoke coverage of plan P1
(``plan_2026_07_03_strategy_runtime_split_and_mcp_wake.md``): construction
fail-fast validation (paper-only, scoped config, UUID7 identity params,
deadline bounds), one-consult-per-window dedup, approved-outcome
target-flat emission with AI-review attribution, and fail-soft behavior
for every consult error mode. The AI-review service and repository are
mocked — the end-to-end DB path lives in the ai_review test suites.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid7

import pytest

from snapper.application.ai_review.service import AiReviewCreateRequest
from snapper.application.ai_review.service import AiReviewDecisionOutcome
from snapper.application.ai_review.service import DelegateBusyError
from snapper.application.ai_review.service import NoLiveDelegateError
from snapper.application.services.signals.service import signal_service
from snapper.core.types import AiReviewStatusEnum
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import StrategyConfig
from snapper.strategies.heartbeat_consult import CONSULT_SEQUENCE_STREAM
from snapper.strategies.heartbeat_consult import DEFAULT_CONSULT_DEADLINE_SECONDS
from snapper.strategies.heartbeat_consult import HeartbeatConsult

_OPEN_AT = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _mock_signal_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace signal persistence with a no-op for these unit tests."""
    monkeypatch.setattr(signal_service, "store_signal", AsyncMock(return_value=""))


def _consult_params(**overrides: Any) -> dict[str, Any]:
    """Build valid consult params, overridable per test."""
    params: dict[str, Any] = {
        "ai_review_user_public_id": str(uuid7()),
        "ai_review_strategy_public_id": str(uuid7()),
        "ai_review_deadline_seconds": DEFAULT_CONSULT_DEADLINE_SECONDS,
    }
    params.update(overrides)
    return params


def _config(**overrides: Any) -> StrategyConfig:
    """Build a valid scoped paper config, overridable per test."""
    base: dict[str, Any] = {
        "name": "heartbeat_consult_test",
        "strategy_class": "HeartbeatConsult",
        "inputs": ["market.kraken.BTC-USD.candles.1h"],
        "outputs": ["BTC-USD"],
        "exchange": "paper",
        "params": _consult_params(),
        "wallet_public_id": str(uuid7()),
        "operator_public_id": str(uuid7()),
    }
    base.update(overrides)
    return StrategyConfig(**base)


def _candle(open_at: datetime = _OPEN_AT, close: float = 50000.0) -> CandleData:
    """Build a 1h kraken candle at the given window."""
    return CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        instrument="BTC-USD",
        timeframe="1h",
        open=close - 100,
        high=close + 100,
        low=close - 200,
        close=close,
        volume=10.0,
        exchange="kraken",
        timestamp=open_at,
        open_at=open_at,
    )


def _approved_outcome() -> AiReviewDecisionOutcome:
    """Build a terminal approved outcome."""
    return AiReviewDecisionOutcome(
        review_public_id=str(uuid7()),
        status=AiReviewStatusEnum.RESOLVED_APPROVED,
        resolution_mode=None,
        decision=None,
        rationale=None,
        dispatch_version=1,
        responding_delegate_public_id=str(uuid7()),
    )


def _rejected_outcome() -> AiReviewDecisionOutcome:
    """Build a terminal rejected outcome."""
    return AiReviewDecisionOutcome(
        review_public_id=str(uuid7()),
        status=AiReviewStatusEnum.RESOLVED_REJECTED,
        resolution_mode=None,
        decision=None,
        rationale=None,
        dispatch_version=1,
        responding_delegate_public_id=str(uuid7()),
    )


class TestHeartbeatConsultConstruction:
    """Fail-fast validation at construction time."""

    def test_valid_config_constructs(self) -> None:
        """Verify a scoped paper config with UUID7 params constructs.

        Given: A paper config with wallet/operator scope and UUID7 params,
        When: HeartbeatConsult is instantiated,
        Then: Consult identity attributes are bound.
        """
        config = _config()
        strategy = HeartbeatConsult(config)
        assert strategy.consult_user_public_id == config.params["ai_review_user_public_id"]
        assert strategy.consult_strategy_public_id == config.params["ai_review_strategy_public_id"]
        assert strategy.consult_deadline_seconds == DEFAULT_CONSULT_DEADLINE_SECONDS

    def test_non_paper_exchange_rejected(self) -> None:
        """Verify a live exchange is rejected.

        Given: A config targeting kraken,
        When: HeartbeatConsult is instantiated,
        Then: ValueError marks the strategy paper-only.
        """
        with pytest.raises(ValueError, match="paper-only"):
            HeartbeatConsult(_config(exchange="kraken"))

    def test_unscoped_config_rejected(self) -> None:
        """Verify missing wallet/operator scope is rejected.

        Given: A config without wallet and operator identities,
        When: HeartbeatConsult is instantiated,
        Then: ValueError demands a scoped config.
        """
        with pytest.raises(ValueError, match="scoped config"):
            HeartbeatConsult(_config(wallet_public_id="", operator_public_id=""))

    def test_non_uuid7_user_rejected(self) -> None:
        """Verify a non-UUID7 user identity is rejected.

        Given: Params with a plain-string user id,
        When: HeartbeatConsult is instantiated,
        Then: ValueError names 'ai_review_user_public_id'.
        """
        params = _consult_params(ai_review_user_public_id="not-a-uuid")
        with pytest.raises(ValueError, match="ai_review_user_public_id"):
            HeartbeatConsult(_config(params=params))

    def test_non_uuid7_strategy_rejected(self) -> None:
        """Verify a non-UUID7 strategy identity is rejected.

        Given: Params with an empty strategy id,
        When: HeartbeatConsult is instantiated,
        Then: ValueError names 'ai_review_strategy_public_id'.
        """
        params = _consult_params(ai_review_strategy_public_id="")
        with pytest.raises(ValueError, match="ai_review_strategy_public_id"):
            HeartbeatConsult(_config(params=params))

    def test_deadline_out_of_range_rejected(self) -> None:
        """Verify an out-of-range deadline is rejected.

        Given: Params with a 301s deadline,
        When: HeartbeatConsult is instantiated,
        Then: ValueError names 'ai_review_deadline_seconds'.
        """
        params = _consult_params(ai_review_deadline_seconds=301)
        with pytest.raises(ValueError, match="ai_review_deadline_seconds"):
            HeartbeatConsult(_config(params=params))

    def test_missing_deadline_rejected(self) -> None:
        """Verify an absent deadline param is rejected.

        Given: Params without 'ai_review_deadline_seconds',
        When: HeartbeatConsult is instantiated,
        Then: ValueError names the param (0 falls outside the range).
        """
        params = _consult_params()
        del params["ai_review_deadline_seconds"]
        with pytest.raises(ValueError, match="ai_review_deadline_seconds"):
            HeartbeatConsult(_config(params=params))


class TestHeartbeatConsultOnCandle:
    """Per-window consult dispatch and emission semantics."""

    @pytest.mark.asyncio
    async def test_approved_outcome_emits_target_flat(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify an approved consult emits the target-flat heartbeat.

        Given: A consult resolving RESOLVED_APPROVED,
        When: A new 1h candle arrives,
        Then: emit_signal publishes strength 0.0 with the outcome stamped.
        """
        strategy = HeartbeatConsult(_config())
        outcome = _approved_outcome()
        monkeypatch.setattr(strategy, "_consult", AsyncMock(return_value=outcome))
        emit = AsyncMock()
        monkeypatch.setattr(strategy, "emit_signal", emit)
        result = await strategy.on_candle("BTC-USD", _candle())
        assert result is None
        emit.assert_awaited_once()
        assert emit.await_args is not None
        signal = emit.await_args.args[0]
        assert signal.strength == 0.0
        assert signal.instrument == "BTC-USD"
        assert emit.await_args.kwargs["outcome"] is outcome

    @pytest.mark.asyncio
    async def test_emit_failure_is_fail_soft(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify an emit-path failure never escapes into the listen loop.

        Given: An approved consult and emit_signal raising,
        When: on_candle runs,
        Then: None is returned and nothing propagates (the strategy
            keeps probing next windows).
        """
        strategy = HeartbeatConsult(_config())
        monkeypatch.setattr(strategy, "_consult", AsyncMock(return_value=_approved_outcome()))
        monkeypatch.setattr(
            strategy, "emit_signal", AsyncMock(side_effect=RuntimeError("publisher down"))
        )
        assert await strategy.on_candle("BTC-USD", _candle()) is None

    @pytest.mark.asyncio
    async def test_rejected_outcome_does_not_emit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a rejected consult never emits.

        Given: A consult resolving RESOLVED_REJECTED,
        When: A new 1h candle arrives,
        Then: No signal is emitted.
        """
        strategy = HeartbeatConsult(_config())
        monkeypatch.setattr(strategy, "_consult", AsyncMock(return_value=_rejected_outcome()))
        emit = AsyncMock()
        monkeypatch.setattr(strategy, "emit_signal", emit)
        assert await strategy.on_candle("BTC-USD", _candle()) is None
        emit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_none_outcome_does_not_emit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify a fallen-through consult never emits.

        Given: A consult returning None (no delegate / error),
        When: A new 1h candle arrives,
        Then: No signal is emitted.
        """
        strategy = HeartbeatConsult(_config())
        monkeypatch.setattr(strategy, "_consult", AsyncMock(return_value=None))
        emit = AsyncMock()
        monkeypatch.setattr(strategy, "emit_signal", emit)
        assert await strategy.on_candle("BTC-USD", _candle()) is None
        emit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_same_window_consults_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify revised bars of an already-consulted window are skipped.

        Given: Two candles sharing the same open_at,
        When: Both flow through on_candle,
        Then: The consult runs exactly once.
        """
        strategy = HeartbeatConsult(_config())
        consult = AsyncMock(return_value=None)
        monkeypatch.setattr(strategy, "_consult", consult)
        await strategy.on_candle("BTC-USD", _candle())
        await strategy.on_candle("BTC-USD", _candle(close=51000.0))
        consult.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_older_window_skipped_newer_consults(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify ordering: older bars skip, a newer window consults again.

        Given: A consulted window, then an older bar, then a newer bar,
        When: All flow through on_candle,
        Then: Only the two distinct forward windows consult.
        """
        strategy = HeartbeatConsult(_config())
        consult = AsyncMock(return_value=None)
        monkeypatch.setattr(strategy, "_consult", consult)
        await strategy.on_candle("BTC-USD", _candle())
        await strategy.on_candle("BTC-USD", _candle(open_at=_OPEN_AT - timedelta(hours=1)))
        await strategy.on_candle("BTC-USD", _candle(open_at=_OPEN_AT + timedelta(hours=1)))
        assert consult.await_count == 2

    @pytest.mark.asyncio
    async def test_reset_clears_consult_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify replay reset re-arms the per-window dedup guard.

        Given: A consulted window followed by reset(),
        When: The same window's candle arrives again,
        Then: The consult runs a second time.
        """
        strategy = HeartbeatConsult(_config())
        consult = AsyncMock(return_value=None)
        monkeypatch.setattr(strategy, "_consult", consult)
        await strategy.on_candle("BTC-USD", _candle())
        await strategy.reset()
        await strategy.on_candle("BTC-USD", _candle())
        assert consult.await_count == 2


class TestHeartbeatConsultConsult:
    """Request construction and fail-soft error handling."""

    def _wire(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        instrument_public_id: str | None,
        primitive: AsyncMock,
    ) -> AsyncMock:
        """Patch repository + primitive for a consult round, return the repo mock."""
        repo = AsyncMock()
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=instrument_public_id)
        monkeypatch.setattr(
            "snapper.strategies.heartbeat_consult.get_repository", lambda db_url: repo
        )
        monkeypatch.setattr(
            "snapper.strategies.heartbeat_consult.create_ai_review_and_await", primitive
        )
        return repo

    @pytest.mark.asyncio
    async def test_builds_request_from_validated_identity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify the consult request carries every validated identity field.

        Given: A resolvable instrument and a succeeding primitive,
        When: _consult runs for a candle,
        Then: The AiReviewCreateRequest carries the config identities,
            the consult sequence stream, and the candle envelope.
        """
        config = _config()
        strategy = HeartbeatConsult(config)
        outcome = _approved_outcome()
        primitive = AsyncMock(return_value=outcome)
        instrument_public_id = str(uuid7())
        self._wire(
            monkeypatch,
            instrument_public_id=instrument_public_id,
            primitive=primitive,
        )
        candle = _candle()
        result = await strategy._consult("BTC-USD", candle)
        assert result is outcome
        primitive.assert_awaited_once()
        assert primitive.await_args is not None
        request = primitive.await_args.args[0]
        assert isinstance(request, AiReviewCreateRequest)
        assert request.user_public_id == config.params["ai_review_user_public_id"]
        assert request.strategy_public_id == config.params["ai_review_strategy_public_id"]
        assert request.operator_public_id == config.operator_public_id
        assert request.wallet_public_id == config.wallet_public_id
        assert request.instrument_public_id == instrument_public_id
        assert request.deadline_seconds == DEFAULT_CONSULT_DEADLINE_SECONDS
        assert request.session_id == strategy._tracker.session_id
        assert request.signal_envelope == {
            "kind": "heartbeat",
            "open_at": candle.open_at.isoformat(),
            "close": candle.close,
        }
        assert request.instrument_metadata == {"last_price": candle.close}
        assert primitive.await_args.kwargs["deadline_seconds"] == DEFAULT_CONSULT_DEADLINE_SECONDS

    @pytest.mark.asyncio
    async def test_consult_sequence_uses_named_stream(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify consult provenance advances the dedicated stream.

        Given: Two consecutive consult rounds,
        When: Both build requests,
        Then: sequence_id advances within the consult stream.
        """
        strategy = HeartbeatConsult(_config())
        primitive = AsyncMock(return_value=None)
        self._wire(monkeypatch, instrument_public_id=str(uuid7()), primitive=primitive)
        expected_first = strategy._tracker.next_sequence(CONSULT_SEQUENCE_STREAM) + 1
        await strategy._consult("BTC-USD", _candle())
        assert primitive.await_args is not None
        first = primitive.await_args.args[0].sequence_id
        await strategy._consult("BTC-USD", _candle())
        assert primitive.await_args is not None
        second = primitive.await_args.args[0].sequence_id
        assert first == expected_first
        assert second == first + 1

    @pytest.mark.asyncio
    async def test_unresolvable_instrument_skips_round(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a missing instrument row skips the round without a create.

        Given: No active instrument row for the symbol,
        When: _consult runs,
        Then: None is returned and the primitive is never awaited.
        """
        strategy = HeartbeatConsult(_config())
        primitive = AsyncMock(return_value=_approved_outcome())
        self._wire(monkeypatch, instrument_public_id=None, primitive=primitive)
        assert await strategy._consult("BTC-USD", _candle()) is None
        primitive.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "admission_error",
        [NoLiveDelegateError("no delegate"), DelegateBusyError("busy")],
    )
    async def test_admission_errors_fall_through(
        self, monkeypatch: pytest.MonkeyPatch, admission_error: Exception
    ) -> None:
        """Verify admission-control errors fall through to None.

        Given: The primitive raising an admission error,
        When: _consult runs,
        Then: None is returned and nothing propagates.
        """
        strategy = HeartbeatConsult(_config())
        primitive = AsyncMock(side_effect=admission_error)
        self._wire(monkeypatch, instrument_public_id=str(uuid7()), primitive=primitive)
        assert await strategy._consult("BTC-USD", _candle()) is None

    @pytest.mark.asyncio
    async def test_unexpected_error_is_fail_soft(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify an unexpected error never escapes the consult round.

        Given: The primitive raising RuntimeError,
        When: _consult runs,
        Then: None is returned and nothing propagates.
        """
        strategy = HeartbeatConsult(_config())
        primitive = AsyncMock(side_effect=RuntimeError("boom"))
        self._wire(monkeypatch, instrument_public_id=str(uuid7()), primitive=primitive)
        assert await strategy._consult("BTC-USD", _candle()) is None
