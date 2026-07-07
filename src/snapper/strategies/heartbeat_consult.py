"""Heartbeat CONSULT strategy exercising the AI-review wake path.

Emits one AI-delegate CONSULT per completed higher-timeframe candle
(1h by default config) and, on an approved decision, publishes a
target-flat signal (``strength=0.0``) so the emit path with AI-review
attribution is exercised without ever opening a position. The strategy
exists to prove the strategy -> ``ai_reviews`` -> MCP-delegate wake ->
decision -> resume loop end-to-end (plan
``plan_2026_07_03_strategy_runtime_split_and_mcp_wake.md`` P1); it is
NOT a trading strategy and refuses to run outside the paper exchange.

Identity requirements: the outbound ``ai_reviews.{user}.{strategy}.request``
frame topic validates both ids as UUID7 at publish time while the review
row commits BEFORE the best-effort publish, so a non-UUID7 id would
create pending rows whose wake frames are silently dropped. Both params
are therefore validated fail-fast at construction.
"""

from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from typing import ClassVar

from loguru import logger

from snapper.application.ai_review.service import AiReviewCreateRequest
from snapper.application.ai_review.service import AiReviewDecisionOutcome
from snapper.application.ai_review.service import DelegateBusyError
from snapper.application.ai_review.service import NoLiveDelegateError
from snapper.application.ai_review.strategy_primitive import create_ai_review_and_await
from snapper.config.settings import get_bootstrap_settings
from snapper.core.ids import is_uuid7
from snapper.core.types import AiReviewStatusEnum
from snapper.core.types import ExchangeEnum
from snapper.core.types import TradeSideEnum
from snapper.data.repository import get_repository
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import StrategyConfig
from snapper.strategies.base import StrategySignal
from snapper.strategies.decorators import create_strategy_process
from snapper.strategies.decorators import register_strategy

CONSULT_SEQUENCE_STREAM = "ai_reviews.consult"
"""Named logical sequence stream for consult provenance.

Keeps ``sequence_id`` allocation for CONSULT rows separate from the
strategy's signal-topic streams so neither interleaves gaps into the
other.
"""

DEFAULT_CONSULT_DEADLINE_SECONDS = 25
"""Default decision deadline per consult round.

Kept well under the 1h bar interval and inside the 5-30s range the
:class:`AiReviewService` documents as typical, so a timed-out round
resolves long before the next bar can open a new one.
"""

MIN_CONSULT_DEADLINE_SECONDS = 5
MAX_CONSULT_DEADLINE_SECONDS = 300


@register_strategy("HeartbeatConsult")
@create_strategy_process(
    process_name="strategy_heartbeat_consult_btc_1h",
    default_config={
        "name": "heartbeat_consult_btc_1h",
        "inputs": ["market.kraken.BTC-USD.candles.1h"],
        "outputs": ["BTC-USD"],
        "exchange": ExchangeEnum.PAPER,
        "params": {
            "ai_review_user_public_id": "",
            "ai_review_strategy_public_id": "",
            "ai_review_deadline_seconds": DEFAULT_CONSULT_DEADLINE_SECONDS,
        },
    },
)
class HeartbeatConsult(BaseStrategy):
    """One CONSULT per new candle window; approved rounds emit target-flat.

    Paper-only by construction: the constructor rejects any non-paper
    exchange so the heartbeat can never carry live-order intent even if
    misconfigured. Every consult failure mode (no live delegate, all
    delegates busy, unresolvable instrument, unexpected errors) is
    fail-soft — the heartbeat loop must never crash the strategy.

    The consult awaits inline inside ``on_candle``, so THIS strategy's
    own listen loop pauses for up to ``consult_deadline_seconds`` per
    round. That is deliberate and safe here — the strategy has a single
    1h input and no other strategies share its process loop — but the
    pattern must not be copied into high-frequency strategies without a
    detached-task design.

    Attributes:
        consult_user_public_id: UUID7 of the strategy owner stamped on
            every review row (DISTINCT from delegate users).
        consult_strategy_public_id: Stable UUID7 identifying this
            strategy instance on review rows and wake-frame topics;
            seeded once by the operator in the process config.
        consult_deadline_seconds: Per-round decision deadline.
    """

    REFERENCE_IDENTITY_PARAMS: ClassVar[Mapping[str, str]] = {"ai_review_user_public_id": "user"}
    SEEDED_IDENTITY_PARAMS: ClassVar[tuple[str, ...]] = ("ai_review_strategy_public_id",)

    def __init__(self, config: StrategyConfig) -> None:
        """Validate consult identity params fail-fast and initialize state.

        Args:
            config: Strategy configuration; must use the paper exchange
                and carry UUID7 ``ai_review_user_public_id`` and
                ``ai_review_strategy_public_id`` params plus a sane
                ``ai_review_deadline_seconds``.

        Raises:
            ValueError: Non-paper exchange, missing/non-UUID7 identity
                params, or an out-of-range deadline.
        """
        super().__init__(config)
        if config.exchange != ExchangeEnum.PAPER:
            raise ValueError(
                f"Strategy {config.name}: HeartbeatConsult is paper-only, "
                f"got exchange '{config.exchange}'"
            )
        if not config.wallet_public_id or not config.operator_public_id:
            raise ValueError(
                f"Strategy {config.name}: HeartbeatConsult requires a scoped config "
                f"(wallet_public_id + operator_public_id) — admission control filters "
                f"delegates by operator membership and wallet grants, so an unscoped "
                f"heartbeat would silently never find a delegate"
            )
        user_public_id = str(self.params.get("ai_review_user_public_id", ""))
        if not is_uuid7(user_public_id):
            raise ValueError(
                f"Strategy {config.name}: param 'ai_review_user_public_id' must be a "
                f"canonical UUID7 (outbound ai_reviews.* topics reject anything else), "
                f"got '{user_public_id}'"
            )
        strategy_public_id = str(self.params.get("ai_review_strategy_public_id", ""))
        if not is_uuid7(strategy_public_id):
            raise ValueError(
                f"Strategy {config.name}: param 'ai_review_strategy_public_id' must be a "
                f"canonical UUID7 seeded once in the process config, "
                f"got '{strategy_public_id}'"
            )
        deadline_seconds = int(self.params.get("ai_review_deadline_seconds", 0) or 0)
        if not MIN_CONSULT_DEADLINE_SECONDS <= deadline_seconds <= MAX_CONSULT_DEADLINE_SECONDS:
            raise ValueError(
                f"Strategy {config.name}: param 'ai_review_deadline_seconds' must be in "
                f"[{MIN_CONSULT_DEADLINE_SECONDS}, {MAX_CONSULT_DEADLINE_SECONDS}], "
                f"got {deadline_seconds}"
            )
        self.consult_user_public_id = user_public_id
        self.consult_strategy_public_id = strategy_public_id
        self.consult_deadline_seconds = deadline_seconds
        self._last_consult_open_at: datetime | None = None

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Run one consult round per NEW candle window.

        A revised or republished bar for an already-consulted ``open_at``
        never re-consults (one decision per window regardless of the
        prior round's outcome). Approved rounds emit the target-flat
        heartbeat signal directly via :meth:`BaseStrategy.emit_signal`
        so the AI-review attribution is stamped; the callback itself
        always returns ``None``. The emit is wrapped fail-soft — an
        emit-path failure (publisher setup, send) must not escape into
        ``_listen_loop`` and stop the strategy, because the heartbeat's
        job is to keep probing the wake path every window.

        Args:
            instrument: The instrument symbol from the candle topic.
            candle: The triggering candle.

        Returns:
            Always ``None`` — emission happens inline with outcome
            attribution, never through the callback return path.
        """
        if self._last_consult_open_at is not None and candle.open_at <= self._last_consult_open_at:
            return None
        self._last_consult_open_at = candle.open_at
        outcome = await self._consult(instrument, candle)
        if outcome is None or outcome.status != AiReviewStatusEnum.RESOLVED_APPROVED:
            return None
        try:
            await self.emit_signal(
                StrategySignal(
                    instrument=instrument,
                    side=TradeSideEnum.BUY,
                    strength=0.0,
                    reason="heartbeat approved",
                    price=candle.close,
                ),
                outcome=outcome,
            )
        except Exception as exc:
            logger.warning(f"Strategy {self.name}: heartbeat emit failed — {exc}")
        return None

    async def reset(self) -> None:
        """Reset per-window consult state for replay."""
        self._last_consult_open_at = None
        logger.info(f"Strategy {self.name}: heartbeat consult state reset for replay")

    async def _consult(self, instrument: str, candle: CandleData) -> AiReviewDecisionOutcome | None:
        """Create + await one CONSULT round, fail-soft on every error.

        Resolves the instrument public id against the candle's SOURCE
        exchange (paper configs subscribe live-venue topics, and
        instrument rows live under the source venue), builds the
        :class:`AiReviewCreateRequest` with the validated identity
        params, and drives ``create_ai_review_and_await``. Uses the
        process-wide cached repository exactly like DB warmup does and
        never disposes it.

        Args:
            instrument: The instrument symbol from the candle topic.
            candle: The triggering candle supplying price context and
                the source exchange.

        Returns:
            The terminal decision outcome, or ``None`` when the round
            could not run or did not produce a decision.
        """
        try:
            repo = get_repository(get_bootstrap_settings().db_url)
            instrument_public_id = await repo.get_instrument_public_id_by_symbol(
                instrument, candle.exchange, datetime.now(UTC)
            )
            if instrument_public_id is None:
                logger.warning(
                    f"Strategy {self.name}: heartbeat consult skipped — no active "
                    f"instrument row for {instrument} on {candle.exchange}"
                )
                return None
            request = AiReviewCreateRequest(
                user_public_id=self.consult_user_public_id,
                operator_public_id=self.config.operator_public_id,
                wallet_public_id=self.config.wallet_public_id,
                instrument_public_id=instrument_public_id,
                strategy_public_id=self.consult_strategy_public_id,
                signal_envelope={
                    "kind": "heartbeat",
                    "open_at": candle.open_at.isoformat(),
                    "close": float(candle.close),
                },
                instrument_metadata={"last_price": float(candle.close)},
                deadline_seconds=self.consult_deadline_seconds,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(CONSULT_SEQUENCE_STREAM),
            )
            return await create_ai_review_and_await(
                request, repo=repo, deadline_seconds=self.consult_deadline_seconds
            )
        except (NoLiveDelegateError, DelegateBusyError) as exc:
            logger.info(
                f"Strategy {self.name}: heartbeat consult fell through — {type(exc).__name__}"
            )
            return None
        except Exception as exc:
            logger.warning(f"Strategy {self.name}: heartbeat consult failed — {exc}")
            return None
