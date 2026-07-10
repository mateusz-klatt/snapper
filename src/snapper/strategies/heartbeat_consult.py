"""Heartbeat CONSULT strategy exercising the AI-review wake path.

Emits one AI-delegate CONSULT per completed candle (1h by default
config) and, on an approved decision, publishes a signal whose strength
is the configurable ``heartbeat_signal_strength`` param. The default
(0.0) keeps the historical target-flat behaviour — the emit path with
AI-review attribution is exercised without ever opening a position —
while a value in ``[0.0, 1.0]`` lets an approved round emit an actionable
PAPER long so the full signal -> order -> fill -> position execution
plane can be exercised end-to-end. The strategy proves the strategy ->
``ai_reviews`` -> MCP-delegate wake -> decision -> resume loop
(plan ``plan_2026_07_03_strategy_runtime_split_and_mcp_wake.md`` P1) and
remains PAPER-ONLY by construction (the constructor rejects any non-paper
exchange), so even an actionable strength can never carry live-order
intent.

Two design points matter for reviewers of this module:

- **Detached consult round.** ``on_candle`` never awaits the consult
  inline — it spawns ``_consult_round`` as a task and returns
  immediately, so the strategy's listen loop keeps draining feed
  heartbeats while the delegate deliberates. The strategy health
  monitor classifies health from ``last_data_timestamp`` recency; an
  inline await used to stall the loop for the full decision deadline
  and page the operator with false "strategy degraded" alerts every
  round. With the round detached, the reported lag stays at the feed
  cadence regardless of the consult deadline, so the deadline may be
  raised (up to ``MAX_CONSULT_DEADLINE_SECONDS``) without any alert
  side effects. At most one round is in flight; a new window arriving
  mid-round is skipped WITHOUT consuming the window, so a later
  republish of that window can still consult once the round resolves.
  A dispatch gate closes while ``stop``/``reset`` drain the in-flight
  round so no fresh round can spawn in the drain gap — two separate
  flags (a permanent stop flag, a reset-scoped flag) so a reset
  unwound by a concurrent stop can never reopen the stop's gate.
- **Self-contained decision envelope.** Every consult carries a
  ``market`` snapshot (trailing SMA/RSI/range/volatility computed from
  the persisted candle history) plus the proposed action, so the
  delegate can decide without any follow-up lookups inside the decision
  deadline. The MCP surface (``get_ohlcv``, ``list_positions``, ...)
  remains available for deeper context when the delegate has time.
  Snapshot construction is fail-soft: on any error the consult still
  runs with the minimal legacy envelope.

Identity requirements: the outbound ``ai_reviews.{user}.{strategy}.request``
frame topic validates both ids as UUID7 at publish time while the review
row commits BEFORE the best-effort publish, so a non-UUID7 id would
create pending rows whose wake frames are silently dropped. Both params
are therefore validated fail-fast at construction.
"""

import asyncio
import contextlib
import math
import statistics
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
from snapper.core.json_types import JsonObject
from snapper.core.types import AiReviewStatusEnum
from snapper.core.types import ExchangeEnum
from snapper.core.types import TradeSideEnum
from snapper.data.repository import Repository
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
resolves long before the next bar can open a new one. Because the
consult round is detached from the listen loop, raising the configured
deadline (up to ``MAX_CONSULT_DEADLINE_SECONDS``) has no health-lag or
alerting side effects — the cap is purely about resolving each window
before the next one opens.
"""

MIN_CONSULT_DEADLINE_SECONDS = 5
MAX_CONSULT_DEADLINE_SECONDS = 300

MIN_HEARTBEAT_SIGNAL_STRENGTH = 0.0
"""Lower bound for the configurable emit strength (0.0 = target-flat, no position)."""

MAX_HEARTBEAT_SIGNAL_STRENGTH = 1.0
"""Upper bound; downstream ``SignalData.strength`` enforces the same [0.0, 1.0] cap."""

DEFAULT_HEARTBEAT_SIGNAL_STRENGTH = 0.0
"""Default emit strength.

0.0 preserves the historical target-flat heartbeat (an approved round
opens no position); a higher value in ``[0.0, 1.0]`` makes an approved
round emit an actionable paper long, exercising the execution plane.
"""

SNAPSHOT_BARS = 200
"""Trailing complete bars read for the consult market snapshot.

200 bars comfortably covers the slowest indicator window (SMA 50) with
headroom for delegates that want to eyeball longer context, while the
canonical-JSON envelope stays a few hundred bytes (indicators only, the
raw series is never embedded) — far under the 16KB envelope cap.
"""

SNAPSHOT_RANGE_START = datetime(1970, 1, 1, tzinfo=UTC)
"""Open-ended lower bound for the snapshot's range query.

The snapshot needs "the trailing ``SNAPSHOT_BARS`` bars opening AT OR
BEFORE the trigger window". ``Repository.get_candles`` only applies an
``open_at`` ceiling in range mode (start AND end), so the epoch serves
as the floor and ``limit`` + ``order='desc'`` bound the depth.
"""

SMA_FAST_BARS = 20
SMA_SLOW_BARS = 50
RSI_BARS = 14
DAY_BARS = 24
"""Indicator windows, in bars of the strategy's input timeframe.

With the default 1h input, ``DAY_BARS`` spans one day — the 24-bar
change / range / volatility fields read as daily context.
"""


def _finite_or_none(value: float) -> float | None:
    """Return ``value`` when finite, else ``None``.

    The AI-review envelope guard rejects non-finite floats outright, so
    every computed field funnels through this filter instead of risking
    the whole consult on a degenerate series.
    """
    return value if math.isfinite(value) else None


def _sma(closes: list[float], bars: int) -> float | None:
    """Simple moving average of the trailing ``bars`` closes.

    Args:
        closes: Close series in ascending bar order.
        bars: Window length.

    Returns:
        The rounded average, or ``None`` when the series is shorter
        than the window.
    """
    if len(closes) < bars:
        return None
    return _finite_or_none(round(sum(closes[-bars:]) / bars, 8))


def _rsi(closes: list[float], bars: int = RSI_BARS) -> float | None:
    """Wilder-smoothed RSI over the full series.

    Args:
        closes: Close series in ascending bar order.
        bars: RSI period.

    Returns:
        RSI in ``[0, 100]`` rounded to 2 decimals; ``None`` when fewer
        than ``bars + 1`` closes exist; ``50.0`` for a perfectly flat
        series (no gains and no losses); ``100.0`` when losses never
        occurred.
    """
    if len(closes) < bars + 1:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for prev, cur in zip(closes, closes[1:], strict=False):
        delta = cur - prev
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains[:bars]) / bars
    avg_loss = sum(losses[:bars]) / bars
    for gain, loss in zip(gains[bars:], losses[bars:], strict=True):
        avg_gain = (avg_gain * (bars - 1) + gain) / bars
        avg_loss = (avg_loss * (bars - 1) + loss) / bars
    if avg_gain == 0.0 and avg_loss == 0.0:
        return 50.0
    if avg_loss == 0.0:
        return 100.0
    return _finite_or_none(round(100.0 - 100.0 / (1.0 + avg_gain / avg_loss), 2))


def _pct_change(current: float, reference: float) -> float | None:
    """Percent change of ``current`` versus ``reference``.

    Returns:
        The rounded percent change, or ``None`` when the reference is
        not positive (a zero/negative reference has no meaningful
        percent interpretation for prices).
    """
    if reference <= 0.0:
        return None
    return _finite_or_none(round((current - reference) / reference * 100.0, 4))


def _realized_vol_pct(closes: list[float], bars: int = DAY_BARS) -> float | None:
    """Population stdev of the trailing ``bars`` simple returns, in percent.

    Args:
        closes: Close series in ascending bar order.
        bars: Number of returns in the window (needs ``bars + 1`` closes).

    Returns:
        The rounded volatility percentage, or ``None`` when the series
        is too short or the window contains ANY non-positive close
        (including the final one — a zero/negative last price would
        otherwise yield a finite but nonsensical volatility).
    """
    if len(closes) < bars + 1:
        return None
    window = closes[-(bars + 1) :]
    if any(value <= 0.0 for value in window):
        return None
    returns = [(cur - prev) / prev for prev, cur in zip(window, window[1:], strict=False)]
    return _finite_or_none(round(statistics.pstdev(returns) * 100.0, 4))


async def _build_market_snapshot(
    repo: Repository, instrument: str, candle: CandleData, as_of: datetime
) -> JsonObject:
    """Compute the self-contained market context for one consult round.

    Reads the trailing ``SNAPSHOT_BARS`` COMPLETE bars of the candle's
    own timeframe from the store with the QUERY ceiling-bounded at the
    trigger window (``end=candle.open_at``): a replayed or republished
    older bar must never see look-ahead context, and it must still get
    its own trailing history even when the store head has advanced more
    than ``SNAPSHOT_BARS`` bars past it. A defensive filter additionally
    drops any nonconforming newer row a misbehaving store might return.
    The triggering bar is appended when the store has not persisted its
    window yet (the trigger typically arrives over the bus before the
    candle writer commits it; comparing ``open_at`` keeps an
    already-persisted trigger from being counted twice). Field values
    are indicators only — the raw series stays out of the envelope so
    its canonical-JSON size is independent of ``SNAPSHOT_BARS``.
    ``None`` fields mean the usable history is too short for that
    indicator, which the delegate should read as "warming up, prefer
    MCP lookups".

    Args:
        repo: Repository handle for the candle read.
        instrument: Instrument symbol from the candle topic.
        candle: The triggering candle.
        as_of: Temporal read anchor, shared with the caller's other
            reads in this round.

    Returns:
        The ``market`` envelope section.
    """
    rows = await repo.get_candles(
        instrument,
        candle.timeframe,
        SNAPSHOT_RANGE_START,
        candle.open_at,
        candle.exchange,
        as_of,
        limit=SNAPSHOT_BARS,
        order="desc",
        complete=True,
    )
    ordered = [row for row in reversed(rows) if row["open_at"] <= candle.open_at]
    closes = [float(row["close"]) for row in ordered]
    highs = [float(row["high"]) for row in ordered]
    lows = [float(row["low"]) for row in ordered]
    if not ordered or candle.open_at > ordered[-1]["open_at"]:
        closes.append(float(candle.close))
        highs.append(float(candle.high))
        lows.append(float(candle.low))
    prev_close = closes[-2] if len(closes) >= 2 else None
    day_ref_close = closes[-(DAY_BARS + 1)] if len(closes) >= DAY_BARS + 1 else None
    return {
        "timeframe": candle.timeframe,
        "bars": len(closes),
        "last_close": _finite_or_none(round(closes[-1], 8)),
        "change_1_bar_pct": (
            _pct_change(closes[-1], prev_close) if prev_close is not None else None
        ),
        "change_24_bar_pct": (
            _pct_change(closes[-1], day_ref_close) if day_ref_close is not None else None
        ),
        "sma_20": _sma(closes, SMA_FAST_BARS),
        "sma_50": _sma(closes, SMA_SLOW_BARS),
        "rsi_14": _rsi(closes),
        "high_24_bar": (
            _finite_or_none(round(max(highs[-DAY_BARS:]), 8)) if len(highs) >= DAY_BARS else None
        ),
        "low_24_bar": (
            _finite_or_none(round(min(lows[-DAY_BARS:]), 8)) if len(lows) >= DAY_BARS else None
        ),
        "realized_vol_24_bar_pct": _realized_vol_pct(closes),
    }


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
            "heartbeat_signal_strength": DEFAULT_HEARTBEAT_SIGNAL_STRENGTH,
        },
    },
)
class HeartbeatConsult(BaseStrategy):
    """One CONSULT per new candle window; approved rounds emit the signal.

    Paper-only by construction: the constructor rejects any non-paper
    exchange so the heartbeat can never carry live-order intent even if
    misconfigured. Every consult failure mode (no live delegate, all
    delegates busy, unresolvable instrument, snapshot errors, unexpected
    errors) is fail-soft — the heartbeat loop must never crash the
    strategy.

    The consult round runs DETACHED from ``on_candle``: the callback
    spawns ``_consult_round`` and returns immediately, so the listen
    loop keeps draining feed heartbeats and the health monitor's data
    lag stays at feed cadence for the whole decision deadline. At most
    one round is in flight at a time; ``stop`` and ``reset`` cancel it.

    Attributes:
        consult_user_public_id: UUID7 of the strategy owner stamped on
            every review row (DISTINCT from delegate users).
        consult_strategy_public_id: Stable UUID7 identifying this
            strategy instance on review rows and wake-frame topics;
            seeded once by the operator in the process config.
        consult_deadline_seconds: Per-round decision deadline.
        consult_signal_strength: Emit strength for an approved round in
            ``[0.0, 1.0]``; 0.0 (default) stays target-flat, higher opens
            an actionable paper long.
    """

    REFERENCE_IDENTITY_PARAMS: ClassVar[Mapping[str, str]] = {"ai_review_user_public_id": "user"}
    SEEDED_IDENTITY_PARAMS: ClassVar[tuple[str, ...]] = ("ai_review_strategy_public_id",)

    def __init__(self, config: StrategyConfig) -> None:
        """Validate consult identity params fail-fast and initialize state.

        Args:
            config: Strategy configuration; must use the paper exchange
                and carry UUID7 ``ai_review_user_public_id`` and
                ``ai_review_strategy_public_id`` params plus a sane
                ``ai_review_deadline_seconds`` and an optional
                ``heartbeat_signal_strength`` in ``[0.0, 1.0]``
                (default 0.0 = target-flat).

        Raises:
            ValueError: Non-paper exchange, missing/non-UUID7 identity
                params, an out-of-range deadline, or an out-of-range
                ``heartbeat_signal_strength``.
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
        signal_strength = float(
            self.params.get("heartbeat_signal_strength", DEFAULT_HEARTBEAT_SIGNAL_STRENGTH)
            or DEFAULT_HEARTBEAT_SIGNAL_STRENGTH
        )
        if not MIN_HEARTBEAT_SIGNAL_STRENGTH <= signal_strength <= MAX_HEARTBEAT_SIGNAL_STRENGTH:
            raise ValueError(
                f"Strategy {config.name}: param 'heartbeat_signal_strength' must be in "
                f"[{MIN_HEARTBEAT_SIGNAL_STRENGTH}, {MAX_HEARTBEAT_SIGNAL_STRENGTH}] "
                f"(downstream SignalData enforces the same bound), got {signal_strength}"
            )
        self.consult_user_public_id = user_public_id
        self.consult_strategy_public_id = strategy_public_id
        self.consult_deadline_seconds = deadline_seconds
        self.consult_signal_strength = signal_strength
        self._last_consult_open_at: datetime | None = None
        self._consult_task: asyncio.Task[None] | None = None
        self._consult_stopped = False
        self._consult_reset_active = False

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Dispatch one DETACHED consult round per NEW candle window.

        A revised or republished bar for an already-consulted ``open_at``
        never re-consults (one decision per window regardless of the
        prior round's outcome). The round itself runs as a spawned task
        so this callback returns immediately and the listen loop keeps
        draining feed heartbeats — the health monitor's data lag must
        never absorb the consult deadline. When a round is still in
        flight as a NEW window arrives, that window is skipped WITHOUT
        being consumed, so a later republish can still consult it. The
        dispatch gate closes while ``stop``/``reset`` drain the
        in-flight round (their ``await`` yields to this listen loop):
        without it a candle landing in that gap would spawn a fresh
        round that outlives the shutdown or dodges the replay reset.

        Args:
            instrument: The instrument symbol from the candle topic.
            candle: The triggering candle.

        Returns:
            Always ``None`` — emission happens inside the detached
            round with outcome attribution, never through the callback
            return path.
        """
        if self._last_consult_open_at is not None and candle.open_at <= self._last_consult_open_at:
            return None
        if self._consult_stopped or self._consult_reset_active:
            logger.debug(
                f"Strategy {self.name}: consult dispatch gate closed — "
                f"skipping window {candle.open_at.isoformat()} (not consumed)"
            )
            return None
        if self._consult_task is not None and not self._consult_task.done():
            logger.warning(
                f"Strategy {self.name}: consult round still in flight — "
                f"skipping window {candle.open_at.isoformat()} (not consumed)"
            )
            return None
        self._last_consult_open_at = candle.open_at
        self._consult_task = asyncio.create_task(self._consult_round(instrument, candle))
        return None

    async def _consult_round(self, instrument: str, candle: CandleData) -> None:
        """Run one detached consult round end-to-end, fail-soft throughout.

        Awaits the consult, and on an approved outcome emits the
        heartbeat signal at the configured strength with the AI-review
        attribution stamped. Every failure (consult errors are already
        absorbed by :meth:`_consult`; emit-path failures are absorbed
        here) is logged and swallowed — a detached task has no caller to
        propagate to, and the heartbeat's job is to keep probing the
        wake path every window. Cancellation propagates untouched so
        ``stop``/``reset`` can drain the round deterministically.

        Args:
            instrument: The instrument symbol from the candle topic.
            candle: The triggering candle.
        """
        try:
            outcome = await self._consult(instrument, candle)
            if outcome is None or outcome.status != AiReviewStatusEnum.RESOLVED_APPROVED:
                return
            await self.emit_signal(
                StrategySignal(
                    instrument=instrument,
                    side=TradeSideEnum.BUY,
                    strength=self.consult_signal_strength,
                    reason="heartbeat approved",
                    price=candle.close,
                ),
                outcome=outcome,
            )
        except Exception as exc:
            logger.warning(f"Strategy {self.name}: heartbeat round failed — {exc}")

    async def _cancel_inflight_round(self) -> None:
        """Cancel and drain the in-flight detached consult round, if any."""
        task = self._consult_task
        self._consult_task = None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def stop(self) -> None:
        """Cancel the in-flight consult round, then stop the strategy.

        The permanent stop flag closes the dispatch gate FIRST and
        stays set forever: draining the cancelled round yields to the
        event loop, and the listen loop (stopped only later by
        ``super().stop()``) could otherwise spawn a fresh round in that
        gap and leak it past shutdown.
        """
        self._consult_stopped = True
        await self._cancel_inflight_round()
        await super().stop()

    async def reset(self) -> None:
        """Reset per-window consult state for replay.

        The reset-scoped flag closes the dispatch gate for the duration
        of the reset so a candle landing while the cancelled round
        drains cannot spawn a round that dodges the reset (and whose
        window marker the reset would then wipe, re-enabling a
        duplicate consult). The flag is DISTINCT from the permanent
        stop flag on purpose: a reset unwound mid-drain (``stop()``
        cancelling the listen task that drives a replay reset) clears
        only its own flag in the ``finally``, so it can never reopen a
        gate that ``stop()`` closed for good.
        """
        self._consult_reset_active = True
        try:
            await self._cancel_inflight_round()
            self._last_consult_open_at = None
        finally:
            self._consult_reset_active = False
        logger.info(f"Strategy {self.name}: heartbeat consult state reset for replay")

    async def _consult(self, instrument: str, candle: CandleData) -> AiReviewDecisionOutcome | None:
        """Create + await one CONSULT round, fail-soft on every error.

        Resolves the instrument public id against the candle's SOURCE
        exchange (paper configs subscribe live-venue topics, and
        instrument rows live under the source venue), builds the
        :class:`AiReviewCreateRequest` with the validated identity
        params and the self-contained market snapshot, and drives
        ``create_ai_review_and_await``. Uses the process-wide cached
        repository exactly like DB warmup does and never disposes it.
        A snapshot failure downgrades the envelope to the minimal
        legacy form instead of skipping the round.

        Args:
            instrument: The instrument symbol from the candle topic.
            candle: The triggering candle supplying price context and
                the source exchange.

        Returns:
            The terminal decision outcome, or ``None`` when the round
            could not run or did not produce a decision.
        """
        try:
            now = datetime.now(UTC)
            repo = get_repository(get_bootstrap_settings().db_url)
            instrument_public_id = await repo.get_instrument_public_id_by_symbol(
                instrument, candle.exchange, now
            )
            if instrument_public_id is None:
                logger.warning(
                    f"Strategy {self.name}: heartbeat consult skipped — no active "
                    f"instrument row for {instrument} on {candle.exchange}"
                )
                return None
            signal_envelope: JsonObject = {
                "kind": "heartbeat",
                "open_at": candle.open_at.isoformat(),
                "close": float(candle.close),
                "proposed_side": TradeSideEnum.BUY.value,
                "proposed_strength": self.consult_signal_strength,
            }
            try:
                signal_envelope["market"] = await _build_market_snapshot(
                    repo, instrument, candle, now
                )
            except Exception as exc:
                logger.warning(f"Strategy {self.name}: market snapshot unavailable — {exc}")
            request = AiReviewCreateRequest(
                user_public_id=self.consult_user_public_id,
                operator_public_id=self.config.operator_public_id,
                wallet_public_id=self.config.wallet_public_id,
                instrument_public_id=instrument_public_id,
                strategy_public_id=self.consult_strategy_public_id,
                signal_envelope=signal_envelope,
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
