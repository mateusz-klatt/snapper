"""N-leg paired-signal helpers for ``BaseStrategy`` subclasses.

Generalizes the 2-leg pattern in :class:`CointegrationPairs` to any
N-leg basket strategy. The existing 2-leg cointegration adopts the
:class:`MultiLegSpreadMixin` while keeping ``instrument1`` /
``instrument2`` available as aliases over ``self.legs[0]`` /
``self.legs[1]``.

Two pieces ship here:

- :func:`resolve_legs` — generalizes
  ``CointegrationPairs._resolve_pair_instruments`` to N legs. Accepts
  either the live-ZMQ shape (one input topic per leg) or the direct-DB
  synthetic shape (one synthetic input plus N output instruments).

- :class:`MultiLegSpreadMixin` — adds ``self.legs``, partner-leg
  iteration helpers, and a pure partner-signal builder that returns one
  ``StrategySignal`` per partner leg in declaration order. The host
  strategy returns ``[primary, *partners]`` from its ``on_candle`` so
  ``BaseStrategy`` emits every leg atomically; there is no side-channel
  queue.
"""

from collections.abc import Callable

from snapper.messaging.schemas.data import CandleData
from snapper.messaging.topics.builders import parse_market_topic
from snapper.strategies.models import StrategyConfig
from snapper.strategies.models import StrategySignal

__all__ = ["MultiLegSpreadMixin", "resolve_legs"]


def _extract_instrument_from_topic(topic: str) -> str:
    """Return the instrument symbol embedded in a market-data topic.

    Falls back to the topic string itself when the topic does not parse
    as a canonical market topic. Matches the behaviour of
    ``CointegrationPairs._extract_instrument`` for the 2-leg
    compatibility path.

    Args:
        topic: ZMQ topic string.

    Returns:
        Extracted instrument symbol.
    """
    parsed = parse_market_topic(topic)
    if parsed is not None:
        return parsed.instrument
    return topic


def resolve_legs(
    config: StrategyConfig,
    expected_count: int | None = None,
) -> tuple[str, ...]:
    """Resolve N pair-trade instruments from a strategy config.

    Two supported invocation shapes:

    - **Live ZMQ process**: ``inputs`` is a length-N list of market-data
      candle topics, one per leg. Pair instruments are extracted via
      :func:`parse_market_topic` (with raw-topic fallback for unit-test
      topics that don't parse).

    - **Direct-DB backtest**: ``DirectDbEngine`` synthesises a single
      ``candles.{exchange}.synthetic.{timeframe}`` input and passes the
      pair instruments through ``outputs``. Detected by the
      ``synthetic`` marker in the input topic; ``outputs`` must then
      carry exactly the leg instruments.

    Args:
        config: Strategy configuration carrying inputs + outputs.
        expected_count: When set, raises ``ValueError`` unless the
            resolved tuple has exactly this many legs. ``None`` accepts
            any count >= 2.

    Returns:
        Ordered tuple of instrument symbols, length >= 2.

    Raises:
        ValueError: If neither invocation shape carries enough leg
            instruments, or if ``expected_count`` is set and not matched.
    """
    legs: tuple[str, ...]
    if len(config.inputs) >= 2 and not any("synthetic" in topic for topic in config.inputs):
        legs = tuple(_extract_instrument_from_topic(topic) for topic in config.inputs)
    elif len(config.inputs) == 1 and "synthetic" in config.inputs[0] and len(config.outputs) >= 2:
        legs = tuple(config.outputs)
    else:
        raise ValueError(
            f"Multi-leg strategy requires either >=2 inputs (live ZMQ) or "
            f"1 synthetic input + >=2 outputs (direct-DB backtest); got "
            f"inputs={config.inputs} outputs={config.outputs}"
        )
    if expected_count is not None and len(legs) != expected_count:
        if len(config.inputs) >= 2 and not any("synthetic" in t for t in config.inputs):
            raise ValueError(
                f"Multi-leg strategy expected exactly {expected_count} inputs (live ZMQ) "
                f"or 1 synthetic input + {expected_count} outputs (direct-DB backtest); "
                f"got {len(legs)} legs from inputs={config.inputs}"
            )
        raise ValueError(f"Expected exactly {expected_count} legs; got {len(legs)}: {legs}")
    return legs


class MultiLegSpreadMixin:
    """Helpers for strategies that emit synchronized signals across N legs.

    Strategies subclassing :class:`BaseStrategy` AND this mixin gain:

    - ``self.legs: tuple[str, ...]`` — populated by
      ``self._init_legs(...)`` from ``self.config``.
    - ``self._partner_legs(current_instrument)`` — leg names other than
      ``current_instrument`` in declaration order.
    - ``self._partner_prices(current_instrument)`` — dict mapping
      partner leg to last buffered close, omitting partners with empty
      buffers (the same warmup gate the cointegration strategy uses).
    - ``self._build_partner_signals(current_instrument, builder)`` —
      a PURE builder returning one ``StrategySignal`` per buffered
      partner. The host strategy returns ``[primary, *partners]`` from
      ``on_candle`` so ``BaseStrategy`` emits every leg atomically.

    The mixin does not subclass ``BaseStrategy``; subclasses do that
    on their own. That keeps the ``BaseStrategy`` MRO linear and avoids
    diamond-inheritance pitfalls.

    Mixin attributes (provided by the cooperating ``BaseStrategy``-shaped
    host):

    - ``config: StrategyConfig`` — read by ``_init_legs``.
    - ``candle_buffer: dict[str, list[CandleData]]`` — read by
      ``_partner_prices``.
    """

    config: StrategyConfig
    candle_buffer: dict[str, list[CandleData]]
    legs: tuple[str, ...]

    def _init_legs(self, expected_count: int | None = None) -> None:
        """Populate ``self.legs`` from ``self.config``.

        Call this from the subclass constructor *after* ``super().__init__``
        has populated ``self.config``.

        Args:
            expected_count: Forwarded to :func:`resolve_legs`.
        """
        self.legs = resolve_legs(self.config, expected_count)

    def _partner_legs(self, current_instrument: str) -> tuple[str, ...]:
        """Return all legs except ``current_instrument`` in declaration order."""
        return tuple(leg for leg in self.legs if leg != current_instrument)

    def _partner_prices(self, current_instrument: str) -> dict[str, float]:
        """Return ``{leg: last_close}`` for partners with buffered candles.

        Partners whose ``candle_buffer`` is empty are omitted; the caller
        decides whether to skip the timestep or emit only for buffered
        partners. This matches ``CointegrationPairs._partner_price``
        which returns ``None`` when no partner candle is buffered.
        """
        out: dict[str, float] = {}
        for leg in self.legs:
            if leg == current_instrument:
                continue
            bars = self.candle_buffer.get(leg, [])
            if bars:
                out[leg] = float(bars[-1].close)
        return out

    def _build_partner_signals(
        self,
        current_instrument: str,
        builder: Callable[[str, float], StrategySignal],
    ) -> list[StrategySignal]:
        """Build one partner-leg ``StrategySignal`` per buffered partner.

        Pure function of ``self.candle_buffer`` and ``self.legs`` — no
        shared mutable state, no emission side effects. For each partner
        leg with at least one buffered candle, calls
        ``builder(leg, last_close)``. The host strategy concatenates the
        result after its primary signal and returns the combined list
        from ``on_candle`` so every leg is emitted atomically.

        Args:
            current_instrument: The leg whose candle is being processed.
            builder: Callable mapping ``(partner_leg, last_close)`` to a
                partner ``StrategySignal``.

        Returns:
            One ``StrategySignal`` per buffered partner, in declaration
            order; empty when no partner has a buffered candle yet.
        """
        return [
            builder(leg, price) for leg, price in self._partner_prices(current_instrument).items()
        ]
