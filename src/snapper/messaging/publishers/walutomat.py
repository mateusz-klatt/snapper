"""Walutomat market data publisher service.

Streams real-time FX quotes from Walutomat exchange via ZeroMQ.
"""

from typing import Any

from loguru import logger

from snapper.application.process_manager.process_parameters import PublisherSymbolsParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.json_types import JsonArray
from snapper.core.json_types import JsonObject
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import PriceBasis
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.infrastructure.symbols.functions import native_to_walutomat_ws
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.publishers.base import VenueFeedHealth


@register_process(
    "walutomat_feed_publisher",
    description="Walutomat market data feed publisher",
    priority=22,
    role=ProcessRoleEnum.CORE,
    restart_policy=ProcessRestartPolicyEnum.ALWAYS,
    tags=("market-data", "publisher", "walutomat"),
    parameters_model=PublisherSymbolsParameters,
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class WalutomatMarketDataPublisher(MarketDataPublisherService[WalutomatExchangeClient]):
    """Market data publisher for Walutomat exchange."""

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Return default parameters for publisher initialization.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with symbols from Walutomat instruments configuration.
        """
        instruments = settings.instruments
        walutomat_symbols = instruments.get(ExchangeEnum.WALUTOMAT, [])
        return {
            "symbols": walutomat_symbols,
        }

    def _supports_public_trades(self) -> bool:
        return False

    def _create_exchange_client(self) -> WalutomatExchangeClient:
        return WalutomatExchangeClient()

    def _get_exchange_name(self) -> MarketDataExchange:
        return ExchangeEnum.WALUTOMAT

    def _candle_source_for(self, timeframe: str) -> str:
        """Walutomat builds its 1m bars from REST quote polling.

        ``source`` names the MECHANISM only, and ``calculated`` therefore spans
        more than one price convention (Kraken futures/equities build theirs
        from trade prints). Which price is polled here — the midpoint of the
        venue's two-sided top-of-book quote — is carried by
        :meth:`_candle_price_basis_for`, not by this tag. Do not invent a
        fourth ``source`` value: the ``candles`` CHECK permits only
        ``native``/``calculated``/``synthesized`` and the grouped candle
        loader branches on it.

        Args:
            timeframe: The candle timeframe label.

        Returns:
            ``calculated`` — these 1m bars are Snapper-computed from polled
            quotes, not venue-precomputed OHLC.
        """
        return "calculated"

    def _candle_price_basis_for(self, timeframe: str) -> PriceBasis | None:
        """Walutomat bars are marked on the top-of-book mid.

        Args:
            timeframe: The candle timeframe label.

        Returns:
            ``quote_mid`` — the venue delivers a two-sided book and no usable
            trade print, so ``(bid + ask) / 2`` is the best available estimate
            of the executable price. Higher-TF rollups of these bars inherit
            the same basis, which is correct.
        """
        return "quote_mid"

    def _venue_feed_health(self) -> VenueFeedHealth:
        """Expose the client's mark-refusal state on the heartbeat.

        Fail-closed is fail-STALE on the ticker plane: it applies no age gate,
        so a chronically refused pair keeps serving its frozen last-good mark
        to position valuation, the caps notional and the paper fill path while
        the feed reads healthy. That is the frozen-mark defect the mid
        convention exists to remove, resurrected one pair at a time, and a
        transition-only log line does not make it visible. The counts,
        durations and windowed refusal fractions therefore ride the heartbeat
        under ``meta.venue``, and a symbol refused continuously for
        :data:`snapper.infrastructure.exchanges.implementations.walutomat.WALUTOMAT_REFUSAL_WARNING_SECONDS`
        — or refused for more than
        :data:`snapper.infrastructure.exchanges.implementations.walutomat.WALUTOMAT_REFUSAL_FRACTION_CEILING`
        of its windowed polls — degrades the feed to WARNING.

        Returns:
            The refused symbols, their cumulative refusal counts, their
            continuous refusal durations and their windowed refusal fractions,
            plus the degraded flag; empty and healthy before the exchange
            client exists (publisher not started).
        """
        client = self._exchange_client
        if client is None:
            return VenueFeedHealth(meta={}, degraded=False)
        report = client.mark_refusal_report()
        symbols: JsonArray = list(report.symbols)
        counts: JsonObject = dict(report.counts)
        seconds: JsonObject = dict(report.seconds)
        fractions: JsonObject = dict(report.fractions)
        refused: JsonObject = {
            "refused_marks": symbols,
            "refused_mark_counts": counts,
            "refused_mark_seconds": seconds,
            "refused_mark_fractions": fractions,
        }
        return VenueFeedHealth(meta=refused, degraded=report.escalated)

    def _wildcard_symbol_universe(self) -> list[str]:
        """Resolve ``["*"]`` into the venue's concrete pair list for seeding.

        Walutomat's publisher forwards the sentinel verbatim and the client
        expands it privately inside ``_subscribe_ticks_impl``, so nothing in
        the publisher ever sees the 44 native pairs — which is why the
        per-symbol lag baseline had nothing to seed and the per-symbol lag
        path was dead on the configuration that actually ships (``instruments``
        defaults to ``["*"]`` for every exchange). Resolving here restores it
        without touching subscription semantics: ``start`` creates and connects
        the exchange client before it seeds, and ``connect`` populates the pair
        map, so the universe is known by the time this runs.

        Resolution is fail-soft. Seeding is observability, so a venue that
        cannot name its pairs must degrade the lag signal, never the publisher
        start path: ``get_supported_pairs`` raises when no payload has been
        fetched and its symbol translation raises on a pair the mapper does not
        know, and either way the sentinel simply contributes nothing.

        Returns:
            Native symbols currently served by the venue, or an empty list when
            the client is absent or cannot name them.
        """
        client = self._exchange_client
        if client is None:
            return []
        try:
            return client.get_supported_pairs()
        except (RuntimeError, ValueError) as exc:
            logger.warning(
                "walutomat publisher: cannot resolve the wildcard for lag seeding ({})", exc
            )
            return []

    async def start(self) -> None:
        """Start the publisher within a connector-registration context.

        Stamps ``_CURRENT_PUBLISHER`` so the per-request egress-pool
        reservation in
        :class:`snapper.infrastructure.network.pooled_httpx_transport.PooledAsyncTransport`
        reads ``"walutomat"`` as the exchange tag. The
        ``allowed_exchanges=["walutomat"]`` filter on the matching
        egress route then pins this publisher's HTTP polling to the
        configured egress tunnel.

        Without this override the pooled transport's
        ``default_exchange_tag="walutomat"`` fallback would still
        produce the same effect — this override is kept for
        symmetry with the Kraken Spot / Equities / Futures
        publishers and to keep the source of truth for the exchange
        tag at the publisher rather than the transport's default.

        The token is reset in ``finally`` so the ContextVar does not
        leak across publisher restarts.
        """
        token = _CURRENT_PUBLISHER.set(self)
        try:
            await super().start()
        finally:
            _CURRENT_PUBLISHER.reset(token)

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for Walutomat.

        The wildcard ``["*"]`` passes through untouched —
        :class:`WalutomatExchangeClient` accepts ``"*"`` as a
        subscribe-all sentinel (already used by
        :class:`WalutomatSnapshotUpdaterService`) so the publisher
        forwards it verbatim and skips per-symbol mapping.

        Args:
            symbols: Input symbols in native format, or ``["*"]``.

        Returns:
            Valid symbols that can be streamed from Walutomat, or
            ``["*"]`` when the caller requested subscribe-all.
        """
        if symbols == ["*"]:
            return ["*"]
        native_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            try:
                native_to_walutomat_ws(symbol)
            except ValueError:
                logger.warning(
                    f"WalutomatMarketDataPublisher: Skipping unknown native symbol {symbol}"
                )
                continue
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            native_symbols.append(symbol)
        return native_symbols

    async def _attempt_liveness_recovery(self, reason: str) -> None:
        """Recover stale polling by breaking backoff sleep and resetting counters."""
        logger.error("walutomat publisher: liveness recovery triggered ({})", reason)
        client = self._exchange_client
        if client is not None:
            client._consecutive_error_count = 0
            client._backoff_attempts = 0
            if client._backoff_wakeup_event is not None:
                client._backoff_wakeup_event.set()
