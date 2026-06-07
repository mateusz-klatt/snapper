"""Walutomat market data publisher service.

Streams real-time FX quotes from Walutomat exchange via ZeroMQ.
"""

from typing import Any

from loguru import logger

from snapper.application.process_manager.process_parameters import PublisherSymbolsParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.exchanges.kraken_sdk_patches import _CURRENT_PUBLISHER
from snapper.infrastructure.symbols.functions import native_to_walutomat_ws
from snapper.messaging.publishers.base import MarketDataPublisherService


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
