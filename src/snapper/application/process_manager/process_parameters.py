"""Per-process parameter models for validated process configuration.

Each model defines the constructor parameters for a specific process type.
These models serve two purposes:

1. Validation at spawn boundary: launcher validates parameters against
   the model before instantiation via ``process_class(**validated.model_dump())``.
2. Schema generation: ``parameters_model.model_json_schema()`` produces
   the JSON Schema exposed via REST API (replaces manual parameters_schema).

Processes keep clean domain constructors. The process manager validates
parameters externally, then instantiates via keyword unpacking.
"""

from snapper.api.schemas.base import StrictBody
from snapper.core.json_types import JsonObject
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange


class BrokerParameters(StrictBody):
    """Parameters for ZmqBrokerProcess.

    Attributes:
        xsub_endpoint: ZMQ XSUB bind endpoint for publishers.
        xpub_endpoint: ZMQ XPUB bind endpoint for subscribers.
    """

    xsub_endpoint: str | None = None
    xpub_endpoint: str | None = None


class LoggerParameters(StrictBody):
    """Parameters for ZmqMessageLogger.

    Attributes:
        log_to_file: Whether to write messages to audit file.
        log_payload: Whether to log message payloads (verbose).
        max_payload_length: Maximum payload characters to log.
        audit_file: Custom audit file path (None for default).
    """

    log_to_file: bool = True
    log_payload: bool = False
    max_payload_length: int = 200
    audit_file: str | None = None


class TraderParameters(StrictBody):
    """Parameters for TraderCoordinator.

    Attributes:
        signal_topics: ZMQ topic prefixes to subscribe to for signals.
    """

    signal_topics: list[str] = ["signals."]


class PublisherSymbolsParameters(StrictBody):
    """Parameters for exchange market data publishers (Kraken, Zonda, Walutomat).

    Attributes:
        symbols: List of native symbols to subscribe and publish.
    """

    symbols: list[str] = []


class PaperPublisherParameters(StrictBody):
    """Parameters for PaperMarketDataPublisher.

    Attributes:
        paper_instruments: Mapping of exchange to symbol lists for replay.
        start_time: Start timestamp for backtesting (Unix seconds).
        end_time: End timestamp for backtesting (Unix seconds).
    """

    paper_instruments: dict[str, list[str]] = {}
    start_time: float | None = None
    end_time: float | None = None


class AggregatesBackfillParameters(StrictBody):
    """Parameters for PolygonAggregatesBackfillService.

    Attributes:
        symbols: Symbols to backfill (empty uses settings default).
        multiplier: Candle multiplier (e.g. 1 for 1-minute).
        timespan: Candle timespan (minute, hour, day, etc.).
        days_back: Number of days to backfill from today.
        resume: Whether to resume from last stored candle.
        save_csv: Whether to save raw API responses as CSV cache.
    """

    symbols: list[str] = []
    multiplier: int = 1
    timespan: str = "minute"
    days_back: int = 7
    resume: bool = True
    save_csv: bool = True


class KrakenFuturesBackfillParameters(StrictBody):
    """Parameters for KrakenFuturesAggregatesBackfillService.

    Attributes:
        symbols: Native symbols to backfill (empty uses all mapped).
        all_symbols: If True, backfill all Kraken Futures symbols.
        timeframe: CCXT candle interval (e.g. 1h, 4h, 1d).
        days_back: Number of days to backfill from today.
        resume: Whether to resume from last stored candle.
    """

    symbols: list[str] = []
    all_symbols: bool = False
    timeframe: str = "1h"
    days_back: int = 90
    resume: bool = True


class KrakenEquitiesBackfillParameters(StrictBody):
    """Parameters for KrakenEquitiesAggregatesBackfillService.

    Kraken Equities (TradFi FCM futures) market-data-only ingest uses
    the internal ``iapi.kraken.com`` ticker/history endpoint. Only
    intervals accepted by that endpoint are allowed downstream; see
    ``_timeframe_to_interval`` in the exchange client for the live-probed
    support set (``1m/5m/15m/30m/1h/1d``).

    Attributes:
        symbols: Native symbols to backfill (empty uses all mapped).
        all_symbols: If True, backfill all Kraken Equities symbols.
        timeframe: Candle interval (``1m``, ``5m``, ``15m``, ``30m``, ``1h``, ``1d``).
        days_back: Number of days to backfill from today.
        resume: Whether to resume from last stored candle.
    """

    symbols: list[str] = []
    all_symbols: bool = False
    timeframe: str = "1h"
    days_back: int = 30
    resume: bool = True


class GroupedDailyBackfillParameters(StrictBody):
    """Parameters for PolygonGroupedDailyBackfillService.

    Attributes:
        market_type: Market type (crypto, stocks, forex).
        days: Number of days to backfill.
        locale: Market locale (global, us).
        save_csv: Whether to save raw responses as CSV cache.
        adjusted: Whether to use adjusted prices.
    """

    market_type: str = "crypto"
    days: int = 3
    locale: str = "global"
    save_csv: bool = True
    adjusted: bool = True


class SymbolUpdaterParameters(StrictBody):
    """Parameters for symbol updater services (Kraken, Zonda, Walutomat).

    Attributes:
        update_threshold_hours: Minimum hours between automatic updates.
        force: If True, bypass the update threshold check.
    """

    update_threshold_hours: int = 24
    force: bool = False


class PolygonSymbolUpdaterParameters(StrictBody):
    """Parameters for PolygonSymbolUpdaterService.

    Extends base symbol updater with insert_new flag.

    Attributes:
        update_threshold_hours: Minimum hours between automatic updates.
        force: If True, bypass the update threshold check.
        insert_new: If True, insert symbols not yet in the database.
    """

    update_threshold_hours: int = 168
    force: bool = False
    insert_new: bool = False


class StrategyProcessParameters(StrictBody):
    """Parameters for dynamically created strategy processes.

    The wrapper contract (name, inputs, outputs, exchange) is stable
    and validated. Only ``params`` stays as JsonObject — each strategy
    defines its own parameter semantics inside that field.

    Attributes:
        name: Strategy instance name.
        inputs: ZMQ topic prefixes to subscribe to.
        outputs: ZMQ topic prefixes to publish to.
        exchange: Exchange for order execution.
        params: Strategy-specific opaque parameters.
        wallet_public_id: Wallet that owns positions and credentials for
            order execution. Empty default for backwards compatibility;
            becomes required after NOT NULL tightening.
        operator_public_id: Trading-identity operator that owns this
            strategy instance. Empty default.
    """

    name: str
    inputs: list[str]
    outputs: list[str]
    exchange: OrderExchange = ExchangeEnum.PAPER
    params: JsonObject | None = None
    wallet_public_id: str = ""
    operator_public_id: str = ""
