"""TypedDict definitions for symbol updater payloads.

Each exchange updater produces a list of these records from
_fetch_symbols(). Using TypedDicts instead of dict[str, Any]
provides compile-time key validation and IDE support.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal
from typing import TypedDict


@dataclass(frozen=True)
class InstrumentMetadataInput:
    """Atomic venue-sourced precision, limit, status, and unit metadata."""

    tick_size: float | None
    lot_size: float | None
    min_order_size: float | None
    max_order_size: float | None
    cost_decimals: int | None
    qty_decimals: int | None
    margin_initial: float | None
    position_limit_long: int | None
    position_limit_short: int | None
    status: str | None
    contract_size: Decimal | None
    quantity_unit: Literal["base_asset", "contract_count"] | None
    spec_source: str | None
    spec_version: str | None
    spec_observed_at: datetime | None
    unit_certified: bool


class _WalutomatSymbolRequired(TypedDict):
    """Required keys emitted by the Walutomat symbol updater."""

    native_symbol: str
    base: str
    quote: str
    symbol: str
    walutomat_rest_symbol: str


class WalutomatSymbolRecord(_WalutomatSymbolRequired, total=False):
    """Walutomat symbol record with all-required fields."""


class _PolygonSymbolRequired(TypedDict):
    """Required keys emitted by the Polygon symbol updater."""

    ticker: str


class PolygonSymbolRecord(_PolygonSymbolRequired, total=False):
    """Polygon symbol record with optional currency metadata."""

    base_currency_symbol: str
    currency_symbol: str
    currency_name: str


class _KrakenSymbolRequired(TypedDict):
    """Required keys emitted by the Kraken Spot symbol updater."""

    native_symbol: str
    base_currency: str
    quote_currency: str


class KrakenSymbolRecord(_KrakenSymbolRequired, total=False):
    """Kraken Spot symbol record with optional alias and class fields.

    ccxt_symbol is str | None because tokenized asset pairs
    produce None from _extract_tokenized_pair().
    margin is "true"/"false" string — present only for REST pairs
    where CCXT market data exposes leverage_buy/leverage_sell.
    is_btnl is the "true" marker for Kraken Bitnomial spot
    discoveries (BTC/USD:BTNL etc.) routed through the BTNL persist
    path with ``can_trade=False`` and an instrument_kind override.
    metadata is present only for authoritative CCXT catalog records;
    WS-only, BTNL, and direct tokenized REST records omit it so they
    cannot refresh or replace venue precision provenance.
    """

    asset_class: str
    ws_only: str
    is_btnl: str
    margin: str
    kraken_websocket_symbol: str
    kraken_rest_symbol: str
    ccxt_symbol: str | None
    metadata: InstrumentMetadataInput
