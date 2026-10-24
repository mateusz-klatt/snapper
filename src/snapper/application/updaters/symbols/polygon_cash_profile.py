"""Interpret explicit Polygon stock reference fields for cash paper admission.

Only common stock and ETF classifications identify a cash share profile here.
This interpretation describes a provider reference observation, never account
eligibility, margin, derivative support, or a promise that cached data is fresh.
Provider timestamps remain provider timestamps; reading the cache does not
refresh them. Historical prefix based crypto and FX kinds carry no new stock
attestation and never preserve a previous stock's metadata block.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Literal

from snapper.application.updaters.symbols.types import InstrumentMetadataInput
from snapper.application.updaters.symbols.types import PolygonSymbolRecord

POLYGON_CASH_PROFILE_SOURCE = "polygon_rest_ticker_reference"
POLYGON_CASH_PROFILE_VERSION = "polygon-cash-v1"


@dataclass(frozen=True, slots=True)
class PolygonCashProfile:
    """One current kind plus a complete replacement of the metadata block."""

    instrument_kind: Literal["spot", "etf"] | None
    metadata: InstrumentMetadataInput


def _currency_code(value: str | None) -> str | None:
    """Accept only an unpadded three letter provider currency code.

    Args:
        value: Provider field that explicitly reports an ISO currency code.

    Returns:
        Uppercase code when its representation is compatible, otherwise None.
    """
    if not isinstance(value, str) or len(value) != 3 or not value.isascii() or not value.isalpha():
        return None
    return value.upper()


def _reported_quote(record: PolygonSymbolRecord) -> str | None:
    """Read the explicit quote code without interpreting arbitrary currency names.

    The documented stock reference example supplies currency_name='usd'. Only
    that documented fallback is accepted when the explicit ISO field is absent.

    Args:
        record: Exact provider reference record retained by the stream and cache.

    Returns:
        Reported quote code, or None when evidence is absent or contradictory.
    """
    symbol = record.get("currency_symbol")
    name_code = _currency_code(record.get("currency_name"))
    if symbol is None:
        return "USD" if name_code == "USD" else None
    symbol_code = _currency_code(symbol)
    if name_code is not None and name_code != symbol_code:
        return None
    return symbol_code


def _provider_timestamp(record: PolygonSymbolRecord, now: datetime) -> datetime | None:
    """Retain the provider's aware reference timestamp without a fabricated TTL.

    Args:
        record: Exact reference record containing an optional accuracy timestamp.
        now: Aware UTC interpretation time, used only to reject future evidence.

    Returns:
        Provider timestamp normalized to UTC, or None for unavailable evidence.
    """
    raw = record.get("last_updated_utc")
    if not isinstance(raw, str):
        return None
    try:
        value = datetime.fromisoformat(raw)
        if value.utcoffset() is None or value > now:
            return None
        return value.astimezone(UTC)
    except ValueError, OverflowError:
        return None


def _stock_profile_kind(
    record: PolygonSymbolRecord, base: str, quote: str | None
) -> Literal["spot", "etf"] | None:
    """Bind a narrow stock profile to exact provider ticker and reported currency.

    Args:
        record: Provider record with explicit market, type, and active status.
        base: Source catalog base identifier, preserved without renaming symbols.
        quote: Source catalog quote identifier.

    Returns:
        Cash share kind only for consistent active CS or ETF stock records.
    """
    if record.get("market") != "stocks" or record.get("active") is not True:
        return None
    if not base or base != record["ticker"] or quote is None or quote != _reported_quote(record):
        return None
    if record.get("type") == "CS":
        return "spot"
    return "etf" if record.get("type") == "ETF" else None


def _legacy_spot_kind(record: PolygonSymbolRecord) -> Literal["spot"] | None:
    """Preserve compatible existing crypto/FX kinds while rejecting contradictions.

    These historical kinds are separate from the new stock attestation. Their
    atomic metadata replacement remains empty, including after a stock change.

    Args:
        record: Exact provider record including its existing ticker identifier.

    Returns:
        Existing spot kind for compatible crypto/FX records, otherwise None.
    """
    ticker = record["ticker"]
    expected = "crypto" if ticker.startswith("X:") else "fx" if ticker.startswith("C:") else None
    if expected is None or record.get("market") not in {None, expected}:
        return None
    accepted_type = "X" if expected == "crypto" else "C"
    return "spot" if record.get("type") in {None, expected, accepted_type} else None


def _replacement_metadata(
    record: PolygonSymbolRecord, provider_at: datetime | None
) -> InstrumentMetadataInput:
    """Create a full noncontract metadata replacement, including explicit revocation.

    Args:
        record: Exact record whose market, type, ticker, and quote enter its digest.
        provider_at: Verified stock reference timestamp, or None to revoke evidence.

    Returns:
        Atomic metadata with a complete provenance triple or entirely empty proof.
    """
    version = None
    if provider_at is not None:
        material = {
            "ticker": record["ticker"],
            "market": record.get("market"),
            "type": record.get("type"),
            "active": record.get("active"),
            "currency_symbol": record.get("currency_symbol"),
            "currency_name": record.get("currency_name"),
            "last_updated_utc": record.get("last_updated_utc"),
        }
        content = json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        digest = hashlib.sha256(content.encode()).hexdigest()
        version = f"{POLYGON_CASH_PROFILE_VERSION}:{record['type']}:{digest}"
    return InstrumentMetadataInput(
        tick_size=None,
        lot_size=None,
        min_order_size=None,
        max_order_size=None,
        cost_decimals=None,
        qty_decimals=None,
        margin_initial=None,
        position_limit_long=None,
        position_limit_short=None,
        status="active" if provider_at is not None else None,
        contract_size=None,
        quantity_unit="base_asset" if provider_at is not None else None,
        native_order_quantity_unit="base_asset" if provider_at is not None else None,
        spec_source=POLYGON_CASH_PROFILE_SOURCE if provider_at is not None else None,
        spec_version=version,
        spec_observed_at=provider_at,
        unit_certified=False,
    )


def interpret_polygon_cash_profile(
    record: PolygonSymbolRecord, base: str, quote: str | None, now: datetime
) -> PolygonCashProfile:
    """Interpret one source record without broadening a symbol's catalog identity.

    Unknown, inactive, malformed, and changed stock records replace the previous
    stock interpretation with None. Compatible historical crypto/FX records keep
    their spot kind but explicitly replace all old stock/contract metadata.

    Args:
        record: Exact Polygon reference response or unchanged cached record.
        base: Current source catalog base identifier.
        quote: Current source catalog quote identifier.
        now: Aware UTC interpretation time; it never replaces provider evidence.

    Returns:
        Explicit cash profile or unproven replacement for this exact source record.

    Raises:
        ValueError: When interpretation time is naive.
    """
    if now.utcoffset() is None:
        raise ValueError("Polygon interpretation time must be timezone aware")
    kind = _stock_profile_kind(record, base, quote)
    provider_at = _provider_timestamp(record, now) if kind is not None else None
    if provider_at is None:
        return PolygonCashProfile(_legacy_spot_kind(record), _replacement_metadata(record, None))
    return PolygonCashProfile(kind, _replacement_metadata(record, provider_at))
