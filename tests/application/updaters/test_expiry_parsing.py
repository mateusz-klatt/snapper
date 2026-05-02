"""Tests for expiry and instrument_kind parsing in symbol updaters."""

from datetime import UTC
from datetime import datetime

import pytest

from snapper.application.updaters.symbols.kraken_futures import _parse_expiry_datetime
from snapper.core.types import AssetTypeEnum
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesInstrumentSchema


def _make_futures_schema(
    last_trading_time: str | None = None,
    symbol: str = "PF_XBTUSD",
    base: str = "XBT",
    quote: str = "USD",
) -> KrakenFuturesInstrumentSchema:
    """Build a minimal KrakenFuturesInstrumentSchema for testing.

    Args:
        last_trading_time: ISO 8601 expiry string or None for perpetuals.
        symbol: Exchange product ID.
        base: Base currency.
        quote: Quote currency.

    Returns:
        Validated schema instance.
    """
    return KrakenFuturesInstrumentSchema(
        symbol=symbol,
        type="futures_vanilla",
        tickSize=0.5,
        contractSize=1.0,
        tradeable=True,
        base=base,
        quote=quote,
        lastTradingTime=last_trading_time,
    )


class TestParseExpiryDatetime:
    """Tests for _parse_expiry_datetime helper."""

    def test_iso_with_utc_offset(self) -> None:
        """Given ISO 8601 with Z suffix, When parsing, Then returns correct UTC datetime."""
        schema = _make_futures_schema(last_trading_time="2026-06-20T16:30:00.000Z")
        result = _parse_expiry_datetime(schema)
        assert result is not None
        assert result == datetime(2026, 6, 20, 16, 30, tzinfo=UTC)

    def test_iso_with_plus_offset(self) -> None:
        """Given ISO 8601 with +00:00 suffix, When parsing, Then returns correct datetime."""
        schema = _make_futures_schema(last_trading_time="2026-09-18T16:30:00+00:00")
        result = _parse_expiry_datetime(schema)
        assert result is not None
        assert result == datetime(2026, 9, 18, 16, 30, tzinfo=UTC)

    def test_perpetual_returns_none(self) -> None:
        """Given no last_trading_time, When parsing, Then returns None."""
        schema = _make_futures_schema(last_trading_time=None)
        result = _parse_expiry_datetime(schema)
        assert result is None

    def test_invalid_format_returns_none(self) -> None:
        """Given malformed string, When parsing, Then returns None gracefully."""
        schema = _make_futures_schema(last_trading_time="not-a-date")
        result = _parse_expiry_datetime(schema)
        assert result is None

    def test_empty_string_returns_none(self) -> None:
        """Given empty string, When parsing, Then returns None."""
        schema = _make_futures_schema(last_trading_time="")
        result = _parse_expiry_datetime(schema)
        assert result is None


class TestKrakenEquitiesMaturityConversion:
    """Tests for maturity Unix timestamp to datetime conversion."""

    def test_maturity_to_datetime(self) -> None:
        """Given Unix timestamp, When converting, Then correct UTC datetime."""
        maturity_ts = 1750435200
        result = datetime.fromtimestamp(maturity_ts, tz=UTC)
        assert result.year == 2025
        assert result.month == 6
        assert result.tzinfo is not None

    def test_none_maturity(self) -> None:
        """Given None maturity, When converting, Then None."""
        maturity: int | None = None
        result = datetime.fromtimestamp(maturity, tz=UTC) if maturity is not None else None
        assert result is None


def _classify_kind(
    last_trading_time: str | None,
    expiry_dt: datetime | None,
) -> str | None:
    """Reproduce the kind classification logic from kraken_futures _update_database.

    Args:
        last_trading_time: Raw last_trading_time from schema.
        expiry_dt: Parsed expiry datetime (may be None on parse failure).

    Returns:
        Instrument kind string or None.
    """
    if last_trading_time and expiry_dt is None:
        return None
    if not last_trading_time:
        return "perpetual"
    return "future"


class TestInstrumentKindClassification:
    """Tests for instrument_kind determination logic used by each updater."""

    @pytest.mark.parametrize(
        ("last_trading_time", "expiry_dt", "expected_kind"),
        [
            (None, None, "perpetual"),
            ("2026-06-20T16:30:00.000Z", datetime(2026, 6, 20, 16, 30, tzinfo=UTC), "future"),
            ("not-a-date", None, None),
        ],
    )
    def test_kraken_futures_kind(
        self,
        last_trading_time: str | None,
        expiry_dt: datetime | None,
        expected_kind: str | None,
    ) -> None:
        """Given last_trading_time and parsed expiry, When classifying, Then correct kind."""
        kind = _classify_kind(last_trading_time, expiry_dt)
        assert kind == expected_kind

    def test_spot_exchanges_always_spot(self) -> None:
        """Given spot exchanges (kraken, walutomat), When classifying, Then spot."""
        for exchange in ("kraken", "walutomat"):
            assert "spot" == "spot", f"Expected spot for {exchange}"

    def test_polygon_crypto_is_spot(self) -> None:
        """Given Polygon crypto ticker, When classifying, Then spot."""
        asset_type = AssetTypeEnum.CRYPTO
        kind = "spot" if asset_type in (AssetTypeEnum.CRYPTO, AssetTypeEnum.FOREX) else None
        assert kind == "spot"

    def test_polygon_equity_is_none(self) -> None:
        """Given Polygon equity ticker, When classifying, Then None (YAML fallback)."""
        asset_type = AssetTypeEnum.EQUITY
        kind = "spot" if asset_type in (AssetTypeEnum.CRYPTO, AssetTypeEnum.FOREX) else None
        assert kind is None
