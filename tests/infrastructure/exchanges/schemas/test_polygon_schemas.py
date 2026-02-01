"""Tests for Polygon exchange schema dataclasses."""

from types import SimpleNamespace

import pytest

from snapper.infrastructure.exchanges.schemas.polygon import PolygonAgg
from snapper.infrastructure.exchanges.schemas.polygon import PolygonGroupedAgg
from snapper.infrastructure.exchanges.schemas.polygon import PolygonPreviousClose
from snapper.infrastructure.exchanges.schemas.polygon import PolygonTicker


class TestPolygonAgg:
    """Tests for Polygon aggregate data schema."""

    def test_from_sdk_agg_with_all_fields(self) -> None:
        """Convert SDK aggregate with all fields to PolygonAgg.

        Given: SDK aggregate object with all OHLCV fields populated,
        When: from_sdk_agg is called,
        Then: All fields are correctly extracted into PolygonAgg.
        """
        sdk_agg = SimpleNamespace(
            open=100.5,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000000.0,
            vwap=102.0,
            timestamp=1640000000000,
            transactions=500,
            otc=False,
        )
        result = PolygonAgg.from_sdk_agg(sdk_agg)
        assert result.open == pytest.approx(100.5)
        assert result.high == pytest.approx(105.0)
        assert result.low == pytest.approx(99.0)
        assert result.close == pytest.approx(103.0)
        assert result.volume == pytest.approx(1000000.0)
        assert result.vwap == pytest.approx(102.0)
        assert result.timestamp == 1640000000000
        assert result.transactions == 500
        assert result.otc is False

    def test_from_sdk_agg_with_missing_fields(self) -> None:
        """Convert SDK aggregate with missing optional fields.

        Given: SDK aggregate with only open and close fields,
        When: from_sdk_agg is called,
        Then: Missing fields default to None.
        """
        sdk_agg = SimpleNamespace(
            open=100.5,
            close=103.0,
        )
        result = PolygonAgg.from_sdk_agg(sdk_agg)
        assert result.open == pytest.approx(100.5)
        assert result.close == pytest.approx(103.0)
        assert result.high is None
        assert result.low is None
        assert result.volume is None
        assert result.vwap is None
        assert result.timestamp is None
        assert result.transactions is None
        assert result.otc is None


class TestPolygonGroupedAgg:
    """Tests for Polygon grouped aggregate data schema."""

    def test_from_sdk_agg_with_full_field_names(self) -> None:
        """Parse grouped aggregate using full field names.

        Given: SDK aggregate with full field names (ticker, open, high, etc.),
        When: from_sdk_agg is called,
        Then: PolygonGroupedAgg is created with all fields.
        """
        sdk_agg = SimpleNamespace(
            ticker="X:BTCUSD",
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            vwap=50250.0,
            timestamp=1640000000000,
            transactions=1000,
        )
        result = PolygonGroupedAgg.from_sdk_agg(sdk_agg)
        assert result.ticker == "X:BTCUSD"
        assert result.open == pytest.approx(50000.0)
        assert result.high == pytest.approx(51000.0)
        assert result.low == pytest.approx(49000.0)
        assert result.close == pytest.approx(50500.0)
        assert result.volume == pytest.approx(100.0)
        assert result.vwap == pytest.approx(50250.0)
        assert result.timestamp == 1640000000000
        assert result.transactions == 1000

    def test_from_sdk_agg_with_short_field_names(self) -> None:
        """Parse grouped aggregate using short field aliases.

        Given: SDK aggregate with short field names (T, o, h, l, c, etc.),
        When: from_sdk_agg is called,
        Then: PolygonGroupedAgg correctly maps aliases to fields.
        """
        sdk_agg = SimpleNamespace(
            T="X:ETHUSD",
            o=3000.0,
            h=3100.0,
            l=2900.0,
            c=3050.0,
            v=500.0,
            vw=3025.0,
            t=1640000000000,
            n=2000,
        )
        result = PolygonGroupedAgg.from_sdk_agg(sdk_agg)
        assert result.ticker == "X:ETHUSD"
        assert result.open == pytest.approx(3000.0)
        assert result.high == pytest.approx(3100.0)
        assert result.low == pytest.approx(2900.0)
        assert result.close == pytest.approx(3050.0)
        assert result.volume == pytest.approx(500.0)
        assert result.vwap == pytest.approx(3025.0)
        assert result.timestamp == 1640000000000
        assert result.transactions == 2000

    def test_from_sdk_agg_prefers_full_names_over_short(self) -> None:
        """Full field names take precedence over short aliases.

        Given: SDK aggregate with both full and short names for same fields,
        When: from_sdk_agg is called,
        Then: Full names are used instead of short aliases.
        """
        sdk_agg = SimpleNamespace(
            ticker="X:BTCUSD",
            T="X:ETHUSD",
            open=50000.0,
            o=3000.0,
        )
        result = PolygonGroupedAgg.from_sdk_agg(sdk_agg)
        assert result.ticker == "X:BTCUSD"
        assert result.open == pytest.approx(50000.0)


class TestPolygonPreviousClose:
    """Tests for Polygon previous close data schema."""

    def test_from_sdk_agg_with_all_fields(self) -> None:
        """Create previous close with all fields populated.

        Given: Ticker string and SDK aggregate with all OHLCV fields,
        When: from_sdk_agg is called,
        Then: PolygonPreviousClose includes ticker and all fields.
        """
        ticker = "X:BTCUSD"
        sdk_agg = SimpleNamespace(
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            volume=100.0,
            vwap=50250.0,
            timestamp=1640000000000,
        )
        result = PolygonPreviousClose.from_sdk_agg(ticker, sdk_agg)
        assert result.ticker == "X:BTCUSD"
        assert result.open == pytest.approx(50000.0)
        assert result.high == pytest.approx(51000.0)
        assert result.low == pytest.approx(49000.0)
        assert result.close == pytest.approx(50500.0)
        assert result.volume == pytest.approx(100.0)
        assert result.vwap == pytest.approx(50250.0)
        assert result.timestamp == 1640000000000

    def test_from_sdk_agg_with_missing_fields(self) -> None:
        """Create previous close with only close price.

        Given: Ticker and SDK aggregate with only close field,
        When: from_sdk_agg is called,
        Then: Missing fields default to None.
        """
        ticker = "C:EURUSD"
        sdk_agg = SimpleNamespace(
            close=1.1850,
        )
        result = PolygonPreviousClose.from_sdk_agg(ticker, sdk_agg)
        assert result.ticker == "C:EURUSD"
        assert result.close == pytest.approx(1.1850)
        assert result.open is None
        assert result.high is None
        assert result.low is None
        assert result.volume is None
        assert result.vwap is None
        assert result.timestamp is None


class TestPolygonTicker:
    """Tests for Polygon ticker metadata schema."""

    def test_from_sdk_ticker_with_all_fields(self) -> None:
        """Convert SDK ticker with full metadata.

        Given: SDK ticker with all metadata fields populated,
        When: from_sdk_ticker is called,
        Then: PolygonTicker contains all metadata.
        """
        sdk_ticker = SimpleNamespace(
            ticker="X:BTCUSD",
            name="Bitcoin / US Dollar",
            market="crypto",
            locale="global",
            currency_symbol="USD",
            currency_name="United States Dollar",
            base_currency_symbol="BTC",
            base_currency_name="Bitcoin",
            active=True,
            last_updated_utc="2024-01-01T00:00:00Z",
        )
        result = PolygonTicker.from_sdk_ticker(sdk_ticker)
        assert result.ticker == "X:BTCUSD"
        assert result.name == "Bitcoin / US Dollar"
        assert result.market == "crypto"
        assert result.locale == "global"
        assert result.currency_symbol == "USD"
        assert result.currency_name == "United States Dollar"
        assert result.base_currency_symbol == "BTC"
        assert result.base_currency_name == "Bitcoin"
        assert result.active is True
        assert result.last_updated_utc == "2024-01-01T00:00:00Z"

    def test_from_sdk_ticker_with_minimal_fields(self) -> None:
        """Convert SDK ticker with only ticker symbol.

        Given: SDK ticker with only ticker field set,
        When: from_sdk_ticker is called,
        Then: All optional fields are None.
        """
        sdk_ticker = SimpleNamespace(
            ticker="C:EURUSD",
        )
        result = PolygonTicker.from_sdk_ticker(sdk_ticker)
        assert result.ticker == "C:EURUSD"
        assert result.name is None
        assert result.market is None
        assert result.locale is None
        assert result.currency_symbol is None
        assert result.currency_name is None
        assert result.base_currency_symbol is None
        assert result.base_currency_name is None
        assert result.active is None
        assert result.last_updated_utc is None

    def test_from_sdk_ticker_with_empty_ticker_uses_empty_string(self) -> None:
        """Handle SDK ticker without ticker field.

        Given: SDK ticker without ticker attribute,
        When: from_sdk_ticker is called,
        Then: Ticker defaults to empty string.
        """
        sdk_ticker = SimpleNamespace(
            name="Test Asset",
        )
        result = PolygonTicker.from_sdk_ticker(sdk_ticker)
        assert result.ticker == ""
        assert result.name == "Test Asset"
