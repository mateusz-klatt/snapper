"""Unit tests for Walutomat exchange schemas."""

from datetime import UTC
from datetime import datetime

import pytest

from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatDayExchange
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatLastExchange
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatMarketResponse


class TestWalutomatLastExchangeValidator:
    """Tests for Walutomat last exchange timestamp validation."""

    def test_parse_ts_with_datetime_object(self) -> None:
        """Verify datetime object is passed through unchanged.

        Given a datetime object with UTC timezone,
        When WalutomatLastExchange is created,
        Then ts field equals the input datetime.
        """
        dt = datetime(2026, 12, 22, 17, 4, 1, 356615, tzinfo=UTC)
        exchange = WalutomatLastExchange(ts=dt, price=4.2161, volume=700)
        assert exchange.ts == dt

    def test_parse_ts_with_iso_string_z_suffix(self) -> None:
        """Verify ISO string with Z suffix is parsed correctly.

        Given a datetime string ending with Z (UTC),
        When WalutomatLastExchange is created,
        Then ts is parsed with correct year, month, day.
        """
        exchange = WalutomatLastExchange(
            ts=datetime.fromisoformat("2026-12-22T17:04:01.356615829Z".replace("Z", "+00:00")),
            price=4.2161,
            volume=700,
        )
        assert exchange.ts.year == 2026
        assert exchange.ts.month == 12
        assert exchange.ts.day == 22


class TestWalutomatDayExchangeValidator:
    """Tests for Walutomat day exchange date validation."""

    def test_parse_day_with_datetime_object(self) -> None:
        """Verify datetime object is passed through unchanged.

        Given a datetime object for midnight UTC,
        When WalutomatDayExchange is created,
        Then day field equals the input datetime.
        """
        dt = datetime(2026, 12, 22, 0, 0, 0, tzinfo=UTC)
        day_exchange = WalutomatDayExchange(day=dt, volume=5772895.4)
        assert day_exchange.day == dt

    def test_parse_day_with_iso_string_z_suffix(self) -> None:
        """Verify ISO date string with Z suffix is parsed correctly.

        Given a date string ending with Z (UTC),
        When WalutomatDayExchange is created,
        Then day is parsed with correct year, month, day.
        """
        day_exchange = WalutomatDayExchange(
            day=datetime.fromisoformat("2026-12-22T00:00:00Z".replace("Z", "+00:00")),
            volume=5772895.4,
        )
        assert day_exchange.day.year == 2026
        assert day_exchange.day.month == 12
        assert day_exchange.day.day == 22


class TestWalutomatMarketResponse:
    """Tests for Walutomat market response parsing."""

    def test_from_api_response(self) -> None:
        """Verify API response is parsed into market response object.

        Given raw API response list with pair data,
        When from_api_response is called,
        Then returns WalutomatMarketResponse with parsed pairs.
        """
        api_data = [
            {
                "pair": "EUR_PLN",
                "bestOffers": {
                    "bid_now": 4.2161,
                    "ask_now": 4.2199,
                    "forex_now": 4.219,
                },
                "lastExchanges": [
                    {"ts": "2026-12-22T17:04:01.356615829Z", "price": 4.2161, "volume": 700}
                ],
                "dayExchanges": [{"day": "2026-12-22T00:00:00Z", "volume": 5772895.4}],
            }
        ]
        response = WalutomatMarketResponse.from_api_response(api_data)
        assert len(response.pairs) == 1
        assert response.pairs[0].pair == "EUR_PLN"

    def test_to_dict(self) -> None:
        """Verify market response converts to dict keyed by pair.

        Given a WalutomatMarketResponse with multiple pairs,
        When to_dict is called,
        Then returns dict with pair names as keys.
        """
        api_data = [
            {
                "pair": "EUR_PLN",
                "bestOffers": {
                    "bid_now": 4.2161,
                    "ask_now": 4.2199,
                    "forex_now": 4.219,
                },
            },
            {
                "pair": "USD_PLN",
                "bestOffers": {
                    "bid_now": 4.0,
                    "ask_now": 4.1,
                    "forex_now": 4.05,
                },
            },
        ]
        response = WalutomatMarketResponse.from_api_response(api_data)
        pairs_dict = response.to_dict()
        assert "EUR_PLN" in pairs_dict
        assert "USD_PLN" in pairs_dict
        assert pairs_dict["EUR_PLN"].best_offers.bid_now == pytest.approx(4.2161)
