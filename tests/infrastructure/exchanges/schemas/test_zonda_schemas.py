"""Unit tests for Zonda exchange schemas."""

from snapper.infrastructure.exchanges.schemas.zonda import ZondaStatsData


class TestZondaStatsDataValidator:
    """Tests for Zonda stats data numeric field validation."""

    def test_coerce_to_float_with_none_returns_zero(self) -> None:
        """Verify None values are coerced to 0.0 for numeric fields.

        Given ZondaStatsData with None values for h, l, v, r24h,
        When the schema is instantiated,
        Then all numeric fields are coerced to 0.0.
        """
        stats = ZondaStatsData(m="BTC-PLN", h=None, l=None, v=None, r24h=None)
        assert stats.h == 0.0
        assert stats.l == 0.0
        assert stats.v == 0.0
        assert stats.r24h == 0.0

    def test_coerce_to_float_with_empty_string_returns_zero(self) -> None:
        """Verify empty strings are coerced to 0.0.

        Given ZondaStatsData with empty strings for numeric fields,
        When the schema is instantiated,
        Then all numeric fields are coerced to 0.0.
        """
        stats = ZondaStatsData(m="ETH-PLN", h="", l="", v="", r24h="")
        assert stats.h == 0.0
        assert stats.l == 0.0
        assert stats.v == 0.0
        assert stats.r24h == 0.0

    def test_coerce_to_float_with_valid_float_string(self) -> None:
        """Verify valid float strings are parsed correctly.

        Given ZondaStatsData with string representations of floats,
        When the schema is instantiated,
        Then strings are parsed to correct float values.
        """
        stats = ZondaStatsData(m="BTC-PLN", h="100.5", l="99.0", v="1000", r24h="0.05")
        assert stats.h == 100.5
        assert stats.l == 99.0
        assert stats.v == 1000.0
        assert stats.r24h == 0.05

    def test_coerce_to_float_with_numeric_values(self) -> None:
        """Verify numeric values are passed through unchanged.

        Given ZondaStatsData with actual float/int values,
        When the schema is instantiated,
        Then values are preserved as floats.
        """
        stats = ZondaStatsData(m="BTC-PLN", h=100.5, l=99.0, v=1000, r24h=0.05)
        assert stats.h == 100.5
        assert stats.l == 99.0
        assert stats.v == 1000.0
        assert stats.r24h == 0.05
