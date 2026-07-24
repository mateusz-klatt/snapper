"""Tests for pure currency-to-USD basket valuation (Phase 5B).

Covers per-currency valuation (identity, currency-invariant zero, fiat conversion
composed off the forex module, crypto conversion off the pre-loaded plane) and
the position/cash partition (the per-(exchange, currency) single cap, cross-
exchange isolation, and leveraged-inventory reporting). Every withheld leg must
return ``None`` with a typed reason rather than fabricate a rate, and every priced
leg must carry the exact plane and candle version it used.
"""

from datetime import UTC
from datetime import datetime

import pytest

from snapper.application.portfolio.basket_valuation import CandleVersionIdentity
from snapper.application.portfolio.basket_valuation import CryptoUsdCandle
from snapper.application.portfolio.basket_valuation import CryptoUsdPlaneMap
from snapper.application.portfolio.basket_valuation import FiatVersionMap
from snapper.application.portfolio.basket_valuation import PositionInventoryEntry
from snapper.application.portfolio.basket_valuation import ValuationEvidence
from snapper.application.portfolio.basket_valuation import attribute_position_inventory
from snapper.application.portfolio.basket_valuation import value_currency
from snapper.application.portfolio.fx_rates import FxRateMap
from snapper.application.portfolio.fx_rates import FxVenueMap

_M = datetime(2026, 7, 19, 19, 22, tzinfo=UTC)
_VERSION = datetime(2026, 7, 19, 19, 22, 30, tzinfo=UTC)
_FIAT_PLANES: FxVenueMap = {
    ("EUR", "USD"): ("EUR", "USD", "kraken"),
    ("PLN", "USD"): ("USD", "PLN", "kraken"),
}


def _fiat_rates(**pairs: float) -> FxRateMap:
    """Build a fiat rate map at :data:`_M` from ``BASEQUOTE=close`` keywords."""
    return {(name[:3], name[3:], "kraken", _M): close for name, close in pairs.items()}


def _fiat_versions(base: str, quote: str) -> FiatVersionMap:
    """Build a fiat version map pinning one ``(base, quote, kraken, _M)`` candle."""
    return {
        (base, quote, "kraken", _M): CandleVersionIdentity(
            instrument_public_id=f"ins-{base.lower()}{quote.lower()}-kraken",
            native_symbol=f"{base}-{quote}",
            candle_id=707,
            candle_public_id=f"cdl-{base.lower()}{quote.lower()}",
            candle_open_at=_M,
            candle_timestamp=_VERSION,
        )
    }


def _crypto_candle(
    base: str,
    exchange: str,
    close: float,
    *,
    instrument_public_id: str = "",
    candle_public_id: str = "",
) -> CryptoUsdCandle:
    """Build one crypto→USD candle carrying a distinguishable version identity."""
    return CryptoUsdCandle(
        base=base,
        quote="USD",
        exchange=exchange,
        native_symbol=f"{base}-USD",
        instrument_public_id=instrument_public_id or f"ins-{base.lower()}-{exchange}",
        candle_id=101,
        candle_public_id=candle_public_id or f"cdl-{base.lower()}-{exchange}",
        candle_open_at=_M,
        candle_timestamp=_VERSION,
        close=close,
    )


def _evidence(
    fiat_rates: FxRateMap | None = None,
    fiat_venues: FxVenueMap | None = None,
    crypto_planes: CryptoUsdPlaneMap | None = None,
    fiat_versions: FiatVersionMap | None = None,
) -> ValuationEvidence:
    """Assemble valuation evidence, defaulting each plane to empty."""
    return ValuationEvidence(
        fiat_rates=fiat_rates or {},
        fiat_venues=_FIAT_PLANES if fiat_venues is None else fiat_venues,
        fiat_versions=fiat_versions or {},
        crypto_planes=crypto_planes or {},
    )


class TestIdentityAndZero:
    """Cover the legs that need no candle evidence at all."""

    def test_usd_is_the_identity_with_unit_rate(self) -> None:
        """A USD balance is its own USD value with identity provenance."""
        leg = value_currency("kraken", "USD", 12.5, _M, _evidence())
        assert leg.usd_value == 12.5
        assert leg.reason is None
        assert leg.provenance is not None
        assert leg.provenance.kind == "identity"
        assert leg.provenance.rate == 1.0
        assert leg.provenance.exchange == "kraken"
        assert leg.provenance.candle is None

    def test_zero_needs_no_rate_evidence(self) -> None:
        """An exact-zero balance values to zero without any plane or provenance."""
        leg = value_currency("kraken", "BTC", 0.0, _M, _evidence())
        assert leg.usd_value == 0.0
        assert leg.provenance is None
        assert leg.reason is None


class TestFiatPath:
    """Cover fiat legs composed off the forex conversion primitives."""

    def test_direct_pair_multiplies_and_records_version_identity(self) -> None:
        """A EUR balance converts through the quoted close and pins its candle."""
        leg = value_currency(
            "kraken",
            "EUR",
            10.0,
            _M,
            _evidence(_fiat_rates(EURUSD=1.25), fiat_versions=_fiat_versions("EUR", "USD")),
        )
        assert leg.usd_value == pytest.approx(12.5)
        assert leg.reason is None
        assert leg.provenance is not None
        assert leg.provenance.kind == "fiat_fx"
        assert (leg.provenance.base, leg.provenance.quote, leg.provenance.exchange) == (
            "EUR",
            "USD",
            "kraken",
        )
        assert leg.provenance.rate == pytest.approx(1.25)
        assert leg.provenance.candle is not None
        assert leg.provenance.candle.candle_id == 707
        assert leg.provenance.candle.candle_public_id == "cdl-eurusd"
        assert leg.provenance.candle.candle_timestamp == _VERSION

    def test_inverse_pair_divides_and_records_the_reciprocal_rate(self) -> None:
        """A PLN balance converts by the reciprocal of the pinned USD-PLN close."""
        leg = value_currency(
            "kraken",
            "PLN",
            8.0,
            _M,
            _evidence(_fiat_rates(USDPLN=4.0), fiat_versions=_fiat_versions("USD", "PLN")),
        )
        assert leg.usd_value == pytest.approx(2.0)
        assert leg.provenance is not None
        assert leg.provenance.rate == pytest.approx(0.25)
        assert leg.provenance.candle is not None
        assert leg.provenance.candle.candle_id == 707

    def test_overflowing_fiat_conversion_is_refused(self) -> None:
        """A finite balance and close whose product overflows withholds the leg."""
        leg = value_currency("kraken", "EUR", 1e308, _M, _evidence(_fiat_rates(EURUSD=1e308)))
        assert leg.usd_value is None
        assert leg.provenance is None
        assert leg.reason == "overflow"

    def test_missing_version_identity_withholds_a_valid_rate(self) -> None:
        """A usable rate with no candle version identity is unattributable and withheld."""
        leg = value_currency("kraken", "EUR", 10.0, _M, _evidence(_fiat_rates(EURUSD=1.25)))
        assert leg.usd_value is None
        assert leg.provenance is None
        assert leg.reason == "missing_version"

    def test_pinned_fiat_plane_missing_minute_withholds(self) -> None:
        """A pinned fiat plane with no close at the minute yields missing_rate."""
        leg = value_currency("kraken", "EUR", 10.0, _M, _evidence(_fiat_rates()))
        assert leg.usd_value is None
        assert leg.provenance is None
        assert leg.reason == "missing_rate"

    def test_pinned_fiat_plane_never_falls_through_to_crypto(self) -> None:
        """A fiat-pinned currency commits to the fiat plane despite a crypto candle."""
        crypto = {("EUR", _M): (_crypto_candle("EUR", "kraken", 9.99),)}
        leg = value_currency(
            "kraken", "EUR", 10.0, _M, _evidence(_fiat_rates(), crypto_planes=crypto)
        )
        assert leg.usd_value is None
        assert leg.reason == "missing_rate"


class TestCryptoPath:
    """Cover crypto legs priced off the pre-loaded crypto→USD plane."""

    def test_holding_exchange_plane_is_preferred(self) -> None:
        """The candle on the balance's own exchange wins over a rival venue."""
        crypto = {
            ("BTC", _M): (
                _crypto_candle("BTC", "binance", 60_000.0),
                _crypto_candle("BTC", "kraken", 61_000.0),
            )
        }
        leg = value_currency("kraken", "BTC", 2.0, _M, _evidence(crypto_planes=crypto))
        assert leg.usd_value == pytest.approx(122_000.0)
        assert leg.provenance is not None
        assert leg.provenance.kind == "crypto_candle"
        assert leg.provenance.exchange == "kraken"
        assert leg.provenance.rate == pytest.approx(61_000.0)
        assert leg.provenance.candle is not None
        assert leg.provenance.candle.candle_id == 101
        assert leg.provenance.candle.candle_public_id == "cdl-btc-kraken"
        assert leg.provenance.candle.candle_timestamp == _VERSION
        assert leg.provenance.candle.native_symbol == "BTC-USD"

    def test_fallback_uses_deterministic_first_plane(self) -> None:
        """Without the holding exchange, the (base, quote, exchange)-first plane wins."""
        crypto = {
            ("BTC", _M): (
                _crypto_candle("BTC", "kraken", 61_000.0),
                _crypto_candle("BTC", "binance", 60_000.0),
            )
        }
        leg = value_currency("coinbase", "BTC", 1.0, _M, _evidence(crypto_planes=crypto))
        assert leg.usd_value == pytest.approx(60_000.0)
        assert leg.provenance is not None
        assert leg.provenance.exchange == "binance"

    def test_missing_currency_withholds(self) -> None:
        """A crypto currency with no candle at the minute yields missing_rate."""
        leg = value_currency("kraken", "BTC", 1.0, _M, _evidence())
        assert leg.usd_value is None
        assert leg.reason == "missing_rate"

    def test_only_non_positive_closes_withholds(self) -> None:
        """Candidates present but with no positive-finite close yield missing_rate."""
        crypto = {
            ("BTC", _M): (
                _crypto_candle("BTC", "kraken", 0.0),
                _crypto_candle("BTC", "binance", -5.0),
            )
        }
        leg = value_currency("kraken", "BTC", 1.0, _M, _evidence(crypto_planes=crypto))
        assert leg.usd_value is None
        assert leg.reason == "missing_rate"

    def test_non_positive_holding_close_is_skipped_for_a_valid_rival(self) -> None:
        """A degenerate holding-exchange close does not block a valid rival plane."""
        crypto = {
            ("BTC", _M): (
                _crypto_candle("BTC", "kraken", 0.0),
                _crypto_candle("BTC", "binance", 60_000.0),
            )
        }
        leg = value_currency("kraken", "BTC", 1.0, _M, _evidence(crypto_planes=crypto))
        assert leg.usd_value == pytest.approx(60_000.0)
        assert leg.provenance is not None
        assert leg.provenance.exchange == "binance"

    def test_negative_balance_keeps_its_sign(self) -> None:
        """A negative crypto quantity values to a negative finite USD amount."""
        crypto = {("BTC", _M): (_crypto_candle("BTC", "kraken", 100.0),)}
        leg = value_currency("kraken", "BTC", -3.0, _M, _evidence(crypto_planes=crypto))
        assert leg.usd_value == pytest.approx(-300.0)

    def test_overflowing_product_is_refused(self) -> None:
        """A finite quantity times a finite close that overflows withholds."""
        crypto = {("BTC", _M): (_crypto_candle("BTC", "kraken", 1e308),)}
        leg = value_currency("kraken", "BTC", 1e308, _M, _evidence(crypto_planes=crypto))
        assert leg.usd_value is None
        assert leg.reason == "overflow"

    def test_two_instruments_on_the_selected_venue_fail_closed(self) -> None:
        """Two distinct instruments pricing one venue plane are an ambiguity, not an election."""
        crypto = {
            ("BTC", _M): (
                _crypto_candle(
                    "BTC",
                    "kraken",
                    60_000.0,
                    instrument_public_id="ins-a",
                    candle_public_id="cdl-a",
                ),
                _crypto_candle(
                    "BTC",
                    "kraken",
                    61_000.0,
                    instrument_public_id="ins-b",
                    candle_public_id="cdl-b",
                ),
            )
        }
        leg = value_currency("kraken", "BTC", 1.0, _M, _evidence(crypto_planes=crypto))
        assert leg.usd_value is None
        assert leg.provenance is None
        assert leg.reason == "ambiguous_plane"

    def test_duplicate_on_unselected_venue_does_not_block_the_selected_plane(self) -> None:
        """An ambiguity on a venue we do not price never withholds the chosen plane."""
        crypto = {
            ("BTC", _M): (
                _crypto_candle("BTC", "kraken", 61_000.0),
                _crypto_candle(
                    "BTC",
                    "binance",
                    60_000.0,
                    instrument_public_id="ins-x",
                    candle_public_id="cdl-x",
                ),
                _crypto_candle(
                    "BTC",
                    "binance",
                    60_500.0,
                    instrument_public_id="ins-y",
                    candle_public_id="cdl-y",
                ),
            )
        }
        leg = value_currency("kraken", "BTC", 1.0, _M, _evidence(crypto_planes=crypto))
        assert leg.usd_value == pytest.approx(61_000.0)
        assert leg.provenance is not None
        assert leg.provenance.exchange == "kraken"


class TestNonFiniteQuantity:
    """Cover the refusal of a non-finite input quantity."""

    def test_infinite_quantity_is_refused(self) -> None:
        """An infinite balance is refused before any plane is consulted."""
        leg = value_currency("kraken", "EUR", float("inf"), _M, _evidence(_fiat_rates(EURUSD=1.25)))
        assert leg.usd_value is None
        assert leg.provenance is None
        assert leg.reason == "non_finite"

    def test_nan_quantity_is_refused(self) -> None:
        """A NaN balance is refused with a non_finite reason."""
        leg = value_currency("kraken", "BTC", float("nan"), _M, _evidence())
        assert leg.usd_value is None
        assert leg.reason == "non_finite"


class TestAttributePositionInventory:
    """Cover the per-(exchange, currency) single-cap position/cash partition."""

    def test_single_balance_caps_the_summed_positions(self) -> None:
        """One BTC observed with 0.8 and 0.7 long positions attributes 1.0, not 1.5."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): 1.0},
            [
                PositionInventoryEntry("kraken", "BTC", 0.8, is_spot_margin=False),
                PositionInventoryEntry("kraken", "BTC", 0.7, is_spot_margin=False),
            ],
        )
        assert not result.leveraged_inventory_excluded
        assert len(result.attributions) == 1
        entry = result.attributions[0]
        assert entry.observed_qty == 1.0
        assert entry.eligible_long_qty == pytest.approx(1.5)
        assert entry.attributed_qty == pytest.approx(1.0)

    def test_large_cash_balance_labels_only_the_position_size(self) -> None:
        """A 10,020-EUR balance with a 20-EUR position attributes 20, cash is the rest."""
        result = attribute_position_inventory(
            {("kraken", "EUR"): 10_020.0},
            [PositionInventoryEntry("kraken", "EUR", 20.0, is_spot_margin=False)],
        )
        entry = result.attributions[0]
        assert entry.attributed_qty == pytest.approx(20.0)
        assert entry.observed_qty - entry.attributed_qty == pytest.approx(10_000.0)

    def test_caps_are_isolated_per_exchange(self) -> None:
        """A position on one exchange never caps the balance on another."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): 1.0, ("binance", "BTC"): 1.0},
            [
                PositionInventoryEntry("kraken", "BTC", 0.4, is_spot_margin=False),
                PositionInventoryEntry("binance", "BTC", 0.9, is_spot_margin=False),
            ],
        )
        attributed = {(a.exchange, a.currency): a.attributed_qty for a in result.attributions}
        assert attributed[("kraken", "BTC")] == pytest.approx(0.4)
        assert attributed[("binance", "BTC")] == pytest.approx(0.9)

    def test_short_position_is_excluded_and_flagged(self) -> None:
        """A short spot position never enters the allocation and raises the flag."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): 1.0},
            [PositionInventoryEntry("kraken", "BTC", -0.5, is_spot_margin=False)],
        )
        assert result.leveraged_inventory_excluded
        assert result.attributions[0].eligible_long_qty == 0.0
        assert result.attributions[0].attributed_qty == 0.0

    def test_spot_margin_position_is_excluded_and_flagged(self) -> None:
        """A leveraged spot-margin position is excluded from the positive cap."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): 1.0},
            [PositionInventoryEntry("kraken", "BTC", 0.6, is_spot_margin=True)],
        )
        assert result.leveraged_inventory_excluded
        assert result.attributions[0].eligible_long_qty == 0.0
        assert result.attributions[0].attributed_qty == 0.0

    def test_zero_quantity_position_is_not_position_bearing(self) -> None:
        """A zero-quantity position neither attributes nor raises the flag."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): 1.0},
            [PositionInventoryEntry("kraken", "BTC", 0.0, is_spot_margin=False)],
        )
        assert not result.leveraged_inventory_excluded
        assert result.attributions[0].eligible_long_qty == 0.0
        assert result.attributions[0].attributed_qty == 0.0

    def test_balance_without_positions_attributes_nothing(self) -> None:
        """A pure-cash balance with no positions attributes zero to positions."""
        result = attribute_position_inventory({("kraken", "USDT"): 500.0}, [])
        assert result.attributions[0].eligible_long_qty == 0.0
        assert result.attributions[0].attributed_qty == 0.0

    def test_negative_balance_clamps_the_attribution_to_zero(self) -> None:
        """A negative observed balance clamps the position label to zero."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): -2.0},
            [PositionInventoryEntry("kraken", "BTC", 1.0, is_spot_margin=False)],
        )
        assert result.attributions[0].attributed_qty == 0.0

    def test_non_finite_balance_attributes_nothing(self) -> None:
        """A non-finite observed balance cannot be attributed and labels zero."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): float("inf")},
            [PositionInventoryEntry("kraken", "BTC", 1.0, is_spot_margin=False)],
        )
        assert result.attributions[0].attributed_qty == 0.0
        assert not result.non_finite_position_excluded

    def test_infinite_position_drops_the_leg_and_flags(self) -> None:
        """An infinite position quantity is corrupt: the whole leg is dropped and flagged."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): 1.0, ("kraken", "ETH"): 2.0},
            [
                PositionInventoryEntry("kraken", "BTC", float("inf"), is_spot_margin=False),
                PositionInventoryEntry("kraken", "ETH", 1.0, is_spot_margin=False),
            ],
        )
        assert result.non_finite_position_excluded
        assert not result.leveraged_inventory_excluded
        keys = {(a.exchange, a.currency) for a in result.attributions}
        assert ("kraken", "BTC") not in keys
        assert ("kraken", "ETH") in keys

    def test_nan_position_drops_the_leg_and_flags(self) -> None:
        """A NaN position quantity never enters the eligible sum and drops its leg."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): 1.0},
            [PositionInventoryEntry("kraken", "BTC", float("nan"), is_spot_margin=False)],
        )
        assert result.non_finite_position_excluded
        assert result.attributions == ()

    def test_corrupt_position_without_a_balance_still_flags(self) -> None:
        """A corrupt position with no observed balance raises the flag with no leg."""
        result = attribute_position_inventory(
            {},
            [PositionInventoryEntry("kraken", "BTC", float("inf"), is_spot_margin=False)],
        )
        assert result.non_finite_position_excluded
        assert result.attributions == ()

    def test_corrupt_spot_margin_position_sets_both_flags(self) -> None:
        """A non-finite spot-margin position is both corrupt AND leveraged inventory."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): 1.0},
            [PositionInventoryEntry("kraken", "BTC", float("inf"), is_spot_margin=True)],
        )
        assert result.non_finite_position_excluded
        assert result.leveraged_inventory_excluded
        assert result.attributions == ()

    def test_overflowing_eligible_sum_poisons_the_key(self) -> None:
        """A per-key eligible sum that overflows is dropped like non-finite input."""
        result = attribute_position_inventory(
            {("kraken", "BTC"): 5.0},
            [
                PositionInventoryEntry("kraken", "BTC", 1e308, is_spot_margin=False),
                PositionInventoryEntry("kraken", "BTC", 1e308, is_spot_margin=False),
            ],
        )
        assert result.non_finite_position_excluded
        assert not result.leveraged_inventory_excluded
        assert result.attributions == ()

    def test_attributions_are_deterministically_ordered(self) -> None:
        """Attributions are emitted in ascending (exchange, currency) order."""
        result = attribute_position_inventory(
            {
                ("kraken", "ETH"): 1.0,
                ("binance", "BTC"): 1.0,
                ("kraken", "BTC"): 1.0,
            },
            [],
        )
        keys = [(a.exchange, a.currency) for a in result.attributions]
        assert keys == [("binance", "BTC"), ("kraken", "BTC"), ("kraken", "ETH")]
