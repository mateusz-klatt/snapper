"""Tests for the pure venue-minimum realizability proof.

The resolver pins every P3-P7 refusal independently, including temporal
selection, identity collisions, quote-side utility, certification gaps and
minimum instability. The partition tests pin the strict P1/P2/P8/P9 boundaries
and both whole-basket Rule-W safeguards.
"""

import math
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta

from snapper.application.portfolio.basket_realizability import MinimumRefusal
from snapper.application.portfolio.basket_realizability import MinimumResolution
from snapper.application.portfolio.basket_realizability import VenueOrderMinimumVersion
from snapper.application.portfolio.basket_realizability import partition_realizable_balances
from snapper.application.portfolio.basket_realizability import resolve_venue_order_minimums

_MINUTE = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_START = _MINUTE - timedelta(hours=1)
_END = _MINUTE + timedelta(hours=1)
_OBSERVED_AT = _START - timedelta(minutes=1)

_BASE = VenueOrderMinimumVersion(
    exchange="kraken",
    currency="TRUMP",
    role="base",
    asset_type="crypto",
    instrument_public_id="ins-trump-usd",
    symbol_public_id="sym-trump-usd",
    native_symbol="TRUMP-USD",
    counter_currency="USD",
    instrument_kind="spot",
    quantity_unit="base_asset",
    status="active",
    spec_public_id="spec-trump-usd",
    spec_source="kraken:ccxt.load_markets",
    spec_version="ccxt-1",
    spec_observed_at=_OBSERVED_AT,
    min_order_size=3.0,
    can_trade=True,
    identity_conflicted=False,
    minimum_unstable=False,
    valid_from=_START,
    valid_to=_END,
)


def _pair(name: str, minimum: float | None = 3.0) -> VenueOrderMinimumVersion:
    """Build one distinguishable qualifying base pair."""
    slug = name.lower()
    return replace(
        _BASE,
        instrument_public_id=f"ins-{slug}",
        symbol_public_id=f"sym-{slug}",
        native_symbol=name,
        spec_public_id=f"spec-{slug}",
        min_order_size=minimum,
    )


def _resolution(
    threshold: float | None = 3.0,
    refusal: MinimumRefusal | None = None,
) -> MinimumResolution:
    """Build one partition input with complete argmin evidence."""
    return MinimumResolution(
        threshold=threshold,
        refusal=refusal,
        pairs_considered=1,
        argmin_instrument_public_id="ins-trump-usd",
        argmin_symbol_public_id="sym-trump-usd",
        argmin_native_symbol="TRUMP-USD",
        argmin_spec_public_id="spec-trump-usd",
        argmin_spec_source="kraken:ccxt.load_markets",
        argmin_spec_version="ccxt-1",
        argmin_spec_observed_at=_OBSERVED_AT,
    )


class TestResolveVenueOrderMinimums:
    """Pin P3-P7 and deterministic threshold selection."""

    def test_single_certified_pair_resolves_its_minimum(self) -> None:
        """One fully proven pair supplies its threshold and argmin identity."""
        resolution = resolve_venue_order_minimums([_BASE], _MINUTE)[("kraken", "TRUMP")]
        assert resolution.threshold == 3.0
        assert resolution.refusal is None
        assert resolution.pairs_considered == 1
        assert resolution.argmin_instrument_public_id == "ins-trump-usd"
        assert resolution.argmin_symbol_public_id == "sym-trump-usd"
        assert resolution.argmin_native_symbol == "TRUMP-USD"
        assert resolution.argmin_spec_public_id == "spec-trump-usd"
        assert resolution.argmin_spec_source == "kraken:ccxt.load_markets"
        assert resolution.argmin_spec_version == "ccxt-1"
        assert resolution.argmin_spec_observed_at == _OBSERVED_AT

    def test_minimum_is_the_min_over_every_qualifying_pair(self) -> None:
        """Synthetic divergent minima select the smallest across all pairs."""
        versions = [
            _pair("TRUMP-USD", 3.0),
            _pair("TRUMP-EUR", 1.0),
            _pair("TRUMP-USDC", 5.0),
        ]
        resolution = resolve_venue_order_minimums(versions, _MINUTE)[("kraken", "TRUMP")]
        assert resolution.threshold == 1.0
        assert resolution.pairs_considered == 3
        assert resolution.argmin_native_symbol == "TRUMP-EUR"

    def test_argmin_ties_break_on_native_symbol(self) -> None:
        """Equal minima choose the lexicographically first native symbol."""
        versions = [_pair("TRUMP-USD", 1.0), _pair("TRUMP-EUR", 1.0)]
        resolution = resolve_venue_order_minimums(versions, _MINUTE)[("kraken", "TRUMP")]
        assert resolution.argmin_native_symbol == "TRUMP-EUR"

    def test_argmin_ties_break_on_spec_identity_independent_of_input_order(self) -> None:
        """Identical overlapping evidence elects one deterministic proof row."""
        later = replace(_BASE, spec_public_id="spec-z")
        earlier = replace(_BASE, spec_public_id="spec-a")
        forward = resolve_venue_order_minimums([later, earlier], _MINUTE)[("kraken", "TRUMP")]
        reverse = resolve_venue_order_minimums([earlier, later], _MINUTE)[("kraken", "TRUMP")]
        assert forward.argmin_spec_public_id == "spec-a"
        assert reverse == forward

    def test_null_minimum_refuses_no_certified_pair(self) -> None:
        """Certified provenance with a null minimum remains UNKNOWN."""
        resolution = resolve_venue_order_minimums(
            [replace(_BASE, min_order_size=None)],
            _MINUTE,
        )[("kraken", "TRUMP")]
        assert resolution.threshold is None
        assert resolution.refusal == "no_certified_pair"
        assert resolution.pairs_considered == 0
        assert resolution.argmin_instrument_public_id is None

    def test_uncertified_spec_refuses(self) -> None:
        """A null provenance source cannot certify a venue minimum."""
        resolution = resolve_venue_order_minimums(
            [replace(_BASE, spec_source=None, spec_version=None, spec_observed_at=None)],
            _MINUTE,
        )[("kraken", "TRUMP")]
        assert resolution.refusal == "no_certified_pair"

    def test_partial_provenance_refuses(self) -> None:
        """Certification requires the complete source/version/observation tuple."""
        for version in (
            replace(_BASE, spec_version=None),
            replace(_BASE, spec_observed_at=None),
        ):
            resolution = resolve_venue_order_minimums([version], _MINUTE)[("kraken", "TRUMP")]
            assert resolution.refusal == "no_certified_pair"

    def test_inactive_status_refuses(self) -> None:
        """An inactive pair does not prove current spot-order utility."""
        resolution = resolve_venue_order_minimums(
            [replace(_BASE, status="inactive")],
            _MINUTE,
        )[("kraken", "TRUMP")]
        assert resolution.refusal == "no_certified_pair"

    def test_null_status_refuses(self) -> None:
        """A null lifecycle state is UNKNOWN rather than active."""
        resolution = resolve_venue_order_minimums(
            [replace(_BASE, status=None)],
            _MINUTE,
        )[("kraken", "TRUMP")]
        assert resolution.refusal == "no_certified_pair"

    def test_contract_count_unit_refuses(self) -> None:
        """A contract-count minimum cannot be applied to a base-asset balance."""
        resolution = resolve_venue_order_minimums(
            [replace(_BASE, quantity_unit="contract_count")],
            _MINUTE,
        )[("kraken", "TRUMP")]
        assert resolution.refusal == "no_certified_pair"

    def test_non_positive_or_non_finite_minimum_refuses(self) -> None:
        """Zero, negative, infinite and NaN minima all remain UNKNOWN."""
        for minimum in (0.0, -1.0, float("inf"), float("nan")):
            resolution = resolve_venue_order_minimums(
                [replace(_BASE, min_order_size=minimum)],
                _MINUTE,
            )[("kraken", "TRUMP")]
            assert resolution.refusal == "no_certified_pair"

    def test_can_trade_false_pair_is_not_counted(self) -> None:
        """A non-tradable pair does not enter predicate set P."""
        resolution = resolve_venue_order_minimums(
            [replace(_BASE, can_trade=False)],
            _MINUTE,
        )[("kraken", "TRUMP")]
        assert resolution.refusal == "no_certified_pair"
        assert resolution.pairs_considered == 0

    def test_mixed_asset_type_on_venue_refuses_identity_ambiguous(self) -> None:
        """Crypto and equity base identities sharing a venue/currency fail P3."""
        equity = replace(
            _pair("PEP"),
            currency="PEP",
            asset_type="equity",
            instrument_public_id="ins-pep-equity",
            symbol_public_id="sym-pep-equity",
        )
        crypto = replace(_pair("PEP-EUR"), currency="PEP")
        resolution = resolve_venue_order_minimums([crypto, equity], _MINUTE)[("kraken", "PEP")]
        assert resolution.refusal == "identity_ambiguous"

    def test_repository_identity_conflict_flag_refuses(self) -> None:
        """A historical cross-version identity conflict also fails P3."""
        resolution = resolve_venue_order_minimums(
            [replace(_BASE, identity_conflicted=True)],
            _MINUTE,
        )[("kraken", "TRUMP")]
        assert resolution.refusal == "identity_ambiguous"

    def test_non_crypto_asset_type_refuses(self) -> None:
        """A unanimous forex identity is never tested against crypto minima."""
        resolution = resolve_venue_order_minimums(
            [replace(_BASE, currency="EUR", asset_type="forex")],
            _MINUTE,
        )[("kraken", "EUR")]
        assert resolution.refusal == "not_crypto"

    def test_quote_currency_refuses(self) -> None:
        """An active certified tradable quote-side pair applies P4."""
        base = replace(_BASE, currency="SOL")
        quote = replace(
            _pair("JITOSOL-SOL", 0.05),
            currency="SOL",
            role="quote",
            counter_currency="JITOSOL",
        )
        resolution = resolve_venue_order_minimums([base, quote], _MINUTE)[("kraken", "SOL")]
        assert resolution.refusal == "is_quote_currency"

    def test_unknown_quote_utility_keeps_the_balance(self) -> None:
        """Missing capability or certification cannot disprove quote-side utility."""
        base = replace(_BASE, currency="SOL")
        for quote in (
            replace(
                _pair("JITOSOL-SOL", 0.05),
                currency="SOL",
                role="quote",
                counter_currency="JITOSOL",
                can_trade=None,
            ),
            replace(
                _pair("JITOSOL-SOL", 0.05),
                currency="SOL",
                role="quote",
                counter_currency="JITOSOL",
                spec_source=None,
                spec_version=None,
                spec_observed_at=None,
            ),
        ):
            resolution = resolve_venue_order_minimums([base, quote], _MINUTE)[("kraken", "SOL")]
            assert resolution.refusal == "is_quote_currency"

    def test_conflicted_quote_identity_keeps_the_balance(self) -> None:
        """Identity ambiguity cannot prove a quote-side pair unusable."""
        base = replace(_BASE, currency="SOL")
        quote = replace(
            _pair("JITOSOL-SOL", 0.05),
            currency="SOL",
            role="quote",
            counter_currency="JITOSOL",
            can_trade=False,
            identity_conflicted=True,
        )
        resolution = resolve_venue_order_minimums([base, quote], _MINUTE)[("kraken", "SOL")]
        assert resolution.refusal == "is_quote_currency"

    def test_unqualified_quote_pair_does_not_refuse(self) -> None:
        """P4 ignores a quote pair without active certified trade utility."""
        quote = replace(
            _pair("JITOSOL-TRUMP", 0.05),
            role="quote",
            counter_currency="JITOSOL",
            status="inactive",
        )
        resolution = resolve_venue_order_minimums([_BASE, quote], _MINUTE)[("kraken", "TRUMP")]
        assert resolution.threshold == 3.0

    def test_tradable_uncertified_sibling_refuses(self) -> None:
        """One tradable sibling outside P prevents subset-based exclusion."""
        sibling = replace(
            _pair("TRUMP-EUR", None),
            spec_source=None,
            spec_version=None,
            spec_observed_at=None,
        )
        resolution = resolve_venue_order_minimums([_BASE, sibling], _MINUTE)[("kraken", "TRUMP")]
        assert resolution.refusal == "uncertified_tradable_pair"
        assert resolution.pairs_considered == 1

    def test_unknown_capability_sibling_refuses(self) -> None:
        """Missing capability cannot prove that a sibling is non-tradable."""
        sibling = replace(_pair("TRUMP-EUR"), can_trade=None)
        resolution = resolve_venue_order_minimums([_BASE, sibling], _MINUTE)[("kraken", "TRUMP")]
        assert resolution.refusal == "uncertified_tradable_pair"
        assert resolution.pairs_considered == 1

    def test_non_tradable_uncertified_sibling_is_ignored(self) -> None:
        """P6 concerns only tradable siblings."""
        sibling = replace(
            _pair("TRUMP-EUR", None),
            can_trade=False,
            spec_source=None,
            spec_version=None,
            spec_observed_at=None,
        )
        resolution = resolve_venue_order_minimums([_BASE, sibling], _MINUTE)[("kraken", "TRUMP")]
        assert resolution.threshold == 3.0
        assert resolution.pairs_considered == 1

    def test_overlapping_unqualified_version_of_same_instrument_refuses(self) -> None:
        """A qualifying twin cannot mask one tradable in-force bad version."""
        overlapping = replace(
            _BASE,
            spec_source=None,
            spec_version=None,
            spec_observed_at=None,
        )
        resolution = resolve_venue_order_minimums(
            [_BASE, overlapping],
            _MINUTE,
        )[("kraken", "TRUMP")]
        assert resolution.refusal == "uncertified_tradable_pair"
        assert resolution.pairs_considered == 1

    def test_changed_minimum_refuses_minimum_unstable(self) -> None:
        """A repository-proven non-null minimum change fails P7."""
        resolution = resolve_venue_order_minimums(
            [replace(_BASE, minimum_unstable=True)],
            _MINUTE,
        )[("kraken", "TRUMP")]
        assert resolution.refusal == "minimum_unstable"
        assert resolution.pairs_considered == 1

    def test_null_to_value_transition_is_stable(self) -> None:
        """A provenance backfill from null to value does not trip P7."""
        old = replace(_BASE, min_order_size=None, valid_to=_MINUTE)
        current = replace(_BASE, valid_from=_MINUTE)
        resolution = resolve_venue_order_minimums([old, current], _MINUTE)[("kraken", "TRUMP")]
        assert resolution.threshold == 3.0
        assert resolution.refusal is None

    def test_minimum_in_force_at_the_minute_is_used(self) -> None:
        """Half-open version bounds select the old and new minima at the seam."""
        seam = _MINUTE
        old = replace(_BASE, min_order_size=2.0, valid_to=seam)
        new = replace(_BASE, min_order_size=3.0, valid_from=seam)
        before = resolve_venue_order_minimums(
            [old, new],
            seam - timedelta(microseconds=1),
        )[("kraken", "TRUMP")]
        at_seam = resolve_venue_order_minimums([old, new], seam)[("kraken", "TRUMP")]
        assert before.threshold == 2.0
        assert at_seam.threshold == 3.0

    def test_out_of_force_version_is_ignored(self) -> None:
        """Rows outside the minute's half-open interval do not affect proof."""
        future = replace(
            _pair("TRUMP-EUR", 1.0),
            valid_from=_END,
            valid_to=_END + timedelta(hours=1),
        )
        resolution = resolve_venue_order_minimums([_BASE, future], _MINUTE)[("kraken", "TRUMP")]
        assert resolution.threshold == 3.0
        assert resolution.pairs_considered == 1

    def test_no_instrument_returns_no_resolution(self) -> None:
        """A quote-only row cannot fabricate a base-asset threshold."""
        quote = replace(_BASE, role="quote")
        assert resolve_venue_order_minimums([quote], _MINUTE) == {}
        assert resolve_venue_order_minimums([], _MINUTE) == {}


class TestPartitionRealizableBalances:
    """Pin strict per-leg boundaries and both Rule-W guards."""

    def test_balance_below_minimum_is_excluded(self) -> None:
        """A positive non-USD leg strictly below its threshold contributes nothing."""
        result = partition_realizable_balances(
            {("kraken", "TRUMP"): 8e-6, ("kraken", "USD"): 1.0},
            {("kraken", "TRUMP"): _resolution()},
        )
        assert result.balances == {("kraken", "USD"): 1.0}
        assert len(result.excluded) == 1
        excluded = result.excluded[0]
        assert excluded.exchange == "kraken"
        assert excluded.currency == "TRUMP"
        assert excluded.quantity == 8e-6
        assert excluded.min_order_size == 3.0
        assert excluded.pairs_considered == 1
        assert excluded.instrument_public_id == "ins-trump-usd"
        assert excluded.symbol_public_id == "sym-trump-usd"
        assert excluded.native_symbol == "TRUMP-USD"
        assert excluded.spec_public_id == "spec-trump-usd"
        assert excluded.spec_source == "kraken:ccxt.load_markets"
        assert excluded.spec_version == "ccxt-1"
        assert excluded.spec_observed_at == _OBSERVED_AT
        assert result.resolved_legs == 1
        assert result.unresolved_legs == 1
        assert result.rule_withheld is False
        assert result.withheld_cause is None

    def test_balance_exactly_at_minimum_is_kept(self) -> None:
        """Equality is realizable because P8 is strictly less-than."""
        result = partition_realizable_balances(
            {("kraken", "TRUMP"): 3.0},
            {("kraken", "TRUMP"): _resolution()},
        )
        assert result.balances == {("kraken", "TRUMP"): 3.0}
        assert result.excluded == ()
        assert result.resolved_legs == 1

    def test_balance_one_ulp_below_minimum_is_excluded(self) -> None:
        """The closest representable value below the threshold still excludes."""
        quantity = math.nextafter(3.0, 0.0)
        result = partition_realizable_balances(
            {("kraken", "TRUMP"): quantity, ("kraken", "USD"): 1.0},
            {("kraken", "TRUMP"): _resolution()},
        )
        assert result.excluded[0].quantity == quantity

    def test_negative_balance_is_never_excluded(self) -> None:
        """A liability stays in equity even when its magnitude is sub-minimum."""
        result = partition_realizable_balances(
            {("kraken", "TRUMP"): -1e-9},
            {("kraken", "TRUMP"): _resolution()},
        )
        assert result.balances == {("kraken", "TRUMP"): -1e-9}
        assert result.excluded == ()

    def test_nan_balance_is_never_excluded(self) -> None:
        """NaN falls through to the existing non-finite valuation path."""
        result = partition_realizable_balances(
            {("kraken", "TRUMP"): float("nan")},
            {("kraken", "TRUMP"): _resolution()},
        )
        assert math.isnan(result.balances[("kraken", "TRUMP")])
        assert result.excluded == ()

    def test_zero_balance_is_not_excluded_or_counted(self) -> None:
        """Exact zero remains the valuation module's currency-invariant branch."""
        result = partition_realizable_balances(
            {("kraken", "TRUMP"): 0.0},
            {("kraken", "TRUMP"): _resolution()},
        )
        assert result.balances == {("kraken", "TRUMP"): 0.0}
        assert result.excluded == ()
        assert result.resolved_legs == 0
        assert result.unresolved_legs == 0

    def test_usd_is_never_excluded(self) -> None:
        """The valuation currency is not itself an order."""
        result = partition_realizable_balances(
            {("kraken", "USD"): 0.01},
            {("kraken", "USD"): _resolution()},
        )
        assert result.balances == {("kraken", "USD"): 0.01}
        assert result.excluded == ()

    def test_unresolved_minimum_keeps_the_balance(self) -> None:
        """Absent, refused and invalid thresholds all count as unresolved."""
        balances = {
            ("kraken", "AAA"): 0.1,
            ("kraken", "BBB"): 0.2,
            ("kraken", "CCC"): 0.3,
        }
        resolutions = {
            ("kraken", "BBB"): _resolution(None),
            ("kraken", "CCC"): _resolution(3.0, "no_certified_pair"),
        }
        result = partition_realizable_balances(balances, resolutions)
        assert result.balances == balances
        assert result.resolved_legs == 0
        assert result.unresolved_legs == 3

    def test_five_excluded_legs_withholds_the_rule(self) -> None:
        """W1 restores every leg when more than four would be excluded."""
        balances = {("kraken", f"CURRENCY{index}"): 0.1 for index in range(5)}
        resolutions = {key: _resolution() for key in balances}
        result = partition_realizable_balances(balances, resolutions)
        assert result.balances == balances
        assert result.excluded == ()
        assert result.rule_withheld is True
        assert result.withheld_cause == "too_many_excluded_legs"
        assert result.resolved_legs == 5

    def test_four_excluded_legs_is_allowed(self) -> None:
        """The W1 boundary permits exactly four exclusions."""
        balances = {("kraken", f"CURRENCY{index}"): 0.1 for index in range(4)}
        balances[("kraken", "USD")] = 1.0
        resolutions = {key: _resolution() for key in balances if key[1] != "USD"}
        result = partition_realizable_balances(balances, resolutions)
        assert result.balances == {("kraken", "USD"): 1.0}
        assert len(result.excluded) == 4
        assert result.rule_withheld is False

    def test_excluding_every_leg_withholds_the_rule(self) -> None:
        """W2 refuses to publish zero for a non-empty observed wallet."""
        balances = {("kraken", "TRUMP"): 0.1}
        result = partition_realizable_balances(
            balances,
            {("kraken", "TRUMP"): _resolution()},
        )
        assert result.balances == balances
        assert result.excluded == ()
        assert result.rule_withheld is True
        assert result.withheld_cause == "would_empty_basket"

    def test_already_empty_basket_does_not_withhold(self) -> None:
        """W2 does not fire for a genuinely empty observed basket."""
        result = partition_realizable_balances({}, {})
        assert result.balances == {}
        assert result.excluded == ()
        assert result.rule_withheld is False
        assert result.withheld_cause is None

    def test_excluded_records_are_sorted_and_distinct(self) -> None:
        """Output ordering is canonical regardless of input mapping order."""
        balances = {
            ("kraken", "ZED"): 0.1,
            ("binance", "AAA"): 0.1,
            ("kraken", "AAA"): 0.1,
            ("kraken", "USD"): 1.0,
        }
        resolutions = {key: _resolution() for key in balances if key != ("kraken", "USD")}
        result = partition_realizable_balances(balances, resolutions)
        keys = [(excluded.exchange, excluded.currency) for excluded in result.excluded]
        assert keys == [
            ("binance", "AAA"),
            ("kraken", "AAA"),
            ("kraken", "ZED"),
        ]
        assert len(keys) == len(set(keys))

    def test_retained_balances_are_sorted(self) -> None:
        """The retained mapping has deterministic key iteration order."""
        balances = {
            ("kraken", "ZED"): 4.0,
            ("binance", "AAA"): 4.0,
        }
        result = partition_realizable_balances(
            balances,
            {key: _resolution() for key in balances},
        )
        assert list(result.balances) == [
            ("binance", "AAA"),
            ("kraken", "ZED"),
        ]
