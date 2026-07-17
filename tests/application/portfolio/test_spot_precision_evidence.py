"""Tests for pure per-asset spot precision evidence derivation."""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import patch

import pytest

import snapper.application.portfolio.spot_precision_evidence as spot_precision_evidence
import snapper.application.portfolio.walutomat_precision_artifact as walutomat_artifact
from snapper.application.portfolio.spot_precision_evidence import SpotInstrumentPrecisionObservation
from snapper.application.portfolio.spot_precision_evidence import (
    derive_walutomat_precision_from_instruments,
)
from snapper.application.portfolio.spot_precision_evidence import (
    derive_walutomat_precision_from_raw_balances,
)
from snapper.application.portfolio.spot_precision_evidence import is_spot_precision_evidence_fresh
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry

_NOW = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
_ARTIFACT = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
_SPEC_SOURCE = walutomat_artifact.WALUTOMAT_DOCUMENTARY_SPEC_SOURCE
_SPEC_VERSION = walutomat_artifact.walutomat_precision_artifact_version(_ARTIFACT)


def _balance(
    *,
    currency: str = "EUR",
    total: float = 100.0,
    free: float | None = 60.0,
    used: float | None = 40.0,
    total_decimal: str | None = "100.00",
    free_decimal: str | None = "60.00",
    used_decimal: str | None = "40.00",
    provenance: str = "venue_raw",
) -> NativeBalanceEntry:
    """Build one raw Walutomat balance observation."""
    return NativeBalanceEntry(
        currency=currency,
        total=total,
        free=free,
        used=used,
        total_decimal=total_decimal,
        free_decimal=free_decimal,
        used_decimal=used_decimal,
        numeric_provenance=provenance,
    )


def _instrument(
    *,
    base: str = "EUR",
    quote: str = "PLN",
    qty_decimals: int | None = 2,
    cost_decimals: int | None = 2,
    source: str | None = _SPEC_SOURCE,
    version: str | None = _SPEC_VERSION,
    observed_at: datetime | None = _ARTIFACT.reviewed_at,
) -> SpotInstrumentPrecisionObservation:
    """Build one Walutomat instrument precision observation."""
    return SpotInstrumentPrecisionObservation(
        base_asset=base,
        quote_asset=quote,
        qty_decimals=qty_decimals,
        cost_decimals=cost_decimals,
        source=source,
        version=version,
        observed_at=observed_at,
    )


def test_raw_balance_derivation_certifies_complete_matching_exponents() -> None:
    """Complete raw strings with one exponent produce stable balance evidence.

    Given: Duplicate canonical balance rows with complete coherent two-place raw triplets.
    When: Walutomat raw balance precision is derived twice.
    Then: One deterministic two-place balance candidate is returned without fee evidence.
    """
    entries = [
        _balance(currency="EUR"),
        _balance(
            total=20.0,
            free=10.0,
            used=10.0,
            total_decimal="20.00",
            free_decimal="10.00",
            used_decimal="10.00",
        ),
    ]

    first = derive_walutomat_precision_from_raw_balances(entries, _NOW)
    second = derive_walutomat_precision_from_raw_balances(entries, _NOW)

    assert len(first) == 1
    assert first == second
    assert first[0].exchange == "walutomat"
    assert first[0].asset == "EUR"
    assert first[0].balance_decimals == 2
    assert first[0].fee_decimals is None
    assert first[0].source == "walutomat:api-v2.0.0:account/balances:venue_raw-decimal-triplet"
    assert first[0].version.startswith("spot-asset-precision-v1:")
    assert first[0].observed_at == _NOW


def test_raw_balance_derivation_applies_floor_to_integer_only_stream() -> None:
    """Documented currency scale prevents integer text from widening tolerance.

    Given: A coherent raw balance triplet rendered as ``100``, ``60``, and ``40``.
    When: Walutomat balance precision is derived with the current artifact.
    Then: The documentary two-place floor certifies a 0.01 quantum, never 1.
    """
    result = derive_walutomat_precision_from_raw_balances(
        [
            _balance(
                total_decimal="100",
                free_decimal="60",
                used_decimal="40",
            )
        ],
        _NOW,
    )

    assert result[0].balance_decimals == 2


@pytest.mark.parametrize(
    "replacement",
    [
        replace(_ARTIFACT, reviewed_at=_ARTIFACT.reviewed_at.replace(tzinfo=None)),
        replace(_ARTIFACT, reviewed_at=_NOW + timedelta(microseconds=1)),
        replace(_ARTIFACT, effective_at=_NOW.date() + timedelta(days=1)),
    ],
)
def test_raw_balance_floor_rejects_an_invalid_temporal_artifact(
    replacement: walutomat_artifact.WalutomatPrecisionArtifact,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A naive, not-yet-reviewed, or future-effective artifact supplies no floor.

    Given: Coherent raw balance text and an artifact with invalid temporal provenance,
    When: The producer attempts to apply the documentary minimum,
    Then: The balance plane is emitted as an explicit noncertifying revocation.
    """
    monkeypatch.setattr(
        walutomat_artifact,
        "WALUTOMAT_PRECISION_ARTIFACT",
        replacement,
    )

    result = derive_walutomat_precision_from_raw_balances([_balance()], _NOW)

    assert result[0].balance_decimals is None


def test_integer_only_stream_cannot_certify_without_an_artifact_floor() -> None:
    """Displayed integer exponents alone are not evidence of venue scale.

    Given: A coherent all-integer stream and an artifact without a currency floor.
    When: Balance precision is derived.
    Then: The displayed zero exponent cannot certify a whole-unit quantum.
    """
    without_floor = replace(
        _ARTIFACT,
        currency_amount_minimum_decimals=None,
    )
    with patch.object(
        walutomat_artifact,
        "WALUTOMAT_PRECISION_ARTIFACT",
        without_floor,
    ):
        result = derive_walutomat_precision_from_raw_balances(
            [
                _balance(
                    total_decimal="100",
                    free_decimal="60",
                    used_decimal="40",
                )
            ],
            _NOW,
        )

    assert result[0].balance_decimals is None


def test_raw_balance_derivation_rejects_exponent_notation() -> None:
    """Exponent notation is outside the venue's plain-decimal grammar.

    Given: A complete raw balance triplet rendered with positive decimal exponents.
    When: Walutomat raw balance precision is derived.
    Then: Balance decimals remain nullable without passing the text to Decimal.
    """
    result = derive_walutomat_precision_from_raw_balances(
        [
            _balance(
                total=100.0,
                free=100.0,
                used=100.0,
                total_decimal="1E+2",
                free_decimal="1E+2",
                used_decimal="1E+2",
            )
        ],
        _NOW,
    )

    assert result[0].balance_decimals is None


@pytest.mark.parametrize(
    "entry",
    [
        _balance(used_decimal=None),
        _balance(used=None),
    ],
)
def test_raw_balance_derivation_leaves_missing_raw_evidence_nullable(
    entry: NativeBalanceEntry,
) -> None:
    """Any missing raw amount or companion prevents balance certification.

    Given: A balance row missing one raw amount or its faithful float companion.
    When: Walutomat raw balance precision is derived.
    Then: Both balance and fee decimal evidence remain nullable.
    """
    result = derive_walutomat_precision_from_raw_balances([entry], _NOW)

    assert result[0].balance_decimals is None
    assert result[0].fee_decimals is None


@pytest.mark.parametrize(
    "entry",
    [
        _balance(provenance="legacy_float"),
        _balance(total=101.0),
        _balance(total=cast(float, True)),
        _balance(total=float("inf")),
        _balance(total_decimal=""),
        _balance(total_decimal=" 100.00"),
        _balance(total_decimal="invalid"),
        _balance(total_decimal="NaN"),
        _balance(total_decimal="1e-257"),
        _balance(total_decimal="١٠٠.٠٠"),
        _balance(total_decimal="+100.00"),
        _balance(total_decimal="-100.00"),
        _balance(total_decimal="1e2"),
        _balance(total_decimal=f"0.{'0' * 257}1", total=0.0),
    ],
)
def test_raw_balance_derivation_rejects_conflicting_or_malformed_evidence(
    entry: NativeBalanceEntry,
) -> None:
    """Conflicting provenance, companions, and malformed numbers remain nullable.

    Given: A balance row with malformed, nonfinite, mismatched, or non-raw evidence.
    When: Walutomat raw balance precision is derived.
    Then: The row cannot certify balance decimal precision.
    """
    result = derive_walutomat_precision_from_raw_balances([entry], _NOW)

    assert result[0].balance_decimals is None


def test_raw_balance_derivation_preserves_an_observed_nondefault_scale() -> None:
    """Authenticated raw scale is recorded without a documentary override.

    Given: One coherent authenticated balance triplet rendered to three places.
    When: Walutomat balance precision is derived.
    Then: The observed three-place scale is retained exactly.
    """
    result = derive_walutomat_precision_from_raw_balances(
        [
            _balance(
                total_decimal="100.000",
                free_decimal="60.000",
                used_decimal="40.000",
            )
        ],
        _NOW,
    )

    assert result[0].balance_decimals == 3


def test_raw_balance_derivation_ratchets_duplicate_asset_scales() -> None:
    """Repeated coherent asset observations ratchet to the tightest scale.

    Given: Two complete rows for one asset with two- and three-place raw values.
    When: Walutomat raw balance precision is merged.
    Then: The asset certifies the maximum three-place scale.
    """
    result = derive_walutomat_precision_from_raw_balances(
        [
            _balance(),
            _balance(
                total_decimal="100.000",
                free_decimal="60.000",
                used_decimal="40.000",
            ),
        ],
        _NOW,
    )

    assert result[0].balance_decimals == 3


def test_raw_balance_derivation_rejects_oversized_input_before_parsing() -> None:
    """The raw-input cap runs before regular-expression and Decimal parsing.

    Given: A venue raw amount larger than the documented 64-character cap.
    When: Balance precision derivation evaluates the triplet.
    Then: The input is rejected without invoking Decimal.
    """
    with patch("snapper.application.portfolio.spot_precision_evidence.Decimal") as decimal_type:
        result = derive_walutomat_precision_from_raw_balances(
            [_balance(total_decimal="1" * 65)],
            _NOW,
        )

    assert result[0].balance_decimals is None
    decimal_type.assert_not_called()


def test_raw_balance_derivation_propagates_duplicate_missing_evidence() -> None:
    """One incomplete duplicate prevents a complete duplicate from certifying.

    Given: One complete and one incomplete raw balance row for the same asset.
    When: Walutomat raw balance precision is merged.
    Then: The complete row cannot hide the missing duplicate evidence.
    """
    result = derive_walutomat_precision_from_raw_balances(
        [_balance(), _balance(total_decimal=None)],
        _NOW,
    )

    assert result[0].balance_decimals is None


def test_raw_balance_derivation_handles_an_empty_response() -> None:
    """An empty venue response produces no fabricated asset evidence.

    Given: No authenticated Walutomat balance rows.
    When: Raw balance precision is derived.
    Then: No persistence candidates are returned.
    """
    assert derive_walutomat_precision_from_raw_balances([], _NOW) == []


@pytest.mark.parametrize("asset", [" eur ", "eur", "", "EU", "EÜR", "EU1"])
def test_raw_balance_derivation_rejects_noncanonical_asset_identity(asset: str) -> None:
    """Malformed or alias-like venue identities never forge a canonical asset key.

    Given: A balance row whose currency is not an exact uppercase ISO code.
    When: Walutomat raw balance precision is derived.
    Then: No rewritten or alias-collapsed persistence candidate is returned.
    """
    assert (
        derive_walutomat_precision_from_raw_balances(
            [_balance(currency=asset)],
            _NOW,
        )
        == []
    )


def test_instrument_derivation_produces_artifact_dated_fee_only_evidence() -> None:
    """The updater cannot promote order precision into balance precision.

    Given: Two pairs sharing an asset and referencing the current reviewed artifact.
    When: Walutomat updater precision evidence is derived.
    Then: Every asset receives fee-only evidence with exact artifact provenance and date.
    """
    observations = [
        _instrument(base="EUR", quote="PLN"),
        _instrument(base="USD", quote="EUR"),
    ]

    result = derive_walutomat_precision_from_instruments(observations, _NOW)

    assert [candidate.asset for candidate in result] == ["EUR", "PLN", "USD"]
    assert {candidate.exchange for candidate in result} == {"walutomat"}
    assert all(candidate.balance_decimals is None for candidate in result)
    assert all(candidate.fee_decimals == 2 for candidate in result)
    assert all(
        candidate.source == walutomat_artifact.WALUTOMAT_DOCUMENTARY_FEE_SOURCE
        for candidate in result
    )
    assert {candidate.version for candidate in result} == {_SPEC_VERSION}
    assert {candidate.observed_at for candidate in result} == {_ARTIFACT.reviewed_at}


def test_instrument_derivation_does_not_redate_evidence_across_runs() -> None:
    """Repeated updater runs retain the documentary review time.

    Given: The same pair is processed before, at, and after the live 12-hour boundary.
    When: Fee evidence is derived on each updater run.
    Then: Every run returns the same version and reviewed_at without re-stamping it.
    """
    run_times = (
        _ARTIFACT.reviewed_at,
        _ARTIFACT.reviewed_at + timedelta(hours=12),
        _ARTIFACT.reviewed_at + timedelta(days=30),
    )
    results = [
        derive_walutomat_precision_from_instruments([_instrument()], run_time)
        for run_time in run_times
    ]

    assert all(result[0].version == _SPEC_VERSION for result in results)
    assert all(result[0].observed_at == _ARTIFACT.reviewed_at for result in results)
    assert all(result[0].fee_decimals == 2 for result in results)


def test_instrument_derivation_keeps_fee_independent_of_order_precision() -> None:
    """Order-precision values cannot create or revoke documentary fee evidence.

    Given: Two artifact-bearing pairs with conflicting quantity and cost precision values.
    When: Walutomat instrument precision is merged.
    Then: Balance stays absent while independently sourced fee precision remains present.
    """
    result = derive_walutomat_precision_from_instruments(
        [
            _instrument(base="EUR", quote="PLN", qty_decimals=2),
            _instrument(base="USD", quote="EUR", cost_decimals=3),
        ],
        _NOW,
    )
    by_asset = {candidate.asset: candidate for candidate in result}

    assert all(candidate.balance_decimals is None for candidate in by_asset.values())
    assert all(candidate.fee_decimals == 2 for candidate in by_asset.values())


@pytest.mark.parametrize(
    ("source", "version", "observed_at", "evaluated_at"),
    [
        (None, _SPEC_VERSION, _ARTIFACT.reviewed_at, _NOW),
        ("other", _SPEC_VERSION, _ARTIFACT.reviewed_at, _NOW),
        (_SPEC_SOURCE, None, _ARTIFACT.reviewed_at, _NOW),
        (_SPEC_SOURCE, f"s4c-1-v1:{'0' * 64}", _ARTIFACT.reviewed_at, _NOW),
        (_SPEC_SOURCE, _SPEC_VERSION, None, _NOW),
        (_SPEC_SOURCE, _SPEC_VERSION, _ARTIFACT.reviewed_at + timedelta(seconds=1), _NOW),
        (_SPEC_SOURCE, _SPEC_VERSION, _ARTIFACT.reviewed_at, _NOW.replace(tzinfo=None)),
        (
            _SPEC_SOURCE,
            _SPEC_VERSION,
            _ARTIFACT.reviewed_at,
            _ARTIFACT.reviewed_at - timedelta(microseconds=1),
        ),
    ],
)
def test_instrument_derivation_rejects_noncurrent_artifact_provenance(
    source: str | None,
    version: str | None,
    observed_at: datetime | None,
    evaluated_at: datetime,
) -> None:
    """Only the exact current artifact can produce fee evidence.

    Given: A pair with missing, forged, mismatched, naive, or future artifact provenance.
    When: Walutomat per-asset precision is derived.
    Then: Fee precision remains nullable despite a superficially valid pair identity.
    """
    result = derive_walutomat_precision_from_instruments(
        [
            _instrument(
                source=source,
                version=version,
                observed_at=observed_at,
            )
        ],
        evaluated_at,
    )
    by_asset = {candidate.asset: candidate for candidate in result}

    assert by_asset["EUR"].balance_decimals is None
    assert by_asset["EUR"].fee_decimals is None


def test_instrument_derivation_handles_an_empty_catalog() -> None:
    """An empty instrument catalog produces no fabricated asset evidence.

    Given: No Walutomat instrument precision observations.
    When: Per-asset precision is derived.
    Then: No persistence candidates are returned.
    """
    assert derive_walutomat_precision_from_instruments([], _NOW) == []


@pytest.mark.parametrize(
    ("base", "quote"),
    [("eur", "PLN"), ("EUR", " pln "), ("EUR", "EUR")],
)
def test_instrument_derivation_rejects_noncanonical_pair_identity(
    base: str,
    quote: str,
) -> None:
    """Unresolved aliases and asset collisions produce no pair-derived evidence.

    Given: A pair with a noncanonical asset code or identical base and quote assets.
    When: Walutomat instrument precision is derived.
    Then: The invalid pair contributes no asset evidence.
    """
    assert (
        derive_walutomat_precision_from_instruments(
            [_instrument(base=base, quote=quote)],
            _NOW,
        )
        == []
    )


@pytest.mark.parametrize(
    ("observed_at", "evaluated_at", "expected"),
    [
        (_NOW - timedelta(hours=11, minutes=59), _NOW, True),
        (_NOW, _NOW, True),
        (_NOW - timedelta(hours=12), _NOW, False),
        (_NOW + timedelta(microseconds=1), _NOW, False),
        (None, _NOW, False),
        (_NOW.replace(tzinfo=None), _NOW, False),
        (_NOW, _NOW.replace(tzinfo=None), False),
    ],
)
def test_precision_freshness_matches_evaluator_semantics(
    observed_at: datetime | None,
    evaluated_at: datetime,
    expected: bool,
) -> None:
    """Freshness is timezone-aware, nonfuture, and strictly under twelve hours.

    Given: Evidence and evaluation timestamps spanning valid and invalid boundary cases.
    When: Spot precision evidence freshness is evaluated.
    Then: The result matches the shipped evaluator's strict twelve-hour semantics.
    """
    assert is_spot_precision_evidence_fresh(observed_at, evaluated_at) is expected


def test_merge_scale_observations_rejects_invalid_minimum() -> None:
    """Verify malformed artifact minimums poison the merged observation.

    Given: Scale observations merged against boolean, negative, and oversized
        minimum-decimals inputs,
    When: The merge runs,
    Then: Each malformed minimum yields a conflicting, uncertifiable result.
    """
    for minimum in (True, -1, 300):
        merged = spot_precision_evidence._merge_scale_observations([], minimum)
        assert merged.decimals is None
        assert merged.status == "conflicting"
