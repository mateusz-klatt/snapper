"""Tests for the pure venue-bound spot precision certification authority."""

from dataclasses import replace
from datetime import date
from datetime import datetime
from datetime import timedelta
from typing import Literal
from typing import cast

import pytest

import snapper.application.portfolio.spot_precision_certification as certification
import snapper.application.portfolio.walutomat_precision_artifact as walutomat_artifact
from snapper.application.portfolio.spot_precision_certification import (
    SpotPrecisionCertificationFailure,
)
from snapper.application.portfolio.spot_precision_certification import SpotPrecisionEvidencePlane
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import SpotAssetPrecisionEvidenceRow

_ARTIFACT = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
_EVALUATED_AT = _ARTIFACT.reviewed_at + timedelta(hours=1)
_ASSET = "EUR"


def _artifact_version() -> str:
    """Return the digest for the currently deployed documentary artifact."""
    return walutomat_artifact.walutomat_precision_artifact_version(
        walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    )


def _balance_decimals() -> int:
    """Return a valid scale satisfying the current documentary balance floor."""
    minimum = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT.currency_amount_minimum_decimals
    return 2 if minimum is None else minimum


def _walutomat_evidence(
    evaluated_at: datetime = _EVALUATED_AT,
) -> SpotAssetPrecisionEvidenceRow:
    """Build exact current Walutomat balance and fee evidence planes."""
    artifact = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    balance_decimals = _balance_decimals()
    return SpotAssetPrecisionEvidenceRow(
        exchange="walutomat",
        asset=_ASSET,
        balance_decimals=balance_decimals,
        balance_source=certification.WALUTOMAT_OBSERVED_BALANCE_SOURCE,
        balance_version=certification.walutomat_observed_balance_precision_version(
            _ASSET,
            balance_decimals,
            "certified",
        ),
        balance_observed_at=evaluated_at - timedelta(minutes=1),
        fee_decimals=artifact.fee_decimals,
        fee_source=walutomat_artifact.WALUTOMAT_DOCUMENTARY_FEE_SOURCE,
        fee_version=_artifact_version(),
        fee_observed_at=artifact.reviewed_at,
    )


def _changed_evidence(
    evidence: SpotAssetPrecisionEvidenceRow,
    **overrides: object,
) -> SpotAssetPrecisionEvidenceRow:
    """Return repository evidence with explicit adversarial field replacements."""
    values: dict[str, object] = dict(evidence)
    values.update(overrides)
    return cast(SpotAssetPrecisionEvidenceRow, values)


def _instrument_spec(
    source: str,
    version: str,
    observed_at: datetime,
) -> InstrumentSpecRow:
    """Build a structurally valid spot instrument specification row."""
    return InstrumentSpecRow(
        instrument_public_id="instrument-test",
        tick_size=0.01,
        lot_size=0.01,
        min_order_size=None,
        max_order_size=None,
        cost_decimals=2,
        qty_decimals=2,
        margin_initial=None,
        position_limit_long=None,
        position_limit_short=None,
        status="active",
        contract_size=None,
        quantity_unit="base_asset",
        spec_source=source,
        spec_version=version,
        spec_observed_at=observed_at,
        unit_certified=False,
        expiry_at=None,
        instrument_kind="spot",
        funding_type=None,
        funding_frequency_hours=None,
        rollover_rate_long=None,
        rollover_rate_short=None,
        max_funding_rate=None,
    )


def _kraken_spec() -> InstrumentSpecRow:
    """Build a fresh Kraken instrument specification."""
    spec = _instrument_spec(
        certification.KRAKEN_INSTRUMENT_SPEC_SOURCE,
        "pending-content-version",
        _EVALUATED_AT - timedelta(minutes=1),
    )
    return _changed_spec(
        spec,
        spec_version=certification.kraken_instrument_precision_version(
            tick_size=spec["tick_size"],
            lot_size=spec["lot_size"],
            min_order_size=spec["min_order_size"],
            max_order_size=spec["max_order_size"],
            cost_decimals=spec["cost_decimals"],
            qty_decimals=spec["qty_decimals"],
            status=spec["status"],
            quantity_unit=spec["quantity_unit"],
        ),
    )


def _walutomat_spec() -> InstrumentSpecRow:
    """Build an exact projection of the current Walutomat artifact."""
    artifact = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    spec = _instrument_spec(
        walutomat_artifact.WALUTOMAT_DOCUMENTARY_SPEC_SOURCE,
        _artifact_version(),
        artifact.reviewed_at,
    )
    return _changed_spec(
        spec,
        tick_size=10.0**-artifact.limit_price_max_decimals,
        lot_size=10.0**-artifact.market_order_volume_decimals,
        cost_decimals=artifact.cost_decimals,
        qty_decimals=artifact.market_order_volume_decimals,
    )


def _changed_spec(spec: InstrumentSpecRow, **overrides: object) -> InstrumentSpecRow:
    """Return a repository specification with selected field replacements."""
    values: dict[str, object] = dict(spec)
    values.update(overrides)
    return cast(InstrumentSpecRow, values)


_BASE_EVIDENCE = _walutomat_evidence()
_BALANCE_DECIMALS = _balance_decimals()
_FEE_DECIMALS = _ARTIFACT.fee_decimals
_OBSERVED_SOURCE = certification.WALUTOMAT_OBSERVED_BALANCE_SOURCE
_DOCUMENTARY_FEE_SOURCE = walutomat_artifact.WALUTOMAT_DOCUMENTARY_FEE_SOURCE
_DOCUMENTARY_SPEC_SOURCE = walutomat_artifact.WALUTOMAT_DOCUMENTARY_SPEC_SOURCE
_ARTIFACT_VERSION = _artifact_version()


@pytest.mark.parametrize(
    (
        "expected_exchange",
        "expected_asset",
        "plane",
        "evidence",
        "evaluated_at",
        "expected_failures",
    ),
    [
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _BASE_EVIDENCE,
            _EVALUATED_AT,
            (),
            id="walutomat-balance-happy",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "fee",
            _BASE_EVIDENCE,
            _EVALUATED_AT,
            (),
            id="walutomat-fee-happy",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "fee",
            _BASE_EVIDENCE,
            _ARTIFACT.reviewed_at + timedelta(days=30),
            (),
            id="documentary-fee-remains-current",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_decimals=_FEE_DECIMALS,
                balance_source=_DOCUMENTARY_FEE_SOURCE,
                balance_version=_ARTIFACT_VERSION,
                balance_observed_at=_ARTIFACT.reviewed_at,
            ),
            _EVALUATED_AT,
            ("source_plane_mismatch",),
            id="documentary-fee-cannot-certify-balance",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "fee",
            _changed_evidence(
                _BASE_EVIDENCE,
                fee_decimals=_BALANCE_DECIMALS,
                fee_source=_OBSERVED_SOURCE,
                fee_version=certification.walutomat_observed_balance_precision_version(
                    _ASSET,
                    _BALANCE_DECIMALS,
                    "certified",
                ),
                fee_observed_at=_EVALUATED_AT - timedelta(minutes=1),
            ),
            _EVALUATED_AT,
            ("source_plane_mismatch",),
            id="observed-balance-cannot-certify-fee",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_source=_DOCUMENTARY_SPEC_SOURCE,
                balance_version=_ARTIFACT_VERSION,
                balance_observed_at=_ARTIFACT.reviewed_at,
            ),
            _EVALUATED_AT,
            ("source_plane_mismatch",),
            id="documentary-spec-cannot-certify-balance",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "fee",
            _changed_evidence(
                _BASE_EVIDENCE,
                fee_source=_DOCUMENTARY_SPEC_SOURCE,
                fee_version=_ARTIFACT_VERSION,
                fee_observed_at=_ARTIFACT.reviewed_at,
            ),
            _EVALUATED_AT,
            ("source_plane_mismatch",),
            id="documentary-spec-cannot-certify-fee",
        ),
        pytest.param(
            "kraken",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                exchange="kraken",
                balance_source=certification.KRAKEN_INSTRUMENT_SPEC_SOURCE,
                balance_version="kraken-test-version",
            ),
            _EVALUATED_AT,
            ("source_plane_mismatch", "content_digest_mismatch"),
            id="kraken-spec-cannot-certify-balance",
        ),
        pytest.param(
            "kraken",
            _ASSET,
            "fee",
            _changed_evidence(
                _BASE_EVIDENCE,
                exchange="kraken",
                fee_source=certification.KRAKEN_INSTRUMENT_SPEC_SOURCE,
                fee_version="kraken-test-version",
                fee_observed_at=_EVALUATED_AT - timedelta(minutes=1),
            ),
            _EVALUATED_AT,
            ("source_plane_mismatch", "content_digest_mismatch"),
            id="kraken-spec-cannot-certify-fee",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_source="unknown:precision-source",
                balance_version="unknown-version",
            ),
            _EVALUATED_AT,
            (
                "source_plane_mismatch",
                "exchange_mismatch",
                "content_digest_mismatch",
                "artifact_not_effective",
                "stale_or_future_evidence",
                "artifact_observation_mismatch",
                "invalid_precision_value",
            ),
            id="unknown-source-fails-closed",
        ),
        pytest.param(
            "kraken",
            _ASSET,
            "balance",
            _BASE_EVIDENCE,
            _EVALUATED_AT,
            ("exchange_mismatch",),
            id="expected-exchange-mismatch",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(_BASE_EVIDENCE, exchange="kraken"),
            _EVALUATED_AT,
            ("exchange_mismatch",),
            id="evidence-row-exchange-mismatch",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_decimals=_BALANCE_DECIMALS - 1,
                balance_version=certification.walutomat_observed_balance_precision_version(
                    _ASSET,
                    _BALANCE_DECIMALS - 1,
                    "certified",
                ),
            ),
            _EVALUATED_AT,
            ("below_artifact_floor",),
            id="same-digest-below-current-floor",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(_BASE_EVIDENCE, balance_version="forged-digest"),
            _EVALUATED_AT,
            ("content_digest_mismatch",),
            id="wrong-content-digest",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                asset="USD",
                balance_version=certification.walutomat_observed_balance_precision_version(
                    "USD",
                    _BALANCE_DECIMALS,
                    "certified",
                ),
            ),
            _EVALUATED_AT,
            ("evidence_identity_mismatch",),
            id="wrong-evidence-asset",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_version=certification.walutomat_observed_balance_precision_version(
                    "USD",
                    _BALANCE_DECIMALS,
                    "certified",
                ),
            ),
            _EVALUATED_AT,
            ("content_digest_mismatch",),
            id="digest-bound-to-wrong-asset",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_observed_at=_EVALUATED_AT
                - certification.SPOT_PRECISION_MAX_AGE
                + timedelta(microseconds=1),
            ),
            _EVALUATED_AT,
            (),
            id="observed-before-twelve-hour-boundary",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_observed_at=_EVALUATED_AT - certification.SPOT_PRECISION_MAX_AGE,
            ),
            _EVALUATED_AT,
            ("stale_or_future_evidence",),
            id="observed-at-twelve-hour-boundary",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_observed_at=_EVALUATED_AT + timedelta(microseconds=1),
            ),
            _EVALUATED_AT,
            ("stale_or_future_evidence", "revoked_or_noncurrent_plane"),
            id="future-observed-evidence",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(_BASE_EVIDENCE, balance_version=None),
            _EVALUATED_AT,
            ("content_digest_mismatch", "revoked_or_noncurrent_plane"),
            id="partial-plane-missing-version",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(_BASE_EVIDENCE, balance_observed_at=None),
            _EVALUATED_AT,
            ("stale_or_future_evidence", "revoked_or_noncurrent_plane"),
            id="partial-plane-missing-observation",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_decimals=None,
                balance_source=None,
                balance_version=None,
                balance_observed_at=None,
            ),
            _EVALUATED_AT,
            (
                "source_plane_mismatch",
                "exchange_mismatch",
                "content_digest_mismatch",
                "artifact_not_effective",
                "stale_or_future_evidence",
                "revoked_or_noncurrent_plane",
                "artifact_observation_mismatch",
                "invalid_precision_value",
            ),
            id="explicit-none-revocation-does-not-use-sibling-plane",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_decimals=None,
                balance_version=certification.walutomat_observed_balance_precision_version(
                    _ASSET,
                    None,
                    "missing",
                ),
            ),
            _EVALUATED_AT,
            (
                "content_digest_mismatch",
                "below_artifact_floor",
                "revoked_or_noncurrent_plane",
                "invalid_precision_value",
            ),
            id="complete-provenance-balance-none-revocation",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "fee",
            _changed_evidence(_BASE_EVIDENCE, fee_decimals=None),
            _EVALUATED_AT,
            ("revoked_or_noncurrent_plane", "invalid_precision_value"),
            id="complete-provenance-fee-none-revocation",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "balance",
            _changed_evidence(
                _BASE_EVIDENCE,
                balance_decimals=certification.SPOT_PRECISION_MAX_DECIMALS + 1,
                balance_version=certification.walutomat_observed_balance_precision_version(
                    _ASSET,
                    certification.SPOT_PRECISION_MAX_DECIMALS + 1,
                    "certified",
                ),
            ),
            _EVALUATED_AT,
            ("invalid_precision_value",),
            id="balance-decimals-out-of-range",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "fee",
            _changed_evidence(
                _BASE_EVIDENCE,
                fee_decimals=_FEE_DECIMALS + 1,
            ),
            _EVALUATED_AT,
            ("invalid_precision_value",),
            id="fee-decimals-do-not-match-quantum",
        ),
        pytest.param(
            "walutomat",
            _ASSET,
            "fee",
            _changed_evidence(
                _BASE_EVIDENCE,
                fee_observed_at=_ARTIFACT.reviewed_at + timedelta(microseconds=1),
            ),
            _EVALUATED_AT,
            ("artifact_observation_mismatch",),
            id="documentary-observation-not-bound-to-review",
        ),
    ],
)
def test_plane_certification_adversarial_matrix(
    expected_exchange: str,
    expected_asset: str,
    plane: SpotPrecisionEvidencePlane,
    evidence: SpotAssetPrecisionEvidenceRow,
    evaluated_at: datetime,
    expected_failures: tuple[SpotPrecisionCertificationFailure, ...],
) -> None:
    """Every plane invariant reports its stable failure and fails closed.

    Given: Exact happy rows and one or more deliberately violated invariants.
    When: The central authority audits and certifies the selected plane.
    Then: The complete ordered failure tuple and Boolean decision agree exactly.
    """
    failures = certification.spot_precision_plane_certification_failures(
        expected_exchange,
        expected_asset,
        plane,
        evidence,
        evaluated_at,
    )

    assert failures == expected_failures
    assert certification.is_spot_precision_plane_certified(
        expected_exchange,
        expected_asset,
        plane,
        evidence,
        evaluated_at,
    ) is not bool(expected_failures)


@pytest.mark.parametrize(
    "invalid_dimension",
    ["naive_reviewed_at", "future_reviewed_at", "future_effective_at"],
)
def test_plane_certification_rejects_invalid_artifact_dates(
    invalid_dimension: Literal[
        "naive_reviewed_at",
        "future_reviewed_at",
        "future_effective_at",
    ],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A naive or future artifact date invalidates even newly digested evidence.

    Given: A deployed artifact with naive or future review/effective provenance.
    When: Evidence is constructed with its newly recomputed correct content digest.
    Then: Certification reports only that the artifact is not yet effective.
    """
    artifact = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    evaluated_at = artifact.reviewed_at + timedelta(hours=1)
    if invalid_dimension == "naive_reviewed_at":
        replacement = replace(
            artifact,
            reviewed_at=artifact.reviewed_at.replace(tzinfo=None),
        )
    elif invalid_dimension == "future_reviewed_at":
        replacement = replace(
            artifact,
            reviewed_at=evaluated_at + timedelta(microseconds=1),
        )
    else:
        replacement = replace(
            artifact,
            effective_at=evaluated_at.date() + timedelta(days=1),
        )
    monkeypatch.setattr(walutomat_artifact, "WALUTOMAT_PRECISION_ARTIFACT", replacement)
    evidence = _walutomat_evidence(evaluated_at)

    assert certification.spot_precision_plane_certification_failures(
        "walutomat",
        _ASSET,
        "balance",
        evidence,
        evaluated_at,
    ) == ("artifact_not_effective",)


@pytest.mark.parametrize(
    "fee_rounding_quantum",
    ["not-a-decimal", "NaN", "0", "0.001"],
)
def test_documentary_fee_rejects_invalid_current_artifact_quantum(
    fee_rounding_quantum: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed, nonpositive, or scale-inconsistent fee quanta fail closed.

    Given: Current documentary content carrying an invalid fee rounding quantum.
    When: Its digest is recomputed and the fee plane is audited.
    Then: The precision-value invariant alone rejects certification.
    """
    artifact = replace(
        walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT,
        fee_rounding_quantum=fee_rounding_quantum,
    )
    monkeypatch.setattr(walutomat_artifact, "WALUTOMAT_PRECISION_ARTIFACT", artifact)
    evidence = _walutomat_evidence(_EVALUATED_AT)

    assert certification.spot_precision_plane_certification_failures(
        "walutomat",
        _ASSET,
        "fee",
        evidence,
        _EVALUATED_AT,
    ) == ("invalid_precision_value",)


def test_content_digest_recomputation_exception_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed canonical artifact content cannot escape digest certification.

    Given: A deployed artifact whose effective date cannot be serialized.
    When: Documentary content digest equality is recomputed.
    Then: The named check catches the malformed boundary and returns false.
    """
    malformed = replace(
        walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT,
        effective_at=cast(date, object()),
    )
    monkeypatch.setattr(walutomat_artifact, "WALUTOMAT_PRECISION_ARTIFACT", malformed)

    assert (
        certification.check_content_digest_equality(
            _ASSET,
            _BALANCE_DECIMALS,
            "candidate-version",
            "documentary_fee",
        )
        is False
    )


@pytest.mark.parametrize("version", [None, "", " padded-version "])
def test_instrument_content_digest_rejects_invalid_version_text(
    version: str | None,
) -> None:
    """Missing, empty, or padded instrument versions fail the digest check.

    Given: An otherwise valid Kraken tuple with invalid version text.
    When: Instrument content equality is checked directly.
    Then: The named digest invariant returns false.
    """
    assert (
        certification.check_instrument_content_digest_equality(
            _changed_spec(_kraken_spec(), spec_version=version),
            "kraken_instrument_spec",
        )
        is False
    )


def test_instrument_content_digest_rejects_an_unsupported_source_kind() -> None:
    """A valid-looking tuple has no digest meaning for an unsupported source.

    Given: A current Kraken row classified as an unknown provenance family.
    When: Instrument content equality is checked directly.
    Then: The unsupported source kind returns false.
    """
    assert (
        certification.check_instrument_content_digest_equality(
            _kraken_spec(),
            "unknown",
        )
        is False
    )


def test_instrument_content_digest_serialization_failure_fails_closed() -> None:
    """Malformed persisted content cannot escape digest comparison.

    Given: A Kraken tuple containing a nonserializable status value.
    When: Its canonical content digest is recomputed.
    Then: Serialization failure is contained and returns false.
    """
    malformed = _changed_spec(_kraken_spec(), status=cast(str, object()))

    assert (
        certification.check_instrument_content_digest_equality(
            malformed,
            "kraken_instrument_spec",
        )
        is False
    )


@pytest.mark.parametrize(
    ("venue_tick", "venue_lot"),
    [
        ("0.0100", "1.000"),
        ("0.1234567890123456789", "0.9876543210987654321"),
    ],
)
def test_kraken_content_version_survives_float_persistence_normalization(
    venue_tick: str,
    venue_lot: str,
) -> None:
    """Lexical zeros and long venue decimals remain recomputable after persistence.

    Given: Kraken precision content before and after its float persistence boundary.
    When: The shared canonical version function hashes both representations.
    Then: Both producer and consumer representations have the same digest.
    """
    venue_version = certification.kraken_instrument_precision_version(
        tick_size=venue_tick,
        lot_size=venue_lot,
        min_order_size="0.010",
        max_order_size="10.00",
        cost_decimals=2,
        qty_decimals=3,
        status="active",
        quantity_unit="base_asset",
    )
    persisted_version = certification.kraken_instrument_precision_version(
        tick_size=float(venue_tick),
        lot_size=float(venue_lot),
        min_order_size=0.01,
        max_order_size=10.0,
        cost_decimals=2,
        qty_decimals=3,
        status="active",
        quantity_unit="base_asset",
    )

    assert venue_version == persisted_version


def test_kraken_content_version_canonicalizes_missing_precision() -> None:
    """Unusable producer precision has one stable explicit-null digest.

    Given: Missing and malformed Kraken tick and lot values.
    When: The shared canonical version function hashes each tuple.
    Then: Both representations map to the same explicit-null content version.
    """
    missing = certification.kraken_instrument_precision_version(
        tick_size=None,
        lot_size=None,
        min_order_size=None,
        max_order_size=None,
        cost_decimals=None,
        qty_decimals=None,
        status=None,
        quantity_unit="base_asset",
    )
    malformed = certification.kraken_instrument_precision_version(
        tick_size="invalid",
        lot_size=False,
        min_order_size=None,
        max_order_size=None,
        cost_decimals=None,
        qty_decimals=None,
        status=None,
        quantity_unit="base_asset",
    )

    assert missing == malformed


def test_observed_balance_floor_check_allows_an_artifact_without_a_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent documentary floor does not invent a floor at certification.

    Given: A deployed artifact explicitly carrying no currency minimum scale.
    When: The observed-balance floor check evaluates a bounded scale.
    Then: The floor-specific check is not applicable and returns true.
    """
    without_floor = replace(
        walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT,
        currency_amount_minimum_decimals=None,
    )
    monkeypatch.setattr(
        walutomat_artifact,
        "WALUTOMAT_PRECISION_ARTIFACT",
        without_floor,
    )

    assert certification.check_artifact_floor(0, "observed_balance") is True


def test_documentary_instrument_malformed_expected_quantum_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed documentary exponent fields cannot escape value certification.

    Given: A persisted happy specification and a deployed artifact with an invalid exponent.
    When: Documentary instrument values are compared to the current artifact.
    Then: Expected-quantum construction catches the malformed field and returns false.
    """
    persisted = _walutomat_spec()
    malformed = replace(
        walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT,
        limit_price_max_decimals=cast(int, object()),
    )
    monkeypatch.setattr(walutomat_artifact, "WALUTOMAT_PRECISION_ARTIFACT", malformed)

    assert certification.check_documentary_instrument_precision_value(persisted) is False


@pytest.mark.parametrize(
    ("expected_exchange", "spec", "expected"),
    [
        pytest.param("kraken", _kraken_spec(), True, id="kraken-happy"),
        pytest.param("walutomat", _walutomat_spec(), True, id="walutomat-happy"),
        pytest.param(
            "walutomat",
            _kraken_spec(),
            False,
            id="kraken-source-relabeled-as-walutomat",
        ),
        pytest.param(
            "kraken",
            _walutomat_spec(),
            False,
            id="walutomat-source-relabeled-as-kraken",
        ),
    ],
)
def test_instrument_certification_is_bound_to_its_account_venue(
    expected_exchange: str,
    spec: InstrumentSpecRow,
    expected: bool,
) -> None:
    """Instrument fallbacks certify only exact compatible account venues.

    Given: Happy instrument rows and the same provenance relabeled across accounts.
    When: The central instrument predicate binds each row to its expected venue.
    Then: Both venue happy paths pass and both cross-venue relabelings fail.
    """
    assert (
        certification.is_spot_instrument_precision_certified(
            expected_exchange,
            spec,
            _EVALUATED_AT,
        )
        is expected
    )


@pytest.mark.parametrize(
    ("expected_exchange", "spec"),
    [
        pytest.param(
            "kraken",
            _changed_spec(_kraken_spec(), tick_size=None),
            id="missing-tick",
        ),
        pytest.param(
            "kraken",
            _changed_spec(_kraken_spec(), tick_size=cast(float, object())),
            id="malformed-tick",
        ),
        pytest.param(
            "kraken",
            _changed_spec(_kraken_spec(), tick_size=float("nan")),
            id="nonfinite-tick",
        ),
        pytest.param(
            "kraken",
            _changed_spec(_kraken_spec(), spec_version="forged-version"),
            id="kraken-forged-content-version",
        ),
        pytest.param(
            "kraken",
            _changed_spec(_kraken_spec(), lot_size=0.02),
            id="kraken-content-changed-after-versioning",
        ),
        pytest.param(
            "kraken",
            _changed_spec(
                _kraken_spec(),
                spec_observed_at=cast(datetime, object()),
            ),
            id="malformed-observed-at",
        ),
        pytest.param(
            "walutomat",
            _changed_spec(_walutomat_spec(), lot_size=None),
            id="missing-documentary-lot",
        ),
        pytest.param(
            "walutomat",
            _changed_spec(_walutomat_spec(), spec_version="forged-version"),
            id="documentary-digest-mismatch",
        ),
        pytest.param(
            "walutomat",
            _changed_spec(
                _walutomat_spec(),
                spec_observed_at=_ARTIFACT.reviewed_at + timedelta(microseconds=1),
            ),
            id="documentary-observation-mismatch",
        ),
        pytest.param(
            "kraken",
            _changed_spec(
                _kraken_spec(),
                spec_observed_at=_EVALUATED_AT - certification.SPOT_PRECISION_MAX_AGE,
            ),
            id="stale-kraken-specification",
        ),
    ],
)
def test_instrument_certification_fail_closed_paths(
    expected_exchange: str,
    spec: InstrumentSpecRow,
) -> None:
    """Every defensive instrument stage rejects its isolated invalid input.

    Given: Structurally malformed or later-stage-invalid instrument evidence.
    When: The central venue-bound instrument predicate evaluates the row.
    Then: Parsing, digest, observation binding, and freshness failures return false.
    """
    assert (
        certification.is_spot_instrument_precision_certified(
            expected_exchange,
            spec,
            _EVALUATED_AT,
        )
        is False
    )


def test_instrument_certification_rejects_malformed_evaluation_time() -> None:
    """A non-datetime evaluation boundary fails closed without escaping.

    Given: An otherwise current Kraken specification and a malformed evaluation time.
    When: The central instrument authority evaluates chronology.
    Then: Certification returns false without raising a boundary exception.
    """
    assert (
        certification.is_spot_instrument_precision_certified(
            "kraken",
            _kraken_spec(),
            cast(datetime, object()),
        )
        is False
    )


@pytest.mark.parametrize(
    ("reviewed_at", "effective_at"),
    [
        pytest.param(
            _EVALUATED_AT + timedelta(microseconds=1),
            _EVALUATED_AT.date(),
            id="future-reviewed-at",
        ),
        pytest.param(
            _ARTIFACT.reviewed_at,
            _EVALUATED_AT.date() + timedelta(days=1),
            id="future-effective-at",
        ),
    ],
)
def test_walutomat_instrument_rejects_future_artifact_dates(
    reviewed_at: datetime,
    effective_at: date,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Walutomat instrument certification enforces both artifact dates.

    Given: A current-looking artifact with a future review or effective date.
    When: Its exact projection and newly recomputed digest are certified.
    Then: The venue-bound instrument predicate rejects the not-yet-current artifact.
    """
    artifact = replace(
        walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT,
        reviewed_at=reviewed_at,
        effective_at=effective_at,
    )
    monkeypatch.setattr(walutomat_artifact, "WALUTOMAT_PRECISION_ARTIFACT", artifact)

    assert (
        certification.is_spot_instrument_precision_certified(
            "walutomat",
            _walutomat_spec(),
            _EVALUATED_AT,
        )
        is False
    )
