"""Pure authority for venue-bound spot precision certification.

The module evaluates one persisted asset evidence plane for one account venue,
asset, plane, and evaluation instant. Producer and consumer code share these
named checks so artifact, provenance, chronology, and freshness rules cannot
drift between derivation and reconciliation.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import InvalidOperation
from typing import Literal

import snapper.application.portfolio.walutomat_precision_artifact as walutomat_artifact
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import SpotAssetPrecisionEvidenceRow

SPOT_PRECISION_MAX_AGE = timedelta(hours=12)
SPOT_PRECISION_MAX_DECIMALS = 256
WALUTOMAT_OBSERVED_BALANCE_SOURCE = (
    "walutomat:api-v2.0.0:account/balances:venue_raw-decimal-triplet"
)
KRAKEN_INSTRUMENT_SPEC_SOURCE = "kraken:ccxt.load_markets"
_KRAKEN_SPEC_ETL_VERSION = "s2a-v1"

type SpotPrecisionDerivationStatus = Literal["certified", "missing", "conflicting"]
type SpotPrecisionEvidencePlane = Literal["balance", "fee"]
type SpotPrecisionEvidenceSourceKind = Literal[
    "documentary_fee",
    "documentary_instrument_spec",
    "kraken_instrument_spec",
    "observed_balance",
    "unknown",
]
type SpotPrecisionCertificationFailure = Literal[
    "artifact_not_effective",
    "artifact_observation_mismatch",
    "below_artifact_floor",
    "content_digest_mismatch",
    "evidence_identity_mismatch",
    "exchange_mismatch",
    "invalid_precision_value",
    "revoked_or_noncurrent_plane",
    "source_plane_mismatch",
    "stale_or_future_evidence",
]


@dataclass(frozen=True)
class _SpotPrecisionPlaneState:
    """One selected persisted plane with its exact row identity."""

    exchange: str
    asset: str
    plane: SpotPrecisionEvidencePlane
    decimals: int | None
    source: str | None
    version: str | None
    observed_at: datetime | None


def precision_evidence_source_kind(
    source: str | None,
) -> SpotPrecisionEvidenceSourceKind:
    """Classify only exact supported spot precision provenance families.

    Args:
        source: Persisted provenance label, or absence.

    Returns:
        Exact supported source kind, or ``unknown``.
    """
    if source == walutomat_artifact.WALUTOMAT_DOCUMENTARY_FEE_SOURCE:
        return "documentary_fee"
    if source == walutomat_artifact.WALUTOMAT_DOCUMENTARY_SPEC_SOURCE:
        return "documentary_instrument_spec"
    if source == WALUTOMAT_OBSERVED_BALANCE_SOURCE:
        return "observed_balance"
    if source == KRAKEN_INSTRUMENT_SPEC_SOURCE:
        return "kraken_instrument_spec"
    return "unknown"


def check_plane_source_compatibility(
    plane: SpotPrecisionEvidencePlane,
    source_kind: SpotPrecisionEvidenceSourceKind,
) -> bool:
    """Require each asset evidence source to certify only its supported plane.

    Args:
        plane: Selected balance or fee evidence plane.
        source_kind: Classified provenance family.

    Returns:
        Whether the source kind is authoritative for the selected plane.
    """
    return (source_kind == "documentary_fee" and plane == "fee") or (
        source_kind == "observed_balance" and plane == "balance"
    )


def check_exchange_binding(
    expected_exchange: str,
    evidence_exchange: str,
    source_kind: SpotPrecisionEvidenceSourceKind,
) -> bool:
    """Bind the row and its exact provenance family to the account venue.

    Args:
        expected_exchange: Canonical account venue being certified.
        evidence_exchange: Venue identity carried by the evidence row.
        source_kind: Classified provenance family.

    Returns:
        Whether row identity and provenance both belong to the account venue.
    """
    source_exchange = {
        "documentary_fee": "walutomat",
        "documentary_instrument_spec": "walutomat",
        "kraken_instrument_spec": "kraken",
        "observed_balance": "walutomat",
    }.get(source_kind)
    return (
        bool(expected_exchange)
        and expected_exchange == expected_exchange.strip().lower()
        and evidence_exchange == expected_exchange
        and source_exchange == expected_exchange
    )


def walutomat_observed_balance_precision_version(
    asset: str,
    balance_decimals: int | None,
    derivation_status: SpotPrecisionDerivationStatus,
) -> str:
    """Hash the complete current Walutomat balance derivation inputs.

    Args:
        asset: Exact canonical asset identity.
        balance_decimals: Derived balance scale, or explicit absence.
        derivation_status: Outcome of the producer derivation.

    Returns:
        Content-addressed version for the complete derivation tuple.
    """
    artifact = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    payload = {
        "asset": asset,
        "balance_decimals": balance_decimals,
        "derivation_status": derivation_status,
        "fee_decimals": None,
        "source": WALUTOMAT_OBSERVED_BALANCE_SOURCE,
        "upstream_sources": [walutomat_artifact.WALUTOMAT_DOCUMENTARY_SPEC_SOURCE],
        "upstream_versions": [walutomat_artifact.walutomat_precision_artifact_version(artifact)],
    }
    content = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return f"spot-asset-precision-v1:{hashlib.sha256(content.encode()).hexdigest()}"


def check_content_digest_equality(
    asset: str,
    decimals: int | None,
    version: str | None,
    source_kind: SpotPrecisionEvidenceSourceKind,
) -> bool:
    """Recompute and compare the canonical content version for its source kind.

    Args:
        asset: Persisted asset identity bound into observed evidence.
        decimals: Persisted scale for the selected plane.
        version: Persisted content-addressed version.
        source_kind: Classified provenance family.

    Returns:
        Whether the persisted version equals the recomputed canonical digest.
    """
    if version is None or not version or version.strip() != version:
        return False
    try:
        if source_kind == "observed_balance":
            return version == walutomat_observed_balance_precision_version(
                asset,
                decimals,
                "certified",
            )
        if source_kind in ("documentary_fee", "documentary_instrument_spec"):
            return version == walutomat_artifact.walutomat_precision_artifact_version(
                walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
            )
        return False
    except AttributeError, TypeError, ValueError:
        return False


def check_artifact_floor(
    decimals: int | None,
    source_kind: SpotPrecisionEvidenceSourceKind,
) -> bool:
    """Enforce the current documentary minimum on floor-carrying evidence.

    Args:
        decimals: Persisted scale for the selected evidence plane.
        source_kind: Classified provenance family.

    Returns:
        Whether the scale satisfies every applicable current artifact floor.
    """
    if source_kind != "observed_balance":
        return True
    minimum = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT.currency_amount_minimum_decimals
    if minimum is None:
        return True
    return (
        isinstance(minimum, int)
        and not isinstance(minimum, bool)
        and 0 <= minimum <= SPOT_PRECISION_MAX_DECIMALS
        and isinstance(decimals, int)
        and not isinstance(decimals, bool)
        and decimals >= minimum
    )


def check_artifact_temporal_validity(
    source_kind: SpotPrecisionEvidenceSourceKind,
    evaluated_at: datetime,
) -> bool:
    """Require an aware reviewed and effective artifact no later than evaluation.

    Args:
        source_kind: Classified provenance family.
        evaluated_at: Certification boundary instant.

    Returns:
        Whether applicable artifact dates are valid at the boundary.
    """
    if source_kind == "kraken_instrument_spec":
        return isinstance(evaluated_at, datetime) and evaluated_at.utcoffset() is not None
    if source_kind not in (
        "documentary_fee",
        "documentary_instrument_spec",
        "observed_balance",
    ):
        return False
    artifact = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    reviewed_at = artifact.reviewed_at
    effective_at = artifact.effective_at
    return (
        isinstance(reviewed_at, datetime)
        and reviewed_at.utcoffset() is not None
        and isinstance(effective_at, date)
        and not isinstance(effective_at, datetime)
        and isinstance(evaluated_at, datetime)
        and evaluated_at.utcoffset() is not None
        and reviewed_at <= evaluated_at
        and effective_at <= evaluated_at.date()
    )


def check_artifact_observation_binding(
    source_kind: SpotPrecisionEvidenceSourceKind,
    observed_at: datetime | None,
) -> bool:
    """Bind documentary observation time exactly to the deployed artifact review.

    Args:
        source_kind: Classified provenance family.
        observed_at: Persisted evidence observation instant.

    Returns:
        Whether documentary evidence carries the exact current review instant.
    """
    if source_kind in ("documentary_fee", "documentary_instrument_spec"):
        return observed_at == walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT.reviewed_at
    return source_kind in ("kraken_instrument_spec", "observed_balance")


def is_spot_precision_evidence_fresh(
    observed_at: datetime | None,
    evaluated_at: datetime,
) -> bool:
    """Check that observed evidence is aware, nonfuture, and under twelve hours.

    Args:
        observed_at: Persisted live observation instant.
        evaluated_at: Certification boundary instant.

    Returns:
        Whether the observation lies inside the strict freshness window.
    """
    if (
        observed_at is None
        or not isinstance(observed_at, datetime)
        or observed_at.utcoffset() is None
        or not isinstance(evaluated_at, datetime)
        or evaluated_at.utcoffset() is None
    ):
        return False
    age = evaluated_at - observed_at
    return timedelta(0) <= age < SPOT_PRECISION_MAX_AGE


def check_source_freshness(
    source_kind: SpotPrecisionEvidenceSourceKind,
    observed_at: datetime | None,
    evaluated_at: datetime,
) -> bool:
    """Apply strict live freshness while current documentary evidence does not age out.

    Args:
        source_kind: Classified provenance family.
        observed_at: Persisted evidence observation instant.
        evaluated_at: Certification boundary instant.

    Returns:
        Whether the source-specific freshness rule holds.
    """
    if source_kind in ("kraken_instrument_spec", "observed_balance"):
        return is_spot_precision_evidence_fresh(observed_at, evaluated_at)
    return source_kind in ("documentary_fee", "documentary_instrument_spec")


def check_plane_chronology_and_revocation(
    decimals: int | None,
    source: str | None,
    version: str | None,
    observed_at: datetime | None,
    evaluated_at: datetime,
) -> bool:
    """Reject a selected revoked, partial, naive, stale-ordered, or future plane.

    Args:
        decimals: Current selected-plane scale, where absence revokes.
        source: Current selected-plane provenance label.
        version: Current selected-plane content version.
        observed_at: Current selected-plane observation instant.
        evaluated_at: Certification boundary instant.

    Returns:
        Whether the current plane is complete, unrevoked, and chronological.
    """
    return (
        decimals is not None
        and source is not None
        and bool(source)
        and source.strip() == source
        and version is not None
        and bool(version)
        and version.strip() == version
        and observed_at is not None
        and isinstance(observed_at, datetime)
        and observed_at.utcoffset() is not None
        and isinstance(evaluated_at, datetime)
        and evaluated_at.utcoffset() is not None
        and observed_at <= evaluated_at
    )


def _quantum(decimals: int | None) -> Decimal | None:
    """Return a bounded positive decimal quantum for an integer scale."""
    if (
        decimals is None
        or not isinstance(decimals, int)
        or isinstance(decimals, bool)
        or decimals < 0
        or decimals > SPOT_PRECISION_MAX_DECIMALS
    ):
        return None
    return Decimal(1).scaleb(-decimals)


def check_precision_value(
    decimals: int | None,
    source_kind: SpotPrecisionEvidenceSourceKind,
) -> bool:
    """Validate bounded scale and exact documentary fee quantum semantics.

    Args:
        decimals: Persisted scale for the selected plane.
        source_kind: Classified provenance family.

    Returns:
        Whether the scale and any documentary quantum are valid.
    """
    quantum = _quantum(decimals)
    if quantum is None:
        return False
    if source_kind != "documentary_fee":
        return source_kind != "unknown"
    artifact = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    try:
        documented_quantum = Decimal(artifact.fee_rounding_quantum)
    except InvalidOperation, TypeError, ValueError:
        return False
    return (
        decimals == artifact.fee_decimals
        and documented_quantum.is_finite()
        and documented_quantum > 0
        and documented_quantum == quantum
    )


def check_evidence_identity(
    expected_asset: str,
    evidence_asset: str,
) -> bool:
    """Bind certification to the exact requested and persisted asset identity.

    Args:
        expected_asset: Exact asset requested by the consumer.
        evidence_asset: Exact asset carried by the evidence row.

    Returns:
        Whether the nonempty identities match without normalization.
    """
    return (
        bool(expected_asset)
        and expected_asset.strip() == expected_asset
        and evidence_asset == expected_asset
    )


def _plane_state(
    evidence: SpotAssetPrecisionEvidenceRow,
    plane: SpotPrecisionEvidencePlane,
) -> _SpotPrecisionPlaneState:
    """Select one plane without falling back to its sibling or historical maximum."""
    if plane == "balance":
        return _SpotPrecisionPlaneState(
            exchange=evidence["exchange"],
            asset=evidence["asset"],
            plane=plane,
            decimals=evidence["balance_decimals"],
            source=evidence["balance_source"],
            version=evidence["balance_version"],
            observed_at=evidence["balance_observed_at"],
        )
    return _SpotPrecisionPlaneState(
        exchange=evidence["exchange"],
        asset=evidence["asset"],
        plane=plane,
        decimals=evidence["fee_decimals"],
        source=evidence["fee_source"],
        version=evidence["fee_version"],
        observed_at=evidence["fee_observed_at"],
    )


def spot_precision_plane_certification_failures(
    expected_exchange: str,
    expected_asset: str,
    plane: SpotPrecisionEvidencePlane,
    evidence: SpotAssetPrecisionEvidenceRow,
    evaluated_at: datetime,
) -> tuple[SpotPrecisionCertificationFailure, ...]:
    """Return every failed named invariant for one selected evidence plane.

    Args:
        expected_exchange: Canonical account venue being certified.
        expected_asset: Exact asset requested by the consumer.
        plane: Selected balance or fee evidence plane.
        evidence: Persisted venue-bound evidence row.
        evaluated_at: Certification boundary instant.

    Returns:
        Ordered stable failure names, or an empty tuple when certified.
    """
    state = _plane_state(evidence, plane)
    source_kind = precision_evidence_source_kind(state.source)
    failures: list[SpotPrecisionCertificationFailure] = []
    if not check_plane_source_compatibility(plane, source_kind):
        failures.append("source_plane_mismatch")
    if not check_exchange_binding(expected_exchange, state.exchange, source_kind):
        failures.append("exchange_mismatch")
    if not check_content_digest_equality(
        state.asset,
        state.decimals,
        state.version,
        source_kind,
    ):
        failures.append("content_digest_mismatch")
    if not check_artifact_floor(state.decimals, source_kind):
        failures.append("below_artifact_floor")
    if not check_artifact_temporal_validity(source_kind, evaluated_at):
        failures.append("artifact_not_effective")
    if not check_source_freshness(source_kind, state.observed_at, evaluated_at):
        failures.append("stale_or_future_evidence")
    if not check_plane_chronology_and_revocation(
        state.decimals,
        state.source,
        state.version,
        state.observed_at,
        evaluated_at,
    ):
        failures.append("revoked_or_noncurrent_plane")
    if not check_artifact_observation_binding(source_kind, state.observed_at):
        failures.append("artifact_observation_mismatch")
    if not check_precision_value(state.decimals, source_kind):
        failures.append("invalid_precision_value")
    if not check_evidence_identity(expected_asset, state.asset):
        failures.append("evidence_identity_mismatch")
    return tuple(failures)


def is_spot_precision_plane_certified(
    expected_exchange: str,
    expected_asset: str,
    plane: SpotPrecisionEvidencePlane,
    evidence: SpotAssetPrecisionEvidenceRow,
    evaluated_at: datetime,
) -> bool:
    """Return whether one exact persisted asset evidence plane certifies.

    Args:
        expected_exchange: Canonical account venue being certified.
        expected_asset: Exact asset requested by the consumer.
        plane: Selected balance or fee evidence plane.
        evidence: Persisted venue-bound evidence row.
        evaluated_at: Certification boundary instant.

    Returns:
        Whether every named plane invariant succeeds.
    """
    return not spot_precision_plane_certification_failures(
        expected_exchange,
        expected_asset,
        plane,
        evidence,
        evaluated_at,
    )


def _finite_positive_decimal(value: object) -> Decimal | None:
    """Parse one nonboolean finite positive decimal-compatible value."""
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation, TypeError, ValueError:
        return None
    if not parsed.is_finite() or parsed <= 0:
        return None
    return parsed


def _persisted_positive_decimal(value: object) -> Decimal | None:
    """Round-trip a positive number through the persisted float representation."""
    parsed = _finite_positive_decimal(value)
    if parsed is None:
        return None
    return _finite_positive_decimal(float(parsed))


def kraken_instrument_precision_version(
    *,
    tick_size: object,
    lot_size: object,
    min_order_size: object,
    max_order_size: object,
    cost_decimals: int | None,
    qty_decimals: int | None,
    status: str | None,
    quantity_unit: str | None,
) -> str:
    """Hash the complete persisted Kraken spot precision tuple canonically.

    Args:
        tick_size: Venue or persisted price increment representation.
        lot_size: Venue or persisted quantity increment representation.
        min_order_size: Venue or persisted minimum quantity.
        max_order_size: Venue or persisted maximum quantity.
        cost_decimals: Persisted quote-cost scale.
        qty_decimals: Persisted base-quantity scale.
        status: Persisted instrument status.
        quantity_unit: Persisted quantity unit.

    Returns:
        Content-addressed version of the persistence-normalized tuple.
    """
    tick = _persisted_positive_decimal(tick_size)
    lot = _persisted_positive_decimal(lot_size)
    minimum = _finite_positive_decimal(min_order_size)
    maximum = _finite_positive_decimal(max_order_size)
    payload = {
        "tick_size": str(tick.normalize()) if tick is not None else None,
        "lot_size": str(lot.normalize()) if lot is not None else None,
        "min_order_size": float(minimum) if minimum is not None else None,
        "max_order_size": float(maximum) if maximum is not None else None,
        "cost_decimals": cost_decimals,
        "qty_decimals": qty_decimals,
        "status": status,
        "quantity_unit": quantity_unit,
    }
    content = json.dumps(payload, allow_nan=False, sort_keys=True, separators=(",", ":"))
    return f"{_KRAKEN_SPEC_ETL_VERSION}:{hashlib.sha256(content.encode()).hexdigest()}"


def check_instrument_content_digest_equality(
    spec: InstrumentSpecRow,
    source_kind: SpotPrecisionEvidenceSourceKind,
) -> bool:
    """Recompute the exact supported instrument content version.

    Args:
        spec: Persisted instrument precision specification.
        source_kind: Classified provenance family.

    Returns:
        Whether the persisted version equals its canonical content digest.
    """
    version = spec["spec_version"]
    if version is None or not version or version.strip() != version:
        return False
    try:
        if source_kind == "kraken_instrument_spec":
            return version == kraken_instrument_precision_version(
                tick_size=spec["tick_size"],
                lot_size=spec["lot_size"],
                min_order_size=spec["min_order_size"],
                max_order_size=spec["max_order_size"],
                cost_decimals=spec["cost_decimals"],
                qty_decimals=spec["qty_decimals"],
                status=spec["status"],
                quantity_unit=spec["quantity_unit"],
            )
        if source_kind == "documentary_instrument_spec":
            return version == walutomat_artifact.walutomat_precision_artifact_version(
                walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
            )
        return False
    except AttributeError, TypeError, ValueError:
        return False


def check_instrument_precision_structure(
    spec: InstrumentSpecRow,
    evaluated_at: datetime,
) -> bool:
    """Validate common exact spot instrument precision fields and chronology.

    Args:
        spec: Persisted instrument precision specification.
        evaluated_at: Certification boundary instant.

    Returns:
        Whether required values, provenance, and chronology are valid.
    """
    observed_at = spec["spec_observed_at"]
    return (
        _finite_positive_decimal(spec["tick_size"]) is not None
        and spec["instrument_kind"] == "spot"
        and spec["quantity_unit"] == "base_asset"
        and spec["status"] == "active"
        and _quantum(spec["qty_decimals"]) is not None
        and _quantum(spec["cost_decimals"]) is not None
        and bool(spec["spec_source"])
        and bool(spec["spec_version"])
        and observed_at is not None
        and isinstance(observed_at, datetime)
        and observed_at.utcoffset() is not None
        and isinstance(evaluated_at, datetime)
        and evaluated_at.utcoffset() is not None
        and observed_at <= evaluated_at
    )


def check_documentary_instrument_precision_value(spec: InstrumentSpecRow) -> bool:
    """Match persisted Walutomat order precision to every relevant artifact field.

    Args:
        spec: Persisted Walutomat instrument precision specification.

    Returns:
        Whether every precision value matches the current artifact exactly.
    """
    artifact = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    tick = _finite_positive_decimal(spec["tick_size"])
    lot = _finite_positive_decimal(spec["lot_size"])
    if tick is None or lot is None:
        return False
    try:
        expected_tick = Decimal(1).scaleb(-artifact.limit_price_max_decimals)
        expected_lot = Decimal(1).scaleb(-artifact.market_order_volume_decimals)
    except InvalidOperation, TypeError, ValueError:
        return False
    return (
        tick == expected_tick
        and lot == expected_lot
        and spec["qty_decimals"] == artifact.market_order_volume_decimals
        and spec["cost_decimals"] == artifact.cost_decimals
    )


def is_spot_instrument_precision_certified(
    expected_exchange: str,
    spec: InstrumentSpecRow,
    evaluated_at: datetime,
) -> bool:
    """Return whether one venue-bound spot instrument precision tuple certifies.

    Args:
        expected_exchange: Canonical account venue being certified.
        spec: Persisted instrument precision specification.
        evaluated_at: Certification boundary instant.

    Returns:
        Whether every applicable venue-bound instrument check succeeds.
    """
    if not check_instrument_precision_structure(spec, evaluated_at):
        return False
    source_kind = precision_evidence_source_kind(spec["spec_source"])
    if not check_exchange_binding(expected_exchange, expected_exchange, source_kind):
        return False
    if not check_instrument_content_digest_equality(spec, source_kind):
        return False
    if not check_artifact_temporal_validity(source_kind, evaluated_at):
        return False
    if not check_artifact_observation_binding(source_kind, spec["spec_observed_at"]):
        return False
    if not check_source_freshness(source_kind, spec["spec_observed_at"], evaluated_at):
        return False
    if source_kind == "documentary_instrument_spec":
        return check_documentary_instrument_precision_value(spec)
    return source_kind == "kraken_instrument_spec"
