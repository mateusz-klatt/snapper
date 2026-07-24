"""Pure derivation of durable spot asset precision evidence.

Walutomat's authenticated balance response and reviewed documentary precision
rules are independent evidence inputs. Balance precision combines the maximum
fractional scale in a coherent raw observation with the current documentary
minimum. Fee precision comes only from the current content-addressed review
artifact and retains that artifact's review time across updater runs.
"""

import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import cast

import snapper.application.portfolio.spot_precision_certification as spot_precision_certification
import snapper.application.portfolio.walutomat_precision_artifact as walutomat_artifact
from snapper.application.portfolio.spot_precision_certification import SPOT_PRECISION_MAX_DECIMALS
from snapper.application.portfolio.spot_precision_certification import (
    WALUTOMAT_OBSERVED_BALANCE_SOURCE,
)
from snapper.application.portfolio.spot_precision_certification import (
    SpotPrecisionDerivationStatus as _DerivationStatus,
)
from snapper.application.portfolio.spot_precision_certification import (
    check_artifact_observation_binding,
)
from snapper.application.portfolio.spot_precision_certification import (
    check_artifact_temporal_validity,
)
from snapper.application.portfolio.spot_precision_certification import check_content_digest_equality
from snapper.application.portfolio.spot_precision_certification import check_exchange_binding
from snapper.application.portfolio.spot_precision_certification import (
    precision_evidence_source_kind,
)
from snapper.application.portfolio.spot_precision_certification import (
    walutomat_observed_balance_precision_version,
)
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry

_MAX_RAW_DECIMAL_LENGTH = 64
_PLAIN_DECIMAL = re.compile(r"\d+(?:\.\d+)?", flags=re.ASCII)


@dataclass(frozen=True)
class SpotAssetPrecisionCandidate:
    """One persistence-ready per-asset precision candidate."""

    exchange: str
    asset: str
    balance_decimals: int | None
    fee_decimals: int | None
    source: str
    version: str
    observed_at: datetime


@dataclass(frozen=True)
class SpotInstrumentPrecisionObservation:
    """Venue instrument metadata used to infer currency amount precision."""

    base_asset: str
    quote_asset: str
    qty_decimals: int | None
    cost_decimals: int | None
    source: str | None
    version: str | None
    observed_at: datetime | None


@dataclass(frozen=True)
class _ScaleObservation:
    """Internal scale candidate with its fail-closed derivation status."""

    decimals: int | None
    status: _DerivationStatus


def is_spot_precision_evidence_fresh(
    observed_at: datetime | None,
    evaluated_at: datetime,
) -> bool:
    """Delegate observed-evidence freshness to the certification authority.

    Args:
        observed_at: Persisted live observation instant.
        evaluated_at: Certification boundary instant.

    Returns:
        Whether the observation lies inside the strict freshness window.
    """
    return spot_precision_certification.is_spot_precision_evidence_fresh(
        observed_at,
        evaluated_at,
    )


def _canonical_asset(asset: str) -> str | None:
    """Accept only exact documented three-letter uppercase Walutomat codes."""
    if len(asset) != 3 or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" for character in asset):
        return None
    return asset


def _decimal_scale(raw: str) -> int | None:
    """Return one bounded scale after enforcing the raw-input size cap.

    The length cap bounds the largest representable exponent well below
    the module scale bound, so no separate exponent guard is needed here;
    ``SPOT_PRECISION_MAX_DECIMALS`` still bounds caller-supplied minimum scales in
    :func:`_merge_scale_observations`.
    """
    if len(raw) > _MAX_RAW_DECIMAL_LENGTH:
        return None
    if _PLAIN_DECIMAL.fullmatch(raw) is None:
        return None
    value = Decimal(raw)
    exponent = cast(int, value.as_tuple().exponent)
    return max(0, -exponent)


def _raw_balance_scale(entry: NativeBalanceEntry) -> _ScaleObservation:
    """Derive the maximum explicit fractional scale in one coherent triplet."""
    raw_values = (entry.total_decimal, entry.free_decimal, entry.used_decimal)
    companions = (entry.total, entry.free, entry.used)
    if any(raw is None for raw in raw_values) or any(companion is None for companion in companions):
        return _ScaleObservation(None, "missing")
    if entry.numeric_provenance != "venue_raw":
        return _ScaleObservation(None, "conflicting")
    scales: list[int] = []
    complete_raw_values = tuple(cast(str, raw) for raw in raw_values)
    complete_companions = tuple(cast(float, companion) for companion in companions)
    for raw, companion in zip(complete_raw_values, complete_companions, strict=True):
        scale = _decimal_scale(raw)
        if (
            scale is None
            or isinstance(companion, bool)
            or not math.isfinite(companion)
            or float(Decimal(raw)) != companion
        ):
            return _ScaleObservation(None, "conflicting")
        if "." in raw:
            scales.append(scale)
    return _ScaleObservation(max(scales) if scales else None, "certified")


def _instrument_artifact_status(
    observation: SpotInstrumentPrecisionObservation,
    evaluated_at: datetime,
) -> _ScaleObservation:
    """Validate one reference to the independently reviewed fee artifact."""
    source_kind = precision_evidence_source_kind(observation.source)
    if (
        source_kind != "documentary_instrument_spec"
        or not check_exchange_binding("walutomat", "walutomat", source_kind)
        or not check_content_digest_equality("", None, observation.version, source_kind)
        or not check_artifact_temporal_validity(source_kind, evaluated_at)
        or not check_artifact_observation_binding(source_kind, observation.observed_at)
    ):
        return _ScaleObservation(None, "missing")
    return _ScaleObservation(
        walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT.fee_decimals,
        "certified",
    )


def _merge_scale_observations(
    observations: Sequence[_ScaleObservation],
    minimum_decimals: int | None = None,
) -> _ScaleObservation:
    """Merge coherent evidence monotonically with an optional documented floor."""
    if any(observation.status == "conflicting" for observation in observations):
        return _ScaleObservation(None, "conflicting")
    if any(observation.status == "missing" for observation in observations):
        return _ScaleObservation(None, "missing")
    if minimum_decimals is not None and (
        isinstance(minimum_decimals, bool)
        or minimum_decimals < 0
        or minimum_decimals > SPOT_PRECISION_MAX_DECIMALS
    ):
        return _ScaleObservation(None, "conflicting")
    decimals = [
        observation.decimals for observation in observations if observation.decimals is not None
    ]
    if minimum_decimals is not None:
        decimals.append(minimum_decimals)
    if not decimals:
        return _ScaleObservation(None, "missing")
    return _ScaleObservation(max(decimals), "certified")


def derive_walutomat_precision_from_raw_balances(
    entries: Sequence[NativeBalanceEntry],
    observed_at: datetime,
) -> list[SpotAssetPrecisionCandidate]:
    """Derive monotonic balance-scale candidates from authenticated raw strings.

    Fee precision is not present in the balance response and therefore remains
    null. Every raw triplet must be complete and coherent. Explicit fractional
    scales merge by maximum. During production, :func:`_merge_scale_observations`
    applies the current artifact's ``currency_amount_minimum_decimals`` floor;
    integer-only input cannot certify without it. At consumption, the certification
    authority independently applies that same artifact field through
    :func:`spot_precision_certification.check_artifact_floor`, preserving the
    fail-closed floor at both boundaries without a redundant producer recheck.

    Args:
        entries: Authenticated Walutomat balance rows retaining raw decimal strings.
        observed_at: Caller-captured time for the balance observation.

    Returns:
        Sorted persistence candidates for exact canonical assets present in the input.
    """
    artifact = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    by_asset: dict[str, list[_ScaleObservation]] = {}
    for entry in entries:
        asset = _canonical_asset(entry.currency)
        if asset is None:
            continue
        by_asset.setdefault(asset, []).append(_raw_balance_scale(entry))
    candidates: list[SpotAssetPrecisionCandidate] = []
    source_kind = precision_evidence_source_kind(WALUTOMAT_OBSERVED_BALANCE_SOURCE)
    artifact_is_usable = check_exchange_binding(
        "walutomat",
        "walutomat",
        source_kind,
    ) and check_artifact_temporal_validity(source_kind, observed_at)
    for asset, observations in sorted(by_asset.items()):
        scale = _merge_scale_observations(
            observations,
            artifact.currency_amount_minimum_decimals if artifact_is_usable else None,
        )
        if artifact.currency_amount_minimum_decimals is not None and not artifact_is_usable:
            scale = _ScaleObservation(None, "missing")
        candidates.append(
            SpotAssetPrecisionCandidate(
                exchange="walutomat",
                asset=asset,
                balance_decimals=scale.decimals,
                fee_decimals=None,
                source=WALUTOMAT_OBSERVED_BALANCE_SOURCE,
                version=walutomat_observed_balance_precision_version(
                    asset,
                    scale.decimals,
                    scale.status,
                ),
                observed_at=observed_at,
            )
        )
    return candidates


def derive_walutomat_precision_from_instruments(
    observations: Sequence[SpotInstrumentPrecisionObservation],
    observed_at: datetime,
) -> list[SpotAssetPrecisionCandidate]:
    """Derive fee-only updater candidates from the current review artifact.

    Instrument rows supply only the exact asset identities to which the
    documentary fee rule applies. Their order precision values never promote
    balance precision. Repeated appearances must all address the exact current
    artifact content, and every result retains the artifact review timestamp
    rather than the updater run time.

    Args:
        observations: Per-pair asset identities and artifact provenance references.
        observed_at: Caller-captured updater time used to reject future artifacts.

    Returns:
        Sorted fee-only persistence candidates with balance precision always nullable.
    """
    by_asset: dict[str, list[_ScaleObservation]] = {}
    for observation in observations:
        base_asset = _canonical_asset(observation.base_asset)
        quote_asset = _canonical_asset(observation.quote_asset)
        if base_asset is None or quote_asset is None or base_asset == quote_asset:
            continue
        status = _instrument_artifact_status(observation, observed_at)
        by_asset.setdefault(base_asset, []).append(status)
        by_asset.setdefault(quote_asset, []).append(status)
    candidates: list[SpotAssetPrecisionCandidate] = []
    artifact = walutomat_artifact.WALUTOMAT_PRECISION_ARTIFACT
    artifact_version = walutomat_artifact.walutomat_precision_artifact_version(artifact)
    for asset, asset_observations in sorted(by_asset.items()):
        scale = _merge_scale_observations(asset_observations)
        fee_decimals = scale.decimals if scale.status == "certified" else None
        candidates.append(
            SpotAssetPrecisionCandidate(
                exchange="walutomat",
                asset=asset,
                balance_decimals=None,
                fee_decimals=fee_decimals,
                source=walutomat_artifact.WALUTOMAT_DOCUMENTARY_FEE_SOURCE,
                version=artifact_version,
                observed_at=artifact.reviewed_at,
            )
        )
    return candidates
