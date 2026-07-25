"""Async orchestrator + mark builder for the P&L timeline (Phase 5A API layer).

The pure :mod:`snapper.application.portfolio.pnl_timeline` builder performs no
I/O. It consumes already-fetched executions, accruals, and a mark lookup whose
money values are all expressed in the requested valuation currency. This module
is the thin async seam that proves the native denomination, loads finalized 1m
candles, converts those inputs, and calls the pure builder.

For each instrument the orchestrator resolves its ``(native_symbol,
source_exchange, instrument_exchange, base_currency, quote_currency)`` history
via :meth:`Repository.get_instrument_symbol_refs`. Every version known at the
response horizon must unanimously carry one base and one non-null quote currency.
Adjacent or overlapping knowledge intervals with the same full price projection
are merged before one projection is required to cover every fill. Missing,
gapped, conflicting, or venue-mismatched evidence remains untrusted and never
reaches the average-cost kernel as a certified price.

A proven foreign quote is converted rather than rejected. Each positive
execution price is converted at the fill's own minute before pool replay, and
each positive mark close is converted at the grid minute it values. General
consumers resolve one exact ``(base, quote, exchange)`` plane per unordered
currency pair by their own exact-minute coverage. A held instrument whose own
currency legs match a needed pair instead uses its canonical oriented source
plane for all of that instrument's conversions on the pair. Neither a rival
venue nor the opposite orientation can fill that instrument plane's gap, while
the gap cannot suppress independently covered peers. No triangulation,
nearest-minute match, or stale carry-forward is permitted. A missing execution
rate becomes ``NaN`` so the pure builder applies its existing
opening-versus-closing trust tiers. A missing mark rate emits no mark, producing
mark-incomplete valuation without tainting mark-independent cumulatives.

The mark for grid minute ``M`` is the close of the finalized candle covering
``[M-1m, M)`` (``open_at == M - 1min``), and its FX rate follows the same
convention. Execution, fee, and accrual conversions use the last rate bar that
had closed at the event's floored minute. Exact zero remains currency-invariant;
an unconvertible nonzero flow is passed as ``NaN`` rather than silently zeroed or
dropped.
"""

import json
import math
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from types import MappingProxyType
from typing import Final
from typing import Literal
from typing import cast
from uuid import UUID
from uuid import uuid7

from loguru import logger
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import TypeAdapter
from pydantic import ValidationError
from pydantic import field_validator

from snapper.application.portfolio.average_cost import FLAT_EPSILON
from snapper.application.portfolio.basket_valuation import CandleVersionIdentity
from snapper.application.portfolio.basket_valuation import FiatVersionMap
from snapper.application.portfolio.execution_chain import ExecutionChainError
from snapper.application.portfolio.fill_booking import booked_signed_quantity
from snapper.application.portfolio.fill_booking import resolve_position_quantity_unit
from snapper.application.portfolio.fx_rates import FxPairKey
from snapper.application.portfolio.fx_rates import FxRateKey
from snapper.application.portfolio.fx_rates import FxRateMap
from snapper.application.portfolio.fx_rates import FxVenueMap
from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_anchor_identity import normalize_portfolio_pnl_valuation_ccy
from snapper.application.portfolio.pnl_anchor_identity import (
    normalize_portfolio_pnl_wallet_public_id,
)
from snapper.application.portfolio.pnl_anchor_identity import portfolio_pnl_anchor_public_id
from snapper.application.portfolio.pnl_timeline import MarkIncompletenessReasonMap
from snapper.application.portfolio.pnl_timeline import MarkMap
from snapper.application.portfolio.pnl_timeline import OpeningPool
from snapper.application.portfolio.pnl_timeline import PnlAttributionContribution
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReason
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
from snapper.application.portfolio.pnl_timeline import PnlInstrumentContribution
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline import PnlTimelineResult
from snapper.application.portfolio.pnl_timeline import TimelineAccrual
from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.application.portfolio.pnl_timeline import TimelineExecutionLineage
from snapper.application.portfolio.pnl_timeline import TimelineOpening
from snapper.application.portfolio.pnl_timeline import TimelineOpeningDerivation
from snapper.application.portfolio.pnl_timeline import TimelineWindow
from snapper.application.portfolio.pnl_timeline import build_pnl_timeline
from snapper.application.portfolio.pnl_timeline import canonical_incompleteness_reasons
from snapper.application.portfolio.pnl_timeline import derive_timeline_opening
from snapper.core.numeric import is_positive_finite
from snapper.data.repository import PnlTimelineAnchorEvidenceMismatchError
from snapper.data.repository import PortfolioPnlSampleQuery
from snapper.data.repository import Repository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION as _PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineAiDecisionMarkerRow
from snapper.data.repository_types import PnlTimelineAppliedAnnulment
from snapper.data.repository_types import PnlTimelineCandleRow
from snapper.data.repository_types import PnlTimelineExecutionLineageRow
from snapper.data.repository_types import PnlTimelineExecutionPrefix
from snapper.data.repository_types import PnlTimelineExecutionPrefixBundle
from snapper.data.repository_types import PnlTimelineExecutionRow
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow
from snapper.data.repository_types import PnlTimelineSignalMarkerRow
from snapper.data.repository_types import PortfolioPnlAnchorRow
from snapper.data.repository_types import PortfolioPnlAnchorWriteEvidence
from snapper.data.repository_types import PortfolioPnlSampleRow

PNL_TIMELINE_MARK_SOURCE = "finalized_1m_candle_close"
"""Provenance label for the mark plane the timeline values against."""

PNL_TIMELINE_CALC_VERSION = "5A.13"
"""Reconstruction algorithm version stamped on every series response.

Bumped whenever the pool replay, decomposition, mark resolution, or public point
contract changes so a cached or persisted point can be told apart from a
re-derivation under a newer contract.
"""

PNL_SAMPLE_CALC_VERSION = _PNL_SAMPLE_CALC_VERSION
"""Re-exposed Phase-5B sample algorithm version.

The canonical definition lives in :mod:`snapper.data.repository_types` so the
data-layer sample validator compares against the real constant rather than a
caller-supplied scope echo; this module re-exposes it for the snapshotter and
overlay callers that already import from here.
"""

PNL_TIMELINE_MAX_WORK_UNITS: Final[int] = 131_040
"""Maximum minute-instrument work for one reconstruction.

The budget preserves the former 91-day allowance for one instrument while
making multi-instrument requests pay for their actual grid fan-out. Empty scopes
use a factor of one because the pure builder still materialises the raw grid.
"""

PNL_TIMELINE_MARKER_LIMIT: Final[int] = 2_000
"""Maximum markers returned, retaining the latest markers deterministically.

Marker reads request one extra row from each independently bounded decision
source. The response exposes both this limit and whether older markers were
omitted, so a busy window never looks indistinguishable from a complete one.
"""

_ANCHOR_SCHEMA_VERSION: Final[Literal[3]] = 3
"""Persisted activation payload version owned by the 5A.13 replay contract.

Bumped 2 -> 3 when the opening payload gained its ``annulments`` audit: an
anchor is permanent and its opening is derived from the EFFECTIVE prefix, so a
payload that did not record which repudiations that derivation folded cannot be
told apart from one derived before any correction existed. A v2 row therefore
fails to parse rather than being read as "no corrections applied" — the shape
change is exactly what the version exists to make loud."""

_STRICT_ANCHOR_CONFIG: Final[ConfigDict] = ConfigDict(
    extra="forbid",
    strict=True,
    frozen=True,
    allow_inf_nan=False,
)
"""Strict finite configuration shared by every persisted anchor model."""

_HEX_DIGITS: Final[frozenset[str]] = frozenset("0123456789abcdef")
"""Lowercase hex alphabet the persisted annulment row digests must use.

Pinned locally rather than imported so the anchor payload validator states its
own storage contract; the digest itself is produced by
``snapper.application.portfolio.execution_chain.execution_row_digest``."""


class _AnchorPoolPayload(BaseModel):
    """Raw historical valuation audit for one surviving opening shard."""

    model_config = _STRICT_ANCHOR_CONFIG

    instrument_public_id: str
    shard_key: str
    exchange: str
    position_qty: float
    historical_entry_price: float
    t0_mark: float
    opening_unrealized_value: float

    @field_validator("instrument_public_id", "shard_key")
    @classmethod
    def _nonempty_identity(cls, value: str) -> str:
        """Require canonical nonempty identities without hidden whitespace."""
        if not value or value != value.strip():
            raise ValueError("anchor pool identities must be nonempty and canonical")
        return value

    @field_validator("exchange")
    @classmethod
    def _canonical_exchange(cls, value: str) -> str:
        """Require the repository's canonical lowercase exchange identity."""
        if not value or value != value.strip().lower():
            raise ValueError("anchor pool exchange must be canonical lowercase")
        return value

    @field_validator("position_qty")
    @classmethod
    def _nonflat_quantity(cls, value: float) -> float:
        """Persist only genuinely surviving non-flat pools."""
        if abs(value) < FLAT_EPSILON:
            raise ValueError("anchor pool quantity must be non-flat")
        return value

    @field_validator("historical_entry_price", "t0_mark")
    @classmethod
    def _positive_price(cls, value: float) -> float:
        """Require positive finite historical basis and t0 mark."""
        if not is_positive_finite(value):
            raise ValueError("anchor pool prices must be positive and finite")
        return value


class _AnchorAnnulmentPayload(BaseModel):
    """One repudiation the anchor's opening derivation provably folded.

    Recorded as the pair that IDENTIFIES a correction — the manifest row's own
    public id and the target execution's immutable id — plus the canonical row
    digest that says which row CONTENT was repudiated. Together they let an
    auditor reconstruct, from the anchor alone, exactly why its opening differs
    from a naive replay of the raw ledger, without trusting the manifest to
    still say the same thing later.
    """

    model_config = _STRICT_ANCHOR_CONFIG

    public_id: str
    target_execution_public_id: str
    target_execution_digest: str
    exchange: str
    scope_sequence: int

    @field_validator("public_id", "target_execution_public_id")
    @classmethod
    def _canonical_uuid(cls, value: str) -> str:
        """Require canonical UUID identities for both correction endpoints."""
        try:
            canonical = str(UUID(value))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("anchor annulment identities must be UUIDs") from exc
        if value != canonical:
            raise ValueError("anchor annulment identities must be canonical")
        return value

    @field_validator("target_execution_digest")
    @classmethod
    def _canonical_digest(cls, value: str) -> str:
        """Require the manifest's lowercase 64-hex canonical row digest."""
        if len(value) != 64 or value != value.lower() or not set(value) <= _HEX_DIGITS:
            raise ValueError("anchor annulment digest must be 64 lowercase hex characters")
        return value

    @field_validator("exchange")
    @classmethod
    def _canonical_exchange(cls, value: str) -> str:
        """Require the repository's canonical lowercase exchange identity."""
        if not value or value != value.strip().lower():
            raise ValueError("anchor annulment exchange must be canonical lowercase")
        return value

    @field_validator("scope_sequence")
    @classmethod
    def _positive_sequence(cls, value: int) -> int:
        """Require a real per-scope commit-order coordinate."""
        if isinstance(value, bool) or value < 1:
            raise ValueError("anchor annulment scope_sequence must be positive")
        return value


class _AnchorOpeningPayload(BaseModel):
    """Canonical v3 opening audit, inventory basket, and folded corrections.

    ``annulments`` is empty for the overwhelmingly common case of an opening
    derived from an uncorrected ledger, and non-empty exactly when the sealed
    prefix excluded a repudiated booking. It is part of the OPENING payload
    rather than a sibling because it is a property of this derivation: the same
    scope certified at a horizon that did not yet know the correction derives a
    different opening, and the anchor must be able to say which one it is.
    """

    model_config = _STRICT_ANCHOR_CONFIG

    schema_version: Literal[3]
    pools: tuple[_AnchorPoolPayload, ...]
    native_basket: dict[str, float]
    annulments: tuple[_AnchorAnnulmentPayload, ...]


class _AnchorWeightPayload(BaseModel):
    """One legacy unattributed quantity weight for an opening shard."""

    model_config = _STRICT_ANCHOR_CONFIG

    origin: Literal["unattributed"]
    strategy_name: None
    quantity: float

    @field_validator("quantity")
    @classmethod
    def _positive_quantity(cls, value: float) -> float:
        """Require a real positive inventory weight."""
        if value <= 0.0:
            raise ValueError("anchor contribution quantity must be positive")
        return value


class _AnchorContributionPoolPayload(BaseModel):
    """Legacy attribution seed for one exact opening shard."""

    model_config = _STRICT_ANCHOR_CONFIG

    instrument_public_id: str
    shard_key: str
    exchange: str
    weights: tuple[_AnchorWeightPayload, ...]

    @field_validator("instrument_public_id", "shard_key")
    @classmethod
    def _nonempty_identity(cls, value: str) -> str:
        """Require canonical nonempty identities without hidden whitespace."""
        if not value or value != value.strip():
            raise ValueError("anchor contribution identities must be nonempty and canonical")
        return value

    @field_validator("exchange")
    @classmethod
    def _canonical_exchange(cls, value: str) -> str:
        """Require the repository's canonical lowercase exchange identity."""
        if not value or value != value.strip().lower():
            raise ValueError("anchor contribution exchange must be canonical lowercase")
        return value


class _AnchorContributionsPayload(BaseModel):
    """Canonical v3 legacy attribution seeds for every opening shard."""

    model_config = _STRICT_ANCHOR_CONFIG

    schema_version: Literal[3]
    pools: tuple[_AnchorContributionPoolPayload, ...]


_WATERMARKS_ADAPTER: Final[TypeAdapter[dict[str, int]]] = TypeAdapter(
    dict[str, int],
    config=ConfigDict(strict=True),
)
"""Strict adapter for the canonical per-exchange execution prefix map."""


@dataclass(frozen=True, slots=True)
class _ResolvedPnlAnchor:
    """Validated rebased opening, frozen t0 marks, and replay watermarks.

    ``annulments`` carries the corrections the persisted opening audit says this
    anchor's derivation folded. It travels with the anchor rather than being
    re-read because it is the only surviving statement of WHICH effective
    history produced these frozen numbers, and every later read has to be able
    to check that the ledger still agrees with it.
    """

    row: PortfolioPnlAnchorRow
    opening: TimelineOpening
    marks: dict[tuple[str, datetime], float]
    watermarks: dict[str, int]
    annulments: tuple[_AnchorAnnulmentPayload, ...]


@dataclass(frozen=True, slots=True)
class _AnchorScope:
    """Canonical activation scope and the exact evidence horizons it uses.

    ``requested_horizon`` is the caller's ORIGINAL horizon argument, and it is
    the exact value handed to every repository read: ``None`` when no horizon
    was requested, otherwise the instant that was. ``knowledge_horizon`` is the
    resolved instant everything else (candles, FX, accruals) needs, so the
    invariant every constructor establishes is ``requested_horizon in (None,
    knowledge_horizon)``. Carrying the nullable instant rather than a flag
    beside it is what keeps "a past horizon exempt from the durability proof"
    unspellable on the way down to the repository.
    """

    wallet_public_id: str
    mode: str
    valuation_ccy: str
    activation_time: datetime
    knowledge_horizon: datetime
    requested_horizon: datetime | None


@dataclass(frozen=True, slots=True)
class _SeriesReadEvidence:
    """One preloaded anchor read and its exact two-cut prefix bundle."""

    visible_anchor: PortfolioPnlAnchorRow | None
    execution_prefix_bundle: PnlTimelineExecutionPrefixBundle


@dataclass(frozen=True, slots=True)
class PnlSeriesReplayOptions:
    """Optional replay inputs for :func:`build_wallet_pnl_series` (D3).

    Bundles the builder's two independent optional replay knobs into one argument
    so its parameter surface stays small: the marker endpoint's preloaded anchor
    read and prefix bundle (avoiding a duplicate durable boundary read), and the
    snapshotter's per-exchange baseline watermark map that turns on R3 late-fill
    replay metadata. Both default to absent, which reproduces the plain series
    build with no preloaded evidence and no replay metadata.
    """

    preloaded_evidence: _SeriesReadEvidence | None = None
    baseline_watermarks: Mapping[str, int] | None = None


_NO_SERIES_REPLAY_OPTIONS: Final[PnlSeriesReplayOptions] = PnlSeriesReplayOptions()
"""Shared empty replay options: no preloaded evidence and no replay metadata.

Used as the immutable default for :func:`build_wallet_pnl_series` so a caller that
supplies neither knob shares one frozen instance instead of allocating per call."""


@dataclass(frozen=True, slots=True)
class PnlSeriesReadPolicy:
    """Separates anchor-mutation permission from money-disclosure eligibility (B1).

    ``allow_anchor_creation`` grants a live request permission to persist a missing
    activation anchor; it is an anchor-mutation capability only. ``current_truth``
    is the independent, safe-by-default OFF capability that alone unlocks the
    Phase-5B observed-equity overlay: the routes set it to ``as_of is None`` (a
    live current-truth read), and the snapshotter and any other internal caller
    leave it ``False`` so present-epoch equity is never attached to a historical or
    forward-only result. The two are deliberately distinct: granting anchor
    creation must never, by itself, disclose present-epoch money.

    ``current_truth_horizon`` is a third, again independent, statement: that the
    caller requested NO knowledge horizon and is asking about whatever is
    durable now. It decides how the annulment manifest is narrowed (see
    ``snapper.data.repository.annulment_knowledge_bound``) and nothing else. It
    is NOT the same fact as ``current_truth`` even though the routes derive both
    from ``as_of is None``: the snapshotter reads at its own unrequested tick —
    a current-truth HORIZON — while deliberately leaving the money-disclosure
    grant off. Conflating them would either leak present-epoch equity into the
    snapshotter's results or make it answer as though it had named a past
    instant. The default is the conservative one: an explicit horizon.
    """

    allow_anchor_creation: bool = True
    current_truth: bool = False
    current_truth_horizon: bool = False


_DEFAULT_READ_POLICY: Final[PnlSeriesReadPolicy] = PnlSeriesReadPolicy()
"""Shared default read policy: anchor creation permitted, overlay disabled.

The overlay-disabled default makes the observed-equity disclosure opt-in, so a
caller that never sets ``current_truth`` (the snapshotter, any historical read)
can never leak present-epoch equity onto its result."""


@dataclass(frozen=True, slots=True)
class _AnchorLoadRequest:
    """One anchor boundary with optional preloaded request evidence."""

    scope: _AnchorScope
    allow_anchor_creation: bool
    preloaded_evidence: _SeriesReadEvidence | None


@dataclass(frozen=True, slots=True)
class _AnchorLoadResult:
    """The visible anchor, its validated scope, and any prefix already captured.

    ``scope`` is the NORMALIZED scope the anchor was actually loaded or created
    against, returned so the series replay reuses the exact horizons and horizon
    INTENT the anchor boundary used rather than re-deriving them from raw
    arguments and risking a different manifest narrowing.
    """

    anchor: _ResolvedPnlAnchor | None
    scope: _AnchorScope
    execution_prefix_bundle: PnlTimelineExecutionPrefixBundle | None


@dataclass(frozen=True, slots=True)
class _SeriesReplayInputs:
    """Bounded suffix rows, lineage, accruals, and global evidence reasons.

    ``applied_annulments`` is read straight off the certified prefix this replay
    consumed, so the series can disclose exactly the corrections its own fold
    applied rather than a set some later read might compute differently.
    """

    loaded_execution_rows: list[PnlTimelineOpeningExecutionRow]
    replayed_execution_rows: list[PnlTimelineOpeningExecutionRow]
    accrual_rows: list[PnlTimelineAccrualRow]
    lineage: dict[str, TimelineExecutionLineage]
    fill_gap_reason: PnlIncompletenessReasonEntry | None
    late_pre_activation_reason: PnlIncompletenessReasonEntry | None
    applied_annulments: tuple[PnlTimelineAppliedAnnulment, ...]


class PnlAnchorEvidenceError(ValueError):
    """The durable evidence plane cannot support a truthful activation anchor."""


def _canonical_anchor_json(model: BaseModel) -> str:
    """Serialize one validated anchor payload deterministically and finitely."""
    return json.dumps(
        model.model_dump(mode="json"),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _canonical_watermarks(watermarks: Mapping[str, int]) -> str:
    """Serialize one validated watermark map in canonical key order."""
    return json.dumps(
        dict(watermarks),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _watermarks_are_valid(watermarks: Mapping[str, int]) -> bool:
    """Return whether every present exchange has a positive canonical sequence."""
    return all(
        bool(exchange)
        and exchange == exchange.strip().lower()
        and not isinstance(sequence, bool)
        and sequence > 0
        for exchange, sequence in watermarks.items()
    )


def _anchor_annulment_payloads(
    annulments: Sequence[PnlTimelineAppliedAnnulment],
) -> tuple[_AnchorAnnulmentPayload, ...]:
    """Project the sealed prefix's applied corrections into the anchor audit.

    Stably ordered by scope coordinate so the persisted canonical JSON is a
    function of WHICH corrections applied and never of the order a read
    happened to return them in.

    Args:
        annulments: The corrections the certified prefix proved and applied.

    Returns:
        The canonical anchor annulment audit entries.
    """
    return tuple(
        _AnchorAnnulmentPayload(
            public_id=annulment["public_id"],
            target_execution_public_id=annulment["target_execution_public_id"],
            target_execution_digest=annulment["target_execution_digest"],
            exchange=annulment["exchange"],
            scope_sequence=annulment["scope_sequence"],
        )
        for annulment in sorted(
            annulments,
            key=lambda row: (row["exchange"], row["scope_sequence"]),
        )
    )


def _anchor_payloads(
    derivation: TimelineOpeningDerivation,
    annulments: Sequence[PnlTimelineAppliedAnnulment],
) -> tuple[_AnchorOpeningPayload, _AnchorContributionsPayload]:
    """Build canonical v3 persisted payloads from a pure opening derivation.

    Args:
        derivation: The pure opening derivation this anchor seeds from.
        annulments: The corrections the sealed activation prefix applied, so
            the permanent anchor records what its opening actually folded.

    Returns:
        The canonical opening and contribution payloads.
    """
    pools = tuple(
        _AnchorPoolPayload(
            instrument_public_id=valuation.instrument_public_id,
            shard_key=valuation.shard_key,
            exchange=valuation.exchange,
            position_qty=valuation.position_qty,
            historical_entry_price=valuation.historical_entry_price,
            t0_mark=valuation.t0_mark,
            opening_unrealized_value=valuation.opening_unrealized_value,
        )
        for valuation in derivation.per_pool
    )
    quantities_by_instrument: dict[str, list[float]] = {}
    for pool in pools:
        quantities_by_instrument.setdefault(pool.instrument_public_id, []).append(pool.position_qty)
    native_basket = {
        instrument_public_id: math.fsum(quantities_by_instrument[instrument_public_id])
        for instrument_public_id in sorted(quantities_by_instrument)
    }
    opening = _AnchorOpeningPayload(
        schema_version=_ANCHOR_SCHEMA_VERSION,
        pools=pools,
        native_basket=native_basket,
        annulments=_anchor_annulment_payloads(annulments),
    )
    contributions = _AnchorContributionsPayload(
        schema_version=_ANCHOR_SCHEMA_VERSION,
        pools=tuple(
            _AnchorContributionPoolPayload(
                instrument_public_id=pool.instrument_public_id,
                shard_key=pool.shard_key,
                exchange=pool.exchange,
                weights=(
                    _AnchorWeightPayload(
                        origin="unattributed",
                        strategy_name=None,
                        quantity=abs(pool.position_qty),
                    ),
                ),
            )
            for pool in pools
        ),
    )
    return opening, contributions


def _require_uuid(value: str, field_name: str) -> None:
    """Require one canonical UUID string in persisted anchor metadata."""
    try:
        canonical = str(UUID(value))
    except (AttributeError, TypeError, ValueError) as exc:
        raise PnlAnchorEvidenceError(f"anchor {field_name} must be a UUID") from exc
    if value != canonical:
        raise PnlAnchorEvidenceError(f"anchor {field_name} must be canonical")


def _validated_anchor_raw_unrealized(row: PortfolioPnlAnchorRow) -> float:
    """Validate canonical row metadata and return its finite raw audit value."""
    try:
        normalized_ccy = normalize_portfolio_pnl_valuation_ccy(row["valuation_ccy"])
        expected_public_id = portfolio_pnl_anchor_public_id(
            row["wallet_public_id"],
            row["mode"],
            normalized_ccy,
        )
    except ValueError as exc:
        raise PnlAnchorEvidenceError("anchor scope identity is invalid") from exc
    if (
        row["valuation_ccy"] != normalized_ccy
        or row["public_id"] != expected_public_id
        or row["epoch_public_id"] != row["public_id"]
        or row["point_kind"] != "anchor"
        or row["calc_version"] != PNL_TIMELINE_CALC_VERSION
        or row["valuation_status"] != "complete"
        or type(row["realized_pnl"]) is not float
        or not _is_exact_zero(row["realized_pnl"])
        or type(row["fee_pnl"]) is not float
        or not _is_exact_zero(row["fee_pnl"])
        or type(row["accrual_pnl"]) is not float
        or not _is_exact_zero(row["accrual_pnl"])
        or type(row["external_flow_adjustment"]) is not float
        or not _is_exact_zero(row["external_flow_adjustment"])
        or row["cash_usd"] is not None
        or row["position_value_usd"] is not None
        or row["drawdown"] is not None
        or row["mark_source"] != PNL_TIMELINE_MARK_SOURCE
        or row["mark_time"] != row["point_time"]
        or row["watermarks_json"] is None
        or row["opening_basket_json"] is None
        or row["contributions_json"] is None
    ):
        raise PnlAnchorEvidenceError("anchor row metadata is not canonical for 5A.13")
    _require_uuid(row["public_id"], "public_id")
    _require_uuid(row["session_id"], "session_id")
    _require_uuid(row["epoch_public_id"], "epoch_public_id")
    if isinstance(row["sequence_id"], bool) or row["sequence_id"] <= 0:
        raise PnlAnchorEvidenceError("anchor sequence_id must be positive")
    if (
        row["point_time"].utcoffset() != timedelta(0)
        or row["point_time"].second != 0
        or row["point_time"].microsecond != 0
        or row["timestamp"].utcoffset() != timedelta(0)
        or row["timestamp"] < row["point_time"]
    ):
        raise PnlAnchorEvidenceError("anchor timestamps must form a canonical UTC activation cut")
    raw_unrealized = row["unrealized_pnl"]
    if not isinstance(raw_unrealized, float) or not math.isfinite(raw_unrealized):
        raise PnlAnchorEvidenceError("anchor raw opening unrealized audit must be finite")
    return raw_unrealized


def _decode_anchor_payloads(
    row: PortfolioPnlAnchorRow,
) -> tuple[_AnchorOpeningPayload, _AnchorContributionsPayload, dict[str, int]]:
    """Strictly decode canonical JSON payloads from one validated anchor row."""
    try:
        opening_payload = _AnchorOpeningPayload.model_validate_json(
            cast(str, row["opening_basket_json"]),
            strict=True,
        )
        contributions_payload = _AnchorContributionsPayload.model_validate_json(
            cast(str, row["contributions_json"]),
            strict=True,
        )
        watermarks = _WATERMARKS_ADAPTER.validate_json(
            cast(str, row["watermarks_json"]),
            strict=True,
        )
    except ValidationError as exc:
        raise PnlAnchorEvidenceError("anchor payload validation failed") from exc
    if (
        not _watermarks_are_valid(watermarks)
        or row["watermarks_json"] != _canonical_watermarks(watermarks)
        or row["opening_basket_json"] != _canonical_anchor_json(opening_payload)
        or row["contributions_json"] != _canonical_anchor_json(contributions_payload)
    ):
        raise PnlAnchorEvidenceError("anchor payload JSON is not canonical")
    return opening_payload, contributions_payload, watermarks


def _validate_anchor_annulment_topology(opening_payload: _AnchorOpeningPayload) -> None:
    """Require stably ordered, uniquely targeted corrections in one anchor audit.

    Mirrors the manifest's own TOTAL uniqueness on both the target execution
    and its scope slot, so a persisted anchor that claims to have folded two
    corrections of one booking is refused rather than believed.

    Args:
        opening_payload: The decoded v3 opening audit.

    Raises:
        PnlAnchorEvidenceError: If the audit is unordered or not uniquely
            targeted.
    """
    annulments = opening_payload.annulments
    expected_order = tuple(sorted(annulments, key=lambda row: (row.exchange, row.scope_sequence)))
    if annulments != expected_order:
        raise PnlAnchorEvidenceError("anchor annulments are not stably ordered")
    targets = [row.target_execution_public_id for row in annulments]
    coordinates = [(row.exchange, row.scope_sequence) for row in annulments]
    if len(set(targets)) != len(targets) or len(set(coordinates)) != len(coordinates):
        raise PnlAnchorEvidenceError("anchor annulments are not uniquely targeted")


def _validate_anchor_payload_topology(
    opening_payload: _AnchorOpeningPayload,
    contributions_payload: _AnchorContributionsPayload,
) -> None:
    """Require stable unique opening pools and matching contribution identities."""
    _validate_anchor_annulment_topology(opening_payload)
    expected_pool_order = tuple(
        sorted(
            opening_payload.pools,
            key=lambda pool: (pool.instrument_public_id, pool.shard_key),
        )
    )
    if opening_payload.pools != expected_pool_order:
        raise PnlAnchorEvidenceError("anchor pools are not stably ordered")
    opening_keys = [(pool.instrument_public_id, pool.shard_key) for pool in opening_payload.pools]
    if len(opening_keys) != len(set(opening_keys)):
        raise PnlAnchorEvidenceError("anchor pools are not unique")
    contribution_keys = [
        (pool.instrument_public_id, pool.shard_key) for pool in contributions_payload.pools
    ]
    if contribution_keys != opening_keys:
        raise PnlAnchorEvidenceError("anchor contribution pools do not match opening pools")


def _opening_pool_from_payload(
    pool: _AnchorPoolPayload,
    contribution: _AnchorContributionPoolPayload,
) -> OpeningPool:
    """Validate one pool's arithmetic and return its rebased kernel seed."""
    expected_unrealized = pool.position_qty * (pool.t0_mark - pool.historical_entry_price)
    if (
        not math.isfinite(expected_unrealized)
        or expected_unrealized != pool.opening_unrealized_value
        or contribution.exchange != pool.exchange
        or len(contribution.weights) != 1
        or contribution.weights[0].origin != "unattributed"
        or contribution.weights[0].strategy_name is not None
        or contribution.weights[0].quantity != abs(pool.position_qty)
    ):
        raise PnlAnchorEvidenceError("anchor pool arithmetic or contribution is inconsistent")
    return OpeningPool(
        instrument_public_id=pool.instrument_public_id,
        shard_key=pool.shard_key,
        exchange=pool.exchange,
        position_qty=pool.position_qty,
        entry_price=pool.t0_mark,
    )


def _resolve_anchor_opening(
    row: PortfolioPnlAnchorRow,
    opening_payload: _AnchorOpeningPayload,
    contributions_payload: _AnchorContributionsPayload,
    raw_unrealized: float,
) -> tuple[TimelineOpening, dict[tuple[str, datetime], float]]:
    """Reconcile native/raw audits and rebuild rebased pools plus frozen marks."""
    quantities_by_instrument: dict[str, list[float]] = {}
    opening_pools: list[OpeningPool] = []
    frozen_marks: dict[tuple[str, datetime], float] = {}
    raw_values: list[float] = []
    for pool, contribution in zip(
        opening_payload.pools,
        contributions_payload.pools,
        strict=True,
    ):
        opening_pool = _opening_pool_from_payload(pool, contribution)
        quantities_by_instrument.setdefault(pool.instrument_public_id, []).append(pool.position_qty)
        mark_key = (pool.instrument_public_id, row["point_time"])
        previous_mark = frozen_marks.setdefault(mark_key, pool.t0_mark)
        if previous_mark != pool.t0_mark:
            raise PnlAnchorEvidenceError("one anchor instrument carries conflicting t0 marks")
        opening_pools.append(opening_pool)
        raw_values.append(pool.opening_unrealized_value)
    try:
        expected_basket = {
            instrument_public_id: math.fsum(quantities_by_instrument[instrument_public_id])
            for instrument_public_id in sorted(quantities_by_instrument)
        }
    except OverflowError as exc:
        raise PnlAnchorEvidenceError("anchor native basket audit overflows") from exc
    if opening_payload.native_basket != expected_basket:
        raise PnlAnchorEvidenceError("anchor native basket does not reconcile with its pools")
    try:
        expected_raw_unrealized = math.fsum(raw_values)
    except OverflowError as exc:
        raise PnlAnchorEvidenceError("anchor raw opening audit overflows") from exc
    if expected_raw_unrealized != raw_unrealized:
        raise PnlAnchorEvidenceError("anchor raw opening audit does not reconcile")
    return (
        TimelineOpening(
            pools=tuple(opening_pools),
            t0=row["point_time"],
        ),
        frozen_marks,
    )


def _parse_anchor(row: PortfolioPnlAnchorRow) -> _ResolvedPnlAnchor:
    """Validate one persisted v2 anchor and rebuild its rebased kernel seed."""
    raw_unrealized = _validated_anchor_raw_unrealized(row)
    opening_payload, contributions_payload, watermarks = _decode_anchor_payloads(row)
    _validate_anchor_payload_topology(opening_payload, contributions_payload)
    opening, frozen_marks = _resolve_anchor_opening(
        row,
        opening_payload,
        contributions_payload,
        raw_unrealized,
    )
    return _ResolvedPnlAnchor(
        row=row,
        opening=opening,
        marks=frozen_marks,
        watermarks=dict(watermarks),
        annulments=opening_payload.annulments,
    )


def _parse_scoped_anchor(
    row: PortfolioPnlAnchorRow,
    wallet_public_id: str,
    mode: str,
    valuation_ccy: str,
) -> _ResolvedPnlAnchor:
    """Parse an anchor and require it to belong to the requested scope."""
    parsed = _parse_anchor(row)
    if (
        row["wallet_public_id"] != wallet_public_id
        or row["mode"] != mode
        or row["valuation_ccy"] != valuation_ccy
    ):
        raise PnlAnchorEvidenceError("visible anchor crossed the requested scope")
    return parsed


@dataclass(frozen=True, slots=True)
class PnlFillMarker:
    """One immutable execution projected as an executed timeline marker."""

    marker_time: datetime
    instrument_public_id: str
    side: str
    size: float
    price: float | None
    execution_public_id: str
    order_public_id: str
    status: str
    kind: Literal["fill"] = field(default="fill", init=False)
    outcome: Literal["executed"] = field(default="executed", init=False)


@dataclass(frozen=True, slots=True)
class PnlSignalMarker:
    """One source signal with its independently established fill outcome."""

    marker_time: datetime
    instrument_public_id: str
    side: str
    strategy_name: str | None
    strength: float
    reason: str
    price: float | None
    signal_public_id: str
    outcome: Literal["executed", "no_fill"]
    status: Literal["executed", "no_fill"]
    kind: Literal["signal"] = field(default="signal", init=False)


@dataclass(frozen=True, slots=True)
class PnlAiDecisionMarker:
    """One append-only AI decision event and its observable outcome."""

    marker_time: datetime
    instrument_public_id: str
    strategy_public_id: str
    review_public_id: str
    event_public_id: str
    decision: str | None
    rationale: str | None
    outcome: Literal["executed", "rejected", "no_fill"]
    status: str
    kind: Literal["ai_decision"] = field(default="ai_decision", init=False)


type PnlTimelineMarker = PnlFillMarker | PnlSignalMarker | PnlAiDecisionMarker
"""Service-layer marker union emitted in chronological chart order."""


@dataclass(frozen=True, slots=True)
class PnlFxRateSource:
    """One used FX plane expressed with conversion provenance."""

    source_currency: str
    valuation_currency: str
    base_currency: str
    quote_currency: str
    exchange: str


class _SampleCoverageAudit(BaseModel):
    """The venue-scope disclosure parsed from one persisted sample's audit JSON."""

    model_config = ConfigDict(extra="ignore", strict=True)

    venue_scope: str
    external_flows_adjusted: bool


class _SampleAuditEnvelope(BaseModel):
    """The minimal projection of a sample's audit JSON the overlay reads back."""

    model_config = ConfigDict(extra="ignore", strict=True)

    coverage: _SampleCoverageAudit


@dataclass(frozen=True, slots=True)
class PnlPointEquityOverlay:
    """The persisted observed-equity stocks overlaid on one series point (D13).

    Every field is populated together from one ``complete`` sample or is ``None``
    together when the point's minute has no qualifying sample. ``equity`` is
    ``cash + position_value`` computed from the SAME persisted floats the sample
    stored, never an independent recompute.
    """

    equity: float | None
    cash: float | None
    position_value: float | None
    drawdown: float | None


@dataclass(frozen=True, slots=True)
class PnlEquityCoverage:
    """Envelope-level disclosure of the observed-equity overlay (D13/R10/R11).

    ``sampled`` is ``True`` only for a current-truth USD scope whose window is
    backed by at least one ``complete`` current-epoch sample; every other scope
    and every fail-closed withholding reports ``sampled=False`` with null/zero
    provenance. ``venue_scope`` and ``external_flows_adjusted`` are surfaced from
    the samples' uniform coverage blocks; ``complete_minutes`` / ``first_minute``
    / ``last_minute`` describe the ``complete`` sample minutes in the requested
    window; ``sample_calc_version`` is the exact backing sample version.
    """

    sampled: bool
    venue_scope: Literal["spot_only"] | None
    external_flows_adjusted: bool | None
    complete_minutes: int
    first_minute: datetime | None
    last_minute: datetime | None
    sample_calc_version: str | None


@dataclass(frozen=True, slots=True)
class _EquityOverlayRequest:
    """The inputs one series computation hands the overlay resolver.

    ``current_truth`` is the caller's explicit, safe-by-default OFF money-disclosure
    grant (``PnlSeriesReadPolicy.current_truth``), which the routes set to ``as_of
    is None`` (a live current-truth read) and which every historical, snapshotter,
    or other internal caller leaves ``False``. It is deliberately decoupled from
    anchor-creation permission, so granting anchor creation never, by itself,
    resolves an overlay.
    """

    wallet_public_id: str
    mode: str
    valuation_ccy: str
    epoch_public_id: str
    from_time: datetime
    to_time: datetime
    current_truth: bool
    points: tuple[PnlTimelinePoint, ...]


@dataclass(frozen=True, slots=True)
class _EquityOverlayResult:
    """The resolved per-minute overlay map and its envelope disclosure."""

    overlay: Mapping[datetime, PnlPointEquityOverlay]
    coverage: PnlEquityCoverage


_EMPTY_EQUITY_OVERLAY: Final[PnlPointEquityOverlay] = PnlPointEquityOverlay(
    equity=None,
    cash=None,
    position_value=None,
    drawdown=None,
)
"""The null overlay served for any point with no qualifying sample."""

_EMPTY_EQUITY_OVERLAY_MAP: Final[Mapping[datetime, PnlPointEquityOverlay]] = MappingProxyType({})
"""Shared immutable empty overlay map for unsampled series results."""

_UNSAMPLED_EQUITY_COVERAGE: Final[PnlEquityCoverage] = PnlEquityCoverage(
    sampled=False,
    venue_scope=None,
    external_flows_adjusted=None,
    complete_minutes=0,
    first_minute=None,
    last_minute=None,
    sample_calc_version=None,
)
"""The disclosure for every unsampled or fail-closed overlay resolution."""

_UNSAMPLED_EQUITY_OVERLAY_RESULT: Final[_EquityOverlayResult] = _EquityOverlayResult(
    overlay=_EMPTY_EQUITY_OVERLAY_MAP,
    coverage=_UNSAMPLED_EQUITY_COVERAGE,
)
"""The resolver result withholding the overlay from the whole response."""


@dataclass(frozen=True)
class PnlWalletSeriesResult(PnlTimelineResult):
    """A pure P&L series augmented with attributable FX rate venues.

    ``replay_metadata`` is the optional Phase-5B late-fill boundary (R3),
    populated only when :func:`build_wallet_pnl_series` is called with a baseline
    watermark map and the scope has a visible anchor with a post-t0 window; it
    stays ``None`` for every existing caller so their result shape is unchanged.

    ``equity_overlay`` maps a point minute to its persisted observed-equity stocks
    (D13) and ``equity_coverage`` is the envelope disclosure. Both default to the
    unsampled shape, so a pre-activation, empty, historical, or non-USD result
    carries a null overlay without a sample read.

    ``applied_annulments`` is the series-level correction disclosure (A3): the
    exact repudiations the certified prefix behind THESE numbers folded away,
    copied from that prefix rather than re-derived. It stays empty for a result
    built without any execution evidence — no anchor, or a wholly pre-activation
    window — because such a result folded nothing and must not claim otherwise.
    """

    rate_sources: tuple[PnlFxRateSource, ...]
    replay_metadata: PnlSeriesReplayMetadata | None = None
    equity_overlay: Mapping[datetime, PnlPointEquityOverlay] = _EMPTY_EQUITY_OVERLAY_MAP
    equity_coverage: PnlEquityCoverage = _UNSAMPLED_EQUITY_COVERAGE
    applied_annulments: tuple[PnlTimelineAppliedAnnulment, ...] = ()

    def equity_overlay_at(self, point_time: datetime) -> PnlPointEquityOverlay:
        """Return the persisted equity overlay for one point, or the null overlay.

        Args:
            point_time: The exact grid minute of the point being projected.

        Returns:
            The overlay stored for that minute, or the all-null overlay when the
            minute has no qualifying sample.
        """
        return self.equity_overlay.get(point_time, _EMPTY_EQUITY_OVERLAY)


@dataclass(frozen=True, slots=True)
class PnlWalletTimelineResult:
    """One reconstructed series with a bounded marker overlay.

    ``applied_annulments`` is the disclosure seam for the series-level
    correction channel: it names, without any re-derivation, exactly which
    repudiations the certified prefix behind THIS result folded. It is carried
    here rather than recomputed downstream because only the fold that excluded
    a booking can honestly say it did — a later independent read could see a
    different manifest and disagree with the numbers already returned. The
    transport-level disclosure that consumes it is a separate slice; an empty
    tuple means the scope's history is uncorrected.
    """

    series: PnlWalletSeriesResult
    markers: tuple[PnlTimelineMarker, ...]
    marker_limit: int
    markers_truncated: bool
    applied_annulments: tuple[PnlTimelineAppliedAnnulment, ...] = ()


class PnlTimelineWorkBudgetError(ValueError):
    """The requested raw grid and instrument fan-out exceed the work budget."""


def _empty_pnl_series(granularity: str, valuation_ccy: str) -> PnlWalletSeriesResult:
    """Return the honest no-anchor result without inventing pre-activation points."""
    if granularity not in {"1m", "5m", "1h", "1d"}:
        raise ValueError(f"Unsupported granularity: {granularity!r}")
    return PnlWalletSeriesResult(
        points=(),
        granularity=granularity,
        valuation_ccy=valuation_ccy,
        rate_sources=(),
    )


def _preactivation_pnl_series(
    anchor: _ResolvedPnlAnchor,
    from_time: datetime,
    to_time: datetime,
    granularity: str,
    valuation_ccy: str,
) -> PnlWalletSeriesResult:
    """Return a wholly pre-t0 grid without demanding post-activation evidence."""
    result = build_pnl_timeline(
        (),
        (),
        anchor.marks,
        TimelineWindow(
            from_time=from_time,
            to_time=to_time,
            granularity=granularity,
            valuation_ccy=valuation_ccy,
        ),
        opening=anchor.opening,
    )
    return PnlWalletSeriesResult(
        points=result.points,
        granularity=result.granularity,
        valuation_ccy=result.valuation_ccy,
        rate_sources=(),
    )


def _execution_rows_with_effective_time(
    execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
) -> list[tuple[PnlTimelineOpeningExecutionRow, datetime]]:
    """Pair each suffix execution with its per-pool monotone-clamped event time.

    The single source of the service-level clamp: each durable shard pool's
    running maximum event time is carried forward so an out-of-order row is placed
    at that maximum. Both the accounting window bound
    (:func:`_execution_rows_effective_through`) and the replay-metadata boundary
    (:func:`_derive_series_replay_metadata`) read this exact result, so the clamp
    arithmetic is written once and never duplicated.

    Args:
        execution_rows: Suffix rows in ``(exchange, scope_sequence)`` order.

    Returns:
        Each row paired with its clamped effective time, preserving input order.
    """
    last_effective: dict[tuple[str, str], datetime] = {}
    paired: list[tuple[PnlTimelineOpeningExecutionRow, datetime]] = []
    for row in execution_rows:
        pool_key = (row["instrument_public_id"], row["shard_key"])
        event_time = row["timestamp"]
        effective_time = max(last_effective.get(pool_key, event_time), event_time)
        last_effective[pool_key] = effective_time
        paired.append((row, effective_time))
    return paired


def _execution_rows_effective_through(
    execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
    to_time: datetime,
) -> list[PnlTimelineOpeningExecutionRow]:
    """Bound accounting consumers by the kernel's monotone-clamped event time."""
    return [
        row
        for row, effective_time in _execution_rows_with_effective_time(execution_rows)
        if effective_time <= to_time
    ]


@dataclass(frozen=True, slots=True)
class PnlSeriesReplayMetadata:
    """Late-fill boundary derived from one series computation's suffix (R3).

    The snapshotter passes the per-exchange baseline watermark map reflected in
    its already-persisted samples; the engine reports, without duplicating the
    clamp, which suffix executions crossed that baseline and how far the watermark
    advanced. ``max_scope_sequence_by_exchange`` is the greatest ``scope_sequence``
    per exchange whose clamped effective time is at or before the series ``to_time``
    — the watermark the samples through ``to_time`` reflect, recorded by the caller
    as the next baseline. ``earliest_affected_minute`` is the minute ceiling of the
    earliest clamped effective time among executions ABOVE the caller's baseline,
    or ``None`` when no suffix execution crosses it. The caller supersedes forward
    only when that minute is at or before its last persisted minute.
    """

    max_scope_sequence_by_exchange: dict[str, int]
    earliest_affected_minute: datetime | None


@dataclass(frozen=True, slots=True)
class _SeriesAssembly:
    """Every envelope-level disclosure wrapped around one built series.

    Bundled rather than passed separately so the assembly seam keeps a small
    parameter surface as disclosures accumulate: FX provenance, the optional
    late-fill boundary, the applied corrections, and the equity-overlay request
    all attach to the same result and are never independently derivable from it.
    """

    rate_sources: tuple[PnlFxRateSource, ...]
    replay_metadata: PnlSeriesReplayMetadata | None
    applied_annulments: tuple[PnlTimelineAppliedAnnulment, ...]
    overlay_request: _EquityOverlayRequest


def _ceil_to_minute(instant: datetime) -> datetime:
    """Return the first minute-aligned instant at or after ``instant``."""
    floored = instant.replace(second=0, microsecond=0)
    return floored if floored == instant else floored + timedelta(minutes=1)


def _derive_series_replay_metadata(
    execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
    baseline_watermarks: Mapping[str, int],
    to_time: datetime,
) -> PnlSeriesReplayMetadata:
    """Derive the late-fill boundary from the clamped suffix and one baseline.

    Reuses :func:`_execution_rows_with_effective_time` so the effective-time clamp
    is never re-implemented. An execution counts toward the advanced watermark only
    when its clamped effective time is at or before ``to_time``; it counts toward
    the earliest affected minute only when its ``scope_sequence`` exceeds the
    caller's baseline for its exchange (defaulting to zero for an unseen exchange).

    Args:
        execution_rows: The suffix rows already filtered above the anchor
            watermark, in commit order.
        baseline_watermarks: Per-exchange watermark the caller's persisted samples
            already reflect.
        to_time: Inclusive series end bounding the advanced watermark.

    Returns:
        The advanced per-exchange watermark and the earliest affected minute.
    """
    max_scope_sequence: dict[str, int] = {}
    earliest_affected: datetime | None = None
    for row, effective_time in _execution_rows_with_effective_time(execution_rows):
        exchange = row["exchange"]
        scope_sequence = row["scope_sequence"]
        if effective_time <= to_time:
            current = max_scope_sequence.get(exchange)
            if current is None or scope_sequence > current:
                max_scope_sequence[exchange] = scope_sequence
        if scope_sequence > baseline_watermarks.get(exchange, 0):
            affected_minute = _ceil_to_minute(effective_time)
            if earliest_affected is None or affected_minute < earliest_affected:
                earliest_affected = affected_minute
    return PnlSeriesReplayMetadata(
        max_scope_sequence_by_exchange=max_scope_sequence,
        earliest_affected_minute=earliest_affected,
    )


def _rate_minute(moment: datetime) -> datetime:
    """Return the grid minute whose closing bar prices a flow at ``moment``.

    Flooring to the minute selects the bar that CLOSED at that instant — the last
    finalized evidence available when the flow happened — matching the mark
    convention exactly, so a fee and the position it belongs to are valued off the
    same bar and neither can look ahead.

    Args:
        moment: Event time of the flow being converted.

    Returns:
        The minute key to look the rate up under.
    """
    return moment.replace(second=0, microsecond=0)


def _is_exact_zero(value: float) -> bool:
    """Return whether a floating-point value is exactly positive or negative zero."""
    return math.isclose(value, 0.0, rel_tol=0.0, abs_tol=0.0)


def build_fx_rates(rows: Sequence[PnlFxRateRow]) -> FxRateMap:
    """Fold FX candle rows into the plane-qualified minute rate map.

    Venue is part of the key, so cross-venue quotes remain distinct evidence. If
    two rows still conflict on the full identity, that key becomes ``NaN`` and
    conversion refuses it rather than allowing row order to elect a price. The
    caller filters the book through one consumer-pinned oriented plane per pair.

    Args:
        rows: Finalized 1m closes from ``get_pnl_fx_rate_candles``.

    Returns:
        Rate map keyed by ``(base, quote, exchange, minute)`` where the minute
        is the instant the bar closed.
    """
    rates: dict[tuple[str, str, str, datetime], float] = {}
    for row in rows:
        key = (
            row["base"],
            row["quote"],
            row["exchange"],
            row["open_at"] + timedelta(minutes=1),
        )
        existing = rates.get(key)
        if existing is not None and existing != row["close"]:
            rates[key] = math.nan
        else:
            rates[key] = row["close"]
    return rates


def _basket_fiat_grid_minutes(start: datetime, end: datetime) -> set[datetime]:
    """Return every minute-aligned grid instant in an inclusive window."""
    minutes: set[datetime] = set()
    cursor = start
    while cursor <= end:
        minutes.add(cursor)
        cursor += timedelta(minutes=1)
    return minutes


def _build_fiat_versions(rows: Sequence[PnlFxRateRow]) -> FiatVersionMap:
    """Index fiat candle version identity by ``(base, quote, exchange, minute)``.

    Deterministic last-writer-wins on a duplicate plane-minute identity; such a
    key already carries a ``NaN`` rate in :func:`build_fx_rates`, so its version
    is never consumed. The minute keys the grid instant the bar closed.

    Args:
        rows: Finalized 1m forex closes from ``get_pnl_fx_rate_candles``.

    Returns:
        Version identity keyed identically to the fiat rate map.
    """
    versions: dict[FxRateKey, CandleVersionIdentity] = {}
    for row in sorted(
        rows,
        key=lambda candle: (
            candle["base"],
            candle["quote"],
            candle["exchange"],
            candle["open_at"],
            candle["candle_id"],
        ),
    ):
        key = (row["base"], row["quote"], row["exchange"], row["open_at"] + timedelta(minutes=1))
        versions[key] = CandleVersionIdentity(
            instrument_public_id=row["instrument_public_id"],
            native_symbol=row["native_symbol"],
            candle_id=row["candle_id"],
            candle_public_id=row["candle_public_id"],
            candle_open_at=row["open_at"],
            candle_timestamp=row["candle_timestamp"],
        )
    return versions


async def load_basket_fiat_evidence(
    repo: Repository,
    currencies: frozenset[str],
    start: datetime,
    end: datetime,
    as_of: datetime,
) -> tuple[FxRateMap, FxVenueMap, FiatVersionMap]:
    """Discover, elect and load the USD fiat forex evidence valuing a basket (D6/R8).

    Every non-USD currency is requested vs USD in both orientations; the existing
    FX plane discovery, coverage election and rate fold are reused verbatim (no new
    conversion math), so a fiat leg values off the SAME finalized forex plane the
    timeline marks use, with the same deterministic tie-break (Walutomat preferred
    for a fully-covered PLN pair). A crypto currency yields no forex plane and is
    silently absent from the venue map, deferring to the crypto plane; a fiat
    currency with no usable close at a minute is absent too and the basket valuator
    fails that minute closed. Scoped to USD because v1 samples only USD scopes (D3).

    Args:
        repo: Repository providing forex plane discovery and candle reads.
        currencies: Distinct basket currencies to price.
        start: Inclusive lower grid minute of the chunk.
        end: Inclusive upper grid minute of the chunk.
        as_of: Knowledge horizon threading every considered instrument, symbol and
            candle version.

    Returns:
        The fiat rate map, the pinned oriented plane per pair, and the parallel
        version identity map — exactly the three fields
        :class:`ValuationEvidence` consumes.
    """
    minutes = _basket_fiat_grid_minutes(start, end)
    requirements: dict[FxPairKey, set[datetime]] = {
        currency_pair_key(currency, "USD"): set(minutes)
        for currency in currencies
        if currency and currency != "USD"
    }
    if not requirements:
        return {}, {}, {}
    candidates = await _discover_fx_candidates(repo, requirements, as_of)
    candidate_planes = _candidate_planes(candidates, {})
    rows = await _load_fx_candidate_rows(repo, requirements, candidate_planes, as_of)
    return (
        build_fx_rates(rows),
        _resolve_fx_planes(requirements, candidate_planes, rows, "USD"),
        _build_fiat_versions(rows),
    )


def _to_timeline_execution(
    row: PnlTimelineOpeningExecutionRow,
    valuation_ccy: str,
    rates: FxRateMap,
    base_asset: str | None = None,
    price_currency: str | None = None,
    venues: FxVenueMap | None = None,
) -> TimelineExecution:
    """Map a repository execution row onto the pure builder's input.

    A positive execution price with a proven ``price_currency`` is converted at
    the fill's exact rate minute before entering the average-cost pool. A missing
    rate becomes ``NaN`` so the builder chooses its existing weakest honest tier.
    Raw non-positive or non-finite prices pass through unchanged so the D1 guard
    remains authoritative. Exact-zero fees pass through regardless of asset; a
    nonzero foreign fee is converted at the same event minute or becomes
    ``NaN``. ``event_time`` is the row's ``timestamp`` axis, never nullable
    ``executed_at``.

    Args:
        row: One ``get_pnl_timeline_executions`` row.
        valuation_ccy: Currency the series is valued in.
        rates: Minute-keyed FX rates used for price and fee conversion.
        base_asset: Proven base asset of the execution instrument.
        price_currency: Proven quote currency of the execution price. ``None``
            leaves the raw price unchanged for callers that enforce trust
            separately.
        venues: Instrument-pinned oriented plane for each converted currency pair.

    Returns:
        The equivalent :class:`TimelineExecution`.
    """
    rate_minute = _rate_minute(row["timestamp"])
    converted_fee = convert_amount(
        row["fee"],
        row["fee_asset"],
        valuation_ccy,
        rate_minute,
        rates,
        venues,
    )
    fee = math.nan if converted_fee is None else converted_fee
    fee_incompleteness_reason: PnlIncompletenessReason | None = (
        "fx_conversion_unproven"
        if math.isfinite(row["fee"])
        and not _is_exact_zero(row["fee"])
        and row["fee_asset"] != valuation_ccy
        and converted_fee is None
        else None
    )
    raw_price = row["price"]
    price = raw_price
    price_incompleteness_reason: PnlIncompletenessReason | None = None
    if price_currency is not None and is_positive_finite(raw_price):
        converted_price = convert_amount(
            raw_price,
            price_currency,
            valuation_ccy,
            rate_minute,
            rates,
            venues,
        )
        price = converted_price if is_positive_finite(converted_price) else math.nan
        if price_currency != valuation_ccy and not is_positive_finite(converted_price):
            price_incompleteness_reason = "fx_conversion_unproven"
    return TimelineExecution(
        order_public_id=row["order_public_id"],
        instrument_public_id=row["instrument_public_id"],
        shard_key=row["shard_key"],
        exchange=row["exchange"],
        scope_sequence=row["scope_sequence"],
        event_time=row["timestamp"],
        side=row["side"],
        size=row["size"],
        position_delta=booked_signed_quantity(
            row["side"],
            row["size"],
            row["fee"],
            row["fee_asset"],
            base_asset or "",
            resolve_position_quantity_unit(row["exchange"]),
        ),
        price=price,
        fee=fee,
        fee_asset=row["fee_asset"],
        price_incompleteness_reason=price_incompleteness_reason,
        fee_incompleteness_reason=fee_incompleteness_reason,
    )


def _build_execution_lineage(
    rows: Sequence[PnlTimelineExecutionLineageRow],
) -> dict[str, TimelineExecutionLineage]:
    """Build a fail-closed order-lineage lookup from repository candidates.

    Exactly one row is required for an order to carry resolved lineage into the
    pure engine. Repeated rows are ambiguous even when their projected values
    happen to match, so the order is removed from the lookup instead of choosing
    a winner. A missing map entry is intentionally classified as unattributed by
    the builder.

    Args:
        rows: Candidate initiating-command lineage rows keyed by order public id.

    Returns:
        Unique lineage keyed by order public id, excluding every duplicate key.
    """
    lineage: dict[str, TimelineExecutionLineage] = {}
    seen: set[str] = set()
    ambiguous: set[str] = set()
    for row in rows:
        order_public_id = row["order_public_id"]
        if order_public_id in seen:
            ambiguous.add(order_public_id)
            lineage.pop(order_public_id, None)
            continue
        seen.add(order_public_id)
        lineage[order_public_id] = TimelineExecutionLineage(
            source_surface=row["source_surface"],
            plan_public_id=row["plan_public_id"],
            signal_public_id=row["signal_public_id"],
            origin=row["origin"],
            strategy_name=row["strategy_name"],
        )
    for order_public_id in ambiguous:
        lineage.pop(order_public_id, None)
    return lineage


def _to_timeline_accrual(
    row: PnlTimelineAccrualRow,
    valuation_ccy: str,
    rates: FxRateMap,
    venues: FxVenueMap | None = None,
) -> TimelineAccrual:
    """Map a repository accrual row onto the pure builder's input.

    Exact zero passes through regardless of asset because it needs no currency
    conversion. A nonzero foreign amount is CONVERTED at the accrual's own minute
    from our finalized 1m candles, and only an unresolvable pair leaves it as
    ``NaN`` so the builder withholds the affected cumulatives.

    Args:
        row: One ``get_accruals_for_pnl`` row.
        valuation_ccy: Currency the series is valued in.
        rates: Minute-keyed FX rates used to convert a foreign-denominated amount.
        venues: Instrument-pinned oriented plane for each converted currency pair.

    Returns:
        The equivalent :class:`TimelineAccrual`.
    """
    converted = convert_amount(
        row["amount"],
        row["amount_asset"],
        valuation_ccy,
        _rate_minute(row["accrued_at"]),
        rates,
        venues,
    )
    amount = math.nan if converted is None else converted
    incompleteness_reason: PnlIncompletenessReason | None = (
        "fx_conversion_unproven"
        if math.isfinite(row["amount"])
        and not _is_exact_zero(row["amount"])
        and row["amount_asset"] != valuation_ccy
        and converted is None
        else None
    )
    return TimelineAccrual(
        instrument_public_id=row["instrument_public_id"],
        accrued_at=row["accrued_at"],
        amount_usd=amount,
        incompleteness_reason=incompleteness_reason,
    )


def _withhold_series_for_global_reason(
    result: PnlTimelineResult,
    global_reason: PnlIncompletenessReasonEntry,
) -> PnlTimelineResult:
    """Post-transform a built series when one global ledger guard fails.

    Fill gaps and late pre-activation suffix rows both invalidate every
    cumulative monetary value. Timestamps and contributing identities remain
    visible, while the exact global cause is merged with independent replay
    reasons.

    Args:
        result: Series built from the currently visible execution prefix.
        global_reason: Global cause stamped by the positive evidence read.

    Returns:
        The same grid and metadata with every point fully untrusted.
    """
    points = tuple(
        PnlTimelinePoint(
            point_time=point.point_time,
            realized_pnl=None,
            fee_pnl=None,
            accrual_pnl=None,
            unrealized_pnl=None,
            net_pnl=None,
            valuation_status="incomplete",
            incompleteness_reasons=canonical_incompleteness_reasons(
                (
                    *point.incompleteness_reasons,
                    global_reason,
                )
            ),
            per_instrument=tuple(
                PnlInstrumentContribution(
                    instrument_public_id=contribution.instrument_public_id,
                    native_symbol=contribution.native_symbol,
                    exchange=contribution.exchange,
                    realized_pnl=None,
                    fee_pnl=None,
                    accrual_pnl=None,
                    unrealized_pnl=None,
                )
                for contribution in point.per_instrument
            ),
            attribution=tuple(
                PnlAttributionContribution(
                    origin=contribution.origin,
                    strategy_name=contribution.strategy_name,
                    realized_pnl=None,
                    fee_pnl=None,
                    accrual_pnl=None,
                    unrealized_pnl=None,
                )
                for contribution in point.attribution
            ),
        )
        for point in result.points
    )
    return PnlTimelineResult(
        points=points,
        granularity=result.granularity,
        valuation_ccy=result.valuation_ccy,
    )


def _with_instrument_display_identity(
    contribution: PnlInstrumentContribution,
    identity: tuple[str, str] | None,
) -> PnlInstrumentContribution:
    """Attach one proven display identity or retain an honest null pair.

    Args:
        contribution: Pure-builder contribution to decorate.
        identity: Proven native symbol and canonical source venue, when available.

    Returns:
        The contribution with only its display metadata replaced.
    """
    return PnlInstrumentContribution(
        instrument_public_id=contribution.instrument_public_id,
        native_symbol=None if identity is None else identity[0],
        exchange=None if identity is None else identity[1],
        realized_pnl=contribution.realized_pnl,
        fee_pnl=contribution.fee_pnl,
        accrual_pnl=contribution.accrual_pnl,
        unrealized_pnl=contribution.unrealized_pnl,
    )


def _instrument_display_identities(
    activity_spans: Mapping[str, tuple[datetime, datetime]],
    refs: Sequence[InstrumentSymbolRefRow],
) -> dict[str, tuple[str, str]]:
    """Prove display identities independently from price denomination.

    Args:
        activity_spans: Inclusive replay activity bounds keyed by instrument.
        refs: Symbol references loaded at the response knowledge horizon.

    Returns:
        Native symbol and canonical source venue for instruments whose relevant
        activity is continuously covered by one unanimous display projection.
    """
    refs_by_instrument: dict[str, list[InstrumentSymbolRefRow]] = {}
    for ref in refs:
        refs_by_instrument.setdefault(ref["instrument_public_id"], []).append(ref)
    identities: dict[str, tuple[str, str]] = {}
    for instrument_public_id, (span_start, span_end) in activity_spans.items():
        candidates = [
            ref
            for ref in refs_by_instrument.get(instrument_public_id, [])
            if ref["valid_from"] <= span_end and ref["valid_to"] > span_start
        ]
        display_projections = {(ref["native_symbol"], ref["exchange"]) for ref in candidates}
        if len(display_projections) != 1:
            continue
        ordered = sorted(candidates, key=lambda ref: (ref["valid_from"], ref["valid_to"]))
        coverage_start = ordered[0]["valid_from"]
        coverage_end = ordered[0]["valid_to"]
        merged_intervals: list[tuple[datetime, datetime]] = []
        for ref in ordered[1:]:
            if ref["valid_from"] > coverage_end:
                merged_intervals.append((coverage_start, coverage_end))
                coverage_start = ref["valid_from"]
                coverage_end = ref["valid_to"]
            else:
                coverage_end = max(coverage_end, ref["valid_to"])
        merged_intervals.append((coverage_start, coverage_end))
        if any(start <= span_start and end > span_end for start, end in merged_intervals):
            identities[instrument_public_id] = next(iter(display_projections))
    return identities


def _with_instrument_display_identities(
    result: PnlTimelineResult,
    identities: Mapping[str, tuple[str, str]],
) -> PnlTimelineResult:
    """Attach only symbol identities proven by the request's ``as_of`` refs.

    Args:
        result: Fully built, downsampled, and gap-withheld series.
        identities: Display projections proven over the relevant replay spans.

    Returns:
        The same series values and causes with nullable contribution identities.
    """
    points = tuple(
        _with_point_instrument_display_identities(point, identities) for point in result.points
    )
    return PnlTimelineResult(
        points=points,
        granularity=result.granularity,
        valuation_ccy=result.valuation_ccy,
    )


def _with_point_instrument_display_identities(
    point: PnlTimelinePoint,
    identities: Mapping[str, tuple[str, str]],
) -> PnlTimelinePoint:
    """Attach proven display identities to one immutable timeline point."""
    return PnlTimelinePoint(
        point_time=point.point_time,
        realized_pnl=point.realized_pnl,
        fee_pnl=point.fee_pnl,
        accrual_pnl=point.accrual_pnl,
        unrealized_pnl=point.unrealized_pnl,
        net_pnl=point.net_pnl,
        valuation_status=point.valuation_status,
        incompleteness_reasons=point.incompleteness_reasons,
        per_instrument=tuple(
            _with_instrument_display_identity(
                contribution,
                identities.get(contribution.instrument_public_id),
            )
            for contribution in point.per_instrument
        ),
        attribution=point.attribution,
    )


async def _scope_has_fill_gap(
    repo: Repository,
    wallet_public_id: str,
    mode: str,
    as_of: datetime | None,
    execution_prefix: PnlTimelineExecutionPrefix,
) -> PnlIncompletenessReasonEntry | None:
    """Consult one bounded durable gap analysis for the complete scope.

    The repository seals every shared venue shard in one exact-scope read,
    resolves one execution prefix, aggregates both sides once by durable shard,
    and compares the resulting multisets bidirectionally. The recovery gap read
    is intentionally not reused because it describes current state rather than
    historical P&L completeness.

    Args:
        repo: Repository providing one scope-level historical gap analysis.
        wallet_public_id: Full wallet scope.
        mode: Trading mode scope.
        as_of: Temporal anchor bounding the consumed execution evidence.
            Both call sites ALWAYS supply the sealed prefix, so this read never
            folds the manifest itself and the instant is used purely to bound
            the venue-shard read — which is why the activation analysis passes
            the activation instant rather than the request horizon.
        execution_prefix: Already sealed execution evidence reused by replay.

    Returns:
        The stamped global reason when the scope has a proven fill gap,
        otherwise ``None``.
    """
    if not await repo.pnl_timeline_scope_has_fill_gap(
        wallet_public_id,
        mode,
        as_of,
        execution_prefix,
    ):
        return None
    return PnlIncompletenessReasonEntry(
        reason="fill_evidence_gap",
        withholding_tier="untrusted",
        withholding_scope="global",
        trigger_instrument_public_id=None,
    )


def _enforce_total_work_budget(
    from_time: datetime,
    to_time: datetime,
    distinct_instrument_count: int,
) -> None:
    """Reject a reconstruction whose raw grid fan-out exceeds the budget.

    Work is measured as inclusive raw grid minutes multiplied by the number of
    distinct instruments participating through executions or accruals. A factor
    of one applies to an empty scope because grid construction itself is still
    linear in the requested minute span.

    Args:
        from_time: Requested series start; its minute floor anchors the grid.
        to_time: Inclusive series end.
        distinct_instrument_count: Unique instruments in all input flows.

    Raises:
        PnlTimelineWorkBudgetError: When minute-instrument work exceeds the
            configured maximum.
    """
    grid_start = from_time.replace(second=0, microsecond=0)
    raw_grid_minutes = int((to_time - grid_start).total_seconds() // 60) + 1
    work_units = raw_grid_minutes * max(1, distinct_instrument_count)
    if work_units > PNL_TIMELINE_MAX_WORK_UNITS:
        raise PnlTimelineWorkBudgetError(
            f"Requested timeline requires {work_units:,} minute-instrument work units; "
            f"maximum is {PNL_TIMELINE_MAX_WORK_UNITS:,}. Shorten the window or narrow "
            "the wallet scope."
        )


async def _load_mark_candles(
    repo: Repository,
    refs: Sequence[InstrumentSymbolRefRow],
    from_time: datetime,
    to_time: datetime,
    as_of: datetime,
) -> list[PnlTimelineCandleRow]:
    """Load one bounded batch of raw mark candles for trusted references.

    Args:
        repo: Repository providing the timeline candle read.
        refs: Trusted symbol references to load.
        from_time: Window start whose minute floor anchors the read.
        to_time: Inclusive series end.
        as_of: Snapshot time threading the candle read.

    Returns:
        Raw finalized one-minute mark candles, or an empty list when no
        references need marks.
    """
    if not refs:
        return []
    grid_start = from_time.replace(second=0, microsecond=0)
    candles = await repo.get_pnl_timeline_candles(
        refs,
        grid_start - timedelta(minutes=1),
        to_time,
        as_of,
    )
    return list(candles)


def _build_marks_and_reasons_from_candles(
    candles: Sequence[PnlTimelineCandleRow],
    quote_by_instrument: Mapping[str, str],
    valuation_ccy: str,
    rates: FxRateMap,
    venues_by_instrument: Mapping[str, FxVenueMap],
) -> tuple[MarkMap, MarkIncompletenessReasonMap]:
    """Convert raw closes and retain exact caller-side FX failures.

    Args:
        candles: Raw finalized mark candles.
        quote_by_instrument: Proven denomination of each candle close.
        valuation_ccy: Currency the returned marks are denominated in.
        rates: Plane-qualified exact-minute FX closes.
        venues_by_instrument: Consumer-pinned planes keyed by instrument and pair.

    Returns:
        Positive converted marks and caller-stamped conversion failures keyed by
        instrument and closing minute.
    """
    marks: dict[tuple[str, datetime], float | None] = {}
    incompleteness_reasons: dict[tuple[str, datetime], PnlIncompletenessReason] = {}
    for candle in candles:
        close = candle["close"]
        if not is_positive_finite(close):
            continue
        instrument_public_id = candle["instrument_public_id"]
        quote_currency = quote_by_instrument[instrument_public_id]
        mark_minute = candle["open_at"] + timedelta(minutes=1)
        converted = convert_amount(
            close,
            quote_currency,
            valuation_ccy,
            mark_minute,
            rates,
            venues_by_instrument.get(instrument_public_id, {}),
        )
        if is_positive_finite(converted):
            marks[(instrument_public_id, mark_minute)] = converted
        else:
            incompleteness_reasons[(instrument_public_id, mark_minute)] = "fx_conversion_unproven"
    return marks, incompleteness_reasons


def _build_marks_from_candles(
    candles: Sequence[PnlTimelineCandleRow],
    quote_by_instrument: Mapping[str, str],
    valuation_ccy: str,
    rates: FxRateMap,
    venues_by_instrument: Mapping[str, FxVenueMap],
) -> MarkMap:
    """Convert positive raw closes through each instrument's FX plane map."""
    marks, _ = _build_marks_and_reasons_from_candles(
        candles,
        quote_by_instrument,
        valuation_ccy,
        rates,
        venues_by_instrument,
    )
    return marks


async def build_marks(
    repo: Repository,
    refs: Sequence[InstrumentSymbolRefRow],
    from_time: datetime,
    to_time: datetime,
    as_of: datetime,
    valuation_ccy: str,
    rates: FxRateMap | None = None,
    venues: FxVenueMap | None = None,
) -> MarkMap:
    """Resolve valuation-currency marks from one batched candle read.

    Every non-null-quote reference participates when a rate map is supplied.
    Each positive candle close is converted from that quote into
    ``valuation_ccy`` at ``open_at + 1min``, the grid minute the bar values
    without look-ahead. A missing exact-minute rate emits no mark. When ``rates``
    is omitted, only already-native references participate, retaining the
    helper's direct-mark mode. The repository resolves each reference on its
    canonical source venue but projects the requesting instrument's own public
    id, preserving PAPER identity in the mark map.

    Args:
        repo: Repository providing the batched timeline candle read.
        refs: Symbol references for the instruments the executions touch.
        from_time: Window start (its minute floor anchors the candle range).
        to_time: Window end.
        as_of: Snapshot time threading the candle read.
        valuation_ccy: Currency the returned marks are denominated in.
        rates: Exact-minute pinned-plane rates for foreign closes. Omission
            selects direct-mark mode and skips foreign references.
        venues: One consumer-pinned oriented plane per unordered currency pair.

    Returns:
        A mapping keyed by ``(instrument_public_id, grid_minute)`` to the
        converted valuation-currency close mark for that minute.
    """
    eligible_refs = [
        ref
        for ref in refs
        if ref["quote_currency"] == valuation_ccy
        or (rates is not None and ref["quote_currency"] is not None)
    ]
    if not eligible_refs:
        return {}
    resolved_rates: FxRateMap = {} if rates is None else rates
    resolved_venues: FxVenueMap = {} if venues is None else venues
    quote_by_instrument = {
        ref["instrument_public_id"]: cast(str, ref["quote_currency"]) for ref in eligible_refs
    }
    candles = await _load_mark_candles(
        repo,
        eligible_refs,
        from_time,
        to_time,
        as_of,
    )
    return _build_marks_from_candles(
        candles,
        quote_by_instrument,
        valuation_ccy,
        resolved_rates,
        dict.fromkeys(quote_by_instrument, resolved_venues),
    )


def _merge_price_ref_intervals(
    refs: Sequence[InstrumentSymbolRefRow],
) -> list[InstrumentSymbolRefRow]:
    """Merge touching intervals that carry the same price-proof projection."""
    grouped: dict[tuple[str, str, str, str, str | None], list[InstrumentSymbolRefRow]] = {}
    for ref in refs:
        key = (
            ref["native_symbol"],
            ref["exchange"],
            ref["instrument_exchange"],
            ref["base_currency"],
            ref["quote_currency"],
        )
        grouped.setdefault(key, []).append(ref)
    merged: list[InstrumentSymbolRefRow] = []
    for group in grouped.values():
        ordered = sorted(group, key=lambda ref: (ref["valid_from"], ref["valid_to"]))
        current = ordered[0].copy()
        for ref in ordered[1:]:
            if ref["valid_from"] > current["valid_to"]:
                merged.append(current)
                current = ref.copy()
                continue
            current["valid_to"] = max(current["valid_to"], ref["valid_to"])
        merged.append(current)
    return merged


type _InstrumentIncompletenessReasons = dict[str, set[PnlIncompletenessReason]]
"""Caller-stamped causal reasons keyed by the affected instrument."""


def _partition_series_price_refs(
    instrument_spans: Mapping[str, tuple[datetime, datetime]],
    refs: Sequence[InstrumentSymbolRefRow],
) -> tuple[list[InstrumentSymbolRefRow], _InstrumentIncompletenessReasons]:
    """Partition instrument spans by convertible price-currency proof.

    Every version known at the response horizon must agree on one base currency
    and one non-null quote currency. The quote may differ from the requested
    valuation currency because the series layer converts it later. Candidates are collapsed by
    their denomination-relevant projection, and touching intervals for one
    projection are merged before exactly one candidate must cover the event
    span. Missing, gapped, conflicting, and null-quote references remain
    untrusted while metadata-only version churn does not blank the series.

    Args:
        instrument_spans: Inclusive event-time bounds keyed by instrument.
        refs: Historical symbol-reference intervals for those instruments.

    Returns:
        Uniquely proven convertible references in input-instrument order and the
        exact stamped reasons for instruments whose execution-price currency is
        untrusted.
    """
    refs_by_instrument: dict[str, list[InstrumentSymbolRefRow]] = {}
    for ref in refs:
        refs_by_instrument.setdefault(ref["instrument_public_id"], []).append(ref)
    trusted_refs: list[InstrumentSymbolRefRow] = []
    untrusted_reasons: _InstrumentIncompletenessReasons = {}
    for instrument_public_id, (span_start, span_end) in instrument_spans.items():
        instrument_refs = refs_by_instrument.get(instrument_public_id, [])
        base_currencies = {ref["base_currency"] for ref in instrument_refs}
        quote_currencies = {ref["quote_currency"] for ref in instrument_refs}
        candidates = _merge_price_ref_intervals(
            [
                ref
                for ref in instrument_refs
                if ref["valid_from"] <= span_end and ref["valid_to"] > span_start
            ]
        )
        if (
            len(base_currencies) == 1
            and len(quote_currencies) == 1
            and None not in quote_currencies
            and len(candidates) == 1
            and candidates[0]["valid_from"] <= span_start
            and candidates[0]["valid_to"] > span_end
        ):
            trusted_refs.append(candidates[0])
        else:
            untrusted_reasons.setdefault(instrument_public_id, set()).add(
                "execution_price_provenance_unproven"
            )
    return trusted_refs, untrusted_reasons


def _partition_price_refs(
    instrument_spans: Mapping[str, tuple[datetime, datetime]],
    refs: Sequence[InstrumentSymbolRefRow],
    valuation_ccy: str,
) -> tuple[list[InstrumentSymbolRefRow], set[str]]:
    """Partition raw marker prices by direct valuation-currency proof.

    Marker prices are intentionally not converted. This gate layers the raw
    overlay's direct-currency requirement on top of the same unanimous quote,
    interval-coverage, and projection proof used by the converted series.

    Args:
        instrument_spans: Inclusive event-time bounds keyed by instrument.
        refs: Historical symbol-reference intervals for those instruments.
        valuation_ccy: Currency a raw marker price must already represent.

    Returns:
        Direct-currency references and every instrument that fails either the
        shared identity proof or the marker-specific valuation gate.
    """
    trusted_refs, untrusted_reasons = _partition_series_price_refs(
        instrument_spans,
        refs,
    )
    untrusted_instruments = set(untrusted_reasons)
    direct_refs: list[InstrumentSymbolRefRow] = []
    for ref in trusted_refs:
        if ref["quote_currency"] == valuation_ccy:
            direct_refs.append(ref)
        else:
            untrusted_instruments.add(ref["instrument_public_id"])
    return direct_refs, untrusted_instruments


def _partition_series_execution_price_refs(
    execution_rows: Sequence[PnlTimelineExecutionRow],
    refs: Sequence[InstrumentSymbolRefRow],
) -> tuple[list[InstrumentSymbolRefRow], _InstrumentIncompletenessReasons]:
    """Prove convertible execution prices over complete instrument spans.

    The shared series proof establishes one non-null quote and one unchanged
    source projection over every fill, then independently requires immutable
    execution venue lineage to match the reference's owning venue.

    Args:
        execution_rows: Scope execution prefix replayed by the P&L kernel.
        refs: Historical symbol-reference intervals for those instruments.

    Returns:
        References whose quote and venue can be converted safely, and the
        stamped per-instrument reasons whose denomination or venue remains
        untrusted.
    """
    spans: dict[str, tuple[datetime, datetime]] = {}
    for row in execution_rows:
        instrument_public_id = row["instrument_public_id"]
        event_time = row["timestamp"]
        existing = spans.get(instrument_public_id)
        if existing is None:
            spans[instrument_public_id] = (event_time, event_time)
        else:
            spans[instrument_public_id] = (
                min(existing[0], event_time),
                max(existing[1], event_time),
            )
    trusted_refs, untrusted_reasons = _partition_series_price_refs(spans, refs)
    trusted_by_instrument = {ref["instrument_public_id"]: ref for ref in trusted_refs}
    for row in execution_rows:
        instrument_public_id = row["instrument_public_id"]
        ref = trusted_by_instrument.get(instrument_public_id)
        if ref is not None and row["exchange"] != ref["instrument_exchange"]:
            untrusted_reasons.setdefault(instrument_public_id, set()).add(
                "execution_price_provenance_unproven"
            )
    if untrusted_reasons:
        trusted_refs = [
            ref for ref in trusted_refs if ref["instrument_public_id"] not in untrusted_reasons
        ]
    return trusted_refs, untrusted_reasons


def _partition_execution_price_refs(
    execution_rows: Sequence[PnlTimelineExecutionRow],
    refs: Sequence[InstrumentSymbolRefRow],
    valuation_ccy: str,
) -> tuple[list[InstrumentSymbolRefRow], set[str]]:
    """Partition replayed execution instruments using their complete spans.

    Args:
        execution_rows: Scope execution prefix replayed by the P&L kernel.
        refs: Historical symbol-reference intervals for those instruments.
        valuation_ccy: Currency every execution price must already represent.

    Returns:
        References that prove one unchanged identity and owning venue covered
        every fill, and the instruments whose execution-price denomination or
        venue remains untrusted.
    """
    trusted_refs, untrusted_reasons = _partition_series_execution_price_refs(
        execution_rows,
        refs,
    )
    untrusted_instruments = set(untrusted_reasons)
    direct_refs: list[InstrumentSymbolRefRow] = []
    for ref in trusted_refs:
        if ref["quote_currency"] == valuation_ccy:
            direct_refs.append(ref)
        else:
            untrusted_instruments.add(ref["instrument_public_id"])
    return direct_refs, untrusted_instruments


type _FxMinuteRequirements = dict[FxPairKey, set[datetime]]
"""Exact conversion minutes grouped by unordered currency pair."""

type _FxInstrumentRequirements = dict[str, _FxMinuteRequirements]
"""Exact conversion minutes grouped first by consuming instrument."""

type _FxIdentityPlanes = dict[str, PnlFxRatePlane]
"""Canonical source plane keyed by the held instrument that justifies it."""

type _FxPlanesByInstrument = dict[str, dict[FxPairKey, PnlFxRatePlane]]
"""Selected conversion planes keyed by consuming instrument and pair."""

type _PoolKey = tuple[str, str]
"""Durable instrument and shard identity used by replay state."""


@dataclass(frozen=True, slots=True)
class _EventFxContext:
    """Immutable denomination evidence used by execution FX replay."""

    base_by_instrument: Mapping[str, str]
    quote_by_instrument: Mapping[str, str]
    valuation_ccy: str


@dataclass(slots=True)
class _EventFxReplayState:
    """Mutable position and trust state for event FX requirements."""

    requirements: _FxInstrumentRequirements
    last_effective: dict[_PoolKey, datetime]
    position_qty: dict[_PoolKey, float]
    basis_unknown: set[_PoolKey]
    untrusted_at: dict[str, datetime]


def _add_fx_minute(
    requirements: _FxInstrumentRequirements,
    instrument_public_id: str,
    currency: str,
    valuation_ccy: str,
    minute: datetime,
) -> None:
    """Add one foreign-currency conversion minute when evidence is required.

    Args:
        requirements: Mutable exact-minute requirements grouped by instrument.
        instrument_public_id: Instrument whose contribution needs the rate.
        currency: Denomination of the value being converted.
        valuation_ccy: Target series currency.
        minute: Exact rate minute needed by the conversion.
    """
    if not currency or currency == valuation_ccy:
        return
    pair_requirements = requirements.setdefault(instrument_public_id, {})
    pair_requirements.setdefault(currency_pair_key(currency, valuation_ccy), set()).add(minute)


def _opening_pool_quantities(opening: TimelineOpening | None) -> dict[_PoolKey, float]:
    """Return opening quantities keyed by their durable replay pools."""
    if opening is None:
        return {}
    return {
        (pool.instrument_public_id, pool.shard_key): pool.position_qty for pool in opening.pools
    }


def _next_effective_time(
    last_effective: dict[_PoolKey, datetime],
    pool_key: _PoolKey,
    event_time: datetime,
) -> datetime:
    """Clamp one pool event monotonically and retain its new replay cursor."""
    effective_time = max(last_effective.get(pool_key, event_time), event_time)
    last_effective[pool_key] = effective_time
    return effective_time


def _record_event_position(
    state: _EventFxReplayState,
    pool_key: _PoolKey,
    old_qty: float,
    signed_size: float,
    price_is_known_valid: bool,
) -> None:
    """Advance one pool quantity and its unknown-basis latch."""
    new_qty = old_qty + signed_size
    if abs(new_qty) < FLAT_EPSILON:
        new_qty = 0.0
        state.basis_unknown.discard(pool_key)
    elif abs(signed_size) > 0.0 and not price_is_known_valid:
        state.basis_unknown.add(pool_key)
    state.position_qty[pool_key] = new_qty


def _add_execution_fx_requirements(
    state: _EventFxReplayState,
    context: _EventFxContext,
    row: PnlTimelineOpeningExecutionRow,
) -> None:
    """Replay one execution into exact FX requirements and trust state."""
    instrument_public_id = row["instrument_public_id"]
    pool_key = (instrument_public_id, row["shard_key"])
    effective_time = _next_effective_time(
        state.last_effective,
        pool_key,
        row["timestamp"],
    )
    if instrument_public_id in state.untrusted_at:
        return
    size = row["size"]
    if not math.isfinite(size) or size < 0.0 or not row["shard_key"]:
        state.untrusted_at[instrument_public_id] = effective_time
        return
    old_qty = state.position_qty.get(pool_key, 0.0)
    signed_size = booked_signed_quantity(
        row["side"],
        size,
        row["fee"],
        row["fee_asset"],
        context.base_by_instrument[instrument_public_id],
        resolve_position_quantity_unit(row["exchange"]),
    )
    position_size = abs(signed_size)
    is_increasing = (old_qty >= 0.0 and signed_size > 0.0) or (old_qty <= 0.0 and signed_size < 0.0)
    closed_qty = 0.0 if is_increasing else min(position_size, abs(old_qty))
    price_is_known_valid = is_positive_finite(row["price"])
    if closed_qty > 0.0 and (not price_is_known_valid or pool_key in state.basis_unknown):
        state.untrusted_at[instrument_public_id] = effective_time
        return
    minute = _rate_minute(row["timestamp"])
    quote_currency = context.quote_by_instrument.get(instrument_public_id)
    if (
        quote_currency is not None
        and not _is_exact_zero(signed_size)
        and price_is_known_valid
        and pool_key not in state.basis_unknown
    ):
        _add_fx_minute(
            state.requirements,
            instrument_public_id,
            quote_currency,
            context.valuation_ccy,
            minute,
        )
    if not _is_exact_zero(row["fee"]):
        _add_fx_minute(
            state.requirements,
            instrument_public_id,
            row["fee_asset"],
            context.valuation_ccy,
            minute,
        )
    _record_event_position(
        state,
        pool_key,
        old_qty,
        signed_size,
        price_is_known_valid,
    )


def _add_accrual_fx_requirement(
    state: _EventFxReplayState,
    valuation_ccy: str,
    accrual: PnlTimelineAccrualRow,
) -> None:
    """Add one publishable nonzero accrual's exact conversion minute."""
    if _is_exact_zero(accrual["amount"]):
        return
    invalid_at = state.untrusted_at.get(accrual["instrument_public_id"])
    if invalid_at is not None and accrual["accrued_at"] >= invalid_at:
        return
    _add_fx_minute(
        state.requirements,
        accrual["instrument_public_id"],
        accrual["amount_asset"],
        valuation_ccy,
        _rate_minute(accrual["accrued_at"]),
    )


def _event_fx_minutes(
    execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
    accrual_rows: Sequence[PnlTimelineAccrualRow],
    base_by_instrument: Mapping[str, str],
    quote_by_instrument: Mapping[str, str],
    valuation_ccy: str,
    opening: TimelineOpening | None = None,
) -> _FxInstrumentRequirements:
    """Collect exact rate minutes needed by prices, fees, and accruals.

    A positive foreign execution price enters the average-cost kernel only after
    conversion at its fill minute. Requirements stop when an invalid size or an
    unknown-basis close permanently withholds that instrument, and prices that
    cannot repair an already unknown basis do not steer peer selection. Nonzero
    foreign fees and accruals need the same proof while their instrument remains
    publishable; exact zero and native-currency values need no candle.

    Args:
        execution_rows: Trusted scope executions replayed by the kernel.
        accrual_rows: Scope accruals, whose amount carries ``amount_asset``.
        base_by_instrument: Proven base asset of each execution instrument.
        quote_by_instrument: Proven execution-price denominations.
        valuation_ccy: Currency the series is valued in.
        opening: Rebased per-shard quantities active before the suffix.

    Returns:
        Exact required minutes grouped by instrument and unordered currency pair.
    """
    state = _EventFxReplayState(
        requirements={},
        last_effective={},
        position_qty=_opening_pool_quantities(opening),
        basis_unknown=set(),
        untrusted_at={},
    )
    context = _EventFxContext(
        base_by_instrument=base_by_instrument,
        quote_by_instrument=quote_by_instrument,
        valuation_ccy=valuation_ccy,
    )
    for row in execution_rows:
        _add_execution_fx_requirements(state, context, row)
    for accrual in accrual_rows:
        _add_accrual_fx_requirement(state, valuation_ccy, accrual)
    return state.requirements


type _EffectiveMarkEvent = tuple[datetime, str, int, str, float, bool]
"""Monotone event time, ordering lineage, quantity delta, and invalidity."""


@dataclass(slots=True)
class _MarkReplayState:
    """Mutable per-instrument position state while visiting mark minutes."""

    event_index: int
    pool_quantities: dict[str, float]
    mark_eligible: bool


@dataclass(frozen=True, slots=True)
class _MarkFxWindow:
    """Immutable grid bounds and global regression shadows for mark selection."""

    grid_start: datetime
    to_time: datetime
    regression_shadows: Sequence[tuple[datetime, datetime]]


def _mark_event_delta(
    row: PnlTimelineOpeningExecutionRow,
    base_asset: str,
) -> tuple[float, bool]:
    """Return one replay delta and whether it permanently invalidates later marks."""
    size = row["size"]
    if not math.isfinite(size) or size < 0.0:
        return 0.0, True
    signed_size = booked_signed_quantity(
        row["side"],
        size,
        row["fee"],
        row["fee_asset"],
        base_asset,
        resolve_position_quantity_unit(row["exchange"]),
    )
    invalid_price = abs(signed_size) > 0.0 and not is_positive_finite(row["price"])
    return signed_size, invalid_price


def _effective_mark_events(
    execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
    base_by_instrument: Mapping[str, str],
    quote_by_instrument: Mapping[str, str],
) -> tuple[dict[str, list[_EffectiveMarkEvent]], list[tuple[datetime, datetime]]]:
    """Build monotone mark replay events and scope-wide regression shadows."""
    effective_events: dict[str, list[_EffectiveMarkEvent]] = {}
    last_effective: dict[_PoolKey, datetime] = {}
    regression_shadows: list[tuple[datetime, datetime]] = []
    for row in execution_rows:
        instrument_public_id = row["instrument_public_id"]
        pool_key = (instrument_public_id, row["shard_key"])
        event_time = row["timestamp"]
        previous_effective = last_effective.get(pool_key)
        if previous_effective is not None and event_time < previous_effective:
            regression_shadows.append((event_time, previous_effective))
        effective_time = _next_effective_time(last_effective, pool_key, event_time)
        if instrument_public_id not in quote_by_instrument:
            continue
        signed_size, invalid_event = _mark_event_delta(
            row,
            base_by_instrument[instrument_public_id],
        )
        effective_events.setdefault(instrument_public_id, []).append(
            (
                effective_time,
                row["exchange"],
                row["scope_sequence"],
                row["shard_key"],
                signed_size,
                invalid_event,
            )
        )
    return effective_events, regression_shadows


def _candles_by_instrument(
    candles: Sequence[PnlTimelineCandleRow],
) -> dict[str, list[PnlTimelineCandleRow]]:
    """Group mark candles by their consuming instrument."""
    grouped: dict[str, list[PnlTimelineCandleRow]] = {}
    for candle in candles:
        grouped.setdefault(candle["instrument_public_id"], []).append(candle)
    return grouped


def _advance_mark_replay(
    state: _MarkReplayState,
    events: Sequence[_EffectiveMarkEvent],
    mark_minute: datetime,
) -> None:
    """Apply all monotone execution events visible at one mark minute."""
    while state.event_index < len(events) and events[state.event_index][0] <= mark_minute:
        event = events[state.event_index]
        shard_key = event[3]
        pool_quantity = state.pool_quantities.get(shard_key, 0.0) + event[4]
        if abs(pool_quantity) < FLAT_EPSILON:
            pool_quantity = 0.0
        state.pool_quantities[shard_key] = pool_quantity
        if event[5]:
            state.mark_eligible = False
        state.event_index += 1


def _mark_minute_is_eligible(
    candle: PnlTimelineCandleRow,
    mark_minute: datetime,
    state: _MarkReplayState,
    window: _MarkFxWindow,
) -> bool:
    """Return whether one positive close truthfully consumes an FX rate."""
    if not is_positive_finite(candle["close"]) or not state.mark_eligible:
        return False
    if mark_minute < window.grid_start or mark_minute > window.to_time:
        return False
    if not any(abs(quantity) >= FLAT_EPSILON for quantity in state.pool_quantities.values()):
        return False
    return not any(
        shadow_start <= mark_minute < shadow_end
        for shadow_start, shadow_end in window.regression_shadows
    )


def _eligible_mark_minutes(
    instrument_public_id: str,
    candles: Sequence[PnlTimelineCandleRow],
    events: Sequence[_EffectiveMarkEvent],
    opening_quantities: Mapping[_PoolKey, float],
    window: _MarkFxWindow,
) -> set[datetime]:
    """Replay one instrument and return the exact minutes needing mark FX."""
    state = _MarkReplayState(
        event_index=0,
        pool_quantities={
            shard_key: quantity
            for (opening_instrument, shard_key), quantity in opening_quantities.items()
            if opening_instrument == instrument_public_id
        },
        mark_eligible=True,
    )
    minutes: set[datetime] = set()
    for candle in sorted(candles, key=lambda item: item["open_at"]):
        mark_minute = candle["open_at"] + timedelta(minutes=1)
        _advance_mark_replay(state, events, mark_minute)
        if _mark_minute_is_eligible(candle, mark_minute, state, window):
            minutes.add(mark_minute)
    return minutes


def _mark_fx_minutes(
    candles: Sequence[PnlTimelineCandleRow],
    execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
    base_by_instrument: Mapping[str, str],
    quote_by_instrument: Mapping[str, str],
    valuation_ccy: str,
    from_time: datetime,
    to_time: datetime,
    opening: TimelineOpening | None = None,
) -> _FxInstrumentRequirements:
    """Collect exact rate minutes for positive marks of non-flat instruments.

    Position quantities replay in the same per-instrument monotone-clamped event
    order as the pure builder. Global regression shadows are derived from the
    complete replay prefix, including instruments whose price identity is not
    trusted. Candles before an opening fill, after a full close, or after a
    known-invalid size or price are not valuation consumers, so they cannot steer
    a shared FX plane.

    Args:
        candles: Raw finalized mark candles already bounded to the chart window.
        execution_rows: Complete scope fills in replay order through the chart end.
        base_by_instrument: Proven base asset of each execution instrument.
        quote_by_instrument: Proven denomination of each candle close.
        valuation_ccy: Currency the series is valued in.
        from_time: Requested chart start whose minute floor anchors the grid.
        to_time: Inclusive requested chart end.
        opening: Rebased durable shard pools active at t0, when present.

    Returns:
        Exact required minutes grouped by instrument and unordered currency pair.
    """
    effective_events, regression_shadows = _effective_mark_events(
        execution_rows,
        base_by_instrument,
        quote_by_instrument,
    )
    needed: _FxInstrumentRequirements = {}
    opening_quantities = _opening_pool_quantities(opening)
    window = _MarkFxWindow(
        grid_start=from_time.replace(second=0, microsecond=0),
        to_time=to_time,
        regression_shadows=regression_shadows,
    )
    for instrument_public_id, instrument_candles in _candles_by_instrument(candles).items():
        events = sorted(effective_events.get(instrument_public_id, []))
        quote_currency = quote_by_instrument[instrument_public_id]
        for mark_minute in _eligible_mark_minutes(
            instrument_public_id,
            instrument_candles,
            events,
            opening_quantities,
            window,
        ):
            _add_fx_minute(
                needed,
                instrument_public_id,
                quote_currency,
                valuation_ccy,
                mark_minute,
            )
    return needed


def _merge_fx_minutes(
    first: Mapping[FxPairKey, set[datetime]],
    second: Mapping[FxPairKey, set[datetime]],
) -> _FxMinuteRequirements:
    """Merge two pair-minute requirement maps without widening either read.

    Args:
        first: First bounded requirement map.
        second: Second bounded requirement map.

    Returns:
        Unioned exact minutes used only for request-wide venue resolution.
    """
    merged: _FxMinuteRequirements = {pair: set(minutes) for pair, minutes in first.items()}
    for pair, minutes in second.items():
        merged.setdefault(pair, set()).update(minutes)
    return merged


def _merge_instrument_fx_minutes(
    first: Mapping[str, _FxMinuteRequirements],
    second: Mapping[str, _FxMinuteRequirements],
) -> _FxInstrumentRequirements:
    """Merge exact-minute requirements while preserving consumer identity.

    Args:
        first: First bounded requirement map grouped by instrument.
        second: Second bounded requirement map grouped by instrument.

    Returns:
        Unioned exact minutes grouped by instrument and pair.
    """
    merged: _FxInstrumentRequirements = {
        instrument_public_id: {pair: set(minutes) for pair, minutes in requirements.items()}
        for instrument_public_id, requirements in first.items()
    }
    for instrument_public_id, requirements in second.items():
        target = merged.setdefault(instrument_public_id, {})
        for pair, minutes in requirements.items():
            target.setdefault(pair, set()).update(minutes)
    return merged


def _collapse_fx_minutes(
    requirements: Mapping[str, _FxMinuteRequirements],
) -> _FxMinuteRequirements:
    """Collapse instrument requirements for bounded repository reads.

    Args:
        requirements: Exact minutes grouped by consuming instrument and pair.

    Returns:
        Unioned exact minutes grouped only by currency pair.
    """
    collapsed: _FxMinuteRequirements = {}
    for instrument_requirements in requirements.values():
        for pair, minutes in instrument_requirements.items():
            collapsed.setdefault(pair, set()).update(minutes)
    return collapsed


def _identity_fx_planes(
    refs: Sequence[InstrumentSymbolRefRow],
    requirements: Mapping[str, _FxMinuteRequirements],
) -> _FxIdentityPlanes:
    """Find canonical held-symbol planes justified by each instrument.

    A trusted held instrument supplies its exact oriented candle plane only when
    that same instrument needs conversion across its own currency legs. The
    plane constrains only that instrument and cannot suppress a peer using the
    same unordered pair.

    Args:
        refs: Trusted held-instrument symbol references.
        requirements: Exact conversion minutes grouped by consuming instrument.

    Returns:
        Canonical oriented source plane keyed by its justifying instrument.
    """
    identities: _FxIdentityPlanes = {}
    for ref in refs:
        instrument_public_id = ref["instrument_public_id"]
        quote_currency = cast(str, ref["quote_currency"])
        pair = currency_pair_key(ref["base_currency"], quote_currency)
        if pair in requirements.get(instrument_public_id, {}):
            identities[instrument_public_id] = (
                ref["base_currency"],
                quote_currency,
                ref["exchange"],
            )
    return identities


def _general_fx_minutes(
    requirements: Mapping[str, _FxMinuteRequirements],
    identity_planes: Mapping[str, PnlFxRatePlane],
) -> _FxMinuteRequirements:
    """Collect minutes whose consumers have no own-pair identity override.

    Identity-only minutes cannot steer the shared plane used by peers. Other
    currency pairs needed by an identity instrument remain general consumers.

    Args:
        requirements: Exact conversion minutes grouped by consuming instrument.
        identity_planes: Canonical plane claims keyed by held instrument.

    Returns:
        General-consumer exact minutes grouped by unordered currency pair.
    """
    general: _FxMinuteRequirements = {}
    for instrument_public_id, instrument_requirements in requirements.items():
        identity_plane = identity_planes.get(instrument_public_id)
        identity_pair = (
            None
            if identity_plane is None
            else currency_pair_key(identity_plane[0], identity_plane[1])
        )
        for pair, minutes in instrument_requirements.items():
            if pair == identity_pair:
                continue
            general.setdefault(pair, set()).update(minutes)
    return general


def _fx_requirement_range(
    requirements: Mapping[FxPairKey, set[datetime]],
) -> tuple[datetime, datetime] | None:
    """Return the bounded candle-open range covering exact rate minutes.

    Args:
        requirements: Exact rate minutes grouped by pair.

    Returns:
        Inclusive candle-open bounds, or ``None`` for no requirements.
    """
    minutes = [minute for pair_minutes in requirements.values() for minute in pair_minutes]
    if not minutes:
        return None
    return min(minutes) - timedelta(minutes=1), max(minutes)


def _oriented_fx_pairs(pairs: Sequence[FxPairKey]) -> list[tuple[str, str]]:
    """Expand unordered pairs into both publication orientations.

    Args:
        pairs: Unordered currency-pair identities.

    Returns:
        Deterministically ordered direct and inverse currency legs.
    """
    oriented: set[tuple[str, str]] = set()
    for first, second in pairs:
        oriented.add((first, second))
        oriented.add((second, first))
    return sorted(oriented)


async def _discover_fx_candidates(
    repo: Repository,
    requirements: Mapping[FxPairKey, set[datetime]],
    as_of: datetime,
) -> list[PnlFxRatePlane]:
    """Discover every spot-FX plane with evidence in one bounded rate window.

    Args:
        repo: Repository providing bounded forex-plane discovery.
        requirements: Exact pair minutes in this mark or event window.
        as_of: Knowledge horizon threading the repository read.

    Returns:
        Concrete oriented venue planes with evidence in this window.
    """
    pairs = sorted(requirements)
    bounds = _fx_requirement_range(requirements)
    if not pairs or bounds is None:
        return []
    return await repo.get_pnl_fx_rate_exchanges(
        _oriented_fx_pairs(pairs),
        bounds[0],
        bounds[1],
        as_of,
    )


def _candidate_planes(
    candidates: Sequence[PnlFxRatePlane],
    identity_planes: Mapping[str, PnlFxRatePlane],
) -> dict[FxPairKey, set[PnlFxRatePlane]]:
    """Combine discovered planes with instrument-owned canonical planes.

    Args:
        candidates: Oriented venue planes discovered across bounded windows.
        identity_planes: Canonical held-symbol planes keyed by instrument.

    Returns:
        Eligible oriented planes grouped by unordered pair.
    """
    planes: dict[FxPairKey, set[PnlFxRatePlane]] = {}
    for candidate in candidates:
        base, quote, _ = candidate
        planes.setdefault(currency_pair_key(base, quote), set()).add(candidate)
    for identity in identity_planes.values():
        base, quote, _ = identity
        planes.setdefault(currency_pair_key(base, quote), set()).add(identity)
    return planes


async def _load_fx_candidate_rows(
    repo: Repository,
    requirements: Mapping[FxPairKey, set[datetime]],
    candidate_planes: Mapping[FxPairKey, set[PnlFxRatePlane]],
    as_of: datetime,
) -> list[PnlFxRateRow]:
    """Load eligible exact oriented planes in one bounded window.

    Args:
        repo: Repository providing exact venue-filtered forex candles.
        requirements: Exact pair minutes in this mark or event window.
        candidate_planes: Eligible discovered and identity planes by pair.
        as_of: Knowledge horizon threading the repository read.

    Returns:
        Candidate rows used to resolve shared and instrument-owned planes.
    """
    bounds = _fx_requirement_range(requirements)
    if bounds is None:
        return []
    planes: set[PnlFxRatePlane] = set()
    for pair in requirements:
        planes.update(candidate_planes.get(pair, set()))
    if not planes:
        return []
    return await repo.get_pnl_fx_rate_candles(
        sorted(planes),
        bounds[0],
        bounds[1],
        as_of,
    )


def _covered_fx_minutes(
    requirements: Mapping[FxPairKey, set[datetime]],
    rows: Sequence[PnlFxRateRow],
) -> dict[PnlFxRatePlane, set[datetime]]:
    """Collect usable exact requirement minutes for every candidate plane."""
    covered: dict[PnlFxRatePlane, set[datetime]] = {}
    for (base, quote, exchange, minute), close in build_fx_rates(rows).items():
        if not is_positive_finite(close):
            continue
        pair = currency_pair_key(base, quote)
        if minute in requirements.get(pair, set()):
            covered.setdefault((base, quote, exchange), set()).add(minute)
    return covered


def _preferred_fx_plane(
    pair: FxPairKey,
    minutes: set[datetime],
    planes: set[PnlFxRatePlane],
    covered: Mapping[PnlFxRatePlane, set[datetime]],
    valuation_ccy: str,
) -> PnlFxRatePlane | None:
    """Select the coverage, venue, and orientation winner for one pair."""
    if not planes:
        return None
    coverage_by_plane = {
        plane: len(covered.get(plane, set()).intersection(minutes)) for plane in planes
    }
    best_coverage = max(coverage_by_plane.values())
    if best_coverage == 0:
        return None
    finalists = {
        plane for plane, coverage in coverage_by_plane.items() if coverage == best_coverage
    }
    if best_coverage == len(minutes) and "PLN" in pair:
        walutomat = {plane for plane in finalists if plane[2] == "walutomat"}
        if walutomat:
            finalists = walutomat
    selected_exchange = min(plane[2] for plane in finalists)
    venue_finalists = {plane for plane in finalists if plane[2] == selected_exchange}
    source_currency = pair[1] if pair[0] == valuation_ccy else pair[0]
    direct = {
        plane
        for plane in venue_finalists
        if (plane[0], plane[1]) == (source_currency, valuation_ccy)
    }
    return min(direct or venue_finalists)


def _resolve_fx_planes(
    requirements: Mapping[FxPairKey, set[datetime]],
    candidate_planes: Mapping[FxPairKey, set[PnlFxRatePlane]],
    rows: Sequence[PnlFxRateRow],
    valuation_ccy: str,
) -> dict[FxPairKey, PnlFxRatePlane]:
    """Resolve one oriented plane per pair for general conversion consumers.

    The oriented series covering the most exact general-consumer minutes wins.
    Identity-only requirements have already been excluded so they cannot steer
    a peer's plane. When several complete planes cover a PLN pair, Walutomat is
    preferred. Other ties preserve lexical venue order, then prefer the
    source-to-valuation orientation. Duplicate conflicts on a full plane-minute
    identity do not count as coverage, and zero usable coverage never wins.

    Args:
        requirements: Exact mark and event minutes for general consumers.
        candidate_planes: Eligible discovered or forced planes by pair.
        rows: Candidate candle rows from both bounded reads.
        valuation_ccy: Target currency defining the preferred conversion direction.

    Returns:
        One selected oriented plane for each resolvable unordered pair.
    """
    covered = _covered_fx_minutes(requirements, rows)
    resolved: dict[FxPairKey, PnlFxRatePlane] = {}
    for pair, minutes in requirements.items():
        planes = candidate_planes.get(pair, set())
        selected = _preferred_fx_plane(
            pair,
            minutes,
            planes,
            covered,
            valuation_ccy,
        )
        if selected is not None:
            resolved[pair] = selected
    return resolved


def _resolve_identity_fx_planes(
    requirements: Mapping[str, _FxMinuteRequirements],
    identity_planes: Mapping[str, PnlFxRatePlane],
    rows: Sequence[PnlFxRateRow],
) -> _FxIdentityPlanes:
    """Pin instrument-owned planes only after they prove usable coverage.

    Partial positive coverage pins the canonical plane for the instrument's
    whole request so a rival cannot fill later gaps. Zero usable coverage does
    not pin or enter provenance, but the unresolved identity claim still blocks
    that instrument from borrowing the shared plane.

    Args:
        requirements: Exact conversion minutes grouped by consuming instrument.
        identity_planes: Canonical source-plane claims keyed by instrument.
        rows: Candidate candle rows from both bounded reads.

    Returns:
        Canonical planes with nonzero usable coverage keyed by instrument.
    """
    covered: dict[PnlFxRatePlane, set[datetime]] = {}
    for (base, quote, exchange, minute), close in build_fx_rates(rows).items():
        if is_positive_finite(close):
            covered.setdefault((base, quote, exchange), set()).add(minute)
    resolved: _FxIdentityPlanes = {}
    for instrument_public_id, plane in identity_planes.items():
        pair = currency_pair_key(plane[0], plane[1])
        minutes = requirements.get(instrument_public_id, {}).get(pair, set())
        if covered.get(plane, set()).intersection(minutes):
            resolved[instrument_public_id] = plane
    return resolved


def _fx_planes_by_instrument(
    requirements: Mapping[str, _FxMinuteRequirements],
    shared_planes: Mapping[FxPairKey, PnlFxRatePlane],
    identity_planes: Mapping[str, PnlFxRatePlane],
    resolved_identity_planes: Mapping[str, PnlFxRatePlane],
) -> _FxPlanesByInstrument:
    """Choose one plane per instrument-pair without cross-consumer borrowing.

    Args:
        requirements: Exact conversion minutes grouped by consuming instrument.
        shared_planes: Coverage-ranked planes for general consumers.
        identity_planes: Canonical source-plane claims keyed by instrument.
        resolved_identity_planes: Identity planes with nonzero usable coverage.

    Returns:
        Selected planes for every instrument and required pair.
    """
    selected: _FxPlanesByInstrument = {}
    for instrument_public_id, instrument_requirements in requirements.items():
        identity_plane = identity_planes.get(instrument_public_id)
        identity_pair = (
            None
            if identity_plane is None
            else currency_pair_key(identity_plane[0], identity_plane[1])
        )
        instrument_planes: dict[FxPairKey, PnlFxRatePlane] = {}
        for pair in instrument_requirements:
            if pair == identity_pair:
                resolved_identity = resolved_identity_planes.get(instrument_public_id)
                if resolved_identity is not None:
                    instrument_planes[pair] = resolved_identity
                continue
            shared_plane = shared_planes.get(pair)
            if shared_plane is not None:
                instrument_planes[pair] = shared_plane
        selected[instrument_public_id] = instrument_planes
    return selected


async def _load_request_fx_rates(
    repo: Repository,
    mark_requirements: Mapping[str, _FxMinuteRequirements],
    event_requirements: Mapping[str, _FxMinuteRequirements],
    identity_planes: Mapping[str, PnlFxRatePlane],
    as_of: datetime,
    valuation_ccy: str,
) -> tuple[
    FxRateMap,
    _FxPlanesByInstrument,
    set[tuple[FxPairKey, PnlFxRatePlane]],
]:
    """Load shared and instrument-owned oriented FX planes for one request.

    Mark and event reads retain their own bounded ranges, so an old opening fill
    never widens the chart-window read. Candidate evidence from both is combined
    before shared and per-instrument resolution, then every unused orientation
    and venue is discarded. Each instrument uses one plane per pair for its
    marks, execution prices, fees, and accruals at every minute.

    Args:
        repo: Repository providing bounded FX plane and candle reads.
        mark_requirements: Mark conversion minutes grouped by instrument.
        event_requirements: Fill and accrual minutes grouped by instrument.
        identity_planes: Canonical held-symbol planes keyed by instrument.
        as_of: Knowledge horizon threading repository reads.
        valuation_ccy: Target currency defining conversion-direction tie-breaks.

    Returns:
        Plane-filtered rates, per-instrument selections, and distinct used planes.
    """
    instrument_requirements = _merge_instrument_fx_minutes(
        mark_requirements,
        event_requirements,
    )
    mark_pair_requirements = _collapse_fx_minutes(mark_requirements)
    event_pair_requirements = _collapse_fx_minutes(event_requirements)
    requirements = _merge_fx_minutes(mark_pair_requirements, event_pair_requirements)
    if not requirements:
        return {}, {}, set()
    mark_candidates = await _discover_fx_candidates(
        repo,
        mark_pair_requirements,
        as_of,
    )
    event_candidates = await _discover_fx_candidates(
        repo,
        event_pair_requirements,
        as_of,
    )
    candidate_planes = _candidate_planes(
        [*mark_candidates, *event_candidates],
        identity_planes,
    )
    mark_rows = await _load_fx_candidate_rows(
        repo,
        mark_pair_requirements,
        candidate_planes,
        as_of,
    )
    event_rows = await _load_fx_candidate_rows(
        repo,
        event_pair_requirements,
        candidate_planes,
        as_of,
    )
    rows = [*mark_rows, *event_rows]
    shared_planes = _resolve_fx_planes(
        _general_fx_minutes(instrument_requirements, identity_planes),
        candidate_planes,
        rows,
        valuation_ccy,
    )
    resolved_identity_planes = _resolve_identity_fx_planes(
        instrument_requirements,
        identity_planes,
        rows,
    )
    planes_by_instrument = _fx_planes_by_instrument(
        instrument_requirements,
        shared_planes,
        identity_planes,
        resolved_identity_planes,
    )
    used_planes = {
        (pair, plane)
        for instrument_planes in planes_by_instrument.values()
        for pair, plane in instrument_planes.items()
    }
    selected_planes = {plane for _, plane in used_planes}
    selected_rows = [
        row for row in rows if (row["base"], row["quote"], row["exchange"]) in selected_planes
    ]
    return build_fx_rates(selected_rows), planes_by_instrument, used_planes


def _opening_nonflat_instruments(
    execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
    base_by_instrument: Mapping[str, str],
) -> set[str]:
    """Return instruments with at least one non-flat durable opening shard."""
    quantities: dict[tuple[str, str], float] = {}
    exchange_by_pool: dict[tuple[str, str], str] = {}
    for row in execution_rows:
        instrument_public_id = row["instrument_public_id"]
        shard_key = row["shard_key"]
        if not instrument_public_id or not shard_key:
            raise PnlAnchorEvidenceError("opening execution pool identity is missing")
        pool_key = (instrument_public_id, shard_key)
        previous_exchange = exchange_by_pool.setdefault(pool_key, row["exchange"])
        if previous_exchange != row["exchange"]:
            raise PnlAnchorEvidenceError("one opening pool spans multiple exchanges")
        size = row["size"]
        if not math.isfinite(size) or size < 0.0:
            raise PnlAnchorEvidenceError("opening execution size is invalid")
        delta = booked_signed_quantity(
            row["side"],
            size,
            row["fee"],
            row["fee_asset"],
            base_by_instrument[instrument_public_id],
            resolve_position_quantity_unit(row["exchange"]),
        )
        if not math.isfinite(delta):
            raise PnlAnchorEvidenceError("opening execution quantity arithmetic is invalid")
        quantity = quantities.get(pool_key, 0.0) + delta
        if not math.isfinite(quantity):
            raise PnlAnchorEvidenceError("opening pool quantity arithmetic overflowed")
        if abs(quantity) < FLAT_EPSILON:
            quantity = 0.0
        quantities[pool_key] = quantity
    return {
        instrument_public_id
        for (instrument_public_id, _), quantity in quantities.items()
        if abs(quantity) >= FLAT_EPSILON
    }


def _opening_mark_requirements(
    candles: Sequence[PnlTimelineCandleRow],
    nonflat_instruments: set[str],
    quote_by_instrument: Mapping[str, str],
    valuation_ccy: str,
    t0: datetime,
) -> _FxInstrumentRequirements:
    """Collect exact t0 FX requirements only for surviving opening pools."""
    requirements: _FxInstrumentRequirements = {}
    for candle in candles:
        instrument_public_id = candle["instrument_public_id"]
        if (
            instrument_public_id in nonflat_instruments
            and candle["open_at"] + timedelta(minutes=1) == t0
            and is_positive_finite(candle["close"])
        ):
            _add_fx_minute(
                requirements,
                instrument_public_id,
                quote_by_instrument[instrument_public_id],
                valuation_ccy,
                t0,
            )
    return requirements


def _opening_marks_from_candles(
    candles: Sequence[PnlTimelineCandleRow],
    nonflat_instruments: set[str],
    quote_by_instrument: Mapping[str, str],
    valuation_ccy: str,
    rates: FxRateMap,
    planes_by_instrument: Mapping[str, FxVenueMap],
    t0: datetime,
) -> dict[str, float]:
    """Resolve exactly one positive finite t0 mark per surviving instrument."""
    marks: dict[str, float] = {}
    seen: set[str] = set()
    for candle in candles:
        instrument_public_id = candle["instrument_public_id"]
        if (
            instrument_public_id not in nonflat_instruments
            or candle["open_at"] + timedelta(minutes=1) != t0
        ):
            continue
        if instrument_public_id in seen:
            raise PnlAnchorEvidenceError("opening mark evidence is duplicated")
        seen.add(instrument_public_id)
        if not is_positive_finite(candle["close"]):
            raise PnlAnchorEvidenceError("opening mark evidence is not positive and finite")
        converted = convert_amount(
            candle["close"],
            quote_by_instrument[instrument_public_id],
            valuation_ccy,
            t0,
            rates,
            planes_by_instrument.get(instrument_public_id, {}),
        )
        if not is_positive_finite(converted):
            raise PnlAnchorEvidenceError("opening mark or FX evidence is unavailable")
        marks[instrument_public_id] = converted
    if set(marks) != nonflat_instruments:
        raise PnlAnchorEvidenceError("opening mark evidence is incomplete")
    return marks


async def _derive_anchor_candidate(
    repo: Repository,
    scope: _AnchorScope,
    execution_prefix: PnlTimelineExecutionPrefix,
) -> PortfolioPnlAnchorRow:
    """Derive one canonical anchor candidate from a single exact ledger prefix."""
    wallet_public_id = scope.wallet_public_id
    mode = cast(Literal["live", "paper"], scope.mode)
    valuation_ccy = scope.valuation_ccy
    activation_time = scope.activation_time
    knowledge_horizon = scope.knowledge_horizon
    prefix = execution_prefix
    fill_gap_reason = await _scope_has_fill_gap(
        repo,
        wallet_public_id,
        mode,
        activation_time,
        prefix,
    )
    if fill_gap_reason is not None:
        raise PnlAnchorEvidenceError("durable fill evidence has a gap at activation")
    execution_rows = prefix["executions"]
    instrument_ids = sorted({row["instrument_public_id"] for row in execution_rows})
    refs = await repo.get_instrument_symbol_refs(instrument_ids, knowledge_horizon)
    trusted_refs, untrusted_reasons = _partition_series_execution_price_refs(
        execution_rows,
        refs,
    )
    trusted_by_instrument = {ref["instrument_public_id"]: ref for ref in trusted_refs}
    if untrusted_reasons or set(trusted_by_instrument) != set(instrument_ids):
        raise PnlAnchorEvidenceError("opening execution price provenance is unproven")
    quote_by_instrument = {
        instrument_public_id: cast(str, ref["quote_currency"])
        for instrument_public_id, ref in trusted_by_instrument.items()
    }
    base_by_instrument = {
        instrument_public_id: ref["base_currency"]
        for instrument_public_id, ref in trusted_by_instrument.items()
    }
    nonflat_instruments = _opening_nonflat_instruments(
        execution_rows,
        base_by_instrument,
    )
    mark_refs = [
        trusted_by_instrument[instrument_public_id]
        for instrument_public_id in sorted(nonflat_instruments)
    ]
    mark_candles = await _load_mark_candles(
        repo,
        mark_refs,
        activation_time,
        activation_time,
        knowledge_horizon,
    )
    event_requirements = _event_fx_minutes(
        execution_rows,
        (),
        base_by_instrument,
        quote_by_instrument,
        valuation_ccy,
    )
    mark_requirements = _opening_mark_requirements(
        mark_candles,
        nonflat_instruments,
        quote_by_instrument,
        valuation_ccy,
        activation_time,
    )
    all_requirements = _merge_instrument_fx_minutes(
        mark_requirements,
        event_requirements,
    )
    identity_planes = _identity_fx_planes(
        trusted_refs,
        all_requirements,
    )
    rates, planes_by_instrument, _ = await _load_request_fx_rates(
        repo,
        mark_requirements,
        event_requirements,
        identity_planes,
        knowledge_horizon,
        valuation_ccy,
    )
    opening_marks = _opening_marks_from_candles(
        mark_candles,
        nonflat_instruments,
        quote_by_instrument,
        valuation_ccy,
        rates,
        planes_by_instrument,
        activation_time,
    )
    executions = [
        _to_timeline_execution(
            row,
            valuation_ccy,
            rates,
            base_asset=base_by_instrument[row["instrument_public_id"]],
            price_currency=quote_by_instrument[row["instrument_public_id"]],
            venues=planes_by_instrument.get(row["instrument_public_id"], {}),
        )
        for row in execution_rows
    ]
    try:
        derivation = derive_timeline_opening(
            executions,
            opening_marks,
            activation_time,
        )
    except ValueError as exc:
        raise PnlAnchorEvidenceError("opening replay or valuation cannot be proven") from exc
    opening_payload, contributions_payload = _anchor_payloads(
        derivation,
        prefix["annulments"],
    )
    public_id = portfolio_pnl_anchor_public_id(
        wallet_public_id,
        mode,
        valuation_ccy,
    )
    return PortfolioPnlAnchorRow(
        public_id=public_id,
        session_id=str(uuid7()),
        sequence_id=1,
        timestamp=knowledge_horizon,
        wallet_public_id=wallet_public_id,
        mode=mode,
        valuation_ccy=valuation_ccy,
        point_time=activation_time,
        point_kind="anchor",
        epoch_public_id=public_id,
        calc_version=PNL_TIMELINE_CALC_VERSION,
        valuation_status="complete",
        realized_pnl=0.0,
        fee_pnl=0.0,
        accrual_pnl=0.0,
        unrealized_pnl=derivation.raw_opening_unrealized_value,
        external_flow_adjustment=0.0,
        cash_usd=None,
        position_value_usd=None,
        drawdown=None,
        mark_source=PNL_TIMELINE_MARK_SOURCE,
        mark_time=activation_time,
        watermarks_json=_canonical_watermarks(prefix["watermarks"]),
        opening_basket_json=_canonical_anchor_json(opening_payload),
        contributions_json=_canonical_anchor_json(contributions_payload),
    )


def _validated_anchor_scope(scope: _AnchorScope) -> _AnchorScope:
    """Normalize one anchor scope and validate both of its UTC horizons."""
    if scope.mode not in ("live", "paper"):
        raise ValueError("P&L anchor mode must be 'live' or 'paper'")
    wallet_public_id = normalize_portfolio_pnl_wallet_public_id(scope.wallet_public_id)
    valuation_ccy = normalize_portfolio_pnl_valuation_ccy(scope.valuation_ccy)
    if (
        scope.activation_time.utcoffset() != timedelta(0)
        or scope.activation_time.second != 0
        or scope.activation_time.microsecond != 0
    ):
        raise ValueError("P&L anchor activation_time must be a UTC minute")
    if (
        scope.knowledge_horizon.utcoffset() != timedelta(0)
        or scope.activation_time > scope.knowledge_horizon
    ):
        raise ValueError("P&L anchor knowledge_horizon must be UTC and not precede activation")
    return _AnchorScope(
        wallet_public_id=wallet_public_id,
        mode=scope.mode,
        valuation_ccy=valuation_ccy,
        activation_time=scope.activation_time,
        knowledge_horizon=scope.knowledge_horizon,
        requested_horizon=scope.requested_horizon,
    )


@dataclass(frozen=True, slots=True)
class _ExecutionPrefixRequest:
    """One two-cut prefix load and the intent its horizons were taken with.

    Bundled so the loader keeps a small parameter surface while carrying the
    horizon INTENT that decides the manifest narrowing; passing the cuts without
    it would let a caller silently inherit the conservative default.
    """

    wallet_public_id: str
    mode: str
    request_as_of: datetime | None
    activation_as_of: datetime


async def _load_execution_prefix_bundle(
    repo: Repository,
    request: _ExecutionPrefixRequest,
) -> PnlTimelineExecutionPrefixBundle:
    """Load independently proven request and activation cuts in one call."""
    return await repo.get_pnl_timeline_execution_prefix_bundle(
        request.wallet_public_id,
        request.mode,
        request.request_as_of,
        request.activation_as_of,
    )


async def _load_anchor_execution_prefix_bundle(
    repo: Repository,
    scope: _AnchorScope,
) -> PnlTimelineExecutionPrefixBundle:
    """Normalize a failure to prove either exact activation bundle snapshot."""
    try:
        return await _load_execution_prefix_bundle(
            repo,
            _ExecutionPrefixRequest(
                wallet_public_id=scope.wallet_public_id,
                mode=scope.mode,
                request_as_of=scope.requested_horizon,
                activation_as_of=scope.activation_time,
            ),
        )
    except Exception as exc:
        raise PnlAnchorEvidenceError("execution prefix cannot be proven at activation") from exc


async def _record_anchor_candidate(
    repo: Repository,
    scope: _AnchorScope,
    execution_prefix_bundle: PnlTimelineExecutionPrefixBundle,
) -> _ResolvedPnlAnchor:
    """Derive, record, and fully validate one canonical activation candidate.

    The bundle handed to the writer carries each cut's applied annulment
    manifest as well as its effective rows, so the atomic equality check the
    writer performs under its fence covers correction drift with no separate
    comparison: a repudiation appended between this derivation and the insert
    fails the write exactly as a late execution does. The candidate's opening
    audit records the same corrections, so the permanent anchor states which
    ones its opening folded.
    """
    request_watermarks = execution_prefix_bundle["request"]["watermarks"]
    activation_prefix = execution_prefix_bundle["activation"]
    activation_watermarks = activation_prefix["watermarks"]
    if not _watermarks_are_valid(request_watermarks) or not _watermarks_are_valid(
        activation_watermarks
    ):
        raise PnlAnchorEvidenceError("execution prefix watermarks are not canonical")
    if any(
        exchange not in request_watermarks or request_watermarks[exchange] < activation_watermark
        for exchange, activation_watermark in activation_watermarks.items()
    ):
        raise PnlAnchorEvidenceError(
            "request execution prefix regressed below the activation watermark"
        )
    candidate = await _derive_anchor_candidate(
        repo,
        scope,
        activation_prefix,
    )
    evidence = PortfolioPnlAnchorWriteEvidence(
        wallet_public_id=scope.wallet_public_id,
        mode=cast(Literal["live", "paper"], scope.mode),
        request_as_of=scope.knowledge_horizon,
        activation_as_of=scope.activation_time,
        requested_as_of=scope.requested_horizon,
        execution_prefix_bundle=execution_prefix_bundle,
    )
    try:
        winner = await repo.record_portfolio_pnl_anchor_if_execution_prefix_matches(
            candidate,
            evidence,
        )
    except PnlTimelineAnchorEvidenceMismatchError as exc:
        raise PnlAnchorEvidenceError("execution prefix changed before anchor persistence") from exc
    return _parse_scoped_anchor(
        winner,
        scope.wallet_public_id,
        scope.mode,
        scope.valuation_ccy,
    )


async def ensure_wallet_pnl_anchor(
    repo: Repository,
    wallet_public_id: str,
    mode: str,
    valuation_ccy: str,
    activation_time: datetime,
    knowledge_horizon: datetime,
) -> PortfolioPnlAnchorRow:
    """Return or durably create one exact ledger-derived activation anchor.

    This is the only public fixture-safe writer. Explicit activation is accepted
    only at a UTC minute no later than its UTC knowledge horizon. A concurrent
    creator is resolved by the deterministic scope public id; the returned winner
    is always parsed through the full v2 contract before use.

    Args:
        repo: Repository boundary providing durable evidence and anchor writes.
        wallet_public_id: Canonical UUID identity of the portfolio wallet.
        mode: Portfolio execution mode, either ``live`` or ``paper``.
        valuation_ccy: Three-letter portfolio valuation currency.
        activation_time: Exact UTC minute at which the anchor becomes active.
        knowledge_horizon: UTC evidence horizon available to the derivation.

    Returns:
        The validated persisted anchor row, including a concurrent winner.

    Raises:
        PnlAnchorEvidenceError: If exact durable evidence cannot prove the anchor.
        ValueError: If the requested scope or timestamps are invalid.
    """
    scope = _validated_anchor_scope(
        _AnchorScope(
            wallet_public_id=wallet_public_id,
            mode=mode,
            valuation_ccy=valuation_ccy,
            activation_time=activation_time,
            knowledge_horizon=knowledge_horizon,
            requested_horizon=knowledge_horizon,
        )
    )
    existing = await repo.get_portfolio_pnl_anchor(
        scope.wallet_public_id,
        scope.mode,
        scope.valuation_ccy,
        None,
    )
    if existing is not None:
        return _parse_scoped_anchor(
            existing,
            scope.wallet_public_id,
            scope.mode,
            scope.valuation_ccy,
        ).row
    execution_prefix_bundle = await _load_anchor_execution_prefix_bundle(
        repo,
        scope,
    )
    return (await _record_anchor_candidate(repo, scope, execution_prefix_bundle)).row


async def _load_or_create_anchor(
    repo: Repository,
    request: _AnchorLoadRequest,
) -> _AnchorLoadResult:
    """Load or create an anchor while retaining its sealed request prefix."""
    scope = _validated_anchor_scope(request.scope)
    if request.preloaded_evidence is None:
        anchor_as_of = None
        if not request.allow_anchor_creation:
            anchor_as_of = scope.knowledge_horizon
        visible = await repo.get_portfolio_pnl_anchor(
            scope.wallet_public_id,
            scope.mode,
            scope.valuation_ccy,
            anchor_as_of,
        )
    else:
        visible = request.preloaded_evidence.visible_anchor
    execution_prefix_bundle = (
        None
        if request.preloaded_evidence is None
        else request.preloaded_evidence.execution_prefix_bundle
    )
    if visible is not None:
        return _AnchorLoadResult(
            scope=scope,
            anchor=_parse_scoped_anchor(
                visible,
                scope.wallet_public_id,
                scope.mode,
                scope.valuation_ccy,
            ),
            execution_prefix_bundle=execution_prefix_bundle,
        )
    if not request.allow_anchor_creation:
        return _AnchorLoadResult(
            scope=scope,
            anchor=None,
            execution_prefix_bundle=execution_prefix_bundle,
        )
    if execution_prefix_bundle is None:
        execution_prefix_bundle = await _load_anchor_execution_prefix_bundle(
            repo,
            scope,
        )
    anchor = await _record_anchor_candidate(
        repo,
        scope,
        execution_prefix_bundle,
    )
    return _AnchorLoadResult(
        scope=scope,
        anchor=anchor,
        execution_prefix_bundle=execution_prefix_bundle,
    )


def _anchor_covered_annulment_identities(
    anchor: _ResolvedPnlAnchor,
    applied: Sequence[PnlTimelineAppliedAnnulment],
) -> tuple[tuple[str, str, str, str, int], ...]:
    """Project the corrections falling inside one anchor's frozen coverage.

    An anchor's opening is derived from the effective history up to its
    per-exchange watermark; everything above it is replayed live. Only the
    corrections at or below that watermark could have changed the frozen
    numbers, so only those are compared. An exchange the anchor never covered
    contributes a watermark of zero and therefore nothing.

    Args:
        anchor: The persisted anchor whose coverage bounds the comparison.
        applied: The corrections the current certified prefix folded.

    Returns:
        Sorted ``(correction id, target id, digest, exchange, sequence)``
        identities inside the anchor's coverage.
    """
    return tuple(
        sorted(
            (
                row["public_id"],
                row["target_execution_public_id"],
                row["target_execution_digest"],
                row["exchange"],
                row["scope_sequence"],
            )
            for row in applied
            if row["scope_sequence"] <= anchor.watermarks.get(row["exchange"], 0)
        )
    )


def _require_anchor_manifest_unchanged(
    anchor: _ResolvedPnlAnchor,
    applied: Sequence[PnlTimelineAppliedAnnulment],
) -> None:
    """Refuse a series whose anchor no longer matches the manifest it froze.

    The blocker this closes. An anchor's opening economics are computed ONCE,
    from the effective history below its watermark, and then frozen forever;
    the series only ever replays the suffix ABOVE that watermark. So a
    correction that lands later and targets a booking BELOW the watermark
    changes the true opening and nothing in the replay path can notice — the
    suffix is untouched, the watermarks still agree, and the series keeps
    reporting an opening derived from a history the ledger no longer has.

    The anchor already records exactly which corrections its derivation folded
    (v3 opening audit), so the check is a direct equality: the corrections the
    CURRENT certified prefix applies within the anchor's coverage must be the
    same set, by correction id, target id, and canonical row digest. Comparing
    digests as well as ids means a correction re-bound to different row content
    is drift too.

    Resolution is deliberately an operator RE-ACTIVATION, not an automatic
    re-derivation: the anchor is the permanent activation seed for a money
    series, and silently recomputing it would erase the very evidence that the
    numbers changed. Failing closed keeps the divergence visible.

    Args:
        anchor: The persisted anchor backing this series.
        applied: The corrections the current certified prefix folded.

    Raises:
        ExecutionChainError: If the anchor's recorded corrections differ from
            the ones the current manifest applies inside its coverage.
    """
    recorded = tuple(
        sorted(
            (
                payload.public_id,
                payload.target_execution_public_id,
                payload.target_execution_digest,
                payload.exchange,
                payload.scope_sequence,
            )
            for payload in anchor.annulments
        )
    )
    current = _anchor_covered_annulment_identities(anchor, applied)
    if recorded != current:
        raise ExecutionChainError(
            "anchor_manifest_drift: "
            f"anchor_public_id={anchor.row['public_id']} "
            f"recorded={len(recorded)} current={len(current)}"
        )


async def _load_series_replay_inputs(
    repo: Repository,
    scope: _AnchorScope,
    to_time: datetime,
    anchor: _ResolvedPnlAnchor,
    execution_prefix_bundle: PnlTimelineExecutionPrefixBundle | None,
) -> _SeriesReplayInputs:
    """Load and bound the post-watermark accounting evidence for one series.

    Takes the whole :class:`_AnchorScope` rather than its parts so the read
    horizon and the INTENT it was taken with cannot be separated on the way down
    — the manifest narrowing depends on both, and a caller that could pass one
    without the other would silently inherit the conservative default.
    """
    wallet_public_id = scope.wallet_public_id
    mode = scope.mode
    as_of = scope.knowledge_horizon
    loaded_bundle = (
        await _load_execution_prefix_bundle(
            repo,
            _ExecutionPrefixRequest(
                wallet_public_id=wallet_public_id,
                mode=mode,
                request_as_of=scope.requested_horizon,
                activation_as_of=as_of,
            ),
        )
        if execution_prefix_bundle is None
        else execution_prefix_bundle
    )
    loaded_prefix = loaded_bundle["request"]
    fill_gap_reason = await _scope_has_fill_gap(
        repo,
        wallet_public_id,
        mode,
        as_of,
        loaded_prefix,
    )
    for exchange, anchor_watermark in anchor.watermarks.items():
        if loaded_prefix["watermarks"].get(exchange, 0) < anchor_watermark:
            raise PnlAnchorEvidenceError(
                "current execution prefix regressed below the activation watermark"
            )
    _require_anchor_manifest_unchanged(anchor, loaded_prefix["annulments"])
    loaded_execution_rows = [
        row
        for row in loaded_prefix["executions"]
        if row["scope_sequence"] > anchor.watermarks.get(row["exchange"], 0)
    ]
    replayed_execution_rows = _execution_rows_effective_through(
        loaded_execution_rows,
        to_time,
    )
    late_pre_activation_reason = (
        PnlIncompletenessReasonEntry(
            reason="late_pre_activation_execution",
            withholding_tier="untrusted",
            withholding_scope="global",
            trigger_instrument_public_id=None,
        )
        if any(row["timestamp"] <= anchor.opening.t0 for row in loaded_execution_rows)
        else None
    )
    order_public_ids = list(
        dict.fromkeys(row["order_public_id"] for row in replayed_execution_rows)
    )
    lineage_rows = await repo.get_pnl_timeline_execution_lineage(order_public_ids, as_of)
    accrual_rows = [
        row
        for row in await repo.get_accruals_for_pnl(wallet_public_id, mode, as_of)
        if anchor.opening.t0 < row["accrued_at"] <= to_time
    ]
    return _SeriesReplayInputs(
        loaded_execution_rows=loaded_execution_rows,
        replayed_execution_rows=replayed_execution_rows,
        accrual_rows=accrual_rows,
        lineage=_build_execution_lineage(lineage_rows),
        fill_gap_reason=fill_gap_reason,
        late_pre_activation_reason=late_pre_activation_reason,
        applied_annulments=tuple(loaded_prefix["annulments"]),
    )


def _series_display_activity_spans(
    opening: TimelineOpening,
    execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
    accrual_rows: Sequence[PnlTimelineAccrualRow],
) -> dict[str, tuple[datetime, datetime]]:
    """Return exact activity bounds used for display-identity proof."""
    activity = [(row["instrument_public_id"], row["timestamp"]) for row in execution_rows]
    activity.extend((pool.instrument_public_id, opening.t0) for pool in opening.pools)
    activity.extend((row["instrument_public_id"], row["accrued_at"]) for row in accrual_rows)
    spans: dict[str, tuple[datetime, datetime]] = {}
    for instrument_public_id, activity_time in activity:
        existing = spans.get(instrument_public_id)
        spans[instrument_public_id] = (
            (activity_time, activity_time)
            if existing is None
            else (
                min(existing[0], activity_time),
                max(existing[1], activity_time),
            )
        )
    return spans


def _sample_point_overlay(sample: PortfolioPnlSampleRow) -> PnlPointEquityOverlay | None:
    """Overlay one complete sample's finite equity trio, or withhold the minute (R4).

    The equity terms are re-checked app-side: a ``complete`` row should carry a
    finite cash/position/drawdown, but a non-finite or out-of-range value fails the
    R4 finiteness predicate and yields no overlay for that minute rather than a
    fabricated number. ``equity`` is ``cash + position_value`` from the SAME stored
    floats, guarded once more so a finite pair that overflows to infinity also
    withholds.
    """
    cash = sample["cash_usd"]
    position_value = sample["position_value_usd"]
    drawdown = sample["drawdown"]
    if (
        cash is None
        or position_value is None
        or drawdown is None
        or not math.isfinite(cash)
        or not math.isfinite(position_value)
        or not math.isfinite(drawdown)
        or not 0.0 <= drawdown <= 1.0
    ):
        return None
    equity = cash + position_value
    if not math.isfinite(equity):
        return None
    return PnlPointEquityOverlay(
        equity=equity,
        cash=cash,
        position_value=position_value,
        drawdown=drawdown,
    )


def _qualified_sample_overlays(
    request: _EquityOverlayRequest,
    samples_by_minute: Mapping[datetime, PortfolioPnlSampleRow],
) -> dict[datetime, PnlPointEquityOverlay]:
    """Validate every complete row's equity trio, logging each malformed one (M1/R4).

    A ``complete`` sample whose equity terms are null, non-finite, out of the
    ``[0, 1]`` drawdown range, or overflow on sum is a data inconsistency: it is
    dropped from the qualified set and logged loudly rather than counted toward the
    coverage disclosure, so a malformed row can never inflate ``sampled`` or the
    minute bounds.
    """
    qualified: dict[datetime, PnlPointEquityOverlay] = {}
    for minute, sample in samples_by_minute.items():
        overlay = _sample_point_overlay(sample)
        if overlay is None:
            logger.error(
                "Dropping malformed complete P&L sample at "
                f"{minute.isoformat()} for wallet {request.wallet_public_id} "
                f"mode {request.mode} epoch {request.epoch_public_id}"
            )
            continue
        qualified[minute] = overlay
    return qualified


def _point_equity_overlays(
    points: Sequence[PnlTimelinePoint],
    qualified_by_minute: Mapping[datetime, PnlPointEquityOverlay],
) -> dict[datetime, PnlPointEquityOverlay]:
    """Select each point its endpoint minute's qualified overlay (endpoint-selection).

    Because a downsampled point's ``point_time`` is its bucket endpoint minute, the
    exact-minute lookup IS endpoint-selection: a bucket whose endpoint minute has no
    qualified sample gets no overlay and the stocks are never aggregated.
    """
    return {
        point.point_time: qualified_by_minute[point.point_time]
        for point in points
        if point.point_time in qualified_by_minute
    }


def _parse_sample_coverage(audit_json: str) -> tuple[str, bool] | None:
    """Parse one sample's coverage block, or ``None`` when its audit is unreadable."""
    try:
        envelope = _SampleAuditEnvelope.model_validate_json(audit_json, strict=True)
    except ValidationError:
        return None
    return envelope.coverage.venue_scope, envelope.coverage.external_flows_adjusted


def _sample_coverage_flags(
    samples: Sequence[PortfolioPnlSampleRow],
) -> tuple[Literal["spot_only"], bool] | None:
    """Fold the samples' coverage blocks into one uniform disclosure, or fail closed.

    Returns the single shared ``(venue_scope, external_flows_adjusted)`` when every
    complete sample agrees and the scope is the supported spot-only v1 scope;
    returns ``None`` when a coverage block is unreadable, the samples disagree, or
    the scope is anything other than ``spot_only`` (R11) so the caller withholds the
    overlay for the whole response.
    """
    disclosures: set[tuple[str, bool]] = set()
    for sample in samples:
        parsed = _parse_sample_coverage(sample["audit_json"])
        if parsed is None:
            return None
        disclosures.add(parsed)
    if len(disclosures) != 1:
        return None
    venue_scope, external_flows_adjusted = next(iter(disclosures))
    if venue_scope != "spot_only":
        return None
    return "spot_only", external_flows_adjusted


def _build_equity_overlay(
    request: _EquityOverlayRequest,
    samples: Sequence[PortfolioPnlSampleRow],
) -> _EquityOverlayResult:
    """Fold the bounded complete-sample read into the response overlay (R4/D13/M1).

    Fail-closed on more than one active sample for a minute or on disagreeing
    coverage blocks: the whole response's overlay is withheld with a loud log. Each
    malformed ``complete`` row is dropped and logged. The disclosure
    (``sampled`` / ``complete_minutes`` / bounds) is computed ONLY from the rows
    that actually produced a point overlay, so a rejected malformed row can never
    inflate it; zero qualified point overlays is reported as unsampled.
    """
    if not samples:
        return _UNSAMPLED_EQUITY_OVERLAY_RESULT
    samples_by_minute: dict[datetime, PortfolioPnlSampleRow] = {}
    for sample in samples:
        if sample["point_time"] in samples_by_minute:
            logger.error(
                "Withholding P&L equity overlay: duplicate active sample at "
                f"{sample['point_time'].isoformat()} for wallet {request.wallet_public_id} "
                f"mode {request.mode} epoch {request.epoch_public_id}"
            )
            return _UNSAMPLED_EQUITY_OVERLAY_RESULT
        samples_by_minute[sample["point_time"]] = sample
    coverage_flags = _sample_coverage_flags(samples)
    if coverage_flags is None:
        logger.error(
            "Withholding P&L equity overlay: sample coverage blocks are unreadable or "
            f"disagree for wallet {request.wallet_public_id} mode {request.mode} "
            f"epoch {request.epoch_public_id}"
        )
        return _UNSAMPLED_EQUITY_OVERLAY_RESULT
    venue_scope, external_flows_adjusted = coverage_flags
    qualified = _qualified_sample_overlays(request, samples_by_minute)
    overlay = _point_equity_overlays(request.points, qualified)
    if not overlay:
        return _UNSAMPLED_EQUITY_OVERLAY_RESULT
    coverage = PnlEquityCoverage(
        sampled=True,
        venue_scope=venue_scope,
        external_flows_adjusted=external_flows_adjusted,
        complete_minutes=len(overlay),
        first_minute=min(overlay),
        last_minute=max(overlay),
        sample_calc_version=PNL_SAMPLE_CALC_VERSION,
    )
    return _EquityOverlayResult(overlay=MappingProxyType(overlay), coverage=coverage)


async def _resolve_equity_overlay(
    repo: Repository,
    request: _EquityOverlayRequest,
) -> _EquityOverlayResult:
    """Load the one bounded complete-sample read and fold it, when eligible (D13/R4).

    Only a current-truth USD scope is eligible; every other scope withholds the
    overlay without a read. The single ``get_portfolio_pnl_samples`` call applies
    the exact R4 predicate set (active, ``point_kind='sample'``, exact scope, the
    current anchor epoch, the exact sample ``calc_version``, ``complete`` status)
    over the requested grid window, so there is one bounded sample read per request.
    """
    if not request.current_truth or request.valuation_ccy != "USD":
        return _UNSAMPLED_EQUITY_OVERLAY_RESULT
    query = PortfolioPnlSampleQuery(
        wallet_public_id=request.wallet_public_id,
        mode=request.mode,
        valuation_ccy=request.valuation_ccy,
        epoch_public_id=request.epoch_public_id,
        calc_version=PNL_SAMPLE_CALC_VERSION,
    )
    samples = await repo.get_portfolio_pnl_samples(
        query,
        request.from_time,
        request.to_time,
        status="complete",
    )
    return _build_equity_overlay(request, samples)


async def _finalize_wallet_pnl_series(
    repo: Repository,
    result: PnlTimelineResult,
    assembly: _SeriesAssembly,
) -> PnlWalletSeriesResult:
    """Resolve the equity overlay and assemble the final wallet series result."""
    overlay = await _resolve_equity_overlay(repo, assembly.overlay_request)
    return PnlWalletSeriesResult(
        points=result.points,
        granularity=result.granularity,
        valuation_ccy=result.valuation_ccy,
        rate_sources=assembly.rate_sources,
        replay_metadata=assembly.replay_metadata,
        equity_overlay=overlay.overlay,
        equity_coverage=overlay.coverage,
        applied_annulments=assembly.applied_annulments,
    )


async def build_wallet_pnl_series(
    repo: Repository,
    wallet_public_id: str,
    mode: str,
    from_time: datetime,
    to_time: datetime,
    granularity: str,
    as_of: datetime,
    valuation_ccy: str = "USD",
    *,
    policy: PnlSeriesReadPolicy = _DEFAULT_READ_POLICY,
    options: PnlSeriesReplayOptions = _NO_SERIES_REPLAY_OPTIONS,
) -> PnlWalletSeriesResult:
    """Reconstruct one wallet/mode scope's Net-P&L-since-activation series.

    Loads the visible durable activation anchor, captures one exact current
    execution-prefix bundle, filters the per-exchange suffix above the frozen
    watermarks, and seeds the shard-aware pure builder from rebased opening
    pools. Historical requests never create an absent anchor and return an
    honest empty series. Durable fill gaps and suffix rows whose timestamp is at
    or before t0 globally withhold the result.

    The Phase-5B observed-equity overlay is disclosed ONLY when
    ``policy.current_truth`` is set — a live current-truth read. That capability
    is independent of ``policy.allow_anchor_creation``: permitting anchor creation
    never, by itself, attaches present-epoch equity (B1), so the snapshotter and
    every historical caller leave ``current_truth`` at its safe-by-default OFF.

    Args:
        repo: Repository providing the scope reads and candle marks.
        wallet_public_id: Wallet scope to reconstruct.
        mode: Trading mode scope (``live``, ``paper``).
        from_time: Inclusive series window start.
        to_time: Inclusive series window end.
        granularity: One of ``'1m'``, ``'5m'``, ``'1h'``, ``'1d'``.
        as_of: Effective knowledge horizon for the execution commit watermark,
            accrual SCD2 versions, and candle SCD2 versions.
        valuation_ccy: Currency the series components are expressed in.
        policy: Anchor-mutation permission (``allow_anchor_creation``) and the
            independent, safe-by-default OFF money-disclosure grant
            (``current_truth``) that alone unlocks the equity overlay. Historical
            routes pass neither capability; the snapshotter permits no overlay.
        options: Optional replay inputs (D3). Its ``preloaded_evidence`` supplies
            the marker endpoint's anchor read and prefix bundle so neither durable
            boundary read is issued twice; its ``baseline_watermarks`` supplies the
            caller's persisted per-exchange watermark map (R3) so, when the scope
            has a post-activation window, the result carries typed replay metadata.
            Omitting either leaves the corresponding behaviour off — no preloaded
            evidence and a ``None`` ``replay_metadata`` respectively.

    Returns:
        The built :class:`PnlTimelineResult` at the requested granularity.

    Raises:
        PnlTimelineWorkBudgetError: When raw grid minute-instrument work is
            above :data:`PNL_TIMELINE_MAX_WORK_UNITS`.
        ValueError: When ``granularity`` is not a supported value (surfaced by
            the pure builder).
    """
    wallet_public_id = normalize_portfolio_pnl_wallet_public_id(wallet_public_id)
    _enforce_total_work_budget(from_time, to_time, 0)
    anchor_load = await _load_or_create_anchor(
        repo,
        _AnchorLoadRequest(
            scope=_AnchorScope(
                wallet_public_id=wallet_public_id,
                mode=mode,
                valuation_ccy=valuation_ccy,
                activation_time=as_of.replace(second=0, microsecond=0),
                knowledge_horizon=as_of,
                requested_horizon=None if policy.current_truth_horizon else as_of,
            ),
            allow_anchor_creation=policy.allow_anchor_creation,
            preloaded_evidence=options.preloaded_evidence,
        ),
    )
    anchor = anchor_load.anchor
    if anchor is None:
        return _empty_pnl_series(granularity, valuation_ccy)
    if to_time < anchor.opening.t0:
        return _preactivation_pnl_series(
            anchor,
            from_time,
            to_time,
            granularity,
            valuation_ccy,
        )
    replay = await _load_series_replay_inputs(
        repo,
        anchor_load.scope,
        to_time,
        anchor,
        anchor_load.execution_prefix_bundle,
    )
    loaded_execution_rows = replay.loaded_execution_rows
    replayed_execution_rows = replay.replayed_execution_rows
    accrual_rows = replay.accrual_rows
    replay_metadata = (
        _derive_series_replay_metadata(loaded_execution_rows, options.baseline_watermarks, to_time)
        if options.baseline_watermarks is not None
        else None
    )
    execution_instrument_ids = list(
        dict.fromkeys(row["instrument_public_id"] for row in replayed_execution_rows)
    )
    opening_instrument_ids = list(
        dict.fromkeys(pool.instrument_public_id for pool in anchor.opening.pools)
    )
    instrument_ids = list(
        dict.fromkeys(
            opening_instrument_ids
            + execution_instrument_ids
            + [row["instrument_public_id"] for row in accrual_rows]
        )
    )
    pool_keys = {(pool.instrument_public_id, pool.shard_key) for pool in anchor.opening.pools}
    pool_keys.update(
        (row["instrument_public_id"], row["shard_key"]) for row in replayed_execution_rows
    )
    pool_instruments = {instrument_public_id for instrument_public_id, _ in pool_keys}
    accrual_only_instruments = {
        row["instrument_public_id"] for row in accrual_rows
    } - pool_instruments
    _enforce_total_work_budget(
        from_time,
        to_time,
        len(pool_keys) + len(accrual_only_instruments),
    )
    refs = await repo.get_instrument_symbol_refs(instrument_ids, as_of) if instrument_ids else []
    suffix_refs, untrusted_price_reasons_by_instrument = _partition_series_execution_price_refs(
        replayed_execution_rows,
        refs,
    )
    opening_spans = {
        instrument_public_id: (
            anchor.opening.t0,
            max(anchor.opening.t0, to_time),
        )
        for instrument_public_id in opening_instrument_ids
    }
    opening_refs, _ = _partition_series_price_refs(
        opening_spans,
        refs,
    )
    untrusted_price_reasons_by_instrument = {
        instrument_public_id: set(reasons)
        for instrument_public_id, reasons in untrusted_price_reasons_by_instrument.items()
    }
    trusted_ref_by_instrument = {
        ref["instrument_public_id"]: ref for ref in [*opening_refs, *suffix_refs]
    }
    trusted_refs = [
        trusted_ref_by_instrument[instrument_public_id]
        for instrument_public_id in instrument_ids
        if instrument_public_id in trusted_ref_by_instrument
    ]
    quote_by_instrument = {
        ref["instrument_public_id"]: cast(str, ref["quote_currency"]) for ref in trusted_refs
    }
    base_by_instrument = {ref["instrument_public_id"]: ref["base_currency"] for ref in trusted_refs}
    mark_candles = await _load_mark_candles(
        repo,
        trusted_refs,
        from_time,
        to_time,
        as_of,
    )
    trusted_instruments = set(quote_by_instrument)
    trusted_replayed_rows = [
        row for row in replayed_execution_rows if row["instrument_public_id"] in trusted_instruments
    ]
    display_activity_spans = _series_display_activity_spans(
        anchor.opening,
        replayed_execution_rows,
        accrual_rows,
    )
    display_identities = _instrument_display_identities(display_activity_spans, refs)
    mark_requirements = _mark_fx_minutes(
        mark_candles,
        loaded_execution_rows,
        base_by_instrument,
        quote_by_instrument,
        valuation_ccy,
        from_time,
        to_time,
        anchor.opening,
    )
    event_requirements = _event_fx_minutes(
        trusted_replayed_rows,
        accrual_rows,
        base_by_instrument,
        quote_by_instrument,
        valuation_ccy,
        anchor.opening,
    )
    instrument_requirements = _merge_instrument_fx_minutes(
        mark_requirements,
        event_requirements,
    )
    identity_planes = _identity_fx_planes(
        trusted_refs,
        instrument_requirements,
    )
    rates, planes_by_instrument, used_planes = await _load_request_fx_rates(
        repo,
        mark_requirements,
        event_requirements,
        identity_planes,
        as_of,
        valuation_ccy,
    )
    marks, mark_incompleteness_reasons = _build_marks_and_reasons_from_candles(
        mark_candles,
        quote_by_instrument,
        valuation_ccy,
        rates,
        planes_by_instrument,
    )
    marks = dict(marks)
    marks.update(anchor.marks)
    executions = [
        _to_timeline_execution(
            row,
            valuation_ccy,
            rates,
            base_asset=base_by_instrument.get(row["instrument_public_id"]),
            price_currency=quote_by_instrument.get(row["instrument_public_id"]),
            venues=planes_by_instrument.get(row["instrument_public_id"], {}),
        )
        for row in loaded_execution_rows
    ]
    accruals = [
        _to_timeline_accrual(
            row,
            valuation_ccy,
            rates,
            planes_by_instrument.get(row["instrument_public_id"], {}),
        )
        for row in accrual_rows
    ]
    window = TimelineWindow(
        from_time=from_time,
        to_time=to_time,
        granularity=granularity,
        valuation_ccy=valuation_ccy,
    )
    result = build_pnl_timeline(
        executions,
        accruals,
        marks,
        window,
        opening=anchor.opening,
        lineage=replay.lineage,
        untrusted_price_reasons_by_instrument=untrusted_price_reasons_by_instrument,
        mark_incompleteness_reasons=mark_incompleteness_reasons,
    )
    for global_reason in (
        replay.fill_gap_reason,
        replay.late_pre_activation_reason,
    ):
        if global_reason is not None:
            result = _withhold_series_for_global_reason(result, global_reason)
    result = _with_instrument_display_identities(
        result,
        display_identities,
    )
    rate_sources = tuple(
        PnlFxRateSource(
            source_currency=second if first == valuation_ccy else first,
            valuation_currency=valuation_ccy,
            base_currency=base_currency,
            quote_currency=quote_currency,
            exchange=exchange,
        )
        for (first, second), (base_currency, quote_currency, exchange) in sorted(used_planes)
    )
    return await _finalize_wallet_pnl_series(
        repo,
        result,
        _SeriesAssembly(
            rate_sources=rate_sources,
            replay_metadata=replay_metadata,
            applied_annulments=replay.applied_annulments,
            overlay_request=_EquityOverlayRequest(
                wallet_public_id=wallet_public_id,
                mode=mode,
                valuation_ccy=valuation_ccy,
                epoch_public_id=anchor.row["epoch_public_id"],
                from_time=from_time,
                to_time=to_time,
                current_truth=policy.current_truth,
                points=result.points,
            ),
        ),
    )


def _fill_marker(
    row: PnlTimelineExecutionRow,
    trusted_price_instruments: set[str],
) -> PnlFillMarker:
    """Project one execution row into a fill marker."""
    return PnlFillMarker(
        marker_time=row["timestamp"],
        instrument_public_id=row["instrument_public_id"],
        side=row["side"],
        size=row["size"],
        price=(
            row["price"]
            if row["instrument_public_id"] in trusted_price_instruments
            and is_positive_finite(row["price"])
            else None
        ),
        execution_public_id=row["public_id"],
        order_public_id=row["order_public_id"],
        status=row["status"],
    )


def _signal_marker(
    row: PnlTimelineSignalMarkerRow,
    trusted_price_instruments: set[str],
) -> PnlSignalMarker:
    """Project one signal row without inferring existence from fills alone."""
    outcome: Literal["executed", "no_fill"] = "executed" if row["has_execution"] else "no_fill"
    return PnlSignalMarker(
        marker_time=row["fired_at"],
        instrument_public_id=row["instrument_public_id"],
        side=row["side"],
        strategy_name=row["strategy_name"],
        strength=row["strength"],
        reason=row["reason"],
        price=(
            row["price"]
            if row["instrument_public_id"] in trusted_price_instruments
            and is_positive_finite(row["price"])
            else None
        ),
        signal_public_id=row["public_id"],
        outcome=outcome,
        status=outcome,
    )


def _payload_string(row: PnlTimelineAiDecisionMarkerRow, key: str) -> str | None:
    """Return one event-payload string without coercing arbitrary JSON."""
    value = row["payload"].get(key)
    return value if isinstance(value, str) else None


def _ai_decision_marker(row: PnlTimelineAiDecisionMarkerRow) -> PnlAiDecisionMarker:
    """Project one AI decision, preserving reject and no-fill outcomes."""
    decision = _payload_string(row, "decision")
    if decision == "reject" or row["new_status"] == "resolved_rejected":
        outcome: Literal["executed", "rejected", "no_fill"] = "rejected"
    elif row["has_execution"]:
        outcome = "executed"
    else:
        outcome = "no_fill"
    return PnlAiDecisionMarker(
        marker_time=row["occurred_at"],
        instrument_public_id=row["instrument_public_id"],
        strategy_public_id=row["strategy_public_id"],
        review_public_id=row["review_public_id"],
        event_public_id=row["event_public_id"],
        decision=decision,
        rationale=_payload_string(row, "rationale"),
        outcome=outcome,
        status=row["new_status"],
    )


def _marker_source_public_id(marker: PnlTimelineMarker) -> str:
    """Return the source identity used to break marker-order ties."""
    if isinstance(marker, PnlFillMarker):
        return marker.execution_public_id
    if isinstance(marker, PnlSignalMarker):
        return marker.signal_public_id
    return marker.event_public_id


def _marker_sort_key(marker: PnlTimelineMarker) -> tuple[datetime, str, str]:
    """Build the deterministic chronological marker ordering key."""
    return marker.marker_time, marker.kind, _marker_source_public_id(marker)


async def build_wallet_pnl_timeline(
    repo: Repository,
    wallet_public_id: str,
    mode: str,
    from_time: datetime,
    to_time: datetime,
    granularity: str,
    as_of: datetime,
    valuation_ccy: str = "USD",
    *,
    policy: PnlSeriesReadPolicy = _DEFAULT_READ_POLICY,
) -> PnlWalletTimelineResult:
    """Build a wallet series plus independently sourced decision markers.

    The execution prefix is loaded once and reused by the existing series
    builder. Signals and append-only AI decision events are read independently,
    which retains declined decisions and signals that never reached an order.
    The marker overlay intentionally remains active for a wholly pre-activation
    window and for historical horizons where no anchor was yet visible.
    Therefore the exact prefix remains required in both cases: an unprovable
    prefix refuses the marker endpoint instead of silently omitting fills or
    misclassifying signal outcomes, while the series-only endpoint can still
    return its honest pre-activation or empty result.
    Each independent marker read asks for ``limit + 1`` newest rows. All marker
    kinds are merged by ``(time, kind, source id)``; if the combined set exceeds
    the public cap, only the latest markers remain and ``markers_truncated`` is
    set so the omission is never silent.

    Args:
        repo: Repository providing series inputs and marker reads.
        wallet_public_id: Wallet scope to reconstruct.
        mode: Trading mode scope.
        from_time: Inclusive series and marker window start.
        to_time: Inclusive series and marker window end.
        granularity: Requested P&L point granularity.
        as_of: Effective knowledge horizon shared by the series, signals, and
            AI decision reads.
        valuation_ccy: Currency the series components are expressed in.
        policy: Anchor-mutation permission and the independent, safe-by-default
            OFF ``current_truth`` grant that alone unlocks the equity overlay,
            forwarded unchanged to the series builder.

    Returns:
        The existing P&L series and its capped marker overlay.

    Raises:
        PnlTimelineWorkBudgetError: When the series work budget is exceeded.
        ValueError: When the pure builder rejects the requested window.
    """
    wallet_public_id = normalize_portfolio_pnl_wallet_public_id(wallet_public_id)
    scope = _validated_anchor_scope(
        _AnchorScope(
            wallet_public_id=wallet_public_id,
            mode=mode,
            valuation_ccy=valuation_ccy,
            activation_time=as_of.replace(second=0, microsecond=0),
            knowledge_horizon=as_of,
            requested_horizon=None if policy.current_truth_horizon else as_of,
        )
    )
    visible_anchor = await repo.get_portfolio_pnl_anchor(
        scope.wallet_public_id,
        scope.mode,
        scope.valuation_ccy,
        None if policy.allow_anchor_creation else scope.knowledge_horizon,
    )
    execution_prefix_bundle = (
        await _load_anchor_execution_prefix_bundle(repo, scope)
        if visible_anchor is None and policy.allow_anchor_creation
        else await _load_execution_prefix_bundle(
            repo,
            _ExecutionPrefixRequest(
                wallet_public_id=scope.wallet_public_id,
                mode=scope.mode,
                request_as_of=scope.requested_horizon,
                activation_as_of=scope.knowledge_horizon,
            ),
        )
    )
    preloaded_evidence = _SeriesReadEvidence(
        visible_anchor=visible_anchor,
        execution_prefix_bundle=execution_prefix_bundle,
    )
    execution_rows = execution_prefix_bundle["request"]["executions"]
    series = await build_wallet_pnl_series(
        repo,
        wallet_public_id,
        mode,
        from_time,
        to_time,
        granularity,
        as_of,
        valuation_ccy=valuation_ccy,
        policy=policy,
        options=PnlSeriesReplayOptions(preloaded_evidence=preloaded_evidence),
    )
    read_limit = PNL_TIMELINE_MARKER_LIMIT + 1
    signal_rows = await repo.get_pnl_timeline_signals(
        wallet_public_id,
        mode,
        from_time,
        to_time,
        as_of,
        read_limit,
    )
    ai_decision_rows = await repo.get_pnl_timeline_ai_decisions(
        wallet_public_id,
        mode,
        from_time,
        to_time,
        as_of,
        read_limit,
    )
    replayed_execution_rows = [row for row in execution_rows if row["timestamp"] <= to_time]
    marker_instrument_ids = list(
        dict.fromkeys(
            [row["instrument_public_id"] for row in replayed_execution_rows]
            + [row["instrument_public_id"] for row in signal_rows if row["price"] is not None]
        )
    )
    marker_refs = (
        await repo.get_instrument_symbol_refs(marker_instrument_ids, as_of)
        if marker_instrument_ids
        else []
    )
    trusted_execution_refs, _ = _partition_execution_price_refs(
        replayed_execution_rows, marker_refs, valuation_ccy
    )
    trusted_execution_instruments = {ref["instrument_public_id"] for ref in trusted_execution_refs}
    signal_spans: dict[str, tuple[datetime, datetime]] = {}
    for row in signal_rows:
        if row["price"] is None:
            continue
        instrument_public_id = row["instrument_public_id"]
        existing = signal_spans.get(instrument_public_id)
        if existing is None:
            signal_spans[instrument_public_id] = (row["fired_at"], row["fired_at"])
        else:
            signal_spans[instrument_public_id] = (
                min(existing[0], row["fired_at"]),
                max(existing[1], row["fired_at"]),
            )
    trusted_signal_refs, _ = _partition_price_refs(signal_spans, marker_refs, valuation_ccy)
    trusted_signal_instruments = {ref["instrument_public_id"] for ref in trusted_signal_refs}
    markers: list[PnlTimelineMarker] = [
        _fill_marker(row, trusted_execution_instruments)
        for row in execution_rows
        if from_time <= row["timestamp"] <= to_time
    ]
    markers.extend(_signal_marker(row, trusted_signal_instruments) for row in signal_rows)
    markers.extend(_ai_decision_marker(row) for row in ai_decision_rows)
    markers.sort(key=_marker_sort_key)
    markers_truncated = len(markers) > PNL_TIMELINE_MARKER_LIMIT
    if markers_truncated:
        markers = markers[-PNL_TIMELINE_MARKER_LIMIT:]
    return PnlWalletTimelineResult(
        series=series,
        markers=tuple(markers),
        marker_limit=PNL_TIMELINE_MARKER_LIMIT,
        markers_truncated=markers_truncated,
        applied_annulments=tuple(execution_prefix_bundle["request"]["annulments"]),
    )
