"""Pure and persisted observation-span logic for value-versioned instrument specs.

Each row in ``instrument_spec_observation_spans`` records a maximal contiguous
run of catalog observations of one exact ``InstrumentSpec`` version row
``(instrument_public_id, spec_public_id, spec_timestamp)`` where consecutive
observations within the span are strictly less than twelve hours apart. Under
this invariant, an evaluation instant ``as_of`` is fresh for that version row
if and only if some span for that version row satisfies
``first_observed_at <= as_of < last_observed_at + 12h``.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Literal
from typing import TypeIs

from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy import tuple_
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session as SyncSession

from snapper.data.models import InstrumentSpec
from snapper.data.models import InstrumentSpecObservationSpan
from snapper.data.repository_types import InstrumentSpecObservationSpanRow
from snapper.data.repository_types import InstrumentSpecRow

SPEC_OBSERVATION_FRESHNESS: Final[timedelta] = timedelta(hours=12)

type SpanWriteDecision = Literal["noop", "extend", "insert"]
type SpecVersionKey = tuple[str, str, datetime]


@dataclass(frozen=True)
class SpecVersionIdentity:
    """Exact version-row identity used to match spans to their owning spec row."""

    instrument_public_id: str
    spec_public_id: str | None = None
    spec_timestamp: datetime | None = None


@dataclass(frozen=True)
class SpecObservationSpanTarget:
    """Validated write payload for recording one spec catalog observation."""

    instrument_public_id: str
    spec_public_id: str
    spec_timestamp: datetime
    spec_version: str | None
    observed_at: datetime
    session_id: str
    sequence_id: int

    def __post_init__(self) -> None:
        """Require aware UTC timestamps for both version identity and observation."""
        if not _is_aware(self.spec_timestamp):
            raise ValueError("spec_timestamp must be timezone-aware")
        if not _is_aware(self.observed_at):
            raise ValueError("observed_at must be timezone-aware")
        object.__setattr__(self, "spec_timestamp", self.spec_timestamp.astimezone(UTC))
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(UTC))


def _is_aware(value: datetime | None) -> TypeIs[datetime]:
    """Return whether a value is a timezone-aware datetime."""
    return isinstance(value, datetime) and value.utcoffset() is not None


def span_row_from_orm(row: InstrumentSpecObservationSpan) -> InstrumentSpecObservationSpanRow:
    """Copy an ORM observation-span row into an immutable dictionary snapshot.

    Args:
        row: Persisted ``InstrumentSpecObservationSpan`` ORM instance.

    Returns:
        Copied dictionary detached from later in-place row updates.
    """
    return InstrumentSpecObservationSpanRow(
        id=int(row.id),
        instrument_public_id=row.instrument_public_id,
        spec_public_id=row.spec_public_id,
        spec_timestamp=row.spec_timestamp,
        spec_version=row.spec_version,
        first_observed_at=row.first_observed_at,
        last_observed_at=row.last_observed_at,
        session_id=row.session_id,
        sequence_id=int(row.sequence_id),
    )


def decide_observation_span_write(
    latest: InstrumentSpecObservationSpanRow | None,
    observed_at: datetime,
    freshness: timedelta = SPEC_OBSERVATION_FRESHNESS,
) -> SpanWriteDecision:
    """Decide whether an observation is a no-op, extends the latest span, or inserts.

    Args:
        latest: The version row's latest span ordered by ``last_observed_at DESC``,
            or ``None`` when no span exists yet for this version row.
        observed_at: Aware catalog fetch instant being recorded.
        freshness: Maximum gap that remains inside one contiguous span.

    Returns:
        ``noop`` for an interior point ``first <= t <= last``, ``extend`` when
        ``0 < t - last < freshness``, or ``insert`` otherwise (including
        ``t < first`` or ``t - last >= freshness``).

    Raises:
        ValueError: When ``observed_at`` or an existing span bound is naive.
    """
    if not _is_aware(observed_at):
        raise ValueError("observed_at must be timezone-aware")
    if latest is None:
        return "insert"
    first = latest["first_observed_at"]
    last = latest["last_observed_at"]
    if not _is_aware(first) or not _is_aware(last):
        raise ValueError("span bounds must be timezone-aware")
    if first <= observed_at <= last:
        return "noop"
    if timedelta(0) < observed_at - last < freshness:
        return "extend"
    return "insert"


def matching_observation_spans(
    spans: Sequence[InstrumentSpecObservationSpanRow],
    identity: SpecVersionIdentity,
) -> tuple[InstrumentSpecObservationSpanRow, ...]:
    """Filter spans to those belonging to one exact instrument spec version row.

    Fails closed (returns an empty tuple) when ``identity`` lacks either
    ``spec_public_id`` or ``spec_timestamp``, preventing spans from another
    version row from vouching for an under-identified row.

    Args:
        spans: Candidate span snapshots.
        identity: Expected instrument and exact version-row key.

    Returns:
        Tuple of spans whose version identity matches ``identity``.
    """
    if identity.spec_public_id is None or identity.spec_timestamp is None:
        return ()
    return tuple(
        span
        for span in spans
        if span["instrument_public_id"] == identity.instrument_public_id
        and span["spec_public_id"] == identity.spec_public_id
        and span["spec_timestamp"] == identity.spec_timestamp
    )


def select_vouching_observation_span(
    spans: Sequence[InstrumentSpecObservationSpanRow],
    as_of: datetime,
    freshness: timedelta = SPEC_OBSERVATION_FRESHNESS,
) -> InstrumentSpecObservationSpanRow | None:
    """Select the deterministic span vouching for freshness at ``as_of``.

    Args:
        spans: Spans belonging to the active spec version row at ``as_of``.
        as_of: Aware evaluation instant.
        freshness: Freshness window length (default 12 hours).

    Returns:
        The vouching span with the greatest ``(last_observed_at, first_observed_at, id)``,
        or ``None`` when no span covers ``as_of``.
    """
    if not _is_aware(as_of):
        return None
    vouching = [
        span
        for span in spans
        if _is_aware(span["first_observed_at"])
        and _is_aware(span["last_observed_at"])
        and span["first_observed_at"] <= as_of < span["last_observed_at"] + freshness
    ]
    if not vouching:
        return None
    return max(
        vouching,
        key=lambda span: (span["last_observed_at"], span["first_observed_at"], span["id"]),
    )


def _spec_identity(spec: InstrumentSpecRow) -> SpecVersionIdentity:
    """Extract the optional version-row identity carried by an InstrumentSpecRow."""
    return SpecVersionIdentity(
        instrument_public_id=spec["instrument_public_id"],
        spec_public_id=spec.get("spec_public_id"),
        spec_timestamp=spec.get("spec_timestamp"),
    )


def is_observation_fresh_for_version(
    spans: Sequence[InstrumentSpecObservationSpanRow] | None,
    identity: SpecVersionIdentity,
    fallback_observed_at: datetime | None,
    as_of: datetime,
    freshness: timedelta = SPEC_OBSERVATION_FRESHNESS,
) -> bool:
    """Evaluate freshness for one spec version row using spans with fallback.

    When ``spans`` is present and non-empty, only spans matching ``identity``
    may vouch for ``as_of`` (failing closed if ``identity`` is incomplete).
    When no spans exist (rows minted before migration 0072, rollback, or
    synthetic fixtures), falls back to ``fallback_observed_at`` requiring
    ``timedelta(0) <= as_of - fallback_observed_at < freshness``. That
    fallback is sound because before migration 0072 every updater run minted a
    new ``instrument_specs`` version row (making ``spec_observed_at`` that
    row's sole real observation), whereas after 0072 every revise writes a
    span.

    Args:
        spans: Candidate observation spans loaded for the version row.
        identity: Exact version-row identity ``(instrument_public_id,
            spec_public_id, spec_timestamp)``.
        fallback_observed_at: Version row's ``spec_observed_at`` for spanless
            historical rows.
        as_of: Aware evaluation instant.
        freshness: Freshness window length (default 12 hours).

    Returns:
        Whether an observation of this spec version exists in
        ``(as_of - freshness, as_of]``.
    """
    if not _is_aware(as_of):
        return False
    if spans:
        matched = matching_observation_spans(spans, identity)
        return select_vouching_observation_span(matched, as_of, freshness) is not None
    if not _is_aware(fallback_observed_at):
        return False
    return timedelta(0) <= as_of - fallback_observed_at < freshness


def is_spec_observation_fresh(
    spec: InstrumentSpecRow,
    as_of: datetime,
    freshness: timedelta = SPEC_OBSERVATION_FRESHNESS,
) -> bool:
    """Evaluate freshness for an InstrumentSpecRow using spans with fallback.

    Delegates to :func:`is_observation_fresh_for_version`, using matching spans
    when present and falling back to ``spec["spec_observed_at"]`` when no span
    exists.

    Args:
        spec: Active specification row at ``as_of``.
        as_of: Aware evaluation instant.
        freshness: Freshness window length (default 12 hours).

    Returns:
        Whether an observation of this spec version exists in
        ``(as_of - freshness, as_of]``.
    """
    return is_observation_fresh_for_version(
        spec.get("observation_spans"),
        _spec_identity(spec),
        spec["spec_observed_at"],
        as_of,
        freshness,
    )


def latest_spec_observation_at(spec: InstrumentSpecRow) -> datetime | None:
    """Return the latest observation time for an active spec version row.

    Uses the maximum ``last_observed_at`` across matching spans when spans
    exist, falling back to ``spec["spec_observed_at"]`` when no span exists.

    Args:
        spec: Active specification row.

    Returns:
        Latest known observation instant for this version row, or ``None``.
    """
    spans = spec.get("observation_spans")
    if spans:
        matched = matching_observation_spans(spans, _spec_identity(spec))
        if matched:
            return max(span["last_observed_at"] for span in matched)
    return spec["spec_observed_at"]


def latest_instrument_spec_observation_at_sync(
    session: SyncSession,
    spec: InstrumentSpec,
) -> datetime | None:
    """Return the latest observation time for a persisted ORM spec version row.

    Queries the latest observation span for ``(spec.instrument_public_id,
    spec.public_id, spec.timestamp)`` inside the caller's sync session and
    falls back to ``spec.spec_observed_at`` when no span exists.

    Args:
        session: Caller-owned synchronous SQLAlchemy session.
        spec: Active persisted ``InstrumentSpec`` ORM version row.

    Returns:
        Latest known observation instant for ``spec``, or ``None``.
    """
    if spec.spec_observed_at is None:
        return None
    latest_observed = session.execute(
        select(InstrumentSpecObservationSpan.last_observed_at)
        .where(
            InstrumentSpecObservationSpan.instrument_public_id == spec.instrument_public_id,
            InstrumentSpecObservationSpan.spec_public_id == spec.public_id,
            InstrumentSpecObservationSpan.spec_timestamp == spec.timestamp,
        )
        .order_by(
            InstrumentSpecObservationSpan.last_observed_at.desc(),
            InstrumentSpecObservationSpan.id.desc(),
        )
        .limit(1)
    ).scalar_one_or_none()
    if _is_aware(latest_observed):
        return latest_observed
    return spec.spec_observed_at


def _latest_span_statement(
    target: SpecObservationSpanTarget,
) -> Select[InstrumentSpecObservationSpan]:
    """Build the locked latest-span lookup query for one spec version row."""
    return (
        select(InstrumentSpecObservationSpan)
        .where(
            InstrumentSpecObservationSpan.instrument_public_id == target.instrument_public_id,
            InstrumentSpecObservationSpan.spec_public_id == target.spec_public_id,
            InstrumentSpecObservationSpan.spec_timestamp == target.spec_timestamp,
        )
        .order_by(
            InstrumentSpecObservationSpan.last_observed_at.desc(),
            InstrumentSpecObservationSpan.id.desc(),
        )
        .limit(1)
        .with_for_update()
    )


def _new_span_orm(target: SpecObservationSpanTarget) -> InstrumentSpecObservationSpan:
    """Construct a fresh singleton observation span ``[t, t]``."""
    return InstrumentSpecObservationSpan(
        instrument_public_id=target.instrument_public_id,
        spec_public_id=target.spec_public_id,
        spec_timestamp=target.spec_timestamp,
        spec_version=target.spec_version,
        first_observed_at=target.observed_at,
        last_observed_at=target.observed_at,
        session_id=target.session_id,
        sequence_id=target.sequence_id,
    )


def record_instrument_spec_observation_span_sync(
    session: SyncSession,
    target: SpecObservationSpanTarget,
) -> SpanWriteDecision:
    """Record one catalog observation in ``instrument_spec_observation_spans`` (sync).

    Must run inside the caller's active transaction alongside the spec revision.

    Args:
        session: Caller-owned synchronous SQLAlchemy session.
        target: Validated version identity and catalog observation timestamp.

    Returns:
        The applied span write decision (``noop``, ``extend``, or ``insert``).
    """
    latest_orm = session.execute(_latest_span_statement(target)).scalar_one_or_none()
    if latest_orm is None:
        session.add(_new_span_orm(target))
        return "insert"
    decision = decide_observation_span_write(span_row_from_orm(latest_orm), target.observed_at)
    if decision == "noop":
        return "noop"
    if decision == "extend":
        session.execute(
            update(InstrumentSpecObservationSpan)
            .where(InstrumentSpecObservationSpan.id == latest_orm.id)
            .values(
                last_observed_at=target.observed_at,
                session_id=target.session_id,
                sequence_id=target.sequence_id,
            )
        )
        return "extend"
    session.add(_new_span_orm(target))
    return "insert"


async def record_instrument_spec_observation_span(
    session: AsyncSession,
    target: SpecObservationSpanTarget,
) -> SpanWriteDecision:
    """Record one catalog observation in ``instrument_spec_observation_spans`` (async).

    Must run inside the caller's active transaction alongside the spec revision.

    Args:
        session: Caller-owned asynchronous SQLAlchemy session.
        target: Validated version identity and catalog observation timestamp.

    Returns:
        The applied span write decision (``noop``, ``extend``, or ``insert``).
    """
    latest_orm = (await session.execute(_latest_span_statement(target))).scalar_one_or_none()
    if latest_orm is None:
        session.add(_new_span_orm(target))
        return "insert"
    decision = decide_observation_span_write(span_row_from_orm(latest_orm), target.observed_at)
    if decision == "noop":
        return "noop"
    if decision == "extend":
        await session.execute(
            update(InstrumentSpecObservationSpan)
            .where(InstrumentSpecObservationSpan.id == latest_orm.id)
            .values(
                last_observed_at=target.observed_at,
                session_id=target.session_id,
                sequence_id=target.sequence_id,
            )
        )
        return "extend"
    session.add(_new_span_orm(target))
    return "insert"


async def load_instrument_spec_observation_spans(
    session: AsyncSession,
    keys: Sequence[SpecVersionKey],
) -> dict[SpecVersionKey, tuple[InstrumentSpecObservationSpanRow, ...]]:
    """Batch-load copied observation spans keyed by exact spec version row.

    Args:
        session: Active asynchronous SQLAlchemy session.
        keys: Sequence of ``(instrument_public_id, spec_public_id, spec_timestamp)`` keys.

    Returns:
        Mapping from each matching version key to its copied spans ordered by
        ``(last_observed_at DESC, id DESC)``.
    """
    unique_keys = list(dict.fromkeys(keys))
    if not unique_keys:
        return {}
    rows = (
        (
            await session.execute(
                select(InstrumentSpecObservationSpan)
                .where(
                    tuple_(
                        InstrumentSpecObservationSpan.instrument_public_id,
                        InstrumentSpecObservationSpan.spec_public_id,
                        InstrumentSpecObservationSpan.spec_timestamp,
                    ).in_(unique_keys)
                )
                .order_by(
                    InstrumentSpecObservationSpan.last_observed_at.desc(),
                    InstrumentSpecObservationSpan.id.desc(),
                )
            )
        )
        .scalars()
        .all()
    )
    grouped: dict[SpecVersionKey, list[InstrumentSpecObservationSpanRow]] = {}
    for row in rows:
        key: SpecVersionKey = (row.instrument_public_id, row.spec_public_id, row.spec_timestamp)
        grouped.setdefault(key, []).append(span_row_from_orm(row))
    return {key: tuple(spans) for key, spans in grouped.items()}
