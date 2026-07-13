"""Per-user trading-caps enforcer.

Transaction-scoped async context manager spanning cap check,
insert, and commit. Holds a per-user :class:`asyncio.Lock` across
the entire guarded block so TOCTOU races between concurrent
submissions for the same user cannot bypass caps.

Two-method API:
    :meth:`guard` — user-bound path. Requires
      ``submission.user_public_id is not None`` and enforces all four
      caps (quantity, open orders, 24h USD notional, 60s cancels).
    :meth:`guard_service_principal` — strategy hot-path. Skips
      all caps and yields :class:`Guard` for UUID7 pre-generation
      consistency; still quotes the admission-time USD notional
      best-effort so the durable row can snapshot
      ``submitted_notional_usd``.

Locking:
    ``WeakValueDictionary[str, asyncio.Lock]`` keyed by
    ``user_public_id`` so idle users are GC'd automatically. A
    caller inside ``async with lock`` keeps a strong reference
    through the critical section, so GC cannot reclaim a live lock.

SQLite is single-process in local dev, so the asyncio lock is
sufficient.
Pricing for the 24h-notional cap is delegated to
:class:`~snapper.application.pricing.usd_converter.USDConverter`.
A :class:`PriceUnavailableError` from the converter is mapped to
:class:`CapsViolationError` with ``cap_type='price_unavailable'``
so the HTTP layer can return the ``caps_price_unavailable``
error code.
"""

import asyncio
import dataclasses
import weakref
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import ROUND_UP
from decimal import Decimal
from decimal import InvalidOperation
from uuid import uuid7

from loguru import logger

from snapper.application.ai_review.citation import validate_ai_review_citation_for_strategy
from snapper.application.pricing.usd_converter import PriceUnavailableError
from snapper.application.pricing.usd_converter import USDConverter
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.core.json_types import JsonObject
from snapper.data.repository import Repository
from snapper.data.repository_types import UserTradingCapsRow
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.schemas.data import CapsViolationAfterAiApproveData

ROLLING_NOTIONAL_WINDOW = timedelta(hours=24)
ROLLING_CANCELS_WINDOW = timedelta(seconds=60)

_BUS_CAPS_VIOLATION_TOPIC = "bus.caps_violation_after_ai_approve"
"""Internal bus topic that
:class:`~snapper.application.ai_review.service.AiReviewService` subscribes
to. Sole publisher is this enforcer, fired exclusively when a
:class:`CapsViolationError` is raised against a submission whose
``ai_review_public_id`` is non-None (i.e. the trade was previously
AI-approved). Strategy-side hot-path submissions and unrelated
manual REST/MCP submissions never trip this branch."""


class CapsViolationError(Exception):
    """Trading cap exceeded — raised inside the enforcer guard.

    Carries structured fields so the ``caps_violation`` HTTP
    response body can surface ``cap_type`` / ``attempted`` /
    ``limit`` to the client. ``cap_type`` takes one of
        ``max_order_quantity_per_instrument``
        ``max_open_orders``
        ``max_daily_notional_usd``
        ``max_cancels_per_minute``
        ``missing_user_public_id`` — caller invoked
          :meth:`TradingCapsEnforcer.guard` without threading a
          user; fail-closed per canonical rule (prevents
          silent cap bypass).
        ``price_unavailable`` — USDConverter could not resolve
          the submission's USD notional. Maps to
          ``caps_price_unavailable``.
    """

    def __init__(
        self,
        cap_type: str,
        attempted: float | None = None,
        limit: float | None = None,
        detail: str = "",
    ) -> None:
        """Initialize with structured fields for HTTP error mapping."""
        self.cap_type = cap_type
        self.attempted = attempted
        self.limit = limit
        self.detail = detail
        parts = [f"cap {cap_type} violated"]
        if attempted is not None and limit is not None:
            parts.append(f"attempted={attempted} limit={limit}")
        if detail:
            parts.append(detail)
        super().__init__(" — ".join(parts))


@dataclass(frozen=True)
class _Valuation:
    """Resolved valuation identity for one admission (internal).

    Attributes:
        valuation_public_id: Identity the USD oracle and source-keyed
            caps use, or ``None`` when the submission carries no
            instrument identity or the resolution failed.
        is_paper: True when the emission identity is a paper
            instrument.
        mapped: True only when a paper identity resolved to an
            EXISTING source instrument.
        failed: True when the repository resolution raised — keyed
            caps must then fail closed (an unresolved identity must
            never read as unbounded).
    """

    valuation_public_id: str | None
    is_paper: bool
    mapped: bool
    failed: bool


@dataclass(frozen=True)
class _NotionalQuote:
    """Admission-time USD quote split into its two consumers (internal).

    Attributes:
        comparison: Precision-safe Decimal for the rolling-cap
            comparison — present even when the value cannot be stored
            (an absurdly large notional must still trip the cap).
        storage: Cent-quantized value representable in NUMERIC(18,2),
            persisted as ``trade_commands.submitted_notional_usd``;
            ``None`` when unquotable or unrepresentable.
    """

    comparison: Decimal | None
    storage: float | None


@dataclass(frozen=True)
class Guard:
    """Payload yielded by :meth:`TradingCapsEnforcer.guard`.

    Attributes:
        submission: The :class:`TradeCommandSubmission` the caller
            passed in — echoed back so downstream ``make_row``
            helpers can read it without capturing it separately.
            The AI-attribution gate yields the ATTRIBUTED rebuild
            (resolved ``user_public_id``), so insert sites must read
            identity fields from here, not from their original
            submission.
        assigned_public_id: UUID7 pre-generated inside the guard
            BEFORE yield so the caller can stamp it onto the
            TradeCommand row. Pre-generation keeps cap accounting
            identity consistent with the inserted row so a
            mid-insert failure can be traced to the same public_id
            seen at cap evaluation.
        submitted_notional_usd: Admission-time USD quote for the
            submission (``USDConverter.to_usd`` of the submitted
            quantity, quantized UP to whole cents), or ``None`` when
            the submission is a cancel, carries no quantity or
            instrument identity, or the oracle could not price it
            and no daily-notional cap forced a fail-closed reject.
            Insert sites persist this verbatim as
            ``trade_commands.submitted_notional_usd`` — an admission
            snapshot, not a universal notional guarantee
            (compensation and guard-scanner inserts bypass the
            enforcer and legitimately stay NULL).
    """

    submission: TradeCommandSubmission
    assigned_public_id: str
    submitted_notional_usd: float | None = None


class TradingCapsEnforcer:
    """Process-singleton enforcer wired at app startup.

    Constructor
        ``TradingCapsEnforcer(repository, pricing, now=...)``
        ``repository`` — shared :class:`Repository` singleton for
          cap lookups + recent-command projections.
        ``pricing`` — shared :class:`USDConverter` for notional
          math.
        ``now`` — optional wall-clock injector; tests override
          to control the 24h / 60s sliding windows
          deterministically.
    """

    def __init__(
        self,
        repository: Repository,
        pricing: USDConverter,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        """Store dependencies and initialize per-user lock map."""
        self._repository = repository
        self._pricing = pricing
        self._now: Callable[[], datetime] = now or (lambda: datetime.now(UTC))
        self._user_locks: weakref.WeakValueDictionary[str, asyncio.Lock] = (
            weakref.WeakValueDictionary()
        )
        self._map_lock = asyncio.Lock()
        self._msg_publisher: MessagePublisher | None = None

    def set_msg_publisher(self, publisher: MessagePublisher | None) -> None:
        """Inject the bus publisher used for ``bus.caps_violation_after_ai_approve``.

        The FastAPI lifespan calls this once the shared ZMQ PUB socket is
        available; tests pass a fake publisher (or ``None`` to clear).
        Decoupling socket ownership from the enforcer keeps the cap path
        trivially testable without ZMQ. Mirrors
        :meth:`AiReviewService.set_msg_publisher` so the same shared
        publisher socket fans out admin.* + ai_reviews.* + bus.* topics
        without doubling broker connection count.

        Args:
            publisher: Configured ``MessagePublisher`` or ``None`` to clear.
        """
        self._msg_publisher = publisher

    async def _get_user_lock(self, user_public_id: str) -> asyncio.Lock:
        """Return the shared :class:`asyncio.Lock` for a user.

        Uses a sync map-mutation lock to ensure two callers racing
        on ``setdefault`` end up sharing the same :class:`Lock`
        instance.
        """
        async with self._map_lock:
            lock = self._user_locks.get(user_public_id)
            if lock is None:
                lock = asyncio.Lock()
                self._user_locks[user_public_id] = lock
            return lock

    @asynccontextmanager
    async def guard(self, submission: TradeCommandSubmission) -> AsyncIterator[Guard]:
        """User-bound enforcement context — fails closed on missing user.

        Args:
            submission: Pre-insert DTO carrying the acting user,
                command type, instrument, and quantity that drive
                cap evaluation.

        Yields:
            A :class:`Guard` echoing the submission + a pre-
            generated UUID7 ``assigned_public_id``. The caller
            performs the DB insert + commit inside the ``async
            with`` block; the per-user lock releases on context
            exit whether the body succeeded or raised.

        Given: a :class:`TradeCommandSubmission` with a non-None
            ``user_public_id``,
        When: the caller enters ``async with enforcer.guard(s):``,
        Then: the enforcer (a) acquires the per-user asyncio lock,
            (b) loads the user's caps row, (c) evaluates every
            applicable cap for ``submission.command_type``, (d)
            pre-generates ``assigned_public_id`` (UUID7), (e)
            yields :class:`Guard` to the caller for the DB insert,
            (f) releases the lock on context exit (whether the
            ``async with`` body committed or raised).

        Raises:
            CapsViolationError: if any cap is exceeded, the user
                is missing, or the USD price oracle is
                unavailable.
        """
        if submission.user_public_id is None:
            raise CapsViolationError(
                "missing_user_public_id",
                detail=(
                    "guard() requires submission.user_public_id — "
                    "service principals must call guard_service_principal()"
                ),
            )
        lock = await self._get_user_lock(submission.user_public_id)
        async with lock:
            try:
                quoted_notional = await self._evaluate_caps(submission)
            except CapsViolationError as exc:
                await self._publish_caps_violation_after_ai_approve(submission, exc)
                raise
            assigned = str(uuid7())
            yield Guard(
                submission=submission,
                assigned_public_id=assigned,
                submitted_notional_usd=quoted_notional,
            )

    @asynccontextmanager
    async def guard_service_principal(
        self, submission: TradeCommandSubmission
    ) -> AsyncIterator[Guard]:
        """Strategy hot-path bypass — no caps, no lock, just UUID7.

        Args:
            submission: Pre-insert DTO for the service-principal
                emission. ``user_public_id`` is expected to be
                ``None`` here; other fields are echoed on the
                yielded :class:`Guard` for the caller's insert
                row construction.

        Yields:
            A :class:`Guard` carrying the original submission + a
            freshly-generated UUID7 ``assigned_public_id``. No lock
            is held and no cap state is consulted.
        Given: a :class:`TradeCommandSubmission` from a service
            principal (strategy engine) where no user is the
            actor
        When: the caller enters ``async with
            enforcer.guard_service_principal(s):``
        Then: cap evaluation is skipped entirely. No lock is
            acquired (no user to contend on).
            ``assigned_public_id`` is still pre-generated so the
            engine sees the same identity-generation pattern as
            the user-bound path. The admission-time USD notional is
            still quoted best-effort (never fail-closed — no user
            means no notional cap) so the durable command row can
            snapshot ``submitted_notional_usd`` when the oracle
            resolves.
        Rationale: the bypass is an *explicit* named method so the
        audit trail at every insert site reveals whether caps are
        on or off. A REST handler that silently drops the user ID
        cannot accidentally hit this path — it would call
        :meth:`guard`, which fails closed with
        ``missing_user_public_id``.
        """
        if submission.command_type != "cancel":
            valuation = await self._resolve_valuation(submission)
        else:
            valuation = _Valuation(
                valuation_public_id=None, is_paper=False, mapped=False, failed=False
            )
        quote = await self._quote_submitted_notional(submission, valuation, fail_closed=False)
        assigned = str(uuid7())
        yield Guard(
            submission=submission,
            assigned_public_id=assigned,
            submitted_notional_usd=quote.storage,
        )

    @asynccontextmanager
    async def guard_with_ai_review_attribution(
        self,
        submission: TradeCommandSubmission,
        *,
        ai_review_public_id: str,
        ai_review_dispatch_version: int | None,
    ) -> AsyncIterator[Guard]:
        """Strategy hot-path AI-attribution gate.

        Validates the citation, resolves ``user_public_id`` from the
        cited ``ai_reviews`` row, rebuilds the submission with the
        attribution + resolved user identity, and delegates to
        :meth:`guard` so caps actually evaluate and
        ``bus.caps_violation_after_ai_approve`` fires on rejection.
        Mirrors the REST/MCP attribution pattern.

        Citation validation invariants (per
        :func:`validate_ai_review_citation_for_strategy`):

        - Row exists.
        - ``row.status == "resolved_approved"`` (catches
          supersede-after-await race).
        - ``row.wallet_public_id == submission.wallet_public_id``
          (defends against engine misrouting).

        ``ai_review_dispatch_version`` is transport-only:
        this method receives the value, threads it onto the
        rebuilt submission, but the validator deliberately does NOT
        compare it against the row's current ``dispatch_version``.
        Dispatch-version dedup is enforced at the bus publisher.

        Args:
            submission: Pre-insert DTO from the engine's strategy
                hot-path. Carries a non-empty ``wallet_public_id``;
                the user_public_id is intentionally None and is
                resolved from the cited row inside this method.
            ai_review_public_id: Citation forwarded from
                :class:`SignalData` after the strategy primitive
                returned a successful CONSULT outcome.
            ai_review_dispatch_version: Companion to
                ``ai_review_public_id`` (transport-only).

        Yields:
            A :class:`Guard` from the underlying :meth:`guard`
            invocation, carrying the attributed submission with the
            resolved user.

        Raises:
            CapsViolationError: When ``submission.wallet_public_id``
                is empty (fail-closed before the row fetch) OR when
                :meth:`guard` raises on a cap violation.
            AiReviewCitationError: When the citation fails any of
                the three strategy-validator invariants.
        """
        if not submission.wallet_public_id:
            raise CapsViolationError(
                "missing_wallet_for_ai_review_attribution",
                detail=(
                    "submission.wallet_public_id required for the strategy "
                    "AI gate; engine state must populate wallet on the "
                    "submission before invoking this guard"
                ),
            )
        review = await validate_ai_review_citation_for_strategy(
            self._repository,
            ai_review_public_id=ai_review_public_id,
            expected_wallet_public_id=submission.wallet_public_id,
        )
        attributed = dataclasses.replace(
            submission,
            user_public_id=review["user_public_id"],
            ai_review_public_id=ai_review_public_id,
            ai_review_dispatch_version=ai_review_dispatch_version,
        )
        async with self.guard(attributed) as guard:
            yield guard

    async def _evaluate_caps(self, submission: TradeCommandSubmission) -> float | None:
        """Dispatch cap evaluation on ``submission.command_type``.

        Submit / replace branches exercise quantity + open-orders
        + notional caps. Cancel branch exercises only the
        cancels-per-minute cap. Cap ordering is stable: quantity and
        open-order checks run BEFORE any pricing so a quantity
        violation is reported even when the oracle is down.

        Returns:
            The admission-time USD notional quote for submit-type
            commands (persisted as
            ``trade_commands.submitted_notional_usd``), or ``None``
            for cancels and unpriceable submissions. The quote is
            computed exactly once regardless of whether a
            daily-notional cap is configured; only a configured cap
            makes an oracle failure fail-closed.
        """
        assert (
            submission.user_public_id is not None
        ), "guard() rejected None user before reaching evaluator"
        user_public_id = submission.user_public_id
        caps = await self._repository.get_user_trading_caps(user_public_id)
        if submission.command_type == "cancel":
            if caps is not None:
                await self._check_cancels_cap(user_public_id, caps)
            return None
        valuation = await self._resolve_valuation(submission)
        if caps is not None:
            self._check_quantity_cap(submission, caps, valuation)
            await self._check_open_orders_cap(user_public_id, caps)
        notional_limit = caps.get("max_daily_notional_usd") if caps is not None else None
        quote = await self._quote_submitted_notional(
            submission, valuation, fail_closed=notional_limit is not None
        )
        if notional_limit is not None and quote.comparison is not None:
            await self._check_notional_cap(user_public_id, quote.comparison, float(notional_limit))
        return quote.storage

    async def _resolve_valuation(self, submission: TradeCommandSubmission) -> _Valuation:
        """Resolve the canonical VALUATION identity for the submission.

        Paper strategy emits carry the PAPER instrument's public id,
        while market snapshots (the USD oracle's key) and operator cap
        configuration live under the SOURCE venue's instrument. The
        repository maps paper→source through
        ``instruments.source_exchange`` (clock-free); non-paper
        instruments resolve to themselves. Resolved ONCE per guard and
        reused for both the per-instrument quantity-cap key and the
        USD notional quote. A repository failure is captured on the
        result (``failed=True``) rather than propagated — keyed caps
        then fail closed while cap-less admissions proceed with a NULL
        snapshot.

        Args:
            submission: The submission being admitted.

        Returns:
            The resolved :class:`_Valuation`.
        """
        if submission.instrument_public_id is None:
            return _Valuation(valuation_public_id=None, is_paper=False, mapped=False, failed=False)
        try:
            resolution = await self._repository.resolve_source_instrument_public_id(
                submission.instrument_public_id
            )
        except Exception as exc:
            logger.warning(
                "caps_enforcer: source-identity resolution failed for "
                f"instrument={submission.instrument_public_id}: {exc}; "
                "keyed caps fail closed, notional snapshot stays NULL"
            )
            return _Valuation(valuation_public_id=None, is_paper=True, mapped=False, failed=True)
        return _Valuation(
            valuation_public_id=resolution["valuation_public_id"],
            is_paper=resolution["is_paper"],
            mapped=resolution["mapped"],
            failed=False,
        )

    async def _quote_submitted_notional(
        self,
        submission: TradeCommandSubmission,
        valuation: _Valuation,
        *,
        fail_closed: bool,
    ) -> _NotionalQuote:
        """Value the submission in USD at admission time.

        Computes ``USDConverter.to_usd(valuation_pid, quantity)`` —
        the current-mark valuation that works for market orders (no
        submit price required), keyed by the SOURCE identity so paper
        strategy emits price against the real venue's snapshot — and
        quantizes the result UPWARD to whole cents so later
        ``NUMERIC(18,2)`` accounting reproduces admission semantics
        without under-counting. The comparison value survives even
        when the quantized value cannot be represented in
        ``NUMERIC(18,2)`` (an absurdly large notional must still trip
        the cap while the storage snapshot stays NULL).

        Args:
            submission: The submission being admitted.
            valuation: Resolved identity from
                :meth:`_resolve_valuation`.
            fail_closed: ``True`` when a daily-notional cap is
                configured for the acting user — an oracle failure or
                unresolved identity must then reject the command
                (``price_unavailable``), matching the pre-existing
                cap semantics. ``False`` keeps the quote strictly
                best-effort: failures log a WARN and yield an empty
                quote so optional recording never becomes a new
                rejection path.

        Returns:
            The :class:`_NotionalQuote` (both fields ``None`` when
            the submission is a cancel, lacks quantity or identity,
            or the oracle could not price it in best-effort mode).

        Raises:
            CapsViolationError: with ``cap_type='price_unavailable'``
                when ``fail_closed`` is set and the value cannot be
                resolved.
        """
        if submission.command_type == "cancel" or submission.quantity is None:
            return _NotionalQuote(comparison=None, storage=None)
        valuation_public_id = valuation.valuation_public_id
        if valuation_public_id is None:
            if fail_closed and valuation.failed:
                raise CapsViolationError(
                    "price_unavailable",
                    detail="source identity resolution failed",
                )
            return _NotionalQuote(comparison=None, storage=None)
        try:
            raw_notional = await self._pricing.to_usd(valuation_public_id, submission.quantity)
        except PriceUnavailableError as exc:
            if fail_closed:
                raise CapsViolationError(
                    "price_unavailable",
                    detail=f"{exc.reason_code}: {exc.detail}",
                ) from exc
            logger.warning(
                "caps_enforcer: submitted-notional quote unavailable for "
                f"instrument={valuation_public_id} "
                f"({exc.reason_code}); persisting NULL notional"
            )
            return _NotionalQuote(comparison=None, storage=None)
        return self._build_notional_quote(
            raw_notional,
            valuation_public_id,
            fail_closed=fail_closed,
        )

    @staticmethod
    def _build_notional_quote(
        raw_notional: Decimal,
        valuation_public_id: str,
        *,
        fail_closed: bool,
    ) -> _NotionalQuote:
        """Validate and normalize a raw admission-time USD notional.

        Args:
            raw_notional: Decimal returned by the USD oracle.
            valuation_public_id: Source instrument identity used for logs.
            fail_closed: Whether invalid oracle values must reject admission.

        Returns:
            Precision-safe comparison and representable storage values.

        Raises:
            CapsViolationError: When the oracle value is invalid and a
                daily-notional cap requires fail-closed evaluation.
        """
        if not raw_notional.is_finite():
            if fail_closed:
                raise CapsViolationError(
                    "price_unavailable",
                    detail="non-finite USD notional from the oracle",
                )
            logger.warning(
                "caps_enforcer: non-finite USD notional for "
                f"instrument={valuation_public_id}; persisting NULL notional"
            )
            return _NotionalQuote(comparison=None, storage=None)
        if raw_notional <= 0:
            if fail_closed:
                raise CapsViolationError(
                    "price_unavailable",
                    detail="non-positive USD notional from the oracle",
                )
            logger.warning(
                "caps_enforcer: non-positive USD notional for "
                f"instrument={valuation_public_id}; persisting NULL notional "
                "(a negative snapshot would shrink later rolling sums)"
            )
            return _NotionalQuote(comparison=None, storage=None)
        try:
            quantized = raw_notional.quantize(Decimal("0.01"), rounding=ROUND_UP)
        except InvalidOperation:
            quantized = None
        comparison = quantized if quantized is not None else raw_notional
        storage: float | None = None
        if quantized is not None and Decimal("0") < quantized < Decimal("1e15"):
            storage = float(quantized)
        else:
            logger.warning(
                "caps_enforcer: USD notional not representable in NUMERIC(18,2) "
                f"for instrument={valuation_public_id}; persisting NULL "
                "notional (cap comparison still applies). The 1e15 bound leaves "
                "float-rounding headroom below the column maximum"
            )
        return _NotionalQuote(comparison=comparison, storage=storage)

    @staticmethod
    def _resolve_per_instrument_quantity_limit(
        submission: TradeCommandSubmission,
        cap_by_instrument: JsonObject,
        valuation: _Valuation,
    ) -> Decimal | None:
        """Resolve and parse a source-keyed per-instrument quantity limit.

        Args:
            submission: Submission carrying the emission identity.
            cap_by_instrument: Quantity limits keyed by instrument identity.
            valuation: Resolved source identity and mapping status.

        Returns:
            Parsed limit, or ``None`` when the mapped instrument is
            unbounded or its configured value is malformed.

        Raises:
            CapsViolationError: When a paper source identity is unresolved
                and no legacy emission-keyed limit can be applied.
        """
        unresolved = valuation.failed or (valuation.is_paper and not valuation.mapped)
        key = valuation.valuation_public_id or submission.instrument_public_id or ""
        per_inst = cap_by_instrument.get(key)
        if per_inst is None:
            legacy_key = submission.instrument_public_id or ""
            if legacy_key and legacy_key != key:
                per_inst = cap_by_instrument.get(legacy_key)
                if per_inst is not None:
                    logger.warning(
                        "caps_enforcer: per-instrument quantity cap matched the "
                        f"legacy emission key {legacy_key} — re-key it to the "
                        f"source identity {key}"
                    )
        if per_inst is None:
            if unresolved:
                raise CapsViolationError(
                    "max_order_quantity_per_instrument",
                    detail=(
                        "source identity unresolved for paper instrument "
                        f"{submission.instrument_public_id} — per-instrument "
                        "caps fail closed until the source_exchange mapping "
                        "exists (run the paper publisher to author it)"
                    ),
                )
            return None
        try:
            return Decimal(str(per_inst))
        except (InvalidOperation, TypeError) as exc:
            logger.warning(
                "caps_enforcer: malformed per-instrument quantity cap — "
                f"key={key} value={per_inst!r}: {exc}"
            )
            return None

    @staticmethod
    def _check_quantity_cap(
        submission: TradeCommandSubmission,
        caps: UserTradingCapsRow,
        valuation: _Valuation,
    ) -> None:
        """Reject if submitted quantity exceeds per-instrument cap.

        JSON-dict form is keyed by source identity with a legacy emission-key
        fallback. Scalar form applies one Decimal to every instrument.
        Missing mapped keys and malformed values remain unbounded.
        """
        cap_raw = caps.get("max_order_quantity_per_instrument")
        if cap_raw is None or submission.quantity is None:
            return
        if isinstance(cap_raw, dict):
            limit = TradingCapsEnforcer._resolve_per_instrument_quantity_limit(
                submission,
                cap_raw,
                valuation,
            )
            if limit is None:
                return
        else:
            try:
                limit = Decimal(str(cap_raw))
            except (InvalidOperation, TypeError) as exc:
                logger.warning(f"caps_enforcer: malformed scalar quantity cap: {exc}")
                return
        if submission.quantity > limit:
            raise CapsViolationError(
                "max_order_quantity_per_instrument",
                attempted=float(submission.quantity),
                limit=float(limit),
            )

    async def _check_open_orders_cap(self, user_public_id: str, caps: UserTradingCapsRow) -> None:
        """Reject if user's active submit+replace count would exceed cap.

        Args:
            user_public_id: Acting user (already non-None — caller
                narrowed the optional at guard entry).
            caps: Active :class:`UserTradingCapsRow` for the user.
        """
        limit = caps.get("max_open_orders")
        if limit is None:
            return
        current = await self._repository.count_user_open_commands(user_public_id)
        if current + 1 > limit:
            raise CapsViolationError(
                "max_open_orders",
                attempted=float(current + 1),
                limit=float(limit),
            )

    async def _check_notional_cap(
        self,
        user_public_id: str,
        new_notional: Decimal,
        limit: float,
    ) -> None:
        """Reject if new submission pushes rolling 24h USD above cap.

        Sum precedence per prior non-rejected LIVE row (``mode='paper'``
        history is excluded by
        :meth:`Repository.get_user_recent_submits` because paper
        commands carry a simulator reference price and simulated
        notional must not consume the user's live allowance):

        1. ``submitted_notional_usd`` — the admission-time USD quote
           snapshotted on the row (covers market orders).
        2. Legacy ``quantity × price`` when the snapshot is NULL but
           a submit price exists (pre-snapshot limit orders).
        3. Skip with a WARN log when the row carries neither (legacy
           market orders — no submit-time valuation survives).

        The CURRENT submission's ``new_notional`` was quoted by
        :meth:`_quote_submitted_notional` (fail-closed, since a cap is
        configured whenever this method runs).

        Args:
            user_public_id: Acting user (narrowed non-None).
            new_notional: Admission quote for the current submission.
            limit: Configured ``max_daily_notional_usd``.

        Raises:
            CapsViolationError: when the rolling sum + new
                notional would exceed ``max_daily_notional_usd``.
        """
        since = self._now() - ROLLING_NOTIONAL_WINDOW
        rows = await self._repository.get_user_recent_submits(user_public_id, since)
        prior_sum = Decimal("0")
        skipped_unpriced = 0
        for r in rows:
            stored_notional = r["submitted_notional_usd"]
            if stored_notional is not None:
                prior_sum += Decimal(str(stored_notional))
            elif r["price"] is not None:
                prior_sum += Decimal(str(r["quantity"])) * Decimal(str(r["price"]))
            else:
                skipped_unpriced += 1
        if skipped_unpriced > 0:
            logger.warning(
                "caps_enforcer: notional sum skipped "
                f"{skipped_unpriced} row(s) missing both submitted "
                f"notional and price for user={user_public_id}"
            )
        total = prior_sum + new_notional
        if total > Decimal(str(limit)):
            raise CapsViolationError(
                "max_daily_notional_usd",
                attempted=float(min(total, Decimal("1e300"))),
                limit=float(limit),
            )

    async def _publish_caps_violation_after_ai_approve(
        self, submission: TradeCommandSubmission, exc: CapsViolationError
    ) -> None:
        """Emit a bus event for an AI-approved trade rejected by caps.

        Fires only when the submission carries an
        ``ai_review_public_id`` (i.e. the trade was previously
        AI-approved via the CONSULT pattern) AND a
        :class:`CapsViolationError` is being raised. Loads the
        ``ai_reviews`` row to populate ``strategy_public_id`` and
        ``dispatch_version`` (dedup key) which the
        :class:`TradeCommandSubmission` does not carry. Pricing-oracle
        violations (``cap_type == "price_unavailable"``) and missing-user
        violations (``cap_type == "missing_user_public_id"``) are
        explicitly skipped — neither maps to a delegate-actionable
        rejection on the WS UI.

        Best-effort + race-safe: the entire helper body is wrapped in
        a top-level try/except so NO failure in this branch (missing
        publisher, get_ai_review raising, payload construction error,
        broker hiccup, lifespan racing in to clear the publisher slot
        mid-await) can replace the original :class:`CapsViolationError`
        the caller is about to raise. The trade rejection is the
        primary contract; the bus broadcast is auxiliary fanout. The
        publisher reference is captured into a local at the top of the
        helper before any await so a concurrent
        :meth:`set_msg_publisher` cannot null the slot mid-helper after
        the None-check passes (shutdown race).

        Cap-type filters (early-return without log noise): pricing
        oracle (``"price_unavailable"``) and missing-user
        (``"missing_user_public_id"``) — neither maps to a
        delegate-actionable rejection on the WS UI.

        Args:
            submission: The :class:`TradeCommandSubmission` that
                tripped the cap.
            exc: The :class:`CapsViolationError` about to propagate to
                the caller.
        """
        if submission.ai_review_public_id is None:
            return
        if exc.cap_type in ("price_unavailable", "missing_user_public_id"):
            return
        if exc.attempted is None or exc.limit is None:
            logger.warning(
                "caps_enforcer: bus.caps_violation_after_ai_approve NOT broadcast for "
                f"review_public_id={submission.ai_review_public_id} cap_type={exc.cap_type}: "
                "attempted/limit field missing on CapsViolationError "
                "(future cap_type without numeric bounds; not delegate-actionable)"
            )
            return
        publisher = self._msg_publisher
        if publisher is None:
            logger.warning(
                "caps_enforcer: bus.caps_violation_after_ai_approve NOT broadcast for "
                f"review_public_id={submission.ai_review_public_id} cap_type={exc.cap_type}: "
                "publisher unavailable"
            )
            return
        try:
            review = await self._repository.get_ai_review(submission.ai_review_public_id)
            if review is None:
                logger.warning(
                    "caps_enforcer: bus.caps_violation_after_ai_approve NOT broadcast for "
                    f"review_public_id={submission.ai_review_public_id}: ai_review row not found"
                )
                return
            tracker = publisher.tracker
            payload = CapsViolationAfterAiApproveData(
                public_id=str(uuid7()),
                timestamp=self._now(),
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(_BUS_CAPS_VIOLATION_TOPIC),
                review_public_id=submission.ai_review_public_id,
                user_public_id=review["user_public_id"],
                strategy_public_id=review["strategy_public_id"],
                wallet_public_id=review["wallet_public_id"],
                instrument_public_id=review["instrument_public_id"],
                cap_type=exc.cap_type,
                attempted=exc.attempted,
                limit=exc.limit,
                dispatch_version=review["dispatch_version"],
            )
            await publisher.send(_BUS_CAPS_VIOLATION_TOPIC, payload)
        except Exception as publish_exc:
            logger.exception(
                "caps_enforcer: failed to broadcast bus.caps_violation_after_ai_approve "
                f"for review_public_id={submission.ai_review_public_id}: {publish_exc} "
                "(original CapsViolationError still propagates to caller)"
            )

    async def _check_cancels_cap(self, user_public_id: str, caps: UserTradingCapsRow) -> None:
        """Reject if user's 60s cancel-submit count would exceed cap.

        Args:
            user_public_id: Acting user (narrowed non-None).
            caps: Active :class:`UserTradingCapsRow`.
        """
        limit = caps.get("max_cancels_per_minute")
        if limit is None:
            return
        since = self._now() - ROLLING_CANCELS_WINDOW
        current = await self._repository.count_user_rolling_cancels(user_public_id, since)
        if current + 1 > limit:
            raise CapsViolationError(
                "max_cancels_per_minute",
                attempted=float(current + 1),
                limit=float(limit),
            )
