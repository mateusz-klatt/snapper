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
      consistency.

Locking:
    ``WeakValueDictionary[str, asyncio.Lock]`` keyed by
    ``user_public_id`` so idle users are GC'd automatically. A
    caller inside ``async with lock`` keeps a strong reference
    through the critical section, so GC cannot reclaim a live lock.

SQLite is single-process in local dev, so the asyncio lock is
sufficient.
Pricing for the 24h-notional cap is delegated to
class:`~snapper.application.pricing.usd_converter.USDConverter`.
A :class:`PriceUnavailableError` from the converter is mapped to
class:`CapsViolationError` with ``cap_type='price_unavailable'``
so the HTTP layer can return the ``caps_price_unavailable``
error code.
"""

import asyncio
import weakref
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import InvalidOperation
from uuid import uuid7

from loguru import logger

from snapper.application.pricing.usd_converter import PriceUnavailableError
from snapper.application.pricing.usd_converter import USDConverter
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.data.repository import Repository
from snapper.data.repository_types import UserTradingCapsRow

ROLLING_NOTIONAL_WINDOW = timedelta(hours=24)
ROLLING_CANCELS_WINDOW = timedelta(seconds=60)


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
          meth:`TradingCapsEnforcer.guard` without threading a
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
class Guard:
    """Payload yielded by :meth:`TradingCapsEnforcer.guard`.

    Attributes:
        submission: The :class:`TradeCommandSubmission` the caller
            passed in — echoed back so downstream ``make_row``
            helpers can read it without capturing it separately.
        assigned_public_id: UUID7 pre-generated inside the guard
            BEFORE yield so the caller can stamp it onto the
            TradeCommand row. Pre-generation keeps cap accounting
            identity consistent with the inserted row so a
            mid-insert failure can be traced to the same public_id
            seen at cap evaluation.
    """

    submission: TradeCommandSubmission
    assigned_public_id: str


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
            await self._evaluate_caps(submission)
            assigned = str(uuid7())
            yield Guard(submission=submission, assigned_public_id=assigned)

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
            the user-bound path.
        Rationale: the bypass is an *explicit* named method so the
        audit trail at every insert site reveals whether caps are
        on or off. A REST handler that silently drops the user ID
        cannot accidentally hit this path — it would call
        meth:`guard`, which fails closed with
        ``missing_user_public_id``.
        """
        assigned = str(uuid7())
        yield Guard(submission=submission, assigned_public_id=assigned)

    async def _evaluate_caps(self, submission: TradeCommandSubmission) -> None:
        """Dispatch cap evaluation on ``submission.command_type``.

        Submit / replace branches exercise quantity + open-orders
        + notional caps. Cancel branch exercises only the
        cancels-per-minute cap per.
        """
        assert (
            submission.user_public_id is not None
        ), "guard() rejected None user before reaching evaluator"
        user_public_id = submission.user_public_id
        caps = await self._repository.get_user_trading_caps(user_public_id)
        if caps is None:
            return
        if submission.command_type == "cancel":
            await self._check_cancels_cap(user_public_id, caps)
            return
        self._check_quantity_cap(submission, caps)
        await self._check_open_orders_cap(user_public_id, caps)
        await self._check_notional_cap(submission, user_public_id, caps)

    @staticmethod
    def _check_quantity_cap(submission: TradeCommandSubmission, caps: UserTradingCapsRow) -> None:
        """Reject if submitted quantity exceeds per-instrument cap.

        JSON-dict form: ``{instrument_public_id: limit}`` per
        instrument. Scalar form: single Decimal applies to every
        instrument. Missing per-instrument key means unbounded.
        """
        cap_raw = caps.get("max_order_quantity_per_instrument")
        if cap_raw is None or submission.quantity is None:
            return
        if isinstance(cap_raw, dict):
            key = submission.instrument_public_id or ""
            per_inst = cap_raw.get(key)
            if per_inst is None:
                return
            try:
                limit = Decimal(str(per_inst))
            except (InvalidOperation, TypeError) as exc:
                logger.warning(
                    "caps_enforcer: malformed per-instrument quantity cap — "
                    f"key={key} value={per_inst!r}: {exc}"
                )
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
        submission: TradeCommandSubmission,
        user_public_id: str,
        caps: UserTradingCapsRow,
    ) -> None:
        """Reject if new submission pushes rolling 24h USD above cap.

        Sum basis: ``submit_qty × submit_price`` per
        prior non-rejected row. For, prior rows where
        ``price IS NULL`` (market orders) are SKIPPED with a WARN
        log — a follow-up plan can stamp the submit-time USD
        notional into a dedicated column when needed.
        The NEW submission's notional is computed via
        meth:`USDConverter.to_usd` (falls back to
        ``CapsViolationError(price_unavailable)`` if the oracle
        is stale / missing).

        Args:
            submission: The :class:`TradeCommandSubmission` being
                evaluated.
            user_public_id: Acting user (narrowed non-None).
            caps: Active :class:`UserTradingCapsRow`.

        Raises:
            CapsViolationError: when the rolling sum + new
                notional would exceed ``max_daily_notional_usd`` or
                when the USD oracle is unavailable.
        """
        limit = caps.get("max_daily_notional_usd")
        if limit is None or submission.quantity is None:
            return
        if submission.instrument_public_id is None:
            return
        try:
            new_notional = await self._pricing.to_usd(
                submission.instrument_public_id, submission.quantity
            )
        except PriceUnavailableError as exc:
            raise CapsViolationError(
                "price_unavailable",
                detail=f"{exc.reason_code}: {exc.detail}",
            ) from exc

        since = self._now() - ROLLING_NOTIONAL_WINDOW
        rows = await self._repository.get_user_recent_submits(user_public_id, since)
        prior_sum = Decimal("0")
        skipped_market = 0
        for r in rows:
            if r["price"] is None:
                skipped_market += 1
                continue
            prior_sum += Decimal(str(r["quantity"])) * Decimal(str(r["price"]))
        if skipped_market > 0:
            logger.warning(
                "caps_enforcer: notional sum skipped "
                f"{skipped_market} market-order row(s) with price=None "
                f"for user={user_public_id}"
            )
        total = prior_sum + new_notional
        if total > Decimal(str(limit)):
            raise CapsViolationError(
                "max_daily_notional_usd",
                attempted=float(total),
                limit=float(limit),
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
