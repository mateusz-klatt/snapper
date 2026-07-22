"""AI delegate lifecycle management.

Bundles the atomic create-delegate flow + the read/update/
deactivate helpers that back the ``/api/ai-delegates`` routes.
Creation is the load-bearing path: a single DB transaction
inserts the :class:`~snapper.data.models.User` row (with
``role=AI_DELEGATE`` and ``created_by_user_public_id`` pointing
at the creating operator) and the per-delegate
:class:`~snapper.data.models.UserTradingCaps` row. The minted
access JWT is long-lived (~3 months) and lives only in the
client; Snapper does not store it.
"""

import asyncio
import secrets
import uuid
from datetime import UTC
from datetime import datetime
from typing import ClassVar
from uuid import uuid7

import bcrypt
from loguru import logger
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.api.schemas.ai_delegates import DelegateCapsBody
from snapper.api.schemas.ai_delegates import DelegateCapsUpdateBody
from snapper.api.schemas.ai_delegates import DelegateCreateBody
from snapper.api.schemas.ai_delegates import DelegateCreatedPayload
from snapper.api.schemas.ai_delegates import DelegateRead
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import role_grants_permission
from snapper.auth.domain.roles import AI_REVIEW_PRINCIPAL_ROLES
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.core.json_types import JsonObject
from snapper.data.models import AiDelegate
from snapper.data.models import User
from snapper.data.models import UserActiveToken
from snapper.data.models import UserOperatorMembership
from snapper.data.models import UserTradingCaps
from snapper.data.repository import Repository
from snapper.data.repository import close_and_insert
from snapper.data.repository import where_active
from snapper.data.repository import where_active_now
from snapper.messaging.infrastructure.publisher import SequenceTracker


def _now_for_join() -> datetime:
    """Return the current UTC time used in JOIN-level temporal predicates."""
    return datetime.now(UTC)


_DELEGATES_TOPIC = "ai_delegates"

MAX_AI_DELEGATES_PER_OWNER: int = 5
"""Cap on live delegates per operator.

Bounds delegate proliferation so an operator can't spray
credentials that later need individual revocation on compromise.
Deactivated delegates drop out of the count (``is_active=True``
only) so rotations stay unbounded.
"""


class DelegateNotFoundError(Exception):
    """Raised when the requested delegate doesn't exist OR isn't owned by the caller."""


class DelegateProliferationError(Exception):
    """Raised when the owner already has :data:`MAX_AI_DELEGATES_PER_OWNER` active delegates."""


class DelegateLabelConflictError(Exception):
    """Raised when the normalised username derived from the label already exists."""


class DelegateOperatorBindingError(Exception):
    """Raised when the delegate cannot be bound to a valid operator.

    The delegate MUST carry a
    ``UserOperatorMembership`` row so its minted tokens decode with
    ``operator_public_ids`` populated; otherwise the MCP
    wallet-scope gate rejects every write. Triggered when
        the caller chose an ``operator_public_id`` outside their
          own authenticated set (cross-operator spoof attempt), or
        the caller has NO primary operator AND did not specify
          one explicitly (ambiguous delegate scope).
    Route handler maps this to HTTP 422 with a ``detail`` that
    names the offending operator so clients can self-correct.
    """


class InvalidOwnerPrincipalError(Exception):
    """Raised when the caller's principal is missing a usable ``user_public_id``.

    A legacy / misconfigured token whose
    ``user_public_id`` decodes to an empty string must not be
    allowed to create a delegate with ``created_by_user_public_id=""``
    that would make every other blank-ID principal see the
    delegate. We fail-closed at the service boundary so the route
    can surface a clean 401 instead of silently minting ownerless
    rows.
    """


class DelegateService:
    """Create + manage AI delegates owned by an operator."""

    _owner_locks: ClassVar[dict[str, asyncio.Lock]] = {}
    """In-process serialization for the proliferation guard + insert.

    The count-then-insert path can race under concurrent
    ``POST /api/ai-delegates`` calls. This map keys
    ``owner_public_id`` to a per-owner :class:`asyncio.Lock` so each
    create operation serializes inside one Python process before DB
    round trips. Cross-process races are still guarded by the
    transactional row lock in :meth:`_guard_proliferation`.

    The dict is a class-level attribute so the lock survives
    across :class:`DelegateService` instances in the same process
    (FastAPI spawns a fresh service per request).
    """

    def __init__(
        self,
        repository: Repository,
        token_manager: TokenManager,
        tracker: SequenceTracker | None = None,
    ) -> None:
        """Wire the service dependencies.

        Args:
            repository: Active SQLAlchemy repository. The service
                opens a single transactional scope per mutating
                call so create/update flows are atomic.
            token_manager: Singleton used to mint + decode the
                delegate's access/refresh pair in
                :meth:`create_delegate`.
            tracker: Optional sequence tracker for DB provenance.
                Each instance allocates its own so tests can
                inject a deterministic one.
        """
        self.repository = repository
        self.token_manager = token_manager
        self._tracker = tracker or SequenceTracker()

    @classmethod
    def _get_owner_lock(cls, owner_public_id: str) -> asyncio.Lock:
        """Return (and lazily create) the per-owner proliferation lock.

        Args:
            owner_public_id: UUID of the creating operator.

        Returns:
            The shared :class:`asyncio.Lock` for this owner. Every
            in-process create call for this owner will acquire the
            same lock, serialising the guard + insert sequence.
        """
        lock = cls._owner_locks.get(owner_public_id)
        if lock is None:
            lock = asyncio.Lock()
            cls._owner_locks[owner_public_id] = lock
        return lock

    async def create_delegate(
        self,
        owner: AuthPrincipal,
        body: DelegateCreateBody,
    ) -> DelegateCreatedPayload:
        """Atomically mint a new AI delegate + trading caps + token pair.

        Steps (all in one transaction)
            1. Resolve + validate the target operator binding via
               :meth:`_resolve_operator_binding` so the delegate
               inherits a concrete ``operator_public_id`` the
               caller is authorised to act AS.
            2. Derive a unique username ``ai-<slug>-<suffix>`` from
               ``body.label`` — retries with a fresh suffix on
               collision up to a small bound.
            3. Insert the :class:`User` row with
               ``role=AI_DELEGATE``
               ``is_active=True``, and
               ``created_by_user_public_id`` pointing at
               ``owner.user_public_id`` so the per-owner listing
               query can filter cheaply via the migration-0012
               ``ix_users_created_by_user_public_id`` index.
            4. Insert the :class:`UserTradingCaps` row with the
               operator-supplied caps (or all-``None`` for
               "inherit defaults").
            5. Insert the :class:`UserOperatorMembership` row with
               ``is_primary=True`` so the delegate has a single
               canonical operator scope — the refresh round-trip
               can later re-resolve identical
               ``operator_public_ids`` from DB.
            6. Mint an access+refresh pair via
               :meth:`TokenManager.create_tokens`. The principal
               passed in carries ``operator_public_ids=[bound]``
               so the minted JWT decodes with populated operator
               scope and
               :func:`~snapper.mcp.auth.validate_user_wallet_scope`
               admits the delegate's first write call.
            7. Insert both ``user_active_tokens`` rows so
               ``verify_token_with_db`` admits them on the next
               request.
        If any step raises, the outer ``async with session`` rolls
        back — no partial User row, no orphan caps, no membership
        without a User, no phantom tokens.

        Args:
            owner: Principal granted AI-integration management; the route's
                permission dependency enforces the capability.
            body: Parsed :class:`DelegateCreateBody` with the
                human-readable label + optional caps.

        Returns:
            :class:`DelegateCreatedPayload` with the delegate's
            read projection + the access/refresh JWT pair. The
            tokens are surfaced EXACTLY ONCE (no re-serve).

        Raises:
            DelegateLabelConflictError: If a unique username can't
                be derived from the label within the retry bound.
            InvalidOwnerPrincipalError: If the caller's principal
                does not carry a usable ``user_public_id``.
            DelegateOperatorBindingError: If
                ``body.operator_public_id`` is outside the caller's
                claim set OR no explicit binding is given and the
                caller has no primary operator.
            DelegateProliferationError: If the owner already owns
                :data:`MAX_AI_DELEGATES_PER_OWNER` active delegates
        """
        self._guard_owner(owner.user_public_id)
        bound_operator_public_id = self._resolve_operator_binding(owner, body)
        async with self._get_owner_lock(owner.user_public_id):
            return await self._create_delegate_locked(
                owner=owner,
                body=body,
                bound_operator_public_id=bound_operator_public_id,
            )

    async def _create_delegate_locked(
        self,
        *,
        owner: AuthPrincipal,
        body: DelegateCreateBody,
        bound_operator_public_id: str,
    ) -> DelegateCreatedPayload:
        """Execute the atomic create once the per-owner lock is held.

        Split out from :meth:`create_delegate` so the lock scope is
        obvious at the call site. The lock wraps the
        :meth:`_guard_proliferation` + insert + commit sequence so
        two concurrent same-owner calls in the same process cannot
        both pass the cap check.
        """
        async with self.repository.session() as session:
            await self._guard_proliferation(session, owner.user_public_id)
            username = await self._reserve_unique_username(session, body.label)
            now = datetime.now(UTC)
            placeholder_password_hash = self._mint_unusable_password_hash()
            delegate_user = User(
                username=username,
                email=None,
                password_hash=placeholder_password_hash,
                role=UserRole.AI_DELEGATE.value,
                is_active=True,
                created_at=now,
                created_by_user_public_id=owner.user_public_id,
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_DELEGATES_TOPIC),
            )
            session.add(delegate_user)
            await session.flush()
            caps_row = UserTradingCaps(
                user_public_id=delegate_user.public_id,
                max_order_quantity_per_instrument=body.caps.max_order_quantity_per_instrument,
                max_open_orders=body.caps.max_open_orders,
                max_daily_notional_usd=body.caps.max_daily_notional_usd,
                max_cancels_per_minute=body.caps.max_cancels_per_minute,
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_DELEGATES_TOPIC),
            )
            session.add(caps_row)
            membership_row = UserOperatorMembership(
                user_public_id=delegate_user.public_id,
                operator_public_id=bound_operator_public_id,
                is_primary=True,
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_DELEGATES_TOPIC),
            )
            session.add(membership_row)
            session.add(
                AiDelegate(
                    public_id=str(uuid.uuid7()),
                    user_public_id=delegate_user.public_id,
                    last_seen_at=None,
                    active_reviews_count=0,
                    created_at=now,
                    updated_at=now,
                )
            )
            delegate_principal = AuthPrincipal(
                username=delegate_user.username,
                role=UserRole.AI_DELEGATE,
                is_active=True,
                user_public_id=delegate_user.public_id,
                operator_public_ids=[bound_operator_public_id],
                primary_operator_public_id=bound_operator_public_id,
            )
            pat = self.token_manager.create_delegate_access_token(
                delegate_principal,
                issued_at=now,
                permissions=body.permissions,
            )
            session.add(
                UserActiveToken(
                    public_id=str(uuid.uuid7()),
                    user_public_id=delegate_user.public_id,
                    jti=pat.jti,
                    token_hash=hash_token(pat.access_token),
                    token_type="access",
                    issued_at=now,
                    expires_at=pat.expires_at,
                )
            )
            await session.commit()
            delegate_read = self._delegate_read_from_rows(
                user_row=delegate_user,
                caps_row=caps_row,
                label=self._label_from_username(delegate_user.username),
            )
        logger.info(
            "create_delegate: owner={} delegate={} username={}",
            owner.user_public_id,
            delegate_user.public_id,
            delegate_user.username,
        )
        return DelegateCreatedPayload(
            delegate=delegate_read,
            access_token=pat.access_token,
            expires_in=pat.expires_in,
        )

    def _resolve_operator_binding(
        self,
        owner: AuthPrincipal,
        body: DelegateCreateBody,
    ) -> str:
        """Pick + validate the operator the new delegate inherits.

         delegate created
        without a :class:`UserOperatorMembership` row has empty
        ``operator_public_ids`` on its minted JWT and is rejected
        by every MCP wallet-scope + operator-scope gate. The
        binding MUST land in the same transaction as the User +
        caps + token rows so a partial insert cannot leak a delegate
        that can authenticate but cannot act.
        Resolution order
            1. Caller whose named set carries ``IMPERSONATE_OPERATOR``
               plus an explicit ``operator_public_id`` → use it unchanged.
            2. Other caller + explicit ``operator_public_id``
               → MUST sit inside ``owner.operator_public_ids``
               otherwise raise :class:`DelegateOperatorBindingError`
               (cross-operator spoof attempt).
            3. No explicit pick → fall back to
               ``owner.primary_operator_public_id``. Empty primary
               + no explicit pick raises — the delegate would be
               ambiguously scoped.

        Args:
            owner: The creating principal. Its
                ``operator_public_ids`` set is the source of
                truth for what the caller may act AS.
            body: Parsed create body — ``body.operator_public_id``
                is the optional explicit selection.

        Returns:
            The validated operator UUID that will be written to
            the ``UserOperatorMembership`` row AND encoded into the
            delegate's minted JWT claims.

        Raises:
            DelegateOperatorBindingError: when the selection is
                outside the caller's claim set OR when no selection
                is provided and the caller has no primary operator.
        """
        explicit = body.operator_public_id
        if explicit is not None:
            if role_grants_permission(owner.role, Permission.IMPERSONATE_OPERATOR):
                return explicit
            if explicit not in owner.operator_public_ids:
                raise DelegateOperatorBindingError(
                    f"Operator '{explicit}' is not in the caller's authenticated set."
                )
            return explicit
        if not owner.primary_operator_public_id:
            raise DelegateOperatorBindingError(
                "Caller has no primary operator — supply `operator_public_id` explicitly."
            )
        return owner.primary_operator_public_id

    async def list_delegates(
        self,
        owner_public_id: str | None = None,
        *,
        operator_public_ids: list[str] | None = None,
    ) -> list[DelegateRead]:
        """Return every active delegate visible through one read scope.

        Management callers retain the historical creator-owner view.
        Read-only callers use operator memberships, matching wallet scope.
        Exactly one scope must be supplied. An empty operator scope returns
        an empty list without querying.
        Excludes deactivated delegates (``is_active=False``) so
        the frontend list view matches the
        ``POST /deactivate`` semantic — a deactivated delegate
        drops out of the list view + detail view reports 404.

        Args:
            owner_public_id: Creator UUID for the historical management view.
            operator_public_ids: Operator memberships for the read-only view.

        Returns:
            List of :class:`DelegateRead` projections in the selected scope.
        """
        if (owner_public_id is None) == (operator_public_ids is None):
            raise InvalidOwnerPrincipalError("Exactly one AI delegate read scope must be supplied.")
        if owner_public_id is not None:
            self._guard_owner(owner_public_id)
        elif not operator_public_ids:
            return []
        async with self.repository.session() as session:
            now = _now_for_join()
            caps_ts, caps_known_to = where_active(UserTradingCaps, now)
            user_ts, user_known_to = where_active(User, now)
            stmt = select(User, UserTradingCaps).join(
                UserTradingCaps,
                (UserTradingCaps.user_public_id == User.public_id) & caps_ts & caps_known_to,
                isouter=True,
            )
            if owner_public_id is not None:
                stmt = stmt.where(User.created_by_user_public_id == owner_public_id)
            else:
                membership_ts, membership_known_to = where_active(UserOperatorMembership, now)
                stmt = stmt.join(
                    UserOperatorMembership,
                    (UserOperatorMembership.user_public_id == User.public_id)
                    & membership_ts
                    & membership_known_to,
                ).where(UserOperatorMembership.operator_public_id.in_(operator_public_ids or []))
            stmt = stmt.where(
                User.role.in_(tuple(sorted(AI_REVIEW_PRINCIPAL_ROLES))),
                User.is_active,
                user_ts,
                user_known_to,
            )
            rows = (await session.execute(stmt)).all()
            delegates: list[DelegateRead] = []
            seen_user_public_ids: set[str] = set()
            for user_row, caps_row in rows:
                if user_row.public_id in seen_user_public_ids:
                    continue
                seen_user_public_ids.add(user_row.public_id)
                delegates.append(
                    self._delegate_read_from_rows(
                        user_row=user_row,
                        caps_row=caps_row,
                        label=self._label_from_username(user_row.username),
                    )
                )
            return delegates

    async def get_delegate(
        self,
        public_id: str,
        owner_public_id: str | None = None,
        *,
        operator_public_ids: list[str] | None = None,
    ) -> DelegateRead:
        """Fetch one active delegate in a creator or membership scope.

        Raises :class:`DelegateNotFoundError` rather than returning
        ``None`` so the route handler can map cleanly to a 404.
        Management callers retain creator ownership. Read-only callers
        may read delegates bound to one of their operators.

        Args:
            public_id: UUID of the delegate to fetch.
            owner_public_id: Creator UUID for the management view.
            operator_public_ids: Operator memberships for read-only access.

        Returns:
            :class:`DelegateRead` projection of the active
            delegate + its caps.

        Raises:
            InvalidOwnerPrincipalError: if ``owner_public_id`` is
                empty.
        """
        if (owner_public_id is None) == (operator_public_ids is None):
            raise InvalidOwnerPrincipalError("Exactly one AI delegate read scope must be supplied.")
        async with self.repository.session() as session:
            if owner_public_id is not None:
                self._guard_owner(owner_public_id)
                user_row, caps_row = await self._load_delegate_with_caps(
                    session, public_id, owner_public_id
                )
            else:
                if not operator_public_ids:
                    raise DelegateNotFoundError(public_id)
                user_row, caps_row = await self._load_delegate_for_operators(
                    session,
                    public_id,
                    operator_public_ids,
                )
        return self._delegate_read_from_rows(
            user_row=user_row,
            caps_row=caps_row,
            label=self._label_from_username(user_row.username),
        )

    async def update_caps(
        self,
        public_id: str,
        owner_public_id: str,
        body: DelegateCapsUpdateBody,
    ) -> DelegateRead:
        """SCD2 close+insert new caps for a delegate the caller owns.

        Closes the current ``user_trading_caps`` row and inserts a
        fresh one carrying ``body.caps``. Cap history is fully
        auditable via SCD2 point-in-time queries.
        Raises :class:`DelegateNotFoundError` when the delegate
        either doesn't exist or is owned by a different operator.

        Args:
            public_id: UUID of the delegate whose caps should
                update.
            owner_public_id: Calling operator's UUID; enforces
                ownership via the DB WHERE clause.
            body: Replacement :class:`DelegateCapsUpdateBody`.
                Every field replaces the corresponding cap; a
                ``None`` field means "unbounded" on that axis.

        Returns:
            :class:`DelegateRead` projection after the new caps
            row has committed.

        Raises:
            InvalidOwnerPrincipalError: if ``owner_public_id`` is
                empty.
        """
        self._guard_owner(owner_public_id)
        async with self.repository.session() as session:
            user_row, _caps = await self._load_delegate_with_caps(
                session, public_id, owner_public_id
            )
            now = datetime.now(UTC)
            new_values: dict[str, object] = {
                "user_public_id": user_row.public_id,
                "max_order_quantity_per_instrument": body.caps.max_order_quantity_per_instrument,
                "max_open_orders": body.caps.max_open_orders,
                "max_daily_notional_usd": body.caps.max_daily_notional_usd,
                "max_cancels_per_minute": body.caps.max_cancels_per_minute,
                "session_id": self._tracker.session_id,
                "sequence_id": self._tracker.next_sequence(_DELEGATES_TOPIC),
            }
            refreshed = await close_and_insert(
                session=session,
                model=UserTradingCaps,
                match_filters=[UserTradingCaps.user_public_id == user_row.public_id],
                new_values=new_values,
                bus_time=now,
            )
            await session.commit()
        return self._delegate_read_from_rows(
            user_row=user_row,
            caps_row=refreshed,
            label=self._label_from_username(user_row.username),
        )

    @staticmethod
    async def _guard_proliferation(session: AsyncSession, owner_public_id: str) -> None:
        """Cap live delegates per operator at :data:`MAX_AI_DELEGATES_PER_OWNER`.

         Bounds the blast radius of a leaked
        operator session. Counts ONLY active (``is_active=True``)
        open-ended delegate Users; deactivated rows don't count
        so rotations remain unbounded. Fails-closed with
        :class:`DelegateProliferationError` so the route can map
        to 409 before the atomic create transaction opens.
        The count
        + insert pair runs under PostgreSQL's default READ
        COMMITTED isolation, which does NOT serialise two
        concurrent ``POST /api/ai-delegates`` calls from the same
        owner — both could read count=4, both pass the guard, and
        both commit (minting 6 delegates through a 5-delegate cap).
        Since the cap is the blast-radius control for a leaked
        operator session, serialisation is a security invariant
        not just a data-integrity nit.
        Closure: lock the owning User row with
        ``SELECT... FOR UPDATE`` BEFORE counting. On PostgreSQL
        this blocks any concurrent transaction that would also lock
        the same owner row until this commit completes; on SQLite
        the write lock already serialises transactions so the
        clause is a no-op. Either way, the count → guard → insert
        sequence becomes atomic per-owner.

        Args:
            session: Active :class:`AsyncSession` from the calling
                transaction.
            owner_public_id: UUID of the creating operator.
        """
        now = _now_for_join()
        user_ts, user_known_to = where_active(User, now)
        lock_stmt = (
            select(User.public_id)
            .where(
                User.public_id == owner_public_id,
                user_ts,
                user_known_to,
            )
            .with_for_update()
        )
        await session.execute(lock_stmt)
        count_stmt = (
            select(func.count())
            .select_from(User)
            .where(
                User.created_by_user_public_id == owner_public_id,
                User.role.in_(tuple(sorted(AI_REVIEW_PRINCIPAL_ROLES))),
                User.is_active,
                User.known_to > now,
            )
        )
        current_count = (await session.execute(count_stmt)).scalar_one()
        if current_count >= MAX_AI_DELEGATES_PER_OWNER:
            raise DelegateProliferationError(
                f"Operator {owner_public_id} already owns {current_count} active "
                f"AI delegates (limit {MAX_AI_DELEGATES_PER_OWNER}). Deactivate an "
                "existing delegate before creating a new one."
            )

    @staticmethod
    def _guard_owner(owner_public_id: str) -> None:
        """Reject empty ``owner_public_id`` so ownerless rows can't appear.

        A legacy/misconfigured token
        could decode with ``user_public_id=""``. Allowing that
        through would persist ``created_by_user_public_id=""`` on
        new delegates, visible to every other blank-ID operator
        principal. Fail closed with
        :class:`InvalidOwnerPrincipalError` so the route layer
        raises 401 instead of silently creating orphan rows.
        """
        if not owner_public_id:
            raise InvalidOwnerPrincipalError(
                "AI delegate management requires a populated user_public_id "
                "on the caller's principal."
            )

    async def _reserve_unique_username(self, session: AsyncSession, label: str) -> str:
        """Derive a unique ``ai-<slug>-<suffix>`` username from the label.

        Probes up to 8 candidate usernames — each collision appends
        a fresh 6-char suffix. The practical collision rate is
        effectively zero under normal load; the bound exists to
        surface pathological inputs loudly rather than spin
        forever.
        """
        slug = self._slugify_label(label)
        for _attempt in range(8):
            candidate = f"ai-{slug}-{self._random_suffix()}"
            stmt = select(User).where(User.username == candidate, *where_active_now(User))
            existing = (await session.execute(stmt)).scalar_one_or_none()
            if existing is None:
                return candidate
        raise DelegateLabelConflictError(
            f"Could not derive a unique username from label '{label}' after 8 attempts"
        )

    async def _load_delegate_with_caps(
        self,
        session: AsyncSession,
        public_id: str,
        owner_public_id: str,
    ) -> tuple[User, UserTradingCaps | None]:
        """Load the SCD2-active delegate User + caps pair for a specific owner.

        Joins ``user_trading_caps`` so the caller gets the caps
        row in one round-trip. The owner guard is enforced at the
        SQL level so a leaked public_id from a different tenant
        still surfaces as ``DelegateNotFoundError``.
        """
        now = _now_for_join()
        caps_ts, caps_known_to = where_active(UserTradingCaps, now)
        user_ts, user_known_to = where_active(User, now)
        stmt = (
            select(User, UserTradingCaps)
            .join(
                UserTradingCaps,
                (UserTradingCaps.user_public_id == User.public_id) & caps_ts & caps_known_to,
                isouter=True,
            )
            .where(
                User.public_id == public_id,
                User.created_by_user_public_id == owner_public_id,
                User.role.in_(tuple(sorted(AI_REVIEW_PRINCIPAL_ROLES))),
                User.is_active,
                user_ts,
                user_known_to,
            )
        )
        row = (await session.execute(stmt)).first()
        if row is None:
            raise DelegateNotFoundError(public_id)
        return row[0], row[1]

    async def _load_delegate_for_operators(
        self,
        session: AsyncSession,
        public_id: str,
        operator_public_ids: list[str],
    ) -> tuple[User, UserTradingCaps | None]:
        """Load one active delegate reachable through operator membership.

        Args:
            session: Active database session.
            public_id: Delegate user identifier.
            operator_public_ids: Caller operator memberships.

        Returns:
            Active delegate user and optional caps row.

        Raises:
            DelegateNotFoundError: If the delegate is absent or outside scope.
        """
        now = _now_for_join()
        caps_ts, caps_known_to = where_active(UserTradingCaps, now)
        user_ts, user_known_to = where_active(User, now)
        membership_ts, membership_known_to = where_active(UserOperatorMembership, now)
        stmt = (
            select(User, UserTradingCaps)
            .join(
                UserTradingCaps,
                (UserTradingCaps.user_public_id == User.public_id) & caps_ts & caps_known_to,
                isouter=True,
            )
            .join(
                UserOperatorMembership,
                (UserOperatorMembership.user_public_id == User.public_id)
                & membership_ts
                & membership_known_to,
            )
            .where(
                User.public_id == public_id,
                UserOperatorMembership.operator_public_id.in_(operator_public_ids),
                User.role.in_(tuple(sorted(AI_REVIEW_PRINCIPAL_ROLES))),
                User.is_active,
                user_ts,
                user_known_to,
            )
        )
        row = (await session.execute(stmt)).first()
        if row is None:
            raise DelegateNotFoundError(public_id)
        return row[0], row[1]

    def _delegate_read_from_rows(
        self,
        *,
        user_row: User,
        caps_row: UserTradingCaps | None,
        label: str,
    ) -> DelegateRead:
        """Project the User + caps ORM rows into the API schema."""
        caps_body = DelegateCapsBody(
            max_order_quantity_per_instrument=self._coerce_caps_json(caps_row),
            max_open_orders=caps_row.max_open_orders if caps_row else None,
            max_daily_notional_usd=(
                float(caps_row.max_daily_notional_usd)
                if caps_row and caps_row.max_daily_notional_usd is not None
                else None
            ),
            max_cancels_per_minute=(caps_row.max_cancels_per_minute if caps_row else None),
        )
        return DelegateRead(
            public_id=user_row.public_id,
            username=user_row.username,
            label=label,
            created_by_user_public_id=user_row.created_by_user_public_id or "",
            created_at=user_row.created_at,
            is_active=user_row.is_active,
            caps=caps_body,
        )

    @staticmethod
    def _coerce_caps_json(caps_row: UserTradingCaps | None) -> JsonObject | None:
        """Pass through the JSON dict; None when no caps row or column is NULL."""
        if caps_row is None:
            return None
        value = caps_row.max_order_quantity_per_instrument
        if value is None:
            return None
        return value

    @staticmethod
    def _slugify_label(label: str) -> str:
        """Lowercase + strip-nonalnum slug used as the username infix."""
        cleaned = "".join(c for c in label.lower() if c.isalnum() or c == "-")
        cleaned = cleaned.strip("-")
        return cleaned or "delegate"

    @staticmethod
    def _random_suffix() -> str:
        """6-char url-safe suffix for username uniqueness."""
        return secrets.token_urlsafe(6).replace("_", "").replace("-", "")[:6].lower()

    @staticmethod
    def _label_from_username(username: str) -> str:
        """Recover the human-readable label from the ``ai-<label>-<suffix>`` username."""
        if not username.startswith("ai-"):
            return username
        stripped = username[3:]
        parts = stripped.rsplit("-", 1)
        if len(parts) == 2 and parts[0]:
            return parts[0]
        return stripped

    @staticmethod
    def _mint_unusable_password_hash() -> str:
        """Generate a bcrypt hash from a random secret.

        AI delegates never authenticate via password — they use
        the access/refresh JWT pair returned from create. But the
        ``User`` model requires a non-null ``password_hash``, so
        we hash a fresh random token the operator never sees. Any
        attacker who somehow triggers the login path with a
        delegate's username will face a password they can't
        recover from the hash.
        """
        random_secret = secrets.token_urlsafe(32).encode("utf-8")
        return bcrypt.hashpw(random_secret, bcrypt.gensalt()).decode("utf-8")


_uuid7 = uuid7
