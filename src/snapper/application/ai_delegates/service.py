"""AI delegate lifecycle management (plan §4 Day 4b).

Bundles the atomic create-delegate flow + the read/update/
deactivate helpers that back the ``/api/ai-delegates`` routes.
Creation is the load-bearing path: a single DB transaction must
insert the :class:`~snapper.data.models.User` row (with
``role=AI_DELEGATE`` and ``created_by_user_public_id`` pointing
at the creating operator), the per-delegate
:class:`~snapper.data.models.UserTradingCaps` row, AND the
:class:`~snapper.data.models.UserActiveToken` rows for the
newly-minted access+refresh pair. Splitting those across
independent transactions opens the same stranded-user failure
mode the Day 3d-A R1 review flagged for refresh rotation — so
this service holds ONE session scope for the whole flow.
"""

import secrets
import uuid
from datetime import UTC
from datetime import datetime
from uuid import uuid7

import bcrypt
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.api.schemas.ai_delegates import DelegateCapsBody
from snapper.api.schemas.ai_delegates import DelegateCapsUpdateBody
from snapper.api.schemas.ai_delegates import DelegateCreateBody
from snapper.api.schemas.ai_delegates import DelegateCreatedPayload
from snapper.api.schemas.ai_delegates import DelegateRead
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.core.json_types import JsonObject
from snapper.data.models import User
from snapper.data.models import UserActiveToken
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


class DelegateNotFoundError(Exception):
    """Raised when the requested delegate doesn't exist OR isn't owned by the caller."""


class DelegateLabelConflictError(Exception):
    """Raised when the normalised username derived from the label already exists."""


class InvalidOwnerPrincipalError(Exception):
    """Raised when the caller's principal is missing a usable ``user_public_id``.

    Day 4b R1 (Copilot MAJOR): a legacy / misconfigured token whose
    ``user_public_id`` decodes to an empty string must not be
    allowed to create a delegate with ``created_by_user_public_id=""``
    — that would make every other blank-ID principal see the
    delegate. We fail-closed at the service boundary so the route
    can surface a clean 401 instead of silently minting ownerless
    rows.
    """


class DelegateService:
    """Create + manage AI delegates owned by an operator."""

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

    async def create_delegate(
        self,
        owner: AuthPrincipal,
        body: DelegateCreateBody,
    ) -> DelegateCreatedPayload:
        """Atomically mint a new AI delegate + trading caps + token pair.

        Steps (all in one transaction):

            1. Derive a unique username ``ai-<slug>-<suffix>`` from
               ``body.label`` — retries with a fresh suffix on
               collision up to a small bound.
            2. Insert the :class:`User` row with
               ``role=AI_DELEGATE``,
               ``is_active=True``, and
               ``created_by_user_public_id`` pointing at
               ``owner.user_public_id`` so the per-owner listing
               query can filter cheaply via the migration-0012
               ``ix_users_created_by_user_public_id`` index.
            3. Insert the :class:`UserTradingCaps` row with the
               operator-supplied caps (or all-``None`` for
               "inherit defaults").
            4. Mint an access+refresh pair via
               :meth:`TokenManager.create_tokens`.
            5. Insert both ``user_active_tokens`` rows so Day 3d-B
               ``verify_token_with_db`` admits them on the next
               request.

        If any step raises, the outer ``async with session`` rolls
        back — no partial User row, no orphan caps, no phantom
        tokens (R2 atomicity guarantee matching Day 3d-A
        ``rotate_user_active_token``).

        Args:
            owner: The creating operator's principal. Must be
                OPERATOR or ADMIN; the ``require_role`` dep on
                the route enforces that.
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
                does not carry a usable ``user_public_id`` (R1
                blank-owner guard).
        """
        self._guard_owner(owner.user_public_id)
        async with self.repository.session() as session:
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
            delegate_principal = AuthPrincipal(
                username=delegate_user.username,
                role=UserRole.AI_DELEGATE,
                is_active=True,
                user_public_id=delegate_user.public_id,
            )
            pair = self.token_manager.create_tokens(delegate_principal)
            access_claims = self.token_manager.decode_fresh_token(pair.access_token)
            refresh_claims = self.token_manager.decode_fresh_token(pair.refresh_token)
            issued_at = datetime.fromtimestamp(access_claims.iat, tz=UTC)
            access_exp = datetime.fromtimestamp(access_claims.exp, tz=UTC)
            refresh_exp = datetime.fromtimestamp(refresh_claims.exp, tz=UTC)
            session.add(
                UserActiveToken(
                    public_id=str(uuid.uuid7()),
                    user_public_id=delegate_user.public_id,
                    jti=access_claims.jti,
                    token_hash=hash_token(pair.access_token),
                    token_type="access",
                    issued_at=issued_at,
                    expires_at=access_exp,
                )
            )
            session.add(
                UserActiveToken(
                    public_id=str(uuid.uuid7()),
                    user_public_id=delegate_user.public_id,
                    jti=refresh_claims.jti,
                    token_hash=hash_token(pair.refresh_token),
                    token_type="refresh",
                    issued_at=issued_at,
                    expires_at=refresh_exp,
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
            access_token=pair.access_token,
            refresh_token=pair.refresh_token,
            expires_in=pair.expires_in,
        )

    async def list_delegates(self, owner_public_id: str) -> list[DelegateRead]:
        """Return every SCD2-active delegate the caller owns.

        Fails closed with :class:`InvalidOwnerPrincipalError` when
        the ``owner_public_id`` is empty — prevents legacy blank-ID
        tokens from enumerating other blank-ID operators' delegates
        (R1 Copilot MAJOR fix).

        Excludes deactivated delegates (``is_active=False``) so
        the frontend list view matches the
        ``POST /deactivate`` semantic — a deactivated delegate
        drops out of the list view + detail view reports 404.

        Args:
            owner_public_id: UUID of the calling operator; the
                ``created_by_user_public_id`` filter scopes the
                result set to just the caller's delegates.

        Returns:
            List of :class:`DelegateRead` projections. Empty when
            the operator has never created a delegate.
        """
        self._guard_owner(owner_public_id)
        async with self.repository.session() as session:
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
                    User.created_by_user_public_id == owner_public_id,
                    User.role == UserRole.AI_DELEGATE.value,
                    User.is_active,
                    user_ts,
                    user_known_to,
                )
            )
            result = await session.execute(stmt)
            delegates: list[DelegateRead] = []
            for user_row, caps_row in result.all():
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
        owner_public_id: str,
    ) -> DelegateRead:
        """Fetch a single active delegate scoped to the caller.

        Raises :class:`DelegateNotFoundError` rather than returning
        ``None`` so the route handler can map cleanly to a 404.
        The owner guard prevents operators from reading each
        other's delegates even via guessed UUIDs.

        Args:
            public_id: UUID of the delegate to fetch.
            owner_public_id: Calling operator's UUID; the
                DB-level WHERE clause enforces the ownership
                predicate so cross-tenant reads surface as
                :class:`DelegateNotFoundError` (404 upstream).

        Returns:
            :class:`DelegateRead` projection of the active
            delegate + its caps.

        Raises:
            InvalidOwnerPrincipalError: if ``owner_public_id`` is
                empty (R1 blank-owner guard).
        """
        self._guard_owner(owner_public_id)
        async with self.repository.session() as session:
            user_row, caps_row = await self._load_delegate_with_caps(
                session, public_id, owner_public_id
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
                empty (R1 blank-owner guard).
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
    def _guard_owner(owner_public_id: str) -> None:
        """Reject empty ``owner_public_id`` so ownerless rows can't appear.

        Day 4b R1 Copilot MAJOR fix: a legacy/misconfigured token
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
                User.role == UserRole.AI_DELEGATE.value,
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
