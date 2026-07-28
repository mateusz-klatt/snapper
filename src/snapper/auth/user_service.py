"""User service module.

This module provides user management operations including
authentication, CRUD operations, and password management.
All User mutations use SCD Type 2 close+insert via close_and_insert.
Login events are temporal: inserted on login, closeable for corrections,
queryable via where_active for point-in-time audit.
"""

from datetime import UTC
from datetime import datetime
from uuid import uuid7

import bcrypt
from loguru import logger
from sqlalchemy import select
from sqlalchemy import update

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import has_effective_permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.user import UserProfile
from snapper.auth.tokens import get_token_manager
from snapper.config.settings import get_settings
from snapper.data.models import User
from snapper.data.models import UserLoginEvent
from snapper.data.repository import close_and_insert
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
from snapper.data.repository import where_active_now
from snapper.data.repository_types import DeskMembershipAttach
from snapper.data.repository_types import UserOperatorMembershipRow
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import UserDeactivatedData
from snapper.messaging.topics.builders import admin_topic

_USERS_TOPIC = "users"
_LOGIN_EVENTS_TOPIC = "login_events"
_USER_DEACTIVATED_TOPIC = "user_deactivated"
_DESK_MEMBERSHIPS_TOPIC = "desk_memberships"


class DeskMembershipAuthorizationError(Exception):
    """Raised when a caller may not manage the target desk."""


class UserService:
    """User management service singleton.

    Provides user authentication, creation, update, and
    password management operations.
    """

    _instance: UserService | None = None
    _initialized: bool = False

    def __new__(cls) -> UserService:
        """Create or return singleton user service instance."""
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """Initialize the user service with database repository."""
        if self._initialized:
            return
        self._initialized = True
        settings = get_settings()
        self.repository = get_repository(settings.db_url)
        self._tracker = SequenceTracker()
        self._msg_publisher: MessagePublisher | None = None

    def set_msg_publisher(self, publisher: MessagePublisher | None) -> None:
        """Inject the bus publisher used for `admin.user_deactivated`.

        Called from the FastAPI lifespan once a ZMQ broker connection
        is available. Injection (rather than self-managed socket) keeps
        the singleton testable: tests substitute a stub implementing
        `send(stream_key, data)` without binding a real ZMQ socket.

        Args:
            publisher: Configured `MessagePublisher` or `None` to clear.
        """
        self._msg_publisher = publisher

    def hash_password(self, password: str) -> str:
        """Hash password using bcrypt.

        Args:
            password: Plain text password.

        Returns:
            Bcrypt-encoded password hash string.
        """
        hashed: bytes = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
        return hashed.decode()

    async def attach_viewer_to_desk(
        self,
        principal: AuthPrincipal,
        operator_public_id: str,
        username: str,
    ) -> UserOperatorMembershipRow:
        """Attach a human VIEWER to a desk for the target's next login.

        No bus event or live-principal rebuild occurs because attachment
        intentionally takes effect only when the target next logs in.

        Args:
            principal: Authenticated caller.
            operator_public_id: Target desk public ID.
            username: Exact username of the target VIEWER.

        Returns:
            Existing or newly created active membership.

        Raises:
            DeskMembershipAuthorizationError: Caller lacks the capability
                or is outside the target desk without being global ADMIN.
        """
        if not has_effective_permission(
            principal.role,
            principal.permissions,
            principal.permission_scope_version,
            Permission.MANAGE_DESK_MEMBERSHIPS,
        ):
            raise DeskMembershipAuthorizationError("MANAGE_DESK_MEMBERSHIPS permission is required")
        is_global_admin = has_effective_permission(
            principal.role,
            principal.permissions,
            principal.permission_scope_version,
            Permission.IMPERSONATE_OPERATOR,
        )
        if not is_global_admin and operator_public_id not in principal.operator_public_ids:
            raise DeskMembershipAuthorizationError(
                "Current membership in the target desk is required"
            )
        now = datetime.now(UTC)
        return await self.repository.attach_viewer_to_desk(
            DeskMembershipAttach(
                username=username,
                operator_public_id=operator_public_id,
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_DESK_MEMBERSHIPS_TOPIC),
            )
        )

    def _verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against stored bcrypt hash.

        Args:
            password: Plain text password to verify.
            password_hash: Stored bcrypt password hash.

        Returns:
            True if password matches.
        """
        matched: bool = bcrypt.checkpw(password.encode(), password_hash.encode())
        return matched

    def _db_user_to_auth_user(self, db_user: User) -> UserProfile:
        """Convert database User to UserProfile.

        Args:
            db_user: Database user model.

        Returns:
            UserProfile schema instance.
        """
        return UserProfile(
            public_id=db_user.public_id,
            timestamp=db_user.timestamp,
            session_id=db_user.session_id,
            sequence_id=db_user.sequence_id,
            username=db_user.username,
            email=db_user.email,
            role=UserRole(db_user.role),
            is_active=db_user.is_active,
            created_at=db_user.created_at,
            default_language=db_user.default_language,
        )

    async def build_auth_principal(self, user: UserProfile) -> AuthPrincipal:
        """Build a fully-populated ``AuthPrincipal`` from a ``UserProfile``.

        Resolves the multi-tenant fields (``user_public_id``,
        ``operator_public_ids``, ``primary_operator_public_id``) from the
        repository. Named permission sets carrying
        :data:`Permission.IMPERSONATE_OPERATOR` automatically receive the
        operator set covering every active operator; other users get only
        their explicit memberships from ``user_operator_memberships``.
        ``active_wallet_public_id``
        is intentionally NOT populated here — it is UI state set by the
        client and round-tripped through token claims.

        Args:
            user: The authenticated user profile.

        Returns:
            ``AuthPrincipal`` ready to feed into ``TokenManager.create_tokens``.
        """
        now = datetime.now(UTC)
        memberships = await self.repository.get_user_operator_memberships(
            user_public_id=user.public_id, as_of=now
        )
        if has_effective_permission(
            user.role,
            None,
            None,
            Permission.IMPERSONATE_OPERATOR,
        ):
            operators = await self.repository.list_active_operators(now)
            operator_public_ids = [op["public_id"] for op in operators]
        else:
            operator_public_ids = [m["operator_public_id"] for m in memberships]
        delegate_public_id: str | None = None
        delegate_row = await self.repository.get_ai_delegate_by_user_public_id(user.public_id)
        if delegate_row is not None:
            delegate_public_id = delegate_row["public_id"]
        primary_match = next((m for m in memberships if m["is_primary"]), None)
        primary_operator_public_id = (
            primary_match["operator_public_id"] if primary_match is not None else ""
        )
        return AuthPrincipal(
            username=user.username,
            role=user.role,
            email=user.email,
            is_active=user.is_active,
            user_public_id=user.public_id,
            operator_public_ids=operator_public_ids,
            primary_operator_public_id=primary_operator_public_id,
            delegate_public_id=delegate_public_id,
        )

    async def authenticate_user(self, username: str, password: str) -> UserProfile | None:
        """Authenticate user by username and password.

        Records a login event in user_login_events on success.

        Args:
            username: User's username.
            password: User's password.

        Returns:
            UserProfile if authenticated, None otherwise.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(
                User.username == username, User.is_active, *where_active_now(User)
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return None
            if not self._verify_password(password, db_user.password_hash):
                return None
            now = datetime.now(UTC)
            login_event = UserLoginEvent(
                user_public_id=db_user.public_id,
                logged_at=now,
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_LOGIN_EVENTS_TOPIC),
            )
            session.add(login_event)
            await session.commit()
            return self._db_user_to_auth_user(db_user)

    async def get_user_by_id(self, user_id: str) -> UserProfile | None:
        """Get active user by ID.

        Args:
            user_id: User's unique identifier (username).

        Returns:
            UserProfile if found and active, None otherwise.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(
                User.username == user_id, User.is_active, *where_active_now(User)
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return None
            return self._db_user_to_auth_user(db_user)

    async def get_user_with_operators(self, user_id: str) -> UserProfile | None:
        """Get active user enriched with operator membership fields.

        Applies the same resolution rule as ``build_auth_principal``:
        named sets carrying ``IMPERSONATE_OPERATOR`` receive every active
        operator's ``public_id``, while other sets receive only their
        explicit ``user_operator_memberships`` entries. The
        ``primary_operator_public_id`` is taken from the membership row
        marked ``is_primary=TRUE`` when present.

        Used by ``/auth/me`` so the frontend OperatorPicker can render
        the accessible operator set without a second round trip.

        Args:
            user_id: User's unique identifier (username).

        Returns:
            UserProfile with operator fields populated, or None.
        """
        profile = await self.get_user_by_id(user_id)
        if profile is None:
            return None
        now = datetime.now(UTC)
        memberships = await self.repository.get_user_operator_memberships(
            user_public_id=profile.public_id, as_of=now
        )
        if has_effective_permission(
            profile.role,
            None,
            None,
            Permission.IMPERSONATE_OPERATOR,
        ):
            operators = await self.repository.list_active_operators(now)
            operator_public_ids = [op["public_id"] for op in operators]
        else:
            operator_public_ids = [m["operator_public_id"] for m in memberships]
        primary_match = next((m for m in memberships if m["is_primary"]), None)
        primary_operator_public_id = (
            primary_match["operator_public_id"] if primary_match is not None else None
        )
        return profile.model_copy(
            update={
                "operator_public_ids": operator_public_ids,
                "primary_operator_public_id": primary_operator_public_id,
            }
        )

    async def get_user_by_username(self, username: str) -> UserProfile | None:
        """Get active user by username.

        Args:
            username: User's username.

        Returns:
            UserProfile if found and active, None otherwise.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(
                User.username == username, User.is_active, *where_active_now(User)
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return None
            return self._db_user_to_auth_user(db_user)

    async def get_all_users(
        self,
        include_inactive: bool = False,
        as_of: datetime | None = None,
    ) -> list[UserProfile]:
        """Get all users.

        Args:
            include_inactive: If True, includes inactive users.
            as_of: Optional point-in-time query (UTC). Defaults to now.

        Returns:
            List of UserProfile instances.
        """
        processing_date = as_of or datetime.now(UTC)
        async with self.repository.session() as session:
            base = select(User).where(*where_active(User, processing_date))
            stmt = base if include_inactive else base.where(User.is_active)
            result = await session.execute(stmt)
            db_users = result.scalars().all()
            return [self._db_user_to_auth_user(db_user) for db_user in db_users]

    async def create_user(
        self,
        username: str,
        password: str,
        email: str | None = None,
        role: UserRole = UserRole.VIEWER,
        is_active: bool = True,
    ) -> UserProfile:
        """Create a new user.

        Args:
            username: Unique username.
            password: Plain text password.
            email: Optional email address.
            role: User role (default VIEWER).
            is_active: Whether account is active.

        Returns:
            Created UserProfile.

        Raises:
            ValueError: If username already exists.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(User.username == username, *where_active_now(User))
            result = await session.execute(stmt)
            existing_user = result.scalar_one_or_none()
            if existing_user:
                raise ValueError(f"User with username '{username}' already exists")
            password_hash = self.hash_password(password)
            now = datetime.now(UTC)
            db_user = User(
                username=username,
                email=email,
                password_hash=password_hash,
                role=role.value,
                is_active=is_active,
                created_at=now,
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_USERS_TOPIC),
            )
            session.add(db_user)
            await session.commit()
            await session.refresh(db_user)
            return self._db_user_to_auth_user(db_user)

    async def update_user(
        self,
        user_id: str,
        email: str | None = None,
        role: UserRole | None = None,
        is_active: bool | None = None,
    ) -> UserProfile | None:
        """Update user attributes via SCD Type 2 close+insert.

        Only provided (non-None) attributes are changed.
        The old row is closed and a new row is inserted carrying
        the same public_id and username.

        Args:
            user_id: User's username.
            email: New email address.
            role: New role.
            is_active: New active status.

        Returns:
            Updated UserProfile or None if not found.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(
                User.username == user_id,
                *where_active_now(User),
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return None
            now = datetime.now(UTC)
            new_values: dict[str, object] = {
                "username": db_user.username,
                "email": email if email is not None else db_user.email,
                "password_hash": db_user.password_hash,
                "role": role.value if role is not None else db_user.role,
                "is_active": is_active if is_active is not None else db_user.is_active,
                "default_language": db_user.default_language,
                "created_at": db_user.created_at,
                "session_id": self._tracker.session_id,
                "sequence_id": self._tracker.next_sequence(_USERS_TOPIC),
            }
            new_row = await close_and_insert(
                session=session,
                model=User,
                match_filters=[User.username == user_id],
                new_values=new_values,
                bus_time=now,
            )
            await session.commit()
            await session.refresh(new_row)
            return self._db_user_to_auth_user(new_row)

    async def update_self_preferences(
        self,
        user_id: str,
        default_language: str | None,
    ) -> UserProfile | None:
        """Update the caller's self-service preferences via SCD2 close+insert.

        Writes a new active ``users`` row with the caller's
        ``default_language`` preference while preserving the other
        profile fields. The admin-facing :meth:`update_user` is
        intentionally separate so ``MANAGE_USERS`` permission stays
        narrowly scoped.

        Args:
            user_id: Caller's username (from auth principal).
            default_language: New ``default_language`` value. Passing
                ``None`` clears the preference (alert pipeline reverts
                to English emission for that user).

        Returns:
            Updated UserProfile or ``None`` if the user was not found.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(
                User.username == user_id,
                *where_active_now(User),
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return None
            now = datetime.now(UTC)
            new_values: dict[str, object | None] = {
                "username": db_user.username,
                "email": db_user.email,
                "password_hash": db_user.password_hash,
                "role": db_user.role,
                "is_active": db_user.is_active,
                "default_language": default_language,
                "created_at": db_user.created_at,
                "session_id": self._tracker.session_id,
                "sequence_id": self._tracker.next_sequence(_USERS_TOPIC),
            }
            new_row = await close_and_insert(
                session=session,
                model=User,
                match_filters=[User.username == user_id],
                new_values=new_values,
                bus_time=now,
            )
            await session.commit()
            await session.refresh(new_row)
            return self._db_user_to_auth_user(new_row)

    async def deactivate_user(self, user_public_id: str, reason: str | None) -> bool:
        """Deactivate a user as the SOLE publisher of `admin.user_deactivated`.

        Implements the canonical kill-switch flow:
        1. SCD2 close+insert on the active `users` row with an inactive state.
        2. `TokenManager.revoke_user_sessions(...)` — direct in-process call
           that revokes every active token row in the `user_active_tokens`
           inventory and seeds the local fast-path blacklist. The token
           method commits its own transaction (`Repository.revoke_user_active_tokens`
           opens its own session) and deliberately does NOT publish a bus
           event so the single-publisher rule is preserved.
        3. Commit the user SCD2 mutation. Token revocation has already
           landed; if this commit fails the user row stays active but
           tokens remain revoked — the safer failure mode (kill switch
           wins over availability).
        4. Publish `admin.user_deactivated` AFTER the commit so
           subscribers always see the committed state. Cross-instance
           `TokenManager` LRU eviction is driven exclusively by this
           bus event.

        Args:
            user_public_id: UUID7 of the user row to deactivate.
            reason: Optional admin-supplied rationale, forwarded
                verbatim to the bus payload for audit.

        Returns:
            ``True`` when the user was found and deactivated, ``False``
            when no active row matches ``user_public_id``.
        """
        async with self.repository.session() as session:
            stmt = (
                select(User)
                .where(
                    User.public_id == user_public_id,
                    User.is_active,
                    *where_active_now(User),
                )
                .with_for_update()
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return False
            now = datetime.now(UTC)
            new_values: dict[str, object | None] = {
                "username": db_user.username,
                "email": db_user.email,
                "password_hash": db_user.password_hash,
                "role": db_user.role,
                "is_active": False,
                "default_language": db_user.default_language,
                "created_at": db_user.created_at,
                "session_id": self._tracker.session_id,
                "sequence_id": self._tracker.next_sequence(_USERS_TOPIC),
            }
            await close_and_insert(
                session=session,
                model=User,
                match_filters=[User.public_id == user_public_id],
                new_values=new_values,
                bus_time=now,
            )
            token_manager = get_token_manager()
            await token_manager.revoke_user_sessions(user_public_id, self.repository)
            await session.commit()
        await self._publish_user_deactivated(
            user_public_id=user_public_id,
            reason=reason,
            deactivated_at=now,
        )
        return True

    async def _publish_user_deactivated(
        self,
        *,
        user_public_id: str,
        reason: str | None,
        deactivated_at: datetime,
    ) -> None:
        """Emit `admin.user_deactivated` after the deactivation commit.

        Best-effort fast path: a missing publisher or send failure
        logs instead of raising because the committed
        inactive SCD2-active ``users`` row is the durable deactivation
        registry. Every lifespan-wired auth listener also polls that
        registry, so a broker hiccup delays cross-instance fanout by
        the fallback scan interval instead of rolling back the kill
        switch.
        """
        if self._msg_publisher is None:
            logger.warning(
                "admin.user_deactivated NOT broadcast for user_public_id={}: "
                "UserService publisher unavailable; DB fallback scanners will converge",
                user_public_id,
            )
            return
        topic = admin_topic(_USER_DEACTIVATED_TOPIC)
        payload = UserDeactivatedData(
            public_id=str(uuid7()),
            timestamp=deactivated_at,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(topic),
            user_public_id=user_public_id,
            deactivated_at=deactivated_at,
            reason=reason,
        )
        try:
            await self._msg_publisher.send(topic, payload)
        except Exception as exc:
            logger.exception(
                "Failed to broadcast admin.user_deactivated for user_public_id={}; "
                "DB fallback scanners will converge: {}",
                user_public_id,
                exc,
            )

    async def delete_user(self, user_id: str) -> bool:
        """Soft-delete user via SCD Type 2 close+insert with inactive state.

        Args:
            user_id: User's username.

        Returns:
            True if deleted, False if not found.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(
                User.username == user_id,
                *where_active_now(User),
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return False
            now = datetime.now(UTC)
            new_values: dict[str, object | None] = {
                "username": db_user.username,
                "email": db_user.email,
                "password_hash": db_user.password_hash,
                "role": db_user.role,
                "is_active": False,
                "default_language": db_user.default_language,
                "created_at": db_user.created_at,
                "session_id": self._tracker.session_id,
                "sequence_id": self._tracker.next_sequence(_USERS_TOPIC),
            }
            await close_and_insert(
                session=session,
                model=User,
                match_filters=[User.username == user_id],
                new_values=new_values,
                bus_time=now,
            )
            await session.commit()
            return True

    async def change_password(self, user_id: str, old_password: str, new_password: str) -> bool:
        """Change user's password via SCD Type 2 close+insert.

        Verifies old password before creating new version.

        Args:
            user_id: User's username.
            old_password: Current password for verification.
            new_password: New password to set.

        Returns:
            True if changed, False if user not found or wrong password.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(
                User.username == user_id, User.is_active, *where_active_now(User)
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return False
            if not self._verify_password(old_password, db_user.password_hash):
                return False
            now = datetime.now(UTC)
            new_values: dict[str, object | None] = {
                "username": db_user.username,
                "email": db_user.email,
                "password_hash": self.hash_password(new_password),
                "role": db_user.role,
                "is_active": db_user.is_active,
                "default_language": db_user.default_language,
                "created_at": db_user.created_at,
                "session_id": self._tracker.session_id,
                "sequence_id": self._tracker.next_sequence(_USERS_TOPIC),
            }
            await close_and_insert(
                session=session,
                model=User,
                match_filters=[User.username == user_id],
                new_values=new_values,
                bus_time=now,
            )
            await session.commit()
            return True

    async def admin_reset_password(self, user_id: str, new_password: str) -> None:
        """Reset user password via admin action (close+insert).

        Args:
            user_id: Username of the target user.
            new_password: New plain-text password.

        Raises:
            ValueError: If user not found.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(
                User.username == user_id,
                *where_active_now(User),
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                raise ValueError(f"User '{user_id}' not found")
            now = datetime.now(UTC)
            new_values: dict[str, object | None] = {
                "username": db_user.username,
                "email": db_user.email,
                "password_hash": self.hash_password(new_password),
                "role": db_user.role,
                "is_active": db_user.is_active,
                "default_language": db_user.default_language,
                "created_at": db_user.created_at,
                "session_id": self._tracker.session_id,
                "sequence_id": self._tracker.next_sequence(_USERS_TOPIC),
            }
            await close_and_insert(
                session=session,
                model=User,
                match_filters=[User.username == user_id],
                new_values=new_values,
                bus_time=now,
            )
            await session.commit()

    async def reset_password_by_username(self, username: str, new_password: str) -> None:
        """Reset user password by username (for CLI).

        Args:
            username: Username of the target user.
            new_password: New plain-text password.

        Raises:
            ValueError: If user not found.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(
                User.username == username,
                *where_active_now(User),
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                raise ValueError(f"User '{username}' not found")
            now = datetime.now(UTC)
            new_values: dict[str, object | None] = {
                "username": db_user.username,
                "email": db_user.email,
                "password_hash": self.hash_password(new_password),
                "role": db_user.role,
                "is_active": db_user.is_active,
                "default_language": db_user.default_language,
                "created_at": db_user.created_at,
                "session_id": self._tracker.session_id,
                "sequence_id": self._tracker.next_sequence(_USERS_TOPIC),
            }
            await close_and_insert(
                session=session,
                model=User,
                match_filters=[User.username == username],
                new_values=new_values,
                bus_time=now,
            )
            await session.commit()

    async def list_login_events(
        self,
        user_public_id: str,
        at: datetime | None = None,
    ) -> list[UserLoginEvent]:
        """List active login events for a user at a point in time.

        Args:
            user_public_id: User's public UUID.
            at: Point-in-time for temporal query. Defaults to now.

        Returns:
            List of active login events ordered by logged_at descending.
        """
        t = at or datetime.now(UTC)
        async with self.repository.session() as session:
            result = await session.execute(
                select(UserLoginEvent)
                .where(
                    UserLoginEvent.user_public_id == user_public_id,
                    *where_active(UserLoginEvent, t),
                )
                .order_by(UserLoginEvent.logged_at.desc())
            )
            return list(result.scalars().all())

    async def close_login_event(self, public_id: str, bus_time: datetime | None = None) -> bool:
        """Close a login event (soft delete).

        Sets known_to on the active login event, hiding it from current
        queries while preserving it for historical audit via as_of.

        Args:
            public_id: Public UUID of the login event to close.
            bus_time: Processing time for the close. Defaults to now.

        Returns:
            True if event was found and closed, False if not found.
        """
        t = bus_time or datetime.now(UTC)
        async with self.repository.session() as session:

            existing = (
                (
                    await session.execute(
                        select(UserLoginEvent).where(
                            UserLoginEvent.public_id == public_id,
                            *where_active(UserLoginEvent, t),
                        )
                    )
                )
                .scalars()
                .first()
            )
            if not existing:
                return False
            await session.execute(
                update(UserLoginEvent).where(UserLoginEvent.id == existing.id).values(known_to=t)
            )
            await session.commit()
            return True

    @classmethod
    def get_instance(cls) -> UserService:
        """Get singleton instance.

        Returns:
            UserService singleton.
        """
        if cls._instance is None:
            cls._instance = UserService()
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear singleton for testing."""
        cls._instance = None


def get_user_service() -> UserService:
    """Get UserService singleton.

    Returns:
        UserService instance.
    """
    return UserService.get_instance()
