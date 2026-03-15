"""User service module.

This module provides user management operations including
authentication, CRUD operations, and password management.
All User mutations use SCD Type 2 close+insert via close_and_insert.
Login events are recorded in an append-only UserLoginEvent table.
"""

from datetime import UTC
from datetime import datetime

import bcrypt
from sqlalchemy import select

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.user import UserProfile
from snapper.config.settings import get_settings
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import User
from snapper.data.models import UserLoginEvent
from snapper.data.repository import close_and_insert
from snapper.data.repository import get_repository


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

    def hash_password(self, password: str) -> str:
        """Hash password using bcrypt.

        Args:
            password: Plain text password.

        Returns:
            Bcrypt-encoded password hash string.
        """
        hashed: bytes = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
        return hashed.decode()

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
            username=db_user.username,
            email=db_user.email,
            role=UserRole(db_user.role),
            is_active=db_user.is_active,
            created_at=db_user.created_at,
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
            stmt = select(User).where(User.username == username, User.is_active)
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
            )
            session.add(login_event)
            await session.commit()
            return self._db_user_to_auth_user(db_user)

    async def get_user_by_id(self, user_id: str) -> UserProfile | None:
        """Get active user by ID.

        Args:
            user_id: User's unique identifier.

        Returns:
            UserProfile if found and active, None otherwise.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(User.username == user_id, User.is_active)
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return None
            return self._db_user_to_auth_user(db_user)

    async def get_user_by_username(self, username: str) -> UserProfile | None:
        """Get active user by username.

        Args:
            username: User's username.

        Returns:
            UserProfile if found and active, None otherwise.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(User.username == username, User.is_active)
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return None
            return self._db_user_to_auth_user(db_user)

    async def get_all_users(self, include_inactive: bool = False) -> list[UserProfile]:
        """Get all users.

        Args:
            include_inactive: If True, includes inactive users.

        Returns:
            List of UserProfile instances.
        """
        async with self.repository.session() as session:
            stmt = select(User) if include_inactive else select(User).where(User.is_active)
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
            stmt = select(User).where(User.username == username)
            result = await session.execute(stmt)
            existing_user = result.scalar_one_or_none()
            if existing_user:
                raise ValueError(f"User with username '{username}' already exists")
            password_hash = self.hash_password(password)
            db_user = User(
                username=username,
                email=email,
                password_hash=password_hash,
                role=role.value,
                is_active=is_active,
                created_at=datetime.now(UTC),
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
                User.known_to == KNOWN_TO_MAX,
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
                "created_at": db_user.created_at,
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

    async def delete_user(self, user_id: str) -> bool:
        """Soft-delete user via SCD Type 2 close+insert with is_active=False.

        Args:
            user_id: User's username.

        Returns:
            True if deleted, False if not found.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(
                User.username == user_id,
                User.known_to == KNOWN_TO_MAX,
            )
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return False
            now = datetime.now(UTC)
            new_values: dict[str, object] = {
                "username": db_user.username,
                "email": db_user.email,
                "password_hash": db_user.password_hash,
                "role": db_user.role,
                "is_active": False,
                "created_at": db_user.created_at,
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
            stmt = select(User).where(User.username == user_id, User.is_active)
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return False
            if not self._verify_password(old_password, db_user.password_hash):
                return False
            now = datetime.now(UTC)
            new_values: dict[str, object] = {
                "username": db_user.username,
                "email": db_user.email,
                "password_hash": self.hash_password(new_password),
                "role": db_user.role,
                "is_active": db_user.is_active,
                "created_at": db_user.created_at,
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
