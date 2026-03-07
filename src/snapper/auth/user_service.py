"""User service module.

This module provides user management operations including
authentication, CRUD operations, and password management.
"""

from datetime import UTC
from datetime import datetime

import bcrypt
from sqlalchemy import select

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.user import UserProfile
from snapper.config.settings import get_settings
from snapper.data.models import User
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
            id=db_user.id,
            username=db_user.username,
            email=db_user.email,
            role=UserRole(db_user.role),
            is_active=db_user.is_active,
            created_at=db_user.created_at,
            last_login=db_user.last_login,
        )

    async def authenticate_user(self, username: str, password: str) -> UserProfile | None:
        """Authenticate user by username and password.

        Updates last_login timestamp on success.

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
            db_user.last_login = datetime.now(UTC)
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
            stmt = select(User).where(User.id == user_id, User.is_active)
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
                id=username,
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
        """Update user attributes.

        Only provided (non-None) attributes are updated.

        Args:
            user_id: User's ID.
            email: New email address.
            role: New role.
            is_active: New active status.

        Returns:
            Updated UserProfile or None if not found.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(User.id == user_id)
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return None
            if email is not None:
                db_user.email = email
            if role is not None:
                db_user.role = role.value
            if is_active is not None:
                db_user.is_active = is_active
            await session.commit()
            await session.refresh(db_user)
            return self._db_user_to_auth_user(db_user)

    async def delete_user(self, user_id: str) -> bool:
        """Soft-delete user by marking inactive.

        Args:
            user_id: User's ID.

        Returns:
            True if deleted, False if not found.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(User.id == user_id)
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return False
            db_user.is_active = False
            await session.commit()
            return True

    async def change_password(self, user_id: str, old_password: str, new_password: str) -> bool:
        """Change user's password.

        Verifies old password before updating.

        Args:
            user_id: User's ID.
            old_password: Current password for verification.
            new_password: New password to set.

        Returns:
            True if changed, False if user not found or wrong password.
        """
        async with self.repository.session() as session:
            stmt = select(User).where(User.id == user_id, User.is_active)
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                return False
            if not self._verify_password(old_password, db_user.password_hash):
                return False
            db_user.password_hash = self.hash_password(new_password)
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
