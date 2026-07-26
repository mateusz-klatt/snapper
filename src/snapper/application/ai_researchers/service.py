"""AI researcher principal provisioning.

Researcher creation intentionally persists only authentication identity
and token inventory rows. It does not create trading caps, operator
memberships, or AI delegate runtime state, so researcher liveness can
never participate in consult admission accounting.
"""

import asyncio
import secrets
from datetime import UTC
from datetime import datetime
from typing import ClassVar
from uuid import uuid7

import bcrypt
from loguru import logger
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.api.schemas.ai_researchers import ResearcherCreateBody
from snapper.api.schemas.ai_researchers import ResearcherCreatedPayload
from snapper.api.schemas.ai_researchers import ResearcherRead
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.data.models import User
from snapper.data.models import UserActiveToken
from snapper.data.repository import Repository
from snapper.data.repository import where_active
from snapper.messaging.infrastructure.publisher import SequenceTracker

_RESEARCHERS_TOPIC = "ai_researchers"

MAX_AI_RESEARCHERS_PER_OWNER: int = 2
"""Maximum number of active researchers owned by one operator.

Two slots allow one live principal and one overlapping credential or
model rotation while bounding credential proliferation.
"""


class ResearcherProliferationError(Exception):
    """Raised when an owner already holds the active researcher cap."""


class InvalidResearcherOwnerPrincipalError(Exception):
    """Raised when the provisioning principal has no stable user identifier."""


class ResearcherService:
    """Provision research-only automation principals."""

    _owner_locks: ClassVar[dict[str, asyncio.Lock]] = {}

    def __init__(
        self,
        repository: Repository,
        token_manager: TokenManager,
        tracker: SequenceTracker | None = None,
    ) -> None:
        """Wire researcher provisioning dependencies.

        Args:
            repository: Active SQLAlchemy repository.
            token_manager: Token manager used to mint the long-lived token.
            tracker: Optional deterministic provenance sequence tracker.
        """
        self.repository = repository
        self.token_manager = token_manager
        self._tracker = tracker or SequenceTracker()

    @classmethod
    def _get_owner_lock(cls, owner_public_id: str) -> asyncio.Lock:
        """Return the shared in-process creation lock for an owner.

        Args:
            owner_public_id: Stable identifier of the provisioning operator.

        Returns:
            Lock serializing the owner's researcher count and insert.
        """
        lock = cls._owner_locks.get(owner_public_id)
        if lock is None:
            lock = asyncio.Lock()
            cls._owner_locks[owner_public_id] = lock
        return lock

    async def create_researcher(
        self,
        owner: AuthPrincipal,
        body: ResearcherCreateBody,
    ) -> ResearcherCreatedPayload:
        """Atomically provision a research-only user and bearer token.

        Args:
            owner: Operator or administrator provisioning the researcher.
            body: Validated label and optional narrower token permission set.

        Returns:
            One-shot researcher projection and long-lived access token.

        Raises:
            InvalidResearcherOwnerPrincipalError: If the owner has no stable
                user public identifier.
            ResearcherProliferationError: If the owner already has two active
                researcher principals.
            PermissionScopeError: If the requested token scope exceeds the
                AI researcher role ceiling.
        """
        self._guard_owner(owner.user_public_id)
        async with self._get_owner_lock(owner.user_public_id):
            return await self._create_researcher_locked(owner=owner, body=body)

    async def _create_researcher_locked(
        self,
        *,
        owner: AuthPrincipal,
        body: ResearcherCreateBody,
    ) -> ResearcherCreatedPayload:
        """Create the researcher while holding its owner's in-process lock.

        Args:
            owner: Validated provisioning principal.
            body: Validated researcher creation body.

        Returns:
            Committed researcher projection and one-shot bearer token.
        """
        async with self.repository.session() as session:
            await self._guard_proliferation(session, owner.user_public_id)
            now = datetime.now(UTC)
            label = self._slugify_label(body.label)
            researcher_user = User(
                username=self._mint_username(label),
                email=None,
                password_hash=self._mint_unusable_password_hash(),
                role=UserRole.AI_RESEARCHER.value,
                is_active=True,
                created_at=now,
                created_by_user_public_id=owner.user_public_id,
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_RESEARCHERS_TOPIC),
            )
            session.add(researcher_user)
            await session.flush()
            researcher_principal = AuthPrincipal(
                username=researcher_user.username,
                role=UserRole.AI_RESEARCHER,
                is_active=True,
                user_public_id=researcher_user.public_id,
                operator_public_ids=[],
                primary_operator_public_id="",
            )
            token = self.token_manager.create_delegate_access_token(
                researcher_principal,
                issued_at=now,
                permissions=body.permissions,
            )
            session.add(
                UserActiveToken(
                    public_id=str(uuid7()),
                    user_public_id=researcher_user.public_id,
                    jti=token.jti,
                    token_hash=hash_token(token.access_token),
                    token_type=TOKEN_TYPE_ACCESS,
                    issued_at=now,
                    expires_at=token.expires_at,
                )
            )
            await session.commit()
            researcher_read = ResearcherRead(
                public_id=researcher_user.public_id,
                username=researcher_user.username,
                label=label,
                created_by_user_public_id=owner.user_public_id,
                created_at=researcher_user.created_at,
                is_active=researcher_user.is_active,
            )
        logger.info(
            "create_researcher: owner={} researcher={} username={}",
            owner.user_public_id,
            researcher_user.public_id,
            researcher_user.username,
        )
        return ResearcherCreatedPayload(
            researcher=researcher_read,
            access_token=token.access_token,
            expires_in=token.expires_in,
        )

    @staticmethod
    async def _guard_proliferation(session: AsyncSession, owner_public_id: str) -> None:
        """Reject creation when the owner has reached the researcher cap.

        Args:
            session: Active transaction used for the owner lock and count.
            owner_public_id: Stable identifier of the provisioning operator.

        Raises:
            ResearcherProliferationError: If the active researcher count is
                already at the configured maximum.
        """
        now = datetime.now(UTC)
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
                User.role == UserRole.AI_RESEARCHER.value,
                User.is_active,
                User.known_to > now,
            )
        )
        current_count = (await session.execute(count_stmt)).scalar_one()
        if current_count >= MAX_AI_RESEARCHERS_PER_OWNER:
            raise ResearcherProliferationError(
                f"Operator {owner_public_id} already owns {current_count} active "
                f"AI researchers (limit {MAX_AI_RESEARCHERS_PER_OWNER}). Deactivate an "
                "existing researcher before creating a new one."
            )

    @staticmethod
    def _guard_owner(owner_public_id: str) -> None:
        """Reject researcher creation for a principal with a blank owner ID.

        Args:
            owner_public_id: Stable identifier carried by the owner principal.

        Raises:
            InvalidResearcherOwnerPrincipalError: If the identifier is blank.
        """
        if not owner_public_id:
            raise InvalidResearcherOwnerPrincipalError(
                "AI researcher provisioning requires a populated user_public_id "
                "on the caller's principal."
            )

    @staticmethod
    def _slugify_label(label: str) -> str:
        """Normalize a researcher label for embedding in its username.

        Args:
            label: Operator-provided human-readable label.

        Returns:
            Lowercase alphanumeric slug or ``researcher`` when empty after
            normalization.
        """
        cleaned = "".join(
            character for character in label.lower() if character.isalnum() or character == "-"
        )
        return cleaned.strip("-") or "researcher"

    @staticmethod
    def _mint_username(label: str) -> str:
        """Build a collision-resistant researcher username.

        Args:
            label: Normalized label no longer than 44 characters.

        Returns:
            Username in ``ai-research-<label>-<suffix>`` form.
        """
        suffix = secrets.token_urlsafe(6).replace("_", "").replace("-", "")[:6].lower()
        return f"ai-research-{label}-{suffix}"

    @staticmethod
    def _mint_unusable_password_hash() -> str:
        """Generate a password hash whose random source is never disclosed.

        Returns:
            Bcrypt hash satisfying the non-null user password column.
        """
        random_secret = secrets.token_urlsafe(32).encode("utf-8")
        return bcrypt.hashpw(random_secret, bcrypt.gensalt()).decode("utf-8")
