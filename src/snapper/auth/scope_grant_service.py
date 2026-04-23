"""ScopeGrantService — SOLE publisher of `admin.scope_revoked`.

Mirrors `UserService` publisher shape: the service owns
`revoke_grant()` which closes the grant via the repository and then
emits a single `admin.scope_revoked` bus event so `WebSocketAuthManager`
(the subscriber) can revalidate any AI_DELEGATE subscriptions live
without waiting for a reconnect.

Stateless singleton: repository reference, tracker ref, optional
publisher ref. No coordinator-owned mutable state. The publisher
ref is injected by the FastAPI lifespan once the shared ZMQ PUB
socket is available; tests stub it with a fake implementing
`send(topic, payload)`.
"""

from datetime import datetime
from typing import Literal
from typing import cast
from uuid import uuid7

from loguru import logger

from snapper.config.settings import get_settings
from snapper.data.repository import get_repository
from snapper.data.repository_types import ScopeGrantRow
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ScopeRevokedData
from snapper.messaging.topics.builders import admin_topic

_SCOPE_REVOKED_TOPIC = "scope_revoked"


class ScopeGrantService:
    """Stateless singleton that owns the ``admin.scope_revoked`` publisher."""

    _instance: ScopeGrantService | None = None
    _initialized: bool = False

    def __new__(cls) -> ScopeGrantService:
        """Create or return the singleton instance."""
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """Initialize repository reference and provenance tracker."""
        if self._initialized:
            return
        self._initialized = True
        settings = get_settings()
        self.repository = get_repository(settings.db_url)
        self._tracker = SequenceTracker()
        self._msg_publisher: MessagePublisher | None = None

    def set_msg_publisher(self, publisher: MessagePublisher | None) -> None:
        """Inject the bus publisher used for ``admin.scope_revoked``.

        Called from the FastAPI lifespan once a ZMQ broker connection
        is available. Injection (rather than self-managed socket)
        keeps the singleton testable.

        Args:
            publisher: Configured ``MessagePublisher`` or ``None`` to clear.
        """
        self._msg_publisher = publisher

    async def revoke_grant(
        self,
        grant_public_id: str,
        revoked_by_user_public_id: str,
        reason: str | None,
        *,
        now: datetime,
    ) -> ScopeGrantRow:
        """Revoke an active scope grant and publish the revocation event.

        Orchestration (single publisher invariant, §D7):

        1. ``repository.revoke_scope_grant`` SCD2-closes the grant
           (atomic, under per-wallet advisory lock on PostgreSQL).
        2. After the close commits, publish ``admin.scope_revoked``
           carrying the full scope identity + revoker metadata.

        Publish failures are logged but do NOT roll back the close — a
        transient broker hiccup must never leave the grant half-revoked.

        Args:
            grant_public_id: Public ID of the active grant.
            revoked_by_user_public_id: Audit identity of the ADMIN user
                initiating the revoke.
            reason: Optional free-form audit note (emitted on the
                event; NOT persisted to the closed grant row).
            now: Bus-time for the SCD2 close + event timestamp.

        Returns:
            The ``ScopeGrantRow`` projection of the grant as it exists
            immediately after the SCD2 close (``known_to == now``).

        Raises:
            ScopeGrantNotFoundError: Grant does not exist OR is already
                closed (double-revoke).
        """
        closed = await self.repository.revoke_scope_grant(
            grant_public_id=grant_public_id,
            revoked_by_user_public_id=revoked_by_user_public_id,
            revoked_at=now,
            reason=reason,
        )
        await self._publish_scope_revoked(
            grant_public_id=closed["public_id"],
            operator_public_id=closed["operator_public_id"],
            wallet_public_id=closed["wallet_public_id"],
            scope_kind=closed["scope_kind"],
            underlying_public_id=closed["underlying_public_id"],
            instrument_public_id=closed["instrument_public_id"],
            revoked_at=now,
            revoked_by_user_public_id=revoked_by_user_public_id,
            reason=reason,
        )
        return closed

    async def _publish_scope_revoked(
        self,
        *,
        grant_public_id: str,
        operator_public_id: str,
        wallet_public_id: str,
        scope_kind: str,
        underlying_public_id: str | None,
        instrument_public_id: str | None,
        revoked_at: datetime,
        revoked_by_user_public_id: str | None,
        reason: str | None,
    ) -> None:
        """Emit ``admin.scope_revoked`` after the repository close commit.

        Best-effort: a missing publisher (singleton spun up before the
        FastAPI lifespan attached one) logs a warning instead of raising
        — the local in-process subscriber (if any) would only matter for
        cross-instance fanout, and the DB close is already the source of
        truth. A send failure degrades to a logged exception so a transient
        broker hiccup never leaves the grant half-revoked.
        """
        topic = admin_topic(_SCOPE_REVOKED_TOPIC)
        if scope_kind not in ("underlying", "instrument"):
            raise ValueError(
                f"invalid scope_kind {scope_kind!r} from repository; "
                "expected 'underlying' or 'instrument'"
            )
        narrowed_scope_kind = cast(Literal["underlying", "instrument"], scope_kind)
        if self._msg_publisher is None:
            logger.warning(
                "admin.scope_revoked NOT broadcast for grant_public_id={}: "
                "ScopeGrantService publisher unavailable (multi-instance fanout disabled)",
                grant_public_id,
            )
            return
        payload = ScopeRevokedData(
            public_id=str(uuid7()),
            timestamp=revoked_at,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(topic),
            grant_public_id=grant_public_id,
            operator_public_id=operator_public_id,
            wallet_public_id=wallet_public_id,
            scope_kind=narrowed_scope_kind,
            underlying_public_id=underlying_public_id,
            instrument_public_id=instrument_public_id,
            revoked_at=revoked_at,
            revoked_by_user_public_id=revoked_by_user_public_id,
            reason=reason,
        )
        try:
            await self._msg_publisher.send(topic, payload)
        except Exception as exc:
            logger.exception(
                "Failed to broadcast admin.scope_revoked for grant_public_id={}: {}",
                grant_public_id,
                exc,
            )

    @classmethod
    def get_instance(cls) -> ScopeGrantService:
        """Get singleton instance.

        Returns:
            ScopeGrantService singleton.
        """
        if cls._instance is None:
            cls._instance = ScopeGrantService()
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear singleton for testing."""
        cls._instance = None


def get_scope_grant_service() -> ScopeGrantService:
    """Get ``ScopeGrantService`` singleton.

    Returns:
        ScopeGrantService instance.
    """
    return ScopeGrantService.get_instance()
