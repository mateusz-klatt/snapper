"""ScopeGrantService — SOLE publisher of the three admin scope events.

Owns create / handover / revoke for ``wallet_operator_scope_grants``
and emits one bus event per mutation so subscribers
(``WebSocketAuthManager`` for AI_DELEGATE revalidation,
``MarketPersistPolicy`` for the persist-set refresh) can react live
without waiting for a reconnect:

- ``admin.scope_granted`` — published by ``create_grant``.
- ``admin.scope_handed_over`` — published by ``handover``.
- ``admin.scope_revoked`` — published by ``revoke_grant``.

All three events are **wake-up signals only** (see ``ScopeRevokedData``
docstring): subscribers ignore payload identity fields for state
rebuild and re-run ``list_scope_grant_instrument_pairs`` against the
post-event DB snapshot.

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

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import role_grants_permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.config.settings import get_settings
from snapper.data.repository import get_repository
from snapper.data.repository_types import CreateScopeGrantRequest
from snapper.data.repository_types import ScopeGrantRow
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ScopeGrantedData
from snapper.messaging.schemas.data import ScopeHandedOverData
from snapper.messaging.schemas.data import ScopeRevokedData
from snapper.messaging.topics.builders import admin_topic

_SCOPE_REVOKED_TOPIC = "scope_revoked"
_SCOPE_GRANTED_TOPIC = "scope_granted"
_SCOPE_HANDED_OVER_TOPIC = "scope_handed_over"


class ScopeGrantService:
    """Stateless singleton that owns the three admin scope-grant publishers."""

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

        Orchestration (single publisher invariant):

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

    async def create_grant(
        self,
        insert_request: CreateScopeGrantRequest,
        *,
        now: datetime,
    ) -> ScopeGrantRow:
        """Insert a new active scope grant and publish ``admin.scope_granted``.

        Orchestration mirrors :meth:`revoke_grant`:

        1. ``repository.create_scope_grant`` performs the insert under
           overlap detection (raises ``ScopeGrantConflictError`` on
           collisions; ``ScopeGrantValidationError`` on XOR mismatch).
        2. After the insert commits, publish ``admin.scope_granted``
           carrying the new grant identity + creator metadata.

        Publish failures are logged but do NOT roll back the insert —
        a transient broker hiccup must never leave the grant in an
        inconsistent half-created state.

        Args:
            insert_request: Validated request including REST-tracker
                provenance (session_id + sequence_id + timestamp).
            now: Bus-time for the event payload. Distinct from the
                ``insert_request.timestamp`` (REST provenance) so the
                event timeline reflects publisher wall-clock.

        Returns:
            The newly-inserted ``ScopeGrantRow``.

        Raises:
            ScopeGrantConflictError: Overlap against an existing active
                grant on the same operator / wallet / scope.
            ScopeGrantValidationError: XOR mismatch between
                ``scope_kind`` and the populated resource id.
            ScopeGrantNotFoundError: Referenced operator / wallet does
                not exist.
        """
        row = await self.repository.create_scope_grant(insert_request)
        await self._publish_scope_granted(
            grant_public_id=row["public_id"],
            operator_public_id=row["operator_public_id"],
            wallet_public_id=row["wallet_public_id"],
            scope_kind=row["scope_kind"],
            underlying_public_id=row["underlying_public_id"],
            instrument_public_id=row["instrument_public_id"],
            granted_at=now,
            granted_by_user_public_id=insert_request["granted_by_user_public_id"],
            reason=insert_request["note"],
        )
        return row

    async def handover(
        self,
        *,
        grant_public_id: str,
        destination_operator_public_id: str,
        handover_by_user_public_id: str,
        reason: str | None,
        session_id: str,
        sequence_id: int,
        now: datetime,
    ) -> tuple[ScopeGrantRow, ScopeGrantRow]:
        """Atomically transfer an active scope grant to a different operator.

        Orchestration mirrors :meth:`revoke_grant` but the repository
        performs a single-transaction SCD2-close + insert under
        cross-scope overlap detection.

        REST-tracker provenance (``session_id`` / ``sequence_id``) is
        passed through to the repository so the new grant row carries
        the originating API call's audit trail. The bus event uses the
        service's own tracker (handover wake-up signals are a separate
        provenance stream from the REST DDL).

        Args:
            grant_public_id: Source grant identity (will be closed).
            destination_operator_public_id: Operator receiving the scope.
            handover_by_user_public_id: ADMIN user driving the handover.
            reason: Optional admin-supplied rationale.
            session_id: REST-tracker session ID (recorded on new row).
            sequence_id: REST-tracker sequence number (recorded on new row).
            now: Bus-time for the SCD2 close + insert + event payload.

        Returns:
            ``(closed_from_grant, new_grant)`` — both as ``ScopeGrantRow``.

        Raises:
            ScopeGrantValidationError: Self-handover or other validation.
            ScopeGrantNotFoundError: Source grant or destination operator
                does not exist.
            ScopeGrantConflictError: Cross-scope overlap against the
                destination operator's existing grants.
        """
        closed, new_row = await self.repository.handover_grant(
            from_grant_public_id=grant_public_id,
            to_operator_public_id=destination_operator_public_id,
            granted_by_user_public_id=handover_by_user_public_id,
            reason=reason,
            session_id=session_id,
            sequence_id=sequence_id,
            timestamp=now,
        )
        await self._publish_scope_handed_over(
            grant_public_id=new_row["public_id"],
            from_operator_public_id=closed["operator_public_id"],
            to_operator_public_id=new_row["operator_public_id"],
            wallet_public_id=new_row["wallet_public_id"],
            scope_kind=new_row["scope_kind"],
            underlying_public_id=new_row["underlying_public_id"],
            instrument_public_id=new_row["instrument_public_id"],
            handover_at=now,
            handover_by_user_public_id=handover_by_user_public_id,
            reason=reason,
        )
        return closed, new_row

    async def _publish_scope_granted(
        self,
        *,
        grant_public_id: str,
        operator_public_id: str,
        wallet_public_id: str,
        scope_kind: str,
        underlying_public_id: str | None,
        instrument_public_id: str | None,
        granted_at: datetime,
        granted_by_user_public_id: str,
        reason: str | None,
    ) -> None:
        """Emit ``admin.scope_granted`` after the repository insert commit.

        Best-effort: a missing publisher logs a warning instead of
        raising. A send failure degrades to a logged exception so a
        transient broker hiccup never leaves the grant un-broadcast.
        Mirrors :meth:`_publish_scope_revoked` exactly.
        """
        topic = admin_topic(_SCOPE_GRANTED_TOPIC)
        if scope_kind not in ("underlying", "instrument"):
            raise ValueError(
                f"invalid scope_kind {scope_kind!r} from repository; "
                "expected 'underlying' or 'instrument'"
            )
        narrowed_scope_kind = cast(Literal["underlying", "instrument"], scope_kind)
        if self._msg_publisher is None:
            logger.warning(
                "admin.scope_granted NOT broadcast for grant_public_id={}: "
                "ScopeGrantService publisher unavailable (multi-instance fanout disabled)",
                grant_public_id,
            )
            return
        payload = ScopeGrantedData(
            public_id=str(uuid7()),
            timestamp=granted_at,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(topic),
            grant_public_id=grant_public_id,
            operator_public_id=operator_public_id,
            wallet_public_id=wallet_public_id,
            scope_kind=narrowed_scope_kind,
            underlying_public_id=underlying_public_id,
            instrument_public_id=instrument_public_id,
            granted_at=granted_at,
            granted_by_user_public_id=granted_by_user_public_id,
            reason=reason,
        )
        try:
            await self._msg_publisher.send(topic, payload)
        except Exception as exc:
            logger.exception(
                "Failed to broadcast admin.scope_granted for grant_public_id={}: {}",
                grant_public_id,
                exc,
            )

    async def _publish_scope_handed_over(
        self,
        *,
        grant_public_id: str,
        from_operator_public_id: str,
        to_operator_public_id: str,
        wallet_public_id: str,
        scope_kind: str,
        underlying_public_id: str | None,
        instrument_public_id: str | None,
        handover_at: datetime,
        handover_by_user_public_id: str,
        reason: str | None,
    ) -> None:
        """Emit ``admin.scope_handed_over`` after the repository transaction commit.

        Same failure contract as :meth:`_publish_scope_granted`.
        """
        topic = admin_topic(_SCOPE_HANDED_OVER_TOPIC)
        if scope_kind not in ("underlying", "instrument"):
            raise ValueError(
                f"invalid scope_kind {scope_kind!r} from repository; "
                "expected 'underlying' or 'instrument'"
            )
        narrowed_scope_kind = cast(Literal["underlying", "instrument"], scope_kind)
        if self._msg_publisher is None:
            logger.warning(
                "admin.scope_handed_over NOT broadcast for grant_public_id={}: "
                "ScopeGrantService publisher unavailable (multi-instance fanout disabled)",
                grant_public_id,
            )
            return
        payload = ScopeHandedOverData(
            public_id=str(uuid7()),
            timestamp=handover_at,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(topic),
            grant_public_id=grant_public_id,
            from_operator_public_id=from_operator_public_id,
            to_operator_public_id=to_operator_public_id,
            wallet_public_id=wallet_public_id,
            scope_kind=narrowed_scope_kind,
            underlying_public_id=underlying_public_id,
            instrument_public_id=instrument_public_id,
            handover_at=handover_at,
            handover_by_user_public_id=handover_by_user_public_id,
            reason=reason,
        )
        try:
            await self._msg_publisher.send(topic, payload)
        except Exception as exc:
            logger.exception(
                "Failed to broadcast admin.scope_handed_over for grant_public_id={}: {}",
                grant_public_id,
                exc,
            )

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

    async def list_accessible_wallet_public_ids(
        self,
        *,
        principal: AuthPrincipal,
        as_of: datetime,
    ) -> set[str]:
        """Return the set of wallet ``public_id`` values the principal can read.

        Mirrors the REST `/api/wallets` and `/api/orders` server-side
        wallet-scope filter so the WebSocket per-frame
        ``orders.events.*`` filter sees the same set of wallets as the
        REST snapshot endpoints — closes the v0.7.0 RBAC asymmetry
        where REST applied scope filtering and WS bridge did not.

        Global-scope permission bypass: a named set carrying
        ``IMPERSONATE_OPERATOR`` receives every active wallet. Empty
        operator-set on a non-global principal
        principal returns an empty set without hitting the
        repository's joined query path — matches the wallet-picker
        contract that "no operator membership = no wallet visibility".

        Args:
            principal: Authenticated caller whose named permissions and
                ``operator_public_ids`` drive the wallet-scope filter.
            as_of: Wall-clock for SCD2-active filtering on operator
                memberships and scope grants.

        Returns:
            Set of wallet ``public_id`` strings the principal can read.
            Empty set when the principal has no operator memberships
            without global scope when no grants reach it transitively.
        """
        if role_grants_permission(principal.role, Permission.IMPERSONATE_OPERATOR):
            rows = await self.repository.list_active_wallets(as_of=as_of)
        elif not principal.operator_public_ids:
            return set()
        else:
            rows = await self.repository.list_accessible_wallets_for_operators(
                operator_public_ids=list(principal.operator_public_ids),
                as_of=as_of,
            )
        return {row["public_id"] for row in rows}

    async def has_grant_for_delegate(
        self,
        *,
        delegate_public_id: str,
        wallet_public_id: str,
        instrument_public_id: str,
        as_of: datetime,
    ) -> bool:
        """AI delegate scope check forwarder.

        Wraps :meth:`Repository.has_grant_for_delegate` so the service stays
        the single SOLE owner of the delegate-scope contract. The repo
        implementation does the four-step resolve (delegate -> user ->
        operator memberships -> instrument-direct or underlying-kind grant);
        this method exists so call sites that already hold the service
        ``Depends`` don't need a separate Repository handle.

        Args:
            delegate_public_id: ``ai_delegates.public_id`` of the AI
                delegate whose scope is being checked.
            wallet_public_id: Target wallet for the scope check.
            instrument_public_id: Target instrument for the scope check.
            as_of: Wall-clock used for SCD2-active filtering on
                memberships and grants.

        Returns:
            ``True`` when the delegate has at least one active grant
            covering ``(wallet, instrument)``; ``False`` otherwise.
        """
        return await self.repository.has_grant_for_delegate(
            delegate_public_id=delegate_public_id,
            wallet_public_id=wallet_public_id,
            instrument_public_id=instrument_public_id,
            as_of=as_of,
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
