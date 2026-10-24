"""Refresh a disposable contract account view from real committed financial notifications."""

import asyncio
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from snapper.core.contract_account_projection import ContractAccountSnapshot
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.financial_publication import ContractPositionPublication
from snapper.data.contract_account_projection_store import ContractAccountProjectionError
from snapper.data.contract_account_projection_store import load_bound_contract_account_snapshot
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.financial_publication_store import get_bound_financial_publication


@dataclass(frozen=True, slots=True)
class _ReceivedPositionPublication:
    """Keep actual delivered notification operands together until their durable graph is checked."""

    topic: str
    public_id: str
    payload: ContractPositionPublication


class ContractAccountProjection:
    """Provide the real v1 publisher callback and explicit restart hydration.

    The exact declared scope is fixed for this instance. Refresh serialization
    covers durable message verification, journal hydration and cache replacement.
    A stale message refreshes current facts instead of replaying a financial
    delta. The cache has no trading authority and exceptions preserve its prior
    value. Cash-only updates require their own durable publication path.
    """

    def __init__(self, repository: AccountingRepository, scope: ExecutionAccountScope) -> None:
        """Bind one disposable observer to an actual financial repository and exact scope.

        Args:
            repository: Real transaction and wallet-fence provider, independent of booking.
            scope: Exact declared wallet, exchange, mode, environment and margin profile.
        """
        self._repository = repository
        self._scope = ExecutionAccountScope.model_validate(scope)
        self._lock = asyncio.Lock()
        self._snapshot: ContractAccountSnapshot | None = None

    @property
    def snapshot(self) -> ContractAccountSnapshot | None:
        """Return the last successfully committed refresh without inventing an initial account.

        Returns:
            The immutable partial view or None before explicit startup/message hydration.
        """
        return self._snapshot

    async def refresh(self) -> ContractAccountSnapshot:
        """Hydrate independently of past publication delivery markers during startup or recovery.

        Returns:
            A current source-bound partial view whose readiness remains held.
        """
        return await self._refresh(None)

    async def __call__(
        self, topic: str, public_id: str, payload: ContractPositionPublication
    ) -> None:
        """Receive the actual FinancialPublish v1 callback without applying a spot fill.

        Args:
            topic: Exact delivered topic, independently checked against the retained intent.
            public_id: Immutable actual publication UUID, preserved across redelivery.
            payload: Actual v1 Execution/position reference payload, never supplied money deltas.
        """
        received = _ReceivedPositionPublication(
            topic, public_id, ContractPositionPublication.model_validate(payload)
        )
        await self._refresh(received)

    async def _refresh(
        self, received: _ReceivedPositionPublication | None
    ) -> ContractAccountSnapshot:
        """Replace the cache only after a real fenced read transaction completes successfully."""
        async with self._lock:
            async with self._repository.session() as session:
                await self._repository._begin_execution_insert_transaction(session)
                await self._repository._acquire_execution_fence_lock(
                    session, self._scope.wallet_public_id
                )
                clock = datetime.now(UTC)
                if received is not None:
                    bound = await get_bound_financial_publication(session, received.public_id)
                    if (
                        bound.topic != received.topic
                        or bound.payload != received.payload
                        or bound.payload.scope != self._scope
                    ):
                        raise ContractAccountProjectionError(
                            "contract_projection_publication_binding_mismatch"
                        )
                    if bound.payload.recorded_at > clock:
                        raise ContractAccountProjectionError(
                            "contract_projection_publication_knowledge_unavailable"
                        )
                snapshot = await load_bound_contract_account_snapshot(session, self._scope, clock)
                await session.commit()
            self._snapshot = snapshot
            return snapshot
