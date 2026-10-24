"""Hydrate exact account facts from declared financial notifications without replaying money deltas."""

import asyncio
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from snapper.core.contract_account_projection import ContractAccountSnapshot
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.versioned_financial_publication_payload import FinancialPublicationPayload
from snapper.core.versioned_financial_publication_payload import (
    validate_financial_publication_payload,
)
from snapper.data.contract_account_projection_store import ContractAccountProjectionError
from snapper.data.contract_account_projection_store import load_bound_contract_account_snapshot
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.financial_publication_history import get_bound_versioned_financial_publication
from snapper.messaging.schemas.financial_publication import FinancialPublicationData


@dataclass(frozen=True, slots=True)
class _ReceivedFinancialPublication:
    """Keep independent notification views together until actual immutable graph rebinding."""

    topic: str
    public_id: str
    payload: FinancialPublicationPayload


class VersionedContractAccountProjection:
    """Observe one exact declared account without applying notional or modeled PnL as cash.

    Startup and restart use explicit refresh independently of local delivery
    markers. Unknown/partial states remain held. Cash-only facts are included
    by hydration and require a separate publication schema. Each declared
    notification version uses its complete historical source reader.
    """

    def __init__(self, repository: AccountingRepository, scope: ExecutionAccountScope) -> None:
        """Bind an independent account reader to its exact real financial namespace.

        Args:
            repository: Actual repository and wallet-fence provider, independent of booking.
            scope: Exact wallet/exchange/mode/environment/margin account without instrument aliases.
        """
        self._repository = repository
        self._scope = ExecutionAccountScope.model_validate(scope)
        self._lock = asyncio.Lock()
        self._snapshot: ContractAccountSnapshot | None = None

    @property
    def snapshot(self) -> ContractAccountSnapshot | None:
        """Return an independent immutable view of the last successfully committed hydration.

        Returns:
            A copied partial source-bound view, or None before startup refresh.
        """
        return (
            None
            if self._snapshot is None
            else ContractAccountSnapshot.model_validate(self._snapshot)
        )

    async def refresh(self) -> ContractAccountSnapshot:
        """Rebind actual retained facts at startup without relying on notification delivery.

        Returns:
            A fresh independent partial account view, with no new admission permission.
        """
        return await self._refresh(None)

    async def __call__(
        self, topic: str, public_id: str, payload: FinancialPublicationPayload
    ) -> None:
        """Verify an actual declared notification before refreshing a disposable account cache.

        Args:
            topic: Exact retained accounting topic, separate from order execution topics.
            public_id: Actual immutable publication UUID, unchanged on redelivery.
            payload: Strict reference-only message whose complete real graph will be rebound.
        """
        received = _ReceivedFinancialPublication(
            topic,
            public_id,
            validate_financial_publication_payload(payload),
        )
        await self._refresh(received)

    async def receive_message(self, topic: str, message: FinancialPublicationData) -> None:
        """Receive an actual typed bus envelope before the same durable source verification.

        Args:
            topic: Actual transport frame topic, independent of the payload's copied field.
            message: Parsed versioned financial envelope preserving the original intent UUID.
        """
        actual = FinancialPublicationData.model_validate(message)
        if actual.topic != topic:
            raise ContractAccountProjectionError("versioned_projection_transport_topic_mismatch")
        await self(topic, actual.public_id, actual.payload)

    async def _refresh(
        self, received: _ReceivedFinancialPublication | None
    ) -> ContractAccountSnapshot:
        """Publish a copied cache view only after the real wallet-fenced read transaction commits."""
        async with self._lock:
            async with self._repository.session() as session:
                await self._repository._begin_execution_insert_transaction(session)
                await self._repository._acquire_execution_fence_lock(
                    session, self._scope.wallet_public_id
                )
                clock = datetime.now(UTC)
                if received is not None:
                    bound = await get_bound_versioned_financial_publication(
                        session, received.public_id
                    )
                    if (
                        bound.topic != received.topic
                        or bound.payload != received.payload
                        or bound.payload.scope != self._scope
                        or bound.payload.recorded_at > clock
                    ):
                        raise ContractAccountProjectionError(
                            "versioned_projection_actual_publication_mismatch"
                        )
                snapshot = await load_bound_contract_account_snapshot(session, self._scope, clock)
                await session.commit()
            self._snapshot = ContractAccountSnapshot.model_validate(snapshot)
            return ContractAccountSnapshot.model_validate(self._snapshot)
