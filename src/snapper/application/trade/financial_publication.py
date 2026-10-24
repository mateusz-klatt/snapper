"""Recover interrupted local publication of committed accounting reference notifications."""

from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from uuid import uuid7

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.financial_publication import ContractPositionPublication
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.financial_publication_delivery import load_pending_financial_publications
from snapper.data.financial_publication_delivery import record_financial_publication_delivery
from snapper.messaging.infrastructure.publisher import SequenceTracker

type FinancialPublish = Callable[[str, str, ContractPositionPublication], Awaitable[None]]


async def publish_pending_financial_notifications(
    repository: AccountingRepository,
    scope: ExecutionAccountScope,
    publish: FinancialPublish,
    tracker: SequenceTracker,
) -> int:
    """Publish one bounded committed page without holding a database fence during network I/O.

    A crash after publication and before the receipt commit causes redelivery of
    the same immutable publication UUID. The callback and all consumers must
    preserve that identity. A local send return is not broker persistence or
    consumer acknowledgement. Stateful consumers must recover independently
    from the durable ledger and deduplicate applied facts; this notification
    path alone cannot establish end-to-end delivery. These accounting
    references never enter the legacy spot fill topic.

    Args:
        repository: Actual financial repository, independent of the caller's booking transaction.
        scope: Exact historical account whose committed notifications should be recovered.
        publish: Actual async publisher accepting topic, immutable message UUID and typed payload.
        tracker: Runtime provenance allocator for local delivery receipts.

    Returns:
        Number of notifications whose local delivery receipts committed in this page.
    """
    pending = await load_pending_financial_publications(repository, scope)
    completed = 0
    for intent in pending:
        await publish(intent.topic, intent.public_id, intent.payload)
        published_at = datetime.now(UTC)
        await record_financial_publication_delivery(
            repository,
            intent.public_id,
            published_at,
            SnapshotWriteProvenance(
                public_id=str(uuid7()),
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(
                    f"accounting.delivery.{scope.exchange}.{scope.wallet_public_id}"
                ),
                timestamp=datetime.now(UTC),
            ),
        )
        completed += 1
    return completed
