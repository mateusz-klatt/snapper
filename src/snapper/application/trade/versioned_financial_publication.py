"""Publish original versioned accounting references after their financial commit."""

from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from uuid import uuid7

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.versioned_financial_publication_payload import FinancialPublicationPayload
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.versioned_financial_publication_delivery import (
    load_pending_versioned_financial_publications,
)
from snapper.data.versioned_financial_publication_delivery import (
    record_versioned_financial_publication_delivery,
)
from snapper.messaging.infrastructure.publisher import SequenceTracker

type VersionedFinancialPublish = Callable[[str, str, FinancialPublicationPayload], Awaitable[None]]


async def publish_pending_versioned_financial_notifications(
    repository: AccountingRepository,
    scope: ExecutionAccountScope,
    publish: VersionedFinancialPublish,
    tracker: SequenceTracker,
) -> int:
    """Recover one committed page without holding a database lock across actual publication.

    Cancellation, callback failure and an uncertain receipt commit leave the
    original publication identity available for recovery. A consumer must bind
    that identity to its retained fact and hydrate its view independently on
    startup. A successful callback does not prove downstream delivery.

    Args:
        repository: Actual repository independent of the original booking transaction.
        scope: Exact account whose committed notifications should be recovered.
        publish: Actual typed publisher preserving the topic and immutable intent UUID.
        tracker: Runtime source of receipt provenance, independent of message identity.

    Returns:
        The count of notifications whose local-return receipts committed in this page.
    """
    pending = await load_pending_versioned_financial_publications(repository, scope)
    completed = 0
    for intent in pending:
        await publish(intent.topic, intent.public_id, intent.payload)
        returned_at = datetime.now(UTC)
        await record_versioned_financial_publication_delivery(
            repository,
            intent.public_id,
            returned_at,
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
