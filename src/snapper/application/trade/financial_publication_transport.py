"""Adapt committed financial notifications to the existing typed message publisher."""

from datetime import UTC
from datetime import datetime

from snapper.core.versioned_financial_publication_payload import FinancialPublicationPayload
from snapper.core.versioned_financial_publication_payload import (
    validate_financial_publication_payload,
)
from snapper.core.versioned_financial_publication_payload import (
    versioned_financial_publication_topic,
)
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.schemas.financial_publication import FinancialPublicationData


class FinancialPublicationTransport:
    """Publish source-bound intent identities with per-send transport provenance.

    A return proves only that the actual local publisher returned. Receipt
    persistence remains the recovery publisher's separate transaction.
    """

    def __init__(self, publisher: MessagePublisher) -> None:
        """Retain the actual typed publisher and its existing runtime provenance tracker.

        Args:
            publisher: Real transport owner whose send path validates and stamps the topic.
        """
        self._publisher = publisher

    async def __call__(
        self, topic: str, public_id: str, payload: FinancialPublicationPayload
    ) -> None:
        """Send one actual immutable intent without replacing its identity on redelivery.

        Args:
            topic: Original topic returned by the complete versioned intent reader.
            public_id: Original immutable publication UUID, independent of the current bus session.
            payload: Original declared notification, independently rebound by the recovery caller.
        """
        actual = validate_financial_publication_payload(payload)
        if topic != versioned_financial_publication_topic(actual):
            raise ValueError(
                "Financial publication transport topic contradicts its original payload"
            )
        tracker = self._publisher.tracker
        message = FinancialPublicationData(
            public_id=public_id,
            wallet_public_id=actual.scope.wallet_public_id,
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(topic),
            timestamp=datetime.now(UTC),
            payload=actual,
        )
        await self._publisher.send(topic, message)
