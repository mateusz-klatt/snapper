"""Carry immutable financial references through the actual typed message transport."""

from typing import Literal
from typing import Self

from pydantic import ConfigDict
from pydantic import Field
from pydantic import StrictInt
from pydantic import model_validator

from snapper.api.schemas.base import StrictDataSchema
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.versioned_financial_publication_payload import FinancialPublicationPayload
from snapper.core.versioned_financial_publication_payload import (
    versioned_financial_publication_topic,
)


class FinancialPublicationData(StrictDataSchema[Literal["financial_publication"]]):
    """Transport one original intent UUID with fresh, independent bus provenance.

    Consumers rebind this identity to the actual durable publication before
    refreshing account state. This envelope supplies no cash delta or order
    instruction, and a valid shape alone establishes no source ownership.
    """

    model_config = ConfigDict(frozen=True, revalidate_instances="always", hide_input_in_errors=True)

    type: Literal["financial_publication"] = "financial_publication"
    public_id: EvidenceUUID
    wallet_public_id: EvidenceUUID
    session_id: EvidenceUUID
    sequence_id: StrictInt = Field(gt=0)
    timestamp: EvidenceInstant
    payload: FinancialPublicationPayload

    @model_validator(mode="after")
    def validate_notification_envelope(self) -> Self:
        """Bind routing copies and transport knowledge to the declared original payload.

        Returns:
            The same envelope after rejecting contradictory wallet, topic or local clocks.
        """
        if self.wallet_public_id != self.payload.scope.wallet_public_id:
            raise ValueError("Financial notification wallet contradicts its original payload")
        if self.timestamp < self.payload.recorded_at:
            raise ValueError("Financial notification transport predates its original recording")
        if self.topic is not None and self.topic != versioned_financial_publication_topic(
            self.payload
        ):
            raise ValueError("Financial notification topic contradicts its original payload")
        return self

    def require_topic(self, topic: str) -> None:
        """Bind the actual received frame to its stamped and payload-derived topic.

        Args:
            topic: Actual transport frame topic, supplied separately from the JSON envelope.
        """
        actual = FinancialPublicationData.model_validate(self)
        if actual.topic != topic or topic != versioned_financial_publication_topic(actual.payload):
            raise ValueError(
                "Financial notification actual transport topic contradicts its payload"
            )

    def publish_to(self, topic: str) -> bytes:
        """Use the actual transport stamping path only after validating its exact destination.

        Args:
            topic: Original version-specific notification topic selected by the retained intent.

        Returns:
            Complete UTF-8 message bytes preserving the immutable publication UUID.
        """
        actual = FinancialPublicationData.model_validate(self)
        if topic != versioned_financial_publication_topic(actual.payload):
            raise ValueError("Financial notification cannot publish to another topic")
        return StrictDataSchema.publish_to(actual, topic)
