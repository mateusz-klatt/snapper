"""Dispatch retained publication versions explicitly without changing legacy canonical readers."""

from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.data.financial_publication_store import BoundFinancialPublication
from snapper.data.financial_publication_store import FinancialPublicationError
from snapper.data.financial_publication_store import get_bound_financial_publication
from snapper.data.models import FinancialPublicationDelivery
from snapper.data.models import FinancialPublicationIntent
from snapper.data.models import is_open_ended_version
from snapper.data.native_financial_publication_store_v2 import BoundNativePositionPublicationV2
from snapper.data.native_financial_publication_store_v2 import (
    get_bound_native_position_publication_v2,
)

type BoundVersionedFinancialPublication = BoundFinancialPublication | BoundNativePositionPublicationV2


class _PublicationVersion(BaseModel):
    """Select exactly one declared codec before its complete actual source rebinding."""

    model_config = ConfigDict(extra="ignore", strict=True)

    schema_version: Literal[1, 2]

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_exact_version(cls, value: object) -> object:
        """Refuse boolean or floating aliases without reader fallback.

        Args:
            value: Original uncoerced retained publication version.

        Returns:
            The same actual integer, awaiting supported literal validation.
        """
        if type(value) is not int:
            raise ValueError("Financial publication version requires an integer")
        return value


async def get_bound_versioned_financial_publication(
    session: AsyncSession, public_id: EvidenceUUID
) -> BoundVersionedFinancialPublication:
    """Rebind a declared publication family through its exact unchanged historical reader.

    Args:
        session: Actual caller-owned historical transaction.
        public_id: Exact retained notification UUID; failed parsing never tries another family.

    Returns:
        A complete source-bound legacy or native notification, without financial admission.
    """
    row = await session.scalar(
        select(FinancialPublicationIntent).where(FinancialPublicationIntent.public_id == public_id)
    )
    if row is None:
        raise FinancialPublicationError("financial_publication_missing")
    version = _PublicationVersion.model_validate_json(row.canonical_payload)
    if version.schema_version == 1:
        return await get_bound_financial_publication(session, public_id)
    return await get_bound_native_position_publication_v2(session, public_id)


def verify_versioned_financial_delivery(
    row: FinancialPublicationDelivery, intent: BoundVersionedFinancialPublication
) -> None:
    """Bind an actual local-return receipt to its complete original declared publication.

    Args:
        row: Actual immutable delivery row being independently audited or replayed.
        intent: Complete original source-bound intent from the declared version reader.
    """
    scope = intent.payload.scope
    actual = (
        row.wallet_public_id,
        row.exchange,
        row.mode,
        row.environment,
        row.margin_account,
        row.publication_public_id,
        row.publication_digest,
        row.delivery_semantics,
    )
    expected = (
        scope.wallet_public_id,
        scope.exchange,
        scope.mode,
        scope.environment,
        scope.margin_account,
        intent.public_id,
        intent.content_digest,
        "publisher_returned",
    )
    if (
        not is_open_ended_version(row.known_to)
        or actual != expected
        or not intent.payload.recorded_at <= row.published_at <= row.timestamp
    ):
        raise FinancialPublicationError("financial_delivery_actual_intent_mismatch")
