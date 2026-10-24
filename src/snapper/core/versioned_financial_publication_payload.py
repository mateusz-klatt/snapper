"""Select immutable financial notification codecs without changing either historical domain."""

from typing import Annotated

from pydantic import Discriminator
from pydantic import Tag
from pydantic import TypeAdapter

from snapper.core.financial_publication import ContractPositionPublication
from snapper.core.native_financial_publication_v2 import NativeContractPositionPublicationV2


def _declared_publication_version(value: object) -> str | None:
    """Select only an explicit exact integer version while retaining each original model validator."""
    version: object
    if isinstance(value, (ContractPositionPublication, NativeContractPositionPublicationV2)):
        version = value.schema_version
    elif isinstance(value, dict):
        version = value.get("schema_version")
    else:
        return None
    if type(version) is not int:
        return None
    if version == 1:
        return "1"
    if version == 2:
        return "2"
    return None


type FinancialPublicationPayload = Annotated[
    Annotated[ContractPositionPublication, Tag("1")]
    | Annotated[NativeContractPositionPublicationV2, Tag("2")],
    Discriminator(_declared_publication_version),
]
_PUBLICATION: TypeAdapter[FinancialPublicationPayload] = TypeAdapter(FinancialPublicationPayload)


def validate_financial_publication_payload(
    payload: FinancialPublicationPayload,
) -> FinancialPublicationPayload:
    """Revalidate the declared notification family and every nested original reference.

    Args:
        payload: A typed but potentially unsafely copied notification.

    Returns:
        The fully validated declared variant, without proving any referenced row exists.
    """
    return _PUBLICATION.validate_python(payload)


def versioned_financial_publication_topic(payload: FinancialPublicationPayload) -> str:
    """Derive the original version-specific topic solely from the validated notification.

    Args:
        payload: Original immutable reference payload, awaiting actual database rebinding.

    Returns:
        The exact existing legacy or native topic, outside executable order and fill families.
    """
    actual = validate_financial_publication_payload(payload)
    family = (
        "accounting.native_position.v2"
        if isinstance(actual, NativeContractPositionPublicationV2)
        else "accounting.contract_position"
    )
    scope = actual.scope
    return (
        f"{family}.{scope.exchange}.{scope.environment}."
        f"{scope.wallet_public_id}.{actual.instrument_public_id}"
    )
