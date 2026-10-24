"""Read admitted position families through complete actual source/unit/claim and predecessor history."""

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.data.native_financial_event_family import AdmittedNativeEventMaterialV3
from snapper.data.native_financial_event_family import get_native_financial_event_family
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_position_history_v2 import BoundNativePositionEventV2
from snapper.data.native_position_history_v2 import load_native_position_history_v2


async def get_bound_native_position_family(
    session: AsyncSession, public_id: str, known_at: datetime
) -> BoundNativePositionEventV2:
    """Require every actual original admission edge before returning a new admitted position family.

    Original native-v2 financial rows stay available through their unchanged
    explicit historical getter. This reader refuses to certify an unadmitted
    row as a version-three original source, without schema guessing or upgrade.

    Args:
        session: Actual caller-owned historical or wallet-fenced account transaction.
        public_id: Exact retained admitted-native position event UUID.
        known_at: Actual local retained-knowledge cutoff, independent of venue execution time.

    Returns:
        Normalized real position operands only after complete admitted source and actual chain rebind.
    """
    material = await get_native_financial_event_family(session, public_id, known_at)
    if not isinstance(material, AdmittedNativeEventMaterialV3) or not isinstance(
        material.payload, ContractPositionEventV2
    ):
        raise NativeFinancialBindingError("native_position_durable_admission_history_unproven")
    value = material.payload.operands
    events = await load_native_position_history_v2(
        session, value.scope, value.instrument_public_id, known_at
    )
    for event in events:
        predecessor = await get_native_financial_event_family(
            session, event.row.public_id, known_at
        )
        if not isinstance(predecessor, AdmittedNativeEventMaterialV3):
            raise NativeFinancialBindingError("native_position_prior_admission_migration_required")
    selected = next((event for event in events if event.row.public_id == public_id), None)
    if selected is None:
        raise NativeFinancialBindingError("native_position_event_not_known_at_cutoff")
    return selected
