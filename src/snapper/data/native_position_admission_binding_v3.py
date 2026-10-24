"""Rebind actual original admission and structural account continuity for native position successors."""

from dataclasses import dataclass
from datetime import datetime
from typing import Self

from pydantic import model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_fill_unit_admission import NativeFillUnitAdmissionEvidence
from snapper.data.native_fill_unit_admission_store import BoundNativeFillUnitAdmission
from snapper.data.native_fill_unit_admission_store import bind_native_fill_unit_admission
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import require_native_financial_binding
from snapper.data.native_position_source_v2 import BoundNativePositionSourceV2
from snapper.data.native_position_source_v2 import NativePositionSourceRequestV2
from snapper.data.native_position_source_v2 import bind_native_position_receipt_v2


class NativePositionSourceRequestV3(NativePositionSourceRequestV2):
    """Explicit original admission/unit evidence alongside the actual independent later log source."""

    fill_unit_admission: NativeFillUnitAdmissionEvidence

    @model_validator(mode="after")
    def validate_original_unit_copy(self) -> Self:
        """Refuse a later request that substitutes another actual original unit UUID.

        Returns:
            The same strict request awaiting actual source and original graph binding.
        """
        if self.unit_snapshot_public_id != self.fill_unit_admission.unit_snapshot.public_id:
            raise ValueError("Admitted native position request names another original unit")
        return self


@dataclass(frozen=True, slots=True)
class BoundAdmittedNativePositionSourceV3:
    """Real original admission/Execution/unit and later account-log graph, without current permission."""

    source: BoundNativePositionSourceV2
    original: BoundNativeFillUnitAdmission


def _structural_profile(authority: NativeAccountAuthorityPayload) -> tuple[str, str, str | None]:
    """Separate structural native account dimensions from legitimately varying balance/profile digests."""
    profile = authority.native_profile
    return profile.native_account_key, profile.account_type, profile.settlement_currency


async def bind_admitted_native_position_source_v3(
    session: AsyncSession, request: NativePositionSourceRequestV3, known_at: datetime
) -> BoundAdmittedNativePositionSourceV3:
    """Rebind exact durable original admission before accepting a later position-log transition.

    Legacy original-family records remain readable through their existing
    historical readers. A new admitted-native record refuses a legacy original
    profile when the actual native structural profile is unproven; it does not
    infer a profile from account labels, balances or native symbol prefixes.

    Args:
        session: Actual caller-owned historical or wallet-fenced financial transaction.
        request: Actual original fill/unit/admission and later native log references.
        known_at: Real position posting or historical knowledge cutoff.

    Returns:
        Full actual original and later graphs, without current ownership or spending permission.
    """
    actual = NativePositionSourceRequestV3.model_validate(request)
    original = await bind_native_fill_unit_admission(
        session, actual.fill, actual.fill_unit_admission, known_at
    )
    base = NativePositionSourceRequestV2.model_validate(
        actual.model_dump(exclude={"fill_unit_admission"})
    )
    later = await bind_native_position_receipt_v2(session, base, known_at)
    fill_authority = original.source.authority.payload
    log_authority = later.financial_source.authority.payload
    if not isinstance(fill_authority, NativeAccountAuthorityPayload) or not isinstance(
        log_authority, NativeAccountAuthorityPayload
    ):
        raise NativeFinancialBindingError("native_position_original_structural_profile_unproven")
    require_native_financial_binding(
        original.source == later.fill and original.unit == later.unit,
        "native_position_original_admission_unit_graph_changed",
    )
    require_native_financial_binding(
        _structural_profile(fill_authority) == _structural_profile(log_authority)
        and original.source.ownership.account_uid == later.financial_source.ownership.account_uid,
        "native_position_actual_structural_profile_or_uid_changed",
    )
    return BoundAdmittedNativePositionSourceV3(later, original)
