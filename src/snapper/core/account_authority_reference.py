"""Explicit immutable authority references without database or transport dependencies.

The reference identifies an existing source family and its actual row knowledge
clock. Its shape alone proves neither that the row exists nor that its account
is current, funded or permitted to trade. Bound readers establish those facts
separately, and each retained payload keeps its original canonical hash domain.
"""

from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict

from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.native_account_authority import NativeAccountAuthorityPayload

type AccountAuthorityFamily = Literal["legacy_route_v1", "native_accounts_v1"]
type AccountAuthorityPayload = ExecutionRouteAuthorityPayload | NativeAccountAuthorityPayload


class AccountAuthorityReference(BaseModel):
    """An exact family-qualified reference, independently rebound before interpretation."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    family: AccountAuthorityFamily
    public_id: EvidenceUUID
    content_digest: EvidenceDigest
    recorded_at: EvidenceInstant
