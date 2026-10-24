"""Add durable original admission edges without rewriting native-v2 arithmetic or historical bytes.

These strict wrappers are proof references, not financial or dispatch grants.
A new financial reader must rebind their exact actual unit/admission graph on
both initial recording and history. Constructing this shape provides no rows.
"""

import hashlib
import json
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import TypeAdapter
from pydantic import field_validator
from pydantic import model_validator

from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.core.native_fill_unit_admission import NativeFillUnitAdmissionEvidence


class _AdmittedNativePositionV3(BaseModel):
    """Retain one independently verifiable original account/unit admission edge."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    schema_version: Literal[3] = 3
    fill_unit_admission: NativeFillUnitAdmissionEvidence

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_integer_version(cls, value: object) -> object:
        """Refuse numeric or string aliases before choosing a new immutable financial domain.

        Args:
            value: Original uncoerced financial wrapper discriminator.

        Returns:
            The same exact integer, awaiting the supported literal validation.
        """
        if type(value) is not int:
            raise ValueError("Admitted native position version requires an integer")
        return value


class AdmittedNativePositionEventV3(_AdmittedNativePositionV3):
    """Wrap one original position-only transition without creating another economic namespace."""

    kind: Literal["contract_position_fill"] = "contract_position_fill"
    position: ContractPositionEventV2

    @model_validator(mode="after")
    def validate_original_unit_edge(self) -> Self:
        """Bind the exact original unit and distinct execution/posting knowledge clocks.

        Returns:
            The unchanged event envelope requiring complete actual historical rebind.
        """
        evidence = self.fill_unit_admission
        value = self.position.operands
        if evidence.unit_snapshot.public_id != value.unit_snapshot_public_id:
            raise ValueError("Admitted native position names another original unit")
        if (
            not evidence.submission_admission.recorded_at
            <= value.execution_known_at
            <= evidence.unit_snapshot.recorded_at
            <= self.position.source.recorded_at
        ):
            raise ValueError("Admitted native position changes its actual original knowledge order")
        return self


type AdmittedNativePositionPayloadV3 = AdmittedNativePositionEventV3

_POSITION: TypeAdapter[AdmittedNativePositionPayloadV3] = TypeAdapter(
    AdmittedNativePositionPayloadV3
)


def canonical_admitted_native_position_v3(payload: AdmittedNativePositionPayloadV3) -> str:
    """Serialize the separate native-admission envelope without changing any inner-v2 bytes.

    Args:
        payload: Strict admission-bound position event; its partial seed keeps the original V2 codec.

    Returns:
        Deterministic strict JSON containing every exact original admission/unit reference.
    """
    value = _POSITION.validate_python(payload)
    return json.dumps(
        value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def admitted_native_position_digest_v3(payload: AdmittedNativePositionPayloadV3) -> str:
    """Hash the complete new witness without partitioning actual native source consumption.

    Args:
        payload: Financial wrapper whose original source graph is independently rebound.

    Returns:
        Lowercase SHA-256 for the complete version-three payload.
    """
    return hashlib.sha256(
        b"snapper:admitted-native-position:v3\x00"
        + canonical_admitted_native_position_v3(payload).encode()
    ).hexdigest()
