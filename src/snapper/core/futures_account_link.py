"""Safe same-response account UUID and IIBAN observation, separate from native v1 history.

An internal account identifier is not an API key or a bank account number.
This shape grants no source authenticity, Spot association or financial access.
Only the closed transport's separately issued receipt proves original capture.
"""

from typing import Annotated
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.native_account_identity import NativeAccountPublicIdentity

type KrakenInternalAccountId = Annotated[
    str,
    Field(
        min_length=19,
        max_length=19,
        pattern=r"^AA[A-Z0-9]{2} [A-Z0-9]{4} [A-Z0-9]{4} [A-Z0-9]{4}$",
    ),
]


class FuturesAccountLinkObservation(BaseModel):
    """Explicit safe projection of one original check, without changing its native v1 proof bytes.

    The operation UUID is independently issued at actual capture. It is not a
    database row identity or evidence of a committed native owner proof. Both
    source clocks and the original environment remain in the nested identity.
    """

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    source_policy: Literal["kraken_futures_key_check_iiban_v1"] = (
        "kraken_futures_key_check_iiban_v1"
    )
    operation_public_id: EvidenceUUID
    identity: NativeAccountPublicIdentity
    internal_account_id: KrakenInternalAccountId
