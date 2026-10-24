"""Safe account identity from the actual original Spot key-info read, without secret fields.

This shape records observed identity and key permissions only. Its construction
does not prove source issuance, a Futures relationship or permission to spend.
The original key, nonce, signatures, IP restrictions and whole body stay transient.
"""

from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.futures_account_link import KrakenInternalAccountId

type SpotKeyPermission = Literal[
    "query-funds",
    "add-funds",
    "withdraw-funds",
    "earn-funds",
    "query-open-trades",
    "query-closed-trades",
    "modify-trades",
    "close-trades",
    "query-ledger",
    "export-data",
    "create-ws-token",
    "add-withdraw-address",
    "update-withdraw-address",
]


class SpotAccountPublicIdentity(BaseModel):
    """Whitelisted original IIBAN, permissions and source clocks for one actual selected Spot key.

    The key-info schema declares snake-case `api_key`, while its example uses
    camel-case `apiKey`. The original exact field name is retained separately;
    simultaneous aliases are refused by the closed parser rather than selected.
    """

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    source_policy: Literal["kraken_spot_key_info_redacted_v1"] = "kraken_spot_key_info_redacted_v1"
    client_selection: ExecutionClientSelection
    internal_account_id: KrakenInternalAccountId
    echoed_key_field: Literal["api_key", "apiKey"]
    permissions: tuple[SpotKeyPermission, ...] = Field(max_length=128)
    request_started_at: EvidenceInstant
    observed_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_original_source(self) -> Self:
        """Require the supported actual production source and original credential chronology.

        Returns:
            The same original public shape, without enabling any financial operation.
        """
        selected = self.client_selection
        if (
            selected.exchange != "kraken"
            or selected.mode != "live"
            or selected.environment != "production"
            or selected.public_api_origin != "https://api.kraken.com"
        ):
            raise ValueError("Spot identity source route is unproven")
        if (
            selected.credential_timestamp is None
            or not selected.credential_timestamp <= self.request_started_at <= self.observed_at
        ):
            raise ValueError("Spot identity source chronology is inconsistent")
        return self
