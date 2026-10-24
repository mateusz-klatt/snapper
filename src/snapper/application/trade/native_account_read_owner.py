"""Type the shared native account-read lifecycle without broadening source issuance.

Only concrete prepared owners carry actual transport-issued key tokens. This
structural interface supplies no proof, grant or alternate source constructor.
"""

from collections.abc import Awaitable
from collections.abc import Callable
from typing import Protocol

from kraken.futures import User

from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_read_store import BoundNativeAccountReadAttempt
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken


class NativeAccountReadOwner(Protocol):
    """Actual retained proof and the corresponding owned transport token required by accounts capture."""

    @property
    def proof(self) -> BoundNativeAccountIdentityProof:
        """Return the actual committed native v1 identity proof.

        Returns:
            Original committed proof, without any additional account association.
        """

    def require_token(self) -> PreparedKeyToken:
        """Return the still-owned original token bound to that proof.

        Returns:
            Same actual transport-issued token pending its original guarded request checks.
        """

    def close(self) -> None:
        """Release unhanded source ownership only after its blocking operation has joined."""


type NativeAccountOwnerPreparation[OwnerT: NativeAccountReadOwner] = Callable[
    [
        AccountingRepository,
        KrakenFuturesExchangeClient,
        BoundNativeAccountReadAttempt,
        ExecutionEvidenceIdentity,
        User,
    ],
    Awaitable[OwnerT],
]
