"""Closed native read companion returning only an actually prepared private-owner token.

The existing public-only read producer remains unchanged. This companion uses
its actual attempt/raw/outcome helpers and exactly one key GET plus one accounts
GET. Safe key identity commits before accounts dispatch and before any socket
registration. A failed account read retains its safe identity history but closes
the unhanded runtime token; it grants no transport or financial permission.
"""

from typing import Never
from typing import SupportsIndex

from snapper.application.trade.native_account_read import _read_native_futures_accounts_with_owner
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyProofReference
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.messaging.infrastructure.publisher import SequenceTracker


class PreparedPrivateOwnerRead:
    """Opaque successful-read handoff; plain public outcome/context construction provides no token."""

    __slots__ = ("outcome", "_token")
    outcome: BoundNativeAccountReadOutcome
    _token: PreparedKeyToken | None

    def __init__(
        self, outcome: BoundNativeAccountReadOutcome, token: PreparedKeyToken | None
    ) -> None:
        """Preserve actual committed outcome and only its same-operation closed transport token."""
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "_token", token)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse relabeling a token with another later public read outcome."""
        raise AttributeError("Prepared private owner read is immutable")

    def __repr__(self) -> str:
        """Expose no key token or signing SDK object through generic log formatting."""
        return "<PreparedPrivateOwnerRead:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse persistence of an in-memory owner bootstrap capability."""
        raise TypeError("Prepared private owner read cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic dataclass conversion/copy traversal into the actual key token."""
        raise TypeError("Prepared private owner read cannot be copied")

    def require_token(self) -> PreparedKeyToken:
        """Require an actual successful same-operation prepared handoff, not outcome shape alone.

        Returns:
            The actual issued proof-bound token, never a newly constructed public-shaped value.
        """
        if self._token is None:
            raise ValueError("private_owner_preparation_unsuccessful")
        return self._token

    def require_proof_reference(self) -> PreparedKeyProofReference:
        """Read the actual safe proof reference while keeping physical key use a separate check.

        Returns:
            Immutable original key-proof/root reference without a fabricated encoded-key argument.
        """
        return self.require_token().require_proof_reference()

    def close(self) -> None:
        """Drop unhanded transient ownership without changing immutable read/identity history."""
        if self._token is not None:
            self._token.close()
            object.__setattr__(self, "_token", None)


async def prepare_native_private_owner(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    tracker: SequenceTracker,
) -> PreparedPrivateOwnerRead:
    """Hand off only the token from the same canonical actual guarded native read flow.

    Args:
        repository: Actual source transaction/fence provider.
        client: Same original selected exchange client and signing SDK User.
        tracker: Actual evidence session/sequence allocator.

    Returns:
        Actual captured or failed outcome with a same-operation token only after source success.
    """
    outcome, owner = await _read_native_futures_accounts_with_owner(repository, client, tracker)
    try:
        return PreparedPrivateOwnerRead(outcome, None if owner is None else owner.take_token())
    finally:
        if owner is not None:
            owner.close()
