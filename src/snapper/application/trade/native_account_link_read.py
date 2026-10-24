"""Capture original native account state with its same-key-response IIBAN companion.

The companion reuses the original accounts attempt, unchanged native v1 proof,
guarded accounts response and terminal outcome. Its live source still requires
a separate durable Spot association and grants no fee or financial permission.
"""

from typing import Never
from typing import SupportsIndex

from kraken.futures import User

from snapper.application.trade.native_account_owner_preflight import require_actual_native_user
from snapper.application.trade.native_account_read import _read_native_accounts_with_preparation
from snapper.application.trade.owned_native_source_read import read_owned_native_source
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import retain_native_account_identity
from snapper.data.native_account_read_store import BoundNativeAccountReadAttempt
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    read_futures_prepared_account_link,
)
from snapper.infrastructure.exchanges.prepared_futures_account_link import (
    PreparedFuturesAccountLink,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.messaging.infrastructure.publisher import SequenceTracker


class PreparedNativeAccountLinkOwner:
    """Own one actual IIBAN companion and its token together until successful source handoff or close."""

    __slots__ = ("proof", "_link")
    proof: BoundNativeAccountIdentityProof
    _link: PreparedFuturesAccountLink | None

    def __init__(
        self, proof: BoundNativeAccountIdentityProof, link: PreparedFuturesAccountLink
    ) -> None:
        """Retain the actual proof and the same original link issued before that proof committed."""
        object.__setattr__(self, "proof", proof)
        object.__setattr__(self, "_link", link)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse ordinary replacement of the actual proof or live account-link source."""
        raise AttributeError("Prepared native account link owner is immutable")

    def __repr__(self) -> str:
        """Expose neither account identifiers nor SDK/signing objects through generic logging."""
        return "<PreparedNativeAccountLinkOwner:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse generic serialization of the actual runtime source and signing token."""
        raise TypeError("Prepared native account link owner cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse duplication of actual source ownership through generic copying."""
        raise TypeError("Prepared native account link owner cannot be copied")

    def require_link(self) -> PreparedFuturesAccountLink:
        """Return the same actual issued account link, still owned as a whole.

        Returns:
            Original link whose token and safe same-response identity remain one local operation.
        """
        if self._link is None:
            raise ValueError("native_account_link_owner_unavailable")
        _ = self._link.token
        return self._link

    def require_token(self) -> PreparedKeyToken:
        """Use the original token for the shared guarded accounts request without transferring it.

        Returns:
            Original token already bound to the actual committed native v1 identity proof.
        """
        return self.require_link().token

    def close(self) -> None:
        """Close the complete link and token after owned blocking work has joined, preserving history."""
        if self._link is not None:
            self._link.close()
            object.__setattr__(self, "_link", None)


async def _prove_native_account_link_owner(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    attempt: BoundNativeAccountReadAttempt,
    identity: ExecutionEvidenceIdentity,
    expected_user: User,
) -> PreparedNativeAccountLinkOwner:
    """Commit the unchanged native proof from the exact single original UUID/IIBAN key response."""
    selected = attempt.payload.client_selection
    user = require_actual_native_user(client, selected, expected_user)
    client._record_rest_call()
    link = await read_owned_native_source(
        client,
        user,
        "prepare_native_private_key_identity",
        lambda: read_futures_prepared_account_link(user, selected),
        lambda result: result.close(),
    )
    handed_off = False
    try:
        require_actual_native_user(client, selected, user)
        proof = await retain_native_account_identity(
            repository, attempt.public_id, link.observation.identity, identity
        )
        require_actual_native_user(client, selected, user)
        if proof.payload.matches_owner:
            link.token.bind_committed_proof(proof.public_id, proof.payload)
        result = PreparedNativeAccountLinkOwner(proof, link)
        handed_off = True
        return result
    finally:
        if not handed_off:
            link.close()


async def read_linked_native_futures_accounts(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    tracker: SequenceTracker,
) -> tuple[BoundNativeAccountReadOutcome, PreparedNativeAccountLinkOwner | None]:
    """Capture one actual key/accounts operation and retain its original IIBAN source on success.

    Args:
        repository: Actual source repository with the existing native read transaction/fence rules.
        client: Same selected Futures client and original SDK User throughout the operation.
        tracker: Producer-owned identity allocator for the existing attempt/proof/source/result rows.

    Returns:
        Original terminal outcome and, only after successful capture, its actual still-owned link.
        The caller must close the entire owner after its last owned read or durable association step.
        A constructed value, successful native read or IIBAN alone grants no Spot relationship.
    """
    return await _read_native_accounts_with_preparation(
        repository, client, tracker, _prove_native_account_link_owner
    )
