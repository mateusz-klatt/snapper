"""Share actual selected-key preflight between account reads and owner preparation.

This module imports no account-read application producer. Both public-only and
runtime-token handoff flows use the same actual key GET, retained proof and
original User continuity checks without circular imports.
"""

from typing import Never
from typing import SupportsIndex

from kraken.futures import User

from snapper.application.trade.owned_native_source_read import read_owned_native_source
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import retain_native_account_identity
from snapper.data.native_account_read_store import BoundNativeAccountReadAttempt
from snapper.data.native_account_read_store import NativeAccountReadBindingError
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_futures_key_identity import (
    read_futures_prepared_key_identity,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken


class PreparedNativeAccountOwner:
    """Actual committed safe proof and its same-operation transport-issued opaque key token."""

    __slots__ = ("proof", "_token")
    proof: BoundNativeAccountIdentityProof
    _token: PreparedKeyToken | None

    def __init__(self, proof: BoundNativeAccountIdentityProof, token: PreparedKeyToken) -> None:
        """Retain only the actual stored proof and exact token bound after its awaited commit."""
        object.__setattr__(self, "proof", proof)
        object.__setattr__(self, "_token", token)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse ordinary replacement of actual proof or prepared key ownership."""
        raise AttributeError("Prepared native account owner is immutable")

    def __repr__(self) -> str:
        """Expose no SDK, expected key or original sensitive endpoint source."""
        return "<PreparedNativeAccountOwner:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse generic persistence of the runtime key capability."""
        raise TypeError("Prepared native account owner cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic dataclass/copy traversal into the actual key token."""
        raise TypeError("Prepared native account owner cannot be copied")

    def require_token(self) -> PreparedKeyToken:
        """Use only a still-owned token which the caller must close or explicitly hand off.

        Returns:
            The same actual issued key token; no expected key is reconstructed.
        """
        if self._token is None:
            raise ValueError("native_account_owner_token_unavailable")
        return self._token

    def take_token(self) -> PreparedKeyToken:
        """Transfer local cleanup responsibility once without reconstructing an expected key.

        Returns:
            The same token whose cleanup responsibility now belongs to the receiving owner.
        """
        token = self.require_token()
        object.__setattr__(self, "_token", None)
        return token

    def close(self) -> None:
        """Close only a token still owned here, preserving already transferred ownership and history."""
        if self._token is not None:
            self._token.close()
            object.__setattr__(self, "_token", None)


def require_actual_native_user(
    client: KrakenFuturesExchangeClient,
    selected: ExecutionClientSelection,
    expected: User | None,
) -> User:
    """Keep actual original SDK object and exact public revision across every asynchronous gap.

    Args:
        client: Actual selected exchange client holding the original User slot.
        selected: Same immutable public source selection recorded by the committed read attempt.
        expected: Exact original instantiated User captured before beginning that attempt.

    Returns:
        The same actual SDK User, without copying any credential secret.
    """
    if (
        expected is None
        or client._user_client is not expected
        or client.execution_client_selection() != selected
    ):
        raise NativeAccountReadBindingError("native_read_actual_client_changed")
    return expected


async def prove_native_account_owner(
    repository: AccountingRepository,
    client: KrakenFuturesExchangeClient,
    attempt: BoundNativeAccountReadAttempt,
    identity: ExecutionEvidenceIdentity,
    expected_user: User,
) -> PreparedNativeAccountOwner:
    """Commit actual safe ownership before any guarded accounts GET or token handoff can begin.

    Args:
        repository: Actual source store sharing the account execution fence.
        client: Same selected exchange client with its original User.
        attempt: Actual committed pending accounts-read attempt.
        identity: Producer-owned key-proof row identity for that exact attempt.
        expected_user: User captured before attempt creation, never a later mutable slot lookup.

    Returns:
        Actual retained proof and, only for matching ownership, a bound transport-issued token.
    """
    selected = attempt.payload.client_selection
    user = require_actual_native_user(client, selected, expected_user)
    client._record_rest_call()
    prepared = await read_owned_native_source(
        client,
        user,
        "prepare_native_private_key_identity",
        lambda: read_futures_prepared_key_identity(user, selected),
        lambda result: result.token.close(),
    )
    handed_off = False
    try:
        require_actual_native_user(client, selected, user)
        proof = await retain_native_account_identity(
            repository, attempt.public_id, prepared.observation, identity
        )
        require_actual_native_user(client, selected, user)
        if proof.payload.matches_owner:
            prepared.token.bind_committed_proof(proof.public_id, proof.payload)
        result = PreparedNativeAccountOwner(proof, prepared.token)
        handed_off = True
        return result
    finally:
        if not handed_off:
            prepared.token.close()
