"""Own one actual Spot key-info source and its original signing User/Session without secret persistence.

Public receipt construction has no issuance. The actual original key remains
only in this live runtime boundary, never in the safe observation or a digest.
Close is explicit and belongs after all owned blocking work has joined.
"""

from dataclasses import dataclass
from datetime import datetime
from hmac import compare_digest
from typing import Never
from typing import SupportsIndex
from uuid import uuid4
from weakref import WeakKeyDictionary

from kraken.spot import User
from requests import Session

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.spot_account_identity import SpotAccountPublicIdentity


class SpotAccountIdentitySourceError(ValueError):
    """The original live Spot source cannot bind this actual signing request."""


@dataclass(frozen=True, slots=True, repr=False)
class _OriginalSpotIdentity:
    """Private original User/Session/key and detached safe observation for one actual operation."""

    user: User
    session: Session
    expected_key: bytes
    observation: SpotAccountPublicIdentity
    operation_public_id: str


_ORIGINAL_IDENTITIES: WeakKeyDictionary[object, _OriginalSpotIdentity] = WeakKeyDictionary()


def _same_key(value: object, expected: bytes) -> bool:
    """Compare only transient exact original key bytes without normalization or retained hashes."""
    if not isinstance(value, str):
        return False
    try:
        return compare_digest(value.encode("utf-8"), expected)
    except UnicodeError:
        return False


class PreparedSpotAccountIdentity:
    """Opaque actual source for subsequent guarded reads, without a durable association or grant."""

    __slots__ = ("__weakref__",)

    def __repr__(self) -> str:
        """Reveal no signing material, account identifier or SDK object in generic log formatting."""
        return "<PreparedSpotAccountIdentity:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse serialization of operation identity or traversal into a credential-bearing owner."""
        raise TypeError("Prepared Spot account identity cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic duplication of the original local signing/source operation."""
        raise TypeError("Prepared Spot account identity cannot be copied")

    def _original(self) -> _OriginalSpotIdentity:
        """Require the original still-issued local operation rather than a caller-constructed shape."""
        original = _ORIGINAL_IDENTITIES.get(self)
        if original is None:
            raise SpotAccountIdentitySourceError("spot_identity_not_transport_issued")
        return original

    @property
    def observation(self) -> SpotAccountPublicIdentity:
        """Expose a detached copy of original safe fields.

        Returns:
            Original IIBAN, permissions, selected revision and clocks, without source authenticity alone.
        """
        return self._original().observation.model_copy(deep=True)

    @property
    def operation_public_id(self) -> str:
        """Expose the actual independent operation UUID.

        Returns:
            Original transport-issued capture identity, not a database row or current account proof.
        """
        return self._original().operation_public_id

    def assert_original_request(
        self,
        actual_user: User,
        selection: ExecutionClientSelection,
        actual_prepared_key: object,
        encoded_at: datetime,
    ) -> SpotAccountPublicIdentity:
        """Check the actual final prepared request against its original User, Session and signing key.

        Args:
            actual_user: Exact instantiated Spot User owned by the closed actual request operation.
            selection: Actual current public credential and endpoint revision at dispatch.
            actual_prepared_key: Final API-Key value after Requests authentication has run.
            encoded_at: Actual local clock after preparation, never a reconstructed provider time.

        Returns:
            Detached original identity only; the receiving producer must bind actual durable sources.
        """
        original = self._original()
        source = original.observation
        if (
            actual_user is not original.user
            or getattr(actual_user, "_SpotClient__session", None) is not original.session
            or source.client_selection.public_api_origin != actual_user.URL
            or selection != source.client_selection
            or not _same_key(getattr(actual_user, "_key", None), original.expected_key)
            or not _same_key(actual_prepared_key, original.expected_key)
        ):
            raise SpotAccountIdentitySourceError("spot_identity_actual_owner_or_key_changed")
        if encoded_at.utcoffset() is None or encoded_at < source.observed_at:
            raise SpotAccountIdentitySourceError("spot_identity_request_clock_unproven")
        return source.model_copy(deep=True)

    def close(self) -> None:
        """Drop this operation's signing/source references after owned work has joined."""
        _ORIGINAL_IDENTITIES.pop(self, None)


def _mint_spot_account_identity(
    actual_user: User,
    actual_session: Session,
    actual_prepared_key: str,
    observation: SpotAccountPublicIdentity,
) -> PreparedSpotAccountIdentity:
    """Issue only after the closed original HTTP key echo and final request comparison succeed.

    Args:
        actual_user: Exact original signing SDK User.
        actual_session: Original routed Session preserved across the completed request.
        actual_prepared_key: Same original matched key, never serialized or hashed.
        observation: Whitelisted original source values after the response has closed.

    Returns:
        Opaque live operation with no implied durable identity proof or financial permission.
    """
    source = SpotAccountPublicIdentity.model_validate(observation).model_copy(deep=True)
    result = PreparedSpotAccountIdentity()
    _ORIGINAL_IDENTITIES[result] = _OriginalSpotIdentity(
        actual_user,
        actual_session,
        actual_prepared_key.encode("utf-8"),
        source,
        str(uuid4()),
    )
    return result
