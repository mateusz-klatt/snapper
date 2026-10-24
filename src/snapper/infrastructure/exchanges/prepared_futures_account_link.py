"""Keep a safe account link bound to the actual original closed key-check operation.

Public construction does not issue a receipt. Registry copies protect the
original observation from nested public model aliases. The actual token still
needs its original committed native proof and independently owned lifecycle;
this receipt grants neither a Spot relationship nor financial permission.
"""

from dataclasses import dataclass
from typing import Never
from typing import SupportsIndex
from uuid import uuid4
from weakref import WeakKeyDictionary

from kraken.futures import User

from snapper.core.futures_account_link import FuturesAccountLinkObservation
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.exchanges.prepared_private_key import _mint_prepared_private_key


class FuturesAccountLinkSourceError(ValueError):
    """The original transport-issued account-link source is absent or closed."""


@dataclass(frozen=True, slots=True)
class _OriginalAccountLink:
    """Private original capture values; no API key or raw response is retained here."""

    observation: FuturesAccountLinkObservation
    token: PreparedKeyToken


_ORIGINAL_LINKS: WeakKeyDictionary[object, _OriginalAccountLink] = WeakKeyDictionary()


class PreparedFuturesAccountLink:
    """Opaque issued capture whose public observations are always detached value copies."""

    __slots__ = ("__weakref__",)

    def __repr__(self) -> str:
        """Expose neither account metadata nor the original signing owner to generic logging."""
        return "<PreparedFuturesAccountLink:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse generic serialization of an issued operation or its live signing token."""
        raise TypeError("Prepared Futures account link cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse copying operation identity or traversing its credential-bearing runtime token."""
        raise TypeError("Prepared Futures account link cannot be copied")

    def _original(self) -> _OriginalAccountLink:
        """Require actual issuance and the original live token before returning private state."""
        original = _ORIGINAL_LINKS.get(self)
        if original is None:
            raise FuturesAccountLinkSourceError("futures_account_link_not_transport_issued")
        original.token._require_open()
        return original

    @property
    def observation(self) -> FuturesAccountLinkObservation:
        """Return only a detached safe source value.

        Returns:
            Deep copy of original safe evidence, never a mutable alias of the issued source.
        """
        return self._original().observation.model_copy(deep=True)

    @property
    def token(self) -> PreparedKeyToken:
        """Expose the same actual token for original-proof binding and ownership handoff.

        Returns:
            Original live transport token, still requiring its actual committed native proof.
        """
        return self._original().token

    def close(self) -> None:
        """Revoke local issuance and close the original token without rewriting historical proof."""
        original = _ORIGINAL_LINKS.pop(self, None)
        if original is not None:
            original.token.close()


def _mint_futures_account_link(
    actual_user: User,
    actual_prepared_key: str,
    identity: NativeAccountPublicIdentity,
    internal_account_id: str,
) -> PreparedFuturesAccountLink:
    """Issue only inside the actual original response/key match with its same-response identifier.

    Args:
        actual_user: The exact SDK User whose final prepared request and response were verified.
        actual_prepared_key: Original matched signing key, retained only by the private token.
        identity: The original whitelisted UUID, permissions, selected credential and local clocks.
        internal_account_id: Strict supported IIBAN extracted from that same original response.

    Returns:
        Opaque original capture; the trusted producer must still commit its native proof.
    """
    observation = FuturesAccountLinkObservation(
        operation_public_id=str(uuid4()),
        identity=identity.model_copy(deep=True),
        internal_account_id=internal_account_id,
    )
    token = _mint_prepared_private_key(actual_user, actual_prepared_key, identity)
    result = PreparedFuturesAccountLink()
    _ORIGINAL_LINKS[result] = _OriginalAccountLink(observation, token)
    return result
