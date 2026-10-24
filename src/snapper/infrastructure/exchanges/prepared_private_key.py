"""Opaque exact-key token for the actual closed identity transport boundary.

Only the successful prepared-header/response-key comparison may mint this
runtime value. A producer binds the actual safe proof after its awaited commit.
This token does not establish socket authentication or financial permission.
"""

from datetime import datetime
from hmac import compare_digest
from typing import Never
from typing import SupportsIndex
from weakref import WeakSet

from kraken.futures import User
from pydantic import BaseModel
from pydantic import ConfigDict

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.core.native_account_identity import native_account_identity_digest

_ISSUED_TOKENS: WeakSet[object] = WeakSet()


class PreparedKeyTokenError(ValueError):
    """The private runtime key/owner proof edge cannot be established without sensitive material."""


class PreparedKeyProofReference(BaseModel):
    """Public same-check reference, still requiring the actual immutable proof loader at persistence."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    public_id: EvidenceUUID
    content_digest: EvidenceDigest
    read_attempt_public_id: EvidenceUUID
    account_uid: EvidenceUUID
    client_selection: ExecutionClientSelection
    key_owner_root_public_id: EvidenceUUID
    key_owner_root_content_digest: EvidenceDigest
    observed_at: EvidenceInstant
    recorded_at: EvidenceInstant


class PreparedKeyToken:
    """Opaque nonserializable exact-key expectation, scoped to the original actual SDK User object.

    The expected key never appears in a property, repr, exception or digest.
    Closing drops local references; Python memory zeroization is not claimed.
    Persistence independently rebinds the public proof/root; this object cannot
    certify that a caller-authored reference was actually committed.
    """

    __slots__ = (
        "_user",
        "_expected_key",
        "_observation",
        "_committed",
        "_closed",
        "__weakref__",
    )
    _user: User | None
    _expected_key: bytes
    _observation: NativeAccountPublicIdentity
    _committed: PreparedKeyProofReference | None
    _closed: bool

    def __init__(
        self, user: User, expected_key: str, observation: NativeAccountPublicIdentity
    ) -> None:
        """Mint only inside successful transport projection while the actual prepared header is known.

        Args:
            user: Exact original SDK object whose selected request was independently verified.
            expected_key: Exact actual prepared request key after response echo comparison.
            observation: Existing whitelisted public identity from that same response.
        """
        try:
            encoded = expected_key.encode("utf-8")
        except UnicodeError:
            raise PreparedKeyTokenError("prepared_key_token_text_invalid") from None
        if not encoded:
            raise PreparedKeyTokenError("prepared_key_token_text_invalid")
        object.__setattr__(self, "_user", user)
        object.__setattr__(self, "_expected_key", encoded)
        object.__setattr__(
            self,
            "_observation",
            NativeAccountPublicIdentity.model_validate(observation),
        )
        object.__setattr__(self, "_committed", None)
        object.__setattr__(self, "_closed", False)

    def __setattr__(self, name: str, value: object) -> Never:
        """Refuse ordinary reassignment of exact prepared-key or actual SDK identity."""
        raise AttributeError("Prepared key token is immutable")

    def __repr__(self) -> str:
        """Reveal no credential-bearing field through ordinary logger formatting."""
        return "<PreparedKeyToken:opaque>"

    def __str__(self) -> str:
        """Reveal no credential-bearing field through ordinary string formatting."""
        return repr(self)

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse serialization before any expected key or SDK field can be traversed."""
        raise TypeError("Prepared key token cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse generic dataclass conversion or copying of the credential-bearing token."""
        raise TypeError("Prepared key token cannot be copied")

    def _require_open(self) -> User:
        """Refuse an expired local token instead of reconstructing one from current SDK state."""
        if self._closed or self._user is None:
            raise PreparedKeyTokenError("prepared_key_token_closed")
        if self not in _ISSUED_TOKENS:
            raise PreparedKeyTokenError("prepared_key_token_not_transport_issued")
        return self._user

    def _matches(self, value: object) -> bool:
        """Compare only in memory without logging or retaining a key hash."""
        if not isinstance(value, str):
            return False
        try:
            return compare_digest(value.encode("utf-8"), self._expected_key)
        except UnicodeError:
            return False

    def bind_committed_proof(
        self, public_id: str, payload: NativeAccountIdentityProofPayload
    ) -> PreparedKeyProofReference:
        """Freeze the exact safe proof after the trusted producer has awaited its actual commit.

        Args:
            public_id: Actual independently retained safe proof UUID, never a new inferred identity.
            payload: Actual bound store result whose source/root will be rebound again at persistence.

        Returns:
            Public immutable proof reference without the expected key or SDK object.
        """
        self._require_open()
        proven = NativeAccountIdentityProofPayload.model_validate(payload)
        if proven.observation != self._observation or not proven.matches_owner:
            raise PreparedKeyTokenError("prepared_key_token_bound_proof_mismatch")
        digest = native_account_identity_digest(proven)
        root = proven.owner_root
        reference = PreparedKeyProofReference(
            public_id=public_id,
            content_digest=digest,
            read_attempt_public_id=proven.attempt_public_id,
            account_uid=proven.observation.account_uid,
            client_selection=proven.observation.client_selection,
            key_owner_root_public_id=public_id if root is None else root.public_id,
            key_owner_root_content_digest=(digest if root is None else root.content_digest),
            observed_at=proven.observation.observed_at,
            recorded_at=proven.recorded_at,
        )
        if self._committed is not None and self._committed != reference:
            raise PreparedKeyTokenError("prepared_key_token_bound_proof_changed")
        object.__setattr__(self, "_committed", reference.model_copy(deep=True))
        return reference.model_copy(deep=True)

    def require_proof_reference(self) -> PreparedKeyProofReference:
        """Expose only the actual immutable committed proof, without pretending a key was dispatched.

        Returns:
            Original safe proof/root reference; actual request still requires assert_prepared_key.
        """
        self._require_open()
        if self._committed is None:
            raise PreparedKeyTokenError("prepared_key_token_proof_not_committed")
        return self._committed.model_copy(deep=True)

    def assert_prepared_key(
        self,
        actual_user: User,
        selection: ExecutionClientSelection,
        encoded_key: object,
        encoded_at: datetime,
    ) -> PreparedKeyProofReference:
        """Refuse mutation at actual auth encoding without claiming successful socket authentication.

        Args:
            actual_user: Exact actual original SDK User, still owned by the selected client.
            selection: Actual current public client revision at this closed dispatch boundary.
            encoded_key: Exact auth dictionary key seen at the actual outbound encoder/send boundary.
            encoded_at: Actual local dispatch clock sampled after SDK challenge waits.

        Returns:
            The bound public key proof; actual socket/ACK/receive-ticket checks remain mandatory.
        """
        original = self._require_open()
        reference = self._committed
        if reference is None:
            raise PreparedKeyTokenError("prepared_key_token_proof_not_committed")
        if (
            actual_user is not original
            or selection != self._observation.client_selection
            or actual_user.url != selection.public_api_origin
            or not self._matches(getattr(actual_user, "_key", None))
            or not self._matches(encoded_key)
        ):
            raise PreparedKeyTokenError("prepared_key_token_actual_client_or_key_changed")
        if encoded_at.utcoffset() is None or encoded_at < reference.recorded_at:
            raise PreparedKeyTokenError("prepared_key_token_dispatch_clock_unproven")
        return reference.model_copy(deep=True)

    def close(self) -> None:
        """Drop local key/User references without changing any retained public proof or stored history."""
        object.__setattr__(self, "_expected_key", b"")
        object.__setattr__(self, "_user", None)
        object.__setattr__(self, "_committed", None)
        object.__setattr__(self, "_closed", True)
        _ISSUED_TOKENS.discard(self)


def _mint_prepared_private_key(
    actual_user: User,
    actual_prepared_key: str,
    observation: NativeAccountPublicIdentity,
) -> PreparedKeyToken:
    """Issue only from the closed exact prepared-header/response-key comparison boundary.

    This internal factory is called by the actual key HTTP transport after its
    strict response projection. Public token construction is deliberately unissued
    and cannot bind or authorize a key. No API caller supplies this invocation.
    """
    token = PreparedKeyToken(actual_user, actual_prepared_key, observation)
    _ISSUED_TOKENS.add(token)
    return token
