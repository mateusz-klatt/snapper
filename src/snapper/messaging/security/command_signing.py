"""HMAC signing and verification for control-plane process commands and acks.

The signing key is DERIVED from the single master secret (``master_password``,
the only environment secret) via PBKDF2-HMAC-SHA256 with a distinct salt, so
every first-party container derives the same key deterministically without
adding a new environment secret (operator direction 2026-07-04: one master
key, derive the rest). HMAC proves possession of that master-derived key: it
blocks unauthenticated broker injection and lets a receiver cross-check
freshness and slug, but it does NOT cryptographically isolate the first-party
containers from one another (they all share the master key). See the
control-plane plan section 6 for the honest threat model.

The signature is carried in-band in the payload's ``signature`` field and is
excluded from the canonical bytes that are signed, so signing and verifying a
payload agree byte-for-byte regardless of the field's current value.
"""

import hashlib
import hmac
import json

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from snapper.core.json_types import JsonObject

_COMMAND_SIGNING_INFO = b"snapper-process-command-signing-v1"
_KDF_ITERATIONS = 100000
_SIGNATURE_FIELD = "signature"
_UNSIGNED_FIELDS = frozenset({_SIGNATURE_FIELD, "topic"})


def command_signing_key(master_password: str) -> bytes:
    """Derive the control-plane HMAC key from the master password.

    Uses PBKDF2-HMAC-SHA256 with the SAME 100,000-iteration work factor as the
    Fernet settings-encryption derivation
    (:mod:`snapper.infrastructure.security.encryption`), so a leaked signature
    is not a cheaper offline oracle for brute-forcing the master password than
    the encrypted settings already are. A domain-tagged salt
    (``sha256(master_password + info)``) makes this key independent of the
    Fernet key derived from the same password. Deterministic: every container
    configured with the same master password derives the same key, which is
    what lets a coordinator verify a nudge the API signed (and vice versa for
    acks).

    Args:
        master_password: The single master secret (the only environment secret).

    Returns:
        A 32-byte HMAC key.
    """
    secret = master_password.encode()
    salt = hashlib.sha256(secret + _COMMAND_SIGNING_INFO).digest()
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=_KDF_ITERATIONS,
    )

    return kdf.derive(secret)


def _canonical_unsigned_bytes(payload: JsonObject) -> bytes:
    """Return the canonical JSON bytes of ``payload`` excluding transport fields.

    Deterministic (sorted keys, compact separators) so the signer and the
    verifier agree byte-for-byte regardless of dict insertion order. Excludes
    both the in-band ``signature`` AND the ``topic`` field: the ZMQ publisher
    stamps the routing ``topic`` onto the payload AFTER it is signed, so a
    verifier over the on-wire payload would otherwise recompute the HMAC over a
    ``topic`` the signer never saw and reject a legitimate message.

    Args:
        payload: The full payload dict (it may carry ``signature`` / ``topic``).

    Returns:
        Canonical UTF-8 JSON bytes with the signature and topic fields removed.
    """
    unsigned = {key: value for key, value in payload.items() if key not in _UNSIGNED_FIELDS}

    return json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()


def sign_command_payload(payload: JsonObject, key: bytes) -> str:
    """Return the hex HMAC-SHA256 signature over the signature-excluded payload.

    Args:
        payload: The payload dict to sign; any existing ``signature`` field is
            ignored so re-signing is idempotent.
        key: The HMAC key from :func:`command_signing_key`.

    Returns:
        The hex-encoded signature to place in the payload's ``signature`` field.
    """
    return hmac.new(key, _canonical_unsigned_bytes(payload), hashlib.sha256).hexdigest()


def verify_command_payload(payload: JsonObject, key: bytes) -> bool:
    """Constant-time verify the in-band ``signature`` field of ``payload``.

    Args:
        payload: The received payload dict including its ``signature`` field.
        key: The HMAC key from :func:`command_signing_key`.

    Returns:
        True if and only if the payload carries a string signature that matches
        the recomputed HMAC (compared in constant time).
    """
    provided = payload.get(_SIGNATURE_FIELD)

    if not isinstance(provided, str):
        return False
    expected = sign_command_payload(payload, key)

    return hmac.compare_digest(provided, expected)
