"""Single derivation site for every master-password-derived internal key.

One-root-secret model (operator direction 2026-07-04): ``MASTER_PASSWORD``
is the ONLY secret the operator provisions (a single ``.env`` parameter);
every internal key — settings encryption (Fernet, see
:mod:`snapper.infrastructure.security.encryption`), control-plane command
signing, JWT auth signing, CSRF token signing — is derived from it with a
distinct purpose tag. Adding a new internal secret means adding a purpose
tag here, NEVER a new environment variable or seeded Setting.

Recipe (LOCKED by golden-vector tests in
``tests/infrastructure/security/test_kdf.py`` — changing ANY ingredient
silently rotates every derived key fleet-wide, which invalidates sessions,
CSRF cookies, MCP delegate tokens, and control-plane signatures at the
next deploy):

- PBKDF2-HMAC-SHA256 at 100,000 iterations — the SAME work factor as the
  Fernet settings-encryption derivation, so no derived key is a cheaper
  offline oracle for brute-forcing the master password than the encrypted
  settings already are.
- Domain-tagged salt ``sha256(master_password + purpose)`` — keys for
  different purposes are cryptographically independent even though they
  share the input secret; a versioned purpose tag (``...-v2``) rotates one
  purpose without touching the master or any sibling key.
- Deterministic — every first-party container configured with the same
  master password derives identical keys with no distribution step.

External credentials (exchange API keys, the Polygon key, the Walutomat
RSA key) cannot be derived — they are third-party issued and stay
Fernet-encrypted at rest in the database under the master-derived key.
"""

import functools
import hashlib

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

_KDF_ITERATIONS = 100000

AUTH_TOKEN_SIGNING_PURPOSE = b"snapper-auth-token-signing-v1"
"""Purpose tag for the JWT auth signing key (``AppSettings.auth_secret_key``)."""

CSRF_TOKEN_SIGNING_PURPOSE = b"snapper-csrf-token-signing-v1"
"""Purpose tag for the CSRF token signing key (``AppSettings.csrf_secret_key``)."""


@functools.lru_cache(maxsize=32)
def derive_key(master_password: str, purpose: bytes, *, length: int = 32) -> bytes:
    """Derive a purpose-scoped key from the single master secret.

    Process-lifetime cached: the derivation is a pure function of its
    arguments and the master password is immutable for a process's
    lifetime (bootstrap env), while consumers read derived keys on HOT
    paths — JWT verification and CSRF checks call this per HTTP request,
    and an uncached PBKDF2 at 100,000 iterations costs ~20 ms of CPU per
    call (a request-amplified DoS surface). The cache holds the same
    secret material that already lives in process memory via settings,
    so it widens no trust boundary.

    Args:
        master_password: The single master secret (the only environment
            secret, from ``MASTER_PASSWORD``).
        purpose: The domain-separating purpose tag, including a version
            suffix (e.g. ``b"snapper-auth-token-signing-v1"``). Bump the
            version to rotate ONE purpose without touching the master.
        length: Derived key length in bytes.

    Returns:
        The derived key bytes.
    """
    secret = master_password.encode()
    salt = hashlib.sha256(secret + purpose).digest()
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=length,
        salt=salt,
        iterations=_KDF_ITERATIONS,
    )

    return kdf.derive(secret)
