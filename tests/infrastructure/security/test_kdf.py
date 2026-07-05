"""Tests for the single master-password key-derivation site."""

from snapper.infrastructure.security.kdf import AUTH_TOKEN_SIGNING_PURPOSE
from snapper.infrastructure.security.kdf import CSRF_TOKEN_SIGNING_PURPOSE
from snapper.infrastructure.security.kdf import derive_key
from snapper.messaging.security.command_signing import command_signing_key


class TestDeriveKey:
    """Recipe lock and domain-separation guarantees."""

    def test_golden_vector_locks_the_recipe(self) -> None:
        """The derivation recipe is frozen by known-answer vectors.

        Changing ANY ingredient (hash, iterations, salt construction,
        length) silently rotates every derived key fleet-wide at the next
        deploy — sessions, CSRF cookies, MCP delegate tokens, and
        control-plane signatures would all be invalidated. These vectors
        make such a change an explicit, reviewed decision.
        """
        assert (
            derive_key("test-master", AUTH_TOKEN_SIGNING_PURPOSE).hex()
            == "a7bb8d51537b0cb09357c6b0271e6f2f12eb9dc139bff55e6bbc4afdfb892761"
        )
        assert (
            derive_key("test-master", CSRF_TOKEN_SIGNING_PURPOSE).hex()
            == "716233da50bcca98d6c430a0c330098475ec84f9a1007c3dc1867a68db6cbb2a"
        )

    def test_purposes_are_domain_separated(self) -> None:
        """Different purpose tags yield independent keys from one master."""
        auth = derive_key("test-master", AUTH_TOKEN_SIGNING_PURPOSE)
        csrf = derive_key("test-master", CSRF_TOKEN_SIGNING_PURPOSE)
        assert auth != csrf

    def test_master_change_rotates_every_purpose(self) -> None:
        """A different master password yields a different key per purpose."""
        assert derive_key("master-a", AUTH_TOKEN_SIGNING_PURPOSE) != derive_key(
            "master-b", AUTH_TOKEN_SIGNING_PURPOSE
        )

    def test_deterministic_and_default_length(self) -> None:
        """Same inputs derive identical 32-byte keys on every call."""
        first = derive_key("test-master", b"snapper-test-v1")
        second = derive_key("test-master", b"snapper-test-v1")
        assert first == second
        assert len(first) == 32

    def test_length_parameter_honored(self) -> None:
        """A custom length yields a key of exactly that many bytes."""
        assert len(derive_key("test-master", b"snapper-test-v1", length=48)) == 48

    def test_version_bump_rotates_one_purpose(self) -> None:
        """A versioned purpose tag rotates that purpose independently."""
        assert derive_key("test-master", b"snapper-test-v1") != derive_key(
            "test-master", b"snapper-test-v2"
        )


class TestCommandSigningDelegation:
    """The control-plane key consolidation rotated nothing."""

    def test_command_key_is_byte_identical_to_the_original_inline_recipe(self) -> None:
        """command_signing_key equals the pre-consolidation construction.

        The golden hex below was computed from the ORIGINAL inlined
        PBKDF2 recipe before it delegated to derive_key — equality proves
        the consolidation did not rotate the fleet's control-plane key.
        """
        assert (
            command_signing_key("snapper_default_master_password_v1").hex()
            == "48e421ae88ca13a9bb24d0adbcba0a20d471ea78ac6e77b167430769072b3b2a"
        )
        assert command_signing_key("test-master") == derive_key(
            "test-master", b"snapper-process-command-signing-v1"
        )


class TestDeriveKeyCache:
    """The hot-path derivation is process-lifetime cached."""

    def test_derive_key_is_lru_cached(self) -> None:
        """Repeated derivations are cache hits, not fresh PBKDF2 runs.

        JWT verification and CSRF checks read derived keys per HTTP
        request; an uncached 100k-iteration PBKDF2 (~20 ms CPU) per
        request would be a request-amplified DoS surface.
        """
        derive_key.cache_clear()
        derive_key("cache-master", b"snapper-test-v1")
        derive_key("cache-master", b"snapper-test-v1")
        info = derive_key.cache_info()
        assert info.hits >= 1
        assert info.misses == 1
