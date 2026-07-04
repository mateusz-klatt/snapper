"""Tests for control-plane command signing (control plane P2.1)."""

import hashlib

from snapper.core.json_types import JsonObject
from snapper.messaging.security.command_signing import command_signing_key
from snapper.messaging.security.command_signing import sign_command_payload
from snapper.messaging.security.command_signing import verify_command_payload


def _payload() -> JsonObject:
    """Build a representative unsigned command payload.

    Returns:
        A command-shaped dict without a signature field.
    """
    return {
        "command_id": "cid-1",
        "coordinator": "coord-2",
        "process_name": "strategy_macd",
        "action": "restart",
        "issued_by": "api",
        "issued_at": "2026-07-04T00:00:00Z",
    }


class TestCommandSigningKey:
    """Tests for the PBKDF2-derived control-plane signing key."""

    def test_key_is_deterministic_and_32_bytes(self) -> None:
        """The derived key is stable per master password and 32 bytes long.

        Given: the same master password,
        When: the key is derived twice,
        Then: both derivations are identical 32-byte keys.
        """
        first = command_signing_key("master")
        second = command_signing_key("master")

        assert first == second
        assert len(first) == 32

    def test_key_differs_per_master_password(self) -> None:
        """Different master passwords derive different keys.

        Given: two different master passwords,
        When: keys are derived,
        Then: the keys differ.
        """
        assert command_signing_key("a") != command_signing_key("b")

    def test_key_is_not_the_raw_salt(self) -> None:
        """The derived key is not simply the sha256 salt of the password.

        Given: a master password,
        When: the key is derived,
        Then: it differs from sha256(password), proving the PBKDF2 derivation
            actually runs rather than returning a trivial hash.
        """
        assert command_signing_key("master") != hashlib.sha256(b"master").digest()


class TestSignVerify:
    """Tests for sign/verify roundtrips and rejections."""

    def test_sign_then_verify_roundtrips(self) -> None:
        """A signed payload verifies with the same key.

        Given: a payload signed with a key,
        When: the signature is attached and verified,
        Then: verification passes.
        """
        key = command_signing_key("master")
        payload = _payload()
        payload["signature"] = sign_command_payload(payload, key)

        assert verify_command_payload(payload, key) is True

    def test_signature_ignores_existing_signature_field(self) -> None:
        """Signing ignores a pre-existing signature field (idempotent).

        Given: a payload,
        When: it is signed, its signature field is overwritten with junk, and re-signed,
        Then: both signatures are identical.
        """
        key = command_signing_key("master")
        payload = _payload()
        first = sign_command_payload(payload, key)
        payload["signature"] = "junk"
        second = sign_command_payload(payload, key)

        assert first == second

    def test_signature_is_field_order_independent(self) -> None:
        """Canonical signing is independent of dict insertion order.

        Given: two payloads with the same content in different key order,
        When: each is signed,
        Then: the signatures are identical.
        """
        key = command_signing_key("master")
        first_order: JsonObject = {"b": 2, "a": 1}
        second_order: JsonObject = {"a": 1, "b": 2}

        assert sign_command_payload(first_order, key) == sign_command_payload(second_order, key)

    def test_verify_rejects_tampered_payload(self) -> None:
        """A modified field fails verification.

        Given: a signed payload,
        When: a field is changed after signing,
        Then: verification fails.
        """
        key = command_signing_key("master")
        payload = _payload()
        payload["signature"] = sign_command_payload(payload, key)
        payload["action"] = "disable"

        assert verify_command_payload(payload, key) is False

    def test_verify_rejects_wrong_key(self) -> None:
        """A signature made with one key fails under another.

        Given: a payload signed with key A,
        When: verified with key B,
        Then: verification fails.
        """
        payload = _payload()
        payload["signature"] = sign_command_payload(payload, command_signing_key("a"))

        assert verify_command_payload(payload, command_signing_key("b")) is False

    def test_verify_rejects_missing_signature(self) -> None:
        """A payload without a signature field fails verification.

        Given: an unsigned payload,
        When: verified,
        Then: verification fails.
        """
        assert verify_command_payload(_payload(), command_signing_key("master")) is False

    def test_verify_rejects_non_string_signature(self) -> None:
        """A non-string signature field fails verification.

        Given: a payload whose signature field is not a string,
        When: verified,
        Then: verification fails.
        """
        payload = _payload()
        payload["signature"] = 12345

        assert verify_command_payload(payload, command_signing_key("master")) is False
