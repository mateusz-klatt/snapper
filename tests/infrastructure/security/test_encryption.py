"""Unit tests for settings encryption service."""

import pytest
from cryptography.fernet import InvalidToken

from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.infrastructure.security.encryption import clear_encryption
from snapper.infrastructure.security.encryption import decrypt_if_encrypted
from snapper.infrastructure.security.encryption import encrypt_if_sensitive
from snapper.infrastructure.security.encryption import force_encrypt_if_cleartext
from snapper.infrastructure.security.encryption import get_encryption_service


class TestSettingsEncryption:
    """Tests for SettingsEncryptionService core functionality."""

    def test_encrypt_decrypt_roundtrip(self) -> None:
        """Test encrypt/decrypt roundtrip.

        Given: SettingsEncryptionService with test password,
        When: encrypting and decrypting a value,
        Then: decrypted value equals original and ciphertext differs.
        """
        encryption = SettingsEncryptionService("test-password")
        original = "super-secret-value"
        encrypted = encryption.encrypt(original)
        decrypted = encryption.decrypt(encrypted)
        assert decrypted == original
        assert encrypted != original

    def test_empty_string_encryption(self) -> None:
        """Test empty string encryption.

        Given: SettingsEncryptionService with test password,
        When: encrypting and decrypting empty string,
        Then: roundtrip preserves empty string.
        """
        encryption = SettingsEncryptionService("test-password")
        original = ""
        encrypted = encryption.encrypt(original)
        decrypted = encryption.decrypt(encrypted)
        assert decrypted == original

    def test_unicode_encryption(self) -> None:
        """Test Unicode character encryption.

        Given: SettingsEncryptionService with test password,
        When: encrypting value with emojis and Unicode,
        Then: roundtrip preserves all special characters.
        """
        encryption = SettingsEncryptionService("test-password")
        original = "\U0001f510 Secret with \u00e9mojis and \u00fc\u0327nic\u00f6de!"
        encrypted = encryption.encrypt(original)
        decrypted = encryption.decrypt(encrypted)
        assert decrypted == original

    def test_different_passwords_produce_different_results(self) -> None:
        """Test different passwords produce different ciphertexts.

        Given: two services with different passwords,
        When: encrypting the same plaintext,
        Then: ciphertexts are different.
        """
        enc1 = SettingsEncryptionService("password1")
        enc2 = SettingsEncryptionService("password2")
        original = "same-plaintext"
        encrypted1 = enc1.encrypt(original)
        encrypted2 = enc2.encrypt(original)
        assert encrypted1 != encrypted2

    def test_decrypt_with_wrong_password_fails(self) -> None:
        """Test decryption with wrong password.

        Given: value encrypted with correct password,
        When: decrypting with wrong password,
        Then: InvalidToken exception is raised.
        """
        enc1 = SettingsEncryptionService("correct-password")
        enc2 = SettingsEncryptionService("wrong-password")
        original = "secret-data"
        encrypted = enc1.encrypt(original)
        with pytest.raises(InvalidToken):
            enc2.decrypt(encrypted)

    def test_is_encrypted_value(self) -> None:
        """Test encrypted value detection.

        Given: SettingsEncryptionService with test password,
        When: checking various values with is_encrypted_value,
        Then: encrypted values return True, plain text returns False.
        """
        encryption = SettingsEncryptionService("test-password")
        encrypted = encryption.encrypt("test-value")
        assert encryption.is_encrypted_value(encrypted)
        assert not encryption.is_encrypted_value("plain-text")
        assert not encryption.is_encrypted_value("")
        assert not encryption.is_encrypted_value("not-base64-!")

    def test_is_sensitive_setting(self) -> None:
        """Test sensitive setting name identification.

        Given: list of setting names,
        When: calling is_sensitive_setting on each,
        Then: keys/secrets/passwords return True, others return False.
        """
        assert SettingsEncryptionService.is_sensitive_setting("api_key")
        assert SettingsEncryptionService.is_sensitive_setting("api_secret")
        assert SettingsEncryptionService.is_sensitive_setting("password")
        assert SettingsEncryptionService.is_sensitive_setting("auth_secret_key")
        assert SettingsEncryptionService.is_sensitive_setting("csrf_secret_key")
        assert SettingsEncryptionService.is_sensitive_setting("KRAKEN_API_SECRET")
        assert SettingsEncryptionService.is_sensitive_setting("private_key")
        assert SettingsEncryptionService.is_sensitive_setting("walutomat_private_key")
        assert SettingsEncryptionService.is_sensitive_setting("credential_data")
        assert not SettingsEncryptionService.is_sensitive_setting("server_host")
        assert not SettingsEncryptionService.is_sensitive_setting("port")
        assert not SettingsEncryptionService.is_sensitive_setting("timeout")
        assert not SettingsEncryptionService.is_sensitive_setting("database_url")
        assert not SettingsEncryptionService.is_sensitive_setting("auth_algorithm")
        assert not SettingsEncryptionService.is_sensitive_setting("auth_token_expire_minutes")
        assert not SettingsEncryptionService.is_sensitive_setting(
            "auth_refresh_token_expire_days_extended"
        )
        assert not SettingsEncryptionService.is_sensitive_setting("csrf_token_expire_minutes")


class TestGetEncryptionService:
    """Tests for get_encryption_service and clear_encryption."""

    def test_get_encryption_service_returns_instance(self) -> None:
        """Test get_encryption_service returns SettingsEncryptionService.

        Given: bootstrap settings available,
        When: calling get_encryption_service,
        Then: returns SettingsEncryptionService instance.
        """
        service = get_encryption_service()
        assert isinstance(service, SettingsEncryptionService)

    def test_clear_encryption_resets_singleton(self) -> None:
        """Test clear_encryption resets the singleton.

        Given: encryption service initialized,
        When: calling clear_encryption then get_encryption_service,
        Then: a new instance is created.
        """
        first = get_encryption_service()
        clear_encryption()
        second = get_encryption_service()
        assert isinstance(second, SettingsEncryptionService)
        assert first is not second


class TestEncryptIfSensitive:
    """Tests for encrypt_if_sensitive helper."""

    def test_encrypt_sensitive_key(self) -> None:
        """Test encrypt_if_sensitive encrypts sensitive keys.

        Given: sensitive key with plaintext value,
        When: calling encrypt_if_sensitive,
        Then: value is encrypted and is_encrypted is True.
        """
        value, is_encrypted = encrypt_if_sensitive("api_secret", "secret-value")
        assert is_encrypted is True
        assert value != "secret-value"

    def test_skip_non_sensitive_key(self) -> None:
        """Test encrypt_if_sensitive skips non-sensitive keys.

        Given: non-sensitive key,
        When: calling encrypt_if_sensitive,
        Then: value unchanged and is_encrypted is False.
        """
        value, is_encrypted = encrypt_if_sensitive("server_host", "localhost")
        assert is_encrypted is False
        assert value == "localhost"

    def test_already_encrypted_value(self) -> None:
        """Test encrypt_if_sensitive skips already encrypted values.

        Given: already encrypted value for sensitive key,
        When: calling encrypt_if_sensitive,
        Then: value unchanged (no double encryption), is_encrypted is True.
        """
        encryption = get_encryption_service()
        encrypted_value = encryption.encrypt("my-secret")
        result_value, is_encrypted = encrypt_if_sensitive("api_secret", encrypted_value)
        assert is_encrypted is True
        assert result_value == encrypted_value


class TestDecryptIfEncrypted:
    """Tests for decrypt_if_encrypted helper."""

    def test_decrypt_encrypted_value(self) -> None:
        """Test decrypt_if_encrypted decrypts encrypted value.

        Given: encrypted value,
        When: calling decrypt_if_encrypted with is_encrypted=True,
        Then: returns original plaintext.
        """
        encryption = get_encryption_service()
        original = "secret-data"
        encrypted = encryption.encrypt(original)
        decrypted = decrypt_if_encrypted(encrypted, True)
        assert decrypted == original

    def test_pass_through_non_encrypted(self) -> None:
        """Test decrypt_if_encrypted passes through non-encrypted values.

        Given: plain value,
        When: calling decrypt_if_encrypted with is_encrypted=False,
        Then: returns value unchanged.
        """
        result = decrypt_if_encrypted("plain-data", False)
        assert result == "plain-data"

    def test_empty_value_returns_empty(self) -> None:
        """Test decrypt_if_encrypted with empty value.

        Given: empty string,
        When: decrypting with either flag,
        Then: returns empty string.
        """
        result = decrypt_if_encrypted("", True)
        assert result == ""
        result = decrypt_if_encrypted("", False)
        assert result == ""

    def test_invalid_encrypted_data_raises(self) -> None:
        """Test decrypt_if_encrypted with invalid encrypted data.

        Given: invalid ciphertext,
        When: calling decrypt_if_encrypted with is_encrypted=True,
        Then: raises RuntimeError about decryption failure.
        """
        invalid_data = "not-valid-encrypted-data"
        with pytest.raises(RuntimeError, match="Failed to decrypt encrypted setting"):
            decrypt_if_encrypted(invalid_data, True)


class TestForceEncryptIfCleartext:
    """Tests for force_encrypt_if_cleartext utility function."""

    def test_force_encrypt_cleartext_sensitive_value(self) -> None:
        """Test force_encrypt_if_cleartext with cleartext sensitive value.

        Given: cleartext sensitive value,
        When: calling force_encrypt_if_cleartext,
        Then: value is encrypted and starts with Fernet prefix.
        """
        cleartext = "my-secret-api-key-12345"
        encrypted_value, is_encrypted = force_encrypt_if_cleartext("polygon_api_key", cleartext)
        assert is_encrypted is True
        assert encrypted_value != cleartext
        assert encrypted_value.startswith("gAAAAAB")

    def test_force_encrypt_already_encrypted_value(self) -> None:
        """Test force_encrypt_if_cleartext with already encrypted value.

        Given: pre-encrypted value,
        When: calling force_encrypt_if_cleartext,
        Then: value unchanged (no double encryption), decrypts to original.
        """
        encryption = get_encryption_service()
        original = "my-secret-key"
        encrypted_once = encryption.encrypt(original)
        encrypted_again, is_encrypted = force_encrypt_if_cleartext(
            "walutomat_api_key", encrypted_once
        )
        assert is_encrypted is True
        assert encrypted_again == encrypted_once
        decrypted = encryption.decrypt(encrypted_again)
        assert decrypted == original

    def test_force_encrypt_non_sensitive_key(self) -> None:
        """Test force_encrypt_if_cleartext with non-sensitive key.

        Given: non-sensitive setting,
        When: calling force_encrypt_if_cleartext,
        Then: value unchanged and is_encrypted is False.
        """
        value = "some-regular-value"
        result_value, is_encrypted = force_encrypt_if_cleartext("regular_setting", value)
        assert is_encrypted is False
        assert result_value == value
