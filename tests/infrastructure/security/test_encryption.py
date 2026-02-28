"""Unit tests for settings encryption service."""

import os
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from cryptography.fernet import InvalidToken

from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.infrastructure.security.encryption import clear_global_encryption
from snapper.infrastructure.security.encryption import decrypt_if_encrypted
from snapper.infrastructure.security.encryption import encrypt_if_sensitive
from snapper.infrastructure.security.encryption import force_encrypt_if_cleartext
from snapper.infrastructure.security.encryption import get_encryption_service
from snapper.infrastructure.security.encryption import get_global_encryption
from snapper.infrastructure.security.encryption import initialize_global_encryption


class TestSettingsEncryption:
    """Tests for SettingsEncryptionService core functionality."""

    def test_encrypt_decrypt_roundtrip(self) -> None:
        """Test encrypt/decrypt roundtrip.

        Given: SettingsEncryptionService with test password,
        When: encrypting and decrypting a value,
        Then: decrypted value equals original and ciphertext differs.
        """
        encryption = SettingsEncryptionService("test-password", "test-salt")
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
        encryption = SettingsEncryptionService("test-password", "test-salt")
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
        encryption = SettingsEncryptionService("test-password", "test-salt")
        original = "🔐 Secret with émojis and ü̧nicöde!"
        encrypted = encryption.encrypt(original)
        decrypted = encryption.decrypt(encrypted)
        assert decrypted == original

    def test_different_passwords_produce_different_results(self) -> None:
        """Test different passwords produce different ciphertexts.

        Given: two services with different passwords,
        When: encrypting the same plaintext,
        Then: ciphertexts are different.
        """
        enc1 = SettingsEncryptionService("password1", "test-salt")
        enc2 = SettingsEncryptionService("password2", "test-salt")
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
        enc1 = SettingsEncryptionService("correct-password", "test-salt")
        enc2 = SettingsEncryptionService("wrong-password", "test-salt")
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
        encryption = SettingsEncryptionService("test-password", "test-salt")
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

    def test_custom_salt(self) -> None:
        """Test custom salt for key derivation.

        Given: two services with same password and salt,
        When: encrypting with one and decrypting with other,
        Then: roundtrip succeeds.
        """
        salt = "custom_salt_1234"
        enc1 = SettingsEncryptionService("password", salt)
        enc2 = SettingsEncryptionService("password", salt)
        original = "test-value"
        encrypted1 = enc1.encrypt(original)
        decrypted2 = enc2.decrypt(encrypted1)
        assert decrypted2 == original


class TestGlobalEncryption:
    """Tests for global encryption service management."""

    def test_get_encryption_service_with_password(self) -> None:
        """Test get_encryption_service with explicit password.

        Given: explicit password string,
        When: calling get_encryption_service,
        Then: returns SettingsEncryptionService instance.
        """
        service = get_encryption_service("test-password")
        assert service is not None
        assert isinstance(service, SettingsEncryptionService)

    def test_get_encryption_service_with_password_and_salt(self) -> None:
        """Test get_encryption_service with password and salt.

        Given: explicit password and salt strings,
        When: calling get_encryption_service,
        Then: returns SettingsEncryptionService instance.
        """
        service = get_encryption_service("test-password", "test-salt-value")
        assert service is not None
        assert isinstance(service, SettingsEncryptionService)

    def test_get_encryption_service_no_password(self) -> None:
        """Test get_encryption_service with no password.

        Given: None as password parameter,
        When: calling get_encryption_service,
        Then: returns SettingsEncryptionService instance (fallback).
        """
        service = get_encryption_service(None)
        assert service is not None
        assert isinstance(service, SettingsEncryptionService)

    @patch.dict(os.environ, {"MASTER_PASSWORD": "env-password"})
    def test_get_encryption_service_from_env(self) -> None:
        """Test get_encryption_service from environment variable.

        Given: MASTER_PASSWORD set in environment,
        When: calling get_encryption_service with no args,
        Then: returns SettingsEncryptionService using env password.
        """
        service = get_encryption_service()
        assert service is not None
        assert isinstance(service, SettingsEncryptionService)

    @patch.dict(os.environ, {}, clear=True)
    def test_get_encryption_service_no_env_password(self) -> None:
        """Test get_encryption_service without env password.

        Given: no MASTER_PASSWORD in environment,
        When: calling get_encryption_service,
        Then: returns SettingsEncryptionService (default fallback).
        """
        service = get_encryption_service()
        assert service is not None
        assert isinstance(service, SettingsEncryptionService)

    def test_initialize_global_encryption(self) -> None:
        """Test global encryption initialization.

        Given: test password string,
        When: calling initialize_global_encryption,
        Then: returns service and get_global_encryption returns same instance.
        """
        encryption = initialize_global_encryption("test-password", "test-salt")
        assert encryption is not None
        assert isinstance(encryption, SettingsEncryptionService)
        global_enc = get_global_encryption()
        assert global_enc is not None
        assert global_enc is encryption

    def test_encrypt_if_sensitive_with_global_encryption(self) -> None:
        """Test encrypt_if_sensitive with global encryption.

        Given: global encryption initialized,
        When: encrypting sensitive and non-sensitive keys,
        Then: sensitive keys are encrypted, non-sensitive are unchanged.
        """
        initialize_global_encryption("test-password", "test-salt")
        value, is_encrypted = encrypt_if_sensitive("api_secret", "secret-value")
        assert is_encrypted is True
        assert value != "secret-value"
        value, is_encrypted = encrypt_if_sensitive("server_host", "localhost")
        assert is_encrypted is False
        assert value == "localhost"

    def test_encrypt_if_sensitive_no_global_encryption(self) -> None:
        """Test encrypt_if_sensitive without global encryption.

        Given: no global encryption initialized,
        When: calling encrypt_if_sensitive,
        Then: value unchanged and is_encrypted is False.
        """
        value, is_encrypted = encrypt_if_sensitive("api_secret", "secret-value")
        if get_global_encryption() is None:
            assert is_encrypted is False
            assert value == "secret-value"

    def test_decrypt_if_encrypted_with_global_encryption(self) -> None:
        """Test decrypt_if_encrypted with global encryption.

        Given: global encryption initialized and value encrypted,
        When: calling decrypt_if_encrypted with encrypted flag,
        Then: returns original value; plain value unchanged.
        """
        encryption = initialize_global_encryption("test-password", "test-salt")
        original = "secret-data"
        encrypted = encryption.encrypt(original)
        decrypted = decrypt_if_encrypted(encrypted, True)
        assert decrypted == original
        plain_value = "plain-data"
        result = decrypt_if_encrypted(plain_value, False)
        assert result == plain_value

    def test_decrypt_if_encrypted_no_global_encryption(self) -> None:
        """Test decrypt_if_encrypted without global encryption.

        Given: global encryption cleared,
        When: calling decrypt_if_encrypted with encrypted flag,
        Then: raises RuntimeError for encrypted, returns plain for non-encrypted.
        """
        clear_global_encryption()
        with pytest.raises(
            RuntimeError, match="Encryption not initialized but encrypted setting found"
        ):
            decrypt_if_encrypted("encrypted-data", True)
        result = decrypt_if_encrypted("plain-data", False)
        assert result == "plain-data"

    def test_decrypt_if_encrypted_empty_value(self) -> None:
        """Test decrypt_if_encrypted with empty value.

        Given: global encryption initialized,
        When: decrypting empty string with either flag,
        Then: returns empty string.
        """
        initialize_global_encryption("test-password", "test-salt")
        result = decrypt_if_encrypted("", True)
        assert result == ""
        result = decrypt_if_encrypted("", False)
        assert result == ""

    def test_decrypt_if_encrypted_invalid_encrypted_data(self) -> None:
        """Test decrypt_if_encrypted with invalid encrypted data.

        Given: global encryption initialized and invalid ciphertext,
        When: calling decrypt_if_encrypted with encrypted flag,
        Then: raises RuntimeError about decryption failure.
        """
        initialize_global_encryption("test-password", "test-salt")
        invalid_data = "not-valid-encrypted-data"
        with pytest.raises(RuntimeError, match="Failed to decrypt encrypted setting"):
            decrypt_if_encrypted(invalid_data, True)


class TestForceEncryptIfCleartext:
    """Tests for force_encrypt_if_cleartext utility function."""

    def test_force_encrypt_cleartext_sensitive_value(self) -> None:
        """Test force_encrypt_if_cleartext with cleartext sensitive value.

        Given: global encryption initialized and cleartext sensitive value,
        When: calling force_encrypt_if_cleartext,
        Then: value is encrypted and starts with Fernet prefix.
        """
        initialize_global_encryption("test-password", "test-salt")
        cleartext = "my-secret-api-key-12345"
        encrypted_value, is_encrypted = force_encrypt_if_cleartext("polygon_api_key", cleartext)
        assert is_encrypted is True
        assert encrypted_value != cleartext
        assert encrypted_value.startswith("gAAAAAB")

    def test_force_encrypt_already_encrypted_value(self) -> None:
        """Test force_encrypt_if_cleartext with already encrypted value.

        Given: global encryption initialized and pre-encrypted value,
        When: calling force_encrypt_if_cleartext,
        Then: value unchanged (no double encryption), decrypts to original.
        """
        initialize_global_encryption("test-password", "test-salt")
        encryption = get_global_encryption()
        assert encryption is not None
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

        Given: global encryption initialized and non-sensitive setting,
        When: calling force_encrypt_if_cleartext,
        Then: value unchanged and is_encrypted is False.
        """
        initialize_global_encryption("test-password", "test-salt")
        value = "some-regular-value"
        result_value, is_encrypted = force_encrypt_if_cleartext("regular_setting", value)
        assert is_encrypted is False
        assert result_value == value

    def test_force_encrypt_without_global_encryption(self) -> None:
        """Test force_encrypt_if_cleartext without global encryption.

        Given: global encryption cleared,
        When: calling force_encrypt_if_cleartext for sensitive key,
        Then: value unchanged and is_encrypted is False.
        """
        clear_global_encryption()
        value = "some-api-key"
        result_value, is_encrypted = force_encrypt_if_cleartext("polygon_api_key", value)
        assert is_encrypted is False
        assert result_value == value

    def test_get_encryption_service_bootstrap_no_password(self) -> None:
        """Test get_encryption_service with bootstrap loader returning no password.

        Given: mocked BootstrapSettingsLoader with no password,
        When: calling get_encryption_service(None, None),
        Then: returns None.
        """
        mock_bootstrap = MagicMock()
        mock_bootstrap.master_password = None
        mock_bootstrap.encryption_salt = None
        with patch(
            "snapper.infrastructure.security.encryption.BootstrapSettingsLoader",
            return_value=mock_bootstrap,
        ):
            result = get_encryption_service(None, None)
            assert result is None
