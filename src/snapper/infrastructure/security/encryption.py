"""Settings encryption using Fernet symmetric encryption.

This module provides the SettingsEncryptionService for protecting sensitive
configuration values like API keys and secrets. It uses PBKDF2-HMAC-SHA256
for key derivation and Fernet (AES-128-CBC + HMAC-SHA256) for encryption.

Security features:
    - Master password-derived encryption keys via PBKDF2
    - 100,000 iterations for key derivation (OWASP recommendation)
    - Automatic detection of encrypted vs cleartext values
    - Pattern-based identification of sensitive settings

Example:
    >>> from snapper.infrastructure.security.encryption import (
    ...     initialize_global_encryption,
    ...     encrypt_if_sensitive,
    ...     decrypt_if_encrypted,
    ... )
    >>> initialize_global_encryption("my-master-password", "my-salt")
    >>> value, encrypted = encrypt_if_sensitive("api_key", "secret123")
    >>> original = decrypt_if_encrypted(value, encrypted)
"""

import base64

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from loguru import logger

from snapper.config.bootstrap import BootstrapSettingsLoader

__all__ = [
    "SettingsEncryptionService",
    "initialize_global_encryption",
    "get_global_encryption",
    "clear_global_encryption",
    "encrypt_if_sensitive",
    "force_encrypt_if_cleartext",
    "decrypt_if_encrypted",
]


class SettingsEncryptionService:
    """Singleton service for encrypting/decrypting sensitive settings.

    Uses Fernet symmetric encryption with PBKDF2 key derivation from a
    master password. The singleton ensures consistent encryption across
    the application.

    Encrypted values are identifiable by the ``gAAAAAB`` prefix (Fernet
    format).

    Attributes:
        master_password: The password bytes used for key derivation.
        salt: Salt bytes for PBKDF2.

    Example:
        >>> service = SettingsEncryptionService("my-password", "my-salt")
        >>> encrypted = service.encrypt("api-key-value")
        >>> decrypted = service.decrypt(encrypted)
    """

    _instance: "SettingsEncryptionService | None" = None
    _init_params: tuple[str, str] | None = None
    _initialized: bool = False

    def __new__(cls, master_password: str, salt: str) -> "SettingsEncryptionService":
        """Create or return the singleton instance.

        Returns existing instance only if called with the same parameters.

        Args:
            master_password: Master password for key derivation.
            salt: Salt for PBKDF2.

        Returns:
            The singleton SettingsEncryptionService instance.
        """
        current_params = (master_password, salt)
        if cls._instance is not None and cls._init_params == current_params:
            return cls._instance
        instance = super().__new__(cls)
        cls._instance = instance
        cls._init_params = current_params
        return instance

    def __init__(self, master_password: str, salt: str) -> None:
        """Initialize the encryption service.

        Args:
            master_password: Master password for key derivation.
            salt: Salt for PBKDF2.
        """
        if self._initialized:
            return
        self._initialized = True
        self.master_password = master_password.encode()
        self.salt = salt.encode()
        self._fernet = self._create_fernet()

    def _create_fernet(self) -> Fernet:
        """Create Fernet instance with derived key.

        Uses PBKDF2-HMAC-SHA256 with 100,000 iterations to derive a
        32-byte key from the master password.

        Returns:
            Fernet instance initialized with the derived key.
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self.salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(self.master_password))
        return Fernet(key)

    def encrypt(self, plaintext: str) -> str:
        """Encrypt a plaintext string.

        Args:
            plaintext: The value to encrypt.

        Returns:
            Fernet-encrypted string (base64 encoded, ``gAAAAAB`` prefix).

        Raises:
            Exception: If encryption fails.
        """
        try:
            encrypted_bytes = self._fernet.encrypt(plaintext.encode())
            return encrypted_bytes.decode()
        except Exception as e:
            logger.error(f"Failed to encrypt value: {e}")
            raise

    def decrypt(self, encrypted: str) -> str:
        """Decrypt an encrypted string.

        Args:
            encrypted: Fernet-encrypted string to decrypt.

        Returns:
            Original plaintext value.

        Raises:
            Exception: If decryption fails (wrong key, corrupted data).
        """
        try:
            decrypted_bytes = self._fernet.decrypt(encrypted.encode())
            return decrypted_bytes.decode()
        except Exception as e:
            logger.error(f"Failed to decrypt value: {e}")
            raise

    def is_encrypted_value(self, value: str) -> bool:
        """Check if a value appears to be Fernet-encrypted.

        Fernet tokens start with ``gAAAAAB`` and are at least 40 characters.

        Args:
            value: String value to check.

        Returns:
            True if value matches Fernet token pattern.
        """
        if not value or len(value) < 40:
            return False
        return value.startswith("gAAAAAB")

    @staticmethod
    def is_sensitive_setting(key: str) -> bool:
        """Check if a setting key indicates sensitive data.

        Matches common patterns for API keys, passwords, and secrets.

        Args:
            key: Setting key name to check.

        Returns:
            True if key matches sensitive patterns.
        """
        sensitive_patterns = [
            "api_key",
            "api_secret",
            "password",
            "secret_key",
            "private_key",
            "credential",
        ]
        key_lower = key.lower()
        return any(pattern in key_lower for pattern in sensitive_patterns)

    @classmethod
    def get_instance(cls) -> "SettingsEncryptionService | None":
        """Get the singleton instance if it exists.

        Returns:
            The singleton instance or None if not initialized.
        """
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear the singleton instance.

        Resets the singleton state. Useful for testing or re-initialization.
        """
        cls._instance = None
        cls._init_params = None


def get_encryption_service(
    master_password: str | None = None, salt: str | None = None
) -> SettingsEncryptionService | None:
    """Get or create an encryption service instance.

    Falls back to bootstrap settings if parameters not provided.

    Args:
        master_password: Optional master password override.
        salt: Optional salt string override.

    Returns:
        SettingsEncryptionService instance, or None if no password available.
    """
    if not master_password or not salt:
        bootstrap = BootstrapSettingsLoader()
        master_password = master_password or bootstrap.master_password
        salt = salt or bootstrap.encryption_salt
    if not master_password:
        logger.warning("No master password provided - encryption disabled")
        return None
    return SettingsEncryptionService(master_password, salt)


def initialize_global_encryption(master_password: str, salt: str) -> SettingsEncryptionService:
    """Initialize the global encryption singleton.

    Must be called before using encrypt_if_sensitive or decrypt_if_encrypted.

    Args:
        master_password: Master password for key derivation.
        salt: Salt for PBKDF2.

    Returns:
        The initialized SettingsEncryptionService instance.
    """
    encryption = SettingsEncryptionService(master_password, salt)
    logger.info("AppSettings encryption initialized")
    return encryption


def get_global_encryption() -> SettingsEncryptionService | None:
    """Get the global encryption singleton.

    Returns:
        The encryption service instance, or None if not initialized.
    """
    return SettingsEncryptionService.get_instance()


def clear_global_encryption() -> None:
    """Clear the global encryption singleton.

    Useful for testing or when changing master password.
    """
    SettingsEncryptionService.clear_instance()


def encrypt_if_sensitive(key: str, value: str) -> tuple[str, bool]:
    """Encrypt a value if the key indicates sensitive data.

    Skips encryption if value is already encrypted or if the key
    doesn't match sensitive patterns.

    Args:
        key: Setting key name (used to detect sensitivity).
        value: Value to potentially encrypt.

    Returns:
        Tuple of (possibly encrypted value, whether encryption was applied).
    """
    encryption = get_global_encryption()
    if encryption and SettingsEncryptionService.is_sensitive_setting(key):
        if encryption.is_encrypted_value(value):
            return value, True
        return encryption.encrypt(value), True
    return value, False


def force_encrypt_if_cleartext(key: str, value: str) -> tuple[str, bool]:
    """Encrypt a cleartext value if the key indicates sensitive data.

    Similar to encrypt_if_sensitive but intended for explicit encryption
    scenarios where the value is known to be cleartext.

    Args:
        key: Setting key name (used to detect sensitivity).
        value: Value to potentially encrypt.

    Returns:
        Tuple of (possibly encrypted value, whether it should be stored encrypted).
    """
    encryption = get_global_encryption()
    if encryption and SettingsEncryptionService.is_sensitive_setting(key):
        if encryption.is_encrypted_value(value):
            return value, True
        return encryption.encrypt(value), True
    return value, False


def decrypt_if_encrypted(value: str, is_encrypted: bool) -> str:
    """Decrypt a value if it was marked as encrypted.

    Args:
        value: Potentially encrypted value.
        is_encrypted: Flag indicating whether decryption is needed.

    Returns:
        Decrypted value, or original value if not encrypted.

    Raises:
        RuntimeError: If encryption not initialized but decryption needed,
            or if decryption fails (wrong password, corrupted data).
    """
    if not is_encrypted:
        return value
    if not value:
        return value
    encryption = get_global_encryption()
    if not encryption:
        raise RuntimeError(
            "Encryption not initialized but encrypted setting found. "
            "Check MASTER_PASSWORD environment variable."
        )
    try:
        return encryption.decrypt(value)
    except Exception as e:
        raise RuntimeError(
            f"Failed to decrypt encrypted setting. This could indicate master password "
            f"rotation or corruption. Original error: {e}"
        ) from e
