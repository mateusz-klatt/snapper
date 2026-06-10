"""Security utilities for sensitive data handling.

This package provides encryption services for protecting sensitive configuration
data such as API keys and secrets. It uses Fernet symmetric encryption with
PBKDF2 key derivation.

Modules:
    encryption: SettingsEncryptionService for encrypting/decrypting sensitive
        settings with password-derived keys.

Security features:
    - PBKDF2-HMAC-SHA256 key derivation with 100,000 iterations
    - Fernet (AES-128-CBC with HMAC) authenticated encryption
    - Automatic detection of encrypted vs cleartext values
    - Thread-safe global encryption service singleton
"""
