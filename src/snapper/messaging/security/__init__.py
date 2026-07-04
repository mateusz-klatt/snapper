"""Signing utilities for the control-plane messaging path.

This package provides HMAC signing and verification for cross-container
process commands and acknowledgements, keyed by an HKDF derivation of the
single master secret.
"""
