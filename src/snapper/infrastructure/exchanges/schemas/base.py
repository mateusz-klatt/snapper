"""Base configuration objects for exchange schemas.

This module provides common Pydantic model configuration objects used
across all exchange-specific schema modules to ensure consistent
validation behavior.
"""

from pydantic import ConfigDict

EXCHANGE_SCHEMA_CONFIG = ConfigDict(
    populate_by_name=True,
    extra="allow",
    strict=True,
)
"""Pydantic config for parsing exchange API responses.

This configuration allows extra fields (since exchange APIs may add new
fields over time) while maintaining strict type validation. Field aliases
are enabled for mapping snake_case to exchange-specific formats.
"""

STRICT_SCHEMA_CONFIG = ConfigDict(
    populate_by_name=True,
    extra="forbid",
    strict=True,
)
"""Pydantic config for outgoing requests to exchange APIs.

This configuration forbids extra fields to catch typos in request
parameters. Used for subscription and order request schemas where
unknown fields would be ignored by the exchange.
"""

__all__ = [
    "EXCHANGE_SCHEMA_CONFIG",
    "STRICT_SCHEMA_CONFIG",
]
