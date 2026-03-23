"""Base classes for exchange schemas.

This module provides base Pydantic model classes for all exchange-specific
schema modules, ensuring consistent validation behavior across exchange
integrations.

Schema hierarchy for exchange boundary::

    BaseModel
    ├── ExchangeRequest   → outgoing requests to exchange APIs (extra="forbid")
    └── ExchangeResponse  → parsing exchange API responses (extra="allow")

Both inherit BaseModel directly (not StrictBody) because exchange schemas
are an external boundary layer, separate from Snapper's internal schema
hierarchy. The configs are equivalent in strictness but semantically distinct.
"""

from pydantic import BaseModel
from pydantic import ConfigDict

EXCHANGE_RESPONSE_CONFIG = ConfigDict(
    populate_by_name=True,
    extra="allow",
    strict=True,
)

EXCHANGE_REQUEST_CONFIG = ConfigDict(
    populate_by_name=True,
    extra="forbid",
    strict=True,
)


class ExchangeResponse(BaseModel):
    """Base for parsing exchange API responses.

    Uses extra="allow" so new fields added by exchange APIs do not break
    parsing. Strict type validation is still enforced on known fields.
    """

    model_config = EXCHANGE_RESPONSE_CONFIG


class ExchangeRequest(BaseModel):
    """Base for outgoing requests to exchange APIs.

    Uses extra="forbid" to catch typos in request parameters — unknown
    fields would be silently ignored by the exchange, hiding bugs.
    """

    model_config = EXCHANGE_REQUEST_CONFIG


__all__ = [
    "EXCHANGE_REQUEST_CONFIG",
    "EXCHANGE_RESPONSE_CONFIG",
    "ExchangeRequest",
    "ExchangeResponse",
]
