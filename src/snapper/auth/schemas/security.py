"""Security schemas module.

This module defines Pydantic schemas for security-related data
structures like CSRF tokens.
"""

from datetime import datetime

from snapper.api.schemas.base import StrictApiSchema


class CsrfToken(StrictApiSchema):
    """CSRF token schema.

    Attributes:
        csrf_token: The CSRF token string.
        expires_at: Token expiration timestamp.
    """

    csrf_token: str
    expires_at: datetime
