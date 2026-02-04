"""Rate limiting configuration for the FastAPI application.

Provides a pre-configured slowapi Limiter instance that uses
the client IP address as the rate limit key.
"""

from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
"""Application-wide rate limiter keyed by client IP address."""

LOGIN_RATE_LIMIT = "5/15minutes"
"""Maximum login attempts per IP within a 15-minute window."""

PASSWORD_CHANGE_RATE_LIMIT = "5/hour"
"""Maximum password change attempts per IP within one hour."""

PASSWORD_RESET_RATE_LIMIT = "10/hour"
"""Maximum admin password reset attempts per IP within one hour."""
