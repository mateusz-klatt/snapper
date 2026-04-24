"""Rate limiting configuration for the FastAPI application.

Provides a pre-configured slowapi Limiter instance that uses
the client IP address as the rate limit key.
"""

import inspect
import math
import time
from typing import Any
from typing import cast

import slowapi.extension as slowapi_extension
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from limits import parse
from limits.limits import RateLimitItem
from slowapi import Limiter
from slowapi.util import get_remote_address


def _patch_slowapi_coroutine_detection() -> None:
    """Patch slowapi to use non-deprecated coroutine detection on Python 3.14+."""
    asyncio_module = cast(Any, slowapi_extension).asyncio
    asyncio_module.iscoroutinefunction = inspect.iscoroutinefunction


_patch_slowapi_coroutine_detection()
limiter = Limiter(key_func=get_remote_address)
"""Application-wide rate limiter keyed by client IP address."""

LOGIN_RATE_LIMIT = "5/15minutes"
"""Maximum failed login attempts per username and IP within a 15-minute window."""

ACCOUNT_CHANGE_RATE_LIMIT = "5/hour"
"""Maximum password change attempts per IP within one hour."""

ACCOUNT_RESET_RATE_LIMIT = "10/hour"
"""Maximum admin password reset attempts per IP within one hour."""

_LOGIN_RATE_LIMIT_ITEM: RateLimitItem = parse(LOGIN_RATE_LIMIT)


def _build_failed_login_identifier(request: Request, username: str) -> str:
    """Build failed-login limiter key scoped to username and source IP.

    Args:
        request: FastAPI request.
        username: Username from login payload.

    Returns:
        Combined identifier used by the limiter storage.
    """
    remote_address = get_remote_address(request)
    normalized_username = username.strip().lower()
    return f"{remote_address}:{normalized_username}"


def _get_retry_after_seconds(identifier: str) -> int:
    """Get seconds until current failed-login window resets.

    Args:
        identifier: Failed-login limiter key.

    Returns:
        Retry-After value in whole seconds.
    """
    window_stats = limiter.limiter.get_window_stats(_LOGIN_RATE_LIMIT_ITEM, identifier)
    remaining_seconds = math.ceil(window_stats.reset_time - time.time())
    return max(1, int(remaining_seconds))


def enforce_failed_login_rate_limit(request: Request, username: str) -> None:
    """Validate failed-login quota and raise 429 when exhausted.

    Args:
        request: FastAPI request.
        username: Username from login payload.

    Raises:
        HTTPException: If failed-login limit is exceeded.
    """
    if not limiter.enabled:
        return
    identifier = _build_failed_login_identifier(request, username)
    is_allowed = limiter.limiter.test(_LOGIN_RATE_LIMIT_ITEM, identifier)
    if is_allowed:
        return
    retry_after = _get_retry_after_seconds(identifier)
    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Too many failed login attempts",
        headers={"Retry-After": str(retry_after)},
    )


def register_failed_login_attempt(request: Request, username: str) -> None:
    """Increment failed-login counter for username and source IP.

    Args:
        request: FastAPI request.
        username: Username from login payload.
    """
    if not limiter.enabled:
        return
    identifier = _build_failed_login_identifier(request, username)
    limiter.limiter.hit(_LOGIN_RATE_LIMIT_ITEM, identifier)


def clear_failed_login_attempts(request: Request, username: str) -> None:
    """Reset failed-login counter after successful authentication.

    Args:
        request: FastAPI request.
        username: Username from login payload.
    """
    if not limiter.enabled:
        return
    identifier = _build_failed_login_identifier(request, username)
    limiter.limiter.clear(_LOGIN_RATE_LIMIT_ITEM, identifier)
