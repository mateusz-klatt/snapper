"""Tests for server locale resolution helpers."""

from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.server._locale_utils import resolve_caller_default_language


def _principal(user_public_id: str = "user-alpha") -> AuthPrincipal:
    """Build a principal with a stable user public ID."""
    return AuthPrincipal(
        username="test-user",
        role=UserRole.ADMIN,
        user_public_id=user_public_id,
    )


@pytest.mark.asyncio
async def test_resolve_caller_default_language_returns_user_value() -> None:
    """User default_language wins when present.

    Given: an authenticated principal with a stored default language,
    When: resolving the caller language,
    Then: the stored language is returned.
    """
    repo = AsyncMock()
    repo.get_default_languages_for_users = AsyncMock(return_value={"user-alpha": "pl"})
    result = await resolve_caller_default_language(cast(Repository, repo), _principal())
    assert result == "pl"
    repo.get_default_languages_for_users.assert_awaited_once_with(["user-alpha"])


@pytest.mark.asyncio
async def test_resolve_caller_default_language_returns_en_when_principal_none() -> None:
    """Missing principal falls back to English.

    Given: no authenticated principal,
    When: resolving the caller language,
    Then: English is returned without querying user preferences.
    """
    repo = AsyncMock()
    repo.get_default_languages_for_users = AsyncMock(return_value={})
    result = await resolve_caller_default_language(cast(Repository, repo), None)
    assert result == "en"
    repo.get_default_languages_for_users.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_caller_default_language_returns_en_when_user_value_none() -> None:
    """Null stored default_language falls back to English.

    Given: an authenticated principal with null stored default_language,
    When: resolving the caller language,
    Then: English is returned.
    """
    repo = AsyncMock()
    repo.get_default_languages_for_users = AsyncMock(return_value={"user-alpha": None})
    result = await resolve_caller_default_language(cast(Repository, repo), _principal())
    assert result == "en"
