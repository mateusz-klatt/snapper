"""Locale resolution helpers for authenticated server routes."""

from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository

_DEFAULT_LANGUAGE = "en"


async def resolve_caller_default_language(
    repo: Repository,
    principal: AuthPrincipal | None,
) -> str:
    """Resolve the caller's preferred backend content language.

    Args:
        repo: Repository used to load the caller's user preference.
        principal: Authenticated caller, or ``None`` for unauthenticated
            guard paths.

    Returns:
        User ``default_language`` when present, otherwise English.
    """
    if principal is None or principal.user_public_id == "":
        return _DEFAULT_LANGUAGE
    languages = await repo.get_default_languages_for_users([principal.user_public_id])
    return languages.get(principal.user_public_id) or _DEFAULT_LANGUAGE
