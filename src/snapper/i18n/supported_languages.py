"""Catalog language codes a user may pick as their ``default_language``.

Union of ``CatalogLanguage`` codes from both the iOS xcstrings catalog
(``ios/Snapper/I18n/CatalogLanguage.swift``) and the frontend i18n
catalog (``frontend/src/i18n/types.ts``). The two clients ship slightly
different code forms for the same languages (e.g. iOS uses ``pt-BR`` and
``nb``; frontend uses ``pt`` and ``no``) — Phase B will introduce a
canonical normalization layer, but Phase A accepts both forms so each
client can send what its own catalog uses without a translation step.

Phase B of `plan_2026_05_17_backend_user_language_i18n.md` will replace
this hardcoded set with a generator-driven constant sourced from the
authoritative xcstrings parse + a frontend-↔-iOS code mapping table.
"""

SUPPORTED_LANGUAGES: frozenset[str] = frozenset(
    {
        "en",
        "ga",
        "pl",
        "de",
        "fr",
        "es",
        "it",
        "nl",
        "pt",
        "pt-BR",
        "sv",
        "no",
        "nb",
        "da",
        "fi",
        "cs",
        "sk",
        "hu",
        "ro",
        "hr",
        "uk",
        "ru",
        "lt",
        "lv",
        "sr",
        "sr-Latn",
        "bs",
        "sq",
        "is",
        "el",
        "tr",
        "fil",
        "ms",
        "id",
        "sw",
        "bn",
        "zh",
        "zh-Hans",
        "zh-Hant",
        "ja",
        "ko",
        "th",
        "vi",
        "my",
        "my-MM",
        "hi",
        "ar",
        "he",
        "fa",
        "hy",
    }
)
"""Frozen set of catalog language codes a user may select as their
``default_language``. Source: union of iOS ``CatalogLanguage`` (45
cases) and frontend ``CatalogLanguage`` (45 cases), differing in 5
codes — total 50. Phase B will collapse the union via a single
normalization layer."""


def is_supported_language(value: str) -> bool:
    """Return True when ``value`` is a known catalog language code.

    Pydantic's ``Literal[...]`` form is preferred at the request-schema
    boundary; this helper exists for service-layer revalidation and for
    tests that exercise unknown codes.

    Args:
        value: Candidate language code (e.g. ``"pl"``, ``"zh-Hans"``).

    Returns:
        ``True`` when ``value`` is in ``SUPPORTED_LANGUAGES``;
        ``False`` otherwise (typos, empty strings, drift from iOS).
    """
    return value in SUPPORTED_LANGUAGES
