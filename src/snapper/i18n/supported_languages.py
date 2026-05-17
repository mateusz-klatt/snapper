"""Catalog language codes a user may pick as their ``default_language``.

Mirrors ``CatalogLanguage`` in ``ios/Snapper/I18n/CatalogLanguage.swift``
(44 cases). Phase B of `plan_2026_05_17_backend_user_language_i18n.md`
will replace this hardcoded set with a parser that reads the actual
xcstrings file at build time, but for Phase A the values are inlined so
the API endpoint can validate input without depending on the catalog
infrastructure.
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
        "pt-BR",
        "sv",
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
        "zh-Hans",
        "zh-Hant",
        "ja",
        "ko",
        "th",
        "vi",
        "my",
        "hi",
        "ar",
        "he",
        "fa",
        "hy",
    }
)
"""Frozen set of 45 catalog language codes a user may select as their
``default_language``. Source of truth mirrors iOS ``CatalogLanguage``;
must stay in sync until Phase B replaces this with a generator-driven
constant. Drift is currently caught by the catalog-parity test."""


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
