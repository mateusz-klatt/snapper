"""Tests for ``snapper.i18n.supported_languages``.

Locks in the set membership against accidental drift. Phase B will
replace the hardcoded set with a generator-driven constant; this test
becomes a parity check at that point.
"""

import pytest

from snapper.i18n.supported_languages import SUPPORTED_LANGUAGES
from snapper.i18n.supported_languages import is_supported_language


def test_set_is_frozen_so_callers_cannot_mutate_the_canonical_list() -> None:
    """Frozen at module level.

    Given: The module-level ``SUPPORTED_LANGUAGES`` constant.
    When: Callers inspect its type.
    Then: It is a ``frozenset`` so it cannot be mutated at runtime.
    """
    assert isinstance(SUPPORTED_LANGUAGES, frozenset)


def test_count_locks_in_50_union_codes() -> None:
    """Locks the union count to 50.

    Given: The union of frontend (45) and iOS (45) catalog codes
        contains 5 codes that differ in shape (``pt`` vs ``pt-BR``,
        ``no`` vs ``nb``, ``zh`` vs ``zh-Hans``, ``sr`` vs ``sr-Latn``,
        ``my`` vs ``my-MM``).
    When: ``SUPPORTED_LANGUAGES`` cardinality is measured.
    Then: The count is exactly 50 — bump deliberately when a new
        language lands in iOS / frontend.
    """
    assert len(SUPPORTED_LANGUAGES) == 50


def test_includes_frontend_only_code_forms() -> None:
    """Frontend code forms are accepted.

    Given: The frontend's ``CatalogLanguage`` uses ``pt``, ``no``,
        ``zh``, ``sr``, ``my-MM`` while iOS uses the alternative forms.
    When: We check membership of frontend forms.
    Then: All are present — the frontend can store its preference
        without a translation step.
    """
    assert "pt" in SUPPORTED_LANGUAGES
    assert "no" in SUPPORTED_LANGUAGES
    assert "zh" in SUPPORTED_LANGUAGES
    assert "sr" in SUPPORTED_LANGUAGES
    assert "my-MM" in SUPPORTED_LANGUAGES


def test_includes_ios_only_code_forms() -> None:
    """The iOS code forms are accepted.

    Given: The iOS ``CatalogLanguage`` uses ``pt-BR``, ``nb``,
        ``zh-Hans``, ``sr-Latn``, ``my``.
    When: We check membership of iOS forms.
    Then: All are present — the iOS app can store its preference
        directly.
    """
    assert "pt-BR" in SUPPORTED_LANGUAGES
    assert "nb" in SUPPORTED_LANGUAGES
    assert "zh-Hans" in SUPPORTED_LANGUAGES
    assert "sr-Latn" in SUPPORTED_LANGUAGES
    assert "my" in SUPPORTED_LANGUAGES


def test_english_is_present_as_the_source_language() -> None:
    """English is the source-language anchor.

    Given: ``en`` is the source language for every translated catalog key.
    When: We check membership of ``"en"`` in ``SUPPORTED_LANGUAGES``.
    Then: It is present so the backend accepts it as a valid preference.
    """
    assert "en" in SUPPORTED_LANGUAGES


def test_polish_is_present_as_the_long_standing_first_translated_locale() -> None:
    """Spot-check for ``pl``.

    Given: Polish shipped at Phase v1 baseline.
    When: We check membership of ``"pl"``.
    Then: It is present.
    """
    assert "pl" in SUPPORTED_LANGUAGES


def test_irish_is_present_as_the_recent_addition() -> None:
    """Spot-check for ``ga``.

    Given: Irish shipped as part of Batch 9 (Filipino / Burmese /
        Swahili / Irish + zh-Hant).
    When: We check membership of ``"ga"``.
    Then: It is present.
    """
    assert "ga" in SUPPORTED_LANGUAGES


@pytest.mark.parametrize(
    "code",
    [
        "pt-BR",
        "sr-Latn",
        "zh-Hans",
        "zh-Hant",
    ],
)
def test_compound_codes_use_dash_form_matching_ios_catalog(code: str) -> None:
    """Compound codes use the BCP-47 dash form.

    Given: The iOS xcstrings catalog stores compound codes with dashes
        (``pt-BR``, ``sr-Latn``, ``zh-Hans``, ``zh-Hant``).
    When: We check membership of each compound code.
    Then: All are present in dash form — underscore variants (``pt_BR``)
        would silently fail catalog key lookup.
    """
    assert code in SUPPORTED_LANGUAGES


def test_is_supported_language_recognises_known_codes() -> None:
    """The helper accepts known codes.

    Given: ``pl`` and ``ar`` are in ``SUPPORTED_LANGUAGES``.
    When: ``is_supported_language`` is called for each.
    Then: It returns ``True`` for both.
    """
    assert is_supported_language("pl") is True
    assert is_supported_language("ar") is True


def test_is_supported_language_rejects_unknown_codes() -> None:
    """Typo and empty rejection.

    Given: ``"xyz"`` / empty string / language-name ``"english"`` are
        not catalog codes.
    When: ``is_supported_language`` is called for each.
    Then: It returns ``False`` so typos surface as 422 at the request
        boundary instead of falling through to a permissive match.
    """
    assert is_supported_language("xyz") is False
    assert is_supported_language("") is False
    assert is_supported_language("english") is False
