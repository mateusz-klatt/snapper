"""Tests for ``scripts/gen_backend_i18n_catalog.py``.

The generator parses ``ios/Snapper/Resources/Localization/Localizable.xcstrings``
and writes one JSON per catalog language to
``src/snapper/i18n/catalogs/``. These tests exercise the pure parsing
helpers + the end-to-end ``generate()`` flow against crafted fixtures
(no real xcstrings I/O).
"""

import json
from pathlib import Path

import pytest

from scripts.gen_backend_i18n_catalog import collect_languages
from scripts.gen_backend_i18n_catalog import extract_value
from scripts.gen_backend_i18n_catalog import generate
from scripts.gen_backend_i18n_catalog import iter_catalog_keys
from scripts.gen_backend_i18n_catalog import main


def _make_xcstrings(strings: dict[str, dict[str, str]]) -> dict[str, object]:
    """Build a minimal xcstrings doc from a ``{key: {lang: value}}`` map."""
    return {
        "sourceLanguage": "en",
        "strings": {
            key: {
                "localizations": {
                    lang: {"stringUnit": {"state": "translated", "value": value}}
                    for lang, value in langs.items()
                }
            }
            for key, langs in strings.items()
        },
    }


class TestIterCatalogKeys:
    """Test suite for the ``alerts.*``-namespace key filter."""

    def test_returns_only_alerts_keys(self) -> None:
        """Filter strips unrelated namespaces.

        Given: An xcstrings doc with mixed namespaces.
        When: ``iter_catalog_keys`` is called.
        Then: Only ``alerts.title.*`` and ``alerts.body.*`` keys come back.
        """
        xcs = _make_xcstrings(
            {
                "alerts.title.x": {"en": "X"},
                "alerts.body.y": {"en": "Y"},
                "auth.login.subtitle": {"en": "ignored"},
                "settings.section.account": {"en": "ignored"},
            }
        )
        assert list(iter_catalog_keys(xcs)) == ["alerts.body.y", "alerts.title.x"]

    def test_raises_when_strings_is_not_a_dict(self) -> None:
        """Defensive: malformed xcstrings file is rejected.

        Given: A doc whose ``strings`` field is a list (corrupt).
        When: ``iter_catalog_keys`` is called.
        Then: ``SystemExit`` is raised so the generator fails loudly.
        """
        with pytest.raises(SystemExit, match="not a dict"):
            list(iter_catalog_keys({"strings": []}))


class TestCollectLanguages:
    """Test suite for the language-set discovery + cross-key consistency check."""

    def test_returns_sorted_language_set(self) -> None:
        """Returns sorted list of languages.

        Given: Two keys with identical language coverage.
        When: ``collect_languages`` is called.
        Then: The sorted union is returned.
        """
        xcs = _make_xcstrings(
            {
                "alerts.title.x": {"en": "X", "pl": "X-PL"},
                "alerts.body.y": {"en": "Y", "pl": "Y-PL"},
            }
        )
        keys = list(iter_catalog_keys(xcs))
        assert collect_languages(xcs, keys) == ["en", "pl"]

    def test_raises_on_language_drift_between_keys(self) -> None:
        """Cross-key language coverage must match.

        Given: Two keys where one has ``en`` only and the other has
            ``en`` + ``pl``.
        When: ``collect_languages`` is called.
        Then: ``SystemExit`` flags the drift.
        """
        xcs = _make_xcstrings(
            {
                "alerts.title.x": {"en": "X", "pl": "X-PL"},
                "alerts.body.y": {"en": "Y"},
            }
        )
        keys = list(iter_catalog_keys(xcs))
        with pytest.raises(SystemExit, match="Language drift"):
            collect_languages(xcs, keys)

    def test_raises_when_keys_empty(self) -> None:
        """Empty key list is invalid.

        Given: A valid xcstrings doc but no keys to inspect.
        When: ``collect_languages`` is called with an empty list.
        Then: ``SystemExit`` surfaces — generator has nothing to do.
        """
        xcs = _make_xcstrings({})
        with pytest.raises(SystemExit, match="empty or has no alerts"):
            collect_languages(xcs, [])

    def test_raises_when_first_entry_not_dict(self) -> None:
        """Defensive guard for malformed first entry.

        Given: An xcstrings doc whose first ``alerts.*`` entry is a
            scalar instead of a dict.
        When: ``collect_languages`` is called.
        Then: ``SystemExit`` flags the corruption.
        """
        xcs: dict[str, object] = {"strings": {"alerts.title.x": "scalar-not-dict"}}
        with pytest.raises(SystemExit, match="not a dict"):
            collect_languages(xcs, ["alerts.title.x"])

    def test_raises_when_first_entry_lacks_localizations(self) -> None:
        """Defensive guard for entry whose localizations is not a dict.

        Given: An entry whose ``localizations`` field is a scalar.
        When: ``collect_languages`` is called.
        Then: ``SystemExit`` flags the type mismatch.
        """
        xcs: dict[str, object] = {"strings": {"alerts.title.x": {"localizations": "scalar"}}}
        with pytest.raises(SystemExit, match="no localizations"):
            collect_languages(xcs, ["alerts.title.x"])

    def test_raises_when_subsequent_entry_not_dict(self) -> None:
        """Cross-key drift: later entry not a dict.

        Given: A doc where the FIRST entry is well-formed but a LATER
            ``alerts.*`` entry is a scalar.
        When: ``collect_languages`` is called over both keys.
        Then: ``SystemExit`` raises on the broken entry.
        """
        xcs: dict[str, object] = {
            "strings": {
                "alerts.body.x": {
                    "localizations": {"en": {"stringUnit": {"state": "translated", "value": "ok"}}}
                },
                "alerts.title.y": "scalar-not-dict",
            }
        }
        with pytest.raises(SystemExit, match="not a dict"):
            collect_languages(xcs, ["alerts.body.x", "alerts.title.y"])

    def test_raises_when_subsequent_entry_lacks_localizations(self) -> None:
        """Cross-key drift: later entry has malformed localizations.

        Given: A doc where a LATER entry's ``localizations`` field is
            a scalar instead of a dict.
        When: ``collect_languages`` is called over both keys.
        Then: ``SystemExit`` raises on the broken entry.
        """
        xcs: dict[str, object] = {
            "strings": {
                "alerts.body.x": {
                    "localizations": {"en": {"stringUnit": {"state": "translated", "value": "ok"}}}
                },
                "alerts.title.y": {"localizations": "scalar"},
            }
        }
        with pytest.raises(SystemExit, match="no localizations"):
            collect_languages(xcs, ["alerts.body.x", "alerts.title.y"])


class TestExtractValue:
    """Test suite for the per-(key, lang) value extraction."""

    def test_returns_string_value(self) -> None:
        """Happy path: returns the localized string.

        Given: A well-formed xcstrings entry.
        When: ``extract_value`` is called for an existing language.
        Then: The string is returned.
        """
        xcs = _make_xcstrings({"alerts.title.x": {"en": "Hello", "pl": "Cześć"}})
        assert extract_value(xcs, "alerts.title.x", "pl") == "Cześć"

    def test_raises_when_language_missing(self) -> None:
        """Missing localization is fatal.

        Given: A key that exists but lacks the requested language.
        When: ``extract_value`` is called.
        Then: ``SystemExit`` raised — generator cannot fabricate a value.
        """
        xcs = _make_xcstrings({"alerts.title.x": {"en": "Hello"}})
        with pytest.raises(SystemExit, match="missing localization"):
            extract_value(xcs, "alerts.title.x", "pl")

    def test_raises_when_strings_top_level_not_dict(self) -> None:
        """Defensive: top-level ``strings`` field must be a dict.

        Given: A doc whose ``strings`` is a scalar.
        When: ``extract_value`` is called.
        Then: ``SystemExit`` flags the structural error.
        """
        xcs: dict[str, object] = {"strings": "scalar"}
        with pytest.raises(SystemExit, match="not a dict"):
            extract_value(xcs, "alerts.title.x", "en")

    def test_raises_when_entry_not_dict(self) -> None:
        """Entry must be a dict.

        Given: A doc where a key's entry is a scalar.
        When: ``extract_value`` is called.
        Then: ``SystemExit`` flags the structural error.
        """
        xcs: dict[str, object] = {"strings": {"alerts.title.x": "scalar"}}
        with pytest.raises(SystemExit, match="not a dict"):
            extract_value(xcs, "alerts.title.x", "en")

    def test_raises_when_entry_lacks_localizations(self) -> None:
        """Entry must have a dict localizations field.

        Given: A doc where an entry's ``localizations`` is a scalar.
        When: ``extract_value`` is called.
        Then: ``SystemExit`` flags the malformed field.
        """
        xcs: dict[str, object] = {"strings": {"alerts.title.x": {"localizations": "scalar"}}}
        with pytest.raises(SystemExit, match="no localizations"):
            extract_value(xcs, "alerts.title.x", "en")

    def test_raises_when_string_unit_missing(self) -> None:
        """The stringUnit field must be present.

        Given: A doc where a localization entry's stringUnit is a scalar.
        When: ``extract_value`` is called.
        Then: ``SystemExit`` flags the malformed field.
        """
        xcs: dict[str, object] = {
            "strings": {"alerts.title.x": {"localizations": {"en": {"stringUnit": "scalar"}}}}
        }
        with pytest.raises(SystemExit, match="no stringUnit"):
            extract_value(xcs, "alerts.title.x", "en")

    def test_raises_when_value_is_not_string(self) -> None:
        """Value must be a string.

        Given: A doc where ``stringUnit.value`` is an integer.
        When: ``extract_value`` is called.
        Then: ``SystemExit`` flags the type mismatch.
        """
        xcs: dict[str, object] = {
            "strings": {"alerts.title.x": {"localizations": {"en": {"stringUnit": {"value": 42}}}}}
        }
        with pytest.raises(SystemExit, match="non-string value"):
            extract_value(xcs, "alerts.title.x", "en")


class TestGenerate:
    """Test suite for the end-to-end ``generate()`` flow."""

    def test_writes_one_json_per_language(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Writes a JSON file per language.

        Given: A small xcstrings fixture with 1 key × 2 languages.
        When: ``generate()`` is called (with patched paths).
        Then: Two JSON files appear in the output dir, each containing
            the single key.
        """
        xcs = _make_xcstrings({"alerts.title.x": {"en": "Hello", "pl": "Cześć"}})
        xcs_path = tmp_path / "Localizable.xcstrings"
        xcs_path.write_text(json.dumps(xcs), encoding="utf-8")
        out_dir = tmp_path / "catalogs"

        monkeypatch.setattr("scripts.gen_backend_i18n_catalog.XCSTRINGS_PATH", xcs_path)
        monkeypatch.setattr("scripts.gen_backend_i18n_catalog.CATALOG_DIR", out_dir)

        generate()

        en_data = json.loads((out_dir / "en.json").read_text(encoding="utf-8"))
        pl_data = json.loads((out_dir / "pl.json").read_text(encoding="utf-8"))
        assert en_data == {"alerts.title.x": "Hello"}
        assert pl_data == {"alerts.title.x": "Cześć"}
        assert "Wrote 2 catalog files" in capsys.readouterr().out

    def test_raises_when_xcstrings_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing source file is fatal.

        Given: ``XCSTRINGS_PATH`` points at a non-existent location.
        When: ``generate()`` is called.
        Then: ``SystemExit`` is raised with the source path.
        """
        missing = tmp_path / "missing.xcstrings"
        monkeypatch.setattr("scripts.gen_backend_i18n_catalog.XCSTRINGS_PATH", missing)
        with pytest.raises(SystemExit, match="xcstrings not found"):
            generate()

    def test_raises_when_xcstrings_is_not_an_object(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Corrupt source (top-level array) is fatal.

        Given: An xcstrings file containing a JSON array.
        When: ``generate()`` is called.
        Then: ``SystemExit`` flags the structural error.
        """
        xcs_path = tmp_path / "Localizable.xcstrings"
        xcs_path.write_text("[]", encoding="utf-8")
        monkeypatch.setattr("scripts.gen_backend_i18n_catalog.XCSTRINGS_PATH", xcs_path)
        with pytest.raises(SystemExit, match="did not parse as a JSON object"):
            generate()

    def test_raises_when_no_alerts_keys(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Source without alerts.* keys is rejected.

        Given: A valid xcstrings file with only non-alerts namespaces.
        When: ``generate()`` is called.
        Then: ``SystemExit`` flags the empty-key-set.
        """
        xcs = _make_xcstrings({"auth.login.subtitle": {"en": "ignored"}})
        xcs_path = tmp_path / "Localizable.xcstrings"
        xcs_path.write_text(json.dumps(xcs), encoding="utf-8")
        monkeypatch.setattr("scripts.gen_backend_i18n_catalog.XCSTRINGS_PATH", xcs_path)
        with pytest.raises(SystemExit, match="no keys matching"):
            generate()


class TestMain:
    """Test suite for the ``__main__`` entry point."""

    def test_returns_zero_on_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """``main()`` returns 0 on a successful run.

        Given: A valid xcstrings fixture pointing at writeable dirs.
        When: ``main()`` is called.
        Then: It returns 0 (no exit code) so the shell sees success.
        """
        xcs = _make_xcstrings({"alerts.title.x": {"en": "Hello"}})
        xcs_path = tmp_path / "Localizable.xcstrings"
        xcs_path.write_text(json.dumps(xcs), encoding="utf-8")
        out_dir = tmp_path / "catalogs"
        monkeypatch.setattr("scripts.gen_backend_i18n_catalog.XCSTRINGS_PATH", xcs_path)
        monkeypatch.setattr("scripts.gen_backend_i18n_catalog.CATALOG_DIR", out_dir)

        assert main() == 0
