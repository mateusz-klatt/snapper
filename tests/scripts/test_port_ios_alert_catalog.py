"""Tests for ``scripts/port_ios_alert_catalog.py`` — xcstrings → JSON port."""

import json
import shutil
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import scripts.port_ios_alert_catalog as port


class TestRewritePlaceholders:
    """Placeholder rewrite from iOS printf-style to i18next ``{{N}}``."""

    def test_empty_template_returns_empty(self) -> None:
        """An empty template round-trips as empty (no substitution work)."""
        assert port.rewrite_placeholders("") == ""

    def test_no_placeholders_unchanged(self) -> None:
        """A template without ``%@``/``%lld``/``%d`` is preserved verbatim."""
        assert port.rewrite_placeholders("Order filled") == "Order filled"

    def test_single_string_placeholder_becomes_zero(self) -> None:
        """First ``%@`` → ``{{0}}`` (positional zero-based)."""
        assert port.rewrite_placeholders("System degraded: %@") == "System degraded: {{0}}"

    def test_multiple_placeholders_index_in_order(self) -> None:
        """Placeholders are numbered in source order, regardless of type mix."""
        result = port.rewrite_placeholders("%@ %@ %@ @ $%@ filled on %@")
        assert result == "{{0}} {{1}} {{2}} @ ${{3}} filled on {{4}}"

    def test_lld_placeholder_treated_as_positional(self) -> None:
        """``%lld`` (long long int) is also rewritten as ``{{N}}``.

        Wire passes strings either way; i18next interpolates positionally.
        """
        result = port.rewrite_placeholders("%@/%@ reported %@ for %lld consecutive heartbeats")
        assert result == "{{0}}/{{1}} reported {{2}} for {{3}} consecutive heartbeats"

    def test_d_placeholder_treated_as_positional(self) -> None:
        """Bare ``%d`` is also rewritten (defensive coverage).

        Not used in the current alerts catalog but supported so future
        templates that pick ``%d`` over ``%lld`` still port cleanly.
        """
        assert port.rewrite_placeholders("Count: %d items") == "Count: {{0}} items"


class TestMapLocale:
    """iOS xcstrings locale code → frontend ``src/locales/`` dir name."""

    def test_known_remaps(self) -> None:
        """All 5 documented mismatches resolve to the frontend dir name."""
        assert port.map_locale("nb") == "no"
        assert port.map_locale("pt-BR") == "pt"
        assert port.map_locale("zh-Hans") == "zh"
        assert port.map_locale("sr-Latn") == "sr"
        assert port.map_locale("my") == "my-MM"

    def test_pass_through_when_no_remap(self) -> None:
        """Codes not in the remap dict are returned unchanged.

        Covers the majority of the 45 locales (en, pl, de, fr, …).
        """
        assert port.map_locale("en") == "en"
        assert port.map_locale("pl") == "pl"
        assert port.map_locale("zh-Hant") == "zh-Hant"
        assert port.map_locale("my-MM") == "my-MM"

    def test_unknown_locale_passes_through(self) -> None:
        """Sanity: a fabricated code does not crash, just round-trips."""
        assert port.map_locale("xx") == "xx"


class TestNestKeys:
    """Flat dotted-key → nested dict structure for i18next JSON."""

    def test_empty_input_returns_empty_dict(self) -> None:
        """No keys → empty root."""
        assert port.nest_keys({}) == {}

    def test_single_flat_key_round_trips(self) -> None:
        """Single-segment key lives at the root."""
        assert port.nest_keys({"foo": "bar"}) == {"foo": "bar"}

    def test_nested_keys_build_intermediate_dicts(self) -> None:
        """Three-segment key produces two intermediate dicts."""
        result = port.nest_keys({"alerts.title.order_fill_full": "Order filled"})
        assert result == {"alerts": {"title": {"order_fill_full": "Order filled"}}}

    def test_sibling_keys_share_parent(self) -> None:
        """Two keys with the same parent prefix nest under one parent."""
        result = port.nest_keys(
            {
                "alerts.title.x": "Title X",
                "alerts.title.y": "Title Y",
            }
        )
        assert result == {"alerts": {"title": {"x": "Title X", "y": "Title Y"}}}

    def test_leaf_then_nested_collision_raises(self) -> None:
        """``foo`` (leaf string) then ``foo.bar`` (nested) is a collision."""
        with pytest.raises(SystemExit, match="key collision"):
            port.nest_keys({"foo": "leaf", "foo.bar": "nested"})


class TestExtractValue:
    """``extract_value`` pulls one string from the xcstrings tree."""

    def _doc(self, key: str = "alerts.test", lang: str = "en", value: str = "Hello") -> dict:
        """Build a minimal xcstrings doc fixture."""
        return {
            "strings": {
                key: {
                    "localizations": {
                        lang: {"stringUnit": {"value": value}},
                    },
                }
            }
        }

    def test_extracts_string_value(self) -> None:
        """Happy path: key + lang present yields the value string."""
        doc = self._doc(value="Sample")
        assert port.extract_value(doc, "alerts.test", "en") == "Sample"

    def test_missing_key_raises(self) -> None:
        """Unknown key fails fast with a clear diagnostic."""
        doc = self._doc()
        with pytest.raises(SystemExit, match="not a dict"):
            port.extract_value(doc, "alerts.absent", "en")

    def test_missing_locale_raises(self) -> None:
        """Known key but missing locale fails fast."""
        doc = self._doc()
        with pytest.raises(SystemExit, match="missing localization"):
            port.extract_value(doc, "alerts.test", "pl")

    def test_non_string_value_raises(self) -> None:
        """Defensive: a non-string ``value`` field fails fast."""
        doc = {
            "strings": {
                "alerts.test": {
                    "localizations": {"en": {"stringUnit": {"value": 123}}},
                }
            }
        }
        with pytest.raises(SystemExit, match="non-string value"):
            port.extract_value(doc, "alerts.test", "en")


class TestIterAlertKeys:
    """Filter the xcstrings keys to the ``alerts.`` namespace."""

    def test_yields_only_alerts_keys(self) -> None:
        """Non-alerts keys are filtered out; alerts.* keys come through sorted."""
        doc: dict[str, object] = {
            "strings": {
                "alerts.title.x": {},
                "common.button": {},
                "alerts.body.y": {},
                "settings.foo": {},
            }
        }
        assert list(port.iter_alert_keys(doc)) == ["alerts.body.y", "alerts.title.x"]

    def test_empty_strings_dict_yields_nothing(self) -> None:
        """No keys at all → empty iter."""
        assert list(port.iter_alert_keys({"strings": {}})) == []

    def test_strings_not_dict_raises(self) -> None:
        """Corrupt xcstrings root fails fast."""
        with pytest.raises(SystemExit, match="file corrupt"):
            list(port.iter_alert_keys({"strings": "broken"}))


class TestExtractValueExtraGuards:
    """Defensive non-isinstance branches in ``extract_value``."""

    def test_strings_root_not_dict_raises(self) -> None:
        """Xcstrings root with non-dict 'strings' fails fast."""
        with pytest.raises(SystemExit, match="not a dict"):
            port.extract_value({"strings": "broken"}, "alerts.x", "en")

    def test_localizations_not_dict_raises(self) -> None:
        """Xcstrings entry whose localizations is not a dict fails fast."""
        doc = {"strings": {"alerts.x": {"localizations": "broken"}}}
        with pytest.raises(SystemExit, match="has no localizations"):
            port.extract_value(doc, "alerts.x", "en")

    def test_string_unit_not_dict_raises(self) -> None:
        """Xcstrings localization without a stringUnit dict fails fast."""
        doc = {"strings": {"alerts.x": {"localizations": {"en": {"stringUnit": "broken"}}}}}
        with pytest.raises(SystemExit, match="no stringUnit"):
            port.extract_value(doc, "alerts.x", "en")


class TestCollectIosLocalesExtraGuards:
    """Defensive non-isinstance branches in ``collect_ios_locales``."""

    def test_strings_not_dict_raises(self) -> None:
        """Xcstrings root with non-dict 'strings' fails fast."""
        with pytest.raises(SystemExit, match="not a dict"):
            port.collect_ios_locales({"strings": "broken"}, ["alerts.x"])

    def test_first_entry_not_dict_raises(self) -> None:
        """The first alerts key with a non-dict entry value fails fast."""
        with pytest.raises(SystemExit, match="is not a dict"):
            port.collect_ios_locales({"strings": {"alerts.x": "broken"}}, ["alerts.x"])

    def test_first_entry_localizations_not_dict_raises(self) -> None:
        """The first alerts key with a non-dict localizations fails fast."""
        doc = {"strings": {"alerts.x": {"localizations": "broken"}}}
        with pytest.raises(SystemExit, match="has no localizations"):
            port.collect_ios_locales(doc, ["alerts.x"])

    def test_later_entry_not_dict_raises(self) -> None:
        """A later alerts key with a non-dict entry value fails fast."""
        doc = {
            "strings": {
                "alerts.a": {"localizations": {"en": {"stringUnit": {"value": "A"}}}},
                "alerts.b": "broken",
            }
        }
        with pytest.raises(SystemExit, match="alerts.b.* is not a dict"):
            port.collect_ios_locales(doc, ["alerts.a", "alerts.b"])

    def test_later_entry_localizations_not_dict_raises(self) -> None:
        """A later alerts key with a non-dict localizations fails fast."""
        doc = {
            "strings": {
                "alerts.a": {"localizations": {"en": {"stringUnit": {"value": "A"}}}},
                "alerts.b": {"localizations": "broken"},
            }
        }
        with pytest.raises(SystemExit, match="alerts.b.* has no localizations"):
            port.collect_ios_locales(doc, ["alerts.a", "alerts.b"])


class TestCollectIosLocales:
    """Locale set discovery + drift detection across alert keys."""

    def test_returns_sorted_locales(self) -> None:
        """Single-key fixture: returns the localizations sorted."""
        doc = {
            "strings": {
                "alerts.x": {
                    "localizations": {
                        "pl": {"stringUnit": {"value": "X"}},
                        "en": {"stringUnit": {"value": "X"}},
                    }
                }
            }
        }
        assert port.collect_ios_locales(doc, ["alerts.x"]) == ["en", "pl"]

    def test_empty_keys_raises(self) -> None:
        """No keys provided → can't sample, fails fast."""
        with pytest.raises(SystemExit, match="no alerts"):
            port.collect_ios_locales({"strings": {}}, [])

    def test_drift_across_keys_raises(self) -> None:
        """Two keys with different localization sets surface as drift."""
        doc: dict[str, object] = {
            "strings": {
                "alerts.x": {"localizations": {"en": {}, "pl": {}}},
                "alerts.y": {"localizations": {"en": {}, "de": {}}},
            }
        }
        with pytest.raises(SystemExit, match="Language drift"):
            port.collect_ios_locales(doc, ["alerts.x", "alerts.y"])


class TestBuildLocalePayload:
    """End-to-end conversion of one (xcstrings, locale) into nested JSON."""

    def test_nests_strips_alerts_prefix_and_rewrites_placeholders(self) -> None:
        """Frontend ``alerts.json`` IS the alerts namespace already.

        The iOS ``alerts.`` prefix is stripped during nesting; otherwise
        ``useTranslation('alerts')`` + ``t('title.x')`` would miss the
        value (the runtime key would be ``alerts.alerts.title.x``).
        """
        doc = {
            "strings": {
                "alerts.title.t": {"localizations": {"en": {"stringUnit": {"value": "Title"}}}},
                "alerts.body.b": {
                    "localizations": {"en": {"stringUnit": {"value": "Body with %@"}}}
                },
            }
        }
        result = port.build_locale_payload(doc, ["alerts.title.t", "alerts.body.b"], "en")
        assert result == {
            "title": {"t": "Title"},
            "body": {"b": "Body with {{0}}"},
        }

    def test_non_alerts_key_raises(self) -> None:
        """Defensive: an upstream filter bug that lets a non-alerts key.

        through fails fast rather than silently producing a top-level
        non-namespaced entry.
        """
        doc = {
            "strings": {"common.button": {"localizations": {"en": {"stringUnit": {"value": "OK"}}}}}
        }
        with pytest.raises(SystemExit, match="non-alerts key"):
            port.build_locale_payload(doc, ["common.button"], "en")

    def test_bare_alerts_prefix_key_raises(self) -> None:
        """Defensive: a literal ``"alerts."`` key (no segment after the dot).

        Fails fast rather than producing an empty key in the nested dict.
        """
        doc = {"strings": {"alerts.": {"localizations": {"en": {"stringUnit": {"value": "X"}}}}}}
        with pytest.raises(SystemExit, match="unexpected bare"):
            port.build_locale_payload(doc, ["alerts."], "en")


class TestUpsertNavAlerts:
    """``upsert_nav_alerts`` writes ``nav.alerts`` into a locale's.

    ``common.json`` without disturbing other top-level keys.
    """

    def test_creates_nav_when_missing(self, tmp_path: Path) -> None:
        """Locale common.json without a 'nav' map gains one."""
        p = tmp_path / "common.json"
        p.write_text('{"chrome": {"brand": "Snapper"}}\n', encoding="utf-8")
        port.upsert_nav_alerts(p, "Alerts")
        loaded = json.loads(p.read_text(encoding="utf-8"))
        assert loaded["nav"] == {"alerts": "Alerts"}
        assert loaded["chrome"] == {"brand": "Snapper"}

    def test_updates_existing_nav_alphabetically(self, tmp_path: Path) -> None:
        """Existing nav keys are preserved and the file ends alphabetically sorted."""
        p = tmp_path / "common.json"
        p.write_text(
            json.dumps({"nav": {"overview": "Overview", "settings": "Settings"}}) + "\n",
            encoding="utf-8",
        )
        port.upsert_nav_alerts(p, "Alerty")
        loaded = json.loads(p.read_text(encoding="utf-8"))
        assert list(loaded["nav"].keys()) == ["alerts", "overview", "settings"]
        assert loaded["nav"]["alerts"] == "Alerty"

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        """common.json must exist; we never create a new locale dir."""
        with pytest.raises(SystemExit, match="common.json missing"):
            port.upsert_nav_alerts(tmp_path / "nope.json", "Alerts")

    def test_non_object_root_raises(self, tmp_path: Path) -> None:
        """Defensive: JSON array at root fails fast."""
        p = tmp_path / "common.json"
        p.write_text("[]\n", encoding="utf-8")
        with pytest.raises(SystemExit, match="not a JSON object"):
            port.upsert_nav_alerts(p, "Alerts")

    def test_non_dict_nav_raises(self, tmp_path: Path) -> None:
        """Defensive: nav field present but wrong type fails fast."""
        p = tmp_path / "common.json"
        p.write_text('{"nav": "broken"}\n', encoding="utf-8")
        with pytest.raises(SystemExit, match="'nav' is not a dict"):
            port.upsert_nav_alerts(p, "Alerts")


class TestGenerateIntegration:
    """``generate()`` end-to-end against a synthetic xcstrings fixture."""

    def _fixture(self, tmp_path: Path) -> tuple[Path, Path]:
        """Set up a minimal xcstrings + frontend locales dir tree.

        Returns ``(xcstrings_path, frontend_locales_dir)``.
        """
        xcstrings_path = tmp_path / "Localizable.xcstrings"
        locales_dir = tmp_path / "locales"
        locales_dir.mkdir()
        for loc in ("en", "pl", "no"):
            d = locales_dir / loc
            d.mkdir()
            (d / "common.json").write_text('{"nav": {}}\n', encoding="utf-8")
        xcstrings_payload = {
            "strings": {
                "alerts.title.x": {
                    "localizations": {
                        "en": {"stringUnit": {"value": "Title X"}},
                        "pl": {"stringUnit": {"value": "Tytuł X"}},
                        "nb": {"stringUnit": {"value": "Tittel X"}},
                    }
                },
                "alerts.navTitle": {
                    "localizations": {
                        "en": {"stringUnit": {"value": "Alerts"}},
                        "pl": {"stringUnit": {"value": "Alerty"}},
                        "nb": {"stringUnit": {"value": "Varsler"}},
                    }
                },
            }
        }
        xcstrings_path.write_text(json.dumps(xcstrings_payload), encoding="utf-8")
        return xcstrings_path, locales_dir

    def test_generate_writes_alerts_and_updates_common(self, tmp_path: Path) -> None:
        """Happy path: 3 locales, 1 alert key + navTitle yields 3 alerts.json.

        files + 3 updated common.json files. Drift remap (``nb`` → ``no``)
        is honored.
        """
        xcstrings_path, locales_dir = self._fixture(tmp_path)
        with patch.object(port, "XCSTRINGS_PATH", xcstrings_path):
            port.generate(locales_dir=locales_dir)
        assert (locales_dir / "en" / "alerts.json").exists()
        assert (locales_dir / "pl" / "alerts.json").exists()
        assert (locales_dir / "no" / "alerts.json").exists()
        pl_payload = json.loads((locales_dir / "pl" / "alerts.json").read_text(encoding="utf-8"))
        assert pl_payload == {"title": {"x": "Tytuł X"}, "navTitle": "Alerty"}
        no_common = json.loads((locales_dir / "no" / "common.json").read_text(encoding="utf-8"))
        assert no_common["nav"]["alerts"] == "Varsler"

    def test_generate_idempotent(self, tmp_path: Path) -> None:
        """Re-running the generator produces byte-identical output."""
        xcstrings_path, locales_dir = self._fixture(tmp_path)
        with patch.object(port, "XCSTRINGS_PATH", xcstrings_path):
            port.generate(locales_dir=locales_dir)
            first = (locales_dir / "pl" / "alerts.json").read_text(encoding="utf-8")
            first_common = (locales_dir / "pl" / "common.json").read_text(encoding="utf-8")
            port.generate(locales_dir=locales_dir)
            second = (locales_dir / "pl" / "alerts.json").read_text(encoding="utf-8")
            second_common = (locales_dir / "pl" / "common.json").read_text(encoding="utf-8")
        assert first == second
        assert first_common == second_common

    def test_generate_missing_xcstrings_raises(self, tmp_path: Path) -> None:
        """Source file absent → SystemExit with the path mentioned."""
        with (
            patch.object(port, "XCSTRINGS_PATH", tmp_path / "nope.xcstrings"),
            pytest.raises(SystemExit, match="not found"),
        ):
            port.generate()

    def test_generate_missing_navtitle_raises(self, tmp_path: Path) -> None:
        """If the source xcstrings ever drops ``alerts.navTitle``, fail.

        fast — the sidebar tab label would otherwise silently lose its
        translation.
        """
        xcstrings_path = tmp_path / "Localizable.xcstrings"
        locales_dir = tmp_path / "locales"
        locales_dir.mkdir()
        (locales_dir / "en").mkdir()
        (locales_dir / "en" / "common.json").write_text('{"nav": {}}\n', encoding="utf-8")
        xcstrings_path.write_text(
            json.dumps(
                {
                    "strings": {
                        "alerts.title.x": {"localizations": {"en": {"stringUnit": {"value": "X"}}}}
                    }
                }
            ),
            encoding="utf-8",
        )
        with (
            patch.object(port, "XCSTRINGS_PATH", xcstrings_path),
            pytest.raises(SystemExit, match="missing the.*navTitle"),
        ):
            port.generate(locales_dir=locales_dir)

    def test_generate_missing_frontend_locale_dir_raises(self, tmp_path: Path) -> None:
        """If a locale exists in iOS but its frontend dir is missing,.

        fail fast with both names — keeps the locale set explicit.
        """
        xcstrings_path, locales_dir = self._fixture(tmp_path)
        shutil.rmtree(locales_dir / "no")
        with patch.object(port, "XCSTRINGS_PATH", xcstrings_path), patch.object(
            port, "FRONTEND_LOCALES_DIR", locales_dir
        ), pytest.raises(SystemExit, match="frontend locale dir missing"):
            port.generate()


class TestGenerateExtraGuards:
    """Defensive raises in ``generate()`` that don't fire through happy paths."""

    def test_xcstrings_not_a_json_object_raises(self, tmp_path: Path) -> None:
        """Xcstrings file that parses to a non-dict (e.g. an array) fails fast."""
        xcstrings_path = tmp_path / "Localizable.xcstrings"
        xcstrings_path.write_text("[]", encoding="utf-8")
        with (
            patch.object(port, "XCSTRINGS_PATH", xcstrings_path),
            pytest.raises(SystemExit, match="did not parse"),
        ):
            port.generate(locales_dir=tmp_path)

    def test_xcstrings_without_alerts_keys_raises(self, tmp_path: Path) -> None:
        """A valid xcstrings with no alerts.* keys is a no-op AND a hard fail."""
        xcstrings_path = tmp_path / "Localizable.xcstrings"
        xcstrings_path.write_text(json.dumps({"strings": {}}), encoding="utf-8")
        with (
            patch.object(port, "XCSTRINGS_PATH", xcstrings_path),
            pytest.raises(SystemExit, match="no alerts"),
        ):
            port.generate(locales_dir=tmp_path)

    def test_target_root_not_exists_raises(self, tmp_path: Path) -> None:
        """The target locales dir must exist; we never auto-create it."""
        xcstrings_path = tmp_path / "Localizable.xcstrings"
        xcstrings_path.write_text(
            json.dumps(
                {
                    "strings": {
                        "alerts.title.x": {
                            "localizations": {"en": {"stringUnit": {"value": "Title"}}}
                        },
                        "alerts.navTitle": {
                            "localizations": {"en": {"stringUnit": {"value": "Alerts"}}}
                        },
                    }
                }
            ),
            encoding="utf-8",
        )
        with (
            patch.object(port, "XCSTRINGS_PATH", xcstrings_path),
            pytest.raises(SystemExit, match="not found"),
        ):
            port.generate(locales_dir=tmp_path / "missing")


class TestCheckDrift:
    """``check_drift()`` — drift mode used by ``make ui-i18n-check-alerts``."""

    def _bootstrap_committed(self, tmp_path: Path) -> tuple[Path, Path]:
        """Build a tmp locales tree with the script's own output as committed.

        Returns ``(xcstrings_path, locales_dir)``.
        """
        xcstrings_path = tmp_path / "Localizable.xcstrings"
        locales_dir = tmp_path / "locales"
        locales_dir.mkdir()
        (locales_dir / "en").mkdir()
        (locales_dir / "en" / "common.json").write_text('{"nav": {}}\n', encoding="utf-8")
        xcstrings_path.write_text(
            json.dumps(
                {
                    "strings": {
                        "alerts.title.x": {
                            "localizations": {"en": {"stringUnit": {"value": "Title X"}}}
                        },
                        "alerts.navTitle": {
                            "localizations": {"en": {"stringUnit": {"value": "Alerts"}}}
                        },
                    }
                }
            ),
            encoding="utf-8",
        )
        with patch.object(port, "XCSTRINGS_PATH", xcstrings_path):
            port.generate(locales_dir=locales_dir)
        return xcstrings_path, locales_dir

    def test_returns_zero_when_committed_matches_regenerated(self, tmp_path: Path) -> None:
        """Happy path: committed files equal what the script would emit."""
        xcstrings_path, locales_dir = self._bootstrap_committed(tmp_path)
        with patch.object(port, "XCSTRINGS_PATH", xcstrings_path), patch.object(
            port, "FRONTEND_LOCALES_DIR", locales_dir
        ):
            assert port.check_drift() == 0

    def test_returns_one_when_alerts_json_is_stale(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """An out-of-date committed ``alerts.json`` triggers drift."""
        xcstrings_path, locales_dir = self._bootstrap_committed(tmp_path)
        stale = locales_dir / "en" / "alerts.json"
        stale.write_text('{"title": {"x": "STALE"}, "navTitle": "Alerts"}\n', encoding="utf-8")
        with patch.object(port, "XCSTRINGS_PATH", xcstrings_path), patch.object(
            port, "FRONTEND_LOCALES_DIR", locales_dir
        ):
            assert port.check_drift() == 1
        err = capsys.readouterr().err
        assert "DIFFERS" in err and "en/alerts.json" in err

    def test_returns_one_when_alerts_json_missing(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A locale dir without ``alerts.json`` is treated as missing.

        (must be regenerated).
        """
        xcstrings_path, locales_dir = self._bootstrap_committed(tmp_path)
        (locales_dir / "en" / "alerts.json").unlink()
        with patch.object(port, "XCSTRINGS_PATH", xcstrings_path), patch.object(
            port, "FRONTEND_LOCALES_DIR", locales_dir
        ):
            assert port.check_drift() == 1
        err = capsys.readouterr().err
        assert "MISSING" in err

    def test_returns_one_when_nav_alerts_changed(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A locale's ``common.json`` with a stale ``nav.alerts`` value.

        is flagged even when ``alerts.json`` is up-to-date.
        """
        xcstrings_path, locales_dir = self._bootstrap_committed(tmp_path)
        common = locales_dir / "en" / "common.json"
        committed = json.loads(common.read_text(encoding="utf-8"))
        committed["nav"]["alerts"] = "STALE_LABEL"
        common.write_text(json.dumps(committed) + "\n", encoding="utf-8")
        with patch.object(port, "XCSTRINGS_PATH", xcstrings_path), patch.object(
            port, "FRONTEND_LOCALES_DIR", locales_dir
        ):
            assert port.check_drift() == 1
        err = capsys.readouterr().err
        assert "nav.alerts" in err

    def test_returns_one_when_locales_dir_missing(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Fast-fail when the target locales dir does not exist."""
        with patch.object(port, "FRONTEND_LOCALES_DIR", tmp_path / "nope"):
            assert port.check_drift() == 1
        err = capsys.readouterr().err
        assert "not found" in err

    def test_check_drift_skips_non_directory_entries(self, tmp_path: Path) -> None:
        """Files at the top of the locales tree (e.g. ``.gitkeep``) are.

        ignored — only locale subdirectories are walked.
        """
        xcstrings_path, locales_dir = self._bootstrap_committed(tmp_path)
        (locales_dir / "stray.txt").write_text("not a locale", encoding="utf-8")
        with (
            patch.object(port, "XCSTRINGS_PATH", xcstrings_path),
            patch.object(port, "FRONTEND_LOCALES_DIR", locales_dir),
        ):
            assert port.check_drift() == 0

    def test_check_nav_alerts_drift_handles_missing_regenerated_side(self, tmp_path: Path) -> None:
        """``_check_nav_alerts_drift`` flags MISSING when either side is empty.

        Covers the ``actual == ""`` half of the early-return guard (the
        committed-side empty case is already covered by another test).
        """
        committed = tmp_path / "committed.json"
        committed.write_text('{"nav": {"alerts": "X"}}\n', encoding="utf-8")
        regenerated = tmp_path / "absent.json"
        diffs = port._check_nav_alerts_drift(committed, regenerated, "test/common.json")

        assert diffs == ["  MISSING: test/common.json"]

    def test_returns_one_when_committed_lacks_nav_alerts(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A locale's ``common.json`` that never had ``nav.alerts``.

        inserted is flagged — covers the case where the port script
        has never been run against a fresh locale dir.
        """
        xcstrings_path, locales_dir = self._bootstrap_committed(tmp_path)
        common = locales_dir / "en" / "common.json"
        committed = json.loads(common.read_text(encoding="utf-8"))
        del committed["nav"]["alerts"]
        common.write_text(json.dumps(committed) + "\n", encoding="utf-8")
        with patch.object(port, "XCSTRINGS_PATH", xcstrings_path), patch.object(
            port, "FRONTEND_LOCALES_DIR", locales_dir
        ):
            assert port.check_drift() == 1
        err = capsys.readouterr().err
        assert "MISSING nav.alerts in committed" in err


class TestMain:
    """``main()`` entry-point: returns 0 on success, raises on failure."""

    def test_main_returns_zero(self) -> None:
        """Module-level ``main()`` returns 0 when generate succeeds.

        Patches argv to the bare script invocation so argparse doesn't
        try to consume pytest's own flags.
        """
        with patch.object(port, "generate") as gen, patch.object(
            sys, "argv", ["port_ios_alert_catalog.py"]
        ):
            gen.return_value = None
            assert port.main() == 0

    def test_main_check_flag_invokes_drift(self) -> None:
        """``--check`` routes to ``check_drift`` and returns its int."""
        with patch.object(port, "check_drift") as drift, patch.object(
            sys, "argv", ["port_ios_alert_catalog.py", "--check"]
        ):
            drift.return_value = 0
            assert port.main() == 0

    def test_main_check_flag_propagates_drift_failure(self) -> None:
        """Drift detected → main() returns 1 (CI fail signal)."""
        with patch.object(port, "check_drift") as drift, patch.object(
            sys, "argv", ["port_ios_alert_catalog.py", "--check"]
        ):
            drift.return_value = 1
            assert port.main() == 1


class TestMergePreservingFrontendKeys:
    """``_merge_preserving_frontend_keys`` carries forward frontend-only keys.

    The merger reads the existing committed ``alerts.json`` and folds in any
    top-level keys that the iOS payload doesn't own (e.g. the ``page``
    subtree added for the Phase E web Alerts header). iOS-owned keys
    always win, so the catalog never drifts.
    """

    def test_merges_existing_page_key_into_ios_payload(self, tmp_path: Path) -> None:
        """Existing ``page`` subtree is preserved across regeneration.

        Given: An existing ``alerts.json`` with a frontend-only ``page``
            subtree alongside iOS-owned keys,
        When: ``_merge_preserving_frontend_keys`` is invoked with a fresh
            iOS payload,
        Then: The returned dict contains both the iOS payload's keys AND
            the ``page`` subtree from the existing file.
        """
        alerts_path = tmp_path / "alerts.json"
        alerts_path.write_text(
            json.dumps(
                {
                    "title": {"order_fill_full": "Old title"},
                    "page": {"subtitle": "Frontend-only subtitle"},
                }
            ),
            encoding="utf-8",
        )
        ios_payload: dict[str, object] = {
            "title": {"order_fill_full": "Fresh title from iOS"},
            "body": {"order_fill_full": "Fresh body"},
        }
        merged = port._merge_preserving_frontend_keys(alerts_path, ios_payload)
        assert merged["page"] == {"subtitle": "Frontend-only subtitle"}
        assert merged["title"] == {"order_fill_full": "Fresh title from iOS"}
        assert merged["body"] == {"order_fill_full": "Fresh body"}

    def test_handles_missing_alerts_file_gracefully(self, tmp_path: Path) -> None:
        """First-time regeneration (no committed file yet) returns the iOS payload as-is.

        Given: A path that does not exist (fresh locale directory),
        When: ``_merge_preserving_frontend_keys`` is invoked,
        Then: Returns a dict equal to the iOS payload, with no crash.
        """
        alerts_path = tmp_path / "missing.json"
        ios_payload: dict[str, object] = {"title": {"x": "y"}}
        merged = port._merge_preserving_frontend_keys(alerts_path, ios_payload)
        assert merged == {"title": {"x": "y"}}

    def test_handles_non_dict_existing_file_gracefully(self, tmp_path: Path) -> None:
        """A non-dict ``alerts.json`` (corrupt file) is treated as empty.

        Given: An existing ``alerts.json`` containing a JSON array rather
            than an object (corrupt / hand-edited),
        When: ``_merge_preserving_frontend_keys`` is invoked,
        Then: The corrupt content is discarded and the iOS payload returned
            untouched, so the next write fixes the file shape.
        """
        alerts_path = tmp_path / "alerts.json"
        alerts_path.write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")
        ios_payload: dict[str, object] = {"title": {"x": "y"}}
        merged = port._merge_preserving_frontend_keys(alerts_path, ios_payload)
        assert merged == {"title": {"x": "y"}}
