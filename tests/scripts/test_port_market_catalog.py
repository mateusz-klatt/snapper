"""Tests for ``scripts/port_market_catalog.py`` — frontend JSON → xcstrings port."""

import json
import sys
from pathlib import Path

import pytest

import scripts.port_market_catalog as port


class TestIsPhaseKey:
    """Prefix predicate accepts keys under the ported market namespaces."""

    def test_namespace_root_match(self) -> None:
        """A bare namespace key (no leaf) is considered ported.

        The walker descends into nested dicts, so this branch is only
        exercised on leaf strings; still need coverage for the
        ``key == bare`` comparison branch.
        """
        assert port._is_phase_key("description") is True
        assert port._is_phase_key("assetClass") is True
        assert port._is_phase_key("sector") is True
        assert port._is_phase_key("related") is True
        assert port._is_phase_key("pairStats") is True
        assert port._is_phase_key("cacheBanner") is True

    def test_leaf_under_phase_prefix(self) -> None:
        """Standard leaf paths under each ported prefix match."""
        assert port._is_phase_key("description.label") is True
        assert port._is_phase_key("description.fallback") is True
        assert port._is_phase_key("assetClass.crypto") is True
        assert port._is_phase_key("sector.precious-metals") is True
        assert port._is_phase_key("related.labelSeparator") is True
        assert port._is_phase_key("related.relationshipType.derivative") is True
        assert port._is_phase_key("pairStats.label") is True
        assert port._is_phase_key("pairStats.chipAriaLabel") is True
        assert port._is_phase_key("cacheBanner.message") is True
        assert port._is_phase_key("cacheBanner.sources.cache") is True

    def test_outside_phase_prefix_rejected(self) -> None:
        """Keys outside the ported market namespaces are filtered out."""
        assert port._is_phase_key("page.title") is False
        assert port._is_phase_key("controls.exchange") is False
        assert port._is_phase_key("chart.title") is False
        assert port._is_phase_key("stats.currentPrice") is False
        assert port._is_phase_key("") is False


class TestWalkAndFlatten:
    """Recursive descent + leaf collection under ported prefixes."""

    def test_flatten_typical_market_payload(self) -> None:
        """A realistic ``market.json`` shape flattens to filtered dotted leaves."""
        payload = {
            "page": {"title": "Market Data"},
            "description": {"label": "L", "fallback": "F"},
            "assetClass": {"crypto": "C"},
            "sector": {"agriculture": "A"},
            "controls": {"exchange": "skip"},
        }
        flat = port._flatten_market_payload(payload)
        assert flat == {
            "description.label": "L",
            "description.fallback": "F",
            "assetClass.crypto": "C",
            "sector.agriculture": "A",
        }

    def test_flatten_empty_payload_returns_empty(self) -> None:
        """Empty input maps to empty output."""
        assert port._flatten_market_payload({}) == {}

    def test_flatten_skips_non_string_leaves(self) -> None:
        """Non-string leaves under ported prefixes are silently dropped."""
        payload = {
            "description": {"label": 42, "fallback": "F"},
            "assetClass": {"crypto": None},
        }
        flat = port._flatten_market_payload(payload)
        assert flat == {"description.fallback": "F"}

    def test_walk_handles_deeply_nested(self) -> None:
        """A nested namespace inside a ported prefix is descended into."""
        payload = {
            "sector": {
                "extra": {"deeply-nested": "X"},
            }
        }
        flat = port._flatten_market_payload(payload)
        assert flat == {"sector.extra.deeply-nested": "X"}

    def test_flatten_related_namespace(self) -> None:
        """The ``related.*`` keys (incl. nested relationshipType) flatten."""
        payload = {
            "related": {
                "labelSeparator": "{{label}}:",
                "exchangeSeparator": "· {{exchange}}",
                "empty": "No related instruments configured for {{instrument}} on {{exchange}}.",
                "relationshipType": {
                    "exact": "Same underlying",
                    "derivative": "Derivatives",
                    "proxy": "Proxies",
                },
            },
            "page": {"title": "skip"},
        }
        flat = port._flatten_market_payload(payload)
        assert flat == {
            "related.labelSeparator": "{{label}}:",
            "related.exchangeSeparator": "· {{exchange}}",
            "related.empty": (
                "No related instruments configured for {{instrument}} on {{exchange}}."
            ),
            "related.relationshipType.exact": "Same underlying",
            "related.relationshipType.derivative": "Derivatives",
            "related.relationshipType.proxy": "Proxies",
        }

    def test_flatten_phase3_namespaces_with_empty_source_preserved(self) -> None:
        """The pairStats/cacheBanner namespaces flatten incl. empty-string source leaf.

        The ``cacheBanner.sources.cache`` leaf carries the empty string
        in the EN catalog — must round-trip without being filtered out.
        """
        payload = {
            "pairStats": {
                "label": "Cointegration:",
                "metric": "ρ {{pearson}} · p {{pvalue}}",
                "chipAriaLabel": "Pair stats with {{symbol}} on {{exchange}}: Pearson {{pearson}}, p-value {{pvalue}}",
            },
            "cacheBanner": {
                "message": "Cache warming up: {{sampleCount}} / {{expected}} candles available {{sourceLabel}}",
                "sources": {
                    "cache": "",
                    "derived": "(derived from 1m)",
                    "db": "(from DB)",
                },
            },
        }
        flat = port._flatten_market_payload(payload)
        assert flat["pairStats.label"] == "Cointegration:"
        assert flat["pairStats.metric"] == "ρ {{pearson}} · p {{pvalue}}"
        assert flat["cacheBanner.message"] == (
            "Cache warming up: {{sampleCount}} / {{expected}} candles available {{sourceLabel}}"
        )
        assert flat["cacheBanner.sources.cache"] == ""
        assert flat["cacheBanner.sources.derived"] == "(derived from 1m)"
        assert flat["cacheBanner.sources.db"] == "(from DB)"


class TestMapLocale:
    """Frontend dir name → iOS xcstrings locale code."""

    def test_known_remaps(self) -> None:
        """All 5 documented mismatches resolve to the iOS code."""
        assert port._map_locale("no") == "nb"
        assert port._map_locale("pt") == "pt-BR"
        assert port._map_locale("zh") == "zh-Hans"
        assert port._map_locale("sr") == "sr-Latn"
        assert port._map_locale("my-MM") == "my"

    def test_pass_through_when_no_remap(self) -> None:
        """Direct-match codes are returned unchanged."""
        assert port._map_locale("en") == "en"
        assert port._map_locale("pl") == "pl"
        assert port._map_locale("zh-Hant") == "zh-Hant"


class TestPlaceholderOrder:
    """``_placeholder_order_from_template`` records appearance order."""

    def test_empty_template_no_placeholders(self) -> None:
        """No tokens → empty order list."""
        assert port._placeholder_order_from_template("") == []
        assert port._placeholder_order_from_template("no tokens here") == []

    def test_single_placeholder(self) -> None:
        """One named token → one entry."""
        assert port._placeholder_order_from_template("Hello {{name}}") == ["name"]

    def test_two_placeholders_in_order(self) -> None:
        """Two distinct tokens → order matches source."""
        order = port._placeholder_order_from_template("{{name}} · {{assetClass}}")
        assert order == ["name", "assetClass"]

    def test_repeated_token_collapses_to_first(self) -> None:
        """A repeated reference to the same name maps to one positional slot (matches i18next's behavior)."""
        order = port._placeholder_order_from_template("{{x}} then {{y}} then {{x}}")
        assert order == ["x", "y"]


class TestRewritePlaceholders:
    """``_rewrite_placeholders`` converts i18next tokens to xcstrings positional."""

    def test_empty_order_returns_input_unchanged(self) -> None:
        """No EN-template order → no work to do."""
        assert port._rewrite_placeholders("{{x}}", []) == "{{x}}"

    def test_single_placeholder_becomes_positional_one(self) -> None:
        """First name in order → ``%1$@``."""
        result = port._rewrite_placeholders("{{name}}", ["name"])
        assert result == "%1$@"

    def test_two_placeholders_position_by_en_order(self) -> None:
        """``%1$@`` = first EN name, ``%2$@`` = second EN name — regardless of where they appear in the translated template."""
        result = port._rewrite_placeholders(
            "{{assetClass}}: {{name}}",
            order=["name", "assetClass"],
        )
        assert result == "%2$@: %1$@"

    def test_unknown_placeholder_left_as_is_with_warning(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A token in a translated template that isn't in the EN order is preserved verbatim and a warning is printed to stderr."""
        result = port._rewrite_placeholders(
            "{{name}} extra {{rogue}}",
            order=["name"],
        )
        assert result == "%1$@ extra {{rogue}}"
        err = capsys.readouterr().err
        assert "rogue" in err and "not in EN template order" in err

    def test_repeated_name_uses_same_positional_index(self) -> None:
        """Repeated name maps to the same ``%N$@`` slot."""
        result = port._rewrite_placeholders("{{x}} then {{x}}", ["x"])
        assert result == "%1$@ then %1$@"


class TestBuildStringUnit:
    """``_build_string_unit`` shape mirrors Apple's xcstrings format."""

    def test_returns_translated_value(self) -> None:
        """``stringUnit`` block contains state + value."""
        assert port._build_string_unit("Hello") == {
            "state": "translated",
            "value": "Hello",
        }

    def test_preserves_empty_string(self) -> None:
        """Empty value is preserved (the cacheBanner ``cache`` source ships ``""``)."""
        assert port._build_string_unit("") == {
            "state": "translated",
            "value": "",
        }


class TestFullCatalogKey:
    """``_full_catalog_key`` prepends the catalog namespace."""

    def test_simple_leaf(self) -> None:
        """Single-segment leaf → prefixed."""
        assert port._full_catalog_key("description.label") == "market.description.label"

    def test_nested_leaf(self) -> None:
        """Multi-segment leaf → prefixed."""
        assert port._full_catalog_key("sector.precious-metals") == "market.sector.precious-metals"


class TestIterPhaseKeys:
    """Market key iterator yields sorted short paths."""

    def test_yields_sorted(self) -> None:
        """Even when input is shuffled, output is alphabetic."""
        payload = {
            "sector.us-tech": "x",
            "description.label": "x",
            "assetClass.crypto": "x",
        }
        result = list(port._iter_phase_keys(payload))
        assert result == sorted(payload.keys())

    def test_empty_input_yields_nothing(self) -> None:
        """Empty payload → no items."""
        assert list(port._iter_phase_keys({})) == []


class TestEnsureStringsObject:
    """``_ensure_strings_object`` returns the mutable ``strings`` dict."""

    def test_existing_dict_returned(self) -> None:
        """An xcstrings doc with a ``strings`` dict returns it verbatim."""
        doc: dict[str, object] = {"strings": {"alerts.x": {}}}
        strings = port._ensure_strings_object(doc)
        assert strings is doc["strings"]

    def test_missing_strings_created(self) -> None:
        """An xcstrings doc with no ``strings`` field gets one created."""
        doc: dict[str, object] = {}
        strings = port._ensure_strings_object(doc)
        assert strings == {}
        assert doc["strings"] is strings

    def test_non_dict_strings_raises(self) -> None:
        """A corrupt ``strings`` field fails fast."""
        with pytest.raises(SystemExit, match="file corrupt"):
            port._ensure_strings_object({"strings": "broken"})


class TestListFrontendLocales:
    """``_list_frontend_locales`` discovers locale dirs with market.json."""

    def test_lists_only_dirs_with_market_json(self, tmp_path: Path) -> None:
        """Skips files and dirs missing the market.json source."""
        (tmp_path / "en").mkdir()
        (tmp_path / "en" / "market.json").write_text("{}", encoding="utf-8")
        (tmp_path / "pl").mkdir()
        (tmp_path / "pl" / "market.json").write_text("{}", encoding="utf-8")
        (tmp_path / "skip").mkdir()
        (tmp_path / "file.txt").write_text("ignore", encoding="utf-8")
        (tmp_path / ".hidden").mkdir()
        (tmp_path / ".hidden" / "market.json").write_text("{}", encoding="utf-8")

        result = port._list_frontend_locales(tmp_path)
        assert result == ["en", "pl"]

    def test_missing_dir_raises(self, tmp_path: Path) -> None:
        """A nonexistent locales root fails fast."""
        with pytest.raises(SystemExit, match="frontend locales dir not found"):
            port._list_frontend_locales(tmp_path / "does-not-exist")


class TestReadLocalePayload:
    """``_read_locale_payload`` reads + flattens one locale's market.json."""

    def test_reads_and_flattens(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Happy path: present file, valid JSON, ported keys flow through."""
        en_dir = tmp_path / "en"
        en_dir.mkdir()
        (en_dir / "market.json").write_text(
            json.dumps(
                {
                    "description": {"label": "Instrument description"},
                    "assetClass": {"crypto": "Cryptocurrency"},
                    "page": {"title": "skipped"},
                }
            ),
            encoding="utf-8",
        )
        monkeypatch.setattr(port, "FRONTEND_LOCALES_DIR", tmp_path)
        result = port._read_locale_payload("en")
        assert result == {
            "description.label": "Instrument description",
            "assetClass.crypto": "Cryptocurrency",
        }

    def test_missing_file_raises(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Absent ``market.json`` fails fast with a path-bearing message."""
        monkeypatch.setattr(port, "FRONTEND_LOCALES_DIR", tmp_path)
        with pytest.raises(SystemExit, match="market.json missing for locale 'en'"):
            port._read_locale_payload("en")

    def test_non_object_root_raises(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A JSON array root (not object) fails fast."""
        en_dir = tmp_path / "en"
        en_dir.mkdir()
        (en_dir / "market.json").write_text("[]", encoding="utf-8")
        monkeypatch.setattr(port, "FRONTEND_LOCALES_DIR", tmp_path)
        with pytest.raises(SystemExit, match="is not a JSON object"):
            port._read_locale_payload("en")


class TestGenerateE2E:
    """End-to-end ``generate()`` integration against tmpdir fixtures."""

    def _setup_frontend(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        locales: dict[str, dict[str, object]],
    ) -> Path:
        """Build a synthetic ``frontend/src/locales`` tree.

        Args:
            tmp_path: pytest tmpdir.
            monkeypatch: pytest monkeypatch fixture (rewires module globals).
            locales: locale-name → market.json payload dict.

        Returns:
            Path to the synthetic frontend root.
        """
        frontend_root = tmp_path / "frontend-locales"
        frontend_root.mkdir()
        for name, payload in locales.items():
            locale_dir = frontend_root / name
            locale_dir.mkdir()
            (locale_dir / "market.json").write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )
        monkeypatch.setattr(port, "FRONTEND_LOCALES_DIR", frontend_root)
        return frontend_root

    def _empty_xcstrings(self, tmp_path: Path) -> Path:
        """Build a minimal xcstrings file with no entries."""
        path = tmp_path / "Localizable.xcstrings"
        path.write_text(
            json.dumps(
                {"sourceLanguage": "en", "strings": {}, "version": "1.0"},
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return path

    def test_full_write_is_deterministic(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two runs produce byte-identical output (idempotent)."""
        self._setup_frontend(
            tmp_path,
            monkeypatch,
            locales={
                "en": {
                    "description": {
                        "label": "Instrument description",
                        "fallback": "{{name}} · {{assetClass}}",
                    },
                    "assetClass": {"crypto": "Cryptocurrency"},
                },
                "pl": {
                    "description": {
                        "label": "Opis instrumentu",
                        "fallback": "{{name}} · {{assetClass}}",
                    },
                    "assetClass": {"crypto": "Kryptowaluta"},
                },
            },
        )
        target = self._empty_xcstrings(tmp_path)

        port.generate(xcstrings_path=target)
        first_bytes = target.read_bytes()
        port.generate(xcstrings_path=target)
        assert target.read_bytes() == first_bytes

    def test_writes_phase_keys_with_positional_placeholders(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """All ported namespaces land in the catalog with ordered placeholders."""
        self._setup_frontend(
            tmp_path,
            monkeypatch,
            locales={
                "en": {
                    "description": {
                        "label": "Instrument description",
                        "fallback": "{{name}} · {{assetClass}}",
                    },
                    "assetClass": {"crypto": "Cryptocurrency"},
                    "sector": {"agriculture": "Agriculture"},
                },
                "pl": {
                    "description": {
                        "label": "Opis instrumentu",
                        "fallback": "{{name}} · {{assetClass}}",
                    },
                    "assetClass": {"crypto": "Kryptowaluta"},
                    "sector": {"agriculture": "Rolnictwo"},
                },
            },
        )
        target = self._empty_xcstrings(tmp_path)

        port.generate(xcstrings_path=target)
        capsys.readouterr()

        doc = json.loads(target.read_text(encoding="utf-8"))
        strings = doc["strings"]
        assert set(strings.keys()) == {
            "market.description.label",
            "market.description.fallback",
            "market.assetClass.crypto",
            "market.sector.agriculture",
        }
        fallback = strings["market.description.fallback"]["localizations"]
        assert fallback["en"]["stringUnit"]["value"] == "%1$@ · %2$@"
        assert fallback["pl"]["stringUnit"]["value"] == "%1$@ · %2$@"
        assert (
            strings["market.assetClass.crypto"]["localizations"]["pl"]["stringUnit"]["value"]
            == "Kryptowaluta"
        )

    def test_existing_unrelated_keys_preserved(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-market keys already in the catalog are not touched."""
        self._setup_frontend(
            tmp_path,
            monkeypatch,
            locales={
                "en": {"description": {"label": "Instrument description"}},
            },
        )
        target = tmp_path / "Localizable.xcstrings"
        target.write_text(
            json.dumps(
                {
                    "sourceLanguage": "en",
                    "strings": {
                        "auth.login.signIn": {
                            "extractionState": "manual",
                            "localizations": {
                                "en": {
                                    "stringUnit": {
                                        "state": "translated",
                                        "value": "Sign in",
                                    }
                                }
                            },
                        }
                    },
                    "version": "1.0",
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        port.generate(xcstrings_path=target)

        doc = json.loads(target.read_text(encoding="utf-8"))
        assert "auth.login.signIn" in doc["strings"]
        assert (
            doc["strings"]["auth.login.signIn"]["localizations"]["en"]["stringUnit"]["value"]
            == "Sign in"
        )

    def test_missing_translation_for_locale_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing locale translations for EN market keys fail loudly."""
        self._setup_frontend(
            tmp_path,
            monkeypatch,
            locales={
                "en": {
                    "description": {"label": "EN label"},
                    "assetClass": {"crypto": "Cryptocurrency"},
                },
                "pl": {
                    "description": {"label": "PL label"},
                },
            },
        )
        target = self._empty_xcstrings(tmp_path)
        with pytest.raises(SystemExit, match="missing 'assetClass.crypto'"):
            port.generate(xcstrings_path=target)

    def test_missing_xcstrings_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An absent target xcstrings fails fast."""
        self._setup_frontend(tmp_path, monkeypatch, locales={"en": {}})
        with pytest.raises(SystemExit, match="xcstrings not found"):
            port.generate(xcstrings_path=tmp_path / "missing.xcstrings")

    def test_xcstrings_not_object_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An xcstrings file whose root is a JSON array fails fast."""
        self._setup_frontend(
            tmp_path,
            monkeypatch,
            locales={"en": {"description": {"label": "x"}}},
        )
        target = tmp_path / "Localizable.xcstrings"
        target.write_text("[]", encoding="utf-8")
        with pytest.raises(SystemExit, match="did not parse as a JSON object"):
            port.generate(xcstrings_path=target)

    def test_empty_en_phase_keys_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """EN file without any ported keys fails fast instead of skipping the port."""
        self._setup_frontend(
            tmp_path,
            monkeypatch,
            locales={"en": {"page": {"title": "no phase keys here"}}},
        )
        target = self._empty_xcstrings(tmp_path)
        with pytest.raises(SystemExit, match="EN market.json contains no Phase-N keys"):
            port.generate(xcstrings_path=target)

    def test_existing_market_entry_with_corrupt_localizations_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An existing market.* entry with a non-dict ``localizations`` is treated as corrupt input."""
        self._setup_frontend(
            tmp_path,
            monkeypatch,
            locales={"en": {"description": {"label": "x"}}},
        )
        target = tmp_path / "Localizable.xcstrings"
        target.write_text(
            json.dumps(
                {
                    "sourceLanguage": "en",
                    "strings": {
                        "market.description.label": {
                            "extractionState": "manual",
                            "localizations": "broken",
                        }
                    },
                    "version": "1.0",
                }
            ),
            encoding="utf-8",
        )
        with pytest.raises(SystemExit, match="non-dict localizations"):
            port.generate(xcstrings_path=target)

    def test_missing_frontend_dir_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An absent FRONTEND_LOCALES_DIR fails fast (separate branch from the inner ``_list_frontend_locales`` guard)."""
        monkeypatch.setattr(
            port,
            "FRONTEND_LOCALES_DIR",
            tmp_path / "no-such-dir",
        )
        with pytest.raises(SystemExit, match="frontend locales dir not found"):
            port.generate(xcstrings_path=tmp_path / "ignored.xcstrings")


class TestCheckDrift:
    """``check_drift()`` re-runs ``generate()`` against a tmpdir copy."""

    def _setup(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        locales: dict[str, dict[str, object]],
        xcstrings_content: str,
    ) -> Path:
        frontend_root = tmp_path / "frontend-locales"
        frontend_root.mkdir()
        for name, payload in locales.items():
            d = frontend_root / name
            d.mkdir()
            (d / "market.json").write_text(json.dumps(payload), encoding="utf-8")
        target = tmp_path / "Localizable.xcstrings"
        target.write_text(xcstrings_content, encoding="utf-8")
        monkeypatch.setattr(port, "FRONTEND_LOCALES_DIR", frontend_root)
        monkeypatch.setattr(port, "XCSTRINGS_PATH", target)
        return target

    def test_no_drift_returns_zero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """When the committed xcstrings already matches a fresh run, check_drift exits 0 quietly."""
        target = self._setup(
            tmp_path,
            monkeypatch,
            locales={"en": {"description": {"label": "Hello"}}},
            xcstrings_content=json.dumps(
                {"sourceLanguage": "en", "strings": {}, "version": "1.0"},
                indent=2,
            )
            + "\n",
        )
        port.generate(xcstrings_path=target)
        capsys.readouterr()
        assert port.check_drift() == 0

    def test_drift_returns_one_with_stderr(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A committed xcstrings that diverges produces exit 1 + diff summary on stderr."""
        self._setup(
            tmp_path,
            monkeypatch,
            locales={"en": {"description": {"label": "Hello"}}},
            xcstrings_content=json.dumps(
                {"sourceLanguage": "en", "strings": {}, "version": "1.0"},
                indent=2,
            )
            + "\n",
        )
        capsys.readouterr()
        assert port.check_drift() == 1
        err = capsys.readouterr().err
        assert "Market catalog drift detected" in err
        assert "Re-run `python scripts/port_market_catalog.py`" in err

    def test_missing_xcstrings_returns_one(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """check_drift() exits 1 cleanly when the target xcstrings is absent rather than the file-system error escaping."""
        monkeypatch.setattr(port, "XCSTRINGS_PATH", tmp_path / "absent.xcstrings")
        assert port.check_drift() == 1
        assert "xcstrings not found" in capsys.readouterr().err


class TestMain:
    """argparse entry point."""

    def test_write_mode_calls_generate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No flags → ``generate()`` called and main exits 0."""
        called: list[bool] = []

        def fake_generate() -> None:
            called.append(True)

        monkeypatch.setattr(port, "generate", fake_generate)
        monkeypatch.setattr(sys, "argv", ["port_market_catalog.py"])
        assert port.main() == 0
        assert called == [True]

    def test_check_mode_returns_drift_exit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``--check`` calls ``check_drift()`` and surfaces its exit code."""
        monkeypatch.setattr(port, "check_drift", lambda: 7)
        monkeypatch.setattr(sys, "argv", ["port_market_catalog.py", "--check"])
        assert port.main() == 7
