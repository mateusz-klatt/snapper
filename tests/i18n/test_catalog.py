"""Tests for ``snapper.i18n.catalog`` — backend i18n loader + resolver."""

from pathlib import Path

import pytest

from snapper.i18n.catalog import load_catalogs_from
from snapper.i18n.catalog import localized
from snapper.i18n.catalog import render
from snapper.i18n.catalog import resolve_alert_strings
from snapper.i18n.catalog import supported_catalog_languages


def test_catalog_loads_all_45_languages() -> None:
    """All language JSON files are committed and loaded.

    Given: the generator runs on every iOS xcstrings change and writes
        one ``<lang>.json`` per catalog language.
    When: ``supported_catalog_languages`` is queried.
    Then: 45 codes are present — the locked-in catalog count.
    """
    langs = supported_catalog_languages()
    assert len(langs) == 45
    assert "en" in langs
    assert "pl" in langs
    assert "ar" in langs
    assert "ga" in langs


def test_en_lookup_returns_source_template() -> None:
    """EN is the source language.

    Given: the EN catalog has every key.
    When: ``localized`` is called for an EN key.
    Then: the raw English template is returned unchanged.
    """
    assert localized("alerts.title.margin_warning", "en") == "Margin warning"


def test_pl_lookup_returns_polish_template() -> None:
    """Spot-check translation for PL.

    Given: PL catalog is generated from xcstrings.
    When: ``localized`` is called for PL.
    Then: the Polish template is returned.
    """
    assert localized("alerts.title.order_fill_full", "pl") == "Zlecenie zrealizowane"


def test_unknown_language_falls_back_to_en() -> None:
    """Unknown language code falls back to EN.

    Given: the user's ``default_language`` is a frontend-only code
        (e.g. ``"pt"`` against the iOS catalog where it's ``"pt-BR"``).
    When: ``localized`` is called for ``"pt"`` (no catalog file).
    Then: the EN template is returned so the user sees content.
    """
    assert localized("alerts.title.order_fill_full", "pt") == "Order filled"


def test_unknown_key_falls_back_to_key_itself() -> None:
    """Unknown key returns the key unchanged.

    Given: a catalog key that wasn't generated (e.g. a typo in the
        rule code).
    When: ``localized`` is called.
    Then: the key itself is returned — better than a crash, surfaces
        the bug at the alert text.
    """
    assert localized("alerts.title.nonexistent_key", "en") == "alerts.title.nonexistent_key"


def test_render_substitutes_placeholders_for_polish_template() -> None:
    """End-to-end render for a body template.

    Given: the PL ``alerts.body.order_fill_full`` template.
    When: ``render`` is called with positional args.
    Then: placeholders are substituted in source order.
    """
    rendered = render(
        "alerts.body.order_fill_full",
        "pl",
        "BUY",
        "100",
        "BTCUSD",
        "50000.00",
        "Kraken",
    )
    assert "BUY" in rendered
    assert "Kraken" in rendered
    assert "%@" not in rendered


def test_render_skips_substitution_for_missing_key() -> None:
    """Missing key bypasses ``render_template`` to avoid a crash.

    Given: a key that doesn't exist.
    When: ``render`` is called with args.
    Then: the key is returned verbatim — extra args don't trigger the
        underlying format-mismatch ValueError.
    """
    result = render("alerts.title.unknown", "en", "ignored")
    assert result == "alerts.title.unknown"


def test_render_works_for_critical_system_error_with_int_arg() -> None:
    """The only template using ``%lld`` round-trips.

    Given: ``alerts.body.critical_system_error`` is the only template
        that mixes ``%@`` and ``%lld``.
    When: ``render`` is called with the canonical 4-arg signature.
    Then: the integer arg renders correctly.
    """
    rendered = render(
        "alerts.body.critical_system_error",
        "en",
        "kraken",
        "spot",
        "WARNING",
        5,
    )
    assert rendered == "kraken/spot reported WARNING for 5 consecutive heartbeats"


def test_load_catalogs_raises_when_directory_missing(tmp_path: Path) -> None:
    """Missing catalog directory surfaces with an actionable error.

    Given: A ``tmp_path`` location that does not contain a catalogs/
        directory.
    When: ``load_catalogs_from`` is called against it.
    Then: ``FileNotFoundError`` is raised with the script-name hint
        so the maintainer knows to run the generator.
    """
    nonexistent = tmp_path / "missing"
    with pytest.raises(FileNotFoundError, match="gen_backend_i18n_catalog"):
        load_catalogs_from(nonexistent)


def test_load_catalogs_raises_when_en_fallback_is_missing(tmp_path: Path) -> None:
    """EN is a required invariant.

    Given: A catalog directory containing only non-EN JSONs.
    When: ``load_catalogs_from`` is called.
    Then: ``FileNotFoundError`` is raised — the EN fallback path is
        load-bearing for unknown-language lookups.
    """
    catalog_dir = tmp_path / "catalogs"
    catalog_dir.mkdir()
    (catalog_dir / "pl.json").write_text("{}", encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="EN fallback"):
        load_catalogs_from(catalog_dir)


def test_load_catalogs_raises_when_file_is_not_a_json_object(tmp_path: Path) -> None:
    """Non-object JSON content is rejected.

    Given: A catalog file containing a JSON list (not an object).
    When: ``load_catalogs_from`` is called.
    Then: ``ValueError`` is raised so a malformed generator output
        cannot silently load as an empty/strange catalog at import.
    """
    catalog_dir = tmp_path / "catalogs"
    catalog_dir.mkdir()
    (catalog_dir / "en.json").write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="did not parse as a JSON object"):
        load_catalogs_from(catalog_dir)


def test_load_catalogs_raises_when_values_are_not_strings(tmp_path: Path) -> None:
    """All catalog values must be strings.

    Given: A catalog with a numeric value (e.g. ``{"alerts.x": 1}``).
    When: ``load_catalogs_from`` is called.
    Then: ``ValueError`` is raised — the resolver and format converter
        both assume string template values.
    """
    catalog_dir = tmp_path / "catalogs"
    catalog_dir.mkdir()
    (catalog_dir / "en.json").write_text('{"alerts.x": 1}', encoding="utf-8")
    with pytest.raises(ValueError, match="non-string"):
        load_catalogs_from(catalog_dir)


_PHASE_C_PAYLOAD = {
    "title_loc_key": "alerts.title.order_fill_full",
    "body_loc_key": "alerts.body.order_fill_full",
    "body_loc_args": ["BUY", "100", "BTCUSD", "50000.00", "Kraken"],
}


class TestResolveAlertStrings:
    """Cross-surface alert resolver — shared by APNs sidecar + REST routes."""

    def test_pl_payload_renders_polish_title_and_body(self) -> None:
        """Happy path: valid loc_keys + matching args resolve via the catalog."""
        title, body = resolve_alert_strings(
            payload=_PHASE_C_PAYLOAD,
            fallback_title="Order filled",
            fallback_body="BUY 100 BTCUSD @ $50000.00 filled on Kraken",
            user_language="pl",
        )
        assert title == "Zlecenie zrealizowane"
        assert "zrealizowane na Kraken" in body

    def test_user_language_none_returns_fallback(self) -> None:
        """No preference → no catalog round-trip, just the stored EN strings."""
        title, body = resolve_alert_strings(
            payload=_PHASE_C_PAYLOAD,
            fallback_title="EN title",
            fallback_body="EN body",
            user_language=None,
        )
        assert title == "EN title"
        assert body == "EN body"

    def test_payload_none_returns_fallback(self) -> None:
        """Legacy row with no payload → fallback (cannot localize without keys)."""
        title, body = resolve_alert_strings(
            payload=None,
            fallback_title="EN title",
            fallback_body="EN body",
            user_language="pl",
        )
        assert title == "EN title"
        assert body == "EN body"

    def test_missing_loc_keys_returns_fallback(self) -> None:
        """Payload without loc_keys (legacy or opted-out rule) → fallback."""
        title, body = resolve_alert_strings(
            payload={"deep_link_path": "/orders/1"},
            fallback_title="EN title",
            fallback_body="EN body",
            user_language="pl",
        )
        assert title == "EN title"
        assert body == "EN body"

    def test_non_list_args_returns_fallback(self) -> None:
        """Malformed args (string instead of list) → fallback all-or-nothing."""
        bad_payload = dict(_PHASE_C_PAYLOAD)
        bad_payload["body_loc_args"] = "not-a-list"
        title, body = resolve_alert_strings(
            payload=bad_payload,
            fallback_title="EN title",
            fallback_body="EN body",
            user_language="pl",
        )
        assert title == "EN title"
        assert body == "EN body"

    def test_catalog_miss_on_either_field_returns_fallback(self) -> None:
        """Partial miss → all-or-nothing EN fallback (never half-localized).

        Title key exists in the catalog; body key intentionally does
        not. The resolver must not return PL title + EN body, since
        that would visibly mix locales in the rendered alert.
        """
        partial_miss = {
            "title_loc_key": "alerts.title.order_fill_full",
            "body_loc_key": "alerts.body.does_not_exist",
            "body_loc_args": [],
        }
        title, body = resolve_alert_strings(
            payload=partial_miss,
            fallback_title="EN title",
            fallback_body="EN body",
            user_language="pl",
        )
        assert title == "EN title"
        assert body == "EN body"

    def test_render_arity_mismatch_returns_fallback(self) -> None:
        """Drift between catalog template + rule args → fallback + warn log.

        Supplies only 2 args for a template that needs 5. ``catalog.render``
        raises ``ValueError``; the resolver catches it so the caller never
        sees a partially-resolved string or a crash.
        """
        wrong_arity = dict(_PHASE_C_PAYLOAD)
        wrong_arity["body_loc_args"] = ["BUY", "100"]
        title, body = resolve_alert_strings(
            payload=wrong_arity,
            fallback_title="EN title",
            fallback_body="EN body",
            user_language="pl",
            log_context="test_render_arity_mismatch",
        )
        assert title == "EN title"
        assert body == "EN body"

    def test_non_string_loc_keys_return_fallback(self) -> None:
        """``title_loc_key``/``body_loc_key`` must be strings to render."""
        bad = {"title_loc_key": 42, "body_loc_key": "alerts.body.order_fill_full"}
        title, body = resolve_alert_strings(
            payload=bad,
            fallback_title="EN title",
            fallback_body="EN body",
            user_language="pl",
        )
        assert title == "EN title"
        assert body == "EN body"
