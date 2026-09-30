"""Tests for ``snapper.i18n.catalog`` — backend i18n loader + resolver."""

from pathlib import Path

import pytest

from snapper.i18n import catalog
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

    Given: the user's language is not a supported catalog or client alias.
    When: ``localized`` is called for an unknown language.
    Then: the EN template is returned so the user sees content.
    """
    assert localized("alerts.title.order_fill_full", "unknown") == "Order filled"


@pytest.mark.parametrize(
    ("alias", "canonical"),
    [("pt", "pt-BR"), ("no", "nb"), ("sr", "sr-Latn"), ("zh", "zh-Hans"), ("my-MM", "my")],
)
def test_client_language_aliases_use_their_existing_catalog(alias: str, canonical: str) -> None:
    """Client aliases resolve to the corresponding translated catalog.

    Given: Either client's identifier for the same supported language.
    When: Alert title and body templates are looked up through the alias.
    Then: Both match the canonical translation rather than English fallback.
    """
    for key in ("alerts.title.order_rejected", "alerts.body.order_unknown"):
        assert localized(key, alias) == localized(key, canonical)
        assert localized(key, alias) != localized(key, "en")


def test_explicit_chinese_scripts_are_not_collapsed() -> None:
    """Explicit Chinese scripts retain their own catalogs.

    Given: Traditional and simplified Chinese catalog identifiers.
    When: The same alert title is looked up for both scripts.
    Then: Each returns its own distinct translated value.
    """
    key = "alerts.title.order_rejected"
    assert localized(key, "zh-Hant") != localized(key, "zh-Hans")


@pytest.mark.parametrize(
    "key",
    [
        "alerts.body.order_rejected",
        "alerts.body.margin_warning",
        "alerts.body.order_unknown",
        "alerts.body.order_unknown_unresolved",
    ],
)
def test_known_order_arguments_are_localized_without_mutation(key: str) -> None:
    """Rendering localizes owned tokens without mutating stored arguments.

    Given: Raw side and fallback-reason values plus a token-shaped instrument ID.
    When: The alert is rendered in Polish.
    Then: Owned tokens are translated, while identifiers and input values remain
        unchanged for later re-rendering in another language.
    """
    args = ["SELL", "0.25", "BUY", "unknown reason"]
    rendered = render(key, "pl", *args)
    assert localized("alerts.argument.side.sell", "pl") in rendered
    assert localized("alerts.argument.reason.unknown", "pl") in rendered
    assert "0.25 BUY" in rendered
    assert "unknown reason" not in rendered
    assert args == ["SELL", "0.25", "BUY", "unknown reason"]


@pytest.mark.parametrize("status", ["healthy", "WARNING", "error"])
def test_health_status_arguments_are_localized(status: str) -> None:
    """Health status localization preserves component and instance identifiers.

    Given: A known status and identifiers resembling semantic tokens.
    When: The health alert is rendered in Polish.
    Then: Only the status argument is translated.
    """
    result = render("alerts.body.critical_system_error", "pl", "warning", "BUY", status, 3)
    assert "warning/BUY" in result
    assert localized(f"alerts.argument.status.{status.lower()}", "pl") in result


def test_external_diagnostics_and_unknown_side_are_preserved() -> None:
    """External diagnostics and unknown directions remain verbatim.

    Given: An unknown side token and an external diagnostic containing a known phrase.
    When: The rejection alert is rendered in Polish.
    Then: Both original values remain intact for diagnosis.
    """
    result = render("alerts.body.order_rejected", "pl", "CUSTOM", "1", "X", "E42 unknown reason")
    assert "CUSTOM 1 X" in result
    assert "E42 unknown reason" in result


def test_missing_argument_catalog_entry_preserves_raw_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing semantic labels preserve readable raw arguments.

    Given: A catalog containing an alert template but no argument translations.
    When: The alert is rendered with known semantic tokens.
    Then: Its original argument values appear instead of untranslated catalog keys.
    """
    monkeypatch.setattr(catalog, "_CATALOGS", {"en": {"alerts.body.order_rejected": "%@ %@ %@ %@"}})
    assert render("alerts.body.order_rejected", "en", "BUY", "1", "X", "unknown reason") == (
        "BUY 1 X unknown reason"
    )


@pytest.mark.parametrize("args", [[], ["BUY"], ["BUY", "1", "X"]])
def test_incomplete_semantic_arguments_preserve_the_stored_safety_message(
    args: list[str],
) -> None:
    """Incomplete arguments retain the complete stored alert instead of crashing.

    Given: An unresolved-order payload missing the side or fallback-reason slot.
    When: The alert resolver attempts to localize the malformed payload.
    Then: Both stored fields survive, including the warning against assuming closure.
    """
    fallback_body = "Venue verification pending, do not assume the position is closed"
    assert resolve_alert_strings(
        payload={
            "title_loc_key": "alerts.title.order_unknown",
            "body_loc_key": "alerts.body.order_unknown_unresolved",
            "body_loc_args": [*args],
        },
        fallback_title="Order state unknown",
        fallback_body=fallback_body,
        user_language="pl",
    ) == ("Order state unknown", fallback_body)


@pytest.mark.parametrize(
    "key", ["alerts.body.order_unknown", "alerts.body.order_unknown_unresolved"]
)
def test_unknown_order_catalog_preserves_explicit_safety_instruction(key: str) -> None:
    """Both unknown-order templates retain the explicit safety instruction.

    Given: An unresolved order and an explicit English language preference.
    When: The legacy or new body key is resolved.
    Then: The pending verification and warning against assuming closure remain.
    """
    _, body = resolve_alert_strings(
        payload={
            "title_loc_key": "alerts.title.order_unknown",
            "body_loc_key": key,
            "body_loc_args": ["BUY", "1", "X", "ambiguous venue response"],
        },
        fallback_title="Order state unknown",
        fallback_body="Fallback must not hide a broken template",
        user_language="en",
    )
    assert "do not assume the position is closed" in body
    assert "venue verification pending" in body
    assert "Ambiguous exchange response" in body


@pytest.mark.parametrize(
    "key", ["alerts.body.order_fill_full", "alerts.body.order_fill_full_quoted"]
)
def test_fill_templates_never_invent_a_dollar_currency(key: str) -> None:
    """Fill templates preserve the provided currency without assuming dollars.

    Given: A BTC-quoted price supplied to a legacy or new fill template.
    When: Each supported language renders the alert.
    Then: Its price and BTC unit remain intact without a dollar prefix or USD label.
    """
    for language in supported_catalog_languages():
        result = render(key, language, "BUY", "1", "ETH/BTC", "0.00234 BTC", "venue")
        assert "0.00234 BTC" in result
        assert "$" not in result
        assert "USD" not in result


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
    assert localized("alerts.argument.side.buy", "pl") in rendered
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
    assert rendered == "kraken/spot reported Warning for 5 consecutive heartbeats"


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
