"""Tests for ``snapper.i18n.format`` — xcstrings → Python placeholder converter."""

import pytest

from snapper.i18n.format import render
from snapper.i18n.format import to_python_template


def test_string_spec_at_alone_becomes_positional_brace() -> None:
    """The ``%@`` spec is the only one used by title-only templates.

    Given: a single-substitution title template.
    When: converted to Python form.
    Then: ``%@`` is replaced by ``{}``.
    """
    assert to_python_template("System degraded: %@") == "System degraded: {}"


def test_long_long_spec_becomes_positional_brace() -> None:
    """The ``%lld`` spec is used by ``alerts.body.critical_system_error``.

    Given: a template carrying both ``%@`` and ``%lld``.
    When: converted to Python form.
    Then: every placeholder becomes ``{}`` and the order is preserved.
    """
    template = "%@/%@ reported %@ for %lld consecutive heartbeats"
    assert to_python_template(template) == "{}/{} reported {} for {} consecutive heartbeats"


def test_render_substitutes_positionally() -> None:
    """End-to-end render with positional args.

    Given: a template with five ``%@`` placeholders.
    When: ``render`` is called with five args.
    Then: each placeholder is substituted in source order.
    """
    template = "%@ %@ %@ @ $%@ filled on %@"
    rendered = render(template, ["BUY", "100", "BTCUSD", "50000.00", "Kraken"])
    assert rendered == "BUY 100 BTCUSD @ $50000.00 filled on Kraken"


def test_render_handles_mixed_string_and_int_args() -> None:
    """Integer args pass through ``str()`` via ``.format()``.

    Given: a template with a ``%lld`` placeholder.
    When: ``render`` is called with a Python int.
    Then: the int renders as decimal text.
    """
    template = "%@/%@ reported %@ for %lld consecutive heartbeats"
    rendered = render(template, ["kraken", "spot", "WARNING", 5])
    assert rendered == "kraken/spot reported WARNING for 5 consecutive heartbeats"


def test_render_raises_when_args_shorter_than_placeholders() -> None:
    """Missing args surfaces a ValueError with a useful message.

    Given: a template with three placeholders.
    When: ``render`` is called with two args.
    Then: ValueError is raised, mentioning the count mismatch.
    """
    with pytest.raises(ValueError, match="needs"):
        render("%@ %@ %@", ["only", "two"])


def test_to_python_template_rejects_unsupported_specs() -> None:
    """Drift guard for new printf specs.

    Given: a template using ``%d`` (not currently supported).
    When: ``to_python_template`` is called.
    Then: ValueError is raised so the maintainer knows to extend the
        converter rather than letting an unknown spec slip through.
    """
    with pytest.raises(ValueError, match="Unsupported format"):
        to_python_template("count is %d")


def test_template_without_specs_is_passthrough() -> None:
    """Plain strings (title-only) without substitutions return unchanged.

    Given: a template with no format specs (e.g. ``"Order filled"``).
    When: converted to Python form.
    Then: the string is returned verbatim.
    """
    assert to_python_template("Margin warning") == "Margin warning"
