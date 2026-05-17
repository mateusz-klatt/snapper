"""Convert iOS xcstrings printf-style format specs to Python ``str.format``.

The iOS catalog stores templates with NSString-style format specifiers
(``%@`` for any object's ``description``, ``%lld`` for ``long long``).
The Python backend cannot consume those directly; this module rewrites
them to ``{}`` positional placeholders so ``str.format(*args)`` produces
the same substitutions.

Supported specs:

- ``%@`` → string substitution. Python passes ``str(arg)`` regardless of
  type, matching NSString's ``description`` behavior closely enough for
  alert payload args.
- ``%lld`` → signed integer. Python ``str(int_arg)`` matches the
  expected rendering.

Unsupported specs (raise at parse time so silent drift surfaces
immediately):

- ``%d``, ``%f``, ``%s`` etc. — not used in the 10 alert templates
  today; if a future key needs them, add an explicit case rather than
  let an unknown spec slip through.
- ``%%`` (literal percent) — none of the alert templates today; if
  added, extend the converter to map ``%%`` → ``%`` after the spec
  replacements.
"""

import re
from collections.abc import Sequence

_SUPPORTED_SPEC_RE: re.Pattern[str] = re.compile(r"%(@|lld)")
"""Matches the two specs used by the alerts.* catalog today."""

_ANY_PERCENT_RE: re.Pattern[str] = re.compile(r"%[^@lld%]?")
"""Sentinel for unsupported specs.

Applied AFTER stripping the supported set; any remaining ``%X`` means
the catalog grew a new format-spec shape we haven't accounted for. The
parser fails loudly so drift surfaces immediately."""


def to_python_template(template: str) -> str:
    """Rewrite an xcstrings template to Python ``str.format`` form.

    Args:
        template: Source string from the iOS catalog, e.g.
            ``"%@/%@ reported %@ for %lld consecutive heartbeats"``.

    Returns:
        The same string with ``%@`` / ``%lld`` replaced by ``{}``
        positional placeholders, e.g.
        ``"{}/{} reported {} for {} consecutive heartbeats"``.

    Raises:
        ValueError: If ``template`` contains a printf-style format
            specifier this module doesn't recognize. Catches silent
            drift if a new alert template ships with e.g. ``%d`` or
            ``%f``.
    """
    rewritten = _SUPPORTED_SPEC_RE.sub("{}", template)
    leftover = _ANY_PERCENT_RE.search(rewritten)
    if leftover is not None:
        raise ValueError(
            f"Unsupported format specifier in template {template!r}: "
            f"found {leftover.group(0)!r} after stripping %@ and %lld. "
            f"Extend snapper.i18n.format._SUPPORTED_SPEC_RE if a new "
            f"spec is intentional."
        )
    return rewritten


def render(template: str, args: Sequence[object]) -> str:
    """Render an xcstrings template by substituting ``args`` positionally.

    Args:
        template: Source string from the iOS catalog (printf-style).
        args: Positional arguments to substitute, in source order.

    Returns:
        The rendered string with all specifiers replaced.

    Raises:
        ValueError: If the template contains an unsupported format
            spec, or if the substitution itself fails (e.g. mismatched
            placeholder count).
    """
    python_template = to_python_template(template)
    try:
        return python_template.format(*args)
    except (IndexError, KeyError) as exc:
        raise ValueError(
            f"Render mismatch for template {template!r}: "
            f"got {len(args)} args, template needs {python_template.count('{}')}. "
            f"Underlying error: {exc}"
        ) from exc
