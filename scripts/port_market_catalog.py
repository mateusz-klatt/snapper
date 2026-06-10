"""Port frontend market.* translations into the iOS xcstrings catalog.

Reads each frontend ``frontend/src/locales/<dir>/market.json`` file,
extracts the configured market namespaces, and
merges the per-locale strings into
``ios/Snapper/Resources/Localization/Localizable.xcstrings``.

Why a separate script rather than reusing
``gen_backend_i18n_catalog.py``: the backend variant emits a flat
``{key: template}`` map for the Python resolver, whereas iOS xcstrings
is a nested document keyed by ``market.<namespace>.<leaf>`` with one
``stringUnit`` per language. Placeholders translate from i18next's
named tokens (``{{name}}`` / ``{{assetClass}}``) to xcstrings positional
string codes (``%1$@`` / ``%2$@``) based on appearance order in the
English template — the English template is the source of truth for
placeholder ordering across the 45 locales. Numeric placeholders
(``%lld`` / ``%d``) are NOT emitted; every i18next named token lowers
to a string-typed xcstrings placeholder regardless of how the value
is consumed at runtime.

Drift between the source JSONs and the committed xcstrings is enforced
by ``make ui-i18n-check-market`` (re-runs the script and ``diff``s the
output against the committed file).

Usage::

    python scripts/port_market_catalog.py          # write mode
    python scripts/port_market_catalog.py --check  # drift check (no writes)

Exits 0 on success. In write mode, exits 1 with a diagnostic if a
required source file is missing, malformed, or a required locale
mapping is unsatisfied. In ``--check`` mode, exits 1 with a per-file
diff summary when the committed xcstrings differs from what the script
would emit.
"""

import argparse
import json
import re
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Final

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
XCSTRINGS_PATH: Final[Path] = REPO_ROOT / "ios/Snapper/Resources/Localization/Localizable.xcstrings"
FRONTEND_LOCALES_DIR: Final[Path] = REPO_ROOT / "frontend/src/locales"

PHASE_PREFIXES: Final[tuple[str, ...]] = (
    "description.",
    "assetClass.",
    "sector.",
    "related.",
    "pairStats.",
    "cacheBanner.",
)
"""Frontend ``market.*`` sub-namespaces this script ports.

The tuple covers descriptive labels (``description.``, ``assetClass.``,
``sector.``), related-instrument UI strings (``related.``),
cointegration summary strings (``pairStats.``), and cache status copy
(``cacheBanner.``). The catalog parity gate fails loudly when the
frontend JSON and iOS view-consumed keys drift apart.
"""

CATALOG_NAMESPACE: Final[str] = "market"
"""xcstrings key prefix. Frontend JSON keys are already nested under
their namespace (no prefix in the JSON), so we prepend ``market.``
during port. Backend resolver consumes the same flat-dotted shape.
"""

FRONTEND_TO_IOS_LOCALE: Final[dict[str, str]] = {
    "no": "nb",
    "pt": "pt-BR",
    "zh": "zh-Hans",
    "sr": "sr-Latn",
    "my-MM": "my",
}
"""Frontend → iOS locale-code remap (5 entries that diverge).

All other locale codes match directly (``ar`` → ``ar`` etc.). This is
the inverse of ``port_ios_alert_catalog.py``'s ``IOS_TO_FRONTEND_LOCALE``
because the data flows in the opposite direction (frontend JSON →
xcstrings).
"""


_I18NEXT_PLACEHOLDER: Final[re.Pattern[str]] = re.compile(r"\{\{(?P<name>[a-zA-Z_]\w*)\}\}")
"""Match i18next-style named placeholders ``{{name}}``."""


def _list_frontend_locales(locales_dir: Path) -> list[str]:
    """List frontend locale directories that contain ``market.json``.

    Args:
        locales_dir: Path to ``frontend/src/locales``.

    Returns:
        Sorted list of locale directory names (e.g. ``["ar", "bn", ...]``).
        Excludes hidden dirs and any locale missing ``market.json``.
    """
    if not locales_dir.exists():
        raise SystemExit(f"frontend locales dir not found at {locales_dir}")
    out: list[str] = []
    for entry in sorted(locales_dir.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name.startswith("."):
            continue
        if (entry / "market.json").exists():
            out.append(entry.name)
    return out


def _map_locale(frontend_dir: str) -> str:
    """Return the iOS xcstrings locale code matching ``frontend_dir``."""
    return FRONTEND_TO_IOS_LOCALE.get(frontend_dir, frontend_dir)


def _is_phase_key(key: str) -> bool:
    """Return ``True`` when ``key`` falls under any ported prefix."""
    for allowed in PHASE_PREFIXES:
        bare = allowed.rstrip(".")
        if key == bare or key.startswith(allowed):
            return True
    return False


def _flatten_market_payload(payload: dict[str, object]) -> dict[str, str]:
    """Return the dotted-key flat map filtered to the ported namespaces.

    Skips keys whose value is not a string (i.e. nested namespaces are
    descended into, leaves outside the ported namespaces are discarded).

    Args:
        payload: Parsed ``market.json`` for one locale.

    Returns:
        Flat ``{"description.label": "Instrument description", ...}``
        map containing only keys under the configured market namespaces.
    """
    flat: dict[str, str] = {}
    _walk_market_payload("", payload, flat)
    return flat


def _walk_market_payload(prefix: str, value: object, flat: dict[str, str]) -> None:
    """Recurse into ``value``, populating ``flat`` with prefix-matched leaves."""
    if isinstance(value, dict):
        for child_key, child_value in value.items():
            joined = child_key if prefix == "" else f"{prefix}.{child_key}"
            _walk_market_payload(joined, child_value, flat)
        return
    if isinstance(value, str) and _is_phase_key(prefix):
        flat[prefix] = value


def _placeholder_order_from_template(template: str) -> list[str]:
    """Return the appearance order of ``{{name}}`` tokens in ``template``.

    Duplicates collapse to first occurrence — i18next supports repeated
    references to the same name, all of which map to the same positional
    index in the xcstrings template.
    """
    seen: list[str] = []
    for match in _I18NEXT_PLACEHOLDER.finditer(template):
        name = match.group("name")
        if name not in seen:
            seen.append(name)
    return seen


def _rewrite_placeholders(value: str, order: list[str]) -> str:
    """Rewrite ``{{name}}`` tokens in ``value`` to xcstrings positional codes.

    ``order`` is the appearance order in the EN template — Nth name maps
    to ``%(N+1)$@``. Names absent from ``order`` are left unchanged with
    a stderr warning (caller treats this as a translation bug).

    Args:
        value: Translated template for some locale.
        order: Placeholder appearance order from the EN template.

    Returns:
        xcstrings-style template with positional placeholders.
    """
    if not order:
        return value

    def _sub(match: re.Match[str]) -> str:
        name = match.group("name")
        if name in order:
            position = order.index(name) + 1
            return f"%{position}$@"
        print(
            f"warn: placeholder {{{{{name}}}}} not in EN template order {order}; left as-is",
            file=sys.stderr,
        )
        return match.group(0)

    return _I18NEXT_PLACEHOLDER.sub(_sub, value)


def _build_string_unit(value: str) -> dict[str, object]:
    """Return the xcstrings ``stringUnit`` block for a translated value."""
    return {"state": "translated", "value": value}


def _full_catalog_key(short_key: str) -> str:
    """Return the xcstrings key for a frontend short key.

    Frontend ``market.json`` is the ``market`` namespace, so its leaf
    keys (``description.label`` etc.) get prepended with ``market.``
    before being written into xcstrings.
    """
    return f"{CATALOG_NAMESPACE}.{short_key}"


def _iter_phase_keys(en_payload: dict[str, str]) -> Iterable[str]:
    """Yield ported short keys present in the EN payload, sorted."""
    yield from sorted(en_payload)


def _ensure_strings_object(xcstrings: dict[str, object]) -> dict[str, object]:
    """Return the (mutable) ``strings`` dict, creating it if absent."""
    strings = xcstrings.get("strings")
    if strings is None:
        new_strings: dict[str, object] = {}
        xcstrings["strings"] = new_strings
        return new_strings
    if not isinstance(strings, dict):
        raise SystemExit("xcstrings 'strings' is not a dict — file corrupt?")
    return strings


def _read_locale_payload(locale_dir_name: str) -> dict[str, str]:
    """Read + flatten ``market.json`` for one frontend locale.

    Args:
        locale_dir_name: Name of the ``frontend/src/locales/<dir>``
            directory.

    Returns:
        Flat ``{short_key: value}`` map filtered to the configured
        market namespaces.
    """
    path = FRONTEND_LOCALES_DIR / locale_dir_name / "market.json"
    if not path.exists():
        raise SystemExit(f"market.json missing for locale {locale_dir_name!r} at {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit(f"{path} is not a JSON object")
    return _flatten_market_payload(raw)


def generate(xcstrings_path: Path | None = None) -> None:
    """Port the configured ``market.*`` translations into the iOS xcstrings.

    Reads the EN template to lock placeholder ordering, then iterates
    over the 45 frontend locales. For each ``(locale, key)`` pair:

    - Looks up the translated value in ``frontend/src/locales/<dir>/market.json``.
    - Rewrites i18next ``{{name}}`` placeholders to xcstrings positional
      codes using the EN appearance order.
    - Merges the resulting ``stringUnit`` into the catalog.

    Args:
        xcstrings_path: Override the target file. ``None`` writes to the
            committed ``XCSTRINGS_PATH``. Tests pass a tmpdir path.
    """
    target_path = xcstrings_path if xcstrings_path is not None else XCSTRINGS_PATH
    if not FRONTEND_LOCALES_DIR.exists():
        raise SystemExit(f"frontend locales dir not found at {FRONTEND_LOCALES_DIR}")
    if not target_path.exists():
        raise SystemExit(f"xcstrings not found at {target_path}")
    xcstrings = json.loads(target_path.read_text(encoding="utf-8"))
    if not isinstance(xcstrings, dict):
        raise SystemExit(f"{target_path} did not parse as a JSON object")
    strings = _ensure_strings_object(xcstrings)

    en_payload = _read_locale_payload("en")
    if not en_payload:
        raise SystemExit("EN market.json contains no Phase-N keys")
    placeholder_orders: dict[str, list[str]] = {
        key: _placeholder_order_from_template(value) for key, value in en_payload.items()
    }

    frontend_locales = _list_frontend_locales(FRONTEND_LOCALES_DIR)
    locale_payloads: dict[str, dict[str, str]] = {
        locale_name: _read_locale_payload(locale_name) for locale_name in frontend_locales
    }
    written_keys = 0
    for short_key in _iter_phase_keys(en_payload):
        full_key = _full_catalog_key(short_key)
        entry = strings.get(full_key)
        if not isinstance(entry, dict):
            entry = {"extractionState": "manual", "localizations": {}}
            strings[full_key] = entry
        localizations = entry.setdefault("localizations", {})
        if not isinstance(localizations, dict):
            raise SystemExit(f"xcstrings entry {full_key!r} has non-dict localizations")
        for locale_name in frontend_locales:
            payload = locale_payloads[locale_name]
            value = payload.get(short_key)
            if value is None:
                raise SystemExit(
                    f"locale {locale_name!r} is missing {short_key!r} (full key {full_key!r}); "
                    f"add it to {FRONTEND_LOCALES_DIR / locale_name / 'market.json'}"
                )
            order = placeholder_orders[short_key]
            rewritten = _rewrite_placeholders(value, order)
            ios_locale = _map_locale(locale_name)
            localizations[ios_locale] = {"stringUnit": _build_string_unit(rewritten)}
        written_keys += 1

    rendered = json.dumps(xcstrings, ensure_ascii=False, indent=2) + "\n"
    parent = target_path.parent
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        delete=False,
        dir=parent,
        prefix=".port-market-",
        suffix=".tmp",
    ) as tmp:
        tmp.write(rendered)
        tmp_path = Path(tmp.name)
    tmp_path.replace(target_path)
    print(f"Wrote {written_keys} keys × {len(frontend_locales)} locales to {target_path}")


def check_drift() -> int:
    """Re-run the port script in a tmpdir, diff against the committed file.

    Returns:
        ``0`` when committed xcstrings already matches what the script
        would emit. ``1`` with a summary printed to stderr when bytes
        differ.
    """
    if not XCSTRINGS_PATH.exists():
        print(f"xcstrings not found at {XCSTRINGS_PATH}", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory() as tmp:
        scratch = Path(tmp) / "Localizable.xcstrings"
        scratch.write_bytes(XCSTRINGS_PATH.read_bytes())
        generate(xcstrings_path=scratch)
        committed = XCSTRINGS_PATH.read_text(encoding="utf-8")
        regenerated = scratch.read_text(encoding="utf-8")
        if committed != regenerated:
            print("Market catalog drift detected:", file=sys.stderr)
            print(f"  committed:   {XCSTRINGS_PATH}", file=sys.stderr)
            print(f"  regenerated: {scratch}", file=sys.stderr)
            print(
                "Re-run `python scripts/port_market_catalog.py` and commit the result.",
                file=sys.stderr,
            )
            return 1
    return 0


def main() -> int:
    """Entry point for ``scripts/port_market_catalog.py``.

    Returns:
        ``0`` on success. ``generate()`` raises ``SystemExit(...)`` on
        every failure path, and ``check_drift()`` returns ``1`` with a
        diff summary on drift.
    """
    parser = argparse.ArgumentParser(description=__doc__ or "")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Drift mode: write to a tmpdir + diff against committed xcstrings. Non-zero on drift.",
    )
    args = parser.parse_args()
    if args.check:
        return check_drift()
    generate()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
