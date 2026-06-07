"""Port the iOS ``alerts.*`` xcstrings catalog into frontend JSON locales.

Reads ``ios/Snapper/Resources/Localization/Localizable.xcstrings``,
filters keys with the ``alerts.`` prefix (30 keys at time of writing),
maps each iOS locale code to its corresponding frontend
``src/locales/<dir>/`` and writes ``alerts.json`` per locale.

Also extracts ``alerts.navTitle`` and writes it back into each locale's
``common.json`` under the ``nav.alerts`` key (used by the sidebar tab
label).

Why a separate script rather than reusing
``gen_backend_i18n_catalog.py``: the backend variant outputs a flat
``{key: template}`` map for the backend resolver, whereas the frontend
catalog wants a nested object structure (``alerts.title.foo`` →
``{alerts: {title: {foo: "..."}}}``) and translates printf-style
placeholders (``%@`` / ``%lld``) to i18next's positional format
(``{{0}}``, ``{{1}}``, ...).

Drift between source xcstrings and committed JSON is enforced by
``make ui-i18n-check-alerts`` (re-runs the script and ``diff``s the
output).

Usage:

    python scripts/port_ios_alert_catalog.py          # write mode
    python scripts/port_ios_alert_catalog.py --check  # drift check (no writes)

Exits 0 on success. In write mode, exits 1 with a diagnostic if the
source file is missing, malformed, or a required locale mapping is
unsatisfied. In ``--check`` mode, exits 1 with a per-file diff
summary when committed output differs from what the script would
emit; wired into ``make ui-check`` via ``ui-i18n-check-alerts``.
"""

import argparse
import json
import re
import shutil
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Final

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
XCSTRINGS_PATH: Final[Path] = REPO_ROOT / "ios/Snapper/Resources/Localization/Localizable.xcstrings"
FRONTEND_LOCALES_DIR: Final[Path] = REPO_ROOT / "frontend/src/locales"

ALERTS_PREFIX: Final[str] = "alerts."

NAV_LABEL_KEY: Final[str] = "alerts.navTitle"
"""Source xcstrings key whose value becomes ``common.nav.alerts``.

NOTE: this is ``alerts.navTitle`` (top-level), NOT
``alerts.detail.navTitle`` (which is the alert-detail screen header).
An earlier revision misattributed this to the detail-screen key.
"""


IOS_TO_FRONTEND_LOCALE: Final[dict[str, str]] = {
    "nb": "no",
    "pt-BR": "pt",
    "zh-Hans": "zh",
    "sr-Latn": "sr",
    "my": "my-MM",
}
"""Locale-code remap for the 5 codes where iOS xcstrings and frontend
``src/locales/`` directory names diverge.

All other locale codes are direct same-name matches.
"""


_PLACEHOLDER_PATTERN: Final[re.Pattern[str]] = re.compile(r"%(?:@|lld|d)")
"""Match printf-style placeholders the iOS xcstrings uses for
positional substitution: ``%@`` (strings), ``%lld`` (long long
integer), ``%d`` (int). Each occurrence is replaced by the i18next
positional token ``{{N}}`` where ``N`` is the zero-based occurrence
index in the template.
"""


def iter_alert_keys(xcstrings: dict[str, object]) -> Iterable[str]:
    """Yield xcstrings keys with the ``alerts.`` prefix.

    Args:
        xcstrings: Parsed xcstrings document.

    Yields:
        Sorted alert keys.
    """
    strings = xcstrings.get("strings", {})
    if not isinstance(strings, dict):
        raise SystemExit("xcstrings 'strings' is not a dict — file corrupt?")
    for key in sorted(strings):
        if key.startswith(ALERTS_PREFIX):
            yield key


def collect_ios_locales(xcstrings: dict[str, object], keys: list[str]) -> list[str]:
    """Resolve the language codes used by the alert-namespace entries.

    Args:
        xcstrings: Parsed xcstrings document.
        keys: Alert keys.

    Returns:
        Sorted iOS locale codes (e.g. ``["ar", "bn", ..., "zh-Hant"]``).
    """
    if not keys:
        raise SystemExit("xcstrings has no alerts.* keys")
    strings = xcstrings["strings"]
    if not isinstance(strings, dict):
        raise SystemExit("xcstrings 'strings' is not a dict")
    first_entry = strings[keys[0]]
    if not isinstance(first_entry, dict):
        raise SystemExit(f"xcstrings entry for {keys[0]!r} is not a dict")
    localizations = first_entry.get("localizations", {})
    if not isinstance(localizations, dict):
        raise SystemExit(f"xcstrings entry for {keys[0]!r} has no localizations")
    reference = set(localizations.keys())
    for k in keys[1:]:
        entry = strings[k]
        if not isinstance(entry, dict):
            raise SystemExit(f"xcstrings entry for {k!r} is not a dict")
        loc = entry.get("localizations", {})
        if not isinstance(loc, dict):
            raise SystemExit(f"xcstrings entry for {k!r} has no localizations")
        diff = reference ^ set(loc.keys())
        if diff:
            raise SystemExit(
                f"Language drift between {keys[0]!r} and {k!r}: differing langs = {sorted(diff)}"
            )
    return sorted(reference)


def map_locale(ios_locale: str) -> str:
    """Map an iOS xcstrings locale code to the matching frontend dir name.

    Args:
        ios_locale: Locale code as it appears in the xcstrings document.

    Returns:
        The matching ``frontend/src/locales/<dir>/`` directory name.
        Returns ``ios_locale`` unchanged when no remap entry exists.
    """
    return IOS_TO_FRONTEND_LOCALE.get(ios_locale, ios_locale)


def extract_value(xcstrings: dict[str, object], key: str, ios_locale: str) -> str:
    """Pull the string value for ``(key, ios_locale)`` from the xcstrings.

    Args:
        xcstrings: Parsed xcstrings document.
        key: Alert key.
        ios_locale: Locale code as it appears in the xcstrings document.

    Returns:
        The string template, with iOS placeholders intact (caller is
        expected to call :func:`rewrite_placeholders` if needed).
    """
    strings = xcstrings["strings"]
    if not isinstance(strings, dict):
        raise SystemExit("xcstrings 'strings' is not a dict")
    entry = strings.get(key)
    if not isinstance(entry, dict):
        raise SystemExit(f"xcstrings entry for {key!r} not a dict")
    localizations = entry.get("localizations", {})
    if not isinstance(localizations, dict):
        raise SystemExit(f"xcstrings entry for {key!r} has no localizations")
    lang_entry = localizations.get(ios_locale)
    if not isinstance(lang_entry, dict):
        raise SystemExit(f"xcstrings entry {key!r}/{ios_locale!r} missing localization")
    string_unit = lang_entry.get("stringUnit", {})
    if not isinstance(string_unit, dict):
        raise SystemExit(f"xcstrings entry {key!r}/{ios_locale!r} has no stringUnit")
    value = string_unit.get("value")
    if not isinstance(value, str):
        raise SystemExit(f"xcstrings entry {key!r}/{ios_locale!r} has non-string value")
    return value


def rewrite_placeholders(template: str) -> str:
    """Translate iOS ``%@``/``%lld``/``%d`` placeholders to i18next ``{{N}}``.

    Positional substitution — the Nth placeholder becomes ``{{N-1}}``
    (zero-based) so it matches the wire shape that backend (Python
    ``{}.format``) and iOS (positional ``%@`` / ``%lld``) both use.

    Args:
        template: Raw xcstrings template string (possibly empty).

    Returns:
        Template with placeholders rewritten. Empty / placeholder-free
        templates are returned unchanged.
    """
    counter = [0]

    def _sub(_match: re.Match[str]) -> str:
        token = "{{" + str(counter[0]) + "}}"
        counter[0] += 1
        return token

    return _PLACEHOLDER_PATTERN.sub(_sub, template)


def nest_keys(flat: dict[str, str]) -> dict[str, object]:
    """Build a nested dict from dotted-key flat mapping.

    ``{"alerts.title.x": "v"}`` -> ``{"alerts": {"title": {"x": "v"}}}``.

    Args:
        flat: Flat key → value mapping. Keys must use ``.`` as the
            nesting separator.

    Returns:
        Nested structure. Raises ``SystemExit`` on key collisions
        (e.g. mixing leaf + nested under the same parent).
    """
    root: dict[str, object] = {}
    for dotted_key, value in flat.items():
        parts = dotted_key.split(".")
        cursor: dict[str, object] = root
        for part in parts[:-1]:
            existing = cursor.get(part)
            if existing is None:
                next_cursor: dict[str, object] = {}
                cursor[part] = next_cursor
                cursor = next_cursor
            elif isinstance(existing, dict):
                cursor = existing
            else:
                raise SystemExit(
                    f"key collision while nesting {dotted_key!r}: "
                    f"intermediate path {'.'.join(parts[: parts.index(part) + 1])!r} "
                    f"is both leaf + nested"
                )
        cursor[parts[-1]] = value
    return root


def build_locale_payload(
    xcstrings: dict[str, object],
    keys: list[str],
    ios_locale: str,
) -> dict[str, object]:
    """Build the ``alerts.json`` payload for one frontend locale.

    The iOS xcstrings keys are ``alerts.title.foo`` etc. The frontend
    catalog file IS the ``alerts`` namespace (loaded via
    ``useTranslation('alerts')``), so we strip the leading ``alerts.``
    before nesting — otherwise the runtime key would be
    ``alerts.alerts.title.foo`` (double-namespaced).

    Args:
        xcstrings: Parsed xcstrings document.
        keys: Alert keys to include (all start with ``alerts.``).
        ios_locale: Source iOS locale code.

    Returns:
        Nested dict ready to JSON-serialize as ``alerts.json``. Looked
        up as ``t('title.foo')`` after ``useTranslation('alerts')``.
    """
    flat: dict[str, str] = {}
    for key in keys:
        if not key.startswith(ALERTS_PREFIX):
            raise SystemExit(
                f"build_locale_payload received non-alerts key {key!r} — "
                f"iter_alert_keys upstream should have filtered it"
            )
        namespaced_key = key[len(ALERTS_PREFIX) :]
        if not namespaced_key:
            raise SystemExit(f"unexpected bare {ALERTS_PREFIX!r} key in xcstrings")
        value = extract_value(xcstrings, key, ios_locale)
        flat[namespaced_key] = rewrite_placeholders(value)
    return nest_keys(flat)


def upsert_nav_alerts(common_path: Path, nav_label: str) -> None:
    """Insert / update ``nav.alerts`` in a locale's ``common.json``.

    Loads the existing file (must exist), ensures the top-level ``nav``
    map exists, and writes ``nav.alerts: nav_label``. Preserves all
    other keys; alphabetically re-sorts only inside ``nav``.

    Args:
        common_path: Path to ``src/locales/<dir>/common.json``.
        nav_label: Localized label for the Alerts tab.
    """
    if not common_path.exists():
        raise SystemExit(f"common.json missing for locale at {common_path}")
    raw = json.loads(common_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit(f"{common_path} is not a JSON object")
    nav = raw.get("nav")
    if nav is None:
        nav = {}
        raw["nav"] = nav
    if not isinstance(nav, dict):
        raise SystemExit(f"{common_path}: 'nav' is not a dict")
    nav["alerts"] = nav_label
    sorted_nav = dict(sorted(nav.items()))
    raw["nav"] = sorted_nav
    rendered = json.dumps(raw, ensure_ascii=False, indent=2) + "\n"
    common_path.write_text(rendered, encoding="utf-8")


def _merge_preserving_frontend_keys(
    alerts_path: Path,
    ios_payload: dict[str, object],
) -> dict[str, object]:
    """Merge ``ios_payload`` with any existing frontend-only top-level keys.

    The xcstrings catalog is the source of truth for keys it owns (e.g.
    ``title``, ``body``, ``alertType``, ``detail``, ``priority``, ``row``,
    ``empty``, ``error``, ``loading``, ``navTitle``, ``accessibility``).
    Frontend-only top-level keys (e.g. ``page`` — the Alerts page header
    subtitle that has no iOS surface) MUST be preserved across regeneration
    runs; otherwise the drift check overwrites them on every CI sweep.

    The merge rule:

    - iOS-owned keys are taken from ``ios_payload`` (verbatim — iOS is the
      single source of truth for these).
    - Top-level keys present in the existing committed file but NOT in
      ``ios_payload`` are carried forward unchanged.

    Returns a new dict sorted by top-level key (alphabetic) for stable
    diffs.
    """
    existing: dict[str, object] = {}
    if alerts_path.exists():
        raw = json.loads(alerts_path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            existing = raw
    merged: dict[str, object] = dict(ios_payload)
    for key, value in existing.items():
        if key not in merged:
            merged[key] = value
    return dict(sorted(merged.items()))


def generate(locales_dir: Path | None = None) -> None:
    """Port the iOS alerts.* xcstrings catalog into all frontend locales.

    Writes one ``alerts.json`` per locale under ``locales_dir`` and
    upserts the ``nav.alerts`` key in each ``common.json``. Idempotent —
    re-running produces identical output.

    Args:
        locales_dir: Target directory containing per-locale subdirs.
            Defaults to ``FRONTEND_LOCALES_DIR`` (the committed
            ``frontend/src/locales/`` path). Tests + the drift checker
            override this to point at a scratch tmpdir.
    """
    target_root = locales_dir if locales_dir is not None else FRONTEND_LOCALES_DIR
    if not XCSTRINGS_PATH.exists():
        raise SystemExit(f"xcstrings not found at {XCSTRINGS_PATH}")
    raw = json.loads(XCSTRINGS_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit("xcstrings did not parse as a JSON object")
    keys = list(iter_alert_keys(raw))
    if not keys:
        raise SystemExit("xcstrings has no alerts.* keys")
    if NAV_LABEL_KEY not in keys:
        raise SystemExit(f"xcstrings missing the {NAV_LABEL_KEY!r} key")
    ios_locales = collect_ios_locales(raw, keys)
    if not target_root.exists():
        raise SystemExit(f"frontend locales dir not found at {target_root}")
    written: list[Path] = []
    for ios_locale in ios_locales:
        frontend_dir_name = map_locale(ios_locale)
        target_dir = target_root / frontend_dir_name
        if not target_dir.exists():
            raise SystemExit(
                f"frontend locale dir missing for iOS locale {ios_locale!r} "
                f"(mapped to {frontend_dir_name!r}): {target_dir}"
            )
        payload = build_locale_payload(raw, keys, ios_locale)
        alerts_path = target_dir / "alerts.json"
        merged = _merge_preserving_frontend_keys(alerts_path, payload)
        rendered = json.dumps(merged, ensure_ascii=False, indent=2) + "\n"
        alerts_path.write_text(rendered, encoding="utf-8")
        nav_label_raw = extract_value(raw, NAV_LABEL_KEY, ios_locale)
        nav_label = rewrite_placeholders(nav_label_raw)
        upsert_nav_alerts(target_dir / "common.json", nav_label)
        written.append(alerts_path)
    print(
        f"Wrote {len(written)} alerts.json + {len(written)} common.json nav.alerts updates "
        f"({len(keys)} alert keys per locale) to {target_root}"
    )


def _read(path: Path) -> str:
    """Read a file's contents; return empty string when absent."""
    if not path.exists():
        return ""

    return path.read_text(encoding="utf-8")


def _check_nav_alerts_drift(committed: Path, regenerated: Path, label: str) -> list[str]:
    """Compare ``nav.alerts`` between committed + regenerated common.json.

    The port script only updates a single key inside each
    ``common.json``, so a byte-level diff would surface unrelated
    edits as false-positive drift. This narrows the comparison to
    just the ``nav.alerts`` value.
    """
    expected = _read(committed)
    actual = _read(regenerated)
    if expected == "" or actual == "":
        return [f"  MISSING: {label}"]
    committed_doc = json.loads(expected)
    regenerated_doc = json.loads(actual)
    committed_label = committed_doc.get("nav", {}).get("alerts")
    regenerated_label = regenerated_doc.get("nav", {}).get("alerts")
    if committed_label is None:
        return [f"  MISSING nav.alerts in committed {label}"]
    if regenerated_label != committed_label:
        return [
            f"  DIFFERS: {label} nav.alerts "
            f"(committed={committed_label!r} != regenerated={regenerated_label!r})"
        ]

    return []


def check_drift() -> int:
    """Re-run the port script in a tmpdir, diff against committed files.

    Returns:
        ``0`` when committed locales already match what the script
        would emit. ``1`` with a summary printed to stderr when any
        file differs / is missing.
    """
    if not FRONTEND_LOCALES_DIR.exists():
        print(
            f"frontend locales dir not found at {FRONTEND_LOCALES_DIR}",
            file=sys.stderr,
        )
        return 1
    committed_dir = FRONTEND_LOCALES_DIR
    with tempfile.TemporaryDirectory() as tmp:
        scratch = Path(tmp) / "locales"
        shutil.copytree(committed_dir, scratch)
        generate(locales_dir=scratch)
        diffs: list[str] = []
        for locale_dir in sorted(scratch.iterdir()):
            if not locale_dir.is_dir():
                continue
            committed_alerts = committed_dir / locale_dir.name / "alerts.json"
            regenerated_alerts = locale_dir / "alerts.json"
            if _read(committed_alerts) != _read(regenerated_alerts):
                if not committed_alerts.exists():
                    diffs.append(f"  MISSING: {locale_dir.name}/alerts.json — re-run port script")
                else:
                    diffs.append(
                        f"  DIFFERS: {locale_dir.name}/alerts.json (committed != regenerated)"
                    )
            diffs.extend(
                _check_nav_alerts_drift(
                    committed_dir / locale_dir.name / "common.json",
                    locale_dir / "common.json",
                    f"{locale_dir.name}/common.json",
                )
            )
        if diffs:
            print("Alerts catalog drift detected:", file=sys.stderr)
            for line in diffs:
                print(line, file=sys.stderr)
            print(
                "Re-run `python scripts/port_ios_alert_catalog.py` and commit the result.",
                file=sys.stderr,
            )
            return 1

    return 0


def main() -> int:
    """Entry point for ``scripts/port_ios_alert_catalog.py``.

    Returns:
        ``0`` on success. ``generate()`` raises ``SystemExit(...)`` on
        every failure path, and ``check_drift()`` returns 1 with a
        diff summary on drift.
    """
    parser = argparse.ArgumentParser(description=__doc__ or "")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Drift mode: write to a tmpdir + diff against committed files. Non-zero on drift.",
    )
    args = parser.parse_args()
    if args.check:
        return check_drift()
    generate()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
