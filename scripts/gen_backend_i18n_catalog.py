"""Generate backend-consumable JSON catalogs from the iOS xcstrings file.

Reads ``ios/Snapper/Resources/Localization/Localizable.xcstrings`` and
writes one JSON file per language to
``src/snapper/i18n/catalogs/<lang>.json`` covering the ``alerts.*``
title/body namespace (12 keys × 45 languages = 540 entries).

Rationale: the backend Docker image does NOT include ``ios/``, so the
catalog cannot be read at runtime. This generator runs at build time
and the resulting JSON files are committed to the repo so production
deployments have them baked in.

Drift between the source xcstrings and the committed catalogs is
enforced by ``scripts/check_type_drift.py`` — CI fails if the
generator output differs from what's checked in.

Usage:

    python scripts/gen_backend_i18n_catalog.py

Exits 0 on success. Exits 1 with a diff summary if any source file is
missing or unparseable.
"""

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Final

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
XCSTRINGS_PATH: Final[Path] = REPO_ROOT / "ios/Snapper/Resources/Localization/Localizable.xcstrings"
CATALOG_DIR: Final[Path] = REPO_ROOT / "src/snapper/i18n/catalogs"

CATALOG_NAMESPACES: Final[tuple[str, ...]] = ("alerts.title.", "alerts.body.")


def iter_catalog_keys(xcstrings: dict[str, object]) -> Iterable[str]:
    """Yield xcstrings keys filtered to ``alerts.{title,body}.*``.

    The backend i18n surface is limited to alert title/body templates for
    push notifications and REST history. Other namespaces (auth, settings,
    common) stay client-only.

    Args:
        xcstrings: Parsed xcstrings document (``{"sourceLanguage": ...,
            "strings": {key: ...}}``).

    Yields:
        Keys matching one of ``CATALOG_NAMESPACES``.
    """
    strings = xcstrings.get("strings", {})
    if not isinstance(strings, dict):
        raise SystemExit("xcstrings 'strings' is not a dict — file corrupt?")
    for key in sorted(strings):
        if any(key.startswith(prefix) for prefix in CATALOG_NAMESPACES):
            yield key


def collect_languages(xcstrings: dict[str, object], keys: list[str]) -> list[str]:
    """Resolve the set of catalog languages present across the keys.

    Every key must have the same language set (CatalogParityTests
    enforces this in iOS); we sample the first key and validate the
    rest match.

    Args:
        xcstrings: Parsed xcstrings document.
        keys: Catalog keys returned by :func:`iter_catalog_keys`.

    Returns:
        Sorted list of language codes (e.g. ``["ar", "bn", ..., "zh-Hant"]``).
    """
    strings = xcstrings["strings"]
    if not isinstance(strings, dict) or not keys:
        raise SystemExit("xcstrings empty or has no alerts.* keys")
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


def extract_value(xcstrings: dict[str, object], key: str, language: str) -> str:
    """Pull the source-language string for ``(key, language)``.

    Args:
        xcstrings: Parsed xcstrings document.
        key: Catalog key.
        language: BCP-47 / catalog language code.

    Returns:
        The string value to write into the backend catalog.
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
    lang_entry = localizations.get(language)
    if not isinstance(lang_entry, dict):
        raise SystemExit(f"xcstrings entry {key!r}/{language!r} missing localization")
    string_unit = lang_entry.get("stringUnit", {})
    if not isinstance(string_unit, dict):
        raise SystemExit(f"xcstrings entry {key!r}/{language!r} has no stringUnit")
    value = string_unit.get("value")
    if not isinstance(value, str):
        raise SystemExit(f"xcstrings entry {key!r}/{language!r} has non-string value")
    return value


def generate() -> None:
    """Generate backend catalog JSONs from xcstrings.

    Writes one JSON per language to ``CATALOG_DIR``. Each file maps
    ``{key: template_string}`` for the alerts.* namespace.

    Output filenames are the catalog language code unchanged
    (``en.json``, ``pl.json``, ``pt-BR.json``, ``sr-Latn.json`` etc.).
    Compound codes with a dash are preserved as-is in the filename.
    """
    if not XCSTRINGS_PATH.exists():
        raise SystemExit(f"xcstrings not found at {XCSTRINGS_PATH}")
    raw = json.loads(XCSTRINGS_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit("xcstrings did not parse as a JSON object")
    keys = list(iter_catalog_keys(raw))
    if not keys:
        raise SystemExit(f"xcstrings has no keys matching {CATALOG_NAMESPACES}")
    languages = collect_languages(raw, keys)
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for lang in languages:
        catalog = {key: extract_value(raw, key, lang) for key in keys}
        out_path = CATALOG_DIR / f"{lang}.json"
        rendered = json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        out_path.write_text(rendered, encoding="utf-8")
        written.append(out_path)
    print(f"Wrote {len(written)} catalog files ({len(keys)} keys each) to {CATALOG_DIR}")


def main() -> int:
    """Entry point for ``scripts/gen_backend_i18n_catalog.py``.

    Returns:
        ``0`` on success. ``generate()`` itself ``raise SystemExit(...)``
        on every failure path, so the only successful return is 0.
    """
    generate()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
