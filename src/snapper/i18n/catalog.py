"""Backend i18n catalog — load + lookup + render.

Loads the generated per-language JSONs at import time and exposes
``localized()`` / ``render()`` helpers.

The catalogs at ``src/snapper/i18n/catalogs/<lang>.json`` are generated
by ``scripts/gen_backend_i18n_catalog.py`` from the iOS xcstrings file
and committed to the repo so production Docker images don't need to
copy ``ios/``.

Lookup semantics mirror the iOS ``LocaleStrings`` helper:

- ``localized(key, language)`` returns the template string for that
  language; falls back to ``en`` on miss; returns ``key`` on EN miss
  (key not in catalog).
- ``render(key, language, *args)`` looks up the template AND
  substitutes positional arguments via ``snapper.i18n.format.render``.

This module is consumed by Phase C+D of
``plan_2026_05_17_backend_user_language_i18n.md`` to localize APNs
title/body and REST alert-history responses based on
``user.default_language``.
"""

import json
from pathlib import Path
from typing import Final

from snapper.i18n.format import render as render_template

_CATALOG_DIR: Final[Path] = Path(__file__).resolve().parent / "catalogs"

_EN: Final[str] = "en"


def load_catalogs_from(catalog_dir: Path) -> dict[str, dict[str, str]]:
    """Read every ``<lang>.json`` file under ``catalog_dir``.

    Exposed (rather than module-private) so unit tests can exercise the
    error paths by pointing at crafted bad fixtures.

    Args:
        catalog_dir: Directory containing one ``<lang>.json`` per
            catalog language.

    Returns:
        ``{language: {key: template}}`` map.

    Raises:
        FileNotFoundError: When ``catalog_dir`` does not exist or the
            EN fallback file is missing — both are required invariants.
        ValueError: When a JSON file does not parse as ``{str: str}``.
    """
    if not catalog_dir.is_dir():
        raise FileNotFoundError(
            f"Backend i18n catalog directory missing at {catalog_dir}. "
            f"Run `python scripts/gen_backend_i18n_catalog.py`."
        )
    catalogs: dict[str, dict[str, str]] = {}
    for path in sorted(catalog_dir.glob("*.json")):
        lang = path.stem
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"Catalog file {path} did not parse as a JSON object")
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in data.items()):
            raise ValueError(f"Catalog file {path} has non-string keys/values")
        catalogs[lang] = data
    if _EN not in catalogs:
        raise FileNotFoundError(
            f"Backend i18n catalog is missing the EN fallback file "
            f"at {catalog_dir / f'{_EN}.json'}. EN is the source-of-truth "
            f"fallback for unrecognized languages."
        )
    return catalogs


_CATALOGS: Final[dict[str, dict[str, str]]] = load_catalogs_from(_CATALOG_DIR)


def supported_catalog_languages() -> frozenset[str]:
    """Return the set of language codes that have an installed catalog.

    Mirrors ``snapper.i18n.supported_languages.SUPPORTED_LANGUAGES`` for
    the codes that actually have backend-side translation content
    (subset until Phase B expands beyond alerts.*).

    Returns:
        Frozenset of installed catalog language codes.
    """
    return frozenset(_CATALOGS.keys())


def localized(key: str, language: str) -> str:
    """Look up the template string for ``(key, language)``.

    Args:
        key: Catalog key (e.g. ``"alerts.body.order_fill_full"``).
        language: Catalog language code (e.g. ``"pl"``, ``"zh-Hans"``).

    Returns:
        The template string for that language. Falls back to the EN
        template if ``language`` has no catalog (e.g. a frontend-only
        code like ``"pt"`` against the iOS catalog set). Falls back to
        ``key`` itself if the key isn't in the EN catalog either.
    """
    catalog = _CATALOGS.get(language)
    if catalog is not None:
        value = catalog.get(key)
        if value is not None:
            return value
    en_catalog = _CATALOGS[_EN]
    return en_catalog.get(key, key)


def render(key: str, language: str, *args: object) -> str:
    """Look up + substitute in one call.

    Args:
        key: Catalog key.
        language: Catalog language code.
        *args: Positional arguments substituted into the template's
            ``%@`` / ``%lld`` placeholders, in source order.

    Returns:
        The rendered string. If the lookup falls back to the key
        itself (catalog miss), ``args`` are silently ignored —
        protects against runtime crashes on a missing catalog entry.
    """
    template = localized(key, language)
    if template == key:
        return key
    return render_template(template, args)
