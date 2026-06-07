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

This module is consumed to localize APNs title/body and REST
alert-history responses based on ``user.default_language``.
"""

import json
from pathlib import Path
from typing import Final

from loguru import logger

from snapper.core.json_types import JsonObject
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
    (currently a subset, scoped to alerts.*).

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


def resolve_alert_strings(
    *,
    payload: JsonObject | None,
    fallback_title: str,
    fallback_body: str,
    user_language: str | None,
    log_context: str | None = None,
) -> tuple[str, str]:
    """Resolve ``(title, body)`` for one alert row, with full EN fallback.

    Single funnel used by both the APNs sidecar (push notifications)
    and the REST alert-history endpoints. The function is intentionally
    payload-shape-aware rather than ``AlertEventRow``-aware so it can
    live in ``snapper.i18n`` without depending on the data layer.

    Falls back to ``(fallback_title, fallback_body)`` (the stored EN
    columns at the call site) when any of:

    - ``user_language`` is ``None`` (user never set a preference)
    - ``payload`` lacks string ``title_loc_key`` / ``body_loc_key``
      (legacy row predating localization, or a rule that opted out)
    - ``title_loc_args`` / ``body_loc_args`` are not lists
    - The catalog lookup misses (``render`` returns the key verbatim)
      on either field — all-or-nothing so the alert never goes out
      half-localized
    - ``catalog.render`` raises ``ValueError`` (template arity drift)

    Args:
        payload: ``AlertEventRow.payload`` JSON (or ``None``).
        fallback_title: Stored EN ``title`` column to return on any
            failure path.
        fallback_body: Stored EN ``body`` column to return on any
            failure path.
        user_language: Recipient's ``User.default_language``
            preference, or ``None`` when never set.
        log_context: Identifier embedded in the render-failure warn
            log (e.g. ``"alert_event=<public_id>"``) — ``None``
            suppresses the contextual tail.

    Returns:
        Two-tuple of ``(title, body)`` strings ready to use in either
        an APNs ``aps.alert`` dict or a REST response field.
    """
    if user_language is None or payload is None:
        return fallback_title, fallback_body
    title_loc_key = payload.get("title_loc_key")
    body_loc_key = payload.get("body_loc_key")
    if not isinstance(title_loc_key, str) or not isinstance(body_loc_key, str):
        return fallback_title, fallback_body
    title_args_raw = payload.get("title_loc_args") or []
    body_args_raw = payload.get("body_loc_args") or []
    if not isinstance(title_args_raw, list) or not isinstance(body_args_raw, list):
        return fallback_title, fallback_body
    title_args = [str(a) for a in title_args_raw]
    body_args = [str(a) for a in body_args_raw]
    try:
        title = render(title_loc_key, user_language, *title_args)
        body = render(body_loc_key, user_language, *body_args)
    except ValueError as exc:
        context = f" ({log_context})" if log_context else ""
        logger.warning(
            "i18n: catalog render mismatch{ctx} for lang={lang} — falling back to EN. {err}",
            ctx=context,
            lang=user_language,
            err=exc,
        )
        return fallback_title, fallback_body
    if title == title_loc_key or body == body_loc_key:
        return fallback_title, fallback_body
    return title, body
