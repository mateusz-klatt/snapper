"""Compose contract for the non-root Caddy dashboard service."""

from pathlib import Path
from typing import Final

import yaml

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent
_COMPOSE_FILE: Final[Path] = _REPO_ROOT / "docker-compose.yml"


def _web_service() -> dict[str, object]:
    """Return the parsed ``snapper-web`` service mapping."""
    document = yaml.safe_load(_COMPOSE_FILE.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    services = document.get("services")
    assert isinstance(services, dict)
    service = services.get("snapper-web")
    assert isinstance(service, dict)
    return service


def test_caddy_uses_service_local_writable_xdg_directories() -> None:
    """Caddy state uses ephemeral writable paths without a shared image home.

    Given: The non-root dashboard service in the Compose contract,
    When: Its XDG environment is inspected,
    Then: Caddy state is directed to exact service-local writable paths.
    """
    service = _web_service()
    environment = service.get("environment")

    assert environment == {
        "XDG_CONFIG_HOME": "/tmp/caddy-config",
        "XDG_DATA_HOME": "/tmp/caddy-data",
    }


def test_caddy_does_not_override_the_non_root_image_user() -> None:
    """The web service inherits the runtime image's ``USER snapper`` contract.

    Given: The dashboard service built from the shared non-root image,
    When: Its Compose user configuration is inspected,
    Then: No service-level user override weakens the image contract.
    """
    assert "user" not in _web_service()


_CADDY_FILE: Final[Path] = _REPO_ROOT / "docker" / "web" / "Caddyfile"
_REDACTED_QUERY_KEYS: Final[tuple[str, ...]] = (
    "state",
    "code",
    "code_challenge",
    "code_verifier",
    "request",
    "client_secret",
    "token",
    "refresh_token",
    "access_token",
)


def _log_filter_blocks() -> list[str]:
    """Return every ``format filter`` block of the real Caddyfile."""
    text = _CADDY_FILE.read_text(encoding="utf-8")
    blocks: list[str] = []
    start = text.find("format filter {")
    while start != -1:
        depth = 0
        for index in range(start, len(text)):
            depth += {"{": 1, "}": -1}.get(text[index], 0)
            if depth == 0 and text[index] == "}":
                blocks.append(text[start : index + 1])
                break
        start = text.find("format filter {", start + 1)
    return blocks


def test_caddy_redacts_oauth_credentials_in_access_and_error_logs() -> None:
    """Both the site access log and the default (error) log filter OAuth credentials.

    Given: The real Caddyfile, verified against caddy:2-alpine v2.11.9 with sentinels,
    When: Its log filter blocks are inspected,
    Then: The global default log and the :8000 access log both replace every
        credential-bearing query key, delete the consent capability header, and
        drop the response Location, whose authorize redirect carries the capability
        in its fragment.
    """
    blocks = _log_filter_blocks()

    assert len(blocks) == 2
    for block in blocks:
        for key in _REDACTED_QUERY_KEYS:
            assert f"replace {key} REDACTED" in block
        assert "request>headers>X-Mcp-Oauth-Request delete" in block
        assert "resp_headers>Location delete" in block


def test_caddy_spa_fallback_serves_the_consent_route() -> None:
    """The catch-all serves index.html, so ``/oauth/authorize`` reaches the SPA.

    Given: The :8000 site of the real Caddyfile,
    When: Its routes are inspected,
    Then: No handle claims ``/oauth`` and the catch-all falls back to ``/index.html``.
    """
    text = _CADDY_FILE.read_text(encoding="utf-8")

    assert "handle /oauth" not in text
    assert "try_files {path} /index.html" in text
