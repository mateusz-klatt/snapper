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
