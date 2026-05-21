"""Lint hook: enforce snapper-egress sidecar has NO host port mapping.

SC.3 of ``proprietary/plans/plan_2026_05_21_snapper_egress_sidecar.md``.

The sidecar runs an unauthenticated SOCKS5 listener and the
healthcheck HTTP endpoint on port 8081. Isolation comes entirely
from the Docker private network — there must be NO ``ports:``
entry on the snapper-egress service (which would publish the
listener to the host) and NO ``network_mode: host``.

The script exits with code 1 if the constraint is violated. Invoked
by ``make check-egress-compose`` and (transitively) ``make
check-all``. Operators who genuinely need to expose the sidecar
(testing only) must remove this hook explicitly — silent override
is intentionally not supported.
"""

import sys
from pathlib import Path
from typing import Final

import yaml

_COMPOSE_FILES: Final[tuple[str, ...]] = (
    "docker-compose.yml",
    "docker-compose.yaml",
    "docker-compose.prod.yml",
    "docker-compose.prod.yaml",
    "docker-compose.override.yml",
    "docker-compose.override.yaml",
    "compose.yml",
    "compose.yaml",
    "compose.prod.yml",
    "compose.prod.yaml",
    "compose.override.yml",
    "compose.override.yaml",
)
"""Compose + override file names the lint hook scans.

The Compose v2 merge-rule allows override files to silently add
``ports:`` / ``network_mode: host`` to the snapper-egress service.
Scanning every recognised name catches the common bypass routes.
For absolute coverage operators should also run
``docker compose config`` against their final stack and grep the
output — documented in the SC.5 operator runbook.
"""

_EGRESS_SERVICE_NAME: Final[str] = "snapper-egress"
"""Service name pinned by the sidecar plan; the lint hook matches on this exactly."""

_INTERPOLATION_MARKER: Final[str] = "${"
"""Compose variable-interpolation prefix.

Catches the obvious bypass ``network_mode: ${EGRESS_NETWORK_MODE:-host}``
or ``ports: ["${EGRESS_HOST_PORT}:8081"]`` at lint time. False
positives are acceptable — any production deployment that genuinely
needs interpolation on these fields should override the hook with
clear intent rather than relying on env-driven flexibility.
"""


def _check_service(service: dict[str, object], compose_path: Path) -> list[str]:
    """Return a list of error strings for one service definition.

    A service is considered offending if it:

    * declares any ``ports:`` entry (which would publish the SOCKS5
      / healthcheck port to the host network), or
    * sets ``network_mode: host`` (which would route the listener
      through the host's network namespace and bypass the Docker
      private-network boundary), or
    * relies on ``${VAR}`` env interpolation on either field
      (catches ``network_mode: ${MODE:-host}`` style bypasses
      that look clean at static lint time but resolve to host
      networking at runtime).
    """
    errors: list[str] = []
    ports = service.get("ports")
    if ports:
        errors.append(
            f"{compose_path}: snapper-egress service has 'ports:' "
            f"{ports!r} — host port mapping is forbidden (the SOCKS5 "
            "listener has NO authentication)."
        )
    if isinstance(ports, list):
        for entry in ports:
            if isinstance(entry, str) and _INTERPOLATION_MARKER in entry:
                errors.append(
                    f"{compose_path}: snapper-egress 'ports:' uses env "
                    f"interpolation {entry!r} — env-driven host-port "
                    "mapping is not allowed (use a static override "
                    "file with no host publish if you really need this)."
                )
    network_mode = service.get("network_mode")
    if network_mode == "host":
        errors.append(
            f"{compose_path}: snapper-egress service uses "
            "network_mode: host — host networking bypasses the "
            "Docker private-network isolation that protects the "
            "unauthenticated SOCKS5 listener."
        )
    if isinstance(network_mode, str) and _INTERPOLATION_MARKER in network_mode:
        errors.append(
            f"{compose_path}: snapper-egress 'network_mode' uses env "
            f"interpolation {network_mode!r} — env-driven host networking "
            "is not allowed (the runtime value could resolve to host)."
        )
    return errors


def main(root: Path | None = None) -> int:
    """Scan known Compose files for snapper-egress safety violations.

    Args:
        root: Optional project root path; defaults to the current
            working directory. Tests pass an explicit Path so they
            can build temporary compose files in a fixture.

    Returns:
        ``0`` when every Compose file either lacks the snapper-egress
        service entirely OR declares it with no host port mapping
        and no host networking. ``1`` on any violation.
    """
    project_root = root or Path.cwd()
    errors: list[str] = []
    found_any = False
    for filename in _COMPOSE_FILES:
        compose_path = project_root / filename
        if not compose_path.exists():
            continue
        found_any = True
        with compose_path.open(encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        services = data.get("services") or {}
        egress = services.get(_EGRESS_SERVICE_NAME)
        if egress is None:
            continue
        errors.extend(_check_service(egress, compose_path))
    if not found_any:
        print("check_egress_compose: no compose file found — nothing to lint")
        return 0
    if errors:
        for err in errors:
            print(err, file=sys.stderr)
        return 1
    print(
        f"check_egress_compose: {_EGRESS_SERVICE_NAME} service "
        "lint passed (no host port mapping, no host networking)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
