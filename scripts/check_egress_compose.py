"""Lint hook: enforce snapper-egress sidecar safety + unified-image invariants.

Two categories of rules enforced on the snapper-egress service block:

1. **Isolation rules**: no ``ports:`` host publish
   (the SOCKS5 listener has NO authentication), no ``network_mode:
   host``, no env-interpolated bypass of either field.

2. **Unified-image rules**: when the
   monolith + sidecar share one image, certain compose attributes
   MUST be present or absent to keep the runtime contract correct.
   Specifically:
       - snapper-egress MUST use the same ``image:`` string as the
         snapper service (single-image deployment invariant).
       - snapper-egress MUST declare ``command: ["egress"]`` (locks
         the CLI dispatch for ``ENTRYPOINT ["snapper"]``).
       - snapper-egress MUST declare ``cap_add: [NET_ADMIN]``
         (required for kernel WG netlink writes).
       - snapper-egress MUST declare ``user: "0:0"`` (root) — the
         unified image defaults to ``USER snapper`` (UID 888), which
         would fail ``pyroute2.WireGuard`` syscalls even with
         NET_ADMIN. This is the sidecar root-user invariant.
       - snapper service MUST NOT declare ``cap_add`` (monolith stays
         unprivileged even though it shares the image).
       - snapper service MUST NOT declare ``user:`` (inherits secure
         default ``USER snapper`` from the image).
       - if snapper mounts SQLite data at ``/app/data``,
         snapper-egress MUST mount the same target so it can read
         the shared settings database.

The script exits with code 1 if any rule is violated. Invoked by
``make check-egress-compose`` and (transitively) ``make check-all``.
"""

import sys
from collections.abc import Mapping
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
output — documented in the operator runbook (docs/snapper-egress.md).
"""

_EGRESS_SERVICE_NAME: Final[str] = "snapper-egress"
"""Service name pinned by the sidecar plan; the lint hook matches on this exactly."""

_MONOLITH_SERVICE_NAME: Final[str] = "snapper"
"""Monolith (FastAPI + bootstrap) service name used for unified-image cross-checks.

The unified-image build enforces that this service shares its ``image:``
with the sidecar and that the monolith does NOT declare ``cap_add`` or
``user:`` overrides (the image's secure default USER snapper inherits).
"""

_REQUIRED_EGRESS_COMMAND: Final[tuple[str, ...]] = ("egress",)
"""Sidecar command must be exactly ``["egress"]`` under the unified image.

Under the unified ``ENTRYPOINT ["snapper"]`` the sidecar is
dispatched via the new ``snapper egress`` CLI subcommand. Any
deviation (e.g. ``["python", "-m", "snapper.egress"]``) would still
work today but breaks the lint invariant + the documented operator
contract.
"""

_REQUIRED_EGRESS_USER: Final[str] = "0:0"
"""Sidecar must run as root under the unified image.

The image defaults to ``USER snapper`` (UID 888) for the monolith
path. Sidecar overrides via ``user: "0:0"`` so ``pyroute2`` netlink
syscalls + ``ip link add type wireguard`` succeed even with the
NET_ADMIN capability already granted.
"""

_REQUIRED_EGRESS_CAP: Final[str] = "NET_ADMIN"
"""Sidecar must have CAP_NET_ADMIN. Kernel WG via pyroute2 needs it."""

_SQLITE_DATA_VOLUME_TARGET: Final[str] = "/app/data"
"""Shared SQLite data directory target used by the monolith and sidecar."""

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


def _volume_target(volume: object) -> str | None:
    """Return the container target path from a Compose volume entry."""
    if isinstance(volume, str):
        parts = volume.split(":")
        if len(parts) >= 2:
            return parts[1]
        return None
    if isinstance(volume, Mapping):
        target = volume.get("target")
        if isinstance(target, str):
            return target
    return None


def _has_volume_target(service: dict[str, object], target: str) -> bool:
    """Return whether ``service.volumes`` contains the requested target."""
    volumes = service.get("volumes") or []
    if not isinstance(volumes, list):
        return False
    return any(_volume_target(volume) == target for volume in volumes)


def _check_unified_image_invariants(services: dict[str, object], compose_path: Path) -> list[str]:
    """Return errors for the unified-image cross-service invariants.

    Runs after the per-service isolation check. Skipped silently
    when either service is absent (allows partial-override compose
    files that don't redeclare every service).

    Args:
        services: The ``services:`` block of one compose file, already
            parsed by ``yaml.safe_load``.
        compose_path: Path to the compose file (used in error messages
            for operator legibility).

    Returns:
        List of human-readable violation strings; empty when the
        invariants hold (or when the relevant services are absent).
    """
    errors: list[str] = []
    monolith = services.get(_MONOLITH_SERVICE_NAME)
    egress = services.get(_EGRESS_SERVICE_NAME)
    if not isinstance(monolith, dict) or not isinstance(egress, dict):
        return errors
    monolith_image = monolith.get("image")
    egress_image = egress.get("image")
    if monolith_image != egress_image:
        errors.append(
            f"{compose_path}: '{_MONOLITH_SERVICE_NAME}.image' "
            f"({monolith_image!r}) must match "
            f"'{_EGRESS_SERVICE_NAME}.image' ({egress_image!r}) — "
            "the unified-image deployment requires both services to "
            "consume the same image tag."
        )
    egress_command = egress.get("command")
    if egress_command != list(_REQUIRED_EGRESS_COMMAND):
        errors.append(
            f"{compose_path}: '{_EGRESS_SERVICE_NAME}.command' must "
            f"be {list(_REQUIRED_EGRESS_COMMAND)!r}, got "
            f"{egress_command!r}. Under ``ENTRYPOINT ['snapper']`` "
            "the sidecar dispatches via the ``snapper egress`` CLI "
            "subcommand."
        )
    egress_user = egress.get("user")
    if egress_user != _REQUIRED_EGRESS_USER:
        errors.append(
            f"{compose_path}: '{_EGRESS_SERVICE_NAME}.user' must be "
            f"{_REQUIRED_EGRESS_USER!r}, got {egress_user!r}. The "
            "unified image defaults to USER snapper (UID 888); "
            "pyroute2 + kernel WG syscalls require root even with "
            "CAP_NET_ADMIN."
        )
    egress_caps = egress.get("cap_add") or []
    if _REQUIRED_EGRESS_CAP not in egress_caps:
        errors.append(
            f"{compose_path}: '{_EGRESS_SERVICE_NAME}.cap_add' must "
            f"include {_REQUIRED_EGRESS_CAP!r}, got {egress_caps!r}. "
            "Kernel WireGuard needs CAP_NET_ADMIN."
        )
    if _has_volume_target(monolith, _SQLITE_DATA_VOLUME_TARGET) and not _has_volume_target(
        egress,
        _SQLITE_DATA_VOLUME_TARGET,
    ):
        errors.append(
            f"{compose_path}: '{_EGRESS_SERVICE_NAME}.volumes' must include "
            f"a volume targeting {_SQLITE_DATA_VOLUME_TARGET!r} when "
            f"'{_MONOLITH_SERVICE_NAME}.volumes' uses that target. The "
            "sidecar reads the same SQLite settings database during "
            "local/default deployments."
        )
    if monolith.get("cap_add"):
        errors.append(
            f"{compose_path}: '{_MONOLITH_SERVICE_NAME}.cap_add' must "
            f"NOT be declared (got {monolith.get('cap_add')!r}); the "
            "monolith service runs unprivileged even though it shares "
            "the image with the sidecar."
        )
    if "user" in monolith:
        errors.append(
            f"{compose_path}: '{_MONOLITH_SERVICE_NAME}.user' must "
            f"NOT be declared (got {monolith.get('user')!r}); the "
            "monolith inherits the secure default USER snapper from "
            "the image."
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
        errors.extend(_check_unified_image_invariants(services, compose_path))
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
