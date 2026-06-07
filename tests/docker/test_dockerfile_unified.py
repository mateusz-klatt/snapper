"""Smoke tests for the unified Dockerfile.

After the unified build merged the per-mode `docker/Dockerfile.egress` into
the root `Dockerfile`, these tests lock the structural invariants that
keep both monolith and sidecar modes working from one image:

- The old per-mode Dockerfile is GONE (guard against re-creation).
- The runtime stage installs every dep both modes need (the union).
- The runtime stage sets ``USER snapper`` as the default secure user.
- The runtime stage exports ``SERVER_HOST=0.0.0.0`` (without it,
  ``snapper server`` falls back to 127.0.0.1 and the compose port
  publish becomes unreachable from the host).
- The runtime stage's ENTRYPOINT is ``["snapper"]`` and CMD is
  ``["server"]`` so the default container runs the monolith;
  sidecar overrides CMD via compose to ``["egress"]``.
"""

from pathlib import Path
from typing import Final

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent
_DOCKERFILE: Final[Path] = _REPO_ROOT / "Dockerfile"
_LEGACY_EGRESS_DOCKERFILE: Final[Path] = _REPO_ROOT / "docker" / "Dockerfile.egress"


def _runtime_stage_lines() -> list[str]:
    """Return the lines belonging to the ``runtime`` build stage.

    The unified Dockerfile carries multiple ``FROM ... AS <name>``
    stages: ``ui-build``, ``py-build``, ``runtime`` (the snapper Python
    runtime that serves the REST + WS surface, runs the egress
    sidecar, etc.), and ``web-runtime`` (the Caddy nginx image that
    serves the React dashboard as a separate container). Tests
    against the snapper runtime layer match the ``runtime`` stage
    explicitly by name so the harness keeps working when additional
    stages land at the end of the file.
    """
    text = _DOCKERFILE.read_text()
    runtime_marker = "FROM python:3.14-slim AS runtime"
    start = text.find(runtime_marker)
    assert start != -1, f"Dockerfile must contain `{runtime_marker}` stage"
    next_from = text.find("\nFROM ", start + len(runtime_marker))
    runtime_block = text[start:next_from] if next_from != -1 else text[start:]
    return runtime_block.splitlines()


def test_dockerfile_exists() -> None:
    """Spec — single Dockerfile at repo root.

    Given the unified Dockerfile,
    When inspecting the repo root,
    Then `Dockerfile` exists.
    """
    assert _DOCKERFILE.exists()
    assert _DOCKERFILE.is_file()


def test_legacy_egress_dockerfile_is_gone() -> None:
    """Spec — `docker/Dockerfile.egress` is removed.

    Given the unified Dockerfile replaces the per-mode sidecar Dockerfile,
    When inspecting the repo,
    Then `docker/Dockerfile.egress` does NOT exist. Guard against
        accidental re-creation that would re-introduce the Docker
        Hub 1-private-repo problem.
    """
    assert not _LEGACY_EGRESS_DOCKERFILE.exists(), (
        f"{_LEGACY_EGRESS_DOCKERFILE} must NOT exist under the unified Dockerfile; "
        "the unified Dockerfile at repo root replaces it."
    )


def test_runtime_stage_installs_iproute2_and_wireguard_tools() -> None:
    """Spec — runtime apt-install line includes sidecar's deps.

    Given the unified Dockerfile runtime stage,
    When scanning its package list,
    Then `iproute2` and `wireguard-tools` are present so the sidecar
        can configure policy routing and the operator can run
        `wg show` for diagnostics.
    """
    runtime = _runtime_stage_lines()
    blob = "\n".join(runtime)
    assert "iproute2" in blob, "runtime stage must install iproute2"
    assert "wireguard-tools" in blob, "runtime stage must install wireguard-tools"


def test_runtime_stage_installs_monolith_deps() -> None:
    """Spec — runtime stage keeps every monolith dep.

    Given the unified Dockerfile runtime stage,
    When scanning its package list,
    Then `ca-certificates` (TLS), `curl` (HEALTHCHECK), and
        `unixodbc` (ODBC drivers) all survive the merge from the
        pre-unification main Dockerfile.
    """
    runtime = _runtime_stage_lines()
    blob = "\n".join(runtime)
    assert "ca-certificates" in blob
    assert "curl" in blob
    assert "unixodbc" in blob


def test_runtime_stage_sets_server_host_zero_zero_zero_zero() -> None:
    """Spec — `SERVER_HOST=0.0.0.0` is in the runtime ENV block.

    Given the unified Dockerfile runtime stage,
    When inspecting the ENV directive,
    Then `SERVER_HOST=0.0.0.0` is present so `snapper server` binds
        0.0.0.0 (otherwise the compose `127.0.0.1:8000:8000` publish
        becomes unreachable from the host).
    """
    runtime = _runtime_stage_lines()
    blob = "\n".join(runtime)
    assert "SERVER_HOST=0.0.0.0" in blob, (
        "runtime stage ENV must export SERVER_HOST=0.0.0.0 so "
        "`snapper server` binds 0.0.0.0 (otherwise the compose port "
        "publish is unreachable)."
    )


def test_runtime_stage_default_user_is_snapper() -> None:
    """Spec — runtime stage USER directive is `snapper` (UID 888).

    Given the unified Dockerfile runtime stage,
    When inspecting the final USER directive,
    Then it is `USER snapper`. Monolith inherits this secure default;
        sidecar overrides via compose `user: "0:0"` because kernel WG
        syscalls need root.
    """
    runtime = _runtime_stage_lines()
    user_lines = [line for line in runtime if line.strip().startswith("USER ")]
    assert user_lines, "runtime stage must declare USER"
    assert (
        user_lines[-1].strip() == "USER snapper"
    ), f"runtime stage USER must be 'snapper', got {user_lines[-1].strip()!r}"


def test_runtime_stage_entrypoint_is_snapper() -> None:
    """Spec — ENTRYPOINT is `["snapper"]`.

    Given the unified Dockerfile runtime stage,
    When inspecting the ENTRYPOINT directive,
    Then it is `["snapper"]`. Combined with `CMD ["server"]` the
        default invocation is `snapper server` (monolith); sidecar
        overrides CMD via compose to `["egress"]` for `snapper egress`
        (the egress CLI subcommand).
    """
    runtime = _runtime_stage_lines()
    entrypoint_lines = [line for line in runtime if line.strip().startswith("ENTRYPOINT")]
    assert entrypoint_lines, "runtime stage must declare ENTRYPOINT"
    assert '["snapper"]' in entrypoint_lines[-1]


def test_runtime_stage_default_cmd_is_server() -> None:
    """Spec — CMD is `["server"]` (default monolith dispatch).

    Given the unified Dockerfile runtime stage,
    When inspecting the CMD directive,
    Then it is `["server"]`. Sidecar overrides via compose to
        `["egress"]`.
    """
    runtime = _runtime_stage_lines()
    cmd_lines = [line for line in runtime if line.strip().startswith("CMD") and "[" in line]
    assert cmd_lines, "runtime stage must declare CMD"
    assert '["server"]' in cmd_lines[-1]


def test_runtime_stage_exposes_both_ports() -> None:
    """Spec — runtime EXPOSE declares both 8000 (monolith) and 8081 (sidecar).

    Given the unified Dockerfile runtime stage,
    When inspecting the EXPOSE directive,
    Then both `8000` and `8081` are declared at the image level so
        either mode can bind without compose having to redeclare.
    """
    runtime = _runtime_stage_lines()
    expose_lines = [line for line in runtime if line.strip().startswith("EXPOSE")]
    assert expose_lines, "runtime stage must declare EXPOSE"
    expose_blob = " ".join(expose_lines)
    assert "8000" in expose_blob
    assert "8081" in expose_blob
