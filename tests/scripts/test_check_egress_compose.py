"""Unit tests for ``scripts/check_egress_compose.py``.

SC.3 lint hook protects the snapper-egress sidecar from accidentally
exposing its unauthenticated SOCKS5 listener to the host network.
"""

import textwrap
from pathlib import Path

import pytest

from scripts import check_egress_compose


def _write(path: Path, contents: str) -> None:
    """Helper — write Compose YAML to a tmp file."""
    path.write_text(textwrap.dedent(contents), encoding="utf-8")


def test_passes_when_no_compose_files(tmp_path: Path) -> None:
    """Spec — missing compose files → exit 0 with a clear message.

    Given a directory with no docker-compose.yml,
    When main runs,
    Then it returns 0 (nothing to lint).
    """
    assert check_egress_compose.main(root=tmp_path) == 0


def test_passes_when_compose_has_no_egress_service(tmp_path: Path) -> None:
    """Spec — compose without snapper-egress is fine.

    Given a compose file with only the API service,
    When main runs,
    Then it returns 0.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper:
            image: snapper:latest
            ports:
              - "127.0.0.1:8000:8000"
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 0


def test_passes_when_egress_has_no_ports_or_host_network(tmp_path: Path) -> None:
    """Spec — clean snapper-egress entry → exit 0.

    Given a compose file declaring snapper-egress with `expose:` only,
    When main runs,
    Then it returns 0.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper-egress:
            image: snapper-egress:latest
            expose:
              - "8081"
            cap_add:
              - NET_ADMIN
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 0


def test_fails_when_egress_has_host_ports(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Spec — `ports:` entry → exit 1 with a clear error.

    Given snapper-egress with a `ports: [...]` mapping,
    When main runs,
    Then it returns 1 and prints a clear error to stderr.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper-egress:
            image: snapper-egress:latest
            ports:
              - "1081:1081"
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    captured = capsys.readouterr()
    assert "ports" in captured.err
    assert "NO authentication" in captured.err


def test_fails_when_egress_uses_host_network(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Spec — network_mode: host → exit 1.

    Given snapper-egress with network_mode: host,
    When main runs,
    Then it returns 1 with the host-networking error.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper-egress:
            image: snapper-egress:latest
            network_mode: host
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    captured = capsys.readouterr()
    assert "host networking" in captured.err


def test_scans_override_files(tmp_path: Path) -> None:
    """Spec — docker-compose.override.yml is also linted.

    Given a clean base compose AND an override with `ports:`,
    When main runs,
    Then it returns 1 because the override's host-port mapping is
        detected. Covers the common Compose-merge bypass.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper-egress:
            image: snapper-egress:latest
            expose:
              - "8081"
        """,
    )
    _write(
        tmp_path / "docker-compose.override.yml",
        """
        services:
          snapper-egress:
            ports:
              - "8081:8081"
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1


def test_fails_on_env_interpolated_network_mode(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Spec — `network_mode: ${VAR:-host}` is rejected at lint time.

    Given an env-interpolated network_mode,
    When main runs,
    Then it returns 1 with a clear error about env-driven bypass.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper-egress:
            image: snapper-egress:latest
            network_mode: ${EGRESS_NETWORK_MODE:-host}
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    captured = capsys.readouterr()
    assert "env interpolation" in captured.err


def test_fails_on_env_interpolated_ports_entry(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Spec — `ports: ["${VAR}:8081"]` is rejected at lint time.

    Given an env-interpolated host port mapping,
    When main runs,
    Then it returns 1 with the env-interpolation error.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper-egress:
            image: snapper-egress:latest
            ports:
              - "${EGRESS_HOST_PORT}:8081"
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    captured = capsys.readouterr()
    assert "env interpolation" in captured.err


def test_scans_prod_compose_file_too(tmp_path: Path) -> None:
    """Spec — docker-compose.prod.yml is also scanned.

    Given a prod compose file violating the constraint AND a clean
        base compose file,
    When main runs,
    Then it returns 1 (the prod file is independently checked).
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper-egress:
            image: snapper-egress:latest
            expose:
              - "8081"
        """,
    )
    _write(
        tmp_path / "docker-compose.prod.yml",
        """
        services:
          snapper-egress:
            image: snapper-egress:latest
            ports:
              - "8081:8081"
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1


def test_scans_yaml_extension_compose_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Spec — `.yaml` extension files are also linted.

    Given a `compose.yaml` (Compose v2 default) with a host-port
        mapping on snapper-egress,
    When main runs,
    Then it returns 1. Pinned by Codex Code Reviewer round 2.
    """
    _write(
        tmp_path / "compose.yaml",
        """
        services:
          snapper-egress:
            image: snapper-egress:latest
            ports:
              - "1081:1081"
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    captured = capsys.readouterr()
    assert "ports" in captured.err


def test_main_module_invocation_returns_systemexit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Spec — running as __main__ exits with the lint return code.

    Given the module's ``if __name__ == "__main__"`` path,
    When invoked directly,
    Then it raises SystemExit with the return value of main().
    """
    monkeypatch.chdir(tmp_path)
    assert check_egress_compose.main() == 0
