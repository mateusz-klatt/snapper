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


_UNIFIED_IMAGE_COMPOSE_OK = """
services:
  snapper:
    image: klattm/snapper:latest
    command: ["server"]
  snapper-egress:
    image: klattm/snapper:latest
    command: ["egress"]
    user: "0:0"
    cap_add:
      - NET_ADMIN
    expose:
      - "8081"
"""
"""Reference compose fragment satisfying every B'.6 unified-image invariant."""


def test_unified_image_invariants_pass_on_correct_compose(tmp_path: Path) -> None:
    """Spec — B'.6 reference compose passes the unified-image lint.

    Given a compose declaring both services with matching ``image:``,
    sidecar command/user/cap_add set, and monolith having neither
    cap_add nor user override,
    When main runs,
    Then it returns 0.
    """
    _write(tmp_path / "docker-compose.yml", _UNIFIED_IMAGE_COMPOSE_OK)
    assert check_egress_compose.main(root=tmp_path) == 0


def test_fails_when_sqlite_data_volume_not_shared_with_egress(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Spec — sidecar shares the monolith's SQLite data target.

    Given snapper mounts ``./data`` at ``/app/data`` but snapper-egress
        does not,
    When main runs,
    Then it returns 1 and names the missing sidecar volume target.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper:
            image: klattm/snapper:latest
            command: ["server"]
            volumes:
              - ./data:/app/data
          snapper-egress:
            image: klattm/snapper:latest
            command: ["egress"]
            user: "0:0"
            cap_add:
              - NET_ADMIN
            expose:
              - "8081"
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    captured = capsys.readouterr()
    assert "snapper-egress.volumes" in captured.err
    assert "/app/data" in captured.err


def test_passes_when_sqlite_data_volume_is_shared_with_egress(tmp_path: Path) -> None:
    """Spec — matching ``/app/data`` targets satisfy the SQLite invariant.

    Given both snapper and snapper-egress mount a volume at
        ``/app/data``,
    When main runs,
    Then it returns 0.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper:
            image: klattm/snapper:latest
            command: ["server"]
            volumes:
              - ./data:/app/data
          snapper-egress:
            image: klattm/snapper:latest
            command: ["egress"]
            user: "0:0"
            cap_add:
              - NET_ADMIN
            expose:
              - "8081"
            volumes:
              - ./data:/app/data
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 0


def test_volume_target_handles_short_syntax_without_target() -> None:
    """Spec — short volume syntax without ``:target`` has no target.

    Given a named volume entry with no container target,
    When _volume_target parses it,
    Then it returns None.
    """
    assert check_egress_compose._volume_target("named-volume") is None


def test_volume_target_handles_long_syntax_target() -> None:
    """Spec — Compose long-syntax volume target is detected.

    Given a long-syntax bind volume with target ``/app/data``,
    When _volume_target parses it,
    Then it returns the target path.
    """
    assert (
        check_egress_compose._volume_target(
            {
                "type": "bind",
                "source": "./data",
                "target": "/app/data",
            }
        )
        == "/app/data"
    )


def test_volume_target_handles_long_syntax_without_string_target() -> None:
    """Spec — long-syntax volume without string target has no target.

    Given a long-syntax volume with a non-string target,
    When _volume_target parses it,
    Then it returns None.
    """
    assert check_egress_compose._volume_target({"target": 123}) is None


def test_volume_target_handles_unknown_volume_shape() -> None:
    """Spec — unsupported volume entry shape has no target.

    Given a volume entry that is neither string nor mapping,
    When _volume_target parses it,
    Then it returns None.
    """
    assert check_egress_compose._volume_target(123) is None


def test_has_volume_target_ignores_non_list_volumes() -> None:
    """Spec — malformed non-list ``volumes`` does not satisfy target checks.

    Given a service whose ``volumes`` field is not a list,
    When _has_volume_target checks for ``/app/data``,
    Then it returns False.
    """
    service: dict[str, object] = {"volumes": {"target": "/app/data"}}
    assert not check_egress_compose._has_volume_target(service, "/app/data")


def test_fails_when_image_tags_differ(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Spec — different ``image:`` per service → exit 1.

    Given snapper service with one image and snapper-egress with a
        different one,
    When main runs,
    Then it returns 1 and the error names both image strings so the
        operator can fix the mismatch.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper:
            image: klattm/snapper:latest
            command: ["server"]
          snapper-egress:
            image: klattm/snapper-egress:latest
            command: ["egress"]
            user: "0:0"
            cap_add:
              - NET_ADMIN
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    err = capsys.readouterr().err
    assert "must match" in err


def test_fails_when_egress_command_is_wrong(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Spec — sidecar ``command`` != ``["egress"]`` → exit 1.

    Given a compose with the sidecar still using the legacy
        ``["python", "-m", "snapper.egress"]`` command,
    When main runs,
    Then it returns 1 and the error names the wrong command. Locks
        the CLI-dispatch invariant against accidental regression.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper:
            image: klattm/snapper:latest
            command: ["server"]
          snapper-egress:
            image: klattm/snapper:latest
            command: ["python", "-m", "snapper.egress"]
            user: "0:0"
            cap_add:
              - NET_ADMIN
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    assert "command" in capsys.readouterr().err


def test_fails_when_egress_user_is_not_root(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Spec — sidecar ``user:`` != ``"0:0"`` → exit 1.

    Given a compose with the sidecar running as a non-root UID,
    When main runs,
    Then it returns 1. The R7 invariant from B'.6 v9 — without root,
        ``pyroute2`` netlink writes fail even with CAP_NET_ADMIN
        granted.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper:
            image: klattm/snapper:latest
            command: ["server"]
          snapper-egress:
            image: klattm/snapper:latest
            command: ["egress"]
            user: "888:888"
            cap_add:
              - NET_ADMIN
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    assert "user" in capsys.readouterr().err


def test_fails_when_egress_missing_net_admin(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Spec — sidecar missing ``cap_add: NET_ADMIN`` → exit 1.

    Given a compose where the sidecar service has no NET_ADMIN cap,
    When main runs,
    Then it returns 1. Kernel WireGuard requires CAP_NET_ADMIN for
        ``ip link add type wireguard``; missing this cap silently
        breaks tunnel bring-up at startup.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper:
            image: klattm/snapper:latest
            command: ["server"]
          snapper-egress:
            image: klattm/snapper:latest
            command: ["egress"]
            user: "0:0"
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    assert "NET_ADMIN" in capsys.readouterr().err


def test_fails_when_monolith_declares_cap_add(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Spec — monolith ``cap_add:`` present → exit 1.

    Given a compose where the monolith service declares cap_add,
    When main runs,
    Then it returns 1. The monolith runs unprivileged even though it
        shares the image with the sidecar — granting it any cap is a
        security regression.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper:
            image: klattm/snapper:latest
            command: ["server"]
            cap_add:
              - NET_ADMIN
          snapper-egress:
            image: klattm/snapper:latest
            command: ["egress"]
            user: "0:0"
            cap_add:
              - NET_ADMIN
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    assert "must NOT be declared" in capsys.readouterr().err


def test_fails_when_monolith_declares_user_override(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Spec — monolith ``user:`` present → exit 1.

    Given a compose where the monolith service declares a user override,
    When main runs,
    Then it returns 1. The monolith inherits secure default
        ``USER snapper`` (UID 888) from the image; operator override
        (especially to root) would erase the security default.
    """
    _write(
        tmp_path / "docker-compose.yml",
        """
        services:
          snapper:
            image: klattm/snapper:latest
            command: ["server"]
            user: "0:0"
          snapper-egress:
            image: klattm/snapper:latest
            command: ["egress"]
            user: "0:0"
            cap_add:
              - NET_ADMIN
        """,
    )
    assert check_egress_compose.main(root=tmp_path) == 1
    assert "must NOT be declared" in capsys.readouterr().err


def test_unified_image_check_skipped_when_monolith_absent(tmp_path: Path) -> None:
    """Spec — partial overrides (sidecar only) don't trigger cross-service check.

    Given a partial override compose declaring only the sidecar
        service (no `snapper:` block),
    When main runs,
    Then it returns 0. The B'.6 unified-image cross-check is
        skipped silently when either service is missing — allows
        partial overrides without forcing every file to redeclare
        the monolith.
    """
    _write(
        tmp_path / "docker-compose.override.yml",
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
