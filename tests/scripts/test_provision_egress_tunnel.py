"""Unit tests for scripts/provision_egress_tunnel.py.

The provisioning helper is a one-shot CLI that operators run inside
the ``snapper-egress`` container after a VPN provider returns peer-side
WG parameters. These tests exercise the argparse surface, the dry-run
short-circuit, the live-write happy path, the private-key validation
guard, and the optional ``--restart-sidecar`` docker shell-out — all
without touching a real DB, a real container, or a real WG kernel
interface.
"""

import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

_HELPER_PATH = (
    Path(__file__).resolve().parent.parent.parent / "scripts" / "provision_egress_tunnel.py"
)
_spec = importlib.util.spec_from_file_location("provision_egress_tunnel", _HELPER_PATH)
assert _spec is not None and _spec.loader is not None
provision = importlib.util.module_from_spec(_spec)
sys.modules["provision_egress_tunnel"] = provision
_spec.loader.exec_module(provision)


def _required_args(privkey_file: Path) -> list[str]:
    """Helper — produce a complete argv slice for an wg-pl-1-shaped tunnel.

    Returns:
        A list of argv tokens covering every required argparse option
        so individual tests can append extra flags (``--dry-run``,
        ``--restart-sidecar``) without redeclaring the full surface.
    """
    return [
        "--tunnel-id",
        "wg-pl-1",
        "--interface",
        "wg-pl1",
        "--address",
        "10.67.178.65",
        "--prefix-length",
        "32",
        "--private-key-file",
        str(privkey_file),
        "--peer-pubkey",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        "--peer-endpoint",
        "203.0.113.66:51820",
        "--socks5-listen-port",
        "1084",
        "--priority",
        "10",
    ]


def _close_and_return_zero(coro: Any) -> int:
    """Helper — close the coroutine produced by ``_amain(args)`` and return 0.

    ``asyncio.run`` is mocked away in the sync-wrapper tests but Python
    still constructs the coroutine before passing it in; without
    closing it a ``RuntimeWarning: coroutine was never awaited`` leaks
    into the test report.
    """
    coro.close()
    return 0


@pytest.fixture
def privkey_file(tmp_path: Path) -> Path:
    """Helper — write a syntactically valid WG private key file.

    Returns:
        Path to a tmp file holding a 44-char base64 ending with ``=``
        (matches the helper's validation guard).
    """
    f = tmp_path / "privkey"
    f.write_text("KCmaH8NJlFtkcaZ2YWttoA0IRvbKTFtLMoBTTQRiuUE=\n")
    return f


class TestParseArgs:
    """Argparse surface."""

    def test_parses_required_args(self, privkey_file: Path) -> None:
        """Spec — every required field is captured from argv.

        Given the canonical argv slice for an wg-pl-1 tunnel,
        When _parse_args runs,
        Then every required arg is set on the Namespace and dry_run
        defaults to False.
        """
        ns = provision._parse_args(_required_args(privkey_file))
        assert ns.tunnel_id == "wg-pl-1"
        assert ns.interface == "wg-pl1"
        assert ns.address == "10.67.178.65"
        assert ns.prefix_length == 32
        assert ns.socks5_listen_port == 1084
        assert ns.priority == 10
        assert ns.allowed_ips == "0.0.0.0/0,::/0"
        assert ns.dry_run is False
        assert ns.restart_sidecar is False

    def test_dry_run_flag_toggles(self, privkey_file: Path) -> None:
        """Spec — passing --dry-run sets ns.dry_run=True.

        Given the canonical argv slice plus ``--dry-run``,
        When _parse_args runs,
        Then ns.dry_run is True.
        """
        ns = provision._parse_args([*_required_args(privkey_file), "--dry-run"])
        assert ns.dry_run is True


class TestAmainDryRun:
    """Dry-run short-circuit — descriptor validates, no DB writes."""

    @pytest.mark.asyncio
    async def test_dry_run_skips_settings_service(
        self, privkey_file: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — dry-run validates descriptor but does not call SettingsService.

        Given --dry-run + valid descriptor,
        When _amain runs,
        Then get_settings_service is NOT invoked and the function
        returns 0.
        """
        monkeypatch.setenv("DB_URL", "sqlite+aiosqlite:///:memory:")
        ns = provision._parse_args([*_required_args(privkey_file), "--dry-run"])
        get_service_mock = AsyncMock()
        with patch.object(provision, "get_settings_service", get_service_mock):
            rc = await provision._amain(ns)
        assert rc == 0
        get_service_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_dry_run_prints_psk_line_when_supplied(
        self,
        privkey_file: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — dry-run + ``--preshared-key`` prints the PSK plan line.

        Given --dry-run + --preshared-key + valid descriptor,
        When _amain runs,
        Then stdout contains the PSK plan line (covers the
        psk-supplied-and-dry-run branch).
        """
        monkeypatch.setenv("DB_URL", "sqlite+aiosqlite:///:memory:")
        ns = provision._parse_args(
            [
                *_required_args(privkey_file),
                "--dry-run",
                "--preshared-key",
                "psk-value-base64=",
            ]
        )
        with patch.object(provision, "get_settings_service", AsyncMock()):
            rc = await provision._amain(ns)
        assert rc == 0
        captured = capsys.readouterr()
        assert "_preshared_key = <encrypted>" in captured.out


class TestAmainInvalidKey:
    """Private-key validation guard."""

    @pytest.mark.asyncio
    async def test_rejects_wrong_length_private_key(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — a key not exactly 44 chars + ending '=' returns exit 2.

        Given a private-key file containing an obviously wrong value,
        When _amain runs,
        Then it returns 2 and emits a stderr complaint.
        """
        monkeypatch.setenv("DB_URL", "sqlite+aiosqlite:///:memory:")
        bad = tmp_path / "bad"
        bad.write_text("nope")
        ns = provision._parse_args(_required_args(bad))
        rc = await provision._amain(ns)
        assert rc == 2

    @pytest.mark.asyncio
    async def test_rejects_symlinked_private_key(
        self,
        privkey_file: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A private-key path cannot redirect the secret read through a symlink."""
        monkeypatch.setenv("DB_URL", "sqlite+aiosqlite:///:memory:")

        def fake_is_symlink(path: Path) -> bool:
            """Report only the selected private-key file as a symlink."""
            return path == privkey_file

        monkeypatch.setattr(Path, "is_symlink", fake_is_symlink)
        ns = provision._parse_args(_required_args(privkey_file))
        rc = await provision._amain(ns)
        assert rc == 2


class TestAmainLiveWrite:
    """Happy-path live write — descriptor + private key + pool merge."""

    @pytest.mark.asyncio
    async def test_writes_three_settings_and_merges_pool(
        self, privkey_file: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — _amain writes descriptor + key + pool when not dry-run.

        Given the canonical argv (no --dry-run) + a mocked
        SettingsService whose ``get_setting('egress_pool')`` returns
        the live 4-route pool,
        When _amain runs,
        Then update_setting is called 3 times: descriptor, private
        key, and pool with the new pl1 route appended.
        """
        monkeypatch.setenv("DB_URL", "sqlite+aiosqlite:///:memory:")
        ns = provision._parse_args(_required_args(privkey_file))
        live_pool: dict[str, Any] = {
            "enabled": True,
            "on_all_quarantined": "wait",
            "routes": [
                {"id": "default", "kind": "direct", "priority": 100, "enabled": True},
                {
                    "id": "wg-ie-1",
                    "kind": "socks5",
                    "proxy_url": "socks5h://snapper-egress:1081",
                    "priority": 10,
                    "enabled": True,
                },
            ],
        }
        service = MagicMock()
        service.update_setting = AsyncMock()
        service.get_setting = MagicMock(return_value=live_pool)
        get_service = AsyncMock(return_value=service)
        with patch.object(provision, "get_settings_service", get_service):
            rc = await provision._amain(ns)
        assert rc == 0
        assert service.update_setting.await_count == 3
        keys_written = [call.args[0] for call in service.update_setting.await_args_list]
        assert keys_written == [
            "egress_tunnel_wg-pl-1",
            "egress_tunnel_wg-pl-1_private_key",
            "egress_pool",
        ]
        merged_pool = service.update_setting.await_args_list[2].args[1]
        assert any(r["id"] == "wg-pl-1" for r in merged_pool["routes"])
        assert merged_pool["enabled"] is True

    @pytest.mark.asyncio
    async def test_writes_preshared_key_when_supplied(
        self, privkey_file: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — --preshared-key triggers a 4th update_setting call.

        Given canonical argv + --preshared-key,
        When _amain runs,
        Then update_setting is awaited 4 times and the PSK key is
        among them.
        """
        monkeypatch.setenv("DB_URL", "sqlite+aiosqlite:///:memory:")
        ns = provision._parse_args(
            [*_required_args(privkey_file), "--preshared-key", "psk-value-base64="]
        )
        service = MagicMock()
        service.update_setting = AsyncMock()
        service.get_setting = MagicMock(return_value=None)
        get_service = AsyncMock(return_value=service)
        with patch.object(provision, "get_settings_service", get_service):
            rc = await provision._amain(ns)
        assert rc == 0
        keys_written = [call.args[0] for call in service.update_setting.await_args_list]
        assert "egress_tunnel_wg-pl-1_preshared_key" in keys_written

    @pytest.mark.asyncio
    async def test_replaces_existing_route_with_same_id(
        self, privkey_file: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — re-running with the same tunnel-id deduplicates pool routes.

        Given a pool that already contains a pl1 route,
        When _amain runs,
        Then the final pool still has exactly one pl1 entry (the new
        one), not two.
        """
        monkeypatch.setenv("DB_URL", "sqlite+aiosqlite:///:memory:")
        ns = provision._parse_args(_required_args(privkey_file))
        live_pool: dict[str, Any] = {
            "enabled": True,
            "on_all_quarantined": "wait",
            "routes": [
                {"id": "default", "kind": "direct", "priority": 100, "enabled": True},
                {
                    "id": "wg-pl-1",
                    "kind": "socks5",
                    "proxy_url": "socks5h://snapper-egress:9999",
                    "priority": 99,
                    "enabled": True,
                },
            ],
        }
        service = MagicMock()
        service.update_setting = AsyncMock()
        service.get_setting = MagicMock(return_value=live_pool)
        with patch.object(provision, "get_settings_service", AsyncMock(return_value=service)):
            await provision._amain(ns)
        merged_pool = service.update_setting.await_args_list[2].args[1]
        pl_routes = [r for r in merged_pool["routes"] if r["id"] == "wg-pl-1"]
        assert len(pl_routes) == 1
        assert pl_routes[0]["priority"] == 10


class TestMain:
    """Sync wrapper invoked by the Docker CMD."""

    def test_main_delegates_to_asyncio_run(self, privkey_file: Path) -> None:
        """Spec — main parses argv + dispatches via asyncio.run.

        Given main called with patched asyncio.run returning 0,
        When main runs,
        Then asyncio.run is called once and main returns 0 without
        invoking subprocess.run.
        """
        run_mock = MagicMock(side_effect=_close_and_return_zero)
        argv_patch = ["prog", *_required_args(privkey_file)]
        with (
            patch.object(provision.asyncio, "run", run_mock),
            patch.object(provision.sys, "argv", argv_patch),
            patch.object(provision.subprocess, "run") as subproc_run,
        ):
            rc = provision.main()
        assert rc == 0
        run_mock.assert_called_once()
        subproc_run.assert_not_called()

    def test_main_propagates_nonzero_rc_without_restart(self, privkey_file: Path) -> None:
        """Spec — non-zero return from asyncio.run skips --restart-sidecar.

        Given asyncio.run returns 2 (invalid private key),
        When main runs with --restart-sidecar,
        Then main returns 2 and subprocess.run is NOT called.
        """

        def _close_and_return_two(coro: Any) -> int:
            coro.close()
            return 2

        run_mock = MagicMock(side_effect=_close_and_return_two)
        argv_patch = ["prog", *_required_args(privkey_file), "--restart-sidecar"]
        with (
            patch.object(provision.asyncio, "run", run_mock),
            patch.object(provision.sys, "argv", argv_patch),
            patch.object(provision.subprocess, "run") as subproc_run,
        ):
            rc = provision.main()
        assert rc == 2
        subproc_run.assert_not_called()

    def test_main_restarts_sidecar_when_flag_set(self, privkey_file: Path) -> None:
        """Spec — --restart-sidecar invokes docker restart after successful _amain.

        Given asyncio.run returns 0 + --restart-sidecar,
        When main runs,
        Then subprocess.run is called with the expected docker
        argument list.
        """
        argv_patch = ["prog", *_required_args(privkey_file), "--restart-sidecar"]
        with (
            patch.object(provision.asyncio, "run", MagicMock(side_effect=_close_and_return_zero)),
            patch.object(provision.sys, "argv", argv_patch),
            patch.object(provision.subprocess, "run") as subproc_run,
        ):
            rc = provision.main()
        assert rc == 0
        subproc_run.assert_called_once_with(["docker", "restart", "snapper-egress"], check=True)

    def test_main_reports_docker_restart_failure(self, privkey_file: Path) -> None:
        """Spec — subprocess.CalledProcessError is caught + main returns 3.

        Given subprocess.run raises CalledProcessError,
        When main runs with --restart-sidecar,
        Then main returns 3 (operator-facing rc for docker failure).
        """
        argv_patch = ["prog", *_required_args(privkey_file), "--restart-sidecar"]
        with (
            patch.object(provision.asyncio, "run", MagicMock(side_effect=_close_and_return_zero)),
            patch.object(provision.sys, "argv", argv_patch),
            patch.object(
                provision.subprocess,
                "run",
                side_effect=subprocess.CalledProcessError(1, ["docker", "restart"]),
            ),
        ):
            rc = provision.main()
        assert rc == 3

    def test_main_reports_missing_docker_binary(self, privkey_file: Path) -> None:
        """Spec — FileNotFoundError on docker exec also yields exit 3.

        Given subprocess.run raises FileNotFoundError (no docker on PATH),
        When main runs with --restart-sidecar,
        Then main returns 3.
        """
        argv_patch = ["prog", *_required_args(privkey_file), "--restart-sidecar"]
        with (
            patch.object(provision.asyncio, "run", MagicMock(side_effect=_close_and_return_zero)),
            patch.object(provision.sys, "argv", argv_patch),
            patch.object(provision.subprocess, "run", side_effect=FileNotFoundError("docker")),
        ):
            rc = provision.main()
        assert rc == 3
