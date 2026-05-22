"""Unit tests for the snapper-egress orchestrator.

SC.4 tests verify the bootstrap order, per-tunnel best-effort
bring-up, healthcheck endpoint contracts, and the SIGTERM-driven
shutdown sequence. Heavy use of monkeypatching keeps the suite
hermetic — no real kernel WG calls, no real SOCKS5 listeners, no
real ip routes.
"""

import asyncio
import signal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.infrastructure.network import egress_sidecar
from snapper.infrastructure.network.egress_sidecar import _bring_down_safe
from snapper.infrastructure.network.egress_sidecar import _bring_up_one
from snapper.infrastructure.network.egress_sidecar import _bring_up_tunnels
from snapper.infrastructure.network.egress_sidecar import _FailedTunnel
from snapper.infrastructure.network.egress_sidecar import _handle_ready
from snapper.infrastructure.network.egress_sidecar import _handle_readyz
from snapper.infrastructure.network.egress_sidecar import _handle_tunnels
from snapper.infrastructure.network.egress_sidecar import _RunningTunnel
from snapper.infrastructure.network.egress_sidecar import _shutdown_tunnels
from snapper.infrastructure.network.egress_sidecar import _SidecarState
from snapper.infrastructure.network.egress_sidecar import _stop_server_safe
from snapper.infrastructure.network.egress_sidecar import install_signal_handlers
from snapper.infrastructure.network.egress_sidecar import run_sidecar
from snapper.infrastructure.network.egress_tunnel_models import LoadedTunnel
from snapper.infrastructure.network.egress_tunnel_models import LoadResult
from snapper.infrastructure.network.egress_tunnel_models import TunnelDescriptor
from snapper.infrastructure.network.egress_tunnel_models import TunnelLoadFailure


def _make_descriptor(
    tunnel_id: str = "eset-de1",
    interface: str = "wg-uk-1",
    address: str = "10.64.12.34",
    socks5_listen_port: int = 1081,
) -> TunnelDescriptor:
    """Helper — build a minimal valid descriptor."""
    return TunnelDescriptor(
        id=tunnel_id,
        interface=interface,
        address=address,
        prefix_length=32,
        peer_pubkey="A" * 44,
        peer_endpoint="vpn.example.com:51820",
        socks5_listen_port=socks5_listen_port,
        priority=10,
    )


def _make_loaded(tunnel_id: str = "eset-de1") -> LoadedTunnel:
    """Helper — wrap a descriptor with placeholder secrets."""
    return LoadedTunnel(
        descriptor=_make_descriptor(tunnel_id=tunnel_id),
        private_key="PRIVKEY",
        preshared_key=None,
    )


def _make_request(state: _SidecarState) -> MagicMock:
    """Helper — minimal request mock exposing the typed AppKey state.

    Uses the production ``_STATE_KEY`` so the handlers see the same
    typed slot they'd see in a real aiohttp app.
    """
    request = MagicMock()
    request.app = {egress_sidecar._STATE_KEY: state}
    return request


@pytest.mark.asyncio
class TestBringUpOne:
    """Per-tunnel best-effort bring-up logic."""

    async def test_happy_path_records_running_tunnel(self) -> None:
        """Spec — successful bring_up + server start → _RunningTunnel.

        Given wg_control.bring_up + Socks5Server.start both succeed,
        When _bring_up_one runs,
        Then state.running has one entry and state.failed is empty.
        """
        state = _SidecarState()
        loaded = _make_loaded()
        server_instance = MagicMock()
        server_instance.start = AsyncMock()
        with (
            patch.object(egress_sidecar.wg_control, "bring_up", new=AsyncMock()),
            patch.object(egress_sidecar, "Socks5Server", return_value=server_instance),
        ):
            await _bring_up_one(loaded, 0, state)
        assert len(state.running) == 1
        assert state.failed == []
        assert state.running[0].tunnel_index == 0

    async def test_bring_up_failure_records_failed_tunnel(self) -> None:
        """Spec — wg_control.bring_up raising → _FailedTunnel with reason.

        Given wg_control.bring_up raises RuntimeError,
        When _bring_up_one runs,
        Then state.failed has one entry tagged ``bring_up: ...``
        and the Socks5Server is NOT instantiated.
        """
        state = _SidecarState()
        loaded = _make_loaded()
        bring_up_mock = AsyncMock(side_effect=RuntimeError("kernel boom"))
        socks5_mock = MagicMock()
        with (
            patch.object(egress_sidecar.wg_control, "bring_up", new=bring_up_mock),
            patch.object(egress_sidecar, "Socks5Server", socks5_mock),
        ):
            await _bring_up_one(loaded, 0, state)
        assert state.running == []
        assert len(state.failed) == 1
        assert state.failed[0].reason.startswith("bring_up:")
        socks5_mock.assert_not_called()

    async def test_server_start_failure_tears_down_wg(self) -> None:
        """Spec — Socks5Server.start raising → bring_down + _FailedTunnel.

        Given wg_control.bring_up succeeds but server.start raises,
        When _bring_up_one runs,
        Then state.failed has one entry tagged ``socks5_start: ...``
        AND wg_control.bring_down was called to clean the kernel state.
        """
        state = _SidecarState()
        loaded = _make_loaded()
        server_instance = MagicMock()
        server_instance.start = AsyncMock(side_effect=OSError("address in use"))
        bring_up_mock = AsyncMock()
        bring_down_mock = AsyncMock()
        with (
            patch.object(egress_sidecar.wg_control, "bring_up", new=bring_up_mock),
            patch.object(egress_sidecar.wg_control, "bring_down", new=bring_down_mock),
            patch.object(egress_sidecar, "Socks5Server", return_value=server_instance),
        ):
            await _bring_up_one(loaded, 0, state)
        assert state.running == []
        assert len(state.failed) == 1
        assert state.failed[0].reason.startswith("socks5_start:")
        bring_down_mock.assert_awaited_once()

    async def test_bring_up_forwards_descriptor_fields(self) -> None:
        """Spec — wg_control.bring_up receives the full descriptor payload.

        Given a descriptor with preshared_key="PSK" and a non-default
            allowed_ips tuple,
        When _bring_up_one runs,
        Then wg_control.bring_up is invoked with matching kwargs.
        """
        state = _SidecarState()
        descriptor = _make_descriptor()
        loaded = LoadedTunnel(
            descriptor=descriptor,
            private_key="PRIV",
            preshared_key="PSK",
        )
        server_instance = MagicMock()
        server_instance.start = AsyncMock()
        bring_up_mock = AsyncMock()
        with (
            patch.object(egress_sidecar.wg_control, "bring_up", new=bring_up_mock),
            patch.object(egress_sidecar, "Socks5Server", return_value=server_instance),
        ):
            await _bring_up_one(loaded, 3, state)
        bring_up_mock.assert_awaited_once()
        kw = bring_up_mock.call_args.kwargs
        assert kw["interface"] == descriptor.interface
        assert kw["address"] == descriptor.address
        assert kw["private_key"] == "PRIV"
        assert kw["peer_pubkey"] == descriptor.peer_pubkey
        assert kw["preshared_key"] == "PSK"
        assert kw["tunnel_index"] == 3
        assert kw["allowed_ips"] == descriptor.allowed_ips


@pytest.mark.asyncio
class TestBringUpTunnels:
    """Loader-failure → per-tunnel orchestration."""

    async def test_load_failures_propagate_to_state(self) -> None:
        """Spec — LoadResult.failures land on state.failed verbatim.

        Given a LoadResult with one failure + no successful tunnels,
        When _bring_up_tunnels runs,
        Then state.failed has the matching entry and state.running stays empty.
        """
        state = _SidecarState()
        load_result = LoadResult(
            tunnels=[],
            failures=[TunnelLoadFailure(tunnel_id="eset-de1", reason="missing private_key")],
        )
        await _bring_up_tunnels(load_result, state)
        assert state.running == []
        assert len(state.failed) == 1
        assert state.failed[0].reason == "missing private_key"

    async def test_per_tunnel_index_increments(self) -> None:
        """Spec — tunnel_index reflects position in the loaded list.

        Given three loaded tunnels,
        When _bring_up_tunnels runs,
        Then bring_up is called with tunnel_index 0, 1, 2 in order.
        """
        state = _SidecarState()
        load_result = LoadResult(
            tunnels=[
                LoadedTunnel(
                    descriptor=_make_descriptor(
                        tunnel_id=f"t{i}",
                        interface=f"wg-t{i}",
                        address=f"10.0.0.{i + 1}",
                        socks5_listen_port=1080 + i,
                    ),
                    private_key="K",
                    preshared_key=None,
                )
                for i in range(3)
            ],
            failures=[],
        )
        server_mock = MagicMock()
        server_mock.start = AsyncMock()
        bring_up_mock = AsyncMock()
        with (
            patch.object(egress_sidecar.wg_control, "bring_up", new=bring_up_mock),
            patch.object(egress_sidecar, "Socks5Server", return_value=server_mock),
        ):
            await _bring_up_tunnels(load_result, state)
        indices = [c.kwargs["tunnel_index"] for c in bring_up_mock.call_args_list]
        assert indices == [0, 1, 2]


@pytest.mark.asyncio
class TestLoadDeclaredTunnelsAfterKernelProbe:
    """Settings load + optional kernel probe coordination."""

    async def test_skips_kernel_probe_when_no_tunnels_load(self) -> None:
        """Spec — empty/default settings do not require host WireGuard.

        Given load_declared_tunnels returns no loaded tunnels,
        When the sidecar runtime loader runs,
        Then check_kernel_wireguard is not called and the result is
            returned unchanged.
        """
        service = MagicMock()
        load_result = LoadResult(tunnels=[], failures=[])
        probe_mock = MagicMock(return_value=None)
        with (
            patch.object(
                egress_sidecar,
                "load_declared_tunnels",
                new=AsyncMock(return_value=load_result),
            ),
            patch.object(egress_sidecar.wg_control, "check_kernel_wireguard", new=probe_mock),
        ):
            result = await egress_sidecar._load_declared_tunnels_after_kernel_probe(service)
        assert result == load_result
        probe_mock.assert_not_called()

    async def test_keeps_loaded_tunnels_when_probe_succeeds(self) -> None:
        """Spec — a successful kernel probe preserves loaded tunnels.

        Given load_declared_tunnels returns one valid tunnel,
        When check_kernel_wireguard succeeds,
        Then the returned LoadResult still contains that tunnel.
        """
        service = MagicMock()
        loaded = _make_loaded("up-1")
        load_result = LoadResult(tunnels=[loaded], failures=[])
        probe_mock = MagicMock(return_value=None)
        with (
            patch.object(
                egress_sidecar,
                "load_declared_tunnels",
                new=AsyncMock(return_value=load_result),
            ),
            patch.object(egress_sidecar.wg_control, "check_kernel_wireguard", new=probe_mock),
        ):
            result = await egress_sidecar._load_declared_tunnels_after_kernel_probe(service)
        assert result == load_result
        probe_mock.assert_called_once()

    async def test_converts_probe_failure_reason_to_tunnel_failure(self) -> None:
        """Spec — kernel probe failure reason becomes a per-tunnel failure.

        Given a loaded tunnel but check_kernel_wireguard returns a
            failure reason,
        When the sidecar runtime loader runs,
        Then the tunnel is not brought up and /ready can still expose
            the failure through the normal failed_tunnels path.
        """
        service = MagicMock()
        loaded = _make_loaded("up-1")
        load_result = LoadResult(tunnels=[loaded], failures=[])
        with (
            patch.object(
                egress_sidecar,
                "load_declared_tunnels",
                new=AsyncMock(return_value=load_result),
            ),
            patch.object(
                egress_sidecar.wg_control,
                "check_kernel_wireguard",
                return_value="snapper-egress missing NET_ADMIN capability",
            ),
        ):
            result = await egress_sidecar._load_declared_tunnels_after_kernel_probe(service)
        assert result.tunnels == []
        assert len(result.failures) == 1
        assert result.failures[0].tunnel_id == "up-1"
        assert (
            result.failures[0].reason == "kernel_probe: snapper-egress missing NET_ADMIN capability"
        )

    async def test_converts_probe_exception_to_tunnel_failure(self) -> None:
        """Spec — unexpected probe exceptions become per-tunnel failures.

        Given a loaded tunnel but the active kernel probe raises,
        When the sidecar runtime loader runs,
        Then the returned LoadResult records the failed tunnel and
            suppresses bring-up for that boot cycle.
        """
        service = MagicMock()
        loaded = _make_loaded("up-1")
        load_result = LoadResult(tunnels=[loaded], failures=[])
        with (
            patch.object(
                egress_sidecar,
                "load_declared_tunnels",
                new=AsyncMock(return_value=load_result),
            ),
            patch.object(
                egress_sidecar.wg_control,
                "check_kernel_wireguard",
                side_effect=RuntimeError("netlink boom"),
            ),
        ):
            result = await egress_sidecar._load_declared_tunnels_after_kernel_probe(service)
        assert result.tunnels == []
        assert len(result.failures) == 1
        assert result.failures[0].tunnel_id == "up-1"
        assert result.failures[0].reason == "kernel_probe: netlink boom"


@pytest.mark.asyncio
class TestShutdownTunnels:
    """Reverse-order shutdown semantics."""

    async def test_shutdown_calls_stop_then_bring_down(self) -> None:
        """Spec — each tunnel sees stop() then bring_down().

        Given two running tunnels,
        When _shutdown_tunnels runs,
        Then for each tunnel the server stop and bring_down are awaited.
        """
        state = _SidecarState()
        for i in range(2):
            srv = MagicMock()
            srv.stop = AsyncMock()
            state.running.append(
                _RunningTunnel(
                    loaded=LoadedTunnel(
                        descriptor=_make_descriptor(
                            tunnel_id=f"t{i}",
                            interface=f"wg-{i}",
                            address=f"10.0.0.{i + 1}",
                            socks5_listen_port=1080 + i,
                        ),
                        private_key="K",
                        preshared_key=None,
                    ),
                    server=srv,
                    tunnel_index=i,
                )
            )
        bring_down_mock = AsyncMock()
        with patch.object(egress_sidecar.wg_control, "bring_down", new=bring_down_mock):
            await _shutdown_tunnels(state)
        for running in state.running:
            running.server.stop.assert_awaited_once()
        assert bring_down_mock.await_count == 2

    async def test_stop_server_safe_swallows_exception(self) -> None:
        """Spec — server.stop raising during shutdown does not propagate.

        Given a server whose stop raises RuntimeError,
        When _stop_server_safe runs,
        Then no exception escapes.
        """
        server = MagicMock()
        server.stop = AsyncMock(side_effect=RuntimeError("boom"))
        running = _RunningTunnel(
            loaded=_make_loaded(),
            server=server,
            tunnel_index=0,
        )
        await _stop_server_safe(running)

    async def test_bring_down_safe_swallows_exception(self) -> None:
        """Spec — wg_control.bring_down raising does not propagate.

        Given wg_control.bring_down raises,
        When _bring_down_safe runs,
        Then no exception escapes.
        """
        with patch.object(
            egress_sidecar.wg_control,
            "bring_down",
            new=AsyncMock(side_effect=RuntimeError("kernel boom")),
        ):
            await _bring_down_safe("wg-uk-1", 0)


@pytest.mark.asyncio
class TestHealthcheckHandlers:
    """JSON shapes of ``/ready``, ``/tunnels``, ``/readyz``."""

    async def test_ready_returns_200_always(self) -> None:
        """Spec — /ready returns 200 even when some tunnels failed.

        Given state with one running + one failed tunnel,
        When _handle_ready runs,
        Then the response is 200 and lists both id arrays.
        """
        state = _SidecarState()
        state.running.append(
            _RunningTunnel(
                loaded=_make_loaded("up-1"),
                server=MagicMock(),
                tunnel_index=0,
            )
        )
        state.failed.append(_FailedTunnel(tunnel_id="bad-1", reason="x"))
        response = await _handle_ready(_make_request(state))
        assert response.status == 200
        body = response.body
        assert isinstance(body, bytes)
        assert b"up-1" in body
        assert b"bad-1" in body

    async def test_tunnels_returns_per_id_status(self) -> None:
        """Spec — /tunnels returns one entry per declared id.

        Given a mix of running + failed tunnels,
        When _handle_tunnels runs,
        Then each tunnel id maps to a status + reason.
        """
        state = _SidecarState()
        state.running.append(
            _RunningTunnel(
                loaded=_make_loaded("up-1"),
                server=MagicMock(),
                tunnel_index=0,
            )
        )
        state.failed.append(_FailedTunnel(tunnel_id="bad-1", reason="kernel boom"))
        response = await _handle_tunnels(_make_request(state))
        assert response.status == 200
        raw_body = response.body
        assert isinstance(raw_body, bytes)
        body = raw_body.decode("utf-8")
        assert "up-1" in body
        assert "bad-1" in body
        assert "kernel boom" in body

    async def test_readyz_returns_200_when_no_failures(self) -> None:
        """Spec — /readyz is 200 only when every tunnel is up.

        Given state with running tunnels and no failures,
        When _handle_readyz runs,
        Then 200 is returned.
        """
        state = _SidecarState()
        state.running.append(
            _RunningTunnel(
                loaded=_make_loaded(),
                server=MagicMock(),
                tunnel_index=0,
            )
        )
        response = await _handle_readyz(_make_request(state))
        assert response.status == 200

    async def test_readyz_returns_503_when_any_failed(self) -> None:
        """Spec — /readyz returns 503 with the failed array when any tunnel failed.

        Given state with one failure,
        When _handle_readyz runs,
        Then 503 + JSON failed array.
        """
        state = _SidecarState()
        state.failed.append(_FailedTunnel(tunnel_id="bad-1", reason="x"))
        response = await _handle_readyz(_make_request(state))
        assert response.status == 503
        body = response.body
        assert isinstance(body, bytes)
        assert b"bad-1" in body


@pytest.mark.asyncio
class TestRunSidecar:
    """End-to-end orchestrator flow with all subsystems mocked."""

    async def test_run_sidecar_bootstrap_then_shutdown(self) -> None:
        """Spec — run_sidecar probes kernel, loads tunnels, starts server, exits 0.

        Given a SettingsService mocked to return one valid tunnel,
        When run_sidecar runs with a pre-set shutdown_event,
        Then it returns 0 AND the tunnel was brought up + torn down.
        """
        service = MagicMock()
        loaded = _make_loaded()
        load_result = LoadResult(tunnels=[loaded], failures=[])
        shutdown_event = asyncio.Event()
        shutdown_event.set()
        server_instance = MagicMock()
        server_instance.start = AsyncMock()
        server_instance.stop = AsyncMock()
        with (
            patch.object(
                egress_sidecar.wg_control,
                "check_kernel_wireguard",
                return_value=None,
            ),
            patch.object(egress_sidecar.wg_control, "bring_up", new=AsyncMock()),
            patch.object(egress_sidecar.wg_control, "bring_down", new=AsyncMock()),
            patch.object(
                egress_sidecar,
                "load_declared_tunnels",
                new=AsyncMock(return_value=load_result),
            ),
            patch.object(egress_sidecar, "Socks5Server", return_value=server_instance),
        ):
            result = await run_sidecar(
                service,
                shutdown_event=shutdown_event,
                healthcheck_port=0,
                healthcheck_host="127.0.0.1",
            )
        assert result == 0
        server_instance.start.assert_awaited_once()
        server_instance.stop.assert_awaited_once()

    async def test_run_sidecar_cleans_tunnels_on_healthcheck_bind_failure(
        self,
    ) -> None:
        """Spec — healthcheck bind failure still runs tunnel shutdown.

        Given tunnel bring-up succeeds but _start_healthcheck_app
            raises (e.g. port 8081 already in use),
        When run_sidecar runs,
        Then the exception propagates AND _shutdown_tunnels still
            runs so the kernel state is clean for the next sidecar
            restart. Pinned by Codex Code Reviewer SC.4 round 1.
        """
        service = MagicMock()
        loaded = _make_loaded()
        load_result = LoadResult(tunnels=[loaded], failures=[])
        server_instance = MagicMock()
        server_instance.start = AsyncMock()
        server_instance.stop = AsyncMock()
        bring_down_mock = AsyncMock()
        with (
            patch.object(
                egress_sidecar.wg_control,
                "check_kernel_wireguard",
                return_value=None,
            ),
            patch.object(egress_sidecar.wg_control, "bring_up", new=AsyncMock()),
            patch.object(egress_sidecar.wg_control, "bring_down", new=bring_down_mock),
            patch.object(
                egress_sidecar,
                "load_declared_tunnels",
                new=AsyncMock(return_value=load_result),
            ),
            patch.object(egress_sidecar, "Socks5Server", return_value=server_instance),
            patch.object(
                egress_sidecar,
                "_start_healthcheck_app",
                new=AsyncMock(side_effect=OSError("address in use")),
            ),
            pytest.raises(OSError, match="address in use"),
        ):
            await run_sidecar(
                service,
                healthcheck_port=0,
                healthcheck_host="127.0.0.1",
            )
        server_instance.stop.assert_awaited_once()
        bring_down_mock.assert_awaited_once()

    async def test_start_healthcheck_app_cleans_runner_on_site_start_failure(
        self,
    ) -> None:
        """Spec — _start_healthcheck_app cleans up AppRunner if TCPSite.start fails.

        Given runner.setup succeeds but site.start raises,
        When _start_healthcheck_app runs,
        Then runner.cleanup is awaited (no leak of the aiohttp
        runner) AND the exception propagates.
        """
        state = _SidecarState()
        runner_instance = MagicMock()
        runner_instance.setup = AsyncMock()
        runner_instance.cleanup = AsyncMock()
        site_instance = MagicMock()
        site_instance.start = AsyncMock(side_effect=OSError("address in use"))
        with (
            patch.object(egress_sidecar.web, "AppRunner", return_value=runner_instance),
            patch.object(egress_sidecar.web, "TCPSite", return_value=site_instance),
            pytest.raises(OSError, match="address in use"),
        ):
            await egress_sidecar._start_healthcheck_app(state, "127.0.0.1", 0)
        runner_instance.cleanup.assert_awaited_once()


class TestInstallSignalHandlers:
    """Wiring of SIGTERM / SIGINT → asyncio.Event.set."""

    def test_install_signal_handlers_wires_sigterm_and_sigint(self) -> None:
        """Spec — install_signal_handlers calls loop.add_signal_handler twice.

        Given a loop mock,
        When install_signal_handlers runs,
        Then add_signal_handler is invoked with SIGTERM AND SIGINT,
            both pointing at shutdown_event.set.
        """
        loop = MagicMock()
        shutdown_event = MagicMock()
        install_signal_handlers(loop, shutdown_event)
        loop.add_signal_handler.assert_any_call(signal.SIGTERM, shutdown_event.set)
        loop.add_signal_handler.assert_any_call(signal.SIGINT, shutdown_event.set)
