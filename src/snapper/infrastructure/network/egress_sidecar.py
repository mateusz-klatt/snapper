"""snapper-egress sidecar orchestrator.

SC.4 of ``proprietary/plans/plan_2026_05_21_snapper_egress_sidecar.md``.

Ties together the per-tunnel pieces shipped in earlier slices:

* SC.0 — ``Socks5Server`` (per-tunnel listener bound to the WG IP).
* SC.1 — ``wg_control.probe_kernel_wireguard`` (runtime probe when
  tunnels are declared) and ``wg_control.bring_up`` / ``bring_down``
  (per-tunnel WireGuard interface + source-based routing).
* SC.2 — ``load_declared_tunnels(service)`` (settings → validated
  descriptors + decrypted keys).

The orchestrator runs three HTTP endpoints on port 8081 for
operator + Compose healthcheck observability:

* ``GET /ready``   — 200 once bootstrap completes (Compose
  depends_on:service_healthy targets this; one failed tunnel must
  NOT block snapper-api startup).
* ``GET /tunnels`` — JSON ``{tunnel_id: {status, reason}}`` for the
  operator dashboard.
* ``GET /readyz``  — 200 only when EVERY declared tunnel is up; 503
  with a ``failed`` array otherwise. Available for stricter Compose
  configs that prefer hard-fail semantics.

Bootstrap order is fixed (settings load → optional kernel probe →
per-tunnel bring-up → start listeners → expose HTTP → wait for
signal). Per-tunnel failures are recorded in the tunnel state map
and surfaced on ``/tunnels``; they do NOT crash the orchestrator.
"""

import asyncio
import signal
from dataclasses import dataclass
from dataclasses import field
from typing import Final

from aiohttp import web
from loguru import logger

from snapper.application.services.settings import SettingsService
from snapper.infrastructure.network import wg_control
from snapper.infrastructure.network.egress_tunnel_models import LoadedTunnel
from snapper.infrastructure.network.egress_tunnel_models import LoadResult
from snapper.infrastructure.network.egress_tunnel_models import TunnelLoadFailure
from snapper.infrastructure.network.egress_tunnel_models import load_declared_tunnels
from snapper.infrastructure.network.socks5_server import Socks5Server

_HEALTHCHECK_PORT: Final[int] = 8081
"""Port the orchestrator binds the /ready / /tunnels / /readyz endpoints to."""

_HEALTHCHECK_HOST: Final[str] = "0.0.0.0"
"""Host the healthcheck app binds to.

Default is the Docker private network only (snapper-egress runs with
no host port mapping per the v4 plan); Compose's healthcheck reaches
it via the in-network DNS.
"""

_STATE_KEY: Final[web.AppKey[_SidecarState]] = web.AppKey("snapper_egress_state")
"""Typed aiohttp app key for the orchestrator state.

The newer aiohttp ``web.AppKey`` API replaces string-keyed app
dictionaries with typed keys. Using it eliminates the
``NotAppKeyWarning`` and gives the handlers strict typing on
``request.app[_STATE_KEY]`` without an ``isinstance`` cast.
"""


@dataclass
class _RunningTunnel:
    """Per-tunnel bookkeeping for tunnels that came up cleanly."""

    loaded: LoadedTunnel
    server: Socks5Server
    tunnel_index: int


@dataclass
class _FailedTunnel:
    """Per-tunnel bookkeeping for tunnels that failed to start."""

    tunnel_id: str
    reason: str


@dataclass
class _SidecarState:
    """Mutable bookkeeping passed to the HTTP handlers.

    The orchestrator builds this once at startup and never reassigns
    its fields — handlers read it and produce JSON responses. The
    dataclass is mutable so the orchestrator can append to the
    lists during bootstrap; once the entrypoint stops accepting new
    work the lists become effectively read-only.
    """

    running: list[_RunningTunnel] = field(default_factory=list)
    failed: list[_FailedTunnel] = field(default_factory=list)


async def run_sidecar(
    settings_service: SettingsService,
    *,
    shutdown_event: asyncio.Event | None = None,
    healthcheck_port: int = _HEALTHCHECK_PORT,
    healthcheck_host: str = _HEALTHCHECK_HOST,
) -> int:
    """Run the snapper-egress orchestrator until shutdown_event fires.

    Bootstrap (per plan §4 sidecar flow):

    1. ``load_declared_tunnels(settings_service)`` enumerates
       declared tunnels + decrypts their keys. If at least one tunnel
       loads, the kernel WireGuard probe runs via ``asyncio.to_thread``
       before the first bring-up. Empty default deployments skip the
       probe so CI and local smoke tests can expose ``/ready`` without
       host WireGuard.
    2. For each successfully loaded tunnel (sorted by id):
         * ``wg_control.bring_up(...)`` — create wg interface,
           configure peer, source-based route.
         * ``Socks5Server.start()`` — bind on the Docker network.
       Per-tunnel failures are caught and turned into ``_FailedTunnel``
       entries on the shared state so the orchestrator stays alive.
    3. Start aiohttp app exposing /ready, /tunnels, /readyz.
    4. Wait for ``shutdown_event`` (set by the SIGTERM/SIGINT
       handlers wired in ``snapper.egress.__main__``).
    5. Reverse the bring-up: stop each Socks5Server, then bring
       down each WG interface.

    Args:
        settings_service: Initialised SettingsService with its cache
            populated. The caller is responsible for ``await
            service.initialize()`` first.
        shutdown_event: Optional event the caller sets to trigger
            graceful shutdown. Defaults to a fresh event the caller
            never sets, which means ``run_sidecar`` runs forever
            (used by the production entrypoint where a signal
            handler sets the event).
        healthcheck_port: Override the default port. Tests use ``0``
            to ask the kernel for any free port; production keeps
            ``8081``.
        healthcheck_host: Override the default host. Tests use
            ``127.0.0.1``; production stays on ``0.0.0.0`` so any
            container on the Docker network can reach it.

    Returns:
        ``0`` for a clean shutdown. A non-zero return is reserved
        for future fail-closed configurations.
    """
    state = _SidecarState()
    load_result = await _load_declared_tunnels_after_kernel_probe(settings_service)
    runner: web.AppRunner | None = None
    try:
        await _bring_up_tunnels(load_result, state)
        runner = await _start_healthcheck_app(state, healthcheck_host, healthcheck_port)
        await (shutdown_event or asyncio.Event()).wait()
    finally:
        if runner is not None:
            await runner.cleanup()
        await _shutdown_tunnels(state)
    return 0


async def _load_declared_tunnels_after_kernel_probe(
    settings_service: SettingsService,
) -> LoadResult:
    """Load declared tunnels and gate real tunnel bring-up on the WG probe.

    Empty/default deployments have no kernel work to do, so they do
    not need WireGuard support just to make ``/ready`` available for
    Docker smoke tests. When at least one tunnel loads, the probe runs
    in a worker thread because pyroute2 0.9.x cannot safely construct
    ``IPRoute`` inside the active asyncio event-loop thread.
    """
    load_result = await load_declared_tunnels(settings_service)
    if not load_result.tunnels:
        return load_result
    failure_reason = await _probe_kernel_wireguard_for_loaded_tunnels()
    if failure_reason is None:
        return load_result
    probe_failures = [
        TunnelLoadFailure(tunnel_id=loaded.descriptor.id, reason=failure_reason)
        for loaded in load_result.tunnels
    ]
    return LoadResult(tunnels=[], failures=[*load_result.failures, *probe_failures])


async def _probe_kernel_wireguard_for_loaded_tunnels() -> str | None:
    """Return a failure reason when the active kernel WireGuard probe fails."""
    try:
        await asyncio.to_thread(wg_control.probe_kernel_wireguard)
    except SystemExit as exc:
        logger.error(
            "sidecar: kernel WireGuard probe exited before tunnel bring-up (code={})",
            exc.code,
        )
        return f"kernel_probe: exited with code {exc.code}"
    except Exception as exc:
        logger.exception("sidecar: kernel WireGuard probe failed before tunnel bring-up")
        return f"kernel_probe: {exc}"
    return None


async def _bring_up_tunnels(load_result: LoadResult, state: _SidecarState) -> None:
    """Bring up each loaded tunnel and record the outcome on ``state``.

    Each tunnel that survives the descriptor/key load is run through
    ``wg_control.bring_up`` and a matching ``Socks5Server``. Any
    exception (kernel error, port already in use, etc.) gets
    converted to a ``_FailedTunnel`` entry — the orchestrator does
    NOT abort, the operator sees the failure on ``/tunnels``.
    """
    for failure in load_result.failures:
        state.failed.append(_FailedTunnel(tunnel_id=failure.tunnel_id, reason=failure.reason))
    for index, loaded in enumerate(load_result.tunnels):
        await _bring_up_one(loaded, index, state)


async def _bring_up_one(loaded: LoadedTunnel, index: int, state: _SidecarState) -> None:
    """Bring up one tunnel — best-effort, all exceptions captured.

    The two-step bring-up (WG interface + SOCKS5 listener) is atomic
    from the operator's perspective: if the SOCKS5 listener fails to
    start we tear down the WG interface so the state is clean for
    the next sidecar restart.
    """
    descriptor = loaded.descriptor
    try:
        await wg_control.bring_up(
            interface=descriptor.interface,
            address=descriptor.address,
            prefix_length=descriptor.prefix_length,
            private_key=loaded.private_key,
            peer_pubkey=descriptor.peer_pubkey,
            peer_endpoint=descriptor.peer_endpoint,
            tunnel_index=index,
            preshared_key=loaded.preshared_key,
            allowed_ips=descriptor.allowed_ips,
        )
    except Exception as exc:
        logger.exception("sidecar: tunnel {} WG bring-up failed", descriptor.id)
        state.failed.append(_FailedTunnel(tunnel_id=descriptor.id, reason=f"bring_up: {exc}"))
        return
    server = Socks5Server(
        bind_addr=descriptor.address,
        listen_port=descriptor.socks5_listen_port,
    )
    try:
        await server.start()
    except Exception as exc:
        logger.exception(
            "sidecar: tunnel {} SOCKS5 listener failed to start",
            descriptor.id,
        )
        await _bring_down_safe(descriptor.interface, index)
        state.failed.append(_FailedTunnel(tunnel_id=descriptor.id, reason=f"socks5_start: {exc}"))
        return
    state.running.append(_RunningTunnel(loaded=loaded, server=server, tunnel_index=index))
    logger.info(
        "sidecar: tunnel {} up — bound {}:{}",
        descriptor.id,
        descriptor.address,
        descriptor.socks5_listen_port,
    )


async def _shutdown_tunnels(state: _SidecarState) -> None:
    """Reverse the bring-up: stop listeners, then tear down WG interfaces."""
    for running in reversed(state.running):
        await _stop_server_safe(running)
        await _bring_down_safe(running.loaded.descriptor.interface, running.tunnel_index)


async def _stop_server_safe(running: _RunningTunnel) -> None:
    """Stop a Socks5Server — log + swallow exceptions during shutdown."""
    try:
        await running.server.stop()
    except Exception:
        logger.exception(
            "sidecar: tunnel {} server stop raised — continuing",
            running.loaded.descriptor.id,
        )


async def _bring_down_safe(interface: str, tunnel_index: int) -> None:
    """Bring a WG interface down — log + swallow exceptions during shutdown."""
    try:
        await wg_control.bring_down(interface=interface, tunnel_index=tunnel_index)
    except Exception:
        logger.exception(
            "sidecar: tunnel interface {} bring_down raised — continuing",
            interface,
        )


async def _start_healthcheck_app(
    state: _SidecarState,
    host: str,
    port: int,
) -> web.AppRunner:
    """Start the aiohttp app with /ready, /tunnels, /readyz routes.

    Returns the ``AppRunner`` so the caller can ``await
    runner.cleanup()`` on shutdown.
    """
    app = web.Application()
    app.router.add_get("/ready", _handle_ready)
    app.router.add_get("/tunnels", _handle_tunnels)
    app.router.add_get("/readyz", _handle_readyz)
    app[_STATE_KEY] = state
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    try:
        await site.start()
    except BaseException:
        await runner.cleanup()
        raise
    logger.info("sidecar: healthcheck app listening on {}:{}", host, port)
    return runner


async def _handle_ready(request: web.Request) -> web.Response:
    """``/ready`` — 200 always once bootstrap completes.

    Compose's depends_on:service_healthy targets this endpoint so
    snapper-api can start when the sidecar container is alive,
    regardless of whether a single tunnel failed.
    """
    await asyncio.sleep(0)
    state = request.app[_STATE_KEY]
    return web.json_response(
        {
            "ready": True,
            "running_tunnels": [r.loaded.descriptor.id for r in state.running],
            "failed_tunnels": [f.tunnel_id for f in state.failed],
        },
        status=200,
    )


async def _handle_tunnels(request: web.Request) -> web.Response:
    """``/tunnels`` — JSON status per declared tunnel.

    For operator dashboards. Returns 200 unconditionally; the
    operator interprets the per-tunnel statuses.
    """
    await asyncio.sleep(0)
    state = request.app[_STATE_KEY]
    payload: dict[str, dict[str, str | None]] = {}
    for running in state.running:
        payload[running.loaded.descriptor.id] = {
            "status": "up",
            "reason": None,
        }
    for failed in state.failed:
        payload[failed.tunnel_id] = {
            "status": "failed",
            "reason": failed.reason,
        }
    return web.json_response(payload, status=200)


async def _handle_readyz(request: web.Request) -> web.Response:
    """``/readyz`` — strict all-up endpoint.

    Returns 200 only when EVERY declared tunnel is running with no
    failures. 503 with a ``failed`` array otherwise. NOT what
    Compose's default healthcheck targets — provided for operators
    who want hard-fail semantics on their Compose configuration.
    """
    await asyncio.sleep(0)
    state = request.app[_STATE_KEY]
    if state.failed:
        return web.json_response(
            {
                "ready": False,
                "failed": [{"tunnel_id": f.tunnel_id, "reason": f.reason} for f in state.failed],
            },
            status=503,
        )
    return web.json_response({"ready": True}, status=200)


def install_signal_handlers(loop: asyncio.AbstractEventLoop, shutdown_event: asyncio.Event) -> None:
    """Wire SIGTERM + SIGINT to set ``shutdown_event``.

    ``loop.add_signal_handler`` is the only safe way to trigger an
    asyncio event from a signal context — direct ``signal.signal``
    callbacks cannot touch asyncio primitives.

    Args:
        loop: The running event loop.
        shutdown_event: The event that should be set when a signal
            arrives. The orchestrator awaits this event in its main
            loop.
    """
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)
