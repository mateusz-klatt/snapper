"""``python -m snapper.egress`` entrypoint for the snapper-egress sidecar.

Wires the CLI args + environment + asyncio loop, then hands off to
``snapper.infrastructure.network.egress_sidecar.run_sidecar``.

Required env vars:

* ``DB_URL``           — same value the snapper-api container uses; the
  SettingsService reads tunnel descriptors + encrypted keys from
  here.
* ``ZMQ_BROKER_XSUB``  — same value the snapper-api container uses;
  needed for SettingsService change-broadcast support.
"""

import argparse
import asyncio
import os
import sys

from loguru import logger

from snapper.application.services.settings import get_settings_service
from snapper.infrastructure.network import wg_control
from snapper.infrastructure.network.egress_sidecar import install_signal_handlers
from snapper.infrastructure.network.egress_sidecar import run_sidecar


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse ``--instance-id`` (informational, defaults to ``snapper-egress``)."""
    parser = argparse.ArgumentParser(
        prog="snapper.egress",
        description="snapper-egress sidecar — multi-tunnel WG + SOCKS5",
    )
    parser.add_argument(
        "--instance-id",
        default="snapper-egress",
        help="Informational identifier surfaced in logs (default: snapper-egress)",
    )
    return parser.parse_args(argv)


async def _async_main(args: argparse.Namespace) -> int:
    """Async entrypoint — initialise SettingsService, then orchestrator."""
    db_url = os.environ.get("DB_URL")
    zmq_xsub = os.environ.get("ZMQ_BROKER_XSUB")
    if not db_url or not zmq_xsub:
        logger.error("snapper-egress: DB_URL and ZMQ_BROKER_XSUB env vars are required")
        return 2
    logger.info("snapper-egress: starting (instance_id={})", args.instance_id)
    settings_service = await get_settings_service(db_url, zmq_xsub)
    shutdown_event = asyncio.Event()
    install_signal_handlers(asyncio.get_running_loop(), shutdown_event)
    return await run_sidecar(settings_service, shutdown_event=shutdown_event)


def main(argv: list[str] | None = None) -> int:
    """Sync wrapper called by the Docker ``CMD``.

    Runs the kernel-WireGuard probe BEFORE ``asyncio.run`` because
    pyroute2 0.9.x's ``IPRoute()`` constructor internally calls
    ``loop.run_until_complete`` and fails with "event loop is already
    running" when invoked from inside an active asyncio loop. The
    probe has no async dependency so the sync placement is cleaner
    than wrapping it in ``asyncio.to_thread``.

    Args:
        argv: Optional CLI argv slice (the real ``sys.argv[1:]`` is
            used by default; tests pass an explicit list).

    Returns:
        Process exit code. ``0`` for a clean shutdown, ``2`` for
        missing env vars.
    """
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    wg_control.probe_kernel_wireguard()
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
