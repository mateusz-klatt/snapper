r"""One-shot provisioning helper for a new snapper-egress WireGuard tunnel.

Used by the operator (Claude) once an external VPN provider (ESET, Mullvad,
Proton, …) returns the peer-side WireGuard parameters. Performs three
distinct DB writes against the live SettingsService:

1. ``egress_tunnel_<id>``                — TunnelDescriptor JSON (plain).
2. ``egress_tunnel_<id>_private_key``    — Fernet-encrypted Curve25519 key.
3. ``egress_pool`` (read-modify-write)   — append a ``socks5`` route and
   force ``enabled: true`` if currently false.

Then triggers a snapper-egress restart so the sidecar picks up the new
tunnel via its bootstrap path. The snapper-api egress pool reconfigures
itself from the ZMQ settings-change event without a hard restart.

Run inside the ``snapper`` container so it shares ``DB_URL`` +
``ZMQ_BROKER_XSUB`` env vars with the live service:

    docker exec -i snapper python /app/scripts/provision_egress_tunnel.py \\
        --tunnel-id eset-ie1-grafton \\
        --interface wg-ie1 \\
        --address 10.64.131.201 \\
        --prefix-length 32 \\
        --private-key-file /app/.local-secrets/wg/eset-ie1-grafton.privkey \\
        --peer-pubkey '<peer-pubkey-44ch>' \\
        --peer-endpoint 'vpn.example.com:51820' \\
        --socks5-listen-port 1081 \\
        --priority 10

The script is intentionally explicit (no defaults that mask provider-side
typos). Each step is idempotent — re-running with the same tunnel-id
overwrites the descriptor + key in place and does not duplicate the route
inside egress_pool.
"""

import argparse
import asyncio
import os
import subprocess
import sys
from pathlib import Path


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """CLI parser — all fields are required to avoid silent provider drift."""
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--tunnel-id", required=True, help="Stable id without underscores (used in setting key)"
    )
    p.add_argument("--interface", required=True, help="WG interface name, ≤15 chars, prefix wg-")
    p.add_argument("--address", required=True, help="Tunnel local IPv4 (e.g. 10.64.131.201)")
    p.add_argument("--prefix-length", type=int, required=True, help="Usually 32 for ESET / Mullvad")
    p.add_argument(
        "--private-key-file",
        required=True,
        help="Path to file holding base64 WG private key (single line)",
    )
    p.add_argument(
        "--peer-pubkey", required=True, help="Peer (gateway) public key, 44 base64 chars"
    )
    p.add_argument("--peer-endpoint", required=True, help="host:port of gateway")
    p.add_argument("--preshared-key", default=None, help="Optional PSK (base64)")
    p.add_argument(
        "--socks5-listen-port",
        type=int,
        required=True,
        help="Port the sidecar binds for SOCKS5 (1024–65535)",
    )
    p.add_argument("--priority", type=int, required=True, help="Pool priority (lower = preferred)")
    p.add_argument(
        "--allowed-ips", default="0.0.0.0/0,::/0", help="Comma-separated CIDRs (default catch-all)"
    )
    p.add_argument(
        "--restart-sidecar", action="store_true", help="Also docker restart snapper-egress"
    )
    p.add_argument(
        "--pool-host", default="snapper-egress", help="Docker DNS name of sidecar (for proxy_url)"
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate descriptor + private key, then print plan without writing DB",
    )
    return p.parse_args(argv)


async def _amain(args: argparse.Namespace) -> int:
    """Async core — performs the three DB writes + pool merge.

    The ``--dry-run`` flag short-circuits AFTER descriptor Pydantic
    validation but BEFORE any DB write or sidecar restart, so the
    operator can sanity-check the schema mapping (especially the
    ``id`` regex and ``interface`` length constraints) without
    polluting the live service.
    """
    from snapper.application.services.settings import get_settings_service
    from snapper.infrastructure.network.egress_tunnel_models import TunnelDescriptor

    db_url = os.environ["DB_URL"]
    zmq_broker = os.environ.get("ZMQ_BROKER_XSUB", "tcp://snapper-zmq-broker:7500")

    private_key = Path(args.private_key_file).read_text().strip()
    if len(private_key) != 44 or not private_key.endswith("="):
        print(f"!! private key length={len(private_key)} (expected 44) — aborting", file=sys.stderr)
        return 2

    descriptor_payload = {
        "id": args.tunnel_id,
        "interface": args.interface,
        "address": args.address,
        "prefix_length": args.prefix_length,
        "peer_pubkey": args.peer_pubkey,
        "peer_endpoint": args.peer_endpoint,
        "allowed_ips": [s.strip() for s in args.allowed_ips.split(",") if s.strip()],
        "socks5_listen_port": args.socks5_listen_port,
        "priority": args.priority,
    }
    descriptor_model = TunnelDescriptor(**descriptor_payload)
    print(f"[ok] descriptor validates: {descriptor_model.id} -> {descriptor_model.interface}")

    proxy_url = f"socks5h://{args.pool_host}:{args.socks5_listen_port}"
    if args.dry_run:
        print("[dry-run] would write:")
        print(f"  - egress_tunnel_{args.tunnel_id} = <descriptor>")
        print(f"  - egress_tunnel_{args.tunnel_id}_private_key = <encrypted 44B>")
        if args.preshared_key:
            print(f"  - egress_tunnel_{args.tunnel_id}_preshared_key = <encrypted>")
        print(f"  - egress_pool: append route id={args.tunnel_id} proxy={proxy_url}")
        return 0

    service = await get_settings_service(db_url, zmq_broker)

    desc_key = f"egress_tunnel_{args.tunnel_id}"
    priv_key_key = f"egress_tunnel_{args.tunnel_id}_private_key"
    print(f"[1/3] writing {desc_key} (descriptor)")
    await service.update_setting(desc_key, descriptor_payload)

    print(f"[2/3] writing {priv_key_key} (auto-encrypted via '_private_key' suffix)")
    await service.update_setting(priv_key_key, private_key)
    if args.preshared_key:
        psk_key = f"egress_tunnel_{args.tunnel_id}_preshared_key"
        print(f"     and {psk_key} (auto-encrypted via '_preshared_key' suffix)")
        await service.update_setting(psk_key, args.preshared_key)

    print("[3/3] merging route into egress_pool")
    pool = service.get_setting("egress_pool")
    if pool is None:
        pool = {"enabled": True, "on_all_quarantined": "wait", "routes": []}
    routes = list(pool.get("routes", []))
    new_route = {
        "id": args.tunnel_id,
        "kind": "socks5",
        "proxy_url": proxy_url,
        "priority": args.priority,
        "enabled": True,
    }
    routes = [r for r in routes if r.get("id") != args.tunnel_id]
    routes.append(new_route)
    pool["routes"] = routes
    pool["enabled"] = True
    await service.update_setting("egress_pool", pool)
    print(f"     route '{args.tunnel_id}' active, pool enabled={pool['enabled']}")
    print(f"     proxy_url={proxy_url}")

    return 0


def main() -> int:
    """Sync wrapper used by Docker exec.

    Returns:
        Process exit code: ``0`` on success, ``2`` for an invalid
        private key, ``3`` when the optional ``--restart-sidecar``
        docker call fails.
    """
    args = _parse_args(sys.argv[1:])
    rc = asyncio.run(_amain(args))
    if rc != 0:
        return rc
    if args.restart_sidecar:
        print("[+] docker restart snapper-egress")
        try:
            subprocess.run(["docker", "restart", "snapper-egress"], check=True)
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            print(f"!! docker restart failed: {exc}", file=sys.stderr)
            return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
