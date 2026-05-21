"""WireGuard control plane for the snapper-egress sidecar.

This module is the SC.1 slice of
``proprietary/plans/plan_2026_05_21_snapper_egress_sidecar.md``. It
wraps ``pyroute2`` to bring up and tear down per-tunnel WireGuard
interfaces with source-based routing inside the sidecar container.

Each tunnel ``T`` gets:

* a kernel WireGuard interface ``wg-<T.interface>`` (e.g. ``wg-uk-1``)
* its own routing table ``RTABLE_T = _RTABLE_BASE + tunnel_index``
* a default route in that table pointing at the WG interface
* an ``ip rule from <T.address> table RTABLE_T priority RP_T``
  (where ``RP_T = _RULE_PRIORITY_BASE + tunnel_index``)

The matching ``Socks5Server`` listener binds outbound sockets to
``T.address`` so the kernel's rule sends those packets through
``wg-<T.interface>``. The pattern mirrors the user's working host
setup (``eset-de1`` interface with ``Table = off`` + PostUp custom
routing).

Operational notes:

* ``probe_kernel_wireguard`` is called once at sidecar startup. It
  performs an actual ``ip link add type wireguard`` round-trip and
  exits with a clear error on either missing kernel module (EOPNOTSUPP)
  or missing NET_ADMIN capability (EPERM).
* ``bring_up`` is idempotent — it always calls ``bring_down`` first
  to clean stale state from a previous sidecar crash. ``bring_down``
  cleans by routing-table id (not by address) so a rotated tunnel IP
  does NOT leave behind stale ``ip rule`` entries.
* All pyroute2 work is sync; the public coroutines wrap it in
  ``asyncio.to_thread`` so the event loop is not blocked.
* The reserved RTABLE / priority range is ``[5000, 5999]`` — operators
  must keep other host routing decisions outside this range.
"""

import asyncio
import errno
import ipaddress
import socket
import sys
from typing import Any
from typing import Final

from loguru import logger
from pyroute2 import IPRoute
from pyroute2 import WireGuard
from pyroute2.netlink.exceptions import NetlinkError

_RTABLE_BASE: Final[int] = 5000
"""Base routing-table id for per-tunnel custom tables.

For tunnel index ``i``, the custom table is ``_RTABLE_BASE + i``.
Reserved range ``[5000, 5999]`` — operators must keep other host
routing decisions outside this range.
"""

_RULE_PRIORITY_BASE: Final[int] = 5000
"""Base ip-rule priority for per-tunnel source-based routing rules."""

_PERSISTENT_KEEPALIVE: Final[int] = 25
"""WireGuard keepalive period in seconds — keeps NAT mapping warm."""

_PROBE_INTERFACE_NAME: Final[str] = "wg-probe-snapper"
"""Throw-away interface name used by :func:`probe_kernel_wireguard`."""

_MAX_TUNNEL_INDEX: Final[int] = 999
"""Maximum allowed tunnel_index so the derived table + priority stay
inside the reserved range ``[5000, 5999]``."""


def probe_kernel_wireguard() -> None:
    """Active round-trip probe for kernel WireGuard + NET_ADMIN availability.

    Performs an ``ip link add type wireguard`` followed by ``ip link
    del`` to verify the kernel module is loaded AND the sidecar
    container has the capabilities to manipulate it. Calls
    ``sys.exit(1)`` with a clear log line on either EOPNOTSUPP
    (kernel module missing) or EPERM (NET_ADMIN missing). Any
    other ``NetlinkError`` propagates so the operator sees the
    underlying cause.

    Tolerates a stale ``wg-probe-snapper`` interface from a previous
    crashed run: the probe deletes any pre-existing interface of that
    name before the create attempt so EEXIST cannot mask the real
    state of the kernel module.
    """
    ipr = IPRoute()
    try:
        _delete_probe_if_present(ipr)
        try:
            ipr.link("add", ifname=_PROBE_INTERFACE_NAME, kind="wireguard")
        except NetlinkError as exc:
            if exc.code == errno.EOPNOTSUPP:
                logger.error(
                    "wg_control: kernel WireGuard not available — "
                    "run `modprobe wireguard` on the Docker host"
                )
                sys.exit(1)
            if exc.code == errno.EPERM:
                logger.error(
                    "wg_control: snapper-egress missing NET_ADMIN "
                    "capability — check Compose cap_add: [NET_ADMIN]"
                )
                sys.exit(1)
            raise
        idx = ipr.link_lookup(ifname=_PROBE_INTERFACE_NAME)
        if idx:
            ipr.link("del", index=idx[0])
    finally:
        ipr.close()


def _delete_probe_if_present(ipr: IPRoute) -> None:
    """Helper — remove ``wg-probe-snapper`` if it survived from a prior run.

    Defensive against the rare case where the sidecar container was
    killed mid-probe. Swallows NetlinkError so a missing link is a
    no-op (the common case).
    """
    try:
        idx = ipr.link_lookup(ifname=_PROBE_INTERFACE_NAME)
    except NetlinkError:
        return
    if not idx:
        return
    try:
        ipr.link("del", index=idx[0])
    except NetlinkError:
        return


def _resolve_endpoint(host_port: str) -> tuple[str, int]:
    """Parse a ``host:port`` endpoint and resolve the host to an IP.

    Splits on the rightmost ``:`` so IPv6 in square brackets
    (``[2001:db8::1]:51820``) is handled correctly. Resolves the
    host name via ``socket.getaddrinfo`` with ``AF_UNSPEC`` so the
    operator can use either IPv4 or IPv6 endpoints transparently.

    Args:
        host_port: An endpoint string in WireGuard ``Endpoint = ...``
            format, e.g. ``"fra-113-wg.whiskergalaxy.com:443"`` or
            ``"[2001:db8::1]:51820"``.

    Returns:
        A ``(resolved_ip, port)`` tuple ready for
        ``pyroute2.WireGuard.set(peer={'endpoint_addr': ip,
        'endpoint_port': port, ...})``.

    Raises:
        ValueError: When the endpoint is missing a port, the port
            is out of range, the host is unresolvable, or the
            resolver returns no addresses.
    """
    last_colon = host_port.rfind(":")
    if last_colon < 0:
        raise ValueError(f"endpoint {host_port!r} missing ':<port>'")
    host = host_port[:last_colon].strip("[]")
    port_str = host_port[last_colon + 1 :]
    try:
        port = int(port_str)
    except ValueError as exc:
        raise ValueError(f"endpoint {host_port!r} port {port_str!r} not an integer") from exc
    if port < 1 or port > 65535:
        raise ValueError(f"endpoint {host_port!r} port {port} out of range")
    try:
        infos = socket.getaddrinfo(
            host,
            port,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_DGRAM,
        )
    except socket.gaierror as exc:
        raise ValueError(f"endpoint {host!r} unresolvable: {exc}") from exc
    if not infos:
        raise ValueError(f"endpoint {host!r} returned no addresses")
    resolved_addr = infos[0][4][0]
    if not isinstance(resolved_addr, str):
        raise ValueError(
            f"endpoint {host!r} resolver returned non-string address {resolved_addr!r}"
        )
    return resolved_addr, port


async def bring_up(
    *,
    interface: str,
    address: str,
    prefix_length: int,
    private_key: str,
    peer_pubkey: str,
    peer_endpoint: str,
    tunnel_index: int,
    preshared_key: str | None = None,
    allowed_ips: tuple[str, ...] = ("0.0.0.0/0", "::/0"),
) -> None:
    """Bring up ``wg-<interface>`` idempotently with source-based routing.

    Steps (each safe to re-run on its own):

    1. ``bring_down(interface, tunnel_index)`` — clears stale state.
       Handles partial prior bring-up that crashed before cleanup.
    2. Resolve ``peer_endpoint`` via ``_resolve_endpoint``.
    3. Create the WG interface via ``IPRoute.link('add', kind=wireguard)``.
    4. Configure peer + keys via ``WireGuard.set``.
    5. Add interface address.
    6. Bring the interface administratively up.
    7. Add default route in custom table ``_RTABLE_BASE + tunnel_index``.
    8. Add ``ip rule from <address> table N priority N``.

    Pyroute2 calls are synchronous and run inside ``asyncio.to_thread``
    so the event loop is not blocked.

    Args:
        interface: Interface name AFTER the ``wg-`` prefix has been
            stripped by the caller (the actual interface created is
            ``interface`` verbatim — caller passes ``"wg-uk-1"``).
        address: IPv4 or IPv6 interface address WITHOUT the prefix
            length (e.g. ``"10.64.12.34"`` or ``"2001:db8::1"``).
            Inner-routing family is inferred from this value.
        prefix_length: The address prefix length (e.g. ``32`` for an
            individual IPv4 address, ``128`` for IPv6).
        private_key: Base64-encoded WireGuard private key.
        peer_pubkey: Base64-encoded WireGuard peer public key.
        peer_endpoint: ``"host:port"`` or ``"[ipv6]:port"`` string
            for the WireGuard peer. Hostname resolution happens
            synchronously inside this call.
        tunnel_index: Stable index in ``[0, 999]`` used to derive
            the custom routing table id and ip-rule priority
            (``_RTABLE_BASE + tunnel_index``). Out-of-range values
            raise ``ValueError`` before any kernel state is touched.
        preshared_key: Optional base64 PSK.
        allowed_ips: WireGuard ``AllowedIPs`` for the peer. Default
            is the full-tunnel pair ``("0.0.0.0/0", "::/0")``.

    Raises:
        ValueError: When ``tunnel_index`` is outside ``[0, 999]``
            or ``address`` is not a valid IP address.
    """
    if tunnel_index < 0 or tunnel_index > _MAX_TUNNEL_INDEX:
        raise ValueError(
            f"tunnel_index {tunnel_index} outside reserved range "
            f"[0, {_MAX_TUNNEL_INDEX}] — would collide with host routing"
        )
    await asyncio.to_thread(
        _bring_up_sync,
        interface,
        address,
        prefix_length,
        private_key,
        peer_pubkey,
        peer_endpoint,
        tunnel_index,
        preshared_key,
        allowed_ips,
    )


def _bring_up_sync(
    interface: str,
    address: str,
    prefix_length: int,
    private_key: str,
    peer_pubkey: str,
    peer_endpoint: str,
    tunnel_index: int,
    preshared_key: str | None,
    allowed_ips: tuple[str, ...],
) -> None:
    """Synchronous core of :func:`bring_up`.

    Runs via ``asyncio.to_thread`` so the public coroutine is
    async-friendly while the actual pyroute2 work stays sync.

    Address-family handling: detected from ``address`` via
    ``ipaddress.ip_address``. Used to pick the right
    ``family``/``src_len`` for the policy rule and ``dst``/``family``
    for the default route. Mixing inner IPv4 and IPv6 in a single
    tunnel is not supported by this slice — caller passes one address
    of one family.
    """
    parsed_address = ipaddress.ip_address(address)
    if parsed_address.version == 4:
        family = socket.AF_INET
        rule_src_len = 32
        default_dst = "0.0.0.0/0"
    else:
        family = socket.AF_INET6
        rule_src_len = 128
        default_dst = "::/0"
    rtable = _RTABLE_BASE + tunnel_index
    priority = _RULE_PRIORITY_BASE + tunnel_index
    _bring_down_sync(interface, rtable, priority)
    endpoint_addr, endpoint_port = _resolve_endpoint(peer_endpoint)
    ipr = IPRoute()
    try:
        ipr.link("add", ifname=interface, kind="wireguard")
        idx_list = ipr.link_lookup(ifname=interface)
        if not idx_list:
            raise RuntimeError(f"wg_control: created {interface!r} but link_lookup found nothing")
        idx = idx_list[0]
        peer: dict[str, Any] = {
            "public_key": peer_pubkey,
            "endpoint_addr": endpoint_addr,
            "endpoint_port": endpoint_port,
            "persistent_keepalive": _PERSISTENT_KEEPALIVE,
            "allowed_ips": list(allowed_ips),
        }
        if preshared_key is not None:
            peer["preshared_key"] = preshared_key
        wg = WireGuard()
        try:
            wg.set(interface, private_key=private_key, peer=peer)
        finally:
            wg.close()
        ipr.addr("add", index=idx, address=address, prefixlen=prefix_length)
        ipr.link("set", index=idx, state="up")
        ipr.route(
            "add",
            dst=default_dst,
            oif=idx,
            table=rtable,
            family=family,
        )
        ipr.rule(
            "add",
            src=address,
            src_len=rule_src_len,
            table=rtable,
            priority=priority,
            family=family,
        )
        logger.info(
            "wg_control: brought up {} addr={}/{} family=v{} peer={}:{} table={} priority={}",
            interface,
            address,
            prefix_length,
            parsed_address.version,
            endpoint_addr,
            endpoint_port,
            rtable,
            priority,
        )
    finally:
        ipr.close()


async def bring_down(*, interface: str, tunnel_index: int) -> None:
    """Idempotent tear-down: remove ip rules, flush table, delete interface.

    Cleans BY ROUTING TABLE ID — not by current address — so a rotated
    tunnel IP does not leave behind stale ``ip rule`` entries.
    Pyroute2 calls run inside ``asyncio.to_thread``.

    Args:
        interface: Interface name (e.g. ``"wg-uk-1"``).
        tunnel_index: Stable index used to derive the custom routing
            table id; matches the ``tunnel_index`` passed to
            :func:`bring_up`.
    """
    rtable = _RTABLE_BASE + tunnel_index
    priority = _RULE_PRIORITY_BASE + tunnel_index
    await asyncio.to_thread(_bring_down_sync, interface, rtable, priority)


def _bring_down_sync(interface: str, rtable: int, priority: int) -> None:
    """Synchronous core of :func:`bring_down`.

    Best-effort per step — each pyroute2 call is wrapped in a
    ``try / except NetlinkError`` so missing entries (stale state,
    fresh container) do not block the rest of the cleanup.
    """
    ipr = IPRoute()
    try:
        _remove_rules_by_table(ipr, rtable)
        _flush_routes(ipr, rtable)
        _delete_interface(ipr, interface)
    finally:
        ipr.close()
    logger.info(
        "wg_control: brought down interface={} table={} priority={}",
        interface,
        rtable,
        priority,
    )


def _remove_rules_by_table(ipr: IPRoute, rtable: int) -> None:
    """Delete every ip-rule pointing at ``rtable``.

    Iterates ``get_rules()`` and deletes each rule whose
    ``FRA_TABLE`` attribute equals ``rtable``. Handles the case
    where an address rotation left multiple stale rules at
    different priorities pointing at the same table.
    """
    try:
        rules = ipr.get_rules()
    except NetlinkError:
        return
    for rule in rules:
        try:
            rule_table = rule.get_attr("FRA_TABLE")
        except (AttributeError, KeyError):
            continue
        if rule_table != rtable:
            continue
        rule_priority = rule.get("priority")
        try:
            if rule_priority is not None:
                ipr.rule("del", table=rtable, priority=rule_priority)
            else:
                ipr.rule("del", table=rtable)
        except NetlinkError:
            continue


def _flush_routes(ipr: IPRoute, rtable: int) -> None:
    """Flush every route in ``rtable``. Silently ignored if missing."""
    try:
        ipr.flush_routes(table=rtable)
    except NetlinkError:
        return


def _delete_interface(ipr: IPRoute, interface: str) -> None:
    """Delete ``interface`` if it exists. No-op when absent."""
    try:
        idx_list = ipr.link_lookup(ifname=interface)
    except NetlinkError:
        return
    if not idx_list:
        return
    try:
        ipr.link("del", index=idx_list[0])
    except NetlinkError:
        return
