"""Pydantic models + loader for ``egress_tunnel_*`` DB settings.

Three setting keys per tunnel:

* ``egress_tunnel_<id>`` — cleartext JSON descriptor:
  ``{interface, address, prefix_length, peer_pubkey, peer_endpoint,
  allowed_ips, dns, socks5_listen_port, priority}``.
* ``egress_tunnel_<id>_private_key`` — auto-encrypted (matches the
  ``_key`` sensitive pattern in ``SettingsService``).
* ``egress_tunnel_<id>_preshared_key`` — auto-encrypted, optional.

``load_declared_tunnels(service)`` enumerates the cache, validates
each descriptor + matched key payload via ``TunnelDescriptor``, and
returns a list of ``LoadedTunnel`` value objects ready for the
orchestrator to feed into ``wg_control.bring_up`` +
``Socks5Server``.
"""

import ipaddress
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated
from typing import Final
from typing import Literal
from typing import Self

from loguru import logger
from pydantic import BaseModel
from pydantic import BeforeValidator
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from snapper.application.services.settings import SettingsService

_TUNNEL_ID_RE: Final[re.Pattern[str]] = re.compile(r"egress_tunnel_([^_]+)$")
"""Key pattern matching tunnel descriptor settings (not key/preshared variants).

Trailing-``$`` + no underscore inside the id captures
``egress_tunnel_<id>`` but NOT ``egress_tunnel_<id>_private_key`` or
``egress_tunnel_<id>_preshared_key``. Operators MUST keep tunnel ids
underscore-free (only hyphens allowed) — enforced by
``TunnelDescriptor.id`` validation.
"""

_MIN_SOCKS5_LISTEN_PORT: Final[int] = 1024
"""Minimum allowed SOCKS5 listener port (above the privileged range)."""

_MAX_SOCKS5_LISTEN_PORT: Final[int] = 65535
"""Maximum allowed SOCKS5 listener port."""

_MAX_TUNNEL_DECLARED: Final[int] = 1000
"""Maximum number of tunnels supported per sidecar — matches
``wg_control._MAX_TUNNEL_INDEX + 1`` so an enumerator index always
fits the reserved RTABLE / priority range ``[5000, 5999]``."""


def _coerce_str_sequence_to_tuple(value: object) -> tuple[str, ...] | object:
    """Pydantic before-validator for ``tuple[str, ...]`` fields fed from JSON.

    Operators write the descriptor as JSON; ``json.loads`` produces
    Python ``list`` for JSON arrays. With ``ConfigDict(strict=True)``
    Pydantic would reject ``list`` for a ``tuple[str, ...]`` field.
    This helper coerces ``list[str]`` (or ``tuple[str, ...]``) into the
    expected ``tuple[str, ...]`` and lets anything else fall through so
    Pydantic's own validator emits the standard error.
    """
    if isinstance(value, list | tuple) and all(isinstance(item, str) for item in value):
        return tuple(value)
    return value


StringSequence = Annotated[tuple[str, ...], BeforeValidator(_coerce_str_sequence_to_tuple)]
"""Type alias for descriptor fields that accept JSON arrays of strings."""


class TunnelDescriptor(BaseModel):
    """Cleartext per-tunnel descriptor stored under ``egress_tunnel_<id>``.

    Sensitive payload (``private_key`` and optional ``preshared_key``)
    lives in separate auto-encrypted settings — never inline.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    id: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-zA-Z0-9][a-zA-Z0-9\-]*$",
    )
    """Tunnel id. Must be unique across the sidecar AND must NOT contain
    underscores (the loader regex relies on the absence of underscores
    to distinguish ``egress_tunnel_<id>`` from
    ``egress_tunnel_<id>_private_key``)."""

    interface: str = Field(
        min_length=1,
        max_length=15,
        pattern=r"^wg-[a-zA-Z0-9\-]+$",
    )
    """Linux interface name. MUST start with ``wg-`` so the cleanup
    logic in ``wg_control.bring_down`` and operator ``wg show`` output
    is unambiguous. Linux caps interface names at 15 chars (IFNAMSIZ)."""

    address: str
    """IPv4 or IPv6 interface address (no prefix). Validated against
    ``ipaddress.ip_address`` so a parse failure surfaces at config
    time, not at WG bring-up time."""

    prefix_length: int = Field(ge=0, le=128)
    """Address prefix length. Must be ``≤ 32`` for IPv4 addresses (the
    validator enforces this in the family check below)."""

    peer_pubkey: str = Field(min_length=44, max_length=44)
    """Base64-encoded WireGuard peer public key (always 44 chars)."""

    peer_endpoint: str = Field(min_length=3)
    """``"host:port"`` or ``"[ipv6]:port"`` string. The wg_control
    helper resolves the host at bring-up time."""

    allowed_ips: StringSequence = ("0.0.0.0/0", "::/0")
    """WireGuard ``AllowedIPs`` for the peer. Default is full-tunnel
    for both families. Accepts JSON arrays — the before-validator
    coerces ``list[str]`` to ``tuple[str, ...]``."""

    dns: StringSequence = ()
    """DNS servers reachable through the tunnel. Informational
    (the SOCKS5 server uses the sidecar's container resolver, not
    tunnel DNS). Operators can still set this for documentation.
    Accepts JSON arrays."""

    socks5_listen_port: int = Field(
        ge=_MIN_SOCKS5_LISTEN_PORT,
        le=_MAX_SOCKS5_LISTEN_PORT,
    )
    """TCP port the sidecar's ``Socks5Server`` for this tunnel binds
    to. Operators must keep these unique per tunnel — duplicate ports
    will fail at ``Socks5Server.start`` with ``OSError: address in use``.
    Convention: ``1081`` for the first tunnel, ``1082`` for the second,
    etc."""

    priority: int = Field(ge=0)
    """Selection priority surfaced to the snapper-api ``egress_pool``
    setting (lower wins). Informational here — the sidecar itself
    treats all tunnels equally. The pool route entry pointing at this
    tunnel's SOCKS5 endpoint copies this value verbatim."""

    @model_validator(mode="after")
    def _check_address_family(self) -> Self:
        """Verify ``address`` parses + matches the prefix_length range.

        IPv4 addresses MUST have ``prefix_length ≤ 32``; IPv6 may use
        up to 128. The shared ``Field(ge=0, le=128)`` upper-bound
        only catches IPv6's max — IPv4's max needs a model-level check.
        """
        try:
            parsed = ipaddress.ip_address(self.address)
        except ValueError as exc:
            raise ValueError(f"address {self.address!r} is not a valid IPv4/IPv6 address") from exc
        if parsed.version == 4 and self.prefix_length > 32:
            raise ValueError(f"prefix_length {self.prefix_length} > 32 for IPv4 address")
        return self


@dataclass(frozen=True)
class LoadedTunnel:
    """Materialised tunnel ready for orchestrator wiring.

    The descriptor + decrypted secret payload arrive as one bundle
    so the orchestrator doesn't have to know about the per-tunnel
    setting layout.
    """

    descriptor: TunnelDescriptor
    private_key: str
    preshared_key: str | None


@dataclass(frozen=True)
class TunnelLoadFailure:
    """Records a tunnel that failed to load.

    Captures parse / validation / missing-private-key errors so the
    orchestrator can surface them on ``/tunnels`` without aborting
    the whole bring-up.
    """

    tunnel_id: str
    reason: str


@dataclass(frozen=True)
class LoadResult:
    """Outcome of a single ``load_declared_tunnels`` call."""

    tunnels: list[LoadedTunnel]
    failures: list[TunnelLoadFailure]


async def load_declared_tunnels(service: SettingsService) -> LoadResult:
    """Enumerate ``egress_tunnel_*`` settings + return loaded tunnels.

    Per-tunnel steps:

    1. Filter cache keys by ``_TUNNEL_ID_RE``.
    2. Parse descriptor JSON via ``TunnelDescriptor.model_validate_json``.
    3. Read ``egress_tunnel_<id>_private_key`` via the sync
       ``service.get_setting`` (decrypts under the hood).
    4. Read optional ``egress_tunnel_<id>_preshared_key``.
    5. Append a ``LoadedTunnel`` on success or a ``TunnelLoadFailure``
       on any parse / validation / missing-key error so the
       orchestrator can surface degraded state on ``/tunnels``.

    Args:
        service: Initialised ``SettingsService`` with its cache
            already populated (``await service.initialize()``).

    Returns:
        ``LoadResult`` with the two parallel lists. ``failures`` is
        empty on the happy path. Total tunnel count is capped at
        ``_MAX_TUNNEL_DECLARED`` — over-the-limit descriptors are
        recorded as failures so the operator sees the rejection.
    """
    cache = await service.get_all_settings()
    matched_ids: list[str] = []
    for key in cache:
        match = _TUNNEL_ID_RE.match(key)
        if match is not None:
            matched_ids.append(match.group(1))
    matched_ids.sort()
    tunnels: list[LoadedTunnel] = []
    failures: list[TunnelLoadFailure] = []
    for index, tunnel_id in enumerate(matched_ids):
        if index >= _MAX_TUNNEL_DECLARED:
            failures.append(
                TunnelLoadFailure(
                    tunnel_id=tunnel_id,
                    reason=(
                        f"tunnel index {index} exceeds maximum "
                        f"{_MAX_TUNNEL_DECLARED - 1} — would push routing "
                        f"table outside the reserved [5000, 5999] range"
                    ),
                )
            )
            continue
        loaded = _load_one_tunnel(service, cache, tunnel_id)
        if isinstance(loaded, LoadedTunnel):
            tunnels.append(loaded)
        else:
            failures.append(loaded)
    return LoadResult(tunnels=tunnels, failures=failures)


def _load_one_tunnel(
    service: SettingsService,
    cache: Mapping[str, object],
    tunnel_id: str,
) -> LoadedTunnel | TunnelLoadFailure:
    """Read + validate one tunnel triple from the settings cache.

    Caller has already determined that ``egress_tunnel_<id>`` is in
    the cache; this helper handles JSON parse, Pydantic validation,
    and the encrypted private/preshared key reads.
    """
    descriptor_payload = cache[f"egress_tunnel_{tunnel_id}"]
    descriptor = _parse_descriptor(tunnel_id, descriptor_payload)
    if isinstance(descriptor, TunnelLoadFailure):
        return descriptor
    private_key = service.get_setting(f"egress_tunnel_{tunnel_id}_private_key")
    if not isinstance(private_key, str) or not private_key:
        return TunnelLoadFailure(
            tunnel_id=tunnel_id,
            reason="missing or empty egress_tunnel_<id>_private_key",
        )
    preshared_raw = service.get_setting(f"egress_tunnel_{tunnel_id}_preshared_key")
    preshared_key: str | None = None
    if preshared_raw is not None:
        if not isinstance(preshared_raw, str):
            return TunnelLoadFailure(
                tunnel_id=tunnel_id,
                reason="egress_tunnel_<id>_preshared_key is not a string",
            )
        if preshared_raw != "":
            preshared_key = preshared_raw
    return LoadedTunnel(
        descriptor=descriptor,
        private_key=private_key,
        preshared_key=preshared_key,
    )


def _parse_descriptor(
    tunnel_id: str,
    payload: object,
) -> TunnelDescriptor | TunnelLoadFailure:
    """Parse the descriptor JSON / dict into a validated ``TunnelDescriptor``.

    Accepts both already-parsed dicts (the common case — the
    ``SettingsService`` cache may store JSON-typed settings as dicts)
    AND raw strings (defensive — operator could store the JSON as a
    plain string column).
    """
    if isinstance(payload, dict):
        merged: dict[str, object] = dict(payload)
        merged["id"] = tunnel_id
        try:
            descriptor = TunnelDescriptor.model_validate(merged)
        except ValueError as exc:
            return _validation_failure(tunnel_id, exc)
        return descriptor
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as exc:
            logger.error(
                "egress_tunnel: tunnel {} JSON parse failed: {}",
                tunnel_id,
                exc,
            )
            return TunnelLoadFailure(
                tunnel_id=tunnel_id,
                reason=f"JSON parse error: {exc}",
            )
        if not isinstance(parsed, dict):
            return TunnelLoadFailure(
                tunnel_id=tunnel_id,
                reason="descriptor JSON must decode to a dict",
            )
        try:
            descriptor = TunnelDescriptor.model_validate({**parsed, "id": tunnel_id})
        except ValueError as exc:
            return _validation_failure(tunnel_id, exc)
        return descriptor
    return TunnelLoadFailure(
        tunnel_id=tunnel_id,
        reason=f"descriptor value has unexpected type {type(payload).__name__}",
    )


def _validation_failure(
    tunnel_id: str,
    exc: BaseException,
) -> TunnelLoadFailure:
    """Helper — log the Pydantic error and build the failure record."""
    logger.error(
        "egress_tunnel: tunnel {} descriptor validation failed: {}",
        tunnel_id,
        exc,
    )
    return TunnelLoadFailure(
        tunnel_id=tunnel_id,
        reason=f"descriptor validation: {exc}",
    )


def get_tunnel_state_label(failure: TunnelLoadFailure | None) -> Literal["up", "failed"]:
    """Helper for orchestrator ``/tunnels`` endpoint.

    Tiny but exported so tests + the orchestrator agree on the
    string literal.

    Args:
        failure: Optional load-failure record for the tunnel. ``None``
            means the tunnel loaded successfully.

    Returns:
        ``"up"`` when ``failure`` is ``None``; otherwise ``"failed"``.
    """
    if failure is None:
        return "up"
    return "failed"
