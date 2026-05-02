"""Persistent APNs HTTP/2 connection pool for iOS Push Foundation.

Wraps ``aioapns.APNs`` with a small, opinionated surface: one
``ApnsClientPool`` instance owns one sandbox client + one production
client (sharing the same PEM private key, only the ``use_sandbox``
flag differs) and routes sends by the target device's recorded
``env`` column. Constructed inside an async context (the
``aioapns.APNs`` constructor calls ``asyncio.get_event_loop()``
internally and raises on Py 3.12+ if invoked outside a running loop).
Returned by ``build_apns_client_pool`` rather than a classmethod so
tests can fake out both ends without needing an event loop.
"""

from dataclasses import dataclass

from aioapns import APNs as _AioApns
from aioapns import NotificationRequest
from aioapns.common import NotificationResult
from aioapns.common import PushType

from snapper.application.notify.apns_config import ApnsConfig
from snapper.core.json_types import JsonObject


@dataclass(frozen=True, slots=True)
class ApnsSendResult:
    """Small, stable result envelope the sidecar logs and persists.

    The sidecar maps a subset of ``NotificationResult`` into this
    shape so the retry policy doesn't leak the
    ``aioapns``-specific response class into the outbox code path.

    Attributes:
        status_code: Numeric HTTP/2 status from APNs
            (200, 410, 429, 500, 503, etc.).
        status: Coarse status string (``success``, ``unregistered``,
            ``throttled``, ``server_error``, ``other``).
        apns_id: The server-minted ``apns-id`` echoed back on success,
            used to correlate with Apple's delivery diagnostics.
            Empty string when absent.
        description: APNs error reason (e.g. ``BadDeviceToken``) or
            empty string on success.

    Note:
        aioapns 4.0 does not expose the ``Retry-After`` header on
        429 responses; the sidecar applies its own backoff schedule
        for all throttled / server-error cases rather than
        reading Apple's hint. This is an acceptable trade-off because
        Apple's hint is advisory and the schedule is already
        exponential with a 5-minute cap.
    """

    status_code: int
    status: str
    apns_id: str
    description: str


class ApnsClientPool:
    """Two-client pool (sandbox + production) sharing one PEM key.

    Constructed inside an async context so ``aioapns``' internal
    ``asyncio.get_event_loop()`` call resolves to the running loop
    (compatibility note for Py 3.12+).

    Args:
        sandbox_client: Optional sandbox-bound ``aioapns.APNs``.
            ``None`` when the config's environment is ``production``
            only.
        production_client: Optional production-bound ``aioapns.APNs``.
            ``None`` when the config's environment is ``sandbox``
            only.

    Construction via ``build_apns_client_pool`` is preferred outside
    tests — it applies the PEM-shared, env-scoped defaults.
    """

    def __init__(
        self,
        sandbox_client: _AioApns | None,
        production_client: _AioApns | None,
    ) -> None:
        """Hold the two env-scoped aioapns clients (either may be None)."""
        if sandbox_client is None and production_client is None:
            raise ValueError(
                "ApnsClientPool needs at least one of sandbox_client "
                "or production_client — both None makes send() fail."
            )
        self._sandbox = sandbox_client
        self._production = production_client

    def _select_client(self, env: str) -> _AioApns:
        """Pick the client that matches the target device's env.

        Args:
            env: ``sandbox`` or ``prod`` (matches
                ``NotificationDevice.env``).

        Returns:
            The matching ``aioapns.APNs`` instance.

        Raises:
            ValueError: If the configured pool doesn't cover the
                requested env (e.g. device registered for ``prod``
                but only the sandbox client was built).
        """
        if env == "sandbox":
            if self._sandbox is None:
                raise ValueError(
                    "ApnsClientPool has no sandbox client but a "
                    "sandbox device send was requested."
                )
            return self._sandbox
        if env == "prod":
            if self._production is None:
                raise ValueError(
                    "ApnsClientPool has no production client but a "
                    "production device send was requested."
                )
            return self._production
        raise ValueError(f"Unknown APNs env {env!r} — must be 'sandbox' or 'prod'.")

    async def send(
        self,
        env: str,
        device_token: str,
        payload: JsonObject,
        apns_topic: str,
        priority: int = 10,
        push_type: str = "alert",
        collapse_id: str | None = None,
    ) -> ApnsSendResult:
        """Send one push notification through the env-matched client.

        Args:
            env: ``sandbox`` or ``prod`` — the device's recorded env.
            device_token: 64-hex APNs device token.
            payload: Full APNs ``aps``-dict payload (includes
                ``{"aps": {...}, ...custom}``). Caller is
                responsible for sizing — see APNs 4KB limit.
            apns_topic: Usually identical to the app bundle id
                (``ApnsConfig.topic``). Forwarded as the
                ``apns-topic`` HTTP/2 header.
            priority: APNs priority (5 = throttleable, 10 =
                immediate). Defaults to 10 per safety-critical
                delivery contract; non-critical senders may lower.
            push_type: APNs push type (``alert`` | ``background`` |
                ``voip`` | ...). Defaults to ``alert``.
            collapse_id: Optional APNs collapse-id — groups
                notifications with the same id so iOS shows only the
                latest. Maps 1:1 onto ``AlertEventData.thread_key``
                at the sidecar mapping layer.

        Returns:
            ``ApnsSendResult`` mapping the aioapns response into the
            coarse-grained shape the outbox persists.
        """
        request = NotificationRequest(
            device_token=device_token,
            message=payload,
            notification_id=None,
            time_to_live=None,
            push_type=PushType(push_type),
            priority=priority,
            collapse_key=collapse_id,
            apns_topic=apns_topic,
        )
        client = self._select_client(env)
        result: NotificationResult = await client.send_notification(request)
        return _map_result(result)


def _map_result(result: NotificationResult) -> ApnsSendResult:
    """Translate an ``aioapns`` ``NotificationResult`` into our envelope.

    Args:
        result: The raw ``aioapns`` response.

    Returns:
        An ``ApnsSendResult`` using coarse-grained ``status`` strings
        the outbox code path pattern-matches on.
    """
    description = result.description or ""
    status_code = _status_code_from(result.status)
    coarse = _coarse_status(status_code, description)
    notification_id = result.notification_id or ""
    return ApnsSendResult(
        status_code=status_code,
        status=coarse,
        apns_id=notification_id,
        description=description,
    )


def _status_code_from(status: str) -> int:
    """Map aioapns' string ``status`` onto the canonical HTTP/2 code.

    ``aioapns`` exposes the status as the numeric HTTP code rendered
    as a string (e.g. ``'200'``, ``'410'``, ``'429'``) — we cast it
    back to int so the retry policy can switch on the same
    integer buckets regardless of the underlying library. Unknown /
    empty strings collapse to 0 (handled as ``other`` coarse status).
    """
    try:
        return int(status)
    except (TypeError, ValueError):
        return 0


def _coarse_status(status_code: int, description: str) -> str:
    """Derive the coarse outbox status from the numeric HTTP/2 code.

    The outbox persists this value on the SCD2 ``alert_deliveries``
    row so retries can dispatch without re-deriving it from a
    library-specific representation.
    """
    if status_code == 200:
        return "success"
    if status_code == 410:
        return "unregistered"
    if status_code == 429:
        return "throttled"
    if 500 <= status_code < 600:
        return "server_error"
    if description:
        return "other"
    return "unknown"


def build_apns_client_pool(config: ApnsConfig) -> ApnsClientPool:
    """Build an ``ApnsClientPool`` from a hydrated ``ApnsConfig``.

    Must be called from within a running asyncio event loop — see the
    note on ``aioapns.APNs`` constructor behavior under Py 3.12+.

    Args:
        config: Hydrated config (PEM string already decoded from
            base64).

    Returns:
        Ready-to-use ``ApnsClientPool``. Sandbox client is built when
        the config's environment is ``sandbox`` or
        ``sandbox_and_production``; production client likewise for
        ``production`` or ``sandbox_and_production``. Both PEM keys
        are shared (only ``use_sandbox`` differs).
    """
    sandbox: _AioApns | None = None
    production: _AioApns | None = None
    env = config.environment
    if env in {"sandbox", "sandbox_and_production"}:
        sandbox = _AioApns(
            key=config.private_key_pem,
            key_id=config.key_id,
            team_id=config.team_id,
            topic=config.topic,
            use_sandbox=True,
        )
    if env in {"production", "sandbox_and_production"}:
        production = _AioApns(
            key=config.private_key_pem,
            key_id=config.key_id,
            team_id=config.team_id,
            topic=config.topic,
            use_sandbox=False,
        )
    return ApnsClientPool(sandbox_client=sandbox, production_client=production)
