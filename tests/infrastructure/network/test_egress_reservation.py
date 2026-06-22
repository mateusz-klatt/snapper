"""Unit tests for EgressReservation lifecycle.

Explicit tests for the dual-release contract, weakref finalizer
behaviour, and quarantine timestamp recording.
"""

import gc
import weakref
from datetime import UTC
from datetime import datetime

from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_pool import EgressPool
from snapper.infrastructure.network.egress_reservation import _finalize_release


def _build_pool() -> EgressPool:
    """Helper — construct a 1-route pool used by all tests in this module."""
    config = EgressPoolConfig(
        enabled=True,
        routes=[RouteConfig(id="d", kind="direct")],
    )
    return EgressPool(config)


class TestRelease:
    """Tests for ``EgressReservation.release`` correctness."""

    def test_release_decrements_pool_in_use_count(self) -> None:
        """Spec — explicit release brings in_use_count back to 0.

        Given a pool with one direct route and one outstanding reservation,
        When release() is called,
        Then the snapshot's in_use_count is 0.
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        snapshot_before = pool.snapshot()[0]
        assert snapshot_before.in_use_count == 1
        reservation.release()
        snapshot_after = pool.snapshot()[0]
        assert snapshot_after.in_use_count == 0

    def test_release_idempotent(self) -> None:
        """Spec — calling release() multiple times does not drive count negative.

        Given a reservation whose release() has been called once,
        When release() is called two more times,
        Then in_use_count stays at 0 (clamped).
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        reservation.release()
        reservation.release()
        reservation.release()
        snapshot = pool.snapshot()[0]
        assert snapshot.in_use_count == 0

    def test_release_detaches_weakref_finalizer(self) -> None:
        """Spec — explicit release detaches the defensive weakref.

        Given a reservation whose release() has run,
        When the reservation is dropped and GC is forced,
        Then the finalizer does NOT fire (in_use_count stays 0,
            does NOT briefly dip to -1 / get clamped from a double
            decrement).

        This contract requires that a release followed by GC produces
        exactly ONE decrement total, with no second invocation of the
        finalize callback.
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        finalizer = reservation._finalizer
        assert finalizer.alive is True
        reservation.release()
        assert finalizer.alive is False
        del reservation
        gc.collect()
        snapshot = pool.snapshot()[0]
        assert snapshot.in_use_count == 0


class TestFinalizer:
    """Tests for the weakref finalizer defensive path."""

    def test_finalizer_releases_when_explicit_release_missing(self) -> None:
        """Spec — finalizer fires on GC when release() is never called.

        Given a reservation that goes out of scope without release(),
        When the GC collects it,
        Then the finalizer runs and pool.in_use_count is decremented
            to 0.

        This proves the defensive path actually engages — without
        it, leaked reservations would gradually exhaust the pool's
        capacity.
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        assert pool.snapshot()[0].in_use_count == 1
        ref = weakref.ref(reservation)
        del reservation
        gc.collect()
        assert ref() is None
        snapshot = pool.snapshot()[0]
        assert snapshot.in_use_count == 0

    def test_release_after_finalizer_fired_is_safe(self) -> None:
        """Spec — direct call to _finalize_release then release is safe.

        Given a pool with one outstanding reservation,
        When the finalize callback is invoked directly (simulating GC),
        And then release() is called on the live reservation,
        Then in_use_count goes 1 -> 0 (via finalizer) -> stays 0
            (clamped via explicit release with detached finalizer).
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        _finalize_release(
            pool,
            reservation.route_id,
            reservation.exchange,
            reservation.traffic_class,
            reservation.connection_kind,
            reservation.target_host,
        )
        assert pool.snapshot()[0].in_use_count == 0
        assert pool.status_snapshot().routes[0].active_reservations == []
        reservation.release()
        assert pool.snapshot()[0].in_use_count == 0


class TestQuarantine:
    """Tests for ``EgressReservation.quarantine`` timestamp + extend semantics."""

    def test_quarantine_extends_existing_deadline(self) -> None:
        """Spec — a longer quarantine call extends the existing deadline.

        Given a route already quarantined for 60 s,
        When quarantine(120, reason=...) is called,
        Then the route's quarantine_until shifts to roughly now + 120 s.
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        reservation.quarantine(60.0, reason="http-429")
        first_deadline = pool.snapshot()[0].quarantine_until
        assert first_deadline is not None
        reservation.quarantine(120.0, reason="http-429")
        second_deadline = pool.snapshot()[0].quarantine_until
        assert second_deadline is not None
        assert second_deadline > first_deadline

    def test_quarantine_never_shortens_deadline(self) -> None:
        """Spec — a shorter quarantine call does NOT shorten the deadline.

        Given a route quarantined for 120 s,
        When quarantine(60, ...) is called,
        Then quarantine_until remains the original (longer) deadline.
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        reservation.quarantine(120.0, reason="http-429")
        long_deadline = pool.snapshot()[0].quarantine_until
        assert long_deadline is not None
        reservation.quarantine(60.0, reason="http-429")
        snapshot = pool.snapshot()[0]
        assert snapshot.quarantine_until == long_deadline

    def test_quarantine_clamps_negative_retry_after(self) -> None:
        """Spec — retry_after_s below 1.0 is clamped to 1.0.

        Given retry_after_s = 0.0,
        When quarantine() is called,
        Then the route is quarantined for at least 1 s (deadline > now).
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        before = datetime.now(UTC)
        reservation.quarantine(0.0, reason="http-429")
        snapshot = pool.snapshot()[0]
        assert snapshot.quarantine_until is not None
        assert snapshot.quarantine_until > before

    def test_quarantine_reason_http_429_records_timestamp(self) -> None:
        """Spec — reason="http-429" sets last_handshake_429_at.

        Given a fresh reservation,
        When quarantine(60, reason="http-429") is called,
        Then RouteState.last_handshake_429_at is non-None and
            last_close_1015_at is still None.
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        reservation.quarantine(60.0, reason="http-429")
        snapshot = pool.snapshot()[0]
        assert snapshot.last_handshake_429_at is not None
        assert snapshot.last_close_1015_at is None

    def test_quarantine_reason_close_1015_records_timestamp(self) -> None:
        """Spec — reason="close-1015" sets last_close_1015_at.

        Given a fresh reservation,
        When quarantine(60, reason="close-1015") is called,
        Then RouteState.last_close_1015_at is non-None and
            last_handshake_429_at is still None.
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        reservation.quarantine(60.0, reason="close-1015")
        snapshot = pool.snapshot()[0]
        assert snapshot.last_close_1015_at is not None
        assert snapshot.last_handshake_429_at is None

    def test_quarantine_reason_ws_connect_error_records_no_cause_marker(self) -> None:
        """Spec — reason="ws-connect-error" advances quarantine, sets no marker.

        Given a fresh reservation,
        When quarantine(30, reason="ws-connect-error") is called,
        Then quarantine_until is set but NEITHER last_close_1015_at NOR
            last_handshake_429_at is touched — a SOCKS connect failure must not
            be mis-reported to operators as a Cloudflare 1015 close or a 429.
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        reservation.quarantine(30.0, reason="ws-connect-error")
        snapshot = pool.snapshot()[0]
        assert snapshot.quarantine_until is not None
        assert snapshot.last_close_1015_at is None
        assert snapshot.last_handshake_429_at is None


class TestWebsocketKwargs:
    """Tests for ``EgressReservation.websocket_kwargs`` output."""

    def test_direct_route_returns_proxy_none(self) -> None:
        """Spec — direct route reservation returns {"proxy": None}.

        Given a reservation borrowed from a direct route,
        When websocket_kwargs() is called,
        Then the result is {"proxy": None}. This MUST override the
        websockets-16 default of proxy=True so HTTPS_PROXY env vars
        cannot silently activate.
        """
        pool = _build_pool()
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        assert reservation.websocket_kwargs() == {"proxy": None}

    def test_socks5_route_returns_proxy_url(self) -> None:
        """Spec — socks5 route reservation returns {"proxy": "<url>"}.

        Given a pool with both a direct and a socks5 route, and
            preferred_route="wg-uk-1",
        When the socks5 route is reserved,
        Then websocket_kwargs() returns {"proxy": "socks5h://x:1081"}.
        """
        config = EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(id="d", kind="direct"),
                RouteConfig(
                    id="wg-uk-1",
                    kind="socks5",
                    proxy_url="socks5h://x:1081",
                    priority=10,
                ),
            ],
        )
        pool = EgressPool(config)
        reservation = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            preferred_route="wg-uk-1",
        )
        assert reservation.websocket_kwargs() == {"proxy": "socks5h://x:1081"}
