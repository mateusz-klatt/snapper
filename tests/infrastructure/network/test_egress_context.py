"""Unit tests for explicit egress traffic identity resolution."""

from collections.abc import Iterator

import pytest

from snapper.infrastructure.exchanges import kraken_sdk_patches
from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.infrastructure.network.egress_context import current_egress_identity
from snapper.infrastructure.network.egress_context import egress_identity
from snapper.infrastructure.network.egress_context import resolve_egress_traffic


class _Publisher:
    """Publisher test double exposing the legacy exchange-name protocol."""

    def __init__(self, exchange: str) -> None:
        """Store the exchange name returned by the double.

        Args:
            exchange: Exchange name to expose through ``_get_exchange_name``.
        """
        self._exchange = exchange

    def _get_exchange_name(self) -> str:
        """Return the configured exchange name.

        Returns:
            Exchange name used by the legacy public fallback.
        """
        return self._exchange


@pytest.fixture(autouse=True)
def _reset_publisher_context() -> Iterator[None]:
    """Clear the publisher ContextVar around each test.

    Yields:
        None while the publisher context is reset.
    """
    token = _CURRENT_PUBLISHER.set(None)
    try:
        yield
    finally:
        _CURRENT_PUBLISHER.reset(token)


def test_egress_identity_sets_and_resets_context() -> None:
    """Spec — explicit identity is scoped and reset.

    Given: No egress identity is active,
    When: ``egress_identity`` is entered and then exited,
    Then: the identity is visible only inside the context.
    """
    assert current_egress_identity() is None
    with egress_identity(
        exchange="kraken",
        traffic_class="private",
        owner="executor",
        operation="order_ws",
    ):
        identity = current_egress_identity()
        assert identity is not None
        assert identity.exchange == "kraken"
        assert identity.traffic_class == "private"
        assert identity.owner == "executor"
        assert identity.operation == "order_ws"
    assert current_egress_identity() is None


def test_resolve_prefers_explicit_identity_over_publisher() -> None:
    """Spec — explicit identity has highest precedence.

    Given: A publisher fallback is set,
    When: an explicit private egress identity is active,
    Then: the resolver returns the explicit exchange and private class.
    """
    token = _CURRENT_PUBLISHER.set(_Publisher("walutomat"))
    try:
        with egress_identity(
            exchange="kraken_futures",
            traffic_class="private",
            owner="executor",
            operation="order_ws",
        ):
            assert resolve_egress_traffic() == ("kraken_futures", "private")
    finally:
        _CURRENT_PUBLISHER.reset(token)


def test_resolve_uses_publisher_as_public_fallback() -> None:
    """Spec — publisher fallback remains public.

    Given: No explicit identity is active,
    When: ``_CURRENT_PUBLISHER`` carries a publisher exchange,
    Then: the resolver returns that exchange with ``"public"`` traffic.
    """
    token = _CURRENT_PUBLISHER.set(_Publisher("kraken_equities"))
    try:
        assert resolve_egress_traffic() == ("kraken_equities", "public")
    finally:
        _CURRENT_PUBLISHER.reset(token)


def test_resolve_defaults_to_public_default_exchange() -> None:
    """Spec — untagged traffic is public by default.

    Given: No explicit identity or publisher fallback exists,
    When: the resolver is called with a default exchange,
    Then: it returns that default exchange as public traffic.
    """
    assert resolve_egress_traffic(default_exchange="kraken") == ("kraken", "public")


def test_kraken_sdk_patches_reuses_egress_context_current_publisher() -> None:
    """Spec — the Kraken shim reuses the ContextVar owned by egress context.

    Given: The egress context owns ``_CURRENT_PUBLISHER``,
    When: Kraken SDK patches are imported,
    Then: The shim module exposes the same ContextVar object without owning
        a second definition.
    """
    assert kraken_sdk_patches._CURRENT_PUBLISHER is _CURRENT_PUBLISHER
