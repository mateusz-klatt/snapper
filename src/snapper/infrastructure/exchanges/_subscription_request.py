"""Replayable WS subscription cache primitives for kraken-SDK clients.

The Phase A.2 reconnect-storm watchdog destroys the SDK connector instance
(holding the SDK's internal ``__subscriptions`` list) when it forces a WS
restart. The newly-created connector starts with an empty subscriptions
list, so the SDK's own ``_recover_subscriptions`` no-ops and the WS is
live but silent.

This module gives the exchange client a place to remember every subscribe
call it has issued so the publisher-level liveness watchdog (or any future
caller that needs to rebuild the WS connection) can replay them via
``_replay_subscriptions``.

The dedup key collapses repeated subscribes to the same
``(channel, symbol-set, parameter-set)`` into a single cache entry; the
``parameters_json`` field captures every parameter that materially affects
the subscribe semantics (``interval`` for OHLC, ``throttle`` and
``asset_class`` for Equities, ``depth`` for the book channel, etc.). The
symbol list is captured separately in the typed ``symbols`` field so it
participates in the dedup key without surviving as a JSON ordering
hazard.
"""

import json
from dataclasses import dataclass
from typing import Literal

from snapper.core.json_types import JsonValue

_SYMBOL_KEYS_TO_STRIP = frozenset({"symbol", "symbols", "product", "products"})
"""Keys removed from ``params`` before computing ``parameters_json``.

Spot/Equities subscribe payloads use ``symbol``; the Kraken Futures SDK
uses ``products`` (and ``product`` is sometimes accepted as a singular
alias). The cache stores the canonical symbol-set in its own ``symbols``
tuple field, so leaving any symbol-bearing key inside ``parameters_json``
would cause two subscribes for the same set-of-symbols-but-different-list-
ordering to be cached as separate entries.
"""


@dataclass(frozen=True, slots=True)
class SubscriptionRequest:
    """Replayable subscribe call recorded by an exchange client.

    The dedup key returned by :meth:`key` collapses repeated subscribes
    to the same logical channel + symbol-set + parameter-set into one
    cache entry. The cache is a ``dict`` keyed by this tuple so a
    second subscribe with the same key replaces the first entry rather
    than appending a duplicate.

    Attributes:
        channel: Logical WS channel name. Matches the kraken-SDK
            ``channel`` field for the Spot ``SpotWSClient`` family
            (``"ticker"`` / ``"trade"`` / ``"ohlc"`` / ``"book"``)
            and is reused as the ``feed`` argument for the Kraken
            Futures ``FuturesWSClient`` API.
        symbols: Tuple of native WS symbol identifiers. Stored as a
            tuple to preserve replay-order while still hashing into
            a ``frozenset`` for the dedup key.
        parameters_json: JSON-canonical encoding of every other
            parameter that materially affects the subscribe call.
            Built via :func:`canonicalise_parameters` which strips
            the symbol-bearing keys and sorts the remaining entries.
            Empty string ``"{}"`` for Futures (which has no extra
            parameters today beyond ``feed`` + ``products``).
    """

    channel: Literal["ticker", "trade", "ohlc", "book"]
    symbols: tuple[str, ...]
    parameters_json: str

    def key(self) -> tuple[str, frozenset[str], str]:
        """Return the dedup key for the exchange-client subscription cache.

        Returns:
            A 3-tuple of ``(channel, frozenset(symbols), parameters_json)``
            suitable as a ``dict`` key. ``frozenset`` makes the key
            order-insensitive over the symbol list.
        """
        return (self.channel, frozenset(self.symbols), self.parameters_json)


def canonicalise_parameters(params: dict[str, JsonValue]) -> str:
    """Return a JSON-canonical encoding of ``params`` with symbol keys stripped.

    Called by every exchange-client ``_subscribe_X_impl`` site to build the
    :attr:`SubscriptionRequest.parameters_json` field. ``sort_keys=True``
    guarantees that two semantically-identical dicts with different
    insertion order produce identical JSON strings.

    Args:
        params: The raw subscribe parameters dict that the exchange client
            would otherwise pass to the SDK's ``subscribe(params=...)``
            call. Must already be JSON-serialisable
            (:data:`~snapper.core.json_types.JsonValue` typing enforces
            this at the type-check layer).

    Returns:
        Canonical JSON string. Empty input produces ``"{}"``.
    """
    sanitised = {k: v for k, v in params.items() if k not in _SYMBOL_KEYS_TO_STRIP}
    return json.dumps(sanitised, sort_keys=True)
