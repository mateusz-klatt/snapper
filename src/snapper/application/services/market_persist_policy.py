"""MarketPersistPolicy — selective DB-persist filter for publisher writes.

The policy resolves "which ``(exchange, data_type, native_symbol)``
triples deserve a DB write" so wildcard-subscribed publishers can
keep firehosing market data into the in-process cache while only a
chosen subset of instruments lands on disk.

The persisted set is derived from three independent inputs combined
under per-data-type mode resolution + overlays:

- **Auto mode** (default): the union of every active wallet-operator
  scope grant, projected through ``InstrumentUnderlyingMapping``
  expansion to ``(exchange, native_symbol)`` pairs. The
  ``ScopeGrantService`` admin-bus events
  (``admin.scope_revoked`` / ``admin.scope_granted`` /
  ``admin.scope_handed_over``) trigger an asynchronous rebuild so
  the persist set tracks live RBAC changes within one event cycle.

- **Explicit mode**: the operator hard-codes a per-exchange allowlist
  via ``market_persist_{ticks|trades|candles}["exchanges"]``. The
  policy uses that list verbatim and ignores wallet-scope grants for
  that data type.

- **Overlays**: ``market_persist_extra`` always **adds** symbols on
  top of the base set; ``market_persist_exclude`` always **subtracts**
  them. Both overlays are per-data-type per-exchange so an operator
  can persist extra candles for chart liquidity without bloating tick
  storage on the same instrument.

The policy is **stateful** (cached mode + overlay + scope-pair maps)
and refreshes through two atomic branches under a single lock:

- **Branch A** fires on the three admin scope events. It re-queries
  active operators + scope-grant instrument pairs and rebuilds the
  three internal frozensets using the cached mode + overlay state
  from the most recent Branch B run.

- **Branch B** fires on ``system.settings``. It re-reads the five
  ``market_persist_*`` settings via :class:`SettingsService`,
  re-resolves the mode + overlay state, then — if any per-type mode
  is now ``"auto"`` — runs Branch A's DB query as well so the new
  mode picks up the current scope-grant set without a second event.

:meth:`initial_rebuild` runs Branch B first then Branch A so the
publisher safety rail at :meth:`mode_for` reads a fully-populated
policy on lifespan startup (an empty modes dict would either crash
the rail or fire false positives).

Hot path: :meth:`should_persist` is ``O(1)`` frozenset membership.
"""

import asyncio
import contextlib
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import Literal
from typing import cast

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.services.settings import SettingsService
from snapper.core.json_types import JsonValue
from snapper.core.types import AllExchange
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm

MarketDataType = Literal["ticks", "trades", "candles"]
"""Per-data-type axis driving the three independent persist sets."""

_DATA_TYPES: tuple[MarketDataType, ...] = ("ticks", "trades", "candles")
"""Iteration order for the persist-set rebuilds + counters."""

_KNOWN_EXCHANGES: tuple[AllExchange, ...] = (
    ExchangeEnum.PAPER,
    ExchangeEnum.KRAKEN,
    ExchangeEnum.KRAKEN_FUTURES,
    ExchangeEnum.KRAKEN_EQUITIES,
    ExchangeEnum.WALUTOMAT,
    ExchangeEnum.POLYGON,
)
"""Mirrors :data:`AllExchange` for default-dict initialisation."""

_ADMIN_SCOPE_REVOKED_TOPIC = "admin.scope_revoked"
_ADMIN_SCOPE_GRANTED_TOPIC = "admin.scope_granted"
_ADMIN_SCOPE_HANDED_OVER_TOPIC = "admin.scope_handed_over"
_SYSTEM_SETTINGS_TOPIC = "system.settings"

_ADMIN_SCOPE_TOPICS: frozenset[str] = frozenset(
    {
        _ADMIN_SCOPE_REVOKED_TOPIC,
        _ADMIN_SCOPE_GRANTED_TOPIC,
        _ADMIN_SCOPE_HANDED_OVER_TOPIC,
    }
)

_DEFAULT_MODE: Literal["auto"] = "auto"
"""Fallback mode when a ``market_persist_*`` setting is missing."""


class _MalformedSettingError(ValueError):
    """Raised by the strict settings parsers on a top-level shape mismatch.

    Caught by :meth:`MarketPersistPolicy._refresh_from_settings` to abort
    the refresh cycle and preserve the previously-applied policy state
    so a single bad admin edit cannot wipe an in-flight policy.
    """


_LISTEN_RECV_BACKOFF_S = 0.1
"""Backoff after a transient ``recv_multipart`` failure (matches WS auth pattern)."""

_PERSIST_TICKS_KEY = "market_persist_ticks"
_PERSIST_TRADES_KEY = "market_persist_trades"
_PERSIST_CANDLES_KEY = "market_persist_candles"
_PERSIST_EXTRA_KEY = "market_persist_extra"
_PERSIST_EXCLUDE_KEY = "market_persist_exclude"

_PERSIST_MODE_KEYS: dict[MarketDataType, str] = {
    "ticks": _PERSIST_TICKS_KEY,
    "trades": _PERSIST_TRADES_KEY,
    "candles": _PERSIST_CANDLES_KEY,
}


class MarketPersistPolicy:
    """Resolves whether a publisher should persist a ``(exchange, data_type, symbol)`` write.

    Instantiated once per process from the FastAPI lifespan and
    injected into every :class:`BasePublisher` so the hot path can
    short-circuit DB writes with a single frozenset membership check.

    The class is **single-instance, async-aware, and lock-protected**:
    Branch A (admin event) and Branch B (settings event) both serialise
    on ``self._lock``, build new frozensets into local tmp vars, then
    swap atomically by re-binding the instance dicts. Reads
    (:meth:`should_persist`, :meth:`mode_for`,
    :meth:`iter_persisted_instruments`) never need the lock because
    a dict re-bind on CPython is an atomic operation visible to other
    coroutines on their next ``await`` point.

    Attributes:
        repository: Source of truth for the wallet-scope-derived set.
        settings_service: Source of truth for the five
            ``market_persist_*`` settings (cache-first).
    """

    def __init__(
        self,
        repository: Repository,
        settings_service: SettingsService,
    ) -> None:
        """Initialise the policy with stub empty state.

        :meth:`initial_rebuild` must be awaited before the publishers
        start, otherwise :meth:`should_persist` returns ``False`` for
        every write and the safety rail at :meth:`mode_for` sees the
        default mode regardless of configuration.

        Args:
            repository: Async repository handle. Used by Branch A.
            settings_service: Settings service singleton. Used by Branch B.
        """
        self.repository = repository
        self.settings_service = settings_service
        self._lock = asyncio.Lock()
        self._ticks: dict[AllExchange, frozenset[str]] = {}
        self._trades: dict[AllExchange, frozenset[str]] = {}
        self._candles: dict[AllExchange, frozenset[str]] = {}
        self._modes: dict[MarketDataType, Literal["auto", "explicit"]] = {
            "ticks": _DEFAULT_MODE,
            "trades": _DEFAULT_MODE,
            "candles": _DEFAULT_MODE,
        }
        self._explicit_by_type: dict[MarketDataType, dict[AllExchange, frozenset[str]]] = {
            "ticks": {},
            "trades": {},
            "candles": {},
        }
        self._extra_by_type: dict[MarketDataType, dict[AllExchange, frozenset[str]]] = {
            "ticks": {},
            "trades": {},
            "candles": {},
        }
        self._exclude_by_type: dict[MarketDataType, dict[AllExchange, frozenset[str]]] = {
            "ticks": {},
            "trades": {},
            "candles": {},
        }
        self._scope_pairs_by_exchange: dict[AllExchange, frozenset[str]] = {}
        self._listen_task: asyncio.Task[None] | None = None
        self._listener_lock = asyncio.Lock()
        self._running = False
        self._zmq_context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None

    def should_persist(
        self,
        exchange: AllExchange,
        data_type: MarketDataType,
        native_symbol: str,
    ) -> bool:
        """Return ``True`` when the publisher should write ``(exchange, type, symbol)`` to DB.

        Hot path: single frozenset membership check. Missing
        ``(exchange, data_type)`` keys return ``False`` so an
        uninitialised policy + wildcard subscribe degrades gracefully
        (no writes vs. a crash). The safety rail at startup catches
        the "wildcard + empty set" misconfig before any tick lands.

        Args:
            exchange: Source exchange identifier.
            data_type: One of ``"ticks"``, ``"trades"``, ``"candles"``.
            native_symbol: Native-format symbol as parsed by the
                publisher's adapter (e.g. ``"BTC-USD"``, ``"PI_XBTUSD"``).

        Returns:
            ``True`` iff the resolved set for ``(exchange, data_type)``
            contains ``native_symbol``.
        """
        target = self._select_persist_map(data_type)
        return native_symbol in target.get(exchange, frozenset())

    def iter_persisted_instruments(
        self, data_type: MarketDataType
    ) -> Iterator[tuple[AllExchange, str]]:
        """Yield every ``(exchange, native_symbol)`` pair persisted for ``data_type``.

        Used by the cache prewarm path to enumerate which instruments
        to backfill from the DB before the first ZMQ frame lands.

        Args:
            data_type: One of ``"ticks"``, ``"trades"``, ``"candles"``.

        Yields:
            ``(exchange, native_symbol)`` tuples in iteration order
            (frozenset insertion order, not guaranteed stable across
            rebuilds — callers must not rely on ordering).
        """
        target = self._select_persist_map(data_type)
        for exchange, symbols in target.items():
            for symbol in symbols:
                yield exchange, symbol

    def mode_for(
        self, exchange: AllExchange, data_type: MarketDataType
    ) -> Literal["auto", "explicit"]:
        """Return the configured mode for ``(exchange, data_type)``.

        Mode is configured globally per data type — the ``exchange``
        argument is accepted for API symmetry with future per-exchange
        override hooks but is currently ignored. Callers (specifically
        the safety rail) use the mode to decide whether the wildcard
        + empty-set guard applies.

        Args:
            exchange: Source exchange (unused at v1; reserved).
            data_type: One of ``"ticks"``, ``"trades"``, ``"candles"``.

        Returns:
            ``"auto"`` (scope-grant-derived) or ``"explicit"`` (configured
            allowlist).
        """
        del exchange
        return self._modes.get(data_type, _DEFAULT_MODE)

    def wallet_scope_pairs_for(self, exchange: AllExchange) -> frozenset[str]:
        """Return the wallet-scope-derived ``native_symbol`` set for ``exchange``.

        Independent of mode + overlays: callers (specifically the
        safety rail) use this to detect the "auto mode + zero grants
        + no overlay" misconfiguration.

        Args:
            exchange: Source exchange.

        Returns:
            Frozenset of native symbols reachable via active wallet-
            operator scope grants at the last rebuild. Empty frozenset
            when no grants reach the exchange OR when
            :meth:`initial_rebuild` has not yet run.
        """
        return self._scope_pairs_by_exchange.get(exchange, frozenset())

    def extra_for(self, exchange: AllExchange, data_type: MarketDataType) -> frozenset[str]:
        """Return the ``market_persist_extra`` overlay for ``(exchange, data_type)``.

        Used by the safety rail's "auto + empty scope + empty extra"
        check: if the operator added an extra allowlist for the data
        type / exchange combo, the publisher is allowed to start even
        when the scope-derived set is empty.

        Args:
            exchange: Source exchange.
            data_type: One of ``"ticks"``, ``"trades"``, ``"candles"``.

        Returns:
            Frozenset of native symbols configured as overlay-include
            for the combo. Empty when none configured.
        """
        return self._extra_by_type.get(data_type, {}).get(exchange, frozenset())

    async def initial_rebuild(self) -> None:
        """Populate mode/overlay state then the scope-grant set.

        Branch B runs FIRST so :attr:`_modes` + :attr:`_explicit_by_type`
        + :attr:`_extra_by_type` + :attr:`_exclude_by_type` reflect the
        configured settings before Branch A starts using them to
        rebuild the per-type frozensets. Without this ordering, the
        first publisher start would either crash on a missing mode
        entry or default-to-auto regardless of the operator's intent.
        """
        await self._refresh_from_settings()
        await self._refresh_from_scope_grants()

    async def start_admin_listener(self, zmq_broker_xpub: str) -> None:
        """Open the SUB socket and start the dispatch task.

        Subscribes to the three admin scope events plus
        ``system.settings``. Mirrors
        :meth:`WebSocketAuthManager.start_admin_listener` for shape +
        idempotency contract. Empty ``zmq_broker_xpub`` skips the
        listener entirely (test mode); :meth:`initial_rebuild` still
        populates state from in-process SettingsService + Repository.

        Args:
            zmq_broker_xpub: Address of the broker's XPUB endpoint.
                Empty string skips the listener entirely.
        """
        async with self._listener_lock:
            if self._listen_task is not None and not self._listen_task.done():
                return
            if self._listen_task is not None:
                await self._reap_listener_unlocked()
            if not zmq_broker_xpub:
                logger.info("MarketPersistPolicy: empty broker XPUB, skipping listener")
                return
            self._zmq_context = zmq.asyncio.Context()
            raw_sub_socket = self._zmq_context.socket(zmq.SUB)
            apply_hwm(raw_sub_socket, rcvhwm=HWM_AUDIT)
            raw_sub_socket.connect(zmq_broker_xpub)
            self._subscriber = ValidatedSubscriber(raw_sub_socket)
            for topic in (
                _ADMIN_SCOPE_REVOKED_TOPIC,
                _ADMIN_SCOPE_GRANTED_TOPIC,
                _ADMIN_SCOPE_HANDED_OVER_TOPIC,
                _SYSTEM_SETTINGS_TOPIC,
            ):
                self._subscriber.subscribe(topic)
            self._running = True
            self._listen_task = asyncio.create_task(self._listen_loop())
            logger.info(
                "MarketPersistPolicy: subscribed to admin.scope_* + system.settings on {}",
                zmq_broker_xpub,
            )

    async def stop(self) -> None:
        """Cancel the listener task and dispose ZMQ resources. Idempotent."""
        async with self._listener_lock:
            await self._reap_listener_unlocked()

    async def _reap_listener_unlocked(self) -> None:
        """Tear down listener resources. Caller MUST hold ``_listener_lock``.

        Captures every resource reference into locals BEFORE clearing
        the attributes so a follow-up :meth:`start_admin_listener`
        (which runs after we release the lock) sees a fully-clean slate.
        """
        self._running = False
        task = self._listen_task
        subscriber = self._subscriber
        context = self._zmq_context
        self._listen_task = None
        self._subscriber = None
        self._zmq_context = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if subscriber is not None:
            with contextlib.suppress(Exception):
                subscriber.close()
        if context is not None:
            with contextlib.suppress(Exception):
                context.term()

    async def _listen_loop(self) -> None:
        """Receive admin + settings frames and dispatch refresh branches.

        Per-message failures (parse errors, recv errors) are caught +
        logged so a single bad frame can never silently stop the
        listener. Only ``CancelledError`` from :meth:`stop` unwinds
        the loop.
        """
        subscriber = self._subscriber
        if subscriber is None:
            return
        try:
            while self._running:
                frame = await self._recv_one_frame(subscriber)
                if frame is None:
                    continue
                topic, _payload = frame
                await self._dispatch_topic(topic)
        except asyncio.CancelledError:
            logger.info("MarketPersistPolicy: listen loop cancelled")
            raise

    async def _recv_one_frame(self, subscriber: ValidatedSubscriber) -> tuple[str, str] | None:
        """Receive and decode one frame; ``None`` on a transient failure."""
        try:
            topic_bytes, payload_bytes = await subscriber.recv_multipart()
            topic = topic_bytes.decode() if isinstance(topic_bytes, bytes) else str(topic_bytes)
            payload = (
                payload_bytes.decode() if isinstance(payload_bytes, bytes) else str(payload_bytes)
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("MarketPersistPolicy listener recv failed: {}", exc)
            await asyncio.sleep(_LISTEN_RECV_BACKOFF_S)
            return None
        return topic, payload

    async def _dispatch_topic(self, topic: str) -> None:
        """Route one decoded topic string to its refresh branch.

        Handler exceptions other than ``CancelledError`` are logged
        and swallowed so one bad frame cannot stop the listener.
        Payloads are intentionally ignored — these are wake-up signals
        only, mirroring the
        :class:`ScopeRevokedData` documented contract. The conditional
        Branch A run on a settings event is handled INSIDE
        :meth:`_refresh_from_settings` under the policy lock so the
        decision cannot race a concurrent settings refresh.
        """
        try:
            if topic in _ADMIN_SCOPE_TOPICS:
                await self._refresh_from_scope_grants()
            elif topic == _SYSTEM_SETTINGS_TOPIC:
                await self._refresh_from_settings()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "MarketPersistPolicy handler failed: topic={} err={}",
                topic,
                exc,
            )

    async def _refresh_from_settings(self) -> None:
        """Branch B: re-read settings, optionally chain Branch A under one lock.

        Strict shape validation: a top-level malformed setting raises
        :class:`_MalformedSettingError` from the parsers and aborts the
        whole refresh — previous policy state is left intact so a single
        bad admin edit cannot wipe an in-flight policy. Per-element
        drops (non-string symbols, unknown exchanges) stay as warnings.

        When the new mode state contains any ``"auto"`` per-type entry,
        the conditional Branch A scope-pair refresh runs under the same
        :attr:`_lock` so dispatch cannot race a concurrent settings
        change.
        """
        try:
            parsed_modes, parsed_explicit, parsed_extra, parsed_exclude = (
                self._parse_all_settings_strict()
            )
        except _MalformedSettingError as exc:
            logger.warning(
                "MarketPersistPolicy: settings refresh aborted ({}); "
                "previous policy state preserved",
                exc,
            )
            return
        async with self._lock:
            self._modes = parsed_modes
            self._explicit_by_type = parsed_explicit
            self._extra_by_type = parsed_extra
            self._exclude_by_type = parsed_exclude
            if any(parsed_modes[t] == "auto" for t in _DATA_TYPES):
                self._scope_pairs_by_exchange = self._index_pairs_by_exchange(
                    await self._query_active_scope_pairs()
                )
            self._rebuild_persist_maps_unlocked()

    async def _refresh_from_scope_grants(self) -> None:
        """Branch A: re-query active operators + scope-grant instrument pairs.

        Holds :attr:`_lock` across the DB query so concurrent admin
        events cannot complete out of order and leave a stale snapshot
        as the last-write-wins value. The lock is async-aware: other
        coroutines yield while we await the repository.
        """
        async with self._lock:
            pairs = await self._query_active_scope_pairs()
            self._scope_pairs_by_exchange = self._index_pairs_by_exchange(pairs)
            self._rebuild_persist_maps_unlocked()

    async def _query_active_scope_pairs(self) -> set[tuple[str, str]]:
        """Query active operators + project to scope-grant ``(exchange, symbol)`` pairs."""
        as_of = datetime.now(UTC)
        operators = await self.repository.list_active_operators(as_of=as_of)
        operator_ids = [op["public_id"] for op in operators]
        if not operator_ids:
            return set()
        return await self.repository.list_scope_grant_instrument_pairs(operator_ids, as_of)

    def _parse_all_settings_strict(
        self,
    ) -> tuple[
        dict[MarketDataType, Literal["auto", "explicit"]],
        dict[MarketDataType, dict[AllExchange, frozenset[str]]],
        dict[MarketDataType, dict[AllExchange, frozenset[str]]],
        dict[MarketDataType, dict[AllExchange, frozenset[str]]],
    ]:
        """Parse all five ``market_persist_*`` settings or raise on shape mismatch.

        Raises:
            _MalformedSettingError: Top-level shape failure on any of
                the five settings (e.g. non-dict, invalid mode value,
                non-dict overlay payload). Caller catches at refresh
                level and preserves previous state.
        """
        new_modes: dict[MarketDataType, Literal["auto", "explicit"]] = {}
        new_explicit: dict[MarketDataType, dict[AllExchange, frozenset[str]]] = {}
        for data_type in _DATA_TYPES:
            key = _PERSIST_MODE_KEYS[data_type]
            cfg = self.settings_service.get_setting(key, default={"mode": _DEFAULT_MODE})
            mode, explicit = self._parse_mode_setting(key, cfg)
            new_modes[data_type] = mode
            new_explicit[data_type] = explicit
        new_extra = self._parse_overlay_setting(
            _PERSIST_EXTRA_KEY,
            self.settings_service.get_setting(_PERSIST_EXTRA_KEY, default={}),
        )
        new_exclude = self._parse_overlay_setting(
            _PERSIST_EXCLUDE_KEY,
            self.settings_service.get_setting(_PERSIST_EXCLUDE_KEY, default={}),
        )
        return new_modes, new_explicit, new_extra, new_exclude

    def _rebuild_persist_maps_unlocked(self) -> None:
        """Recompute the three per-type frozenset maps. Caller holds ``_lock``."""
        self._ticks = self._resolve_for_type("ticks")
        self._trades = self._resolve_for_type("trades")
        self._candles = self._resolve_for_type("candles")

    def _resolve_for_type(self, data_type: MarketDataType) -> dict[AllExchange, frozenset[str]]:
        """Combine mode + overlays into the final ``(exchange -> frozenset)`` map."""
        mode = self._modes.get(data_type, _DEFAULT_MODE)
        explicit_for_type = self._explicit_by_type.get(data_type, {})
        extra_for_type = self._extra_by_type.get(data_type, {})
        exclude_for_type = self._exclude_by_type.get(data_type, {})
        candidate_exchanges = self._candidate_exchanges(
            mode, explicit_for_type, extra_for_type, exclude_for_type
        )
        resolved: dict[AllExchange, frozenset[str]] = {}
        for exchange in candidate_exchanges:
            if mode == "explicit":
                base = explicit_for_type.get(exchange, frozenset())
            else:
                base = self._scope_pairs_by_exchange.get(exchange, frozenset())
            extra = extra_for_type.get(exchange, frozenset())
            exclude = exclude_for_type.get(exchange, frozenset())
            resolved[exchange] = frozenset((base | extra) - exclude)
        return resolved

    def _candidate_exchanges(
        self,
        mode: Literal["auto", "explicit"],
        explicit_for_type: dict[AllExchange, frozenset[str]],
        extra_for_type: dict[AllExchange, frozenset[str]],
        exclude_for_type: dict[AllExchange, frozenset[str]],
    ) -> set[AllExchange]:
        """Union of every exchange contributing to the final resolved map."""
        candidates: set[AllExchange] = set(extra_for_type) | set(exclude_for_type)
        if mode == "explicit":
            candidates |= set(explicit_for_type)
        else:
            candidates |= set(self._scope_pairs_by_exchange)
        return candidates

    def _select_persist_map(self, data_type: MarketDataType) -> dict[AllExchange, frozenset[str]]:
        """Return the resolved persist map for ``data_type``."""
        if data_type == "ticks":
            return self._ticks
        if data_type == "trades":
            return self._trades
        return self._candles

    @staticmethod
    def _index_pairs_by_exchange(
        pairs: set[tuple[str, str]],
    ) -> dict[AllExchange, frozenset[str]]:
        """Group ``(exchange, native_symbol)`` pairs into per-exchange frozensets."""
        buckets: dict[AllExchange, set[str]] = {}
        valid: frozenset[str] = frozenset(_KNOWN_EXCHANGES)
        for exchange_raw, native_symbol in pairs:
            if exchange_raw not in valid:
                logger.warning(
                    "MarketPersistPolicy: scope-grant pair references unknown exchange {!r}; "
                    "dropping pair {!r}",
                    exchange_raw,
                    (exchange_raw, native_symbol),
                )
                continue
            exchange = cast(AllExchange, exchange_raw)
            buckets.setdefault(exchange, set()).add(native_symbol)
        return {exchange: frozenset(symbols) for exchange, symbols in buckets.items()}

    @staticmethod
    def _parse_mode_setting(
        key: str, cfg: Any
    ) -> tuple[Literal["auto", "explicit"], dict[AllExchange, frozenset[str]]]:
        """Validate one ``market_persist_{type}`` setting (strict).

        Raises:
            _MalformedSettingError: ``cfg`` is not a dict, the ``mode``
                value is not one of ``"auto"`` / ``"explicit"``, or
                ``mode="explicit"`` carries a non-dict ``exchanges``
                payload. Per-element drops (non-string symbols, unknown
                exchanges) stay as warnings inside
                :meth:`_parse_exchange_symbol_map`.
        """
        if not isinstance(cfg, dict):
            raise _MalformedSettingError(f"{key} is not a dict ({type(cfg).__name__})")
        raw_mode = cfg.get("mode", _DEFAULT_MODE)
        if raw_mode not in ("auto", "explicit"):
            raise _MalformedSettingError(f"{key} has invalid mode {raw_mode!r}")
        mode = cast(Literal["auto", "explicit"], raw_mode)
        explicit: dict[AllExchange, frozenset[str]] = {}
        if mode == "explicit":
            exchanges = cfg.get("exchanges", {})
            if not isinstance(exchanges, dict):
                raise _MalformedSettingError(
                    f"{key}.exchanges is not a dict ({type(exchanges).__name__})"
                )
            explicit = MarketPersistPolicy._parse_exchange_symbol_map(key, exchanges)
        return mode, explicit

    @staticmethod
    def _parse_overlay_setting(
        key: str, cfg: Any
    ) -> dict[MarketDataType, dict[AllExchange, frozenset[str]]]:
        """Validate ``market_persist_extra`` / ``market_persist_exclude`` shape (strict).

        Raises:
            _MalformedSettingError: ``cfg`` is not a dict, or any of the
                three per-data-type entries is not a dict. Per-element
                drops stay as warnings inside
                :meth:`_parse_exchange_symbol_map`.
        """
        result: dict[MarketDataType, dict[AllExchange, frozenset[str]]] = {
            "ticks": {},
            "trades": {},
            "candles": {},
        }
        if not isinstance(cfg, dict):
            raise _MalformedSettingError(f"{key} is not a dict ({type(cfg).__name__})")
        for data_type in _DATA_TYPES:
            per_type = cfg.get(data_type, {})
            if not isinstance(per_type, dict):
                raise _MalformedSettingError(
                    f"{key}.{data_type} is not a dict ({type(per_type).__name__})"
                )
            result[data_type] = MarketPersistPolicy._parse_exchange_symbol_map(
                f"{key}.{data_type}", per_type
            )
        return result

    @staticmethod
    def _parse_exchange_symbol_map(
        context: str, raw: dict[Any, Any]
    ) -> dict[AllExchange, frozenset[str]]:
        """Coerce ``{<exchange>: [<symbol>, ...]}`` into the canonical map."""
        result: dict[AllExchange, frozenset[str]] = {}
        valid: frozenset[str] = frozenset(_KNOWN_EXCHANGES)
        for exchange_raw, symbols_raw in raw.items():
            if exchange_raw not in valid:
                logger.warning(
                    "MarketPersistPolicy: {} references unknown exchange {!r}; skipping",
                    context,
                    exchange_raw,
                )
                continue
            if not isinstance(symbols_raw, list):
                logger.warning(
                    "MarketPersistPolicy: {}.{} is not a list ({!r}); skipping",
                    context,
                    exchange_raw,
                    type(symbols_raw).__name__,
                )
                continue
            symbols = MarketPersistPolicy._coerce_symbol_list(
                f"{context}.{exchange_raw}", cast(list[JsonValue], symbols_raw)
            )
            result[cast(AllExchange, exchange_raw)] = symbols
        return result

    @staticmethod
    def _coerce_symbol_list(context: str, raw: list[JsonValue]) -> frozenset[str]:
        """Return a frozenset of strings; drop non-string entries with a warning."""
        cleaned: set[str] = set()
        for item in raw:
            if isinstance(item, str):
                cleaned.add(item)
            else:
                logger.warning(
                    "MarketPersistPolicy: {} contains non-string entry {!r}; dropping",
                    context,
                    item,
                )
        return frozenset(cleaned)
