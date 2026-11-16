"""Witness exact selected Kraken aliases without catalog loading or network access."""

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from types import TracebackType
from typing import NoReturn
from typing import cast

import pytest
from kraken.spot import SpotWSClient
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.config.settings import AppSettings
from snapper.core.market_snapshot_freshness import MAX_REFRESH_TARGETS
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow
from snapper.data.selected_market_snapshot_aliases import SelectedKrakenTickerAlias
from snapper.infrastructure.exchanges._subscription_request import SubscriptionRequest
from snapper.infrastructure.exchanges.implementations import kraken as kraken_module
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.market_data import selected_kraken
from snapper.infrastructure.market_data.selected_kraken import SelectedKrakenTickerCodec
from snapper.infrastructure.market_data.selected_kraken import prepare_selected_kraken_adapter
from snapper.infrastructure.market_data.selected_snapshot_collector import (
    collect_selected_snapshots,
)
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.publisher import SequenceTracker


def _target(
    native: str = "BTC-EUR", identity: str = "instrument-a"
) -> MarketSnapshotRefreshTargetRow:
    """Provide synthetic explicit current instrument metadata.

    Args:
        native: Selected native subscription symbol.
        identity: Selected instrument identity.

    Returns:
        Complete typed metadata without inferred aliases.
    """
    return {
        "instrument_public_id": identity,
        "native_symbol": native,
        "exchange": "kraken",
        "base": "BTC",
        "quote": "EUR",
        "asset_type": "crypto",
    }


def _alias(
    native: str = "BTC-EUR", identity: str = "instrument-a", wire: str = "BTC/EUR"
) -> SelectedKrakenTickerAlias:
    """Provide exact synthetic alias evidence independent of native naming.

    Args:
        native: Resolved native identity.
        identity: Requested instrument identity.
        wire: Exact current alias recorded by the venue catalog.

    Returns:
        One current selected alias projection.
    """
    return SelectedKrakenTickerAlias(
        instrument_public_id=identity,
        symbol_public_id=f"symbol-{identity}",
        native_symbol=native,
        base="BTC",
        quote="EUR",
        asset_type="crypto",
        alias_public_id=f"alias-{identity}",
        exchange_symbol=wire,
        alias_row_id=1 if identity == "instrument-a" else 2,
    )


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[KrakenExchangeClient]:
    """Own an actual anonymous client whose default connection entry is forbidden.

    Args:
        monkeypatch: Fixture-local replacement owner.

    Yields:
        An anonymous client with no opened sockets or REST work.
    """
    owned = KrakenExchangeClient()

    async def forbidden_connect() -> NoReturn:
        """Reject an unexpected real connection during an offline witness."""
        raise AssertionError("A real connection was attempted")

    monkeypatch.setattr(owned, "_ensure_ws_connected", forbidden_connect)
    monkeypatch.setattr(owned, "connect", forbidden_connect)
    monkeypatch.setattr(selected_kraken, "KrakenExchangeClient", lambda: owned)
    try:
        yield owned
    finally:
        await KrakenExchangeClient.disconnect(owned)


def _aliases(
    monkeypatch: pytest.MonkeyPatch, aliases: tuple[SelectedKrakenTickerAlias, ...]
) -> list[list[str]]:
    """Replace only the database boundary while recording its explicit scope.

    Args:
        monkeypatch: Fixture-local replacement owner.
        aliases: Previously resolved synthetic current alias evidence.

    Returns:
        A mutable record of exact requested instrument lists.
    """
    calls: list[list[str]] = []

    async def read(
        session: AsyncSession, identities: Sequence[str], deadline: float
    ) -> tuple[SelectedKrakenTickerAlias, ...]:
        """Return selected fixture evidence without opening a database connection."""
        calls.append(list(identities))
        return aliases

    monkeypatch.setattr(selected_kraken, "read_selected_kraken_ticker_aliases", read)
    return calls


@dataclass
class _Socket:
    """Minimal in-memory WebSocket supporting the actual Kraken subscription loop."""

    client: KrakenExchangeClient
    subscriptions: list[dict[str, object]] = field(default_factory=list)
    closed: bool = False
    exception_occur: bool = False

    async def __aenter__(self) -> _Socket:
        """Enter the synthetic socket context.

        Returns:
            This socket without opening a transport.
        """
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Record context closure without suppressing errors.

        Args:
            exception_type: Exception class supplied by the context manager.
            exception: Exception supplied by the context manager.
            traceback: Traceback supplied by the context manager.
        """
        await self.close()

    async def subscribe(self, params: dict[str, object], req_id: int | None = None) -> None:
        """Deliver venue frames through the actual client receive parser.

        Args:
            params: Exact ticker subscription parameters.
            req_id: Optional client request identity.
        """
        self.subscriptions.append(params)
        self.client._handle_ticker_data(
            [
                {"symbol": "UNSELECTED/EUR", "last": 1.0},
                {"symbol": "wire-alias-b", "last": 2.0},
                {"symbol": "wire-alias-a", "last": 3.0},
            ]
        )

    async def close(self) -> None:
        """Record explicit closure without external resources."""
        self.closed = True


@pytest.mark.asyncio
async def test_selected_client_round_trip_avoids_global_mapper_and_rest(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Collect only admitted instruments through the existing anonymous Kraken client.

    Given: Two selected instruments have explicit aliases unlike their native names.
    When: The actual subscription loop receives selected and unrelated venue frames.
    Then: It subscribes exactly, maps selected ticks, closes and never loads a catalog.
    """
    targets = [_target(), _target("OTHER-EUR", "instrument-b")]
    calls = _aliases(
        monkeypatch,
        (_alias(wire="wire-alias-a"), _alias("OTHER-EUR", "instrument-b", "wire-alias-b")),
    )

    def forbidden_mapper() -> NoReturn:
        """Refuse any global catalog construction in the selected path."""
        raise AssertionError("The global mapper was consulted")

    monkeypatch.setattr(SymbolMapperService, "get_instance", forbidden_mapper)
    monkeypatch.setattr(kraken_module, "get_settings", forbidden_mapper)
    socket = _Socket(client)

    async def connect_selected() -> None:
        """Install the socket fake without touching REST or credentials."""
        client._ws_client = cast(SpotWSClient, socket)

    monkeypatch.setattr(client, "_ensure_ws_connected", connect_selected)
    deadline = asyncio.get_running_loop().time() + 1
    adapter = await prepare_selected_kraken_adapter(AsyncSession(), targets, deadline)
    batch = await collect_selected_snapshots(adapter, targets, deadline, SequenceTracker())
    await adapter.disconnect(deadline)

    assert calls == [["instrument-a", "instrument-b"]]
    assert client.api_key is None and client.api_secret is None
    assert adapter.alias_proofs == (
        _alias(wire="wire-alias-a").proof,
        _alias("OTHER-EUR", "instrument-b", "wire-alias-b").proof,
    )
    assert socket.subscriptions == [
        {"channel": "ticker", "symbol": ["wire-alias-a", "wire-alias-b"], "snapshot": True}
    ]
    assert {row.instrument_public_id for row in batch.snapshots} == {"instrument-a", "instrument-b"}
    assert batch.missing_instrument_ids == ()
    assert batch.timed_out is False
    assert socket.closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "targets",
    [
        [],
        [_target()] * (MAX_REFRESH_TARGETS + 1),
        [_target() | {"exchange": "walutomat"}],
        [_target() | {"quote": ""}],
        [_target() | {"base": " padded "}],
        [_target("*")],
        [_target(), _target("changed", "instrument-a")],
        [_target(), _target("BTC-EUR", "instrument-b")],
    ],
)
async def test_invalid_targets_refuse_before_alias_query_or_connection(
    client: KrakenExchangeClient,
    monkeypatch: pytest.MonkeyPatch,
    targets: list[MarketSnapshotRefreshTargetRow],
) -> None:
    """Prevent scope expansion or venue substitution before any asynchronous work.

    Given: Selected input is empty, excessive, unsupported or ambiguous.
    When: Preparation validates the required target set.
    Then: It refuses before alias lookup, client configuration or connection.
    """
    calls = _aliases(monkeypatch, ())

    with pytest.raises(ValueError):
        await prepare_selected_kraken_adapter(
            AsyncSession(), targets, asyncio.get_running_loop().time() + 1
        )

    assert calls == []
    assert client._ticker_symbol_codec is None


@pytest.mark.asyncio
async def test_identical_duplicate_targets_are_deduplicated(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Allow repeated identical input without duplicating the subscription scope.

    Given: The same resolved target appears twice with identical metadata.
    When: Preparation reads its current alias evidence.
    Then: Exactly one instrument is queried and one native symbol is admitted.
    """
    calls = _aliases(monkeypatch, (_alias(),))
    adapter = await prepare_selected_kraken_adapter(
        AsyncSession(), [_target(), _target()], asyncio.get_running_loop().time() + 1
    )

    assert calls == [["instrument-a"]]
    assert adapter._codec.to_wire_symbols(["BTC-EUR"]) == ["BTC/EUR"]
    assert client._ticker_symbol_codec is adapter._codec
    assert adapter.alias_proofs == (_alias().proof,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "alias",
    [
        replace(_alias(), native_symbol="changed"),
        replace(_alias(), base="changed"),
        replace(_alias(), quote="changed"),
        replace(_alias(), asset_type="forex"),
        replace(_alias(), exchange_symbol=""),
        replace(_alias(), exchange_symbol=" padded "),
        replace(_alias(), exchange_symbol="*"),
    ],
)
async def test_changed_metadata_or_unusable_alias_refuses_before_client_configuration(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch, alias: SelectedKrakenTickerAlias
) -> None:
    """Fail closed when selected metadata no longer matches the current alias join.

    Given: Current alias evidence is malformed or disagrees with the resolved target.
    When: Preparation binds exact native and wire identities.
    Then: It rejects the evidence without configuring or connecting a client.
    """
    _aliases(monkeypatch, (alias,))

    with pytest.raises(ValueError, match="metadata or ticker alias changed"):
        await prepare_selected_kraken_adapter(
            AsyncSession(), [_target()], asyncio.get_running_loop().time() + 1
        )

    assert client._ticker_symbol_codec is None


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate_native", [False, True])
async def test_ambiguous_alias_evidence_has_no_fallback(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch, duplicate_native: bool
) -> None:
    """Reject duplicate native or wire mappings even if an upstream reader regresses.

    Given: Current alias evidence maps two rows to the same native or wire identity.
    When: The selected codec is prepared.
    Then: It refuses the ambiguity instead of choosing one row silently.
    """
    aliases = (
        (_alias(), _alias())
        if duplicate_native
        else (_alias(), _alias("OTHER-EUR", "instrument-b", "BTC/EUR"))
    )
    _aliases(monkeypatch, aliases)
    targets = [_target()] if duplicate_native else [_target(), _target("OTHER-EUR", "instrument-b")]

    with pytest.raises(ValueError, match="aliases are ambiguous"):
        await prepare_selected_kraken_adapter(
            AsyncSession(), targets, asyncio.get_running_loop().time() + 1
        )

    assert client._ticker_symbol_codec is None


@pytest.mark.parametrize("payload", [None, {}, "not a ticker array"])
def test_codec_ignores_nonlist_payloads(payload: object) -> None:
    """Refuse malformed envelopes without any catalog fallback.

    Given: A payload is not a Kraken ticker array.
    When: The selected codec parses the frame.
    Then: It emits no ticks.
    """
    codec = SelectedKrakenTickerCodec({"native": "wire"}, {"wire": "native"})
    assert codec.parse_ticks(payload) == []


def test_codec_keeps_only_valid_selected_ticks_and_existing_numeric_defaults() -> None:
    """Preserve the established ticker model while restricting symbol resolution.

    Given: A batch mixes invalid, unrelated, selected minimal and selected full frames.
    When: The selected codec validates and translates it.
    Then: Only selected frames survive with existing numeric default semantics.
    """
    codec = SelectedKrakenTickerCodec({"native": "wire"}, {"wire": "native"})
    fields = (
        "bid",
        "bid_qty",
        "ask",
        "ask_qty",
        "last",
        "volume",
        "vwap",
        "low",
        "high",
        "change",
        "change_pct",
    )
    full: dict[str, object] = dict.fromkeys(fields, 2.0)
    full["symbol"] = "wire"

    ticks = codec.parse_ticks([None, {}, {"symbol": "unselected"}, {"symbol": "wire"}, full])

    assert [tick.symbol for tick in ticks] == ["native", "native"]
    assert ticks[0].last == 0.0
    assert ticks[1].last == 2.0
    assert all(getattr(ticks[1], name) == 2.0 for name in fields)


@pytest.mark.asyncio
@pytest.mark.parametrize("symbols", [[], ["*"], ["OTHER-EUR"], ["BTC-EUR", "BTC-EUR"]])
async def test_changed_subscription_scope_refuses_before_iterator_connection(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch, symbols: list[str]
) -> None:
    """Validate explicit symbol equality synchronously before returning an iterator.

    Given: A prepared adapter admits exactly one native symbol.
    When: A caller supplies an empty, wildcard, unknown or duplicate subscription.
    Then: Both adapter and configured client refuse before starting a connection.
    """
    _aliases(monkeypatch, (_alias(),))
    adapter = await prepare_selected_kraken_adapter(
        AsyncSession(), [_target()], asyncio.get_running_loop().time() + 1
    )

    with pytest.raises(ValueError, match="complete selected scope"):
        adapter.subscribe_ticks(symbols)
    with pytest.raises(ValueError, match="complete selected scope"):
        client.subscribe_ticks(symbols)


@pytest.mark.asyncio
async def test_elapsed_adapter_budget_refuses_subscription(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prevent lazy subscription after the original lifecycle budget has expired.

    Given: A valid selected adapter has an already elapsed absolute deadline.
    When: Its caller requests the otherwise valid native subscription.
    Then: It raises timeout before the client iterator can connect.
    """
    _aliases(monkeypatch, (_alias(),))
    adapter = await prepare_selected_kraken_adapter(AsyncSession(), [_target()], -1)

    with pytest.raises(TimeoutError, match="subscription deadline elapsed"):
        adapter.subscribe_ticks(["BTC-EUR"])


def test_default_client_symbol_mapping_is_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve established conversion when a client has no selected codec.

    Given: A preexisting-style client instance has not configured a selected codec.
    When: Its default symbol conversion receives explicit or wildcard input.
    Then: It uses the established converter and preserves wildcard behavior.
    """
    client = KrakenExchangeClient.__new__(KrakenExchangeClient)
    monkeypatch.setattr(
        kraken_module, "native_to_kraken_websocket", lambda native: f"wire:{native}"
    )

    assert client._ticker_wire_symbols(["BTC-EUR"]) == ["wire:BTC-EUR"]
    assert client._ticker_wire_symbols(["*"]) == ["*"]


@pytest.mark.asyncio
async def test_configured_codec_cannot_be_replaced(client: KrakenExchangeClient) -> None:
    """Keep a selected client's identity map fixed for its lifetime.

    Given: A fresh client has accepted one selected ticker codec.
    When: Another codec is configured on the same instance.
    Then: The existing mapping remains and replacement is refused.
    """
    codec = SelectedKrakenTickerCodec({"native": "wire"}, {"wire": "native"})
    client.configure_ticker_symbol_codec(codec)

    with pytest.raises(RuntimeError, match="fresh Kraken client"):
        client.configure_ticker_symbol_codec(codec)


@pytest.mark.asyncio
async def test_codec_cannot_be_attached_to_an_open_socket(client: KrakenExchangeClient) -> None:
    """Prevent a selected mapping from changing an active transport's semantics.

    Given: A client already owns a WebSocket transport.
    When: A caller attempts to bind a selected codec.
    Then: The client refuses the late configuration.
    """
    client._ws_client = cast(SpotWSClient, _Socket(client))
    codec = SelectedKrakenTickerCodec({"native": "wire"}, {"wire": "native"})

    with pytest.raises(RuntimeError, match="fresh Kraken client"):
        client.configure_ticker_symbol_codec(codec)


@pytest.mark.asyncio
async def test_codec_cannot_change_a_preexisting_subscription(client: KrakenExchangeClient) -> None:
    """Reject selected mapping changes when reconnectable subscription state exists.

    Given: A client has a cached ticker subscription but no connected socket.
    When: A caller attempts to attach a selected codec.
    Then: The client refuses to reinterpret the existing subscription state.
    """
    request = SubscriptionRequest(channel="ticker", symbols=("wire",), parameters_json="{}")
    client._subscription_cache[request.key()] = request
    codec = SelectedKrakenTickerCodec({"native": "wire"}, {"wire": "native"})

    with pytest.raises(RuntimeError, match="fresh Kraken client"):
        client.configure_ticker_symbol_codec(codec)


@pytest.mark.asyncio
async def test_disconnect_uses_the_original_budget(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not extend cleanup time when a caller supplies a later deadline.

    Given: A prepared client cleanup blocks beyond the original short budget.
    When: Its adapter is asked to disconnect with a much later deadline.
    Then: Cleanup is cancelled at the original deadline and timeout propagates.
    """
    _aliases(monkeypatch, (_alias(),))
    deadline = asyncio.get_running_loop().time() + 0.01
    adapter = await prepare_selected_kraken_adapter(AsyncSession(), [_target()], deadline)

    async def blocked() -> None:
        """Wait for the adapter's own cancellation without opening resources."""
        await asyncio.Event().wait()

    monkeypatch.setattr(client, "disconnect", blocked)

    with pytest.raises(TimeoutError):
        await adapter.disconnect(deadline + 10)


@pytest.mark.asyncio
async def test_disconnect_propagates_caller_cancellation_even_if_client_swallows_it(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not report successful cleanup after a new caller cancellation was swallowed.

    Given: Cleanup catches a caller cancellation and returns before its own deadline.
    When: The adapter checks cancellation ownership after cleanup.
    Then: The caller still receives CancelledError.
    """
    _aliases(monkeypatch, (_alias(),))
    deadline = asyncio.get_running_loop().time() + 1
    adapter = await prepare_selected_kraken_adapter(AsyncSession(), [_target()], deadline)
    entered = asyncio.Event()

    async def swallowing() -> None:
        """Expose and swallow one synthetic cancellation request."""
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return

    monkeypatch.setattr(client, "disconnect", swallowing)
    task = asyncio.create_task(adapter.disconnect(deadline))
    await entered.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_disconnect_rejects_a_swallowed_owned_timeout(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep timeout visible even when SDK cleanup swallows timeout cancellation.

    Given: Cleanup catches the cancellation caused by the adapter deadline.
    When: It returns after the deadline without clearing task ownership.
    Then: The adapter raises timeout instead of claiming success.
    """
    _aliases(monkeypatch, (_alias(),))
    deadline = asyncio.get_running_loop().time() + 0.01
    adapter = await prepare_selected_kraken_adapter(AsyncSession(), [_target()], deadline)

    async def swallowing() -> None:
        """Swallow a timeout request while retaining its cancellation count."""
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return

    monkeypatch.setattr(client, "disconnect", swallowing)

    with pytest.raises(TimeoutError, match="cleanup deadline elapsed"):
        await adapter.disconnect(deadline)


@pytest.mark.asyncio
async def test_disconnect_requires_a_finite_deadline_and_owning_task(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse malformed cleanup ownership before invoking client cleanup.

    Given: A prepared adapter receives an invalid deadline or cannot find its task.
    When: Cleanup validates its boundary.
    Then: It reports the relevant contract error.
    """
    _aliases(monkeypatch, (_alias(),))
    deadline = asyncio.get_running_loop().time() + 1
    adapter = await prepare_selected_kraken_adapter(AsyncSession(), [_target()], deadline)

    with pytest.raises(ValueError, match="finite deadline"):
        await adapter.disconnect(float("nan"))
    monkeypatch.setattr(asyncio, "current_task", lambda: None)
    with pytest.raises(RuntimeError, match="owning task"):
        await adapter.disconnect(deadline)


@pytest.mark.asyncio
async def test_disconnect_preserves_unrelated_operational_timeout(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Distinguish client operational failures from the adapter's deadline.

    Given: Cleanup raises an operational timeout before the adapter deadline.
    When: The adapter awaits that cleanup.
    Then: The exact operational exception propagates to its caller.
    """
    _aliases(monkeypatch, (_alias(),))
    deadline = asyncio.get_running_loop().time() + 1
    adapter = await prepare_selected_kraken_adapter(AsyncSession(), [_target()], deadline)
    failure = TimeoutError("synthetic operational failure")

    async def failing() -> NoReturn:
        """Raise a driver error without expiring the adapter's budget."""
        raise failure

    monkeypatch.setattr(client, "disconnect", failing)
    with pytest.raises(TimeoutError) as raised:
        await adapter.disconnect(deadline)

    assert raised.value is failure


@pytest.mark.asyncio
async def test_disconnect_does_not_misclassify_an_existing_cancellation_count(
    client: KrakenExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Count only cancellation requests arriving during this adapter operation.

    Given: The caller has handled one previous cancellation without clearing its count.
    When: Client cleanup succeeds without any new cancellation request.
    Then: The adapter accepts cleanup and preserves the caller's original count.
    """
    _aliases(monkeypatch, (_alias(),))
    deadline = asyncio.get_running_loop().time() + 1
    adapter = await prepare_selected_kraken_adapter(AsyncSession(), [_target()], deadline)

    async def already_cancelled() -> int:
        """Own an existing cancellation request in an isolated child task."""
        task = asyncio.current_task()
        assert task is not None
        task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.sleep(0)
        await adapter.disconnect(deadline)
        return task.cancelling()

    assert await asyncio.create_task(already_cancelled()) == 1


@pytest.mark.asyncio
async def test_anonymous_constructor_and_disconnect_do_not_load_default_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Construct the real public client without loading application bootstrap configuration.

    Given: The default application settings factory raises if invoked.
    When: An actual anonymous Kraken client is constructed and disconnected.
    Then: It retains empty credentials and never invokes the settings factory.
    """

    def forbidden_settings() -> NoReturn:
        """Refuse a default application configuration read in a public-only client."""
        raise AssertionError("Default application settings were loaded")

    monkeypatch.setattr(kraken_module, "get_settings", forbidden_settings)
    owned = KrakenExchangeClient()
    try:
        assert owned.api_key is None
        assert owned.api_secret is None
        assert "settings" not in vars(owned)
    finally:
        await owned.disconnect()


@pytest.mark.asyncio
async def test_selected_factory_uses_real_client_without_default_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prepare the complete selected adapter without secretly loading application defaults.

    Given: Exact selected aliases are available and default settings access is forbidden.
    When: The actual selected factory constructs its anonymous client and then disconnects.
    Then: It exposes the admitted alias proof without any application configuration read.
    """

    def forbidden_settings() -> NoReturn:
        """Reject implicit application configuration while preparing selected public data."""
        raise AssertionError("Default application settings were loaded")

    monkeypatch.setattr(kraken_module, "get_settings", forbidden_settings)
    _aliases(monkeypatch, (_alias(),))
    deadline = asyncio.get_running_loop().time() + 1
    adapter = await prepare_selected_kraken_adapter(AsyncSession(), [_target()], deadline)
    try:
        assert isinstance(adapter._client, KrakenExchangeClient)
        assert adapter._client.api_key is None and adapter._client.api_secret is None
        assert adapter.alias_proofs == (_alias().proof,)
        assert "settings" not in vars(adapter._client)
    finally:
        await adapter.disconnect(deadline)


@pytest.mark.asyncio
async def test_default_settings_are_loaded_only_once_on_first_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retain normal fallback settings access while deferring its bootstrap side effect.

    Given: The settings factory records calls and returns one typed settings object.
    When: A real client is constructed and its settings are subsequently read twice.
    Then: Construction performs no read and the first settings value is cached.
    """
    fallback = AppSettings.__new__(AppSettings)
    calls: list[bool] = []

    def load_settings() -> AppSettings:
        """Return a typed identity witness without initializing application configuration."""
        calls.append(True)
        return fallback

    monkeypatch.setattr(kraken_module, "get_settings", load_settings)
    owned = KrakenExchangeClient()
    try:
        assert calls == []
        assert owned.settings is fallback
        assert owned.settings is fallback
        assert calls == [True]
    finally:
        await owned.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize("read_before_assignment", [False, True])
async def test_owner_can_assign_settings_before_or_after_fallback_cache(
    monkeypatch: pytest.MonkeyPatch, read_before_assignment: bool
) -> None:
    """Preserve publisher and existing fixture assignment semantics.

    Given: A client may already have cached its fallback settings.
    When: Its owning publisher assigns a different settings instance.
    Then: Later reads return that exact instance without another default bootstrap call.
    """
    fallback = AppSettings.__new__(AppSettings)
    assigned = AppSettings.__new__(AppSettings)
    calls: list[bool] = []

    def load_settings() -> AppSettings:
        """Count fallback reads while returning only an inert typed settings identity."""
        calls.append(True)
        return fallback

    monkeypatch.setattr(kraken_module, "get_settings", load_settings)
    owned = KrakenExchangeClient()
    try:
        if read_before_assignment:
            assert owned.settings is fallback
        owned.settings = assigned
        assert owned.settings is assigned
        assert calls == ([True] if read_before_assignment else [])
    finally:
        await owned.disconnect()
