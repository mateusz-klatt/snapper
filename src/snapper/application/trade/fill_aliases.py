"""Prove execution alias equivalence using immutable durable fill witnesses."""

from collections.abc import Collection

from snapper.data.repository_types import VenueEventRow

_FILL_FIELDS = (
    "shard_key",
    "exchange",
    "instrument",
    "mode",
    "wallet_public_id",
    "client_order_id",
    "exchange_order_id",
    "cum_fill_size",
    "venue_timestamp",
    "side",
    "fill_size",
    "fill_price",
    "fee",
    "fee_asset",
)
_LIVE_IDENTITY_FIELDS = (
    "shard_key",
    "exchange",
    "instrument",
    "mode",
    "wallet_public_id",
    "client_order_id",
    "exchange_order_id",
    "cum_fill_size",
    "venue_timestamp",
    "side",
    "fill_price",
    "fee_asset",
)


def fill_aliases(event: VenueEventRow) -> tuple[str, ...]:
    """Return the same native or id-less aliases used by TradeService dedup.

    Args:
        event: Fill observation whose aliases are needed.

    Returns:
        Native identifiers, or the shared live/replay fallback identifier.
    """
    native = tuple(key for key in (event.get("exec_id"), event.get("trade_id")) if key)
    return native or (
        f"fallback-{event.get('client_order_id')}-{event.get('fill_size')}-{event.get('fill_price')}",
    )


def _fill_witness(event: VenueEventRow) -> tuple[object, ...]:
    """Identify one fill by its native identifiers, scope, lineage and economics."""
    return (event.get("exec_id"), event.get("trade_id")) + tuple(
        event.get(field) for field in _FILL_FIELDS
    )


def _alias_witnesses(events: list[VenueEventRow]) -> dict[str, VenueEventRow]:
    """Index every durable fill without discarding duplicate collision evidence.

    Args:
        events: Complete durable observations for the scope being verified.

    Returns:
        Each alias mapped to its sole immutable fill witness.

    Raises:
        ValueError: One alias denotes distinct fills or contradictory witnesses.
    """
    witnesses: dict[str, VenueEventRow] = {}
    for event in events:
        if event["event_type"] != "fill_observed":
            continue
        witness = _fill_witness(event)
        for alias in fill_aliases(event):
            if _fill_witness(witnesses.setdefault(alias, event)) != witness:
                raise ValueError(f"ambiguous execution alias: {alias}")
    return witnesses


def aliases_cover_same_fills(
    restored: Collection[str], replayed: Collection[str], events: list[VenueEventRow]
) -> bool:
    """Require complete equality of uniquely witnessed fills before normalization.

    Args:
        restored: Legacy checkpoint aliases before any normalization.
        replayed: Aliases produced by the current chronological replay.
        events: Full durable history, including duplicates hidden by replay dedup.

    Returns:
        Whether both unsaturated alias sets name exactly all durable fills.
        Unknown aliases, ambiguous witnesses and missing fills always refuse.
    """
    if len(restored) >= 10_000 or len(replayed) >= 10_000:
        return False
    try:
        witnesses = _alias_witnesses(events)
        restored_fills = {_fill_witness(witnesses[alias]) for alias in restored}
        replayed_fills = {_fill_witness(witnesses[alias]) for alias in replayed}
    except (KeyError, ValueError):
        return False
    return (
        restored_fills == replayed_fills == {_fill_witness(event) for event in witnesses.values()}
    )


def _live_identity_matches(durable: VenueEventRow, live: VenueEventRow) -> bool:
    """Match stable order and cumulative identity without comparing absorbed deltas."""
    return all(durable.get(field) == live.get(field) for field in _LIVE_IDENTITY_FIELDS)


def resolve_live_fill_event(
    live: VenueEventRow, events: list[VenueEventRow]
) -> VenueEventRow | None:
    """Resolve a live native identifier to exactly one matching durable fill.

    ExecutionData.trade_id carries the venue exec identifier. Its other native
    alias is read from the already committed fill row, never inferred from a
    spelling or borrowed from another fill in the order's history. Published
    quantity and fees can absorb an earlier unpublished durable fill, so they
    do not bind the alias. Durable witnesses must still agree on all economics;
    startup separately compares the complete replay's exact economic digest.

    Args:
        live: Live shadow observation carrying the published native identifier.
        events: Durable fill history for this client order.

    Returns:
        The exact durable fill, or None when no durable alias is available yet.

    Raises:
        ValueError: An alias is ambiguous or its durable fill contradicts live data.
    """
    witnesses = _alias_witnesses(events)
    event = witnesses.get(fill_aliases(live)[0])
    if event is None and not (live.get("exec_id") or live.get("trade_id")):
        candidates = {
            _fill_witness(durable): durable
            for durable in witnesses.values()
            if _live_identity_matches(durable, live)
        }
        if len(candidates) > 1:
            raise ValueError("ambiguous execution alias for an id-less publication")
        event = next(iter(candidates.values()), None)
    if event is None:
        return None
    if not _live_identity_matches(event, live):
        raise ValueError("live execution contradicts its durable fill witness")
    return event
