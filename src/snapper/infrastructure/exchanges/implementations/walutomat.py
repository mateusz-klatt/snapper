"""Walutomat FX exchange client implementation.

This module provides WalutomatExchangeClient for interacting with Walutomat,
a Polish FX exchange specializing in currency pairs like EUR/PLN, USD/PLN.
It supports:

REST API Operations:
    - Market data: tickers via polling
    - Order management: create, cancel, get orders
    - Account: balance inquiries
    - Execution streaming: polling-based fill detection

Features:
    - HTTP polling for real-time ticker updates
    - Polling-based execution streaming (no WebSocket available)
    - RSA signature authentication for private API
    - Candle building from tick data
    - Support for both public and authenticated endpoints

Mark convention: every price this module publishes as ``last`` (and every
1m candle price derived from it) is the midpoint of the venue's own two-sided
top-of-book quote, computed once in :func:`walutomat_quote`. The venue's
``forex_now`` field is an externally-sourced reference rate, is not a traded
price, and is never routed into a price slot.

Note: Walutomat does not provide WebSocket API, so real-time data is
obtained through periodic HTTP polling at configurable intervals.

Authentication uses RSA-SHA256 signatures with the private key provided
either as PEM format or base64-encoded PEM.
"""

import asyncio
import base64
import contextlib
import math
import time
from collections import deque
from collections.abc import AsyncIterator
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from typing import Any
from typing import Final
from urllib.parse import urlencode

import httpx
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from loguru import logger

from snapper.core.numeric import is_positive_finite
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerSnapshot
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryItem
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryTip
from snapper.infrastructure.exchanges.contracts import VenueOrderFillLegs
from snapper.infrastructure.exchanges.errors import AmbiguousOrderSubmitError
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatBestOffer
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatMarketPair
from snapper.infrastructure.exchanges.schemas.walutomat import WalutomatMarketResponse
from snapper.infrastructure.network.pooled_httpx_transport import PooledAsyncTransport
from snapper.infrastructure.symbols.functions import native_to_walutomat_rest
from snapper.infrastructure.symbols.functions import native_to_walutomat_ws
from snapper.infrastructure.symbols.functions import walutomat_rest_to_native
from snapper.infrastructure.symbols.functions import walutomat_ws_to_native


class _RaisingAsyncIterator[T](AsyncIterator[T]):
    """Async iterator that raises the provided exception on iteration."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def __aiter__(self) -> _RaisingAsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        raise self._exc


_NOT_CONNECTED_MSG = "Not connected - call connect() first"
_AUTH_REQUIRED_MSG = "Trading requires authentication - provide api_key and private_key"
_DISAPPEARED_RETRY_MAX = 10
"""Failed final-state queries before the disappeared-order retry escalates.

Escalation is a single CRITICAL log — the retry itself never stops and
no terminal state is ever fabricated."""
_EXEC_ID_BASIS_UNITS = 1e8


def _walutomat_exec_id(order_id: str, cumulative: float) -> str:
    """Deterministic execution id for a cumulative-snapshot fill.

    Walutomat reports no per-fill identifiers, only the order's running
    cumulative — so the identity of an emission IS ``(orderId,
    cumulative-at-emission)``. Basis-unit integer encoding avoids float
    repr drift across processes and replays. The disappeared-order
    TERMINAL emission appends ``-t``: it can legitimately repeat the
    last active emission's cumulative while carrying the FIRST terminal
    status, and an identical id would make the executor's dedup drop
    the status upgrade (its cum-anchored quantity delta is zero either
    way).

    Args:
        order_id: Venue order id.
        cumulative: The order's cumulative filled quantity at emission.

    Returns:
        A stable ``wal-{oid}-c{basis_units}`` identifier.
    """
    return f"wal-{order_id}-c{int(round(cumulative * _EXEC_ID_BASIS_UNITS))}"


def _parse_walutomat_exec_id(exec_id: str) -> tuple[str, int, bool] | None:
    """Decode ``wal-{orderId}-c{basis_units}[-t]`` into its witness components.

    Inverse of :func:`_walutomat_exec_id`. Returns ``None`` for any id that does
    not match the scheme, so a foreign or malformed exec id is unmappable and
    fails the witness bijection closed. The cumulative's ``-c`` is always the LAST
    one (appended after the order id), so an order id that itself contains ``-c``
    still parses correctly.

    Args:
        exec_id: The stored ``Execution.exec_id``.

    Returns:
        ``(order_id, cumulative_basis_units, is_terminal)`` or ``None``.
    """
    if not exec_id.startswith("wal-"):
        return None
    body = exec_id[len("wal-") :]
    is_terminal = body.endswith("-t")
    if is_terminal:
        body = body[: -len("-t")]
    order_id, separator, basis_units = body.rpartition("-c")
    if not separator or not order_id or not basis_units.isdigit():
        return None
    return order_id, int(basis_units), is_terminal


def _parse_walutomat_recon_exec_id(exec_id: str) -> tuple[str, int, bool] | None:
    """Decode the executor's legacy corrective ``recon-{orderId}-c{cumulative!r}``.

    The venue-agnostic reconciliation backstop
    (``ExchangeExecutorService._build_corrective_fill``) books a gap fill with
    ``exec_id = f"recon-{oid}-c{filled!r}"`` — the cumulative is a Python float repr
    (e.g. ``20.04``), NOT the basis-unit integer the streamed ``wal-`` scheme uses. A
    Walutomat marketable-limit that fills inside one poll interval never surfaces on
    the active-orders endpoint, so the adapter emits no ``wal-`` fill and this
    corrective is the sole committed execution; its cumulative is still the venue's
    own reported base volume, identical in meaning to :func:`_walutomat_exec_id`.
    Re-quantizing that float with the SAME ``int(round(value * _EXEC_ID_BASIS_UNITS))``
    expression makes a recon id and a hypothetical ``wal-`` id for the same fill decode
    to identical basis units, so the witness lands on the same account/history fill
    boundary. A corrective is always an active fill, never the disappeared-order ``-t``
    terminal, so ``is_terminal`` is ``False``. Fails closed (``None``) on any
    non-``recon-`` id, an empty order id or tail, or a non-numeric, non-finite,
    negative, or non-canonical-repr tail, preserving the foreign-id contract.

    Args:
        exec_id: The stored ``Execution.exec_id``.

    Returns:
        ``(order_id, cumulative_basis_units, is_terminal)`` or ``None``.
    """
    if not exec_id.startswith("recon-"):
        return None
    body = exec_id[len("recon-") :]
    order_id, separator, cumulative_repr = body.rpartition("-c")
    if not separator or not order_id or not cumulative_repr:
        return None
    try:
        cumulative = float(cumulative_repr)
    except ValueError:
        return None
    if not math.isfinite(cumulative) or cumulative < 0 or repr(cumulative) != cumulative_repr:
        return None
    return order_id, int(round(cumulative * _EXEC_ID_BASIS_UNITS)), False


@dataclass
class _TrackedOrder:
    """Internal state for polling-based execution tracking.

    Stores the last-known snapshot of an active order so that
    fill deltas and disappearance events can be computed across
    polling cycles.
    """

    order_id: str
    cl_ord_id: str
    symbol: str
    side: OrderSideEnum
    order_type: ExchangeOrderTypeEnum
    amount: float
    filled: float
    price: float
    counter_filled: float | None = None
    counter_filled_decimal: str | None = None
    filled_decimal: str | None = None


@dataclass(frozen=True)
class _TerminalExecutionFields:
    """Execution economics resolved for a terminal order snapshot."""

    last_qty: float | None
    last_price: float | None
    average_price: float | None
    counter_amount_decimal: str | None
    cum_fee: float | None
    cum_fee_currency: str | None


def _snapshot_tracked_order(order: ExchangeOrderSnapshot) -> _TrackedOrder:
    """Convert an order snapshot into the tracked polling state."""
    return _TrackedOrder(
        order_id=order.id,
        cl_ord_id=order.client_order_id or "",
        symbol=order.symbol,
        side=order.side,
        order_type=order.type,
        amount=order.amount,
        filled=order.filled,
        price=order.price or 0.0,
        counter_filled=order.counter_filled,
        counter_filled_decimal=order.counter_filled_decimal,
        filled_decimal=order.filled_decimal,
    )


def _disappeared_fill_update(
    final: ExchangeOrderSnapshot,
    tracked: _TrackedOrder,
    order_status: ExchangeOrderStatusEnum,
    fields: _TerminalExecutionFields,
) -> ExecutionUpdate:
    """Build one fill event from a terminal disappeared-order snapshot."""
    exec_id = _walutomat_exec_id(final.id, final.filled)
    if order_status is ExchangeOrderStatusEnum.FILLED:
        exec_id += "-t"
    return ExecutionUpdate(
        order_id=final.id,
        exec_type="trade",
        symbol=final.symbol,
        side=final.side,
        order_type=final.type,
        order_status=order_status,
        timestamp=datetime.now(UTC),
        cum_qty=final.filled,
        cum_qty_decimal=final.filled_decimal,
        cum_cost=final.counter_filled,
        exec_id=exec_id,
        cl_ord_id=final.client_order_id or tracked.cl_ord_id,
        order_qty=final.amount,
        limit_price=final.price,
        last_qty=fields.last_qty,
        last_price=fields.last_price,
        average_price=fields.average_price,
        counter_amount_decimal=fields.counter_amount_decimal,
        cum_fee=fields.cum_fee,
        cum_fee_decimal=final.fee_decimal if fields.cum_fee is not None else None,
        cum_fee_currency=fields.cum_fee_currency,
    )


def _disappeared_cancel_update(
    final: ExchangeOrderSnapshot,
    tracked: _TrackedOrder,
) -> ExecutionUpdate:
    """Build the terminal cancellation event for a disappeared order."""
    return ExecutionUpdate(
        order_id=final.id,
        exec_type="canceled",
        symbol=final.symbol,
        side=final.side,
        order_type=final.type,
        order_status=ExchangeOrderStatusEnum.CANCELED,
        timestamp=datetime.now(UTC),
        cum_qty=final.filled,
        cl_ord_id=final.client_order_id or tracked.cl_ord_id,
        order_qty=final.amount,
        limit_price=final.price,
    )


def _effective_price_fields(
    order: ExchangeOrderSnapshot, previous: _TrackedOrder | None
) -> tuple[float | None, float | None, float | None, str | None]:
    """Derive the effective per-fill economics from two-sided cumulatives.

    Walutomat permits price improvement, so the execution price is NEVER the
    limit price: the cumulative effective price is ``counter_cum / filled_cum``
    and the per-delta effective price is ``Δcounter / Δfilled`` between poll
    snapshots, computed in exact ``Decimal`` from the venue's own strings and
    floated only at the boundary. The EXACT per-fill counter amount ``Δcounter``
    is returned verbatim as a decimal string so the reconciliation replay can
    fold the true quote movement without the half-tick price-improvement
    tolerance. Returns all-``None`` when the snapshot carries no counter
    cumulative (the caller falls back to the legacy limit-price shape with a
    warning); omits the ``last_*`` pair and the exact counter (a zero-delta
    terminal is a status-only frame) when the fill delta is not positive or the
    previous snapshot lacks counter data to delta against.

    Args:
        order: The current order snapshot (two-sided cumulatives).
        previous: The last tracked snapshot, or ``None`` on the first emission.

    Returns:
        The per-delta quantity and price (both ``None`` when not derivable), the
        cumulative effective average price (``None`` without counter data), and
        the exact per-fill counter amount decimal string (``None`` unless a
        positive fill delta was derived).
    """
    if order.counter_filled_decimal is None:
        return None, None, None, None
    counter_cum = Decimal(order.counter_filled_decimal)
    filled_cum = (
        Decimal(order.filled_decimal)
        if order.filled_decimal is not None
        else Decimal(str(order.filled))
    )
    if filled_cum <= 0 or counter_cum <= 0:
        return None, None, None, None
    average_price = float(counter_cum / filled_cum)
    if previous is None:
        previous_filled = Decimal(0)
        previous_counter: Decimal | None = Decimal(0)
    else:
        previous_filled = (
            Decimal(previous.filled_decimal)
            if previous.filled_decimal is not None
            else Decimal(str(previous.filled))
        )
        previous_counter = (
            Decimal(previous.counter_filled_decimal)
            if previous.counter_filled_decimal is not None
            else None
        )
    delta_filled = filled_cum - previous_filled
    if previous_counter is None or delta_filled <= 0:
        return None, None, average_price, None
    delta_counter = counter_cum - previous_counter
    if delta_counter <= 0:
        return None, None, average_price, None
    return (
        float(delta_filled),
        float(delta_counter / delta_filled),
        average_price,
        str(delta_counter),
    )


def _terminal_execution_fields(
    final: ExchangeOrderSnapshot,
    tracked: _TrackedOrder,
) -> _TerminalExecutionFields:
    """Resolve terminal execution economics before status-specific emission."""
    last_qty, last_price, average_price, counter_amount_decimal = _effective_price_fields(
        final,
        tracked,
    )
    if average_price is None:
        logger.warning(
            f"Walutomat order {final.id}: no counter cumulative on terminal "
            f"snapshot — falling back to limit price for the execution economics"
        )
        average_price = final.price
    cum_fee = final.fee if final.fee and final.fee_currency else None
    return _TerminalExecutionFields(
        last_qty=last_qty,
        last_price=last_price,
        average_price=average_price,
        counter_amount_decimal=counter_amount_decimal,
        cum_fee=cum_fee,
        cum_fee_currency=final.fee_currency if cum_fee is not None else None,
    )


def _should_emit_active_execution(
    order: ExchangeOrderSnapshot,
    previous: _TrackedOrder | None,
) -> bool:
    """Return whether the current poll should emit an active-order fill update."""
    if previous is None:
        return order.filled > 0
    return order.filled > previous.filled


def _active_execution_status(order: ExchangeOrderSnapshot) -> ExchangeOrderStatusEnum:
    """Derive active-order execution status from cumulative fill progress."""
    if math.isclose(order.filled, order.amount):
        return ExchangeOrderStatusEnum.FILLED
    return ExchangeOrderStatusEnum.PARTIALLY_FILLED


async def _wait_for_wakeup(event: asyncio.Event) -> None:
    """Wait for a backoff wakeup event with a ``None`` task result."""
    await event.wait()


def _require_finite_balance_amount(balance_data: dict[str, Any], field: str) -> float:
    """Extract a present, finite amount from one Walutomat balances row.

    The native balance reader is a faithfulness boundary: an absent field
    or a non-finite value is a venue/data fault that must surface, never be
    coerced to ``0`` (a fabricated zero would read downstream as an
    authoritative empty balance).

    Args:
        balance_data: One ``account/balances`` result row.
        field: Amount field to read (``balanceTotal``, ``balanceAvailable``
            or ``balanceReserved``).

    Returns:
        The parsed finite amount.

    Raises:
        ValueError: If the field is absent or the parsed value is not finite.
    """
    if field not in balance_data:
        raise ValueError(f"Walutomat balance row missing '{field}': {balance_data}")
    amount = float(balance_data[field])
    if not math.isfinite(amount):
        raise ValueError(f"Walutomat balance '{field}' is not finite: {balance_data}")
    return amount


def _parse_native_balance(balance_data: dict[str, Any]) -> NativeBalanceEntry:
    """Strictly parse one Walutomat balances row into a native entry.

    Walutomat is an FX cash venue that always reports a faithful
    available/reserved/total split, so every amount populates the entry.
    The currency and each amount must be present and finite; anything
    missing or non-finite RAISES rather than degrading to a fabricated
    zero balance.

    Args:
        balance_data: One ``account/balances`` result row.

    Returns:
        The faithful native per-currency balance entry.

    Raises:
        ValueError: If the currency key is absent, an amount key is absent,
            or a parsed amount is not finite.
    """
    currency = balance_data.get("currency")
    if not isinstance(currency, str) or not currency:
        raise ValueError("Walutomat balance row has a missing or invalid currency")
    total_raw = balance_data.get("balanceTotal")
    free_raw = balance_data.get("balanceAvailable")
    used_raw = balance_data.get("balanceReserved")
    return NativeBalanceEntry(
        currency=currency,
        total=_require_finite_balance_amount(balance_data, "balanceTotal"),
        free=_require_finite_balance_amount(balance_data, "balanceAvailable"),
        used=_require_finite_balance_amount(balance_data, "balanceReserved"),
        total_decimal=total_raw if isinstance(total_raw, str) else None,
        free_decimal=free_raw if isinstance(free_raw, str) else None,
        used_decimal=used_raw if isinstance(used_raw, str) else None,
        numeric_provenance=(
            "venue_raw"
            if any(isinstance(value, str) for value in (total_raw, free_raw, used_raw))
            else "legacy_float"
        ),
    )


def _parse_walutomat_decimal(raw: str) -> Decimal:
    """Parse a Walutomat amount string into an exact Decimal.

    Walutomat renders amounts either bare (``"-999.00"``) or suffixed with the
    currency code (``"-432.43 PLN"``); both forms appear in the v2.0.0 spec
    examples. Exactly one numeric token with an optional ALPHABETIC suffix is
    accepted — any other shape (for example a digit-grouped ``"1 234.56"``)
    raises rather than silently truncating to the first group, because a wrong
    parsed amount would feed the anchor composition.

    Args:
        raw: The venue amount string.

    Returns:
        The exact signed amount.

    Raises:
        ValueError: If the amount is empty, its numeric token is not a decimal,
            or the string has extra non-currency tokens.
    """
    tokens = raw.split()
    if not tokens:
        raise ValueError(f"empty Walutomat amount: {raw!r}")
    if len(tokens) > 2 or (len(tokens) == 2 and not tokens[1].isalpha()):
        raise ValueError(f"ambiguous Walutomat amount: {raw!r}")
    try:
        return Decimal(tokens[0])
    except InvalidOperation as exc:
        raise ValueError(f"invalid Walutomat amount: {raw!r}") from exc


def _walutomat_operation_detail(operation_details: object, key: str) -> str | None:
    """Return a value from a Walutomat ``operationDetails`` key/value array.

    ``operationDetails`` is a flat array of ``{"key": ..., "value": ...}`` pairs
    (not a nested object), so a linear scan resolves a key. Returns ``None`` when
    the array is malformed, the key is absent, or its value is not a string.

    Args:
        operation_details: The raw ``operationDetails`` value from a history row.
        key: The detail key to resolve (e.g. ``"orderId"``).

    Returns:
        The string value for the key, or ``None`` when it is absent.
    """
    if not isinstance(operation_details, list):
        return None
    for detail in operation_details:
        if isinstance(detail, dict) and detail.get("key") == key:
            value = detail.get("value")
            return value if isinstance(value, str) else None
    return None


def _parse_walutomat_history_item(row: dict[str, Any]) -> VenueAccountHistoryItem:
    """Parse one Walutomat ``account/history`` row into a typed history item.

    ``operationAmount`` / ``balanceAfter`` are parsed exactly, tolerating both
    the bare-decimal and amount-plus-currency renderings the v2.0.0 spec shows.
    ``order_id`` is pulled from the flat ``operationDetails`` key/value array
    (absent on non-order rows); ``transaction_id`` groups a fill's two currency
    legs (absent on non-order rows).

    Args:
        row: One ``account/history`` result row.

    Returns:
        The typed account-history item.

    Raises:
        ValueError: If a required field is missing or an amount is malformed.
    """
    transaction_id = row.get("transactionId")
    ordered_by = row.get("orderedBy")
    submit_id = row.get("submitId")
    return VenueAccountHistoryItem(
        item_id=int(row["historyItemId"]),
        operation_type=str(row["operationType"]),
        operation_amount=_parse_walutomat_decimal(row["operationAmount"]),
        balance_after=_parse_walutomat_decimal(row["balanceAfter"]),
        currency=str(row["currency"]),
        transaction_id=transaction_id if isinstance(transaction_id, str) else None,
        ordered_by=ordered_by if isinstance(ordered_by, str) else "",
        order_id=_walutomat_operation_detail(row.get("operationDetails"), "orderId"),
        submit_id=submit_id if isinstance(submit_id, str) else None,
        correcting_entry=row.get("correctingEntry") is True,
    )


def _filled_order_from_history(
    items: tuple[VenueAccountHistoryItem, ...],
    client_order_id: str,
    symbol: str,
) -> ExchangeOrderSnapshot | None:
    """Build a non-terminal existence witness from observed history fills."""
    base_currency, quote_currency = symbol.split("-")
    matching_order_ids = {
        item.order_id
        for item in items
        if item.submit_id == client_order_id
        and item.operation_type == "MARKET_FX"
        and item.order_id is not None
        and not item.correcting_entry
    }
    if len(matching_order_ids) != 1:
        return None
    order_id = next(iter(matching_order_ids))
    order_items = [
        item
        for item in items
        if item.order_id == order_id
        and item.submit_id == client_order_id
        and item.operation_type == "MARKET_FX"
        and not item.correcting_entry
    ]
    base_amount = sum(
        (item.operation_amount for item in order_items if item.currency == base_currency),
        Decimal(0),
    )
    quote_amount = sum(
        (item.operation_amount for item in order_items if item.currency == quote_currency),
        Decimal(0),
    )
    if base_amount == 0 or quote_amount == 0 or base_amount * quote_amount >= 0:
        return None
    filled = abs(base_amount)
    counter_filled = abs(quote_amount)
    price = counter_filled / filled
    return ExchangeOrderSnapshot(
        id=order_id,
        client_order_id=client_order_id,
        symbol=symbol,
        side=OrderSideEnum.BUY if base_amount > 0 else OrderSideEnum.SELL,
        type=ExchangeOrderTypeEnum.MARKET,
        amount=float(filled),
        price=float(price),
        status=ExchangeOrderStatusEnum.OPEN,
        filled=float(filled),
        remaining=float(filled),
        timestamp=time.time(),
        amount_decimal=str(filled),
        price_decimal=str(price),
        filled_decimal=str(filled),
        counter_filled=float(counter_filled),
        counter_filled_decimal=str(counter_filled),
        amount_is_order_size=False,
    )


def _append_history_page(
    rows: list[VenueAccountHistoryItem],
    page: list[VenueAccountHistoryItem],
    cursor: int,
    upto_item_id: int,
) -> tuple[int, bool] | None:
    """Append one monotone page and report whether it crossed the upper bound."""
    for item in page:
        if item.item_id <= cursor:
            logger.warning(
                f"Walutomat history range: item id {item.item_id} did not advance "
                f"past cursor {cursor} — refusing the unfaithful walk"
            )
            return None
        if item.item_id > upto_item_id:
            return cursor, True
        rows.append(item)
        cursor = item.item_id
    return cursor, False


WALUTOMAT_MAX_RELATIVE_SPREAD: Final[float] = 0.25
"""Widest ``(ask - bid) / mid`` whose midpoint is still accepted as a mark.

Calibrated against the FULL 44-pair production distribution, not one pair. The
predecessor value ``0.02`` was justified as "17x the EUR-PLN spread" and that
single-pair reading was wrong about the venue: measured across every walutomat
pair the median relative spread is 83.7 bps, only TRY-PLN exceeds 200 bps (at
1197 bps, chronically), the tightest pairs still below the old ceiling were
GBP-CHF at 176.6 bps, DKK-PLN at 164.6 and CNY-PLN at 157.0, and 15 of the 44
pairs sit between 100 and 200 bps - one doubling away from silence. Wide
spreads are this peer-to-peer venue's NORMAL condition, not a pathology.

The two populations are separable: honest thin books on this venue reach about
12 percent relative spread, while broken books start around 50 percent.
``0.25`` sits between them - 2x headroom over chronically wide TRY-PLN and 14x
over the tightest survivor - so it refuses only books whose midpoint is not a
price under any reading. Per-pair ceilings are the fallback if one pair's own
spread ever proves too volatile for a single global bound; do not pay that
operational cost before a measurement demands it.

The ceiling is NOT deleted, and that was proposed and refuted - recorded here
because the reasoning generalises. Every consumer of this mark is MONOTONE:
``TrailingStopEvaluator.on_tick`` ratchets ``peak_price`` and never lowers it,
the four equity peaks are ``max()``, and the Phase-5B causal peak derives from
persisted samples F6 forbids rewriting. On a peer-to-peer book a single dust
order (bid 0.01 against ask 4.31) is positive, finite and uncrossed, so it
passes all three structural refusals, and it prints a mid roughly 50 percent
off - which ratchets a trailing stop into a phantom breach and a REAL forced
close, or a peak into a permanent phantom drawdown. A refusal hole costs one
stale minute; a wrong value in a ratchet costs state that never recovers by
market action. The mark-convention plan also relies on this ceiling by name: a
size gate was rejected there (the venue payload carries no sizes) and the
relative-spread refusal was accepted in its place as the bound on self-marking,
so removing it would reopen a decision whose alternative is unavailable.

Do not tighten it without measurement either - a tighter gate manufactures
candle holes, and holes flip PnL plane election."""

WALUTOMAT_REFUSAL_WARNING_SECONDS: Final[float] = 600.0
"""How long one symbol may stay continuously refused before the feed degrades.

The measured phenomenon this bounds is the duration of a run of continuously
UNUSABLE polled books - NOT the venue's healthy tick cadence. Those are
different phenomena and only the first one can extend a refusal: the latch is
cleared by :meth:`WalutomatExchangeClient._note_recovered_mark`, which runs on
EVERY usable poll, and the per-symbol lag baseline is refreshed before tick
deduplication, so a healthy pair whose book simply has not moved is
refusal-free and lag-fresh at every poll. Post-deduplication silence between
emitted ticks therefore has no causal path into a refusal duration, and an
earlier revision of this docstring was wrong to bound one with the other.

At the default 10 s ``polling_interval`` this value is 60 consecutive refused
polls. Sixty unusable books in a row is a chronic condition on a venue whose
NORMAL state is a wide but two-sided book (see
:data:`WALUTOMAT_MAX_RELATIVE_SPREAD`), so the false-positive risk here is the
same as it was at the hour this replaced - while a real blackout now pages
roughly fifty minutes sooner.

No measured distribution of refusal durations exists yet: the venue has never
been observed with refusal instrumentation in place, so this is a conservative
OPERATIONAL choice, not a measured threshold. Revisit it against
``refused_mark_seconds`` once the heartbeat has accumulated real history."""

WALUTOMAT_REFUSAL_WINDOW_SECONDS: Final[float] = 3600.0
"""Rolling window over which the per-symbol refusal FRACTION is measured.

The continuous-streak bound alone cannot see a flapping book, because recovery
pops the streak clock: a pair that is unusable on 99 percent of polls but
flickers usable once an hour resets its streak forever and never escalates,
while its transition logs run to thousands of lines a day. The dust-order
shape :data:`WALUTOMAT_MAX_RELATIVE_SPREAD` describes - a peer-to-peer order
appearing and being cancelled - is exactly such a generator. One hour of poll
outcomes is long enough that an hourly flicker cannot hide the surrounding
refusals and short enough that a resolved incident ages out of the signal."""

WALUTOMAT_REFUSAL_WINDOW_MIN_POLLS: Final[int] = 60
"""Poll outcomes a symbol needs inside the window before its fraction counts.

Without a floor the very first refused poll reads as a 100 percent refusal
fraction. Sixty outcomes is ten minutes at the default 10 s cadence, which
matches :data:`WALUTOMAT_REFUSAL_WARNING_SECONDS` so neither rule can escalate
on less than ten minutes of evidence."""

WALUTOMAT_REFUSAL_FRACTION_CEILING: Final[float] = 0.5
"""Share of a symbol's windowed polls that may be refused before degrading.

A book unusable for more than half of the last hour is chronic no matter how
often it flickers usable in between. This is a deliberately loose bound: it
exists to catch the flapping case the streak rule structurally cannot see, not
to page on an occasional wide print, and like the streak bound it is an
operational choice with no measured distribution behind it yet."""

WALUTOMAT_TRANSITION_LOG_COOLDOWN_SECONDS: Final[float] = 300.0
"""Minimum gap between two logged transitions of the same kind for one symbol.

A flapping book crosses the refused/recovered boundary on almost every poll,
and the transition-only logging that keeps a 44-pair sweep quiet does nothing
to bound THAT: at a 10 s cadence an alternating pair emits ~8600 lines a day
on its own. Entry and recovery are rate-limited separately so the first pair
of lines - the informative ones - always survives, after which each kind is
capped at one line per five minutes per symbol and the suppressed count rides
the next line that gets through. The counts and durations on the heartbeat are
the current-state signal; the log is only the transition record."""


@dataclass(frozen=True)
class WalutomatQuote:
    """A Walutomat top-of-book quote that passed every usability refusal.

    Carrying ``bid`` and ``ask`` alongside ``mark`` is load-bearing rather than
    stylistic: :func:`snapper.core.numeric.is_positive_finite` returns
    ``TypeIs[float]``, so the narrowing performed inside :func:`walutomat_quote`
    cannot reach a caller that receives only the mark. Returning the narrowed
    sides with the mark is what lets every call site drop the ``or 0.0``
    fallbacks instead of re-adding them to satisfy the type checker.
    """

    bid: float
    ask: float
    mark: float


def walutomat_quote(offer: WalutomatBestOffer) -> WalutomatQuote | None:
    """Derive the marked quote for one Walutomat pair, or refuse it.

    The mark is the midpoint of the venue's own two-sided top-of-book quote.
    ``forex_now`` is an externally-sourced reference rate that is not a traded
    price and is never read here. Refusals return ``None`` - the caller emits
    no tick, appends nothing to the tick buffer and therefore builds no bar.
    No substituted number is ever produced.

    Refusals, in order:

    1. ``bid_now`` is not positive and finite (covers ``None``, ``0.0``,
       negatives, NaN and infinities).
    2. ``ask_now`` is not positive and finite.
    3. The book is crossed (``bid_now > ask_now``) - a venue data
       inconsistency whose midpoint is not a price. A LOCKED book
       (``bid == ask``) is accepted: spread 0, mark equal to both sides,
       and refusing it would create holes for no gain.
    4. The relative spread exceeds :data:`WALUTOMAT_MAX_RELATIVE_SPREAD`.

    Args:
        offer: The venue's ``bestOffers`` payload for one pair.

    Returns:
        The narrowed sides plus their midpoint, or ``None`` when the book
        carries no usable two-sided quote.
    """
    bid = offer.bid_now
    ask = offer.ask_now
    if not is_positive_finite(bid):
        return None
    if not is_positive_finite(ask):
        return None
    if bid > ask:
        return None
    mark = (bid + ask) / 2.0
    if (ask - bid) / mark > WALUTOMAT_MAX_RELATIVE_SPREAD:
        return None
    return WalutomatQuote(bid=bid, ask=ask, mark=mark)


def walutomat_quote_refusal(offer: WalutomatBestOffer) -> str | None:
    """Name the refusal :func:`walutomat_quote` would apply to ``offer``.

    Logging-only companion to :func:`walutomat_quote`, which returns ``None``
    without a reason so that its narrowing stays exact. The two agree by
    construction: this returns ``None`` exactly when :func:`walutomat_quote`
    returns a quote, and a regression test pins that equivalence so the
    message can never describe a decision that was not taken.

    Args:
        offer: The venue's ``bestOffers`` payload for one pair.

    Returns:
        A short reason label, or ``None`` when the quote is usable.
    """
    bid = offer.bid_now
    ask = offer.ask_now
    if not is_positive_finite(bid):
        return "bid_now is not a positive finite number"
    if not is_positive_finite(ask):
        return "ask_now is not a positive finite number"
    if bid > ask:
        return "crossed book"
    if (ask - bid) / ((bid + ask) / 2.0) > WALUTOMAT_MAX_RELATIVE_SPREAD:
        return f"relative spread above {WALUTOMAT_MAX_RELATIVE_SPREAD}"
    return None


_REFUSED_TRANSITION: Final = "refused"
"""Rate-limit key for the entry-into-refusal log line."""

_RECOVERED_TRANSITION: Final = "recovered"
"""Rate-limit key for the exit-from-refusal log line."""


def _suppressed_suffix(suppressed: int) -> str:
    """Render the suppressed-transition tail appended to a transition log.

    Args:
        suppressed: Transitions of this kind dropped by the per-symbol rate
            limit since the previous emitted line.

    Returns:
        An empty string when nothing was suppressed, otherwise a short tail
        naming the count so a flapping book is legible from one line.
    """
    if suppressed == 0:
        return ""
    return f" [{suppressed} further transition(s) suppressed]"


@dataclass(frozen=True)
class WalutomatMarkRefusalReport:
    """Observable state of the fail-closed mark refusals, for the heartbeat.

    Fail-closed without visibility is fail-STALE. The ticker plane applies no
    age gate, so a chronically refused pair keeps serving its frozen last-good
    mark to position valuation, the caps notional and the paper fill path while
    the feed looks healthy - the exact frozen-mark defect the mid convention
    exists to remove, resurrected one pair at a time. This report is what makes
    that condition legible outside the log file.

    Attributes:
        symbols: Symbols currently in the refused state, sorted.
        counts: Cumulative refused-poll count per symbol, sorted by symbol and
            NOT reset on recovery, so an intermittently refusing pair
            accumulates evidence instead of hiding between transitions.
        seconds: Whole seconds each currently-refused symbol has been
            continuously refused.
        fractions: Share of the last :data:`WALUTOMAT_REFUSAL_WINDOW_SECONDS`
            of poll outcomes that were refusals, for every symbol with at
            least :data:`WALUTOMAT_REFUSAL_WINDOW_MIN_POLLS` outcomes in the
            window and at least one refusal among them. Symbols with a clean
            window are omitted so the heartbeat carries evidence rather than
            44 zeroes.
        escalated: Whether any symbol has been continuously refused for at
            least :data:`WALUTOMAT_REFUSAL_WARNING_SECONDS`, OR has a windowed
            refusal fraction above
            :data:`WALUTOMAT_REFUSAL_FRACTION_CEILING`. The second rule exists
            because recovery pops the streak clock, so a mostly-unusable book
            that flickers usable often enough never satisfies the first.
    """

    symbols: tuple[str, ...]
    counts: Mapping[str, int]
    seconds: Mapping[str, int]
    fractions: Mapping[str, float]
    escalated: bool


class WalutomatExchangeClient(ExchangeClientBase):
    """Walutomat FX exchange client with REST API support.

    This client provides access to the Walutomat Polish FX exchange
    through HTTP REST API with polling for real-time data.

    The client implements RSA-SHA256 authentication for private API
    endpoints and builds candles from tick data since Walutomat
    doesn't provide OHLCV data directly.

    Attributes:
        market_data_url: Public market data endpoint URL.
        api_base_url: Base URL for authenticated API.
        polling_interval: Interval between market data polls in seconds.
        timeout: HTTP request timeout in seconds.
    """

    balance_capability = CapabilityStatus.SUPPORTED
    """Walutomat reports faithful FX cash balances via ``account/balances``."""
    position_observation_capability = CapabilityStatus.NOT_APPLICABLE
    position_capability = CapabilityStatus.NOT_APPLICABLE
    """FX spot venue: there are no derivatives positions to track."""
    account_history_capability = CapabilityStatus.SUPPORTED
    """Walutomat exposes a faithful append-only ``account/history`` ledger, the
    substrate the spot-anchor bootstrap seals and the witness join folds."""

    def __init__(
        self,
        polling_interval: float = 10.0,
        timeout: float = 5.0,
        api_key: str | None = None,
        private_key_data: str | None = None,
        repository: Repository | None = None,
        execution_poll_interval: float = 5.0,
    ) -> None:
        """Initialize Walutomat exchange client.

        Args:
            polling_interval: Interval between market data polls (default: 10s).
            timeout: HTTP request timeout in seconds (default: 5s).
            api_key: Walutomat API key for authenticated requests.
            private_key_data: RSA private key (PEM or base64-encoded PEM).
            repository: Database repository for order/execution logging.
            execution_poll_interval: Interval between execution polls (default: 5s).

        Raises:
            ValueError: If private key format is invalid.
        """
        super().__init__(repository=repository, exchange_name=ExchangeEnum.WALUTOMAT)
        self.market_data_url = "https://user.walutomat.pl/api/public/marketBrief"
        self.api_base_url = "https://api.walutomat.pl/api/v2.0.0"
        self.polling_interval = polling_interval
        self.timeout = timeout
        self._api_key = api_key
        self._private_key: Any | None = None
        if private_key_data:
            if private_key_data.strip().startswith("-----BEGIN"):
                pem_bytes = private_key_data.encode()
            else:
                try:
                    pem_bytes = base64.b64decode(private_key_data)
                except Exception as e:
                    raise ValueError(
                        f"Invalid private key format - expected PEM or base64-encoded PEM: {e}"
                    ) from e
            try:
                self._private_key = serialization.load_pem_private_key(pem_bytes, password=None)
            except Exception as e:
                raise ValueError(f"Failed to load RSA private key: {e}") from e
        self._http_client: httpx.AsyncClient | None = None
        self._running = False
        self._tick_queue: asyncio.Queue[TickerUpdate] = asyncio.Queue()
        self._candle_queue: asyncio.Queue[CandleUpdate] = asyncio.Queue()
        self._polling_task: asyncio.Task[None] | None = None
        self._candle_builder_task: asyncio.Task[None] | None = None
        self._last_data: dict[str, WalutomatMarketPair] | None = None
        self._consecutive_error_count: int = 0
        self._backoff_attempts: int = 0
        self._backoff_until: float = 0.0
        self._backoff_wakeup_event: asyncio.Event | None = None
        self._max_consecutive_errors = 5
        self._tick_buffers: dict[str, list[tuple[float, float]]] = {}
        self._refused_marks: set[str] = set()
        self._refused_mark_counts: dict[str, int] = {}
        self._refused_since: dict[str, float] = {}
        self._poll_outcomes: dict[str, deque[tuple[float, bool]]] = {}
        self._transition_logged_at: dict[tuple[str, str], float] = {}
        self._suppressed_transitions: dict[tuple[str, str], int] = {}
        self._execution_poll_interval = execution_poll_interval
        self._execution_idle_interval = 30.0
        self._execution_wake: asyncio.Event = asyncio.Event()
        self._disappeared_retry_counts: dict[str, int] = {}

    def _require_connected(self) -> httpx.AsyncClient:
        """Verify HTTP client is connected and return it.

        Returns:
            The connected HTTP client.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._http_client:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        return self._http_client

    def _require_authenticated(self) -> httpx.AsyncClient:
        """Verify client is connected and has authentication credentials.

        Returns:
            The connected HTTP client.

        Raises:
            RuntimeError: If not connected or missing credentials.
        """
        client = self._require_connected()
        if not self._api_key or not self._private_key:
            raise RuntimeError(_AUTH_REQUIRED_MSG)
        return client

    async def connect(self) -> None:
        """Connect to Walutomat API and verify connectivity.

        Raises:
            ConnectionError: If initial API request fails.
        """
        if self._running:
            logger.warning("WalutomatExchangeClient already connected")
            return
        logger.info("Connecting to Walutomat API...")
        http_client = httpx.AsyncClient(
            transport=PooledAsyncTransport(),
            timeout=httpx.Timeout(self.timeout),
            follow_redirects=True,
        )
        self._http_client = http_client
        try:
            data = await self._fetch_market_data()
            self._last_data = data
            pair_count = len(data)
            logger.info(f"Walutomat API connected - {pair_count} pairs available")
        except BaseException as e:
            self._http_client = None
            with contextlib.suppress(Exception):
                await http_client.aclose()
            if isinstance(e, Exception):
                raise ConnectionError(f"Failed to connect to Walutomat API: {e}") from e
            raise
        self._running = True

    async def disconnect(self) -> None:
        """Disconnect from Walutomat and stop polling tasks.

        The HTTP client close is NOT gated on ``_running``: a cancelled
        ``connect()`` leaves ``_running`` False with the client object
        already allocated, and the ``__aenter__`` failure cleanup calls
        this method expecting it to release that client.
        """
        if self._running:
            logger.info("Disconnecting from Walutomat API...")
            self._running = False
            if self._polling_task:
                self._polling_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._polling_task
                self._polling_task = None
            if self._candle_builder_task:
                self._candle_builder_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._candle_builder_task
                self._candle_builder_task = None
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
            logger.info("Disconnected from Walutomat API")

    async def _fetch_market_data(self) -> dict[str, WalutomatMarketPair]:
        """Fetch current market data from Walutomat public API.

        Returns:
            Dictionary mapping Walutomat symbol to market pair data.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._http_client:
            raise RuntimeError("Not connected")
        await self._acquire_rest_slot()
        response = await self._http_client.get(self.market_data_url)
        response.raise_for_status()
        data_list = response.json()
        market_response = WalutomatMarketResponse.from_api_response(data_list)
        return market_response.to_dict()

    def _sign_request(self, timestamp: str, endpoint: str, body: str = "") -> str:
        """Sign a request using RSA-SHA256.

        Args:
            timestamp: ISO 8601 timestamp for the request.
            endpoint: API endpoint path.
            body: Request body content.

        Returns:
            Base64-encoded signature string.

        Raises:
            RuntimeError: If private key not configured.
        """
        if not self._private_key:
            raise RuntimeError(
                "Trading API requires authentication - provide api_key and private_key"
            )
        data_to_sign = f"{timestamp}{endpoint}{body}"
        signature = self._private_key.sign(
            data_to_sign.encode(),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode()

    def _get_auth_headers(self, endpoint: str, body: str = "") -> dict[str, str]:
        """Build authentication headers for a request.

        Args:
            endpoint: API endpoint path.
            body: Request body content.

        Returns:
            Dictionary of authentication headers.

        Raises:
            RuntimeError: If API key not configured.
        """
        if not self._api_key:
            raise RuntimeError("Trading API requires authentication - provide api_key")
        headers = {"X-API-Key": self._api_key}
        if self._private_key:
            timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            signature = self._sign_request(timestamp, endpoint, body)
            headers["X-API-Signature"] = signature
            headers["X-API-Timestamp"] = timestamp
        return headers

    def _build_ticker_from_pair(self, native_symbol: str, quote: WalutomatQuote) -> TickerUpdate:
        """Build a TickerUpdate from an accepted Walutomat quote.

        ``last`` carries the top-of-book mid. ``vwap``, ``low`` and ``high`` are
        contractually 24-hour aggregates (``vwap_24h`` / ``low_24h`` /
        ``high_24h`` downstream) that ``marketBrief`` does not report, so they
        are honest zeros rather than an instantaneous price wearing a 24h
        label - the same choice the Kraken futures adapter makes for ``vwap``.
        ``volume`` and the two quantity fields are zero for the same reason:
        the payload carries no sizes and no volume.

        Args:
            native_symbol: Native symbol string.
            quote: Accepted top-of-book quote carrying the narrowed sides.

        Returns:
            TickerUpdate with current market data.
        """
        return TickerUpdate(
            symbol=native_symbol,
            bid=quote.bid,
            bid_qty=0.0,
            ask=quote.ask,
            ask_qty=0.0,
            last=quote.mark,
            volume=0.0,
            vwap=0.0,
            low=0.0,
            high=0.0,
            change=0.0,
            change_pct=0.0,
        )

    def _record_poll_outcome(self, native_symbol: str, *, refused: bool) -> None:
        """Append one poll outcome to the symbol's rolling refusal window.

        Every polled pair lands here exactly once per poll, whether its book
        was usable, unusable or missing from the payload altogether, so the
        window's denominator is the real poll count rather than the refusal
        count. Outcomes older than :data:`WALUTOMAT_REFUSAL_WINDOW_SECONDS` are
        dropped on append, which bounds the memory at one entry per poll per
        symbol for one window (~360 entries per pair at the default cadence).

        Args:
            native_symbol: Native symbol whose book was just polled.
            refused: Whether that poll produced no usable mark.

        Returns:
            None.
        """
        now = time.monotonic()
        window = self._poll_outcomes.setdefault(native_symbol, deque())
        window.append((now, refused))
        cutoff = now - WALUTOMAT_REFUSAL_WINDOW_SECONDS
        while window and window[0][0] < cutoff:
            window.popleft()

    def _claim_transition_log(self, native_symbol: str, kind: str) -> int | None:
        """Decide whether this refusal-state transition may emit a log line.

        Rate limiting is keyed on ``(symbol, kind)`` rather than on the symbol
        alone so a first refusal and its first recovery both survive - those
        two lines are the informative ones - while a book that flaps across
        the boundary every poll is capped at one line per kind per
        :data:`WALUTOMAT_TRANSITION_LOG_COOLDOWN_SECONDS`. Suppressed
        transitions are counted, not discarded, and the count rides the next
        line that gets through so the flapping itself stays visible.

        Args:
            native_symbol: Native symbol that changed refusal state.
            kind: Transition kind, ``"refused"`` or ``"recovered"``.

        Returns:
            The number of transitions of this kind suppressed since the last
            emitted line, or ``None`` when this transition must stay silent.
        """
        key = (native_symbol, kind)
        now = time.monotonic()
        last = self._transition_logged_at.get(key)
        if last is not None and now - last < WALUTOMAT_TRANSITION_LOG_COOLDOWN_SECONDS:
            self._suppressed_transitions[key] = self._suppressed_transitions.get(key, 0) + 1
            return None
        self._transition_logged_at[key] = now
        return self._suppressed_transitions.pop(key, 0)

    def _latch_refusal(self, native_symbol: str) -> bool:
        """Count one refused poll and report whether it entered the state.

        The COUNTER and the rolling window are updated on every refused poll,
        before the latch check. The log line alone is current-state-free and
        scrolls away, so a chronic refusal would otherwise be invisible after
        the first minute of an operator's attention; the counter, the entry
        instant and the window are what :meth:`mark_refusal_report` publishes
        into the heartbeat.

        Args:
            native_symbol: Native symbol whose mark was refused.

        Returns:
            ``True`` when this poll moved the symbol INTO the refused state
            (so the caller may log a transition), ``False`` when it was
            already latched.
        """
        self._refused_mark_counts[native_symbol] = (
            self._refused_mark_counts.get(native_symbol, 0) + 1
        )
        self._record_poll_outcome(native_symbol, refused=True)
        if native_symbol in self._refused_marks:
            return False
        self._refused_marks.add(native_symbol)
        self._refused_since[native_symbol] = time.monotonic()
        return True

    def _note_refused_mark(self, native_symbol: str, offer: WalutomatBestOffer) -> None:
        """Count every refusal and warn on entry into the refused state.

        A refusal is a hole in the tick and candle planes, so it must be
        observable; but the poller sweeps ~44 pairs every interval, so a
        per-poll warning would be spam. The symbol is therefore latched in
        ``_refused_marks``, only the transition is logged, and that log is
        additionally rate-limited per symbol
        (:meth:`_claim_transition_log`) so a flapping book cannot turn the
        transition record into thousands of daily lines.

        Args:
            native_symbol: Native symbol whose mark was refused.
            offer: The raw ``bestOffers`` payload that was refused.

        Returns:
            None.
        """
        if not self._latch_refusal(native_symbol):
            return
        suppressed = self._claim_transition_log(native_symbol, _REFUSED_TRANSITION)
        if suppressed is None:
            return
        logger.warning(
            "Walutomat {} mark refused ({}) — raw bid_now={} ask_now={}{}",
            native_symbol,
            walutomat_quote_refusal(offer),
            offer.bid_now,
            offer.ask_now,
            _suppressed_suffix(suppressed),
        )

    def _note_absent_pair(self, native_symbol: str) -> None:
        """Latch a subscribed pair that vanished from the venue payload.

        A pair the venue simply stops returning used to produce a DEBUG line
        and nothing else: no latch, no clock, no count, and - because
        ``_last_data_timestamps`` is only written on delivery - no per-symbol
        lag either until the publisher's own seed made the symbol visible.
        That is the TRY-PLN blackout shape with zero observability, so it is
        treated here as exactly what it is: a refusal. The last thing known
        about the pair is that no usable mark arrived, and its frozen last-good
        mark keeps serving valuation just as it would for a pair whose book is
        still arriving and still unusable.

        Args:
            native_symbol: Native symbol missing from the venue payload.

        Returns:
            None.
        """
        if not self._latch_refusal(native_symbol):
            return
        suppressed = self._claim_transition_log(native_symbol, _REFUSED_TRANSITION)
        if suppressed is None:
            return
        logger.warning(
            "Walutomat {} mark refused (pair absent from the venue payload){}",
            native_symbol,
            _suppressed_suffix(suppressed),
        )

    def _note_recovered_mark(self, native_symbol: str) -> None:
        """Log once when a previously refused symbol produces a usable quote.

        Recovery clears the latch and the continuous-refusal clock but NOT the
        cumulative counter or the rolling window: a pair that flaps between
        usable and refused books is a real data-quality signal, and zeroing its
        count on every recovery would erase exactly that evidence. The window
        is what turns that evidence into an escalation, because the streak
        clock this method pops can never reach the chronic bound on a flapping
        pair.

        Args:
            native_symbol: Native symbol whose mark became usable again.

        Returns:
            None.
        """
        self._record_poll_outcome(native_symbol, refused=False)
        if native_symbol not in self._refused_marks:
            return
        self._refused_marks.discard(native_symbol)
        self._refused_since.pop(native_symbol, None)
        suppressed = self._claim_transition_log(native_symbol, _RECOVERED_TRANSITION)
        if suppressed is None:
            return
        logger.info(
            "Walutomat {} mark recovered — two-sided quote usable again{}",
            native_symbol,
            _suppressed_suffix(suppressed),
        )

    def _refusal_fractions(self, now: float) -> dict[str, float]:
        """Compute each symbol's refused share of the rolling poll window.

        Symbols with fewer than :data:`WALUTOMAT_REFUSAL_WINDOW_MIN_POLLS`
        outcomes in the window are omitted (too little evidence to read), and
        so are symbols with a clean window - publishing 44 zeroes every
        heartbeat would bury the one pair that matters.

        Args:
            now: Reference monotonic instant, shared with the streak clock so
                one report cannot mix two readings of time.

        Returns:
            Refused fraction per symbol, sorted by symbol, rounded to four
            decimals for a stable heartbeat payload.
        """
        cutoff = now - WALUTOMAT_REFUSAL_WINDOW_SECONDS
        fractions: dict[str, float] = {}
        for symbol, window in sorted(self._poll_outcomes.items()):
            recent = [refused for observed_at, refused in window if observed_at >= cutoff]
            refused_count = sum(1 for value in recent if value)
            if len(recent) < WALUTOMAT_REFUSAL_WINDOW_MIN_POLLS or refused_count == 0:
                continue
            fractions[symbol] = round(refused_count / len(recent), 4)
        return fractions

    def mark_refusal_report(self) -> WalutomatMarkRefusalReport:
        """Report the current mark-refusal state for the publisher heartbeat.

        The exchange client owns the refusal decision, but the heartbeat is
        built by the publisher, so this read-only accessor is the seam between
        them. A symbol that stops appearing in the venue payload entirely keeps
        its latch and its clock, which is deliberate: the last thing known
        about it is a refusal, and its mark is just as frozen as a pair whose
        book is still arriving and still unusable.

        Escalation is the OR of two rules on purpose. The streak rule catches
        an unbroken blackout; the fraction rule catches a book that is mostly
        unusable but flickers usable often enough to keep resetting the streak
        clock, which the streak rule structurally cannot see.

        Returns:
            Currently refused symbols, cumulative per-symbol refusal counts,
            per-symbol continuous refusal durations in whole seconds, windowed
            refusal fractions, and whether either escalation rule has fired.
        """
        now = time.monotonic()
        seconds = {
            symbol: int(now - since) for symbol, since in sorted(self._refused_since.items())
        }
        fractions = self._refusal_fractions(now)
        escalated = any(
            value >= WALUTOMAT_REFUSAL_WARNING_SECONDS for value in seconds.values()
        ) or any(value > WALUTOMAT_REFUSAL_FRACTION_CEILING for value in fractions.values())
        return WalutomatMarkRefusalReport(
            symbols=tuple(sorted(self._refused_marks)),
            counts=dict(sorted(self._refused_mark_counts.items())),
            seconds=seconds,
            fractions=fractions,
            escalated=escalated,
        )

    async def _process_polling_data(
        self, data: dict[str, WalutomatMarketPair], symbol_map: dict[str, str]
    ) -> None:
        """Process fetched market data and enqueue tickers.

        A pair whose book carries no usable two-sided quote is skipped
        entirely: no ticker is enqueued and nothing is appended to the tick
        buffer, so the minute simply has no bar rather than a fabricated one.

        A subscribed pair missing from the payload altogether is treated as a
        refusal (:meth:`_note_absent_pair`) rather than as a DEBUG line: it
        produces the same frozen mark and the same candle hole as an unusable
        book, so it must produce the same observable state.

        Args:
            data: Fetched market data keyed by Walutomat symbol.
            symbol_map: Mapping of Walutomat symbol to native symbol.
        """
        for wal_symbol, native_symbol in symbol_map.items():
            if wal_symbol not in data:
                self._note_absent_pair(native_symbol)
                continue
            pair_data = data[wal_symbol]
            quote = walutomat_quote(pair_data.best_offers)
            if quote is None:
                self._note_refused_mark(native_symbol, pair_data.best_offers)
                continue
            self._note_recovered_mark(native_symbol)
            ticker = self._build_ticker_from_pair(native_symbol, quote)
            await self._tick_queue.put(ticker)
            ts = time.time()
            if native_symbol not in self._tick_buffers:
                self._tick_buffers[native_symbol] = []
            self._tick_buffers[native_symbol].append((ts, quote.mark))
            logger.debug(f"{native_symbol}: bid={ticker.bid:.4f} ask={ticker.ask:.4f}")

    def _handle_http_error(self, error: httpx.HTTPError) -> None:
        """Handle an HTTP error during polling.

        Increments the consecutive error counter and enters an
        exponential backoff state after the configured threshold. The
        polling task stays alive throughout backoff and can be woken
        by the publisher liveness recovery hook.

        Args:
            error: The HTTP error that occurred.

        Returns:
            None.
        """
        self._consecutive_error_count += 1
        if self._consecutive_error_count < self._max_consecutive_errors:
            logger.warning(
                "Walutomat API error ({}/{}) — will retry: {}",
                self._consecutive_error_count,
                self._max_consecutive_errors,
                error,
            )
            return
        self._backoff_attempts += 1
        backoff_s = min(60 * (2 ** (self._backoff_attempts - 1)), 1800)
        self._backoff_until = time.monotonic() + backoff_s
        if self._backoff_wakeup_event is not None:
            self._backoff_wakeup_event.clear()
        logger.error(
            "Walutomat API error ({}/{}) — entering backoff {}s (attempt {}): {}",
            self._consecutive_error_count,
            self._max_consecutive_errors,
            backoff_s,
            self._backoff_attempts,
            error,
        )

    async def _polling_loop(self, symbols: list[str]) -> None:
        """Run the market data polling loop.

        Args:
            symbols: List of native symbols to poll.
        """
        logger.info(f"Starting Walutomat polling (interval: {self.polling_interval}s)")
        symbol_map = {native_to_walutomat_ws(s): s for s in symbols}
        while self._running:
            try:
                await self._wait_for_backoff_if_needed()
                data = await self._fetch_market_data()
                self._last_data = data
                self._reset_polling_error_state()
                await self._process_polling_data(data, symbol_map)
            except httpx.HTTPError as e:
                self._handle_http_error(e)
            except Exception as e:
                logger.exception(f"Unexpected error in polling loop: {e}")
            await asyncio.sleep(self.polling_interval)
        logger.info("Walutomat polling stopped")

    def _ensure_backoff_wakeup_event(self) -> asyncio.Event:
        """Return the wakeup event used to interrupt HTTP backoff.

        Returns:
            The existing or newly created wakeup event.
        """
        if self._backoff_wakeup_event is None:
            self._backoff_wakeup_event = asyncio.Event()
        return self._backoff_wakeup_event

    async def _wait_for_backoff_if_needed(self) -> None:
        """Sleep until HTTP backoff expires or recovery wakes the loop.

        Returns:
            None.
        """
        wakeup_event = self._ensure_backoff_wakeup_event()
        now = time.monotonic()
        if now >= self._backoff_until:
            return
        await self._wait_for_backoff_sleep(self._backoff_until - now, wakeup_event)
        wakeup_event.clear()
        self._backoff_until = 0.0

    async def _wait_for_backoff_sleep(self, sleep_s: float, wakeup_event: asyncio.Event) -> None:
        """Wait for either the backoff timer or wakeup event.

        Args:
            sleep_s: Remaining backoff seconds.
            wakeup_event: Event set by liveness recovery.

        Returns:
            None.

        Raises:
            asyncio.CancelledError: Propagated when the polling task is cancelled.
        """
        sleep_task: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(sleep_s))
        wakeup_task: asyncio.Task[None] = asyncio.create_task(_wait_for_wakeup(wakeup_event))
        tasks: set[asyncio.Task[None]] = {sleep_task, wakeup_task}
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if wakeup_task in done:
                logger.info("Walutomat: backoff sleep broken early by wakeup event")
        finally:
            await self._cancel_unfinished_backoff_tasks(tasks)

    @staticmethod
    async def _cancel_unfinished_backoff_tasks(tasks: set[asyncio.Task[None]]) -> None:
        """Cancel pending backoff helper tasks.

        Args:
            tasks: Backoff sleep and wakeup tasks.

        Returns:
            None.
        """
        for task in tasks:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    def _reset_polling_error_state(self) -> None:
        """Clear HTTP error counters after a successful poll.

        Returns:
            None.
        """
        if self._consecutive_error_count == 0 and self._backoff_attempts == 0:
            return
        logger.info(f"Walutomat recovered after {self._backoff_attempts} backoff attempts")
        self._consecutive_error_count = 0
        self._backoff_attempts = 0
        self._backoff_until = 0.0

    async def _build_candle_for_symbol(
        self,
        symbol: str,
        ticks: list[tuple[float, float]],
        prev_minute_start: int,
        current_minute: int,
    ) -> None:
        """Build and emit a 1-minute candle for a single symbol.

        ``trades`` is zero, not the tick count: the buffer holds polls of the
        top-of-book quote, and no trade is observed on this feed. Counting
        polls would publish a trade count the venue never reported.

        Args:
            symbol: Native symbol string.
            ticks: List of (timestamp, price) tick tuples.
            prev_minute_start: Start timestamp of the previous minute.
            current_minute: Start timestamp of the current minute.
        """
        minute_ticks = [
            (ts, price) for ts, price in ticks if prev_minute_start <= ts < current_minute
        ]
        if minute_ticks:
            prices = [price for _, price in minute_ticks]
            candle = CandleUpdate(
                symbol=symbol,
                open=prices[0],
                high=max(prices),
                low=min(prices),
                close=prices[-1],
                volume=0.0,
                vwap=sum(prices) / len(prices),
                trades=0,
                interval_begin=datetime.fromtimestamp(prev_minute_start, UTC),
                interval=1,
            )
            await self._candle_queue.put(candle)
            logger.debug(
                f"{symbol} 1m candle: O={candle.open:.4f} H={candle.high:.4f} "
                f"L={candle.low:.4f} C={candle.close:.4f} ({len(prices)} ticks)"
            )
        self._tick_buffers[symbol] = [(ts, price) for ts, price in ticks if ts >= current_minute]

    async def _candle_builder_loop(self) -> None:
        """Build 1-minute candles from accumulated tick data."""
        logger.info("Starting Walutomat candle builder (1m interval)")
        while self._running:
            now = time.time()
            seconds_until_next_minute = 60 - (now % 60)
            await asyncio.sleep(seconds_until_next_minute)
            if not self._running:
                break
            current_minute = int(time.time() // 60) * 60
            prev_minute_start = current_minute - 60
            for symbol in tuple(self._tick_buffers):
                ticks = self._tick_buffers.get(symbol, [])
                if not ticks:
                    continue
                await self._build_candle_for_symbol(
                    symbol, ticks, prev_minute_start, current_minute
                )
        logger.info("Walutomat candle builder stopped")

    async def get_ticker(self, symbol: str) -> TickerSnapshot:
        """Fetch current ticker for a symbol.

        Args:
            symbol: Trading pair in native format (e.g., 'EUR-PLN').

        Returns:
            Current ticker snapshot.

        Raises:
            RuntimeError: If not connected.
            ValueError: If symbol not found, or if the symbol's book carries
                no usable two-sided quote (a distinct message - a snapshot is
                never returned with a fabricated price).
        """
        self._require_connected()
        data = await self._fetch_market_data()
        wal_symbol = native_to_walutomat_ws(symbol)
        if wal_symbol not in data:
            available = ", ".join(data.keys())
            raise ValueError(f"Symbol {symbol} not found. Available: {available}")
        pair_data = data[wal_symbol]
        quote = walutomat_quote(pair_data.best_offers)
        if quote is None:
            raise ValueError(f"Symbol {symbol} has no usable two-sided quote")
        return TickerSnapshot(
            symbol=symbol,
            bid=quote.bid,
            ask=quote.ask,
            last=quote.mark,
            timestamp=time.time(),
        )

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since: int | None = None,
        limit: int | None = None,
    ) -> list[OhlcvSnapshot]:
        """Get OHLCV data (not supported by Walutomat).

        Args:
            symbol: Trading pair (ignored).
            timeframe: Candle interval (ignored).
            since: Start timestamp (ignored).
            limit: Maximum candles (ignored).

        Returns:
            Empty list - Walutomat does not provide OHLCV data.
        """
        logger.warning("Walutomat does not provide OhlcvSnapshot data - returning empty list")
        return []

    def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Subscribe to ticker updates via HTTP polling.

        Args:
            symbols: List of symbols or ['*'] for all.

        Returns:
            AsyncIterator yielding TickerUpdate for each price change.

        Raises:
            RuntimeError: If not connected.
        """
        return self._subscribe_ticks_impl(symbols)

    async def _subscribe_ticks_impl(self, symbols: list[str]) -> AsyncIterator[TickerUpdate]:
        """Implement ticker polling subscription.

        Args:
            symbols: List of symbols or ['*'] for all.

        Yields:
            TickerUpdate for each price change.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        if symbols == ["*"]:
            symbols = self.get_supported_pairs()
            logger.info(f"Wildcard subscription - monitoring {len(symbols)} pairs")
        if not self._polling_task or self._polling_task.done():
            self._polling_task = asyncio.create_task(self._polling_loop(symbols))
        while self._running:
            try:
                ticker = await asyncio.wait_for(self._tick_queue.get(), timeout=1.0)
                yield ticker
            except TimeoutError:
                continue

    def subscribe_candles(
        self,
        symbols: list[str],
        timeframe: str = "1m",
    ) -> AsyncIterator[CandleUpdate]:
        """Subscribe to 1-minute candles built from tick data.

        Args:
            symbols: List of symbols or ['*'] for all.
            timeframe: Must be '1m' (only supported interval).

        Returns:
            AsyncIterator yielding CandleUpdate for each completed candle.

        Raises:
            RuntimeError: If not connected.
            NotImplementedError: If timeframe is not '1m'.
        """
        return self._subscribe_candles_impl(symbols, timeframe=timeframe)

    async def _subscribe_candles_impl(
        self,
        symbols: list[str],
        *,
        timeframe: str,
    ) -> AsyncIterator[CandleUpdate]:
        """Implement candle subscription based on tick polling.

        Args:
            symbols: List of symbols or ['*'] for all.
            timeframe: Candle interval (only '1m' supported).

        Yields:
            CandleUpdate for each completed candle.

        Raises:
            RuntimeError: If not connected.
            NotImplementedError: If timeframe is not '1m'.
        """
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        if timeframe != "1m":
            raise NotImplementedError(f"Walutomat only supports 1m candles, not {timeframe}")
        if symbols == ["*"]:
            symbols = self.get_supported_pairs()
            logger.info(f"Wildcard candle subscription - monitoring {len(symbols)} pairs")
        if not self._polling_task or self._polling_task.done():
            self._polling_task = asyncio.create_task(self._polling_loop(symbols))
        if not self._candle_builder_task or self._candle_builder_task.done():
            self._candle_builder_task = asyncio.create_task(self._candle_builder_loop())
        while self._running:
            try:
                candle = await asyncio.wait_for(self._candle_queue.get(), timeout=1.0)
                if candle.symbol in symbols:
                    yield candle
            except TimeoutError:
                continue

    def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[TradeUpdate]:
        """Not implemented - Walutomat does not provide public trade feed.

        Args:
            symbols: List of symbols (unused).

        Yields:
            Never yields; raises before producing any value.

        Raises:
            NotImplementedError: Always raised.
        """
        _ = symbols
        return _RaisingAsyncIterator[TradeUpdate](
            NotImplementedError("Walutomat does not provide public trade feed")
        )

    def subscribe_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Subscribe to execution updates via polling.

        Polls active orders at ``_execution_poll_interval`` and yields
        ``ExecutionUpdate`` items when fill quantities increase or orders
        disappear from the active-orders endpoint.

        The first poll seeds tracking state without yielding to avoid
        bogus fills for orders already tracked by executor recovery.

        Yields:
            ExecutionUpdate for each detected fill delta or terminal event.

        Raises:
            RuntimeError: If not connected or missing credentials.
        """
        return self._poll_executions()

    async def _poll_executions(self) -> AsyncIterator[ExecutionUpdate]:
        """Poll active orders and yield fill updates.

        First poll seeds tracking state without yielding (avoids bogus fills
        for orders already tracked by executor recovery). Subsequent polls
        detect fill deltas and order disappearances.

        Yields:
            ExecutionUpdate for each detected fill or disappearance.
        """
        self._require_authenticated()

        tracked: dict[str, _TrackedOrder] = {}
        first_poll = True

        while self._running:
            try:
                orders = await self.get_orders()
                current_ids, updates = self._collect_active_execution_updates(
                    orders,
                    tracked,
                    first_poll,
                )
                for update in updates:
                    yield update

                if not first_poll:
                    async for event in self._emit_disappeared_execution_updates(
                        tracked,
                        current_ids,
                    ):
                        yield event

                first_poll = False

            except httpx.HTTPError as exc:
                first_poll = False
                logger.warning(
                    "Walutomat execution poll transient HTTP error — will retry on next cycle: {}",
                    exc,
                )
            except Exception:
                first_poll = False
                logger.exception("Walutomat execution poll failed")

            await self._wait_for_execution_poll(tracked)

    def _collect_active_execution_updates(
        self,
        orders: list[ExchangeOrderSnapshot],
        tracked: dict[str, _TrackedOrder],
        first_poll: bool,
    ) -> tuple[set[str], list[ExecutionUpdate]]:
        """Update tracked active orders and return any new fill events."""
        current_ids: set[str] = set()
        updates: list[ExecutionUpdate] = []
        for order in orders:
            current_ids.add(order.id)
            previous = tracked.get(order.id)
            tracked[order.id] = _snapshot_tracked_order(order)
            if first_poll or not _should_emit_active_execution(order, previous):
                continue
            updates.append(self._build_active_execution_update(order, previous))
        return current_ids, updates

    def _build_active_execution_update(
        self, order: ExchangeOrderSnapshot, previous: _TrackedOrder | None
    ) -> ExecutionUpdate:
        """Build an execution update for a fill detected on an active order.

        ``exec_id`` is DETERMINISTIC — ``f(orderId, cumulative)`` in basis
        units (#145 P2-5) — so a redelivered or replayed poll delta dedupes
        by identity everywhere downstream instead of leaning on the fragile
        ``(cid, size, price)`` fallback tuple. The order's commission is
        passed as the CUMULATIVE ``cum_fee`` (Walutomat snapshots carry the
        order's running commission, not per-fill fees); the executor's fee
        watermark turns it into per-emission deltas — attaching the full
        snapshot fee to every delta used to multi-charge partial fills.

        The execution price is the EFFECTIVE price derived from the venue's
        two-sided cumulatives (``last_price`` per delta, ``average_price``
        cumulative VWAP) — Walutomat permits price improvement, so the limit
        price is only a bound, never the fill economics. A snapshot without
        counter data degrades to the legacy limit-price shape with a warning;
        an absorbed publish gap is priced at the latest delta's effective price
        (exact repair would need a committed counter-cumulative anchor —
        deferred, documented).
        """
        last_qty, last_price, average_price, counter_amount_decimal = _effective_price_fields(
            order, previous
        )
        if average_price is None:
            logger.warning(
                f"Walutomat order {order.id}: no counter cumulative on snapshot — "
                f"falling back to limit price for the execution economics"
            )
            average_price = order.price
        return ExecutionUpdate(
            order_id=order.id,
            exec_type="trade",
            symbol=order.symbol,
            side=order.side,
            order_type=order.type,
            order_status=_active_execution_status(order),
            timestamp=datetime.now(UTC),
            cum_qty=order.filled,
            cum_qty_decimal=order.filled_decimal,
            cum_cost=order.counter_filled,
            exec_id=_walutomat_exec_id(order.id, order.filled),
            cl_ord_id=order.client_order_id or "",
            order_qty=order.amount,
            limit_price=order.price,
            last_qty=last_qty,
            last_price=last_price,
            average_price=average_price,
            counter_amount_decimal=counter_amount_decimal,
            cum_fee=order.fee if order.fee and order.fee_currency else None,
            cum_fee_decimal=order.fee_decimal if order.fee and order.fee_currency else None,
            cum_fee_currency=order.fee_currency if order.fee and order.fee_currency else None,
        )

    async def _emit_disappeared_execution_updates(
        self,
        tracked: dict[str, _TrackedOrder],
        current_ids: set[str],
    ) -> AsyncIterator[ExecutionUpdate]:
        """Yield terminal events for orders missing from the active-order response."""
        disappeared = set(tracked) - current_ids
        for order_id in disappeared:
            tracked_order = tracked[order_id]
            resolved_terminal = False
            async for event in self._resolve_disappeared(order_id, tracked_order):
                resolved_terminal = True
                yield event
            if resolved_terminal:
                tracked.pop(order_id, None)

    async def _wait_for_execution_poll(self, tracked: dict[str, _TrackedOrder]) -> None:
        """Wait until the next active or idle execution poll cycle."""
        if tracked:
            await asyncio.sleep(self._execution_poll_interval)
            return
        self._execution_wake.clear()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                self._execution_wake.wait(),
                timeout=self._execution_idle_interval,
            )

    async def _resolve_disappeared(
        self,
        oid: str,
        tracked: _TrackedOrder,
    ) -> AsyncIterator[ExecutionUpdate]:
        """Resolve terminal state for an order that disappeared from active list.

        Queries the findOrders endpoint for actual final state. Emits a fill
        event before the cancel event when there is an unreported fill delta
        (prevents fill loss when executor short-circuits canceled events).

        A failed final-state query NEVER guesses (#145 P2-5): the old
        fallback fabricated filled-vs-canceled from ``math.isclose`` —
        a wrong FILLED creates phantom position, a wrong CANCELED frees
        engine intent while the order may have filled. Instead the order
        stays tracked and the query retries every poll cycle; after
        ``_DISAPPEARED_RETRY_MAX`` consecutive failures it escalates to a
        single CRITICAL log and keeps retrying. The executor's own 60s
        recon converges the pending entry independently through
        ``get_order``.

        Args:
            oid: Exchange order ID that disappeared.
            tracked: Last-known tracked state from polling.

        Yields:
            One or two ExecutionUpdate events depending on final state.
        """
        try:
            final = await self.get_order(oid)
            if final.status == ExchangeOrderStatusEnum.OPEN:
                logger.warning(
                    "Order {} disappeared from active list but API reports OPEN, "
                    "possible transient omission — skipping terminal event",
                    oid,
                )
                return
            fields = _terminal_execution_fields(final, tracked)
            if final.status == ExchangeOrderStatusEnum.CLOSED:
                yield _disappeared_fill_update(
                    final,
                    tracked,
                    ExchangeOrderStatusEnum.FILLED,
                    fields,
                )
            elif final.status == ExchangeOrderStatusEnum.CANCELED:
                if final.filled > tracked.filled:
                    yield _disappeared_fill_update(
                        final,
                        tracked,
                        ExchangeOrderStatusEnum.PARTIALLY_FILLED,
                        fields,
                    )
                yield _disappeared_cancel_update(final, tracked)
            self._disappeared_retry_counts.pop(oid, None)
        except Exception as exc:
            self._record_disappeared_failure(oid, exc)

    def _record_disappeared_failure(self, oid: str, exc: Exception) -> None:
        """Record a failed terminal query without guessing order state."""
        count = self._disappeared_retry_counts.get(oid, 0) + 1
        self._disappeared_retry_counts[oid] = count
        if count == _DISAPPEARED_RETRY_MAX:
            logger.critical(
                "Walutomat order {} disappeared {} polls ago and the final-state "
                "query keeps failing ({}) — NOT guessing a terminal state; the "
                "order stays tracked and the query retries every poll until venue "
                "truth is available (operator attention required)",
                oid,
                count,
                exc,
            )
            return
        logger.warning(
            "Failed to query final state for disappeared order {} "
            "(attempt {}): {} — retrying next poll, never guessing "
            "filled-vs-canceled",
            oid,
            count,
            exc,
        )

    def subscribe_instruments(self, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to instrument/pair information.

        Args:
            **kwargs: Ignored parameters.

        Yields:
            Dictionary with instrument details for each pair.

        Raises:
            RuntimeError: If not connected.
        """
        _ = kwargs
        return self._subscribe_instruments_impl()

    async def _subscribe_instruments_impl(self) -> AsyncIterator[dict[str, Any]]:
        if not self._running:
            raise RuntimeError(_NOT_CONNECTED_MSG)
        try:
            data = await self._fetch_market_data()
            for wal_symbol in data:
                parts = wal_symbol.split("_")
                if len(parts) != 2:
                    logger.warning(f"Unexpected symbol format: {wal_symbol}")
                    continue
                base, quote = parts
                yield {
                    "symbol": wal_symbol,
                    "walutomat_rest_symbol": f"{base}{quote}",
                    "native_symbol": f"{base}-{quote}",
                    "base": base,
                    "quote": quote,
                }
            logger.info(f"Yielded {len(data)} instrument pairs")
        except Exception as e:
            logger.error(f"Error fetching instruments: {e}")
            raise

    async def create_order(self, request: ExchangeOrderRequest) -> ExchangeOrderSnapshot:
        """Create a new FX order on Walutomat.

        The venue's ``submitId`` is ``request.client_order_id``
        verbatim, never a substitute. It is echoed back as the
        snapshot's ``client_order_id`` and persisted from there, and it
        is the value ``_verify_ambiguous_submit`` and the cross-restart
        dispatched sweep both look the order up by. A fabricated
        fallback would leave the venue holding the order under an id no
        recovery path can query. ``ExchangeOrderRequest`` guarantees the
        id is present and non-empty, so no guard is needed here.

        Transport failures are split by safety class:
        connection-setup errors (connection refused, connect timeout, no
        pool slot, SOCKS handshake) provably happened BEFORE the request
        left the process and re-raise plain — safe to reject. Anything
        after send (read/write timeout, reset, protocol error, gateway
        5xx, unparseable success body) is wrapped in
        ``AmbiguousOrderSubmitError`` — Walutomat may have accepted the
        order under this ``submitId``. A 4xx and an explicit
        ``success=false`` body are authoritative venue answers and stay
        definitive rejections.

        Args:
            request: Order parameters.

        Returns:
            Created order snapshot.

        Raises:
            RuntimeError: If not connected or not authenticated, or the
                venue answered ``success=false``.
            ValueError: If the request is stop-typed — Walutomat's FX
                market API has no stop orders. Raised BEFORE any network
                send, so the failure is provably-not-placed and the
                executor's definitive-reject branch (publish REJECTED)
                is the honest disposition, never an ambiguous park
                (#156).
            AmbiguousOrderSubmitError: If the call failed in a way where
                the order MAY exist on the venue.
        """
        if request.type in (
            ExchangeOrderTypeEnum.STOP_LOSS,
            ExchangeOrderTypeEnum.STOP_LOSS_LIMIT,
        ):
            raise ValueError("Walutomat does not support stop orders")
        client = self._require_authenticated()
        walutomat_rest_symbol = native_to_walutomat_rest(request.symbol)
        base_currency = request.symbol.split("-")[0]
        submit_id = request.client_order_id
        body_params = {
            "currencyPair": walutomat_rest_symbol,
            "buySell": request.side.value.upper(),
            "volume": f"{request.amount:.2f}",
            "volumeCurrency": base_currency,
            "dryRun": "false",
            "submitId": submit_id,
        }
        if request.price:
            body_params["limitPrice"] = f"{request.price:.4f}"
        body = urlencode(body_params)
        endpoint = "/api/v2.0.0/market_fx/orders"
        headers = self._get_auth_headers(endpoint, body)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        url = f"{self.api_base_url}/market_fx/orders"
        await self._acquire_rest_slot()
        try:
            response = await client.post(url, content=body, headers=headers)
        except httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout, httpx.ProxyError:
            raise
        except httpx.TransportError as e:
            raise AmbiguousOrderSubmitError(
                client_order_id=submit_id,
                instrument=request.symbol,
                message=f"Walutomat create_order transport failure (order may exist): {e}",
            ) from e
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code >= 500:
                raise AmbiguousOrderSubmitError(
                    client_order_id=submit_id,
                    instrument=request.symbol,
                    message=f"Walutomat create_order gateway error {e.response.status_code} "
                    f"(order may exist): {e}",
                ) from e
            raise
        try:
            result = response.json()
        except ValueError as e:
            raise AmbiguousOrderSubmitError(
                client_order_id=submit_id,
                instrument=request.symbol,
                message=f"Walutomat create_order returned unparseable body (order may exist): {e}",
            ) from e
        if not result.get("success"):
            raise RuntimeError(f"ExchangeOrderSnapshot creation failed: {result}")
        try:
            order_id = result["result"]["orderId"]
        except (KeyError, TypeError) as e:
            raise AmbiguousOrderSubmitError(
                client_order_id=submit_id,
                instrument=request.symbol,
                message=f"Walutomat accepted the submit but the response lacks orderId "
                f"(order may exist): {result}",
            ) from e
        if not isinstance(order_id, str) or not order_id:
            raise AmbiguousOrderSubmitError(
                client_order_id=submit_id,
                instrument=request.symbol,
                message=(
                    f"Walutomat accepted the submit but returned an unusable orderId "
                    f"{order_id!r} (order may exist): a falsy id would be misread as a "
                    f"definitive rejection downstream"
                ),
            )
        logger.info(
            f"Created order {order_id}: {request.side.value} {request.amount} {request.symbol}"
        )
        order = ExchangeOrderSnapshot(
            id=order_id,
            client_order_id=submit_id,
            symbol=request.symbol,
            side=request.side,
            type=request.type,
            amount=request.amount,
            price=request.price,
            status=ExchangeOrderStatusEnum.PENDING,
            filled=0.0,
            remaining=request.amount,
            timestamp=time.time(),
        )
        db_result = await self._log_order_to_db(request, order)
        if db_result is not None:
            order.db_order_id = db_result[0]
            order.db_order_public_id = db_result[1]
        self._execution_wake.set()
        return order

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Withdraw an order from the market.

        Uses the v2 close endpoint which returns full order details
        including final fill amounts and commission.

        Args:
            order_id: Order ID to cancel.
            symbol: Trading pair (unused, kept for interface compatibility).

        Returns:
            Closed order snapshot with accurate final state.

        Raises:
            RuntimeError: If not connected or not authenticated, or cancel fails.
        """
        _ = symbol
        client = self._require_authenticated()
        endpoint = "/api/v2.0.0/market_fx/orders/close"
        body = f"orderId={order_id}"
        headers = self._get_auth_headers(endpoint, body)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        url = f"{self.api_base_url}/market_fx/orders/close"
        await self._acquire_rest_slot()
        response = await client.post(url, content=body, headers=headers)
        response.raise_for_status()
        result = response.json()
        if not result.get("success"):
            raise RuntimeError(f"Order cancellation failed: {result}")
        logger.info("Cancelled order {}", order_id)
        return self._parse_walutomat_order(result["result"])

    async def get_order(self, order_id: str, symbol: str | None = None) -> ExchangeOrderSnapshot:
        """Get order details by ID, including completed and canceled orders.

        Uses the findOrders endpoint which returns orders in any state,
        not just active ones.

        Args:
            order_id: Order ID to fetch.
            symbol: Trading pair (unused, kept for interface compatibility).

        Returns:
            Order snapshot.

        Raises:
            RuntimeError: If not connected or not authenticated.
            ValueError: If order not found.
        """
        _ = symbol
        client = self._require_authenticated()
        endpoint = f"/api/v2.0.0/market_fx/orders?orderId={order_id}"
        headers = self._get_auth_headers(endpoint, "")
        url = f"{self.api_base_url}/market_fx/orders?orderId={order_id}"
        await self._acquire_rest_slot()
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if not result.get("success") or not result.get("result"):
            raise ValueError(f"Order {order_id} not found")
        return self._parse_walutomat_order(result["result"][0])

    async def read_order_fill_legs(self, order_id: str) -> VenueOrderFillLegs | None:
        """Read one order's cumulative per-leg fill totals (S4c-3 witness join).

        Mirrors :meth:`get_order`'s authenticated ``GET market_fx/orders`` call
        but returns the exact per-leg cumulative totals the witness builder folds
        against the account-history legs, instead of the collapsed single-side
        fill an order snapshot keeps. Every amount is parsed as exact ``Decimal``
        from the venue's own strings. Returns ``None`` when the venue does not
        know the order id; raises on transport or a non-success envelope so the
        observer degrades to no-bootstrap without losing the balance snapshot.

        Args:
            order_id: Exchange order id whose cumulative fill legs are read.

        Returns:
            The order's cumulative bought/sold/commission legs, or ``None`` when
            the venue does not know the order id.

        Raises:
            RuntimeError: If not connected, not authenticated, or the venue
                returns a non-success envelope.
        """
        client = self._require_authenticated()
        endpoint = f"/api/v2.0.0/market_fx/orders?orderId={order_id}"
        headers = self._get_auth_headers(endpoint, "")
        url = f"{self.api_base_url}/market_fx/orders?orderId={order_id}"
        await self._acquire_rest_slot()
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if result.get("success") is not True:
            raise RuntimeError("Walutomat order fill legs envelope did not report success")
        rows = result.get("result")
        if not rows:
            return None
        return self._parse_walutomat_order_fill_legs(rows[0])

    @staticmethod
    def _parse_walutomat_order_fill_legs(order_data: dict[str, Any]) -> VenueOrderFillLegs:
        """Parse a Walutomat order payload into its cumulative per-leg fill totals.

        Uses the venue's direct ``boughtCurrency`` / ``soldCurrency`` fields (no
        pair inference) and parses every amount as exact ``Decimal``.
        ``boughtAmount`` is the gross (pre-commission) cumulative the witness
        builder folds against the account-history MARKET_FX legs; ``buy_sell``
        tells the builder which cumulative denominates the executions' base volume.

        Args:
            order_data: Raw order object from the market_fx/orders response.

        Returns:
            The order's cumulative bought/sold/commission legs.

        Raises:
            ValueError: If a required amount field is missing or malformed.
        """
        return VenueOrderFillLegs(
            order_id=str(order_data["orderId"]),
            bought_amount=_parse_walutomat_decimal(order_data["boughtAmount"]),
            sold_amount=_parse_walutomat_decimal(order_data["soldAmount"]),
            commission_amount=_parse_walutomat_decimal(order_data.get("commissionAmount") or "0"),
            bought_currency=str(order_data["boughtCurrency"]),
            sold_currency=str(order_data["soldCurrency"]),
            commission_currency=str(order_data.get("commissionCurrency") or ""),
            buy_sell=str(order_data["buySell"]),
        )

    def parse_execution_exec_id(self, exec_id: str) -> tuple[str, int, bool] | None:
        """Decode a Walutomat exec id into its witness components (see base).

        Accepts BOTH the streamed ``wal-{orderId}-c{basis_units}[-t]`` scheme and the
        executor's legacy corrective ``recon-{orderId}-c{cumulative!r}`` scheme; both
        decode to ``(order_id, cumulative_basis_units, is_terminal)`` with identical
        basis units for the same fill, so a corrective-booked instant fill witnesses
        exactly as a streamed fill would. A Walutomat marketable-limit that fills
        inside one poll interval never surfaces on the active-orders endpoint, so the
        adapter emits no ``wal-`` fill and the executor's reconciliation backstop books
        the sole execution with a ``recon-`` id; without this dual decode that frozen,
        append-only row would leave the scope permanently un-anchorable.

        Args:
            exec_id: The stored ``Execution.exec_id``.

        Returns:
            ``(order_id, cumulative_basis_units, is_terminal)`` or ``None`` when the id
            matches neither Walutomat scheme.
        """
        streamed = _parse_walutomat_exec_id(exec_id)
        if streamed is not None:
            return streamed
        return _parse_walutomat_recon_exec_id(exec_id)

    async def find_order_by_client_id(
        self, client_order_id: str, symbol: str | None = None
    ) -> ExchangeOrderSnapshot | None:
        """Verify an ambiguous submit from active orders or positive fill history.

        Walutomat echoes the submit ``submitId`` as ``client_order_id``
        on active order details, so an active hit is authoritative:
        the order exists and can be adopted as ACCEPTED. A fast-filled
        order leaves that endpoint, but ``account/history`` positively
        proves it when two opposite-signed ``MARKET_FX`` currency legs
        carry the submit id and order id. History absence proves
        nothing: an accepted-then-cancelled zero-fill order has no fill
        legs, so every miss still raises and parks UNKNOWN.

        Args:
            client_order_id: Submit id sent with the original order.
            symbol: Optional native symbol to narrow the active-order
                scan.

        Returns:
            A matching active-order snapshot or a positive history
            existence witness synthesized from observed fills.

        Raises:
            NotImplementedError: When the active set contains no match
                and absence cannot be proven authoritatively.
            Exception: If the active-order query fails.
        """
        orders = await self.get_orders(symbol=symbol)
        for order in orders:
            if order.client_order_id == client_order_id:
                return order
        if symbol is not None:
            history_tip = await self.read_account_history_tip(200)
            if history_tip is not None:
                filled_order = _filled_order_from_history(
                    history_tip.items, client_order_id, symbol
                )
                if filled_order is not None:
                    return filled_order
        raise NotImplementedError("Walutomat cannot authoritatively verify absence by submitId")

    @staticmethod
    def _parse_walutomat_order(order_data: dict[str, Any]) -> ExchangeOrderSnapshot:
        """Parse a single Walutomat order response into an ExchangeOrderSnapshot.

        ``filled`` stays the side-specific GROSS base cumulative (the witness
        identity — never normalized); the OPPOSITE cumulative is carried as
        ``counter_filled`` (a BUY's ``soldAmount``, a SELL's ``boughtAmount``),
        parsed through the hardened exact-decimal path, so effective execution
        prices under price improvement derive from the venue's own two-sided
        truth instead of the limit price.

        Args:
            order_data: Raw order data dictionary from Walutomat API.

        Returns:
            Parsed ExchangeOrderSnapshot with side-aware fill, counter-amount
            cumulative, status mapping, and commission data.
        """
        is_buy = order_data["buySell"] == "BUY"
        fill_field = "boughtAmount" if is_buy else "soldAmount"
        counter_field = "soldAmount" if is_buy else "boughtAmount"
        filled = float(order_data.get(fill_field, 0))
        volume = float(order_data["volume"])
        raw_counter = order_data.get(counter_field)
        counter_decimal = (
            str(_parse_walutomat_decimal(raw_counter)) if isinstance(raw_counter, str) else None
        )

        if order_data["status"] == "ACTIVE":
            status = ExchangeOrderStatusEnum.OPEN
        elif order_data.get("completion", 0) == 100:
            status = ExchangeOrderStatusEnum.CLOSED
        else:
            status = ExchangeOrderStatusEnum.CANCELED

        commission_str = order_data.get("commissionAmount", "0")
        commission = float(commission_str)
        raw_filled = order_data.get(fill_field, 0)
        raw_volume = order_data["volume"]
        raw_price = order_data["limitPrice"]

        return ExchangeOrderSnapshot(
            id=order_data["orderId"],
            client_order_id=order_data.get("submitId"),
            symbol=walutomat_rest_to_native(order_data["currencyPair"]),
            side=OrderSideEnum.BUY if is_buy else OrderSideEnum.SELL,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=volume,
            price=float(order_data["limitPrice"]),
            status=status,
            filled=filled,
            remaining=max(volume - filled, 0.0),
            timestamp=time.time(),
            fee=commission if commission > 0 else None,
            fee_currency=order_data.get("commissionCurrency"),
            amount_decimal=raw_volume if isinstance(raw_volume, str) else None,
            price_decimal=raw_price if isinstance(raw_price, str) else None,
            filled_decimal=raw_filled if isinstance(raw_filled, str) else None,
            fee_decimal=commission_str if isinstance(commission_str, str) else None,
            counter_filled=float(counter_decimal) if counter_decimal is not None else None,
            counter_filled_decimal=counter_decimal,
        )

    async def get_orders(
        self,
        symbol: str | None = None,
        status: ExchangeOrderStatusEnum | None = None,
        limit: int | None = None,
    ) -> list[ExchangeOrderSnapshot]:
        """Get list of active orders.

        Args:
            symbol: Filter by trading pair.
            status: Filter by status.
            limit: Maximum orders to return.

        Returns:
            List of order snapshots.

        Raises:
            RuntimeError: If not connected or not authenticated.
        """
        client = self._require_authenticated()
        endpoint = "/api/v2.0.0/market_fx/orders/active"
        headers = self._get_auth_headers(endpoint, "")
        url = f"{self.api_base_url}/market_fx/orders/active"
        await self._acquire_rest_slot()
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if not result.get("success"):
            raise RuntimeError(f"Failed to fetch orders: {result}")
        orders = []
        for order_data in result["result"]:
            order = self._parse_walutomat_order(order_data)
            if symbol and order.symbol != symbol:
                continue
            if status and order.status != status:
                continue
            orders.append(order)
            if limit and len(orders) >= limit:
                break
        return orders

    async def get_balance(self, currency: str | None = None) -> dict[str, AccountBalance]:
        """Get account balances.

        Args:
            currency: Filter by specific currency.

        Returns:
            Dictionary of currency to balance info.

        Raises:
            RuntimeError: If not connected or not authenticated.
        """
        client = self._require_connected()
        if not self._api_key:
            raise RuntimeError("Trading requires authentication - provide api_key")
        endpoint = "/api/v2.0.0/account/balances"
        headers = self._get_auth_headers(endpoint, "")
        url = f"{self.api_base_url}/account/balances"
        await self._acquire_rest_slot()
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if not result.get("success"):
            raise RuntimeError(f"Failed to fetch balances: {result}")
        balances = {}
        for balance_data in result["result"]:
            curr = balance_data["currency"]
            if currency and curr != currency:
                continue
            balance = AccountBalance(
                currency=curr,
                free=float(balance_data.get("balanceAvailable", 0)),
                used=float(balance_data.get("balanceReserved", 0)),
                total=float(balance_data.get("balanceTotal", 0)),
            )
            balances[curr] = balance
        return balances

    async def read_native_balances(self) -> list[NativeBalanceEntry]:
        """Read faithful native per-currency FX cash balances (PnL Phase 3).

        Mirrors :meth:`get_balance`'s authenticated ``GET /account/balances``
        call but returns strict native entries for the account observer: it
        RAISES on a non-success envelope and on any row whose currency or
        amount is missing or non-finite, never coercing an absent amount to
        zero. Walutomat always reports a faithful available/reserved/total
        split, so ``free``/``used`` are always populated. An empty balances
        list yields ``[]``.

        Returns:
            Faithful native per-currency balance entries.

        Raises:
            RuntimeError: If not connected, missing API credentials, or the
                venue returns a non-success envelope.
            ValueError: If a balance row is missing its currency or an amount,
                or reports a non-finite amount.
        """
        client = self._require_connected()
        if not self._api_key:
            raise RuntimeError("Trading requires authentication - provide api_key")
        endpoint = "/api/v2.0.0/account/balances"
        headers = self._get_auth_headers(endpoint, "")
        url = f"{self.api_base_url}/account/balances"
        await self._acquire_rest_slot()
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if result.get("success") is not True:
            raise RuntimeError("Walutomat balances envelope did not report success")
        rows = result.get("result")
        if not isinstance(rows, list):
            raise ValueError("Walutomat balances result is not a list")
        entries: list[NativeBalanceEntry] = []
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("Walutomat balance row is not a dict")
            entries.append(_parse_native_balance(row))
        return entries

    async def read_account_history_tip(self, limit: int) -> VenueAccountHistoryTip | None:
        """Read the newest ``account/history`` page for the spot-anchor bootstrap.

        One authenticated, signed ``GET /account/history`` (this endpoint requires
        ``X-API-Signature``, which :meth:`_get_auth_headers` supplies) with
        ``itemLimit`` and the venue's default DESC order, so the newest item's
        ``historyItemId`` is the tip ``H0``. The page is sorted descending here so
        the bootstrap's balance-chain fold sees each currency's most-recent row
        first regardless of venue order. Returns the tip and its page, or ``None``
        when the account has no history at all (no tip to seal). Raises on
        transport or a non-success envelope so the observer degrades to
        no-bootstrap without losing the balance snapshot.

        Args:
            limit: Maximum number of newest history items to page (``itemLimit``).

        Returns:
            The newest tip and its descending page, or ``None`` when the account
            history is empty.

        Raises:
            RuntimeError: If not connected, not authenticated, or the venue
                returns a non-success envelope.
            ValueError: If the result payload is not a list or a row is malformed.
        """
        client = self._require_authenticated()
        endpoint = f"/api/v2.0.0/account/history?itemLimit={limit}"
        headers = self._get_auth_headers(endpoint, "")
        url = f"{self.api_base_url}/account/history?itemLimit={limit}"
        await self._acquire_rest_slot()
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        result = response.json()
        if result.get("success") is not True:
            raise RuntimeError("Walutomat account history envelope did not report success")
        rows = result.get("result")
        if not isinstance(rows, list):
            raise ValueError("Walutomat account history result is not a list")
        if not rows:
            return None
        parsed = [_parse_walutomat_history_item(row) for row in rows]
        items = tuple(sorted(parsed, key=lambda item: item.item_id, reverse=True))
        return VenueAccountHistoryTip(
            item_id=items[0].item_id, items=items, reached_genesis=len(rows) < limit
        )

    async def read_account_history_range(
        self, continue_from: int, upto_item_id: int, item_limit: int = 200, max_pages: int = 25
    ) -> tuple[VenueAccountHistoryItem, ...] | None:
        """Page the history range ``(continue_from, upto_item_id]`` ascending.

        Explicit ``sortOrder=ASC`` (the venue default is DESC) with the
        EXCLUSIVE ``continueFrom`` cursor, one signed GET per page; strictly
        increasing item ids are enforced across pages (a regression is venue
        corruption and returns ``None``). Rows above ``upto_item_id`` end the
        walk and are excluded; a short page ends it at the history tip. The
        certificate's validator owns the reached-the-bound refusal.

        Args:
            continue_from: Exclusive lower cursor.
            upto_item_id: Inclusive upper bound.
            item_limit: Per-page size (venue max 200).
            max_pages: Hard page cap bounding the walk.

        Returns:
            The ordered ascending in-range rows, or ``None`` on an unfaithful
            read (transport failure mid-walk, id regression).

        Raises:
            RuntimeError: If not connected, not authenticated, or the venue
                returns a non-success envelope on the first page.
            ValueError: If a result payload is not a list or a row is malformed.
        """
        client = self._require_authenticated()
        rows: list[VenueAccountHistoryItem] = []
        cursor = continue_from
        for _page in range(max_pages):
            query = f"continueFrom={cursor}&itemLimit={item_limit}&sortOrder=ASC"
            endpoint = f"/api/v2.0.0/account/history?{query}"
            headers = self._get_auth_headers(endpoint, "")
            url = f"{self.api_base_url}/account/history?{query}"
            await self._acquire_rest_slot()
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            result = response.json()
            if result.get("success") is not True:
                raise RuntimeError("Walutomat account history envelope did not report success")
            payload = result.get("result")
            if not isinstance(payload, list):
                raise ValueError("Walutomat account history result is not a list")
            page = [_parse_walutomat_history_item(row) for row in payload]
            progress = _append_history_page(rows, page, cursor, upto_item_id)
            if progress is None:
                return None
            cursor, crossed_upper_bound = progress
            if crossed_upper_bound or len(page) < item_limit or cursor >= upto_item_id:
                return tuple(rows)
        return tuple(rows)

    def get_supported_pairs(self) -> list[str]:
        """Get list of available trading pairs.

        Returns:
            List of supported FX pairs.

        Raises:
            RuntimeError: If no data available.
        """
        if not self._last_data:
            raise RuntimeError("No data available - call connect() or get_ticker() first")
        return [walutomat_ws_to_native(symbol) for symbol in self._last_data]
