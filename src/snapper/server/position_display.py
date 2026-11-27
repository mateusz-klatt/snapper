"""Build position API payloads with derived dust and precision fields."""

from collections.abc import Mapping
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from math import isfinite
from typing import cast

from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.pnl_timeline_service import load_basket_fiat_evidence
from snapper.application.portfolio.position_display import PositionDisplay
from snapper.application.portfolio.position_display import PositionSpecSteps
from snapper.application.portfolio.position_display import display_for_quantity
from snapper.application.portfolio.position_display import display_steps_for_fact
from snapper.application.portfolio.position_display import optional_float
from snapper.config.settings import get_settings
from snapper.data.repository import Repository
from snapper.data.repository_types import PositionRow
from snapper.messaging.schemas.data import PositionData

__all__ = [
    "build_position_payloads",
    "load_position_quote_prices",
    "load_position_spec_steps",
    "load_usd_per_quote",
    "position_data_from_row",
    "position_dust_threshold",
]

_USD = "USD"
type PositionPriceKey = tuple[str, str, str, str]


def position_dust_threshold() -> Decimal:
    """Return the configured USD dust notional threshold.

    Returns:
        Threshold from bootstrap settings, in USD.
    """
    return get_settings().position_dust_notional_threshold


async def load_usd_per_quote(
    repo: Repository,
    quotes: Sequence[str],
    as_of: datetime,
) -> dict[str, Decimal]:
    """Return the portfolio FX rate of one quote unit in USD.

    The rate is ``convert_amount`` of one unit at the grid minute containing
    ``as_of``, using the same fiat-candle discovery as portfolio valuation.
    USD is always present at 1 and does not read a candle. A quote with no
    pinned plane is omitted so the caller skips the notional rule.

    Args:
        repo: Repository that discovers forex planes and candles.
        quotes: Quote currencies referenced by the response.
        as_of: Knowledge horizon and the instant being priced.

    Returns:
        Positive USD-per-quote rates keyed by currency. Missing quotes are
        absent. ``USD`` maps to ``1``.
    """
    rates: dict[str, Decimal] = {_USD: Decimal(1)}
    needed = frozenset(quote for quote in quotes if quote and quote != _USD)
    if not needed:
        return rates
    minute = as_of.replace(second=0, microsecond=0)
    fx_rates, venues, _versions = await load_basket_fiat_evidence(
        repo,
        needed,
        minute,
        minute,
        as_of,
    )
    for quote in sorted(needed):
        parsed = _positive_usd_rate(convert_amount(1.0, quote, _USD, minute, fx_rates, venues))
        if parsed is not None:
            rates[quote] = parsed
    return rates


async def load_position_spec_steps(
    repo: Repository,
    instrument_public_ids: Sequence[str],
    as_of: datetime,
) -> dict[str, PositionSpecSteps]:
    """Load display steps and USD rates for one instrument set.

    Spec columns are read in one batched query. Non-USD quotes share one
    portfolio FX load. An instrument that the query does not return is
    absent, which skips both the minimum and the notional rule.

    Args:
        repo: Repository that reads active instrument facts.
        instrument_public_ids: Instrument ids referenced by the response.
            Blank ids are ignored and duplicates are read once.
        as_of: Point in time for the spec and FX reads.

    Returns:
        Display steps keyed by instrument public id.
    """
    distinct = tuple(dict.fromkeys(item for item in instrument_public_ids if item))
    if not distinct:
        return {}
    facts = _mapping_rows(await repo.get_position_display_facts(distinct, as_of))
    rates = await load_usd_per_quote(repo, _quote_currencies(facts), as_of)
    loaded: dict[str, PositionSpecSteps] = {}
    for fact in facts:
        instrument_id = fact.get("instrument_public_id")
        if not isinstance(instrument_id, str) or not instrument_id:
            continue
        quote = fact.get("quote")
        rate = rates.get(quote) if isinstance(quote, str) else None
        loaded[instrument_id] = display_steps_for_fact(fact, rate)
    return loaded


async def load_position_quote_prices(
    repo: Repository,
    as_of: datetime,
) -> dict[PositionPriceKey, tuple[float | None, float | None]]:
    """Return the current mark and entry for each open position identity.

    The read uses the same current-mark overlay as ``GET /api/positions``
    without an ``as_of``. The key is instrument, exchange, mode, and wallet.

    Args:
        repo: Repository that reads positions.
        as_of: Point in time shared with the cycle read.

    Returns:
        ``(mark_price, average_price)`` keyed by the position identity.
        A non-list repository result contributes no prices.
    """
    prices: dict[PositionPriceKey, tuple[float | None, float | None]] = {}
    for row in _mapping_rows(await repo.get_positions(as_of=as_of, current_marks=True)):
        key = _price_key(row)
        if key is None:
            continue
        prices[key] = (
            _fresh_display_mark(row),
            optional_float(row.get("average_price")),
        )
    return prices


def position_data_from_row(
    row: Mapping[str, object],
    steps: PositionSpecSteps | None,
    notional_threshold: Decimal,
) -> PositionData:
    """Project one position row and its venue steps into the API schema.

    Args:
        row: Position projection returned by the repository.
        steps: Venue display steps, quote, and USD rate. None skips both
            dust rules.
        notional_threshold: USD dust threshold.

    Returns:
        Position payload including ``is_dust``, ``quantity_decimals``,
        and ``price_decimals``.
    """
    display = _display_for_row(row, steps, notional_threshold)
    payload: dict[str, object] = dict(row)
    if not isinstance(payload.get("instrument_public_id"), str):
        payload["instrument_public_id"] = ""
    payload["is_dust"] = display.is_dust
    payload["quantity_decimals"] = display.quantity_decimals
    payload["price_decimals"] = display.price_decimals
    return PositionData.model_validate(payload)


async def build_position_payloads(
    repo: Repository,
    rows: Sequence[PositionRow],
    as_of: datetime,
) -> list[PositionData]:
    """Attach dust and precision fields to a position list.

    Args:
        repo: Repository that reads active instrument facts and FX candles.
        rows: Position rows already scoped to the caller.
        as_of: Point in time shared with the position read.

    Returns:
        API position payloads in the same order as ``rows``.
    """
    threshold = position_dust_threshold()
    instrument_ids = [
        instrument_id for instrument_id in (_instrument_id(row) for row in rows) if instrument_id
    ]
    specs = await load_position_spec_steps(repo, instrument_ids, as_of)
    payloads: list[PositionData] = []
    for row in rows:
        instrument_id = _instrument_id(row)
        steps = specs.get(instrument_id) if instrument_id else None
        payloads.append(position_data_from_row(row, steps, threshold))
    return payloads


def _display_for_row(
    row: Mapping[str, object],
    steps: PositionSpecSteps | None,
    notional_threshold: Decimal,
) -> PositionDisplay:
    """Classify one repository row, treating a missing quantity as unusable."""
    quantity = optional_float(row.get("quantity"))
    return display_for_quantity(
        quantity=float("nan") if quantity is None else quantity,
        steps=steps,
        notional_threshold=notional_threshold,
        mark_price=_fresh_display_mark(row),
        average_price=optional_float(row.get("average_price")),
    )


def _fresh_display_mark(row: Mapping[str, object]) -> float | None:
    """Use fresh marks for dust classification and preserve the payload price."""
    if row.get("mark_stale") is True:
        return None
    return optional_float(row.get("mark_price"))


def _instrument_id(row: Mapping[str, object]) -> str:
    """Return the row's instrument id, or an empty string when it is absent."""
    value = row.get("instrument_public_id", "")
    if isinstance(value, str):
        return value
    return ""


def _quote_currencies(facts: Sequence[Mapping[str, object]]) -> list[str]:
    """Return the quote currencies present on fact rows."""
    quotes: list[str] = []
    for fact in facts:
        quote = fact.get("quote")
        if isinstance(quote, str) and quote:
            quotes.append(quote)
    return quotes


def _mapping_rows(raw: object) -> list[Mapping[str, object]]:
    """Return mapping rows, ignoring a non-list repository result."""
    if isinstance(raw, str | bytes) or not isinstance(raw, Sequence):
        return []
    rows: list[Mapping[str, object]] = []
    for item in raw:
        if isinstance(item, Mapping):
            rows.append(cast(Mapping[str, object], item))
    return rows


def _price_key(row: Mapping[str, object]) -> PositionPriceKey | None:
    """Return the position identity, or None when any part is missing."""
    instrument_id = row.get("instrument_public_id")
    exchange = row.get("exchange")
    mode = row.get("mode")
    wallet = row.get("wallet_public_id")
    if (
        not isinstance(instrument_id, str)
        or not instrument_id
        or not isinstance(exchange, str)
        or not exchange
        or not isinstance(mode, str)
        or not mode
        or not isinstance(wallet, str)
        or not wallet
    ):
        return None
    return (instrument_id, exchange, mode, wallet)


def _positive_usd_rate(value: float | None) -> Decimal | None:
    """Return a positive finite conversion as Decimal, or None."""
    if value is None or not isfinite(value) or value <= 0:
        return None
    return Decimal(str(value))
