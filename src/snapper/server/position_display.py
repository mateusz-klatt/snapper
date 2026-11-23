"""Build position API payloads with derived dust and precision fields."""

from collections.abc import Mapping
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

from snapper.application.portfolio.position_display import PositionDisplay
from snapper.application.portfolio.position_display import PositionSpecSteps
from snapper.application.portfolio.position_display import display_for_quantity
from snapper.application.portfolio.position_display import optional_float
from snapper.application.portfolio.position_display import position_spec_steps
from snapper.config.settings import get_settings
from snapper.data.repository import Repository
from snapper.data.repository_types import PositionRow
from snapper.messaging.schemas.data import PositionData

__all__ = [
    "build_position_payloads",
    "load_position_spec_steps",
    "position_data_from_row",
    "position_dust_threshold",
]


def position_dust_threshold() -> Decimal:
    """Return the configured quote-currency dust notional threshold.

    Returns:
        Threshold from bootstrap settings, in the position's quote currency.
    """
    return get_settings().position_dust_notional_threshold


async def load_position_spec_steps(
    repo: Repository,
    instrument_public_ids: Sequence[str],
    as_of: datetime,
) -> dict[str, PositionSpecSteps]:
    """Load lot, tick, and minimum size for each distinct instrument.

    Args:
        repo: Repository that reads active instrument specs.
        instrument_public_ids: Instrument ids referenced by the response.
            Blank ids are ignored and duplicates are read once.
        as_of: Point in time for the spec read.

    Returns:
        Display steps keyed by instrument public id. An instrument with
        no spec maps to empty steps.
    """
    loaded: dict[str, PositionSpecSteps] = {}
    for instrument_public_id in dict.fromkeys(instrument_public_ids):
        if not instrument_public_id:
            continue
        spec = await repo.get_instrument_spec(instrument_public_id, as_of)
        loaded[instrument_public_id] = position_spec_steps(spec)
    return loaded


def position_data_from_row(
    row: Mapping[str, object],
    steps: PositionSpecSteps | None,
    notional_threshold: Decimal,
) -> PositionData:
    """Project one position row and its venue steps into the API schema.

    Args:
        row: Position projection returned by the repository.
        steps: Venue display steps. None applies the notional rule only.
        notional_threshold: Quote-currency dust threshold.

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
        repo: Repository that reads active instrument specs.
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
        mark_price=optional_float(row.get("mark_price")),
        average_price=optional_float(row.get("average_price")),
    )


def _instrument_id(row: Mapping[str, object]) -> str:
    """Return the row's instrument id, or an empty string when it is absent."""
    value = row.get("instrument_public_id", "")
    if isinstance(value, str):
        return value
    return ""
