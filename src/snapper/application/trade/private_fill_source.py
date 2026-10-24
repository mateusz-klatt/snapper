"""Interpret one actual Futures fill child without promoting legacy float projections."""

import base64
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import DecimalException
from typing import Literal
from typing import NoReturn
from typing import cast

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import PositiveEvidenceDecimal
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)


class PrivateFillSourceError(ValueError):
    """Retained financial source cannot prove one ordinary fill's native operands."""


class FuturesPrivateFill(BaseModel):
    """One wire-selected fill, with account and provider identities kept case-sensitive.

    Quantities remain in the provider's native unit until the actual historical
    instrument definition supplies its dimension. A subscription sequence is not
    a complete account-history cursor. This object alone authorizes no posting.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    provider_symbol: EvidenceText
    venue_account_id: EvidenceText | None
    execution_id: EvidenceText
    order_id: EvidenceText
    client_order_id: EvidenceText | None
    side: Literal["buy", "sell"]
    quantity: PositiveEvidenceDecimal
    price: PositiveEvidenceDecimal
    remaining_quantity: EvidenceDecimal = Field(ge=0)
    fee: EvidenceDecimal
    fee_currency: EvidenceText
    fill_type: Literal[
        "maker",
        "taker",
        "takerAfterEdit",
        "liquidation",
        "assignee",
        "assignor",
        "unwindBankrupt",
        "unwindCounterparty",
    ]
    occurred_at: EvidenceInstant
    subscription_sequence: int = Field(ge=1)
    target_json_pointer: str
    feed: Literal["fills", "fills_snapshot"]


def _refuse(reason: str) -> NoReturn:
    """Leave the original receipt held instead of fabricating a financial operand."""
    raise PrivateFillSourceError(reason)


def _document(observation: RawAccountingObservation) -> dict[str, object]:
    """Require genuine whole-frame source bytes and the actual Futures source route."""
    if observation.scope.exchange != "kraken_futures":
        _refuse("private_fill_source_exchange_mismatch")
    if observation.representation != "wire_bytes_base64":
        _refuse("private_fill_original_wire_required")
    try:
        return decode_private_execution_receipt(
            base64.b64decode(observation.material, validate=True)
        )
    except (ValueError, UnicodeError) as error:
        raise PrivateFillSourceError("private_fill_wire_json_invalid") from error


def _selected_fill(
    observation: RawAccountingObservation, document: dict[str, object]
) -> dict[str, object]:
    """Resolve only the persisted canonical child selector, never a guessed first match."""
    feed = document.get("feed")
    if not isinstance(feed, str) or feed not in {"fills", "fills_snapshot"}:
        _refuse("private_fill_feed_unsupported")
    pointer = observation.target_json_pointer
    if pointer is None or not pointer.startswith("/fills/"):
        _refuse("private_fill_selector_required")
    index = pointer.removeprefix("/fills/")
    if not index.isascii() or not index.isdecimal() or (len(index) > 1 and index.startswith("0")):
        _refuse("private_fill_selector_noncanonical")
    fills = document.get("fills")
    if not isinstance(fills, list) or int(index) >= len(fills):
        _refuse("private_fill_selected_child_missing")
    child = fills[int(index)]
    if not isinstance(child, dict):
        _refuse("private_fill_selected_child_invalid")
    return cast(dict[str, object], child)


def _text(record: dict[str, object], name: str) -> str:
    """Retain exact nonblank provider spelling without case folding or symbol inference."""
    value = record.get(name)
    if not isinstance(value, str) or not value.strip():
        _refuse(f"private_fill_text_unavailable:{name}")
    return value


def _optional_text(record: dict[str, object], name: str) -> str | None:
    """Keep a missing optional provider identity absent rather than deriving another one."""
    return None if record.get(name) is None else _text(record, name)


def _number(record: dict[str, object], name: str) -> Decimal:
    """Interpret original exact JSON numbers or text, rejecting booleans and float mirrors."""
    value = record.get(name)
    if isinstance(value, bool) or not isinstance(value, Decimal | int | str):
        _refuse(f"private_fill_number_unavailable:{name}")
    try:
        number = Decimal(value)
    except DecimalException as error:
        raise PrivateFillSourceError(f"private_fill_number_invalid:{name}") from error
    if not number.is_finite():
        _refuse(f"private_fill_number_nonfinite:{name}")
    return number


def _positive_integer(record: dict[str, object], name: str) -> int:
    """Require an actual positive integer token for the subscription sequence and clock."""
    value = record.get(name)
    if type(value) is not int or value <= 0:
        _refuse(f"private_fill_integer_unavailable:{name}")
    return value


def _occurred_at(record: dict[str, object]) -> datetime:
    """Convert the exact venue millisecond timestamp without a floating-point epoch."""
    milliseconds = _positive_integer(record, "time")
    try:
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=milliseconds)
    except OverflowError as error:
        raise PrivateFillSourceError("private_fill_time_out_of_range") from error


def parse_futures_private_fill(observation: RawAccountingObservation) -> FuturesPrivateFill:
    """Decode one persisted selected fill with source-native quantities, fees and identities.

    Args:
        observation: Retained actual whole-frame receipt and canonical child pointer.

    Returns:
        One exact source fact without inferred currency, units or account-log equivalence.

    Raises:
        PrivateFillSourceError: If genuine source bytes or an essential native operand are absent.
        ValueError: If quantities, direction or source classification are unsupported.
    """
    document = _document(observation)
    record = _selected_fill(observation, document)
    buy = record.get("buy")
    if type(buy) is not bool:
        _refuse("private_fill_direction_unavailable")
    return FuturesPrivateFill.model_validate(
        {
            "provider_symbol": _text(record, "instrument"),
            "venue_account_id": _optional_text(document, "account"),
            "execution_id": _text(record, "fill_id"),
            "order_id": _text(record, "order_id"),
            "client_order_id": _optional_text(record, "cli_ord_id"),
            "side": "buy" if buy else "sell",
            "quantity": _number(record, "qty"),
            "price": _number(record, "price"),
            "remaining_quantity": _number(record, "remaining_order_qty"),
            "fee": _number(record, "fee_paid"),
            "fee_currency": _text(record, "fee_currency"),
            "fill_type": _text(record, "fill_type"),
            "occurred_at": _occurred_at(record),
            "subscription_sequence": _positive_integer(record, "seq"),
            "target_json_pointer": observation.target_json_pointer,
            "feed": document["feed"],
        }
    )
