"""Interpret selected native account-log entries from preserved financial wire receipts.

Balance legs remain native currency facts. Neither collateral valuations nor
position changes are relabeled USD cash. Selection and numeric precision are
checked against the original complete frame; durable source/account binding and
ledger deduplication belong to the accounting transaction writer.
"""

import base64
import json
from datetime import datetime
from decimal import Decimal
from decimal import DecimalException
from decimal import Inexact
from decimal import localcontext
from typing import NoReturn
from typing import Self
from typing import cast

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText


class FuturesAccountLogError(ValueError):
    """Source fidelity, selection or native leg dimensions do not establish accounting."""


class FuturesAccountLogEntry(BaseModel):
    """One strictly selected native source event, retaining position and cash fields separately."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    source_log_id: int = Field(gt=0)
    booking_uid: EvidenceText
    asset: EvidenceText
    margin_account: EvidenceText
    info: EvidenceText
    occurred_at: EvidenceInstant
    old_balance: EvidenceDecimal
    new_balance: EvidenceDecimal
    target_json_pointer: str
    collateral: EvidenceText | None
    contract: EvidenceText | None
    execution: EvidenceText | None
    old_average_entry_price: EvidenceDecimal | None
    new_average_entry_price: EvidenceDecimal | None
    trade_price: EvidenceDecimal | None
    mark_price: EvidenceDecimal | None
    realized_pnl: EvidenceDecimal | None
    fee: EvidenceDecimal | None
    realized_funding: EvidenceDecimal | None
    liquidation_fee: EvidenceDecimal | None
    conversion_spread_percentage: EvidenceDecimal | None
    funding_rate: EvidenceDecimal | None = None
    venue_account_id: EvidenceText | None = None


class FuturesNativeCashLeg(BaseModel):
    """An observed cash balance change in the source's exact native asset namespace."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    entry: FuturesAccountLogEntry
    native_currency: EvidenceText
    cash_delta: EvidenceDecimal

    @model_validator(mode="after")
    def validate_cash_leg(self) -> Self:
        """Keep native denomination and exact balance conservation attached to the source.

        Returns:
            The same immutable leg after independently checking its projected fields.
        """
        currency, delta = _cash_operands(self.entry)
        if (self.native_currency, self.cash_delta) != (currency, delta):
            _refuse("native_cash_projection_contradicts_source")
        return self


def _refuse(reason: str) -> NoReturn:
    """Stop interpretation while the already committed raw receipt remains held."""
    raise FuturesAccountLogError(reason)


def _reject_constant(value: str) -> NoReturn:
    """Refuse JSON NaN/infinity instead of treating malformed money as a number."""
    _refuse(f"native_json_constant_unsupported:{value}")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Refuse duplicate decoded keys before selecting a financial child."""
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            _refuse("native_json_duplicate_key")
        result[name] = value
    return result


def _wire_document(observation: RawAccountingObservation) -> dict[str, object]:
    """Decode original private-frame bytes with exact numeric lexemes and no key shadowing."""
    if observation.scope.exchange != "kraken_futures":
        _refuse("futures_account_log_source_exchange_mismatch")
    if observation.representation != "wire_bytes_base64":
        _refuse("account_log_original_wire_evidence_required")
    try:
        raw = base64.b64decode(observation.material, validate=True)
        value: object = json.loads(
            raw,
            parse_float=Decimal,
            parse_int=Decimal,
            parse_constant=_reject_constant,
            object_pairs_hook=_unique_object,
        )
    except (UnicodeError, ValueError) as error:
        raise FuturesAccountLogError("account_log_wire_json_invalid") from error
    if not isinstance(value, dict):
        _refuse("account_log_wire_object_required")
    return cast(dict[str, object], value)


def _selected_entry(
    observation: RawAccountingObservation, document: dict[str, object]
) -> tuple[dict[str, object], str]:
    """Bind one child to its persisted canonical pointer in the original whole frame."""
    envelope: dict[str, object] = observation.model_dump(mode="python")
    pointer = envelope.get("target_json_pointer")
    if not isinstance(pointer, str):
        _refuse("account_log_child_selector_required")
    feed = document.get("feed")
    if feed == "account_log":
        if pointer != "/new_entry":
            _refuse("account_log_live_selector_mismatch")
        child = document.get("new_entry")
    elif feed == "account_log_snapshot":
        prefix = "/logs/"
        index = pointer.removeprefix(prefix)
        if not pointer.startswith(prefix) or not index.isascii() or not index.isdecimal():
            _refuse("account_log_snapshot_selector_mismatch")
        if len(index) > 1 and index.startswith("0"):
            _refuse("account_log_snapshot_selector_noncanonical")
        logs = document.get("logs")
        if not isinstance(logs, list) or int(index) >= len(logs):
            _refuse("account_log_snapshot_child_missing")
        child = logs[int(index)]
    else:
        _refuse("account_log_feed_unsupported")
    if not isinstance(child, dict):
        _refuse("account_log_child_object_required")
    return cast(dict[str, object], child), pointer


def _text(entry: dict[str, object], name: str) -> str:
    """Require a provider text identity without case folding or symbol inference."""
    value = entry.get(name)
    if not isinstance(value, str) or not value.strip():
        _refuse(f"account_log_text_unavailable:{name}")
    return value


def _optional_text(entry: dict[str, object], name: str) -> str | None:
    """Retain documented empty/null optional identities as honestly absent."""
    value = entry.get(name)
    if value is None or value == "":
        return None
    return _text(entry, name)


def _number(entry: dict[str, object], name: str) -> Decimal:
    """Read an exact wire number or decimal text without accepting float mirrors."""
    value = entry.get(name)
    if not isinstance(value, Decimal | str):
        _refuse(f"account_log_number_unavailable:{name}")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except DecimalException as error:
        raise FuturesAccountLogError(f"account_log_number_invalid:{name}") from error
    if not result.is_finite():
        _refuse(f"account_log_number_nonfinite:{name}")
    return result


def _optional_number(entry: dict[str, object], name: str) -> Decimal | None:
    """Keep an absent fee/funding component distinct from known zero."""
    return None if entry.get(name) is None else _number(entry, name)


def _source_id(entry: dict[str, object]) -> int:
    """Require a literal positive source-log integer without claiming contiguous sequence."""
    value = entry.get("id")
    if not isinstance(value, Decimal) or value.as_tuple().exponent != 0 or value <= 0:
        _refuse("account_log_source_id_invalid")
    return int(value)


def _occurred_at(entry: dict[str, object]) -> datetime:
    """Parse the venue's explicit aware time without making it a commit-order cursor."""
    try:
        instant = datetime.fromisoformat(_text(entry, "date"))
    except ValueError as error:
        raise FuturesAccountLogError("account_log_source_time_invalid") from error
    if instant.utcoffset() is None:
        _refuse("account_log_source_time_naive")
    return instant


def parse_futures_account_log(
    observation: RawAccountingObservation,
) -> FuturesAccountLogEntry:
    """Interpret one selected native log entry without assigning it to a cash or position book.

    Args:
        observation: A committed whole-frame receipt and its persisted exact child selector.

    Returns:
        Native account-log fields with exact Decimal operands and opaque source identities.
        This does not establish an authenticated account or completed ledger application.
    """
    evidence = RawAccountingObservation.model_validate(observation)
    document = _wire_document(evidence)
    entry, pointer = _selected_entry(evidence, document)
    optional_numbers = {
        name: _optional_number(entry, name)
        for name in (
            "old_average_entry_price",
            "new_average_entry_price",
            "trade_price",
            "mark_price",
            "realized_pnl",
            "fee",
            "realized_funding",
            "funding_rate",
            "liquidation_fee",
            "conversion_spread_percentage",
        )
    }
    return FuturesAccountLogEntry.model_validate(
        {
            "source_log_id": _source_id(entry),
            "booking_uid": _text(entry, "booking_uid"),
            "asset": _text(entry, "asset"),
            "margin_account": _text(entry, "margin_account"),
            "info": _text(entry, "info"),
            "occurred_at": _occurred_at(entry),
            "old_balance": _number(entry, "old_balance"),
            "new_balance": _number(entry, "new_balance"),
            "target_json_pointer": pointer,
            "venue_account_id": (_text(document, "account") if "account" in document else None),
            "collateral": _optional_text(entry, "collateral"),
            "contract": _optional_text(entry, "contract"),
            "execution": _optional_text(entry, "execution"),
            **optional_numbers,
        }
    )


def _cash_operands(entry: FuturesAccountLogEntry) -> tuple[str, Decimal]:
    """Recognize only explicit native currency balances and exactly conserve their delta."""
    evidence = FuturesAccountLogEntry.model_validate(entry)
    if evidence.collateral is None or evidence.asset != evidence.collateral:
        _refuse("native_cash_currency_leg_unproven")
    if evidence.asset in {"flex_collateral_value", "cash_collateral_value"}:
        _refuse("collateral_valuation_is_not_native_cash")
    try:
        with localcontext() as context:
            context.traps[Inexact] = True
            delta = evidence.new_balance - evidence.old_balance
    except DecimalException as error:
        raise FuturesAccountLogError("native_cash_delta_precision_unavailable") from error
    return evidence.asset, delta


def native_futures_cash_leg(entry: FuturesAccountLogEntry) -> FuturesNativeCashLeg:
    """Recognize an explicitly currency-labeled balance leg without a position-name heuristic.

    Args:
        entry: Strict native account-log observation, still requiring durable account binding.

    Returns:
        The observed same-native-currency balance delta. P&L, fee and funding
        fields remain components of this delta and are not posted again.
    """
    evidence = FuturesAccountLogEntry.model_validate(entry)
    currency, delta = _cash_operands(evidence)
    return FuturesNativeCashLeg(entry=evidence, native_currency=currency, cash_delta=delta)
