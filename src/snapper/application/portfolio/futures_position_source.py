"""Separate native position source observations from cash and derived entry-price arithmetic.

The caller must bind the provider contract identifier through a real historical
source alias before using these operands. An associated contract is insufficient
to turn a currency balance into a position. Source average entry prices remain
observations; this module does not infer the venue's rounding policy or replace
them with an arithmetic average.
"""

from decimal import Decimal
from decimal import DecimalException
from decimal import Inexact
from decimal import localcontext
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import model_validator

from snapper.application.portfolio.futures_account_log import FuturesAccountLogEntry
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import PositiveEvidenceDecimal


class FuturesPositionSourceError(ValueError):
    """A selected native log does not establish an exact provider-contract position leg."""


def _basis(quantity: Decimal, price: Decimal | None) -> Decimal | None:
    """Require source entry basis exactly for a nonflat observed position."""
    if quantity.is_zero():
        if price is not None and not price.is_zero():
            raise FuturesPositionSourceError("flat_source_position_has_entry_price")
        return None
    if price is None or price <= 0:
        raise FuturesPositionSourceError("nonflat_source_position_basis_unavailable")
    return price


def _position_delta(entry: FuturesAccountLogEntry) -> Decimal:
    """Keep source quantity conservation exact before interpreting its economic unit."""
    try:
        with localcontext() as context:
            context.traps[Inexact] = True
            return entry.new_balance - entry.old_balance
    except DecimalException as error:
        raise FuturesPositionSourceError(
            "source_position_quantity_precision_unavailable"
        ) from error


class FuturesPositionSourceLeg(BaseModel):
    """Observed signed native position quantities and source-reported entry bases."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    source_instrument_public_id: EvidenceUUID
    provider_contract_symbol: EvidenceText
    source_entry: FuturesAccountLogEntry
    quantity_before: EvidenceDecimal
    quantity_after: EvidenceDecimal
    quantity_delta: EvidenceDecimal
    entry_price_before: PositiveEvidenceDecimal | None
    entry_price_after: PositiveEvidenceDecimal | None

    @model_validator(mode="after")
    def validate_position_projection(self) -> Self:
        """Bind observed units and bases to the exact source contract and balance fields.

        Returns:
            The same immutable source leg, still requiring durable alias/account proof.
        """
        entry = self.source_entry
        if entry.asset == entry.collateral:
            raise FuturesPositionSourceError("native_currency_log_is_not_a_position")
        if entry.asset in {"flex_collateral_value", "cash_collateral_value"}:
            raise FuturesPositionSourceError("collateral_valuation_is_not_a_position")
        if entry.asset != self.provider_contract_symbol:
            raise FuturesPositionSourceError("source_position_contract_identity_unproven")
        if entry.contract is not None and entry.contract != self.provider_contract_symbol:
            raise FuturesPositionSourceError("source_position_associated_contract_contradiction")
        actual = (
            self.quantity_before,
            self.quantity_after,
            self.quantity_delta,
            self.entry_price_before,
            self.entry_price_after,
        )
        expected = (
            entry.old_balance,
            entry.new_balance,
            _position_delta(entry),
            _basis(entry.old_balance, entry.old_average_entry_price),
            _basis(entry.new_balance, entry.new_average_entry_price),
        )
        if actual != expected:
            raise FuturesPositionSourceError("source_position_projection_contradiction")
        return self


def native_futures_position_leg(
    entry: FuturesAccountLogEntry,
    provider_contract_symbol: str,
    source_instrument_public_id: str,
) -> FuturesPositionSourceLeg:
    """Project an explicit provider-contract leg without a symbol-prefix or case inference.

    Args:
        entry: Strict selected source log, already retained with its original wire bytes.
        provider_contract_symbol: Exact provider identifier bound by a historical alias row.
        source_instrument_public_id: Immutable actual source instrument UUID, not a Paper proxy.

    Returns:
        Native signed quantity and observed basis operands. The durable financial
        caller must prove alias, account, execution and economic-unit bindings.
    """
    source = FuturesAccountLogEntry.model_validate(entry)
    return FuturesPositionSourceLeg(
        source_instrument_public_id=source_instrument_public_id,
        provider_contract_symbol=provider_contract_symbol,
        source_entry=source,
        quantity_before=source.old_balance,
        quantity_after=source.new_balance,
        quantity_delta=_position_delta(source),
        entry_price_before=_basis(source.old_balance, source.old_average_entry_price),
        entry_price_after=_basis(source.new_balance, source.new_average_entry_price),
    )
