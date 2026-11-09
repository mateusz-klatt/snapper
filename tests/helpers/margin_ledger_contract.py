"""Typed published margin excerpts and explicitly synthetic contract examples.

These samples are transcribed from sanitized repository Markdown, not original
venue responses. They cannot prove native pagination, scope or replay completeness.
"""

from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict


class _FixtureModel(BaseModel):
    """Reject undeclared fixture fields and accidental mutation of evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class NativeLedgerSample(_FixtureModel):
    """Preserve published native monetary strings, raw assets and opaque aliases."""

    alias: str
    refid: str
    at: datetime
    aclass: Literal["currency"]
    asset: str
    type: Literal["trade", "margin", "collateralconversion", "rollover", "settled"]
    subtype: Literal["", "tradespot"]
    amount: str
    fee: str
    balance: str


class NativeTradeSample(_FixtureModel):
    """Retain only trade fields and close links actually published in the excerpt."""

    alias: str
    order_alias: str
    at: datetime
    pair: str
    side: Literal["buy", "sell"]
    leverage: int
    margin: str
    fee_value: str
    closing: bool
    position_state: Literal["closed"] | None
    opening_trade_alias: str | None


class InternalAccrualSample(_FixtureModel):
    """Preserve internal A1-A4 values without classifying them as native fees."""

    alias: str
    boundary: datetime
    inserted_at: datetime
    quantity: str
    notional: str
    rate: str
    amount: str
    amount_asset: Literal["EUR"]
    active: Literal[True]


class CommandStateSample(_FixtureModel):
    """Keep observed states separate from causality and omitted terminal-time evidence.

    A missing terminal_at field means the excerpt supplies no value; an explicit
    null records the observed absence of a terminal time on an unresolved command.
    """

    venue: Literal["kraken", "walutomat"]
    order_alias: str | None
    public_id: str
    type: Literal["create", "cancel"]
    created_at: datetime
    status: Literal["accepted", "dispatched", "failed"]
    terminal_at: datetime | None = None
    order_status: Literal["canceled"] | None
    filled_size: str | None
    associated_events: tuple[Literal["order_accepted", "order_interlock_blocked"], ...]
    causal_cancel_acknowledgment: Literal[False]


class SyntheticNativeRollover(NativeLedgerSample):
    """Describe a hypothetical tiny posted fee without claiming recorded evidence."""

    provenance: Literal["synthetic"]
    purpose: str


class MarginLedgerContract(_FixtureModel):
    """Bind bounded excerpts and the synthetic example to explicit evidence limits."""

    provenance: Literal["published_sanitized_excerpt"]
    source: str
    coverage: Literal["samples_only_not_complete_native_history"]
    recorded_pages_available: Literal[False]
    missing_recorded_evidence: tuple[str, ...]
    ledgers: tuple[NativeLedgerSample, ...]
    trades: tuple[NativeTradeSample, ...]
    internal_accruals: tuple[InternalAccrualSample, ...]
    commands: tuple[CommandStateSample, ...]
    synthetic_native_rollover: SyntheticNativeRollover


def margin_ledger_contract() -> MarginLedgerContract:
    """Load the self-contained typed excerpt without requiring proprietary files.

    Returns:
        Published samples with original decimal strings and explicit provenance.
    """
    path = Path(__file__).resolve().parents[1] / "fixtures" / "margin_ledger_slice1.json"
    return MarginLedgerContract.model_validate_json(path.read_text(encoding="utf-8"))
