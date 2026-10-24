"""Propose append-only xStocks view adjustments without moving raw units or cash.

The durable caller owns receipt retention, account/source-anchor verification,
persistent event dedup, correction authorization and transactional projection
updates. This fold is deterministic over an already resolved ordered stream.
Historical corrections require replay from an earlier proven position anchor;
they cannot be applied silently to holdings acquired after their effectivity.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from fractions import Fraction
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.xstock_rebase import XStockDecimal
from snapper.core.xstock_rebase import XStockInterpretation
from snapper.core.xstock_rebase import XStockMultiplierWitness
from snapper.core.xstock_rebase import XStockRebaseError
from snapper.core.xstock_rebase import XStockUnitScope
from snapper.core.xstock_rebase import exact_xstock_decimal
from snapper.core.xstock_rebase import require_xstock_multiplier

XSTOCK_CORPORATE_ACTION_HASH_DOMAIN = b"snapper:xstock-corporate-action:v1\x00"


class XStockRebaseReplayRequiredError(XStockRebaseError):
    """An old, corrected or noncontiguous event needs durable lookup and ordered replay."""


class _LedgerEvidence(BaseModel):
    """Immutable proposals revalidated before any fold, including unchecked copies."""

    model_config = ConfigDict(frozen=True, extra="forbid", revalidate_instances="always")


class XStockRebasePosition(_LedgerEvidence):
    """Cash-profile raw holdings and quote cost basis from a separately proven anchor.

    The anchor UUID is a reference, not proof by itself. The owning transaction
    must establish the account namespace, source and retained-state compatibility.
    Margin/short accounting is outside this cash-profile fold.
    """

    scope: XStockUnitScope
    position_anchor_public_id: EvidenceUUID
    raw_quantity: XStockDecimal
    quote_cost_basis: XStockDecimal
    multiplier: XStockMultiplierWitness
    economic_at: EvidenceInstant
    action_sequence: int = Field(default=0, ge=0, strict=True)
    last_action_public_id: EvidenceUUID | None = None
    last_action_digest: EvidenceDigest | None = None

    @model_validator(mode="after")
    def validate_position(self) -> Self:
        """Require compatible cash holdings, explicit scope and a complete local tip.

        Returns:
            The same state without certifying its external financial anchor.
        """
        if self.raw_quantity < 0 or self.quote_cost_basis < 0:
            raise ValueError("xStocks cash-profile holdings and cost basis must be nonnegative")
        if self.raw_quantity == 0 and self.quote_cost_basis != 0:
            raise ValueError("An empty xStocks position cannot retain quote cost basis")
        if self.multiplier.scope != self.scope:
            raise ValueError("xStocks position and multiplier scopes differ")
        if not self.multiplier.effective_from <= self.economic_at < self.multiplier.effective_until:
            raise ValueError("xStocks position multiplier is not effective for its state")
        has_tip = self.last_action_public_id is not None and self.last_action_digest is not None
        if (self.last_action_public_id is None) != (self.last_action_digest is None):
            raise ValueError("xStocks action identity and digest must be supplied together")
        if (self.action_sequence > 0) != has_tip:
            raise ValueError("xStocks action sequence and retained tip disagree")
        return self


class XStockCorporateAction(_LedgerEvidence):
    """One immutable source proposal with a logical transition slot.

    The logical sequence is separate from the physical append-only ledger row
    sequence. A correction appends a new row while retaining the superseded
    transition slot in the caller's explicitly resolved replay stream.
    """

    public_id: EvidenceUUID
    transition_sequence: int = Field(ge=1, strict=True)
    source_event_id: EvidenceText
    source_receipt_digest: EvidenceDigest
    kind: Literal["dividend_reinvestment", "split", "reverse_split", "correction"]
    before: XStockMultiplierWitness
    after: XStockMultiplierWitness
    effective_at: EvidenceInstant
    known_at: EvidenceInstant
    supersedes_public_id: EvidenceUUID | None = None

    @model_validator(mode="after")
    def validate_source(self) -> Self:
        """Require one identity, an actual multiplier transition and explicit correction intent.

        Returns:
            The same source proposal; persisted correction-target proof is still required.
        """
        if self.before.scope != self.after.scope:
            raise ValueError("xStocks corporate action crosses an asset or network scope")
        if self.before.public_id == self.after.public_id:
            raise ValueError(
                "Different xStocks observations require different immutable identities"
            )
        if self.after.effective_from != self.effective_at:
            raise ValueError("xStocks action and new multiplier effectivity disagree")
        if self.known_at < max(self.before.known_at, self.after.known_at):
            raise ValueError("xStocks action cannot predate its multiplier knowledge")
        if (self.kind == "correction") != (self.supersedes_public_id is not None):
            raise ValueError("xStocks correction requires its explicit superseded event identity")
        if self.supersedes_public_id == self.public_id:
            raise ValueError("An xStocks action cannot supersede itself")
        return self


@dataclass(frozen=True, slots=True)
class XStockCorporateActionOutcome:
    """A proposed view transition with no raw quantity, cost-basis or cash posting."""

    position: XStockRebasePosition
    created: bool
    equity_quantity_before: Decimal
    equity_quantity_after: Decimal
    raw_quantity_delta: Decimal = Decimal(0)
    quote_cost_basis_delta: Decimal = Decimal(0)
    cash_delta: Decimal = Decimal(0)


def _require(condition: bool, reason: str) -> None:
    """Refuse an interpretation without mutating the supplied immutable position."""
    if not condition:
        raise XStockRebaseError(reason)


def xstock_corporate_action_digest(action: XStockCorporateAction) -> str:
    """Hash the complete immutable source proposal, separately from its event identity.

    Args:
        action: The exact revalidated corporate-action and multiplier evidence.

    Returns:
        Versioned content identity used by the durable event ledger.
    """
    evidence = XStockCorporateAction.model_validate(action)
    material = json.dumps(evidence.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(XSTOCK_CORPORATE_ACTION_HASH_DOMAIN + material.encode()).hexdigest()


def _equity_quantity(raw_quantity: Decimal, witness: XStockMultiplierWitness) -> Decimal:
    """Calculate a display quantity without using it as a new asset or cash booking."""
    return exact_xstock_decimal(Fraction(raw_quantity) * Fraction(witness.multiplier))


def _require_direction(action: XStockCorporateAction) -> None:
    """Require the documented multiplier direction for each supported original event."""
    if action.kind == "split":
        _require(action.after.multiplier > action.before.multiplier, "xstock_split_not_increasing")
    elif action.kind == "reverse_split":
        _require(
            action.after.multiplier < action.before.multiplier,
            "xstock_reverse_split_not_decreasing",
        )
    elif action.kind == "dividend_reinvestment":
        _require(
            action.after.multiplier >= action.before.multiplier,
            "xstock_reinvestment_not_increasing",
        )


def _require_order(position: XStockRebasePosition, action: XStockCorporateAction) -> None:
    """Preserve an ordered financial history instead of adjusting later holdings in place."""
    if action.transition_sequence != position.action_sequence + 1:
        raise XStockRebaseReplayRequiredError("xstock_action_requires_persistent_lookup_or_replay")
    if action.effective_at <= position.economic_at:
        raise XStockRebaseReplayRequiredError("xstock_historical_action_requires_replay")
    _require(position.multiplier == action.before, "xstock_action_predecessor_mismatch")
    _require(
        action.before.effective_until == action.effective_at,
        "xstock_multiplier_history_has_gap_or_overlap",
    )


def fold_xstock_corporate_action(
    position: XStockRebasePosition,
    action: XStockCorporateAction,
    clocks: XStockInterpretation,
) -> XStockCorporateActionOutcome:
    """Propose one ordered view change while retaining canonical holdings and cash.

    Args:
        position: A separately anchored cash-profile position and retained action tip.
        action: A persisted or persistable event with proven predecessor/source identities.
        clocks: Exact event effectivity and the available interpretation knowledge.

    Returns:
        An immutable proposal; its persistence and projection update must be atomic.
    """
    state = XStockRebasePosition.model_validate(position)
    event = XStockCorporateAction.model_validate(action)
    context = XStockInterpretation.model_validate(clocks)
    _require(context.economic_at == event.effective_at, "xstock_action_economic_clock_mismatch")
    _require(event.known_at <= context.interpreted_at, "xstock_action_not_yet_known")
    require_xstock_multiplier(event.after, state.scope, context)
    digest = xstock_corporate_action_digest(event)
    if state.last_action_public_id == event.public_id:
        _require(state.last_action_digest == digest, "xstock_action_identity_content_conflict")
        _require(state.multiplier == event.after, "xstock_action_retry_state_mismatch")
        quantity = _equity_quantity(state.raw_quantity, state.multiplier)
        return XStockCorporateActionOutcome(state, False, quantity, quantity)
    _require_order(state, event)
    _require_direction(event)
    updated = XStockRebasePosition(
        scope=state.scope,
        position_anchor_public_id=state.position_anchor_public_id,
        raw_quantity=state.raw_quantity,
        quote_cost_basis=state.quote_cost_basis,
        multiplier=event.after,
        economic_at=event.effective_at,
        action_sequence=event.transition_sequence,
        last_action_public_id=event.public_id,
        last_action_digest=digest,
    )
    return XStockCorporateActionOutcome(
        updated,
        True,
        _equity_quantity(state.raw_quantity, event.before),
        _equity_quantity(state.raw_quantity, event.after),
    )


def replay_xstock_corporate_actions(
    seed: XStockRebasePosition,
    actions: tuple[XStockCorporateAction, ...],
    interpreted_at: datetime,
) -> XStockRebasePosition:
    """Deterministically fold an already deduplicated, correction-resolved event stream.

    Args:
        seed: The proven original holdings anchor before the selected events take effect.
        actions: Authoritative ordered events, retaining explicit supersession identities.
        interpreted_at: The knowledge clock for this replay, never a backdated observation.

    Returns:
        The same proposed state as applying the resolved events transactionally in order.
    """
    state = XStockRebasePosition.model_validate(seed)
    require_xstock_multiplier(
        state.multiplier,
        state.scope,
        XStockInterpretation(economic_at=state.economic_at, interpreted_at=interpreted_at),
    )
    for action in actions:
        event = XStockCorporateAction.model_validate(action)
        context = XStockInterpretation(
            economic_at=event.effective_at, interpreted_at=interpreted_at
        )
        state = fold_xstock_corporate_action(state, event, context).position
    return state
