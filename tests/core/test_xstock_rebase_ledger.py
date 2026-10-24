"""Corporate-action proposals preserve raw holdings and require deterministic correction replay."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from snapper.core.xstock_rebase import XStockInterpretation
from snapper.core.xstock_rebase import XStockMultiplierWitness
from snapper.core.xstock_rebase import XStockRebaseError
from snapper.core.xstock_rebase import XStockUnitScope
from snapper.core.xstock_rebase_ledger import XStockCorporateAction
from snapper.core.xstock_rebase_ledger import XStockRebasePosition
from snapper.core.xstock_rebase_ledger import XStockRebaseReplayRequiredError
from snapper.core.xstock_rebase_ledger import fold_xstock_corporate_action
from snapper.core.xstock_rebase_ledger import replay_xstock_corporate_actions
from snapper.core.xstock_rebase_ledger import xstock_corporate_action_digest

_T0 = datetime(2026, 9, 1, tzinfo=UTC)
_T1 = _T0 + timedelta(days=1)
_T2 = _T1 + timedelta(days=1)
_T3 = _T2 + timedelta(days=1)
_NOW = _T3 + timedelta(days=1)
_FIRST_ACTION = "00000000-0000-4000-8000-000000000011"


def _scope(**updates: object) -> XStockUnitScope:
    """Supply independently specified catalog, quote and chain identities."""
    payload: dict[str, object] = {
        "exchange": "kraken",
        "instrument_public_id": "00000000-0000-4000-8000-000000000001",
        "spv_asset_id": "AAPLSPV",
        "underlying_asset_id": "US0378331005",
        "quote_asset_id": "USD",
        "network": "solana",
        "token_contract": "test-issuer-mint-AAPL",
    }
    return XStockUnitScope.model_validate(payload | updates)


def _witness(**updates: object) -> XStockMultiplierWitness:
    """Provide the original multiplier from a bounded historical observation."""
    payload: dict[str, object] = {
        "public_id": "00000000-0000-4000-8000-000000000002",
        "scope": _scope(),
        "multiplier": "1",
        "source": "test-provider:multiplier-history",
        "source_record_id": "multiplier-before",
        "source_receipt_digest": "a" * 64,
        "effective_from": _T0,
        "effective_until": _T1,
        "observed_at": _T0,
        "known_at": _T0,
    }
    return XStockMultiplierWitness.model_validate(payload | updates)


def _after(**updates: object) -> XStockMultiplierWitness:
    """Provide a distinct post-split witness with its own receipt and clocks."""
    payload: dict[str, object] = {
        "public_id": "00000000-0000-4000-8000-000000000003",
        "multiplier": "2",
        "source_record_id": "multiplier-after",
        "source_receipt_digest": "b" * 64,
        "effective_from": _T1,
        "effective_until": _T2,
        "observed_at": _T1,
        "known_at": _T1,
    }
    return _witness(**(payload | updates))


def _position(**updates: object) -> XStockRebasePosition:
    """Seed explicit cash-profile holdings without implying that a UUID proves them."""
    payload: dict[str, object] = {
        "scope": _scope(),
        "position_anchor_public_id": "00000000-0000-4000-8000-000000000010",
        "raw_quantity": "3",
        "quote_cost_basis": "600",
        "multiplier": _witness(),
        "economic_at": _T0 + timedelta(hours=1),
    }
    return XStockRebasePosition.model_validate(payload | updates)


def _action(**updates: object) -> XStockCorporateAction:
    """Construct one independently authored split observation and logical transition slot."""
    payload: dict[str, object] = {
        "public_id": _FIRST_ACTION,
        "transition_sequence": 1,
        "source_event_id": "test-provider:split:1",
        "source_receipt_digest": "c" * 64,
        "kind": "split",
        "before": _witness(),
        "after": _after(),
        "effective_at": _T1,
        "known_at": _T1,
    }
    return XStockCorporateAction.model_validate(payload | updates)


def _clocks(**updates: object) -> XStockInterpretation:
    """Interpret the event using a later explicit knowledge clock."""
    payload: dict[str, object] = {"economic_at": _T1, "interpreted_at": _NOW}
    return XStockInterpretation.model_validate(payload | updates)


@pytest.mark.parametrize(
    ("kind", "before", "after", "old_equity", "new_equity"),
    [
        ("split", "1", "2", "3", "6"),
        ("reverse_split", "2", "1", "6", "3"),
        ("dividend_reinvestment", "1", "1.1", "3", "3.3"),
        ("dividend_reinvestment", "1", "1", "3", "3"),
    ],
)
def test_corporate_actions_change_views_without_creating_cash_or_raw_units(
    kind: str, before: str, after: str, old_equity: str, new_equity: str
) -> None:
    """Supported rebases preserve canonical assets and quote cost basis.

    Given: Proven raw holdings with before and after multiplier observations.
    When: A split, reverse split or reinvestment proposal is folded.
    Then: Equity exposure changes while raw units, cost basis and cash stay unchanged.
    """
    original = _position(multiplier=_witness(multiplier=before))
    event = _action(kind=kind, before=original.multiplier, after=_after(multiplier=after))
    result = fold_xstock_corporate_action(original, event, _clocks())
    assert result.created
    assert result.equity_quantity_before == Decimal(old_equity)
    assert result.equity_quantity_after == Decimal(new_equity)
    assert result.position.raw_quantity == original.raw_quantity == Decimal("3")
    assert result.position.quote_cost_basis == original.quote_cost_basis == Decimal("600")
    assert result.raw_quantity_delta == result.quote_cost_basis_delta == result.cash_delta == 0
    assert result.position.last_action_digest == xstock_corporate_action_digest(event)


def test_exact_last_event_retry_has_no_second_projection_delta() -> None:
    """A persisted last event is idempotent without an in-memory history set.

    Given: A position whose last action identity and content match the retry.
    When: The exact immutable event is folded again.
    Then: State is unchanged and both before and after views equal the current view.
    """
    event = _action()
    first = fold_xstock_corporate_action(_position(), event, _clocks())
    retry = fold_xstock_corporate_action(first.position, event, _clocks())
    assert not retry.created
    assert retry.position == first.position
    assert retry.equity_quantity_before == retry.equity_quantity_after == Decimal("6")
    assert retry.cash_delta == retry.raw_quantity_delta == 0


def test_reused_event_identity_with_different_content_is_a_conflict() -> None:
    """Stable identity cannot silently absorb altered provider receipt evidence.

    Given: A position already carrying the original event's immutable digest.
    When: The same identity is retried with a different source receipt.
    Then: The altered content refuses without advancing the position.
    """
    state = fold_xstock_corporate_action(_position(), _action(), _clocks()).position
    with pytest.raises(XStockRebaseError, match="identity_content_conflict"):
        fold_xstock_corporate_action(state, _action(source_receipt_digest="d" * 64), _clocks())


def test_retry_rejects_a_corrupted_multiplier_even_with_a_valid_retained_tip() -> None:
    """An event marker alone is not proof that the retained projection is compatible.

    Given: The original event tip but a substituted multiplier observation in state.
    When: The original event is retried.
    Then: Its state mismatch refuses instead of treating the marker as authority.
    """
    state = fold_xstock_corporate_action(_position(), _action(), _clocks()).position
    altered = XStockRebasePosition.model_validate(
        state.model_dump() | {"multiplier": _after(source_record_id="substituted")}
    )
    with pytest.raises(XStockRebaseError, match="retry_state_mismatch"):
        fold_xstock_corporate_action(altered, _action(), _clocks())


def test_resolved_correction_requires_replay_from_the_prior_anchor() -> None:
    """A late correction is not a new adjustment to later holdings.

    Given: The original split is applied and a later provider correction supersedes it.
    When: Direct mutation is attempted and then the resolved stream is replayed from seed.
    Then: Direct mutation refuses while replay deterministically installs corrected evidence.
    """
    original = _action()
    applied = fold_xstock_corporate_action(_position(), original, _clocks()).position
    corrected = _action(
        public_id="00000000-0000-4000-8000-000000000099",
        kind="correction",
        supersedes_public_id=original.public_id,
        after=_after(
            public_id="00000000-0000-4000-8000-000000000098",
            multiplier="4",
            observed_at=_T3,
            known_at=_T3,
        ),
        known_at=_T3,
    )
    with pytest.raises(XStockRebaseReplayRequiredError):
        fold_xstock_corporate_action(applied, corrected, _clocks())
    replayed = replay_xstock_corporate_actions(_position(), (corrected,), _NOW)
    assert replayed.multiplier.multiplier == Decimal("4")
    assert replayed.raw_quantity == Decimal("3")
    assert replayed.quote_cost_basis == Decimal("600")
    assert replayed.last_action_public_id == corrected.public_id
    assert original.after.multiplier == Decimal("2")


def test_late_event_in_the_next_logical_slot_requires_replay() -> None:
    """An append sequence cannot make an older economic event safe to apply now.

    Given: State already at the original action's effective time.
    When: A new logical event has that same historical economic timestamp.
    Then: The chronology guard requires replay before adjusting any view.
    """
    state = fold_xstock_corporate_action(_position(), _action(), _clocks()).position
    late = _action(
        public_id="00000000-0000-4000-8000-000000000095",
        transition_sequence=2,
        kind="correction",
        supersedes_public_id=_FIRST_ACTION,
        before=state.multiplier,
        after=_after(public_id="00000000-0000-4000-8000-000000000096", multiplier="3"),
    )
    with pytest.raises(XStockRebaseReplayRequiredError, match="historical_action"):
        fold_xstock_corporate_action(state, late, _clocks())


def test_replay_is_identical_to_ordered_application_and_accepts_an_exact_tip_retry() -> None:
    """Resolved histories reproduce state without silently sorting or dropping facts.

    Given: A split followed by a separately witnessed reverse split.
    When: The same ordered source history is replayed, including one exact adjacent retry.
    Then: Both paths produce identical immutable position and action-tip content.
    """
    first = _action()
    second = _action(
        public_id="00000000-0000-4000-8000-000000000012",
        transition_sequence=2,
        kind="reverse_split",
        before=first.after,
        after=_after(
            public_id="00000000-0000-4000-8000-000000000004",
            multiplier="1",
            effective_from=_T2,
            effective_until=_T3,
            observed_at=_T2,
            known_at=_T2,
        ),
        effective_at=_T2,
        known_at=_T2,
    )
    once = fold_xstock_corporate_action(_position(), first, _clocks()).position
    twice = fold_xstock_corporate_action(once, second, _clocks(economic_at=_T2)).position
    assert replay_xstock_corporate_actions(_position(), (first, first, second), _NOW) == twice
    assert replay_xstock_corporate_actions(_position(), (), _NOW) == _position()
    with pytest.raises(XStockRebaseReplayRequiredError):
        replay_xstock_corporate_actions(_position(), (second, first), _NOW)
    with pytest.raises(XStockRebaseReplayRequiredError):
        replay_xstock_corporate_actions(_position(), (first, second, first), _NOW)


@pytest.mark.parametrize(
    ("kind", "before", "after"),
    [
        ("split", "2", "1"),
        ("split", "1", "1"),
        ("reverse_split", "1", "2"),
        ("reverse_split", "1", "1"),
        ("dividend_reinvestment", "2", "1"),
    ],
)
def test_original_event_directions_are_not_guessed(kind: str, before: str, after: str) -> None:
    """Event labels must agree with the supplied multiplier transition.

    Given: A split, reverse split or reinvestment with an incompatible ratio direction.
    When: Its proposal is folded against the matching original witness.
    Then: It refuses instead of manufacturing a plausible corporate adjustment.
    """
    state = _position(multiplier=_witness(multiplier=before))
    with pytest.raises(XStockRebaseError):
        fold_xstock_corporate_action(
            state,
            _action(kind=kind, before=state.multiplier, after=_after(multiplier=after)),
            _clocks(),
        )


@pytest.mark.parametrize("until", [_T1 - timedelta(hours=1), _T1 + timedelta(hours=1)])
def test_multiplier_history_gaps_and_overlaps_refuse(until: datetime) -> None:
    """A rebase cannot silently bridge incomplete effective-time evidence.

    Given: A predecessor observation ending before or after the new multiplier begins.
    When: The otherwise compatible action is folded.
    Then: The missing or contradictory history interval refuses.
    """
    state = _position(multiplier=_witness(effective_until=until))
    with pytest.raises(XStockRebaseError, match="gap_or_overlap"):
        fold_xstock_corporate_action(state, _action(before=state.multiplier), _clocks())


def test_action_cannot_replace_a_different_predecessor_observation() -> None:
    """Equal numeric multipliers do not erase immutable provenance differences.

    Given: State anchored to one exact predecessor witness.
    When: An action refers to another observation with equal numeric value.
    Then: The expected immutable predecessor check refuses.
    """
    other = _witness(public_id="00000000-0000-4000-8000-000000000097")
    with pytest.raises(XStockRebaseError, match="predecessor_mismatch"):
        fold_xstock_corporate_action(_position(), _action(before=other), _clocks())


@pytest.mark.parametrize(
    "changes",
    [
        {"economic_at": _T1 + timedelta(microseconds=1)},
        {"interpreted_at": _T1 - timedelta(microseconds=1), "economic_at": _T0},
    ],
)
def test_fold_requires_the_exact_action_economic_clock(changes: dict[str, datetime]) -> None:
    """A generic current clock cannot substitute for the event's effective instant.

    Given: A context naming a different economic observation.
    When: It is supplied to the corporate-action fold.
    Then: The mismatched interpretation refuses.
    """
    with pytest.raises(XStockRebaseError, match="economic_clock_mismatch"):
        fold_xstock_corporate_action(_position(), _action(), _clocks(**changes))


def test_event_knowledge_is_checked_before_an_exact_retry_is_returned() -> None:
    """An existing tip is not a bypass for unavailable source knowledge.

    Given: A correctly applied event whose knowledge is later than a proposed replay clock.
    When: That event is retried under the earlier clock.
    Then: The future-knowledge guard refuses even though its identity already exists.
    """
    event = _action(known_at=_T3)
    state = fold_xstock_corporate_action(_position(), event, _clocks()).position
    with pytest.raises(XStockRebaseError, match="not_yet_known"):
        fold_xstock_corporate_action(state, event, _clocks(interpreted_at=_T2))


@pytest.mark.parametrize(
    "updates",
    [
        {"raw_quantity": "-1"},
        {"quote_cost_basis": "-1"},
        {"raw_quantity": "0"},
        {"scope": _scope(exchange="paper")},
        {"economic_at": _T1},
        {"last_action_public_id": _FIRST_ACTION},
        {"last_action_digest": "a" * 64},
        {"action_sequence": 1},
        {"action_sequence": True},
        {
            "action_sequence": 0,
            "last_action_public_id": _FIRST_ACTION,
            "last_action_digest": "a" * 64,
        },
    ],
)
def test_invalid_position_anchors_and_tips_do_not_enter_the_fold(
    updates: dict[str, object],
) -> None:
    """A UUID or marker alone cannot establish compatible retained financial state.

    Given: Negative cash-profile holdings, inconsistent scope or an incomplete local tip.
    When: The proposed anchored position is constructed.
    Then: Validation refuses before a corporate action can affect its projection.
    """
    with pytest.raises(ValidationError):
        _position(**updates)


def test_empty_position_retains_zero_assets_cost_and_cash_through_a_split() -> None:
    """An explicit empty anchor remains empty after a view-only corporate action.

    Given: Proven zero raw quantity and zero quote cost basis.
    When: A valid split is folded.
    Then: Neither displayed exposure nor actual financial balances are created.
    """
    result = fold_xstock_corporate_action(
        _position(raw_quantity="0", quote_cost_basis="0"), _action(), _clocks()
    )
    assert result.equity_quantity_before == result.equity_quantity_after == 0
    assert (
        result.position.raw_quantity == result.position.quote_cost_basis == result.cash_delta == 0
    )


@pytest.mark.parametrize(
    "updates",
    [
        {"after": _after(scope=_scope(network="ethereum"))},
        {"after": _after(public_id=_witness().public_id)},
        {"effective_at": _T2},
        {"known_at": _T0},
        {"kind": "correction"},
        {"supersedes_public_id": _FIRST_ACTION},
        {"kind": "correction", "supersedes_public_id": _FIRST_ACTION},
    ],
)
def test_invalid_corporate_action_provenance_is_rejected(updates: dict[str, object]) -> None:
    """Corporate-action evidence must retain independent identities and honest clocks.

    Given: A cross-network transition, reused witness, future knowledge or invalid correction.
    When: The immutable proposal is constructed.
    Then: Its inconsistent source contract refuses.
    """
    with pytest.raises(ValidationError):
        _action(**updates)


def test_corporate_action_consumers_revalidate_unchecked_model_updates() -> None:
    """Frozen copies cannot turn an invalid sequence into accepted history.

    Given: An unchecked copy replacing a positive logical transition slot with zero.
    When: A hash or fold consumer receives it.
    Then: Model validation refuses before returning a content identity or state change.
    """
    invalid = _action().model_copy(update={"transition_sequence": 0})
    with pytest.raises(ValidationError):
        xstock_corporate_action_digest(invalid)
    with pytest.raises(ValidationError):
        fold_xstock_corporate_action(_position(), invalid, _clocks())
