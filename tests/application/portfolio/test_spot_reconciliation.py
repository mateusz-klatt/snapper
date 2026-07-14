"""Decision-table tests for the pure spot execution-replay evaluator."""

import json
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import cast

import pytest

import snapper.application.portfolio.spot_reconciliation as spot_module
from snapper.application.portfolio.spot_reconciliation import SpotAssetPrecisionEvidence
from snapper.application.portfolio.spot_reconciliation import SpotInstrumentIdentity
from snapper.application.portfolio.spot_reconciliation import SpotReplayBoundary
from snapper.application.portfolio.spot_reconciliation import SpotReplayExecutionRow
from snapper.application.portfolio.spot_reconciliation import evaluate
from snapper.application.portfolio.spot_reconciliation import is_effective_spot_precision_certified
from snapper.core.types import ExecutionMode
from snapper.core.types import OrderExchange
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import SpotReconciliationAnchorRow
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.messaging.schemas.data import AccountBalanceEntry
from snapper.messaging.schemas.data import PortfolioAccountState

_NOW = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
_WALLET = "00000000-0000-7000-8000-000000000101"
_ANCHOR = "00000000-0000-7000-8000-000000000301"
_INSTRUMENT = "00000000-0000-7000-8000-000000000401"
_UNSET = object()


def _account(
    balances: list[AccountBalanceEntry] | None = None,
    *,
    mode: str = "live",
) -> PortfolioAccountState:
    """Build one fresh authoritative venue balance view."""
    return PortfolioAccountState(
        session_id="00000000-0000-7000-8000-000000000501",
        sequence_id=11,
        public_id="00000000-0000-7000-8000-000000000201",
        timestamp=_NOW,
        wallet_public_id=_WALLET,
        exchange=cast(OrderExchange, "kraken"),
        mode=cast(ExecutionMode, mode),
        sync_status="observed",
        effective_status="observed",
        is_authoritative=True,
        balance_status="observed",
        position_status="not_applicable",
        valuation_status="native_only",
        balances=(
            balances
            if balances is not None
            else [
                AccountBalanceEntry(
                    currency="BTC",
                    total=1.1,
                    total_decimal="1.1",
                    numeric_provenance="venue_raw",
                ),
                AccountBalanceEntry(
                    currency="USD",
                    total=98.99,
                    total_decimal="98.99",
                    numeric_provenance="venue_raw",
                ),
            ]
        ),
        open_positions=None,
        balance_observed_at=_NOW - timedelta(seconds=1),
        position_observed_at=None,
        authoritative_until=_NOW + timedelta(minutes=5),
        current_attempt_observation_id=41,
        balance_payload_source_observation_id=41,
        position_payload_source_observation_id=None,
        error=None,
    )


def _anchor() -> SpotReconciliationAnchorRow:
    """Build a certified exact bootstrap inventory."""
    row: SpotReconciliationAnchorRow = {
        "public_id": _ANCHOR,
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "venue_account_state_public_id": "00000000-0000-7000-8000-000000000200",
        "balance_observation_id": 40,
        "source_watermark_kind": "execution_id",
        "source_watermark": 10,
        "balances_json": '{"BTC":"1","USD":"100"}',
        "first_request_started_at": _NOW - timedelta(minutes=2),
        "first_request_completed_at": _NOW - timedelta(minutes=2) + timedelta(seconds=1),
        "second_request_started_at": _NOW - timedelta(minutes=2) + timedelta(seconds=1),
        "second_request_completed_at": _NOW - timedelta(minutes=2) + timedelta(seconds=2),
        "boundary_status": "double_read_equal",
        "inventory_status": "certified_full",
        "margin_status": "cash",
        "provenance": "test",
        "session_id": "00000000-0000-7000-8000-000000000500",
        "sequence_id": 10,
        "timestamp": _NOW - timedelta(minutes=2) + timedelta(seconds=2),
    }
    return row


def _boundary(**overrides: object) -> SpotReplayBoundary:
    """Build a complete fixed range with a related venue cursor."""
    values: dict[str, object] = {
        "source_watermark": 11,
        "range_complete": True,
        "watermark_captured_before_balance": True,
        "request_started_at": _NOW - timedelta(seconds=2),
        "request_completed_at": _NOW - timedelta(seconds=1),
        "venue_cursor": "11",
        "venue_cursor_certified": True,
        "inventory_complete": True,
        "inventory_truncated": False,
    }
    values.update(overrides)
    return SpotReplayBoundary(**values)


def _execution(**overrides: object) -> SpotReplayExecutionRow:
    """Build one raw-decimal buy with a quote-asset fee."""
    values: dict[str, object] = {
        "execution_id": 11,
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "status": "filled",
        "instrument_public_id": _INSTRUMENT,
        "symbol": "BTC/USD",
        "base_asset": "BTC",
        "quote_asset": "USD",
        "side": "buy",
        "price": 10.0,
        "size": 0.1,
        "fee": 0.01,
        "fee_asset": "USD",
        "price_decimal": "10",
        "size_decimal": "0.1",
        "fee_decimal": "0.01",
        "numeric_provenance": "venue_raw",
    }
    values.update(overrides)
    return SpotReplayExecutionRow(**values)


def _spec(
    spec_observed_at: datetime | None = _NOW - timedelta(minutes=1),
) -> InstrumentSpecRow:
    """Build fresh independent spot precision evidence."""
    row: InstrumentSpecRow = {
        "instrument_public_id": _INSTRUMENT,
        "tick_size": 0.01,
        "lot_size": 0.00000001,
        "min_order_size": None,
        "max_order_size": None,
        "cost_decimals": 8,
        "qty_decimals": 8,
        "margin_initial": None,
        "position_limit_long": None,
        "position_limit_short": None,
        "status": "active",
        "contract_size": None,
        "quantity_unit": "base_asset",
        "spec_source": "kraken:ccxt.load_markets",
        "spec_version": "s3-test",
        "spec_observed_at": spec_observed_at,
        "unit_certified": False,
        "expiry_at": None,
        "instrument_kind": "spot",
        "funding_type": None,
        "funding_frequency_hours": None,
        "rollover_rate_long": None,
        "rollover_rate_short": None,
        "max_funding_rate": None,
    }
    return row


def _precision(asset: str) -> SpotAssetPrecisionEvidence:
    """Build fresh certified balance and fee precision."""
    return SpotAssetPrecisionEvidence(
        asset=asset,
        balance_decimals=8,
        fee_decimals=8,
        source="venue asset precision",
        version="s3-test",
        observed_at=_NOW - timedelta(minutes=1),
        certified=True,
    )


def _changed_anchor(**overrides: object) -> SpotReconciliationAnchorRow:
    """Return anchor evidence with intentional adversarial overrides."""
    values: dict[str, object] = dict(_anchor())
    values.update(overrides)
    return cast(SpotReconciliationAnchorRow, values)


def _changed_spec(**overrides: object) -> InstrumentSpecRow:
    """Return spot specification evidence with adversarial overrides."""
    values: dict[str, object] = dict(_spec())
    values.update(overrides)
    return cast(InstrumentSpecRow, values)


def _evaluate(
    *,
    account: PortfolioAccountState | None = None,
    anchor: SpotReconciliationAnchorRow | None | object = _UNSET,
    replay: list[SpotReplayExecutionRow] | None | object = _UNSET,
    boundary: SpotReplayBoundary | None = None,
    margin: tuple[str, ...] = (),
    liabilities: dict[str, Decimal] | None = None,
    capability: CapabilityStatus = CapabilityStatus.NOT_APPLICABLE,
    instruments: dict[str, SpotInstrumentIdentity] | None = None,
    specs: dict[str, InstrumentSpecRow | None] | None = None,
    precisions: dict[str, SpotAssetPrecisionEvidence] | None = None,
    confirmed: frozenset[str] = frozenset({"BTC", "USD"}),
    now: datetime = _NOW,
) -> PortfolioReconciliationEvaluationRow:
    """Invoke the evaluator with a complete default evidence bundle."""
    resolved_anchor = (
        _anchor() if anchor is _UNSET else cast(SpotReconciliationAnchorRow | None, anchor)
    )
    resolved_replay = (
        [_execution()] if replay is _UNSET else cast(list[SpotReplayExecutionRow] | None, replay)
    )
    return evaluate(
        anchor=resolved_anchor,
        replay=resolved_replay,
        replay_boundary=_boundary() if boundary is None else boundary,
        venue_account=_account() if account is None else account,
        instruments_by_public_id=(
            instruments
            if instruments is not None
            else {
                _INSTRUMENT: SpotInstrumentIdentity(
                    instrument_public_id=_INSTRUMENT,
                    symbol="BTC/USD",
                    base_asset="BTC",
                    quote_asset="USD",
                )
            }
        ),
        specs_by_instrument_public_id=specs if specs is not None else {_INSTRUMENT: _spec()},
        asset_precisions=(
            precisions
            if precisions is not None
            else {"BTC": _precision("BTC"), "USD": _precision("USD")}
        ),
        previously_confirmed_assets=confirmed,
        liability_totals={} if liabilities is None else liabilities,
        margin_indicators=margin,
        position_capability=capability,
        now=now,
    )


def test_exact_raw_buy_match_and_canonical_evidence() -> None:
    """Raw buy replay produces exact deltas and a fully certified match.

    Given: Exact raw operands, certified precision, inventory, and boundary evidence,
    When: The fixed execution range is replayed against matching venue totals,
    Then: A full match carries canonical exact evidence for PostgreSQL verification.
    """
    result = _evaluate()
    assert result["evaluation_status"] == "matched"
    assert result["anchor_public_id"] == _ANCHOR
    assert result["source_watermark"] == 11
    expected = json.loads(cast(str, result["expected_json"]))
    assert expected["assets"]["BTC"] == {
        "anchor_total": "1",
        "liability_delta": "0",
        "replay_delta": "0.1",
        "total": "1.1",
    }
    assert expected["assets"]["USD"]["replay_delta"] == "-1.01"
    tolerance = json.loads(cast(str, result["tolerance_json"]))
    assert Decimal(tolerance["assets"]["BTC"]["precision_floor"]) == Decimal("0.00000001")
    assert Decimal(tolerance["assets"]["USD"]["absolute_tolerance"]) == Decimal("0.001")
    assert cast(str, result["expected_json"]) == json.dumps(
        expected, allow_nan=False, separators=(",", ":"), sort_keys=True
    )


def test_cursorless_mismatch_is_full_but_cursorless_clean_is_incomplete() -> None:
    """A cursorless boundary cannot bless equality but does not hide drift.

    Given: Clean and drifting comparisons without a certified venue cursor,
    When: Both comparisons are evaluated,
    Then: Drift is full mismatch while numerical equality remains incomplete.
    """
    cursorless = _boundary(venue_cursor=None, venue_cursor_certified=False)
    mismatch_account = _account(
        [
            AccountBalanceEntry(
                currency="BTC",
                total=1.0,
                total_decimal="1",
                numeric_provenance="venue_raw",
            ),
            AccountBalanceEntry(
                currency="USD",
                total=98.99,
                total_decimal="98.99",
                numeric_provenance="venue_raw",
            ),
        ]
    )
    mismatch = _evaluate(account=mismatch_account, boundary=cursorless)
    clean = _evaluate(boundary=cursorless)
    assert mismatch["evaluation_status"] == "mismatched"
    assert mismatch["anchor_public_id"] == _ANCHOR
    assert clean["evaluation_status"] == "incomplete"
    assert clean["error"] == "uncertified_boundary"
    assert clean["anchor_public_id"] is None


def test_sell_third_asset_fee_and_legacy_terms_are_replayed() -> None:
    """Sell direction, third-asset fees, and legacy bounds remain visible.

    Given: A legacy-float sell charged in a third asset,
    When: Its cash deltas and conservative bounds are replayed,
    Then: Every asset delta and legacy accumulation term remains visible.
    """
    account = _account(
        [
            AccountBalanceEntry(currency="BTC", total=0.9),
            AccountBalanceEntry(currency="USD", total=101.0),
            AccountBalanceEntry(currency="KSM", total=-0.02),
        ]
    )
    execution = _execution(
        side="sell",
        price=10.0,
        size=0.1,
        fee=0.02,
        fee_asset="KSM",
        price_decimal=None,
        size_decimal=None,
        fee_decimal=None,
        numeric_provenance="legacy_float",
    )
    result = evaluate(
        anchor=_anchor(),
        replay=[execution],
        replay_boundary=_boundary(),
        venue_account=account,
        instruments_by_public_id={
            _INSTRUMENT: SpotInstrumentIdentity(_INSTRUMENT, "BTC/USD", "BTC", "USD")
        },
        specs_by_instrument_public_id={_INSTRUMENT: _spec()},
        asset_precisions={asset: _precision(asset) for asset in ("BTC", "USD", "KSM")},
        previously_confirmed_assets=frozenset({"BTC", "USD"}),
        liability_totals={},
        margin_indicators=(),
        position_capability=CapabilityStatus.NOT_APPLICABLE,
        now=_NOW,
    )
    assert result["evaluation_status"] == "matched"
    expected = json.loads(cast(str, result["expected_json"]))
    assert expected["assets"]["BTC"]["replay_delta"] == "-0.1"
    assert expected["assets"]["USD"]["replay_delta"] == "1"
    assert expected["assets"]["KSM"]["replay_delta"] == "-0.02"
    tolerance = json.loads(cast(str, result["tolerance_json"]))
    assert tolerance["assets"]["USD"]["legacy_term_count"] >= 2


@pytest.mark.parametrize(
    ("capability", "status", "reason"),
    [
        (CapabilityStatus.SUPPORTED, "unsupported", "futures_position_required"),
        (
            CapabilityStatus.UNSUPPORTED,
            "incomplete",
            "ambiguous_position_capability",
        ),
    ],
)
def test_capability_routing(capability: CapabilityStatus, status: str, reason: str) -> None:
    """Supported positions route away while ambiguous capability fails closed.

    Given: A non-spot structural position capability,
    When: The spot evaluator routes the account,
    Then: It returns unsupported or incomplete without full anchor lineage.
    """
    result = _evaluate(capability=capability)
    assert result["evaluation_status"] == status
    assert result["error"] == reason
    assert result["anchor_public_id"] is None


def test_margin_liability_and_inventory_tripwires_precede_cash_status() -> None:
    """Margin and wholesale inventory loss cannot become cash drift or match.

    Given: Margin, liability, empty, and single-disappearance scenarios,
    When: Each scenario is evaluated before cash status selection,
    Then: Unsafe wholesale cases are incomplete and one absence compares as zero.
    """
    margin = _evaluate(margin=("margin_borrow",))
    liability = _evaluate(liabilities={"USD": Decimal("1")})
    empty = _evaluate(account=_account([]))
    one_asset = _account(
        [
            AccountBalanceEntry(
                currency="BTC",
                total=1.1,
                total_decimal="1.1",
                numeric_provenance="venue_raw",
            )
        ]
    )
    missing_one = _evaluate(account=one_asset)
    assert margin["error"] == "unsupported_margin"
    assert liability["error"] == "unsupported_margin"
    assert empty["error"] == "suspect_partial"
    assert missing_one["evaluation_status"] == "mismatched"


def test_incomplete_replay_precision_and_raw_conflict_fail_closed() -> None:
    """Boundary, precision, and raw ambiguity never produce full truth.

    Given: An incomplete range, stale precision, and conflicting raw companion,
    When: Certification and replay validation run,
    Then: Each unsafe input fails closed with its stable reason.
    """
    incomplete_boundary = _evaluate(boundary=_boundary(range_complete=False))
    conflict = _evaluate(replay=[_execution(size=0.2, size_decimal="0.1")])
    stale_spec = _spec(spec_observed_at=_NOW - timedelta(days=1))
    assert is_effective_spot_precision_certified(_spec(), _NOW) is True
    assert is_effective_spot_precision_certified(stale_spec, _NOW) is False
    assert incomplete_boundary["error"] == "incomplete_replay_boundary"
    assert conflict["error"] == "raw_numeric_companion_conflict"


def test_paper_and_simulated_raise_and_inputs_are_not_mutated() -> None:
    """Non-live truth is rejected and caller-owned replay order is unchanged.

    Given: Permuted live replay plus paper and simulated account truth,
    When: The evaluator is invoked for each case,
    Then: Live input remains unchanged and non-live invocations raise.
    """
    replay = [_execution(execution_id=12), _execution(execution_id=11)]
    original = replay.copy()
    result = _evaluate(replay=replay, boundary=_boundary(source_watermark=12))
    assert replay == original
    assert result["evaluation_status"] in ("matched", "mismatched")
    with pytest.raises(ValueError, match="live account"):
        _evaluate(account=_account(mode="paper"))
    with pytest.raises(ValueError, match="simulated"):
        _evaluate(capability=CapabilityStatus.SIMULATED)


class _ExplodingMapping(dict[str, SpotInstrumentIdentity]):
    """Mapping boundary that exposes unexpected evaluator defect handling."""

    def get(
        self, key: str, default: SpotInstrumentIdentity | None = None
    ) -> SpotInstrumentIdentity | None:
        """Raise a deterministic unexpected failure."""
        raise RuntimeError("x" * 600)

    def __iter__(self) -> Iterator[str]:
        """Retain a concrete mapping iterator type."""
        return super().__iter__()


def test_unexpected_failure_becomes_bounded_error() -> None:
    """Unexpected defects produce a bounded non-empty error row.

    Given: An instrument mapping that raises an unexpected long failure,
    When: Cash evaluation reaches that internal boundary,
    Then: The evaluator returns a non-full error bounded to S1 limits.
    """
    result = evaluate(
        anchor=_anchor(),
        replay=[_execution()],
        replay_boundary=_boundary(),
        venue_account=_account(),
        instruments_by_public_id=_ExplodingMapping(),
        specs_by_instrument_public_id={_INSTRUMENT: _spec()},
        asset_precisions={"BTC": _precision("BTC"), "USD": _precision("USD")},
        previously_confirmed_assets=frozenset({"BTC", "USD"}),
        liability_totals={},
        margin_indicators=(),
        position_capability=CapabilityStatus.NOT_APPLICABLE,
        now=_NOW,
    )
    assert result["evaluation_status"] == "error"
    assert len(cast(str, result["error"])) == 512
    assert result["anchor_public_id"] is None


class _InvalidDecimalText:
    """Boundary object whose text cannot be parsed as a decimal."""

    def __str__(self) -> str:
        """Return deliberately malformed numeric text."""
        return "invalid"


@pytest.mark.parametrize("decimals", [None, True, -1, 257])
def test_quantum_rejects_missing_boolean_negative_and_unbounded_counts(
    decimals: int | None,
) -> None:
    """Every invalid precision count produces honest absence.

    Given: A missing, boolean, negative, or unbounded decimal count,
    When: Its quantum is derived,
    Then: Certification returns honest absence.
    """
    assert spot_module._quantum(decimals) is None


@pytest.mark.parametrize("raw", ["", " 1", "invalid", "NaN", "1e-257"])
def test_exact_decimal_parser_rejects_malformed_nonfinite_and_unbounded(
    raw: str,
) -> None:
    """Exact decimal parsing rejects every ambiguous textual boundary.

    Given: Malformed, non-finite, or unbounded decimal text,
    When: The exact boundary parser consumes it,
    Then: It raises the stable incomplete signal.
    """
    with pytest.raises(spot_module._IncompleteError):
        spot_module._finite_decimal_string(raw)


@pytest.mark.parametrize(
    ("legacy", "raw", "provenance", "reason"),
    [
        (True, None, "legacy_float", "invalid_value"),
        (_InvalidDecimalText(), None, "legacy_float", "invalid_value"),
        (float("nan"), None, "legacy_float", "invalid_value"),
        (1.0, None, "invented", "raw_numeric_provenance_conflict"),
        (1.0, "1", "legacy_float", "raw_numeric_provenance_conflict"),
        (2.0, "1", "venue_raw", "raw_numeric_companion_conflict"),
    ],
)
def test_numeric_resolution_rejects_bool_malformed_nonfinite_and_conflicts(
    legacy: object,
    raw: str | None,
    provenance: str,
    reason: str,
) -> None:
    """Raw and compatibility values cannot disagree or forge provenance.

    Given: Invalid legacy values or conflicting raw numeric evidence,
    When: One numeric operand is resolved,
    Then: The resolver rejects it with the expected incomplete reason.
    """
    with pytest.raises(spot_module._IncompleteError, match=reason):
        spot_module._resolve_number(cast(float, legacy), raw, provenance, "value")


@pytest.mark.parametrize(
    ("balances_json", "reason"),
    [
        ('{"USD":"1","USD":"2"}', "duplicate_anchor_asset"),
        ("not-json", "malformed_anchor_balances"),
        ("{}", "empty_or_malformed_anchor"),
        ('{"USD":1}', "non_string_anchor_balance"),
        ('{" USD":"1"}', "invalid_anchor_asset"),
        ('{"USD":"1.0"}', "noncanonical_anchor_balances"),
    ],
)
def test_anchor_balance_parser_rejects_ambiguous_or_noncanonical_payloads(
    balances_json: str,
    reason: str,
) -> None:
    """Anchor inventory remains a unique canonical exact-decimal object.

    Given: Duplicate, malformed, empty, or noncanonical anchor JSON,
    When: The bootstrap inventory is evaluated,
    Then: The account is incomplete with a stable parser reason.
    """
    result = _evaluate(anchor=_changed_anchor(balances_json=balances_json))
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == reason


def test_duplicate_and_invalid_venue_assets_fail_closed() -> None:
    """Venue paging duplication and invalid asset identities are ambiguous.

    Given: Duplicate and empty venue currency rows,
    When: Venue TOTAL balances are resolved,
    Then: Both payloads fail closed without summing or dropping rows.
    """
    duplicate = AccountBalanceEntry(currency="USD", total=1.0)
    duplicate_result = _evaluate(account=_account([duplicate, duplicate]))
    invalid_result = _evaluate(account=_account([AccountBalanceEntry(currency="", total=1.0)]))
    assert duplicate_result["error"] == "duplicate_venue_asset"
    assert invalid_result["error"] == "invalid_venue_asset"


@pytest.mark.parametrize(
    "spec",
    [
        _changed_spec(tick_size=None),
        _changed_spec(tick_size=True),
        _changed_spec(spec_observed_at=None),
        _changed_spec(spec_observed_at=datetime(2026, 7, 14, 11, 0)),
        _changed_spec(tick_size=float("nan")),
        _changed_spec(tick_size=0.0),
        _changed_spec(instrument_kind="future"),
        _changed_spec(quantity_unit="contract_count"),
        _changed_spec(status="inactive"),
        _changed_spec(qty_decimals=None),
        _changed_spec(cost_decimals=None),
        _changed_spec(spec_source=None),
        _changed_spec(spec_version=None),
        _changed_spec(spec_observed_at=_NOW + timedelta(seconds=1)),
    ],
)
def test_spot_precision_predicate_rejects_each_uncertified_component(
    spec: InstrumentSpecRow,
) -> None:
    """Spot precision certification requires every independent component.

    Given: A specification missing or corrupting one required component,
    When: Effective spot precision is checked,
    Then: Certification is false.
    """
    assert is_effective_spot_precision_certified(spec, _NOW) is False


@pytest.mark.parametrize(
    "evidence",
    [
        None,
        replace(_precision("BTC"), asset="XBT"),
        replace(_precision("BTC"), certified=False),
        replace(_precision("BTC"), observed_at=None),
        replace(_precision("BTC"), observed_at=datetime(2026, 7, 14, 11, 0)),
        replace(_precision("BTC"), observed_at=_NOW + timedelta(seconds=1)),
        replace(_precision("BTC"), source=None),
        replace(_precision("BTC"), version=None),
        replace(_precision("BTC"), balance_decimals=None),
        replace(_precision("BTC"), fee_decimals=None),
    ],
)
def test_asset_precision_rejects_missing_stale_or_incomplete_evidence(
    evidence: SpotAssetPrecisionEvidence | None,
) -> None:
    """Every balance and fee precision ambiguity fails closed.

    Given: Missing, stale, foreign, or incomplete asset precision evidence,
    When: The account comparison requests that asset precision,
    Then: The evaluation is incomplete with a certification reason.
    """
    precisions = {"BTC": evidence} if evidence is not None else {}
    result = _evaluate(precisions=precisions)
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] in (
        "missing_asset_precision",
        "stale_or_uncertified_asset_precision",
    )


@pytest.mark.parametrize(
    "update",
    [
        {"effective_status": "stale"},
        {"is_authoritative": False},
        {"balance_status": "error"},
        {"balances": None},
        {"public_id": ""},
        {"wallet_public_id": ""},
        {"session_id": ""},
        {"current_attempt_observation_id": None},
        {"balance_payload_source_observation_id": 40},
        {"balance_observed_at": None},
        {"balance_observed_at": datetime(2026, 7, 14, 11, 0)},
        {"balance_observed_at": _NOW + timedelta(seconds=1)},
        {"authoritative_until": None},
        {"authoritative_until": datetime(2026, 7, 14, 13, 0)},
        {"authoritative_until": _NOW - timedelta(seconds=1)},
    ],
)
def test_incoherent_or_stale_account_components_are_incomplete(
    update: dict[str, object],
) -> None:
    """Every required venue balance provenance component is fail-closed.

    Given: A venue account view with one stale or incoherent component,
    When: Balance authority is validated,
    Then: The account produces only an incomplete evaluation.
    """
    result = _evaluate(account=_account().model_copy(update=update))
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == "incomplete_venue_account"


@pytest.mark.parametrize(
    ("anchor", "reason"),
    [
        (None, "missing_anchor"),
        (_changed_anchor(public_id=""), "missing_anchor"),
        (_changed_anchor(wallet_public_id="other"), "foreign_anchor"),
        (_changed_anchor(exchange="other"), "foreign_anchor"),
        (_changed_anchor(mode="paper"), "foreign_anchor"),
        (_changed_anchor(source_watermark_kind="venue_event_id"), "invalid_anchor_watermark"),
        (_changed_anchor(source_watermark=-1), "invalid_anchor_watermark"),
        (_changed_anchor(source_watermark=12), "invalid_anchor_watermark"),
        (_changed_anchor(venue_account_state_public_id=""), "malformed_anchor"),
        (_changed_anchor(balance_observation_id=0), "malformed_anchor"),
        (_changed_anchor(session_id=""), "malformed_anchor"),
        (_changed_anchor(provenance=""), "malformed_anchor"),
        (_changed_anchor(boundary_status="invented"), "malformed_anchor"),
        (_changed_anchor(inventory_status="invented"), "malformed_anchor"),
        (_changed_anchor(margin_status="invented"), "malformed_anchor"),
        (_changed_anchor(inventory_status="suspect_partial"), "suspect_partial"),
        (_changed_anchor(margin_status="unknown"), "unsupported_margin"),
        (
            _changed_anchor(first_request_started_at=datetime(2026, 7, 14, 10, 0)),
            "invalid_anchor_boundary",
        ),
        (
            _changed_anchor(first_request_completed_at=_NOW - timedelta(minutes=3)),
            "invalid_anchor_boundary",
        ),
    ],
)
def test_anchor_scope_status_and_boundary_fail_closed(
    anchor: SpotReconciliationAnchorRow | None,
    reason: str,
) -> None:
    """Malformed, foreign, unstable, or margin anchors never support truth.

    Given: An anchor with one invalid scope, status, or boundary component,
    When: Bootstrap lineage is validated,
    Then: The evaluation is incomplete with the expected reason.
    """
    result = _evaluate(anchor=anchor)
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == reason


@pytest.mark.parametrize(
    ("boundary", "reason"),
    [
        (_boundary(source_watermark=True), "incomplete_replay_boundary"),
        (_boundary(source_watermark=-1), "incomplete_replay_boundary"),
        (_boundary(watermark_captured_before_balance=False), "incomplete_replay_boundary"),
        (
            _boundary(request_started_at=datetime(2026, 7, 14, 11, 0)),
            "invalid_balance_request_boundary",
        ),
        (
            _boundary(request_completed_at=datetime(2026, 7, 14, 11, 0)),
            "invalid_balance_request_boundary",
        ),
        (
            _boundary(
                request_started_at=_NOW - timedelta(seconds=1),
                request_completed_at=_NOW - timedelta(seconds=2),
            ),
            "invalid_balance_request_boundary",
        ),
        (
            _boundary(request_completed_at=_NOW + timedelta(seconds=1)),
            "invalid_balance_request_boundary",
        ),
        (
            _boundary(venue_cursor=None, venue_cursor_certified=True),
            "invalid_venue_cursor_certificate",
        ),
    ],
)
def test_replay_boundary_rejects_bad_ordering_clocks_and_cursor_certificate(
    boundary: SpotReplayBoundary,
    reason: str,
) -> None:
    """The fixed range must be ordered, aware, and honestly certified.

    Given: A boundary with invalid watermark, clocks, ordering, or cursor evidence,
    When: The replay boundary is validated,
    Then: It fails closed before any cash comparison.
    """
    result = _evaluate(boundary=boundary)
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == reason


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (_execution(execution_id=True), "duplicate_or_invalid_execution_id"),
        (_execution(execution_id=10), "execution_before_anchor_watermark"),
        (_execution(wallet_public_id="other"), "execution_scope_mismatch"),
        (_execution(exchange="other"), "execution_scope_mismatch"),
        (_execution(mode="paper"), "execution_scope_mismatch"),
        (_execution(status="open"), "invalid_execution_status"),
        (_execution(instrument_public_id="unknown"), "unresolved_or_conflicting_instrument"),
        (_execution(symbol="ETH/USD"), "unresolved_or_conflicting_instrument"),
        (_execution(base_asset="USD"), "unresolved_or_conflicting_instrument"),
        (_execution(quote_asset="BTC"), "unresolved_or_conflicting_instrument"),
        (_execution(base_asset="BTC", quote_asset="BTC"), "unresolved_or_conflicting_instrument"),
        (_execution(price=0.0, price_decimal="0"), "invalid_execution_economics"),
        (_execution(size=-1.0, size_decimal="-1"), "invalid_execution_economics"),
        (_execution(fee=-1.0, fee_decimal="-1"), "invalid_execution_economics"),
        (_execution(side="hold"), "invalid_execution_side"),
        (_execution(fee_asset="", fee=0.01), "missing_fee_asset"),
    ],
)
def test_invalid_execution_identity_scope_and_economics_fail_closed(
    row: SpotReplayExecutionRow,
    reason: str,
) -> None:
    """No invalid durable fill is skipped into an authoritative comparison.

    Given: One execution with invalid identity, scope, status, or economics,
    When: The fixed range is replayed,
    Then: The row makes the account incomplete with a stable reason.
    """
    result = _evaluate(replay=[row])
    assert result["evaluation_status"] == "incomplete"
    assert result["error"] == reason


def test_execution_duplicates_missing_specs_and_beyond_watermark_handling() -> None:
    """Duplicates and missing specs fail while later executions are ignored.

    Given: Duplicate, uncertified, later, and zero-fee replay variants,
    When: Their bounded ranges are evaluated,
    Then: Ambiguity fails closed and out-of-range rows do not enter replay.
    """
    duplicate = _evaluate(replay=[_execution(), _execution()])
    missing_spec = _evaluate(specs={})
    mismatched_spec = _evaluate(specs={_INSTRUMENT: _changed_spec(instrument_public_id="other")})
    later = _evaluate(replay=[_execution(execution_id=12)], boundary=_boundary())
    zero_fee = _evaluate(replay=[_execution(fee=0.0, fee_decimal="0", fee_asset="")])
    assert duplicate["error"] == "duplicate_or_invalid_execution_id"
    assert missing_spec["error"] == "missing_spot_precision"
    assert mismatched_spec["error"] == "missing_spot_precision"
    assert later["evaluation_status"] == "mismatched"
    assert zero_fee["evaluation_status"] == "mismatched"


def test_instrument_alias_collision_and_uncertified_spec_fail_closed() -> None:
    """Same-asset pairs and stale instrument precision are incomplete.

    Given: A canonical alias collision and an inactive precision specification,
    When: Instrument evidence is validated,
    Then: Neither case can produce full reconciliation truth.
    """
    same_asset_identity = SpotInstrumentIdentity(_INSTRUMENT, "BTC/BTC", "BTC", "BTC")
    collision = _evaluate(
        replay=[_execution(symbol="BTC/BTC", base_asset="BTC", quote_asset="BTC")],
        instruments={_INSTRUMENT: same_asset_identity},
    )
    stale = _evaluate(specs={_INSTRUMENT: _changed_spec(status="inactive")})
    assert collision["error"] == "asset_alias_collision"
    assert stale["error"] == "stale_or_uncertified_spot_precision"


def test_inventory_precision_and_liability_edge_cases_fail_closed() -> None:
    """Inventory, liability, and completeness edge cases fail closed.

    Given: Truncation, wholesale loss, invalid assets, and uncertified clean states,
    When: Inventory and liability tripwires run,
    Then: Each account remains incomplete for its stable reason.
    """
    truncated = _evaluate(boundary=_boundary(inventory_truncated=True))
    two_missing = _evaluate(
        account=_account(
            [
                AccountBalanceEntry(
                    currency="KSM",
                    total=1.0,
                    total_decimal="1",
                    numeric_provenance="venue_raw",
                )
            ]
        ),
        precisions={
            "BTC": _precision("BTC"),
            "USD": _precision("USD"),
            "KSM": _precision("KSM"),
        },
    )
    invalid_confirmed = _evaluate(confirmed=frozenset({""}))
    invalid_liability = _evaluate(liabilities={"USD": Decimal("NaN")})
    uncertified_inventory = _evaluate(boundary=_boundary(inventory_complete=False))
    uncertified_anchor = _evaluate(anchor=_changed_anchor(inventory_status="uncertified"))
    assert truncated["error"] == "suspect_partial"
    assert two_missing["error"] == "suspect_partial"
    assert invalid_confirmed["error"] == "invalid_confirmed_asset"
    assert invalid_liability["error"] == "unsupported_margin"
    assert uncertified_inventory["error"] == "uncertified_inventory"
    assert uncertified_anchor["error"] == "uncertified_inventory"


def test_missing_replay_and_unknown_capability_are_not_authoritative() -> None:
    """Unavailable ranges are incomplete and forged capabilities are rejected.

    Given: Honest replay absence and a forged capability value,
    When: The evaluator routes both invocations,
    Then: Absence is incomplete and the forged enum raises.
    """
    missing = _evaluate(replay=None)
    assert missing["error"] == "missing_replay"
    with pytest.raises(ValueError, match="unknown spot position capability"):
        _evaluate(capability=cast(CapabilityStatus, "invented"))


def test_defensive_precision_parsing_handles_invalid_string_object() -> None:
    """A float-typed object with invalid text cannot certify precision.

    Given: An adversarial tick object whose string is not numeric,
    When: Spot precision certification parses the tick,
    Then: The predicate returns false without leaking an exception.
    """
    spec = _changed_spec(tick_size=cast(float, _InvalidDecimalText()))
    assert is_effective_spot_precision_certified(spec, _NOW) is False


def test_zero_fee_asset_and_zero_liability_remain_in_asset_union() -> None:
    """Zero evidence remains in the asset union without changing totals.

    Given: A named zero-fee asset and an explicit zero liability,
    When: The complete cash comparison is serialized,
    Then: The asset remains present with an exact zero total.
    """
    zero_fee = _execution(fee=0.0, fee_decimal="0", fee_asset="KSM")
    result = _evaluate(
        replay=[zero_fee],
        account=_account(
            [
                AccountBalanceEntry(
                    currency="BTC",
                    total=1.1,
                    total_decimal="1.1",
                    numeric_provenance="venue_raw",
                ),
                AccountBalanceEntry(
                    currency="USD",
                    total=99.0,
                    total_decimal="99",
                    numeric_provenance="venue_raw",
                ),
            ]
        ),
        liabilities={"KSM": Decimal(0)},
        precisions={
            "BTC": _precision("BTC"),
            "USD": _precision("USD"),
            "KSM": _precision("KSM"),
        },
    )
    assert result["evaluation_status"] == "matched"
    expected = json.loads(cast(str, result["expected_json"]))
    assert expected["assets"]["KSM"]["total"] == "0"


def test_invalid_zero_liability_type_reaches_cash_validation() -> None:
    """A false-zero non-Decimal liability fails exact cash arithmetic.

    Given: A numerically zero liability with the wrong runtime type,
    When: Exact liability validation runs,
    Then: The account is incomplete for invalid liability evidence.
    """
    liabilities = cast(dict[str, Decimal], {"KSM": 0})
    result = _evaluate(liabilities=liabilities)
    assert result["error"] == "invalid_liability"


def test_fee_tolerance_skips_unrelated_replay_rows() -> None:
    """Per-asset fee bounds ignore rows charged in another asset.

    Given: Two replay rows whose fee assets differ,
    When: Each asset's legacy fee bound is accumulated,
    Then: Unrelated rows are skipped and comparison remains deterministic.
    """
    result = _evaluate(
        replay=[
            _execution(),
            _execution(
                execution_id=12,
                size=0.01,
                size_decimal="0.01",
                fee=0.0,
                fee_decimal="0",
                fee_asset="KSM",
            ),
        ],
        boundary=_boundary(source_watermark=12),
        precisions={
            "BTC": _precision("BTC"),
            "USD": _precision("USD"),
            "KSM": _precision("KSM"),
        },
    )
    assert result["evaluation_status"] == "mismatched"


def test_defensive_unreachable_precision_and_tolerance_guards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive guards survive forged upstream certification predicates.

    Given: Monkeypatched predicates that admit impossible precision states,
    When: Downstream precision and tolerance guards execute,
    Then: Every forged state still returns incomplete rather than matched.
    """
    monkeypatch.setattr(spot_module, "is_effective_spot_precision_certified", lambda *_: True)
    invalid_spec = _evaluate(specs={_INSTRUMENT: _changed_spec(qty_decimals=None)})
    assert invalid_spec["error"] == "invalid_spot_precision"

    monkeypatch.setattr(
        spot_module,
        "_asset_precision",
        lambda *_: replace(_precision("BTC"), balance_decimals=None),
    )
    invalid_balance = _evaluate()
    assert invalid_balance["error"] == "invalid_balance_precision"

    monkeypatch.setattr(
        spot_module,
        "_asset_precision",
        lambda asset, *_: replace(_precision(asset), fee_decimals=None),
    )
    invalid_fee = _evaluate()
    assert invalid_fee["error"] == "invalid_fee_precision"

    monkeypatch.setattr(spot_module, "_add_floor", lambda *_: None)
    zero_tolerance = _evaluate(
        replay=[],
        account=_account(
            [
                AccountBalanceEntry(
                    currency="BTC",
                    total=1.0,
                    total_decimal="1",
                    numeric_provenance="venue_raw",
                ),
                AccountBalanceEntry(
                    currency="USD",
                    total=100.0,
                    total_decimal="100",
                    numeric_provenance="venue_raw",
                ),
            ]
        ),
    )
    assert zero_tolerance["error"] == "zero_or_missing_tolerance"


def test_defensive_empty_union_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    """A forged parser result cannot turn an empty comparison into a match.

    Given: Forged anchor and venue parsers returning empty inventories,
    When: The evaluator builds its comparison union,
    Then: The empty union fails closed as incomplete.
    """
    monkeypatch.setattr(spot_module, "_anchor_balances", lambda *_: {})
    monkeypatch.setattr(spot_module, "_venue_balances", lambda *_: ({}, {}))
    result = _evaluate(replay=[], confirmed=frozenset())
    assert result["error"] == "empty_inventory"
