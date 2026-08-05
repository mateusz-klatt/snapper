"""Contracts for the read-only F6 activation report and alarm decision."""

import json
import math
import re
from collections.abc import Iterator
from dataclasses import asdict
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from snapper.application.portfolio.basket_realizability import VenueOrderMinimumVersion
from snapper.application.portfolio.pnl_snapshotter import resolve_spot_venues
from snapper.application.process_manager.executor_topology import ExecutorTopology
from snapper.cli.app import app
from snapper.cli.pnl_minimum_activation_gate import ActivationGateReport
from snapper.cli.pnl_minimum_activation_gate import ActivationGateRequest
from snapper.cli.pnl_minimum_activation_gate import GateBasketMinute
from snapper.cli.pnl_minimum_activation_gate import GateCommandRequest
from snapper.cli.pnl_minimum_activation_gate import GatePurpose
from snapper.cli.pnl_minimum_activation_gate import LiveWalletScopeEvidence
from snapper.cli.pnl_minimum_activation_gate import ScopeCredentialEvidence
from snapper.cli.pnl_minimum_activation_gate import _minute_impact
from snapper.cli.pnl_minimum_activation_gate import _run_configured_report
from snapper.cli.pnl_minimum_activation_gate import build_activation_report
from snapper.cli.pnl_minimum_activation_gate import evaluate_activation_threshold
from snapper.cli.pnl_minimum_activation_gate import index_usd_closes
from snapper.cli.pnl_minimum_activation_gate import reconstruct_observation_attempts
from snapper.cli.pnl_minimum_activation_gate import report_as_json
from snapper.cli.pnl_minimum_activation_gate import resolve_live_wallet_scope
from snapper.cli.pnl_minimum_activation_gate import run_activation_report
from snapper.cli.pnl_minimum_activation_gate import select_best_usd_close
from snapper.data.repository import Repository
from snapper.data.repository_types import PnlCryptoUsdPlaneRow
from snapper.data.repository_types import PnlVenueOrderMinimumVersionRow
from snapper.data.repository_types import PortfolioPnlAnchorRow
from snapper.data.repository_types import PortfolioPnlSampleRow
from snapper.data.repository_types import VenueAccountObservationAttemptRow
from snapper.data.repository_types import WalletCredentialRow

_AS_OF = datetime(2026, 8, 2, 12, 5, tzinfo=UTC)
_MINUTE = datetime(2026, 8, 2, 12, 3, tzinfo=UTC)


def _scope_evidence() -> LiveWalletScopeEvidence:
    """Build non-secret scope provenance for pure report tests."""
    return LiveWalletScopeEvidence(
        credential_catalog_as_of=_AS_OF,
        active_credential_catalog_size=1,
        resolved_mint_wallet_public_id=None,
        credentials=(
            ScopeCredentialEvidence(
                public_id="credential",
                exchange="kraken",
                credential_type="api_key",
                executor_disposition="run",
                spot_eligible=True,
                included_in_denominator=True,
            ),
        ),
    )


def _request(
    *,
    window_minutes: int = 1,
    purpose: GatePurpose = "inspect",
) -> ActivationGateRequest:
    """Build one deterministic finalized-grid request."""
    return ActivationGateRequest(
        wallet_public_id="wallet",
        mode="live",
        exchanges=("kraken",),
        as_of=_AS_OF,
        window_minutes=window_minutes,
        purpose=purpose,
        scope_evidence=_scope_evidence(),
    )


def _minimum(
    currency: str = "TRUMP",
    threshold: float = 3.0,
) -> VenueOrderMinimumVersion:
    """Build one qualifying temporal minimum proof."""
    return VenueOrderMinimumVersion(
        exchange="kraken",
        currency=currency,
        role="base",
        asset_type="crypto",
        instrument_public_id=f"instrument-{currency}",
        symbol_public_id=f"symbol-{currency}",
        native_symbol=f"{currency}-USD",
        counter_currency="USD",
        instrument_kind="spot",
        quantity_unit="base_asset",
        status="active",
        spec_public_id=f"spec-{currency}",
        spec_source="kraken",
        spec_version="v1",
        spec_observed_at=_MINUTE - timedelta(days=1),
        min_order_size=threshold,
        can_trade=True,
        identity_conflicted=False,
        minimum_unstable=False,
        valid_from=_MINUTE - timedelta(days=1),
        valid_to=_MINUTE + timedelta(days=1),
    )


def _close(
    close: float = 4.0,
    *,
    currency: str = "TRUMP",
    instrument: str = "price-instrument",
    open_at: datetime | None = None,
    candle_id: int = 1,
) -> PnlCryptoUsdPlaneRow:
    """Build one direct certified crypto-to-USD close."""
    minute = _MINUTE - timedelta(minutes=1) if open_at is None else open_at
    return {
        "base": currency,
        "quote": "USD",
        "exchange": "kraken",
        "native_symbol": f"{currency}-USD",
        "instrument_public_id": instrument,
        "candle_id": candle_id,
        "candle_public_id": f"candle-{candle_id}",
        "open_at": minute,
        "close": close,
        "candle_timestamp": minute + timedelta(seconds=30),
    }


def _sample(equity: float, minute: datetime = _MINUTE) -> PortfolioPnlSampleRow:
    """Build the minimum complete sample shape consumed by the report."""
    return {
        "public_id": "sample",
        "session_id": "session",
        "sequence_id": 1,
        "timestamp": minute,
        "wallet_public_id": "wallet",
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": minute,
        "point_kind": "sample",
        "epoch_public_id": "epoch",
        "calc_version": "5B.1",
        "valuation_status": "complete",
        "realized_pnl": 0.0,
        "fee_pnl": 0.0,
        "accrual_pnl": 0.0,
        "external_flow_adjustment": 0.0,
        "unrealized_pnl": 0.0,
        "cash_usd": equity / 2.0,
        "position_value_usd": equity / 2.0,
        "drawdown": 0.0,
        "mark_source": "1m_close",
        "mark_time": minute,
        "audit_json": "{}",
        "watermarks_json": "{}",
    }


def _basket(
    balances: dict[tuple[str, str], float] | None,
    *,
    minute: datetime = _MINUTE,
    reasons: tuple[str, ...] = (),
) -> GateBasketMinute:
    """Build one report input minute."""
    return GateBasketMinute(minute=minute, balances=balances, reason_codes=reasons)


def _minimum_row(minimum: VenueOrderMinimumVersion | None = None) -> PnlVenueOrderMinimumVersionRow:
    """Project one pure minimum proof into its repository row shape."""
    value = _minimum() if minimum is None else minimum
    return cast(PnlVenueOrderMinimumVersionRow, asdict(value))


def _attempt(minute: datetime, *, observed: bool = True) -> VenueAccountObservationAttemptRow:
    """Build one latest venue-account attempt for a report minute."""
    return {
        "id": 1,
        "public_id": "observation",
        "wallet_public_id": "wallet",
        "exchange": "kraken",
        "mode": "live",
        "attempt_status": "success" if observed else "failed",
        "balance_status": "observed" if observed else "failed",
        "position_status": "observed",
        "balances_json": (
            '[{"currency":"TRUMP","total":1.0},{"currency":"USD","total":100.0}]'
            if observed
            else None
        ),
        "open_positions_json": "[]",
        "balance_observed_at": minute - timedelta(seconds=30) if observed else None,
        "position_observed_at": minute - timedelta(seconds=30),
        "error": None if observed else "poll failed",
        "timestamp": minute - timedelta(seconds=15),
        "session_id": "session",
        "sequence_id": 1,
    }


def _credential(
    exchange: str,
    *,
    wallet: str = "wallet",
    credential_type: str = "api_key",
    public_id: str | None = None,
) -> WalletCredentialRow:
    """Build one active credential while keeping a recognizable secret payload."""
    return {
        "public_id": public_id or f"credential-{wallet}-{exchange}",
        "wallet_public_id": wallet,
        "exchange": exchange,
        "credential_type": credential_type,
        "encrypted_payload": "DO_NOT_SERIALIZE_SECRET_PAYLOAD",
        "label": "DO_NOT_SERIALIZE_LABEL",
        "timestamp": _AS_OF - timedelta(days=1),
        "session_id": "session",
        "sequence_id": 1,
    }


def _anchor() -> PortfolioPnlAnchorRow:
    """Build the active P&L anchor needed to scope complete samples."""
    return {
        "public_id": "anchor",
        "session_id": "session",
        "sequence_id": 1,
        "timestamp": _MINUTE - timedelta(days=2),
        "wallet_public_id": "wallet",
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": _MINUTE - timedelta(days=2),
        "point_kind": "anchor",
        "epoch_public_id": "epoch",
        "calc_version": "5B.1",
        "valuation_status": "complete",
        "realized_pnl": 0.0,
        "fee_pnl": 0.0,
        "accrual_pnl": 0.0,
        "unrealized_pnl": None,
        "external_flow_adjustment": 0.0,
        "cash_usd": None,
        "position_value_usd": None,
        "drawdown": None,
        "mark_source": None,
        "mark_time": None,
        "watermarks_json": None,
        "opening_basket_json": None,
        "contributions_json": None,
    }


@pytest.fixture
def repository() -> MagicMock:
    """Provide a repository double with explicit async report reads."""
    return MagicMock(spec=Repository)


@pytest.fixture
def runner() -> CliRunner:
    """Provide an isolated Typer runner."""
    return CliRunner()


_ANSI_STYLE = re.compile(r"\x1b\[[0-9;]*m")
"""Matches one ANSI SGR sequence in captured CLI output."""


def _unstyled(text: str) -> str:
    """Strip ANSI styling before matching against Typer's own error text.

    Typer renders usage errors through Rich, which highlights the offending
    option and emits it as SEPARATELY STYLED RUNS: with colour on, ``--as-of``
    leaves the renderer as an SGR sequence, then ``-``, then another sequence,
    then ``-as``, then another, then ``-of``.
    The literal substring is therefore absent from the raw capture, and an
    assertion against it passes wherever colour is off and fails wherever it
    is on. That is exactly how this test passed locally and failed in CI,
    taking the rest of the test body — and the coverage of every refusal path
    it exercises — down with it.

    Only Typer's OWN messages need this. Our refusals go through plain
    ``typer.echo`` and are never styled.

    Args:
        text: Raw captured stdout or stderr.

    Returns:
        The same text with every SGR sequence removed.
    """
    return _ANSI_STYLE.sub("", text)


def test_request_refuses_an_unbounded_or_ambiguous_window() -> None:
    """A report must name a real venue and a bounded aware UTC horizon.

    Given: Empty or malformed scope and temporal request fields.
    When: The immutable activation request validates its construction.
    Then: Every ambiguous or unbounded shape is refused with a typed error.
    """
    with pytest.raises(ValueError, match="exchange"):
        ActivationGateRequest(
            wallet_public_id="wallet",
            mode="live",
            exchanges=(),
            as_of=_AS_OF,
            window_minutes=60,
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        ActivationGateRequest(
            wallet_public_id="wallet",
            mode="live",
            exchanges=("kraken",),
            as_of=_AS_OF.replace(tzinfo=None),
            window_minutes=60,
        )
    with pytest.raises(ValueError, match="window_minutes"):
        ActivationGateRequest(
            wallet_public_id="wallet",
            mode="live",
            exchanges=("kraken",),
            as_of=_AS_OF,
            window_minutes=0,
        )
    with pytest.raises(ValueError, match="mode"):
        ActivationGateRequest(
            wallet_public_id="wallet",
            mode="invalid",
            exchanges=("kraken",),
            as_of=_AS_OF,
            window_minutes=60,
        )
    with pytest.raises(ValueError, match="wallet"):
        ActivationGateRequest(
            wallet_public_id="",
            mode="live",
            exchanges=("kraken",),
            as_of=_AS_OF,
            window_minutes=60,
        )


def test_request_normalizes_repeated_venues_and_builds_finalized_grid() -> None:
    """The read is deterministic and ends two minutes behind its horizon.

    Given: Repeated venues and a horizon carrying seconds.
    When: The request exposes its canonical finalized grid.
    Then: Venues are deduplicated and exactly three ascending minutes are returned.
    """
    request = ActivationGateRequest(
        wallet_public_id="wallet",
        mode="live",
        exchanges=("kraken", "kraken", "walutomat"),
        as_of=_AS_OF + timedelta(seconds=51),
        window_minutes=3,
    )
    assert request.exchanges == ("kraken", "walutomat")
    assert request.minutes == (
        datetime(2026, 8, 2, 12, 1, tzinfo=UTC),
        datetime(2026, 8, 2, 12, 2, tzinfo=UTC),
        datetime(2026, 8, 2, 12, 3, tzinfo=UTC),
    )
    assert math.isfinite(request.as_of.timestamp())


def test_activation_and_standing_requests_enforce_duration_contracts() -> None:
    """Activation is exactly 1440 minutes and standing checks cannot be shorter.

    Given: Activation and standing request purposes around the 24-hour boundary.
    When: Their immutable request contracts validate 1439 and 1440 minutes.
    Then: Only exact activation and at-least-24-hour standing windows are accepted.
    """
    with pytest.raises(ValueError, match="exactly 1440"):
        _request(window_minutes=1439, purpose="activation")
    assert _request(window_minutes=1440, purpose="activation").window_minutes == 1440
    with pytest.raises(ValueError, match="at least 1440"):
        _request(window_minutes=1439, purpose="standing")
    assert _request(window_minutes=1440, purpose="standing").window_minutes == 1440
    assert _request(window_minutes=60, purpose="inspect").purpose == "inspect"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"wallet_public_id": ""}, "wallet_public_id"),
        ({"as_of": _AS_OF.replace(tzinfo=None)}, "timezone-aware"),
        ({"window_minutes": 0}, "window_minutes"),
        ({"window_minutes": 1439, "purpose": "activation"}, "exactly 1440"),
    ],
)
def test_command_request_refuses_every_ambiguous_pre_scope_shape(
    overrides: dict[str, object],
    message: str,
) -> None:
    """Pre-scope validation rejects malformed identity, time, duration, and purpose.

    Given: One malformed field in an otherwise valid command request.
    When: The immutable pre-scope request validates the input.
    Then: It raises the exact refusal before any repository access.
    """
    values: dict[str, object] = {
        "wallet_public_id": "wallet",
        "as_of": _AS_OF,
        "window_minutes": 60,
        "purpose": "inspect",
    }
    values.update(overrides)
    with pytest.raises(ValueError, match=message):
        GateCommandRequest(
            wallet_public_id=cast(str, values["wallet_public_id"]),
            as_of=cast(datetime, values["as_of"]),
            window_minutes=cast(int, values["window_minutes"]),
            purpose=cast(GatePurpose, values["purpose"]),
        )


def test_observation_cursor_reconstructs_point_read_semantics() -> None:
    """One bounded stream reconstructs every latest-at-minute point read exactly.

    Given: An opening seed, a later failure, and a same-time higher-id recovery.
    When: The pure cursor folds them over a three-minute grid.
    Then: Every exchange winner equals an independent point-read reconstruction.
    """
    request = _request(window_minutes=3)
    first, second, third = request.minutes
    seed = _attempt(first)
    seed["timestamp"] = first - timedelta(minutes=5)
    failed = _attempt(second, observed=False)
    failed["id"] = 2
    failed["timestamp"] = second
    recovered = _attempt(second)
    recovered["id"] = 3
    recovered["timestamp"] = second
    rows = [recovered, seed, failed]
    reconstructed = reconstruct_observation_attempts(request, rows)
    expected = []
    for minute in (first, second, third):
        candidates = [row for row in rows if row["timestamp"] <= minute]
        winner = max(candidates, key=lambda row: (row["timestamp"], row["id"]))
        expected.append({"kraken": winner})
    assert reconstructed == tuple(expected)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("wallet_public_id", "foreign", "foreign wallet"),
        ("mode", "paper", "foreign mode"),
        ("exchange", "foreign", "unexpected exchange"),
        ("timestamp", _MINUTE + timedelta(minutes=1), "beyond"),
    ],
)
def test_observation_cursor_refuses_foreign_or_out_of_window_rows(
    field: str,
    value: object,
    message: str,
) -> None:
    """Repository contract drift fails closed before cursor reconstruction.

    Given: One stream row outside the requested wallet, mode, venue, or horizon.
    When: The pure reconstruction boundary validates the stream.
    Then: It refuses the foreign evidence instead of folding it into a basket.
    """
    row = _attempt(_MINUTE)
    cast(dict[str, object], row)[field] = value
    with pytest.raises(ValueError, match=message):
        reconstruct_observation_attempts(_request(), [row])


@pytest.mark.asyncio
async def test_live_scope_matches_snapshotter_topology_and_omits_secrets(
    repository: MagicMock,
) -> None:
    """Gate venue authority is the snapshotter denominator with safe provenance.

    Given: Spot, futures, paper, and another wallet's active credentials.
    When: The gate resolves its live wallet scope from the complete catalogue.
    Then: Venues match ``resolve_spot_venues`` and evidence contains no payload or label.
    """
    credentials = [
        _credential("kraken"),
        _credential("kraken_futures"),
        _credential("paper", credential_type="paper"),
        _credential("kraken", wallet="other"),
    ]
    repository.list_active_wallet_credentials = AsyncMock(return_value=credentials)
    exchanges, evidence = await resolve_live_wallet_scope(
        cast(Repository, repository), "wallet", _AS_OF, None
    )
    topology = ExecutorTopology(
        mint_wallet_public_id="",
        exchanges_with_other_wallets=frozenset({"kraken"}),
    )
    expected = resolve_spot_venues(credentials, topology)["wallet"]
    assert exchanges == tuple(sorted(expected)) == ("kraken",)
    payload = json.dumps(asdict(evidence), default=str)
    assert "DO_NOT_SERIALIZE_SECRET_PAYLOAD" not in payload
    assert "DO_NOT_SERIALIZE_LABEL" not in payload
    assert evidence.active_credential_catalog_size == 4
    assert [item.included_in_denominator for item in evidence.credentials] == [True, False, False]


@pytest.mark.asyncio
async def test_live_scope_refuses_an_executor_excluded_wallet(
    repository: MagicMock,
) -> None:
    """A manually named venue cannot resurrect a topology-excluded denominator.

    Given: A declared mint wallet and a second wallet holding the same exchange.
    When: Shared topology excludes the declared wallet's executor-backed credential.
    Then: Scope resolution fails closed instead of producing an empty or manual scope.
    """
    credentials = [_credential("kraken"), _credential("kraken", wallet="other")]
    repository.list_active_wallet_credentials = AsyncMock(return_value=credentials)
    repository.get_orders_total_count = AsyncMock(return_value=0)
    repository.list_active_scope_grants_for_wallet = AsyncMock(return_value=[])
    with pytest.raises(ValueError, match="no executor-backed"):
        await resolve_live_wallet_scope(cast(Repository, repository), "wallet", _AS_OF, "wallet")


@pytest.mark.asyncio
async def test_live_scope_refuses_wallet_absent_from_active_catalogue(
    repository: MagicMock,
) -> None:
    """A wallet absent from the shared catalogue cannot acquire a manual denominator.

    Given: An active credential catalogue containing only another wallet.
    When: The gate resolves the requested wallet through shared topology.
    Then: It refuses the absent scope instead of accepting operator venue input.
    """
    repository.list_active_wallet_credentials = AsyncMock(
        return_value=[_credential("kraken", wallet="other")]
    )
    with pytest.raises(ValueError, match="no active credential"):
        await resolve_live_wallet_scope(cast(Repository, repository), "wallet", _AS_OF, None)


def test_best_close_is_conservative_temporal_and_bounded() -> None:
    """Price selection never looks ahead and keeps each plane's latest close.

    Given: Fresh, stale, future, invalid, and unrelated direct-USD candles.
    When: The report selects a bound price for one currency and minute.
    Then: It uses the highest latest-per-plane causal close and rejects every unsafe row.
    """
    rows = [
        _close(2.0, instrument="a", open_at=_MINUTE - timedelta(minutes=2), candle_id=1),
        _close(3.0, instrument="a", open_at=_MINUTE - timedelta(minutes=1), candle_id=2),
        _close(1.0, instrument="a", open_at=_MINUTE - timedelta(minutes=4), candle_id=9),
        _close(4.0, instrument="b", open_at=_MINUTE - timedelta(minutes=3), candle_id=3),
        _close(99.0, instrument="future", open_at=_MINUTE + timedelta(minutes=1), candle_id=4),
        _close(
            101.0,
            instrument="stale",
            open_at=_MINUTE - timedelta(hours=24, minutes=1),
            candle_id=5,
        ),
        _close(-1.0, instrument="bad", candle_id=6),
        _close(float("nan"), instrument="nan", candle_id=7),
        _close(200.0, currency="OTHER", instrument="other", candle_id=8),
    ]
    index = index_usd_closes(rows)
    selected = select_best_usd_close(index, "TRUMP", _MINUTE)
    assert selected is not None
    assert selected.close == 4.0
    assert selected.instrument_public_id == "b"
    assert selected.age_seconds == 180
    assert select_best_usd_close(index, "MISSING", _MINUTE) is None
    assert (
        select_best_usd_close(
            index_usd_closes([_close(10.0, instrument="not-closed", open_at=_MINUTE)]),
            "TRUMP",
            _MINUTE,
        )
        is None
    )


def test_best_close_breaks_equal_price_ties_deterministically() -> None:
    """Reordered equal-price evidence cannot change the selected provenance.

    Given: Two independent price planes carrying the same usable close.
    When: Their repository order is reversed before selection.
    Then: The exact selected candle evidence remains identical.
    """
    first = _close(4.0, instrument="a", candle_id=1)
    second = _close(4.0, instrument="b", candle_id=2)
    forward = select_best_usd_close(index_usd_closes([first, second]), "TRUMP", _MINUTE)
    reverse = select_best_usd_close(index_usd_closes([second, first]), "TRUMP", _MINUTE)
    assert forward == reverse


def test_report_sizes_every_exclusion_against_a_conservative_equity_floor() -> None:
    """A two-minute impact uses aggregate bounds and the lowest complete equity.

    Given: Two authoritative sub-minimum balances, temporal minima, prices, and complete samples.
    When: The pure activation report evaluates the two-minute window.
    Then: Every exclusion is sized and divided by the lowest positive complete equity.
    """
    request = _request(window_minutes=2)
    first_minute, second_minute = request.minutes
    baskets = [
        _basket({("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0}, minute=first_minute),
        _basket({("kraken", "TRUMP"): 2.0, ("kraken", "USD"): 100.0}, minute=second_minute),
    ]
    minimum = replace(
        _minimum(),
        valid_from=first_minute - timedelta(days=1),
        valid_to=second_minute + timedelta(days=1),
    )
    prices = [
        _close(2.0, instrument="a", open_at=first_minute - timedelta(minutes=1)),
        _close(4.0, instrument="b", open_at=first_minute - timedelta(minutes=2), candle_id=2),
    ]
    report = build_activation_report(
        request,
        baskets,
        [minimum],
        prices,
        [_sample(1200.0, first_minute), _sample(1000.0, second_minute)],
    )
    assert report.authoritative_minutes == 2
    assert report.unauthoritative_minutes == 0
    assert report.exclusion_minutes == 2
    assert report.excluded_legs == 2
    assert report.resolved_legs_total == 2
    assert report.unresolved_legs_total == 2
    assert report.equity_floor_usd == 1000.0
    assert report.equity_floor_minute == second_minute
    assert report.max_known_minute_bound_usd == 12.0
    assert report.max_known_minute_bound_share == 0.012
    assert report.incomplete_causes == ()
    assert [impact.causal_equity_floor_usd for impact in report.impacts] == [1200.0, 1000.0]
    assert [impact.known_bound_share for impact in report.impacts] == [0.01, 0.012]
    assert report.impacts[0].legs[0].bound_usd == 12.0
    assert report.impacts[0].legs[0].price is not None


def test_equity_denominator_ignores_old_samples_and_requires_causal_coverage() -> None:
    """A stale pre-window positive sample cannot authorize an impacted minute.

    Given: One report with only a day-old sample and one with a sample after the impact.
    When: Causal denominator evidence is assessed inside the exact report window.
    Then: The old sample is absent and the future sample cannot price the earlier impact.
    """
    one_minute = _request()
    old = build_activation_report(
        one_minute,
        [_basket({("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0})],
        [_minimum()],
        [_close()],
        [_sample(1000.0, one_minute.minutes[0] - timedelta(hours=24))],
    )
    assert old.equity_sample_count == 0
    assert old.incomplete_causes == ("no_positive_complete_equity",)

    request = _request(window_minutes=2)
    first, second = request.minutes
    minimum = replace(
        _minimum(),
        valid_from=first - timedelta(days=1),
        valid_to=second + timedelta(days=1),
    )
    future_only = build_activation_report(
        request,
        [
            _basket(
                {("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0},
                minute=first,
            ),
            _basket({("kraken", "TRUMP"): 3.0}, minute=second),
        ],
        [minimum],
        [_close(open_at=first - timedelta(minutes=1))],
        [_sample(1000.0, second)],
    )
    assert future_only.equity_floor_usd == 1000.0
    assert future_only.max_known_minute_bound_share is None
    assert future_only.incomplete_causes == ("missing_causal_equity",)


def test_equity_denominator_requires_fresh_impact_and_window_end_samples() -> None:
    """Both each exclusion and the report end require a sample no older than five minutes.

    Given: Seven-minute windows with one complete sample at their first minute.
    When: An exclusion occurs either at the first or final minute.
    Then: Current coverage or the exclusion's own causal coverage fails stale.
    """
    request = _request(window_minutes=7)
    first, *_, last = request.minutes
    minimum = replace(
        _minimum(),
        valid_from=first - timedelta(days=1),
        valid_to=last + timedelta(days=1),
    )
    current_stale = build_activation_report(
        request,
        [
            _basket(
                (
                    {("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0}
                    if minute == first
                    else {("kraken", "TRUMP"): 3.0}
                ),
                minute=minute,
            )
            for minute in request.minutes
        ],
        [minimum],
        [_close(open_at=first - timedelta(minutes=1))],
        [_sample(1000.0, first)],
    )
    assert current_stale.max_known_minute_bound_share == 0.012
    assert current_stale.incomplete_causes == ("stale_complete_equity",)

    impact_stale = build_activation_report(
        request,
        [
            _basket(
                (
                    {("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0}
                    if minute == last
                    else {("kraken", "TRUMP"): 3.0}
                ),
                minute=minute,
            )
            for minute in request.minutes
        ],
        [minimum],
        [_close(open_at=first - timedelta(minutes=1))],
        [_sample(1000.0, first)],
    )
    assert impact_stale.max_known_minute_bound_share is None
    assert impact_stale.incomplete_causes == ("stale_complete_equity",)


def test_equity_freshness_accepts_the_exact_five_minute_boundary() -> None:
    """A five-minute-old sample is fresh for both the impact and window end.

    Given: A six-minute window whose only complete sample is at its first minute.
    When: The final minute carries the only proposed exclusion.
    Then: The exact five-minute age remains complete and supplies its causal floor.
    """
    request = _request(window_minutes=6)
    first, *_, last = request.minutes
    minimum = replace(
        _minimum(),
        valid_from=first - timedelta(days=1),
        valid_to=last + timedelta(days=1),
    )
    report = build_activation_report(
        request,
        [
            _basket(
                (
                    {("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0}
                    if minute == last
                    else {("kraken", "TRUMP"): 3.0}
                ),
                minute=minute,
            )
            for minute in request.minutes
        ],
        [minimum],
        [_close(open_at=first - timedelta(minutes=1))],
        [_sample(1000.0, first)],
    )
    assert report.incomplete_causes == ()
    assert report.impacts[0].causal_equity_floor_minute == first
    assert report.max_known_minute_bound_share == 0.012


def test_invalid_complete_equity_cannot_hide_behind_a_valid_sample() -> None:
    """Missing, non-positive, and timezone-ambiguous complete rows fail closed.

    Given: A valid causal sample alongside malformed complete sample versions.
    When: The denominator window is validated before threshold evaluation.
    Then: The valid share remains visible but the artifact is incomplete.
    """
    valid = _sample(1000.0)
    missing = _sample(1000.0)
    missing["cash_usd"] = None
    non_positive = _sample(0.0)
    naive = _sample(1000.0)
    naive["point_time"] = _MINUTE.replace(tzinfo=None)
    naive_incomplete = _sample(1000.0)
    naive_incomplete["point_time"] = _MINUTE.replace(tzinfo=None)
    naive_incomplete["valuation_status"] = "incomplete"
    future = _sample(1000.0, _MINUTE + timedelta(minutes=1))
    report = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0})],
        [_minimum()],
        [_close()],
        [valid, missing, non_positive, naive, naive_incomplete, future],
    )
    assert report.equity_sample_count == 1
    assert report.max_known_minute_bound_share == 0.012
    assert report.incomplete_causes == ("invalid_complete_equity",)
    assert evaluate_activation_threshold(report, 0.02).status == "incomplete"


def test_report_indexes_price_evidence_once_for_all_minutes() -> None:
    """Price selection avoids a minutes-times-rows rescan without semantic drift.

    Given: Two authoritative minutes sharing one candle corpus.
    When: The pure report sizes both exclusions.
    Then: The corpus index is built exactly once and both selected bounds remain equal.
    """
    request = _request(window_minutes=2)
    baskets = [
        _basket(
            {("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0},
            minute=minute,
        )
        for minute in request.minutes
    ]
    minimum = replace(
        _minimum(),
        valid_from=request.minutes[0] - timedelta(days=1),
        valid_to=request.minutes[-1] + timedelta(days=1),
    )
    with patch(
        "snapper.cli.pnl_minimum_activation_gate.index_usd_closes",
        wraps=index_usd_closes,
    ) as indexer:
        report = build_activation_report(
            request,
            baskets,
            [minimum],
            [_close(open_at=request.minutes[0] - timedelta(minutes=1))],
            [_sample(1000.0, minute) for minute in request.minutes],
        )
    indexer.assert_called_once()
    assert [impact.complete_bound_usd for impact in report.impacts] == [12.0, 12.0]


def test_exact_activation_window_refuses_a_1439_minute_authority_prefix() -> None:
    """A nominal 24-hour artifact cannot pass with one missing authoritative minute.

    Given: An exact 1440-minute activation request with only 1439 basket witnesses.
    When: Completeness is folded across the requested grid.
    Then: The artifact is partial and threshold evaluation remains incomplete.
    """
    request = _request(window_minutes=1440, purpose="activation")
    baskets = [
        _basket({("kraken", "TRUMP"): 3.0}, minute=minute) for minute in request.minutes[:-1]
    ]
    report = build_activation_report(request, baskets, [_minimum()], [], [])
    assert report.authoritative_minutes == 1439
    assert report.unauthoritative_minutes == 1
    assert report.incomplete_causes == ("partial_authoritative_window",)
    assert evaluate_activation_threshold(report, 0.01).status == "incomplete"


def test_basket_minute_and_report_input_refuse_ambiguous_shapes() -> None:
    """No caller can smuggle two authority states or duplicate a grid minute.

    Given: Missing, contradictory, outside-window, and duplicate basket inputs.
    When: Basket state and report indexing validate the input collection.
    Then: Each ambiguity is refused before the predicate can run.
    """
    with pytest.raises(ValueError, match="balances or refusal reasons"):
        _basket(None)
    with pytest.raises(ValueError, match="balances or refusal reasons"):
        _basket({}, reasons=("basket_stale",))
    request = _request()
    outside = _basket({}, minute=request.minutes[0] - timedelta(minutes=1))
    with pytest.raises(ValueError, match="outside"):
        build_activation_report(request, [outside], [], [], [])
    minute = _basket({})
    with pytest.raises(ValueError, match="duplicate"):
        build_activation_report(request, [minute, minute], [], [], [])


def test_private_minute_evaluator_keeps_a_refused_basket_inert() -> None:
    """The defensive pure boundary cannot evaluate unauthoritative balances.

    Given: A minute refused by the existing authoritative-basket gate.
    When: The lower-level impact evaluator is called defensively.
    Then: It returns no impact, proof counts, or incomplete bound cause.
    """
    impact, resolved, unresolved, causes = _minute_impact(
        _basket(None, reasons=("basket_stale",)),
        [_minimum()],
        index_usd_closes([_close()]),
    )
    assert (impact, resolved, unresolved, causes) == (None, 0, 0, ())


def test_threshold_alarm_is_strict_and_fail_closed() -> None:
    """Equality passes, a known excess alarms, and incomplete evidence refuses.

    Given: Complete and price-incomplete reports around one operator threshold.
    When: The scheduler verdict evaluates exact equality and a strict excess.
    Then: Equality passes, the excess breaches, and missing evidence stays incomplete.
    """
    report = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0})],
        [_minimum()],
        [_close()],
        [_sample(1000.0)],
    )
    assert evaluate_activation_threshold(report, 0.012).status == "pass"
    assert evaluate_activation_threshold(report, 0.011).status == "breach"
    incomplete = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0})],
        [_minimum()],
        [],
        [_sample(1000.0)],
    )
    assert incomplete.incomplete_causes == ("missing_usd_close",)
    assert evaluate_activation_threshold(incomplete, 0.02).status == "incomplete"


def test_known_breach_precedes_partial_window_and_rule_w_incompleteness() -> None:
    """Incomplete evidence cannot suppress an already-proven threshold alarm.

    Given: One priced breach, one Rule-W refusal, and one missing grid minute.
    When: The report folds completeness before evaluating the operator threshold.
    Then: Both incomplete causes remain visible while the verdict is still breach.
    """
    request = _request(window_minutes=3)
    first, second, _ = request.minutes
    currencies = ("A", "B", "C", "D", "E")
    report = build_activation_report(
        request,
        [
            _basket(
                {("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0},
                minute=first,
            ),
            _basket(
                {("kraken", currency): 1.0 for currency in currencies},
                minute=second,
            ),
        ],
        [
            replace(_minimum(), valid_from=first - timedelta(days=1)),
            *[
                replace(
                    _minimum(currency, 2.0),
                    valid_from=first - timedelta(days=1),
                )
                for currency in currencies
            ],
        ],
        [_close(4.0, open_at=first - timedelta(minutes=1))],
        [_sample(100.0, first)],
    )
    assert report.incomplete_causes == (
        "partial_authoritative_window",
        "rule_w_withheld",
    )
    assert report.max_known_minute_bound_share == 0.12
    assert evaluate_activation_threshold(report, 0.1).status == "breach"


@pytest.mark.parametrize("threshold", [0.0, -0.1, 1.1, math.inf, math.nan])
def test_threshold_refuses_non_fractional_values(threshold: float) -> None:
    """An alarm threshold must be a finite positive share no greater than one.

    Given: A zero, negative, excessive, infinite, or NaN operator threshold.
    When: Threshold evaluation validates the stated equity fraction.
    Then: The malformed threshold is refused before a verdict is emitted.
    """
    report = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 3.0})],
        [_minimum()],
        [],
        [],
    )
    with pytest.raises(ValueError, match="max_bound_share"):
        evaluate_activation_threshold(report, threshold)


def test_vacuous_and_unpriceable_reports_refuse_activation() -> None:
    """No authoritative minute, no resolved proof, and no equity all fail closed.

    Given: Three reports missing basket authority, minimum proof, or positive equity.
    When: Their completeness causes are folded.
    Then: Each missing load-bearing evidence plane receives its exact refusal cause.
    """
    no_authority = build_activation_report(
        _request(),
        [_basket(None, reasons=("basket_stale",))],
        [],
        [],
        [],
    )
    assert no_authority.incomplete_causes == (
        "no_authoritative_minutes",
        "no_resolved_minimum_legs",
    )
    no_proof = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 1.0})],
        [],
        [],
        [],
    )
    assert no_proof.incomplete_causes == ("no_resolved_minimum_legs",)
    no_equity = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0})],
        [_minimum()],
        [_close()],
        [_sample(-1.0), _sample(float("nan"))],
    )
    assert no_equity.incomplete_causes == (
        "no_positive_complete_equity",
        "invalid_complete_equity",
    )
    incomplete_sample = _sample(1000.0)
    incomplete_sample["valuation_status"] = "incomplete"
    incomplete_sample["cash_usd"] = None
    incomplete_sample["position_value_usd"] = None
    still_no_equity = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0})],
        [_minimum()],
        [_close()],
        [incomplete_sample],
    )
    assert still_no_equity.incomplete_causes == ("no_positive_complete_equity",)


def test_no_exclusion_needs_no_equity_but_still_exercises_the_proof() -> None:
    """A resolved balance at the minimum is a complete zero-impact witness.

    Given: An authoritative balance exactly equal to its proven venue minimum.
    When: The report runs without price or equity evidence.
    Then: The exercised predicate produces a complete zero-impact passing report.
    """
    report = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 3.0})],
        [_minimum()],
        [],
        [],
    )
    assert report.incomplete_causes == ()
    assert report.max_known_minute_bound_share == 0.0
    assert evaluate_activation_threshold(report, 0.01).status == "pass"


def test_rule_w_refusals_are_visible_without_fabricating_an_impact() -> None:
    """The report names both whole-basket guards while preserving zero impact.

    Given: Five excludable legs and a separate exclusion that would empty its basket.
    When: Rule W evaluates both authoritative minutes.
    Then: The report names each refusal while recording no fabricated exclusion bound.
    """
    currencies = ("A", "B", "C", "D", "E")
    balances = {("kraken", currency): 1.0 for currency in currencies}
    too_many = build_activation_report(
        _request(),
        [_basket(balances)],
        [_minimum(currency, 2.0) for currency in currencies],
        [],
        [],
    )
    assert too_many.rule_withheld_minutes == 1
    assert too_many.withheld_cause_counts == {"too_many_excluded_legs": 1}
    assert too_many.excluded_legs == 0
    assert too_many.incomplete_causes == ("rule_w_withheld",)
    empty = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 1.0})],
        [_minimum()],
        [],
        [],
    )
    assert empty.withheld_cause_counts == {"would_empty_basket": 1}
    assert empty.incomplete_causes == ("rule_w_withheld",)


def test_positive_finite_bound_overflow_is_a_definite_breach() -> None:
    """Multiplication overflow is a definite breach rather than missing evidence.

    Given: One finite exclusion bound and another leg whose multiplication overflows.
    When: Completeness and the threshold verdict are evaluated together.
    Then: Overflow is disclosed as a capped finite lower bound and always breaches.
    """
    report = build_activation_report(
        _request(),
        [
            _basket(
                {
                    ("kraken", "TRUMP"): 1.0,
                    ("kraken", "OTHER"): 1.0,
                    ("kraken", "USD"): 100.0,
                }
            )
        ],
        [_minimum(), _minimum("OTHER", 1e308)],
        [_close(4.0), _close(1e308, currency="OTHER", instrument="other")],
        [_sample(100.0)],
    )
    assert report.incomplete_causes == ()
    assert report.definite_breach is True
    assert report.impacts[0].bound_overflow is True
    assert sum(leg.bound_overflow for leg in report.impacts[0].legs) == 1
    assert report.max_known_minute_bound_share is not None
    assert math.isfinite(report.max_known_minute_bound_share)
    assert evaluate_activation_threshold(report, 1.0).status == "breach"


def test_aggregate_and_share_overflow_remain_finite_fail_closed_evidence() -> None:
    """Finite legs cannot overflow either the JSON aggregate or alarm ratio.

    Given: Four huge finite leg bounds and a separate tiny positive equity floor.
    When: Aggregate bounds and equity shares are calculated for JSON output.
    Then: Both overflow shapes remain finite, fail closed, and retain a breach signal.
    """
    currencies = ("A", "B", "C", "D")
    aggregate = build_activation_report(
        _request(),
        [
            _basket(
                {
                    **{("kraken", currency): 1.0 for currency in currencies},
                    ("kraken", "USD"): 100.0,
                }
            )
        ],
        [_minimum(currency, 1e308) for currency in currencies],
        [_close(1.0, currency=currency, instrument=currency) for currency in currencies],
        [_sample(100.0)],
    )
    assert aggregate.incomplete_causes == ()
    assert aggregate.definite_breach is True
    assert aggregate.max_known_minute_bound_usd > 1e308
    assert math.isfinite(aggregate.max_known_minute_bound_share or math.inf)
    assert evaluate_activation_threshold(aggregate, 1.0).status == "breach"
    tiny_equity = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 1.0})],
        [_minimum("TRUMP", 1e308)],
        [_close(1.0)],
        [_sample(1e-308)],
    )
    assert tiny_equity.max_known_minute_bound_share is not None
    assert math.isfinite(tiny_equity.max_known_minute_bound_share)
    assert tiny_equity.max_known_minute_bound_share > 1.0
    assert evaluate_activation_threshold(tiny_equity, 1.0).status == "breach"
    json.dumps(report_as_json(tiny_equity), allow_nan=False)


def test_report_json_preserves_scope_evidence_and_alarm() -> None:
    """The machine report contains the provenance needed for review and scheduling.

    Given: A complete report and a passing operator threshold decision.
    When: The result is projected into its versioned JSON schema.
    Then: Scope, verdict, exclusion, minimum, price, and bound evidence remain present.
    """
    report = build_activation_report(
        _request(),
        [_basket({("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0})],
        [_minimum()],
        [_close()],
        [_sample(1000.0)],
    )
    decision = evaluate_activation_threshold(report, 0.02)
    payload = report_as_json(report, decision)
    assert payload["schema_version"] == 2
    assert payload["purpose"] == "inspect"
    assert payload["activation_eligible"] is False
    assert payload["wallet_public_id"] == "wallet"
    scope_evidence = payload["scope_evidence"]
    assert isinstance(scope_evidence, dict)
    assert scope_evidence["active_credential_catalog_size"] == 1
    assert payload["gate"] == {
        "status": "pass",
        "max_bound_share": 0.02,
        "observed_max_bound_share": 0.012,
    }
    impacts = payload["impacts"]
    assert isinstance(impacts, list)
    impact = impacts[0]
    assert isinstance(impact, dict)
    legs = impact["legs"]
    assert isinstance(legs, list)
    leg = legs[0]
    assert isinstance(leg, dict)
    assert leg["currency"] == "TRUMP"
    assert leg["bound_usd"] == 12.0
    unscoped_request = replace(report.request, scope_evidence=None)
    assert report_as_json(replace(report, request=unscoped_request))["scope_evidence"] is None


@pytest.mark.asyncio
async def test_runner_loads_every_plane_and_keeps_publication_disconnected(
    repository: MagicMock,
) -> None:
    """The operator report uses only existing reads and never calls a writer.

    Given: A repository double supplying all four required evidence planes.
    When: The asynchronous runner builds a two-minute activation report.
    Then: Its windows are exact and no record, supersede, or retract method is called.
    """
    request = _request(window_minutes=2)

    repository.get_venue_account_observation_attempt_stream = AsyncMock(
        return_value=[_attempt(request.minutes[0])]
    )
    repository.get_pnl_spot_order_minimum_window = AsyncMock(return_value=[_minimum_row()])
    repository.get_pnl_crypto_usd_plane_candles = AsyncMock(
        return_value=[_close(open_at=request.minutes[0] - timedelta(minutes=1))]
    )
    repository.get_portfolio_pnl_anchor = AsyncMock(return_value=_anchor())
    repository.get_portfolio_pnl_samples = AsyncMock(
        return_value=[_sample(1000.0, minute) for minute in request.minutes]
    )
    report = await run_activation_report(cast(Repository, repository), request)
    assert report.authoritative_minutes == 2
    assert report.excluded_legs == 2
    assert report.incomplete_causes == ()
    observation_call = repository.get_venue_account_observation_attempt_stream.await_args
    assert observation_call.args == (
        "wallet",
        ("kraken",),
        "live",
        request.minutes[0],
        request.minutes[-1],
    )
    repository.get_venue_account_observation_attempt_stream.assert_awaited_once()
    minimum_call = repository.get_pnl_spot_order_minimum_window.await_args
    assert minimum_call.args[0] == ("kraken",)
    assert minimum_call.args[1] == ("TRUMP", "USD")
    assert minimum_call.args[2] == request.minutes[0]
    assert minimum_call.args[3] == request.minutes[-1] + timedelta(minutes=1)
    price_call = repository.get_pnl_crypto_usd_plane_candles.await_args
    assert price_call.args[0] == ("TRUMP",)
    assert price_call.args[1] == request.minutes[0] - timedelta(hours=24)
    assert price_call.args[2] == request.minutes[-1] - timedelta(minutes=1)
    sample_call = repository.get_portfolio_pnl_samples.await_args
    assert sample_call.args[1:] == (request.minutes[0], request.minutes[-1])
    assert sample_call.kwargs == {}
    assert not repository.method_calls or all(
        not call_name.startswith(("record_", "supersede_", "retract_"))
        for call_name, _, _ in repository.method_calls
    )


@pytest.mark.asyncio
async def test_runner_preserves_basket_refusals_and_skips_unused_reads(
    repository: MagicMock,
) -> None:
    """An unauthoritative basket stays refused and cannot trigger unused reads.

    Given: The latest account attempt failed its balance-authority gate.
    When: The asynchronous report runner evaluates the minute.
    Then: The refusal survives and minimum, price, anchor, and sample reads are skipped.
    """
    request = _request()
    repository.get_venue_account_observation_attempt_stream = AsyncMock(
        return_value=[_attempt(request.minutes[0], observed=False)]
    )
    repository.get_pnl_spot_order_minimum_window = AsyncMock(return_value=[])
    repository.get_pnl_crypto_usd_plane_candles = AsyncMock()
    repository.get_portfolio_pnl_anchor = AsyncMock()
    repository.get_portfolio_pnl_samples = AsyncMock()
    report = await run_activation_report(cast(Repository, repository), request)
    assert report.authoritative_minutes == 0
    assert report.unauthoritative_reason_counts == {"basket_stale": 1}
    assert report.incomplete_causes == (
        "no_authoritative_minutes",
        "no_resolved_minimum_legs",
    )
    repository.get_pnl_spot_order_minimum_window.assert_not_awaited()
    repository.get_pnl_crypto_usd_plane_candles.assert_not_awaited()
    repository.get_portfolio_pnl_anchor.assert_not_awaited()
    repository.get_portfolio_pnl_samples.assert_not_awaited()


@pytest.mark.asyncio
async def test_runner_reports_missing_anchor_as_incomplete_equity(
    repository: MagicMock,
) -> None:
    """An impact without an active sample epoch cannot acquire a fake denominator.

    Given: A proven priced exclusion but no active portfolio P&L anchor.
    When: The runner tries to load the current epoch's complete equity samples.
    Then: It skips the unscoped sample read and reports missing positive equity.
    """
    request = _request()
    repository.get_venue_account_observation_attempt_stream = AsyncMock(
        return_value=[_attempt(request.minutes[0])]
    )
    repository.get_pnl_spot_order_minimum_window = AsyncMock(return_value=[_minimum_row()])
    repository.get_pnl_crypto_usd_plane_candles = AsyncMock(return_value=[_close()])
    repository.get_portfolio_pnl_anchor = AsyncMock(return_value=None)
    repository.get_portfolio_pnl_samples = AsyncMock()
    report = await run_activation_report(cast(Repository, repository), request)
    assert report.incomplete_causes == ("no_positive_complete_equity",)
    repository.get_portfolio_pnl_samples.assert_not_awaited()


def _cli_report(
    command: GateCommandRequest | None = None,
    *,
    missing_price: bool = False,
) -> ActivationGateReport:
    """Build a complete report and optionally rebind its command artifact scope."""
    base_request = _request(window_minutes=60)
    base = build_activation_report(
        base_request,
        [
            _basket(
                {("kraken", "TRUMP"): 1.0, ("kraken", "USD"): 100.0},
                minute=minute,
            )
            for minute in base_request.minutes
        ],
        [_minimum()],
        ([] if missing_price else [_close(open_at=base_request.minutes[0] - timedelta(minutes=1))]),
        [_sample(1000.0, minute) for minute in base_request.minutes],
    )
    if command is None:
        return base
    request = ActivationGateRequest(
        wallet_public_id=command.wallet_public_id,
        mode="live",
        exchanges=("kraken",),
        as_of=command.as_of,
        window_minutes=command.window_minutes,
        purpose=command.purpose,
        scope_evidence=_scope_evidence(),
    )
    return replace(
        base,
        request=request,
        authoritative_minutes=command.window_minutes,
        unauthoritative_minutes=0,
    )


@pytest.mark.asyncio
async def test_configured_runner_resolves_scope_fresh_and_always_disposes() -> None:
    """Configured execution derives scope before the report and closes repositories.

    Given: Bootstrap settings, a fresh pin reader, and a resolved live denominator.
    When: The configured boundary executes one inspection.
    Then: It builds the scoped request without manual venues and disposes exactly once.
    """
    command = GateCommandRequest(
        wallet_public_id="wallet",
        as_of=_AS_OF,
        window_minutes=60,
        purpose="inspect",
    )
    bootstrap = MagicMock(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xsub="tcp://127.0.0.1:7500",
    )
    repository = MagicMock(spec=Repository)
    settings_service = MagicMock()
    settings_service.get_setting_fresh = AsyncMock(return_value="wallet-pin")
    scope = AsyncMock(return_value=(("kraken",), _scope_evidence()))
    run = AsyncMock(return_value=_cli_report(command))
    dispose = AsyncMock(return_value=None)
    with (
        patch(
            "snapper.cli.pnl_minimum_activation_gate.get_bootstrap_settings",
            return_value=bootstrap,
        ),
        patch(
            "snapper.cli.pnl_minimum_activation_gate.get_repository",
            return_value=repository,
        ),
        patch(
            "snapper.cli.pnl_minimum_activation_gate.SettingsService",
            return_value=settings_service,
        ),
        patch(
            "snapper.cli.pnl_minimum_activation_gate.resolve_live_wallet_scope",
            new=scope,
        ),
        patch("snapper.cli.pnl_minimum_activation_gate.run_activation_report", new=run),
        patch("snapper.cli.pnl_minimum_activation_gate.dispose_repositories", new=dispose),
    ):
        result = await _run_configured_report(command)
    assert result.request.purpose == "inspect"
    settings_service.get_setting_fresh.assert_awaited_once()
    scope.assert_awaited_once_with(repository, "wallet", _AS_OF, "wallet-pin")
    assert run.await_args is not None
    scoped_request = run.await_args.args[1]
    assert isinstance(scoped_request, ActivationGateRequest)
    assert scoped_request.exchanges == ("kraken",)
    dispose.assert_awaited_once()


@pytest.fixture
def cli_dependencies() -> Iterator[AsyncMock]:
    """Replace the configured asynchronous boundary for CLI-only tests."""
    run = AsyncMock(side_effect=lambda command: _cli_report(command))
    with patch("snapper.cli.pnl_minimum_activation_gate._run_configured_report", new=run):
        yield run


def test_report_command_is_exact_threshold_bound_and_activation_eligible(
    runner: CliRunner,
    cli_dependencies: AsyncMock,
) -> None:
    """The one-time report is exactly 24 hours and cannot omit its threshold.

    Given: A complete configured report and an explicit accepted equity share.
    When: The current-only report command captures its evidence horizon.
    Then: Its artifact is threshold-bound, 1440 minutes, and activation-eligible.
    """
    run = cli_dependencies
    before = datetime.now(UTC)
    result = runner.invoke(
        app,
        [
            "pnl-minimum-gate",
            "report",
            "--wallet",
            "wallet",
            "--max-bound-share",
            "0.02",
        ],
    )
    after = datetime.now(UTC)
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["gate"]["status"] == "pass"
    assert payload["minutes_requested"] == 1440
    assert payload["purpose"] == "activation"
    assert payload["activation_eligible"] is True
    assert run.await_args is not None
    request = run.await_args.args[0]
    assert isinstance(request, GateCommandRequest)
    assert request.window_minutes == 1440
    assert before <= request.as_of <= after
    unscoped_report = _cli_report(request)
    unscoped_request = replace(unscoped_report.request, scope_evidence=None)
    unscoped_report = replace(unscoped_report, request=unscoped_request)
    decision = evaluate_activation_threshold(unscoped_report, 0.02)
    assert report_as_json(unscoped_report, decision)["activation_eligible"] is False


def test_check_command_uses_stable_alarm_exit_codes(
    runner: CliRunner,
    cli_dependencies: AsyncMock,
) -> None:
    """Schedulers receive 0 for pass, 4 for breach, and 3 for incomplete.

    Given: Complete passing, complete breaching, and price-incomplete reports.
    When: The standing check runs under the same operator threshold.
    Then: JSON verdicts and shell exit codes distinguish all three outcomes.
    """
    run = cli_dependencies
    base = [
        "pnl-minimum-gate",
        "check",
        "--wallet",
        "wallet",
        "--hours",
        "24",
    ]
    passed = runner.invoke(app, [*base, "--max-bound-share", "0.02"])
    assert passed.exit_code == 0
    assert json.loads(passed.stdout)["gate"]["status"] == "pass"
    breached = runner.invoke(app, [*base, "--max-bound-share", "0.01"])
    assert breached.exit_code == 4
    assert json.loads(breached.stdout)["gate"]["status"] == "breach"
    report_breach = runner.invoke(
        app,
        [
            "pnl-minimum-gate",
            "report",
            "--wallet",
            "wallet",
            "--max-bound-share",
            "0.01",
        ],
    )
    assert report_breach.exit_code == 4
    run.side_effect = lambda command: _cli_report(command, missing_price=True)
    incomplete = runner.invoke(app, [*base, "--max-bound-share", "0.02"])
    assert incomplete.exit_code == 3
    assert json.loads(incomplete.stdout)["gate"]["status"] == "incomplete"


def test_inspect_is_explicitly_ineligible_and_report_keeps_incomplete_exit(
    runner: CliRunner,
    cli_dependencies: AsyncMock,
) -> None:
    """Short diagnostics never masquerade as activation and incomplete reports refuse.

    Given: A complete one-hour inspection and an activation report missing a USD close.
    When: Both commands emit their canonical artifacts.
    Then: Inspect is ineligible and the threshold-bound report exits incomplete.
    """
    run = cli_dependencies
    inspected = runner.invoke(
        app,
        ["pnl-minimum-gate", "inspect", "--wallet", "wallet", "--hours", "1"],
    )
    assert inspected.exit_code == 0
    inspect_payload = json.loads(inspected.stdout)
    assert inspect_payload["purpose"] == "inspect"
    assert inspect_payload["activation_eligible"] is False
    run.side_effect = lambda command: _cli_report(command, missing_price=True)
    result = runner.invoke(
        app,
        [
            "pnl-minimum-gate",
            "report",
            "--wallet",
            "wallet",
            "--max-bound-share",
            "0.02",
        ],
    )
    assert result.exit_code == 3
    assert "missing_usd_close" in result.stdout
    incomplete_inspect = runner.invoke(
        app,
        ["pnl-minimum-gate", "inspect", "--wallet", "wallet", "--hours", "1"],
    )
    assert incomplete_inspect.exit_code == 3


def test_cli_refuses_backdating_thresholds_manual_scope_and_read_secrets(
    runner: CliRunner,
    cli_dependencies: AsyncMock,
) -> None:
    """Malformed operator input and read failures use distinct safe exits.

    Given: Backdating, invalid duration/threshold, manual scope, and a secret-bearing failure.
    When: Each command crosses its earliest validation or read boundary.
    Then: Validation avoids the database and syntax keeps Typer's distinct exit code.
    """
    run = cli_dependencies
    backdated = runner.invoke(
        app,
        [
            "pnl-minimum-gate",
            "report",
            "--wallet",
            "wallet",
            "--max-bound-share",
            "0.02",
            "--as-of",
            "not-a-time",
        ],
    )
    assert backdated.exit_code == 2
    assert "--as-of" in _unstyled(backdated.stderr)
    naive = runner.invoke(
        app,
        [
            "pnl-minimum-gate",
            "report",
            "--wallet",
            "wallet",
            "--max-bound-share",
            "0.02",
            "--as-of",
            "2026-08-02T12:05:00",
        ],
    )
    assert naive.exit_code == 2
    invalid_threshold = runner.invoke(
        app,
        [
            "pnl-minimum-gate",
            "check",
            "--wallet",
            "wallet",
            "--max-bound-share",
            "2",
        ],
    )
    assert invalid_threshold.exit_code == 1
    invalid_report_threshold = runner.invoke(
        app,
        [
            "pnl-minimum-gate",
            "report",
            "--wallet",
            "wallet",
            "--max-bound-share",
            "2",
        ],
    )
    assert invalid_report_threshold.exit_code == 1
    assert run.await_count == 0
    missing_option = runner.invoke(app, ["pnl-minimum-gate", "report", "--wallet", "wallet"])
    assert missing_option.exit_code == 2
    short_check = runner.invoke(
        app,
        [
            "pnl-minimum-gate",
            "check",
            "--wallet",
            "wallet",
            "--hours",
            "23",
            "--max-bound-share",
            "0.02",
        ],
    )
    assert short_check.exit_code == 1
    invalid_inspect = runner.invoke(
        app,
        ["pnl-minimum-gate", "inspect", "--wallet", "wallet", "--hours", "0"],
    )
    assert invalid_inspect.exit_code == 1
    manual_scope = runner.invoke(
        app,
        [
            "pnl-minimum-gate",
            "inspect",
            "--wallet",
            "wallet",
            "--exchange",
            "kraken",
        ],
    )
    assert manual_scope.exit_code == 2
    secret = "postgresql://operator:DO_NOT_LEAK@database/snapper"
    run.side_effect = RuntimeError(secret)
    failed = runner.invoke(
        app,
        [
            "pnl-minimum-gate",
            "report",
            "--wallet",
            "wallet",
            "--max-bound-share",
            "0.02",
        ],
    )
    assert failed.exit_code == 1
    assert failed.stderr == "refused: activation evidence read failed\n"
    assert secret not in failed.stderr


def test_cli_group_is_registered() -> None:
    """The installed Snapper CLI exposes the operator-only gate surface.

    Given: The fully assembled root Typer application.
    When: Its registered command groups are inspected.
    Then: The F6 report and monitor group is reachable by its documented name.
    """
    assert "pnl-minimum-gate" in [group.name for group in app.registered_groups]
