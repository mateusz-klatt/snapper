"""Branch-level tests for the pure Phase-5B snapshotter tick planner."""

import json
import math
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from typing import get_args
from uuid import uuid7

import pytest
from sqlalchemy import create_engine

from snapper.application.portfolio.basket_valuation import CandleVersionIdentity
from snapper.application.portfolio.basket_valuation import CryptoUsdCandle
from snapper.application.portfolio.basket_valuation import PositionInventoryEntry
from snapper.application.portfolio.basket_valuation import ValuationEvidence
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_anchor_identity import portfolio_pnl_anchor_public_id
from snapper.application.portfolio.pnl_snapshot_planner import _POINT_REASON_TO_SAMPLE_CODE
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_snapshot_planner import MinuteInputs
from snapper.application.portfolio.pnl_snapshot_planner import PlannedSample
from snapper.application.portfolio.pnl_snapshot_planner import PositionVersion
from snapper.application.portfolio.pnl_snapshot_planner import SelfHealCandidate
from snapper.application.portfolio.pnl_snapshot_planner import _incomplete_audit_json
from snapper.application.portfolio.pnl_snapshot_planner import _PartitionOutcome
from snapper.application.portfolio.pnl_snapshot_planner import assemble_minute_sample
from snapper.application.portfolio.pnl_snapshot_planner import evaluate_basket
from snapper.application.portfolio.pnl_snapshot_planner import plan_catchup_chunks
from snapper.application.portfolio.pnl_snapshot_planner import plan_catchup_window
from snapper.application.portfolio.pnl_snapshot_planner import plan_chunk_samples
from snapper.application.portfolio.pnl_snapshot_planner import plan_late_fill_recompute
from snapper.application.portfolio.pnl_snapshot_planner import plan_self_heal_minutes
from snapper.application.portfolio.pnl_snapshot_planner import resolve_drawdown
from snapper.application.portfolio.pnl_snapshot_planner import value_basket
from snapper.application.portfolio.pnl_snapshotter import _extract_reason_codes
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReason
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import PortfolioPnlPoint
from snapper.data.models import VenueAccountObservation
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PNL_SAMPLE_FINAL_REASONS
from snapper.data.repository_types import PNL_SAMPLE_NEVER_PERSIST_REASONS
from snapper.data.repository_types import PNL_SAMPLE_REASON_CODES
from snapper.data.repository_types import PNL_SAMPLE_RETRYABLE_REASONS
from snapper.data.repository_types import PortfolioPnlSampleRow
from snapper.data.repository_types import SampleReasonCode
from snapper.data.repository_types import VenueAccountObservationAttemptRow

_T0 = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_M1 = _T0 + timedelta(minutes=1)
_M2 = _T0 + timedelta(minutes=2)
_M3 = _T0 + timedelta(minutes=3)
_WALLET = "0000face-0000-7000-8000-0000000000a1"
_EPOCH = "00000000-0000-7000-8000-000000000102"
_SESSION = "00000000-0000-7000-8000-000000000103"


def _complete_point(point_time: datetime, *, unrealized: float = 5.0) -> PnlTimelinePoint:
    """Build a mark-complete P&L point carrying finite cumulatives."""
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=1.0,
        fee_pnl=-0.5,
        accrual_pnl=0.25,
        unrealized_pnl=unrealized,
        net_pnl=1.0 - 0.5 + 0.25 + unrealized,
        valuation_status="complete",
        incompleteness_reasons=(),
        per_instrument=(),
        attribution=(),
    )


def _mark_incomplete_point(point_time: datetime) -> PnlTimelinePoint:
    """Build a mark-incomplete point: cumulatives kept, unrealized withheld."""
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=1.0,
        fee_pnl=-0.5,
        accrual_pnl=0.25,
        unrealized_pnl=None,
        net_pnl=None,
        valuation_status="incomplete",
        incompleteness_reasons=(
            PnlIncompletenessReasonEntry(
                reason="mark_unavailable",
                withholding_tier="mark_incomplete",
                withholding_scope="global",
                trigger_instrument_public_id=None,
            ),
        ),
        per_instrument=(),
        attribution=(),
    )


def _mark_incomplete_point_with(
    point_time: datetime, *reasons: PnlIncompletenessReason
) -> PnlTimelinePoint:
    """Build a mark-incomplete point carrying the given global causal reasons.

    All reasons are stamped at the ``mark_incomplete`` tier with global scope and
    ordered by reason so the point's canonical-reasons invariant holds.
    """
    entries = tuple(
        PnlIncompletenessReasonEntry(
            reason=reason,
            withholding_tier="mark_incomplete",
            withholding_scope="global",
            trigger_instrument_public_id=None,
        )
        for reason in sorted(reasons)
    )
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=1.0,
        fee_pnl=-0.5,
        accrual_pnl=0.25,
        unrealized_pnl=None,
        net_pnl=None,
        valuation_status="incomplete",
        incompleteness_reasons=entries,
        per_instrument=(),
        attribution=(),
    )


def _untrusted_point(point_time: datetime) -> PnlTimelinePoint:
    """Build an untrusted point whose cumulatives are all withheld."""
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=None,
        fee_pnl=None,
        accrual_pnl=None,
        unrealized_pnl=None,
        net_pnl=None,
        valuation_status="incomplete",
        incompleteness_reasons=(
            PnlIncompletenessReasonEntry(
                reason="fill_evidence_gap",
                withholding_tier="untrusted",
                withholding_scope="global",
                trigger_instrument_public_id=None,
            ),
        ),
        per_instrument=(),
        attribution=(),
    )


def _attempt(
    *,
    minute: datetime = _M1,
    observed_at: datetime | None = None,
    balance_status: str = "observed",
    balances_json: str | None = '[{"currency":"USD","total":1000.0}]',
) -> VenueAccountObservationAttemptRow:
    """Build one temporal observation attempt row for the basket gate."""
    resolved_observed_at = minute - timedelta(seconds=30) if observed_at is None else observed_at
    return {
        "id": 1,
        "public_id": "obs-1",
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "attempt_status": "observed",
        "balance_status": balance_status,
        "position_status": "not_applicable",
        "balances_json": balances_json,
        "open_positions_json": None,
        "balance_observed_at": resolved_observed_at if balance_status == "observed" else None,
        "position_observed_at": None,
        "error": None,
        "timestamp": minute,
        "session_id": _SESSION,
        "sequence_id": 1,
    }


def _crypto_candle(
    base: str, minute: datetime, close: float, *, exchange: str = "kraken"
) -> CryptoUsdCandle:
    """Build one crypto→USD candle valuing the given grid minute."""
    return CryptoUsdCandle(
        base=base,
        quote="USD",
        exchange=exchange,
        native_symbol=f"{base}/USD",
        instrument_public_id=f"inst-{base}-{exchange}",
        candle_id=42,
        candle_public_id=f"cndl-{base}",
        candle_open_at=minute - timedelta(minutes=1),
        candle_timestamp=minute - timedelta(minutes=1),
        close=close,
    )


def _crypto_evidence(*candles: CryptoUsdCandle) -> ValuationEvidence:
    """Bundle crypto candles into evidence with an empty fiat plane."""
    planes: dict[tuple[str, datetime], list[CryptoUsdCandle]] = {}
    for candle in candles:
        minute = candle.candle_open_at + timedelta(minutes=1)
        planes.setdefault((candle.base, minute), []).append(candle)
    return ValuationEvidence(fiat_rates={}, fiat_venues={}, fiat_versions={}, crypto_planes=planes)


def _fiat_evidence(minute: datetime, close: float) -> ValuationEvidence:
    """Build evidence pinning one EUR→USD fiat plane at a minute."""
    version = CandleVersionIdentity(
        instrument_public_id="inst-eur",
        native_symbol="EUR/USD",
        candle_id=7,
        candle_public_id="cndl-eur",
        candle_open_at=minute - timedelta(minutes=1),
        candle_timestamp=minute - timedelta(minutes=1),
    )
    return ValuationEvidence(
        fiat_rates={("EUR", "USD", "kraken", minute): close},
        fiat_venues={currency_pair_key("EUR", "USD"): ("EUR", "USD", "kraken")},
        fiat_versions={("EUR", "USD", "kraken", minute): version},
        crypto_planes={},
    )


def _pln_evidence(minute: datetime, close: float) -> ValuationEvidence:
    """Build evidence pinning one inverse USD→PLN fiat plane at a minute."""
    version = CandleVersionIdentity(
        instrument_public_id="inst-pln",
        native_symbol="USD/PLN",
        candle_id=8,
        candle_public_id="cndl-pln",
        candle_open_at=minute - timedelta(minutes=1),
        candle_timestamp=minute - timedelta(minutes=1),
    )
    return ValuationEvidence(
        fiat_rates={("USD", "PLN", "walutomat", minute): close},
        fiat_venues={currency_pair_key("PLN", "USD"): ("USD", "PLN", "walutomat")},
        fiat_versions={("USD", "PLN", "walutomat", minute): version},
        crypto_planes={},
    )


def _attempts(minute: datetime) -> dict[str, VenueAccountObservationAttemptRow]:
    """Return one authoritative kraken attempt for a minute."""
    return {"kraken": _attempt(minute=minute)}


class TestPlanCatchupWindow:
    """The finalized catch-up window bound."""

    def test_first_sample_resumes_after_t0(self) -> None:
        """With no prior sample the window opens at ``t0 + 1m``."""
        now = _T0 + timedelta(minutes=10)
        window = plan_catchup_window(now, None, _T0)
        assert window == ChunkWindow(start=_M1, end=now - timedelta(minutes=2))

    def test_resumes_after_last_sample(self) -> None:
        """A prior sample opens the window one minute past it."""
        now = _T0 + timedelta(minutes=10)
        window = plan_catchup_window(now, _M2, _T0)
        assert window is not None
        assert window.start == _M3

    def test_returns_none_when_nothing_finalized(self) -> None:
        """A start beyond the finalized end yields no window."""
        now = _T0 + timedelta(minutes=2)
        assert plan_catchup_window(now, _M2, _T0) is None

    def test_floors_now_before_subtracting_lag(self) -> None:
        """The end floors ``now`` to the minute before removing the lag."""
        now = _T0 + timedelta(minutes=10, seconds=45)
        window = plan_catchup_window(now, None, _T0)
        assert window is not None
        assert window.end == _T0 + timedelta(minutes=8)


class TestPlanCatchupChunks:
    """Budget-bounded catch-up chunking."""

    def test_single_chunk_when_within_span(self) -> None:
        """A short window is one chunk."""
        window = ChunkWindow(start=_M1, end=_M3)
        assert plan_catchup_chunks(window, 1) == (window,)

    def test_splits_on_max_chunk_minutes(self) -> None:
        """A window longer than the cap splits at the cap boundary."""
        window = ChunkWindow(start=_M1, end=_M1 + timedelta(minutes=1500))
        chunks = plan_catchup_chunks(window, 1, max_chunk_minutes=1440)
        assert chunks[0] == ChunkWindow(start=_M1, end=_M1 + timedelta(minutes=1439))
        assert chunks[1].start == _M1 + timedelta(minutes=1440)
        assert chunks[-1].end == window.end

    def test_budget_narrows_span_below_cap(self) -> None:
        """A high pool count shrinks the chunk span under the minute cap."""
        window = ChunkWindow(start=_M1, end=_M1 + timedelta(minutes=20))
        chunks = plan_catchup_chunks(window, 100, max_chunk_minutes=1440, work_budget=1000)
        assert int((chunks[0].end - chunks[0].start) / timedelta(minutes=1)) + 1 == 10

    def test_span_never_below_one_minute(self) -> None:
        """A pool count above the budget still yields single-minute chunks."""
        window = ChunkWindow(start=_M1, end=_M2)
        chunks = plan_catchup_chunks(window, 10, work_budget=1)
        assert chunks == (ChunkWindow(_M1, _M1), ChunkWindow(_M2, _M2))


class TestPlanLateFillRecompute:
    """The late-fill recompute boundary."""

    def test_none_earliest_returns_none(self) -> None:
        """No affected minute means no recompute."""
        assert plan_late_fill_recompute(None, _M2) is None

    def test_none_last_returns_none(self) -> None:
        """A scope with no persisted sample cannot be invalidated."""
        assert plan_late_fill_recompute(_M1, None) is None

    def test_future_affected_returns_none(self) -> None:
        """An affected minute past the last persisted one is normal catch-up."""
        assert plan_late_fill_recompute(_M3, _M2) is None

    def test_affected_at_or_before_last_returns_start(self) -> None:
        """An affected minute inside history triggers recompute from it."""
        assert plan_late_fill_recompute(_M1, _M2) == _M1
        assert plan_late_fill_recompute(_M2, _M2) == _M2


class TestPlanSelfHeal:
    """Retryable ``incomplete`` self-heal selection."""

    def test_retryable_within_window_selected(self) -> None:
        """A retryable minute inside the lookback is eligible."""
        now = _M3 + timedelta(minutes=1)
        candidate = SelfHealCandidate(point_time=_M2, reason_codes=frozenset({"missing_mark"}))
        assert plan_self_heal_minutes([candidate], now) == (_M2,)

    def test_terminal_reason_excluded(self) -> None:
        """A minute carrying a terminal reason is never retried."""
        now = _M3
        candidate = SelfHealCandidate(point_time=_M2, reason_codes=frozenset({"non_finite"}))
        assert plan_self_heal_minutes([candidate], now) == ()

    def test_mixed_reasons_excluded(self) -> None:
        """A minute with any terminal reason is excluded even beside retryables."""
        now = _M3
        candidate = SelfHealCandidate(
            point_time=_M2, reason_codes=frozenset({"missing_mark", "non_finite"})
        )
        assert plan_self_heal_minutes([candidate], now) == ()

    def test_empty_reason_excluded(self) -> None:
        """A candidate with no reason codes is not eligible."""
        now = _M3
        candidate = SelfHealCandidate(point_time=_M2, reason_codes=frozenset())
        assert plan_self_heal_minutes([candidate], now) == ()

    def test_older_than_lookback_excluded(self) -> None:
        """A retryable minute older than the lookback is honest and final."""
        now = _M2 + timedelta(minutes=30)
        candidate = SelfHealCandidate(point_time=_M2, reason_codes=frozenset({"missing_mark"}))
        assert plan_self_heal_minutes([candidate], now) == ()

    def test_results_are_sorted(self) -> None:
        """Eligible minutes are returned ascending."""
        now = _M3 + timedelta(minutes=1)
        candidates = [
            SelfHealCandidate(point_time=_M3, reason_codes=frozenset({"basket_stale"})),
            SelfHealCandidate(point_time=_M2, reason_codes=frozenset({"missing_fx_rate"})),
        ]
        assert plan_self_heal_minutes(candidates, now) == (_M2, _M3)

    def test_unknown_code_stays_eligible(self) -> None:
        """An unrecognised code is NOT final, so its minute stays eligible.

        The executable statement of forward compatibility, and the property the
        whole two-release rollout rests on: this reader must keep healing a minute
        a newer writer stamped with a code it has never heard of. The gate is a
        FINAL deny-list precisely so this case answers "retry", because refusing
        is permanent while retrying costs at most the bounded lookback.
        """
        now = _M3 + timedelta(minutes=1)
        candidate = SelfHealCandidate(point_time=_M2, reason_codes=frozenset({"a_future_code"}))
        assert plan_self_heal_minutes([candidate], now) == (_M2,)

    def test_mixed_unknown_and_final_excluded(self) -> None:
        """A known terminal code still excludes the minute beside an unknown one."""
        now = _M3
        candidate = SelfHealCandidate(
            point_time=_M2, reason_codes=frozenset({"a_future_code", "non_finite"})
        )
        assert plan_self_heal_minutes([candidate], now) == ()

    @pytest.mark.parametrize("code", sorted(PNL_SAMPLE_RETRYABLE_REASONS))
    def test_every_retryable_code_is_eligible_alone(self, code: str) -> None:
        """Every retryable code on its own makes its minute eligible."""
        now = _M3 + timedelta(minutes=1)
        candidate = SelfHealCandidate(point_time=_M2, reason_codes=frozenset({code}))
        assert plan_self_heal_minutes([candidate], now) == (_M2,)

    @pytest.mark.parametrize("code", sorted(PNL_SAMPLE_FINAL_REASONS))
    def test_every_final_code_is_excluded_alone(self, code: str) -> None:
        """Every final code on its own excludes its minute."""
        now = _M3 + timedelta(minutes=1)
        candidate = SelfHealCandidate(point_time=_M2, reason_codes=frozenset({code}))
        assert plan_self_heal_minutes([candidate], now) == ()


class TestResolveDrawdown:
    """The per-minute drawdown recursion and its demotions."""

    def test_first_minute_is_zero_drawdown(self) -> None:
        """A first positive equity sets the peak at zero drawdown."""
        outcome = resolve_drawdown(None, 100.0)
        assert outcome.drawdown == 0.0
        assert outcome.peak == 100.0
        assert outcome.demoted is False

    def test_new_high_advances_peak(self) -> None:
        """Equity above the prior peak becomes the new peak at zero drawdown."""
        outcome = resolve_drawdown(100.0, 150.0)
        assert outcome.drawdown == 0.0
        assert outcome.peak == 150.0

    def test_drop_from_peak_is_fraction(self) -> None:
        """Equity below the peak yields the drawdown fraction, peak unchanged."""
        outcome = resolve_drawdown(100.0, 75.0)
        assert outcome.drawdown == pytest.approx(0.25)
        assert outcome.peak == 100.0

    def test_non_finite_prior_peak_demotes(self) -> None:
        """A corrupt persisted peak demotes the minute without moving the peak."""
        outcome = resolve_drawdown(math.inf, 100.0)
        assert outcome.demoted is True
        assert outcome.drawdown is None
        assert outcome.peak == math.inf

    def test_zero_equity_zero_peak_is_rest(self) -> None:
        """An empty portfolio at a zero peak is a real zero drawdown."""
        outcome = resolve_drawdown(None, 0.0)
        assert outcome.drawdown == 0.0
        assert outcome.peak == 0.0

    def test_negative_equity_above_band_demotes(self) -> None:
        """Negative equity against a positive peak exceeds the band and demotes."""
        outcome = resolve_drawdown(100.0, -50.0)
        assert outcome.demoted is True
        assert outcome.peak == 100.0

    def test_negative_first_equity_demotes(self) -> None:
        """A first negative equity yields a non-positive peak and demotes."""
        outcome = resolve_drawdown(None, -10.0)
        assert outcome.demoted is True
        assert outcome.drawdown is None


class TestEvaluateBasket:
    """The per-minute basket authority gate."""

    def test_authoritative_single_venue(self) -> None:
        """One fresh observed venue yields the whole basket and its record."""
        outcome = evaluate_basket(_M1, frozenset({"kraken"}), _attempts(_M1))
        assert outcome.reason_codes == frozenset()
        assert outcome.observed_balances == {("kraken", "USD"): 1000.0}
        assert outcome.observations[0]["observation_public_id"] == "obs-1"

    def test_missing_venue(self) -> None:
        """An expected venue with no attempt is a missing-venue basket."""
        outcome = evaluate_basket(_M1, frozenset({"kraken"}), {})
        assert outcome.reason_codes == frozenset({"basket_missing_venue"})

    def test_unobserved_attempt_is_stale(self) -> None:
        """A latest attempt that failed to observe is a stale basket."""
        attempts = {"kraken": _attempt(minute=_M1, balance_status="error")}
        outcome = evaluate_basket(_M1, frozenset({"kraken"}), attempts)
        assert outcome.reason_codes == frozenset({"basket_stale"})

    def test_look_ahead_observation_is_future_clock(self) -> None:
        """A balance observed after the minute is a terminal clock anomaly (D1).

        The observation read already cut on bus time, so an ``observed_at`` past
        the minute cannot be a transient staleness — it is a clock inversion, so
        the minute withholds with the FINAL ``future_clock`` rather than the
        retryable ``basket_stale``.
        """
        attempts = {"kraken": _attempt(minute=_M1, observed_at=_M1 + timedelta(seconds=10))}
        outcome = evaluate_basket(_M1, frozenset({"kraken"}), attempts)
        assert outcome.reason_codes == frozenset({"future_clock"})

    def test_observed_without_timestamp_is_stale(self) -> None:
        """An ``observed`` attempt lacking an observed-at timestamp is stale.

        With no ``balance_observed_at`` the authority window cannot be
        reconstructed, so the basket withholds with the retryable ``basket_stale``
        rather than the terminal ``future_clock``.
        """
        attempt = _attempt(minute=_M1)
        attempt["balance_observed_at"] = None
        outcome = evaluate_basket(_M1, frozenset({"kraken"}), {"kraken": attempt})
        assert outcome.reason_codes == frozenset({"basket_stale"})

    def test_elapsed_authority_window_is_stale(self) -> None:
        """An observation older than the freshness ceiling is a stale basket."""
        attempts = {"kraken": _attempt(minute=_M1, observed_at=_M1 - timedelta(seconds=400))}
        outcome = evaluate_basket(_M1, frozenset({"kraken"}), attempts)
        assert outcome.reason_codes == frozenset({"basket_stale"})

    def test_missing_balances_json_is_non_finite(self) -> None:
        """An observed attempt with no balances payload is structurally broken."""
        attempts = {"kraken": _attempt(minute=_M1, balances_json=None)}
        outcome = evaluate_basket(_M1, frozenset({"kraken"}), attempts)
        assert outcome.reason_codes == frozenset({"non_finite"})

    def test_synthetic_collateral_currency_is_non_finite(self) -> None:
        """A synthetic collateral currency fails the basket closed."""
        payload = '[{"currency":"USD_collateral_value","total":1.0}]'
        attempts = {"kraken": _attempt(minute=_M1, balances_json=payload)}
        outcome = evaluate_basket(_M1, frozenset({"kraken"}), attempts)
        assert outcome.reason_codes == frozenset({"non_finite"})

    @pytest.mark.parametrize(
        "payload",
        [
            "{not json",
            '{"currency":"USD"}',
            '[{"currency":"","total":1.0}]',
            '[{"currency":"USD","total":"x"}]',
            '[{"currency":"USD","total":true}]',
            '["USD"]',
            '[{"currency":"USD","total":1e400}]',
        ],
    )
    def test_malformed_balances_are_non_finite(self, payload: str) -> None:
        """Any structurally invalid balances payload is terminal ``non_finite``."""
        attempts = {"kraken": _attempt(minute=_M1, balances_json=payload)}
        outcome = evaluate_basket(_M1, frozenset({"kraken"}), attempts)
        assert outcome.reason_codes == frozenset({"non_finite"})

    def test_multiple_currency_entries_aggregate(self) -> None:
        """Repeated currencies on one venue accumulate."""
        payload = '[{"currency":"USD","total":10.0},{"currency":"USD","total":5.0}]'
        attempts = {"kraken": _attempt(minute=_M1, balances_json=payload)}
        outcome = evaluate_basket(_M1, frozenset({"kraken"}), attempts)
        assert outcome.observed_balances == {("kraken", "USD"): 15.0}


class TestValueBasket:
    """The per-minute basket-to-USD valuation."""

    def test_prices_crypto_and_usd(self) -> None:
        """A crypto plus USD basket sums into one USD equity total."""
        evidence = _crypto_evidence(_crypto_candle("BTC", _M1, 60000.0))
        balances = {("kraken", "BTC"): 0.5, ("kraken", "USD"): 100.0}
        outcome = value_basket(balances, _M1, evidence)
        assert outcome.equity == pytest.approx(30100.0)
        btc = next(record for record in outcome.valuation if record["currency"] == "BTC")
        assert btc["close"] == 60000.0
        assert btc["orientation"] == "crypto"

    def test_missing_crypto_plane_is_missing_mark(self) -> None:
        """An unpriceable crypto leg withholds with ``missing_mark``."""
        balances = {("kraken", "BTC"): 0.5}
        outcome = value_basket(balances, _M1, _crypto_evidence())
        assert outcome.equity is None
        assert outcome.reason_codes == frozenset({"missing_mark"})

    def test_missing_fiat_rate_is_missing_fx_rate(self) -> None:
        """An unpriceable fiat leg withholds with ``missing_fx_rate``."""
        evidence = ValuationEvidence(
            fiat_rates={},
            fiat_venues={currency_pair_key("EUR", "USD"): ("EUR", "USD", "kraken")},
            fiat_versions={},
            crypto_planes={},
        )
        outcome = value_basket({("kraken", "EUR"): 10.0}, _M1, evidence)
        assert outcome.reason_codes == frozenset({"missing_fx_rate"})

    def test_priced_fiat_leg(self) -> None:
        """A pinned fiat plane prices its leg into USD."""
        outcome = value_basket({("kraken", "EUR"): 10.0}, _M1, _fiat_evidence(_M1, 1.1))
        assert outcome.equity == pytest.approx(11.0)

    def test_non_finite_quantity_is_non_finite(self) -> None:
        """A non-finite balance quantity is terminal."""
        outcome = value_basket({("kraken", "BTC"): math.inf}, _M1, _crypto_evidence())
        assert outcome.reason_codes == frozenset({"non_finite"})

    def test_overflowing_sum_is_non_finite(self) -> None:
        """Finite legs whose sum overflows to infinity are terminal."""
        evidence = _crypto_evidence(
            _crypto_candle("BTC", _M1, 1e308), _crypto_candle("ETH", _M1, 1e308)
        )
        balances = {("kraken", "BTC"): 1.0, ("kraken", "ETH"): 1.0}
        outcome = value_basket(balances, _M1, evidence)
        assert outcome.reason_codes == frozenset({"non_finite"})

    def test_zero_balance_leg_has_no_provenance(self) -> None:
        """A zero-quantity currency prices to zero and records no valuation leg."""
        balances = {("kraken", "USD"): 100.0, ("kraken", "BTC"): 0.0}
        outcome = value_basket(balances, _M1, _crypto_evidence())
        assert outcome.equity == 100.0
        assert all(record["currency"] != "BTC" for record in outcome.valuation)

    def test_empty_basket_is_zero_equity(self) -> None:
        """An empty basket prices to zero with no valuation records."""
        outcome = value_basket({}, _M1, _crypto_evidence())
        assert outcome.equity == 0.0
        assert outcome.valuation == ()


class TestAssembleMinuteSample:
    """The combined R1 truth table for one minute."""

    def test_untrusted_point_writes_no_row(self) -> None:
        """An untrusted P&L point produces no sample and holds the peak."""
        inputs = MinuteInputs(_untrusted_point(_M1), _attempts(_M1), _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), 50.0)
        assert plan.sample is None
        assert plan.peak == 50.0

    def test_mark_incomplete_keeps_cumulatives(self) -> None:
        """A mark-incomplete point persists cumulatives with ``missing_mark``."""
        inputs = MinuteInputs(_mark_incomplete_point(_M1), _attempts(_M1), _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
        assert plan.sample is not None
        assert plan.sample.valuation_status == "incomplete"
        assert plan.sample.realized_pnl == 1.0
        assert json.loads(plan.sample.audit_json)["reason_codes"] == ["missing_mark"]

    def test_mark_incomplete_fx_reason_maps_to_missing_fx_rate(self) -> None:
        """An ``fx_conversion_unproven`` mark reason persists as ``missing_fx_rate`` (D1)."""
        point = _mark_incomplete_point_with(_M1, "fx_conversion_unproven")
        inputs = MinuteInputs(point, _attempts(_M1), _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
        assert plan.sample is not None
        assert json.loads(plan.sample.audit_json)["reason_codes"] == ["missing_fx_rate"]

    def test_mark_incomplete_non_finite_reason_maps_to_non_finite(self) -> None:
        """A non-finite unrealized mark reason persists as terminal ``non_finite`` (D1)."""
        point = _mark_incomplete_point_with(_M1, "unrealized_non_finite")
        inputs = MinuteInputs(point, _attempts(_M1), _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
        assert plan.sample is not None
        assert json.loads(plan.sample.audit_json)["reason_codes"] == ["non_finite"]

    def test_mark_incomplete_unmapped_reason_falls_back_to_missing_mark(self) -> None:
        """A mark-incomplete cause outside the map falls back to ``missing_mark`` (D1)."""
        point = _mark_incomplete_point_with(_M1, "cost_basis_unavailable")
        inputs = MinuteInputs(point, _attempts(_M1), _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
        assert plan.sample is not None
        assert json.loads(plan.sample.audit_json)["reason_codes"] == ["missing_mark"]

    def test_mark_incomplete_distinct_reasons_map_to_distinct_codes(self) -> None:
        """Distinct mark reasons map to distinct codes rather than flattening (D1)."""
        point = _mark_incomplete_point_with(_M1, "mark_unavailable", "fx_conversion_unproven")
        inputs = MinuteInputs(point, _attempts(_M1), _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
        assert plan.sample is not None
        assert json.loads(plan.sample.audit_json)["reason_codes"] == [
            "missing_fx_rate",
            "missing_mark",
        ]

    def test_complete_minute(self) -> None:
        """A mark-complete, authoritative, priced minute is a complete sample."""
        inputs = MinuteInputs(_complete_point(_M1), _attempts(_M1), _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
        assert plan.sample is not None
        assert plan.sample.valuation_status == "complete"
        assert plan.sample.cash_usd == 1000.0
        assert plan.sample.position_value_usd == 0.0
        assert plan.sample.drawdown == 0.0
        assert plan.peak == 1000.0

    def test_complete_point_with_stale_basket_is_incomplete(self) -> None:
        """A complete point over a stale basket demotes to a basket reason."""
        attempts = {"kraken": _attempt(minute=_M1, balance_status="error")}
        inputs = MinuteInputs(_complete_point(_M1), attempts, _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
        assert plan.sample is not None
        assert json.loads(plan.sample.audit_json)["reason_codes"] == ["basket_stale"]

    def test_mark_incomplete_and_missing_venue_unions_reasons(self) -> None:
        """A mark-incomplete point plus a missing venue unions both reasons."""
        inputs = MinuteInputs(_mark_incomplete_point(_M1), {}, _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
        assert plan.sample is not None
        assert json.loads(plan.sample.audit_json)["reason_codes"] == [
            "basket_missing_venue",
            "missing_mark",
        ]

    def test_complete_point_unpriceable_basket_is_incomplete(self) -> None:
        """A complete point whose basket cannot price withholds the equity plane."""
        payload = '[{"currency":"BTC","total":1.0}]'
        attempts = {"kraken": _attempt(minute=_M1, balances_json=payload)}
        inputs = MinuteInputs(_complete_point(_M1), attempts, _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
        assert plan.sample is not None
        assert plan.sample.valuation_status == "incomplete"
        assert json.loads(plan.sample.audit_json)["reason_codes"] == ["missing_mark"]

    def test_complete_point_demoted_drawdown_is_non_finite(self) -> None:
        """A priced minute whose peak cannot draw down demotes to ``non_finite``."""
        inputs = MinuteInputs(_complete_point(_M1), _attempts(_M1), _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), math.inf)
        assert plan.sample is not None
        assert plan.sample.valuation_status == "incomplete"
        assert json.loads(plan.sample.audit_json)["reason_codes"] == ["non_finite"]

    def test_position_partition_labels_position_value(self) -> None:
        """A held spot position labels part of the equity as position value."""
        evidence = _crypto_evidence(_crypto_candle("BTC", _M1, 100.0))
        payload = '[{"currency":"BTC","total":2.0}]'
        attempts = {"kraken": _attempt(minute=_M1, balances_json=payload)}
        versions = [_version("kraken", "BTC", 1.5, valid_from=_T0)]
        inputs = MinuteInputs(_complete_point(_M1), attempts, evidence)
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), versions, None)
        assert plan.sample is not None
        assert plan.sample.position_value_usd == pytest.approx(150.0)
        assert plan.sample.cash_usd == pytest.approx(50.0)
        assert json.loads(plan.sample.audit_json)["coverage"]["venue_scope"] == "spot_only"

    def test_position_opened_after_minute_is_not_labelled(self) -> None:
        """A position whose version opens after the minute never labels it (A2)."""
        evidence = _crypto_evidence(_crypto_candle("BTC", _M1, 100.0))
        payload = '[{"currency":"BTC","total":2.0}]'
        attempts = {"kraken": _attempt(minute=_M1, balances_json=payload)}
        versions = [_version("kraken", "BTC", 1.5, valid_from=_M2)]
        inputs = MinuteInputs(_complete_point(_M1), attempts, evidence)
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), versions, None)
        assert plan.sample is not None
        assert plan.sample.position_value_usd == 0.0
        assert plan.sample.cash_usd == pytest.approx(200.0)

    def test_inverse_fiat_full_attribution_yields_exactly_zero_cash(self) -> None:
        """A fully-attributed inverse-fiat balance leaves cash bit-exactly zero (N2)."""
        evidence = _pln_evidence(_M1, 4.0)
        payload = '[{"currency":"PLN","total":400.0}]'
        attempts = {"walutomat": _attempt(minute=_M1, balances_json=payload)}
        versions = [_version("walutomat", "PLN", 400.0, valid_from=_T0)]
        inputs = MinuteInputs(_complete_point(_M1), attempts, evidence)
        plan = assemble_minute_sample(inputs, frozenset({"walutomat"}), versions, None)
        assert plan.sample is not None
        assert plan.sample.position_value_usd == pytest.approx(100.0)
        assert plan.sample.cash_usd == 0.0

    def test_inverse_fiat_partial_attribution_splits_exactly(self) -> None:
        """A partially-attributed inverse-fiat balance splits without ULP residue (N2)."""
        evidence = _pln_evidence(_M1, 4.0)
        payload = '[{"currency":"PLN","total":400.0}]'
        attempts = {"walutomat": _attempt(minute=_M1, balances_json=payload)}
        versions = [_version("walutomat", "PLN", 100.0, valid_from=_T0)]
        inputs = MinuteInputs(_complete_point(_M1), attempts, evidence)
        plan = assemble_minute_sample(inputs, frozenset({"walutomat"}), versions, None)
        assert plan.sample is not None
        assert plan.sample.position_value_usd == pytest.approx(25.0)
        assert plan.sample.cash_usd == pytest.approx(75.0)

    def test_high_magnitude_full_attribution_yields_exactly_zero_cash(self) -> None:
        """A 1e16 + 1 + 1 fully-attributed basket sums identically ⇒ cash exactly 0 (P2)."""
        attempts = {
            "kraken": _attempt(minute=_M1, balances_json='[{"currency":"USD","total":1e16}]'),
            "binance": _attempt(minute=_M1, balances_json='[{"currency":"USD","total":1.0}]'),
            "walutomat": _attempt(minute=_M1, balances_json='[{"currency":"USD","total":1.0}]'),
        }
        versions = [
            _version("kraken", "USD", 1e16, valid_from=_T0),
            _version("binance", "USD", 1.0, valid_from=_T0),
            _version("walutomat", "USD", 1.0, valid_from=_T0),
        ]
        inputs = MinuteInputs(_complete_point(_M1), attempts, _crypto_evidence())
        venues = frozenset({"kraken", "binance", "walutomat"})
        plan = assemble_minute_sample(inputs, venues, versions, None)
        assert plan.sample is not None
        assert plan.sample.cash_usd == 0.0
        assert plan.sample.position_value_usd == math.fsum([1e16, 1.0, 1.0])

    def test_overflowing_aggregation_demotes_to_incomplete(self) -> None:
        """An equity aggregation that overflows demotes the minute (P2)."""
        attempts = {
            "kraken": _attempt(minute=_M1, balances_json='[{"currency":"USD","total":1e308}]'),
            "binance": _attempt(minute=_M1, balances_json='[{"currency":"USD","total":1e308}]'),
        }
        inputs = MinuteInputs(_complete_point(_M1), attempts, _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken", "binance"}), (), None)
        assert plan.sample is not None
        assert plan.sample.valuation_status == "incomplete"
        assert json.loads(plan.sample.audit_json)["reason_codes"] == ["non_finite"]

    def test_partition_overflow_demotes_the_minute(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A partition aggregation overflow demotes the minute even with finite equity (P2)."""
        monkeypatch.setattr(
            "snapper.application.portfolio.pnl_snapshot_planner._partition_position",
            lambda *args: _PartitionOutcome(
                position_value=None, leveraged_excluded=False, non_finite_excluded=False
            ),
        )
        inputs = MinuteInputs(_complete_point(_M1), _attempts(_M1), _crypto_evidence())
        plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
        assert plan.sample is not None
        assert plan.sample.valuation_status == "incomplete"
        assert json.loads(plan.sample.audit_json)["reason_codes"] == ["non_finite"]


class TestPlanChunkSamples:
    """Peak threading across a chunk of minutes."""

    def test_threads_peak_across_complete_minutes(self) -> None:
        """The running peak advances only across complete minutes."""
        minute_inputs = [
            MinuteInputs(
                _complete_point(_M1), {"kraken": _attempt(minute=_M1)}, _crypto_evidence()
            ),
            MinuteInputs(
                _complete_point(_M2),
                {
                    "kraken": _attempt(
                        minute=_M2, balances_json='[{"currency":"USD","total":800.0}]'
                    )
                },
                _crypto_evidence(),
            ),
        ]
        plan = plan_chunk_samples(minute_inputs, frozenset({"kraken"}), (), None)
        assert len(plan.samples) == 2
        assert plan.samples[1].drawdown == pytest.approx(0.2)
        assert plan.peak == 1000.0

    def test_untrusted_minute_omitted(self) -> None:
        """An untrusted minute contributes no row and holds the peak."""
        minute_inputs = [MinuteInputs(_untrusted_point(_M1), _attempts(_M1), _crypto_evidence())]
        plan = plan_chunk_samples(minute_inputs, frozenset({"kraken"}), (), 42.0)
        assert plan.samples == ()
        assert plan.peak == 42.0


class TestReasonCodeContract:
    """The single canonical reason-code declaration and its partition.

    These assertions carry the whole contract on their own: a frozenset member and
    a Literal member are DATA, not branches, so coverage exerts no pressure on
    them whatsoever. A code added to one declaration and forgotten in another is
    caught here or nowhere.
    """

    def test_literal_matches_the_partition_union(self) -> None:
        """The Literal's members are exactly the union of the two partitions.

        ``SampleReasonCode`` is a PEP-695 alias, so ``get_args`` on the alias
        object itself returns ``()`` and this assertion would pass vacuously
        against an empty left-hand side; ``.__value__`` unwraps it to the real
        ``Literal`` whose args are the members.
        """
        assert set(get_args(SampleReasonCode.__value__)) == (
            PNL_SAMPLE_RETRYABLE_REASONS | PNL_SAMPLE_FINAL_REASONS
        )

    def test_partitions_are_disjoint(self) -> None:
        """No code is both retryable and final."""
        assert PNL_SAMPLE_RETRYABLE_REASONS.isdisjoint(PNL_SAMPLE_FINAL_REASONS)

    def test_contract_cardinality(self) -> None:
        """The canonical set and the final partition have their declared sizes.

        Guards the tests parametrized off these frozensets: pytest's
        ``empty_parameter_set_mark`` is unset, so a regression that emptied a set
        would silently SKIP every case rather than fail.
        """
        assert len(PNL_SAMPLE_REASON_CODES) == 8
        assert len(PNL_SAMPLE_FINAL_REASONS) == 4

    def test_never_persist_is_final(self) -> None:
        """A never-persisted code is terminal, never retryable."""
        assert PNL_SAMPLE_NEVER_PERSIST_REASONS <= PNL_SAMPLE_FINAL_REASONS

    def test_mapped_codes_are_canonical(self) -> None:
        """Every code the 5A reason map can emit is writable by the validator."""
        assert set(_POINT_REASON_TO_SAMPLE_CODE.values()) <= PNL_SAMPLE_REASON_CODES

    @pytest.mark.parametrize("code", sorted(PNL_SAMPLE_REASON_CODES))
    def test_every_code_round_trips_its_partition(self, code: str) -> None:
        """Each canonical code survives write-then-read and lands in its partition.

        Parametrized off the real frozenset rather than ``get_args`` so a
        ``get_args`` regression cannot empty the parameter set. The path exercised
        is the production one end to end: the planner's audit envelope, the
        service's reader, and the planner's eligibility gate.
        """
        audit = _incomplete_audit_json(frozenset({cast(SampleReasonCode, code)}))
        candidate = SelfHealCandidate(point_time=_M2, reason_codes=_extract_reason_codes(audit))
        expected = (_M2,) if code in PNL_SAMPLE_RETRYABLE_REASONS else ()
        assert plan_self_heal_minutes([candidate], _M3 + timedelta(minutes=1)) == expected


def _to_row(sample: PlannedSample) -> PortfolioPnlSampleRow:
    """Stamp writer provenance onto a planned sample, mirroring the service."""
    return {
        "public_id": str(uuid7()),
        "session_id": _SESSION,
        "sequence_id": 5,
        "timestamp": sample.point_time,
        "wallet_public_id": _WALLET,
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": sample.point_time,
        "point_kind": "sample",
        "epoch_public_id": _EPOCH,
        "calc_version": PNL_SAMPLE_CALC_VERSION,
        "valuation_status": sample.valuation_status,
        "realized_pnl": sample.realized_pnl,
        "fee_pnl": sample.fee_pnl,
        "accrual_pnl": sample.accrual_pnl,
        "external_flow_adjustment": 0.0,
        "unrealized_pnl": sample.unrealized_pnl,
        "cash_usd": sample.cash_usd,
        "position_value_usd": sample.position_value_usd,
        "drawdown": sample.drawdown,
        "mark_source": sample.mark_source,
        "mark_time": sample.mark_time,
        "audit_json": sample.audit_json,
        "watermarks_json": '{"kraken":5}',
    }


def _version(
    exchange: str,
    base_currency: str,
    quantity: float,
    *,
    valid_from: datetime,
    is_spot_margin: bool = False,
) -> PositionVersion:
    """Build one temporal position version active from ``valid_from`` onward."""
    return PositionVersion(
        entry=PositionInventoryEntry(
            exchange=exchange,
            base_currency=base_currency,
            quantity=quantity,
            is_spot_margin=is_spot_margin,
        ),
        valid_from=valid_from,
        valid_to=datetime(2099, 1, 1, tzinfo=UTC),
    )


def _scope() -> PortfolioPnlSampleScope:
    """Build the write scope backing the round-trip insert."""
    return PortfolioPnlSampleScope(
        wallet_public_id=_WALLET,
        mode="live",
        valuation_ccy="USD",
        epoch_public_id=_EPOCH,
        anchor_point_time=_T0,
    )


def _plan_complete() -> PlannedSample:
    """Plan one canonical complete sample for the round-trip proof."""
    inputs = MinuteInputs(_complete_point(_M1), _attempts(_M1), _crypto_evidence())
    plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
    assert plan.sample is not None
    return plan.sample


def _plan_incomplete() -> PlannedSample:
    """Plan one canonical incomplete sample for the round-trip proof."""
    inputs = MinuteInputs(_mark_incomplete_point(_M2), _attempts(_M2), _crypto_evidence())
    plan = assemble_minute_sample(inputs, frozenset({"kraken"}), (), None)
    assert plan.sample is not None
    return plan.sample


class TestPlannerOutputAcceptedByValidator:
    """Planner rows satisfy the S2 sample validator and writer verbatim."""

    def test_complete_row_validates(self) -> None:
        """A planned complete row passes the combined-status validator."""
        SQLAlchemyRepository._validate_portfolio_pnl_sample(_to_row(_plan_complete()), _scope())

    def test_incomplete_row_validates(self) -> None:
        """A planned incomplete row passes the combined-status validator."""
        SQLAlchemyRepository._validate_portfolio_pnl_sample(_to_row(_plan_incomplete()), _scope())

    @pytest.mark.asyncio
    async def test_writer_accepts_planner_rows_in_sqlite(self, tmp_path: Path) -> None:
        """The real SQLite writer persists both planned rows for a scope."""
        db_path = tmp_path / "planner-roundtrip.db"
        schema_engine = create_engine(f"sqlite:///{db_path}")
        PortfolioPnlPoint.__table__.create(schema_engine)
        VenueAccountObservation.__table__.create(schema_engine)
        schema_engine.dispose()
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
        try:
            async with repo.session() as session:
                session.add(_anchor_orm())
                await session.commit()
            result = await repo.record_portfolio_pnl_samples(
                [_to_row(_plan_complete()), _to_row(_plan_incomplete())], _scope()
            )
            assert result["inserted"] == (_M1, _M2)
        finally:
            await repo.engine.dispose()


def _anchor_orm() -> PortfolioPnlPoint:
    """Build the durable USD activation anchor the writer verifies against."""
    return PortfolioPnlPoint(
        public_id=portfolio_pnl_anchor_public_id(_WALLET, "live", "USD"),
        session_id=_SESSION,
        sequence_id=1,
        timestamp=_T0,
        wallet_public_id=_WALLET,
        mode="live",
        valuation_ccy="USD",
        point_time=_T0,
        point_kind="anchor",
        epoch_public_id=_EPOCH,
        calc_version="5A.13",
        valuation_status="complete",
        realized_pnl=0.0,
        fee_pnl=0.0,
        accrual_pnl=0.0,
        external_flow_adjustment=0.0,
        unrealized_pnl=5.0,
        mark_source="finalized_1m",
        mark_time=_T0,
        opening_basket_json="{}",
        known_to=KNOWN_TO_MAX,
    )
