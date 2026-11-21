"""Width, requirement-set, post-certification, and Sonar checkpoint tests."""

import ast
import math
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from snapper.application.portfolio import pnl_conversion_mark_evidence as conversion
from snapper.application.portfolio import pnl_timeline_service as service
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointFallback
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import (
    verify_checkpoint_for_resume,
)
from snapper.application.portfolio.pnl_timeline import TimelineAccrual
from snapper.application.portfolio.pnl_timeline import TimelineExecution
from snapper.core.numeric import is_positive_finite
from snapper.data.pnl_correction_state import IdAggregate
from snapper.data.pnl_correction_state import PnlOpeningCheckpointRecord
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlTimelineAccrualRow
from snapper.data.repository_types import PnlTimelineCandleRow
from snapper.data.repository_types import PnlTimelineOpeningExecutionRow
from tests.application.portfolio import test_pnl_opening_checkpoint as checkpoint_fixtures
from tests.application.portfolio import test_pnl_opening_checkpoint_capture as capture
from tests.application.portfolio import test_pnl_opening_checkpoint_resume as resume_fixtures
from tests.application.portfolio import test_pnl_opening_checkpoint_verifier as verifier
from tests.application.portfolio import test_pnl_timeline_service as fixtures

_Requirement = tuple[str, str, str, datetime]


@dataclass
class _RequirementLog:
    """Requirement sets observed from each anchor or resumed valuation."""

    marks: list[set[_Requirement]] = field(default_factory=list)
    events: list[set[_Requirement]] = field(default_factory=list)
    conversions: list[set[_Requirement]] = field(default_factory=list)


def _flatten_requirements(
    requirements: dict[str, dict[tuple[str, str], set[datetime]]],
) -> set[_Requirement]:
    """Flatten instrument/pair/minute requirements into one comparable set.

    Args:
        requirements: Service requirements grouped by instrument and pair.

    Returns:
        Flattened instrument, first leg, second leg, and minute tuples.
    """
    return {
        (instrument, pair[0], pair[1], minute)
        for instrument, pair_requirements in requirements.items()
        for pair, minutes in pair_requirements.items()
        for minute in minutes
    }


def _execution_conversions(
    row: PnlTimelineOpeningExecutionRow,
    price_currency: str | None,
    valuation_ccy: str,
) -> set[_Requirement]:
    """Project the actual foreign fee and price conversions of one execution.

    Args:
        row: Source execution passed to the conversion mapper.
        price_currency: Proven execution-price currency.
        valuation_ccy: Target valuation currency.

    Returns:
        Conversion requirements for the row's foreign inputs.
    """
    result: set[_Requirement] = set()
    minute = row["timestamp"].replace(second=0, microsecond=0)
    currencies = []
    if math.isfinite(row["fee"]) and row["fee"] != 0.0:
        currencies.append(row["fee_asset"])
    if price_currency is not None and is_positive_finite(row["price"]):
        currencies.append(price_currency)
    for currency in currencies:
        if currency != valuation_ccy:
            pair = currency_pair_key(currency, valuation_ccy)
            result.add((row["instrument_public_id"], pair[0], pair[1], minute))
    return result


def _install_requirement_spies(
    monkeypatch: pytest.MonkeyPatch,
    log: _RequirementLog,
) -> None:
    """Spy FX requirement loading and stateless execution/accrual conversion.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        log: Mutable per-build requirement log.
    """
    original_fx = service._load_wallet_series_fx
    original_execution = conversion._execution_with_conversion_mark_evidence
    original_accrual = conversion._accrual_with_conversion_mark_evidence

    async def spy_fx(
        repo: Repository,
        request: service._FxRequest,
        options: service.PnlSeriesReplayOptions,
        policy: service.PnlSeriesReadPolicy,
    ) -> service._SeriesFxEvidence:
        """Record mark and event sets before delegating to the real loader.

        Args:
            repo: Repository being read.
            request: FX requirement request.
            options: Replay options.
            policy: Series read policy.

        Returns:
            The real loaded FX evidence.
        """
        log.marks.append(_flatten_requirements(dict(request.mark_requirements)))
        log.events.append(_flatten_requirements(dict(request.event_requirements)))
        return await original_fx(repo, request, options, policy)

    def spy_execution(
        converted: TimelineExecution,
        row: PnlTimelineOpeningExecutionRow,
        price_currency: str | None,
        requirements: dict[str, dict[tuple[str, str], set[datetime]]],
        valuation_ccy: str,
    ) -> TimelineExecution:
        """Record actual execution conversions and call the real mapper.

        Args:
            converted: Converted engine execution.
            row: Source execution row.
            price_currency: Proven price currency.
            requirements: Event requirements used for the flag.
            valuation_ccy: Target valuation currency.

        Returns:
            The real mapper's execution.
        """
        log.conversions[-1].update(_execution_conversions(row, price_currency, valuation_ccy))
        return original_execution(converted, row, price_currency, requirements, valuation_ccy)

    def spy_accrual(
        converted: TimelineAccrual,
        row: PnlTimelineAccrualRow,
        requirements: dict[str, dict[tuple[str, str], set[datetime]]],
        valuation_ccy: str,
    ) -> TimelineAccrual:
        """Record actual accrual conversion and call the real mapper.

        Args:
            converted: Converted engine accrual.
            row: Source accrual row.
            requirements: Event requirements used for the flag.
            valuation_ccy: Target valuation currency.

        Returns:
            The real mapper's accrual.
        """
        minute = row["accrued_at"].replace(second=0, microsecond=0)
        if math.isfinite(row["amount"]) and row["amount"] != 0.0:
            currency = row["amount_asset"]
            if currency != valuation_ccy:
                pair = currency_pair_key(currency, valuation_ccy)
                log.conversions[-1].add((row["instrument_public_id"], pair[0], pair[1], minute))
        return original_accrual(converted, row, requirements, valuation_ccy)

    monkeypatch.setattr(service, "_load_wallet_series_fx", spy_fx)
    monkeypatch.setattr(conversion, "_execution_with_conversion_mark_evidence", spy_execution)
    monkeypatch.setattr(conversion, "_accrual_with_conversion_mark_evidence", spy_accrual)


def _restricted(requirements: set[_Requirement], tip: datetime) -> set[_Requirement]:
    """Restrict a requirement set to the current suffix window.

    Args:
        requirements: Flattened requirements from one walk or conversion pass.
        tip: Inclusive suffix end.

    Returns:
        Requirements strictly after C and at or before the supplied tip.
    """
    return {entry for entry in requirements if resume_fixtures._C < entry[3] <= tip}


async def _read_width(
    repo: resume_fixtures._ResumeRepo,
    resume: PnlCheckpointResume | None,
    width: int,
    log: _RequirementLog,
) -> service.PnlWalletSeriesResult:
    """Read one suffix width while recording its conversion set.

    Args:
        repo: Repository fake.
        resume: Verified resume state or ``None`` for anchor replay.
        width: Chunk width in minutes after C.
        log: Requirement log receiving this build's sets.

    Returns:
        The completed wallet series result.
    """
    log.conversions.append(set())
    tip = resume_fixtures._C + timedelta(minutes=width)
    options = replace(
        resume_fixtures._resume_options(resume),
        replay_generation_tip=resume_fixtures._C + timedelta(minutes=width),
    )
    if resume is None:
        repo._execution_watermarks = {"kraken": 4}
    return await service.build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        resume_fixtures._C + timedelta(minutes=width),
        "1m",
        tip + timedelta(minutes=1),
        "USD",
        policy=service.PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )


@pytest.mark.parametrize("width", [1, 7, 60, 1440])
async def test_width_and_requirement_sets_match_anchor_replay(
    monkeypatch: pytest.MonkeyPatch,
    width: int,
) -> None:
    """Match points and mark/event/conversion requirements at every chunk width.

    Given: A capture with deferred fills and a regression shadow crossing C.
    When: Anchor and resumed reads cover the same suffix width.
    Then: Points and reasons are bit-identical and all three requirement sets
        equal the anchor sets restricted to (C, T].

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        width: Chunk width in minutes.
    """
    repo = resume_fixtures._ResumeRepo()
    resume = await resume_fixtures._verified_resume(monkeypatch, repo)
    log = _RequirementLog()
    _install_requirement_spies(monkeypatch, log)
    tip = resume_fixtures._C + timedelta(minutes=width)
    anchor = await _read_width(resume_fixtures._ResumeRepo(), None, width, log)
    resumed = await _read_width(repo, resume, width, log)
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)
    assert log.marks == [_restricted(log.marks[0], tip), _restricted(log.marks[0], tip)]
    assert _restricted(log.events[1], tip) == _restricted(log.events[0], tip)
    assert log.conversions[1] == log.conversions[0]
    repo._assert_no_prefix_read()
    assert repo.resume_accrual_bounds == [(resume_fixtures._C, tip)]


async def test_prefix_accrual_change_after_certification_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Detect a prefix accrual inserted after certification at the resuming as_of.

    Given: A verified checkpoint and an accrual inserted inside its certified prefix.
    When: The verifier recomputes the accrual fingerprint at the resuming horizon.
    Then: It returns ``checkpoint_accrual_prefix_changed`` and anchor points stay unchanged.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = resume_fixtures._ResumeRepo()
    resume = await resume_fixtures._verified_resume(monkeypatch, repo)
    repo._accruals.append(fixtures._accrual_row(fixtures._I1, 2, 3.0, "EUR"))
    repo._fx_rows.append(fixtures._fx_row("EUR", "USD", 2, 2.0))
    monkeypatch.setattr(
        repo,
        "get_pnl_accrual_prefix_fingerprint",
        AsyncMock(return_value=IdAggregate(1, 1, 1, fixtures._m(2))),
    )
    request = replace(capture._request(), raw_execution_watermarks={"kraken": 3})
    repo._execution_watermarks = {"kraken": 3}
    outcome = await verify_checkpoint_for_resume(
        repo,
        resume.record,
        verifier._scope(request),
        fixtures._m(10),
        replace(
            verifier._eligibility(request),
            from_time=fixtures._m(5),
            frontier_tip=resume_fixtures._T,
            replay_generation_tip=resume_fixtures._T,
        ),
    )
    assert isinstance(outcome, PnlCheckpointFallback)
    assert outcome.reason == "checkpoint_accrual_prefix_changed"
    assert outcome.delete_required
    after = await resume_fixtures._read_window(repo, None)
    reference_repo = resume_fixtures._ResumeRepo()
    reference_repo._accruals.append(fixtures._accrual_row(fixtures._I1, 2, 3.0, "EUR"))
    reference_repo._fx_rows.append(fixtures._fx_row("EUR", "USD", 2, 2.0))
    reference = await resume_fixtures._read_window(reference_repo, None)
    checkpoint_fixtures._assert_points_bit_identical(reference.points, after.points)


def _is_broad_assertion_handler(handler: ast.ExceptHandler) -> bool:
    """Return whether an exception handler catches AssertionError or Exception.

    Args:
        handler: AST exception handler to inspect.

    Returns:
        ``True`` for either forbidden broad handler type.
    """
    if isinstance(handler.type, ast.Name):
        return handler.type.id in {"AssertionError", "Exception"}
    if isinstance(handler.type, ast.Tuple):
        return any(
            isinstance(item, ast.Name) and item.id in {"AssertionError", "Exception"}
            for item in handler.type.elts
        )
    return False


def test_new_checkpoint_tests_have_no_assert_in_broad_try() -> None:
    """Enforce python:S5779 across the new checkpoint test files.

    Given: The resume, capture, verifier, and width test sources.
    When: AST inspection finds a try containing assert under a broad handler.
    Then: No violation is present.
    """
    directory = Path(__file__).parent
    names = (
        "test_pnl_opening_checkpoint_resume.py",
        "test_pnl_opening_checkpoint_capture.py",
        "test_pnl_opening_checkpoint_verifier.py",
        "test_pnl_opening_checkpoint_width.py",
    )
    violations: list[str] = []
    for name in names:
        tree = ast.parse((directory / name).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Try) and any(
                _is_broad_assertion_handler(handler) for handler in node.handlers
            ):
                if any(isinstance(child, ast.Assert) for child in ast.walk(node)):
                    violations.append(f"{name}:{node.lineno}")
    assert violations == []


class _BoundedCandleResumeRepo(resume_fixtures._ResumeRepo):
    """Resume fake whose candle read honors the repository open_at range."""

    async def get_pnl_timeline_candles(
        self,
        refs: Sequence[InstrumentSymbolRefRow],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[PnlTimelineCandleRow]:
        """Return only candles whose open_at lies inside ``[start, end]``.

        Args:
            refs: Trusted symbol references to load candles for.
            start: Inclusive lower candle-open bound.
            end: Inclusive upper candle-open bound.
            as_of: Knowledge horizon.

        Returns:
            Range-bounded candle rows matching the real repository contract.
        """
        rows = await super().get_pnl_timeline_candles(refs, start, end, as_of)
        return [row for row in rows if start <= row["open_at"] <= end]


def _prefix_mark_edge_repo(prefix_event: str) -> _BoundedCandleResumeRepo:
    """Build a non-USD instrument with an event after its last pre-C candle.

    Args:
        prefix_event: One of ``"open"``, ``"close"``, ``"invalid_after_candle"``,
            or ``"invalid_no_candles"``.

    Returns:
        Configured range-bounded resume fake for capture and replay comparison.
    """
    repo = _BoundedCandleResumeRepo()
    if prefix_event == "open":
        executions = [
            fixtures._exec_row(fixtures._I1, 1, 3, "buy", 1.0, 100.0, 0.0, "USD"),
        ]
        prefix_candles = [fixtures._candle(fixtures._m(1), 100.0, fixtures._I1)]
    elif prefix_event == "close":
        executions = [
            fixtures._exec_row(fixtures._I1, 1, 1, "buy", 1.0, 100.0, 0.0, "USD"),
            fixtures._exec_row(fixtures._I1, 2, 3, "sell", 1.0, 102.0, 0.0, "USD"),
        ]
        prefix_candles = [fixtures._candle(fixtures._m(1), 100.0, fixtures._I1)]
    elif prefix_event == "invalid_after_candle":
        executions = [
            fixtures._exec_row(fixtures._I1, 1, 1, "buy", 1.0, 100.0, 0.0, "USD"),
            fixtures._exec_row(fixtures._I1, 2, 3, "buy", 1.0, 0.0, 0.0, "USD"),
        ]
        prefix_candles = [fixtures._candle(fixtures._m(1), 100.0, fixtures._I1)]
    else:
        executions = [
            fixtures._exec_row(fixtures._I1, 1, 3, "buy", 1.0, 0.0, 0.0, "USD"),
        ]
        prefix_candles = []
    suffix_candles = [
        fixtures._candle(fixtures._m(minute), 105.0 + minute, fixtures._I1)
        for minute in range(4, 8)
    ]
    repo._executions = executions
    repo._accruals = []
    repo._refs = [fixtures._ref(fixtures._I1, "BTC-EUR", "EUR")]
    repo._candles = [*prefix_candles, *suffix_candles]
    repo._lineage = [fixtures._lineage_row(row["order_public_id"], "rest") for row in executions]
    repo._fx_rows = [fixtures._fx_row("EUR", "USD", minute, 1.5) for minute in range(1, 10)]
    repo._execution_watermarks = {"kraken": len(executions)}
    return repo


@pytest.mark.parametrize(
    "prefix_event",
    ["open", "close", "invalid_after_candle", "invalid_no_candles"],
)
async def test_resume_mark_walk_matches_anchor_after_last_prefix_candle(
    monkeypatch: pytest.MonkeyPatch,
    prefix_event: str,
) -> None:
    """Advance mark walk state to C across all instruments even without candles.

    Given: A non-USD instrument with an open, close, or invalid fill after its
        last candle before C (or with zero candles before C).
    When: The checkpoint is captured at C and resumed through T alongside an
        anchor replay of the same window.
    Then: Resumed points and mark FX requirement sets match anchor replay.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        prefix_event: Prefix mark-walk edge constellation under test.
    """
    repo = _prefix_mark_edge_repo(prefix_event)
    watermark = len(repo._executions)
    capture._permit_capture(monkeypatch)
    request = replace(capture._request(), raw_execution_watermarks={"kraken": watermark})
    capture_options = replace(capture._options(request), replay_generation_tip=resume_fixtures._C)
    captured = await service.build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(0),
        resume_fixtures._C,
        "1m",
        capture._AS_OF,
        "USD",
        policy=service.PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=capture_options,
    )
    assert captured.checkpoint_write is not None
    write = captured.checkpoint_write
    record = PnlOpeningCheckpointRecord(write.certificate, write.state_json, fixtures._m(10))
    outcome = await verify_checkpoint_for_resume(
        repo,
        record,
        verifier._scope(request),
        fixtures._m(10),
        replace(
            verifier._eligibility(request),
            from_time=fixtures._m(5),
            frontier_tip=resume_fixtures._T,
            replay_generation_tip=resume_fixtures._T,
        ),
    )
    assert isinstance(outcome, PnlCheckpointResume)
    log = _RequirementLog()
    _install_requirement_spies(monkeypatch, log)
    options = replace(
        resume_fixtures._resume_options(outcome),
        frozen_execution_watermarks={"kraken": watermark},
    )
    anchor_repo = _prefix_mark_edge_repo(prefix_event)
    anchor = await service.build_wallet_pnl_series(
        anchor_repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        resume_fixtures._T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=service.PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=replace(options, checkpoint_resume=None),
    )
    resumed = await service.build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        resume_fixtures._T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=service.PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    checkpoint_fixtures._assert_points_bit_identical(anchor.points, resumed.points)
    assert log.marks[1] == _restricted(log.marks[0], resume_fixtures._T)


async def test_resume_mark_requirements_seeds_last_effective_from_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seed _effective_mark_events from engine_state.last_effective_by_pool.

    Given: A captured checkpoint with an untrusted instrument fill before C whose
        pool cursor exists in engine_state.last_effective_by_pool but not in
        event_walk_state.last_effective.
    When: _resume_mark_requirements computes suffix mark FX requirements.
    Then: _effective_mark_events receives the checkpoint's last_effective_by_pool.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    repo = _BoundedCandleResumeRepo()
    repo._executions = [
        fixtures._exec_row(fixtures._I1, 1, 1, "buy", 1.0, 100.0, 0.0, "USD"),
        fixtures._exec_row(fixtures._I2, 2, 2, "buy", 1.0, 50.0, 0.0, "USD"),
    ]
    repo._accruals = []
    repo._refs = [
        fixtures._ref(fixtures._I1, "BTC-EUR", "EUR"),
        fixtures._ref(fixtures._I2, "UNTRUSTED", None),
    ]
    repo._candles = [
        fixtures._candle(fixtures._m(minute), 101.0, fixtures._I1) for minute in range(9)
    ]
    repo._lineage = [
        fixtures._lineage_row(row["order_public_id"], "rest") for row in repo._executions
    ]
    repo._fx_rows = [fixtures._fx_row("EUR", "USD", minute, 1.5) for minute in range(1, 10)]
    repo._execution_watermarks = {"kraken": 2}
    resume = await resume_fixtures._capture_and_verify_custom(monkeypatch, repo, {"kraken": 2})
    seen_initial: list[dict[tuple[str, str], datetime]] = []
    original = service._effective_mark_events

    def spy_effective(
        execution_rows: Sequence[PnlTimelineOpeningExecutionRow],
        base_by_instrument: dict[str, str],
        quote_by_instrument: dict[str, str],
        initial_last_effective: dict[tuple[str, str], datetime] | None = None,
    ) -> tuple[
        dict[str, list[service._EffectiveMarkEvent]],
        list[tuple[datetime, datetime]],
    ]:
        """Record the seeded initial_last_effective map before delegating.

        Args:
            execution_rows: Suffix execution rows.
            base_by_instrument: Proven base assets.
            quote_by_instrument: Proven quote assets.
            initial_last_effective: Seeded per-pool cursors.

        Returns:
            Unchanged mark events and regression shadows.
        """
        seen_initial.append(dict(initial_last_effective or {}))
        return original(
            execution_rows,
            base_by_instrument,
            quote_by_instrument,
            initial_last_effective,
        )

    monkeypatch.setattr(service, "_effective_mark_events", spy_effective)
    options = replace(
        resume_fixtures._resume_options(resume),
        frozen_execution_watermarks={"kraken": 2},
    )
    await service.build_wallet_pnl_series(
        repo,
        fixtures._W1,
        "live",
        fixtures._m(5),
        resume_fixtures._T,
        "1m",
        fixtures._m(10),
        "USD",
        policy=service.PnlSeriesReadPolicy(allow_anchor_creation=False),
        options=options,
    )
    expected_cursor = dict(service.resume_engine_opening(resume).last_effective_by_pool)
    assert (fixtures._I2, repo._executions[1]["shard_key"]) in expected_cursor
    assert seen_initial == [expected_cursor]
