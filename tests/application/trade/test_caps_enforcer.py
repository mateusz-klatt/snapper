"""Unit tests for :class:`TradingCapsEnforcer`.

Covers:
    - ``guard()`` fails closed when ``submission.user_public_id``
      is None (the fail-closed principal rule — prevents silent cap
      bypass from a REST handler that forgets to thread the user).
    - ``guard_service_principal()`` bypasses all caps AND releases
      no lock (strategy hot path).
    - All four caps: max_order_quantity_per_instrument (both
      scalar and per-instrument JSON form), max_open_orders,
      max_daily_notional_usd (with and without prior rows; market
      orders skipped with WARN), max_cancels_per_minute.
    - ``caps == None`` admits every submission unchanged.
    - Per-user asyncio.Lock prevents TOCTOU: two concurrent
      submissions for the same user serialize across the
      check+yield boundary.
    - Malformed JSON cap value is logged + treated as unbounded
      (fails open with warning rather than crashing the
      submission).
    - PriceUnavailableError from converter maps to
      CapsViolationError(cap_type='price_unavailable').
"""

import asyncio
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.pricing.usd_converter import PriceUnavailableError
from snapper.application.pricing.usd_converter import USDConverter
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import Guard
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentSourceResolution
from snapper.data.repository_types import UserRecentSubmitRow
from snapper.data.repository_types import UserTradingCapsRow
from snapper.messaging.infrastructure.publisher import SequenceTracker

_NOW = datetime(2026, 4, 18, 12, 0, 0, tzinfo=UTC)


def _submission(
    *,
    user: str | None = "user-1",
    command_type: str = "submit",
    quantity: Decimal | None = Decimal("1"),
    instrument_public_id: str | None = "inst-btc",
    price: Decimal | None = None,
) -> TradeCommandSubmission:
    """Construct a minimal :class:`TradeCommandSubmission` for tests."""
    return TradeCommandSubmission(
        user_public_id=user,
        operator_public_id="op-1",
        wallet_public_id="wallet-1",
        instrument_public_id=instrument_public_id,
        command_type=command_type,
        side="buy",
        order_type="market",
        quantity=quantity,
        price=price,
        source_surface="mcp",
        idempotency_key=None,
    )


def _caps(**overrides: object) -> UserTradingCapsRow:
    """Construct a fully-populated caps row, overriding selected fields."""
    base: UserTradingCapsRow = {
        "public_id": "caps-1",
        "user_public_id": "user-1",
        "max_order_quantity_per_instrument": None,
        "max_open_orders": None,
        "max_daily_notional_usd": None,
        "max_cancels_per_minute": None,
    }
    for k, v in overrides.items():
        cast(dict[str, object], base)[k] = v
    return base


async def _echo_pid(instrument_public_id: str) -> InstrumentSourceResolution:
    """Echo the input pid (identity source-resolver stub).

    Args:
        instrument_public_id: Emission-side instrument identity.

    Returns:
        A non-paper identity resolution echoing the input.
    """
    return {
        "valuation_public_id": instrument_public_id,
        "is_paper": False,
        "mapped": False,
    }


def _stub_repo(
    caps: UserTradingCapsRow | None = None,
    *,
    open_commands: int = 0,
    recent_submits: list[UserRecentSubmitRow] | None = None,
    rolling_cancels: int = 0,
) -> Repository:
    """Build a MagicMock Repository honoring the caps-enforcer contract."""
    repo = MagicMock()
    repo.get_user_trading_caps = AsyncMock(return_value=caps)
    repo.count_user_open_commands = AsyncMock(return_value=open_commands)
    repo.get_user_recent_submits = AsyncMock(return_value=recent_submits or [])
    repo.count_user_rolling_cancels = AsyncMock(return_value=rolling_cancels)

    repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
    return cast(Repository, repo)


def _stub_pricing(usd_notional: Decimal | None = None) -> USDConverter:
    """Build a MagicMock USDConverter returning ``usd_notional``."""
    pricing = MagicMock(spec=USDConverter)
    if usd_notional is not None:
        pricing.to_usd = AsyncMock(return_value=usd_notional)
    else:
        pricing.to_usd = AsyncMock(return_value=Decimal("1000"))
    return cast(USDConverter, pricing)


@pytest.mark.asyncio
async def test_guard_rejects_none_user_public_id() -> None:
    """``guard()`` raises ``missing_user_public_id`` when user is None.

    Given: a submission with ``user_public_id=None`` (a REST handler
        forgot to thread the principal),
    When: the caller enters ``async with enforcer.guard(s):``,
    Then: :class:`CapsViolationError` with
        ``cap_type='missing_user_public_id'`` is raised BEFORE any
        DB access — prevents silent cap bypass.
    """
    enforcer = TradingCapsEnforcer(_stub_repo(), _stub_pricing(), now=lambda: _NOW)
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(_submission(user=None)):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "missing_user_public_id"


@pytest.mark.asyncio
async def test_guard_service_principal_bypasses_caps() -> None:
    """``guard_service_principal()`` skips cap evaluation entirely.

    Given: a submission with ``user_public_id=None`` (strategy
        hot path) and a caps row that would normally reject,
    When: the caller enters
        ``async with enforcer.guard_service_principal(s):``,
    Then: the enforcer yields :class:`Guard` without consulting
        any cap; the repo is never queried.
    """
    repo = _stub_repo(_caps(max_open_orders=0))
    enforcer = TradingCapsEnforcer(repo, _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard_service_principal(_submission(user=None)) as guard:
        assert isinstance(guard, Guard)
        assert guard.assigned_public_id
    cast(MagicMock, repo).get_user_trading_caps.assert_not_awaited()


@pytest.mark.asyncio
async def test_guard_admits_when_caps_row_is_missing() -> None:
    """A user with no caps row is treated as unbounded on every axis.

    Given: ``get_user_trading_caps`` returns None,
    When: ``guard()`` evaluates a 10-unit submit,
    Then: the enforcer yields :class:`Guard` unchanged — missing
        caps row = unbounded policy.
    """
    enforcer = TradingCapsEnforcer(_stub_repo(None), _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(_submission(quantity=Decimal("10"))) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_scalar_quantity_cap_rejects_over_limit() -> None:
    """Scalar ``max_order_quantity_per_instrument`` rejects above the limit.

    Given: a scalar cap of ``2`` and a submit for ``quantity=3``,
    When: ``guard()`` runs,
    Then: :class:`CapsViolationError` with
        ``cap_type='max_order_quantity_per_instrument'`` is raised.
    """
    caps = _caps(max_order_quantity_per_instrument="2")
    enforcer = TradingCapsEnforcer(_stub_repo(caps), _stub_pricing(), now=lambda: _NOW)
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(_submission(quantity=Decimal("3"))):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_order_quantity_per_instrument"


@pytest.mark.asyncio
async def test_scalar_quantity_cap_admits_at_or_below_limit() -> None:
    """Scalar quantity cap admits a submit exactly at the limit.

    Given: a scalar cap of ``2`` and a submit for ``quantity=2``,
    When: ``guard()`` runs,
    Then: the guard yields — boundary is inclusive.
    """
    caps = _caps(max_order_quantity_per_instrument="2")
    enforcer = TradingCapsEnforcer(_stub_repo(caps), _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(_submission(quantity=Decimal("2"))) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_per_instrument_quantity_cap_rejects_matching_key() -> None:
    """Per-instrument JSON cap rejects only for the keyed instrument.

    Given: JSON cap ``{"inst-btc": "1"}`` and a submit for 2 units
        of ``inst-btc``,
    When: ``guard()`` runs,
    Then: :class:`CapsViolationError` is raised with the quantity
        cap type.
    """
    caps = _caps(max_order_quantity_per_instrument={"inst-btc": "1"})
    enforcer = TradingCapsEnforcer(_stub_repo(caps), _stub_pricing(), now=lambda: _NOW)
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(_submission(quantity=Decimal("2"))):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_order_quantity_per_instrument"


@pytest.mark.asyncio
async def test_per_instrument_quantity_cap_admits_unkeyed_instrument() -> None:
    """Per-instrument JSON cap admits instruments not in the dict.

    Given: JSON cap ``{"inst-btc": "1"}`` and a submit for 100
        units of ``inst-eth`` (not in the dict),
    When: ``guard()`` runs,
    Then: the guard yields — missing per-instrument key is
        treated as unbounded for that instrument.
    """
    caps = _caps(max_order_quantity_per_instrument={"inst-btc": "1"})
    enforcer = TradingCapsEnforcer(_stub_repo(caps), _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(
        _submission(instrument_public_id="inst-eth", quantity=Decimal("100"))
    ) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_malformed_scalar_quantity_cap_fails_open_with_warn() -> None:
    """Malformed scalar cap value is logged + treated as unbounded.

    Given: a scalar cap of ``"not-a-number"``,
    When: ``guard()`` runs,
    Then: the enforcer admits the submission (fails open) rather
        than crashing — operator can fix the misconfig without
        halting trading.
    """
    caps = _caps(max_order_quantity_per_instrument="not-a-number")
    enforcer = TradingCapsEnforcer(_stub_repo(caps), _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(_submission(quantity=Decimal("999"))) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_malformed_per_instrument_cap_fails_open_with_warn() -> None:
    """Malformed JSON per-instrument value fails open with warning.

    Given: JSON cap ``{"inst-btc": "oops"}``,
    When: a submit for ``inst-btc`` runs,
    Then: the guard yields — malformed value treated as unbounded
        for that instrument.
    """
    caps = _caps(max_order_quantity_per_instrument={"inst-btc": "oops"})
    enforcer = TradingCapsEnforcer(_stub_repo(caps), _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(_submission(quantity=Decimal("999"))) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_open_orders_cap_rejects_on_overflow() -> None:
    """``max_open_orders`` rejects when ``current + 1 > limit``.

    Given: ``max_open_orders=3`` and the repo reports 3 open
        commands already,
    When: a new submit runs,
    Then: :class:`CapsViolationError` with
        ``cap_type='max_open_orders'`` is raised.
    """
    caps = _caps(max_open_orders=3)
    enforcer = TradingCapsEnforcer(
        _stub_repo(caps, open_commands=3), _stub_pricing(), now=lambda: _NOW
    )
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(_submission()):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_open_orders"


@pytest.mark.asyncio
async def test_open_orders_cap_admits_when_below_limit() -> None:
    """``max_open_orders`` admits at ``current + 1 == limit``.

    Given: ``max_open_orders=3`` and 2 open commands,
    When: a new submit runs,
    Then: the guard yields — boundary is inclusive at equality.
    """
    caps = _caps(max_open_orders=3)
    enforcer = TradingCapsEnforcer(
        _stub_repo(caps, open_commands=2), _stub_pricing(), now=lambda: _NOW
    )
    async with enforcer.guard(_submission()) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_notional_cap_rejects_when_sum_over_limit() -> None:
    """Rolling 24h notional cap rejects on summed exceedance.

    Given: ``max_daily_notional_usd=10_000``, prior rows summing
        to 8_000, a new submission worth 3_000 USD per converter,
    When: ``guard()`` runs,
    Then: :class:`CapsViolationError` with
        ``cap_type='max_daily_notional_usd'`` is raised.
    """
    caps = _caps(max_daily_notional_usd=10_000.0)
    prior: list[UserRecentSubmitRow] = [
        {
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "quantity": 2.0,
            "price": 4000.0,
            "submitted_notional_usd": None,
        },
    ]
    enforcer = TradingCapsEnforcer(
        _stub_repo(caps, recent_submits=prior),
        _stub_pricing(Decimal("3000")),
        now=lambda: _NOW,
    )
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(_submission()):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_daily_notional_usd"


@pytest.mark.asyncio
async def test_notional_cap_admits_when_sum_within_limit() -> None:
    """Rolling 24h notional cap admits when sum stays within limit.

    Given: ``max_daily_notional_usd=10_000``, prior rows totaling
        5_000, new submission worth 3_000 USD,
    When: ``guard()`` runs,
    Then: the guard yields.
    """
    caps = _caps(max_daily_notional_usd=10_000.0)
    prior: list[UserRecentSubmitRow] = [
        {
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "quantity": 1.0,
            "price": 5000.0,
            "submitted_notional_usd": None,
        },
    ]
    enforcer = TradingCapsEnforcer(
        _stub_repo(caps, recent_submits=prior),
        _stub_pricing(Decimal("3000")),
        now=lambda: _NOW,
    )
    async with enforcer.guard(_submission()) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_notional_cap_skips_market_orders_without_price() -> None:
    """Prior market orders with ``price=None`` are skipped from sum.

    Given: a prior row with ``price=None``,
    When: the notional cap evaluates,
    Then: that row contributes 0 to the rolling sum (documented enforcer emits a WARN log).
    """
    caps = _caps(max_daily_notional_usd=10_000.0)
    prior: list[UserRecentSubmitRow] = [
        {
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "quantity": 999.0,
            "price": None,
            "submitted_notional_usd": None,
        },
    ]
    enforcer = TradingCapsEnforcer(
        _stub_repo(caps, recent_submits=prior),
        _stub_pricing(Decimal("100")),
        now=lambda: _NOW,
    )
    async with enforcer.guard(_submission()) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_notional_cap_skips_when_no_instrument_public_id() -> None:
    """Submission without instrument_public_id cannot be priced — skip.

    Given: ``submission.instrument_public_id is None``,
    When: the notional cap evaluates,
    Then: the enforcer admits (no way to compute USD notional
        without the instrument identity).
    """
    caps = _caps(max_daily_notional_usd=10_000.0)
    enforcer = TradingCapsEnforcer(_stub_repo(caps), _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(_submission(instrument_public_id=None)) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_notional_cap_maps_price_unavailable_to_caps_violation() -> None:
    """PriceUnavailableError from converter maps to CapsViolationError.

    Given: a notional cap active and a converter that raises
        ``PriceUnavailableError`` (e.g., stale snapshot),
    When: ``guard()`` runs,
    Then: :class:`CapsViolationError` with
        ``cap_type='price_unavailable'`` is raised — HTTP layer
        surfaces the ``caps_price_unavailable`` code.
    """
    caps = _caps(max_daily_notional_usd=10_000.0)
    pricing = MagicMock(spec=USDConverter)
    pricing.to_usd = AsyncMock(
        side_effect=PriceUnavailableError("price_stale", "inst-btc", "age=9999s")
    )
    enforcer = TradingCapsEnforcer(_stub_repo(caps), cast(USDConverter, pricing), now=lambda: _NOW)
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(_submission()):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "price_unavailable"


@pytest.mark.asyncio
async def test_cancel_cap_rejects_when_exceeded() -> None:
    """``max_cancels_per_minute`` rejects on sliding-60s exceedance.

    Given: ``max_cancels_per_minute=5`` and 5 prior cancels in
        the last 60 seconds,
    When: a new cancel runs,
    Then: :class:`CapsViolationError` with
        ``cap_type='max_cancels_per_minute'`` is raised.
    """
    caps = _caps(max_cancels_per_minute=5)
    enforcer = TradingCapsEnforcer(
        _stub_repo(caps, rolling_cancels=5), _stub_pricing(), now=lambda: _NOW
    )
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(_submission(command_type="cancel", quantity=None)):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_cancels_per_minute"


@pytest.mark.asyncio
async def test_cancel_cap_admits_when_below_limit() -> None:
    """Cancel submissions within rate budget are admitted.

    Given: ``max_cancels_per_minute=5`` and 2 prior cancels,
    When: a new cancel runs,
    Then: the guard yields.
    """
    caps = _caps(max_cancels_per_minute=5)
    enforcer = TradingCapsEnforcer(
        _stub_repo(caps, rolling_cancels=2), _stub_pricing(), now=lambda: _NOW
    )
    async with enforcer.guard(_submission(command_type="cancel", quantity=None)) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_cancel_path_does_not_consult_quantity_or_notional_caps() -> None:
    """Cancel command_type skips submit-path caps entirely.

    Given: caps with every axis at ``0`` (would reject every
        submit),
    When: a cancel runs,
    Then: only ``count_user_rolling_cancels`` is consulted;
        ``count_user_open_commands`` /
        ``get_user_recent_submits`` / converter are never touched.
    """
    caps = _caps(
        max_order_quantity_per_instrument="0",
        max_open_orders=0,
        max_daily_notional_usd=0.0,
        max_cancels_per_minute=10,
    )
    repo = _stub_repo(caps, rolling_cancels=2)
    pricing = _stub_pricing()
    enforcer = TradingCapsEnforcer(repo, pricing, now=lambda: _NOW)
    async with enforcer.guard(_submission(command_type="cancel", quantity=None)) as _guard_cancel:
        assert _guard_cancel is not None
    cast(MagicMock, repo).count_user_open_commands.assert_not_awaited()
    cast(MagicMock, repo).get_user_recent_submits.assert_not_awaited()
    cast(MagicMock, pricing).to_usd.assert_not_awaited()


@pytest.mark.asyncio
async def test_guard_yields_pre_generated_uuid7() -> None:
    """:class:`Guard` carries a pre-generated UUID7 public_id.

    Given: a non-rejected submission,
    When: ``guard()`` yields,
    Then: ``guard.assigned_public_id`` is a non-empty string and
        stable for the duration of the ``async with`` block.
    """
    enforcer = TradingCapsEnforcer(_stub_repo(), _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(_submission()) as guard:
        first = guard.assigned_public_id
        assert first
        assert guard.assigned_public_id == first


@pytest.mark.asyncio
async def test_cancel_cap_admits_when_limit_is_none() -> None:
    """``max_cancels_per_minute=None`` admits every cancel unchanged.

    Given: caps row with ``max_cancels_per_minute=None``,
    When: a cancel runs,
    Then: the repo cancel-count method is NEVER consulted — the
        cap short-circuits in the ``is None`` branch.
    """
    caps = _caps(max_cancels_per_minute=None)
    repo = _stub_repo(caps)
    enforcer = TradingCapsEnforcer(repo, _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(_submission(command_type="cancel", quantity=None)) as _guard_cancel:
        assert _guard_cancel is not None
    cast(MagicMock, repo).count_user_rolling_cancels.assert_not_awaited()


@pytest.mark.asyncio
async def test_per_user_lock_serializes_concurrent_submissions_for_same_user() -> None:
    """Two concurrent ``guard()`` calls for one user serialize correctly.

    Given: caps ``max_open_orders=1`` and repo that reports
        ``count_user_open_commands`` based on a counter that the
        first guard increments on entry,
    When: two ``guard(...)`` calls race on asyncio.gather(),
    Then: the second acquires the lock only after the first
        releases; the second sees the incremented count and
        rejects. This proves the lock spans check+yield, closing
        the TOCTOU window.
    """
    counter = {"n": 0}

    def fake_count(_user: str) -> int:
        return counter["n"]

    def fake_caps(_user: str) -> UserTradingCapsRow:
        return _caps(max_open_orders=1)

    repo = MagicMock()
    repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
    repo.get_user_trading_caps = AsyncMock(side_effect=fake_caps)
    repo.count_user_open_commands = AsyncMock(side_effect=fake_count)
    repo.get_user_recent_submits = AsyncMock(return_value=[])
    repo.count_user_rolling_cancels = AsyncMock(return_value=0)
    enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)

    async def one_submit() -> bool:
        try:
            async with enforcer.guard(_submission()):
                counter["n"] += 1
                await asyncio.sleep(0)
            return True
        except CapsViolationError:
            return False

    results = await asyncio.gather(one_submit(), one_submit())
    assert sorted(results) == [False, True]


def _ai_review_row(
    *,
    review_public_id: str = "rev-1",
    user_public_id: str = "user-1",
    strategy_public_id: str = "strat-1",
    wallet_public_id: str = "wallet-1",
    instrument_public_id: str = "inst-btc",
    dispatch_version: int = 3,
    status: str = "resolved_approved",
) -> dict[str, object]:
    """Build a minimal AiReviewRow dict for publisher + strategy gate tests.

    The publisher branch reads ``user_public_id`` / ``strategy_public_id``
    / ``wallet_public_id`` / ``instrument_public_id`` /
    ``dispatch_version`` off the row before publishing the bus event;
    The strategy gate also reads ``status`` to enforce the
    supersede-after-await invariant. Other columns are irrelevant
    for those paths so we cast a partial dict through the Repository
    mock's ``get_ai_review`` AsyncMock return.
    """
    return {
        "public_id": review_public_id,
        "user_public_id": user_public_id,
        "strategy_public_id": strategy_public_id,
        "wallet_public_id": wallet_public_id,
        "instrument_public_id": instrument_public_id,
        "dispatch_version": dispatch_version,
        "status": status,
    }


def _ai_review_submission(
    *,
    review_public_id: str | None = "rev-1",
    quantity: Decimal | None = Decimal("9999"),
) -> TradeCommandSubmission:
    """Submission carrying ``ai_review_public_id`` so the enforcer publishes."""
    return TradeCommandSubmission(
        user_public_id="user-1",
        operator_public_id="op-1",
        wallet_public_id="wallet-1",
        instrument_public_id="inst-btc",
        command_type="submit",
        side="buy",
        order_type="market",
        quantity=quantity,
        price=None,
        source_surface="strategy",
        idempotency_key=None,
        ai_review_public_id=review_public_id,
    )


def _publisher_with_tracker() -> MagicMock:
    """Build a MagicMock publisher with a real :class:`SequenceTracker`."""
    publisher = MagicMock()
    publisher.send = AsyncMock()
    publisher.tracker = SequenceTracker()
    return publisher


class TestCapsViolationAfterAiApprovePublish:
    """bus.caps_violation_after_ai_approve publisher branch.

    Verifies the four-way decision tree on the
    :meth:`TradingCapsEnforcer.guard` exception path:

    1. AI-approved submission + cap exceeded + publisher wired ->
       publish + raise.
    2. Non-AI submission (ai_review_public_id is None) + cap exceeded ->
       raise WITHOUT publish.
    3. AI-approved submission + cap exceeded + publisher missing ->
       raise WITHOUT publish + log warning (graceful degradation).
    4. Pricing-oracle / missing-user violations skip the publish branch
       even when ai_review_public_id is set (neither maps to a
       delegate-actionable rejection).
    """

    @pytest.mark.asyncio
    async def test_publishes_when_ai_review_id_set_and_cap_exceeded(self) -> None:
        """Cap violation on AI-approved submission -> publish bus event + raise."""
        caps = _caps(max_order_quantity_per_instrument=Decimal("10"))
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_user_trading_caps = AsyncMock(return_value=caps)
        repo.count_user_open_commands = AsyncMock(return_value=0)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        repo.get_ai_review = AsyncMock(
            return_value=_ai_review_row(review_public_id="rev-1", dispatch_version=4)
        )
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        publisher = _publisher_with_tracker()
        enforcer.set_msg_publisher(publisher)
        with pytest.raises(CapsViolationError) as exc_info:
            async with enforcer.guard(
                _ai_review_submission(review_public_id="rev-1", quantity=Decimal("100"))
            ):
                pass
        assert exc_info.value.cap_type == "max_order_quantity_per_instrument"
        publisher.send.assert_awaited_once()
        topic, payload = publisher.send.await_args.args
        assert topic == "bus.caps_violation_after_ai_approve"
        assert payload.review_public_id == "rev-1"
        assert payload.cap_type == "max_order_quantity_per_instrument"
        assert payload.dispatch_version == 4
        assert payload.attempted == pytest.approx(100.0)
        assert payload.limit == pytest.approx(10.0)
        repo.get_ai_review.assert_awaited_once_with("rev-1")

    @pytest.mark.asyncio
    async def test_does_not_publish_when_ai_review_id_is_none(self) -> None:
        """Non-AI submission cap violation -> raise WITHOUT publish."""
        caps = _caps(max_order_quantity_per_instrument=Decimal("10"))
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_user_trading_caps = AsyncMock(return_value=caps)
        repo.count_user_open_commands = AsyncMock(return_value=0)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        repo.get_ai_review = AsyncMock()
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        publisher = _publisher_with_tracker()
        enforcer.set_msg_publisher(publisher)
        with pytest.raises(CapsViolationError):
            async with enforcer.guard(_submission(quantity=Decimal("100"))):
                pass
        publisher.send.assert_not_awaited()
        repo.get_ai_review.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_publisher_missing_logs_warning_and_still_raises(self) -> None:
        """Missing publisher -> raise CapsViolationError (graceful degradation)."""
        caps = _caps(max_order_quantity_per_instrument=Decimal("10"))
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_user_trading_caps = AsyncMock(return_value=caps)
        repo.count_user_open_commands = AsyncMock(return_value=0)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        repo.get_ai_review = AsyncMock()
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        with pytest.raises(CapsViolationError):
            async with enforcer.guard(
                _ai_review_submission(review_public_id="rev-1", quantity=Decimal("100"))
            ):
                pass
        repo.get_ai_review.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_send_failure_swallowed_and_violation_still_raises(self) -> None:
        """Publisher.send raising -> log + still propagate CapsViolationError.

        A broken broker connection must not convert a real cap rejection
        into a transport-error mask. The trade MUST still be rejected.
        """
        caps = _caps(max_order_quantity_per_instrument=Decimal("10"))
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_user_trading_caps = AsyncMock(return_value=caps)
        repo.count_user_open_commands = AsyncMock(return_value=0)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        repo.get_ai_review = AsyncMock(return_value=_ai_review_row())
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        publisher = _publisher_with_tracker()
        publisher.send = AsyncMock(side_effect=RuntimeError("broker down"))
        enforcer.set_msg_publisher(publisher)
        with pytest.raises(CapsViolationError):
            async with enforcer.guard(
                _ai_review_submission(review_public_id="rev-1", quantity=Decimal("100"))
            ):
                pass

    @pytest.mark.asyncio
    async def test_review_row_missing_logs_warning_and_skips_publish(self) -> None:
        """Stale ai_review_public_id (row gone) -> warn + skip publish + still raise."""
        caps = _caps(max_order_quantity_per_instrument=Decimal("10"))
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_user_trading_caps = AsyncMock(return_value=caps)
        repo.count_user_open_commands = AsyncMock(return_value=0)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        repo.get_ai_review = AsyncMock(return_value=None)
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        publisher = _publisher_with_tracker()
        enforcer.set_msg_publisher(publisher)
        with pytest.raises(CapsViolationError):
            async with enforcer.guard(
                _ai_review_submission(review_public_id="missing-rev", quantity=Decimal("100"))
            ):
                pass
        publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_price_unavailable_skips_publish_branch(self) -> None:
        """price_unavailable cap_type does NOT publish — not delegate-actionable."""
        caps = _caps(max_daily_notional_usd=Decimal("1000"))
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_user_trading_caps = AsyncMock(return_value=caps)
        repo.count_user_open_commands = AsyncMock(return_value=0)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        repo.get_ai_review = AsyncMock()
        pricing = MagicMock(spec=USDConverter)
        pricing.to_usd = AsyncMock(
            side_effect=PriceUnavailableError(
                reason="oracle_down", instrument_public_id="inst-btc", detail="x"
            )
        )
        enforcer = TradingCapsEnforcer(
            cast(Repository, repo), cast(USDConverter, pricing), now=lambda: _NOW
        )
        publisher = _publisher_with_tracker()
        enforcer.set_msg_publisher(publisher)
        with pytest.raises(CapsViolationError) as exc_info:
            async with enforcer.guard(
                _ai_review_submission(review_public_id="rev-1", quantity=Decimal("1"))
            ):
                pass
        assert exc_info.value.cap_type == "price_unavailable"
        publisher.send.assert_not_awaited()
        repo.get_ai_review.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_set_msg_publisher_clears_with_none(self) -> None:
        """``set_msg_publisher(None)`` resets the slot."""
        repo = _stub_repo()
        enforcer = TradingCapsEnforcer(repo, _stub_pricing(), now=lambda: _NOW)
        publisher = _publisher_with_tracker()
        enforcer.set_msg_publisher(publisher)
        assert enforcer._msg_publisher is publisher
        enforcer.set_msg_publisher(None)
        assert enforcer._msg_publisher is None

    @pytest.mark.asyncio
    async def test_get_ai_review_raising_does_not_mask_caps_violation(self) -> None:
        """Race-safety: a DB error in get_ai_review must NOT replace the cap rejection.

        An earlier revision wrapped only ``send()`` in try/except. Any
        pre-send failure (DB hiccup on get_ai_review, payload validation
        error, tracker race during shutdown) would replace the
        :class:`CapsViolationError` with the unrelated transport
        exception — the trade would still be rejected by the caller's
        flow but the error surface would be wrong. Pin the contract
        that the cap rejection is the primary user-visible error
        regardless of how the auxiliary fanout fails.

        Given a publisher wired + repo.get_ai_review raises
            DBConnectionError,
        When guard() runs against an AI-approved submission that
            trips a cap,
        Then CapsViolationError still propagates to the caller (NOT a
            DBConnectionError).
        """
        caps = _caps(max_order_quantity_per_instrument=Decimal("10"))
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_user_trading_caps = AsyncMock(return_value=caps)
        repo.count_user_open_commands = AsyncMock(return_value=0)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        repo.get_ai_review = AsyncMock(side_effect=RuntimeError("db hiccup"))
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        publisher = _publisher_with_tracker()
        enforcer.set_msg_publisher(publisher)
        with pytest.raises(CapsViolationError) as exc_info:
            async with enforcer.guard(
                _ai_review_submission(review_public_id="rev-1", quantity=Decimal("100"))
            ):
                pass
        assert exc_info.value.cap_type == "max_order_quantity_per_instrument"
        publisher.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_skips_publish_when_attempted_or_limit_is_none(self) -> None:
        """Defensive guard: hand-crafted CapsViolationError without numeric bounds.

        Today every real cap_type populates attempted+limit, but the
        guard exists so a future cap_type that lacks numeric bounds
        (e.g. a binary policy switch) cannot crash the publisher path
        on the NotNone Pydantic field constraint. We exercise the
        branch by calling the helper directly with a synthetic exception.
        """
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_ai_review = AsyncMock(return_value=_ai_review_row())
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        publisher = _publisher_with_tracker()
        enforcer.set_msg_publisher(publisher)
        synthetic_exc = CapsViolationError("future_binary_policy", attempted=None, limit=None)
        await enforcer._publish_caps_violation_after_ai_approve(
            _ai_review_submission(review_public_id="rev-1"), synthetic_exc
        )
        publisher.send.assert_not_awaited()
        repo.get_ai_review.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_publishes_on_open_orders_cap_exceeded(self) -> None:
        """The publish branch fires for ``max_open_orders`` too (every cap branch)."""
        caps = _caps(max_open_orders=2)
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_user_trading_caps = AsyncMock(return_value=caps)
        repo.count_user_open_commands = AsyncMock(return_value=2)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        repo.get_ai_review = AsyncMock(return_value=_ai_review_row())
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        publisher = _publisher_with_tracker()
        enforcer.set_msg_publisher(publisher)
        with pytest.raises(CapsViolationError):
            async with enforcer.guard(
                _ai_review_submission(review_public_id="rev-1", quantity=Decimal("1"))
            ):
                pass
        publisher.send.assert_awaited_once()
        _, payload = publisher.send.await_args.args
        assert payload.cap_type == "max_open_orders"


class TestGuardWithAiReviewAttribution:
    """Strategy hot-path AI-attribution gate.

    Verifies the new ``guard_with_ai_review_attribution`` context
    manager closes the loop:

    1. Resolves user_public_id from the cited row + delegates to
       ``guard()`` so caps actually evaluate.
    2. Cap rejection routes through
       ``_publish_caps_violation_after_ai_approve`` with the row's
       dispatch_version, NOT the carried tuple's value.
    3. Empty submission wallet fails closed BEFORE the row fetch
       (operator-actionable error).
    4. ``ai_review_dispatch_version`` carried on the submission is
       transport-only — a mismatch vs the row does NOT raise.
    """

    @pytest.mark.asyncio
    async def test_happy_path_resolves_user_from_cited_row(self) -> None:
        """The attribution gate stamps user_public_id from the row.

        Given a strategy submission with ``user_public_id=None`` and a
            cited row whose user is "user-1",
        When the gate yields,
        Then the wrapped ``guard()`` sees the rebuilt submission with
            ``user_public_id="user-1"`` (proven by the recorded caps
            evaluation falling through cleanly with no caps row).
        """
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_ai_review = AsyncMock(
            return_value=_ai_review_row(review_public_id="rev-strat", dispatch_version=2)
        )
        repo.get_user_trading_caps = AsyncMock(return_value=None)
        repo.count_user_open_commands = AsyncMock(return_value=0)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        submission = _submission(user=None)
        async with enforcer.guard_with_ai_review_attribution(
            submission,
            ai_review_public_id="rev-strat",
            ai_review_dispatch_version=2,
        ) as guard:
            assert guard.submission.user_public_id == "user-1"
            assert guard.submission.ai_review_public_id == "rev-strat"
            assert guard.submission.ai_review_dispatch_version == 2

    @pytest.mark.asyncio
    async def test_empty_wallet_fails_closed_before_row_fetch(self) -> None:
        """Submission with empty wallet raises BEFORE repo.get_ai_review.

        Wallet-preflight rationale: a strategy operator who left
        ``StrategyConfig.wallet_public_id`` at the
        default ``""`` sees a loud, operator-actionable failure on
        first AI-attributed emit, NOT a confusing wallet-mismatch
        deeper in the validator.
        """
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_ai_review = AsyncMock()
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        empty_wallet = TradeCommandSubmission(
            user_public_id=None,
            operator_public_id="op-1",
            wallet_public_id="",
            instrument_public_id="inst-btc",
            command_type="submit",
            side="buy",
            order_type="market",
            quantity=Decimal("1"),
            price=None,
            source_surface="strategy",
            idempotency_key=None,
        )
        with pytest.raises(CapsViolationError, match="missing_wallet_for_ai_review_attribution"):
            async with enforcer.guard_with_ai_review_attribution(
                empty_wallet,
                ai_review_public_id="rev-strat",
                ai_review_dispatch_version=1,
            ):
                pass
        repo.get_ai_review.assert_not_called()

    @pytest.mark.asyncio
    async def test_cap_rejection_publishes_with_row_dispatch_version(self) -> None:
        """Cap rejection on AI-attributed strategy emit publishes bus event.

        Transport-only contract: even if the carried
        ``ai_review_dispatch_version`` differs from the row's, the
        published event uses the row-of-record value.
        """
        caps = _caps(max_order_quantity_per_instrument=Decimal("10"))
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_ai_review = AsyncMock(
            return_value=_ai_review_row(review_public_id="rev-strat", dispatch_version=5)
        )
        repo.get_user_trading_caps = AsyncMock(return_value=caps)
        repo.count_user_open_commands = AsyncMock(return_value=0)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        publisher = _publisher_with_tracker()
        enforcer.set_msg_publisher(publisher)
        big_submission = _submission(user=None, quantity=Decimal("99"))
        with pytest.raises(CapsViolationError):
            async with enforcer.guard_with_ai_review_attribution(
                big_submission,
                ai_review_public_id="rev-strat",
                ai_review_dispatch_version=999,
            ):
                pass
        publisher.send.assert_awaited_once()
        _, payload = publisher.send.await_args.args
        assert payload.review_public_id == "rev-strat"
        assert payload.dispatch_version == 5

    @pytest.mark.asyncio
    async def test_ignores_dispatch_version_mismatch_on_happy_path(self) -> None:
        """Strategy validator does NOT compare versions.

        The carried ``ai_review_dispatch_version`` differs from the
        cited row's; the gate yields successfully (no
        ``AiReviewCitationError`` raised).
        """
        repo = MagicMock()
        repo.resolve_source_instrument_public_id = AsyncMock(side_effect=_echo_pid)
        repo.get_ai_review = AsyncMock(
            return_value=_ai_review_row(review_public_id="rev-strat", dispatch_version=1)
        )
        repo.get_user_trading_caps = AsyncMock(return_value=None)
        repo.count_user_open_commands = AsyncMock(return_value=0)
        repo.get_user_recent_submits = AsyncMock(return_value=[])
        repo.count_user_rolling_cancels = AsyncMock(return_value=0)
        enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)
        submission = _submission(user=None)
        async with enforcer.guard_with_ai_review_attribution(
            submission,
            ai_review_public_id="rev-strat",
            ai_review_dispatch_version=42,
        ) as guard:
            assert guard.submission.ai_review_dispatch_version == 42


@pytest.mark.asyncio
async def test_guard_quotes_notional_without_caps_row() -> None:
    """Admission notional is quoted even when the user has no caps row.

    Given: no caps row for the user and a converter valuing the
        submission at 1000 USD,
    When: ``guard()`` yields,
    Then: ``Guard.submitted_notional_usd == 1000.0`` — the snapshot
        column populates regardless of cap configuration.
    """
    enforcer = TradingCapsEnforcer(_stub_repo(None), _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(_submission()) as guard:
        assert guard.submitted_notional_usd == 1000.0


@pytest.mark.asyncio
async def test_guard_quote_quantizes_up_to_cents() -> None:
    """The admission quote rounds UP to whole cents.

    Given: a converter returning ``3000.001`` USD,
    When: ``guard()`` yields,
    Then: the quote persists as ``3000.01`` so NUMERIC(18,2)
        accounting never under-counts admission notional.
    """
    enforcer = TradingCapsEnforcer(
        _stub_repo(None), _stub_pricing(Decimal("3000.001")), now=lambda: _NOW
    )
    async with enforcer.guard(_submission()) as guard:
        assert guard.submitted_notional_usd == 3000.01


@pytest.mark.asyncio
async def test_guard_quote_fails_open_without_notional_cap() -> None:
    """Oracle failure without a configured cap yields a NULL quote.

    Given: no ``max_daily_notional_usd`` cap and a converter raising
        ``PriceUnavailableError``,
    When: ``guard()`` runs,
    Then: the guard still yields (no new rejection path) and the
        quote is None — recording stays strictly best-effort.
    """
    pricing = MagicMock(spec=USDConverter)
    pricing.to_usd = AsyncMock(side_effect=PriceUnavailableError("snapshot_missing", "inst-btc"))
    enforcer = TradingCapsEnforcer(
        _stub_repo(_caps()), cast(USDConverter, pricing), now=lambda: _NOW
    )
    async with enforcer.guard(_submission()) as guard:
        assert guard.submitted_notional_usd is None


@pytest.mark.asyncio
async def test_guard_cancel_yields_null_notional() -> None:
    """Cancels carry no admission notional.

    Given: a cancel-type submission,
    When: ``guard()`` yields,
    Then: the quote is None and the converter is never invoked.
    """
    pricing = _stub_pricing()
    enforcer = TradingCapsEnforcer(_stub_repo(None), pricing, now=lambda: _NOW)
    async with enforcer.guard(_submission(command_type="cancel", quantity=None)) as guard:
        assert guard.submitted_notional_usd is None
    assert cast(MagicMock, pricing.to_usd).await_count == 0


@pytest.mark.asyncio
async def test_service_principal_quotes_notional_best_effort() -> None:
    """The service-principal path snapshots the admission notional.

    Given: a strategy hot-path submission and a working converter,
    When: ``guard_service_principal()`` yields,
    Then: ``Guard.submitted_notional_usd`` carries the quote while no
        cap state is consulted.
    """
    repo = _stub_repo(None)
    enforcer = TradingCapsEnforcer(repo, _stub_pricing(Decimal("42.5")), now=lambda: _NOW)
    async with enforcer.guard_service_principal(_submission(user=None)) as guard:
        assert guard.submitted_notional_usd == 42.5
    assert cast(MagicMock, cast(MagicMock, repo).get_user_trading_caps).await_count == 0


@pytest.mark.asyncio
async def test_service_principal_quote_failure_yields_none() -> None:
    """Service-principal quote failures never reject.

    Given: a converter raising ``PriceUnavailableError`` (the paper
        source-identity gap) on the service-principal path,
    When: ``guard_service_principal()`` runs,
    Then: the guard yields with a None quote — a pricing failure must
        never block a strategy emit that carries no cap.
    """
    pricing = MagicMock(spec=USDConverter)
    pricing.to_usd = AsyncMock(side_effect=PriceUnavailableError("snapshot_missing", "inst-paper"))
    enforcer = TradingCapsEnforcer(_stub_repo(None), cast(USDConverter, pricing), now=lambda: _NOW)
    async with enforcer.guard_service_principal(_submission(user=None)) as guard:
        assert guard.submitted_notional_usd is None


@pytest.mark.asyncio
async def test_notional_sum_prefers_stored_snapshot_over_price_product() -> None:
    """Rolling-sum precedence: stored snapshot beats quantity × price.

    Given: ``max_daily_notional_usd=10_000``, one prior row carrying
        ``submitted_notional_usd=8000`` alongside a misleadingly tiny
        ``quantity × price`` product, and a new 3000 USD submission,
    When: ``guard()`` runs,
    Then: the stored snapshot drives the sum (8000 + 3000 > 10000)
        and the cap rejects.
    """
    caps = _caps(max_daily_notional_usd=10_000.0)
    prior: list[UserRecentSubmitRow] = [
        {
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "quantity": 1.0,
            "price": 1.0,
            "submitted_notional_usd": 8000.0,
        },
    ]
    enforcer = TradingCapsEnforcer(
        _stub_repo(caps, recent_submits=prior),
        _stub_pricing(Decimal("3000")),
        now=lambda: _NOW,
    )
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(_submission()):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_daily_notional_usd"


@pytest.mark.asyncio
async def test_notional_sum_counts_stored_market_order_snapshot() -> None:
    """Market-order rows with a stored snapshot now count in the sum.

    Given: a prior market-order row (``price=None``) carrying
        ``submitted_notional_usd=5000`` and a 3000 USD submission
        against a 10_000 cap,
    When: ``guard()`` runs,
    Then: the guard admits at 8000 — and the same row would have been
        silently skipped before the snapshot column existed.
    """
    caps = _caps(max_daily_notional_usd=10_000.0)
    prior: list[UserRecentSubmitRow] = [
        {
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "quantity": 0.1,
            "price": None,
            "submitted_notional_usd": 5000.0,
        },
    ]
    enforcer = TradingCapsEnforcer(
        _stub_repo(caps, recent_submits=prior),
        _stub_pricing(Decimal("3000")),
        now=lambda: _NOW,
    )
    async with enforcer.guard(_submission()) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_quantity_cap_keys_by_resolved_source_identity() -> None:
    """Per-instrument quantity caps key by the SOURCE identity.

    Given: a submission carrying the PAPER instrument identity whose
        resolver maps it to the source identity, and a per-instrument
        dict cap configured against the SOURCE key,
    When: ``guard()`` runs with an over-limit quantity,
    Then: the cap rejects — operators configure against the source.
    """
    caps = _caps(max_order_quantity_per_instrument={"inst-src": "1"})
    repo = _stub_repo(caps)
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).side_effect = None
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).return_value = {
        "valuation_public_id": "inst-src",
        "is_paper": True,
        "mapped": True,
    }
    enforcer = TradingCapsEnforcer(repo, _stub_pricing(), now=lambda: _NOW)
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(
            _submission(instrument_public_id="inst-paper", quantity=Decimal("2"))
        ):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_order_quantity_per_instrument"


@pytest.mark.asyncio
async def test_quantity_cap_honours_legacy_emission_key_with_warning() -> None:
    """A legacy paper-keyed quantity cap still enforces (with a WARN).

    Given: a per-instrument dict cap configured against the LEGACY
        emission (paper) identity while the resolver maps the
        submission to a different source identity,
    When: ``guard()`` runs with an over-limit quantity,
    Then: the legacy key still rejects — pre-mapping limits are never
        silently dropped.
    """
    caps = _caps(max_order_quantity_per_instrument={"inst-paper": "1"})
    repo = _stub_repo(caps)
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).side_effect = None
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).return_value = {
        "valuation_public_id": "inst-src",
        "is_paper": True,
        "mapped": True,
    }
    enforcer = TradingCapsEnforcer(repo, _stub_pricing(), now=lambda: _NOW)
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(
            _submission(instrument_public_id="inst-paper", quantity=Decimal("2"))
        ):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_order_quantity_per_instrument"


@pytest.mark.asyncio
async def test_notional_quote_prices_resolved_source_identity() -> None:
    """The USD quote is keyed by the resolved SOURCE identity.

    Given: a paper-identity submission whose resolver maps to the
        source instrument,
    When: ``guard()`` quotes the admission notional,
    Then: ``USDConverter.to_usd`` receives the SOURCE identity — the
        snapshot only exists under the source venue.
    """
    repo = _stub_repo(None)
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).side_effect = None
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).return_value = {
        "valuation_public_id": "inst-src",
        "is_paper": True,
        "mapped": True,
    }
    pricing = _stub_pricing(Decimal("500"))
    enforcer = TradingCapsEnforcer(repo, pricing, now=lambda: _NOW)
    async with enforcer.guard(_submission(instrument_public_id="inst-paper")) as guard:
        assert guard.submitted_notional_usd == 500.0
    priced_pid = cast(MagicMock, pricing.to_usd).call_args.args[0]
    assert priced_pid == "inst-src"


@pytest.mark.asyncio
async def test_quantity_cap_fails_closed_for_unmapped_paper_identity() -> None:
    """A dict quantity cap + unresolved paper identity rejects.

    Given: a per-instrument dict cap and a PAPER submission whose
        source identity is UNMAPPED (no matching key can exist under
        the source convention),
    When: ``guard()`` runs,
    Then: the cap fails closed — "key not found" for an unresolved
        identity must never read as unbounded.
    """
    caps = _caps(max_order_quantity_per_instrument={"inst-src": "1"})
    repo = _stub_repo(caps)
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).side_effect = None
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).return_value = {
        "valuation_public_id": "inst-paper",
        "is_paper": True,
        "mapped": False,
    }
    enforcer = TradingCapsEnforcer(repo, _stub_pricing(), now=lambda: _NOW)
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(
            _submission(instrument_public_id="inst-paper", quantity=Decimal("0.5"))
        ):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_order_quantity_per_instrument"
    assert "unresolved" in exc.value.detail


@pytest.mark.asyncio
async def test_resolution_failure_fails_closed_only_with_keyed_caps() -> None:
    """A resolver failure warns + NULLs without caps, rejects with them.

    Given: a repository whose identity resolution raises,
    When: ``guard()`` runs for a user WITHOUT any caps row,
    Then: the guard yields with a NULL notional snapshot (no new
        rejection path) — and the same failure WITH a daily-notional
        cap rejects as ``price_unavailable``.
    """
    repo = _stub_repo(None)
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).side_effect = (
        RuntimeError("db down")
    )
    enforcer = TradingCapsEnforcer(repo, _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(_submission()) as guard:
        assert guard.submitted_notional_usd is None

    capped_repo = _stub_repo(_caps(max_daily_notional_usd=10_000.0))
    cast(
        MagicMock, cast(MagicMock, capped_repo).resolve_source_instrument_public_id
    ).side_effect = RuntimeError("db down")
    capped = TradingCapsEnforcer(capped_repo, _stub_pricing(), now=lambda: _NOW)
    with pytest.raises(CapsViolationError) as exc:
        async with capped.guard(_submission()):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "price_unavailable"


@pytest.mark.asyncio
async def test_quote_survives_unrepresentable_notional() -> None:
    """An absurd notional still trips the cap but stores NULL.

    Given: an oracle valuing the submission beyond NUMERIC(18,2)
        range and a 10k daily-notional cap,
    When: ``guard()`` runs,
    Then: the comparison still applies (cap rejects) — and WITHOUT a
        cap the guard yields with a NULL storage snapshot.
    """
    huge = Decimal("1e20")
    capped = TradingCapsEnforcer(
        _stub_repo(_caps(max_daily_notional_usd=10_000.0)),
        _stub_pricing(huge),
        now=lambda: _NOW,
    )
    with pytest.raises(CapsViolationError) as exc:
        async with capped.guard(_submission()):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_daily_notional_usd"

    uncapped = TradingCapsEnforcer(_stub_repo(None), _stub_pricing(huge), now=lambda: _NOW)
    async with uncapped.guard(_submission()) as guard:
        assert guard.submitted_notional_usd is None


@pytest.mark.asyncio
async def test_quote_rejects_non_finite_notional_with_cap() -> None:
    """A non-finite oracle value fails closed with a cap, warns without.

    Given: an oracle returning ``Decimal('Infinity')``,
    When: ``guard()`` runs with and without a daily-notional cap,
    Then: the capped path rejects as ``price_unavailable`` and the
        cap-less path yields with a NULL snapshot.
    """
    infinite = Decimal("Infinity")
    capped = TradingCapsEnforcer(
        _stub_repo(_caps(max_daily_notional_usd=10_000.0)),
        _stub_pricing(infinite),
        now=lambda: _NOW,
    )
    with pytest.raises(CapsViolationError) as exc:
        async with capped.guard(_submission()):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "price_unavailable"

    uncapped = TradingCapsEnforcer(_stub_repo(None), _stub_pricing(infinite), now=lambda: _NOW)
    async with uncapped.guard(_submission()) as guard:
        assert guard.submitted_notional_usd is None


@pytest.mark.asyncio
async def test_quote_unquantizable_notional_still_trips_cap() -> None:
    """A notional too large to even quantize still trips the cap.

    Given: an oracle value whose cent-quantization exceeds the Decimal
        context precision (``1e30``),
    When: ``guard()`` runs with a 10k daily-notional cap,
    Then: the RAW Decimal drives the comparison and the cap rejects.
    """
    enforcer = TradingCapsEnforcer(
        _stub_repo(_caps(max_daily_notional_usd=10_000.0)),
        _stub_pricing(Decimal("1e30")),
        now=lambda: _NOW,
    )
    with pytest.raises(CapsViolationError) as exc:
        async with enforcer.guard(_submission()):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "max_daily_notional_usd"


@pytest.mark.asyncio
async def test_service_principal_cancel_skips_quote() -> None:
    """A service-principal cancel never resolves or quotes.

    Given: a cancel-type submission on the strategy hot path,
    When: ``guard_service_principal()`` yields,
    Then: the notional snapshot is None and neither the resolver nor
        the oracle is consulted.
    """
    repo = _stub_repo(None)
    pricing = _stub_pricing()
    enforcer = TradingCapsEnforcer(repo, pricing, now=lambda: _NOW)
    async with enforcer.guard_service_principal(
        _submission(user=None, command_type="cancel", quantity=None)
    ) as guard:
        assert guard.submitted_notional_usd is None
    assert (
        cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).await_count == 0
    )
    assert cast(MagicMock, pricing.to_usd).await_count == 0


@pytest.mark.asyncio
async def test_quantity_cap_admits_mapped_identity_without_matching_key() -> None:
    """A MAPPED identity with no matching dict key stays unbounded.

    Given: a mapped paper resolution and a per-instrument dict cap
        keyed by an unrelated instrument (neither source nor legacy
        key matches),
    When: ``guard()`` runs,
    Then: the guard admits — only UNRESOLVED identities fail closed.
    """
    caps = _caps(max_order_quantity_per_instrument={"inst-other": "1"})
    repo = _stub_repo(caps)
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).side_effect = None
    cast(MagicMock, cast(MagicMock, repo).resolve_source_instrument_public_id).return_value = {
        "valuation_public_id": "inst-src",
        "is_paper": True,
        "mapped": True,
    }
    enforcer = TradingCapsEnforcer(repo, _stub_pricing(), now=lambda: _NOW)
    async with enforcer.guard(
        _submission(instrument_public_id="inst-paper", quantity=Decimal("5"))
    ) as guard:
        assert isinstance(guard, Guard)


@pytest.mark.asyncio
async def test_quote_rejects_non_positive_notional_with_cap() -> None:
    """A non-positive oracle value fails closed with a cap, warns without.

    Given: an oracle returning a NEGATIVE notional (a poisoned
        snapshot would otherwise shrink later rolling sums, and
        ``-1e20`` would overflow the storage column),
    When: ``guard()`` runs with and without a daily-notional cap,
    Then: the capped path rejects as ``price_unavailable`` and the
        cap-less path yields with a NULL snapshot.
    """
    negative = Decimal("-1e20")
    capped = TradingCapsEnforcer(
        _stub_repo(_caps(max_daily_notional_usd=10_000.0)),
        _stub_pricing(negative),
        now=lambda: _NOW,
    )
    with pytest.raises(CapsViolationError) as exc:
        async with capped.guard(_submission()):
            raise AssertionError("guard should have rejected before yield")
    assert exc.value.cap_type == "price_unavailable"

    uncapped = TradingCapsEnforcer(_stub_repo(None), _stub_pricing(negative), now=lambda: _NOW)
    async with uncapped.guard(_submission()) as guard:
        assert guard.submitted_notional_usd is None
