"""Unit tests for :class:`TradingCapsEnforcer` — Phase A Day 1c (§3.5.2).

Covers:
    - ``guard()`` fails closed when ``submission.user_public_id``
      is None (plan §3.4 canonical rule — prevents silent cap
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
from snapper.data.repository_types import UserRecentSubmitRow
from snapper.data.repository_types import UserTradingCapsRow

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
        base[k] = v  # type: ignore[literal-required]
    return base


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
        DB access — prevents silent cap bypass (§3.4 canonical
        rule + 4-model consultation Q4 resolution).
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
        assert guard.assigned_public_id  # UUID7 still populated
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
        {"instrument": "BTC-USD", "exchange": "kraken", "quantity": 2.0, "price": 4000.0},
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
        {"instrument": "BTC-USD", "exchange": "kraken", "quantity": 1.0, "price": 5000.0},
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
    Then: that row contributes 0 to the rolling sum (documented
        Phase A limitation; enforcer emits a WARN log).
    """
    caps = _caps(max_daily_notional_usd=10_000.0)
    prior: list[UserRecentSubmitRow] = [
        {
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "quantity": 999.0,
            "price": None,
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
        surfaces the §9.2 ``caps_price_unavailable`` code.
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
        the TOCTOU window (§3.5.2 R2-B2 resolution).
    """
    counter = {"n": 0}

    def fake_count(_user: str) -> int:
        return counter["n"]

    def fake_caps(_user: str) -> UserTradingCapsRow:
        return _caps(max_open_orders=1)

    repo = MagicMock()
    repo.get_user_trading_caps = AsyncMock(side_effect=fake_caps)
    repo.count_user_open_commands = AsyncMock(side_effect=fake_count)
    repo.get_user_recent_submits = AsyncMock(return_value=[])
    repo.count_user_rolling_cancels = AsyncMock(return_value=0)
    enforcer = TradingCapsEnforcer(cast(Repository, repo), _stub_pricing(), now=lambda: _NOW)

    async def one_submit() -> bool:
        try:
            async with enforcer.guard(_submission()):
                counter["n"] += 1
                await asyncio.sleep(0)  # yield inside critical section
            return True
        except CapsViolationError:
            return False

    results = await asyncio.gather(one_submit(), one_submit())
    assert sorted(results) == [False, True]
