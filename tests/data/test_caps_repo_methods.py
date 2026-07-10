"""Repository-method coverage for TradingCapsEnforcer accessors.

Exercises the four concrete methods on
:class:`SQLAlchemyRepository`:
    - :meth:`get_user_trading_caps`
    - :meth:`count_user_open_commands`
    - :meth:`get_user_recent_submits`
    - :meth:`count_user_rolling_cancels`

Uses an in-memory aiosqlite DB + ``create_all()`` so the tests
stay self-contained without the main conftest migration fixtures.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

import pytest

from snapper.core.types import TradeCommandStatusEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import TradeCommand
from snapper.data.models import UserTradingCaps
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 4, 18, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
async def repo() -> SQLAlchemyRepository:
    """Fresh in-memory repo with the schema applied."""
    r = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await r.create_all()
    return r


async def _insert_caps(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    max_open_orders: int | None = None,
    max_daily_notional_usd: Decimal | None = None,
    max_cancels_per_minute: int | None = None,
) -> None:
    """Seed an active :class:`UserTradingCaps` row."""
    async with repo.session() as s:
        caps = UserTradingCaps(
            public_id=f"caps-{user_public_id}",
            timestamp=_NOW,
            known_to=KNOWN_TO_MAX,
            session_id="seed",
            sequence_id=1,
            user_public_id=user_public_id,
            max_order_quantity_per_instrument=None,
            max_open_orders=max_open_orders,
            max_daily_notional_usd=max_daily_notional_usd,
            max_cancels_per_minute=max_cancels_per_minute,
        )
        s.add(caps)
        await s.commit()


async def _insert_trade_command(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    command_type: str = "submit",
    status: str = TradeCommandStatusEnum.DISPATCHED,
    created_at: datetime | None = None,
    quantity: float = 1.0,
    price: float | None = 100.0,
    instrument: str = "BTC-USD",
    exchange: str = "kraken",
    mode: str = "live",
) -> None:
    """Seed a :class:`TradeCommand` row with defaults for cap-test scenarios."""
    async with repo.session() as s:
        cmd = TradeCommand(
            timestamp=created_at or _NOW,
            known_to=KNOWN_TO_MAX,
            session_id="seed",
            sequence_id=1,
            command_type=command_type,
            shard_key=f"{exchange}.{instrument}.{mode}",
            wallet_public_id="",
            operator_public_id=None,
            user_public_id=user_public_id,
            exchange=exchange,
            instrument=instrument,
            mode=mode,
            strategy_id="manual",
            client_order_id=f"cid-{command_type}-{status}",
            venue_client_id=f"vcid-{command_type}-{status}",
            side="buy",
            order_type="market",
            quantity=quantity,
            price=price,
            status=status,
            created_at=created_at or _NOW,
            correlation_id=f"corr-{command_type}",
        )
        s.add(cmd)
        await s.commit()


@pytest.mark.asyncio
async def test_get_user_trading_caps_returns_row_when_present(
    repo: SQLAlchemyRepository,
) -> None:
    """Active caps row is projected to the TypedDict schema.

    Given: a seeded ``user_trading_caps`` row with numeric limits,
    When: ``get_user_trading_caps`` is invoked,
    Then: the returned dict carries every cap field verbatim and
        the Numeric notional is normalized to ``float``.
    """
    await _insert_caps(
        repo,
        user_public_id="user-1",
        max_open_orders=5,
        max_daily_notional_usd=Decimal("10000"),
        max_cancels_per_minute=20,
    )
    row = await repo.get_user_trading_caps("user-1")
    assert row is not None
    assert row["max_open_orders"] == 5
    assert row["max_daily_notional_usd"] == 10000.0
    assert row["max_cancels_per_minute"] == 20


@pytest.mark.asyncio
async def test_get_user_trading_caps_returns_none_when_absent(
    repo: SQLAlchemyRepository,
) -> None:
    """No caps row → ``None`` (enforcer treats as unbounded).

    Given: a repo with no ``user_trading_caps`` rows,
    When: ``get_user_trading_caps`` runs for a user,
    Then: ``None`` is returned so the caps enforcer admits every
        submission from that user.
    """
    result = await repo.get_user_trading_caps("ghost-user")
    assert result is None


@pytest.mark.asyncio
async def test_count_user_open_commands_counts_non_terminal_submits(
    repo: SQLAlchemyRepository,
) -> None:
    """Non-terminal submit-type rows (create/submit/replace) count; cancels excluded.

    Given: a user with one dispatched ``create`` (REST),
        one dispatched ``submit`` (strategy/engine vocab), one
        dispatched ``replace``, one terminal filled ``submit``, and
        one dispatched ``cancel``,
    When: ``count_user_open_commands`` runs,
    Then: the count is 3 (the three non-terminal submit-type rows).
        The filled row and the cancel row are excluded
        (in-flight exposure basis). The ``create`` row is counted —
        REST/plan inserts persist this vocabulary and the cap must
        see them to enforce ``max_open_orders``.
    """
    await _insert_trade_command(
        repo,
        user_public_id="user-open",
        command_type="create",
        status=TradeCommandStatusEnum.DISPATCHED,
    )
    await _insert_trade_command(
        repo,
        user_public_id="user-open",
        command_type="submit",
        status=TradeCommandStatusEnum.DISPATCHED,
    )
    await _insert_trade_command(
        repo,
        user_public_id="user-open",
        command_type="submit",
        status=TradeCommandStatusEnum.FILLED,
    )
    await _insert_trade_command(
        repo,
        user_public_id="user-open",
        command_type="cancel",
        status=TradeCommandStatusEnum.DISPATCHED,
    )
    await _insert_trade_command(
        repo,
        user_public_id="user-open",
        command_type="replace",
        status=TradeCommandStatusEnum.DISPATCHED,
    )
    count = await repo.count_user_open_commands("user-open")
    assert count == 3


@pytest.mark.asyncio
async def test_get_user_recent_submits_filters_to_window(
    repo: SQLAlchemyRepository,
) -> None:
    """Submit-type rows (create/submit/replace) inside the window are returned.

    Given: a user with an in-window ``submit`` (strategy vocab),
        an in-window ``create`` (REST), an out-of-window
        ``submit``, and an in-window ``submit`` with status=rejected,
    When: ``get_user_recent_submits`` runs with ``since=now-1h``,
    Then: the two in-window non-rejected rows are returned. The
        ``create`` row MUST be included — REST routes persist this
        vocabulary and the 24h rolling notional cap would be
        bypassed if the query filter missed it.
    """
    await _insert_trade_command(
        repo,
        user_public_id="user-submits",
        command_type="submit",
        created_at=_NOW,
        quantity=2.0,
        price=500.0,
    )
    await _insert_trade_command(
        repo,
        user_public_id="user-submits",
        command_type="create",
        created_at=_NOW - timedelta(minutes=15),
        quantity=3.0,
        price=200.0,
    )
    await _insert_trade_command(
        repo,
        user_public_id="user-submits",
        command_type="submit",
        created_at=_NOW - timedelta(hours=3),
        quantity=99.0,
        price=1.0,
    )
    await _insert_trade_command(
        repo,
        user_public_id="user-submits",
        command_type="submit",
        status=TradeCommandStatusEnum.REJECTED,
        created_at=_NOW,
        quantity=999.0,
        price=1.0,
    )
    rows = await repo.get_user_recent_submits("user-submits", since=_NOW - timedelta(hours=1))
    assert len(rows) == 2
    notionals = sorted(r["quantity"] * r["price"] for r in rows if r["price"] is not None)
    assert notionals == [600.0, 1000.0]


@pytest.mark.asyncio
async def test_get_user_recent_submits_excludes_paper_mode(
    repo: SQLAlchemyRepository,
) -> None:
    """Paper-mode submits are excluded from the rolling 24h notional basis.

    Given: a user with an in-window live ``submit`` carrying a price and
        an in-window paper ``submit`` carrying a simulator reference
        price on the same ``price`` column,
    When: ``get_user_recent_submits`` runs with ``since=now-1h``,
    Then: only the live row is returned. Counting the paper reference
        notional against the rolling 24h cap would let simulated paper
        activity exhaust the user's LIVE trading allowance.
    """
    await _insert_trade_command(
        repo,
        user_public_id="user-paper",
        command_type="submit",
        mode="live",
        created_at=_NOW,
        quantity=2.0,
        price=500.0,
    )
    await _insert_trade_command(
        repo,
        user_public_id="user-paper",
        command_type="submit",
        mode="paper",
        created_at=_NOW,
        quantity=7.0,
        price=1000.0,
    )
    rows = await repo.get_user_recent_submits("user-paper", since=_NOW - timedelta(hours=1))
    assert len(rows) == 1
    assert rows[0]["quantity"] == 2.0
    assert rows[0]["price"] == 500.0


@pytest.mark.asyncio
async def test_count_user_rolling_cancels_includes_all_statuses(
    repo: SQLAlchemyRepository,
) -> None:
    """Cancels in the sliding window count regardless of terminal state.

    Given: three cancel rows for a user — one dispatched, one
        rejected (failed at venue), both inside the 60s window,
        plus one cancel outside the window,
    When: ``count_user_rolling_cancels`` runs with
        ``since=now-60s``,
    Then: the count is 2 — terminal status of the cancel is not
        filtered because the cap limits submit frequency of cancel
        intents (rejected cancels still consume the rate budget).
    """
    await _insert_trade_command(
        repo,
        user_public_id="user-cancels",
        command_type="cancel",
        status=TradeCommandStatusEnum.DISPATCHED,
        created_at=_NOW,
    )
    await _insert_trade_command(
        repo,
        user_public_id="user-cancels",
        command_type="cancel",
        status=TradeCommandStatusEnum.REJECTED,
        created_at=_NOW - timedelta(seconds=30),
    )
    await _insert_trade_command(
        repo,
        user_public_id="user-cancels",
        command_type="cancel",
        status=TradeCommandStatusEnum.DISPATCHED,
        created_at=_NOW - timedelta(minutes=5),
    )
    count = await repo.count_user_rolling_cancels(
        "user-cancels", since=_NOW - timedelta(seconds=60)
    )
    assert count == 2
