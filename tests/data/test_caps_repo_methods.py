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
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Symbol
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


async def _legacy_catalog(repo: SQLAlchemyRepository, quote: str | None) -> None:
    """Persist venue-bound catalog evidence predating the historical submission."""
    async with repo.session() as session:
        session.add(
            Symbol(
                public_id="legacy-symbol",
                native_symbol="OPAQUE-MARKET",
                base="BTC",
                quote=quote,
                asset_type="index" if quote is None else "crypto",
                created_at=_NOW - timedelta(hours=2),
                timestamp=_NOW - timedelta(hours=2),
                known_to=KNOWN_TO_MAX,
                session_id="test",
                sequence_id=1,
            )
        )
        session.add(
            Instrument(
                public_id="legacy-instrument",
                symbol_public_id="legacy-symbol",
                exchange="kraken",
                timestamp=_NOW - timedelta(hours=2),
                known_to=KNOWN_TO_MAX,
                session_id="test",
                sequence_id=1,
            )
        )
        await session.commit()


@pytest.mark.parametrize("quote,expected", [("USD", True), ("EUR", False), (None, False)])
async def test_legacy_quote_evidence_comes_from_catalog_not_native_spelling(
    repo: SQLAlchemyRepository, quote: str | None, expected: bool
) -> None:
    """Prove legacy quote currency through temporal venue identity and metadata.

    Given: an opaque instrument label whose historical catalog quote is USD, EUR or missing.
    When: recent live submissions are projected for the rolling cap.
    Then: only explicit historical USD evidence enables the legacy price fallback.
    """
    await _legacy_catalog(repo, quote)
    await _insert_trade_command(repo, user_public_id="user", instrument="OPAQUE-MARKET")
    rows = await repo.get_user_recent_submits("user", _NOW - timedelta(hours=24))
    assert len(rows) == 1
    assert rows[0]["legacy_usd_base_notional_is_proven"] is expected


@pytest.mark.parametrize(
    "change",
    [
        "symbol_future",
        "instrument_future",
        "symbol_closed",
        "instrument_closed",
        "venue",
        "missing",
    ],
)
async def test_legacy_quote_needs_evidence_active_at_command_creation(
    repo: SQLAlchemyRepository, change: str
) -> None:
    """Refuse catalog evidence from another venue or outside the submission instant.

    Given: a USD catalog identity whose only evidence is missing or temporally ineligible.
    When: a historical live command is projected.
    Then: its missing USD value cannot borrow that unrelated currency interpretation.
    """
    if change != "missing":
        await _legacy_catalog(repo, "USD")
        async with repo.session() as session:
            symbol = await session.get(Symbol, 1)
            instrument = await session.get(Instrument, 1)
            assert symbol is not None and instrument is not None
            if change == "symbol_future":
                symbol.timestamp = _NOW + timedelta(seconds=1)
            elif change == "instrument_future":
                instrument.timestamp = _NOW + timedelta(seconds=1)
            elif change == "symbol_closed":
                symbol.known_to = _NOW
            elif change == "instrument_closed":
                instrument.known_to = _NOW
            else:
                instrument.exchange = "walutomat"
            await session.commit()
    await _insert_trade_command(repo, user_public_id="user", instrument="OPAQUE-MARKET")
    rows = await repo.get_user_recent_submits("user", _NOW - timedelta(hours=24))
    assert rows[0]["legacy_usd_base_notional_is_proven"] is False


@pytest.mark.parametrize("old_quote,new_quote", [("EUR", "USD"), ("USD", "EUR")])
async def test_legacy_quote_is_not_reinterpreted_using_current_catalog(
    repo: SQLAlchemyRepository, old_quote: str, new_quote: str
) -> None:
    """Preserve the quote dimension at submission even after a catalog version changes.

    Given: a historical instrument quote that changes one second after order creation.
    When: the current command is counted after that metadata change.
    Then: the old quote alone determines whether the legacy fallback is evidenced in USD.
    """
    await _legacy_catalog(repo, old_quote)
    async with repo.session() as session:
        old = await session.get(Symbol, 1)
        assert old is not None
        old.known_to = _NOW + timedelta(seconds=1)
        session.add(
            Symbol(
                public_id=old.public_id,
                native_symbol=old.native_symbol,
                base="BTC",
                quote=new_quote,
                asset_type="crypto",
                created_at=old.created_at,
                timestamp=old.known_to,
                known_to=KNOWN_TO_MAX,
                session_id="test",
                sequence_id=2,
            )
        )
        await session.commit()
    await _insert_trade_command(repo, user_public_id="user", instrument="OPAQUE-MARKET")
    rows = await repo.get_user_recent_submits("user", _NOW - timedelta(hours=24))
    assert rows[0]["legacy_usd_base_notional_is_proven"] is (old_quote == "USD")


async def test_cancel_command_is_excluded_while_cancelled_create_counts(
    repo: SQLAlchemyRepository,
) -> None:
    """Exclude a cancellation intent while retaining its already-admitted create exposure.

    Given: one cancelled live create command and its separate cancel command.
    When: rolling-cap membership is read.
    Then: only the create contributes, without counting the cancellation twice.
    """
    await _insert_trade_command(
        repo,
        user_public_id="user",
        command_type="create",
        status=TradeCommandStatusEnum.CANCELLED,
        quantity=2,
    )
    await _insert_trade_command(
        repo,
        user_public_id="user",
        command_type="cancel",
        status=TradeCommandStatusEnum.CANCELLED,
        quantity=99,
    )
    rows = await repo.get_user_recent_submits("user", _NOW - timedelta(hours=24))
    assert [row["quantity"] for row in rows] == [2]


@pytest.mark.parametrize("signal", ["venue", "quantity", "kind", "payoff"])
async def test_usd_quote_cannot_prove_legacy_contract_notional(
    repo: SQLAlchemyRepository, signal: str
) -> None:
    """Refuse derivative or unknown units rather than treating contracts as base assets.

    Given: a USD-quoted instrument with contract quantity, product, payoff or venue evidence.
    When: its NULL-USD history is projected for the capped admission.
    Then: quantity times price is not certified as its historical USD exposure.
    """
    await _legacy_catalog(repo, "USD")
    exchange = "kraken_futures" if signal == "venue" else "kraken"
    async with repo.session() as session:
        instrument = await session.get(Instrument, 1)
        assert instrument is not None
        instrument.exchange = exchange
        session.add(
            InstrumentSpec(
                public_id="legacy-spec",
                instrument_public_id=instrument.public_id,
                timestamp=_NOW - timedelta(hours=1),
                known_to=KNOWN_TO_MAX,
                session_id="test",
                sequence_id=1,
                quantity_unit="contract_count" if signal == "quantity" else "base_asset",
                contract_size=Decimal(10),
                instrument_kind="future" if signal == "kind" else "spot",
                contract_payoff_model="linear" if signal == "payoff" else None,
                contract_value_currency="BTC" if signal == "payoff" else None,
                settlement_currency="USD" if signal == "payoff" else None,
            )
        )
        await session.commit()
    await _insert_trade_command(
        repo, user_public_id="user", instrument="OPAQUE-MARKET", exchange=exchange
    )
    rows = await repo.get_user_recent_submits("user", _NOW - timedelta(hours=24))
    assert rows[0]["legacy_usd_base_notional_is_proven"] is False


@pytest.mark.parametrize("contract_first", [False, True])
async def test_new_specification_does_not_change_legacy_quantity_interpretation(
    repo: SQLAlchemyRepository, contract_first: bool
) -> None:
    """Resolve spot versus contract units at creation without current catalog reinterpretation.

    Given: contract quantity evidence that starts before or after the prior submission.
    When: the historical command is projected after the specification is present.
    Then: only the evidence available at creation determines whether base math is safe.
    """
    await _legacy_catalog(repo, "USD")
    async with repo.session() as session:
        session.add(
            InstrumentSpec(
                public_id="contract-spec",
                instrument_public_id="legacy-instrument",
                timestamp=(
                    _NOW - timedelta(seconds=1) if contract_first else _NOW + timedelta(seconds=1)
                ),
                known_to=KNOWN_TO_MAX,
                session_id="test",
                sequence_id=1,
                quantity_unit="contract_count",
                contract_size=Decimal(10),
            )
        )
        await session.commit()
    await _insert_trade_command(repo, user_public_id="user", instrument="OPAQUE-MARKET")
    rows = await repo.get_user_recent_submits("user", _NOW - timedelta(hours=24))
    assert rows[0]["legacy_usd_base_notional_is_proven"] is (not contract_first)


async def test_conflicting_temporal_catalog_evidence_cannot_certify_usd_history(
    repo: SQLAlchemyRepository,
) -> None:
    """Fail closed when overlapping catalog histories disagree on the order's currency.

    Given: one native venue identity with overlapping USD and EUR interpretations.
    When: its historical command is read for the rolling cap.
    Then: a matching USD row cannot hide contradictory historical currency evidence.
    """
    await _legacy_catalog(repo, "USD")
    async with repo.session() as session:
        original = await session.get(Symbol, 1)
        assert original is not None
        original.known_to = _NOW + timedelta(seconds=1)
        session.add(
            Symbol(
                public_id="other-symbol",
                native_symbol="OPAQUE-MARKET",
                base="BTC",
                quote="EUR",
                asset_type="crypto",
                timestamp=_NOW - timedelta(seconds=1),
                known_to=_NOW + timedelta(seconds=1),
                created_at=_NOW - timedelta(seconds=1),
                session_id="test",
                sequence_id=1,
            )
        )
        session.add(
            Instrument(
                public_id="other-instrument",
                symbol_public_id="other-symbol",
                exchange="kraken",
                timestamp=_NOW - timedelta(seconds=1),
                known_to=KNOWN_TO_MAX,
                session_id="test",
                sequence_id=1,
            )
        )
        await session.commit()
    await _insert_trade_command(repo, user_public_id="user", instrument="OPAQUE-MARKET")
    rows = await repo.get_user_recent_submits("user", _NOW - timedelta(hours=24))
    assert rows[0]["legacy_usd_base_notional_is_proven"] is False
