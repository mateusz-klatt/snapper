"""One-shot demo data seeder for README/LinkedIn screenshots.

Inserts a small but realistic set of facts on top of an already-migrated
``dev`` profile database (paper wallet, default operator, instruments).
Idempotent: skips when ``orders`` table already has rows, so re-runs are a
no-op.

Demo set as of 2026-05-05 (entries chosen to match REAL Kraken Futures
``market_snapshots`` so unrealized P&L is plausible against the live mark):

- LONG BTC-USD-PERP opened 2026-04-21 @ $76,820.50 (mid-70k post-dip);
  current mark $79,781 → unrealized +$420.79 on 0.1421 BTC.
- SHORT ETH-USD-PERP opened 2026-04-25 @ $2,820.40 (pre-crash level);
  current mark $2,345.60 → unrealized +$2,231.56 on 4.7 ETH = +16.8% on
  notional $13,256. Relative-value: ETH lagged BTC's bounce hard.
- One open limit buy BTC + one cancelled stop sell ETH
- Three completed backtest runs (RSI BTC, MACD BTC, RSI XLE) so the
  Backtests/Compare page renders meaningful rows.

Bypasses fact -> projection chain: positions are inserted directly so the
screenshots don't require a running paper executor / strategy runtime.
This is a screenshot tool, not a production loader.
"""

import hashlib
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import uuid7

import bcrypt
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.pool import NullPool

from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.messaging.infrastructure.publisher import SequenceTracker

KNOWN_TO_MAX_STR = "9999-12-31 23:59:59.000000"


def _sync_db_url(url: str) -> str:
    """Convert async sqlite URL to sync for direct engine use."""
    if "aiosqlite" in url:
        return url.replace("sqlite+aiosqlite://", "sqlite://")
    return url


def _lookup_instrument(conn: Connection, native_symbol: str, exchange: str) -> str | None:
    """Find the active instrument public_id for a (symbol, exchange) pair."""
    row = conn.execute(
        text(
            "SELECT i.public_id FROM instruments i "
            "JOIN symbols s ON i.symbol_public_id = s.public_id "
            "WHERE s.native_symbol = :sym AND i.exchange = :ex "
            "AND i.known_to = :ka AND s.known_to = :ka LIMIT 1"
        ),
        {"sym": native_symbol, "ex": exchange, "ka": KNOWN_TO_MAX_STR},
    ).first()
    return row[0] if row else None


def _lookup_paper_wallet(conn: Connection) -> str | None:
    """Find the paper wallet public_id seeded from dev.toml."""
    row = conn.execute(
        text(
            "SELECT public_id FROM wallets "
            "WHERE label = 'paper' AND is_paper = 1 "
            "ORDER BY id ASC LIMIT 1"
        ),
    ).first()
    return row[0] if row else None


def _lookup_operator(conn: Connection) -> str | None:
    """Find the default operator public_id seeded by multi-tenant bootstrap."""
    row = conn.execute(
        text("SELECT public_id FROM operators WHERE label = 'default' ORDER BY id ASC LIMIT 1"),
    ).first()
    return row[0] if row else None


def _insert_order(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    wallet: str,
    operator: str,
    instrument: str,
    side: str,
    order_type: str,
    price: float | None,
    size: float,
    status: str,
    filled_size: float,
    average_price: float | None,
    created_at: datetime,
) -> str:
    """Insert one order row and return its public_id."""
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO orders "
            "(public_id, instrument_public_id, mode, wallet_public_id, operator_public_id, "
            " client_order_id, exchange_order_id, created_at, updated_at, side, order_type, "
            " price, size, status, time_in_force, filled_size, average_price, error, "
            " leverage, reduce_only, plan_public_id, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:public_id, :instrument, 'paper', :wallet, :operator, "
            " :coid, :exoid, :created_at, :updated_at, :side, :order_type, "
            " :price, :size, :status, 'gtc', :filled_size, :avg_price, NULL, "
            " NULL, 0, NULL, "
            " :ts, :known_to, :sid, :seq)"
        ),
        {
            "public_id": public_id,
            "instrument": instrument,
            "wallet": wallet,
            "operator": operator,
            "coid": f"demo-{public_id}",
            "exoid": f"ex-{public_id}",
            "created_at": str(created_at),
            "updated_at": str(created_at),
            "side": side,
            "order_type": order_type,
            "price": price,
            "size": size,
            "status": status,
            "filled_size": filled_size,
            "avg_price": average_price,
            "ts": str(created_at),
            "known_to": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("orders"),
        },
    )
    return public_id


def _insert_execution(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    wallet: str,
    operator: str,
    order_public_id: str,
    side: str,
    status: str,
    price: float,
    size: float,
    fee: float,
    executed_at: datetime,
) -> str:
    """Insert one execution row and return its public_id."""
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO executions "
            "(public_id, order_public_id, wallet_public_id, operator_public_id, "
            " exec_id, trade_id, side, status, price, size, fee, fee_asset, "
            " executed_at, liquidity_role, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:public_id, :opid, :wallet, :operator, "
            " :exid, :tid, :side, :status, :price, :size, :fee, 'USD', "
            " :exec_at, 'taker', "
            " :ts, :known_to, :sid, :seq)"
        ),
        {
            "public_id": public_id,
            "opid": order_public_id,
            "wallet": wallet,
            "operator": operator,
            "exid": f"exec-{public_id[:8]}",
            "tid": f"trade-{public_id[:8]}",
            "side": side,
            "status": status,
            "price": price,
            "size": size,
            "fee": fee,
            "exec_at": str(executed_at),
            "ts": str(executed_at),
            "known_to": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("executions"),
        },
    )
    return public_id


def _insert_position(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    wallet: str,
    instrument: str,
    quantity: float,
    average_price: float,
    unrealized_pnl: float,
    realized_pnl: float,
    ts: datetime,
) -> str:
    """Insert one position projection row and return its public_id."""
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO positions "
            "(public_id, instrument_public_id, mode, wallet_public_id, "
            " quantity, average_price, unrealized_pnl, realized_pnl, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:public_id, :instrument, 'paper', :wallet, "
            " :qty, :avg, :upnl, :rpnl, "
            " :ts, :known_to, :sid, :seq)"
        ),
        {
            "public_id": public_id,
            "instrument": instrument,
            "wallet": wallet,
            "qty": quantity,
            "avg": average_price,
            "upnl": unrealized_pnl,
            "rpnl": realized_pnl,
            "ts": str(ts),
            "known_to": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("positions"),
        },
    )
    return public_id


def _insert_backtest_run(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    wallet: str,
    instrument: str,
    exchange: str,
    strategy_name: str,
    strategy_params: dict[str, object],
    timeframe: str,
    start: datetime,
    end: datetime,
    initial_cash: float,
    status: str,
    started_at: datetime,
    completed_at: datetime | None,
) -> str:
    """Insert one backtest_runs row and return its public_id."""
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO backtest_runs "
            "(public_id, wallet_public_id, operator_public_id, "
            " strategy_name, strategy_params, "
            " instrument_public_id, exchange, mode, timeframe, "
            " start_date, end_date, initial_cash, status, "
            " created_by_user_id, started_at, completed_at, error, "
            " process_name, execution_mode, fill_model, "
            " slippage_bps, commission_bps, config_hash, target_execution_exchange, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:public_id, :wallet, NULL, "
            " :strategy, :params, "
            " :instrument, :exchange, 'paper', :tf, "
            " :start, :end, :cash, :status, "
            " 'demo', :started, :completed, NULL, "
            " NULL, 'direct_db', 'market', "
            " 5.0, 8.0, NULL, NULL, "
            " :ts, :known_to, :sid, :seq)"
        ),
        {
            "public_id": public_id,
            "wallet": wallet,
            "strategy": strategy_name,
            "params": json.dumps(strategy_params),
            "instrument": instrument,
            "exchange": exchange,
            "tf": timeframe,
            "start": str(start),
            "end": str(end),
            "cash": initial_cash,
            "status": status,
            "started": str(started_at),
            "completed": str(completed_at) if completed_at else None,
            "ts": str(start),
            "known_to": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("backtest_runs"),
        },
    )
    return public_id


def _seed_ai_delegate_review(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    operator: str,
    wallet: str,
    instrument: str,
) -> None:
    """Seed one ``ai_delegate`` user + pending ``ai_review`` row.

    Creates an ``ai_demo`` login (password ``DemoSnapper2026!``), the
    matching ``ai_delegates`` row, an operator membership, an instrument
    scope grant, and a pending review on the supplied instrument with a
    Strait-of-Hormuz oil-volatility rationale embedded in the signal
    envelope. Lets the AI Reviews tab render real data instead of the
    "Reserved for AI delegates" empty state.
    """
    now = datetime.now(tz=UTC)
    delegate_user_pid = str(uuid7())
    pwd_hash = bcrypt.hashpw(b"DemoSnapper2026!", bcrypt.gensalt()).decode()
    conn.execute(
        text(
            "INSERT INTO users "
            "(public_id, username, email, password_hash, role, is_active, "
            " created_at, timestamp, known_to, session_id, sequence_id) "
            "VALUES (:pid, 'ai_demo', 'ai_demo@snapper.local', :pw, 'ai_delegate', 1, "
            " :now, :now, :ka, :sid, :seq)"
        ),
        {
            "pid": delegate_user_pid,
            "pw": pwd_hash,
            "now": str(now),
            "ka": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("users"),
        },
    )

    delegate_pid = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO ai_delegates "
            "(public_id, user_public_id, last_seen_at, active_reviews_count, created_at, updated_at) "
            "VALUES (:pid, :uid, :now, 1, :now, :now)"
        ),
        {"pid": delegate_pid, "uid": delegate_user_pid, "now": str(now)},
    )

    conn.execute(
        text(
            "INSERT INTO user_operator_memberships "
            "(public_id, user_public_id, operator_public_id, is_primary, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES (:pid, :uid, :op, 1, :now, :ka, :sid, :seq)"
        ),
        {
            "pid": str(uuid7()),
            "uid": delegate_user_pid,
            "op": operator,
            "now": str(now),
            "ka": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("user_operator_memberships"),
        },
    )

    conn.execute(
        text(
            "INSERT INTO wallet_operator_scope_grants "
            "(public_id, operator_public_id, wallet_public_id, granted_by_user_public_id, "
            " scope_kind, instrument_public_id, note, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES (:pid, :op, :wal, :grantor, 'instrument', :inst, "
            "  'Demo CLM6 oil scope for ai_demo delegate', "
            "  :now, :ka, :sid, :seq)"
        ),
        {
            "pid": str(uuid7()),
            "op": operator,
            "wal": wallet,
            "grantor": delegate_user_pid,
            "inst": instrument,
            "now": str(now),
            "ka": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("wallet_operator_scope_grants"),
        },
    )

    review_pid = str(uuid7())
    created = now - timedelta(minutes=4)
    fanout_after = now - timedelta(minutes=1)
    deadline = now + timedelta(minutes=11)
    envelope = {
        "type": "signal",
        "side": "buy",
        "symbol": "CLM6-NYMEX",
        "timeframe": "1h",
        "strategy": "HormuzCrudeShock",
        "thesis": (
            "Breakout continuation above $104 after Strait-of-Hormuz news flow; "
            "WTI crude intraday range $99.30 – $109.86 on supply-route risk."
        ),
        "strength": 0.71,
        "news_anchors": [
            "newsnow.co.uk: Brent crude headlines",
            "Reuters: Hormuz route disruption fears",
        ],
    }
    metadata = {
        "symbol": "CLM6-NYMEX",
        "venue": "NYMEX",
        "last_price": 104.27,
        "intraday_low": 99.30,
        "intraday_high": 109.86,
        "theme": "Strait-of-Hormuz volatility",
    }
    snap_hash = hashlib.sha256(json.dumps(envelope, sort_keys=True).encode()).hexdigest()
    conn.execute(
        text(
            "INSERT INTO ai_reviews "
            "(public_id, session_id, sequence_id, "
            " user_public_id, operator_public_id, wallet_public_id, "
            " instrument_public_id, strategy_public_id, "
            " selected_delegate_public_id, responding_delegate_public_id, "
            " resolution_mode, status, signal_envelope, signal_snapshot_hash, "
            " instrument_metadata, deadline, fanout_after, decision, rationale, "
            " dispatch_version, counter_decremented_at, "
            " created_at, updated_at, resolved_at) "
            "VALUES (:pid, :sid, :seq, "
            " :uid, :op, :wal, "
            " :inst, :strat, "
            " :dlg, NULL, "
            " NULL, 'pending', :env, :snap, "
            " :meta, :deadline, :fanout, NULL, NULL, "
            " 0, NULL, "
            " :created, :now, NULL)"
        ),
        {
            "pid": review_pid,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("ai_reviews"),
            "uid": delegate_user_pid,
            "op": operator,
            "wal": wallet,
            "inst": instrument,
            "strat": str(uuid7()),
            "dlg": delegate_pid,
            "env": json.dumps(envelope),
            "snap": snap_hash,
            "meta": json.dumps(metadata),
            "deadline": str(deadline),
            "fanout": str(fanout_after),
            "created": str(created),
            "now": str(now),
        },
    )

    conn.execute(
        text(
            "INSERT INTO ai_review_events "
            "(public_id, review_public_id, event_type, actor_delegate_public_id, "
            " previous_status, new_status, payload, occurred_at) "
            "VALUES (:pid, :rev, 'created', NULL, NULL, 'pending', :payload, :ts)"
        ),
        {
            "pid": str(uuid7()),
            "rev": review_pid,
            "payload": json.dumps({"selected_delegate": delegate_pid}),
            "ts": str(created),
        },
    )
    conn.execute(
        text(
            "INSERT INTO ai_review_events "
            "(public_id, review_public_id, event_type, actor_delegate_public_id, "
            " previous_status, new_status, payload, occurred_at) "
            "VALUES (:pid, :rev, 'fanout_dispatched', :dlg, 'pending', 'fanout_dispatched', :p, :ts)"
        ),
        {
            "pid": str(uuid7()),
            "rev": review_pid,
            "dlg": delegate_pid,
            "p": json.dumps({"reason": "primary_quorum_reached", "delegates_pinged": 1}),
            "ts": str(fanout_after),
        },
    )


def main() -> None:
    """Run the demo seed end-to-end."""
    db_url = _sync_db_url(BootstrapSettingsLoader().db_url)
    engine = create_engine(db_url, poolclass=NullPool)
    tracker = SequenceTracker()

    with engine.connect() as conn:
        wallet = _lookup_paper_wallet(conn)
        operator = _lookup_operator(conn)
        if not wallet or not operator:
            raise RuntimeError(
                "paper wallet or default operator not found — run `make migrate-dev` first"
            )

        btc_perp = _lookup_instrument(conn, "BTC-USD-PERP", "kraken_futures")
        eth_perp = _lookup_instrument(conn, "ETH-USD-PERP", "kraken_futures")
        clm6 = _lookup_instrument(conn, "CLM6-NYMEX", "kraken_equities")
        gcm6 = _lookup_instrument(conn, "GCM6-COMEX", "kraken_equities")

        if not btc_perp or not eth_perp:
            raise RuntimeError(
                f"required instruments not found — btc_perp={btc_perp} eth_perp={eth_perp}; "
                "run `make run-static` to populate symbols"
            )

        existing_orders = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() or 0
        if existing_orders > 0:
            print(f"orders table already has {existing_orders} rows, skipping demo seed")
            return

        order1_t = datetime(2026, 4, 21, 9, 14, 32, tzinfo=UTC)
        order1 = _insert_order(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            instrument=btc_perp,
            side="buy",
            order_type="market",
            price=None,
            size=0.1421,
            status="filled",
            filled_size=0.1421,
            average_price=76820.50,
            created_at=order1_t,
        )
        _insert_execution(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            order_public_id=order1,
            side="buy",
            status="filled",
            price=76820.50,
            size=0.1421,
            fee=10.92,
            executed_at=order1_t,
        )
        _insert_position(
            conn,
            tracker,
            wallet=wallet,
            instrument=btc_perp,
            quantity=0.1421,
            average_price=76820.50,
            unrealized_pnl=420.79,
            realized_pnl=0.0,
            ts=order1_t,
        )

        order2_t = datetime(2026, 4, 25, 14, 18, 42, tzinfo=UTC)
        order2 = _insert_order(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            instrument=eth_perp,
            side="sell",
            order_type="market",
            price=None,
            size=4.7,
            status="filled",
            filled_size=4.7,
            average_price=2820.40,
            created_at=order2_t,
        )
        _insert_execution(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            order_public_id=order2,
            side="sell",
            status="filled",
            price=2820.40,
            size=4.7,
            fee=10.61,
            executed_at=order2_t,
        )
        _insert_position(
            conn,
            tracker,
            wallet=wallet,
            instrument=eth_perp,
            quantity=-4.7,
            average_price=2820.40,
            unrealized_pnl=2231.56,
            realized_pnl=0.0,
            ts=order2_t,
        )

        order3_t = datetime(2026, 5, 4, 22, 11, 7, tzinfo=UTC)
        _insert_order(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            instrument=btc_perp,
            side="buy",
            order_type="limit",
            price=77500.0,
            size=0.0843,
            status="open",
            filled_size=0.0,
            average_price=None,
            created_at=order3_t,
        )

        order4_t = datetime(2026, 5, 1, 16, 8, 52, tzinfo=UTC)
        _insert_order(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            instrument=eth_perp,
            side="sell",
            order_type="stop",
            price=2950.0,
            size=2.5,
            status="cancelled",
            filled_size=0.0,
            average_price=None,
            created_at=order4_t,
        )

        bt_start = datetime(2025, 11, 1, tzinfo=UTC)
        bt_end = datetime(2026, 4, 30, tzinfo=UTC)

        _insert_backtest_run(
            conn,
            tracker,
            wallet=wallet,
            instrument=btc_perp,
            exchange="kraken_futures",
            strategy_name="RsiReversion",
            strategy_params={"period": 14, "oversold": 28, "overbought": 72},
            timeframe="1h",
            start=bt_start,
            end=bt_end,
            initial_cash=10000.0,
            status="completed",
            started_at=datetime(2026, 5, 4, 21, 0, 0, tzinfo=UTC),
            completed_at=datetime(2026, 5, 4, 21, 14, 32, tzinfo=UTC),
        )

        _insert_backtest_run(
            conn,
            tracker,
            wallet=wallet,
            instrument=btc_perp,
            exchange="kraken_futures",
            strategy_name="MacdCrossover",
            strategy_params={"fast": 12, "slow": 26, "signal": 9},
            timeframe="1h",
            start=bt_start,
            end=bt_end,
            initial_cash=10000.0,
            status="completed",
            started_at=datetime(2026, 5, 4, 21, 14, 33, tzinfo=UTC),
            completed_at=datetime(2026, 5, 4, 21, 28, 11, tzinfo=UTC),
        )

        if clm6:
            _insert_backtest_run(
                conn,
                tracker,
                wallet=wallet,
                instrument=clm6,
                exchange="kraken_equities",
                strategy_name="RsiReversion",
                strategy_params={"period": 14, "oversold": 30, "overbought": 70},
                timeframe="1d",
                start=datetime(2025, 5, 1, tzinfo=UTC),
                end=bt_end,
                initial_cash=10000.0,
                status="completed",
                started_at=datetime(2026, 5, 4, 21, 28, 12, tzinfo=UTC),
                completed_at=datetime(2026, 5, 4, 21, 39, 50, tzinfo=UTC),
            )

        if gcm6:
            _insert_backtest_run(
                conn,
                tracker,
                wallet=wallet,
                instrument=gcm6,
                exchange="kraken_equities",
                strategy_name="MacdCrossover",
                strategy_params={"fast": 8, "slow": 21, "signal": 5},
                timeframe="1d",
                start=datetime(2025, 5, 1, tzinfo=UTC),
                end=bt_end,
                initial_cash=10000.0,
                status="completed",
                started_at=datetime(2026, 5, 4, 21, 39, 51, tzinfo=UTC),
                completed_at=datetime(2026, 5, 4, 21, 51, 33, tzinfo=UTC),
            )

        if clm6:
            _seed_ai_delegate_review(
                conn,
                tracker,
                operator=operator,
                wallet=wallet,
                instrument=clm6,
            )

        conn.commit()
    engine.dispose()
    print("demo seed inserted: 4 orders, 2 executions, 2 positions, 3 backtests")


if __name__ == "__main__":
    main()
