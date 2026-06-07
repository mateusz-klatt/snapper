"""Backtest comparison diff computation.

Pure functions that turn two sets of run artifacts into the diff
shapes exposed by ``GET /api/backtests/compare/{id}``. Diff is
always recomputed on GET from the current artifact rows so a
metric-schema change never stales a persisted diff.
"""

from collections import Counter
from collections import defaultdict
from datetime import datetime
from typing import Any
from typing import Literal
from typing import cast

from snapper.application.backtest.metrics import PROMOTED_METRIC_NAMES
from snapper.data.repository_types import BacktestEquityPointRow
from snapper.data.repository_types import BacktestResultRow
from snapper.data.repository_types import BacktestSignalRow
from snapper.data.repository_types import BacktestTradeRow

_EXPLICIT_METRIC_NAMES: tuple[str, ...] = (
    "total_trades",
    "winning_trades",
    "losing_trades",
    "total_pnl",
    "max_drawdown",
    "sharpe_ratio",
    "win_rate",
    "profit_factor",
    "final_equity",
    "max_equity",
    "sortino_ratio",
    "cagr",
    "calmar_ratio",
    "expectancy",
    "avg_trade_pnl",
    "max_drawdown_duration_seconds",
    "exposure_ratio",
    "turnover_ratio",
)

_TradeBucketKey = tuple[str, datetime, str, int, int]
_TradeLeg = Literal["a", "b"]


def _promoted_lookup(result: BacktestResultRow | None, name: str) -> float | None:
    """Typed-column-first with JSON fallback for the 5 promoted names.

    ``is not None`` coalescing (NOT Python truthiness) so a typed
    ``0.0`` is preserved against a stale non-zero JSON fallback.
    """
    if result is None:
        return None
    typed = cast(Any, result).get(name)
    if isinstance(typed, int | float):
        return float(typed)
    extra = cast(dict[str, Any], cast(Any, result).get("extra_metrics") or {})
    fallback = extra.get(name)
    return float(fallback) if isinstance(fallback, int | float) else None


def _read_metric(result: BacktestResultRow | None, name: str) -> float | None:
    """Resolve a metric value from a result row.

    For the 5 promoted names uses typed-else-JSON precedence; for
    everything else reads the typed column directly.
    """
    if result is None:
        return None
    if name in PROMOTED_METRIC_NAMES:
        return _promoted_lookup(result, name)
    value = cast(Any, result).get(name)
    if isinstance(value, int | float):
        return float(value)
    return None


def _delta_pct(a: float | None, b: float | None) -> tuple[float | None, float | None]:
    """Compute (delta, pct) where delta = b - a and pct = delta / |a|.

    Returns (None, None) when either side is None or a == 0 (pct
    undefined). Delta stays defined when a is non-zero and b is None
    (and vice versa) only when both are non-null — we keep both
    None when either leg is None.
    """
    if a is None or b is None:
        return None, None
    delta = b - a
    if a == 0:
        return delta, None
    return delta, delta / abs(a)


def compute_metrics_diff(
    result_a: BacktestResultRow | None, result_b: BacktestResultRow | None
) -> list[dict[str, Any]]:
    """Build the side-by-side metrics diff.

    Each metric name emits exactly one row. The 5 promoted names are
    explicitly subtracted from the ``extra_metrics`` union so a
    mixed-vintage pair (pre-0005 leg with JSON-only value vs
    post-0005 leg with typed column) produces exactly one row per
    promoted name.

    Args:
        result_a: Result row for the first leg (None when missing).
        result_b: Result row for the second leg (None when missing).

    Returns:
        List of dicts with ``name``, ``run_a``, ``run_b``, ``delta``
        and ``pct`` keys — one per metric.
    """
    rows: list[dict[str, Any]] = []
    for name in _EXPLICIT_METRIC_NAMES:
        a = _read_metric(result_a, name)
        b = _read_metric(result_b, name)
        delta, pct = _delta_pct(a, b)
        rows.append({"name": name, "run_a": a, "run_b": b, "delta": delta, "pct": pct})
    extra_a = cast(
        dict[str, Any],
        (cast(Any, result_a).get("extra_metrics") if result_a is not None else None) or {},
    )
    extra_b = cast(
        dict[str, Any],
        (cast(Any, result_b).get("extra_metrics") if result_b is not None else None) or {},
    )
    extra_keys = (set(extra_a) | set(extra_b)) - PROMOTED_METRIC_NAMES
    for name in sorted(extra_keys):
        a_val = extra_a.get(name)
        b_val = extra_b.get(name)
        a = float(a_val) if isinstance(a_val, int | float) else None
        b = float(b_val) if isinstance(b_val, int | float) else None
        delta, pct = _delta_pct(a, b)
        rows.append({"name": name, "run_a": a, "run_b": b, "delta": delta, "pct": pct})
    return rows


def compute_equity_overlay(
    equity_a: list[BacktestEquityPointRow], equity_b: list[BacktestEquityPointRow]
) -> list[dict[str, Any]]:
    """Full-outer-join equity samples on ``point_time``.

    One-sided samples carry ``None`` for the missing leg so the
    frontend can render gaps explicitly.

    Args:
        equity_a: Equity points from the first leg.
        equity_b: Equity points from the second leg.

    Returns:
        Sorted list of dicts with ``point_time``, ``equity_a``,
        ``equity_b`` keys.
    """
    by_time: dict[datetime, dict[str, float | None]] = {}
    for point in equity_a:
        by_time.setdefault(point["point_time"], {"equity_a": None, "equity_b": None})
        by_time[point["point_time"]]["equity_a"] = float(point["equity"])
    for point in equity_b:
        by_time.setdefault(point["point_time"], {"equity_a": None, "equity_b": None})
        by_time[point["point_time"]]["equity_b"] = float(point["equity"])
    return [
        {"point_time": t, "equity_a": v["equity_a"], "equity_b": v["equity_b"]}
        for t, v in sorted(by_time.items())
    ]


def _q(x: float) -> int:
    """Quantize a float to 8 decimals for multiset key equality.

    1e-8 is below the tick size of every supported exchange so real
    ticks stay distinguishable while IEEE 754 drift collapses.
    """
    return round(x * 1e8)


def _trade_bucket_key(trade: BacktestTradeRow) -> _TradeBucketKey:
    """Return the deterministic bucket key used for multiset matching."""
    return (
        trade["instrument"],
        trade["executed_at"],
        trade["side"],
        _q(trade["quantity"]),
        _q(trade["price"]),
    )


def _empty_trade_bucket() -> dict[_TradeLeg, list[BacktestTradeRow]]:
    """Create one empty A/B bucket for grouped trade diffing."""
    return {"a": [], "b": []}


def _append_trade_leg(
    buckets: dict[_TradeBucketKey, dict[_TradeLeg, list[BacktestTradeRow]]],
    leg_name: _TradeLeg,
    trades: list[BacktestTradeRow],
) -> None:
    """Append one leg of trades into the shared multiset buckets."""
    for trade in trades:
        buckets[_trade_bucket_key(trade)][leg_name].append(trade)


def _trade_pnl_value(raw_pnl: int | float | None) -> float | None:
    """Normalize an optional trade PnL to float-or-None."""
    if raw_pnl is None:
        return None
    return float(raw_pnl)


def _common_trade_diff_row(
    key: _TradeBucketKey,
    trade_a: BacktestTradeRow,
    trade_b: BacktestTradeRow,
) -> dict[str, object]:
    """Build the diff row for one matched A/B trade pair."""
    pnl_a = _trade_pnl_value(trade_a.get("pnl"))
    pnl_b = _trade_pnl_value(trade_b.get("pnl"))
    pnl_delta = None if pnl_a is None or pnl_b is None else pnl_b - pnl_a
    return {
        "instrument": key[0],
        "executed_at": key[1],
        "side": key[2],
        "quantity": float(trade_a["quantity"]),
        "price": float(trade_a["price"]),
        "leg": "common",
        "pnl_a": pnl_a,
        "pnl_b": pnl_b,
        "pnl_delta": pnl_delta,
    }


def _surplus_trade_diff_row(
    leg_name: _TradeLeg,
    trade: BacktestTradeRow,
) -> dict[str, object]:
    """Build the diff row for one surplus trade on leg A or B."""
    pnl_val = _trade_pnl_value(trade.get("pnl"))
    return {
        "instrument": trade["instrument"],
        "executed_at": trade["executed_at"],
        "side": trade["side"],
        "quantity": float(trade["quantity"]),
        "price": float(trade["price"]),
        "leg": leg_name,
        "pnl_a": pnl_val if leg_name == "a" else None,
        "pnl_b": pnl_val if leg_name == "b" else None,
        "pnl_delta": None,
    }


def _bucket_trade_rows(
    key: _TradeBucketKey,
    sides: dict[_TradeLeg, list[BacktestTradeRow]],
) -> list[dict[str, object]]:
    """Expand one grouped multiset bucket into common and surplus rows."""
    a_list = sorted(sides["a"], key=lambda trade: trade["public_id"])
    b_list = sorted(sides["b"], key=lambda trade: trade["public_id"])
    common_count = min(len(a_list), len(b_list))
    common_rows = [
        _common_trade_diff_row(key, trade_a, trade_b)
        for trade_a, trade_b in zip(a_list, b_list, strict=False)
    ]
    surplus_rows = [_surplus_trade_diff_row("a", trade) for trade in a_list[common_count:]]
    surplus_rows.extend(_surplus_trade_diff_row("b", trade) for trade in b_list[common_count:])
    return common_rows + surplus_rows


def compute_trades_diff(
    trades_a: list[BacktestTradeRow], trades_b: list[BacktestTradeRow]
) -> list[dict[str, Any]]:
    """Multiset match on (instrument, executed_at, side, qty, price).

    Two buckets with counts (n_a, n_b) produce ``min(n_a, n_b)``
    common pairs and ``|n_a - n_b|`` singletons on the surplus side.
    Trades within a bucket are sorted by ``public_id`` ascending so
    the match is deterministic.

    Args:
        trades_a: Trade rows from the first leg.
        trades_b: Trade rows from the second leg.

    Returns:
        List of diff entries — ``leg="common"`` with ``pnl_delta`` or
        ``leg="a"|"b"`` singletons.
    """
    buckets: dict[_TradeBucketKey, dict[_TradeLeg, list[BacktestTradeRow]]] = defaultdict(
        _empty_trade_bucket
    )
    _append_trade_leg(buckets, "a", trades_a)
    _append_trade_leg(buckets, "b", trades_b)
    out: list[dict[str, Any]] = []
    for key, sides in buckets.items():
        out.extend(_bucket_trade_rows(key, sides))
    return out


def compute_signals_diff(
    signals_a: list[BacktestSignalRow], signals_b: list[BacktestSignalRow]
) -> list[dict[str, Any]]:
    """Multiset match on (instrument, signal_time, signal_type).

    Args:
        signals_a: Signals from the first leg.
        signals_b: Signals from the second leg.

    Returns:
        List of diff entries with ``leg`` discriminator.
    """
    key_a = Counter((s["instrument"], s["signal_time"], s["signal_type"]) for s in signals_a)
    key_b = Counter((s["instrument"], s["signal_time"], s["signal_type"]) for s in signals_b)
    out: list[dict[str, Any]] = []
    for key in set(key_a) | set(key_b):
        count_a = key_a.get(key, 0)
        count_b = key_b.get(key, 0)
        common = min(count_a, count_b)
        for _ in range(common):
            out.append(
                {
                    "instrument": key[0],
                    "signal_time": key[1],
                    "signal_type": key[2],
                    "leg": "common",
                }
            )
        for _ in range(count_a - common):
            out.append(
                {
                    "instrument": key[0],
                    "signal_time": key[1],
                    "signal_type": key[2],
                    "leg": "a",
                }
            )
        for _ in range(count_b - common):
            out.append(
                {
                    "instrument": key[0],
                    "signal_time": key[1],
                    "signal_type": key[2],
                    "leg": "b",
                }
            )
    return out
