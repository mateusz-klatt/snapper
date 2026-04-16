"""Backtest comparison diff computation (Phase 2c Step 4).

Pure functions that turn two sets of run artifacts into the diff
shapes exposed by ``GET /api/backtests/compare/{id}``. Diff is
always recomputed on GET from the current artifact rows so a
metric-schema change never stales a persisted diff — see plan §4.3.
"""

from collections import Counter
from collections import defaultdict
from datetime import datetime
from typing import Any

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


def _promoted_lookup(result: BacktestResultRow | None, name: str) -> float | None:
    """Typed-column-first with JSON fallback for the 5 promoted names.

    ``is not None`` coalescing (NOT Python truthiness) so a typed
    ``0.0`` is preserved against a stale non-zero JSON fallback
    (plan §4.3 R6 gpt-5.3-codex fix).
    """
    if result is None:
        return None
    typed = result.get(name)
    if typed is not None:
        return float(typed)
    extra = result.get("extra_metrics") or {}
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
    value = result.get(name)
    if value is None:
        return None
    return float(value) if isinstance(value, int | float) else None


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
    """Build the side-by-side metrics diff (plan §4.3).

    Each metric name emits exactly one row. The 5 promoted names are
    explicitly subtracted from the ``extra_metrics`` union so a
    mixed-vintage pair (pre-0005 leg with JSON-only value vs
    post-0005 leg with typed column) produces exactly one row per
    promoted name.
    """
    rows: list[dict[str, Any]] = []
    for name in _EXPLICIT_METRIC_NAMES:
        a = _read_metric(result_a, name)
        b = _read_metric(result_b, name)
        delta, pct = _delta_pct(a, b)
        rows.append({"name": name, "run_a": a, "run_b": b, "delta": delta, "pct": pct})
    extra_a = (result_a or {}).get("extra_metrics") or {}
    extra_b = (result_b or {}).get("extra_metrics") or {}
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


def compute_trades_diff(
    trades_a: list[BacktestTradeRow], trades_b: list[BacktestTradeRow]
) -> list[dict[str, Any]]:
    """Multiset match on (instrument, executed_at, side, qty, price).

    Two buckets with counts (n_a, n_b) produce ``min(n_a, n_b)``
    common pairs and ``|n_a - n_b|`` singletons on the surplus side.
    Trades within a bucket are sorted by ``public_id`` ascending so
    the match is deterministic.
    """
    buckets: dict[tuple[str, datetime, str, int, int], dict[str, list[BacktestTradeRow]]] = (
        defaultdict(lambda: {"a": [], "b": []})
    )
    for trade in trades_a:
        key = (
            trade["instrument"],
            trade["executed_at"],
            trade["side"],
            _q(trade["quantity"]),
            _q(trade["price"]),
        )
        buckets[key]["a"].append(trade)
    for trade in trades_b:
        key = (
            trade["instrument"],
            trade["executed_at"],
            trade["side"],
            _q(trade["quantity"]),
            _q(trade["price"]),
        )
        buckets[key]["b"].append(trade)
    out: list[dict[str, Any]] = []
    for key, sides in buckets.items():
        a_list = sorted(sides["a"], key=lambda t: t["public_id"])
        b_list = sorted(sides["b"], key=lambda t: t["public_id"])
        common_count = min(len(a_list), len(b_list))
        for i in range(common_count):
            pnl_a = a_list[i].get("pnl")
            pnl_b = b_list[i].get("pnl")
            pnl_delta = None if pnl_a is None or pnl_b is None else float(pnl_b) - float(pnl_a)
            out.append(
                {
                    "instrument": key[0],
                    "executed_at": key[1],
                    "side": key[2],
                    "quantity": float(a_list[i]["quantity"]),
                    "price": float(a_list[i]["price"]),
                    "leg": "common",
                    "pnl_a": float(pnl_a) if pnl_a is not None else None,
                    "pnl_b": float(pnl_b) if pnl_b is not None else None,
                    "pnl_delta": pnl_delta,
                }
            )
        for leg_name, surplus in (("a", a_list[common_count:]), ("b", b_list[common_count:])):
            for trade in surplus:
                out.append(
                    {
                        "instrument": trade["instrument"],
                        "executed_at": trade["executed_at"],
                        "side": trade["side"],
                        "quantity": float(trade["quantity"]),
                        "price": float(trade["price"]),
                        "leg": leg_name,
                        "pnl_a": (
                            float(trade["pnl"])
                            if leg_name == "a" and trade.get("pnl") is not None
                            else None
                        ),
                        "pnl_b": (
                            float(trade["pnl"])
                            if leg_name == "b" and trade.get("pnl") is not None
                            else None
                        ),
                        "pnl_delta": None,
                    }
                )
    return out


def compute_signals_diff(
    signals_a: list[BacktestSignalRow], signals_b: list[BacktestSignalRow]
) -> list[dict[str, Any]]:
    """Multiset match on (instrument, signal_time, signal_type)."""
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
