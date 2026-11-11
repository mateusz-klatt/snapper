"""Regression tests pinning Walutomat's external-only history capability.

The spot anchor bootstrap classifies a PAYIN, PAYOUT, TRANSFER or DIRECT_FX
history row as external activity only while the Walutomat adapter pins that this
system initiates none of them. These tests hold that pin to the adapter's actual
outbound writer set, read from its source: the signed endpoint registry and the
HTTP write call sites. A new writer, or a new signed endpoint of any kind, fails
here and forces the operation-taxonomy review before the pin may stay.
"""

import ast
from pathlib import Path

import snapper
import snapper.infrastructure.exchanges.implementations.walutomat as walutomat_module
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient

_SIGNED_PREFIX = "/api/v2.0.0/"
_WRITE_METHODS = frozenset({"post", "put", "patch", "delete", "request", "send", "stream"})
_QUEUE_RECEIVERS = frozenset({"self._tick_queue", "self._candle_queue"})
_EXTERNAL_ONLY_MARKERS = ("payin", "payout", "transfer", "direct_fx")


def _adapter_tree() -> ast.Module:
    """Parse the Walutomat adapter's source."""
    return ast.parse(Path(walutomat_module.__file__).read_text(encoding="utf-8"))


def _signed_endpoint_paths(node: ast.AST) -> set[str]:
    """Return every signed API endpoint path literal under a node, without its query.

    Args:
        node: The syntax tree to scan.

    Returns:
        The endpoint paths relative to the signed API prefix.
    """
    paths: set[str] = set()
    for child in ast.walk(node):
        if (
            isinstance(child, ast.Constant)
            and isinstance(child.value, str)
            and child.value.startswith(_SIGNED_PREFIX)
        ):
            paths.add(child.value.removeprefix(_SIGNED_PREFIX).split("?")[0])
    return paths


def _outbound_writers(tree: ast.Module) -> set[tuple[str, frozenset[str]]]:
    """Map every HTTP write call site to its function and the endpoints it signs.

    Args:
        tree: The adapter's syntax tree.

    Returns:
        One ``(function name, signed endpoint paths)`` pair per writing function.
    """
    writers: set[tuple[str, frozenset[str]]] = set()
    for function in ast.walk(tree):
        if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for call in ast.walk(function):
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr in _WRITE_METHODS
                and ast.unparse(call.func.value) not in _QUEUE_RECEIVERS
            ):
                writers.add((function.name, frozenset(_signed_endpoint_paths(function))))
    return writers


def test_walutomat_pins_that_it_initiates_no_external_only_operation() -> None:
    """The adapter pins the capability to false while the base fails closed.

    Given: The Walutomat adapter and the exchange client base.
    When: Their external-only capability declarations are read.
    Then: Walutomat pins ``False`` and the base default is the fail-closed ``True``.
    """
    assert WalutomatExchangeClient.initiates_external_only_history_operations is False
    assert ExchangeClientBase.initiates_external_only_history_operations is True


def test_the_signed_endpoint_registry_is_exactly_market_fx_and_account_reads() -> None:
    """The adapter signs requests only for market FX orders and account reads.

    Given: The adapter's source.
    When: Every signed endpoint literal is collected.
    Then: The registry is exactly the known set, and no endpoint names a
        deposit, withdrawal, transfer, or direct exchange.
    """
    paths = _signed_endpoint_paths(_adapter_tree())
    assert paths == {
        "account/balances",
        "account/history",
        "market_fx/orders",
        "market_fx/orders/active",
        "market_fx/orders/close",
    }
    assert not any(marker in path.lower() for path in paths for marker in _EXTERNAL_ONLY_MARKERS)


def test_the_only_outbound_writers_place_and_close_market_fx_orders() -> None:
    """Every HTTP write call site is an order placement or an order close.

    Given: The adapter's source.
    When: Every HTTP write call site is mapped to its function and signed endpoint.
    Then: The writers are exactly order creation and cancellation on ``market_fx``.
    """
    assert _outbound_writers(_adapter_tree()) == {
        ("create_order", frozenset({"market_fx/orders"})),
        ("cancel_order", frozenset({"market_fx/orders/close"})),
    }


def test_no_other_module_signs_a_walutomat_request() -> None:
    """Only the adapter carries Walutomat's signed API path, so no other writer exists.

    Given: Every source module of the package.
    When: Each is scanned for the signed Walutomat API path prefix.
    Then: Only the Walutomat adapter carries it.
    """
    package_root = Path(snapper.__file__).parent
    carriers = {
        path.relative_to(package_root).as_posix()
        for path in package_root.rglob("*.py")
        if _signed_endpoint_paths(ast.parse(path.read_text(encoding="utf-8")))
    }
    assert carriers == {"infrastructure/exchanges/implementations/walutomat.py"}
