"""Meta-audit — the TradFi cross-asset reference strategy must not auto-register.

The module ``snapper.strategies.examples.tradfi_observe_crypto_execute``
ships as an illustration of the cross-asset pattern (observe a
market-data-only TradFi instrument, execute on a crypto instrument).
It MUST stay out of the process registry — otherwise the "copy + add
decorators to activate" contract in its module docstring becomes
misleading and an unattended runtime might launch a reference
implementation without operator intent.

This test:

1. Imports the module (force-loads its module-level code so decorator
   side effects would fire if accidentally added).
2. Asserts the class name is absent from ``get_registered_processes``.
3. AST-scans the file for ``@register_strategy`` + ``@create_strategy_process``
   decorators and fails if any appear.
"""

import ast
from pathlib import Path

from snapper.application.process_manager.registry import get_registered_processes
from snapper.strategies.examples import tradfi_observe_crypto_execute
from snapper.strategies.factory import StrategyFactory

_REFERENCE_MODULE_PATH = Path(tradfi_observe_crypto_execute.__file__).resolve()
_FORBIDDEN_DECORATORS: frozenset[str] = frozenset(
    {"register_strategy", "create_strategy_process", "register_process"}
)


def test_reference_strategy_not_in_process_registry() -> None:
    """No process registry entry resolves back to the reference module.

    Given: ``snapper.strategies.examples.tradfi_observe_crypto_execute`` imported,
    When: ``get_registered_processes`` is inspected,
    Then: no registry entry's class resolves to that module path. Registry
        keys are process-name strings (not class names), so the real
        guardrail is the module-path round-trip, not the class-name check.
        Any escape of ``@register_process``/``@create_strategy_process``
        into the example module would surface as an entry whose
        ``class_type.__module__`` equals the reference module.
    """
    registry = get_registered_processes()
    module_path = tradfi_observe_crypto_execute.__name__
    offenders = {
        name: entry.class_ref.__module__
        for name, entry in registry.items()
        if entry.class_ref.__module__ == module_path
    }
    assert (
        not offenders
    ), f"reference module {module_path!r} leaked into the process registry: " + ", ".join(
        f"{name}={mod}" for name, mod in sorted(offenders.items())
    )


def test_reference_module_has_no_registry_decorators() -> None:
    """AST scan — no ``@register_strategy`` / ``@create_strategy_process``.

    Given: the reference module on disk,
    When: its AST is walked,
    Then: no decorator resolves to any of the forbidden registry names.
        Prevents the illustration from silently becoming a real process
        if a future maintainer re-adds decorators.
    """
    tree = ast.parse(_REFERENCE_MODULE_PATH.read_text(encoding="utf-8"))
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            decorator_name = _decorator_name(decorator)
            if decorator_name in _FORBIDDEN_DECORATORS:
                offenders.append(f"{node.name}@{decorator_name}")
    assert (
        not offenders
    ), "reference strategy is decorated with forbidden registry names: " + ", ".join(offenders)


def _decorator_name(decorator: ast.expr) -> str | None:
    """Extract the decorator's bare name (strips arguments, attribute chain)."""
    if isinstance(decorator, ast.Call):
        return _decorator_name(decorator.func)
    if isinstance(decorator, ast.Name):
        return decorator.id
    if isinstance(decorator, ast.Attribute):
        return decorator.attr
    return None


def test_reference_class_not_in_strategy_factory() -> None:
    """The reference class must not land in ``StrategyFactory.STRATEGY_CLASSES``.

    Given: the reference module imported,
    When: ``StrategyFactory.STRATEGY_CLASSES`` is inspected,
    Then: no entry resolves to the reference module. Catches the
        alias-bypass the AST scan can miss: even if a future maintainer
        writes ``from ... import register_strategy as rs`` and
        decorates the class with ``@rs(...)``, the factory side-effect
        would still populate this dict, and this test would fail.
    """
    module_path = tradfi_observe_crypto_execute.__name__
    offenders = {
        name: cls.__module__
        for name, cls in StrategyFactory.STRATEGY_CLASSES.items()
        if cls.__module__ == module_path
    }
    assert (
        not offenders
    ), f"reference module {module_path!r} leaked into StrategyFactory: " + ", ".join(
        f"{name}={mod}" for name, mod in sorted(offenders.items())
    )
