"""Module autoloading utilities for dynamic import.

This module provides utilities for recursively importing all submodules
under a given package. Useful for:
    - Loading all strategy classes for registration
    - Ensuring all decorated functions are executed
    - Testing import integrity across the codebase

Example:
    Import all modules under snapper.strategies::

        from snapper.utils.autoload import import_all_under

        imported_count = import_all_under("snapper.strategies")
        print(f"Imported {imported_count} modules")
"""

import importlib
import importlib.util
import pkgutil
import sys
from collections.abc import Sequence


def _walk_module_names(
    root: str, exclude_parts: Sequence[str] = ("tests", "testing", "__pycache__", "_test")
) -> list[str]:
    """Recursively find all module names under a root package.

    Args:
        root: Root package name (e.g., 'snapper.strategies').
        exclude_parts: Path components to exclude from search.

    Returns:
        List of fully qualified module names.
    """
    spec = importlib.util.find_spec(root)
    if not spec or not spec.submodule_search_locations:
        return []
    modules: list[str] = []
    for _, name, _ in pkgutil.walk_packages(spec.submodule_search_locations, prefix=f"{root}."):
        parts = name.split(".")
        if any(excluded in parts for excluded in exclude_parts):
            continue
        modules.append(name)
    return modules


def import_all_under(
    root: str,
    exclude_parts: Sequence[str] = ("tests", "testing", "__pycache__", "_test"),
    on_error: str = "warn",
) -> int:
    """Import all submodules under a root package.

    Args:
        root: Root package name to import from.
        exclude_parts: Path components to exclude.
        on_error: Error handling mode:
            - 'raise': Re-raise import errors
            - 'warn': Print warning and continue
            - 'ignore': Silently skip failed imports

    Returns:
        Number of newly imported modules.
    """
    modules = _walk_module_names(root, exclude_parts)
    imported = 0
    for module_name in modules:
        if module_name in sys.modules:
            continue
        try:
            importlib.import_module(module_name)
            imported += 1
        except Exception as e:
            if on_error == "raise":
                raise
            if on_error == "warn":
                print(f"[autoload] Warning: Failed to import {module_name}: {e}")
    return imported
