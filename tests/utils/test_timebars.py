"""Unit tests for autoload functionality."""

import importlib
import importlib.util
import types
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sequence
from importlib.abc import Loader
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec
from typing import Any
from typing import cast

import pytest

from snapper.utils import autoload


class DummyModule(types.ModuleType):
    """Mock module for testing autoload functionality."""

    def __init__(self, name: str) -> None:
        """Initialize the instance."""
        super().__init__(name)
        self.loaded = False

    def mark_loaded(self) -> None:
        """Mark this module as loaded."""
        self.loaded = True


class DummyLoader(Loader):
    """Mock loader for testing module imports."""

    def __init__(self, module: DummyModule) -> None:
        """Initialize the instance."""
        self.module = module

    def create_module(self, spec: ModuleSpec) -> types.ModuleType | None:
        """Create the module instance."""
        return self.module

    def exec_module(self, module: types.ModuleType) -> None:
        """Execute the module and mark it as loaded."""
        if isinstance(module, DummyModule):
            module.mark_loaded()


class DummyFinder(MetaPathFinder):
    """Mock meta path finder for testing module discovery."""

    def __init__(self, root: str, modules: dict[str, DummyModule]) -> None:
        """Initialize the instance."""
        self.root = root
        self.modules = modules

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: Any | None = None,
    ) -> ModuleSpec | None:
        """Find module spec for the given module name."""
        if fullname not in self.modules:
            return None
        spec = ModuleSpec(fullname, DummyLoader(self.modules[fullname]))
        if fullname == self.root:
            spec.submodule_search_locations = ["<virtual>"]
        return spec


@pytest.fixture()
def temp_modules(monkeypatch: pytest.MonkeyPatch) -> dict[str, DummyModule]:
    """Provide mocked module entries for autoload testing."""
    base = "autoload_pkg"
    entries: dict[str, DummyModule] = {
        base: DummyModule(base),
        f"{base}.keep": DummyModule(f"{base}.keep"),
        f"{base}.exclude": DummyModule(f"{base}.exclude"),
        f"{base}.skip.tests": DummyModule(f"{base}.skip.tests"),
    }
    finder = DummyFinder(base, entries)

    def fake_import(name: str) -> DummyModule:
        module = entries[name]
        module.mark_loaded()
        return module

    monkeypatch.setattr(importlib, "import_module", fake_import)
    find_spec_callable = cast(
        Callable[[str, Sequence[str] | None, Any | None], ModuleSpec | None],
        finder.find_spec,
    )
    monkeypatch.setattr(importlib.util, "find_spec", find_spec_callable)

    def fake_walk_packages(
        search: Sequence[str] | None, prefix: str = ""
    ) -> Iterator[tuple[None, str, bool]]:
        yield (None, f"{base}.keep", False)
        yield (None, f"{base}.exclude", False)
        yield (None, f"{base}.skip.tests", False)

    monkeypatch.setattr(importlib, "invalidate_caches", lambda: None)
    monkeypatch.setattr("snapper.utils.autoload.pkgutil.walk_packages", fake_walk_packages)
    return entries


def test_walk_module_names_respects_exclusions(temp_modules: dict[str, DummyModule]) -> None:
    """Test _walk_module_names respects exclusion patterns.

    Given: module hierarchy with 'keep', 'exclude', 'skip' modules,
    When: walking with exclusions ('exclude', 'skip'),
    Then: only 'keep' module returned.
    """
    walk_modules = cast(
        Callable[[str, Sequence[str]], list[str]],
        autoload.__dict__["_walk_module_names"],
    )
    modules = walk_modules("autoload_pkg", ("exclude", "skip"))
    assert "autoload_pkg.keep" in modules
    assert "autoload_pkg.exclude" not in modules
    assert "autoload_pkg.skip.tests" not in modules


def test_import_all_under_warns_on_error(
    monkeypatch: pytest.MonkeyPatch, temp_modules: dict[str, DummyModule]
) -> None:
    """Test import_all_under warns on import error.

    Given: fake_import that raises RuntimeError for 'exclude',
    When: calling import_all_under with on_error='warn',
    Then: prints warning and continues importing others.
    """
    import_calls: list[str] = []

    def fake_import(name: str) -> DummyModule:
        import_calls.append(name)
        if name.endswith("exclude"):
            raise RuntimeError("boom")
        module = temp_modules[name]
        module.mark_loaded()
        return module

    monkeypatch.setattr(importlib, "import_module", fake_import)
    printed: list[str] = []

    def fake_print(msg: str) -> None:
        printed.append(msg)

    monkeypatch.setattr("builtins.print", fake_print)
    imported_count = autoload.import_all_under("autoload_pkg", exclude_parts=(), on_error="warn")
    assert imported_count == 2
    assert any("Failed to import autoload_pkg.exclude" in entry for entry in printed)


def test_import_all_under_returns_zero_for_missing_package() -> None:
    """Test import_all_under returns 0 for missing package.

    Given: nonexistent package name,
    When: calling import_all_under,
    Then: returns 0 imports.
    """
    missing_root = "autoload_nonexistent_package_for_test"
    imported_count = autoload.import_all_under(missing_root)
    assert imported_count == 0


def test_import_all_under_imports_modules(temp_modules: dict[str, DummyModule]) -> None:
    """Test import_all_under imports all modules.

    Given: temp_modules fixture with 'keep' and 'exclude',
    When: calling import_all_under without exclusions,
    Then: both modules loaded.
    """
    imported_count = autoload.import_all_under("autoload_pkg")
    assert imported_count == 2
    assert temp_modules["autoload_pkg.keep"].loaded is True
    assert temp_modules["autoload_pkg.exclude"].loaded is True


def test_import_all_under_ignore_errors(
    monkeypatch: pytest.MonkeyPatch, temp_modules: dict[str, DummyModule]
) -> None:
    """Test import_all_under with on_error='ignore'.

    Given: fake_import that raises for 'exclude',
    When: calling import_all_under with on_error='ignore',
    Then: no warning printed, continues silently.
    """
    import_calls: list[str] = []

    def fake_import(name: str) -> DummyModule:
        import_calls.append(name)
        if name.endswith("exclude"):
            raise RuntimeError("boom")
        module = temp_modules[name]
        module.mark_loaded()
        return module

    monkeypatch.setattr(importlib, "import_module", fake_import)
    printed: list[str] = []

    def fake_print(msg: str) -> None:
        printed.append(msg)

    monkeypatch.setattr("builtins.print", fake_print)
    imported_count = autoload.import_all_under("autoload_pkg", exclude_parts=(), on_error="ignore")
    assert imported_count == 2
    assert len(printed) == 0


def test_import_all_under_raise_errors(
    monkeypatch: pytest.MonkeyPatch, temp_modules: dict[str, DummyModule]
) -> None:
    """Test import_all_under with on_error='raise'.

    Given: fake_import that raises for 'exclude',
    When: calling import_all_under with on_error='raise',
    Then: RuntimeError propagated.
    """
    import_calls: list[str] = []

    def fake_import(name: str) -> DummyModule:
        import_calls.append(name)
        if name.endswith("exclude"):
            raise RuntimeError("boom")
        module = temp_modules[name]
        module.mark_loaded()
        return module

    monkeypatch.setattr(importlib, "import_module", fake_import)
    with pytest.raises(RuntimeError, match="boom"):
        autoload.import_all_under("autoload_pkg", exclude_parts=(), on_error="raise")


def test_walk_module_names_returns_empty_for_missing_package() -> None:
    """Test _walk_module_names returns empty for missing package.

    Given: nonexistent package name,
    When: calling _walk_module_names,
    Then: returns empty list.
    """
    walk_modules = cast(
        Callable[[str, Sequence[str]], list[str]],
        autoload.__dict__["_walk_module_names"],
    )
    modules = walk_modules("nonexistent_package_xyz", ())
    assert modules == []
