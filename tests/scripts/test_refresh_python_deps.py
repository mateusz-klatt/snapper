"""Tests for the dependency-graph-derived constraint refresh.

The point of the script is that it names no library: it discovers which of our
direct dependencies another package pins exactly and holds only those back. These
tests pin that derivation — including the real shape that motivated it (ccxt
pinning aiohttp) — plus the classification of every constraint form Poetry can
write, so a range is never mistaken for an exact pin and silently frozen.
"""

from pathlib import Path
from typing import Final
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from scripts.refresh_python_deps import _default_root
from scripts.refresh_python_deps import build_command
from scripts.refresh_python_deps import direct_dependency_names
from scripts.refresh_python_deps import exactly_pinned_dependencies
from scripts.refresh_python_deps import main

_PROJECT: Final = """
[tool.poetry.dependencies]
python = "^3.14"
aiohttp = "^3.14.1"
ccxt = "^4.5.67"
loguru = "^0.7.0"

[tool.poetry.group.dev.dependencies]
pytest = "^9.0.0"
"""

_LOCK: Final = """
[[package]]
name = "ccxt"
version = "4.5.67"

[package.dependencies]
aiohttp = "3.14.1"
certifi = ">=2018.1.18"

[[package]]
name = "loguru"
version = "0.7.0"
"""


class TestDirectDependencyNames:
    """Cover reading our own declared dependencies."""

    def test_collects_main_and_group_dependencies(self) -> None:
        """Both the main table and optional groups contribute names."""
        assert direct_dependency_names(_PROJECT) == {"aiohttp", "ccxt", "loguru", "pytest"}

    def test_python_marker_is_not_a_dependency(self) -> None:
        """The interpreter constraint is never a package to bump."""
        assert "python" not in direct_dependency_names(_PROJECT)

    def test_missing_tables_yield_no_names(self) -> None:
        """A project without dependency tables produces an empty set."""
        assert direct_dependency_names("[tool.poetry]\nname = 'x'\n") == set()

    def test_malformed_group_entries_are_skipped(self) -> None:
        """A group that is not a table cannot contribute names."""
        text = _PROJECT + '\n[tool.poetry.group]\nbroken = "not-a-table"\n'
        assert "broken" not in direct_dependency_names(text)


class TestExactlyPinnedDependencies:
    """Cover the derivation that decides what must not be bumped."""

    def test_detects_the_real_ccxt_aiohttp_pin(self) -> None:
        """A package pinning one of our direct deps exactly holds only that one back.

        This is the shape that broke `make py-refresh`: raising aiohttp's floor
        past ccxt's exact requirement made the solver unsatisfiable.
        """
        direct = direct_dependency_names(_PROJECT)
        assert exactly_pinned_dependencies(_LOCK, direct) == ["aiohttp"]

    def test_range_constraints_are_not_treated_as_pins(self) -> None:
        """A ranged requirement stays bumpable; only exact ones are held."""
        direct = {"certifi"}
        assert exactly_pinned_dependencies(_LOCK, direct) == []

    def test_transitive_only_packages_are_ignored(self) -> None:
        """A pinned package we do not declare directly is not our concern."""
        assert exactly_pinned_dependencies(_LOCK, set()) == []

    def test_equality_operator_counts_as_exact(self) -> None:
        """Poetry's ``==`` spelling is recognised alongside the bare form."""
        lock = '[[package]]\nname = "a"\n\n[package.dependencies]\nb = "==1.0"\n'
        assert exactly_pinned_dependencies(lock, {"b"}) == ["b"]

    @pytest.mark.parametrize(
        "constraint",
        [
            pytest.param(">=1.0", id="lower_bound"),
            pytest.param("^1.0", id="caret"),
            pytest.param("~1.0", id="tilde"),
            pytest.param("*", id="wildcard"),
            pytest.param(">=1.0,<2.0", id="range"),
            pytest.param("", id="empty"),
        ],
    )
    def test_flexible_constraints_are_never_held(self, constraint: str) -> None:
        """Every non-exact spelling stays eligible for a bump."""
        lock = f'[[package]]\nname = "a"\n\n[package.dependencies]\nb = "{constraint}"\n'
        assert exactly_pinned_dependencies(lock, {"b"}) == []

    def test_structured_requirements_are_not_pins(self) -> None:
        """A table or list requirement carries markers and is left bumpable."""
        lock = (
            '[[package]]\nname = "a"\n\n[package.dependencies]\n'
            'b = {version = "1.0", markers = "sys_platform == \'linux\'"}\n'
        )
        assert exactly_pinned_dependencies(lock, {"b"}) == []

    def test_name_normalization_matches_across_spellings(self) -> None:
        """Underscore and case differences still resolve to the same package."""
        lock = '[[package]]\nname = "a"\n\n[package.dependencies]\nMy_Pkg = "1.0"\n'
        assert exactly_pinned_dependencies(lock, {"my-pkg"}) == ["my-pkg"]

    def test_packages_without_dependency_tables_are_skipped(self) -> None:
        """A lock entry with no dependencies contributes nothing."""
        assert exactly_pinned_dependencies('[[package]]\nname = "solo"\n', {"solo"}) == []

    def test_malformed_package_list_yields_nothing(self) -> None:
        """A lock whose package entry is not a table cannot pin anything."""
        assert exactly_pinned_dependencies('package = "not-a-list"\n', {"b"}) == []


class TestBuildCommand:
    """Cover the constructed Poetry invocation."""

    def test_exclusions_become_repeated_flags(self) -> None:
        """Each held package contributes its own ``--exclude`` pair."""
        command = build_command(["aiohttp", "urllib3"])
        assert command[-4:] == ["--exclude", "aiohttp", "--exclude", "urllib3"]
        assert "--latest" in command

    def test_no_exclusions_bumps_everything(self) -> None:
        """With nothing pinned upstream the bump runs unrestricted."""
        assert "--exclude" not in build_command([])


class TestMain:
    """Cover the entry point's wiring and tolerance."""

    def _write(self, root: Path) -> None:
        """Seed a project and lock exhibiting the ccxt/aiohttp pin."""
        (root / "pyproject.toml").write_text(_PROJECT, encoding="utf-8")
        (root / "poetry.lock").write_text(_LOCK, encoding="utf-8")

    def test_derived_exclusion_reaches_the_command(self, tmp_path: Path) -> None:
        """The discovered pin is passed to Poetry without being hard-coded."""
        self._write(tmp_path)
        run = MagicMock(return_value=MagicMock(returncode=0))
        with (
            patch("scripts.refresh_python_deps._default_root", return_value=tmp_path),
            patch("scripts.refresh_python_deps.subprocess.run", run),
        ):
            assert main() == 0
        assert run.call_args.args[0][-2:] == ["--exclude", "aiohttp"]

    def test_missing_files_are_a_no_op(self, tmp_path: Path) -> None:
        """Without a lock or project file the step does nothing and succeeds."""
        run = MagicMock()
        with (
            patch("scripts.refresh_python_deps._default_root", return_value=tmp_path),
            patch("scripts.refresh_python_deps.subprocess.run", run),
        ):
            assert main() == 0
        run.assert_not_called()

    def test_bump_failure_does_not_fail_the_refresh(self, tmp_path: Path) -> None:
        """A non-zero bump is reported but stays advisory.

        The constraint bump is best-effort; the lock refresh that follows is the
        step that must succeed, so a failure here must not abort the target.
        """
        self._write(tmp_path)
        run = MagicMock(return_value=MagicMock(returncode=1))
        with (
            patch("scripts.refresh_python_deps._default_root", return_value=tmp_path),
            patch("scripts.refresh_python_deps.subprocess.run", run),
        ):
            assert main() == 0


class TestUncoveredEdges:
    """Cover the remaining structural branches."""

    def test_default_root_is_the_repository_root(self) -> None:
        """The script locates the repo as its own parent directory."""
        root = _default_root()
        assert (root / "scripts" / "refresh_python_deps.py").is_file()

    def test_non_table_package_entries_are_skipped(self) -> None:
        """A package list holding a bare value is skipped, not crashed on."""
        assert exactly_pinned_dependencies('package = ["not-a-table"]\n', {"b"}) == []

    def test_group_without_dependency_table_is_skipped(self) -> None:
        """A declared group carrying no dependencies table contributes nothing."""
        text = _PROJECT + "\n[tool.poetry.group.empty]\noptional = true\n"
        assert direct_dependency_names(text) == {"aiohttp", "ccxt", "loguru", "pytest"}

    def test_runs_unrestricted_when_nothing_is_pinned(self, tmp_path: Path) -> None:
        """With no upstream exact pin the bump runs with no exclusions at all."""
        (tmp_path / "pyproject.toml").write_text(_PROJECT, encoding="utf-8")
        (tmp_path / "poetry.lock").write_text('[[package]]\nname = "solo"\n', encoding="utf-8")
        run = MagicMock(return_value=MagicMock(returncode=0))
        with (
            patch("scripts.refresh_python_deps._default_root", return_value=tmp_path),
            patch("scripts.refresh_python_deps.subprocess.run", run),
        ):
            assert main() == 0
        assert "--exclude" not in run.call_args.args[0]
