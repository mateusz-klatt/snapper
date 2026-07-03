"""Tests for template-based class resolution and extra-package discovery (P4).

Configs created FROM a registered template persist a function-local
wrapper ``class_path`` that can never be imported dynamically. The
resolution contract (resolver → spawner validation/command → runner)
falls back to the template's registry entry, and process discovery can
import extra out-of-tree packages fail-soft.
"""

import json
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.process_manager.config_resolver import import_process_class
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.launcher import _copy_process_config_with_mode
from snapper.application.process_manager.launcher import _copy_process_config_with_parameters
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.registry import discover_processes
from snapper.application.process_manager.spawner import ProcessSpawnerService
from snapper.application.process_manager.spawner import _build_process_command
from snapper.core.types import ProcessModeEnum
from snapper.server.process_runner import _resolve_process_class

_LOCALS_PATH = "snapper.strategies.process_wrapper.create_strategy_process.<locals>.StrategyProcess"


class _TemplateClass:
    """Stand-in registered template class."""


def _fake_registry() -> dict[str, MagicMock]:
    """Build a registry carrying one template entry."""
    entry = MagicMock()
    entry.class_ref = _TemplateClass
    return {"strategy_heartbeat_consult_btc_1h": entry}


class TestImportProcessClassTemplateFallback:
    """Resolver order: process_name -> template_name -> dynamic import."""

    def test_unimportable_locals_path_resolves_via_template(self) -> None:
        """Verify the template registry entry rescues a <locals> class_path.

        Given: A config name absent from the registry but a registered
            template, and a function-local class_path,
        When: import_process_class runs,
        Then: The template's class resolves (no dynamic import attempt).
        """
        with patch(
            "snapper.application.process_manager.config_resolver.get_registered_processes",
            return_value=_fake_registry(),
        ):
            resolved = import_process_class(
                _LOCALS_PATH,
                process_name="strategy_heartbeat_e2e",
                template_name="strategy_heartbeat_consult_btc_1h",
            )
        assert resolved is _TemplateClass

    def test_process_name_still_wins_over_template(self) -> None:
        """Verify a registry-native name resolves before the template.

        Given: Both the process name and a template registered,
        When: import_process_class runs,
        Then: The process-name entry wins.
        """

        class _NativeClass:
            """Registry-native class."""

        registry = _fake_registry()
        native = MagicMock()
        native.class_ref = _NativeClass
        registry["native_process"] = native
        with patch(
            "snapper.application.process_manager.config_resolver.get_registered_processes",
            return_value=registry,
        ):
            resolved = import_process_class(
                _LOCALS_PATH,
                process_name="native_process",
                template_name="strategy_heartbeat_consult_btc_1h",
            )
        assert resolved is _NativeClass

    def test_unimportable_path_without_template_raises(self) -> None:
        """Verify the legacy failure stays for template-less configs.

        Given: No registry match and a function-local class_path,
        When: import_process_class runs,
        Then: ImportError raises.
        """
        with (
            patch(
                "snapper.application.process_manager.config_resolver.get_registered_processes",
                return_value={},
            ),
            pytest.raises(ImportError),
        ):
            import_process_class(_LOCALS_PATH, process_name="strategy_orphan")


class TestCopyHelpersPreserveTemplate:
    """Config copies (scope resolution, mode force) must not drop template."""

    def test_parameter_copy_preserves_template(self) -> None:
        """Verify the wallet-scope parameter copy keeps the template.

        Given: A template-backed strategy config entering scope resolution,
        When: The parameters-replacing copy runs,
        Then: template survives (else autostart would lose registry
            class resolution and fail the function-local import).
        """
        config = ProcessConfigModel(
            name="strategy_heartbeat_e2e",
            enabled=True,
            mode="thread",
            class_path=_LOCALS_PATH,
            method="start",
            parameters={},
            template="strategy_heartbeat_consult_btc_1h",
        )
        copied = _copy_process_config_with_parameters(config, {"wallet_public_id": "w"})
        assert copied.template == "strategy_heartbeat_consult_btc_1h"

    def test_mode_copy_preserves_template(self) -> None:
        """Verify the PROCESS-mode force copy keeps the template.

        Given: A template-backed config forced to PROCESS mode,
        When: The mode-replacing copy runs,
        Then: template survives for the spawner/runner resolution.
        """
        config = ProcessConfigModel(
            name="strategy_heartbeat_e2e",
            enabled=True,
            mode="thread",
            class_path=_LOCALS_PATH,
            method="start",
            parameters={},
            template="strategy_heartbeat_consult_btc_1h",
        )
        copied = _copy_process_config_with_mode(config, ProcessModeEnum.PROCESS)
        assert copied.template == "strategy_heartbeat_consult_btc_1h"


class TestSpawnerTemplateSupport:
    """Spawner validation and command building carry the template."""

    def test_validate_accepts_registered_template(self) -> None:
        """Verify validation short-circuits for a registered template.

        Given: An unimportable class_path but a registered template,
        When: _validate_class_path runs,
        Then: No error raises (the runner resolves via the registry).
        """
        spawner = ProcessSpawnerService()
        with patch(
            "snapper.application.process_manager.spawner.get_registered_processes",
            return_value=_fake_registry(),
        ):
            spawner._validate_class_path(
                "strategy_heartbeat_e2e", _LOCALS_PATH, "strategy_heartbeat_consult_btc_1h"
            )

    def test_validate_rejects_unimportable_without_template(self) -> None:
        """Verify template-less validation keeps failing fast.

        Given: An unimportable class_path and no template,
        When: _validate_class_path runs,
        Then: RuntimeError raises.
        """
        spawner = ProcessSpawnerService()
        with pytest.raises(RuntimeError, match="invalid class"):
            spawner._validate_class_path("strategy_orphan", _LOCALS_PATH, None)

    def test_command_json_carries_template_name(self) -> None:
        """Verify the runner config JSON includes template_name.

        Given: A spawn command built with a template,
        When: The --config payload is parsed,
        Then: template_name rides along for the runner's resolution.
        """
        cmd = _build_process_command(
            "strategy_heartbeat_e2e",
            _LOCALS_PATH,
            "start",
            {},
            "strategy_heartbeat_consult_btc_1h",
        )
        config = json.loads(cmd[cmd.index("--config") + 1])
        assert config["template_name"] == "strategy_heartbeat_consult_btc_1h"


class TestRunnerTemplateResolution:
    """process_runner resolves the class via the registry template."""

    def test_runner_resolves_via_template(self) -> None:
        """Verify the runner takes the registry path for template configs.

        Given: A registered template and an unimportable class_path,
        When: _resolve_process_class runs,
        Then: The template class resolves and discovery ran first.
        """
        with (
            patch("snapper.server.process_runner.discover_processes") as mock_discover,
            patch(
                "snapper.server.process_runner.get_registered_processes",
                return_value=_fake_registry(),
            ),
        ):
            resolved = _resolve_process_class(
                _LOCALS_PATH, "strategy_heartbeat_e2e", "strategy_heartbeat_consult_btc_1h"
            )
        assert resolved is _TemplateClass
        mock_discover.assert_called_once()

    def test_runner_falls_back_to_dynamic_import(self) -> None:
        """Verify a plain importable class_path skips the registry.

        Given: No template,
        When: _resolve_process_class runs on a real class path,
        Then: The dynamic import resolves it.
        """
        resolved = _resolve_process_class(
            "snapper.application.process_manager.spawner.ProcessSpawnerService",
            "spawner",
            None,
        )
        assert resolved.__name__ == "ProcessSpawnerService"
        assert resolved.__module__ == "snapper.application.process_manager.spawner"


class TestExtraPackageDiscovery:
    """STRATEGY_EXTRA_PACKAGES imports out-of-tree modules fail-soft."""

    def test_extra_package_imported_by_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify a mounted extra package imports and resolves by PATH.

        Given: A synthetic top-level package on sys.path listed in
            STRATEGY_EXTRA_PACKAGES,
        When: discover_processes runs,
        Then: The package's module imports from the mounted path (no
            same-named package elsewhere can shadow it silently).
        """
        pkg_dir = tmp_path / "xstrategies"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text('"""Synthetic strategy package."""\n')
        (pkg_dir / "probe.py").write_text('"""Probe module."""\nPROBE = 1\n')
        monkeypatch.syspath_prepend(str(tmp_path))
        fake_bootstrap = MagicMock()
        fake_bootstrap.strategy_extra_packages = "xstrategies"
        with patch(
            "snapper.application.process_manager.registry.get_bootstrap_settings",
            return_value=fake_bootstrap,
        ):
            discover_processes()
        probe = sys.modules["xstrategies.probe"]
        assert isinstance(probe, types.ModuleType)
        assert probe.__file__ is not None
        assert probe.__file__.startswith(str(pkg_dir))

    def test_missing_extra_package_is_fail_soft(self) -> None:
        """Verify a broken package mount cannot abort discovery.

        Given: STRATEGY_EXTRA_PACKAGES naming a package whose import
            raises,
        When: discover_processes runs,
        Then: No exception propagates (OSS deployments unaffected).
        """
        fake_bootstrap = MagicMock()
        fake_bootstrap.strategy_extra_packages = "package_that_does_not_exist"

        def _explode(package: str) -> int:
            if package == "package_that_does_not_exist":
                raise ModuleNotFoundError(package)
            return 0

        with (
            patch(
                "snapper.application.process_manager.registry.get_bootstrap_settings",
                return_value=fake_bootstrap,
            ),
            patch(
                "snapper.application.process_manager.registry.import_all_under",
                side_effect=_explode,
            ),
        ):
            discover_processes()


class TestLauncherSummaryWrapper:
    """The public snapshot wrapper delegates to the private emitter."""

    @pytest.mark.asyncio
    async def test_wrapper_delegates(self) -> None:
        """Verify emit_summary_snapshot calls the private emitter once.

        Given: A launcher with a recorded private emitter,
        When: The public wrapper runs,
        Then: The private emitter is awaited exactly once.
        """
        launcher = ProcessLauncherService(MagicMock())
        private = AsyncMock()
        launcher._emit_summary_snapshot = private
        await launcher.emit_summary_snapshot()
        private.assert_awaited_once()


class TestRunnerResolutionBranches:
    """Runner fallthrough branches for unusual registry/import shapes."""

    def test_template_missing_from_registry_falls_to_dynamic(self) -> None:
        """Verify an unregistered template falls through to dynamic import.

        Given: A template name absent from the registry and an importable
            class_path,
        When: _resolve_process_class runs,
        Then: The dynamic import resolves the class.
        """
        with (
            patch("snapper.server.process_runner.discover_processes"),
            patch(
                "snapper.server.process_runner.get_registered_processes",
                return_value={},
            ),
        ):
            resolved = _resolve_process_class(
                "snapper.application.process_manager.spawner.ProcessSpawnerService",
                "spawner",
                "template_not_registered",
            )
        assert resolved.__name__ == "ProcessSpawnerService"

    def test_dynamic_non_class_raises_import_error(self) -> None:
        """Verify a non-class dynamic attribute raises ImportError.

        Given: A class_path resolving to a module attribute that is not
            a class,
        When: _resolve_process_class runs,
        Then: ImportError raises.
        """
        with pytest.raises(ImportError, match="is not a class"):
            _resolve_process_class(
                "snapper.application.process_manager.spawner._build_process_command",
                "not_a_class",
                None,
            )
