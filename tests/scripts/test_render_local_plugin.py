"""Tests for the local Snapper MCP plugin renderer."""

import json
from pathlib import Path

import pytest

from scripts.render_local_plugin import CLAUDE_PLUGIN_SUBDIR
from scripts.render_local_plugin import MARKETPLACE_KEY
from scripts.render_local_plugin import PLACEHOLDER
from scripts.render_local_plugin import PLUGIN_DIR_NAME
from scripts.render_local_plugin import _default_repo_root
from scripts.render_local_plugin import _default_settings_path
from scripts.render_local_plugin import _replace_placeholder
from scripts.render_local_plugin import main
from scripts.render_local_plugin import render_plugin
from scripts.render_local_plugin import update_claude_settings


def _write_template(repo_root: Path, name: str, content: str) -> Path:
    """Write a templated JSON file under the integrations plugin directory."""
    template_dir = repo_root / "integrations" / PLUGIN_DIR_NAME / CLAUDE_PLUGIN_SUBDIR
    template_dir.mkdir(parents=True, exist_ok=True)
    target = template_dir / name
    target.write_text(content, encoding="utf-8")
    return target


def _claude_settings_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Configure a temporary Claude home and return its settings path."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    settings = tmp_path / ".claude" / "settings.json"
    settings.parent.mkdir()
    return settings


class TestRenderPlugin:
    """Coverage for ``render_plugin`` placeholder substitution."""

    def test_renders_placeholder_to_repo_root(self, tmp_path: Path) -> None:
        """Every occurrence of the placeholder is replaced with the repo root."""
        _write_template(
            tmp_path,
            "plugin.json",
            json.dumps(
                {
                    "args": [f"{PLACEHOLDER}/integrations/snapper-mcp/dist/index.js"],
                    "enabled": True,
                    "retries": 1,
                }
            ),
        )

        plugin_dir = render_plugin(tmp_path)

        rendered_path = plugin_dir / CLAUDE_PLUGIN_SUBDIR / "plugin.json"
        rendered = json.loads(rendered_path.read_text(encoding="utf-8"))
        assert rendered == {
            "args": [f"{tmp_path}/integrations/snapper-mcp/dist/index.js"],
            "enabled": True,
            "retries": 1,
        }
        assert plugin_dir == tmp_path / "data" / PLUGIN_DIR_NAME

    def test_placeholder_replacement_preserves_windows_paths_as_json(self) -> None:
        """Windows-style replacements remain valid JSON after rendering."""
        rendered = _replace_placeholder(
            {"args": [f"{PLACEHOLDER}\\integrations\\snapper-mcp\\dist\\index.js"]},
            r"C:\projects\snapper",
        )
        encoded = json.dumps(rendered)

        assert json.loads(encoded) == {
            "args": [r"C:\projects\snapper\integrations\snapper-mcp\dist\index.js"]
        }

    def test_renders_all_json_files_in_template_dir(self, tmp_path: Path) -> None:
        """Every JSON template in the source dir is rendered, non-JSON files are skipped."""
        _write_template(tmp_path, "plugin.json", json.dumps({"path": PLACEHOLDER}))
        _write_template(tmp_path, "marketplace.json", json.dumps({"version": "0.0.0-local"}))
        non_json = tmp_path / "integrations" / PLUGIN_DIR_NAME / CLAUDE_PLUGIN_SUBDIR / "README.md"
        non_json.write_text("# not a template", encoding="utf-8")

        plugin_dir = render_plugin(tmp_path)

        rendered_files = sorted(p.name for p in (plugin_dir / CLAUDE_PLUGIN_SUBDIR).iterdir())
        assert rendered_files == ["marketplace.json", "plugin.json"]

    def test_render_is_idempotent(self, tmp_path: Path) -> None:
        """Re-running render against the same templates yields identical output."""
        _write_template(tmp_path, "plugin.json", json.dumps({"path": PLACEHOLDER}))

        first = render_plugin(tmp_path)
        first_content = (first / CLAUDE_PLUGIN_SUBDIR / "plugin.json").read_text(encoding="utf-8")

        second = render_plugin(tmp_path)
        second_content = (second / CLAUDE_PLUGIN_SUBDIR / "plugin.json").read_text(encoding="utf-8")

        assert first == second
        assert first_content == second_content


class TestUpdateClaudeSettings:
    """Coverage for ``update_claude_settings`` patching with backup."""

    def test_raises_when_settings_missing(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A missing settings file fails loudly so misconfiguration surfaces."""
        missing = _claude_settings_path(monkeypatch, tmp_path)

        with pytest.raises(FileNotFoundError, match="Claude Code settings not found"):
            update_claude_settings(missing, tmp_path / "plugin")

    def test_rejects_settings_path_outside_claude_home(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Only the configured Claude settings file can be patched."""
        _claude_settings_path(monkeypatch, tmp_path)
        settings = tmp_path / "other" / ".claude" / "settings.json"
        settings.parent.mkdir(parents=True)
        settings.write_text(json.dumps({}), encoding="utf-8")

        with pytest.raises(ValueError, match="must stay under"):
            update_claude_settings(settings, tmp_path / "plugin")

    def test_rejects_wrong_settings_filename(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The patcher is limited to the canonical Claude settings filename."""
        _claude_settings_path(monkeypatch, tmp_path)
        settings = tmp_path / ".claude" / "settings.local.json"
        settings.write_text(json.dumps({}), encoding="utf-8")

        with pytest.raises(ValueError, match="must be named settings.json"):
            update_claude_settings(settings, tmp_path / "plugin")

    def test_rejects_settings_directory(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The settings target must be a regular JSON file."""
        settings = _claude_settings_path(monkeypatch, tmp_path)
        settings.mkdir()

        with pytest.raises(ValueError, match="regular file"):
            update_claude_settings(settings, tmp_path / "plugin")

    def test_rejects_non_object_settings(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The root settings payload must be a JSON object."""
        settings = _claude_settings_path(monkeypatch, tmp_path)
        settings.write_text(json.dumps([]), encoding="utf-8")

        with pytest.raises(ValueError, match="Claude settings must be a JSON object"):
            update_claude_settings(settings, tmp_path / "plugin")

    def test_rejects_non_object_nested_settings(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Nested marketplace settings must remain JSON objects."""
        settings = _claude_settings_path(monkeypatch, tmp_path)
        settings.write_text(json.dumps({"extraKnownMarketplaces": []}), encoding="utf-8")

        with pytest.raises(ValueError, match="extraKnownMarketplaces"):
            update_claude_settings(settings, tmp_path / "plugin")

    def test_creates_marketplace_entry_when_absent(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A settings file without the entry gains a fresh ``directory`` source."""
        settings = _claude_settings_path(monkeypatch, tmp_path)
        settings.write_text(json.dumps({"permissions": {"allow": []}}), encoding="utf-8")
        plugin_dir = tmp_path / "data" / PLUGIN_DIR_NAME

        modified = update_claude_settings(settings, plugin_dir)

        assert modified is True
        data = json.loads(settings.read_text(encoding="utf-8"))
        entry = data["extraKnownMarketplaces"][MARKETPLACE_KEY]
        assert entry["source"] == {"source": "directory", "path": str(plugin_dir)}
        backup = settings.with_suffix(settings.suffix + ".bak")
        assert backup.exists()
        assert json.loads(backup.read_text(encoding="utf-8")) == {"permissions": {"allow": []}}

    def test_updates_existing_path(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """An existing entry with a different path is overwritten and backed up."""
        settings = _claude_settings_path(monkeypatch, tmp_path)
        original = {
            "extraKnownMarketplaces": {
                MARKETPLACE_KEY: {
                    "source": {"source": "directory", "path": "/old/path"},
                },
            },
        }
        settings.write_text(json.dumps(original), encoding="utf-8")
        plugin_dir = tmp_path / "data" / PLUGIN_DIR_NAME

        modified = update_claude_settings(settings, plugin_dir)

        assert modified is True
        data = json.loads(settings.read_text(encoding="utf-8"))
        assert data["extraKnownMarketplaces"][MARKETPLACE_KEY]["source"]["path"] == str(plugin_dir)

    def test_no_op_when_already_pointing_at_plugin(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """When the settings already match, no rewrite or backup is performed."""
        plugin_dir = (tmp_path / "data" / PLUGIN_DIR_NAME).resolve()
        settings = _claude_settings_path(monkeypatch, tmp_path)
        original = {
            "extraKnownMarketplaces": {
                MARKETPLACE_KEY: {
                    "source": {"source": "directory", "path": str(plugin_dir)},
                },
            },
        }
        raw = json.dumps(original)
        settings.write_text(raw, encoding="utf-8")

        modified = update_claude_settings(settings, plugin_dir)

        assert modified is False
        assert settings.read_text(encoding="utf-8") == raw
        assert not settings.with_suffix(settings.suffix + ".bak").exists()


class TestDefaultPaths:
    """Coverage for the default-path helpers used when ``main`` runs unattended."""

    def test_default_repo_root_points_at_snapper_checkout(self) -> None:
        """The fallback repo root is the parent of the ``scripts/`` directory."""
        result = _default_repo_root()

        assert (result / "scripts" / "render_local_plugin.py").exists()

    def test_default_settings_path_lives_under_user_home(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The fallback settings path resolves to ``~/.claude/settings.json``."""
        monkeypatch.setattr(Path, "home", lambda: tmp_path)

        result = _default_settings_path()

        assert result == tmp_path / ".claude" / "settings.json"


class TestMain:
    """Coverage for the ``main`` entry-point orchestration."""

    def test_renders_and_patches_settings(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """A first run renders the plugin and patches the settings file."""
        repo_root = tmp_path / "repo"
        _write_template(repo_root, "plugin.json", json.dumps({"path": PLACEHOLDER}))
        settings = _claude_settings_path(monkeypatch, tmp_path)
        settings.write_text(json.dumps({}), encoding="utf-8")

        exit_code = main(repo_root=repo_root, settings_path=settings)

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "Rendered plugin to" in captured.out
        assert "Updated" in captured.out
        data = json.loads(settings.read_text(encoding="utf-8"))
        rendered_dir = repo_root / "data" / PLUGIN_DIR_NAME
        assert data["extraKnownMarketplaces"][MARKETPLACE_KEY]["source"]["path"] == str(
            rendered_dir
        )

    def test_main_falls_back_to_default_paths(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """When called without arguments, ``main`` resolves defaults via the helpers."""
        repo_root = tmp_path / "repo"
        _write_template(repo_root, "plugin.json", json.dumps({"path": PLACEHOLDER}))
        settings = _claude_settings_path(monkeypatch, tmp_path)
        settings.write_text(json.dumps({}), encoding="utf-8")

        monkeypatch.setattr("scripts.render_local_plugin._default_repo_root", lambda: repo_root)

        exit_code = main()

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "Rendered plugin to" in captured.out

    def test_main_reports_no_changes_when_already_synced(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """A second run with an unchanged settings entry prints the no-op message."""
        repo_root = tmp_path / "repo"
        _write_template(repo_root, "plugin.json", json.dumps({"path": PLACEHOLDER}))
        plugin_dir = (repo_root / "data" / PLUGIN_DIR_NAME).resolve()
        settings = _claude_settings_path(monkeypatch, tmp_path)
        settings.write_text(
            json.dumps(
                {
                    "extraKnownMarketplaces": {
                        MARKETPLACE_KEY: {
                            "source": {"source": "directory", "path": str(plugin_dir)},
                        },
                    },
                },
            ),
            encoding="utf-8",
        )

        exit_code = main(repo_root=repo_root, settings_path=settings)

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "No changes to" in captured.out
