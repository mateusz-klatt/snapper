"""Render the local Snapper MCP plugin into data/ with absolute paths.

Reads JSON templates from ``integrations/snapper-mcp-local-plugin/.claude-plugin/``,
substitutes the literal placeholder ``__SNAPPER_REPO_ROOT__`` with the
absolute repo-root path of the current checkout, and writes the rendered
files into ``data/snapper-mcp-local-plugin/.claude-plugin/`` (gitignored).

Also patches ``~/.claude/settings.json`` so the ``snapper-mcp-local``
marketplace entry points at the rendered directory. A ``.bak`` copy of the
previous settings is written next to it before any edit.
"""

import json
import shutil
from pathlib import Path
from typing import cast

from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue

PLACEHOLDER = "__SNAPPER_REPO_ROOT__"
PLUGIN_DIR_NAME = "snapper-mcp-local-plugin"
CLAUDE_PLUGIN_SUBDIR = ".claude-plugin"
MARKETPLACE_KEY = "snapper-mcp-local"
CLAUDE_SETTINGS_FILENAME = "settings.json"


def _json_object(value: object, description: str) -> JsonObject:
    """Return a JSON object or raise a clear validation error."""
    if not isinstance(value, dict):
        raise ValueError(f"{description} must be a JSON object")
    return value


def _get_or_create_json_object(parent: JsonObject, key: str) -> JsonObject:
    """Return an existing nested JSON object or create an empty one."""
    value: JsonValue | None = parent.get(key)
    if value is None:
        child: JsonObject = {}
        parent[key] = child
        return child
    if not isinstance(value, dict):
        raise ValueError(f"Claude settings field {key!r} must be a JSON object")
    return value


def _replace_placeholder(value: JsonValue, replacement: str) -> JsonValue:
    """Return a JSON value with placeholder occurrences replaced in strings."""
    if isinstance(value, str):
        return value.replace(PLACEHOLDER, replacement)
    if isinstance(value, list):
        return [_replace_placeholder(item, replacement) for item in value]
    if isinstance(value, dict):
        return {key: _replace_placeholder(child, replacement) for key, child in value.items()}
    return value


def render_plugin(repo_root: Path) -> Path:
    """Render all templated JSON files into the gitignored output directory.

    Args:
        repo_root: Absolute path of the snapper checkout.

    Returns:
        Absolute path of the rendered plugin directory (parent of
        ``.claude-plugin``), suitable as the ``directory`` source path for
        Claude Code's ``extraKnownMarketplaces`` entry.
    """
    template_dir = repo_root / "integrations" / PLUGIN_DIR_NAME / CLAUDE_PLUGIN_SUBDIR
    output_root = repo_root / "data" / PLUGIN_DIR_NAME
    output_dir = output_root / CLAUDE_PLUGIN_SUBDIR
    output_dir.mkdir(parents=True, exist_ok=True)

    repo_root_str = str(repo_root)
    for src in sorted(template_dir.glob("*.json")):
        template = cast(JsonValue, json.loads(src.read_text(encoding="utf-8")))
        rendered = _replace_placeholder(template, repo_root_str)
        (output_dir / src.name).write_text(
            json.dumps(rendered, indent=2) + "\n",
            encoding="utf-8",
        )

    return output_root


def update_claude_settings(settings_path: Path, plugin_dir: Path) -> bool:
    """Patch the global Claude Code settings to point at the rendered plugin.

    Args:
        settings_path: Path to ``~/.claude/settings.json``.
        plugin_dir: Absolute path of the rendered plugin directory.

    Returns:
        True if the file was modified (and a ``.bak`` written), False when
        the entry already pointed at ``plugin_dir`` and nothing was rewritten.

    Raises:
        FileNotFoundError: If ``settings_path`` does not exist — Claude Code
            must be configured at least once before this script runs.
    """
    resolved_path = settings_path.expanduser().resolve()
    allowed_dir = (Path.home() / ".claude").resolve()
    if resolved_path.parent != allowed_dir:
        raise ValueError(f"Claude settings path must stay under {allowed_dir}")
    if resolved_path.name != CLAUDE_SETTINGS_FILENAME:
        raise ValueError(f"Claude settings path must be named {CLAUDE_SETTINGS_FILENAME}")
    if not resolved_path.exists():
        raise FileNotFoundError(
            f"Claude Code settings not found at {resolved_path}; "
            "configure Claude Code at least once before running render_local_plugin."
        )
    if not resolved_path.is_file():
        raise ValueError(f"Claude settings path must be a regular file: {resolved_path}")

    raw = resolved_path.read_text(encoding="utf-8")
    data = _json_object(json.loads(raw), "Claude settings")
    marketplaces = _get_or_create_json_object(data, "extraKnownMarketplaces")
    entry = _get_or_create_json_object(marketplaces, MARKETPLACE_KEY)
    source = _get_or_create_json_object(entry, "source")

    new_path = str(plugin_dir.expanduser().resolve())
    if source.get("source") == "directory" and source.get("path") == new_path:
        return False

    backup_path = resolved_path.with_suffix(resolved_path.suffix + ".bak")
    shutil.copy2(resolved_path, backup_path)

    source["source"] = "directory"
    source["path"] = new_path
    with resolved_path.open("w", encoding="utf-8") as settings_file:
        json.dump(data, settings_file, indent=2)
        settings_file.write("\n")
    return True


def _default_repo_root() -> Path:
    """Return the snapper repo root inferred from this module's location."""
    return Path(__file__).resolve().parent.parent


def _default_settings_path() -> Path:
    """Return the default Claude Code settings path under the user's home."""
    return Path.home() / ".claude" / "settings.json"


def main(
    repo_root: Path | None = None,
    settings_path: Path | None = None,
) -> int:
    """Run the render workflow.

    Args:
        repo_root: Override for the repo-root path (defaults to the parent
            of the directory containing this script).
        settings_path: Override for the Claude settings path (defaults to
            ``~/.claude/settings.json``).

    Returns:
        Exit code (0 on success).
    """
    if repo_root is None:
        repo_root = _default_repo_root()
    if settings_path is None:
        settings_path = _default_settings_path()

    plugin_dir = render_plugin(repo_root)
    print(f"Rendered plugin to {plugin_dir}")

    if update_claude_settings(settings_path, plugin_dir):
        print(f"Updated {settings_path} (backup: {settings_path}.bak)")
    else:
        print(f"No changes to {settings_path} (already pointing at rendered dir)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
