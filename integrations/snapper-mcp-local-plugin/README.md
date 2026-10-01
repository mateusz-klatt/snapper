# snapper-mcp-local-plugin (template)

Templates for the local-dev Claude Code plugin that wires Claude Code to a
running localhost Snapper instance via the in-repo MCP bridge.

The JSON files in `.claude-plugin/` use the literal placeholder
`__SNAPPER_REPO_ROOT__` instead of an absolute path, so the directory is safe
to commit.

To activate the plugin on this machine:

```bash
make local-plugin
```

That target renders the templates into `data/snapper-mcp-local-plugin/`
(gitignored) with absolute paths from the current checkout, and patches
`~/.claude/settings.json` so the `snapper-mcp-local` marketplace entry points
at the rendered directory. A `.bak` copy of the previous settings is written
next to it before any edit.

The renderer also copies the plugin's `skills/` tree and substitutes checkout
paths there. It qualifies monitor skill triggers with the plugin name so the
host can associate `/snapper-mcp-local:wake` with the correct monitor. A manual
monitor invocation must use the absolute path to this plugin's own `env.json`;
the ambient `CLAUDE_PLUGIN_DATA` shell variable may belong to another plugin.
