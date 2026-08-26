#!/bin/sh
set -eu

seed_kimi_region() {
    if [ ! -f "$HOME/.kimi-code/region" ]; then
        mkdir -p "$HOME/.kimi-code"
        printf 'global\n' > "$HOME/.kimi-code/region"
    fi
}

enforce_codex_update_check_off() {
    mkdir -p "$HOME/.codex"
    python - <<'PY'
import os, pathlib, re
path = pathlib.Path(os.environ["HOME"]) / ".codex" / "config.toml"
key = "check_for_update_on_startup"
if not path.exists():
    path.write_text(f"{key} = false\n")
    raise SystemExit(0)
text = path.read_text()
lines = text.splitlines(keepends=True)
root_end = len(lines)
for i, line in enumerate(lines):
    if re.match(r"\s*\[", line):
        root_end = i
        break
root = lines[:root_end]
root = [l for l in root if not re.match(rf"\s*{key}\s*=", l)]
root.insert(0, f"{key} = false\n")
path.write_text("".join(root + lines[root_end:]))
import tomllib
parsed = tomllib.loads(path.read_text())
assert parsed[key] is False, "root-level enforcement failed"
PY
}

seed_kimi_region
enforce_codex_update_check_off

if [ "${AGENT_CONSOLE_MODE:-idle}" = "delegate" ]; then
    if [ -z "${SNAPPER_DELEGATE_WS_URL:-}" ] && [ ! -r /run/secrets/delegate ] && [ ! -d /run/secrets/delegate ]; then
        echo "CRITICAL: AGENT_CONSOLE_MODE=delegate but no delegate wiring (SNAPPER_DELEGATE_* env or /run/secrets/delegate) — refusing to idle as a fake-healthy delegate" >&2
        exit 1
    fi
    exec python -m snapper_delegate.pid1
fi

exec sleep infinity
