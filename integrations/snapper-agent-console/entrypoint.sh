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
import os, pathlib, re, tempfile, tomllib
path = pathlib.Path(os.environ["HOME"]) / ".codex" / "config.toml"
key = "check_for_update_on_startup"
if not path.exists():
    path.write_text(f"{key} = false\n")
    raise SystemExit(0)
text = path.read_text()
try:
    parsed = tomllib.loads(text)
except tomllib.TOMLDecodeError as exc:
    print(f"WARN: ~/.codex/config.toml is not valid TOML ({exc}); leaving it untouched", flush=True)
    raise SystemExit(0)
if parsed.get(key) is False:
    raise SystemExit(0)
lines = text.splitlines(keepends=True)
root_end = len(lines)
for i, line in enumerate(lines):
    if re.match(r"\s*\[", line):
        root_end = i
        break
root = lines[:root_end]
root = [l for l in root if not re.match(rf"\s*(['\"]?){key}\1\s*=", l)]
root.insert(0, f"{key} = false\n")
candidate = "".join(root + lines[root_end:])
try:
    reparsed = tomllib.loads(candidate)
    assert reparsed.get(key) is False
except (tomllib.TOMLDecodeError, AssertionError) as exc:
    print(f"WARN: refusing rewrite that would corrupt config.toml ({exc}); leaving original", flush=True)
    raise SystemExit(0)
tmp = tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, suffix=".tmp")
tmp.write(candidate)
tmp.close()
os.replace(tmp.name, path)
PY
}

seed_kimi_region
enforce_codex_update_check_off

if [ "${AGENT_CONSOLE_MODE:-idle}" = "delegate" ]; then
    python - <<'PY' || { echo "CRITICAL: AGENT_CONSOLE_MODE=delegate but pid1 configuration is invalid (see above) — refusing to idle as a fake-healthy delegate" >&2; exit 1; }
import os, sys
from snapper_delegate.pid1 import RunnerOnlyConfigurationError, load_runner_configuration
try:
    load_runner_configuration(os.environ)
except RunnerOnlyConfigurationError as exc:
    print(f"delegate preflight: configuration rejected by canonical loader: {exc}", file=sys.stderr)
    raise SystemExit(1)
PY
    exec python -m snapper_delegate.pid1
fi

exec sleep infinity
