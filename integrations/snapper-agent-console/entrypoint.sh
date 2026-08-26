#!/bin/sh
set -eu

seed_kimi_region() {
    if [ ! -f "$HOME/.kimi-code/region" ]; then
        mkdir -p "$HOME/.kimi-code"
        printf 'global\n' > "$HOME/.kimi-code/region"
    fi
}

seed_codex_update_check_off() {
    mkdir -p "$HOME/.codex"
    if [ ! -f "$HOME/.codex/config.toml" ]; then
        printf 'check_for_update_on_startup = false\n' > "$HOME/.codex/config.toml"
    elif ! grep -q '^check_for_update_on_startup' "$HOME/.codex/config.toml"; then
        printf 'check_for_update_on_startup = false\n' >> "$HOME/.codex/config.toml"
    fi
}

seed_kimi_region
seed_codex_update_check_off

if [ "${AGENT_CONSOLE_MODE:-idle}" = "delegate" ]; then
    exec python -m snapper_delegate.pid1
fi

exec sleep infinity
