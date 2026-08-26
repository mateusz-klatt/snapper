#!/bin/sh
set -eu

seed_kimi_region() {
    if [ ! -f "$HOME/.kimi-code/region" ]; then
        mkdir -p "$HOME/.kimi-code"
        printf 'global\n' > "$HOME/.kimi-code/region"
    fi
}

seed_codex_update_check_off() {
    if [ ! -f "$HOME/.codex/config.toml" ]; then
        mkdir -p "$HOME/.codex"
        printf 'check_for_update_on_startup = false\n' > "$HOME/.codex/config.toml"
    fi
}

seed_kimi_region
seed_codex_update_check_off

if [ "${AGENT_CONSOLE_MODE:-idle}" = "delegate" ]; then
    exec python -m snapper_delegate.pid1
fi

exec sleep infinity
