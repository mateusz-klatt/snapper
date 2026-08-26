# Interactive agent console: the blackbox Python delegate PLUS the native CLIs
# of the popular coding agents, one image, attached to over a PTY.
#
# Base is the blackbox delegate image itself (operator direction 2026-08-26:
# one container carrying every CLI and our Python OpenAI-API code), so
# `python -m snapper_delegate.pid1` keeps working here unchanged. Every CLI is
# fetched from the pinned artifact URL recorded in cli-manifest.toml and
# verified against the checksum recorded there — no vendor install.sh runs in
# this build. Single-file CLIs land straight in /usr/local/bin; the two
# multi-file packages (codex, cursor) live under /usr/local/lib with their
# entrypoints linked into /usr/local/bin, because their binaries resolve
# helpers by relative path.
#
# Build (from the repo root; the blackbox image must exist first; context is
# integrations/ so the build can bake snapper-mcp from the sibling package):
#   docker build -t snapper-delegate:blackbox integrations/snapper-delegate
#   docker build -t snapper-agent-console \
#     -f integrations/snapper-agent-console/Dockerfile integrations
# Attach:
#   docker compose -f integrations/snapper-agent-console/compose.agent-console.yml up -d
#   docker compose -f ... exec -it agent-console tmux new-session -A -s agent

ARG BASE_IMAGE=snapper-delegate:blackbox

FROM ${BASE_IMAGE} AS base

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl tini tmux git zstd less procps \
    && rm -rf /var/lib/apt/lists/*

FROM base AS cli-claude
ARG CLAUDE_VERSION=2.1.246
ARG CLAUDE_SHA256=1a0a662dc1bb938eaec38545abce9a4a69113d7d7f7c5e1a553ea276617b906a
RUN curl -fsSL "https://downloads.claude.ai/claude-code-releases/${CLAUDE_VERSION}/linux-x64/claude" \
        -o /usr/local/bin/claude \
    && echo "${CLAUDE_SHA256}  /usr/local/bin/claude" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/claude

FROM base AS cli-codex
ARG CODEX_VERSION=0.150.0
ARG CODEX_SHA256=2d27b8569ca760eeb2bf21fa12bd31f59ac42b81617b340cf8958e9b38d743d5
RUN curl -fsSL "https://releases.openai.com/codex/releases/${CODEX_VERSION}/codex-package-x86_64-unknown-linux-musl.tar.gz" \
        -o /tmp/codex.tar.gz \
    && echo "${CODEX_SHA256}  /tmp/codex.tar.gz" | sha256sum -c - \
    && mkdir -p /usr/local/lib/codex \
    && tar -xzf /tmp/codex.tar.gz -C /usr/local/lib/codex \
    && rm /tmp/codex.tar.gz \
    && test -x /usr/local/lib/codex/bin/codex \
    && ln -s /usr/local/lib/codex/bin/codex /usr/local/bin/codex

FROM base AS cli-copilot
ARG COPILOT_VERSION=1.0.80
ARG COPILOT_SHA256=039933c9247686131c4406abb1d439bdbf68103edc1ff585bd70d5b0dc940f72
RUN curl -fsSL "https://github.com/github/copilot-cli/releases/download/v${COPILOT_VERSION}/copilot-linux-x64.tar.gz" \
        -o /tmp/copilot.tar.gz \
    && echo "${COPILOT_SHA256}  /tmp/copilot.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/copilot.tar.gz -C /usr/local/bin copilot \
    && rm /tmp/copilot.tar.gz \
    && chmod 0755 /usr/local/bin/copilot

FROM base AS cli-kimi
ARG KIMI_VERSION=0.38.0
ARG KIMI_SHA256=7f18b701ea751d14bf051776747ca0339f8d59693aec9051276933067a914b00
RUN curl -fsSL "https://code.kimi.ai/kimi-code/binaries/${KIMI_VERSION}/kimi-code-linux-x64" \
        -o /usr/local/bin/kimi \
    && echo "${KIMI_SHA256}  /usr/local/bin/kimi" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/kimi

FROM base AS cli-agy
ARG AGY_VERSION=1.1.21-6424454201475072
ARG AGY_SHA512=3de7552ef089c136c0f570cdc9c04652278e02c1d41ed3911ad5f9f1b8c3bd567643aa401a1916060ea32a1b17fbaf90cbb417071db33f467880fcd848868d92
RUN curl -fsSL "https://storage.googleapis.com/antigravity-public/antigravity-cli/${AGY_VERSION}/linux-x64/cli_linux_x64.tar.gz" \
        -o /tmp/agy.tar.gz \
    && echo "${AGY_SHA512}  /tmp/agy.tar.gz" | sha512sum -c - \
    && mkdir -p /tmp/agy && tar -xzf /tmp/agy.tar.gz -C /tmp/agy \
    && install -m 0755 "$(find /tmp/agy -maxdepth 2 -type f -name antigravity | head -1)" /usr/local/bin/agy \
    && rm -rf /tmp/agy /tmp/agy.tar.gz

FROM base AS cli-cursor
ARG CURSOR_VERSION=2026.08.25-3e8eec8
ARG CURSOR_SHA256=7a212e5a17ff9316f5acc78808e33c536940d5455645022e6388d99ba48c8425
RUN curl -fsSL "https://downloads.cursor.com/lab/${CURSOR_VERSION}/linux/x64/agent-cli-package.tar.gz" \
        -o /tmp/cursor.tar.gz \
    && echo "${CURSOR_SHA256}  /tmp/cursor.tar.gz" | sha256sum -c - \
    && mkdir -p /usr/local/lib/cursor-agent \
    && tar -xzf /tmp/cursor.tar.gz -C /usr/local/lib/cursor-agent --strip-components=1 \
    && rm /tmp/cursor.tar.gz \
    && printf '#!/bin/sh\nexec /usr/local/lib/cursor-agent/cursor-agent --disable-auto-update "$@"\n' \
        > /usr/local/bin/cursor-agent \
    && chmod 0755 /usr/local/bin/cursor-agent

FROM node:26-slim AS mcp-build
COPY snapper-mcp /build/snapper-mcp
RUN cd /build/snapper-mcp && npm ci && npm run build && npm prune --omit=dev

FROM base AS cli-grok
ARG GROK_VERSION=1.0.5
ARG GROK_SHA256=9ba87444e1819e8f6104adbbf4676a870c204380aa5c3e1c38a926c4ea677238
RUN curl -fsSL "https://x.ai/cli/grok-${GROK_VERSION}-linux-x86_64" \
        -o /usr/local/bin/grok \
    && echo "${GROK_SHA256}  /usr/local/bin/grok" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/grok

FROM base AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends libatomic1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=cli-claude /usr/local/bin/claude /usr/local/bin/claude
COPY --from=cli-codex /usr/local/lib/codex /usr/local/lib/codex
RUN ln -s /usr/local/lib/codex/bin/codex /usr/local/bin/codex
COPY --from=cli-copilot /usr/local/bin/copilot /usr/local/bin/copilot
COPY --from=cli-kimi /usr/local/bin/kimi /usr/local/bin/kimi
COPY --from=cli-agy /usr/local/bin/agy /usr/local/bin/agy
COPY --from=cli-cursor /usr/local/lib/cursor-agent /usr/local/lib/cursor-agent
COPY --from=cli-cursor /usr/local/bin/cursor-agent /usr/local/bin/cursor-agent
COPY --from=cli-grok /usr/local/bin/grok /usr/local/bin/grok
COPY --from=node:26-slim /usr/local/bin/node /usr/local/bin/node
COPY --from=mcp-build /build/snapper-mcp/dist /usr/local/lib/snapper-mcp/dist
COPY --from=mcp-build /build/snapper-mcp/node_modules /usr/local/lib/snapper-mcp/node_modules
COPY --from=mcp-build /build/snapper-mcp/package.json /usr/local/lib/snapper-mcp/package.json
COPY snapper-mcp/LICENSE /usr/local/lib/snapper-mcp/LICENSE
RUN printf '#!/bin/sh\n# Default to the compose-mounted PAT config unless the caller picks one.\nPAT=/run/secrets/snapper-mcp/config.json\ncase " $* " in *" --config"*|*"--config="*|*" --base-url"*|*"--base-url="*) exec node /usr/local/lib/snapper-mcp/dist/index.js "$@" ;; esac\nif [ -r "$PAT" ]; then exec node /usr/local/lib/snapper-mcp/dist/index.js --config "$PAT" "$@"; fi\nexec node /usr/local/lib/snapper-mcp/dist/index.js "$@"\n' \
        > /usr/local/bin/snapper-mcp \
    && chmod 0755 /usr/local/bin/snapper-mcp
COPY snapper-agent-console/entrypoint.sh /usr/local/bin/agent-console-entrypoint
RUN chmod 0755 /usr/local/bin/agent-console-entrypoint \
    && mkdir -p /home/delegate && chown 888:888 /home/delegate

ENV HOME=/home/delegate \
    SHELL=/bin/bash \
    DISABLE_AUTOUPDATER=1 \
    COPILOT_AUTO_UPDATE=false \
    GROK_DISABLE_AUTOUPDATER=1 \
    KIMI_NO_MODIFY_PATH=1

USER 888:888
ENTRYPOINT ["tini", "--", "/usr/local/bin/agent-console-entrypoint"]
