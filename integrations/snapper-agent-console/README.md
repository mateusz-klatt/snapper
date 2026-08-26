# snapper-agent-console

One interactive image carrying the native CLIs of the popular coding agents,
attached to over an allocated PTY, with `snapper-mcp` available inside. The
operator picked the single-image layout deliberately: the verification pass
(2026-08-26, one reader agent per installer) showed 5 of 7 CLIs are
essentially one self-contained executable; codex ships 4 helper binaries and
cursor is a bundled node app — both accepted by the operator and carried under
`/usr/local/lib` with their entrypoints linked into `/usr/local/bin`. Facts
and pins per CLI live in `cli-manifest.toml`.

The image builds FROM the blackbox delegate image, so one container carries
BOTH the Python OpenAI-API delegate (`python -m snapper_delegate.pid1`,
start it with `AGENT_CONSOLE_MODE=delegate`) AND every native CLI. The
blackbox image itself stays unchanged and remains the only unattended
WS-wake deployment; this console is for a human (or operator-supervised
agent) at a PTY.

## Layout

- `Dockerfile` — one fetch stage per CLI so a version bump invalidates only
  its own layer; every artifact is checksum-verified against
  `cli-manifest.toml` and no vendor install.sh executes during the build.
- `cli-manifest.toml` — the lock manifest: exact version, resolved artifact
  URL, checksum (vendor-published, or TOFU-frozen for cursor and grok whose
  vendors publish none), install path, update switch-off, notes.
- `compose.agent-console.yml` — profile-gated service: no ports, non-root,
  isolated home volume, PAT for `snapper-mcp` mounted read-only, worktree
  mounted explicitly.

## Attach

```bash
cd integrations/snapper-agent-console
AGENT_CONSOLE_PAT_CONFIG=/path/to/pat-config.json \
AGENT_CONSOLE_WORKSPACE=/path/to/worktree \
  docker compose -f compose.agent-console.yml --profile agent-console up -d agent-console
docker compose -f compose.agent-console.yml exec -it agent-console \
  tmux new-session -A -s agent /bin/bash
```

The runtime user has no login shell (`nologin`), so the image sets
`SHELL=/bin/bash` and the attach command names the shell explicitly.

`tmux` keeps the session across disconnects; a second pane typically runs
`snapper-mcp watch`. PID1 is a neutral idle process under `tini` — no CLI
autostarts, the human picks one.

## Auth

Each CLI keeps its own login state under the mounted home volume
(`agent-console-home`), created by running the CLI's native login inside the
PTY. Nothing vendor-specific is baked into the image. The Snapper PAT config
for `snapper-mcp` is a separate read-only mount — see
`compose.agent-console.yml`.

## Never publish this image

The built image embeds proprietary vendor binaries (Claude Code, Codex,
Copilot, Kimi, agy, Cursor, Grok). Their licenses do not grant redistribution:
the image is local/private-registry only; the recipe (this directory) is what
may be shared. `snapper-mcp` is MIT and ships with its LICENSE at
`/usr/local/lib/snapper-mcp/LICENSE`.

## Delegate mode

`AGENT_CONSOLE_MODE=delegate` execs `python -m snapper_delegate.pid1` instead
of the idle PTY holder. That mode additionally needs the blackbox delegate's
own configuration (`SNAPPER_DELEGATE_*` environment references and its
`/run/secrets/delegate` files, exactly as the blackbox compose supplies them) —
without those pid1 has nothing to connect to. This file intentionally does not
duplicate that wiring; copy it from the blackbox deployment when enabling the
mode.

## Shared-home risk (accepted)

All vendor CLIs share one `$HOME` volume, so any CLI (or anything it runs) can
read every other CLI's login state. This is inherent to the operator-mandated
single-image, single-home design and is accepted for this operator-supervised
console; do NOT reuse this image for mutually untrusted agents. The
`snapper-mcp` PAT is mounted read-only, which prevents mutation but not
reading.

## Ollama

Deliberately NOT in this image: it is a model server (weights, VRAM, its own
service lifecycle), not an agent CLI. Run it as its own opt-in service; the
CLIs here can point at it over the network where a vendor supports an
OpenAI-compatible endpoint.
