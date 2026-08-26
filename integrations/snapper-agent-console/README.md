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
docker compose --profile agent-console up -d snapper-agent-console
docker compose exec -it snapper-agent-console tmux new-session -A -s agent
```

`tmux` keeps the session across disconnects; a second pane typically runs
`snapper-mcp watch`. PID1 is a neutral idle process under `tini` — no CLI
autostarts, the human picks one.

## Auth

Each CLI keeps its own login state under the mounted home volume
(`agent-console-home`), created by running the CLI's native login inside the
PTY. Nothing vendor-specific is baked into the image. The Snapper PAT config
for `snapper-mcp` is a separate read-only mount — see
`compose.agent-console.yml`.

## Ollama

Deliberately NOT in this image: it is a model server (weights, VRAM, its own
service lifecycle), not an agent CLI. Run it as its own opt-in service; the
CLIs here can point at it over the network where a vendor supports an
OpenAI-compatible endpoint.
