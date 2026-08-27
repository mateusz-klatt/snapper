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
  A `lineage` stage pins the inherited base to the working tree: `BASE_IMAGE`
  is a mutable tag, so a stale `snapper-delegate:blackbox` would let the
  console's pid1 tests pass against code the image does not actually ship.
  The stage compares the delegate module set and every module byte-for-byte
  and fails the build with a named remedy if they diverge; the runtime stage
  depends on the digest manifest it emits (`/usr/local/share/agent-console/
  delegate-lineage.sha256`), so the check cannot be skipped, and no delegate
  source enters a runtime layer.
- `cli-manifest.toml` — the lock manifest: exact version, resolved artifact
  URL, checksum (vendor-published, or TOFU-frozen for cursor and grok whose
  vendors publish none), install path, update switch-off, notes.
- `compose.agent-console.yml` — profile-gated service: no ports, non-root,
  isolated home volume, PAT for `snapper-mcp` mounted read-only, worktree
  mounted explicitly.
- `codex-requirements.toml` — baked to `/etc/codex/requirements.toml`: Codex's
  managed-policy layer pins `check_for_update_on_startup = false` above any
  user config (verified with `codex doctor` on the pinned 0.150.0: user
  `true` still resolves to effective `false`). The entrypoint never reads or
  rewrites the user's `~/.codex/config.toml`.
- `licenses/` — vendored license texts for the bundled third-party components,
  copied to `/usr/local/share/licenses/vendored`; `licenses/PROVENANCE.md`
  records the pinned source ref and SHA-256 of every text plus the
  per-component version evidence (binary-reported for rg and zsh,
  source-inferred for bwrap, lockfile-derived for ratatui, binary-reported
  *and* source-pinned for the kimi bundle), and
  `licenses/THIRD-PARTY-INVENTORY.md` maps each binary in the codex package
  to its component, version, and license text, and does the same layer by
  layer for the Kimi Code SEA bundle — including a list of what it does NOT
  close. `PROVENANCE.md` additionally records, in a table, the exact
  enumerations behind every claim of absence, because three earlier revisions
  of the kimi claim were wrong in ways a hand-guessed probe cannot
  distinguish from a real absence.

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
`snapper-mcp` (stdio MCP server; the mounted PAT config at /run/secrets/snapper-mcp/config.json is picked up by default). PID1 is a neutral idle process under `tini` — no CLI
autostarts, the human picks one.

## Auth

Each CLI keeps its own login state under the mounted home volume
(`agent-console-home`), created by running the CLI's native login inside the
PTY. Nothing vendor-specific is baked into the image. The Snapper PAT config
for `snapper-mcp` is a separate read-only mount — see
`compose.agent-console.yml`.

## Codex sandbox under the hardened posture

With `cap_drop: ALL` + `no-new-privileges`, `bwrap` cannot create a user
namespace, so `codex sandbox` is unavailable INSIDE this container by design —
the container itself is the isolation boundary here. Run codex with its
sandbox disabled (`--sandbox danger-full-access`) and rely on the container
posture, or, if in-container bwrap is genuinely needed, relax the service
with `security_opt: ["seccomp=unconfined"]` plus a kernel allowing
unprivileged user namespaces — an explicit, documented trade the default
compose does not make.

## Never publish this image

The built image embeds vendor CLI binaries with mixed redistribution terms
(verified against each vendor's published license, see `cli-manifest.toml`
and `licenses/PROVENANCE.md`):

- **Redistribution permitted with notices**: Codex CLI (Apache-2.0, and its
  upstream NOTICE travels with it in `licenses/codex-cli-NOTICE`), Node.js
  (MIT), `snapper-mcp` (MIT, LICENSE at `/usr/local/lib/snapper-mcp/LICENSE`),
  and **Kimi Code CLI** (MIT) — with an explicit reservation, below.
- **Kimi Code CLI, stated precisely**: its *code* grant is confirmed MIT
  (`MoonshotAI/kimi-code` @ tag `@moonshot-ai/kimi-code@0.38.0`, text vendored
  at `licenses/kimi-code-0.38.0-LICENSE`). What is **not** confirmed is full
  redistribution closure for the *bundle*: the artifact is a single 182 MB
  Node SEA that embeds a complete Node 24.15.0 runtime, force-bundled JS, a
  prebuilt native module and a 534-file prebuilt web UI, and it ships with no
  LICENSE, no NOTICE and no SBOM. Those are two different questions and
  earlier revisions of this README conflated them. The four texts that are
  known to be required travel in `licenses/`; the items that cannot be closed
  from public evidence are itemised in `licenses/THIRD-PARTY-INVENTORY.md`.
- **Conditional**: Copilot CLI — the GitHub Copilot CLI License permits
  unmodified copies only as part of an application or service and prohibits
  standalone distribution.
- **No redistribution grant found**: Claude Code, agy (Google), Cursor, and
  Grok (no published license; xAI terms).

The last group alone forces the conclusion: the image is local/private-registry
only; the recipe (this directory, including the vendored license texts) is
what may be shared.

Two of the seven CLIs are bundles whose one-line license label understates
what they actually contain, and both are documented layer-by-layer in
`licenses/THIRD-PARTY-INVENTORY.md` rather than by that label alone.

"Kimi Code CLI is MIT" is the first. The MIT grant covers Moonshot's own code;
the shipped file additionally carries a verbatim Node **24.15.0** executable
whose composite license text is measurably *not* inside the binary (hence
`licenses/node-24.15.0-LICENSE`, double-pinned against the official Node
tarball), plus `pi-tui` and OpenTUI-derived code whose MIT copyright notices
are likewise absent from the artifact. Genuinely open: the Rust crate closure
behind the bundled clipboard module, the erased module identities inside the
force-bundled JS, and the prebuilt `dist-web` tree, which upstream states is
synced from a `code-app` repository that is not among the MoonshotAI
organisation's 43 public repositories.

Note that "Codex CLI is Apache-2.0" understates what its package contains: the
sandbox helper `bwrap` is three layers — an Apache-2.0 Rust crate whose
`build.rs` embeds bubblewrap's **LGPL-2.0-or-later** C and statically links
**libcap (BSD-3-Clause OR GPL-2.0-only)**. The copyleft layers are exactly why
`licenses/PROVENANCE.md` pins the corresponding source (the vendored tree) and
the exact libcap tarball rather than only naming a license.

## Delegate mode

`AGENT_CONSOLE_MODE=delegate` execs `python -m snapper_delegate.pid1` with
`SNAPPER_PID1_STRICT=1` instead of the idle PTY holder. That mode additionally
needs the blackbox delegate's own configuration (`SNAPPER_DELEGATE_*`
environment references and its `/run/secrets/delegate` files, exactly as the
blackbox compose supplies them) — without those pid1 has nothing to connect
to. Compose passes `AGENT_CONSOLE_MODE` through from the host environment, but
the delegate secrets/network wiring intentionally lives with the blackbox
deployment — copy it from there into an override file when enabling the mode.

Fail-closed is enforced by pid1 itself, not by a shell preflight: under
strict startup the ONE canonical configuration load (the same
`load_runner_configuration` call that produces the runner's parameters) logs
CRITICAL and terminates the process with exit code 1 on any error. There is
no separate validate-then-reload window, so mutating the bind-mounted secrets
between a preflight and the real load cannot produce a fake-healthy idle
delegate. Under `restart: unless-stopped` a misconfiguration shows up as a
visible restart loop, which is the point. The blackbox deployment does not
set the strict flag and keeps its documented signal-aware idle contract.

## Shared-home risk (accepted)

All vendor CLIs share one `$HOME` volume, so any CLI (or anything it runs) can
read every other CLI's login state. This is inherent to the operator-mandated
single-image, single-home design and is accepted for this operator-supervised
console; do NOT reuse this image for mutually untrusted agents. The
`snapper-mcp` PAT is mounted read-only with `create_host_path: false` (a
missing host file fails the mount instead of silently creating a directory);
the host file must be readable by uid 888 (e.g. `chown :888` + mode 0640 —
0600 owned by another user will NOT be readable in the container). Read-only
prevents mutation but not reading by co-resident CLIs.

## Ollama

Deliberately NOT in this image: it is a model server (weights, VRAM, its own
service lifecycle), not an agent CLI. Run it as its own opt-in service; the
CLIs here can point at it over the network where a vendor supports an
OpenAI-compatible endpoint.
