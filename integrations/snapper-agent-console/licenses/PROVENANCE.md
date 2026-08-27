# License text provenance

Every file in this directory was fetched once from a pinned, immutable
upstream ref, its SHA-256 recorded below, and committed to this repository so
the image build is deterministic (no network fetch, no synthetic fallback).
To re-verify: `sha256sum <file>` against this table, and the source URL
against the pinned tag or commit.

The bundled helper versions were read from the running binaries inside the
checksummed codex 0.150.0 package (not assumed from tags): `rg --version` →
ripgrep 15.2.0, `bwrap --version` → "bubblewrap built for Codex" (vendored
tree at openai/codex tag rust-v0.150.0 declares meson version 0.11.2),
`zsh --version` → zsh 5.9.0.3-test. The texts below are taken from exactly
those sources.

| File | Source (pinned) | SHA-256 |
|---|---|---|
| `codex-cli-LICENSE` | `openai/codex` @ `rust-v0.150.0` `/LICENSE` (Apache-2.0) | `d17f227e4df5da1600391338865ce0f3055211760a36688f816941d58232d8dc` |
| `ripgrep-15.2.0-LICENSE-MIT` | `BurntSushi/ripgrep` @ `15.2.0` `/LICENSE-MIT` | `0f96a83840e146e43c0ec96a22ec1f392e0680e6c1226e6f3ba87e0740af850f` |
| `ripgrep-15.2.0-UNLICENSE` | `BurntSushi/ripgrep` @ `15.2.0` `/UNLICENSE` | `7e12e5df4bae12cb21581ba157ced20e1986a0508dd10d0e8a4ab9a4cf94e85c` |
| `ripgrep-15.2.0-COPYING` | `BurntSushi/ripgrep` @ `15.2.0` `/COPYING` (dual-license statement) | `01c266bced4a434da0051174d6bee16a4c82cf634e2679b6155d40d75012390f` |
| `bubblewrap-0.11.2-COPYING` | `openai/codex` @ `rust-v0.150.0` `/codex-rs/vendor/bubblewrap/COPYING` (LGPL-2.0-or-later) | `b7993225104d90ddd8024fd838faf300bea5e83d91203eab98e29512acebd69c` |
| `zsh-5.9.0.3-test-LICENCE` | `zsh-users/zsh` @ `zsh-5.9.0.3-test` `/LICENCE` | `d06fdf3ef9b1ec69d6b9e170b0a9516fbad3523261ff1668bde3bfea6e0ef5f5` |
| `ratatui-0.30.2-LICENSE` | `ratatui/ratatui` @ `ratatui-v0.30.2` `/LICENSE` (MIT); version from `codex-rs/Cargo.lock` @ `rust-v0.150.0` | `50eb43e8d742c9c61a9391e42b2184fce54dbd1893a1bb1c85b8c9ee217ab1f5` |
| `kimi-cli-LICENSE` | `MoonshotAI/kimi-cli` @ commit `c4f2102a51448f5041caa445c8a804d97279debe` `/LICENSE` (Apache-2.0) | `58d1e17ffe5109a7ae296caafcadfdbe6a7d176f0bc4ab01e12a689b0499d8bd` |

## Recorded absences and caveats

- **codex NOTICE**: `openai/codex` @ `rust-v0.150.0` publishes no `/NOTICE`
  file (verified HTTP 404 on 2026-08-27). No NOTICE is shipped and none is
  synthesized.
- **bubblewrap (LGPL) source / relink material**: the binary is OpenAI's own
  build of the tree vendored at
  `https://github.com/openai/codex/tree/rust-v0.150.0/codex-rs/vendor/bubblewrap`
  (meson `version : '0.11.2'`). That pinned tree is the corresponding source.
- **kimi-cli**: the shipped binary reports 0.38.0, a version with no matching
  public tag in `MoonshotAI/kimi-cli` (releases there are numbered 1.x). The
  upstream project is Apache-2.0 (text pinned to the commit above), but the
  exact source state of binary 0.38.0 is not publicly mapped; treat the
  binary's redistribution grant as unconfirmed. The image stays private
  regardless (see README "Never publish this image").
- **node LICENSE** is copied in the Dockerfile from the `node:26-slim` image
  itself (the artifact carries its own text), not from this directory.
