# License text provenance

Every vendored license text in this directory (all files except this one and
`THIRD-PARTY-INVENTORY.md`, which are authored locally) was fetched once from
a pinned, immutable upstream ref, its SHA-256 recorded below, and committed to
this repository so the image build is deterministic (no network fetch, no
synthetic fallback). To re-verify: `sha256sum <file>` against this table, and
the source URL against the pinned tag or commit. The table is additionally
pinned by `tests/docker/test_agent_console_entrypoint.py`, which recomputes
every digest against the committed file.

## Version evidence, per component (not uniform)

| Component | Version | Evidence |
|---|---|---|
| ripgrep | 15.2.0 | **binary-reported**: `codex-path/rg --version` → `ripgrep 15.2.0 (rev e89fff89ac)` |
| zsh | 5.9.0.3-test | **binary-reported**: `codex-resources/zsh/bin/zsh --version` → `zsh 5.9.0.3-test (x86_64-pc-linux-gnu)` |
| bubblewrap | 0.11.2 | **source-inferred**, NOT binary-reported: `codex-resources/bwrap --version` prints only `bubblewrap built for Codex`; 0.11.2 comes from `meson.build` (`version : '0.11.2'`) in the vendored tree at `openai/codex` @ `rust-v0.150.0` `/codex-rs/vendor/bubblewrap/` |
| ratatui | 0.30.2 | **lockfile-derived**: `codex-rs/Cargo.lock` @ `rust-v0.150.0` |

## License texts

| File | Source (pinned) | SHA-256 |
|---|---|---|
| `codex-cli-LICENSE` | `openai/codex` @ `rust-v0.150.0` `/LICENSE` (Apache-2.0) | `d17f227e4df5da1600391338865ce0f3055211760a36688f816941d58232d8dc` |
| `ripgrep-15.2.0-LICENSE-MIT` | `BurntSushi/ripgrep` @ `15.2.0` `/LICENSE-MIT` | `0f96a83840e146e43c0ec96a22ec1f392e0680e6c1226e6f3ba87e0740af850f` |
| `ripgrep-15.2.0-UNLICENSE` | `BurntSushi/ripgrep` @ `15.2.0` `/UNLICENSE` | `7e12e5df4bae12cb21581ba157ced20e1986a0508dd10d0e8a4ab9a4cf94e85c` |
| `ripgrep-15.2.0-COPYING` | `BurntSushi/ripgrep` @ `15.2.0` `/COPYING` (dual-license statement) | `01c266bced4a434da0051174d6bee16a4c82cf634e2679b6155d40d75012390f` |
| `bubblewrap-0.11.2-COPYING` | `openai/codex` @ `rust-v0.150.0` `/codex-rs/vendor/bubblewrap/COPYING` (LGPL-2.0-or-later); byte-exact upstream text, including its original trailing whitespace (a `.gitattributes` entry exempts this directory from whitespace linting rather than editing licensed text) | `b7993225104d90ddd8024fd838faf300bea5e83d91203eab98e29512acebd69c` |
| `zsh-5.9.0.3-test-LICENCE` | `zsh-users/zsh` @ commit `77045ef899e53b9598bebc5a41db93a548a40ca6` `/LICENCE` — the exact source commit the codex zsh asset is built from (see below); bit-identical to the same file at tag `zsh-5.9.0.3-test` (verified: same digest) | `d06fdf3ef9b1ec69d6b9e170b0a9516fbad3523261ff1668bde3bfea6e0ef5f5` |
| `ratatui-0.30.2-LICENSE` | `ratatui/ratatui` @ `ratatui-v0.30.2` `/LICENSE` (MIT) | `50eb43e8d742c9c61a9391e42b2184fce54dbd1893a1bb1c85b8c9ee217ab1f5` |
| `kimi-cli-LICENSE` | `MoonshotAI/kimi-cli` @ commit `c4f2102a51448f5041caa445c8a804d97279debe` `/LICENSE` (Apache-2.0) | `58d1e17ffe5109a7ae296caafcadfdbe6a7d176f0bc4ab01e12a689b0499d8bd` |

## The zsh helper is a PATCHED codex asset, not a stock zsh build

The shipped `codex-resources/zsh/bin/zsh` is built by OpenAI's release
workflow `.github/workflows/rust-release-zsh.yml` (@ `rust-v0.150.0`), which
pins:

- upstream source: `zsh-users/zsh` commit
  `77045ef899e53b9598bebc5a41db93a548a40ca6` (`ZSH_COMMIT`),
- patch: `/codex-rs/shell-escalation/patches/zsh-exec-wrapper.patch`
  (@ `rust-v0.150.0`, verified present).

The vendored LICENCE text above is taken from exactly that source commit; the
patch adds an exec wrapper and does not carry its own license terms. Anyone
rebuilding or auditing the patched binary should start from that
commit + patch + workflow triple, not from a release tag of stock zsh.

## Recorded absences and caveats

- **codex NOTICE**: `openai/codex` @ `rust-v0.150.0` publishes no `/NOTICE`
  file (verified HTTP 404 on 2026-08-27). No NOTICE is shipped and none is
  synthesized.
- **bubblewrap (LGPL) source / relink material**: the binary is OpenAI's own
  build of the tree vendored at
  `https://github.com/openai/codex/tree/rust-v0.150.0/codex-rs/vendor/bubblewrap`.
  That pinned tree is the corresponding source.
- **kimi-cli**: the shipped binary reports 0.38.0, a version with no matching
  public tag in `MoonshotAI/kimi-cli` (releases there are numbered 1.x). The
  upstream project is Apache-2.0 (text pinned to the commit above), but the
  exact source state of binary 0.38.0 is not publicly mapped; treat the
  binary's redistribution grant as unconfirmed. The image stays private
  regardless (see README "Never publish this image").
- **node LICENSE** is copied in the Dockerfile from the `node:26-slim` image
  itself (the artifact carries its own text), not from this directory.
