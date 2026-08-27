# Third-party inventory — bundled codex package (pinned rust-v0.150.0)

Per-binary inventory of the checksummed
`codex-package-x86_64-unknown-linux-musl.tar.gz` (codex-cli 0.150.0), with the
license text shipped beside this file for every component named here. Version
evidence is NOT uniform across components — rg and zsh report their versions
from the binary, bubblewrap's binary reports only "built for Codex" and its
0.11.2 is inferred from the vendored meson source, ratatui's version comes
from the Cargo.lock — see the "Version evidence" table in `PROVENANCE.md` for
the per-component basis and the SHA-256 digests.

| Path in package | Component | Version | License (text file) |
|---|---|---|---|
| `bin/codex` | Codex CLI (Rust) | 0.150.0 | Apache-2.0 (`codex-cli-LICENSE`) |
| `bin/codex-code-mode-host` | Codex code-mode host (Rust, same workspace) | 0.150.0 | Apache-2.0 (`codex-cli-LICENSE`) |
| `codex-path/rg` | ripgrep | 15.2.0 | MIT or Unlicense, user's choice (`ripgrep-15.2.0-LICENSE-MIT`, `ripgrep-15.2.0-UNLICENSE`, statement in `ripgrep-15.2.0-COPYING`) |
| `codex-resources/bwrap` | bubblewrap, OpenAI build of the tree vendored in `codex-rs/vendor/bubblewrap` | 0.11.2 | LGPL-2.0-or-later (`bubblewrap-0.11.2-COPYING`); corresponding source is the pinned vendored tree |
| `codex-resources/zsh/bin/zsh` | zsh — PATCHED codex asset: built from `zsh-users/zsh` commit `77045ef8` + `codex-rs/shell-escalation/patches/zsh-exec-wrapper.patch` via `.github/workflows/rust-release-zsh.yml` (all @ `rust-v0.150.0`) | 5.9.0.3-test (binary-reported) | zsh license, MIT-like (`zsh-5.9.0.3-test-LICENCE`, text from the exact source commit) |

## Statically linked Rust crates

The two Rust binaries aggregate crates under their respective licenses. The
authoritative transitive set is
`https://github.com/openai/codex/blob/rust-v0.150.0/codex-rs/Cargo.lock`
(a lockfile, not a license inventory: it names crates and versions, carries no
license fields, and includes dev/test/other-platform entries that are not in
these binaries). The one crate upstream calls out for attribution, Ratatui, is
shipped in full: MIT text at `ratatui-0.30.2-LICENSE`, version taken from that
Cargo.lock. A complete per-binary crate attribution requires running
`cargo-about`/`cargo metadata` against `codex-rs` at the pinned tag; that is
deferred to the CI/SBOM milestone (M12) together with SBOM generation and
image signing, and this image remains private either way.
