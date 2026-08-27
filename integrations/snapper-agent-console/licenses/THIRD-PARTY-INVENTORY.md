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
| `bin/codex` | Codex CLI (Rust) | 0.150.0 | Apache-2.0 (`codex-cli-LICENSE`), attribution in `codex-cli-NOTICE` |
| `bin/codex-code-mode-host` | Codex code-mode host (Rust, same workspace) | 0.150.0 | Apache-2.0 (`codex-cli-LICENSE`), attribution in `codex-cli-NOTICE` |
| `codex-path/rg` | ripgrep | 15.2.0 | MIT or Unlicense, user's choice (`ripgrep-15.2.0-LICENSE-MIT`, `ripgrep-15.2.0-UNLICENSE`, statement in `ripgrep-15.2.0-COPYING`) |
| `codex-resources/bwrap` | THREE layers, not one: (1) the Apache-2.0 Rust crate `codex-bwrap` from `openai/codex` — `codex-rs/bwrap/Cargo.toml` declares `name = "codex-bwrap"` with `[[bin]] name = "bwrap"`, inheriting `license = "Apache-2.0"` from `codex-rs/Cargo.toml` `[workspace.package]`; (2) its `build.rs` compiles the vendored bubblewrap C (`bubblewrap.c`, `bind-mount.c`, `network.c`, `utils.c`) from `codex-rs/vendor/bubblewrap`, renaming `main` to `bwrap_main` and stamping `PACKAGE_STRING "bubblewrap built for Codex"`; (3) that same `build.rs` hard-requires libcap via `pkg_config` and emits `cargo:rustc-link-lib=cap`, which the official musl build resolves **statically** — `.github/scripts/install-musl-build-tools.sh` pins libcap `2.75` (tarball sha256 `de4e7e06…d83632`), builds it, installs only `libcap.a`, and restricts `PKG_CONFIG_LIBDIR` for the target to that prefix. All @ `rust-v0.150.0` | codex-bwrap 0.150.0 (workspace); bubblewrap 0.11.2 (`meson.build`); libcap 2.75 (musl build script) | THREE: Apache-2.0 for the crate (`codex-cli-LICENSE`, attribution in `codex-cli-NOTICE`) + LGPL-2.0-or-later for the vendored bubblewrap C (`bubblewrap-0.11.2-COPYING`; corresponding source is the pinned vendored tree) + `BSD-3-Clause OR GPL-2.0-only` for the statically linked libcap (`libcap-2.75-License`; note **GPL-2.0-only**, and one upstream file carries both texts) |
| `codex-resources/zsh/bin/zsh` | zsh — PATCHED codex asset: built from `zsh-users/zsh` commit `77045ef8` + `codex-rs/shell-escalation/patches/zsh-exec-wrapper.patch` via `.github/workflows/rust-release-zsh.yml` (all @ `rust-v0.150.0`) | 5.9.0.3-test (binary-reported) | TWO layers: upstream zsh under the zsh license, MIT-like (`zsh-5.9.0.3-test-LICENCE`, text from the exact source commit), PLUS OpenAI's patch contribution, which comes from the Apache-2.0 `openai/codex` repository (`codex-cli-LICENSE`, attribution in `codex-cli-NOTICE`) — the shipped binary is a combination, not stock zsh |

## Statically linked Rust crates

There are **three** Rust binaries from the `openai/codex` workspace in this package, not two: `bin/codex`, `bin/codex-code-mode-host`, and `codex-resources/bwrap` (crate `codex-bwrap`, which is a Rust executable that merely embeds bubblewrap's C via `build.rs` and additionally links libcap statically), and all three aggregate crates under their respective licenses.

The authoritative transitive crate set is
`https://github.com/openai/codex/blob/rust-v0.150.0/codex-rs/Cargo.lock`
(a lockfile, not a license inventory: it names crates and versions, carries no
license fields, and includes dev/test/other-platform entries that are not in
these binaries). The one crate upstream calls out for attribution, Ratatui, is
shipped in full: MIT text at `ratatui-0.30.2-LICENSE`, version taken from that
Cargo.lock, and upstream's own attribution of it travels with the vendored
`codex-cli-NOTICE`. A complete per-binary crate attribution requires running
`cargo-about`/`cargo metadata` against `codex-rs` at the pinned tag; that is
deferred to the CI/SBOM milestone (M12) together with SBOM generation and
image signing, and this image remains private either way.
