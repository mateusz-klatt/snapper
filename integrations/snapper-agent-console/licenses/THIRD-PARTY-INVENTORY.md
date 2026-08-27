# Third-party inventory

Two artifacts in this image are multi-component bundles rather than single
programs, and each gets its own section below: the codex package, whose
contents are enumerable to the file level, and the Kimi Code CLI binary, whose
contents are only partially enumerable and whose open items are listed
explicitly rather than glossed as "third-party components".

## Bundled codex package (pinned rust-v0.150.0)

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

### Statically linked Rust crates

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

## Kimi Code CLI binary (pinned 0.38.0) — a bundle, not a program

`/usr/local/bin/kimi` is one file of 182 062 272 bytes, sha256
`7f18b701ea751d14bf051776747ca0339f8d59693aec9051276933067a914b00` (the
`cli-manifest.toml` pin; the measurements below were taken on that exact file).
It is a Node **Single Executable Application**: upstream
`apps/kimi-code/scripts/native/03-inject.mjs` copies the build host's
`process.execPath` and `postject`-injects a `NODE_SEA_BLOB`, and
`02-sea-blob.mjs` packs the force-bundled JS, the per-target native assets and
the whole `dist-web` tree into that blob. The vendor ships no LICENSE, no
NOTICE and no SBOM beside it, and the upstream repository contains no
third-party inventory either (verified by exhaustive tree walk — see
`PROVENANCE.md`).

The upstream source is `MoonshotAI/kimi-code` @ tag
`@moonshot-ai/kimi-code@0.38.0` (annotated tag `488fe6bb…b5446`, peeling to
commit `0999454bdcb5ddd98f39bffee434dcf0a810f394`). This is **not** the
similarly-numbered legacy Python `kimi-cli` project; see the caveat in
`PROVENANCE.md` for why that mattered three times.

| Layer | What it is | Evidence it is in THIS artifact | License (text file) |
|---|---|---|---|
| Kimi Code CLI application code | The product itself, TypeScript bundled to `main.cjs` | `kimi-code` ×248 and `0.38.0` ×1 in the ELF; upstream `apps/kimi-code/package.json` @ the pinned commit declares `"license": "MIT"` | MIT (`kimi-code-0.38.0-LICENSE`) |
| Node.js runtime, **24.15.0** | A complete Node executable, copied verbatim and then rewritten by `postject` | `v24.15.0` ×5 and `NODE_SEA_BLOB` ×2 in the ELF; upstream `.nvmrc` = `24.15.0`, selected in CI via `node-version-file: .nvmrc` | Node composite license (`node-24.15.0-LICENSE`) — covers Node plus OpenSSL, V8, ICU, zlib, llhttp and the rest. The artifact embeds the runtime but **not** its license text (measured: 0 hits for two distinctive LICENSE sentences), so the text has to travel separately |
| `@moonshot-ai/pi-tui` 0.84.4 | Workspace TUI framework, bundled as JS. On linux-x64 it contributes **no** native helper: `native-deps.mjs` maps that target to an empty file list | `pi-tui` ×71 in the ELF; `Mario Zechner` ×0, i.e. the copyright notice is absent from the artifact | MIT (`pi-tui-0.84.4-LICENSE`) |
| OpenTUI-derived input handling | `packages/pi-tui/src/stdin-buffer.ts` is stated upstream to be "Based on code from OpenTUI"; `src/keys.ts` cites `sst/opentui` @ `7da92b40…` | `opentui` ×2 in the ELF | MIT (`opentui-7da92b40-LICENSE`), taken from OpenTUI's own repository because kimi-code ships no OpenTUI license file |
| `@mariozechner/clipboard-linux-x64-gnu` 0.3.9 | Prebuilt Rust napi module, collected as a SEA native asset by `native-deps.mjs` | `clipboard-linux-x64-gnu` ×12 in the ELF | MIT **by SPDX declaration only** — upstream publishes no license text at all (see UNKNOWN 2 below). Nothing vendored, because there is nothing to vendor |
| `dist-web` prebuilt web UI | 534 files, ~34 MB, packed into the SEA blob as web assets | `dist-web` ×1072 and `index.html` ×17 in the ELF | **UNKNOWN** (see UNKNOWN 4 below) |

### Open items — what this inventory does NOT close

1. **Rust crate license closure inside the clipboard module.** The npm tarball
   carries a SLSA v1 provenance attestation binding it to commit
   `3a0f58eb7250a9a46ad77863bc4618eee099a248` of
   `github.com/badlogic/clipboard` (now `earendil-works/clipboard`), so the
   binary *is* linked to a source commit. But `Cargo.lock` there names 122
   packages with zero license fields, and no SBOM is published.
2. **The clipboard component has no copyright notice to redistribute.** MIT
   appears only as `"license": "MIT"` in `package.json`; an exhaustive walk of
   that repository (63 blobs, untruncated) finds no LICENSE/COPYING/NOTICE
   file and `Cargo.toml` has no `license` key. An upstream defect we cannot
   fix locally and must not paper over by synthesising a notice.
3. **The bundled JS module set is not enumerable from the artifact.**
   `01-bundle.mjs` force-bundles the dependency graph through `tsdown` into
   `main.cjs`, erasing per-package identity before the blob is built.
4. **`dist-web` originates outside any public repository.**
   `scripts/check-web-assets.mjs` states that the web UI lives in the
   `code-app` repo and is synced in as committed build output; exhaustive
   enumeration of the MoonshotAI org (43 public repositories) finds no
   `code-app`. Its own dependencies (a Vite build including KaTeX fonts among
   others) therefore cannot be inventoried from public material.
5. **No byte-level identity claim for the embedded Node build.** The version
   matches from both ends, but CI resolves Node through `actions/setup-node`
   (repackaged builds) and `postject` rewrites the executable afterwards.

Items 1, 3 and 4 are what a real SBOM would answer; they are deferred to the
CI/SBOM milestone (M12) alongside the codex crate attribution above. Item 2
cannot be closed by us at all, and item 4 cannot be closed by us if the
`code-app` repository is private. The image remains private either way.
